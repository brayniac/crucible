//! Connection state for io_uring driver.
//!
//! Uses a zero-copy send approach: data accumulates in a BytesMut buffer,
//! then chunks are split off and frozen into immutable Bytes for SendZc.
//! This avoids copying between buffers - freeze() is just an Arc increment.

use crate::recv_state::ConnectionRecvState;
use bytes::{Bytes, BytesMut};
use std::os::unix::io::RawFd;

/// Size of send chunks (64KB).
const SEND_CHUNK_SIZE: usize = 64 * 1024;

/// Shrink threshold for fragmentation buffer.
const FRAG_SHRINK_THRESHOLD: usize = 256 * 1024;

/// Default fragmentation buffer capacity.
const FRAG_DEFAULT_CAPACITY: usize = 64 * 1024;

/// Per-connection state for io_uring driver.
///
/// Uses a zero-copy send design: data accumulates in frag_buf (BytesMut),
/// then chunks are split off and frozen into send_chunk (Bytes) for SendZc.
/// The freeze() operation is O(1) - just an Arc increment, no data copy.
pub struct UringConnection {
    /// The socket file descriptor.
    pub raw_fd: RawFd,
    /// Registered fd slot index for io_uring fixed files.
    pub fixed_slot: u32,
    /// Generation counter for detecting stale recv completions.
    pub generation: u32,
    /// Receive state for the with_recv_buf API.
    pub recv_state: ConnectionRecvState,

    // === Send buffer state ===
    /// Current in-flight send chunk (immutable, safe for SendZc).
    /// Bytes is reference-counted and immutable, so the kernel can safely
    /// read from it until we receive the SendZc notification.
    send_chunk: Option<Bytes>,
    /// Offset into send_chunk for partial sends.
    send_pos: usize,
    /// Count of SendZc operations in flight (waiting for notif).
    /// Can be > 1 due to partial sends submitting continuation SendZc.
    sends_in_flight: u32,

    /// Fragmentation buffer for data waiting to be sent.
    /// Grows as needed for large sends, shrinks when cleared.
    frag_buf: BytesMut,

    // === Recv state ===
    /// Whether multishot recv is currently active.
    pub multishot_active: bool,
    /// Whether a single-shot recv is pending (for zero-copy mode).
    pub single_recv_pending: bool,
    /// Whether to use single-shot recv mode (disables multishot).
    pub use_single_recv: bool,
    /// User's destination buffer for single-shot recv.
    pub user_recv_buf: Option<(*mut u8, usize)>,
    /// Number of consecutive re-arm failures for this connection.
    pub rearm_failures: u8,

    // === Connection state ===
    /// Whether this connection is being closed.
    pub closing: bool,
}

// Safety: The user_recv_buf pointer points to memory owned by the Connection's
// IoBuffer, which lives in the same slab slot. When the UringDriver is moved
// between threads, all connections move with it.
unsafe impl Send for UringConnection {}

impl UringConnection {
    /// Create a new connection with the given generation counter.
    pub fn new(raw_fd: RawFd, fixed_slot: u32, generation: u32) -> Self {
        Self {
            raw_fd,
            fixed_slot,
            generation,
            recv_state: ConnectionRecvState::default(),
            send_chunk: None,
            send_pos: 0,
            sends_in_flight: 0,
            frag_buf: BytesMut::with_capacity(FRAG_DEFAULT_CAPACITY),
            multishot_active: false,
            single_recv_pending: false,
            use_single_recv: false,
            user_recv_buf: None,
            rearm_failures: 0,
            closing: false,
        }
    }

    /// Queue data for sending.
    ///
    /// Data is appended to the fragmentation buffer. Call `prepare_send()`
    /// to get data ready for SendZc submission.
    #[inline]
    pub fn queue_send(&mut self, data: &[u8]) {
        self.frag_buf.extend_from_slice(data);
    }

    /// Prepare the next chunk for sending.
    ///
    /// Splits a chunk from frag_buf and freezes it into an immutable Bytes.
    /// This is zero-copy - freeze() is just an Arc increment.
    /// Returns a pointer and length for SendZc. Returns None if:
    /// - A send is already in flight
    /// - No data to send
    #[inline]
    pub fn prepare_send(&mut self) -> Option<(*const u8, usize)> {
        if self.sends_in_flight > 0 {
            return None;
        }

        if self.frag_buf.is_empty() {
            return None;
        }

        // Split off next chunk and freeze it (zero-copy - just Arc increment)
        let chunk_size = self.frag_buf.len().min(SEND_CHUNK_SIZE);
        let chunk = self.frag_buf.split_to(chunk_size).freeze();

        self.send_pos = 0;
        let ptr = chunk.as_ptr();
        let len = chunk.len();
        self.send_chunk = Some(chunk);

        // Shrink frag_buf if it grew too large
        if self.frag_buf.is_empty() && self.frag_buf.capacity() > FRAG_SHRINK_THRESHOLD {
            self.frag_buf = BytesMut::with_capacity(FRAG_DEFAULT_CAPACITY);
        }

        Some((ptr, len))
    }

    /// Mark send as in-flight (called after submitting SendZc).
    #[inline]
    pub fn mark_send_in_flight(&mut self) {
        self.sends_in_flight += 1;
    }

    /// Handle send completion (result, not notif).
    ///
    /// Advances the send position. Returns true if more data remains
    /// in the current send chunk (partial send).
    #[inline]
    pub fn on_send_complete(&mut self, bytes_sent: usize) -> bool {
        self.send_pos += bytes_sent;
        self.send_chunk
            .as_ref()
            .map(|c| self.send_pos < c.len())
            .unwrap_or(false)
    }

    /// Get pointer and length for continuing a partial send.
    #[inline]
    pub fn remaining_send(&self) -> (*const u8, usize) {
        match &self.send_chunk {
            Some(chunk) => {
                let ptr = unsafe { chunk.as_ptr().add(self.send_pos) };
                let len = chunk.len() - self.send_pos;
                (ptr, len)
            }
            None => (std::ptr::null(), 0),
        }
    }

    /// Handle send notification (kernel done with buffer).
    ///
    /// Drops the send chunk (releasing the Bytes) and returns true when
    /// all sends are complete (count reaches 0).
    #[inline]
    pub fn on_send_notif(&mut self) -> bool {
        self.sends_in_flight = self.sends_in_flight.saturating_sub(1);
        if self.sends_in_flight == 0 {
            // Kernel is done with the buffer, release it
            self.send_chunk = None;
            self.send_pos = 0;
            true
        } else {
            false
        }
    }

    /// Check if there's data waiting to be sent.
    #[inline]
    pub fn has_pending_data(&self) -> bool {
        !self.frag_buf.is_empty()
    }

    /// Check if all sends are complete (nothing in flight, no pending data).
    #[inline]
    pub fn all_sends_complete(&self) -> bool {
        self.sends_in_flight == 0 && self.frag_buf.is_empty() && self.send_chunk.is_none()
    }
}
