//! Connection state for io_uring driver.
//!
//! Implements double-buffering for SendZc safety - when a zero-copy send
//! is in flight, we cannot modify the buffer until we receive the kernel
//! notification that it's done with the data.

use crate::recv_state::ConnectionRecvState;
use std::os::unix::io::RawFd;

/// Per-connection state for io_uring driver.
///
/// Uses double buffering for write buffers to avoid corruption when
/// SendZc is in flight while new data needs to be sent.
pub struct UringConnection {
    /// The socket file descriptor.
    pub raw_fd: RawFd,
    /// Registered fd slot index for io_uring fixed files.
    pub fixed_slot: u32,
    /// Generation counter for detecting stale recv completions.
    ///
    /// When a connection is created, it gets a unique generation number.
    /// This is encoded in the user_data of single-shot recv operations.
    /// On completion, we check if the generation matches to detect stale
    /// completions from a previous connection that reused the same slab slot.
    pub generation: u32,
    /// Receive state for the with_recv_buf API.
    pub recv_state: ConnectionRecvState,
    /// Double send buffers for async send safety.
    send_bufs: [Vec<u8>; 2],
    /// How much of each send buffer has been sent.
    send_pos: [usize; 2],
    /// Count of in-flight SendZc operations per buffer.
    ///
    /// Each SendZc generates a notif when the kernel releases the buffer.
    /// With partial sends, multiple SendZc operations can reference the same buffer.
    /// Buffer is only safe to reuse when count reaches 0.
    in_flight_count: [u8; 2],
    /// Which buffer to use for the next send (0 or 1).
    current_buf: usize,
    /// Whether multishot recv is currently active.
    pub multishot_active: bool,
    /// Whether a single-shot recv is pending (for zero-copy mode).
    pub single_recv_pending: bool,
    /// Whether to use single-shot recv mode (disables multishot).
    pub use_single_recv: bool,
    /// User's destination buffer for single-shot recv (pointer + length).
    ///
    /// When a pooled recv completes successfully, data is copied from the
    /// pool buffer to this user buffer. The pointer remains valid as long
    /// as the connection exists because it points into the connection's
    /// IoBuffer which cannot reallocate while loaned.
    pub user_recv_buf: Option<(*mut u8, usize)>,
    /// Number of consecutive re-arm failures for this connection.
    ///
    /// Reset to 0 when recv is successfully submitted. If this exceeds
    /// a threshold, an error completion is emitted to notify the caller
    /// that this connection is stuck.
    pub rearm_failures: u8,
}

// Safety: The user_recv_buf pointer points to memory owned by the Connection's
// IoBuffer, which lives in the same slab slot. When the UringDriver is moved
// between threads, all connections move with it. The pointer is only dereferenced
// when the connection still exists (validated by generation check).
unsafe impl Send for UringConnection {}

impl UringConnection {
    /// Create a new connection with the given generation counter.
    pub fn new(raw_fd: RawFd, fixed_slot: u32, generation: u32) -> Self {
        Self {
            raw_fd,
            fixed_slot,
            generation,
            recv_state: ConnectionRecvState::default(),
            // 64KB per buffer to handle large response backlogs
            send_bufs: [Vec::with_capacity(65536), Vec::with_capacity(65536)],
            send_pos: [0, 0],
            in_flight_count: [0, 0],
            current_buf: 0,
            multishot_active: false,
            single_recv_pending: false,
            use_single_recv: false,
            user_recv_buf: None,
            rearm_failures: 0,
        }
    }

    /// Check if we can send data (at least one buffer is available).
    #[inline]
    #[allow(dead_code)]
    pub fn can_send(&self) -> bool {
        self.in_flight_count[0] == 0 || self.in_flight_count[1] == 0
    }

    /// Get an available buffer index for sending, or None if both are in flight.
    #[inline]
    fn get_available_buf(&self) -> Option<usize> {
        if self.in_flight_count[self.current_buf] == 0 {
            Some(self.current_buf)
        } else if self.in_flight_count[1 - self.current_buf] == 0 {
            Some(1 - self.current_buf)
        } else {
            None
        }
    }

    /// Append data to a send buffer.
    ///
    /// Returns the buffer index used, or None if no buffer was available.
    #[inline]
    pub fn append_send_data(&mut self, data: &[u8]) -> Option<usize> {
        let buf_idx = self.get_available_buf()?;

        // If this buffer was fully sent, reset it
        if self.send_pos[buf_idx] >= self.send_bufs[buf_idx].len() {
            self.send_bufs[buf_idx].clear();
            self.send_pos[buf_idx] = 0;
        }

        self.send_bufs[buf_idx].extend_from_slice(data);
        self.current_buf = buf_idx;

        Some(buf_idx)
    }

    /// Check if there's data waiting to be sent in the specified buffer.
    #[inline]
    pub fn has_pending_send(&self, buf_idx: usize) -> bool {
        self.send_pos[buf_idx] < self.send_bufs[buf_idx].len()
    }

    /// Get a stable pointer to the pending send data.
    ///
    /// # Safety
    /// The returned pointer is valid as long as in_flight_count[buf_idx] > 0,
    /// which prevents the buffer from being cleared or reallocated.
    #[inline]
    pub fn send_buf_ptr(&self, buf_idx: usize) -> *const u8 {
        unsafe { self.send_bufs[buf_idx].as_ptr().add(self.send_pos[buf_idx]) }
    }

    /// Get the length of pending send data.
    #[inline]
    pub fn pending_send_len(&self, buf_idx: usize) -> usize {
        self.send_bufs[buf_idx].len() - self.send_pos[buf_idx]
    }

    /// Get the send position for a buffer.
    #[inline]
    pub fn send_pos(&self, buf_idx: usize) -> usize {
        self.send_pos[buf_idx]
    }

    /// Get the total length of data in a send buffer.
    #[inline]
    pub fn send_buf_len(&self, buf_idx: usize) -> usize {
        self.send_bufs[buf_idx].len()
    }

    /// Increment the in-flight count for a buffer (called when submitting SendZc).
    #[inline]
    pub fn increment_in_flight(&mut self, buf_idx: usize) {
        self.in_flight_count[buf_idx] = self.in_flight_count[buf_idx].saturating_add(1);
    }

    /// Decrement the in-flight count for a buffer (called when notif is received).
    ///
    /// Buffer becomes available for reuse when count reaches 0.
    #[inline]
    pub fn decrement_in_flight(&mut self, buf_idx: usize) {
        self.in_flight_count[buf_idx] = self.in_flight_count[buf_idx].saturating_sub(1);
    }

    /// Check if a buffer is in flight (has pending SendZc operations).
    #[inline]
    #[allow(dead_code)]
    pub fn is_in_flight(&self, buf_idx: usize) -> bool {
        self.in_flight_count[buf_idx] > 0
    }

    /// Mark some send data as sent for the specified buffer.
    #[inline]
    pub fn advance_send(&mut self, buf_idx: usize, n: usize) {
        self.send_pos[buf_idx] += n;
    }
}
