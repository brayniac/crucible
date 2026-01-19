//! Connection state for io_uring driver.
//!
//! Uses a pipelined zero-copy send approach: data accumulates in a BytesMut buffer,
//! then chunks are split off and frozen into immutable Bytes for SendZc.
//! Multiple chunks can be in-flight simultaneously for better throughput.

use crate::recv_state::ConnectionRecvState;
use bytes::{Bytes, BytesMut};
use std::os::unix::io::RawFd;

/// Size of send chunks (64KB).
const SEND_CHUNK_SIZE: usize = 64 * 1024;

/// Shrink threshold for fragmentation buffer.
const FRAG_SHRINK_THRESHOLD: usize = 256 * 1024;

/// Default fragmentation buffer capacity.
const FRAG_DEFAULT_CAPACITY: usize = 64 * 1024;

/// Maximum number of send operations in flight per connection.
/// Uses 4 bits in user_data encoding, so max is 15.
pub const MAX_SENDS_IN_FLIGHT: usize = 8;

/// Maximum number of iovecs for scatter-gather sends.
pub const MAX_IOVECS: usize = 4;

/// An in-flight send operation.
pub struct InFlightSend {
    /// The buffer(s) being sent. Keeps data alive until NOTIF.
    pub buffers: SendBuffers,
    /// Position for partial sends (only used for Single).
    pub pos: usize,
    /// Number of NOTIF messages still expected.
    pub notifs_pending: u32,
}

/// Buffers for an in-flight send.
pub enum SendBuffers {
    /// Single contiguous buffer (from frag_buf freeze or owned).
    Single(Bytes),
    /// Multiple buffers for scatter-gather sends.
    /// The iovecs and msghdr are stored inline to avoid allocation.
    Vectored(VectoredSend),
}

/// State for a vectored (scatter-gather) send operation.
pub struct VectoredSend {
    /// Owned buffers that must stay alive until NOTIF.
    pub buffers: [Option<Bytes>; MAX_IOVECS],
    /// Number of valid buffers.
    pub count: usize,
    /// iovecs for SendMsg (point into buffers).
    pub iovecs: [libc::iovec; MAX_IOVECS],
    /// msghdr for SendMsg.
    pub msghdr: libc::msghdr,
}

impl VectoredSend {
    /// Create a new vectored send from owned Bytes buffers.
    pub fn new(buffers: Vec<Bytes>) -> Self {
        let mut result = Self {
            buffers: [None, None, None, None],
            count: 0,
            iovecs: [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; MAX_IOVECS],
            msghdr: unsafe { std::mem::zeroed() },
        };

        for (i, buf) in buffers.into_iter().take(MAX_IOVECS).enumerate() {
            result.iovecs[i] = libc::iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            };
            result.buffers[i] = Some(buf);
            result.count = i + 1;
        }

        // Set up msghdr to point to iovecs
        result.msghdr.msg_iov = result.iovecs.as_mut_ptr();
        result.msghdr.msg_iovlen = result.count;

        result
    }

    /// Get the msghdr pointer for SendMsg.
    pub fn msghdr_ptr(&self) -> *const libc::msghdr {
        &self.msghdr as *const _
    }

    /// Get total bytes across all buffers.
    pub fn total_len(&self) -> usize {
        self.buffers[..self.count]
            .iter()
            .filter_map(|b| b.as_ref())
            .map(|b| b.len())
            .sum()
    }
}

/// Per-connection state for io_uring driver.
///
/// Uses a pipelined zero-copy send design: data accumulates in frag_buf (BytesMut),
/// then chunks are split off and frozen into send_slots for SendZc.
/// Multiple sends can be in-flight simultaneously for better throughput.
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
    /// In-flight send operations, indexed by slot.
    /// Each slot can hold a single or vectored send.
    send_slots: [Option<InFlightSend>; MAX_SENDS_IN_FLIGHT],
    /// Bitmap of free slots (bit N = slot N is free).
    free_slots: u8,
    /// Count of sends in flight (for quick checks).
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
    /// Whether a direct recv (into raw pointer) is pending.
    pub direct_recv_pending: bool,
    /// Raw pointer for direct recv (true zero-copy into segment memory).
    pub direct_recv_ptr: Option<(*mut u8, usize)>,
    /// Whether a TCP recvmsg is pending.
    pub tcp_recvmsg_pending: bool,
    /// iovec storage for TCP recvmsg (kept valid during async operation).
    pub tcp_recvmsg_iovecs: [libc::iovec; MAX_TCP_RECVMSG_IOVECS],
    /// Number of valid iovecs in tcp_recvmsg_iovecs.
    pub tcp_recvmsg_iovec_count: usize,
    /// msghdr for TCP recvmsg (must be kept alive during async operation).
    pub tcp_recvmsg_msghdr: libc::msghdr,
    /// Number of consecutive re-arm failures for this connection.
    pub rearm_failures: u8,

    // === Connection state ===
    /// Whether this connection is being closed.
    pub closing: bool,
}

/// Maximum number of iovecs for TCP recvmsg scatter-gather receive.
pub const MAX_TCP_RECVMSG_IOVECS: usize = 4;

// Safety: The user_recv_buf pointer points to memory owned by the Connection's
// IoBuffer, which lives in the same slab slot. When the UringDriver is moved
// between threads, all connections move with it.
// The iovecs in VectoredSend point to Bytes buffers owned by the same struct.
unsafe impl Send for UringConnection {}

const EMPTY_SEND_SLOT: Option<InFlightSend> = None;

impl UringConnection {
    /// Create a new connection with the given generation counter.
    pub fn new(raw_fd: RawFd, fixed_slot: u32, generation: u32) -> Self {
        Self {
            raw_fd,
            fixed_slot,
            generation,
            recv_state: ConnectionRecvState::default(),
            send_slots: [EMPTY_SEND_SLOT; MAX_SENDS_IN_FLIGHT],
            free_slots: 0xFF, // All 8 slots free
            sends_in_flight: 0,
            frag_buf: BytesMut::with_capacity(FRAG_DEFAULT_CAPACITY),
            multishot_active: false,
            single_recv_pending: false,
            use_single_recv: false,
            user_recv_buf: None,
            direct_recv_pending: false,
            direct_recv_ptr: None,
            tcp_recvmsg_pending: false,
            tcp_recvmsg_iovecs: [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }; MAX_TCP_RECVMSG_IOVECS],
            tcp_recvmsg_iovec_count: 0,
            tcp_recvmsg_msghdr: unsafe { std::mem::zeroed() },
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

    /// Allocate a send slot. Returns the slot index, or None if all slots are in use.
    #[inline]
    fn alloc_send_slot(&mut self) -> Option<u8> {
        if self.free_slots == 0 {
            return None;
        }
        let slot = self.free_slots.trailing_zeros() as u8;
        self.free_slots &= !(1 << slot);
        Some(slot)
    }

    /// Free a send slot.
    #[inline]
    fn free_send_slot(&mut self, slot: u8) {
        debug_assert!(slot < MAX_SENDS_IN_FLIGHT as u8);
        self.send_slots[slot as usize] = None;
        self.free_slots |= 1 << slot;
    }

    /// Prepare the next chunk for sending.
    ///
    /// Splits a chunk from frag_buf and freezes it into an immutable Bytes.
    /// This is zero-copy - freeze() is just an Arc increment.
    /// Returns (slot_index, pointer, length) for SendZc. Returns None if:
    /// - All send slots are in use
    /// - No data to send
    #[inline]
    pub fn prepare_send(&mut self) -> Option<(u8, *const u8, usize)> {
        if self.frag_buf.is_empty() {
            return None;
        }

        let slot = self.alloc_send_slot()?;

        // Split off next chunk and freeze it (zero-copy - just Arc increment)
        let chunk_size = self.frag_buf.len().min(SEND_CHUNK_SIZE);
        let chunk = self.frag_buf.split_to(chunk_size).freeze();

        let ptr = chunk.as_ptr();
        let len = chunk.len();

        self.send_slots[slot as usize] = Some(InFlightSend {
            buffers: SendBuffers::Single(chunk),
            pos: 0,
            notifs_pending: 1,
        });
        self.sends_in_flight += 1;

        // Shrink frag_buf if it grew too large
        if self.frag_buf.is_empty() && self.frag_buf.capacity() > FRAG_SHRINK_THRESHOLD {
            self.frag_buf = BytesMut::with_capacity(FRAG_DEFAULT_CAPACITY);
        }

        Some((slot, ptr, len))
    }

    /// Prepare the next chunk for regular (non-zero-copy) sending.
    ///
    /// Like `prepare_send()` but sets notifs_pending = 0 since regular sends
    /// don't generate a notification - the slot can be freed immediately
    /// when the send completes.
    #[inline]
    pub fn prepare_send_regular(&mut self) -> Option<(u8, *const u8, usize)> {
        if self.frag_buf.is_empty() {
            return None;
        }

        let slot = self.alloc_send_slot()?;

        let chunk_size = self.frag_buf.len().min(SEND_CHUNK_SIZE);
        let chunk = self.frag_buf.split_to(chunk_size).freeze();

        let ptr = chunk.as_ptr();
        let len = chunk.len();

        self.send_slots[slot as usize] = Some(InFlightSend {
            buffers: SendBuffers::Single(chunk),
            pos: 0,
            notifs_pending: 0, // No notification expected for regular send
        });
        self.sends_in_flight += 1;

        if self.frag_buf.is_empty() && self.frag_buf.capacity() > FRAG_SHRINK_THRESHOLD {
            self.frag_buf = BytesMut::with_capacity(FRAG_DEFAULT_CAPACITY);
        }

        Some((slot, ptr, len))
    }

    /// Free a send slot for regular (non-zero-copy) sends.
    ///
    /// For regular sends, we free the slot immediately when the send completes
    /// (no notification to wait for). This combines freeing the slot and
    /// decrementing sends_in_flight.
    #[inline]
    pub fn free_send_slot_regular(&mut self, slot: u8) {
        self.free_send_slot(slot);
        self.sends_in_flight = self.sends_in_flight.saturating_sub(1);
    }

    /// Prepare a vectored (scatter-gather) send from owned Bytes buffers.
    ///
    /// Returns (slot_index, msghdr_ptr, total_len) for SendMsg. Returns None if:
    /// - All send slots are in use
    /// - No buffers provided
    #[inline]
    pub fn prepare_vectored_send(
        &mut self,
        buffers: Vec<Bytes>,
    ) -> Option<(u8, *const libc::msghdr, usize)> {
        let slot = self.alloc_send_slot()?;

        let vectored = VectoredSend::new(buffers);
        if vectored.count == 0 {
            self.free_slots |= 1 << slot; // Return slot
            return None;
        }

        let msghdr_ptr = vectored.msghdr_ptr();
        let total_len = vectored.total_len();

        self.send_slots[slot as usize] = Some(InFlightSend {
            buffers: SendBuffers::Vectored(vectored),
            pos: 0,
            notifs_pending: 1,
        });
        self.sends_in_flight += 1;

        Some((slot, msghdr_ptr, total_len))
    }

    /// Get the send slot for updating on completion.
    #[inline]
    pub fn get_send_slot_mut(&mut self, slot: u8) -> Option<&mut InFlightSend> {
        self.send_slots.get_mut(slot as usize)?.as_mut()
    }

    /// Increment notifs_pending for a slot (e.g., for partial send continuation).
    #[inline]
    pub fn increment_notifs(&mut self, slot: u8) {
        if let Some(send) = self
            .send_slots
            .get_mut(slot as usize)
            .and_then(|s| s.as_mut())
        {
            send.notifs_pending += 1;
        }
    }

    /// Handle send completion (result, not notif).
    ///
    /// Advances the send position. Returns true if more data remains
    /// in the current send chunk (partial send).
    #[inline]
    pub fn on_send_complete(&mut self, slot: u8, bytes_sent: usize) -> bool {
        if let Some(send) = self
            .send_slots
            .get_mut(slot as usize)
            .and_then(|s| s.as_mut())
        {
            send.pos += bytes_sent;
            match &send.buffers {
                SendBuffers::Single(chunk) => send.pos < chunk.len(),
                SendBuffers::Vectored(v) => send.pos < v.total_len(),
            }
        } else {
            false
        }
    }

    /// Get pointer and length for continuing a partial send.
    #[inline]
    pub fn remaining_send(&self, slot: u8) -> (*const u8, usize) {
        if let Some(Some(send)) = self.send_slots.get(slot as usize) {
            match &send.buffers {
                SendBuffers::Single(chunk) => {
                    if send.pos < chunk.len() {
                        let ptr = unsafe { chunk.as_ptr().add(send.pos) };
                        let len = chunk.len() - send.pos;
                        return (ptr, len);
                    }
                }
                SendBuffers::Vectored(_) => {
                    // Partial sends for vectored are more complex - not supported yet
                    // For now, the kernel handles partial vectored sends internally
                }
            }
        }
        (std::ptr::null(), 0)
    }

    /// Handle send notification (kernel done with buffer).
    ///
    /// Returns (slot_freed, should_continue) where:
    /// - slot_freed: true if this slot is now free (all notifs received)
    /// - should_continue: true if there's pending data to send
    #[inline]
    pub fn on_send_notif(&mut self, slot: u8) -> (bool, bool) {
        let slot_freed = if let Some(send) = self
            .send_slots
            .get_mut(slot as usize)
            .and_then(|s| s.as_mut())
        {
            send.notifs_pending = send.notifs_pending.saturating_sub(1);
            if send.notifs_pending == 0 {
                self.free_send_slot(slot);
                self.sends_in_flight = self.sends_in_flight.saturating_sub(1);
                true
            } else {
                false
            }
        } else {
            false
        };

        let should_continue = slot_freed && !self.closing && self.has_pending_data();
        (slot_freed, should_continue)
    }

    /// Check if there's data waiting to be sent.
    #[inline]
    pub fn has_pending_data(&self) -> bool {
        !self.frag_buf.is_empty()
    }

    /// Check if all sends are complete (nothing in flight, no pending data).
    #[inline]
    pub fn all_sends_complete(&self) -> bool {
        self.sends_in_flight == 0 && self.frag_buf.is_empty()
    }

    /// Check if we can accept more send operations.
    #[inline]
    pub fn can_send(&self) -> bool {
        self.free_slots != 0
    }

    /// Get the number of sends in flight.
    #[inline]
    pub fn sends_in_flight(&self) -> u32 {
        self.sends_in_flight
    }
}
