//! io_uring-based I/O driver for Linux.
//!
//! This module provides a high-performance I/O driver using Linux's io_uring
//! subsystem. It supports advanced features like:
//!
//! - Multishot recv (single submission, multiple completions)
//! - SendZc (zero-copy send with kernel notification)
//! - Ring-provided buffers (kernel-managed buffer pool)
//! - Registered file descriptors (reduced syscall overhead)
//! - Multishot accept (efficient connection acceptance)
//! - UDP recvmsg/sendmsg with ECN support

mod buf_ring;
mod connection;
mod recv_pool;
mod registered_files;

use crate::driver::{IoDriver, RecvBuf};
use crate::recv_state::{BufferSlice, BufferSource};
use crate::types::{
    Completion, CompletionKind, ConnId, ListenerId, RecvMeta, SendMeta, UdpSocketId,
};
use crate::udp::{self, CMSG_BUFFER_SIZE};
use buf_ring::BufRing;
use connection::UringConnection;
use io_uring::cqueue;
use io_uring::opcode::{self, AcceptMulti, RecvMulti, SendZc};
use io_uring::squeue;
use io_uring::types::Fixed;
use io_uring::{IoUring, Probe};
use recv_pool::RecvBufferPool;
use registered_files::RegisteredFiles;
use slab::Slab;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::os::unix::io::{AsRawFd, IntoRawFd, RawFd};
use std::time::Duration;

/// User data encoding for operations.
/// Format: (id << 8) | (buf_idx << 4) | op_type
const OP_SEND: u64 = 1;
const OP_MULTISHOT_RECV: u64 = 2;
const OP_ACCEPT: u64 = 3;
const OP_SINGLE_RECV: u64 = 4;
const OP_UDP_RECVMSG: u64 = 5;
const OP_UDP_SENDMSG: u64 = 6;

/// Buffer group ID for recv operations.
const RECV_BGID: u16 = 0;

/// Encode user_data for io_uring operations.
///
/// Layout (64 bits total):
/// - bits 40-63: generation (24 bits) - for detecting stale completions
/// - bits 8-39: id (32 bits) - connection/listener/socket id
/// - bits 4-7: buf_idx (4 bits) - buffer index for send operations
/// - bits 0-3: op (4 bits) - operation type
#[inline]
fn encode_user_data(id: usize, generation: u32, buf_idx: u8, op: u64) -> u64 {
    ((generation as u64 & 0xFFFFFF) << 40)
        | ((id as u64 & 0xFFFF_FFFF) << 8)
        | ((buf_idx as u64) << 4)
        | op
}

#[inline]
fn decode_user_data(user_data: u64) -> (usize, u32, u8, u64) {
    (
        ((user_data >> 8) & 0xFFFF_FFFF) as usize,
        (user_data >> 40) as u32,
        ((user_data >> 4) & 0xF) as u8,
        user_data & 0xF,
    )
}

/// Encode user_data for pooled single-shot recv.
///
/// Layout (64 bits):
/// - bits 0-3: op (4 bits)
/// - bits 4-19: buffer_id (16 bits) - pool buffer slot
/// - bits 20-39: conn_id (20 bits) - up to ~1M connections
/// - bits 40-63: generation (24 bits)
#[inline]
fn encode_pooled_recv(conn_id: usize, generation: u32, buf_id: u16, op: u64) -> u64 {
    ((generation as u64) << 40) | ((conn_id as u64 & 0xFFFFF) << 20) | ((buf_id as u64) << 4) | op
}

/// Decode user_data for pooled single-shot recv.
#[inline]
fn decode_pooled_recv(user_data: u64) -> (usize, u32, u16, u64) {
    (
        ((user_data >> 20) & 0xFFFFF) as usize, // conn_id (20 bits)
        (user_data >> 40) as u32,               // generation (24 bits)
        ((user_data >> 4) & 0xFFFF) as u16,     // buf_id (16 bits)
        user_data & 0xF,                        // op (4 bits)
    )
}

/// Listener state for io_uring.
struct UringListener {
    raw_fd: RawFd,
    fixed_slot: u32,
    multishot_active: bool,
    /// When true, accepted connections are not auto-registered.
    /// Instead, AcceptRaw completions are emitted with the raw fd.
    raw_mode: bool,
}

/// UDP socket state for io_uring.
struct UringUdpSocket {
    raw_fd: RawFd,
    fixed_slot: u32,
    /// Bound address (used to determine IPv4 vs IPv6 for cmsg parsing).
    bound_addr: SocketAddr,
    /// Whether a recvmsg operation is pending.
    recv_pending: bool,
    /// Whether a sendmsg operation is pending.
    send_pending: bool,
    /// Control message buffer for recvmsg (reused across operations).
    cmsg_buf: Vec<u8>,
    /// sockaddr storage for recvmsg.
    addr_storage: libc::sockaddr_storage,
    /// msghdr for recvmsg (must be kept alive during async operation).
    msghdr: libc::msghdr,
    /// iovec for recvmsg (must be kept alive during async operation).
    iovec: libc::iovec,
}

impl UringUdpSocket {
    fn new(raw_fd: RawFd, fixed_slot: u32, bound_addr: SocketAddr) -> Self {
        Self {
            raw_fd,
            fixed_slot,
            bound_addr,
            recv_pending: false,
            send_pending: false,
            cmsg_buf: vec![0u8; CMSG_BUFFER_SIZE],
            addr_storage: unsafe { mem::zeroed() },
            msghdr: unsafe { mem::zeroed() },
            iovec: unsafe { mem::zeroed() },
        }
    }
}

/// io_uring-based I/O driver.
///
/// Requires Linux 6.0+ for full feature support (multishot recv/accept,
/// SendZc, ring-provided buffers).
pub struct UringDriver {
    ring: IoUring,
    connections: Slab<UringConnection>,
    listeners: Slab<UringListener>,
    udp_sockets: Slab<UringUdpSocket>,
    registered_files: RegisteredFiles,
    buf_ring: BufRing,
    /// Buffer pool for single-shot recv operations.
    ///
    /// Buffers in this pool outlive individual connections, preventing
    /// use-after-free when a connection closes with a pending recv.
    recv_pool: RecvBufferPool,
    pending_completions: Vec<Completion>,
    recv_mode: crate::types::RecvMode,
    /// Generation counter for new connections.
    ///
    /// Incremented each time a new connection is created. Used to detect
    /// stale completions when connection IDs are reused.
    next_generation: u32,
}

// SAFETY: UringDriver can be safely sent between threads.
// The raw pointers in msghdr within UringUdpSocket point to data owned by the same
// UringUdpSocket struct (addr_storage, cmsg_buf, iovec). When the driver is moved,
// all owned data moves together. The driver is designed for single-threaded use
// but can be transferred between threads safely.
unsafe impl Send for UringDriver {}

impl UringDriver {
    /// Create a new io_uring driver with default settings.
    pub fn new() -> io::Result<Self> {
        Self::with_config(
            256,
            16384,
            256,
            8192,
            false,
            crate::types::RecvMode::default(),
        )
    }

    /// Create a new io_uring driver with custom configuration.
    ///
    /// Requires Linux 6.0+ kernel with support for multishot recv/accept,
    /// SendZc, and ring-provided buffers.
    pub fn with_config(
        sq_depth: u32,
        buffer_size: usize,
        buffer_count: u16,
        max_connections: u32,
        sqpoll: bool,
        recv_mode: crate::types::RecvMode,
    ) -> io::Result<Self> {
        // Verify kernel supports required features
        if !is_supported() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring requires Linux 6.0+ with SendZc support",
            ));
        }

        let mut builder = IoUring::builder();
        if sqpoll {
            builder.setup_sqpoll(1000);
        }

        let ring = builder.build(sq_depth)?;

        // Register sparse file table
        let registered_files = RegisteredFiles::new(max_connections);
        ring.submitter().register_files_sparse(max_connections)?;

        // Create buffer ring for multishot recv (needed even in single-shot mode for UDP)
        let ring_entries = buffer_count.next_power_of_two();
        let buf_ring = BufRing::new(ring_entries, buffer_size)?;

        // Register the buffer ring
        unsafe {
            ring.submitter().register_buf_ring_with_flags(
                buf_ring.ring_addr(),
                ring_entries,
                RECV_BGID,
                0,
            )?;
        }

        // Create recv buffer pool for single-shot mode.
        // Pool buffers outlive connections to prevent use-after-free.
        let recv_pool = RecvBufferPool::new(ring_entries, buffer_size);

        Ok(Self {
            ring,
            connections: Slab::with_capacity(max_connections as usize),
            listeners: Slab::with_capacity(16),
            udp_sockets: Slab::with_capacity(64),
            registered_files,
            buf_ring,
            recv_pool,
            pending_completions: Vec::with_capacity(256),
            recv_mode,
            next_generation: 0,
        })
    }

    /// Submit a multishot recv for a connection.
    fn submit_multishot_recv(
        &mut self,
        conn_id: usize,
        generation: u32,
        fixed_slot: u32,
    ) -> io::Result<()> {
        let recv_op = RecvMulti::new(Fixed(fixed_slot), RECV_BGID)
            .build()
            .user_data(encode_user_data(conn_id, generation, 0, OP_MULTISHOT_RECV));

        unsafe {
            self.ring
                .submission()
                .push(&recv_op)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    /// Submit a single-shot recv for a connection (internal helper).
    /// Data will be appended to recv_state on completion.
    fn submit_single_recv_internal(
        &mut self,
        conn_id: usize,
        generation: u32,
        fixed_slot: u32,
    ) -> io::Result<()> {
        // Check if recv is already pending
        if let Some(conn) = self.connections.get(conn_id)
            && conn.single_recv_pending
        {
            return Ok(()); // Already have a recv pending
        }

        // Allocate a buffer from the pool
        let (buf_id, pool_ptr, pool_len) = self
            .recv_pool
            .alloc(conn_id, generation)
            .ok_or_else(|| io::Error::other("recv buffer pool exhausted"))?;

        // Submit single-shot recv with pool buffer
        let recv_op = opcode::Recv::new(Fixed(fixed_slot), pool_ptr, pool_len as u32)
            .build()
            .user_data(encode_pooled_recv(
                conn_id,
                generation,
                buf_id,
                OP_SINGLE_RECV,
            ));

        unsafe {
            if self.ring.submission().push(&recv_op).is_err() {
                self.recv_pool.free(buf_id);
                return Err(io::Error::other("SQ full"));
            }
        }

        // Mark recv as pending
        if let Some(conn) = self.connections.get_mut(conn_id) {
            conn.single_recv_pending = true;
        }

        Ok(())
    }

    /// Submit a multishot accept for a listener.
    fn submit_multishot_accept(&mut self, listener_id: usize, fixed_slot: u32) -> io::Result<()> {
        let accept_op = AcceptMulti::new(Fixed(fixed_slot))
            .build()
            .user_data(encode_user_data(listener_id, 0, 0, OP_ACCEPT));

        unsafe {
            self.ring
                .submission()
                .push(&accept_op)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    /// Submit a SendZc operation.
    fn submit_send_zc(
        &mut self,
        conn_id: usize,
        fixed_slot: u32,
        buf_idx: u8,
        data: &[u8],
    ) -> io::Result<()> {
        let send_op = SendZc::new(Fixed(fixed_slot), data.as_ptr(), data.len() as u32)
            .build()
            .user_data(encode_user_data(conn_id, 0, buf_idx, OP_SEND));

        unsafe {
            self.ring
                .submission()
                .push(&send_op)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    /// Process a completion queue entry.
    fn process_cqe(&mut self, cqe: io_uring::cqueue::Entry) {
        let user_data = cqe.user_data();
        let result = cqe.result();
        let flags = cqe.flags();

        // Extract op type first (bottom 4 bits)
        let op = user_data & 0xF;

        match op {
            OP_MULTISHOT_RECV => {
                let (id, generation, _, _) = decode_user_data(user_data);
                self.handle_multishot_recv(id, generation, result, flags);
            }
            OP_SEND => {
                let (id, _, buf_idx, _) = decode_user_data(user_data);
                self.handle_send(id, buf_idx as usize, result, flags);
            }
            OP_ACCEPT => {
                let (id, _, _, _) = decode_user_data(user_data);
                self.handle_accept(id, result, flags);
            }
            OP_SINGLE_RECV => {
                // Single-shot recv uses pooled buffers - decode all fields
                let (conn_id, generation, buf_id, _) = decode_pooled_recv(user_data);
                self.handle_single_recv(conn_id, generation, buf_id, result);
            }
            OP_UDP_RECVMSG => {
                let (id, _, _, _) = decode_user_data(user_data);
                self.handle_udp_recvmsg(id, result);
            }
            OP_UDP_SENDMSG => {
                let (id, _, _, _) = decode_user_data(user_data);
                self.handle_udp_sendmsg(id, result);
            }
            _ => {}
        }
    }

    fn handle_multishot_recv(&mut self, conn_id: usize, generation: u32, result: i32, flags: u32) {
        // Validate connection exists and generation matches to detect stale completions
        match self.connections.get(conn_id) {
            Some(c) if c.generation == generation => {}
            Some(_) => {
                // Stale completion from old connection - return buffer and ignore
                if let Some(buf_id) = cqueue::buffer_select(flags) {
                    self.buf_ring.return_buffer(buf_id);
                }
                return;
            }
            None => {
                // Connection no longer exists - return buffer and ignore
                if let Some(buf_id) = cqueue::buffer_select(flags) {
                    self.buf_ring.return_buffer(buf_id);
                }
                return;
            }
        }
        // Create ConnId with validated generation for completions
        let full_conn_id = ConnId::with_generation(conn_id, generation);

        if result == 0 {
            // Clean EOF - client closed connection gracefully
            if let Some(buf_id) = cqueue::buffer_select(flags) {
                self.buf_ring.return_buffer(buf_id);
            }
            self.pending_completions
                .push(Completion::new(CompletionKind::Closed {
                    conn_id: full_conn_id,
                }));
            return;
        }

        if result < 0 {
            // Recv error
            if let Some(buf_id) = cqueue::buffer_select(flags) {
                self.buf_ring.return_buffer(buf_id);
            }
            self.pending_completions
                .push(Completion::new(CompletionKind::Error {
                    conn_id: full_conn_id,
                    error: io::Error::from_raw_os_error(-result),
                }));
            return;
        }

        // Get buffer ID from completion
        let buf_id = match cqueue::buffer_select(flags) {
            Some(id) => id,
            None => return,
        };

        let n = result as usize;

        // Multishot mode: copy data and return buffer immediately
        // Zero-copy isn't practical for multishot because the ring has limited buffers
        // (typically 256) and holding them would quickly exhaust the ring, blocking
        // further recvs. The kernel continuously delivers completions, so we must
        // return buffers promptly to keep the pipeline flowing.
        let buf_data = &self.buf_ring.get(buf_id)[..n];
        if let Some(conn) = self.connections.get_mut(conn_id) {
            conn.recv_state.append_owned(buf_data);
        }

        // Return buffer to ring immediately so kernel can reuse it
        self.buf_ring.return_buffer(buf_id);

        self.pending_completions
            .push(Completion::new(CompletionKind::Recv {
                conn_id: full_conn_id,
            }));

        // Re-arm multishot if needed
        if !cqueue::more(flags) {
            let conn_info = self
                .connections
                .get(conn_id)
                .map(|c| (c.fixed_slot, c.generation));
            if let Some((fixed_slot, conn_gen)) = conn_info {
                if let Some(conn) = self.connections.get_mut(conn_id) {
                    conn.multishot_active = false;
                }
                if self
                    .submit_multishot_recv(conn_id, conn_gen, fixed_slot)
                    .is_ok()
                    && let Some(conn) = self.connections.get_mut(conn_id)
                {
                    conn.multishot_active = true;
                } else {
                    // Failed to re-arm multishot recv - connection would be orphaned.
                    // Emit an error so the server can clean up.
                    self.pending_completions
                        .push(Completion::new(CompletionKind::Error {
                            conn_id: full_conn_id,
                            error: io::Error::other("failed to re-arm multishot recv"),
                        }));
                }
            }
        }
    }

    fn handle_single_recv(&mut self, conn_id: usize, generation: u32, buf_id: u16, result: i32) {
        // IMPORTANT: Always free the pool buffer, even for stale/error completions.
        // The pool buffer is safe to access because it outlives connections.
        // This is the key safety property that prevents use-after-free.

        // Check if this is a stale completion from a closed connection.
        // With pooled buffers, stale completions are safe - the data was written
        // to a pool buffer (still valid), not a freed connection buffer.
        let conn = match self.connections.get_mut(conn_id) {
            Some(c) if c.generation == generation => c,
            Some(_) => {
                // Generation mismatch - stale completion from old connection.
                // Data was safely written to pool buffer, just discard it.
                self.recv_pool.free(buf_id);
                return;
            }
            None => {
                // Connection doesn't exist - stale completion.
                // Data was safely written to pool buffer, just discard it.
                self.recv_pool.free(buf_id);
                return;
            }
        };

        // Clear pending flag and get user buffer info
        conn.single_recv_pending = false;
        let user_buf = conn.user_recv_buf.take();

        if result == 0 {
            // EOF - peer closed connection
            self.recv_pool.free(buf_id);
            self.pending_completions
                .push(Completion::new(CompletionKind::Closed {
                    conn_id: ConnId::new(conn_id),
                }));
            return;
        }

        if result < 0 {
            // Recv error
            self.recv_pool.free(buf_id);
            self.pending_completions
                .push(Completion::new(CompletionKind::Error {
                    conn_id: ConnId::new(conn_id),
                    error: io::Error::from_raw_os_error(-result),
                }));
            return;
        }

        // Success - zero-copy handoff of pool buffer
        let bytes = result as usize;

        // Get data from pool buffer (validated by conn_id/generation)
        if let Some(pool_data) = self.recv_pool.get(buf_id, conn_id, generation) {
            let data = &pool_data[..bytes];

            // Copy to user's buffer if provided (legacy submit_recv API)
            if let Some((user_ptr, user_len)) = user_buf {
                let copy_len = std::cmp::min(bytes, user_len);
                // Safety: user_ptr is valid because connection still exists,
                // and the IoBuffer it points to cannot reallocate while loaned.
                unsafe {
                    std::ptr::copy_nonoverlapping(data.as_ptr(), user_ptr, copy_len);
                }
            }

            // Zero-copy handoff to recv_state - buffer is NOT freed here.
            // It will be freed when the user consumes the data via with_recv_buf.
            if let Some(conn) = self.connections.get_mut(conn_id) {
                let slice = BufferSlice::from_pool(buf_id, data.as_ptr(), bytes);
                conn.recv_state.on_recv(slice);
            }
        } else {
            // Buffer validation failed - free it
            self.recv_pool.free(buf_id);
        }

        // Generate Recv completion (same as multishot) since data is in recv_state
        self.pending_completions
            .push(Completion::new(CompletionKind::Recv {
                conn_id: ConnId::new(conn_id),
            }));

        // Note: next recv is submitted in poll() to handle SQ full cases
    }

    fn handle_send(&mut self, conn_id: usize, _buf_idx: usize, result: i32, flags: u32) {
        // Check if this is a SendZc notification
        if cqueue::notif(flags) {
            // Decrement in-flight count and check if all sends are done
            let (_all_done, should_continue, should_finish_close) =
                if let Some(conn) = self.connections.get_mut(conn_id) {
                    let all_done = conn.on_send_notif();
                    let should_close = conn.closing && conn.all_sends_complete();
                    // Only continue when ALL notifs received (all_done) to avoid corrupting send_buf
                    let should_continue = all_done && !conn.closing && conn.has_pending_data();
                    (all_done, should_continue, should_close)
                } else {
                    (false, false, false)
                };

            if should_finish_close {
                self.finish_deferred_close(conn_id);
            } else if should_continue {
                // All sends complete, more data to send - prepare next chunk
                self.continue_send(conn_id);
            }
            // If !all_done, we're still waiting for more notifs before we can reuse send_buf
            return;
        }

        // Check if connection exists and is not closing
        let is_closing = self
            .connections
            .get(conn_id)
            .map(|c| c.closing)
            .unwrap_or(true);

        if is_closing {
            // Connection is closing or doesn't exist, ignore send result
            return;
        }

        if result <= 0 {
            // Send error (result == 0 means connection closed, result < 0 is error)
            let error = if result == 0 {
                io::Error::new(io::ErrorKind::WriteZero, "send returned 0")
            } else {
                io::Error::from_raw_os_error(-result)
            };
            self.pending_completions
                .push(Completion::new(CompletionKind::Error {
                    conn_id: ConnId::new(conn_id),
                    error,
                }));
            return;
        }

        let n = result as usize;

        // Handle partial send - continue sending remaining data in current chunk
        let has_remaining = if let Some(conn) = self.connections.get_mut(conn_id) {
            conn.on_send_complete(n)
        } else {
            false
        };

        if has_remaining {
            // Partial send - continue with remaining data in send buffer
            if let Some(conn) = self.connections.get_mut(conn_id) {
                let (ptr, len) = conn.remaining_send();
                let fixed_slot = conn.fixed_slot;
                // Mark this continuation as in-flight (will get its own notif)
                conn.mark_send_in_flight();
                let send_data = unsafe { std::slice::from_raw_parts(ptr, len) };
                let _ = self.submit_send_zc(conn_id, fixed_slot, 0, send_data);
            }
        }

        self.pending_completions
            .push(Completion::new(CompletionKind::SendReady {
                conn_id: ConnId::new(conn_id),
            }));
    }

    /// Continue sending from fragmentation buffer after notif.
    fn continue_send(&mut self, conn_id: usize) {
        if let Some(conn) = self.connections.get_mut(conn_id)
            && let Some((ptr, len)) = conn.prepare_send()
        {
            let fixed_slot = conn.fixed_slot;
            conn.mark_send_in_flight();

            let send_data = unsafe { std::slice::from_raw_parts(ptr, len) };
            let _ = self.submit_send_zc(conn_id, fixed_slot, 0, send_data);
        }
    }

    fn handle_accept(&mut self, listener_id: usize, result: i32, flags: u32) {
        if !self.listeners.contains(listener_id) {
            return;
        }

        if result < 0 {
            self.pending_completions
                .push(Completion::new(CompletionKind::ListenerError {
                    listener_id: ListenerId::new(listener_id),
                    error: io::Error::from_raw_os_error(-result),
                }));
            return;
        }

        let new_fd = result;

        // Set TCP_NODELAY
        unsafe {
            let optval: libc::c_int = 1;
            libc::setsockopt(
                new_fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        // Check if this listener is in raw mode
        let raw_mode = self
            .listeners
            .get(listener_id)
            .map(|l| l.raw_mode)
            .unwrap_or(false);

        if raw_mode {
            // Raw mode: don't register, just emit AcceptRaw with the fd
            let addr = unsafe {
                let mut storage: libc::sockaddr_storage = std::mem::zeroed();
                let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                libc::getpeername(
                    new_fd,
                    &mut storage as *mut _ as *mut libc::sockaddr,
                    &mut len,
                );
                sockaddr_to_socketaddr(&storage)
            };

            self.pending_completions
                .push(Completion::new(CompletionKind::AcceptRaw {
                    listener_id: ListenerId::new(listener_id),
                    raw_fd: new_fd,
                    addr,
                }));
        } else {
            // Normal mode: register the connection automatically
            // Allocate a registered fd slot
            let fixed_slot = match self.registered_files.alloc() {
                Some(slot) => slot,
                None => {
                    unsafe { libc::close(new_fd) };
                    return;
                }
            };

            // Register the fd
            let fds = [new_fd];
            if self
                .ring
                .submitter()
                .register_files_update(fixed_slot, &fds)
                .is_err()
            {
                self.registered_files.free(fixed_slot);
                unsafe { libc::close(new_fd) };
                return;
            }

            // Create connection
            let entry = self.connections.vacant_entry();
            let conn_id = entry.key();

            let generation = self.next_generation;
            self.next_generation = self.next_generation.wrapping_add(1);
            let conn = UringConnection::new(new_fd, fixed_slot, generation);
            entry.insert(conn);

            // Don't auto-submit multishot recv - let caller decide via submit_recv()
            // This enables zero-copy recv when caller uses submit_recv()

            // Get peer address
            let addr = unsafe {
                let mut storage: libc::sockaddr_storage = std::mem::zeroed();
                let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                libc::getpeername(
                    new_fd,
                    &mut storage as *mut _ as *mut libc::sockaddr,
                    &mut len,
                );
                sockaddr_to_socketaddr(&storage)
            };

            self.pending_completions
                .push(Completion::new(CompletionKind::Accept {
                    listener_id: ListenerId::new(listener_id),
                    conn_id: ConnId::new(conn_id),
                    addr,
                }));
        }

        // Re-arm multishot accept if needed
        if !cqueue::more(flags) {
            let fixed_slot = self.listeners.get(listener_id).map(|l| l.fixed_slot);
            if let Some(fixed_slot) = fixed_slot {
                if let Some(listener) = self.listeners.get_mut(listener_id) {
                    listener.multishot_active = false;
                }
                if self
                    .submit_multishot_accept(listener_id, fixed_slot)
                    .is_ok()
                    && let Some(listener) = self.listeners.get_mut(listener_id)
                {
                    listener.multishot_active = true;
                }
            }
        }
    }

    fn handle_udp_recvmsg(&mut self, socket_id: usize, result: i32) {
        // Clear pending flag
        if let Some(sock) = self.udp_sockets.get_mut(socket_id) {
            sock.recv_pending = false;
        }

        if !self.udp_sockets.contains(socket_id) {
            return;
        }

        if result < 0 {
            // recvmsg error
            self.pending_completions
                .push(Completion::new(CompletionKind::UdpError {
                    socket_id: UdpSocketId::new(socket_id),
                    error: io::Error::from_raw_os_error(-result),
                }));
            return;
        }

        let len = result as usize;

        // Parse control messages to extract ECN and local address
        let meta = if let Some(sock) = self.udp_sockets.get(socket_id) {
            // Parse source address from msghdr
            let source = udp::sockaddr_to_std(&sock.addr_storage, sock.msghdr.msg_namelen)
                .unwrap_or(sock.bound_addr);

            // Parse control messages
            udp::parse_control_messages(&sock.msghdr, source, len)
        } else {
            return;
        };

        self.pending_completions
            .push(Completion::new(CompletionKind::RecvMsgComplete {
                socket_id: UdpSocketId::new(socket_id),
                meta,
            }));
    }

    fn handle_udp_sendmsg(&mut self, socket_id: usize, result: i32) {
        // Clear pending flag
        if let Some(sock) = self.udp_sockets.get_mut(socket_id) {
            sock.send_pending = false;
        }

        if !self.udp_sockets.contains(socket_id) {
            return;
        }

        if result < 0 {
            // sendmsg error
            self.pending_completions
                .push(Completion::new(CompletionKind::UdpError {
                    socket_id: UdpSocketId::new(socket_id),
                    error: io::Error::from_raw_os_error(-result),
                }));
            return;
        }

        self.pending_completions
            .push(Completion::new(CompletionKind::SendMsgComplete {
                socket_id: UdpSocketId::new(socket_id),
                bytes: result as usize,
            }));
    }

    /// Complete deferred close for a connection after all sends finish.
    fn finish_deferred_close(&mut self, conn_id: usize) {
        if let Some(conn) = self.connections.try_remove(conn_id) {
            // fd was already closed and deregistered in close()
            // Just drop the connection to free the send buffers
            drop(conn);
        }
    }
}

impl IoDriver for UringDriver {
    fn listen(&mut self, addr: SocketAddr, backlog: u32) -> io::Result<ListenerId> {
        // Create socket
        let socket = socket2::Socket::new(
            match addr {
                SocketAddr::V4(_) => socket2::Domain::IPV4,
                SocketAddr::V6(_) => socket2::Domain::IPV6,
            },
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;

        socket.set_reuse_address(true)?;

        // Enable SO_REUSEPORT
        let fd = socket.as_raw_fd();
        let optval: libc::c_int = 1;
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        socket.listen(backlog as i32)?;

        let raw_fd = socket.into_raw_fd();

        // Allocate registered fd slot
        let fixed_slot = self
            .registered_files
            .alloc()
            .ok_or_else(|| io::Error::other("no free fd slots"))?;

        // Register the fd
        let fds = [raw_fd];
        if let Err(e) = self
            .ring
            .submitter()
            .register_files_update(fixed_slot, &fds)
        {
            self.registered_files.free(fixed_slot);
            unsafe { libc::close(raw_fd) };
            return Err(e);
        }

        let entry = self.listeners.vacant_entry();
        let id = entry.key();

        let listener = UringListener {
            raw_fd,
            fixed_slot,
            multishot_active: false,
            raw_mode: false,
        };
        entry.insert(listener);

        // Submit multishot accept - if this fails, clean up and return error
        if let Err(e) = self.submit_multishot_accept(id, fixed_slot) {
            self.listeners.try_remove(id);
            let fds = [-1i32];
            let _ = self
                .ring
                .submitter()
                .register_files_update(fixed_slot, &fds);
            self.registered_files.free(fixed_slot);
            unsafe { libc::close(raw_fd) };
            return Err(e);
        }

        if let Some(listener) = self.listeners.get_mut(id) {
            listener.multishot_active = true;
        }

        self.ring.submit()?;

        Ok(ListenerId::new(id))
    }

    fn listen_raw(&mut self, addr: SocketAddr, backlog: u32) -> io::Result<ListenerId> {
        // Create socket
        let socket = socket2::Socket::new(
            match addr {
                SocketAddr::V4(_) => socket2::Domain::IPV4,
                SocketAddr::V6(_) => socket2::Domain::IPV6,
            },
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;

        socket.set_reuse_address(true)?;
        // Note: NO SO_REUSEPORT for raw mode - single acceptor pattern
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        socket.listen(backlog as i32)?;

        let raw_fd = socket.into_raw_fd();

        // Allocate registered fd slot
        let fixed_slot = self
            .registered_files
            .alloc()
            .ok_or_else(|| io::Error::other("no free fd slots"))?;

        // Register the fd
        let fds = [raw_fd];
        if let Err(e) = self
            .ring
            .submitter()
            .register_files_update(fixed_slot, &fds)
        {
            self.registered_files.free(fixed_slot);
            unsafe { libc::close(raw_fd) };
            return Err(e);
        }

        let entry = self.listeners.vacant_entry();
        let id = entry.key();

        let listener = UringListener {
            raw_fd,
            fixed_slot,
            multishot_active: false,
            raw_mode: true,
        };
        entry.insert(listener);

        // Submit multishot accept - if this fails, clean up and return error
        if let Err(e) = self.submit_multishot_accept(id, fixed_slot) {
            self.listeners.try_remove(id);
            let fds = [-1i32];
            let _ = self
                .ring
                .submitter()
                .register_files_update(fixed_slot, &fds);
            self.registered_files.free(fixed_slot);
            unsafe { libc::close(raw_fd) };
            return Err(e);
        }

        if let Some(listener) = self.listeners.get_mut(id) {
            listener.multishot_active = true;
        }

        self.ring.submit()?;

        Ok(ListenerId::new(id))
    }

    fn close_listener(&mut self, id: ListenerId) -> io::Result<()> {
        if let Some(listener) = self.listeners.try_remove(id.as_usize()) {
            // Update registered fd to -1
            let fds = [-1i32];
            let _ = self
                .ring
                .submitter()
                .register_files_update(listener.fixed_slot, &fds);
            self.registered_files.free(listener.fixed_slot);
            unsafe { libc::close(listener.raw_fd) };
        }
        Ok(())
    }

    fn register(&mut self, stream: TcpStream) -> io::Result<ConnId> {
        stream.set_nonblocking(true)?;
        let raw_fd = stream.into_raw_fd();

        // Set TCP_NODELAY
        unsafe {
            let optval: libc::c_int = 1;
            libc::setsockopt(
                raw_fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        // Allocate registered fd slot
        let fixed_slot = self
            .registered_files
            .alloc()
            .ok_or_else(|| io::Error::other("no free fd slots"))?;

        // Register the fd
        let fds = [raw_fd];
        if let Err(e) = self
            .ring
            .submitter()
            .register_files_update(fixed_slot, &fds)
        {
            self.registered_files.free(fixed_slot);
            unsafe { libc::close(raw_fd) };
            return Err(e);
        }

        let entry = self.connections.vacant_entry();
        let conn_id = entry.key();

        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);
        let conn = UringConnection::new(raw_fd, fixed_slot, generation);
        entry.insert(conn);

        // Automatically start receiving data
        let recv_result = if self.recv_mode == crate::types::RecvMode::Multishot {
            let result = self.submit_multishot_recv(conn_id, generation, fixed_slot);
            if result.is_ok()
                && let Some(conn) = self.connections.get_mut(conn_id)
            {
                conn.multishot_active = true;
            }
            result
        } else {
            self.submit_single_recv_internal(conn_id, generation, fixed_slot)
        };

        if let Err(e) = recv_result {
            self.connections.try_remove(conn_id);
            let fds = [-1i32];
            let _ = self
                .ring
                .submitter()
                .register_files_update(fixed_slot, &fds);
            self.registered_files.free(fixed_slot);
            unsafe { libc::close(raw_fd) };
            return Err(e);
        }

        Ok(ConnId::new(conn_id))
    }

    fn register_fd(&mut self, raw_fd: RawFd) -> io::Result<ConnId> {
        // Set non-blocking
        unsafe {
            let flags = libc::fcntl(raw_fd, libc::F_GETFL);
            libc::fcntl(raw_fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        // TCP_NODELAY is already set by handle_accept in raw mode

        // Allocate registered fd slot
        let fixed_slot = self
            .registered_files
            .alloc()
            .ok_or_else(|| io::Error::other("no free fd slots"))?;

        // Register the fd
        let fds = [raw_fd];
        if let Err(e) = self
            .ring
            .submitter()
            .register_files_update(fixed_slot, &fds)
        {
            self.registered_files.free(fixed_slot);
            unsafe { libc::close(raw_fd) };
            return Err(e);
        }

        let entry = self.connections.vacant_entry();
        let conn_id = entry.key();

        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);
        let conn = UringConnection::new(raw_fd, fixed_slot, generation);
        entry.insert(conn);

        // Automatically start receiving data
        let recv_result = if self.recv_mode == crate::types::RecvMode::Multishot {
            let result = self.submit_multishot_recv(conn_id, generation, fixed_slot);
            if result.is_ok()
                && let Some(conn) = self.connections.get_mut(conn_id)
            {
                conn.multishot_active = true;
            }
            result
        } else {
            self.submit_single_recv_internal(conn_id, generation, fixed_slot)
        };

        if let Err(e) = recv_result {
            self.connections.try_remove(conn_id);
            let fds = [-1i32];
            let _ = self
                .ring
                .submitter()
                .register_files_update(fixed_slot, &fds);
            self.registered_files.free(fixed_slot);
            unsafe { libc::close(raw_fd) };
            return Err(e);
        }

        Ok(ConnId::new(conn_id))
    }

    fn close(&mut self, id: ConnId) -> io::Result<()> {
        let conn_id = id.as_usize();

        // Check if connection exists and has in-flight sends
        let has_in_flight = self
            .connections
            .get(conn_id)
            .map(|c| !c.all_sends_complete())
            .unwrap_or(false);

        if has_in_flight {
            // Defer full cleanup until SendZc notifs arrive.
            // Close the fd and deregister, but keep connection in slab
            // so the send buffers remain valid for the kernel.
            if let Some(conn) = self.connections.get_mut(conn_id) {
                conn.closing = true;

                // Return any held recv buffers to their pools
                conn.recv_state.clear();
                for pending in conn.recv_state.take_pending_returns() {
                    match pending.source {
                        BufferSource::Ring => {
                            self.buf_ring.return_buffer(pending.buf_id);
                        }
                        BufferSource::Pool => {
                            self.recv_pool.free(pending.buf_id);
                        }
                    }
                }

                // Deregister and close fd - kernel will see errors but that's ok
                let fds = [-1i32];
                let _ = self
                    .ring
                    .submitter()
                    .register_files_update(conn.fixed_slot, &fds);
                self.registered_files.free(conn.fixed_slot);
                unsafe { libc::close(conn.raw_fd) };
            }
        } else {
            // No in-flight sends, safe to remove immediately
            if let Some(mut conn) = self.connections.try_remove(conn_id) {
                // Return any held recv buffers to their pools
                conn.recv_state.clear();
                for pending in conn.recv_state.take_pending_returns() {
                    match pending.source {
                        BufferSource::Ring => {
                            self.buf_ring.return_buffer(pending.buf_id);
                        }
                        BufferSource::Pool => {
                            self.recv_pool.free(pending.buf_id);
                        }
                    }
                }

                // Update registered fd to -1
                let fds = [-1i32];
                let _ = self
                    .ring
                    .submitter()
                    .register_files_update(conn.fixed_slot, &fds);
                self.registered_files.free(conn.fixed_slot);
                unsafe { libc::close(conn.raw_fd) };
            }
        }
        Ok(())
    }

    fn take_fd(&mut self, id: ConnId) -> io::Result<RawFd> {
        if let Some(mut conn) = self.connections.try_remove(id.as_usize()) {
            // Return any held recv buffers to their pools
            conn.recv_state.clear();
            for pending in conn.recv_state.take_pending_returns() {
                match pending.source {
                    BufferSource::Ring => {
                        self.buf_ring.return_buffer(pending.buf_id);
                    }
                    BufferSource::Pool => {
                        self.recv_pool.free(pending.buf_id);
                    }
                }
            }

            // Update registered fd to -1 (deregister)
            let fds = [-1i32];
            let _ = self
                .ring
                .submitter()
                .register_files_update(conn.fixed_slot, &fds);
            self.registered_files.free(conn.fixed_slot);
            // Return the fd WITHOUT closing it
            Ok(conn.raw_fd)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "connection not found",
            ))
        }
    }

    fn send(&mut self, id: ConnId, data: &[u8]) -> io::Result<usize> {
        let conn_id = id.as_usize();
        let conn = self
            .connections
            .get_mut(conn_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

        // Don't allow sending to closing connections
        if conn.closing {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection is closing",
            ));
        }

        // Queue data in fragmentation buffer
        conn.queue_send(data);

        // Try to start sending if not already in flight
        if let Some((ptr, len)) = conn.prepare_send() {
            let fixed_slot = conn.fixed_slot;
            conn.mark_send_in_flight();

            let send_data = unsafe { std::slice::from_raw_parts(ptr, len) };
            self.submit_send_zc(conn_id, fixed_slot, 0, send_data)?;
        }

        Ok(data.len())
    }

    fn recv(&mut self, id: ConnId, buf: &mut [u8]) -> io::Result<usize> {
        // Note: Prefer with_recv_buf() for zero-copy access.
        // This method copies data from recv_state to the provided buffer.
        let (n, pending_returns): (usize, Vec<_>) = {
            let conn = self
                .connections
                .get_mut(id.as_usize())
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

            let available = conn.recv_state.as_slice();
            if available.is_empty() {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }

            let n = std::cmp::min(buf.len(), available.len());
            buf[..n].copy_from_slice(&available[..n]);
            conn.recv_state.consume(n);

            (n, conn.recv_state.take_pending_returns().collect())
        };

        // Return consumed buffers to their respective pools
        for pending in pending_returns {
            match pending.source {
                BufferSource::Ring => {
                    self.buf_ring.return_buffer(pending.buf_id);
                }
                BufferSource::Pool => {
                    self.recv_pool.free(pending.buf_id);
                }
            }
        }

        Ok(n)
    }

    fn submit_recv(&mut self, id: ConnId, buf: &mut [u8]) -> io::Result<()> {
        let conn_id = id.as_usize();
        let conn = self
            .connections
            .get_mut(conn_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

        // Only one recv can be pending at a time
        if conn.single_recv_pending {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        // Mark this connection as using single-shot mode (disables multishot)
        conn.use_single_recv = true;

        let fixed_slot = conn.fixed_slot;
        let generation = conn.generation;

        // Store user's buffer pointer for copying data on completion.
        // Safety: The pointer remains valid as long as the connection exists
        // because it points into the connection's IoBuffer which cannot
        // reallocate while loaned to the kernel.
        conn.user_recv_buf = Some((buf.as_mut_ptr(), buf.len()));

        // Allocate a buffer from the pool instead of using the user's buffer.
        // Pool buffers outlive connections, preventing use-after-free if the
        // connection closes while recv is pending.
        let (buf_id, pool_ptr, pool_len) = self
            .recv_pool
            .alloc(conn_id, generation)
            .ok_or_else(|| io::Error::other("recv buffer pool exhausted"))?;

        // Use the smaller of pool buffer size and user buffer size
        let recv_len = std::cmp::min(pool_len, buf.len()) as u32;

        // Submit single-shot recv with pool buffer.
        // Encode pool buffer ID in user_data for completion handling.
        let recv_op = opcode::Recv::new(Fixed(fixed_slot), pool_ptr, recv_len)
            .build()
            .user_data(encode_pooled_recv(
                conn_id,
                generation,
                buf_id,
                OP_SINGLE_RECV,
            ));

        unsafe {
            if self.ring.submission().push(&recv_op).is_err() {
                // Failed to submit - free the pool buffer
                self.recv_pool.free(buf_id);
                if let Some(conn) = self.connections.get_mut(conn_id) {
                    conn.user_recv_buf = None;
                }
                return Err(io::Error::other("SQ full"));
            }
        }

        // Mark recv as pending
        if let Some(conn) = self.connections.get_mut(conn_id) {
            conn.single_recv_pending = true;
        }

        Ok(())
    }

    fn with_recv_buf(&mut self, id: ConnId, f: &mut dyn FnMut(&mut dyn RecvBuf)) -> io::Result<()> {
        // Collect pending returns after running callback
        let pending_returns: Vec<_> = {
            let conn = self
                .connections
                .get_mut(id.as_usize())
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

            // Create a wrapper that implements RecvBuf using the connection's recv_state
            struct UringRecvBuf<'a> {
                state: &'a mut crate::recv_state::ConnectionRecvState,
            }

            impl RecvBuf for UringRecvBuf<'_> {
                fn as_slice(&self) -> &[u8] {
                    self.state.as_slice()
                }

                fn len(&self) -> usize {
                    self.state.available()
                }

                fn consume(&mut self, n: usize) {
                    self.state.consume(n);
                }
            }

            let mut buf = UringRecvBuf {
                state: &mut conn.recv_state,
            };
            f(&mut buf);

            // Collect any buffers that were fully consumed and need to be returned
            conn.recv_state.take_pending_returns().collect()
        };

        // Return consumed buffers to their respective pools
        for pending in pending_returns {
            match pending.source {
                BufferSource::Ring => {
                    self.buf_ring.return_buffer(pending.buf_id);
                }
                BufferSource::Pool => {
                    self.recv_pool.free(pending.buf_id);
                }
            }
        }

        Ok(())
    }

    fn poll(&mut self, timeout: Option<Duration>) -> io::Result<usize> {
        self.pending_completions.clear();

        // Re-arm recv for connections that need it
        // This handles both single-shot mode (no pending recv) and multishot mode
        // (multishot deactivated due to re-arm failure)
        let to_rearm: Vec<_> = self
            .connections
            .iter()
            .filter(|(_, c)| !c.single_recv_pending && !c.multishot_active)
            .map(|(id, c)| (id, c.generation, c.fixed_slot, c.rearm_failures))
            .collect();

        // Maximum consecutive re-arm failures before emitting an error
        const MAX_REARM_FAILURES: u8 = 10;

        for (conn_id, generation, fixed_slot, failures) in to_rearm {
            let success = if self.recv_mode == crate::types::RecvMode::SingleShot {
                self.submit_single_recv_internal(conn_id, generation, fixed_slot)
                    .is_ok()
            } else {
                // Multishot mode - re-arm multishot recv
                self.submit_multishot_recv(conn_id, generation, fixed_slot)
                    .is_ok()
            };

            if let Some(conn) = self.connections.get_mut(conn_id) {
                if success {
                    conn.rearm_failures = 0;
                    if self.recv_mode == crate::types::RecvMode::Multishot {
                        conn.multishot_active = true;
                    }
                } else {
                    conn.rearm_failures = failures.saturating_add(1);

                    // If we've failed too many times, emit an error so the caller
                    // knows this connection is stuck and can take action
                    if conn.rearm_failures >= MAX_REARM_FAILURES {
                        self.pending_completions
                            .push(Completion::new(CompletionKind::Error {
                                conn_id: ConnId::new(conn_id),
                                error: io::Error::other(
                                    "failed to re-arm recv: buffer pool exhausted or SQ full",
                                ),
                            }));
                        // Reset counter to avoid spamming errors
                        conn.rearm_failures = 0;
                    }
                }
            }
        }

        // Submit pending operations
        self.ring.submit()?;

        // Wait for completions
        if let Some(t) = timeout {
            let ts = io_uring::types::Timespec::new()
                .sec(t.as_secs())
                .nsec(t.subsec_nanos());
            let args = io_uring::types::SubmitArgs::new().timespec(&ts);
            let _ = self.ring.submitter().submit_with_args(1, &args);
        } else {
            self.ring.submit_and_wait(1)?;
        }

        // Collect completions
        let cqes: Vec<_> = self.ring.completion().collect();
        let count = cqes.len();

        for cqe in cqes {
            self.process_cqe(cqe);
        }

        Ok(count)
    }

    fn drain_completions(&mut self) -> Vec<Completion> {
        std::mem::take(&mut self.pending_completions)
    }

    fn connection_count(&self) -> usize {
        self.connections.len()
    }

    fn listener_count(&self) -> usize {
        self.listeners.len()
    }

    fn raw_fd(&self, id: ConnId) -> Option<RawFd> {
        self.connections.get(id.as_usize()).map(|c| c.raw_fd)
    }

    // === UDP socket operations ===

    fn bind_udp(&mut self, addr: SocketAddr) -> io::Result<UdpSocketId> {
        // Create UDP socket
        let socket = socket2::Socket::new(
            match addr {
                SocketAddr::V4(_) => socket2::Domain::IPV4,
                SocketAddr::V6(_) => socket2::Domain::IPV6,
            },
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )?;

        socket.set_reuse_address(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;

        let raw_fd = socket.into_raw_fd();

        // Configure socket for ECN and pktinfo
        udp::configure_socket(raw_fd, &addr)?;

        // Allocate registered fd slot
        let fixed_slot = self
            .registered_files
            .alloc()
            .ok_or_else(|| io::Error::other("no free fd slots"))?;

        // Register the fd
        let fds = [raw_fd];
        if let Err(e) = self
            .ring
            .submitter()
            .register_files_update(fixed_slot, &fds)
        {
            self.registered_files.free(fixed_slot);
            unsafe { libc::close(raw_fd) };
            return Err(e);
        }

        let entry = self.udp_sockets.vacant_entry();
        let id = entry.key();

        entry.insert(UringUdpSocket::new(raw_fd, fixed_slot, addr));

        Ok(UdpSocketId::new(id))
    }

    fn close_udp(&mut self, id: UdpSocketId) -> io::Result<()> {
        if let Some(sock) = self.udp_sockets.try_remove(id.as_usize()) {
            // Update registered fd to -1
            let fds = [-1i32];
            let _ = self
                .ring
                .submitter()
                .register_files_update(sock.fixed_slot, &fds);
            self.registered_files.free(sock.fixed_slot);
            unsafe { libc::close(sock.raw_fd) };
        }
        Ok(())
    }

    fn submit_recvmsg(&mut self, id: UdpSocketId, buf: &mut [u8]) -> io::Result<()> {
        let socket_id = id.as_usize();
        let sock = self
            .udp_sockets
            .get_mut(socket_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "UDP socket not found"))?;

        if sock.recv_pending {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        // Set up iovec
        sock.iovec.iov_base = buf.as_mut_ptr() as *mut libc::c_void;
        sock.iovec.iov_len = buf.len();

        // Set up msghdr
        sock.msghdr.msg_name = &mut sock.addr_storage as *mut _ as *mut libc::c_void;
        sock.msghdr.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        sock.msghdr.msg_iov = &mut sock.iovec;
        sock.msghdr.msg_iovlen = 1;
        sock.msghdr.msg_control = sock.cmsg_buf.as_mut_ptr() as *mut libc::c_void;
        sock.msghdr.msg_controllen = sock.cmsg_buf.len();
        sock.msghdr.msg_flags = 0;

        let fixed_slot = sock.fixed_slot;
        let msghdr_ptr = &sock.msghdr as *const libc::msghdr;

        // Submit recvmsg operation
        let recvmsg_op = opcode::RecvMsg::new(Fixed(fixed_slot), msghdr_ptr as *mut _)
            .build()
            .user_data(encode_user_data(socket_id, 0, 0, OP_UDP_RECVMSG));

        unsafe {
            self.ring
                .submission()
                .push(&recvmsg_op)
                .map_err(|_| io::Error::other("SQ full"))?;
        }

        // Mark recv as pending
        if let Some(sock) = self.udp_sockets.get_mut(socket_id) {
            sock.recv_pending = true;
        }

        Ok(())
    }

    fn submit_sendmsg(&mut self, id: UdpSocketId, data: &[u8], meta: &SendMeta) -> io::Result<()> {
        let socket_id = id.as_usize();
        let sock = self
            .udp_sockets
            .get_mut(socket_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "UDP socket not found"))?;

        if sock.send_pending {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        // Set ECN on the socket
        udp::set_ecn(sock.raw_fd, &meta.dest, meta.ecn)?;

        // Convert destination address
        let (dest_storage, dest_len) = udp::std_to_sockaddr(&meta.dest);
        sock.addr_storage = dest_storage;

        // Set up iovec
        sock.iovec.iov_base = data.as_ptr() as *mut libc::c_void;
        sock.iovec.iov_len = data.len();

        // Set up msghdr
        sock.msghdr.msg_name = &mut sock.addr_storage as *mut _ as *mut libc::c_void;
        sock.msghdr.msg_namelen = dest_len;
        sock.msghdr.msg_iov = &mut sock.iovec;
        sock.msghdr.msg_iovlen = 1;
        sock.msghdr.msg_control = std::ptr::null_mut();
        sock.msghdr.msg_controllen = 0;
        sock.msghdr.msg_flags = 0;

        let fixed_slot = sock.fixed_slot;
        let msghdr_ptr = &sock.msghdr as *const libc::msghdr;

        // Submit sendmsg operation
        let sendmsg_op = opcode::SendMsg::new(Fixed(fixed_slot), msghdr_ptr as *const _)
            .build()
            .user_data(encode_user_data(socket_id, 0, 0, OP_UDP_SENDMSG));

        unsafe {
            self.ring
                .submission()
                .push(&sendmsg_op)
                .map_err(|_| io::Error::other("SQ full"))?;
        }

        // Mark send as pending
        if let Some(sock) = self.udp_sockets.get_mut(socket_id) {
            sock.send_pending = true;
        }

        Ok(())
    }

    fn recvmsg(&mut self, id: UdpSocketId, buf: &mut [u8]) -> io::Result<RecvMeta> {
        let sock = self
            .udp_sockets
            .get_mut(id.as_usize())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "UDP socket not found"))?;

        // Set up for synchronous recvmsg
        let mut iovec = libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        };

        let mut addr_storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
        let mut cmsg_buf = [0u8; CMSG_BUFFER_SIZE];

        let mut msghdr: libc::msghdr = unsafe { mem::zeroed() };
        msghdr.msg_name = &mut addr_storage as *mut _ as *mut libc::c_void;
        msghdr.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        msghdr.msg_iov = &mut iovec;
        msghdr.msg_iovlen = 1;
        msghdr.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
        msghdr.msg_controllen = cmsg_buf.len();
        msghdr.msg_flags = 0;

        let result = unsafe { libc::recvmsg(sock.raw_fd, &mut msghdr, 0) };

        if result < 0 {
            return Err(io::Error::last_os_error());
        }

        let len = result as usize;

        // Parse source address
        let source =
            udp::sockaddr_to_std(&addr_storage, msghdr.msg_namelen).unwrap_or(sock.bound_addr);

        // Parse control messages
        Ok(udp::parse_control_messages(&msghdr, source, len))
    }

    fn sendmsg(&mut self, id: UdpSocketId, data: &[u8], meta: &SendMeta) -> io::Result<usize> {
        let sock = self
            .udp_sockets
            .get(id.as_usize())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "UDP socket not found"))?;

        // Set ECN on the socket
        udp::set_ecn(sock.raw_fd, &meta.dest, meta.ecn)?;

        // Convert destination address
        let (dest_storage, dest_len) = udp::std_to_sockaddr(&meta.dest);

        let mut iovec = libc::iovec {
            iov_base: data.as_ptr() as *mut libc::c_void,
            iov_len: data.len(),
        };

        let mut msghdr: libc::msghdr = unsafe { mem::zeroed() };
        msghdr.msg_name = &dest_storage as *const _ as *mut libc::c_void;
        msghdr.msg_namelen = dest_len;
        msghdr.msg_iov = &mut iovec;
        msghdr.msg_iovlen = 1;
        msghdr.msg_control = std::ptr::null_mut();
        msghdr.msg_controllen = 0;
        msghdr.msg_flags = 0;

        let result = unsafe { libc::sendmsg(sock.raw_fd, &msghdr, 0) };

        if result < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(result as usize)
    }

    fn udp_socket_count(&self) -> usize {
        self.udp_sockets.len()
    }

    fn udp_raw_fd(&self, id: UdpSocketId) -> Option<RawFd> {
        self.udp_sockets.get(id.as_usize()).map(|s| s.raw_fd)
    }
}

/// Check if io_uring is supported on this system.
///
/// Returns true only if the kernel supports all required features:
/// - SendZc (zero-copy send) - requires Linux 6.0+
/// - Multishot recv
/// - Multishot accept
/// - Ring-provided buffers
pub fn is_supported() -> bool {
    // Try to create a small ring and probe for SendZc (6.0+ indicator)
    match IoUring::<squeue::Entry, cqueue::Entry>::builder().build(8) {
        Ok(ring) => {
            let mut probe = Probe::new();
            if ring.submitter().register_probe(&mut probe).is_err() {
                return false;
            }
            // SendZc requires 6.0+, which implies all other features we need
            probe.is_supported(opcode::SendZc::CODE)
        }
        Err(_) => false,
    }
}

/// Convert a sockaddr_storage to SocketAddr.
unsafe fn sockaddr_to_socketaddr(storage: &libc::sockaddr_storage) -> SocketAddr {
    match storage.ss_family as i32 {
        libc::AF_INET => {
            let addr = unsafe { *(storage as *const _ as *const libc::sockaddr_in) };
            SocketAddr::V4(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr)),
                u16::from_be(addr.sin_port),
            ))
        }
        libc::AF_INET6 => {
            let addr = unsafe { *(storage as *const _ as *const libc::sockaddr_in6) };
            SocketAddr::V6(std::net::SocketAddrV6::new(
                std::net::Ipv6Addr::from(addr.sin6_addr.s6_addr),
                u16::from_be(addr.sin6_port),
                addr.sin6_flowinfo,
                addr.sin6_scope_id,
            ))
        }
        _ => SocketAddr::V4(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::UNSPECIFIED,
            0,
        )),
    }
}
