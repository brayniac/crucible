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
    Completion, CompletionKind, ConnId, DriverCapabilities, ListenerId, RecvMeta, SendMeta,
    SendMode, UdpSocketId,
};
use crate::udp::{self, CMSG_BUFFER_SIZE};
use crate::zero_copy::BoxedZeroCopy;
use arrayvec::ArrayVec;
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
const OP_SENDMSG: u64 = 7; // TCP scatter-gather send
const OP_ZC_SENDMSG: u64 = 8; // Zero-copy sendmsg via send_owned
const OP_DIRECT_RECV: u64 = 9; // Zero-copy recv into raw pointer
const OP_TCP_RECVMSG: u64 = 10; // TCP scatter-gather recvmsg

/// Maximum number of iovecs for zero-copy sends.
/// 8 covers typical responses (header + value + trailer = 3) with room to spare.
const MAX_ZC_IOVECS: usize = 8;

/// Pending zero-copy send that owns the buffer and supporting structures.
///
/// Stored in a Slab for O(1) allocation without heap boxing per send.
/// The buffer owns the data (e.g., holds ItemRef), and iovecs point into it.
/// Uses ArrayVec for inline iovec storage, avoiding heap allocation.
struct PendingZcSend {
    /// The buffer being sent (owns the data, e.g., holds ItemRef).
    /// Not read directly - exists to keep data alive until send completes.
    #[allow(dead_code)]
    buffer: BoxedZeroCopy,
    /// iovec array pointing into buffer's data (inline, no heap allocation).
    iovecs: ArrayVec<libc::iovec, MAX_ZC_IOVECS>,
    /// msghdr for sendmsg.
    msghdr: libc::msghdr,
}

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

/// Encode user_data for zero-copy vectored send.
///
/// Layout (64 bits):
/// - bits 0-3: op (4 bits)
/// - bits 4-19: slab_idx (16 bits) - pending send slab index
/// - bits 20-39: conn_id (20 bits) - up to ~1M connections
/// - bits 40-63: generation (24 bits)
#[inline]
fn encode_zc_sendmsg(conn_id: usize, generation: u32, slab_idx: usize, op: u64) -> u64 {
    ((generation as u64) << 40)
        | ((conn_id as u64 & 0xFFFFF) << 20)
        | ((slab_idx as u64 & 0xFFFF) << 4)
        | op
}

/// Decode user_data for zero-copy vectored send.
#[inline]
fn decode_zc_sendmsg(user_data: u64) -> (usize, u32, usize, u64) {
    (
        ((user_data >> 20) & 0xFFFFF) as usize, // conn_id (20 bits)
        (user_data >> 40) as u32,               // generation (24 bits)
        ((user_data >> 4) & 0xFFFF) as usize,   // slab_idx (16 bits)
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
    /// Scratch buffer for collecting CQEs during poll (reused to avoid allocation).
    cqe_scratch: Vec<io_uring::cqueue::Entry>,
    /// Scratch buffer for collecting connections needing recv re-arm (reused to avoid allocation).
    rearm_scratch: Vec<(usize, u32, u32, u8)>,
    recv_mode: crate::types::RecvMode,
    /// Generation counter for new connections.
    ///
    /// Incremented each time a new connection is created. Used to detect
    /// stale completions when connection IDs are reused.
    next_generation: u32,
    /// Probed capabilities for this driver instance.
    capabilities: DriverCapabilities,
    /// Configured send mode.
    send_mode: SendMode,
    /// Pending zero-copy sends awaiting completion.
    /// Slab provides O(1) insert/remove and stable addresses for iovecs.
    pending_zc_sends: Slab<PendingZcSend>,
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

        // Probe capabilities
        let capabilities = probe_capabilities(&ring);

        Ok(Self {
            ring,
            connections: Slab::with_capacity(max_connections as usize),
            listeners: Slab::with_capacity(16),
            udp_sockets: Slab::with_capacity(64),
            registered_files,
            buf_ring,
            recv_pool,
            pending_completions: Vec::with_capacity(8192),
            cqe_scratch: Vec::with_capacity(8192),
            rearm_scratch: Vec::with_capacity(8192),
            recv_mode,
            next_generation: 0,
            capabilities,
            send_mode: SendMode::default(),
            pending_zc_sends: Slab::with_capacity(256),
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
        self.submit_send_zc_flags(conn_id, fixed_slot, buf_idx, data, 0)
    }

    /// Submit a SendZc operation with optional flags (MSG_MORE, etc).
    fn submit_send_zc_flags(
        &mut self,
        conn_id: usize,
        fixed_slot: u32,
        buf_idx: u8,
        data: &[u8],
        msg_flags: i32,
    ) -> io::Result<()> {
        let mut send_builder = SendZc::new(Fixed(fixed_slot), data.as_ptr(), data.len() as u32);
        if msg_flags != 0 {
            send_builder = send_builder.flags(msg_flags);
        }
        let send_op = send_builder
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

    /// Submit a SendMsgZc operation for TCP scatter-gather with zero-copy.
    ///
    /// Uses IORING_OP_SENDMSG_ZC which sends data directly from user buffers
    /// without copying to kernel socket buffers. A NOTIF completion is sent
    /// when the kernel is done with the buffers.
    fn submit_tcp_sendmsg_zc(
        &mut self,
        conn_id: usize,
        fixed_slot: u32,
        slot: u8,
        msghdr: *const libc::msghdr,
    ) -> io::Result<()> {
        let sendmsg_op = opcode::SendMsgZc::new(Fixed(fixed_slot), msghdr as *const _)
            .build()
            .user_data(encode_user_data(conn_id, 0, slot, OP_SENDMSG));

        unsafe {
            self.ring
                .submission()
                .push(&sendmsg_op)
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
            OP_SENDMSG => {
                // Regular TCP scatter-gather send (not via send_owned)
                let (id, _, buf_idx, _) = decode_user_data(user_data);
                self.handle_send(id, buf_idx as usize, result, flags);
            }
            OP_ZC_SENDMSG => {
                // Zero-copy sendmsg completion from send_owned
                let (conn_id, _generation, slab_idx, _) = decode_zc_sendmsg(user_data);

                // Remove and drop the PendingZcSend, releasing the BoxedZeroCopy
                // which in turn releases the ItemRef or other held resources
                if self.pending_zc_sends.contains(slab_idx) {
                    let _ = self.pending_zc_sends.remove(slab_idx);
                }

                // Emit completion notification
                self.handle_send(conn_id, slab_idx, result, flags);
            }
            OP_DIRECT_RECV => {
                // Zero-copy recv directly into user memory (e.g., segment)
                let (conn_id, generation, _, _) = decode_user_data(user_data);
                self.handle_direct_recv(conn_id, generation, result);
            }
            OP_TCP_RECVMSG => {
                // TCP scatter-gather recvmsg completion
                let (conn_id, generation, _, _) = decode_user_data(user_data);
                self.handle_tcp_recvmsg(conn_id, generation, result);
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

        // Check if kernel will deliver more completions for this multishot operation
        let has_more = cqueue::more(flags);

        self.pending_completions.push(Completion::with_more(
            CompletionKind::Recv {
                conn_id: full_conn_id,
            },
            has_more,
        ));

        // Re-arm multishot if needed
        if !has_more {
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

        // Create ConnId with generation for all completions
        let full_conn_id = ConnId::with_generation(conn_id, generation);

        if result == 0 {
            // EOF - peer closed connection
            self.recv_pool.free(buf_id);
            self.pending_completions
                .push(Completion::new(CompletionKind::Closed {
                    conn_id: full_conn_id,
                }));
            return;
        }

        if result < 0 {
            // Recv error
            self.recv_pool.free(buf_id);
            self.pending_completions
                .push(Completion::new(CompletionKind::Error {
                    conn_id: full_conn_id,
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
                conn_id: full_conn_id,
            }));

        // Note: next recv is submitted in poll() to handle SQ full cases
    }

    /// Handle completion of a direct recv (zero-copy into user memory).
    ///
    /// Unlike single_recv which uses pooled buffers, direct_recv writes
    /// directly to user-provided memory (e.g., segment memory for cache SET).
    /// No data copying is needed - just report the result.
    fn handle_direct_recv(&mut self, conn_id: usize, generation: u32, result: i32) {
        // Check if this is a stale completion
        let conn = match self.connections.get_mut(conn_id) {
            Some(c) if c.generation == generation => c,
            Some(_) => {
                // Stale completion - the connection was reused
                return;
            }
            None => {
                // Connection doesn't exist anymore
                return;
            }
        };

        // Clear pending flag and pointer
        conn.direct_recv_pending = false;
        let _direct_ptr = conn.direct_recv_ptr.take();

        // Create ConnId with generation for completions
        let full_conn_id = ConnId::with_generation(conn_id, generation);

        if result == 0 {
            // EOF - peer closed connection
            // Caller is responsible for cleaning up any reservation
            self.pending_completions
                .push(Completion::new(CompletionKind::Closed {
                    conn_id: full_conn_id,
                }));
            return;
        }

        if result < 0 {
            // Recv error - caller should cancel any pending reservation
            self.pending_completions
                .push(Completion::new(CompletionKind::Error {
                    conn_id: full_conn_id,
                    error: io::Error::from_raw_os_error(-result),
                }));
            return;
        }

        // Success - data was written directly to user memory (true zero-copy)
        let bytes = result as usize;
        self.pending_completions
            .push(Completion::new(CompletionKind::RecvComplete {
                conn_id: full_conn_id,
                bytes,
            }));
    }

    /// Handle completion of a TCP recvmsg (scatter-gather receive).
    fn handle_tcp_recvmsg(&mut self, conn_id: usize, generation: u32, result: i32) {
        // Check if this is a stale completion
        let conn = match self.connections.get_mut(conn_id) {
            Some(c) if c.generation == generation => c,
            Some(_) => {
                // Stale completion - the connection was reused
                return;
            }
            None => {
                // Connection doesn't exist anymore
                return;
            }
        };

        // Clear pending flag and state
        conn.tcp_recvmsg_pending = false;
        conn.tcp_recvmsg_iovec_count = 0;

        // Create ConnId with generation for completions
        let full_conn_id = ConnId::with_generation(conn_id, generation);

        if result == 0 {
            // EOF - peer closed connection
            self.pending_completions
                .push(Completion::new(CompletionKind::Closed {
                    conn_id: full_conn_id,
                }));
            return;
        }

        if result < 0 {
            // Recv error
            self.pending_completions
                .push(Completion::new(CompletionKind::Error {
                    conn_id: full_conn_id,
                    error: io::Error::from_raw_os_error(-result),
                }));
            return;
        }

        // Success - data was written to user's iovecs
        let bytes = result as usize;
        self.pending_completions
            .push(Completion::new(CompletionKind::RecvComplete {
                conn_id: full_conn_id,
                bytes,
            }));
    }

    fn handle_send(&mut self, conn_id: usize, slot: usize, result: i32, flags: u32) {
        let slot = slot as u8;

        // Check if this is a SendZc notification
        if cqueue::notif(flags) {
            // Handle notification for this slot
            let (should_continue, should_finish_close) =
                if let Some(conn) = self.connections.get_mut(conn_id) {
                    let (slot_freed, should_continue) = conn.on_send_notif(slot);
                    let should_close = slot_freed && conn.closing && conn.all_sends_complete();
                    (should_continue, should_close)
                } else {
                    (false, false)
                };

            if should_finish_close {
                self.finish_deferred_close(conn_id);
            } else if should_continue {
                // Slot freed, more data to send - prepare next chunk
                self.continue_send(conn_id);
            }
            return;
        }

        // Get connection info including generation for ConnId
        let (is_closing, generation) = self
            .connections
            .get(conn_id)
            .map(|c| (c.closing, c.generation))
            .unwrap_or((true, 0));

        if is_closing {
            // Connection is closing or doesn't exist, ignore send result
            return;
        }

        // Create ConnId with generation for completions
        let full_conn_id = ConnId::with_generation(conn_id, generation);

        if result <= 0 {
            // Send error (result == 0 means connection closed, result < 0 is error)
            let error = if result == 0 {
                io::Error::new(io::ErrorKind::WriteZero, "send returned 0")
            } else {
                io::Error::from_raw_os_error(-result)
            };
            self.pending_completions
                .push(Completion::new(CompletionKind::Error {
                    conn_id: full_conn_id,
                    error,
                }));
            return;
        }

        let n = result as usize;

        // Handle partial send - continue sending remaining data in current chunk
        let has_remaining = if let Some(conn) = self.connections.get_mut(conn_id) {
            conn.on_send_complete(slot, n)
        } else {
            false
        };

        if has_remaining {
            // Partial send - continue with remaining data in send buffer
            if let Some(conn) = self.connections.get_mut(conn_id) {
                let (ptr, len) = conn.remaining_send(slot);
                if !ptr.is_null() && len > 0 {
                    let fixed_slot = conn.fixed_slot;
                    // Increment notifs for this continuation (same slot)
                    conn.increment_notifs(slot);
                    let send_data = unsafe { std::slice::from_raw_parts(ptr, len) };
                    let _ = self.submit_send_zc(conn_id, fixed_slot, slot, send_data);
                }
            }
        }

        self.pending_completions
            .push(Completion::new(CompletionKind::SendReady {
                conn_id: full_conn_id,
            }));
    }

    /// Continue sending from fragmentation buffer after a slot is freed.
    fn continue_send(&mut self, conn_id: usize) {
        if let Some(conn) = self.connections.get_mut(conn_id)
            && let Some((slot, ptr, len)) = conn.prepare_send()
        {
            let fixed_slot = conn.fixed_slot;
            let send_data = unsafe { std::slice::from_raw_parts(ptr, len) };
            let _ = self.submit_send_zc(conn_id, fixed_slot, slot, send_data);
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

        // Check if kernel will deliver more completions for this multishot operation
        let has_more = cqueue::more(flags);

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

            self.pending_completions.push(Completion::with_more(
                CompletionKind::AcceptRaw {
                    listener_id: ListenerId::new(listener_id),
                    raw_fd: new_fd,
                    addr,
                },
                has_more,
            ));
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

            self.pending_completions.push(Completion::with_more(
                CompletionKind::Accept {
                    listener_id: ListenerId::new(listener_id),
                    conn_id: ConnId::with_generation(conn_id, generation),
                    addr,
                },
                has_more,
            ));
        }

        // Re-arm multishot accept if needed
        if !has_more {
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

        // Queue a SendReady completion so the client knows the socket is ready for sending.
        // For non-blocking connect, the connection may already be established.
        self.pending_completions
            .push(Completion::new(CompletionKind::SendReady {
                conn_id: ConnId::with_generation(conn_id, generation),
            }));

        Ok(ConnId::with_generation(conn_id, generation))
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

        // Queue a SendReady completion so the client knows the socket is ready for sending.
        self.pending_completions
            .push(Completion::new(CompletionKind::SendReady {
                conn_id: ConnId::with_generation(conn_id, generation),
            }));

        Ok(ConnId::with_generation(conn_id, generation))
    }

    fn close(&mut self, id: ConnId) -> io::Result<()> {
        let conn_id = id.slot();

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
        if let Some(mut conn) = self.connections.try_remove(id.slot()) {
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
        let conn_id = id.slot();
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

        // Try to start sending if slots available
        // prepare_send allocates a slot and sets up the send
        if let Some((slot, ptr, len)) = conn.prepare_send() {
            let fixed_slot = conn.fixed_slot;
            let send_data = unsafe { std::slice::from_raw_parts(ptr, len) };
            self.submit_send_zc(conn_id, fixed_slot, slot, send_data)?;
        }

        Ok(data.len())
    }

    fn send_vectored_owned(&mut self, id: ConnId, buffers: Vec<bytes::Bytes>) -> io::Result<usize> {
        let conn_id = id.slot();
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

        // Prepare vectored send - allocates slot and stores buffers
        let (slot, msghdr_ptr, total_len) = conn
            .prepare_vectored_send(buffers)
            .ok_or_else(|| io::Error::from(io::ErrorKind::WouldBlock))?;

        let fixed_slot = conn.fixed_slot;

        // Submit SendMsgZc operation for true zero-copy scatter-gather
        self.submit_tcp_sendmsg_zc(conn_id, fixed_slot, slot, msghdr_ptr)?;

        Ok(total_len)
    }

    fn send_with_flags(
        &mut self,
        id: ConnId,
        data: &[u8],
        flags: crate::types::SendFlags,
    ) -> io::Result<usize> {
        let conn_id = id.slot();
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

        // Convert SendFlags to libc flags
        let mut msg_flags = 0;
        if flags.contains(crate::types::SendFlags::MORE) {
            msg_flags |= libc::MSG_MORE;
        }

        // Queue data in fragmentation buffer
        conn.queue_send(data);

        // Try to start sending if slots available
        if let Some((slot, ptr, len)) = conn.prepare_send() {
            let fixed_slot = conn.fixed_slot;
            let send_data = unsafe { std::slice::from_raw_parts(ptr, len) };
            self.submit_send_zc_flags(conn_id, fixed_slot, slot, send_data, msg_flags)?;
        }

        Ok(data.len())
    }

    fn recv(&mut self, id: ConnId, buf: &mut [u8]) -> io::Result<usize> {
        // Note: Prefer with_recv_buf() for zero-copy access.
        // This method copies data from recv_state to the provided buffer.
        let (n, pending_returns): (usize, Vec<_>) = {
            let conn = self
                .connections
                .get_mut(id.slot())
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
        let conn_id = id.slot();
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

    fn submit_recv_into(&mut self, id: ConnId, ptr: *mut u8, len: usize) -> io::Result<()> {
        let conn_id = id.slot();
        let conn = self
            .connections
            .get_mut(conn_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

        // Only one recv can be pending at a time
        if conn.single_recv_pending || conn.direct_recv_pending {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        // Mark this connection as using direct recv mode
        conn.use_single_recv = true;

        let fixed_slot = conn.fixed_slot;
        let generation = conn.generation;

        // Store the direct pointer for validation on completion
        conn.direct_recv_ptr = Some((ptr, len));

        // Submit recv directly to user's pointer (true zero-copy)
        // Uses standard user_data encoding since we don't need pool buffer tracking
        let recv_op = opcode::Recv::new(Fixed(fixed_slot), ptr, len as u32)
            .build()
            .user_data(encode_user_data(conn_id, generation, 0, OP_DIRECT_RECV));

        unsafe {
            if self.ring.submission().push(&recv_op).is_err() {
                // Failed to submit - clear state
                if let Some(conn) = self.connections.get_mut(conn_id) {
                    conn.direct_recv_ptr = None;
                }
                return Err(io::Error::other("SQ full"));
            }
        }

        // Mark recv as pending
        if let Some(conn) = self.connections.get_mut(conn_id) {
            conn.direct_recv_pending = true;
        }

        Ok(())
    }

    fn submit_recvmsg_tcp(&mut self, id: ConnId, iovecs: &[libc::iovec]) -> io::Result<()> {
        use connection::MAX_TCP_RECVMSG_IOVECS;

        let conn_id = id.slot();
        let conn = self
            .connections
            .get_mut(conn_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

        // Check for too many iovecs
        if iovecs.len() > MAX_TCP_RECVMSG_IOVECS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "too many iovecs ({} > {})",
                    iovecs.len(),
                    MAX_TCP_RECVMSG_IOVECS
                ),
            ));
        }

        // Only one recv can be pending at a time
        if conn.single_recv_pending || conn.direct_recv_pending || conn.tcp_recvmsg_pending {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        // Mark this connection as using single-shot mode (disables multishot)
        conn.use_single_recv = true;

        let fixed_slot = conn.fixed_slot;
        let generation = conn.generation;

        // Copy iovecs to connection-owned storage (they must outlive the async op)
        conn.tcp_recvmsg_iovec_count = iovecs.len();
        conn.tcp_recvmsg_iovecs[..iovecs.len()].copy_from_slice(iovecs);

        // Set up msghdr pointing to our stored iovecs
        conn.tcp_recvmsg_msghdr = unsafe { std::mem::zeroed() };
        conn.tcp_recvmsg_msghdr.msg_iov = conn.tcp_recvmsg_iovecs.as_mut_ptr();
        conn.tcp_recvmsg_msghdr.msg_iovlen = iovecs.len() as _;

        // Get pointer to msghdr (it's stored in connection, so outlives the call)
        let msghdr_ptr = &conn.tcp_recvmsg_msghdr as *const libc::msghdr;

        // Submit recvmsg operation
        let recvmsg_op = opcode::RecvMsg::new(Fixed(fixed_slot), msghdr_ptr as *mut _)
            .build()
            .user_data(encode_user_data(conn_id, generation, 0, OP_TCP_RECVMSG));

        unsafe {
            if self.ring.submission().push(&recvmsg_op).is_err() {
                // Failed to submit - clear state
                if let Some(conn) = self.connections.get_mut(conn_id) {
                    conn.tcp_recvmsg_iovec_count = 0;
                }
                return Err(io::Error::other("SQ full"));
            }
        }

        // Mark recvmsg as pending
        if let Some(conn) = self.connections.get_mut(conn_id) {
            conn.tcp_recvmsg_pending = true;
        }

        Ok(())
    }

    fn with_recv_buf(&mut self, id: ConnId, f: &mut dyn FnMut(&mut dyn RecvBuf)) -> io::Result<()> {
        // Collect pending returns after running callback
        let pending_returns: Vec<_> = {
            let conn = self
                .connections
                .get_mut(id.slot())
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
        // Note: We do NOT clear pending_completions here because register() and
        // register_fd() may have queued completions for newly registered connections.
        // drain_completions() clears the vec via mem::take().

        // Re-arm recv for connections that need it
        // This handles both single-shot mode (no pending recv) and multishot mode
        // (multishot deactivated due to re-arm failure)
        // Skip connections with direct_recv_pending or tcp_recvmsg_pending - they manage their own recv
        // Reuse scratch buffer to avoid allocation
        self.rearm_scratch.clear();
        self.rearm_scratch.extend(
            self.connections
                .iter()
                .filter(|(_, c)| {
                    !c.single_recv_pending
                        && !c.multishot_active
                        && !c.direct_recv_pending
                        && !c.tcp_recvmsg_pending
                })
                .map(|(id, c)| (id, c.generation, c.fixed_slot, c.rearm_failures)),
        );

        // Maximum consecutive re-arm failures before emitting an error
        const MAX_REARM_FAILURES: u8 = 10;

        for i in 0..self.rearm_scratch.len() {
            let (conn_id, generation, fixed_slot, failures) = self.rearm_scratch[i];
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
                                conn_id: ConnId::with_generation(conn_id, generation),
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

        // Collect completions - reuse scratch buffer to avoid allocation
        self.cqe_scratch.extend(self.ring.completion());
        let count = self.cqe_scratch.len();

        // Process in order (FIFO) - critical for correct data sequencing with multishot recv
        // Take ownership temporarily to avoid borrow conflict with process_cqe
        let mut cqes = std::mem::take(&mut self.cqe_scratch);
        for cqe in cqes.drain(..) {
            self.process_cqe(cqe);
        }
        self.cqe_scratch = cqes; // restore empty vec with capacity preserved

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
        self.connections.get(id.slot()).map(|c| c.raw_fd)
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

    fn capabilities(&self) -> DriverCapabilities {
        self.capabilities
    }

    fn send_mode(&self) -> SendMode {
        self.send_mode
    }

    fn set_send_mode(&mut self, mode: SendMode) {
        self.send_mode = mode;
    }

    fn send_owned(&mut self, id: ConnId, buffer: BoxedZeroCopy) -> io::Result<usize> {
        let conn_id = id.as_usize();

        // Get connection info
        let conn = self
            .connections
            .get(conn_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

        // Check generation
        if conn.generation != id.generation() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "stale connection ID",
            ));
        }

        let fixed_slot = conn.fixed_slot;
        let total_len = buffer.total_len();

        // Build iovecs from buffer's slices (inline storage, no heap allocation)
        let slices = buffer.io_slices();
        if slices.len() > MAX_ZC_IOVECS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("too many iovecs: {} > {}", slices.len(), MAX_ZC_IOVECS),
            ));
        }
        let iovecs: ArrayVec<libc::iovec, MAX_ZC_IOVECS> = slices
            .iter()
            .map(|s| libc::iovec {
                iov_base: s.as_ptr() as *mut _,
                iov_len: s.len(),
            })
            .collect();

        // Insert into slab - this gives us stable storage without boxing
        let entry = self.pending_zc_sends.vacant_entry();
        let slab_idx = entry.key();
        entry.insert(PendingZcSend {
            buffer,
            iovecs,
            msghdr: unsafe { std::mem::zeroed() },
        });

        // Get reference to update msghdr with iovec pointer
        // SAFETY: We just inserted at slab_idx, and Slab provides stable addresses
        let pending = self.pending_zc_sends.get_mut(slab_idx).unwrap();
        pending.msghdr.msg_iov = pending.iovecs.as_mut_ptr();
        pending.msghdr.msg_iovlen = pending.iovecs.len();

        // Build SendMsg entry - encode slab_idx in user_data for completion lookup
        let generation = conn.generation;
        let user_data = encode_zc_sendmsg(conn_id, generation, slab_idx, OP_ZC_SENDMSG);
        let sendmsg_entry = opcode::SendMsg::new(Fixed(fixed_slot), &pending.msghdr)
            .build()
            .user_data(user_data);

        // SAFETY: msghdr and iovecs point to data owned by PendingZcSend in slab
        // which is kept alive until completion removes it
        unsafe {
            if let Err(e) = self.ring.submission().push(&sendmsg_entry) {
                // Clean up on failure
                self.pending_zc_sends.remove(slab_idx);
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("submission queue full: {:?}", e),
                ));
            }
        }

        self.ring.submit()?;

        Ok(total_len)
    }
}

/// Probe io_uring capabilities from the ring.
fn probe_capabilities(ring: &IoUring) -> DriverCapabilities {
    let mut caps = DriverCapabilities::empty();
    let mut probe = Probe::new();

    if ring.submitter().register_probe(&mut probe).is_ok() {
        // Check for SendZc (requires 6.0+)
        if probe.is_supported(opcode::SendZc::CODE) {
            caps |= DriverCapabilities::ZEROCOPY_SEND;
        }

        // Check for multishot recv (RecvMulti)
        if probe.is_supported(opcode::RecvMulti::CODE) {
            caps |= DriverCapabilities::MULTISHOT_RECV;
        }

        // Check for multishot accept (AcceptMulti)
        if probe.is_supported(opcode::AcceptMulti::CODE) {
            caps |= DriverCapabilities::MULTISHOT_ACCEPT;
        }

        // Check for async cancel
        if probe.is_supported(opcode::AsyncCancel::CODE) {
            caps |= DriverCapabilities::ASYNC_CANCEL;
        }
    }

    // These are always available when io_uring is available
    caps |= DriverCapabilities::PROVIDED_BUFFERS;
    caps |= DriverCapabilities::FIXED_BUFFERS;
    caps |= DriverCapabilities::VECTORED_IO;
    caps |= DriverCapabilities::DIRECT_DESCRIPTORS;

    caps
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
