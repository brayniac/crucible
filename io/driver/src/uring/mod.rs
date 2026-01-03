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

mod buf_ring;
mod connection;
mod registered_files;

use crate::driver::IoDriver;
use crate::types::{Completion, CompletionKind, ConnId, ListenerId};
use buf_ring::BufRing;
use connection::UringConnection;
use io_uring::cqueue;
use io_uring::opcode::{self, AcceptMulti, RecvMulti, SendZc};
use io_uring::squeue;
use io_uring::types::Fixed;
use io_uring::{IoUring, Probe};
use registered_files::RegisteredFiles;
use slab::Slab;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::os::unix::io::{AsRawFd, IntoRawFd, RawFd};
use std::time::Duration;

/// User data encoding for operations.
/// Format: (id << 8) | (buf_idx << 4) | op_type
const OP_SEND: u64 = 1;
const OP_MULTISHOT_RECV: u64 = 2;
const OP_ACCEPT: u64 = 3;

/// Buffer group ID for recv operations.
const RECV_BGID: u16 = 0;

#[inline]
fn encode_user_data(id: usize, buf_idx: u8, op: u64) -> u64 {
    ((id as u64) << 8) | ((buf_idx as u64) << 4) | op
}

#[inline]
fn decode_user_data(user_data: u64) -> (usize, u8, u64) {
    (
        (user_data >> 8) as usize,
        ((user_data >> 4) & 0xF) as u8,
        user_data & 0xF,
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

/// io_uring-based I/O driver.
///
/// Requires Linux 6.0+ for full feature support (multishot recv/accept,
/// SendZc, ring-provided buffers).
pub struct UringDriver {
    ring: IoUring,
    connections: Slab<UringConnection>,
    listeners: Slab<UringListener>,
    registered_files: RegisteredFiles,
    buf_ring: BufRing,
    pending_completions: Vec<Completion>,
    buffer_size: usize,
}

impl UringDriver {
    /// Create a new io_uring driver with default settings.
    pub fn new() -> io::Result<Self> {
        Self::with_config(256, 16384, 256, 8192, false)
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

        // Create buffer ring for multishot recv
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

        Ok(Self {
            ring,
            connections: Slab::with_capacity(max_connections as usize),
            listeners: Slab::with_capacity(16),
            registered_files,
            buf_ring,
            pending_completions: Vec::with_capacity(256),
            buffer_size,
        })
    }

    /// Submit a multishot recv for a connection.
    fn submit_multishot_recv(&mut self, conn_id: usize, fixed_slot: u32) -> io::Result<()> {
        let recv_op = RecvMulti::new(Fixed(fixed_slot), RECV_BGID)
            .build()
            .user_data(encode_user_data(conn_id, 0, OP_MULTISHOT_RECV));

        unsafe {
            self.ring
                .submission()
                .push(&recv_op)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    /// Submit a multishot accept for a listener.
    fn submit_multishot_accept(&mut self, listener_id: usize, fixed_slot: u32) -> io::Result<()> {
        let accept_op = AcceptMulti::new(Fixed(fixed_slot))
            .build()
            .user_data(encode_user_data(listener_id, 0, OP_ACCEPT));

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
            .user_data(encode_user_data(conn_id, buf_idx, OP_SEND));

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
        let (id, buf_idx, op) = decode_user_data(user_data);
        let result = cqe.result();
        let flags = cqe.flags();

        match op {
            OP_MULTISHOT_RECV => {
                self.handle_multishot_recv(id, result, flags);
            }
            OP_SEND => {
                self.handle_send(id, buf_idx as usize, result, flags);
            }
            OP_ACCEPT => {
                self.handle_accept(id, result, flags);
            }
            _ => {}
        }
    }

    fn handle_multishot_recv(&mut self, conn_id: usize, result: i32, flags: u32) {
        if !self.connections.contains(conn_id) {
            // Return buffer if allocated
            if let Some(buf_id) = cqueue::buffer_select(flags) {
                self.buf_ring.return_buffer(buf_id);
            }
            return;
        }

        if result <= 0 {
            // Connection closed or error
            if let Some(buf_id) = cqueue::buffer_select(flags) {
                self.buf_ring.return_buffer(buf_id);
            }
            self.pending_completions
                .push(Completion::new(CompletionKind::Closed {
                    conn_id: ConnId::new(conn_id),
                }));
            return;
        }

        // Get buffer ID from completion
        let buf_id = match cqueue::buffer_select(flags) {
            Some(id) => id,
            None => return,
        };

        let n = result as usize;

        // Copy data from provided buffer to connection buffer
        let buf_data = &self.buf_ring.get(buf_id)[..n];
        if let Some(conn) = self.connections.get_mut(conn_id) {
            conn.append_recv_data(buf_data);
        }

        // Return buffer to ring
        self.buf_ring.return_buffer(buf_id);

        self.pending_completions
            .push(Completion::new(CompletionKind::Recv {
                conn_id: ConnId::new(conn_id),
            }));

        // Re-arm multishot if needed
        if !cqueue::more(flags) {
            let fixed_slot = self.connections.get(conn_id).map(|c| c.fixed_slot);
            if let Some(fixed_slot) = fixed_slot {
                if let Some(conn) = self.connections.get_mut(conn_id) {
                    conn.multishot_active = false;
                }
                if self.submit_multishot_recv(conn_id, fixed_slot).is_ok()
                    && let Some(conn) = self.connections.get_mut(conn_id)
                {
                    conn.multishot_active = true;
                } else {
                    // Failed to re-arm multishot recv - connection would be orphaned.
                    // Emit an error so the server can clean up.
                    self.pending_completions
                        .push(Completion::new(CompletionKind::Error {
                            conn_id: ConnId::new(conn_id),
                            error: io::Error::other("failed to re-arm multishot recv"),
                        }));
                }
            }
        }
    }

    fn handle_send(&mut self, conn_id: usize, buf_idx: usize, result: i32, flags: u32) {
        // Check if this is a SendZc notification
        if cqueue::notif(flags) {
            if let Some(conn) = self.connections.get_mut(conn_id) {
                conn.decrement_in_flight(buf_idx);
            }
            return;
        }

        if !self.connections.contains(conn_id) {
            return;
        }

        if result <= 0 {
            self.pending_completions
                .push(Completion::new(CompletionKind::Closed {
                    conn_id: ConnId::new(conn_id),
                }));
            return;
        }

        let n = result as usize;
        if let Some(conn) = self.connections.get_mut(conn_id) {
            conn.advance_send(buf_idx, n);

            // If more data to send, submit another send
            if conn.has_pending_send(buf_idx) {
                conn.increment_in_flight(buf_idx);
                // Get stable pointer to the actual send buffer (not a copy!)
                let send_ptr = conn.send_buf_ptr(buf_idx);
                let send_len = conn.pending_send_len(buf_idx);
                let fixed_slot = conn.fixed_slot;

                // Use the stable buffer pointer for zero-copy send
                let send_data = unsafe { std::slice::from_raw_parts(send_ptr, send_len) };
                let _ = self.submit_send_zc(conn_id, fixed_slot, buf_idx as u8, send_data);
            }
        }

        self.pending_completions
            .push(Completion::new(CompletionKind::SendReady {
                conn_id: ConnId::new(conn_id),
            }));
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

            let conn = UringConnection::new(new_fd, fixed_slot, self.buffer_size);
            entry.insert(conn);

            // Submit multishot recv - if this fails, clean up and skip this connection
            if self.submit_multishot_recv(conn_id, fixed_slot).is_err() {
                self.connections.try_remove(conn_id);
                let fds = [-1i32];
                let _ = self
                    .ring
                    .submitter()
                    .register_files_update(fixed_slot, &fds);
                self.registered_files.free(fixed_slot);
                unsafe { libc::close(new_fd) };
                return;
            }

            if let Some(conn) = self.connections.get_mut(conn_id) {
                conn.multishot_active = true;
            }

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

        let conn = UringConnection::new(raw_fd, fixed_slot, self.buffer_size);
        entry.insert(conn);

        // Submit multishot recv - if this fails, clean up and return error
        if let Err(e) = self.submit_multishot_recv(conn_id, fixed_slot) {
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

        if let Some(conn) = self.connections.get_mut(conn_id) {
            conn.multishot_active = true;
        }

        self.ring.submit()?;

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

        let conn = UringConnection::new(raw_fd, fixed_slot, self.buffer_size);
        entry.insert(conn);

        // Submit multishot recv - if this fails, clean up and return error
        if let Err(e) = self.submit_multishot_recv(conn_id, fixed_slot) {
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

        if let Some(conn) = self.connections.get_mut(conn_id) {
            conn.multishot_active = true;
        }

        self.ring.submit()?;

        Ok(ConnId::new(conn_id))
    }

    fn close(&mut self, id: ConnId) -> io::Result<()> {
        if let Some(conn) = self.connections.try_remove(id.as_usize()) {
            // Update registered fd to -1
            let fds = [-1i32];
            let _ = self
                .ring
                .submitter()
                .register_files_update(conn.fixed_slot, &fds);
            self.registered_files.free(conn.fixed_slot);
            unsafe { libc::close(conn.raw_fd) };
        }
        Ok(())
    }

    fn take_fd(&mut self, id: ConnId) -> io::Result<RawFd> {
        if let Some(conn) = self.connections.try_remove(id.as_usize()) {
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
        let conn = self
            .connections
            .get_mut(id.as_usize())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

        // Append data to send buffer
        let buf_idx = conn
            .append_send_data(data)
            .ok_or_else(|| io::Error::from(io::ErrorKind::WouldBlock))?;

        // If this is the first data in the buffer, submit a send
        if conn.send_pos(buf_idx) == 0
            || conn.send_pos(buf_idx) >= conn.send_buf_len(buf_idx) - data.len()
        {
            conn.increment_in_flight(buf_idx);
            // Get stable pointer to the actual send buffer (not a copy!)
            // This is safe because send_bufs don't reallocate while in_flight_count > 0
            let send_ptr = conn.send_buf_ptr(buf_idx);
            let send_len = conn.pending_send_len(buf_idx);
            let fixed_slot = conn.fixed_slot;

            // Use the stable buffer pointer for zero-copy send
            let send_data = unsafe { std::slice::from_raw_parts(send_ptr, send_len) };
            self.submit_send_zc(id.as_usize(), fixed_slot, buf_idx as u8, send_data)?;
        }

        Ok(data.len())
    }

    fn recv(&mut self, id: ConnId, buf: &mut [u8]) -> io::Result<usize> {
        let conn = self
            .connections
            .get_mut(id.as_usize())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "connection not found"))?;

        if conn.recv_data.is_empty() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        let n = std::cmp::min(buf.len(), conn.recv_data.len());
        buf[..n].copy_from_slice(&conn.recv_data[..n]);
        conn.recv_data.drain(..n);

        Ok(n)
    }

    fn poll(&mut self, timeout: Option<Duration>) -> io::Result<usize> {
        self.pending_completions.clear();

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
