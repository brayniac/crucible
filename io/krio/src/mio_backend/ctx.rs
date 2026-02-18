//! DriverCtx, EventHandler, and supporting types for the mio backend.
//!
//! These mirror the io_uring backend's public API so that application code
//! can be compiled against either backend without changes.

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;

use crate::buffer::send_copy::SendCopyPool;
use crate::connection::ConnectionTable;
use crate::guard::GuardBox;

// ── ConnToken / UdpToken ─────────────────────────────────────────────

/// Opaque connection handle. Identical to the io_uring backend's `ConnToken`.
#[derive(Debug, Clone, Copy)]
pub struct ConnToken {
    pub(crate) index: u32,
    pub(crate) generation: u32,
}

impl ConnToken {
    pub(crate) fn new(index: u32, generation: u32) -> Self {
        ConnToken { index, generation }
    }

    /// Slot index — use for per-connection state arrays.
    #[inline]
    pub fn index(&self) -> u32 {
        self.index
    }
}

/// Opaque handle for a bound UDP socket.
///
/// UDP is not supported on the mio backend; this type exists only for API
/// compatibility with the io_uring backend's `EventHandler` trait.
#[derive(Debug, Clone, Copy)]
pub struct UdpToken {
    #[allow(dead_code)]
    pub(crate) index: u32,
}

impl UdpToken {
    /// Slot index.
    #[inline]
    pub fn index(&self) -> u32 {
        self.index
    }
}

// ── SendPart ─────────────────────────────────────────────────────────

/// Pre-classified part for scatter-gather sends.
pub enum SendPart {
    /// Data that will be copied into the send pool.
    Copy(Vec<u8>),
    /// Zero-copy guard (on the mio backend the data is still copied, but the
    /// API remains the same).
    Guard(GuardBox),
}

// ── Per-connection send state ────────────────────────────────────────

/// Tracks in-flight and queued sends for a single connection.
pub(crate) struct MioConnSend {
    /// Whether there is an in-flight write (data being written or queued).
    pub in_flight: bool,
    /// Pool slot currently being written. `u16::MAX` = none.
    pub current_slot: u16,
    /// Pointer into the pool slot at the current write offset.
    pub current_ptr: *const u8,
    /// Bytes remaining to write from the current slot.
    pub current_remaining: u32,
    /// Original total length of the current slot (for `on_send_complete`).
    pub current_total: u32,
    /// Queue of pending sends: (pool_slot, ptr, len).
    pub queue: VecDeque<(u16, *const u8, u32)>,
}

// Safety: MioConnSend is only accessed from a single worker thread.
unsafe impl Send for MioConnSend {}

impl MioConnSend {
    pub fn new() -> Self {
        MioConnSend {
            in_flight: false,
            current_slot: u16::MAX,
            current_ptr: std::ptr::null(),
            current_remaining: 0,
            current_total: 0,
            queue: VecDeque::new(),
        }
    }
}

/// Pending outbound connect with optional timeout.
pub(crate) struct PendingConnect {
    pub conn_index: u32,
    pub deadline: Option<std::time::Instant>,
}

// ── DriverCtx ────────────────────────────────────────────────────────

/// I/O context passed to [`EventHandler`] callbacks (mio backend).
///
/// Provides the same public API as the io_uring backend's `DriverCtx`.
pub struct DriverCtx<'a> {
    pub(crate) connections: &'a mut ConnectionTable,
    pub(crate) send_copy_pool: &'a mut SendCopyPool,
    pub(crate) conn_fds: &'a mut Vec<RawFd>,
    pub(crate) send_states: &'a mut Vec<MioConnSend>,
    pub(crate) shutdown_requested: &'a mut bool,
    pub(crate) tcp_nodelay: bool,
    pub(crate) connect_addrs: &'a mut Vec<libc::sockaddr_storage>,
    pub(crate) pending_connects: &'a mut Vec<PendingConnect>,
    pub(crate) pending_send_completions: &'a mut Vec<(u32, u32, io::Result<u32>)>,
    pub(crate) pending_close: &'a mut Vec<u32>,
    pub(crate) max_connections: u32,
}

impl<'a> DriverCtx<'a> {
    /// Send data on a connection (copying send).
    ///
    /// Data is copied to an internal pool. The `on_send_complete` callback fires
    /// when the write completes (possibly on a later event loop iteration).
    pub fn send(&mut self, conn: ConnToken, data: &[u8]) -> io::Result<()> {
        let idx = conn.index as usize;
        let fd = *self
            .conn_fds
            .get(idx)
            .filter(|&&fd| fd >= 0)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "connection not active")
            })?;

        let conn_state = self
            .connections
            .get(conn.index)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "connection not active")
            })?;
        if conn_state.generation != conn.generation {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stale connection token",
            ));
        }

        let (slot, ptr, len) = self
            .send_copy_pool
            .copy_in(data)
            .ok_or_else(|| {
                crate::metrics::SEND_POOL_EXHAUSTED.increment();
                io::Error::new(io::ErrorKind::Other, "send pool exhausted")
            })?;

        self.do_send_slot(conn, fd, slot, ptr, len)
    }

    /// Start building a scatter-gather send.
    pub fn send_parts(&mut self, conn: ConnToken) -> SendBuilder<'_, 'a> {
        SendBuilder {
            ctx: self,
            conn,
            parts: Vec::new(),
            total_len: 0,
            _guards: Vec::new(),
        }
    }

    /// Close a connection.
    pub fn close(&mut self, conn: ConnToken) {
        if let Some(state) = self.connections.get(conn.index) {
            if state.generation != conn.generation {
                return;
            }
        } else {
            return;
        }
        self.pending_close.push(conn.index);
    }

    /// Initiate an outbound TCP connection.
    pub fn connect(&mut self, addr: SocketAddr) -> io::Result<ConnToken> {
        self.connect_inner(addr, None)
    }

    /// Initiate an outbound TCP connection with a timeout.
    pub fn connect_with_timeout(
        &mut self,
        addr: SocketAddr,
        timeout_ms: u64,
    ) -> io::Result<ConnToken> {
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(timeout_ms);
        self.connect_inner(addr, Some(deadline))
    }

    /// Request graceful shutdown of all workers.
    pub fn request_shutdown(&mut self) {
        *self.shutdown_requested = true;
    }

    /// Get the peer address for a connection.
    pub fn peer_addr(&self, conn: ConnToken) -> Option<SocketAddr> {
        self.connections.get(conn.index).and_then(|s| s.peer_addr)
    }

    /// Check whether a connection is outbound (initiated via `connect()`).
    pub fn is_outbound(&self, conn: ConnToken) -> bool {
        self.connections
            .get(conn.index)
            .map_or(false, |s| s.outbound)
    }

    /// Initiate a write-side shutdown (half-close).
    pub fn shutdown_write(&mut self, conn: ConnToken) {
        let idx = conn.index as usize;
        if let Some(&fd) = self.conn_fds.get(idx) {
            if fd >= 0 {
                unsafe {
                    libc::shutdown(fd, libc::SHUT_WR);
                }
            }
        }
    }

    /// Maximum IO_LINK chain length. Always 0 on the mio backend (no chaining).
    pub fn max_chain_length(&self) -> u16 {
        0
    }

    // ── Internal helpers ─────────────────────────────────────────────

    fn connect_inner(
        &mut self,
        addr: SocketAddr,
        deadline: Option<std::time::Instant>,
    ) -> io::Result<ConnToken> {
        let conn_index = self
            .connections
            .allocate_outbound()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "connection limit reached",
                )
            })?;

        let domain = if addr.is_ipv4() {
            libc::AF_INET
        } else {
            libc::AF_INET6
        };
        let fd = unsafe {
            libc::socket(
                domain,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        if fd < 0 {
            self.connections.release(conn_index);
            return Err(io::Error::last_os_error());
        }

        if self.tcp_nodelay {
            let optval: libc::c_int = 1;
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NODELAY,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        // Fill sockaddr
        let idx = conn_index as usize;
        if idx >= self.connect_addrs.len() {
            self.connect_addrs
                .resize(idx + 1, unsafe { std::mem::zeroed() });
        }
        let storage = &mut self.connect_addrs[idx];
        let addr_len = fill_sockaddr_storage(storage, addr);

        let ret = unsafe {
            libc::connect(
                fd,
                storage as *const _ as *const libc::sockaddr,
                addr_len,
            )
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::EINPROGRESS) {
                unsafe {
                    libc::close(fd);
                }
                self.connections.release(conn_index);
                return Err(err);
            }
        }

        // Store fd
        if idx >= self.conn_fds.len() {
            self.conn_fds.resize(idx + 1, -1);
        }
        self.conn_fds[idx] = fd;

        if idx >= self.send_states.len() {
            self.send_states.resize_with(idx + 1, MioConnSend::new);
        }

        // Store peer addr
        if let Some(state) = self.connections.get_mut(conn_index) {
            state.peer_addr = Some(addr);
        }

        self.pending_connects.push(PendingConnect {
            conn_index,
            deadline,
        });

        let generation = self.connections.generation(conn_index);
        Ok(ConnToken::new(conn_index, generation))
    }

    /// Internal: send a pool slot, trying immediate write then queueing.
    pub(crate) fn do_send_slot(
        &mut self,
        conn: ConnToken,
        fd: RawFd,
        slot: u16,
        ptr: *const u8,
        len: u32,
    ) -> io::Result<()> {
        let idx = conn.index as usize;
        if idx >= self.send_states.len() {
            self.send_states.resize_with(idx + 1, MioConnSend::new);
        }
        let send_state = &mut self.send_states[idx];

        if send_state.in_flight {
            // Queue for later
            send_state.queue.push_back((slot, ptr, len));
            return Ok(());
        }

        // Try immediate write
        send_state.in_flight = true;
        send_state.current_slot = slot;
        send_state.current_total = len;

        let n = unsafe { libc::write(fd, ptr as *const libc::c_void, len as usize) };

        if n > 0 && n as u32 >= len {
            // Fully written
            let original_len = self.send_copy_pool.original_len(slot);
            self.send_copy_pool.release(slot);
            send_state.in_flight = false;
            send_state.current_slot = u16::MAX;
            let generation = self.connections.generation(conn.index);
            self.pending_send_completions
                .push((conn.index, generation, Ok(original_len)));
            crate::metrics::BYTES_SENT.add(original_len as _);
        } else if n > 0 {
            // Partial write — advance
            let written = n as u32;
            send_state.current_ptr = ptr.wrapping_add(written as usize);
            send_state.current_remaining = len - written;
            crate::metrics::BYTES_SENT.add(written as _);
        } else if n == 0
            || (n < 0
                && io::Error::last_os_error().kind() == io::ErrorKind::WouldBlock)
        {
            // EAGAIN — save state for writable event
            send_state.current_ptr = ptr;
            send_state.current_remaining = len;
        } else {
            // Error
            let err = io::Error::last_os_error();
            self.send_copy_pool.release(slot);
            send_state.in_flight = false;
            send_state.current_slot = u16::MAX;
            let generation = self.connections.generation(conn.index);
            self.pending_send_completions
                .push((conn.index, generation, Err(err)));
        }

        Ok(())
    }
}

// ── SendBuilder ──────────────────────────────────────────────────────

/// Builder for scatter-gather sends (mio backend).
///
/// On the mio backend, all parts (copy and guard) are gathered into a single
/// contiguous buffer in the send copy pool. Guards are dropped after gathering
/// (no zero-copy). The API is the same as the io_uring backend.
pub struct SendBuilder<'a, 'b> {
    ctx: &'a mut DriverCtx<'b>,
    conn: ConnToken,
    parts: Vec<(*const u8, usize)>,
    total_len: usize,
    // Holds guards alive until submit() gathers the data.
    _guards: Vec<GuardBox>,
}

impl<'a, 'b> SendBuilder<'a, 'b> {
    /// Add a copy part (data is copied into the send pool on submit).
    pub fn copy(mut self, data: &[u8]) -> Self {
        if !data.is_empty() {
            self.parts.push((data.as_ptr(), data.len()));
            self.total_len += data.len();
        }
        self
    }

    /// Add a guard part (data is copied on the mio backend; guard is dropped
    /// after gathering).
    pub fn guard(mut self, guard: GuardBox) -> Self {
        let (ptr, len) = guard.as_ptr_len();
        if len > 0 {
            self.parts.push((ptr, len as usize));
            self.total_len += len as usize;
        }
        self._guards.push(guard);
        self
    }

    /// Submit the gathered send.
    pub fn submit(self) -> io::Result<()> {
        if self.total_len == 0 {
            return Ok(());
        }

        let idx = self.conn.index as usize;
        let fd = *self
            .ctx
            .conn_fds
            .get(idx)
            .filter(|&&fd| fd >= 0)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "connection not active")
            })?;

        // Gather all parts into one pool slot.
        let (slot, ptr, len) = unsafe {
            self.ctx
                .send_copy_pool
                .copy_in_gather(&self.parts, self.total_len)
        }
        .ok_or_else(|| {
            crate::metrics::SEND_POOL_EXHAUSTED.increment();
            io::Error::new(io::ErrorKind::Other, "send pool exhausted")
        })?;

        // Guards are dropped here (end of function scope).
        self.ctx.do_send_slot(self.conn, fd, slot, ptr, len)
    }
}

// ── EventHandler ─────────────────────────────────────────────────────

/// Trait for callback-driven event handlers (mio backend).
///
/// This has the same signature as the io_uring backend's `EventHandler`.
pub trait EventHandler: Send + 'static {
    /// Called when a new inbound connection is accepted.
    fn on_accept(&mut self, ctx: &mut DriverCtx, conn: ConnToken);

    /// Called when an outbound `connect()` completes.
    fn on_connect(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken, _result: io::Result<()>) {}

    /// Called when data has been received on a connection.
    ///
    /// Returns the number of bytes consumed. Unconsumed bytes are retained
    /// in the accumulator and presented again with the next chunk.
    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize;

    /// Called when a send operation completes.
    fn on_send_complete(
        &mut self,
        ctx: &mut DriverCtx,
        conn: ConnToken,
        result: io::Result<u32>,
    );

    /// Called when a connection is closed.
    fn on_close(&mut self, ctx: &mut DriverCtx, conn: ConnToken);

    /// Called once per event loop iteration (after processing all events).
    fn on_tick(&mut self, _ctx: &mut DriverCtx) {}

    /// Called when a UDP datagram is received (not supported on mio backend).
    fn on_datagram(
        &mut self,
        _ctx: &mut DriverCtx,
        _socket: UdpToken,
        _data: &[u8],
        _peer: SocketAddr,
    ) {
    }

    /// Called when an NVMe command completes (not supported on mio backend).
    fn on_nvme_complete(
        &mut self,
        _ctx: &mut DriverCtx,
        _completion: crate::nvme::NvmeCompletion,
    ) {
    }

    /// Called when a direct I/O operation completes (not supported on mio backend).
    fn on_direct_io_complete(
        &mut self,
        _ctx: &mut DriverCtx,
        _completion: crate::direct_io::DirectIoCompletion,
    ) {
    }

    /// Called when an external thread signals via eventfd (see
    /// `ShutdownHandle::worker_eventfds()`).
    fn on_notify(&mut self, _ctx: &mut DriverCtx) {}

    /// Create a per-worker instance of this handler.
    fn create_for_worker(worker_id: usize) -> Self
    where
        Self: Sized;
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Fill a `sockaddr_storage` from a Rust `SocketAddr`.
pub(crate) fn fill_sockaddr_storage(
    storage: &mut libc::sockaddr_storage,
    addr: SocketAddr,
) -> libc::socklen_t {
    *storage = unsafe { std::mem::zeroed() };
    match addr {
        SocketAddr::V4(v4) => {
            let sa = storage as *mut _ as *mut libc::sockaddr_in;
            unsafe {
                (*sa).sin_family = libc::AF_INET as libc::sa_family_t;
                (*sa).sin_port = v4.port().to_be();
                (*sa).sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            }
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(v6) => {
            let sa = storage as *mut _ as *mut libc::sockaddr_in6;
            unsafe {
                (*sa).sin6_family = libc::AF_INET6 as libc::sa_family_t;
                (*sa).sin6_port = v6.port().to_be();
                (*sa).sin6_flowinfo = v6.flowinfo();
                (*sa).sin6_addr.s6_addr = v6.ip().octets();
                (*sa).sin6_scope_id = v6.scope_id();
            }
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    }
}
