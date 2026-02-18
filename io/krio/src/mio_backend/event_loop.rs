//! mio-based event loop implementation.
//!
//! This provides the same push-based callback model as the io_uring event loop
//! but uses mio (epoll on Linux) for I/O readiness notification.

use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossbeam_channel::Receiver;
use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token};

use crate::accumulator::AccumulatorTable;
use crate::buffer::send_copy::SendCopyPool;
use crate::config::Config;
use crate::connection::ConnectionTable;
use crate::error::Error;

use super::ctx::{
    ConnToken, DriverCtx, EventHandler, MioConnSend, PendingConnect,
};

/// Special mio token for the eventfd (worker wake-up).
const EVENTFD_TOKEN: Token = Token(usize::MAX);

/// All driver state that can be borrowed via `DriverCtx`.
///
/// Separated from `handler` and `accumulators` so that Rust's
/// field-level borrow splitting allows calling handler callbacks
/// while holding a `DriverCtx`.
struct MioDriver {
    poll: Poll,
    events: Events,

    // Connection state
    connections: ConnectionTable,
    conn_fds: Vec<RawFd>,
    send_states: Vec<MioConnSend>,

    // Buffers
    send_copy_pool: SendCopyPool,
    recv_buf: Vec<u8>,

    // Acceptor channel
    accept_rx: Option<Receiver<(RawFd, SocketAddr)>>,

    // Wake mechanism
    eventfd: RawFd,

    // State
    shutdown_flag: Arc<AtomicBool>,
    shutdown_requested: bool,
    tcp_nodelay: bool,
    connect_addrs: Vec<libc::sockaddr_storage>,
    pending_connects: Vec<PendingConnect>,
    pending_send_completions: Vec<(u32, u32, io::Result<u32>)>,
    pending_close: Vec<u32>,
    max_connections: u32,
}

impl MioDriver {
    fn make_ctx(&mut self) -> DriverCtx<'_> {
        DriverCtx {
            connections: &mut self.connections,
            send_copy_pool: &mut self.send_copy_pool,
            conn_fds: &mut self.conn_fds,
            send_states: &mut self.send_states,
            shutdown_requested: &mut self.shutdown_requested,
            tcp_nodelay: self.tcp_nodelay,
            connect_addrs: &mut self.connect_addrs,
            pending_connects: &mut self.pending_connects,
            pending_send_completions: &mut self.pending_send_completions,
            pending_close: &mut self.pending_close,
            max_connections: self.max_connections,
        }
    }
}

/// mio-based event loop that drives an [`EventHandler`] via epoll readiness.
pub struct MioEventLoop<H: EventHandler> {
    driver: MioDriver,
    handler: H,
    accumulators: AccumulatorTable,
}

impl<H: EventHandler> MioEventLoop<H> {
    /// Create a new mio event loop.
    pub fn new(
        config: &Config,
        handler: H,
        accept_rx: Option<Receiver<(RawFd, SocketAddr)>>,
        eventfd: RawFd,
        shutdown_flag: Arc<AtomicBool>,
    ) -> Result<Self, Error> {
        let poll = Poll::new().map_err(Error::Io)?;
        let events = Events::with_capacity(1024);

        let max_connections = config.max_connections;
        let connections = ConnectionTable::new(max_connections);
        let accumulators =
            AccumulatorTable::new(max_connections, config.recv_accumulator_capacity);

        let mut conn_fds = Vec::with_capacity(max_connections as usize);
        conn_fds.resize(max_connections as usize, -1i32);

        let mut send_states = Vec::with_capacity(max_connections as usize);
        for _ in 0..max_connections {
            send_states.push(MioConnSend::new());
        }

        let send_copy_pool =
            SendCopyPool::new(config.send_copy_count, config.send_copy_slot_size);

        let recv_buf = vec![0u8; config.recv_buffer.buffer_size as usize];

        // Register eventfd with mio for acceptor/shutdown wakeups.
        poll.registry()
            .register(
                &mut SourceFd(&eventfd),
                EVENTFD_TOKEN,
                Interest::READABLE,
            )
            .map_err(Error::Io)?;

        Ok(MioEventLoop {
            driver: MioDriver {
                poll,
                events,
                connections,
                conn_fds,
                send_states,
                send_copy_pool,
                recv_buf,
                accept_rx,
                eventfd,
                shutdown_flag,
                shutdown_requested: false,
                tcp_nodelay: config.tcp_nodelay,
                connect_addrs: vec![unsafe { std::mem::zeroed() }; max_connections as usize],
                pending_connects: Vec::new(),
                pending_send_completions: Vec::new(),
                pending_close: Vec::new(),
                max_connections,
            },
            handler,
            accumulators,
        })
    }

    /// Run the event loop until shutdown.
    pub fn run(&mut self) -> Result<(), Error> {
        let tick_timeout = Duration::from_micros(
            if self.driver.shutdown_flag.load(Ordering::Relaxed) {
                0
            } else {
                1000 // 1ms default tick
            },
        );

        loop {
            // Poll for events (with tick timeout).
            match self
                .driver
                .poll
                .poll(&mut self.driver.events, Some(tick_timeout))
            {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Error::Io(e)),
            }

            // Process all events.
            // We collect tokens first to avoid borrow issues with self.
            let mut readable = Vec::new();
            let mut writable = Vec::new();
            let mut eventfd_signaled = false;

            for event in self.driver.events.iter() {
                let token = event.token();
                if token == EVENTFD_TOKEN {
                    eventfd_signaled = true;
                    continue;
                }

                let conn_index = token.0 as u32;
                if conn_index >= self.driver.max_connections {
                    continue;
                }

                if event.is_error() || event.is_read_closed() {
                    // Connection error or remote close — handle in readable path
                    readable.push(conn_index);
                    continue;
                }

                if event.is_readable() {
                    readable.push(conn_index);
                }
                if event.is_writable() {
                    writable.push(conn_index);
                }
            }

            // Handle eventfd (acceptor wakeup / external signal).
            if eventfd_signaled {
                self.drain_eventfd();
                self.drain_accept_channel();
                // Re-register eventfd (edge-triggered, consumed by read).
                let _ = self.driver.poll.registry().reregister(
                    &mut SourceFd(&self.driver.eventfd),
                    EVENTFD_TOKEN,
                    Interest::READABLE,
                );
            }

            // Handle writable events first (complete pending sends).
            for conn_index in writable {
                if self.driver.conn_fds[conn_index as usize] < 0 {
                    continue;
                }
                self.handle_writable(conn_index);
            }

            // Handle readable events.
            for conn_index in readable {
                if self.driver.conn_fds[conn_index as usize] < 0 {
                    continue;
                }
                self.handle_readable(conn_index);
            }

            // Check pending connect timeouts.
            self.check_connect_timeouts();

            // Fire deferred send completions.
            self.fire_pending_send_completions();

            // Process deferred closes.
            self.process_pending_closes();

            // on_tick callback.
            {
                let mut ctx = self.driver.make_ctx();
                self.handler.on_tick(&mut ctx);
            }

            // Fire any send completions generated by on_tick.
            self.fire_pending_send_completions();
            self.process_pending_closes();

            // Check shutdown.
            if self.driver.shutdown_requested
                || self.driver.shutdown_flag.load(Ordering::Relaxed)
            {
                self.run_shutdown();
                return Ok(());
            }
        }
    }

    // ── Event handlers ───────────────────────────────────────────────

    fn handle_readable(&mut self, conn_index: u32) {
        let fd = self.driver.conn_fds[conn_index as usize];
        if fd < 0 {
            return;
        }

        // Check if this is a connecting socket.
        if let Some(state) = self.driver.connections.get(conn_index) {
            if matches!(state.recv_mode, crate::connection::RecvMode::Connecting) {
                // Connect result — handled via writable, ignore readable during connect.
                return;
            }
            if matches!(state.recv_mode, crate::connection::RecvMode::Closed) {
                return;
            }
        } else {
            return;
        }

        // Edge-triggered: read in a loop until EAGAIN.
        let mut total_read = 0usize;
        loop {
            let n = unsafe {
                libc::read(
                    fd,
                    self.driver.recv_buf.as_mut_ptr() as *mut libc::c_void,
                    self.driver.recv_buf.len(),
                )
            };

            if n > 0 {
                let bytes = n as usize;
                total_read += bytes;
                self.accumulators
                    .append(conn_index, &self.driver.recv_buf[..bytes]);
                crate::metrics::BYTES_RECEIVED.add(bytes as _);

                if bytes < self.driver.recv_buf.len() {
                    // Read less than buffer size — no more data available.
                    break;
                }
                // Buffer was full — there might be more data, loop.
            } else if n == 0 {
                // EOF — remote closed.
                self.close_connection(conn_index);
                return;
            } else {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    break;
                }
                // Real error.
                self.close_connection(conn_index);
                return;
            }
        }

        if total_read == 0 {
            return;
        }

        // Call on_data with accumulated data.
        self.dispatch_on_data(conn_index);
    }

    fn dispatch_on_data(&mut self, conn_index: u32) {
        // Extract generation first (Copy value — borrow released immediately).
        let generation = match self.driver.connections.get(conn_index) {
            Some(state) => state.generation,
            None => return,
        };
        let token = ConnToken::new(conn_index, generation);

        // Three disjoint field borrows:
        // - self.accumulators (immutable, for data)
        // - self.driver (mutable, for DriverCtx)
        // - self.handler (mutable, for on_data callback)
        let data = self.accumulators.data(conn_index);
        if data.is_empty() {
            return;
        }
        let mut ctx = self.driver.make_ctx();
        let consumed = self.handler.on_data(&mut ctx, token, data);
        drop(ctx);

        if consumed > 0 {
            self.accumulators.consume(conn_index, consumed);
        }
    }

    fn handle_writable(&mut self, conn_index: u32) {
        let fd = self.driver.conn_fds[conn_index as usize];
        if fd < 0 {
            return;
        }

        // Check if this is a pending connect.
        if let Some(state) = self.driver.connections.get(conn_index) {
            if matches!(state.recv_mode, crate::connection::RecvMode::Connecting) {
                self.complete_connect(conn_index, fd);
                return;
            }
        }

        // Normal writable — drain send queue.
        Self::drain_writes(&mut self.driver, conn_index, fd);
    }

    /// Drain pending writes for a connection (edge-triggered: loop until EAGAIN).
    fn drain_writes(driver: &mut MioDriver, conn_index: u32, fd: RawFd) {
        let send_state = &mut driver.send_states[conn_index as usize];
        if !send_state.in_flight {
            return;
        }

        loop {
            let ptr = send_state.current_ptr;
            let remaining = send_state.current_remaining;

            if remaining == 0 {
                // Should not happen, but be safe.
                break;
            }

            let n = unsafe {
                libc::write(fd, ptr as *const libc::c_void, remaining as usize)
            };

            if n > 0 {
                let written = n as u32;
                crate::metrics::BYTES_SENT.add(written as _);

                if written >= remaining {
                    // Current slot fully written.
                    let slot = send_state.current_slot;
                    let original_len = driver.send_copy_pool.original_len(slot);
                    driver.send_copy_pool.release(slot);

                    let generation = driver.connections.generation(conn_index);
                    driver
                        .pending_send_completions
                        .push((conn_index, generation, Ok(original_len)));

                    // Pop next queued slot.
                    if let Some((next_slot, next_ptr, next_len)) =
                        send_state.queue.pop_front()
                    {
                        send_state.current_slot = next_slot;
                        send_state.current_ptr = next_ptr;
                        send_state.current_remaining = next_len;
                        send_state.current_total = next_len;
                        // Continue loop to try writing the next slot.
                    } else {
                        send_state.in_flight = false;
                        send_state.current_slot = u16::MAX;
                        break;
                    }
                } else {
                    // Partial write — advance pointer.
                    send_state.current_ptr =
                        send_state.current_ptr.wrapping_add(written as usize);
                    send_state.current_remaining -= written;
                    // Continue loop to try writing more.
                }
            } else if n == 0
                || (n < 0
                    && io::Error::last_os_error().kind() == io::ErrorKind::WouldBlock)
            {
                // EAGAIN — wait for next writable event.
                break;
            } else {
                // Write error — release all pending sends.
                let err = io::Error::last_os_error();
                let slot = send_state.current_slot;
                driver.send_copy_pool.release(slot);
                for (qs, _, _) in send_state.queue.drain(..) {
                    driver.send_copy_pool.release(qs);
                }
                send_state.in_flight = false;
                send_state.current_slot = u16::MAX;

                let generation = driver.connections.generation(conn_index);
                driver
                    .pending_send_completions
                    .push((conn_index, generation, Err(err)));
                break;
            }
        }
    }

    /// Complete an outbound connect (writable event on a connecting socket).
    fn complete_connect(&mut self, conn_index: u32, fd: RawFd) {
        // Check SO_ERROR to see if connect succeeded.
        let mut err_val: libc::c_int = 0;
        let mut err_len: libc::socklen_t =
            std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                &mut err_val as *mut _ as *mut libc::c_void,
                &mut err_len,
            )
        };

        let result = if ret < 0 {
            Err(io::Error::last_os_error())
        } else if err_val != 0 {
            Err(io::Error::from_raw_os_error(err_val))
        } else {
            Ok(())
        };

        // Remove from pending connects.
        self.driver
            .pending_connects
            .retain(|pc| pc.conn_index != conn_index);

        if result.is_ok() {
            // Mark as established.
            if let Some(state) = self.driver.connections.get_mut(conn_index) {
                state.recv_mode = crate::connection::RecvMode::Multi;
                state.established = true;
            }

            // Re-register for readable + writable.
            let _ = self.driver.poll.registry().reregister(
                &mut SourceFd(&fd),
                Token(conn_index as usize),
                Interest::READABLE | Interest::WRITABLE,
            );

            let generation = self.driver.connections.generation(conn_index);
            let token = ConnToken::new(conn_index, generation);
            let mut ctx = self.driver.make_ctx();
            self.handler.on_connect(&mut ctx, token, Ok(()));
        } else {
            // Connect failed — close and fire callback.
            let generation = self.driver.connections.generation(conn_index);
            let token = ConnToken::new(conn_index, generation);
            let err = result.unwrap_err();

            // Deregister and close.
            let _ = self
                .driver
                .poll
                .registry()
                .deregister(&mut SourceFd(&fd));
            unsafe {
                libc::close(fd);
            }
            self.driver.conn_fds[conn_index as usize] = -1;
            self.driver.connections.release(conn_index);

            let mut ctx = self.driver.make_ctx();
            self.handler.on_connect(&mut ctx, token, Err(err));
        }
    }

    // ── Accept handling ──────────────────────────────────────────────

    fn drain_eventfd(&mut self) {
        // Read the eventfd to reset it.
        let mut buf = [0u8; 8];
        unsafe {
            libc::read(
                self.driver.eventfd,
                buf.as_mut_ptr() as *mut libc::c_void,
                8,
            );
        }
    }

    fn drain_accept_channel(&mut self) {
        // Clone the receiver handle to avoid holding an immutable borrow on
        // self.driver across the loop body (crossbeam Receiver::clone is cheap).
        let rx = match self.driver.accept_rx {
            Some(ref rx) => rx.clone(),
            None => return,
        };

        while let Ok((fd, peer_addr)) = rx.try_recv() {
            // Allocate connection slot.
            let conn_index = match self.driver.connections.allocate() {
                Some(idx) => idx,
                None => {
                    unsafe {
                        libc::close(fd);
                    }
                    continue;
                }
            };

            let idx = conn_index as usize;
            self.driver.conn_fds[idx] = fd;
            self.driver.send_states[idx] = MioConnSend::new();

            if let Some(state) = self.driver.connections.get_mut(conn_index) {
                state.peer_addr = Some(peer_addr);
                state.established = true;
            }

            // Register with mio for readable + writable (edge-triggered).
            if let Err(_e) = self.driver.poll.registry().register(
                &mut SourceFd(&fd),
                Token(idx),
                Interest::READABLE | Interest::WRITABLE,
            ) {
                unsafe {
                    libc::close(fd);
                }
                self.driver.conn_fds[idx] = -1;
                self.driver.connections.release(conn_index);
                continue;
            }

            crate::metrics::CONNECTIONS_ACCEPTED.increment();
            crate::metrics::CONNECTIONS_ACTIVE
                .set(self.driver.connections.active_count() as _);

            let generation = self.driver.connections.generation(conn_index);
            let token = ConnToken::new(conn_index, generation);
            let mut ctx = self.driver.make_ctx();
            self.handler.on_accept(&mut ctx, token);

            // Fire any send completions generated by on_accept.
            self.fire_pending_send_completions();
            self.process_pending_closes();
        }
    }

    // ── Deferred operations ──────────────────────────────────────────

    fn fire_pending_send_completions(&mut self) {
        loop {
            let completions =
                std::mem::take(&mut self.driver.pending_send_completions);
            if completions.is_empty() {
                break;
            }
            for (conn_index, generation, result) in completions {
                // Check connection is still active with same generation.
                let current_gen = self.driver.connections.generation(conn_index);
                if current_gen != generation {
                    continue;
                }
                if self.driver.connections.get(conn_index).is_none() {
                    continue;
                }
                let token = ConnToken::new(conn_index, generation);
                let mut ctx = self.driver.make_ctx();
                self.handler.on_send_complete(&mut ctx, token, result);
            }
        }
    }

    fn process_pending_closes(&mut self) {
        let closes = std::mem::take(&mut self.driver.pending_close);
        for conn_index in closes {
            self.close_connection(conn_index);
        }
    }

    fn check_connect_timeouts(&mut self) {
        let now = std::time::Instant::now();
        let mut timed_out = Vec::new();

        self.driver.pending_connects.retain(|pc| {
            if let Some(deadline) = pc.deadline {
                if now >= deadline {
                    timed_out.push(pc.conn_index);
                    return false;
                }
            }
            true
        });

        for conn_index in timed_out {
            let fd = self.driver.conn_fds[conn_index as usize];
            if fd < 0 {
                continue;
            }

            let generation = self.driver.connections.generation(conn_index);
            let token = ConnToken::new(conn_index, generation);

            // Deregister and close.
            let _ = self
                .driver
                .poll
                .registry()
                .deregister(&mut SourceFd(&fd));
            unsafe {
                libc::close(fd);
            }
            self.driver.conn_fds[conn_index as usize] = -1;
            self.driver.connections.release(conn_index);

            let err = io::Error::new(io::ErrorKind::TimedOut, "connect timed out");
            let mut ctx = self.driver.make_ctx();
            self.handler.on_connect(&mut ctx, token, Err(err));
        }
    }

    // ── Connection lifecycle ─────────────────────────────────────────

    fn close_connection(&mut self, conn_index: u32) {
        let fd = self.driver.conn_fds[conn_index as usize];
        if fd < 0 {
            return;
        }

        let was_established = self
            .driver
            .connections
            .get(conn_index)
            .map_or(false, |s| s.established);
        let generation = self.driver.connections.generation(conn_index);

        // Release pending sends.
        let send_state = &mut self.driver.send_states[conn_index as usize];
        if send_state.in_flight {
            if send_state.current_slot != u16::MAX {
                self.driver.send_copy_pool.release(send_state.current_slot);
            }
            for (slot, _, _) in send_state.queue.drain(..) {
                self.driver.send_copy_pool.release(slot);
            }
            send_state.in_flight = false;
            send_state.current_slot = u16::MAX;
        }

        // Remove any pending send completions for this connection.
        self.driver
            .pending_send_completions
            .retain(|(ci, _, _)| *ci != conn_index);

        // Remove from pending connects.
        self.driver
            .pending_connects
            .retain(|pc| pc.conn_index != conn_index);

        // Deregister from mio.
        let _ = self
            .driver
            .poll
            .registry()
            .deregister(&mut SourceFd(&fd));

        // Close fd.
        unsafe {
            libc::close(fd);
        }
        self.driver.conn_fds[conn_index as usize] = -1;

        // Reset accumulator.
        self.accumulators.reset(conn_index);

        // Release connection slot.
        self.driver.connections.release(conn_index);

        crate::metrics::CONNECTIONS_CLOSED.increment();
        crate::metrics::CONNECTIONS_ACTIVE
            .set(self.driver.connections.active_count() as _);

        // Fire on_close if the connection was established.
        if was_established {
            let token = ConnToken::new(conn_index, generation);
            let mut ctx = self.driver.make_ctx();
            self.handler.on_close(&mut ctx, token);
        }
    }

    // ── Shutdown ─────────────────────────────────────────────────────

    fn run_shutdown(&mut self) {
        // Close all active connections.
        for i in 0..self.driver.max_connections {
            if self.driver.conn_fds[i as usize] >= 0 {
                self.close_connection(i);
            }
        }
    }
}
