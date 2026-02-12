use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use io_uring::cqueue;

use crate::accumulator::AccumulatorTable;
use crate::buffer::fixed::FixedBufferRegistry;
use crate::buffer::provided::ProvidedBufRing;
use crate::buffer::send_copy::SendCopyPool;
use crate::buffer::send_slab::InFlightSendSlab;
use crate::completion::{OpTag, UserData};
use crate::config::Config;
use crate::connection::{ConnectionTable, RecvMode};
use crate::handler::{ConnToken, DriverCtx, EventHandler};
use crate::ring::Ring;

/// The core event loop that drives io_uring and dispatches to the EventHandler.
pub struct EventLoop<H: EventHandler> {
    ring: Ring,
    connections: ConnectionTable,
    fixed_buffers: FixedBufferRegistry,
    provided_bufs: ProvidedBufRing,
    send_copy_pool: SendCopyPool,
    send_slab: InFlightSendSlab,
    accumulators: AccumulatorTable,
    handler: H,
    pending_replenish: Vec<u16>,
    accept_rx: Option<crossbeam_channel::Receiver<(RawFd, SocketAddr)>>,
    eventfd: RawFd,
    eventfd_buf: [u8; 8],
    /// Deadline-based flush interval. None = disabled (SQPOLL or explicit 0).
    flush_interval: Option<Duration>,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_local: bool,
    #[cfg(feature = "tls")]
    tls_table: Option<crate::tls::TlsTable>,
    #[cfg(feature = "tls")]
    tls_scratch: Vec<u8>,
    /// Pre-allocated sockaddr storage for outbound connect SQEs.
    connect_addrs: Vec<libc::sockaddr_storage>,
    /// Pre-allocated timespec storage for connect timeouts.
    connect_timespecs: Vec<io_uring::types::Timespec>,
    /// Pre-allocated batch buffer for draining CQEs.
    cqe_batch: Vec<(u64, i32, u32)>,
    /// Whether to set TCP_NODELAY on connections.
    tcp_nodelay: bool,
    /// Tick timeout duration. When set, a timeout SQE ensures the event loop
    /// wakes periodically even when no I/O completions are pending.
    tick_timeout_ts: Option<io_uring::types::Timespec>,
    /// Whether a tick timeout SQE is currently in-flight.
    tick_timeout_armed: bool,
}

impl<H: EventHandler> EventLoop<H> {
    /// Create a new event loop for a worker thread.
    pub fn new(
        config: &Config,
        handler: H,
        accept_rx: Option<crossbeam_channel::Receiver<(RawFd, SocketAddr)>>,
        eventfd: RawFd,
        shutdown_flag: Arc<AtomicBool>,
    ) -> Result<Self, crate::error::Error> {
        let ring = Ring::setup(config)?;

        let fixed_buffers = FixedBufferRegistry::new(&config.registered_regions);

        let provided_bufs = ProvidedBufRing::new(
            config.recv_buffer.bgid,
            config.recv_buffer.ring_size,
            config.recv_buffer.buffer_size,
        )?;

        // Register resources with the kernel
        ring.register_buffers(&fixed_buffers)?;
        ring.register_files_sparse(config.max_connections)?;
        ring.register_buf_ring(&provided_bufs)?;

        let connections = ConnectionTable::new(config.max_connections);
        let send_copy_pool = SendCopyPool::new(config.send_copy_count, config.send_copy_slot_size);
        let send_slab = InFlightSendSlab::new(config.send_slab_slots);
        let accumulators =
            AccumulatorTable::new(config.max_connections, config.recv_accumulator_capacity);

        // Deadline flush: disabled when SQPOLL (kernel polls SQ) or interval is 0.
        let flush_interval = if config.sqpoll || config.flush_interval_us == 0 {
            None
        } else {
            Some(Duration::from_micros(config.flush_interval_us))
        };

        #[cfg(feature = "tls")]
        let tls_table = {
            let has_server = config.tls.is_some();
            let has_client = config.tls_client.is_some();
            if has_server || has_client {
                Some(crate::tls::TlsTable::new(
                    config.max_connections,
                    config.tls.as_ref().map(|tc| tc.server_config.clone()),
                    config
                        .tls_client
                        .as_ref()
                        .map(|tc| tc.client_config.clone()),
                ))
            } else {
                None
            }
        };
        #[cfg(feature = "tls")]
        let tls_scratch = vec![0u8; 16384];

        let mut connect_addrs = Vec::with_capacity(config.max_connections as usize);
        connect_addrs.resize(config.max_connections as usize, unsafe {
            std::mem::zeroed()
        });

        let mut connect_timespecs = Vec::with_capacity(config.max_connections as usize);
        connect_timespecs.resize(
            config.max_connections as usize,
            io_uring::types::Timespec::new(),
        );

        Ok(EventLoop {
            ring,
            connections,
            fixed_buffers,
            provided_bufs,
            send_copy_pool,
            send_slab,
            accumulators,
            handler,
            pending_replenish: Vec::with_capacity(config.recv_buffer.ring_size as usize),
            accept_rx,
            eventfd,
            eventfd_buf: [0u8; 8],
            flush_interval,
            shutdown_flag,
            shutdown_local: false,
            #[cfg(feature = "tls")]
            tls_table,
            #[cfg(feature = "tls")]
            tls_scratch,
            connect_addrs,
            connect_timespecs,
            cqe_batch: Vec::with_capacity(config.sq_entries as usize * 4),
            tcp_nodelay: config.tcp_nodelay,
            tick_timeout_ts: if config.tick_timeout_us > 0 {
                Some(
                    io_uring::types::Timespec::new()
                        .sec(config.tick_timeout_us / 1_000_000)
                        .nsec((config.tick_timeout_us % 1_000_000) as u32 * 1000),
                )
            } else {
                None
            },
            tick_timeout_armed: false,
        })
    }

    /// Run the event loop. This blocks the current thread.
    pub fn run(&mut self) -> Result<(), crate::error::Error> {
        // Always arm eventfd read — needed for shutdown wakeup even in client-only mode.
        self.ring
            .submit_eventfd_read(self.eventfd, self.eventfd_buf.as_mut_ptr())?;

        // Kick the eventfd so the first submit_and_wait(1) returns immediately.
        // Without this, client-only mode (no acceptor) would deadlock because
        // nobody writes to the eventfd, so the eventfd read never completes.
        // In server mode this causes one harmless handle_eventfd_read at startup.
        let kick: u64 = 1;
        unsafe {
            libc::write(self.eventfd, &kick as *const u64 as *const libc::c_void, 8);
        }

        loop {
            // Arm a tick timeout before blocking so on_tick fires periodically
            // even when no I/O completions are pending (e.g., client-only mode
            // between phases).
            if !self.tick_timeout_armed
                && let Some(ref ts) = self.tick_timeout_ts
            {
                let ud = UserData::encode(OpTag::TickTimeout, 0, 0);
                let _ = self.ring.submit_tick_timeout(ts as *const _, ud.raw());
                self.tick_timeout_armed = true;
            }

            self.ring.submit_and_wait(1)?;

            self.drain_completions();

            // Check for shutdown after processing completions.
            if self.shutdown_local || self.shutdown_flag.load(Ordering::Relaxed) {
                self.run_shutdown();
                return Ok(());
            }

            // Batch replenish recv buffers.
            if !self.pending_replenish.is_empty() {
                self.provided_bufs.replenish_batch(&self.pending_replenish);
                self.pending_replenish.clear();
            }

            // on_tick callback
            let mut ctx = DriverCtx {
                ring: &mut self.ring,
                connections: &mut self.connections,
                fixed_buffers: &mut self.fixed_buffers,
                send_copy_pool: &mut self.send_copy_pool,
                send_slab: &mut self.send_slab,
                #[cfg(feature = "tls")]
                tls_table: match self.tls_table {
                    Some(ref mut t) => t as *mut crate::tls::TlsTable,
                    None => std::ptr::null_mut(),
                },
                shutdown_requested: &mut self.shutdown_local,
                connect_addrs: &mut self.connect_addrs,
                tcp_nodelay: self.tcp_nodelay,
                connect_timespecs: &mut self.connect_timespecs,
            };
            self.handler.on_tick(&mut ctx);
        }
    }

    /// Shutdown: close all connections, drain remaining CQEs, close eventfd.
    fn run_shutdown(&mut self) {
        // 1. Close all active connections.
        let max = self.connections.max_slots();
        for i in 0..max {
            if self.connections.get(i).is_some() {
                let _ = self.ring.submit_close(i);
            }
        }

        // 2. Submit + drain loop until all connections are closed.
        //    Arm a timeout SQE each iteration so submit_and_wait(1) never blocks
        //    indefinitely (the tick timeout from the main loop is not armed here).
        let shutdown_ts = io_uring::types::Timespec::new().nsec(100_000_000); // 100ms
        for _ in 0..100 {
            if self.connections.active_count() == 0 {
                break;
            }
            let ud = UserData::encode(OpTag::TickTimeout, 0, 0);
            let _ = self.ring.submit_tick_timeout(&shutdown_ts, ud.raw());
            if self.ring.submit_and_wait(1).is_err() {
                break;
            }

            self.cqe_batch.clear();
            {
                let cq = self.ring.ring.completion();
                for cqe in cq {
                    self.cqe_batch
                        .push((cqe.user_data(), cqe.result(), cqe.flags()));
                }
            }

            for i in 0..self.cqe_batch.len() {
                let (user_data_raw, _result, flags) = self.cqe_batch[i];
                let ud = UserData(user_data_raw);
                let tag = match ud.tag() {
                    Some(t) => t,
                    None => continue,
                };

                match tag {
                    OpTag::Send => {
                        let pool_slot = ud.payload() as u16;
                        self.send_copy_pool.release(pool_slot);
                    }
                    OpTag::SendMsgZc => {
                        let slab_idx = ud.payload() as u16;
                        if !self.send_slab.in_use(slab_idx) {
                            continue;
                        }
                        if cqueue::notif(flags) {
                            self.send_slab.dec_pending_notifs(slab_idx);
                            if self.send_slab.should_release(slab_idx) {
                                let pool_slot = self.send_slab.release(slab_idx);
                                if pool_slot != u16::MAX {
                                    self.send_copy_pool.release(pool_slot);
                                }
                            }
                        } else {
                            self.send_slab.inc_pending_notifs(slab_idx);
                            self.send_slab.mark_awaiting_notifications(slab_idx);
                            if self.send_slab.should_release(slab_idx) {
                                let pool_slot = self.send_slab.release(slab_idx);
                                if pool_slot != u16::MAX {
                                    self.send_copy_pool.release(pool_slot);
                                }
                            }
                        }
                    }
                    OpTag::Close => {
                        let conn_index = ud.conn_index();
                        #[cfg(feature = "tls")]
                        if let Some(ref mut tls_table) = self.tls_table {
                            tls_table.remove(conn_index);
                        }
                        self.connections.release(conn_index);
                    }
                    #[cfg(feature = "tls")]
                    OpTag::TlsSend => {
                        let pool_slot = ud.payload() as u16;
                        self.send_copy_pool.release(pool_slot);
                    }
                    _ => {}
                }
            }
        }

        // 3. Close the eventfd.
        unsafe {
            libc::close(self.eventfd);
        }
    }

    fn drain_completions(&mut self) {
        self.cqe_batch.clear();

        {
            let cq = self.ring.ring.completion();
            for cqe in cq {
                self.cqe_batch
                    .push((cqe.user_data(), cqe.result(), cqe.flags()));
            }
        }

        if let Some(interval) = self.flush_interval {
            let mut last_flush = Instant::now();
            for i in 0..self.cqe_batch.len() {
                let (user_data_raw, result, flags) = self.cqe_batch[i];
                self.dispatch_cqe(user_data_raw, result, flags);
                let now = Instant::now();
                if now.duration_since(last_flush) >= interval {
                    let _ = self.ring.flush();
                    last_flush = now;
                }
            }
        } else {
            for i in 0..self.cqe_batch.len() {
                let (user_data_raw, result, flags) = self.cqe_batch[i];
                self.dispatch_cqe(user_data_raw, result, flags);
            }
        }
    }

    fn dispatch_cqe(&mut self, user_data_raw: u64, result: i32, flags: u32) {
        let ud = UserData(user_data_raw);
        let tag = match ud.tag() {
            Some(t) => t,
            None => return,
        };

        match tag {
            OpTag::RecvMulti => self.handle_recv_multi(ud, result, flags),
            OpTag::Send => self.handle_send(ud, result),
            OpTag::SendMsgZc => self.handle_send_msg_zc(ud, result, flags),
            OpTag::Close => self.handle_close(ud),
            OpTag::Shutdown => {}
            OpTag::EventFdRead => self.handle_eventfd_read(),
            #[cfg(feature = "tls")]
            OpTag::TlsSend => self.handle_tls_send(ud, result),
            OpTag::Connect => self.handle_connect(ud, result),
            OpTag::Timeout => self.handle_timeout(ud, result),
            OpTag::Cancel => {} // informational only
            OpTag::TickTimeout => {
                self.tick_timeout_armed = false;
            }
        }
    }

    fn handle_recv_multi(&mut self, ud: UserData, result: i32, flags: u32) {
        let conn_index = ud.conn_index();
        let has_more = cqueue::more(flags);

        let conn = match self.connections.get(conn_index) {
            Some(c) => c,
            None => return,
        };
        let generation = conn.generation;
        let token = ConnToken::new(conn_index, generation);

        if result <= 0 {
            if result == 0 {
                // EOF
                self.close_connection(conn_index);
                return;
            }
            let errno = -result;
            if errno == libc::ENOBUFS {
                if !has_more {
                    let _ = self.ring.submit_multishot_recv(conn_index);
                }
            } else if errno == libc::ECANCELED {
                // User-initiated cancel — don't re-arm, don't close.
                return;
            } else if !has_more {
                self.close_connection(conn_index);
            }
            return;
        }

        // Extract buffer ID from CQE flags.
        let bid = match cqueue::buffer_select(flags) {
            Some(bid) => bid,
            None => return,
        };

        let bytes_received = result as u32;
        let (buf_ptr, _) = self.provided_bufs.get_buffer(bid);

        let data = unsafe { std::slice::from_raw_parts(buf_ptr, bytes_received as usize) };

        // Schedule buffer for replenishment.
        self.pending_replenish.push(bid);

        // TLS path
        #[cfg(feature = "tls")]
        let is_tls_conn = self.tls_table.as_ref().is_some_and(|t| t.has(conn_index));
        #[cfg(not(feature = "tls"))]
        let is_tls_conn = false;

        if is_tls_conn {
            #[cfg(feature = "tls")]
            {
                let tls_table = self.tls_table.as_mut().unwrap();
                let result = crate::tls::feed_tls_recv(
                    tls_table,
                    &mut self.accumulators,
                    &mut self.ring,
                    &mut self.send_copy_pool,
                    &mut self.tls_scratch,
                    conn_index,
                    data,
                );

                match result {
                    crate::tls::TlsRecvResult::HandshakeJustCompleted => {
                        let is_outbound = self
                            .connections
                            .get(conn_index)
                            .map(|c| c.outbound)
                            .unwrap_or(false);

                        if is_outbound {
                            if let Some(cs) = self.connections.get_mut(conn_index) {
                                cs.established = true;
                            }
                            let tls_table = self.tls_table.as_mut().unwrap();
                            let mut ctx = DriverCtx {
                                ring: &mut self.ring,
                                connections: &mut self.connections,
                                fixed_buffers: &mut self.fixed_buffers,
                                send_copy_pool: &mut self.send_copy_pool,
                                send_slab: &mut self.send_slab,
                                tls_table: tls_table as *mut crate::tls::TlsTable,
                                shutdown_requested: &mut self.shutdown_local,
                                connect_addrs: &mut self.connect_addrs,
                                tcp_nodelay: self.tcp_nodelay,
                                connect_timespecs: &mut self.connect_timespecs,
                            };
                            self.handler.on_connect(&mut ctx, token, Ok(()));
                        } else {
                            if let Some(cs) = self.connections.get_mut(conn_index) {
                                cs.established = true;
                            }
                            let tls_table = self.tls_table.as_mut().unwrap();
                            let mut ctx = DriverCtx {
                                ring: &mut self.ring,
                                connections: &mut self.connections,
                                fixed_buffers: &mut self.fixed_buffers,
                                send_copy_pool: &mut self.send_copy_pool,
                                send_slab: &mut self.send_slab,
                                tls_table: tls_table as *mut crate::tls::TlsTable,
                                shutdown_requested: &mut self.shutdown_local,
                                connect_addrs: &mut self.connect_addrs,
                                tcp_nodelay: self.tcp_nodelay,
                                connect_timespecs: &mut self.connect_timespecs,
                            };
                            self.handler.on_accept(&mut ctx, token);
                        }

                        call_on_data(
                            &mut self.handler,
                            &mut self.accumulators,
                            &mut self.ring,
                            &mut self.connections,
                            &mut self.fixed_buffers,
                            &mut self.send_copy_pool,
                            &mut self.send_slab,
                            &mut self.shutdown_local,
                            conn_index,
                            token,
                            #[cfg(feature = "tls")]
                            &mut self.tls_table,
                            &mut self.connect_addrs,
                            self.tcp_nodelay,
                            &mut self.connect_timespecs,
                        );
                    }
                    crate::tls::TlsRecvResult::Ok => {
                        call_on_data(
                            &mut self.handler,
                            &mut self.accumulators,
                            &mut self.ring,
                            &mut self.connections,
                            &mut self.fixed_buffers,
                            &mut self.send_copy_pool,
                            &mut self.send_slab,
                            &mut self.shutdown_local,
                            conn_index,
                            token,
                            #[cfg(feature = "tls")]
                            &mut self.tls_table,
                            &mut self.connect_addrs,
                            self.tcp_nodelay,
                            &mut self.connect_timespecs,
                        );
                    }
                    crate::tls::TlsRecvResult::Error(_) | crate::tls::TlsRecvResult::Closed => {
                        self.close_connection(conn_index);
                    }
                }
            }
        } else {
            // Plaintext path
            self.accumulators.append(conn_index, data);

            call_on_data(
                &mut self.handler,
                &mut self.accumulators,
                &mut self.ring,
                &mut self.connections,
                &mut self.fixed_buffers,
                &mut self.send_copy_pool,
                &mut self.send_slab,
                &mut self.shutdown_local,
                conn_index,
                token,
                #[cfg(feature = "tls")]
                &mut self.tls_table,
                &mut self.connect_addrs,
                self.tcp_nodelay,
                &mut self.connect_timespecs,
            );
        }

        if !has_more
            && let Some(conn) = self.connections.get(conn_index)
            && matches!(conn.recv_mode, RecvMode::Multi)
        {
            let _ = self.ring.submit_multishot_recv(conn_index);
        }
    }

    fn handle_eventfd_read(&mut self) {
        let accept_rx = match self.accept_rx {
            Some(ref rx) => rx,
            None => return,
        };

        // Drain all (fd, addr) pairs from the channel.
        while let Ok((raw_fd, peer_addr)) = accept_rx.try_recv() {
            let conn_index = match self.connections.allocate() {
                Some(idx) => idx,
                None => {
                    unsafe {
                        libc::close(raw_fd);
                    }
                    continue;
                }
            };

            // Store peer address.
            if let Some(cs) = self.connections.get_mut(conn_index) {
                cs.peer_addr = Some(peer_addr);
            }

            // Register the fd in the direct file table, then close the original.
            if self
                .ring
                .register_files_update(conn_index, &[raw_fd])
                .is_err()
            {
                self.connections.release(conn_index);
                unsafe {
                    libc::close(raw_fd);
                }
                continue;
            }
            unsafe {
                libc::close(raw_fd);
            }

            // Reset accumulator for this connection.
            self.accumulators.reset(conn_index);

            // Arm multishot recv.
            let _ = self.ring.submit_multishot_recv(conn_index);

            // TLS path: create TLS state and defer on_accept until handshake completes.
            #[cfg(feature = "tls")]
            if let Some(ref mut tls_table) = self.tls_table
                && tls_table.has_server_config()
            {
                tls_table.create(conn_index);
                continue; // skip on_accept — deferred until handshake completes
            }

            // Plaintext path: mark established and call on_accept immediately.
            if let Some(cs) = self.connections.get_mut(conn_index) {
                cs.established = true;
            }
            let generation = self.connections.generation(conn_index);
            let token = ConnToken::new(conn_index, generation);
            let mut ctx = DriverCtx {
                ring: &mut self.ring,
                connections: &mut self.connections,
                fixed_buffers: &mut self.fixed_buffers,
                send_copy_pool: &mut self.send_copy_pool,
                send_slab: &mut self.send_slab,
                #[cfg(feature = "tls")]
                tls_table: match self.tls_table {
                    Some(ref mut t) => t as *mut crate::tls::TlsTable,
                    None => std::ptr::null_mut(),
                },
                shutdown_requested: &mut self.shutdown_local,
                connect_addrs: &mut self.connect_addrs,
                tcp_nodelay: self.tcp_nodelay,
                connect_timespecs: &mut self.connect_timespecs,
            };
            self.handler.on_accept(&mut ctx, token);
        }

        // Re-arm eventfd read, unless shutdown was requested.
        if !self.shutdown_flag.load(Ordering::Relaxed) {
            let _ = self
                .ring
                .submit_eventfd_read(self.eventfd, self.eventfd_buf.as_mut_ptr());
        }
    }

    fn handle_send(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let pool_slot = ud.payload() as u16;

        if result > 0 {
            if let Some((ptr, remaining)) =
                self.send_copy_pool.try_advance(pool_slot, result as u32)
            {
                let _ = self
                    .ring
                    .submit_send_copied(conn_index, ptr, remaining, pool_slot);
                return;
            }
            let total = self.send_copy_pool.original_len(pool_slot);
            self.send_copy_pool.release(pool_slot);

            let conn = match self.connections.get(conn_index) {
                Some(c) => c,
                None => return,
            };
            let generation = conn.generation;
            let token = ConnToken::new(conn_index, generation);

            let mut ctx = DriverCtx {
                ring: &mut self.ring,
                connections: &mut self.connections,
                fixed_buffers: &mut self.fixed_buffers,
                send_copy_pool: &mut self.send_copy_pool,
                send_slab: &mut self.send_slab,
                #[cfg(feature = "tls")]
                tls_table: match self.tls_table {
                    Some(ref mut t) => t as *mut crate::tls::TlsTable,
                    None => std::ptr::null_mut(),
                },
                shutdown_requested: &mut self.shutdown_local,
                connect_addrs: &mut self.connect_addrs,
                tcp_nodelay: self.tcp_nodelay,
                connect_timespecs: &mut self.connect_timespecs,
            };
            self.handler.on_send_complete(&mut ctx, token, Ok(total));
            return;
        }

        self.send_copy_pool.release(pool_slot);

        let conn = match self.connections.get(conn_index) {
            Some(c) => c,
            None => return,
        };
        let generation = conn.generation;
        let token = ConnToken::new(conn_index, generation);

        let io_result = if result == 0 {
            Ok(0u32)
        } else {
            Err(io::Error::from_raw_os_error(-result))
        };

        let mut ctx = DriverCtx {
            ring: &mut self.ring,
            connections: &mut self.connections,
            fixed_buffers: &mut self.fixed_buffers,
            send_copy_pool: &mut self.send_copy_pool,
            send_slab: &mut self.send_slab,
            #[cfg(feature = "tls")]
            tls_table: match self.tls_table {
                Some(ref mut t) => t as *mut crate::tls::TlsTable,
                None => std::ptr::null_mut(),
            },
            shutdown_requested: &mut self.shutdown_local,
            connect_addrs: &mut self.connect_addrs,
            tcp_nodelay: self.tcp_nodelay,
            connect_timespecs: &mut self.connect_timespecs,
        };
        self.handler.on_send_complete(&mut ctx, token, io_result);
    }

    fn handle_send_msg_zc(&mut self, ud: UserData, result: i32, flags: u32) {
        let conn_index = ud.conn_index();
        let slab_idx = ud.payload() as u16;

        if !self.send_slab.in_use(slab_idx) {
            return;
        }

        if cqueue::notif(flags) {
            self.send_slab.dec_pending_notifs(slab_idx);
            if self.send_slab.should_release(slab_idx) {
                let pool_slot = self.send_slab.release(slab_idx);
                if pool_slot != u16::MAX {
                    self.send_copy_pool.release(pool_slot);
                }
            }
            return;
        }

        self.send_slab.inc_pending_notifs(slab_idx);

        if result > 0
            && let Some(msg_ptr) = self.send_slab.try_advance(slab_idx, result as u32)
            && self
                .ring
                .submit_send_msg_zc(conn_index, msg_ptr, slab_idx)
                .is_ok()
        {
            return;
        }

        self.send_slab.mark_awaiting_notifications(slab_idx);

        let total_len = self.send_slab.total_len(slab_idx);
        let should_release = self.send_slab.should_release(slab_idx);

        if should_release {
            let pool_slot = self.send_slab.release(slab_idx);
            if pool_slot != u16::MAX {
                self.send_copy_pool.release(pool_slot);
            }
        }

        let conn = match self.connections.get(conn_index) {
            Some(c) => c,
            None => return,
        };
        let generation = conn.generation;
        let token = ConnToken::new(conn_index, generation);

        let io_result = if result >= 0 {
            Ok(total_len)
        } else {
            Err(io::Error::from_raw_os_error(-result))
        };

        let mut ctx = DriverCtx {
            ring: &mut self.ring,
            connections: &mut self.connections,
            fixed_buffers: &mut self.fixed_buffers,
            send_copy_pool: &mut self.send_copy_pool,
            send_slab: &mut self.send_slab,
            #[cfg(feature = "tls")]
            tls_table: match self.tls_table {
                Some(ref mut t) => t as *mut crate::tls::TlsTable,
                None => std::ptr::null_mut(),
            },
            shutdown_requested: &mut self.shutdown_local,
            connect_addrs: &mut self.connect_addrs,
            tcp_nodelay: self.tcp_nodelay,
            connect_timespecs: &mut self.connect_timespecs,
        };
        self.handler.on_send_complete(&mut ctx, token, io_result);
    }

    fn handle_connect(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();

        let conn_state = match self.connections.get(conn_index) {
            Some(c) => c,
            None => return,
        };
        let generation = conn_state.generation;
        let token = ConnToken::new(conn_index, generation);

        if result < 0 {
            let errno = -result;

            // ECANCELED and no timeout armed = user-initiated cancel.
            if errno == libc::ECANCELED {
                let timeout_armed = self
                    .connections
                    .get(conn_index)
                    .map(|c| c.connect_timeout_armed)
                    .unwrap_or(false);
                if !timeout_armed {
                    // User-initiated cancel.
                    let err = io::Error::from_raw_os_error(errno);
                    let mut ctx = DriverCtx {
                        ring: &mut self.ring,
                        connections: &mut self.connections,
                        fixed_buffers: &mut self.fixed_buffers,
                        send_copy_pool: &mut self.send_copy_pool,
                        send_slab: &mut self.send_slab,
                        #[cfg(feature = "tls")]
                        tls_table: match self.tls_table {
                            Some(ref mut t) => t as *mut crate::tls::TlsTable,
                            None => std::ptr::null_mut(),
                        },
                        shutdown_requested: &mut self.shutdown_local,
                        connect_addrs: &mut self.connect_addrs,
                        tcp_nodelay: self.tcp_nodelay,
                        connect_timespecs: &mut self.connect_timespecs,
                    };
                    self.handler.on_connect(&mut ctx, token, Err(err));
                    self.close_connection(conn_index);
                    return;
                }
                // Timeout-initiated cancel — handle_timeout will fire on_connect.
                // Clean up the timeout flag; the Timeout CQE will handle the rest.
                if let Some(cs) = self.connections.get_mut(conn_index) {
                    cs.connect_timeout_armed = false;
                }
                return;
            }

            // Cancel the timeout if one was armed.
            if self
                .connections
                .get(conn_index)
                .map(|c| c.connect_timeout_armed)
                .unwrap_or(false)
            {
                let timeout_ud = UserData::encode(OpTag::Timeout, conn_index, 0);
                let _ = self.ring.submit_async_cancel(timeout_ud.raw(), conn_index);
                if let Some(cs) = self.connections.get_mut(conn_index) {
                    cs.connect_timeout_armed = false;
                }
            }

            // Connect failed.
            #[cfg(feature = "tls")]
            if let Some(ref mut tls_table) = self.tls_table {
                tls_table.remove(conn_index);
            }

            let err = io::Error::from_raw_os_error(errno);
            let mut ctx = DriverCtx {
                ring: &mut self.ring,
                connections: &mut self.connections,
                fixed_buffers: &mut self.fixed_buffers,
                send_copy_pool: &mut self.send_copy_pool,
                send_slab: &mut self.send_slab,
                #[cfg(feature = "tls")]
                tls_table: match self.tls_table {
                    Some(ref mut t) => t as *mut crate::tls::TlsTable,
                    None => std::ptr::null_mut(),
                },
                shutdown_requested: &mut self.shutdown_local,
                connect_addrs: &mut self.connect_addrs,
                tcp_nodelay: self.tcp_nodelay,
                connect_timespecs: &mut self.connect_timespecs,
            };
            self.handler.on_connect(&mut ctx, token, Err(err));

            self.close_connection(conn_index);
            return;
        }

        // Connect succeeded — cancel timeout if armed.
        let timeout_was_armed = self
            .connections
            .get(conn_index)
            .map(|c| c.connect_timeout_armed)
            .unwrap_or(false);
        if timeout_was_armed {
            // Check if the connection is already closed (timeout already fired).
            let still_connecting = self
                .connections
                .get(conn_index)
                .map(|c| matches!(c.recv_mode, RecvMode::Connecting))
                .unwrap_or(false);
            if !still_connecting {
                // Timeout already fired and closed this connection — discard late success.
                if let Some(cs) = self.connections.get_mut(conn_index) {
                    cs.connect_timeout_armed = false;
                }
                return;
            }
            let timeout_ud = UserData::encode(OpTag::Timeout, conn_index, 0);
            let _ = self.ring.submit_async_cancel(timeout_ud.raw(), conn_index);
            if let Some(cs) = self.connections.get_mut(conn_index) {
                cs.connect_timeout_armed = false;
            }
        }

        // Reset accumulator.
        self.accumulators.reset(conn_index);

        // TLS client path
        #[cfg(feature = "tls")]
        if let Some(ref mut tls_table) = self.tls_table
            && tls_table.get_mut(conn_index).is_some()
        {
            crate::tls::flush_tls_output(
                tls_table,
                &mut self.ring,
                &mut self.send_copy_pool,
                conn_index,
            );

            if let Some(cs) = self.connections.get_mut(conn_index) {
                cs.recv_mode = RecvMode::Multi;
            }
            let _ = self.ring.submit_multishot_recv(conn_index);
            return;
        }

        // Plaintext path
        if let Some(cs) = self.connections.get_mut(conn_index) {
            cs.recv_mode = RecvMode::Multi;
            cs.established = true;
        }
        let _ = self.ring.submit_multishot_recv(conn_index);

        let mut ctx = DriverCtx {
            ring: &mut self.ring,
            connections: &mut self.connections,
            fixed_buffers: &mut self.fixed_buffers,
            send_copy_pool: &mut self.send_copy_pool,
            send_slab: &mut self.send_slab,
            #[cfg(feature = "tls")]
            tls_table: match self.tls_table {
                Some(ref mut t) => t as *mut crate::tls::TlsTable,
                None => std::ptr::null_mut(),
            },
            shutdown_requested: &mut self.shutdown_local,
            connect_addrs: &mut self.connect_addrs,
            tcp_nodelay: self.tcp_nodelay,
            connect_timespecs: &mut self.connect_timespecs,
        };
        self.handler.on_connect(&mut ctx, token, Ok(()));
    }

    fn handle_timeout(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();

        // Timeout fired (result == -ETIME) means the connect timed out.
        // If result == -ECANCELED, the timeout was cancelled (connect succeeded first).
        if result != -libc::ETIME {
            return;
        }

        let conn = match self.connections.get(conn_index) {
            Some(c) => c,
            None => return,
        };

        if !matches!(conn.recv_mode, RecvMode::Connecting) {
            // Connection already completed or closed.
            return;
        }

        let generation = conn.generation;
        let token = ConnToken::new(conn_index, generation);

        // Cancel the in-flight connect SQE.
        let connect_ud = UserData::encode(OpTag::Connect, conn_index, 0);
        let _ = self.ring.submit_async_cancel(connect_ud.raw(), conn_index);

        // Leave connect_timeout_armed == true so that when the Connect CQE
        // arrives (ECANCELED or even success), handle_connect sees the flag
        // and takes the silent-return branch instead of firing a second
        // on_connect callback.

        // Fire on_connect with TimedOut error.
        let err = io::Error::new(io::ErrorKind::TimedOut, "connect timed out");

        #[cfg(feature = "tls")]
        if let Some(ref mut tls_table) = self.tls_table {
            tls_table.remove(conn_index);
        }

        let mut ctx = DriverCtx {
            ring: &mut self.ring,
            connections: &mut self.connections,
            fixed_buffers: &mut self.fixed_buffers,
            send_copy_pool: &mut self.send_copy_pool,
            send_slab: &mut self.send_slab,
            #[cfg(feature = "tls")]
            tls_table: match self.tls_table {
                Some(ref mut t) => t as *mut crate::tls::TlsTable,
                None => std::ptr::null_mut(),
            },
            shutdown_requested: &mut self.shutdown_local,
            connect_addrs: &mut self.connect_addrs,
            tcp_nodelay: self.tcp_nodelay,
            connect_timespecs: &mut self.connect_timespecs,
        };
        self.handler.on_connect(&mut ctx, token, Err(err));

        self.close_connection(conn_index);
    }

    fn handle_close(&mut self, ud: UserData) {
        let conn_index = ud.conn_index();

        let was_established = self
            .connections
            .get(conn_index)
            .map(|c| c.established)
            .unwrap_or(false);

        // TLS cleanup.
        #[cfg(feature = "tls")]
        if let Some(ref mut tls_table) = self.tls_table {
            tls_table.remove(conn_index);
        }

        if !was_established {
            self.connections.release(conn_index);
            return;
        }

        // Fire on_close BEFORE release so peer_addr() and other queries
        // still work inside the callback.
        let generation = self.connections.generation(conn_index);
        let token = ConnToken::new(conn_index, generation);
        let mut ctx = DriverCtx {
            ring: &mut self.ring,
            connections: &mut self.connections,
            fixed_buffers: &mut self.fixed_buffers,
            send_copy_pool: &mut self.send_copy_pool,
            send_slab: &mut self.send_slab,
            #[cfg(feature = "tls")]
            tls_table: match self.tls_table {
                Some(ref mut t) => t as *mut crate::tls::TlsTable,
                None => std::ptr::null_mut(),
            },
            shutdown_requested: &mut self.shutdown_local,
            connect_addrs: &mut self.connect_addrs,
            tcp_nodelay: self.tcp_nodelay,
            connect_timespecs: &mut self.connect_timespecs,
        };
        self.handler.on_close(&mut ctx, token);

        self.connections.release(conn_index);
    }

    #[cfg(feature = "tls")]
    fn handle_tls_send(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let pool_slot = ud.payload() as u16;

        if result > 0
            && let Some((ptr, remaining)) =
                self.send_copy_pool.try_advance(pool_slot, result as u32)
        {
            let _ = self
                .ring
                .submit_tls_send(conn_index, ptr, remaining, pool_slot);
            return;
        }
        self.send_copy_pool.release(pool_slot);
    }

    fn close_connection(&mut self, conn_index: u32) {
        if let Some(conn) = self.connections.get_mut(conn_index) {
            if matches!(conn.recv_mode, RecvMode::Closed) {
                return; // already closing — avoid double Close SQE
            }
            conn.recv_mode = RecvMode::Closed;
        } else {
            return;
        }
        let _ = self.ring.submit_close(conn_index);
    }
}

/// Free function for borrow-splitting: calls handler.on_data with disjoint borrows.
#[allow(clippy::too_many_arguments)]
fn call_on_data<H: EventHandler>(
    handler: &mut H,
    accumulators: &mut AccumulatorTable,
    ring: &mut Ring,
    connections: &mut ConnectionTable,
    fixed_buffers: &mut FixedBufferRegistry,
    send_copy_pool: &mut SendCopyPool,
    send_slab: &mut InFlightSendSlab,
    shutdown_local: &mut bool,
    conn_index: u32,
    token: ConnToken,
    #[cfg(feature = "tls")] tls_table: &mut Option<crate::tls::TlsTable>,
    connect_addrs: &mut Vec<libc::sockaddr_storage>,
    tcp_nodelay: bool,
    connect_timespecs: &mut Vec<io_uring::types::Timespec>,
) {
    let data = accumulators.data(conn_index);
    if data.is_empty() {
        return;
    }
    let mut ctx = DriverCtx {
        ring,
        connections,
        fixed_buffers,
        send_copy_pool,
        send_slab,
        #[cfg(feature = "tls")]
        tls_table: match tls_table {
            Some(t) => t as *mut crate::tls::TlsTable,
            None => std::ptr::null_mut(),
        },
        shutdown_requested: shutdown_local,
        connect_addrs,
        tcp_nodelay,
        connect_timespecs,
    };
    let consumed = handler.on_data(&mut ctx, token, data);
    accumulators.consume(conn_index, consumed);
}
