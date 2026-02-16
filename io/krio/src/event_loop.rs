use std::io;
use std::sync::atomic::Ordering;
use std::time::Instant;

use io_uring::cqueue;

use crate::chain::ChainEvent;
use crate::completion::{OpTag, UserData};
use crate::connection::RecvMode;
use crate::driver::Driver;
use crate::handler::{ConnToken, DriverCtx, EventHandler};

/// The core event loop that drives io_uring and dispatches to the EventHandler.
pub struct EventLoop<H: EventHandler> {
    pub(crate) driver: Driver,
    handler: H,
}

impl<H: EventHandler> EventLoop<H> {
    /// Create a new event loop for a worker thread.
    pub fn new(
        config: &crate::config::Config,
        handler: H,
        accept_rx: Option<crossbeam_channel::Receiver<(std::os::fd::RawFd, std::net::SocketAddr)>>,
        eventfd: std::os::fd::RawFd,
        shutdown_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Self, crate::error::Error> {
        let driver = Driver::new(config, accept_rx, eventfd, shutdown_flag)?;
        Ok(EventLoop { driver, handler })
    }

    /// Run the event loop. This blocks the current thread.
    pub fn run(&mut self) -> Result<(), crate::error::Error> {
        // Always arm eventfd read — needed for shutdown wakeup even in client-only mode.
        self.driver
            .ring
            .submit_eventfd_read(self.driver.eventfd, self.driver.eventfd_buf.as_mut_ptr())?;

        // Kick the eventfd so the first submit_and_wait(1) returns immediately.
        // Without this, client-only mode (no acceptor) would deadlock because
        // nobody writes to the eventfd, so the eventfd read never completes.
        // In server mode this causes one harmless handle_eventfd_read at startup.
        let kick: u64 = 1;
        unsafe {
            libc::write(
                self.driver.eventfd,
                &kick as *const u64 as *const libc::c_void,
                8,
            );
        }

        loop {
            // Arm a tick timeout before blocking so on_tick fires periodically
            // even when no I/O completions are pending (e.g., client-only mode
            // between phases).
            if !self.driver.tick_timeout_armed
                && let Some(ref ts) = self.driver.tick_timeout_ts
            {
                let ud = UserData::encode(OpTag::TickTimeout, 0, 0);
                let _ = self
                    .driver
                    .ring
                    .submit_tick_timeout(ts as *const _, ud.raw());
                self.driver.tick_timeout_armed = true;
            }

            self.driver.ring.submit_and_wait(1)?;

            self.drain_completions();

            // Check for shutdown after processing completions.
            if self.driver.shutdown_local || self.driver.shutdown_flag.load(Ordering::Relaxed) {
                self.driver.run_shutdown();
                return Ok(());
            }

            // Batch replenish recv buffers.
            if !self.driver.pending_replenish.is_empty() {
                self.driver
                    .provided_bufs
                    .replenish_batch(&self.driver.pending_replenish);
                self.driver.pending_replenish.clear();
            }

            // on_tick callback
            let mut ctx = self.driver.make_ctx();
            self.handler.on_tick(&mut ctx);
        }
    }

    fn drain_completions(&mut self) {
        self.driver.cqe_batch.clear();

        {
            let cq = self.driver.ring.ring.completion();
            for cqe in cq {
                self.driver
                    .cqe_batch
                    .push((cqe.user_data(), cqe.result(), cqe.flags()));
            }
        }

        if let Some(interval) = self.driver.flush_interval {
            let mut last_flush = Instant::now();
            for i in 0..self.driver.cqe_batch.len() {
                let (user_data_raw, result, flags) = self.driver.cqe_batch[i];
                self.dispatch_cqe(user_data_raw, result, flags);
                let now = Instant::now();
                if now.duration_since(last_flush) >= interval {
                    let _ = self.driver.ring.flush();
                    last_flush = now;
                }
            }
        } else {
            for i in 0..self.driver.cqe_batch.len() {
                let (user_data_raw, result, flags) = self.driver.cqe_batch[i];
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
                self.driver.tick_timeout_armed = false;
            }
            OpTag::Timer => {} // only used in async event loop
        }
    }

    fn handle_recv_multi(&mut self, ud: UserData, result: i32, flags: u32) {
        let conn_index = ud.conn_index();
        let has_more = cqueue::more(flags);

        let conn = match self.driver.connections.get(conn_index) {
            Some(c) => c,
            None => return,
        };
        let generation = conn.generation;
        let token = ConnToken::new(conn_index, generation);

        if result <= 0 {
            if result == 0 {
                // EOF
                self.driver.close_connection(conn_index);
                return;
            }
            let errno = -result;
            if errno == libc::ENOBUFS {
                if !has_more {
                    let _ = self.driver.ring.submit_multishot_recv(conn_index);
                }
            } else if errno == libc::ECANCELED {
                // User-initiated cancel — don't re-arm, don't close.
                return;
            } else if !has_more {
                self.driver.close_connection(conn_index);
            }
            return;
        }

        // Extract buffer ID from CQE flags.
        let bid = match cqueue::buffer_select(flags) {
            Some(bid) => bid,
            None => return,
        };

        let bytes_received = result as u32;
        let (buf_ptr, _) = self.driver.provided_bufs.get_buffer(bid);

        let data = unsafe { std::slice::from_raw_parts(buf_ptr, bytes_received as usize) };

        // Schedule buffer for replenishment.
        self.driver.pending_replenish.push(bid);

        // TLS path
        #[cfg(feature = "tls")]
        let is_tls_conn = self
            .driver
            .tls_table
            .as_ref()
            .is_some_and(|t| t.has(conn_index));
        #[cfg(not(feature = "tls"))]
        let is_tls_conn = false;

        if is_tls_conn {
            #[cfg(feature = "tls")]
            {
                let tls_table = self.driver.tls_table.as_mut().unwrap();
                let result = crate::tls::feed_tls_recv(
                    tls_table,
                    &mut self.driver.accumulators,
                    &mut self.driver.ring,
                    &mut self.driver.send_copy_pool,
                    &mut self.driver.tls_scratch,
                    conn_index,
                    data,
                );

                match result {
                    crate::tls::TlsRecvResult::HandshakeJustCompleted => {
                        let is_outbound = self
                            .driver
                            .connections
                            .get(conn_index)
                            .map(|c| c.outbound)
                            .unwrap_or(false);

                        if is_outbound {
                            if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                                cs.established = true;
                            }
                            let mut ctx = self.driver.make_ctx();
                            self.handler.on_connect(&mut ctx, token, Ok(()));
                        } else {
                            if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                                cs.established = true;
                            }
                            let mut ctx = self.driver.make_ctx();
                            self.handler.on_accept(&mut ctx, token);
                        }

                        call_on_data(&mut self.handler, &mut self.driver, conn_index, token);
                    }
                    crate::tls::TlsRecvResult::Ok => {
                        call_on_data(&mut self.handler, &mut self.driver, conn_index, token);
                    }
                    crate::tls::TlsRecvResult::Error(_) | crate::tls::TlsRecvResult::Closed => {
                        self.driver.close_connection(conn_index);
                    }
                }
            }
        } else {
            // Plaintext path
            self.driver.accumulators.append(conn_index, data);

            call_on_data(&mut self.handler, &mut self.driver, conn_index, token);
        }

        if !has_more
            && let Some(conn) = self.driver.connections.get(conn_index)
            && matches!(conn.recv_mode, RecvMode::Multi)
        {
            let _ = self.driver.ring.submit_multishot_recv(conn_index);
        }
    }

    fn handle_eventfd_read(&mut self) {
        // Drain accept channel (server mode only).
        {
            loop {
                let item = match self.driver.accept_rx {
                    Some(ref rx) => rx.try_recv().ok(),
                    None => None,
                };
                let Some((raw_fd, peer_addr)) = item else {
                    break;
                };
                let conn_index = match self.driver.connections.allocate() {
                    Some(idx) => idx,
                    None => {
                        unsafe {
                            libc::close(raw_fd);
                        }
                        continue;
                    }
                };

                // Store peer address.
                if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                    cs.peer_addr = Some(peer_addr);
                }

                // Register the fd in the direct file table, then close the original.
                if self
                    .driver
                    .ring
                    .register_files_update(conn_index, &[raw_fd])
                    .is_err()
                {
                    self.driver.connections.release(conn_index);
                    unsafe {
                        libc::close(raw_fd);
                    }
                    continue;
                }
                unsafe {
                    libc::close(raw_fd);
                }

                // Reset accumulator for this connection.
                self.driver.accumulators.reset(conn_index);

                // Arm multishot recv.
                let _ = self.driver.ring.submit_multishot_recv(conn_index);

                // TLS path: create TLS state and defer on_accept until handshake completes.
                #[cfg(feature = "tls")]
                if let Some(ref mut tls_table) = self.driver.tls_table
                    && tls_table.has_server_config()
                {
                    tls_table.create(conn_index);
                    continue; // skip on_accept — deferred until handshake completes
                }

                // Plaintext path: mark established and call on_accept immediately.
                if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                    cs.established = true;
                }
                let generation = self.driver.connections.generation(conn_index);
                let token = ConnToken::new(conn_index, generation);
                let mut ctx = self.driver.make_ctx();
                self.handler.on_accept(&mut ctx, token);
            }
        }

        // Always call on_notify — this is the external wakeup hook.
        {
            let mut ctx = self.driver.make_ctx();
            self.handler.on_notify(&mut ctx);
        }

        // Re-arm eventfd read, unless shutdown was requested.
        if !self.driver.shutdown_flag.load(Ordering::Relaxed) {
            let _ = self
                .driver
                .ring
                .submit_eventfd_read(self.driver.eventfd, self.driver.eventfd_buf.as_mut_ptr());
        }
    }

    fn handle_send(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let pool_slot = ud.payload() as u16;

        // Chain path: accumulate CQE result, release pool slot immediately.
        if self.driver.chain_table.is_active(conn_index) {
            self.driver.send_copy_pool.release(pool_slot);
            let event = self.driver.chain_table.on_operation_cqe(conn_index, result);
            if matches!(event, ChainEvent::Complete { .. }) {
                self.fire_chain_complete(conn_index);
            }
            return;
        }

        if result > 0 {
            if let Some((ptr, remaining)) = self
                .driver
                .send_copy_pool
                .try_advance(pool_slot, result as u32)
            {
                let _ = self
                    .driver
                    .ring
                    .submit_send_copied(conn_index, ptr, remaining, pool_slot);
                return;
            }
            let total = self.driver.send_copy_pool.original_len(pool_slot);
            self.driver.send_copy_pool.release(pool_slot);

            // Submit next queued send immediately (before on_send_complete).
            self.driver.submit_next_queued(conn_index);

            let conn = match self.driver.connections.get(conn_index) {
                Some(c) => c,
                None => return,
            };
            let generation = conn.generation;
            let token = ConnToken::new(conn_index, generation);

            let mut ctx = self.driver.make_ctx();
            self.handler.on_send_complete(&mut ctx, token, Ok(total));

            // After sends drain, replay accumulated recv data that was deferred
            // due to write backpressure.
            self.replay_accumulated(conn_index);
            return;
        }

        self.driver.send_copy_pool.release(pool_slot);

        // Error or zero-length send — drain the queue.
        self.driver.drain_conn_send_queue(conn_index);

        let conn = match self.driver.connections.get(conn_index) {
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

        let mut ctx = self.driver.make_ctx();
        self.handler.on_send_complete(&mut ctx, token, io_result);
    }

    fn handle_send_msg_zc(&mut self, ud: UserData, result: i32, flags: u32) {
        let conn_index = ud.conn_index();
        let slab_idx = ud.payload() as u16;

        if !self.driver.send_slab.in_use(slab_idx) {
            return;
        }

        // Chain path: track CQE results and ZC notifications.
        if self.driver.chain_table.is_active(conn_index) {
            if cqueue::notif(flags) {
                // ZC notification — release slab if ready.
                self.driver.send_slab.dec_pending_notifs(slab_idx);
                if self.driver.send_slab.should_release(slab_idx) {
                    let ps = self.driver.send_slab.release(slab_idx);
                    if ps != u16::MAX {
                        self.driver.send_copy_pool.release(ps);
                    }
                }
                let event = self.driver.chain_table.on_notif_cqe(conn_index);
                if matches!(event, ChainEvent::Complete { .. }) {
                    self.fire_chain_complete(conn_index);
                }
                return;
            }
            // Operation CQE
            if result == -libc::ECANCELED {
                // Cancelled — no NOTIF coming, release slab immediately.
                let ps = self.driver.send_slab.release(slab_idx);
                if ps != u16::MAX {
                    self.driver.send_copy_pool.release(ps);
                }
            } else if result >= 0 {
                // Success (full or partial) — NOTIF will come.
                self.driver.send_slab.inc_pending_notifs(slab_idx);
                self.driver.send_slab.mark_awaiting_notifications(slab_idx);
                self.driver.chain_table.inc_zc_notif(conn_index);
            } else {
                // Other error — no NOTIF, release immediately.
                let ps = self.driver.send_slab.release(slab_idx);
                if ps != u16::MAX {
                    self.driver.send_copy_pool.release(ps);
                }
            }
            let event = self.driver.chain_table.on_operation_cqe(conn_index, result);
            if matches!(event, ChainEvent::Complete { .. }) {
                self.fire_chain_complete(conn_index);
            }
            return;
        }

        if cqueue::notif(flags) {
            self.driver.send_slab.dec_pending_notifs(slab_idx);
            if self.driver.send_slab.should_release(slab_idx) {
                let pool_slot = self.driver.send_slab.release(slab_idx);
                if pool_slot != u16::MAX {
                    self.driver.send_copy_pool.release(pool_slot);
                }
            }
            return;
        }

        self.driver.send_slab.inc_pending_notifs(slab_idx);

        if result > 0
            && let Some(msg_ptr) = self.driver.send_slab.try_advance(slab_idx, result as u32)
            && self
                .driver
                .ring
                .submit_send_msg_zc(conn_index, msg_ptr, slab_idx)
                .is_ok()
        {
            return;
        }

        self.driver.send_slab.mark_awaiting_notifications(slab_idx);

        let total_len = self.driver.send_slab.total_len(slab_idx);
        let should_release = self.driver.send_slab.should_release(slab_idx);

        if should_release {
            let pool_slot = self.driver.send_slab.release(slab_idx);
            if pool_slot != u16::MAX {
                self.driver.send_copy_pool.release(pool_slot);
            }
        }

        // Submit next queued send (or drain queue on error) before on_send_complete.
        if result >= 0 {
            self.driver.submit_next_queued(conn_index);
        } else {
            self.driver.drain_conn_send_queue(conn_index);
        }

        let conn = match self.driver.connections.get(conn_index) {
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

        let mut ctx = self.driver.make_ctx();
        self.handler.on_send_complete(&mut ctx, token, io_result);

        // After sends drain, replay accumulated recv data that was deferred
        // due to write backpressure.
        if result >= 0 {
            self.replay_accumulated(conn_index);
        }
    }

    fn handle_connect(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();

        let conn_state = match self.driver.connections.get(conn_index) {
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
                    .driver
                    .connections
                    .get(conn_index)
                    .map(|c| c.connect_timeout_armed)
                    .unwrap_or(false);
                if !timeout_armed {
                    // User-initiated cancel.
                    let err = io::Error::from_raw_os_error(errno);
                    let mut ctx = self.driver.make_ctx();
                    self.handler.on_connect(&mut ctx, token, Err(err));
                    self.driver.close_connection(conn_index);
                    return;
                }
                // Timeout-initiated cancel — handle_timeout will fire on_connect.
                // Clean up the timeout flag; the Timeout CQE will handle the rest.
                if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                    cs.connect_timeout_armed = false;
                }
                return;
            }

            // Cancel the timeout if one was armed.
            if self
                .driver
                .connections
                .get(conn_index)
                .map(|c| c.connect_timeout_armed)
                .unwrap_or(false)
            {
                let timeout_ud = UserData::encode(OpTag::Timeout, conn_index, 0);
                let _ = self
                    .driver
                    .ring
                    .submit_async_cancel(timeout_ud.raw(), conn_index);
                if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                    cs.connect_timeout_armed = false;
                }
            }

            // Connect failed.
            #[cfg(feature = "tls")]
            if let Some(ref mut tls_table) = self.driver.tls_table {
                tls_table.remove(conn_index);
            }

            let err = io::Error::from_raw_os_error(errno);
            let mut ctx = self.driver.make_ctx();
            self.handler.on_connect(&mut ctx, token, Err(err));

            self.driver.close_connection(conn_index);
            return;
        }

        // Connect succeeded — cancel timeout if armed.
        let timeout_was_armed = self
            .driver
            .connections
            .get(conn_index)
            .map(|c| c.connect_timeout_armed)
            .unwrap_or(false);
        if timeout_was_armed {
            // Check if the connection is already closed (timeout already fired).
            let still_connecting = self
                .driver
                .connections
                .get(conn_index)
                .map(|c| matches!(c.recv_mode, RecvMode::Connecting))
                .unwrap_or(false);
            if !still_connecting {
                // Timeout already fired and closed this connection — discard late success.
                if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                    cs.connect_timeout_armed = false;
                }
                return;
            }
            let timeout_ud = UserData::encode(OpTag::Timeout, conn_index, 0);
            let _ = self
                .driver
                .ring
                .submit_async_cancel(timeout_ud.raw(), conn_index);
            if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                cs.connect_timeout_armed = false;
            }
        }

        // Reset accumulator.
        self.driver.accumulators.reset(conn_index);

        // TLS client path
        #[cfg(feature = "tls")]
        if let Some(ref mut tls_table) = self.driver.tls_table
            && tls_table.get_mut(conn_index).is_some()
        {
            crate::tls::flush_tls_output(
                tls_table,
                &mut self.driver.ring,
                &mut self.driver.send_copy_pool,
                conn_index,
            );

            if let Some(cs) = self.driver.connections.get_mut(conn_index) {
                cs.recv_mode = RecvMode::Multi;
            }
            let _ = self.driver.ring.submit_multishot_recv(conn_index);
            return;
        }

        // Plaintext path
        if let Some(cs) = self.driver.connections.get_mut(conn_index) {
            cs.recv_mode = RecvMode::Multi;
            cs.established = true;
        }
        let _ = self.driver.ring.submit_multishot_recv(conn_index);

        let mut ctx = self.driver.make_ctx();
        self.handler.on_connect(&mut ctx, token, Ok(()));
    }

    fn handle_timeout(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();

        // Timeout fired (result == -ETIME) means the connect timed out.
        // If result == -ECANCELED, the timeout was cancelled (connect succeeded first).
        if result != -libc::ETIME {
            return;
        }

        let conn = match self.driver.connections.get(conn_index) {
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
        let _ = self
            .driver
            .ring
            .submit_async_cancel(connect_ud.raw(), conn_index);

        // Leave connect_timeout_armed == true so that when the Connect CQE
        // arrives (ECANCELED or even success), handle_connect sees the flag
        // and takes the silent-return branch instead of firing a second
        // on_connect callback.

        // Fire on_connect with TimedOut error.
        let err = io::Error::new(io::ErrorKind::TimedOut, "connect timed out");

        #[cfg(feature = "tls")]
        if let Some(ref mut tls_table) = self.driver.tls_table {
            tls_table.remove(conn_index);
        }

        let mut ctx = self.driver.make_ctx();
        self.handler.on_connect(&mut ctx, token, Err(err));

        self.driver.close_connection(conn_index);
    }

    fn handle_close(&mut self, ud: UserData) {
        let conn_index = ud.conn_index();

        let was_established = self
            .driver
            .connections
            .get(conn_index)
            .map(|c| c.established)
            .unwrap_or(false);

        // TLS cleanup.
        #[cfg(feature = "tls")]
        if let Some(ref mut tls_table) = self.driver.tls_table {
            tls_table.remove(conn_index);
        }

        if !was_established {
            self.driver.connections.release(conn_index);
            return;
        }

        // Fire on_close BEFORE release so peer_addr() and other queries
        // still work inside the callback.
        let generation = self.driver.connections.generation(conn_index);
        let token = ConnToken::new(conn_index, generation);
        let mut ctx = self.driver.make_ctx();
        self.handler.on_close(&mut ctx, token);

        self.driver.connections.release(conn_index);
    }

    #[cfg(feature = "tls")]
    fn handle_tls_send(&mut self, ud: UserData, result: i32) {
        let conn_index = ud.conn_index();
        let pool_slot = ud.payload() as u16;

        if result > 0
            && let Some((ptr, remaining)) = self
                .driver
                .send_copy_pool
                .try_advance(pool_slot, result as u32)
        {
            let _ = self
                .driver
                .ring
                .submit_tls_send(conn_index, ptr, remaining, pool_slot);
            return;
        }
        self.driver.send_copy_pool.release(pool_slot);
    }

    /// Replay accumulated recv data for a connection after write backpressure
    /// has been relieved. If `on_data` previously returned without consuming all
    /// data (because pending writes exceeded the backpressure threshold), the
    /// unconsumed data sits in the accumulator. After a send completes and
    /// frees write capacity, we re-invoke `on_data` to process the remaining
    /// commands.
    fn replay_accumulated(&mut self, conn_index: u32) {
        if self.driver.accumulators.data(conn_index).is_empty() {
            return;
        }

        let conn = match self.driver.connections.get(conn_index) {
            Some(c) => c,
            None => return,
        };
        let generation = conn.generation;
        let token = ConnToken::new(conn_index, generation);

        call_on_data(&mut self.handler, &mut self.driver, conn_index, token);
    }

    /// Fire on_send_complete for a completed chain, then replay accumulated data.
    fn fire_chain_complete(&mut self, conn_index: u32) {
        let chain = match self.driver.chain_table.take(conn_index) {
            Some(c) => c,
            None => return,
        };

        let conn = match self.driver.connections.get(conn_index) {
            Some(c) => c,
            None => return,
        };
        let generation = conn.generation;
        let token = ConnToken::new(conn_index, generation);

        let io_result = match chain.first_error {
            Some(errno) => Err(io::Error::from_raw_os_error(-errno)),
            None => Ok(chain.bytes_sent),
        };

        // Submit next queued send (or drain on error) before on_send_complete.
        if chain.first_error.is_none() {
            self.driver.submit_next_queued(conn_index);
        } else {
            self.driver.drain_conn_send_queue(conn_index);
        }

        let mut ctx = self.driver.make_ctx();
        self.handler.on_send_complete(&mut ctx, token, io_result);

        // After chain completes, replay accumulated recv data.
        if chain.first_error.is_none() {
            self.replay_accumulated(conn_index);
        }
    }
}

/// Free function for borrow-splitting: calls handler.on_data with disjoint borrows.
fn call_on_data<H: EventHandler>(
    handler: &mut H,
    driver: &mut Driver,
    conn_index: u32,
    token: ConnToken,
) {
    let data = driver.accumulators.data(conn_index);
    if data.is_empty() {
        return;
    }
    let mut ctx = DriverCtx {
        ring: &mut driver.ring,
        connections: &mut driver.connections,
        fixed_buffers: &mut driver.fixed_buffers,
        send_copy_pool: &mut driver.send_copy_pool,
        send_slab: &mut driver.send_slab,
        #[cfg(feature = "tls")]
        tls_table: match driver.tls_table {
            Some(ref mut t) => t as *mut crate::tls::TlsTable,
            None => std::ptr::null_mut(),
        },
        shutdown_requested: &mut driver.shutdown_local,
        connect_addrs: &mut driver.connect_addrs,
        tcp_nodelay: driver.tcp_nodelay,
        connect_timespecs: &mut driver.connect_timespecs,
        chain_table: &mut driver.chain_table,
        max_chain_length: driver.max_chain_length,
        send_queues: &mut driver.send_queues,
    };
    let consumed = handler.on_data(&mut ctx, token, data);
    driver.accumulators.consume(conn_index, consumed);
}
