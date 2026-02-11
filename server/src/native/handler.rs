//! Kompio EventHandler implementation for the cache server.

use crate::connection::{Connection, SliceRecvBuf};
use crate::metrics::{CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE, CloseReason, WorkerStats};
use bytes::Bytes;
use cache_core::Cache;
use kompio::{
    ConnToken, DriverCtx, EventHandler, GuardBox, MAX_GUARDS, MAX_IOVECS, RegionId, SendGuard,
};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Per-worker configuration passed to ServerHandler during creation.
pub(crate) struct HandlerConfig<C: Cache> {
    pub cache: Arc<C>,
    pub stats: Arc<Vec<WorkerStats>>,
    pub shutdown: Arc<AtomicBool>,
    pub max_value_size: usize,
    pub allow_flush: bool,
    pub send_copy_slot_size: usize,
}

// Global channel for distributing HandlerConfigs to worker threads.
// Uses Mutex<Option<...>> instead of OnceLock so multiple server instances
// (e.g., in tests) can each register their own channel.
// A launch mutex serializes the init + launch + worker-startup sequence
// to prevent config channel races between concurrent server instances.
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

static CONFIG_CHANNEL: Mutex<Option<Box<dyn std::any::Any + Send>>> = Mutex::new(None);
static LAUNCH_MUTEX: Mutex<()> = Mutex::new(());
static WORKERS_INITIALIZED: AtomicUsize = AtomicUsize::new(0);
static WORKERS_EXPECTED: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn init_config_channel<C: Cache + 'static>(
    rx: crossbeam_channel::Receiver<HandlerConfig<C>>,
    num_workers: usize,
) {
    WORKERS_INITIALIZED.store(0, Ordering::SeqCst);
    WORKERS_EXPECTED.store(num_workers, Ordering::SeqCst);
    let mut guard = CONFIG_CHANNEL.lock().unwrap();
    *guard = Some(Box::new(rx));
}

fn take_config<C: Cache + 'static>() -> HandlerConfig<C> {
    let guard = CONFIG_CHANNEL.lock().unwrap();
    let channel = guard.as_ref().expect("config channel not initialized");
    let rx = channel
        .downcast_ref::<crossbeam_channel::Receiver<HandlerConfig<C>>>()
        .expect("config channel type mismatch");
    let config = rx.recv().expect("no more handler configs available");
    drop(guard);
    WORKERS_INITIALIZED.fetch_add(1, Ordering::SeqCst);
    config
}

/// Acquire a lock that serializes server launch sequences.
/// Hold this across init_config_channel + launch + wait_for_workers.
pub(crate) fn launch_lock() -> std::sync::MutexGuard<'static, ()> {
    LAUNCH_MUTEX.lock().unwrap()
}

/// Block until all workers have received their configs.
pub(crate) fn wait_for_workers() {
    let expected = WORKERS_EXPECTED.load(Ordering::SeqCst);
    while WORKERS_INITIALIZED.load(Ordering::SeqCst) < expected {
        std::thread::yield_now();
    }
}

/// Kompio event handler for the cache server.
pub(crate) struct ServerHandler<C: Cache> {
    cache: Arc<C>,
    connections: Vec<Option<Connection>>,
    stats: Arc<Vec<WorkerStats>>,
    worker_id: usize,
    shutdown: Arc<AtomicBool>,
    max_value_size: usize,
    allow_flush: bool,
    send_copy_slot_size: usize,
}

impl<C: Cache + 'static> EventHandler for ServerHandler<C> {
    fn create_for_worker(worker_id: usize) -> Self {
        // Set metrics thread shard to avoid false sharing
        metrics::set_thread_shard(worker_id);

        let config = take_config::<C>();

        ServerHandler {
            cache: config.cache,
            connections: Vec::with_capacity(4096),
            stats: config.stats,
            worker_id,
            shutdown: config.shutdown,
            max_value_size: config.max_value_size,
            allow_flush: config.allow_flush,
            send_copy_slot_size: config.send_copy_slot_size,
        }
    }

    fn on_accept(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        CONNECTIONS_ACCEPTED.increment();
        CONNECTIONS_ACTIVE.increment();
        self.stats[self.worker_id].inc_accepts();

        let idx = conn.index();
        if idx >= self.connections.len() {
            self.connections.resize_with(idx + 1, || None);
        }

        self.connections[idx] = Some(Connection::with_options(
            self.max_value_size,
            self.allow_flush,
        ));
    }

    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        self.stats[self.worker_id].inc_recv();
        let idx = conn.index();

        // Backpressure: if too much write data is pending, refuse more data
        let should_process = self
            .connections
            .get(idx)
            .and_then(|c| c.as_ref())
            .map(|c| c.should_read())
            .unwrap_or(false);

        if !should_process {
            self.stats[self.worker_id].inc_backpressure();
            return 0;
        }

        // Process commands from the received data
        let mut buf = SliceRecvBuf::new(data);

        if let Some(c) = self.connections.get_mut(idx).and_then(|c| c.as_mut()) {
            c.process_from(&mut buf, &*self.cache);

            if c.should_close() {
                self.close_connection(ctx, conn, CloseReason::ProtocolClose);
                return buf.consumed();
            }

            // Send pending response data
            if send_pending(ctx, c, conn, self.send_copy_slot_size).is_err() {
                self.close_connection(ctx, conn, CloseReason::SendError);
                return buf.consumed();
            }
        }

        let consumed = buf.consumed();
        self.stats[self.worker_id].add_bytes_received(consumed as u64);
        consumed
    }

    fn on_send_complete(&mut self, ctx: &mut DriverCtx, conn: ConnToken, result: io::Result<u32>) {
        self.stats[self.worker_id].inc_send_ready();
        let idx = conn.index();

        match result {
            Ok(n) => {
                self.stats[self.worker_id].add_bytes_sent(n as u64);
            }
            Err(_) => {
                self.close_connection(ctx, conn, CloseReason::SendError);
                return;
            }
        }

        // After send completes, try draining more pending data
        if let Some(c) = self.connections.get_mut(idx).and_then(|c| c.as_mut())
            && c.has_pending_write()
            && send_pending(ctx, c, conn, self.send_copy_slot_size).is_err()
        {
            self.close_connection(ctx, conn, CloseReason::SendError);
        }
    }

    fn on_close(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        self.close_connection_state(conn, CloseReason::ClosedEvent);
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx) {
        self.stats[self.worker_id].inc_poll();

        if self.shutdown.load(Ordering::Relaxed) {
            let active = self.connections.iter().filter(|c| c.is_some()).count();
            if active == 0 {
                ctx.request_shutdown();
            }
        }
    }
}

impl<C: Cache> ServerHandler<C> {
    /// Close a connection: issue close to kompio and clean up state.
    fn close_connection(&mut self, ctx: &mut DriverCtx, conn: ConnToken, reason: CloseReason) {
        ctx.close(conn);
        self.close_connection_state(conn, reason);
    }

    /// Clean up connection state only (when kompio already reported close).
    fn close_connection_state(&mut self, conn: ConnToken, reason: CloseReason) {
        let idx = conn.index();
        if let Some(slot) = self.connections.get_mut(idx)
            && slot.take().is_some()
        {
            CONNECTIONS_ACTIVE.decrement();
            self.stats[self.worker_id].inc_close(reason);
        }
    }
}

/// Minimum part size to use zero-copy guard path instead of copy.
const GUARD_MIN_SIZE: usize = 1024;

/// Zero-copy send guard backed by a `Bytes` handle.
///
/// `Bytes` is 32 bytes on 64-bit (Arc-based), fitting in GuardBox's 64-byte
/// inline storage. `Bytes::clone()` is just an Arc refcount bump.
struct BytesGuard(Bytes);

impl SendGuard for BytesGuard {
    fn as_ptr_len(&self) -> (*const u8, u32) {
        (self.0.as_ptr(), self.0.len() as u32)
    }
    fn region(&self) -> RegionId {
        RegionId::UNREGISTERED
    }
}

/// Send pending response data for a connection using scatter-gather.
///
/// Gathers all pending parts via `collect_pending_writes()` and submits
/// them as a single `SendMsgZc` operation (up to MAX_IOVECS parts).
/// Large parts (>= GUARD_MIN_SIZE) use zero-copy guards; small parts
/// are copied into the send pool.
///
/// If there are more than MAX_IOVECS parts, the first batch is sent and
/// `on_send_complete` will call us again for the remainder.
///
/// Returns `Err(())` on fatal send error.
#[inline]
fn send_pending(
    ctx: &mut DriverCtx,
    conn: &mut Connection,
    token: ConnToken,
    slot_size: usize,
) -> Result<(), ()> {
    if !conn.has_pending_write() {
        return Ok(());
    }

    let parts = conn.collect_pending_writes();
    if parts.is_empty() {
        return Ok(());
    }

    // Fast path: single small part — use simple copy send (avoids slab overhead).
    if parts.len() == 1 && parts[0].len() <= slot_size {
        let data = &parts[0];
        match ctx.send(token, data) {
            Ok(()) => {
                conn.advance_write(data.len());
                return Ok(());
            }
            Err(e) if e.kind() == io::ErrorKind::Other => return Ok(()),
            Err(_) => return Err(()),
        }
    }

    // Scatter-gather path: build a send_parts() call with mixed copy + guard parts.
    let mut builder = ctx.send_parts(token);
    let mut total_advance = 0usize;
    let mut copy_budget = slot_size;
    let mut guard_count = 0usize;

    for (i, part) in parts.iter().enumerate() {
        if i >= MAX_IOVECS {
            break;
        }

        if part.len() >= GUARD_MIN_SIZE && guard_count < MAX_GUARDS {
            // Zero-copy guard path for large parts.
            builder = builder.guard(GuardBox::new(BytesGuard(part.clone())));
            guard_count += 1;
        } else if part.len() <= copy_budget {
            // Copy path for small parts.
            builder = builder.copy(part);
            copy_budget -= part.len();
        } else {
            // Can't fit this part — stop here, defer to next round.
            break;
        }

        total_advance += part.len();
    }

    if total_advance == 0 {
        // Nothing could be submitted (pool/slab exhausted), wait for on_send_complete.
        return Ok(());
    }

    match builder.submit() {
        Ok(()) => {
            conn.advance_write(total_advance);
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::Other => {
            // Send pool or slab exhausted — stop sending, wait for on_send_complete.
            Ok(())
        }
        Err(_) => Err(()),
    }
}
