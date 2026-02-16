//! Krio AsyncEventHandler implementation for the cache server.
//!
//! Mirrors `native/handler.rs` but uses krio's async API (one task per connection).
//! The per-connection async task reuses `Connection::process_from()` for parsing
//! and command execution, then drains pending writes via copy sends (small
//! protocol framing) and zero-copy guard sends (large values).

use crate::connection::{Connection, SliceRecvBuf};
use crate::metrics::{CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE, WorkerStats};
use cache_core::Cache;
use bytes::Bytes;
use krio::{AsyncEventHandler, ConnCtx, DriverCtx, GuardBox, MAX_GUARDS, RegionId, SendGuard};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

// ── Config channel ──────────────────────────────────────────────────────

/// Per-worker configuration passed to AsyncServerHandler during creation.
pub(crate) struct HandlerConfig<C: Cache> {
    pub cache: Arc<C>,
    pub stats: Arc<Vec<WorkerStats>>,
    pub shutdown: Arc<AtomicBool>,
    pub max_value_size: usize,
    pub allow_flush: bool,
    pub send_copy_slot_size: usize,
}

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

pub(crate) fn launch_lock() -> std::sync::MutexGuard<'static, ()> {
    LAUNCH_MUTEX.lock().unwrap()
}

pub(crate) fn wait_for_workers() {
    let expected = WORKERS_EXPECTED.load(Ordering::SeqCst);
    while WORKERS_INITIALIZED.load(Ordering::SeqCst) < expected {
        std::thread::yield_now();
    }
}

// ── AsyncServerHandler ──────────────────────────────────────────────────

/// Krio async event handler for the cache server.
pub(crate) struct AsyncServerHandler<C: Cache> {
    cache: Arc<C>,
    #[allow(dead_code)]
    stats: Arc<Vec<WorkerStats>>,
    #[allow(dead_code)]
    worker_id: usize,
    shutdown: Arc<AtomicBool>,
    max_value_size: usize,
    allow_flush: bool,
    send_copy_slot_size: usize,
}

impl<C: Cache + 'static> AsyncEventHandler for AsyncServerHandler<C> {
    fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        CONNECTIONS_ACCEPTED.increment();
        CONNECTIONS_ACTIVE.increment();

        let cache = Arc::clone(&self.cache);
        let max_value_size = self.max_value_size;
        let allow_flush = self.allow_flush;
        let slot_size = self.send_copy_slot_size;

        Box::pin(handle_connection(
            conn,
            cache,
            max_value_size,
            allow_flush,
            slot_size,
        ))
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx<'_>) {
        if self.shutdown.load(Ordering::Relaxed) {
            ctx.request_shutdown();
        }
    }

    fn create_for_worker(worker_id: usize) -> Self {
        metrics::set_thread_shard(worker_id);

        let config = take_config::<C>();

        AsyncServerHandler {
            cache: config.cache,
            stats: config.stats,
            worker_id,
            shutdown: config.shutdown,
            max_value_size: config.max_value_size,
            allow_flush: config.allow_flush,
            send_copy_slot_size: config.send_copy_slot_size,
        }
    }
}

// ── Per-connection async task ───────────────────────────────────────────

/// Handle a single connection's lifetime as an async task.
///
/// Loops reading data via `with_data`, processing commands through the shared
/// `Connection::process_from()`, and draining responses via `ConnCtx::send()`.
async fn handle_connection<C: Cache>(
    conn: ConnCtx,
    cache: Arc<C>,
    max_value_size: usize,
    allow_flush: bool,
    slot_size: usize,
) {
    let mut connection = Connection::with_options(max_value_size, allow_flush);

    loop {
        // Backpressure: if write queue is full, await a send completion first.
        if !connection.should_read() && connection.has_pending_write() {
            if drain_pending(&conn, &mut connection, slot_size, true)
                .await
                .is_err()
            {
                break;
            }
            continue;
        }

        let consumed = conn
            .with_data(|data| {
                if data.is_empty() {
                    return 0; // EOF
                }

                let mut buf = SliceRecvBuf::new(data);
                connection.process_from(&mut buf, &*cache);
                buf.consumed()
            })
            .await;

        // Drain pending responses.
        if connection.has_pending_write()
            && drain_pending(&conn, &mut connection, slot_size, false)
                .await
                .is_err()
        {
            break;
        }

        if consumed == 0 {
            break;
        }

        if connection.should_close() {
            conn.close();
            break;
        }
    }

    CONNECTIONS_ACTIVE.decrement();
}

// ── Send helpers ────────────────────────────────────────────────────────

/// Minimum part size to use zero-copy guard path instead of copy.
const GUARD_MIN_SIZE: usize = 1024;

/// Zero-copy send guard backed by a `Bytes` handle.
struct BytesGuard(Bytes);

impl SendGuard for BytesGuard {
    fn as_ptr_len(&self) -> (*const u8, u32) {
        (self.0.as_ptr(), self.0.len() as u32)
    }
    fn region(&self) -> RegionId {
        RegionId::UNREGISTERED
    }
}

/// Drain all pending response data for a connection.
///
/// Small parts (< 1KB protocol framing) use fire-and-forget copy sends via the
/// copy pool. Large parts (≥ 1KB values) are batched into scatter-gather sends
/// (up to 4 guards per SQE) and submitted fire-and-forget via `build()`. This
/// matches the callback handler's zero-copy send path with no per-send yield.
///
/// If the send slab is exhausted (4096 slots, rare under normal load), the
/// fallback copies the data via `send_await()` which yields to the event loop,
/// allowing CQE processing to free slab entries.
///
/// When `must_yield` is true (backpressure path), the first send is awaitable
/// to guarantee at least one yield so the event loop can process CQEs.
async fn drain_pending(
    conn: &ConnCtx,
    connection: &mut Connection,
    slot_size: usize,
    must_yield: bool,
) -> Result<(), ()> {
    loop {
        if !connection.has_pending_write() {
            return Ok(());
        }

        let parts = connection.collect_pending_writes();
        if parts.is_empty() {
            return Ok(());
        }

        let mut advanced = 0usize;
        let mut yielded = false;
        // When must_yield is set (backpressure), the first send operation is
        // made awaitable so we yield to the event loop for CQE processing.
        let mut need_yield = must_yield;
        let mut guard_batch: Vec<Bytes> = Vec::new();

        for part in &parts {
            if part.len() >= GUARD_MIN_SIZE {
                guard_batch.push(part.clone()); // Arc refcount bump only
                if guard_batch.len() >= MAX_GUARDS {
                    let batch_len: usize = guard_batch.iter().map(|b| b.len()).sum();
                    let force_await = need_yield;
                    match flush_guard_batch(
                        conn,
                        &mut guard_batch,
                        slot_size,
                        &mut yielded,
                        force_await,
                    )
                    .await
                    {
                        Ok(()) => {
                            advanced += batch_len;
                            if force_await {
                                need_yield = false;
                            }
                        }
                        Err(true) => {
                            connection.advance_write(advanced);
                            return Err(());
                        }
                        Err(false) => {
                            connection.advance_write(advanced);
                            return Ok(());
                        }
                    }
                }
            } else {
                // Flush pending guard batch before small part.
                if !guard_batch.is_empty() {
                    let batch_len: usize = guard_batch.iter().map(|b| b.len()).sum();
                    let force_await = need_yield;
                    match flush_guard_batch(
                        conn,
                        &mut guard_batch,
                        slot_size,
                        &mut yielded,
                        force_await,
                    )
                    .await
                    {
                        Ok(()) => {
                            advanced += batch_len;
                            if force_await {
                                need_yield = false;
                            }
                        }
                        Err(true) => {
                            connection.advance_write(advanced);
                            return Err(());
                        }
                        Err(false) => {
                            connection.advance_write(advanced);
                            return Ok(());
                        }
                    }
                }

                // Small part: copy send (awaitable if we still need to yield).
                let mut offset = 0;
                while offset < part.len() {
                    let end = (offset + slot_size).min(part.len());
                    if need_yield {
                        match conn.send_await(&part[offset..end]) {
                            Ok(fut) => {
                                yielded = true;
                                need_yield = false;
                                if fut.await.is_err() {
                                    connection.advance_write(advanced + offset);
                                    return Err(());
                                }
                                offset = end;
                            }
                            Err(e) if e.kind() == io::ErrorKind::Other => {
                                connection.advance_write(advanced + offset);
                                return Ok(());
                            }
                            Err(_) => return Err(()),
                        }
                    } else {
                        match conn.send(&part[offset..end]) {
                            Ok(()) => offset = end,
                            Err(e) if e.kind() == io::ErrorKind::Other => {
                                connection.advance_write(advanced + offset);
                                return Ok(());
                            }
                            Err(_) => return Err(()),
                        }
                    }
                }
                advanced += part.len();
            }
        }

        // Flush remaining guard batch.
        if !guard_batch.is_empty() {
            let batch_len: usize = guard_batch.iter().map(|b| b.len()).sum();
            let force_await = need_yield;
            match flush_guard_batch(
                conn,
                &mut guard_batch,
                slot_size,
                &mut yielded,
                force_await,
            )
            .await
            {
                Ok(()) => advanced += batch_len,
                Err(true) => {
                    connection.advance_write(advanced);
                    return Err(());
                }
                Err(false) => {
                    connection.advance_write(advanced);
                    return Ok(());
                }
            }
        }

        connection.advance_write(advanced);
    }
}

/// Flush a batch of large parts as a guard scatter-gather send.
///
/// When `force_await` is true (backpressure path), uses `build_await()` for an
/// awaitable scatter-gather send that yields to the event loop. Otherwise uses
/// fire-and-forget `build()`.
///
/// On slab exhaustion, falls back to copying the data via `send_await()` which
/// yields to the event loop (setting `*yielded = true`).
///
/// Returns `Ok(())` on success, `Err(true)` on fatal error, `Err(false)` on
/// pool exhaustion (caller should advance and retry later).
async fn flush_guard_batch(
    conn: &ConnCtx,
    batch: &mut Vec<Bytes>,
    slot_size: usize,
    yielded: &mut bool,
    force_await: bool,
) -> Result<(), bool> {
    let parts = std::mem::take(batch);
    // Clone cheaply (Arc bumps) in case we need the copy fallback.
    let fallback = parts.clone();

    if force_await {
        // Awaitable guard scatter-gather — yields to the event loop.
        let result = conn.send_parts().build_await(move |mut b| {
            for part in &parts {
                b = b.guard(GuardBox::new(BytesGuard(part.clone())));
            }
            b.submit()
        });
        match result {
            Ok(fut) => {
                *yielded = true;
                if fut.await.is_err() {
                    return Err(true);
                }
                return Ok(());
            }
            Err(e) if e.kind() == io::ErrorKind::Other => {
                // Slab exhausted — fall through to copy fallback.
            }
            Err(_) => return Err(true),
        }
    } else {
        // Fire-and-forget guard scatter-gather (no yield, no copy).
        let result = conn.send_parts().build(move |mut b| {
            for part in &parts {
                b = b.guard(GuardBox::new(BytesGuard(part.clone())));
            }
            b.submit()
        });
        match result {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::Other => {
                // Slab exhausted — fall through to copy fallback.
            }
            Err(_) => return Err(true),
        }
    }

    // Slab exhausted — fall back to copy sends with await for yield.
    for part in &fallback {
        let mut offset = 0;
        while offset < part.len() {
            let end = (offset + slot_size).min(part.len());
            match conn.send_await(&part[offset..end]) {
                Ok(fut) => {
                    *yielded = true;
                    if fut.await.is_err() {
                        return Err(true);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Other => return Err(false),
                Err(_) => return Err(true),
            }
            offset = end;
        }
    }
    Ok(())
}
