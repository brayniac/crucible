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
use krio::{AsyncEventHandler, ConnCtx, DriverCtx, GuardBox, RegionId, SendGuard};
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
/// copy pool (8192 × 16KB slots). Large parts (≥ 1KB values) use zero-copy
/// guard sends via `build_await()`, which yields to the event loop after each
/// guard send. This limits slab usage to at most 1 entry per connection in
/// flight, preventing the slab exhaustion deadlock that occurs when
/// fire-and-forget guard sends pile up with no CQE processing.
///
/// When `must_yield` is true (backpressure path), the last send is always
/// awaitable to guarantee at least one yield even when all parts are small.
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

        let total_bytes: usize = parts.iter().map(|p| p.len()).sum();
        let mut advanced = 0usize;
        let mut yielded = false;

        for part in &parts {
            if part.len() >= GUARD_MIN_SIZE {
                // Zero-copy path: single-guard awaitable send.
                // Await yields to the event loop, freeing the slab entry before
                // the next guard send — at most 1 slab entry per connection.
                let guard_part = part.clone(); // Arc refcount bump only
                let result = conn.send_parts().build_await(move |b| {
                    b.guard(GuardBox::new(BytesGuard(guard_part))).submit()
                });
                match result {
                    Ok(fut) => {
                        advanced += part.len();
                        if fut.await.is_err() {
                            connection.advance_write(advanced);
                            return Err(());
                        }
                        yielded = true;
                    }
                    Err(e) if e.kind() == io::ErrorKind::Other => {
                        connection.advance_write(advanced);
                        return Ok(());
                    }
                    Err(_) => return Err(()),
                }
            } else if must_yield && !yielded && advanced + part.len() == total_bytes {
                // Backpressure: await the final small part to guarantee a yield.
                let mut offset = 0;
                while offset < part.len() {
                    let end = (offset + slot_size).min(part.len());
                    let chunk = &part[offset..end];
                    let is_last = end == part.len();

                    if is_last {
                        match conn.send_await(chunk) {
                            Ok(fut) => {
                                advanced += end;
                                if fut.await.is_err() {
                                    connection.advance_write(advanced);
                                    return Err(());
                                }
                            }
                            Err(e) if e.kind() == io::ErrorKind::Other => {
                                connection.advance_write(advanced + offset);
                                return Ok(());
                            }
                            Err(_) => return Err(()),
                        }
                    } else {
                        match conn.send(chunk) {
                            Ok(()) => offset = end,
                            Err(e) if e.kind() == io::ErrorKind::Other => {
                                connection.advance_write(advanced + offset);
                                return Ok(());
                            }
                            Err(_) => return Err(()),
                        }
                    }
                }
            } else {
                // Small part: fire-and-forget copy send.
                let mut offset = 0;
                while offset < part.len() {
                    let end = (offset + slot_size).min(part.len());
                    match conn.send(&part[offset..end]) {
                        Ok(()) => offset = end,
                        Err(e) if e.kind() == io::ErrorKind::Other => {
                            connection.advance_write(advanced + offset);
                            return Ok(());
                        }
                        Err(_) => return Err(()),
                    }
                }
                advanced += part.len();
            }
        }

        connection.advance_write(advanced);
    }
}
