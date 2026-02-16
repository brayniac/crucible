//! Krio AsyncEventHandler implementation for the cache server.
//!
//! Mirrors `native/handler.rs` but uses krio's async API (one task per connection).
//! The per-connection async task reuses `Connection::process_from()` for parsing
//! and command execution, then drains pending writes via `ConnCtx::send_parts()`.

use crate::connection::{Connection, SliceRecvBuf};
use crate::metrics::{CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE, WorkerStats};
use bytes::Bytes;
use cache_core::Cache;
use krio::{
    AsyncEventHandler, ConnCtx, DriverCtx, GuardBox, MAX_GUARDS, MAX_IOVECS, RegionId, SendGuard,
};
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
/// `Connection::process_from()`, and draining responses via `send_parts()`.
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
            if send_pending_await(&conn, &mut connection, slot_size)
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

        // Drain pending responses (fire-and-forget).
        if connection.has_pending_write() {
            if send_pending(&conn, &mut connection, slot_size).is_err() {
                break;
            }
        }

        if consumed == 0 {
            // EOF — drain remaining writes before exiting.
            while connection.has_pending_write() {
                if send_pending(&conn, &mut connection, slot_size).is_err() {
                    break;
                }
            }
            break;
        }

        if connection.should_close() {
            while connection.has_pending_write() {
                if send_pending(&conn, &mut connection, slot_size).is_err() {
                    break;
                }
            }
            conn.close();
            break;
        }
    }

    CONNECTIONS_ACTIVE.decrement();
}

// ── Send helpers ────────────────────────────────────────────────────────

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

/// Send all pending response data for a connection (fire-and-forget).
///
/// Mirrors `native/handler.rs::send_pending()` but uses `ConnCtx` instead of `DriverCtx`.
#[inline]
fn send_pending(
    conn: &ConnCtx,
    connection: &mut Connection,
    slot_size: usize,
) -> Result<(), ()> {
    loop {
        if !connection.has_pending_write() {
            return Ok(());
        }

        let parts = connection.collect_pending_writes();
        if parts.is_empty() {
            return Ok(());
        }

        // Fast path: single small part — use simple copy send.
        if parts.len() == 1 && parts[0].len() <= slot_size {
            let data = &parts[0];
            match conn.send(data) {
                Ok(()) => {
                    connection.advance_write(data.len());
                    continue;
                }
                Err(e) if e.kind() == io::ErrorKind::Other => return Ok(()),
                Err(_) => return Err(()),
            }
        }

        // Scatter-gather path: use guards for all parts.
        // In the async API, SendBuilder's data lifetime makes .copy() awkward
        // with owned data. Using guards for everything is simpler and Bytes::clone()
        // is just an Arc refcount bump (~20 cycles per part).
        let mut guard_parts: Vec<Bytes> = Vec::new();
        let mut total_advance = 0usize;

        for (i, part) in parts.iter().enumerate() {
            if i >= MAX_IOVECS || guard_parts.len() >= MAX_GUARDS {
                break;
            }
            guard_parts.push(part.clone());
            total_advance += part.len();
        }

        if total_advance == 0 {
            return Ok(());
        }

        let result = conn.send_parts().build(move |mut b| {
            for part in &guard_parts {
                b = b.guard(GuardBox::new(BytesGuard(part.clone())));
            }
            b.submit()
        });

        match result {
            Ok(()) => {
                connection.advance_write(total_advance);
            }
            Err(e) if e.kind() == io::ErrorKind::Other => {
                return Ok(());
            }
            Err(_) => return Err(()),
        }
    }
}

/// Send pending data and await completion for backpressure relief.
async fn send_pending_await(
    conn: &ConnCtx,
    connection: &mut Connection,
    slot_size: usize,
) -> Result<(), ()> {
    if !connection.has_pending_write() {
        return Ok(());
    }

    let parts = connection.collect_pending_writes();
    if parts.is_empty() {
        return Ok(());
    }

    // Fast path: single small part.
    if parts.len() == 1 && parts[0].len() <= slot_size {
        let data = &parts[0];
        let len = data.len();
        match conn.send_await(data) {
            Ok(fut) => {
                connection.advance_write(len);
                match fut.await {
                    Ok(_) => return Ok(()),
                    Err(_) => return Err(()),
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Other => return Ok(()),
            Err(_) => return Err(()),
        }
    }

    // Scatter-gather path with await.
    let mut guard_parts: Vec<Bytes> = Vec::new();
    let mut total_advance = 0usize;

    for (i, part) in parts.iter().enumerate() {
        if i >= MAX_IOVECS || guard_parts.len() >= MAX_GUARDS {
            break;
        }
        guard_parts.push(part.clone());
        total_advance += part.len();
    }

    if total_advance == 0 {
        return Ok(());
    }

    let result = conn.send_parts().build_await(move |mut b| {
        for part in &guard_parts {
            b = b.guard(GuardBox::new(BytesGuard(part.clone())));
        }
        b.submit()
    });

    match result {
        Ok(fut) => {
            connection.advance_write(total_advance);
            match fut.await {
                Ok(_) => Ok(()),
                Err(_) => Err(()),
            }
        }
        Err(e) if e.kind() == io::ErrorKind::Other => Ok(()),
        Err(_) => Err(()),
    }
}
