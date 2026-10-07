//! Krio AsyncEventHandler implementation for the cache server.
//!
//! Mirrors `native/handler.rs` but uses ringline's async API (one task per connection).
//! The per-connection async task reuses `Connection::process_from()` for parsing
//! and command execution, then drains pending writes via copy sends (small
//! protocol framing) and zero-copy guard sends (large values).

use crate::connection::{Connection, PendingDiskReadInfo, SliceRecvBuf, Suspended};
use crate::disk_io::DiskBackend;
use crate::disk_io::DiskIoWorkerConfig;
use crate::metrics::{
    CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE, DISK_FLUSH_ERRORS, DISK_FLUSHES, DISK_KEY_READS,
    DISK_READ_ERRORS, DISK_READ_HITS, DISK_READ_MISSES, DISK_READS, HITS, MISSES,
};
use bytes::Bytes;
use cache_core::Cache;
use cache_core::disk::AlignedBufferPool;
use parking_lot::Mutex;
use ringline::{AsyncEventHandler, ConnCtx, DriverCtx, GuardBox, RegionId, SendGuard, SendPart};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;

// ── Config channel ──────────────────────────────────────────────────────

/// Per-worker configuration passed to AsyncServerHandler during creation.
pub(crate) struct HandlerConfig<C: Cache> {
    pub cache: Arc<C>,
    pub shutdown: Arc<AtomicBool>,
    pub max_value_size: usize,
    pub allow_flush: bool,
    pub send_copy_slot_size: usize,
    /// Maximum time (in microseconds) to retry a SET that failed due to
    /// eviction pressure. 0 disables retry.
    pub set_retry_timeout_us: u64,
    /// Disk I/O configuration. When set, workers open the device/file
    /// lazily on first accept and enable disk read support.
    pub disk_io_config: Option<DiskIoWorkerConfig>,
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
    let mut guard = CONFIG_CHANNEL.lock();
    *guard = Some(Box::new(rx));
}

fn take_config<C: Cache + 'static>() -> HandlerConfig<C> {
    let guard = CONFIG_CHANNEL.lock();
    let channel = guard.as_ref().expect("config channel not initialized");
    let rx = channel
        .downcast_ref::<crossbeam_channel::Receiver<HandlerConfig<C>>>()
        .expect("config channel type mismatch");
    let config = rx.recv().expect("no more handler configs available");
    drop(guard);
    WORKERS_INITIALIZED.fetch_add(1, Ordering::SeqCst);
    config
}

pub(crate) fn launch_lock() -> parking_lot::MutexGuard<'static, ()> {
    LAUNCH_MUTEX.lock()
}

pub(crate) fn wait_for_workers() {
    let expected = WORKERS_EXPECTED.load(Ordering::SeqCst);
    while WORKERS_INITIALIZED.load(Ordering::SeqCst) < expected {
        std::thread::yield_now();
    }
}

// ── AsyncServerHandler ──────────────────────────────────────────────────

/// Per-worker disk I/O state for the async handler.
///
/// Shared via `Rc<RefCell<...>>` so the per-connection async tasks can
/// borrow it. This is safe because ringline is single-threaded per worker.
struct AsyncDiskIo {
    backend: DiskBackend,
    read_buffer_pool: AlignedBufferPool,
}

/// Krio async event handler for the cache server.
pub(crate) struct AsyncServerHandler<C: Cache> {
    cache: Arc<C>,
    shutdown: Arc<AtomicBool>,
    max_value_size: usize,
    allow_flush: bool,
    send_copy_slot_size: usize,
    set_retry_timeout_us: u64,
    /// Shared disk I/O state, lazily initialized on first accept.
    /// Uses `Arc<Mutex<...>>` because `AsyncEventHandler` requires `Send`.
    /// The Mutex is never contended (ringline is single-threaded per worker).
    disk_io: Arc<Mutex<Option<AsyncDiskIo>>>,
    /// Saved config for deferred disk I/O initialization (needs executor context).
    disk_io_config: Mutex<Option<DiskIoWorkerConfig>>,
}

impl<C: Cache + 'static> AsyncEventHandler for AsyncServerHandler<C> {
    fn on_accept(&self, conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        CONNECTIONS_ACCEPTED.increment();
        CONNECTIONS_ACTIVE.increment();

        let cache = Arc::clone(&self.cache);
        let cfg = ConnConfig {
            max_value_size: self.max_value_size,
            allow_flush: self.allow_flush,
            slot_size: self.send_copy_slot_size,
            set_retry_timeout_us: self.set_retry_timeout_us,
        };
        let disk_io = Arc::clone(&self.disk_io);

        Box::pin(handle_connection(conn, cache, cfg, disk_io))
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx<'_>) {
        if self.shutdown.load(Ordering::Relaxed) {
            ctx.request_shutdown();
        }
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        // Spawn a flush worker task if disk I/O is configured.
        let disk_io_config = self.disk_io_config.lock().take()?;
        let disk_io = Arc::clone(&self.disk_io);
        let cache = Arc::clone(&self.cache);
        let shutdown = Arc::clone(&self.shutdown);
        Some(Box::pin(flush_worker(
            disk_io_config,
            disk_io,
            cache,
            shutdown,
        )))
    }

    fn create_for_worker(worker_id: usize) -> Self {
        metrics::set_thread_shard(worker_id);

        let config = take_config::<C>();

        AsyncServerHandler {
            cache: config.cache,
            shutdown: config.shutdown,
            max_value_size: config.max_value_size,
            allow_flush: config.allow_flush,
            send_copy_slot_size: config.send_copy_slot_size,
            set_retry_timeout_us: config.set_retry_timeout_us,
            disk_io: Arc::new(Mutex::new(None)),
            disk_io_config: Mutex::new(config.disk_io_config),
        }
    }
}

/// Background flush worker task.
///
/// Eagerly initializes disk I/O and then loops, draining the cache's flush
/// queue and submitting io_uring writes. Runs as a standalone ringline task
/// alongside per-connection tasks.
async fn flush_worker<C: Cache>(
    config: DiskIoWorkerConfig,
    disk_io: Arc<Mutex<Option<AsyncDiskIo>>>,
    cache: Arc<C>,
    shutdown: Arc<AtomicBool>,
) {
    // Initialize disk I/O eagerly so connections can read from disk immediately.
    let flush_backend = match init_async_disk_io(&config) {
        Ok(state) => {
            let backend = state.backend;
            *disk_io.lock() = Some(state);
            backend
        }
        Err(e) => {
            tracing::error!("Failed to initialize async disk I/O: {e}");
            return;
        }
    };

    const MAX_FLUSH_RETRIES: u32 = 3;

    // Requests that failed and need retrying, with attempt counts.
    let mut retry_queue: Vec<(cache_core::disk::FlushRequest, u32)> = Vec::new();

    // Loop: sleep briefly, drain flush queue, submit writes, complete flushes.
    loop {
        ringline::sleep(std::time::Duration::from_millis(1)).await;

        if shutdown.load(Ordering::Relaxed) {
            return;
        }

        let flush_requests = cache.take_flush_queue();
        if flush_requests.is_empty() && retry_queue.is_empty() {
            continue;
        }

        // Combine new requests (attempt 0) with retries.
        let pending: Vec<(cache_core::disk::FlushRequest, u32)> = flush_requests
            .into_iter()
            .map(|r| (r, 0))
            .chain(retry_queue.drain(..))
            .collect();

        for (req, attempt) in pending {
            let result = match &flush_backend {
                DiskBackend::DirectIo { file, .. } => unsafe {
                    match ringline::direct_io_write(
                        *file,
                        req.disk_offset,
                        req.buffer_ptr,
                        req.buffer_len,
                    ) {
                        Ok(fut) => fut
                            .await
                            .and_then(|written| full_write(written, req.buffer_len)),
                        Err(e) => Err(e),
                    }
                },
                DiskBackend::Nvme { device, block_size } => {
                    let lba = req.disk_offset / *block_size as u64;
                    let num_blocks = (req.buffer_len / *block_size) as u16;
                    // SAFETY: the flush buffer is the segment's write buffer,
                    // held by a pin until `complete_flush`, which runs after
                    // the future resolves.
                    match unsafe {
                        ringline::nvme_write(
                            *device,
                            lba,
                            num_blocks,
                            req.buffer_ptr as u64,
                            req.buffer_len,
                        )
                    } {
                        Ok(fut) => fut.await,
                        Err(e) => Err(e),
                    }
                }
            };

            DISK_FLUSHES.increment();
            match result {
                Ok(_) => {
                    // Success: detach the write buffer; it returns to the pool
                    // when the last reader unpins.
                    cache.complete_flush(req.segment_id);
                }
                Err(e) if attempt < MAX_FLUSH_RETRIES => {
                    DISK_FLUSH_ERRORS.increment();
                    tracing::warn!(
                        segment_id = req.segment_id,
                        attempt = attempt + 1,
                        max_retries = MAX_FLUSH_RETRIES,
                        "Disk flush failed, will retry: {e}"
                    );
                    retry_queue.push((req, attempt + 1));
                }
                Err(e) => {
                    DISK_FLUSH_ERRORS.increment();
                    tracing::error!(
                        segment_id = req.segment_id,
                        "Disk flush failed after {MAX_FLUSH_RETRIES} retries, \
                         data in this segment will be lost: {e}"
                    );
                    cache.complete_flush(req.segment_id);
                }
            }
        }
    }
}

/// A Direct I/O write's result as a flush outcome: an error unless it wrote
/// all `len` bytes. A short write leaves the end of the segment unwritten on
/// disk, so it is retried like a failed one.
fn full_write(written: i32, len: u32) -> io::Result<i32> {
    if written >= 0 && written as u32 == len {
        Ok(written)
    } else {
        Err(io::Error::other(format!(
            "short disk write: {written} of {len} bytes"
        )))
    }
}

/// Initialize disk I/O for this worker using ringline async free functions.
///
/// Must be called from within the executor context (e.g., during `on_start`).
fn init_async_disk_io(config: &DiskIoWorkerConfig) -> io::Result<AsyncDiskIo> {
    let backend = match &config.backend {
        cache_core::DiskIoBackend::Nvme { device_path, nsid } => {
            let device = ringline::open_nvme_device(device_path, *nsid)?;
            DiskBackend::Nvme {
                device,
                block_size: config.block_size,
            }
        }

        cache_core::DiskIoBackend::DirectIo => {
            let file = ringline::open_direct_io_file(&config.path)?;
            DiskBackend::DirectIo {
                file,
                block_size: config.block_size,
            }
        }
    };

    let read_buffer_pool = AlignedBufferPool::new(
        config.read_buffer_count,
        config.read_buffer_size,
        config.block_size as usize,
    );

    Ok(AsyncDiskIo {
        backend,
        read_buffer_pool,
    })
}

// ── Per-connection async task ───────────────────────────────────────────

/// Per-connection configuration cloned into each async task.
#[derive(Clone, Copy)]
struct ConnConfig {
    max_value_size: usize,
    allow_flush: bool,
    slot_size: usize,
    set_retry_timeout_us: u64,
}

/// Handle a single connection's lifetime as an async task.
///
/// Loops reading data via `with_data`, processing commands through the shared
/// `Connection::process_from()`, and draining responses via `ConnCtx::send()`.
async fn handle_connection<C: Cache>(
    conn: ConnCtx,
    cache: Arc<C>,
    cfg: ConnConfig,
    disk_io: Arc<Mutex<Option<AsyncDiskIo>>>,
) {
    let mut connection = if cfg.set_retry_timeout_us > 0 {
        Connection::with_retry(cfg.max_value_size, cfg.allow_flush)
    } else {
        Connection::with_options(cfg.max_value_size, cfg.allow_flush)
    };
    // Segment pins for the keys the current command has read; see
    // `resume_suspended`.
    let mut key_pins: Vec<DiskReadRef<'_, C>> = Vec::new();

    loop {
        // Backpressure: if write queue is full, drain pending writes first.
        if !connection.should_read() && connection.has_pending_write() {
            let pending_before = connection.pending_write_len();
            if drain_pending(&conn, &mut connection, cfg.slot_size, true)
                .await
                .is_err()
            {
                break;
            }
            // If drain made progress, loop back to check backpressure again.
            // If no progress (e.g. SQE full), yield to the executor so it can
            // process CQEs and free resources before we retry.
            if connection.pending_write_len() >= pending_before && connection.has_pending_write() {
                yield_once().await;
            }
            continue;
        }

        let key_memo = connection.key_memo.clone();
        let consumed = conn
            .with_data(|data| {
                if data.is_empty() {
                    return ringline::ParseResult::Consumed(0); // EOF
                }

                let mut buf = SliceRecvBuf::new(data);
                cache_core::with_key_memo(&key_memo, || connection.process_from(&mut buf, &*cache));
                ringline::ParseResult::Consumed(buf.consumed())
            })
            .await;

        // Streaming recv sink loop: if process_from entered a streaming state
        // (large SET), bypass the accumulator by writing CQE data directly into
        // the reservation's segment/vec memory.
        while connection.is_streaming_recv() {
            if let Some((ptr, remaining)) = connection.streaming_recv_target() {
                unsafe {
                    conn.set_recv_sink(ptr, remaining);
                }
            }
            conn.recv_ready().await;
            let sink_bytes = conn.take_recv_sink();
            if sink_bytes > 0 {
                connection.advance_streaming_recv(sink_bytes);
            }
            // Process any overflow data in the accumulator (trailing CRLF, next commands).
            let processed = conn.try_with_data(|data| {
                let mut buf = SliceRecvBuf::new(data);
                cache_core::with_key_memo(&key_memo, || connection.process_from(&mut buf, &*cache));
                ringline::ParseResult::Consumed(buf.consumed())
            });
            if sink_bytes == 0
                && !matches!(processed, Some(ringline::ParseResult::Consumed(n)) if n > 0)
            {
                break; // no progress — connection closed
            }
        }

        // Commands waiting on disk reads: a key read for a suspended
        // command, or a disk-tier GET's item read. Either can lead to the
        // other: a retried command can be a GET that needs its item read,
        // and a GET whose read finds another key runs again.
        let mut closed = false;
        loop {
            if connection.suspended.is_some() {
                resume_suspended(&disk_io, &*cache, &mut connection, &mut key_pins).await;
            }
            let Some(pending_info) = connection.pending_disk_read.take() else {
                break;
            };
            if disk_io.lock().is_some() {
                if submit_and_await_disk_read(
                    &disk_io,
                    &*cache,
                    &conn,
                    &mut connection,
                    pending_info,
                    cfg.slot_size,
                    &mut key_pins,
                )
                .await
                .is_err()
                {
                    closed = true;
                    break;
                }
            } else {
                // No disk I/O configured — treat as miss.
                cache
                    .release_disk_read(pending_info.params.segment_id, pending_info.params.pool_id);
                connection.key_memo.clear();
                DISK_READ_ERRORS.increment();
                MISSES.increment();
                connection.write_miss_response();
            }
        }
        if connection.key_memo.is_empty() {
            key_pins.clear();
        }
        if closed {
            break;
        }

        // Drain pending responses.
        if connection.has_pending_write()
            && drain_pending(&conn, &mut connection, cfg.slot_size, false)
                .await
                .is_err()
        {
            break;
        }

        // Retry eviction loop for SET that failed with OutOfMemory.
        if connection.has_pending_retry() {
            use crate::metrics::SET_RETRIES;
            use std::time::Duration;

            SET_RETRIES.increment();
            let deadline =
                ringline::Deadline::after(Duration::from_micros(cfg.set_retry_timeout_us));
            loop {
                // A key read is progress, not waiting on eviction: it does
                // not count against the deadline, and the SET is retried at
                // once.
                if let Some(location) = connection.key_memo.unresolved() {
                    if !record_key(&disk_io, &*cache, &mut connection, location, &mut key_pins)
                        .await
                    {
                        connection
                            .abandon_retry_with_error(cache_core::CacheError::SegmentNotAccessible);
                        break;
                    }
                } else {
                    ringline::sleep(Duration::from_micros(50)).await;
                }
                let key_memo = connection.key_memo.clone();
                if cache_core::with_key_memo(&key_memo, || connection.retry_set(&*cache)) {
                    break; // succeeded or gave up on non-retryable error
                }
                if connection.key_memo.unresolved().is_none() && deadline.remaining().is_zero() {
                    connection.abandon_retry(); // silent drop + SET_ERRORS
                    break;
                }
            }
            // A retried SET that left its duplicate resolution pending.
            if connection.suspended.is_some() {
                resume_suspended(&disk_io, &*cache, &mut connection, &mut key_pins).await;
            }
            if connection.key_memo.is_empty() {
                key_pins.clear();
            }
            // Drain the retry's response.
            if connection.has_pending_write()
                && drain_pending(&conn, &mut connection, cfg.slot_size, false)
                    .await
                    .is_err()
            {
                break;
            }
        }

        if consumed == 0 {
            break;
        }

        if connection.should_close() {
            conn.close();
            break;
        }
    }

    connection.abandon_suspended(&*cache);
    drop(key_pins);
    CONNECTIONS_ACTIVE.decrement();
}

/// Run the connection's suspended command again, reading the key it waits on
/// first, until nothing is suspended or a retried GET needs its item read.
/// If the key cannot be read, the command fails with an error.
///
/// A key read's segment pin is kept in `key_pins` until the key memo is
/// empty, so the segment is not reused while the memo holds its key.
async fn resume_suspended<'c, C: Cache>(
    disk_io: &Arc<Mutex<Option<AsyncDiskIo>>>,
    cache: &'c C,
    connection: &mut Connection,
    key_pins: &mut Vec<DiskReadRef<'c, C>>,
) {
    while connection.suspended.is_some() && connection.pending_disk_read.is_none() {
        // A suspended command waits on `unresolved`; a pending resolve on the
        // location its resolution met.
        let location = connection.key_memo.unresolved().or_else(|| {
            matches!(connection.suspended, Some(Suspended::Resolve))
                .then(|| {
                    connection
                        .key_memo
                        .pending_resolve()
                        .map(|(_, location)| location)
                })
                .flatten()
        });
        match location {
            Some(location) => {
                if !record_key(disk_io, cache, connection, location, key_pins).await {
                    connection.fail_suspended(cache);
                    return;
                }
            }
            // A suspended command always names the key it waits on; running
            // it again with nothing new to read would not make progress.
            None if !matches!(connection.suspended, Some(Suspended::Resolve)) => {
                debug_assert!(false, "a command suspended without an unresolved key");
                connection.fail_suspended(cache);
                return;
            }
            None => {}
        }
        let key_memo = connection.key_memo.clone();
        cache_core::with_key_memo(&key_memo, || connection.resume(cache));
    }
}

/// Attempts at reading a key before the command that needs it fails.
const KEY_READ_ATTEMPTS: usize = 3;

/// Read the key at `location` and record it in the connection's key memo,
/// trying up to [`KEY_READ_ATTEMPTS`] times. Returns `false` if every read
/// failed; nothing is recorded then, as recording no key would let a write
/// add an entry beside a live one.
async fn record_key<'c, C: Cache>(
    disk_io: &Arc<Mutex<Option<AsyncDiskIo>>>,
    cache: &'c C,
    connection: &mut Connection,
    location: cache_core::Location,
    key_pins: &mut Vec<DiskReadRef<'c, C>>,
) -> bool {
    for _ in 0..KEY_READ_ATTEMPTS {
        match read_key(disk_io, cache, location, key_pins).await {
            KeyRead::Key(key) => {
                connection.key_memo.record(location, Some(key));
                return true;
            }
            KeyRead::Unpinnable => {
                connection.key_memo.record(location, None);
                return true;
            }
            KeyRead::Failed => DISK_READ_ERRORS.increment(),
        }
    }
    false
}

/// The outcome of reading an entry's key from disk.
enum KeyRead {
    /// The key stored at the entry's location.
    Key(Vec<u8>),
    /// `Cache::key_read` could not pin the entry's segment: it is freed,
    /// condemned, expired or reused, so the entry holds no key.
    Unpinnable,
    /// The read failed, returned too few bytes, or its header did not parse.
    Failed,
}

/// How long `read_key` waits between attempts to take a pooled read buffer.
const BUFFER_WAIT: std::time::Duration = std::time::Duration::from_micros(50);

/// Attempts `read_key` makes at taking a pooled read buffer (about one
/// second) before the read fails.
const BUFFER_ATTEMPTS: usize = 20_000;

/// Read the key stored at `location`, keeping its segment pin in `key_pins`.
async fn read_key<'c, C: Cache>(
    disk_io: &Arc<Mutex<Option<AsyncDiskIo>>>,
    cache: &'c C,
    location: cache_core::Location,
    key_pins: &mut Vec<DiskReadRef<'c, C>>,
) -> KeyRead {
    let Some(params) = cache.key_read(location) else {
        return KeyRead::Unpinnable;
    };
    let pin = DiskReadRef {
        cache,
        segment_id: params.segment_id,
        pool_id: params.pool_id,
    };
    let mut buffer = None;
    for _ in 0..BUFFER_ATTEMPTS {
        let allocated = match disk_io.lock().as_mut() {
            Some(dio) => dio.read_buffer_pool.allocate(),
            None => return KeyRead::Failed,
        };
        if allocated.is_some() {
            buffer = allocated;
            break;
        }
        ringline::sleep(BUFFER_WAIT).await;
    }
    let Some(mut buffer) = buffer else {
        return KeyRead::Failed;
    };
    let release_buffer = |buffer| {
        if let Some(dio) = disk_io.lock().as_mut() {
            dio.read_buffer_pool.release(buffer);
        }
    };
    if params.read_len as usize > buffer.capacity() {
        release_buffer(buffer);
        return KeyRead::Failed;
    }
    DISK_KEY_READS.increment();
    // SAFETY: `buffer` is a pooled read buffer of at least `read_len` bytes.
    // If this future is dropped mid-read, `buffer` is never returned to the
    // pool, so the kernel's write lands in memory nothing else uses.
    let read = unsafe {
        read_disk(
            disk_io,
            params.disk_offset,
            buffer.as_mut_ptr(),
            params.read_len as usize,
        )
    }
    .await;
    let key = read.ok().and_then(|valid_len| {
        // SAFETY: the read returned `valid_len` bytes into `buffer`.
        let data = unsafe { buffer.as_slice(valid_len) };
        crate::disk_io::stored_key_from_disk_read(data, params.item_offset as usize)
            .map(<[u8]>::to_vec)
    });
    release_buffer(buffer);
    match key {
        Some(key) => {
            key_pins.push(pin);
            KeyRead::Key(key)
        }
        None => KeyRead::Failed,
    }
}

/// Releases a disk segment's read reference when dropped, including when
/// the disk read future is dropped before its reads complete.
struct DiskReadRef<'a, C: Cache> {
    cache: &'a C,
    segment_id: u32,
    pool_id: u8,
}

impl<C: Cache> Drop for DiskReadRef<'_, C> {
    fn drop(&mut self) {
        self.cache.release_disk_read(self.segment_id, self.pool_id);
    }
}

/// Read a disk-tier item and write the protocol response: the value, or a
/// miss if the read fails or finds another key's or a deleted item.
///
/// The first read fills a pooled buffer; an item that runs past it is read
/// again in full into a buffer of its own, whose value is sent without a
/// copy. The segment's read reference is released once the reads complete.
async fn submit_and_await_disk_read<'c, C: Cache>(
    disk_io: &Arc<Mutex<Option<AsyncDiskIo>>>,
    cache: &'c C,
    conn: &ConnCtx,
    connection: &mut Connection,
    pending_info: PendingDiskReadInfo,
    slot_size: usize,
    key_pins: &mut Vec<DiskReadRef<'c, C>>,
) -> Result<(), ()> {
    let segment_id = pending_info.params.segment_id;
    let pool_id = pending_info.params.pool_id;
    let segment_ref = DiskReadRef {
        cache,
        segment_id,
        pool_id,
    };

    let release_buffer = |buffer| {
        if let Some(dio) = disk_io.lock().as_mut() {
            dio.read_buffer_pool.release(buffer);
        }
    };
    let miss_on_error = |connection: &mut Connection, e: &dyn std::fmt::Display| {
        connection.key_memo.clear();
        DISK_READ_ERRORS.increment();
        MISSES.increment();
        tracing::warn!(segment_id, pool_id, "Disk read failed: {e}");
        connection.write_miss_response();
    };

    // 1. Allocate aligned read buffer.
    let mut buffer: cache_core::disk::AlignedBuffer = {
        let mut dio = disk_io.lock();
        let dio = dio
            .as_mut()
            .expect("disk_io must be Some when submit_and_await_disk_read is called");
        match dio.read_buffer_pool.allocate() {
            Some(buf) => buf,
            None => {
                DISK_READ_ERRORS.increment();
                MISSES.increment();
                connection.key_memo.clear();
                connection.write_miss_response();
                return Ok(());
            }
        }
    };

    // A read longer than the buffer would have the kernel write past it.
    // `read_buffer_size` covers every read `lookup` asks for while the disk
    // tier's `block_size` is no larger than `DiskIoWorkerConfig`'s; both are
    // `DISK_BLOCK_SIZE`.
    if pending_info.params.read_len as usize > buffer.capacity() {
        DISK_READ_ERRORS.increment();
        MISSES.increment();
        tracing::warn!(
            read_len = pending_info.params.read_len,
            capacity = buffer.capacity(),
            "disk read longer than its buffer"
        );
        connection.key_memo.clear();
        connection.write_miss_response();
        release_buffer(buffer);
        return Ok(());
    }

    // 2. Read one block from the item's start (two blocks on disk unless the
    // item starts on a block boundary).
    let item_offset = pending_info.params.item_offset as usize;
    let read_len = pending_info.params.read_len as usize;
    let disk_offset = pending_info.params.disk_offset;
    DISK_READS.increment();
    // SAFETY: `buffer` is a pooled read buffer of at least `read_len` bytes
    // (checked above). If this future is dropped mid-read, `buffer` is never
    // returned to the pool, so the kernel's write lands in memory nothing
    // else uses.
    let valid_len =
        match unsafe { read_disk(disk_io, disk_offset, buffer.as_mut_ptr(), read_len) }.await {
            Ok(n) => n,
            Err(e) => {
                miss_on_error(connection, &e);
                release_buffer(buffer);
                return Ok(());
            }
        };

    // 3. Locate the value. Only the first `valid_len` bytes of `buffer` come
    // from this read; the rest hold an earlier read's data. A value that fits
    // is copied out and the buffer returned to the pool at once. An item
    // longer than the first read is read again in full into a
    // `LargeReadBuffer`.
    // SAFETY: the read returned `valid_len` bytes into `buffer`.
    let first = unsafe { buffer.as_slice(valid_len) };
    let key = &pending_info.key;

    // The entry was matched without its key, which is only on disk. If the
    // item holds another key, record it and run the GET again: its lookup
    // then answers this entry from the key read and moves on.
    let read_key = crate::disk_io::key_from_disk_read(first, item_offset);
    if read_key != Some(key.as_slice()) {
        connection
            .key_memo
            .record(pending_info.params.location, read_key.map(<[u8]>::to_vec));
        release_buffer(buffer);
        key_pins.push(segment_ref);
        connection.suspended = Some(Suspended::Command(pending_info.command));
        return Ok(());
    }

    let value = match crate::disk_io::item_len_from_disk_read(first, item_offset) {
        Some(item_len) if item_offset + item_len <= valid_len => {
            let value = crate::disk_io::value_range_from_disk_read(first, item_offset, key)
                .map(|range| Bytes::copy_from_slice(&first[range]));
            release_buffer(buffer);
            Ok(value)
        }
        Some(_) if !crate::disk_io::key_matches(first, item_offset, key) => {
            release_buffer(buffer);
            Ok(None)
        }
        Some(item_len) => {
            release_buffer(buffer);
            DISK_READS.increment();
            read_large_item(disk_io, &pending_info.params, item_len, key).await
        }
        None => {
            release_buffer(buffer);
            Ok(None)
        }
    };
    // Both reads have completed and the value, if any, is in a buffer of
    // this request's, not in the segment.
    drop(segment_ref);
    connection.key_memo.clear();

    match value {
        Ok(Some(value)) => {
            DISK_READ_HITS.increment();
            HITS.increment();
            connection.write_disk_read_response(&pending_info.response_ctx, value);
        }
        Ok(None) => {
            DISK_READ_MISSES.increment();
            MISSES.increment();
            connection.write_miss_response();
        }
        Err(e) => miss_on_error(connection, &e),
    }

    // 4. Drain the response.
    if connection.has_pending_write()
        && drain_pending(conn, connection, slot_size, false)
            .await
            .is_err()
    {
        return Err(());
    }
    Ok(())
}

/// Largest single NVMe passthrough read issued by `read_disk`. 128 KiB is an
/// assumed lower bound on the device's maximum data transfer size; the
/// device's actual limit is not queried. Longer reads are split into several
/// commands.
const NVME_MAX_READ: usize = 128 * 1024;

/// Read `len` bytes at `offset` into `buf`, returning how many bytes the read
/// produced. A Direct I/O read can return fewer bytes than asked; an NVMe
/// read either transfers everything or fails.
///
/// # Safety
///
/// `buf` is valid, writable and aligned to the block size for `len` bytes
/// until the kernel completes the read. Dropping the returned future does
/// not cancel the read, so if it is dropped early the memory must be neither
/// freed nor reused.
async unsafe fn read_disk(
    disk_io: &Arc<Mutex<Option<AsyncDiskIo>>>,
    offset: u64,
    buf: *mut u8,
    len: usize,
) -> std::io::Result<usize> {
    let backend = disk_io
        .lock()
        .as_ref()
        .map(|dio| dio.backend)
        .ok_or_else(|| std::io::Error::other("disk I/O is not initialized"))?;
    match backend {
        DiskBackend::DirectIo { file, .. } => {
            // SAFETY: the caller upholds this function's contract.
            let read = unsafe { ringline::direct_io_read(file, offset, buf, len as u32) }?;
            let n = read.await?;
            Ok((n.max(0) as usize).min(len))
        }
        DiskBackend::Nvme { device, block_size } => {
            let mut done = 0;
            while done < len {
                let chunk = (len - done).min(NVME_MAX_READ);
                let lba = (offset + done as u64) / block_size as u64;
                let num_blocks = (chunk / block_size as usize) as u16;
                // SAFETY: the caller upholds this function's contract, and
                // `done + chunk <= len`.
                let read = unsafe {
                    ringline::nvme_read(device, lba, num_blocks, buf.add(done) as u64, chunk as u32)
                }?;
                // Passthrough completes with the NVMe status: 0 is success,
                // anything else is a device error.
                let status = read.await?;
                if status != 0 {
                    return Err(std::io::Error::other(format!(
                        "NVMe read status {status:#x}"
                    )));
                }
                done += chunk;
            }
            Ok(len)
        }
    }
}

/// Read an item that runs past the first read: its whole block-aligned
/// range from the first read's `disk_offset`, into a buffer of its own.
/// `item_len` is the length the item's header gave.
///
/// An item that would extend past its segment's end is an error: its header
/// is corrupt.
async fn read_large_item(
    disk_io: &Arc<Mutex<Option<AsyncDiskIo>>>,
    params: &cache_core::DiskReadParams,
    item_len: usize,
    key: &[u8],
) -> std::io::Result<Option<Bytes>> {
    let block = disk_io
        .lock()
        .as_ref()
        .map(|dio| match dio.backend {
            DiskBackend::DirectIo { block_size, .. } | DiskBackend::Nvme { block_size, .. } => {
                block_size as usize
            }
        })
        .ok_or_else(|| std::io::Error::other("disk I/O is not initialized"))?;
    let item_offset = params.item_offset as usize;
    let item_end = item_offset + item_len;
    let len = item_end.div_ceil(block) * block;
    if params.disk_offset + len as u64 > params.segment_end {
        return Err(std::io::Error::other(format!(
            "item of {item_len} bytes at offset {} extends past its segment",
            params.disk_offset + item_offset as u64
        )));
    }
    let buffer = crate::disk_io::LargeReadBuffer::new(len, block)
        .ok_or_else(|| std::io::Error::other("disk read buffer allocation failed"))?;
    let mut buffer = std::mem::ManuallyDrop::new(buffer);
    // SAFETY: `buffer` holds `len` bytes aligned to the block size. It stays
    // in a `ManuallyDrop` until the read completes, so if this future is
    // dropped mid-read the buffer is leaked rather than freed while the
    // kernel writes into it.
    let read = unsafe { read_disk(disk_io, params.disk_offset, buffer.as_mut_ptr(), len) }.await;
    let buffer = std::mem::ManuallyDrop::into_inner(buffer);
    let read = read?;
    if read < item_end {
        return Ok(None);
    }
    let range =
        crate::disk_io::value_range_from_disk_read(&buffer.as_slice()[..read], item_offset, key);
    Ok(range.map(|range| Bytes::from_owner(crate::disk_io::DiskReadValue::new(buffer, range))))
}

// ── Send helpers ────────────────────────────────────────────────────────

/// Minimum part size to use zero-copy guard path instead of copy.
const GUARD_MIN_SIZE: usize = 1024;

/// Maximum iovecs in a single scatter-gather send SQE.
///
/// Mirrors ringline's internal `MAX_IOVECS` (unexported since 0.3.0). A batch
/// larger than this is rejected whole by `submit_batch`, so we split here.
const MAX_IOVECS: usize = 32;

/// Maximum zero-copy guards in a single scatter-gather send SQE.
///
/// Mirrors ringline's internal `MAX_GUARDS` (unexported since 0.3.0).
const MAX_GUARDS: usize = 8;

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
/// Batches consecutive parts into scatter-gather SQEs with mixed copy + guard
/// parts, exactly matching the callback handler's `send_pending` path. Small
/// parts (< 1KB) are copy iovecs; large parts (≥ 1KB) are zero-copy guards.
/// Up to MAX_IOVECS parts and MAX_GUARDS guards per SQE.
///
/// When `must_yield` is true (backpressure path), the first SQE is awaited to
/// completion, so the connection paces itself against the wire instead of
/// queueing responses as fast as it can build them. Awaiting also parks the
/// task, which is what lets the worker's event loop block on a completion
/// rather than spin.
async fn drain_pending(
    conn: &ConnCtx,
    connection: &mut Connection,
    slot_size: usize,
    must_yield: bool,
) -> Result<(), ()> {
    let mut need_yield = must_yield;

    loop {
        if !connection.has_pending_write() {
            return Ok(());
        }

        let parts = connection.collect_pending_writes();
        if parts.is_empty() {
            return Ok(());
        }

        let mut advanced = 0usize;
        let mut part_idx = 0;

        while part_idx < parts.len() {
            // Fast path: single small part — simple copy send.
            if parts.len() - part_idx == 1 && parts[part_idx].len() <= slot_size {
                let data = &parts[part_idx];
                if need_yield {
                    match conn.send(data) {
                        Ok(fut) => {
                            need_yield = false;
                            if fut.await.is_err() {
                                connection.advance_write(advanced);
                                return Err(());
                            }
                        }
                        Err(e) if e.kind() == io::ErrorKind::Other => {
                            connection.advance_write(advanced);
                            return Ok(());
                        }
                        Err(_) => return Err(()),
                    }
                } else {
                    match conn.send_nowait(data) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::Other => {
                            connection.advance_write(advanced);
                            return Ok(());
                        }
                        Err(_) => return Err(()),
                    }
                }
                advanced += data.len();
                part_idx += 1;
                continue;
            }

            // Scatter-gather path: batch mixed copy + guard parts into one SQE.
            let mut batch = Vec::with_capacity(MAX_IOVECS.min(parts.len() - part_idx));
            let mut copy_budget = slot_size;
            let mut guard_count = 0usize;
            let mut batch_bytes = 0usize;

            for part in &parts[part_idx..] {
                if batch.len() >= MAX_IOVECS {
                    break;
                }
                if part.len() >= GUARD_MIN_SIZE && guard_count < MAX_GUARDS {
                    batch.push(SendPart::Guard(GuardBox::new(BytesGuard(part.clone()))));
                    guard_count += 1;
                } else if part.len() <= copy_budget {
                    batch.push(SendPart::Copy(part));
                    copy_budget -= part.len();
                } else {
                    break;
                }
                batch_bytes += part.len();
            }

            if batch.is_empty() {
                // Can't fit anything (single part too large for copy, guard limit hit).
                // Fall back to copy send with chunking.
                let part = &parts[part_idx];
                let mut offset = 0;
                while offset < part.len() {
                    let end = (offset + slot_size).min(part.len());
                    if need_yield {
                        match conn.send(&part[offset..end]) {
                            Ok(fut) => {
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
                        match conn.send_nowait(&part[offset..end]) {
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
                part_idx += 1;
                continue;
            }

            let batch_count = batch.len();

            let result = if need_yield {
                match conn.send_parts().submit_batch_await(batch) {
                    Ok((_, fut)) => {
                        need_yield = false;
                        if fut.await.is_err() {
                            connection.advance_write(advanced);
                            return Err(());
                        }
                        Ok(())
                    }
                    Err(e) if e.kind() == io::ErrorKind::Other => {
                        connection.advance_write(advanced);
                        return Ok(());
                    }
                    Err(_) => Err(()),
                }
            } else {
                match conn.send_parts().submit_batch(batch) {
                    Ok(_) => Ok(()),
                    Err(e) if e.kind() == io::ErrorKind::Other => {
                        connection.advance_write(advanced);
                        return Ok(());
                    }
                    Err(_) => Err(()),
                }
            };

            if result.is_err() {
                return Err(());
            }

            advanced += batch_bytes;
            part_idx += batch_count;
        }

        connection.advance_write(advanced);
    }
}

/// Yield once to the executor, allowing it to process pending CQEs.
///
/// Returns `Pending` on the first poll, then `Ready(())` on subsequent polls.
/// This is used when the SQE is full during backpressure draining — yielding
/// lets the executor process completions and free SQE slots.
fn yield_once() -> impl Future<Output = ()> {
    let mut yielded = false;
    std::future::poll_fn(move |cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
}

#[cfg(test)]
mod tests {
    use super::full_write;

    /// A flush counts as done only when the write covered the whole buffer.
    #[test]
    fn a_short_flush_write_is_an_error() {
        assert_eq!(full_write(8192, 8192).unwrap(), 8192);
        assert!(full_write(4096, 8192).is_err(), "a short write passed");
        assert!(full_write(0, 8192).is_err());
        assert!(full_write(-5, 8192).is_err());
    }
}
