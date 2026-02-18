//! Krio AsyncEventHandler implementation for the cache server.
//!
//! Mirrors `native/handler.rs` but uses krio's async API (one task per connection).
//! The per-connection async task reuses `Connection::process_from()` for parsing
//! and command execution, then drains pending writes via copy sends (small
//! protocol framing) and zero-copy guard sends (large values).

use crate::connection::{Connection, PendingDiskReadInfo, SliceRecvBuf};
use crate::disk_io::DiskBackend;
use crate::metrics::{CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE, WorkerStats};
use crate::native::handler::DiskIoWorkerConfig;
use bytes::Bytes;
use cache_core::Cache;
use cache_core::disk::AlignedBufferPool;
use krio::{
    AsyncEventHandler, ConnCtx, DriverCtx, GuardBox, MAX_GUARDS, MAX_IOVECS, RegionId, SendGuard,
    SendPart,
};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// ── Config channel ──────────────────────────────────────────────────────

/// Per-worker configuration passed to AsyncServerHandler during creation.
pub(crate) struct HandlerConfig<C: Cache> {
    pub cache: Arc<C>,
    pub stats: Arc<Vec<WorkerStats>>,
    pub shutdown: Arc<AtomicBool>,
    pub max_value_size: usize,
    pub allow_flush: bool,
    pub send_copy_slot_size: usize,
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

/// Per-worker disk I/O state for the async handler.
///
/// Shared via `Rc<RefCell<...>>` so the per-connection async tasks can
/// borrow it. This is safe because krio is single-threaded per worker.
struct AsyncDiskIo {
    backend: DiskBackend,
    read_buffer_pool: AlignedBufferPool,
}

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
    /// Shared disk I/O state, lazily initialized on first accept.
    /// Uses `Arc<Mutex<...>>` because `AsyncEventHandler` requires `Send`.
    /// The Mutex is never contended (krio is single-threaded per worker).
    disk_io: Arc<Mutex<Option<AsyncDiskIo>>>,
    /// Saved config for deferred disk I/O initialization (needs executor context).
    disk_io_config: Mutex<Option<DiskIoWorkerConfig>>,
}

impl<C: Cache + 'static> AsyncEventHandler for AsyncServerHandler<C> {
    fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        CONNECTIONS_ACCEPTED.increment();
        CONNECTIONS_ACTIVE.increment();

        // Take the disk I/O config (if not yet consumed) so the first async
        // task to run can initialize it inside the executor context.
        let pending_disk_io_config = self.disk_io_config.lock().unwrap().take();

        let cache = Arc::clone(&self.cache);
        let max_value_size = self.max_value_size;
        let allow_flush = self.allow_flush;
        let slot_size = self.send_copy_slot_size;
        let disk_io = Arc::clone(&self.disk_io);

        Box::pin(handle_connection(
            conn,
            cache,
            max_value_size,
            allow_flush,
            slot_size,
            disk_io,
            pending_disk_io_config,
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
            disk_io: Arc::new(Mutex::new(None)),
            disk_io_config: Mutex::new(config.disk_io_config),
        }
    }
}

/// Initialize disk I/O for this worker using krio async free functions.
///
/// Must be called from within the executor context (e.g., during `on_accept`).
fn init_async_disk_io(config: DiskIoWorkerConfig) -> io::Result<AsyncDiskIo> {
    let backend = match &config.backend {
        cache_core::DiskIoBackend::Nvme { device_path, nsid } => {
            let device = krio::open_nvme_device(device_path, *nsid)?;
            DiskBackend::Nvme {
                device,
                block_size: config.block_size,
            }
        }

        cache_core::DiskIoBackend::DirectIo => {
            let file = krio::open_direct_io_file(&config.path)?;
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
    disk_io: Arc<Mutex<Option<AsyncDiskIo>>>,
    pending_disk_io_config: Option<DiskIoWorkerConfig>,
) {
    // Lazy-initialize disk I/O on the first connection task that carries
    // the config. This runs inside the executor context (poll_ready_tasks)
    // where krio::open_direct_io_file() is available.
    if let Some(dio_config) = pending_disk_io_config {
        let needs_init = disk_io.lock().unwrap().is_none();
        if needs_init {
            match init_async_disk_io(dio_config) {
                Ok(state) => {
                    *disk_io.lock().unwrap() = Some(state);
                }
                Err(e) => {
                    tracing::error!("Failed to initialize async disk I/O: {e}");
                }
            }
        }
    }

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
                connection.process_from(&mut buf, &*cache);
                buf.consumed()
            });
            if sink_bytes == 0 && processed.unwrap_or(0) == 0 {
                break; // no progress — connection closed
            }
        }

        // Handle pending disk read (disk-tier GET).
        if connection.pending_disk_read.is_some() {
            let has_disk_io = disk_io.lock().unwrap().is_some();
            if has_disk_io {
                let pending_info = connection.pending_disk_read.take().unwrap();
                match submit_and_await_disk_read(&disk_io, &conn, &mut connection, pending_info, slot_size).await {
                    Ok(()) => {}
                    Err(()) => break,
                }
            } else {
                // No disk I/O configured — treat as miss.
                connection.pending_disk_read.take();
                connection.write_miss_response();
            }
        }

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

/// Submit a disk read via io_uring and await its completion inline.
///
/// On success, parses the item from the read buffer and writes the protocol
/// response. On failure, writes a miss response.
async fn submit_and_await_disk_read(
    disk_io: &Arc<Mutex<Option<AsyncDiskIo>>>,
    conn: &ConnCtx,
    connection: &mut Connection,
    pending_info: PendingDiskReadInfo,
    slot_size: usize,
) -> Result<(), ()> {
    // 1. Allocate aligned read buffer.
    let mut buffer: cache_core::disk::AlignedBuffer = {
        let mut dio = disk_io.lock().unwrap();
        let dio = dio.as_mut().unwrap();
        match dio.read_buffer_pool.allocate() {
            Some(buf) => buf,
            None => {
                connection.write_miss_response();
                return Ok(());
            }
        }
    };

    // 2. Submit io_uring read and await completion.
    // Extract what we need from disk_io under the lock, then drop the lock
    // before awaiting (the future spans a suspend point).
    let future = {
        let dio = disk_io.lock().unwrap();
        let dio = dio.as_ref().unwrap();
        match &dio.backend {
            DiskBackend::DirectIo { file, .. } => unsafe {
                krio::direct_io_read(
                    *file,
                    pending_info.params.disk_offset,
                    buffer.as_mut_ptr(),
                    pending_info.params.read_len,
                )
            },
            DiskBackend::Nvme { device, block_size } => {
                let lba = pending_info.params.disk_offset / *block_size as u64;
                let num_blocks = (pending_info.params.read_len / *block_size) as u16;
                krio::nvme_read(*device, lba, num_blocks, buffer.addr(), pending_info.params.read_len)
            }
        }
    };

    let result = match future {
        Ok(fut) => fut.await,
        Err(e) => Err(e),
    };

    // 3. Parse result and write response (same logic as native/handler.rs).
    if result.is_err() {
        connection.write_miss_response();
        disk_io.lock().unwrap().as_mut().unwrap().read_buffer_pool.release(buffer);
        return Ok(());
    }

    let item_offset = pending_info.params.item_offset as usize;
    let buf_slice = unsafe { buffer.as_slice(pending_info.params.read_len as usize) };
    let header_size = cache_core::BasicHeader::SIZE;

    if item_offset + header_size > buf_slice.len() {
        connection.write_miss_response();
        disk_io.lock().unwrap().as_mut().unwrap().read_buffer_pool.release(buffer);
        return Ok(());
    }

    let header = cache_core::BasicHeader::from_bytes_unchecked(
        &buf_slice[item_offset..item_offset + header_size],
    );

    if header.is_deleted() {
        connection.write_miss_response();
        disk_io.lock().unwrap().as_mut().unwrap().read_buffer_pool.release(buffer);
        return Ok(());
    }

    let key_start = item_offset + header_size + header.optional_len() as usize;
    let value_start = key_start + header.key_len() as usize;
    let value_len = header.value_len() as usize;
    let value_end = value_start + value_len;

    if value_end > buf_slice.len() {
        connection.write_miss_response();
        disk_io.lock().unwrap().as_mut().unwrap().read_buffer_pool.release(buffer);
        return Ok(());
    }

    let value_bytes = &buf_slice[value_start..value_end];
    connection.write_disk_read_response(&pending_info.response_ctx, value_bytes);

    // 4. Drain the response.
    if connection.has_pending_write() {
        drain_pending(conn, connection, slot_size, false).await.map_err(|_| ())?;
    }

    disk_io.lock().unwrap().as_mut().unwrap().read_buffer_pool.release(buffer);
    Ok(())
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
/// Batches consecutive parts into scatter-gather SQEs with mixed copy + guard
/// parts, exactly matching the callback handler's `send_pending` path. Small
/// parts (< 1KB) are copy iovecs; large parts (≥ 1KB) are zero-copy guards.
/// Up to MAX_IOVECS parts and MAX_GUARDS guards per SQE.
///
/// When `must_yield` is true (backpressure path), the first SQE uses
/// `submit_batch_await` to guarantee at least one yield.
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
                    match conn.send_await(data) {
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
                    match conn.send(data) {
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
                        match conn.send_await(&part[offset..end]) {
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
