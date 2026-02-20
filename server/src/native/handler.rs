//! Krio EventHandler implementation for the cache server.

use crate::connection::{Connection, SliceRecvBuf};
use crate::disk_io::{DiskBackend, DiskIoState, PendingDiskRead};
use crate::metrics::{
    CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE, CloseReason, DISK_FLUSHES, DISK_FLUSH_ERRORS,
    DISK_READS, DISK_READ_ERRORS, DISK_READ_HITS, DISK_READ_MISSES, HITS, MISSES, WorkerStats,
};
use bytes::Bytes;
use cache_core::Cache;
use krio::{
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
    /// Disk I/O configuration. When set, workers open the device/file
    /// in `create_for_worker` and enable disk read/write support.
    pub disk_io_config: Option<DiskIoWorkerConfig>,
}

/// Configuration for per-worker disk I/O initialization.
pub(crate) struct DiskIoWorkerConfig {
    /// Backend type.
    pub backend: cache_core::DiskIoBackend,
    /// Path to the disk file (used by DirectIo backend).
    pub path: String,
    /// Number of read buffers per worker.
    pub read_buffer_count: usize,
    /// Size of each read buffer (typically one block = 4096).
    pub read_buffer_size: usize,
    /// Block size for alignment.
    pub block_size: u32,
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

/// Krio event handler for the cache server.
pub(crate) struct ServerHandler<C: Cache> {
    cache: Arc<C>,
    connections: Vec<Option<Connection>>,
    stats: Arc<Vec<WorkerStats>>,
    worker_id: usize,
    shutdown: Arc<AtomicBool>,
    max_value_size: usize,
    allow_flush: bool,
    send_copy_slot_size: usize,
    /// Per-worker disk I/O state. None if disk tier is not configured.
    disk_io: Option<DiskIoState>,
    /// Saved config for deferred disk I/O initialization in create_for_worker.
    disk_io_config: Option<DiskIoWorkerConfig>,
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
            disk_io: None,
            disk_io_config: config.disk_io_config,
        }
    }

    fn on_accept(&mut self, ctx: &mut DriverCtx, conn: ConnToken) {
        CONNECTIONS_ACCEPTED.increment();
        CONNECTIONS_ACTIVE.increment();
        self.stats[self.worker_id].inc_accepts();

        // Lazy-initialize disk I/O on first accept (needs DriverCtx for device open)
        if self.disk_io.is_none()
            && let Some(dio_config) = self.disk_io_config.take()
        {
            match Self::init_disk_io(ctx, dio_config) {
                Ok(state) => {
                    self.disk_io = Some(state);
                }
                Err(e) => {
                    tracing::error!("Failed to initialize disk I/O: {e}");
                }
            }
        }

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

            // Check if a disk read is pending — submit io_uring read
            if c.pending_disk_read.is_some() {
                if let Some(disk_io) = &mut self.disk_io {
                    let pending_info = c.pending_disk_read.take().unwrap();
                    if let Err(e) = Self::submit_disk_read(ctx, disk_io, conn, pending_info) {
                        tracing::warn!("Disk read submit failed: {e}");
                        DISK_READ_ERRORS.increment();
                        MISSES.increment();
                        // Send miss response since we can't do the disk read
                        c.write_miss_response();
                    }
                } else {
                    // No disk I/O configured but got DiskRead result — treat as miss
                    c.pending_disk_read.take();
                    DISK_READ_ERRORS.increment();
                    MISSES.increment();
                    c.write_miss_response();
                }
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
        let idx = conn.index();
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

        // Submit pending disk tier flushes (write buffers → disk).
        if self.disk_io.is_some() {
            self.submit_pending_flushes(ctx);
        }

        if self.shutdown.load(Ordering::Relaxed) {
            let active = self.connections.iter().filter(|c| c.is_some()).count();
            if active == 0 {
                ctx.request_shutdown();
            }
        }
    }

    fn on_nvme_complete(&mut self, ctx: &mut DriverCtx, completion: krio::NvmeCompletion) {
        self.handle_disk_read_complete(ctx, completion.seq, completion.is_success());
    }

    fn on_direct_io_complete(&mut self, ctx: &mut DriverCtx, completion: krio::DirectIoCompletion) {
        use krio::DirectIoOp;
        match completion.op {
            DirectIoOp::Read => {
                self.handle_disk_read_complete(ctx, completion.seq, completion.is_success());
            }
            DirectIoOp::Write => {
                self.handle_disk_flush_complete(completion.seq, completion.is_success());
            }
            DirectIoOp::Fsync => {
                // No action needed for fsync completions
            }
        }
    }
}

impl<C: Cache> ServerHandler<C> {
    /// Close a connection: issue close to krio and clean up state.
    fn close_connection(&mut self, ctx: &mut DriverCtx, conn: ConnToken, reason: CloseReason) {
        ctx.close(conn);
        self.close_connection_state(conn, reason);
    }

    /// Clean up connection state only (when krio already reported close).
    fn close_connection_state(&mut self, conn: ConnToken, reason: CloseReason) {
        let idx = conn.index();
        if let Some(slot) = self.connections.get_mut(idx)
            && slot.take().is_some()
        {
            CONNECTIONS_ACTIVE.decrement();
            self.stats[self.worker_id].inc_close(reason);
        }
    }

    /// Initialize disk I/O for this worker.
    ///
    /// Opens the NVMe device or Direct I/O file and creates the read buffer pool.
    fn init_disk_io(ctx: &mut DriverCtx, config: DiskIoWorkerConfig) -> io::Result<DiskIoState> {
        let backend = match &config.backend {
            cache_core::DiskIoBackend::Nvme { device_path, nsid } => {
                let device = ctx.open_nvme_device(device_path, *nsid)?;
                DiskBackend::Nvme {
                    device,
                    block_size: config.block_size,
                }
            }
            cache_core::DiskIoBackend::DirectIo => {
                let file = ctx.open_direct_io_file(&config.path)?;
                DiskBackend::DirectIo {
                    file,
                    block_size: config.block_size,
                }
            }
        };

        let read_buffer_pool = cache_core::AlignedBufferPool::new(
            config.read_buffer_count,
            config.read_buffer_size,
            config.block_size as usize,
        );

        Ok(DiskIoState::new(backend, read_buffer_pool, 256))
    }

    /// Submit a disk read via io_uring.
    fn submit_disk_read(
        ctx: &mut DriverCtx,
        disk_io: &mut DiskIoState,
        conn: ConnToken,
        pending_info: crate::connection::PendingDiskReadInfo,
    ) -> io::Result<()> {
        // Allocate a read buffer
        let mut buffer = disk_io
            .read_buffer_pool
            .allocate()
            .ok_or_else(|| io::Error::other("read buffer pool exhausted"))?;

        let seq = match &disk_io.backend {
            DiskBackend::Nvme { device, block_size } => {
                let lba = pending_info.params.disk_offset / *block_size as u64;
                let num_blocks = (pending_info.params.read_len / *block_size) as u16;
                ctx.nvme_read(
                    *device,
                    lba,
                    num_blocks,
                    buffer.addr(),
                    pending_info.params.read_len,
                )?
            }
            DiskBackend::DirectIo { file, .. } => unsafe {
                ctx.direct_io_read(
                    *file,
                    pending_info.params.disk_offset,
                    buffer.as_mut_ptr(),
                    pending_info.params.read_len,
                )?
            },
        };

        // Store pending read for completion correlation
        disk_io.store_pending_read(
            seq,
            PendingDiskRead {
                conn,
                buffer,
                params: pending_info.params,
                response_ctx: pending_info.response_ctx,
            },
        );

        DISK_READS.increment();
        Ok(())
    }

    /// Handle a disk read completion (from NVMe or Direct I/O).
    fn handle_disk_read_complete(&mut self, ctx: &mut DriverCtx, seq: u32, success: bool) {
        let Some(disk_io) = &mut self.disk_io else {
            return;
        };

        let Some(pending) = disk_io.take_pending_read(seq) else {
            return;
        };

        let conn_idx = pending.conn.index();
        let conn_opt = self.connections.get_mut(conn_idx).and_then(|c| c.as_mut());

        if !success {
            // Disk read failed — send miss response and resume
            DISK_READ_ERRORS.increment();
            MISSES.increment();
            if let Some(c) = conn_opt {
                c.write_miss_response();
                c.pending_disk_read = None;
                let _ = send_pending(ctx, c, pending.conn, self.send_copy_slot_size);
            }
            // Release read buffer and segment ref_count
            disk_io.release_read_buffer(pending.buffer);
            self.cache
                .release_disk_read(pending.params.segment_id, pending.params.pool_id);
            return;
        }

        // Parse item from read buffer
        let item_offset = pending.params.item_offset as usize;
        let buf_slice = unsafe { pending.buffer.as_slice(pending.params.read_len as usize) };

        // Parse the BasicHeader at the item offset
        let header_size = cache_core::BasicHeader::SIZE;
        if item_offset + header_size > buf_slice.len() {
            // Invalid offset — send miss
            DISK_READ_MISSES.increment();
            MISSES.increment();
            if let Some(c) = conn_opt {
                c.write_miss_response();
                c.pending_disk_read = None;
                let _ = send_pending(ctx, c, pending.conn, self.send_copy_slot_size);
            }
            disk_io.release_read_buffer(pending.buffer);
            return;
        }

        let header =
            cache_core::BasicHeader::from_bytes(&buf_slice[item_offset..item_offset + header_size]);

        if header.is_deleted() {
            DISK_READ_MISSES.increment();
            MISSES.increment();
            if let Some(c) = conn_opt {
                c.write_miss_response();
                c.pending_disk_read = None;
                let _ = send_pending(ctx, c, pending.conn, self.send_copy_slot_size);
            }
            disk_io.release_read_buffer(pending.buffer);
            return;
        }

        // Extract value from the read buffer
        let key_start = item_offset + header_size + header.optional_len() as usize;
        let value_start = key_start + header.key_len() as usize;
        let value_len = header.value_len() as usize;
        let value_end = value_start + value_len;

        if value_end > buf_slice.len() {
            // Item spans beyond our read buffer — would need a second read.
            // For now, treat as miss (TODO: implement multi-block reads).
            DISK_READ_MISSES.increment();
            MISSES.increment();
            if let Some(c) = conn_opt {
                c.write_miss_response();
                c.pending_disk_read = None;
                let _ = send_pending(ctx, c, pending.conn, self.send_copy_slot_size);
            }
            disk_io.release_read_buffer(pending.buffer);
            return;
        }

        let value_bytes = &buf_slice[value_start..value_end];

        // Build protocol response — disk read succeeded
        DISK_READ_HITS.increment();
        HITS.increment();
        if let Some(c) = conn_opt {
            c.write_disk_read_response(&pending.response_ctx, value_bytes);
            c.pending_disk_read = None;

            // Resume sending and processing
            let _ = send_pending(ctx, c, pending.conn, self.send_copy_slot_size);
        }

        // Release read buffer and disk segment ref_count
        disk_io.release_read_buffer(pending.buffer);
        self.cache
            .release_disk_read(pending.params.segment_id, pending.params.pool_id);
    }

    /// Handle a disk flush (write) completion.
    fn handle_disk_flush_complete(&mut self, seq: u32, success: bool) {
        let Some(disk_io) = &mut self.disk_io else {
            return;
        };

        let Some(pending) = disk_io.take_pending_flush(seq) else {
            return;
        };

        DISK_FLUSHES.increment();
        if !success {
            DISK_FLUSH_ERRORS.increment();
            tracing::warn!("Disk flush failed for seq={seq}");
            // On failure, still detach the write buffer to avoid leaking it.
            // The data is lost for this segment, but items will be evicted
            // from the hashtable on the next lookup (read returns garbage).
        }

        // Detach write buffer and return it to the pool so it can be reused.
        self.cache.complete_flush(pending.segment_id);
    }

    /// Drain the cache's disk flush queue and submit io_uring writes.
    fn submit_pending_flushes(&mut self, ctx: &mut DriverCtx) {
        let flush_requests = self.cache.take_flush_queue();
        if flush_requests.is_empty() {
            return;
        }

        let Some(disk_io) = &mut self.disk_io else {
            return;
        };

        for req in flush_requests {
            let result = match &disk_io.backend {
                DiskBackend::Nvme { device, block_size } => {
                    let lba = req.disk_offset / *block_size as u64;
                    let num_blocks = (req.buffer_len / *block_size) as u16;
                    ctx.nvme_write(
                        *device,
                        lba,
                        num_blocks,
                        req.buffer_ptr as u64, // addr
                        req.buffer_len,
                    )
                }
                DiskBackend::DirectIo { file, .. } => unsafe {
                    ctx.direct_io_write(
                        *file,
                        req.disk_offset,
                        req.buffer_ptr,
                        req.buffer_len,
                    )
                },
            };

            match result {
                Ok(seq) => {
                    disk_io.store_pending_flush(
                        seq,
                        crate::disk_io::PendingFlush {
                            segment_id: req.segment_id,
                        },
                    );
                }
                Err(e) => {
                    DISK_FLUSH_ERRORS.increment();
                    tracing::warn!("Disk flush submit failed: {e}");
                    // Detach write buffer anyway to avoid permanent leak
                    self.cache.complete_flush(req.segment_id);
                }
            }
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

/// Send ALL pending response data for a connection using scatter-gather.
///
/// Loops to submit ALL pending batches. Krio's per-connection send queue
/// ensures at most one SQE is in-flight at a time — excess sends are queued
/// internally and submitted immediately from the CQE completion handler,
/// with zero gap between consecutive sends.
///
/// Returns `Err(())` on fatal send error.
#[inline]
fn send_pending(
    ctx: &mut DriverCtx,
    conn: &mut Connection,
    token: ConnToken,
    slot_size: usize,
) -> Result<(), ()> {
    loop {
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
                    continue;
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
                builder = builder.guard(GuardBox::new(BytesGuard(part.clone())));
                guard_count += 1;
            } else if part.len() <= copy_budget {
                builder = builder.copy(part);
                copy_budget -= part.len();
            } else {
                break;
            }

            total_advance += part.len();
        }

        if total_advance == 0 {
            // Pool/slab exhausted — stop, on_send_complete will retry.
            return Ok(());
        }

        match builder.submit() {
            Ok(()) => {
                conn.advance_write(total_advance);
                // Loop to submit more batches — krio queues if in-flight.
            }
            Err(e) if e.kind() == io::ErrorKind::Other => {
                // Send pool or slab exhausted — stop, wait for on_send_complete.
                return Ok(());
            }
            Err(_) => return Err(()),
        }
    }
}
