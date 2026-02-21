//! Per-worker disk I/O state for io_uring-based disk reads and flushes.
//!
//! This module provides the server-side orchestration for disk-backed cache
//! operations. The cache layer ([`cache_core::disk::IoUringDiskLayer`]) manages segment metadata
//! and decides what to read; this module handles the actual I/O submission
//! and completion via ringline's NVMe or Direct I/O APIs.

use cache_core::disk::{AlignedBuffer, AlignedBufferPool, DiskReadParams};
use ringline::{ConnToken, DirectIoFile, NvmeDevice};

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

/// Backend for disk I/O operations.
#[derive(Clone, Copy)]
pub enum DiskBackend {
    /// NVMe passthrough via `/dev/ng*` character device.
    Nvme {
        device: NvmeDevice,
        /// NVMe logical block size in bytes (typically 512 or 4096).
        block_size: u32,
    },
    /// `O_DIRECT` via regular file.
    DirectIo {
        file: DirectIoFile,
        /// Filesystem block size (typically 4096).
        block_size: u32,
    },
}

/// State for a pending disk read operation.
pub struct PendingDiskRead {
    /// Connection that initiated this read.
    pub conn: ConnToken,
    /// The allocated read buffer (returned to pool on completion).
    pub buffer: AlignedBuffer,
    /// Parameters from the cache layer.
    pub params: DiskReadParams,
    /// Protocol-specific context for building the response.
    pub response_ctx: DiskReadResponseCtx,
}

/// Protocol-specific context saved when a disk read is initiated.
///
/// Contains enough information to build the correct protocol response
/// when the disk read completes.
pub enum DiskReadResponseCtx {
    /// RESP protocol GET.
    Resp,
    /// Memcache ASCII GET.
    MemcacheAscii {
        /// Key bytes (needed for VALUE response header).
        key: Vec<u8>,
    },
    /// Memcache binary GET/GETK/GETQ/GETKQ.
    MemcacheBinary {
        /// Key bytes (needed for GETK responses).
        key: Vec<u8>,
        /// Original opcode for response.
        opcode: u8,
        /// Opaque value from request header.
        opaque: u32,
        /// Whether this is a quiet command (no response on miss).
        quiet: bool,
    },
}

/// Per-worker disk I/O state.
pub struct DiskIoState {
    /// The I/O backend (NVMe or Direct I/O).
    pub backend: DiskBackend,
    /// Pool of aligned buffers for staging disk reads.
    pub read_buffer_pool: AlignedBufferPool,
    /// Pending read operations, indexed by io_uring sequence number.
    /// Uses a sparse Vec — most slots are None.
    pending_reads: Vec<Option<PendingDiskRead>>,
    /// Pending flush (segment write) operations, indexed by io_uring sequence number.
    pending_flushes: Vec<Option<PendingFlush>>,
}

/// State for a pending segment flush (write to disk).
pub struct PendingFlush {
    /// Segment ID being flushed.
    pub segment_id: u32,
}

impl DiskIoState {
    /// Create new disk I/O state.
    ///
    /// # Parameters
    /// - `backend`: NVMe or Direct I/O backend
    /// - `read_buffer_pool`: Pool of aligned buffers for disk reads
    /// - `max_pending`: Maximum number of concurrent pending operations
    pub fn new(
        backend: DiskBackend,
        read_buffer_pool: AlignedBufferPool,
        max_pending: usize,
    ) -> Self {
        let mut pending_reads = Vec::with_capacity(max_pending);
        pending_reads.resize_with(max_pending, || None);
        let mut pending_flushes = Vec::with_capacity(max_pending);
        pending_flushes.resize_with(max_pending, || None);

        Self {
            backend,
            read_buffer_pool,
            pending_reads,
            pending_flushes,
        }
    }

    /// Store a pending read operation by sequence number.
    pub fn store_pending_read(&mut self, seq: u32, pending: PendingDiskRead) {
        let idx = seq as usize;
        if idx >= self.pending_reads.len() {
            self.pending_reads.resize_with(idx + 1, || None);
        }
        self.pending_reads[idx] = Some(pending);
    }

    /// Take a pending read operation by sequence number.
    pub fn take_pending_read(&mut self, seq: u32) -> Option<PendingDiskRead> {
        let idx = seq as usize;
        self.pending_reads.get_mut(idx)?.take()
    }

    /// Store a pending flush operation by sequence number.
    pub fn store_pending_flush(&mut self, seq: u32, pending: PendingFlush) {
        let idx = seq as usize;
        if idx >= self.pending_flushes.len() {
            self.pending_flushes.resize_with(idx + 1, || None);
        }
        self.pending_flushes[idx] = Some(pending);
    }

    /// Take a pending flush operation by sequence number.
    pub fn take_pending_flush(&mut self, seq: u32) -> Option<PendingFlush> {
        let idx = seq as usize;
        self.pending_flushes.get_mut(idx)?.take()
    }

    /// Return a read buffer to the pool.
    pub fn release_read_buffer(&mut self, buf: AlignedBuffer) {
        self.read_buffer_pool.release(buf);
    }

    /// Get the I/O block size for the backend.
    pub fn block_size(&self) -> u32 {
        match &self.backend {
            DiskBackend::Nvme { block_size, .. } => *block_size,
            DiskBackend::DirectIo { block_size, .. } => *block_size,
        }
    }
}
