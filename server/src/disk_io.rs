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

/// The value a completed disk read holds for `key`, or `None` to answer a
/// miss.
///
/// `item_offset` is where the item starts within `buf`, the bytes the read
/// returned. A deleted item, one that does not fit in `buf`, or one stored
/// under a different key is a miss.
///
/// The key check is the one the index could not make. A committed disk
/// segment has no bytes in memory, so the hashtable matched this item on its
/// 12-bit tag alone; about one lookup in 4,096 per occupied slot names some
/// other key's item. Serving it would answer with another key's value.
pub(crate) fn value_from_disk_read<'a>(
    buf: &'a [u8],
    item_offset: usize,
    key: &[u8],
) -> Option<&'a [u8]> {
    let header_size = cache_core::BasicHeader::SIZE;
    if item_offset + header_size > buf.len() {
        return None;
    }

    // Decoded from a private copy. `BasicHeader::from_ptr` reads the flags
    // byte through an atomic view, which needs provenance permitting writes,
    // and a `&[u8]` cannot give that. `buf` is a completed read no other
    // thread touches, so copying the header out is sound and keeps the atomic
    // view off a read-only reference.
    let mut header_bytes = [0u8; cache_core::BasicHeader::SIZE];
    header_bytes.copy_from_slice(&buf[item_offset..item_offset + header_size]);
    let header = unsafe { cache_core::BasicHeader::from_ptr(header_bytes.as_mut_ptr()) };
    if header.is_deleted() {
        return None;
    }

    let key_start = item_offset + header_size + header.optional_len() as usize;
    let value_start = key_start + header.key_len() as usize;
    let value_end = value_start + header.value_len() as usize;
    if buf.get(key_start..value_start)? != key {
        return None;
    }
    buf.get(value_start..value_end)
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

#[cfg(test)]
mod value_from_disk_read_tests {
    use super::value_from_disk_read;
    use cache_core::BasicHeader;

    /// An item as a disk segment holds it: header, key, value.
    fn item(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut buf = vec![0u8; BasicHeader::SIZE];
        BasicHeader::new(key.len() as u8, 0, value.len() as u32).to_bytes(&mut buf);
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        buf
    }

    #[test]
    fn the_requested_key_reads_its_value() {
        let buf = item(b"key-a", b"value-a");
        assert_eq!(
            value_from_disk_read(&buf, 0, b"key-a"),
            Some(&b"value-a"[..])
        );

        let mut padded = vec![0u8; 512];
        padded.extend_from_slice(&buf);
        assert_eq!(
            value_from_disk_read(&padded, 512, b"key-a"),
            Some(&b"value-a"[..])
        );
    }

    /// The index matches a committed disk item on a 12-bit tag alone, so a
    /// read can return a different key's item. Serving its value answers the
    /// request with another key's data.
    #[test]
    fn another_keys_item_is_a_miss() {
        let buf = item(b"key-a", b"value-a");
        assert_eq!(value_from_disk_read(&buf, 0, b"key-b"), None, "same length");
        assert_eq!(
            value_from_disk_read(&buf, 0, b"other-key"),
            None,
            "different length"
        );
    }
}
