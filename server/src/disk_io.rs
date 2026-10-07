//! Per-worker disk I/O state for io_uring-based disk reads and flushes.
//!
//! This module provides the server-side orchestration for disk-backed cache
//! operations. The cache layer ([`cache_core::disk::IoUringDiskLayer`]) manages segment metadata
//! and decides what to read; this module handles the actual I/O submission
//! and completion via ringline's NVMe or Direct I/O APIs.

use cache_core::disk::{AlignedBuffer, AlignedBufferPool, DiskReadParams};
use ringline::{ConnToken, DirectIoFile, NvmeDevice};

/// Block size of the io_uring disk tier, shared by the cache's
/// `IoUringDiskTierConfig` and the server's `DiskIoWorkerConfig`. Read buffers
/// are sized from it, so the two must agree.
pub const DISK_BLOCK_SIZE: u32 = 4096;

/// Read buffer size for a disk read of one block at an item's offset.
///
/// Items are 512-byte aligned, not block aligned, so a block-sized read
/// starting at an item spans two blocks unless the item starts on a block
/// boundary. See `IoUringPool::item_disk_range`.
pub(crate) const fn read_buffer_size(block_size: u32) -> usize {
    2 * block_size as usize
}

/// Configuration for per-worker disk I/O initialization.
pub(crate) struct DiskIoWorkerConfig {
    /// Backend type.
    pub backend: cache_core::DiskIoBackend,
    /// Path to the disk file (used by DirectIo backend).
    pub path: String,
    /// Number of read buffers per worker.
    pub read_buffer_count: usize,
    /// Size of each read buffer; see [`read_buffer_size`].
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

/// Test helper: the slice [`value_range_from_disk_read`] names.
#[cfg(test)]
pub(crate) fn value_from_disk_read<'a>(
    buf: &'a [u8],
    item_offset: usize,
    key: &[u8],
) -> Option<&'a [u8]> {
    value_range_from_disk_read(buf, item_offset, key).map(|range| &buf[range])
}

/// The byte range of the value a completed disk read holds for `key`, or
/// `None` to answer a miss.
///
/// `item_offset` is where the item starts within `buf`, the bytes the read
/// returned. A deleted item, one with an invalid header, one that does not
/// fit in `buf`, or one stored under a different key is a miss.
///
/// The key check is the only comparison of the key. A committed disk
/// segment's keys are only on disk, so the hashtable matched this item by
/// its 12-bit tag alone; another key's item reaches here whenever tags
/// collide. Serving it would answer with another key's value.
pub(crate) fn value_range_from_disk_read(
    buf: &[u8],
    item_offset: usize,
    key: &[u8],
) -> Option<std::ops::Range<usize>> {
    let header = header_from_disk_read(buf, item_offset)?;
    let key_start = item_offset + cache_core::BasicHeader::SIZE + header.optional_len() as usize;
    let value_start = key_start + header.key_len() as usize;
    let value_end = value_start + header.value_len() as usize;
    if buf.get(key_start..value_start)? != key || value_end > buf.len() {
        return None;
    }
    Some(value_start..value_end)
}

/// Whether the item at `item_offset` in `buf` is live and stored under
/// `key`. The header, optional data and key must be in `buf`; the value
/// need not be.
pub(crate) fn key_matches(buf: &[u8], item_offset: usize, key: &[u8]) -> bool {
    let Some(header) = header_from_disk_read(buf, item_offset) else {
        return false;
    };
    let key_start = item_offset + cache_core::BasicHeader::SIZE + header.optional_len() as usize;
    buf.get(key_start..key_start + header.key_len() as usize) == Some(key)
}

/// The length of the item starting at `item_offset` -- header, optional
/// data, key and value -- read from its header, which must be in `buf`.
/// `None` if the header is not in `buf`, is invalid, or marks the item
/// deleted.
pub(crate) fn item_len_from_disk_read(buf: &[u8], item_offset: usize) -> Option<usize> {
    let header = header_from_disk_read(buf, item_offset)?;
    Some(
        cache_core::BasicHeader::SIZE
            + header.optional_len() as usize
            + header.key_len() as usize
            + header.value_len() as usize,
    )
}

/// The live item header at `item_offset` in `buf`, or `None` if it is not
/// in `buf`, is invalid, or marks the item deleted.
fn header_from_disk_read(buf: &[u8], item_offset: usize) -> Option<cache_core::BasicHeader> {
    let header_size = cache_core::BasicHeader::SIZE;
    if item_offset + header_size > buf.len() {
        return None;
    }

    // Decoded from a private copy. `BasicHeader::try_from_ptr` reads the
    // flags byte through an atomic view, which needs provenance permitting
    // writes, and a `&[u8]` cannot give that. `buf` is a completed read no
    // other thread touches, so copying the header out is sound and keeps the
    // atomic view off a read-only reference.
    let mut header_bytes = [0u8; cache_core::BasicHeader::SIZE];
    header_bytes.copy_from_slice(&buf[item_offset..item_offset + header_size]);
    // SAFETY: `header_bytes` is `SIZE` writable bytes.
    let header = unsafe { cache_core::BasicHeader::try_from_ptr(header_bytes.as_mut_ptr()) }?;
    (!header.is_deleted()).then_some(header)
}

/// A block-aligned heap buffer for a disk read of an item that runs past
/// the pooled read buffer.
pub(crate) struct LargeReadBuffer {
    ptr: std::ptr::NonNull<u8>,
    layout: std::alloc::Layout,
}

// SAFETY: the buffer is plain memory owned by this value.
unsafe impl Send for LargeReadBuffer {}
unsafe impl Sync for LargeReadBuffer {}

impl LargeReadBuffer {
    /// A zeroed buffer of `len` bytes aligned to `align`, or `None` if the
    /// allocation fails or the layout is invalid.
    pub(crate) fn new(len: usize, align: usize) -> Option<Self> {
        let layout = std::alloc::Layout::from_size_align(len.max(1), align).ok()?;
        // SAFETY: the layout has a nonzero size.
        let ptr = std::ptr::NonNull::new(unsafe { std::alloc::alloc_zeroed(layout) })?;
        Some(Self { ptr, layout })
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` names `layout.size()` initialized (zeroed, then
        // read into) bytes owned by this value.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.layout.size()) }
    }
}

impl Drop for LargeReadBuffer {
    fn drop(&mut self) {
        // SAFETY: allocated in `new` with this layout.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

/// A value read from disk into its own buffer. Held until the last `Bytes`
/// built from it by `Bytes::from_owner` is dropped, so a zero-copy send
/// reads straight from the read buffer.
pub(crate) struct DiskReadValue {
    buffer: LargeReadBuffer,
    range: std::ops::Range<usize>,
}

impl DiskReadValue {
    /// The value at `range` in `buffer`.
    pub(crate) fn new(buffer: LargeReadBuffer, range: std::ops::Range<usize>) -> Self {
        debug_assert!(range.end <= buffer.as_slice().len());
        Self { buffer, range }
    }
}

impl AsRef<[u8]> for DiskReadValue {
    fn as_ref(&self) -> &[u8] {
        &self.buffer.as_slice()[self.range.clone()]
    }
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
mod read_buffer_size_tests {
    use super::read_buffer_size;
    use cache_core::disk::IoUringPool;

    /// Every read `TieredCache::lookup` asks for -- one block from an item's
    /// offset -- fits a read buffer, wherever the item sits in its segment.
    /// A one-block buffer is too small for 7 of every 8 item offsets.
    #[test]
    fn every_block_read_fits_a_read_buffer() {
        let block = 4096;
        let segment_size = 1024 * 1024;
        let pool = IoUringPool::new(0, 2, segment_size, block);
        for segment_id in 0..2 {
            for offset in (0..segment_size as u32).step_by(512) {
                let (_, read_len, within) = pool.item_disk_range(segment_id, offset, block);
                assert!(
                    read_len as usize <= read_buffer_size(block),
                    "offset {offset}: read of {read_len} bytes into a {}-byte buffer",
                    read_buffer_size(block)
                );
                assert!(within < read_len, "offset {offset}: item outside the read");
            }
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

    /// A disk read can return a different key's item (a key-hash collision,
    /// or a flush that failed). Serving its value answers the request with
    /// another key's data.
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
    /// The first block of a large item gives the item's whole length, so the
    /// reader knows how much more to read; the value itself is not in the
    /// first block and reads as absent there.
    #[test]
    fn an_item_longer_than_the_read_reports_its_length() {
        use super::{item_len_from_disk_read, value_range_from_disk_read};

        let value = vec![b'v'; 10_000];
        let whole = item(b"key-a", &value);
        let first_block = &whole[..4096];

        assert_eq!(
            item_len_from_disk_read(first_block, 0),
            Some(BasicHeader::SIZE + 5 + 10_000)
        );
        assert_eq!(value_range_from_disk_read(first_block, 0, b"key-a"), None);
        assert_eq!(
            value_range_from_disk_read(&whole, 0, b"key-a"),
            Some(BasicHeader::SIZE + 5..whole.len())
        );
    }

    /// The header, optional data and key decide a key match; the value need
    /// not have been read.
    #[test]
    fn a_key_matches_without_its_value() {
        let item = item(b"key-a", &[b'v'; 64]);
        let header_and_key = BasicHeader::SIZE + 5;
        assert!(super::key_matches(&item[..header_and_key], 0, b"key-a"));
        assert!(!super::key_matches(&item[..header_and_key], 0, b"key-b"));
        assert!(!super::key_matches(
            &item[..header_and_key - 1],
            0,
            b"key-a"
        ));
    }

    /// A large-read value is its range of the buffer.
    #[test]
    fn a_disk_read_value_is_its_range_of_the_buffer() {
        let item = item(b"key-a", b"value-a");
        let mut buffer = super::LargeReadBuffer::new(4096, 4096).expect("buffer");
        // SAFETY: the buffer holds 4096 bytes.
        unsafe { std::ptr::copy_nonoverlapping(item.as_ptr(), buffer.as_mut_ptr(), item.len()) };
        let range = super::value_range_from_disk_read(buffer.as_slice(), 0, b"key-a").unwrap();
        let bytes = bytes::Bytes::from_owner(super::DiskReadValue::new(buffer, range));
        assert_eq!(&bytes[..], b"value-a");
    }
}
