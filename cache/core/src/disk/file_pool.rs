//! File-backed segment pool using memory-mapped I/O.
//!
//! [`FilePool`] provides a segment pool similar to [`MemoryPool`] but backed
//! by a file on disk using mmap. This enables caches larger than available RAM.

use crate::disk::config::{DiskConfig, SyncMode};
use crate::disk::file_segment::FileSegment;
use crate::pool::RamPool;
use crate::segment::Segment;
use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// Magic bytes identifying a Crucible disk cache file.
pub const FILE_MAGIC: [u8; 8] = *b"CRUCIBLE";

/// Current file format version.
pub const FILE_VERSION: u32 = 1;

/// Header size (64 bytes, cache line aligned).
pub const HEADER_SIZE: usize = 64;

/// Header stored at the beginning of the disk cache file.
///
/// The header contains metadata needed to validate and recover the cache.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FilePoolHeader {
    /// Magic bytes: "CRUCIBLE"
    pub magic: [u8; 8],
    /// File format version
    pub version: u32,
    /// Size of each segment in bytes
    pub segment_size: u32,
    /// Number of segments in the pool
    pub segment_count: u32,
    /// Whether segments use per-item TTL (1) or segment-level TTL (0)
    pub per_item_ttl: u8,
    /// Pool ID (0-3)
    pub pool_id: u8,
    /// Reserved for future use
    pub _reserved: [u8; 42],
}

impl FilePoolHeader {
    /// Create a new header with the given parameters.
    pub fn new(pool_id: u8, per_item_ttl: bool, segment_size: u32, segment_count: u32) -> Self {
        Self {
            magic: FILE_MAGIC,
            version: FILE_VERSION,
            segment_size,
            segment_count,
            per_item_ttl: per_item_ttl as u8,
            pool_id,
            _reserved: [0u8; 42],
        }
    }

    /// Validate that the header is correct.
    pub fn validate(&self) -> io::Result<()> {
        if self.magic != FILE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid magic bytes in disk cache file",
            ));
        }
        if self.version != FILE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Unsupported file version {} (expected {})",
                    self.version, FILE_VERSION
                ),
            ));
        }
        Ok(())
    }

    /// Read a header from bytes.
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "File too small to contain header",
            ));
        }

        // Safety: We're reading into a repr(C) struct with known layout
        let header = unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const FilePoolHeader) };
        header.validate()?;
        Ok(header)
    }

    /// Write the header to bytes.
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        // Safety: We're writing from a repr(C) struct with known layout
        unsafe {
            std::ptr::copy_nonoverlapping(
                self as *const FilePoolHeader as *const u8,
                bytes.as_mut_ptr(),
                std::mem::size_of::<FilePoolHeader>(),
            );
        }
        bytes
    }
}

/// A pool of segments backed by a memory-mapped file.
///
/// FilePool provides the same interface as [`MemoryPool`] but stores
/// segments in a file on disk. This enables cache sizes larger than
/// available RAM.
///
/// # File Layout
///
/// ```text
/// +------------------+
/// | FilePoolHeader   |  64 bytes
/// +------------------+
/// | Segment 0        |  segment_size bytes
/// | Segment 1        |  segment_size bytes
/// | ...              |
/// | Segment N-1      |  segment_size bytes
/// +------------------+
/// ```
///
/// # Thread Safety
///
/// FilePool is `Send + Sync`. All operations use lock-free atomics.
pub struct FilePool {
    /// The underlying file handle.
    file: File,

    /// Memory-mapped view of the file.
    mmap: MmapMut,

    /// Segment metadata.
    segments: Vec<FileSegment<'static>>,

    /// Lock-free free list.
    free_queue: crossbeam_deque::Injector<u32>,

    /// Pool ID (0-3).
    pool_id: u8,

    /// Whether this pool uses per-item TTL.
    is_per_item_ttl: bool,

    /// Size of each segment in bytes.
    segment_size: usize,

    /// Synchronization mode.
    sync_mode: SyncMode,
}

// SAFETY: FilePool is safe to send/share between threads because:
// 1. mmap memory is allocated once and never moves until Drop
// 2. All segment access uses atomic operations
// 3. free_queue (Injector) is already Send + Sync
unsafe impl Send for FilePool {}
unsafe impl Sync for FilePool {}

impl FilePool {
    /// Get the sync mode for this pool.
    pub fn sync_mode(&self) -> SyncMode {
        self.sync_mode
    }

    /// Check if this pool uses per-item TTL.
    #[inline]
    pub fn is_per_item_ttl(&self) -> bool {
        self.is_per_item_ttl
    }

    /// Flush dirty segments to disk.
    ///
    /// Returns the number of segments that were synced.
    pub fn sync(&self) -> io::Result<usize> {
        let mut synced = 0;
        for segment in &self.segments {
            if segment.is_dirty() {
                segment.clear_dirty();
                synced += 1;
            }
        }
        if synced > 0 {
            self.mmap.flush()?;
        }
        Ok(synced)
    }

    /// Asynchronously flush dirty segments to disk.
    ///
    /// Returns immediately after queueing the flush.
    pub fn sync_async(&self) -> io::Result<usize> {
        let mut synced = 0;
        for segment in &self.segments {
            if segment.is_dirty() {
                segment.clear_dirty();
                synced += 1;
            }
        }
        if synced > 0 {
            self.mmap.flush_async()?;
        }
        Ok(synced)
    }

    /// Get the underlying file handle.
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Return an iterator over all segment IDs.
    pub fn segment_ids(&self) -> impl Iterator<Item = u32> {
        0..self.segments.len() as u32
    }

    /// Get the header from the mmap.
    pub fn header(&self) -> FilePoolHeader {
        // Safety: The first HEADER_SIZE bytes contain the header
        FilePoolHeader::from_bytes(&self.mmap[..HEADER_SIZE]).expect("Header should be valid")
    }

    /// Reset all segments to Free state and rebuild the free queue.
    ///
    /// This is used during flush operations to reset the entire pool.
    pub fn reset_all(&self) {
        // Drain the free queue first
        loop {
            match self.free_queue.steal() {
                crossbeam_deque::Steal::Empty => break,
                crossbeam_deque::Steal::Retry => continue,
                crossbeam_deque::Steal::Success(_) => continue,
            }
        }

        // Reset each segment and add it back to the free queue
        for (id, segment) in self.segments.iter().enumerate() {
            segment.force_free();
            self.free_queue.push(id as u32);
        }
    }
}

impl Drop for FilePool {
    fn drop(&mut self) {
        // Sync any remaining dirty data before dropping
        let _ = self.sync();
        // Clear segments before dropping mmap
        self.segments.clear();
    }
}

impl RamPool for FilePool {
    type Segment = FileSegment<'static>;

    fn pool_id(&self) -> u8 {
        self.pool_id
    }

    fn get(&self, id: u32) -> Option<&FileSegment<'static>> {
        self.segments.get(id as usize)
    }

    fn segment_count(&self) -> usize {
        self.segments.len()
    }

    fn segment_size(&self) -> usize {
        self.segment_size
    }

    fn reserve(&self) -> Option<u32> {
        match self.free_queue.steal() {
            crossbeam_deque::Steal::Success(segment_id) => {
                let segment = &self.segments[segment_id as usize];

                // Transition Free -> Reserved
                if !segment.try_reserve() {
                    // Segment not in Free state - push back and return None
                    self.free_queue.push(segment_id);
                    return None;
                }

                Some(segment_id)
            }
            crossbeam_deque::Steal::Empty | crossbeam_deque::Steal::Retry => None,
        }
    }

    fn release(&self, id: u32) {
        let id_usize = id as usize;

        if id_usize >= self.segments.len() {
            panic!("Invalid segment ID: {id}");
        }

        let segment = &self.segments[id_usize];

        // Transition to Free state
        if segment.try_release() {
            self.free_queue.push(id);
        }
    }

    fn free_count(&self) -> usize {
        self.free_queue.len()
    }
}

/// Builder for creating a FilePool.
pub struct FilePoolBuilder {
    pool_id: u8,
    is_per_item_ttl: bool,
    segment_size: usize,
    path: std::path::PathBuf,
    size: usize,
    sync_mode: SyncMode,
    create_new: bool,
}

impl FilePoolBuilder {
    /// Create a new builder with the given pool ID.
    ///
    /// Pool IDs should be in the range 0-3 (2 bits).
    pub fn new(pool_id: u8) -> Self {
        debug_assert!(pool_id <= 3, "pool_id must be 0-3");
        Self {
            pool_id,
            is_per_item_ttl: false,
            segment_size: 1024 * 1024, // 1MB default
            path: std::path::PathBuf::from("cache.dat"),
            size: 1024 * 1024 * 1024, // 1GB default
            sync_mode: SyncMode::default(),
            create_new: true,
        }
    }

    /// Configure segments to use per-item TTL headers.
    pub fn per_item_ttl(mut self, enabled: bool) -> Self {
        self.is_per_item_ttl = enabled;
        self
    }

    /// Set the segment size in bytes.
    pub fn segment_size(mut self, size: usize) -> Self {
        self.segment_size = size;
        self
    }

    /// Set the file path.
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        self.path = path.as_ref().to_path_buf();
        self
    }

    /// Set the total storage size in bytes.
    pub fn size(mut self, size: usize) -> Self {
        self.size = size;
        self
    }

    /// Set the synchronization mode.
    pub fn sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    /// Set whether to create a new file or try to open an existing one.
    pub fn create_new(mut self, create: bool) -> Self {
        self.create_new = create;
        self
    }

    /// Build from disk config.
    pub fn from_config(pool_id: u8, config: &DiskConfig, default_segment_size: usize) -> Self {
        let segment_size = config.segment_size.unwrap_or(default_segment_size);
        Self {
            pool_id,
            is_per_item_ttl: false,
            segment_size,
            path: config.path.clone(),
            size: config.size,
            sync_mode: config.sync_mode,
            create_new: !config.recover_on_startup,
        }
    }

    /// Build the file pool.
    pub fn build(self) -> io::Result<FilePool> {
        let num_segments = self.size / self.segment_size;
        if num_segments == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "size must be >= segment_size",
            ));
        }

        let total_file_size = HEADER_SIZE + num_segments * self.segment_size;

        // Create parent directories if needed
        if let Some(parent) = self.path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)?;
        }

        // Open or create the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)?;

        // Check if this is an existing file we should try to recover
        let file_len = file.metadata()?.len() as usize;
        let is_new = file_len == 0 || self.create_new;

        if is_new {
            // Set the file size
            file.set_len(total_file_size as u64)?;
        } else if file_len != total_file_size {
            // Existing file has different size - recreate
            file.set_len(total_file_size as u64)?;
        }

        // Memory-map the file
        let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        if is_new {
            // Write the header
            let header = FilePoolHeader::new(
                self.pool_id,
                self.is_per_item_ttl,
                self.segment_size as u32,
                num_segments as u32,
            );
            let header_bytes = header.to_bytes();
            mmap[..HEADER_SIZE].copy_from_slice(&header_bytes);
        } else {
            // Validate existing header
            let header = FilePoolHeader::from_bytes(&mmap[..HEADER_SIZE])?;

            // Check configuration matches
            if header.segment_size != self.segment_size as u32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Segment size mismatch: file has {} but config has {}",
                        header.segment_size, self.segment_size
                    ),
                ));
            }
            if header.segment_count != num_segments as u32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Segment count mismatch: file has {} but config has {}",
                        header.segment_count, num_segments
                    ),
                ));
            }
        }

        // Initialize segments
        let mut segments = Vec::with_capacity(num_segments);
        let free_queue = crossbeam_deque::Injector::new();

        for id in 0..num_segments {
            let offset = HEADER_SIZE + id * self.segment_size;
            let segment_ptr = unsafe { mmap.as_mut_ptr().add(offset) };

            let segment = unsafe {
                FileSegment::new(
                    self.pool_id,
                    self.is_per_item_ttl,
                    id as u32,
                    segment_ptr,
                    self.segment_size,
                )
            };

            segments.push(segment);

            // Only add to free queue if this is a new file
            // For recovery, we'll rebuild the free list after scanning
            if is_new {
                free_queue.push(id as u32);
            }
        }

        Ok(FilePool {
            file,
            mmap,
            segments,
            free_queue,
            pool_id: self.pool_id,
            is_per_item_ttl: self.is_per_item_ttl,
            segment_size: self.segment_size,
            sync_mode: self.sync_mode,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::State;
    use tempfile::tempdir;

    fn create_test_pool() -> (tempfile::TempDir, FilePool) {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_cache.dat");

        let pool = FilePoolBuilder::new(2)
            .segment_size(64 * 1024) // 64KB
            .path(&path)
            .size(640 * 1024) // 640KB = 10 segments
            .build()
            .expect("Failed to create test pool");

        (dir, pool)
    }

    #[test]
    fn test_pool_creation() {
        let (_dir, pool) = create_test_pool();
        assert_eq!(pool.segment_count(), 10);
        assert_eq!(pool.segment_size(), 64 * 1024);
        assert_eq!(pool.pool_id(), 2);
    }

    #[test]
    fn test_header_roundtrip() {
        let header = FilePoolHeader::new(2, false, 1024 * 1024, 100);
        let bytes = header.to_bytes();
        let recovered = FilePoolHeader::from_bytes(&bytes).expect("Should parse");

        assert_eq!(recovered.magic, FILE_MAGIC);
        assert_eq!(recovered.version, FILE_VERSION);
        assert_eq!(recovered.pool_id, 2);
        assert_eq!(recovered.segment_size, 1024 * 1024);
        assert_eq!(recovered.segment_count, 100);
        assert_eq!(recovered.per_item_ttl, 0);
    }

    #[test]
    fn test_reserve_and_release() {
        let (_dir, pool) = create_test_pool();

        // Reserve all segments
        let mut reserved = Vec::new();
        for _ in 0..10 {
            let id = pool.reserve();
            assert!(id.is_some());
            reserved.push(id.unwrap());
        }

        // No more segments available
        assert!(pool.reserve().is_none());

        // Release one
        pool.release(reserved[0]);

        // Can reserve again
        let id = pool.reserve();
        assert!(id.is_some());
    }

    #[test]
    fn test_segment_state() {
        let (_dir, pool) = create_test_pool();

        let id = pool.reserve().unwrap();
        let segment = pool.get(id).unwrap();

        assert_eq!(segment.state(), State::Reserved);
        assert_eq!(segment.id(), id);
        assert_eq!(segment.pool_id(), 2);

        pool.release(id);
        assert_eq!(segment.state(), State::Free);
    }

    #[test]
    fn test_sync() {
        let (_dir, pool) = create_test_pool();

        let id = pool.reserve().unwrap();
        let segment = pool.get(id).unwrap();

        // Segment should be dirty after reservation
        assert!(segment.is_dirty());

        // Sync should clear dirty flag
        let synced = pool.sync().expect("Sync should succeed");
        assert_eq!(synced, 1);
        assert!(!segment.is_dirty());
    }

    #[test]
    fn test_file_persistence() {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("persist_test.dat");

        // Create and write to pool
        {
            let pool = FilePoolBuilder::new(2)
                .segment_size(64 * 1024)
                .path(&path)
                .size(128 * 1024) // 2 segments
                .create_new(true)
                .build()
                .expect("Failed to create pool");

            let id = pool.reserve().unwrap();
            let segment = pool.get(id).unwrap();

            // Transition to Live and write data
            segment.cas_metadata(State::Reserved, State::Live, None, None);
            segment.append_item(b"key", b"value", b"");

            pool.sync().expect("Sync should succeed");
        }

        // Reopen and verify header
        {
            let pool = FilePoolBuilder::new(2)
                .segment_size(64 * 1024)
                .path(&path)
                .size(128 * 1024)
                .create_new(false)
                .build()
                .expect("Failed to reopen pool");

            let header = pool.header();
            assert_eq!(header.pool_id, 2);
            assert_eq!(header.segment_size, 64 * 1024);
            assert_eq!(header.segment_count, 2);
        }
    }

    #[test]
    fn test_reset_all() {
        let (_dir, pool) = create_test_pool();

        // Reserve some segments
        let id1 = pool.reserve().unwrap();
        let id2 = pool.reserve().unwrap();

        assert_eq!(pool.free_count(), 8);

        // Reset all
        pool.reset_all();

        // All segments should be free
        assert_eq!(pool.free_count(), 10);
        assert_eq!(pool.get(id1).unwrap().state(), State::Free);
        assert_eq!(pool.get(id2).unwrap().state(), State::Free);
    }
}
