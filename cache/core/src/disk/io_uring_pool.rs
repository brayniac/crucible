//! Pool managing disk segment metadata for io_uring-based disk I/O.
//!
//! [`IoUringPool`] holds an array of [`DiskSegmentMeta`] entries — the
//! in-RAM metadata for segments whose data lives on disk. It manages
//! segment allocation via a lock-free free queue, and provides disk
//! offset calculations for reading/writing segment data.

use crate::pool::RamPool;
use crate::segment::Segment;

use super::disk_segment_meta::DiskSegmentMeta;

/// Pool of disk segment metadata entries.
///
/// Each entry tracks the state machine, chain pointers, and statistics
/// for a disk-backed segment. The actual segment data resides on disk
/// (or in a temporary write buffer attached to the metadata).
///
/// # Disk Layout
///
/// Segments are laid out sequentially on the device/file:
/// ```text
/// [Segment 0: segment_size bytes]
/// [Segment 1: segment_size bytes]
/// ...
/// [Segment N-1: segment_size bytes]
/// ```
///
/// The byte offset of segment `i` is `i * segment_size`.
pub struct IoUringPool {
    /// In-RAM metadata for each disk segment.
    segments: Vec<DiskSegmentMeta>,
    /// Lock-free free queue for segment allocation.
    free_queue: Box<crossbeam_deque::Injector<u32>>,
    /// Spare queue for segments that need special handling.
    #[allow(dead_code)]
    spare_queue: Box<crossbeam_deque::Injector<u32>>,
    /// Pool ID (0-3).
    pool_id: u8,
    /// Size of each segment in bytes.
    segment_size: usize,
    /// I/O block size (typically 4096).
    block_size: u32,
    /// Total number of segments.
    segment_count: usize,
}

// SAFETY: All internal state is either immutable after construction
// (segments vec, pool_id, sizes) or thread-safe (Injector queues,
// atomic fields in DiskSegmentMeta).
unsafe impl Send for IoUringPool {}
unsafe impl Sync for IoUringPool {}

impl IoUringPool {
    /// Create a new io_uring pool.
    ///
    /// # Parameters
    /// - `pool_id`: Pool ID (0-3)
    /// - `segment_count`: Number of segments
    /// - `segment_size`: Size of each segment in bytes
    /// - `block_size`: I/O block size (typically 4096)
    pub fn new(pool_id: u8, segment_count: usize, segment_size: usize, block_size: u32) -> Self {
        assert!(pool_id <= 3, "pool_id must be 0-3");
        assert!(segment_count > 0, "segment_count must be > 0");
        assert!(segment_size > 0, "segment_size must be > 0");
        assert!(
            block_size > 0 && block_size.is_power_of_two(),
            "block_size must be a power of two"
        );

        let free_queue = Box::new(crossbeam_deque::Injector::new());
        let spare_queue = Box::new(crossbeam_deque::Injector::new());

        let free_queue_ptr = &*free_queue as *const crossbeam_deque::Injector<u32>;

        let mut segments = Vec::with_capacity(segment_count);
        for i in 0..segment_count {
            let disk_offset = i as u64 * segment_size as u64;
            let meta = DiskSegmentMeta::new(
                pool_id,
                i as u32,
                segment_size as u32,
                disk_offset,
                free_queue_ptr,
            );
            segments.push(meta);

            // All segments start as free
            free_queue.push(i as u32);
        }

        Self {
            segments,
            free_queue,
            spare_queue,
            pool_id,
            segment_size,
            block_size,
            segment_count,
        }
    }

    /// Get a reference to a disk segment metadata entry.
    #[inline]
    pub fn get_meta(&self, id: u32) -> Option<&DiskSegmentMeta> {
        self.segments.get(id as usize)
    }

    /// Get the byte offset of a segment on the device/file.
    #[inline]
    pub fn segment_disk_offset(&self, id: u32) -> u64 {
        id as u64 * self.segment_size as u64
    }

    /// Compute the block-aligned disk read range for an item.
    ///
    /// Given an item's location within a segment, returns the byte range
    /// (offset, length) to read from disk, aligned to block boundaries.
    ///
    /// # Parameters
    /// - `segment_id`: The segment containing the item
    /// - `item_offset`: The item's byte offset within the segment
    /// - `read_size`: How many bytes to read (typically block_size for first read)
    ///
    /// # Returns
    /// `(disk_offset, aligned_len, offset_within_block)` where:
    /// - `disk_offset`: Block-aligned byte offset to start reading from
    /// - `aligned_len`: Block-aligned number of bytes to read
    /// - `offset_within_block`: Item's offset within the read buffer
    pub fn item_disk_range(
        &self,
        segment_id: u32,
        item_offset: u32,
        read_size: u32,
    ) -> (u64, u32, u32) {
        let segment_offset = self.segment_disk_offset(segment_id);
        let abs_offset = segment_offset + item_offset as u64;

        let block_mask = self.block_size as u64 - 1;
        let aligned_start = abs_offset & !block_mask;
        let offset_within_block = (abs_offset - aligned_start) as u32;

        // Compute aligned read length covering the requested range
        let end = abs_offset + read_size as u64;
        let aligned_end = (end + block_mask) & !block_mask;
        let aligned_len = (aligned_end - aligned_start) as u32;

        (aligned_start, aligned_len, offset_within_block)
    }

    /// Get the I/O block size.
    #[inline]
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Get the segment size.
    #[inline]
    pub fn segment_size(&self) -> usize {
        self.segment_size
    }

    /// Reset all segments to Free state and rebuild free queue.
    pub fn reset_all(&self) {
        for segment in &self.segments {
            segment.reset();
        }
        // Re-populate free queue
        for i in 0..self.segment_count {
            self.free_queue.push(i as u32);
        }
    }
}

impl RamPool for IoUringPool {
    type Segment = DiskSegmentMeta;

    #[inline]
    fn pool_id(&self) -> u8 {
        self.pool_id
    }

    fn get(&self, id: u32) -> Option<&DiskSegmentMeta> {
        self.segments.get(id as usize)
    }

    #[inline]
    fn segment_count(&self) -> usize {
        self.segment_count
    }

    #[inline]
    fn segment_size(&self) -> usize {
        self.segment_size
    }

    fn reserve(&self) -> Option<u32> {
        loop {
            match self.free_queue.steal() {
                crossbeam_deque::Steal::Success(id) => {
                    let segment = &self.segments[id as usize];
                    if segment.try_reserve() {
                        return Some(id);
                    }
                    // Someone else grabbed it, try again
                }
                crossbeam_deque::Steal::Empty => return None,
                crossbeam_deque::Steal::Retry => continue,
            }
        }
    }

    fn release(&self, id: u32) {
        let segment = &self.segments[id as usize];
        segment.try_release();
        self.free_queue.push(id);
    }

    fn free_count(&self) -> usize {
        // Approximate count by checking segment states
        self.segments
            .iter()
            .filter(|s| s.state() == crate::state::State::Free)
            .count()
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_pool_creation() {
        let pool = IoUringPool::new(2, 10, 1024 * 1024, 4096);
        assert_eq!(pool.pool_id(), 2);
        assert_eq!(pool.segment_count(), 10);
        assert_eq!(pool.segment_size(), 1024 * 1024);
        assert_eq!(pool.block_size(), 4096);
    }

    #[test]
    fn test_segment_disk_offset() {
        let pool = IoUringPool::new(0, 4, 8 * 1024 * 1024, 4096);

        assert_eq!(pool.segment_disk_offset(0), 0);
        assert_eq!(pool.segment_disk_offset(1), 8 * 1024 * 1024);
        assert_eq!(pool.segment_disk_offset(2), 16 * 1024 * 1024);
    }

    #[test]
    fn test_item_disk_range_aligned() {
        let pool = IoUringPool::new(0, 4, 1024 * 1024, 4096);

        // Item at offset 0 in segment 0 — already aligned
        let (offset, len, within) = pool.item_disk_range(0, 0, 4096);
        assert_eq!(offset, 0);
        assert_eq!(len, 4096);
        assert_eq!(within, 0);
    }

    #[test]
    fn test_item_disk_range_unaligned() {
        let pool = IoUringPool::new(0, 4, 1024 * 1024, 4096);

        // Item at offset 100 in segment 0 — needs alignment
        let (offset, len, within) = pool.item_disk_range(0, 100, 4096);
        assert_eq!(offset, 0); // Aligned down
        assert_eq!(within, 100); // 100 bytes into the block
        assert_eq!(len, 8192); // Need 2 blocks to cover 100..4196
    }

    #[test]
    fn test_reserve_release() {
        let pool = IoUringPool::new(0, 3, 4096, 4096);
        assert_eq!(pool.free_count(), 3);

        let id1 = pool.reserve().unwrap();
        let id2 = pool.reserve().unwrap();
        let id3 = pool.reserve().unwrap();
        assert!(pool.reserve().is_none()); // Exhausted

        pool.release(id1);
        let id4 = pool.reserve().unwrap();
        assert_eq!(id4, id1);

        pool.release(id2);
        pool.release(id3);
        pool.release(id4);
    }

    #[test]
    fn test_get_meta() {
        let pool = IoUringPool::new(1, 5, 1024 * 1024, 4096);

        let meta = pool.get_meta(0).unwrap();
        assert_eq!(meta.id(), 0);
        assert_eq!(meta.pool_id(), 1);
        assert_eq!(meta.disk_offset(), 0);

        let meta = pool.get_meta(4).unwrap();
        assert_eq!(meta.id(), 4);
        assert_eq!(meta.disk_offset(), 4 * 1024 * 1024);

        assert!(pool.get_meta(5).is_none());
    }
}
