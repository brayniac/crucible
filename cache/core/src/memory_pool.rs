//! In-memory pool backed by hugepage allocation.
//!
//! [`MemoryPool`] manages a collection of [`SliceSegment`]s backed by a
//! contiguous memory region, optionally using hugepages for better TLB
//! performance.
//!
//! # Two-Queue Design
//!
//! The pool maintains two separate free queues:
//! - **free_queue**: For normal segment allocation
//! - **spare_queue**: Reserved for compaction operations
//!
//! This separation ensures compaction always has segments available,
//! even under high memory pressure from normal allocation.

use crate::hugepage::{HugepageAllocation, HugepageSize, allocate_on_node};
use crate::pool::RamPool;
use crate::segment::Segment;
use crate::slice_segment::SliceSegment;
use crate::sync::{AtomicU32, Ordering};

/// A pool of segments backed by in-memory allocation.
///
/// The pool allocates a contiguous memory region at construction time
/// and partitions it into fixed-size segments. Two lock-free free lists
/// track available segments: one for normal allocation, one reserved
/// for compaction.
///
/// # Thread Safety
///
/// MemoryPool is `Send + Sync`. All operations use lock-free atomics.
pub struct MemoryPool {
    /// Backing memory allocation (handles deallocation on drop).
    heap: HugepageAllocation,

    /// Segment metadata.
    segments: Vec<SliceSegment<'static>>,

    /// Lock-free free list for normal allocation.
    /// Boxed for stable address.
    free_queue: Box<crossbeam_deque::Injector<u32>>,

    /// Lock-free free list reserved for compaction operations.
    /// Boxed for stable address.
    spare_queue: Box<crossbeam_deque::Injector<u32>>,

    /// Target capacity for spare queue (typically = num_workers).
    spare_capacity: u32,

    /// Current count of segments in spare queue (for fast checking).
    spare_count: AtomicU32,

    /// Pool ID (0-3).
    pool_id: u8,

    /// Whether this pool uses per-item TTL (TtlHeader) or segment-level TTL (BasicHeader).
    is_per_item_ttl: bool,

    /// Size of each segment in bytes.
    segment_size: usize,
}

// SAFETY: MemoryPool is safe to send/share between threads because:
// 1. heap is allocated once and never moved until Drop
// 2. All segment access uses atomic operations
// 3. free_queue and spare_queue (Injector) are already Send + Sync
unsafe impl Send for MemoryPool {}
unsafe impl Sync for MemoryPool {}

impl MemoryPool {
    /// Get the page size that was actually used for the allocation.
    pub fn page_size(&self) -> crate::hugepage::AllocatedPageSize {
        self.heap.page_size()
    }

    /// Check if this pool uses per-item TTL (TtlHeader) or segment-level TTL (BasicHeader).
    #[inline]
    pub fn is_per_item_ttl(&self) -> bool {
        self.is_per_item_ttl
    }

    /// Reserve a segment from the spare queue (for compaction operations).
    ///
    /// This should only be called by compaction code. The spare queue is
    /// reserved specifically for compaction to ensure segments are always
    /// available even under high memory pressure.
    ///
    /// If the spare queue is empty, falls back to the main free queue.
    /// This ensures compaction can proceed even if spare capacity is
    /// temporarily exhausted.
    ///
    /// # Returns
    /// `Some(segment_id)` if a segment was available, `None` otherwise.
    pub fn reserve_spare(&self) -> Option<u32> {
        // Try spare queue first
        match self.spare_queue.steal() {
            crossbeam_deque::Steal::Success(segment_id) => {
                let segment = &self.segments[segment_id as usize];

                // Transition Free -> Reserved
                if !segment.try_reserve() {
                    // Segment not in Free state - push back and retry
                    self.spare_queue.push(segment_id);
                    return self.reserve_spare(); // Retry
                }

                self.spare_count.fetch_sub(1, Ordering::Relaxed);
                return Some(segment_id);
            }
            crossbeam_deque::Steal::Retry => return self.reserve_spare(), // Retry
            crossbeam_deque::Steal::Empty => {}
        }

        // Fallback: try main free queue if spare is empty
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

    /// Release a segment back to the appropriate queue with automatic balancing.
    ///
    /// This method is called by guard Drop when a segment in AwaitingRelease
    /// state has its last reader drop. It automatically replenishes the spare
    /// queue if below target capacity.
    ///
    /// # Safety
    /// The segment must be in a releasable state (Free or AwaitingRelease with
    /// ref_count == 0).
    pub fn release_segment(&self, id: u32) {
        let id_usize = id as usize;

        if id_usize >= self.segments.len() {
            return; // Invalid ID, ignore
        }

        let segment = &self.segments[id_usize];

        // Transition to Free state
        if !segment.try_release() {
            return; // Already free or invalid state
        }

        // Replenish spare queue if below target, else go to main queue
        let spare_count = self.spare_count.load(Ordering::Relaxed);
        if spare_count < self.spare_capacity {
            self.spare_queue.push(id);
            self.spare_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.free_queue.push(id);
        }
    }

    /// Get the current spare queue count.
    #[inline]
    pub fn spare_count(&self) -> u32 {
        self.spare_count.load(Ordering::Relaxed)
    }

    /// Get the configured spare capacity.
    #[inline]
    pub fn spare_capacity(&self) -> u32 {
        self.spare_capacity
    }
}

impl Drop for MemoryPool {
    fn drop(&mut self) {
        // Clear segments first to drop any references to heap memory
        self.segments.clear();
        // HugepageAllocation handles deallocation automatically
    }
}

impl RamPool for MemoryPool {
    type Segment = SliceSegment<'static>;

    fn pool_id(&self) -> u8 {
        self.pool_id
    }

    fn get(&self, id: u32) -> Option<&SliceSegment<'static>> {
        self.segments.get(id as usize)
    }

    fn segment_count(&self) -> usize {
        self.segments.len()
    }

    fn segment_size(&self) -> usize {
        self.segment_size
    }

    fn reserve(&self) -> Option<u32> {
        // Normal allocation only uses free_queue, never spare_queue
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
            // Replenish spare queue if below target, else go to main queue
            let spare_count = self.spare_count.load(Ordering::Relaxed);
            if spare_count < self.spare_capacity {
                self.spare_queue.push(id);
                self.spare_count.fetch_add(1, Ordering::Relaxed);
            } else {
                self.free_queue.push(id);
            }
        }
        // If already Free, don't push again (would create duplicate)
    }

    fn free_count(&self) -> usize {
        // Total free = free_queue + spare_queue
        self.free_queue.len() + self.spare_queue.len()
    }
}

impl MemoryPool {
    /// Reset all segments to Free state and rebuild the free queues.
    ///
    /// This is used during flush operations to reset the entire pool.
    /// All segments are reset to their initial state and distributed
    /// between the free queue and spare queue.
    ///
    /// # Safety
    ///
    /// This should only be called when no concurrent operations are accessing
    /// the segments (e.g., after the hashtable has been cleared).
    pub fn reset_all(&self) {
        // Drain both queues first (discard all current entries)
        loop {
            match self.free_queue.steal() {
                crossbeam_deque::Steal::Empty => break,
                crossbeam_deque::Steal::Retry => continue,
                crossbeam_deque::Steal::Success(_) => continue,
            }
        }
        loop {
            match self.spare_queue.steal() {
                crossbeam_deque::Steal::Empty => break,
                crossbeam_deque::Steal::Retry => continue,
                crossbeam_deque::Steal::Success(_) => continue,
            }
        }

        // Reset spare count
        self.spare_count.store(0, Ordering::Relaxed);

        // Reset each segment and distribute between queues
        for (id, segment) in self.segments.iter().enumerate() {
            segment.force_free();

            // First spare_capacity segments go to spare queue
            if (id as u32) < self.spare_capacity {
                self.spare_queue.push(id as u32);
                self.spare_count.fetch_add(1, Ordering::Relaxed);
            } else {
                self.free_queue.push(id as u32);
            }
        }
    }
}

/// Builder for creating a MemoryPool.
pub struct MemoryPoolBuilder {
    pool_id: u8,
    is_per_item_ttl: bool,
    segment_size: usize,
    heap_size: usize,
    hugepage_size: HugepageSize,
    numa_node: Option<u32>,
    spare_capacity: u32,
}

impl MemoryPoolBuilder {
    /// Create a new builder with the given pool ID.
    ///
    /// Pool IDs should be in the range 0-3 (2 bits).
    pub fn new(pool_id: u8) -> Self {
        debug_assert!(pool_id <= 3, "pool_id must be 0-3");
        Self {
            pool_id,
            is_per_item_ttl: false,
            segment_size: 1024 * 1024,   // 1MB default
            heap_size: 64 * 1024 * 1024, // 64MB default
            hugepage_size: HugepageSize::None,
            numa_node: None,
            spare_capacity: 4, // Default: 4 spare segments for compaction
        }
    }

    /// Set the spare capacity (number of segments reserved for compaction).
    ///
    /// The spare queue is reserved for compaction operations to ensure
    /// segments are always available even under high memory pressure.
    /// Recommended value: number of worker threads.
    ///
    /// Default: 4
    pub fn spare_capacity(mut self, capacity: u32) -> Self {
        self.spare_capacity = capacity;
        self
    }

    /// Configure segments to use per-item TTL headers.
    ///
    /// - `false` (default): Segment-level TTL using `BasicHeader`
    /// - `true`: Per-item TTL using `TtlHeader`
    pub fn per_item_ttl(mut self, enabled: bool) -> Self {
        self.is_per_item_ttl = enabled;
        self
    }

    /// Set the segment size in bytes (default: 1MB).
    pub fn segment_size(mut self, size: usize) -> Self {
        self.segment_size = size;
        self
    }

    /// Set the total heap size in bytes (default: 64MB).
    ///
    /// The number of segments is `heap_size / segment_size`.
    pub fn heap_size(mut self, size: usize) -> Self {
        self.heap_size = size;
        self
    }

    /// Set the hugepage size preference.
    ///
    /// - `HugepageSize::None` - Regular 4KB pages (default)
    /// - `HugepageSize::TwoMegabyte` - Try 2MB hugepages
    /// - `HugepageSize::OneGigabyte` - Try 1GB hugepages
    ///
    /// Falls back to regular pages if hugepages are unavailable.
    pub fn hugepage_size(mut self, size: HugepageSize) -> Self {
        self.hugepage_size = size;
        self
    }

    /// Set the NUMA node to bind memory to (Linux only).
    ///
    /// When set, the allocated memory will be bound to the specified NUMA node
    /// using `mbind()`. This ensures memory locality for CPUs on that node.
    ///
    /// # Example
    /// ```ignore
    /// let pool = MemoryPoolBuilder::new(0)
    ///     .heap_size(1024 * 1024 * 1024)  // 1GB
    ///     .numa_node(0)  // Bind to NUMA node 0
    ///     .build()?;
    /// ```
    pub fn numa_node(mut self, node: u32) -> Self {
        self.numa_node = Some(node);
        self
    }

    /// Build the memory pool.
    pub fn build(self) -> Result<MemoryPool, std::io::Error> {
        let num_segments = self.heap_size / self.segment_size;
        if num_segments == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "heap_size must be >= segment_size",
            ));
        }

        // Ensure spare_capacity doesn't exceed available segments
        let spare_capacity = self.spare_capacity.min(num_segments as u32);

        let actual_size = num_segments * self.segment_size;

        // Allocate backing memory (optionally bound to NUMA node)
        let heap = allocate_on_node(actual_size, self.hugepage_size, self.numa_node)?;

        // Create both queues (boxed for stable address)
        let free_queue = Box::new(crossbeam_deque::Injector::new());
        let spare_queue = Box::new(crossbeam_deque::Injector::new());

        // Get pointer to free_queue for segments (guards will push here on release)
        let free_queue_ptr: *const crossbeam_deque::Injector<u32> = &*free_queue;

        // Initialize segments with pointer to free_queue
        let mut segments = Vec::with_capacity(num_segments);

        for id in 0..num_segments {
            let offset = id * self.segment_size;
            let segment_ptr = unsafe { heap.as_ptr().add(offset) };

            let segment = unsafe {
                SliceSegment::new(
                    self.pool_id,
                    self.is_per_item_ttl,
                    id as u32,
                    segment_ptr,
                    self.segment_size,
                    free_queue_ptr,
                )
            };

            segments.push(segment);

            // Distribute segments between queues
            if (id as u32) < spare_capacity {
                spare_queue.push(id as u32);
            } else {
                free_queue.push(id as u32);
            }
        }

        Ok(MemoryPool {
            heap,
            segments,
            free_queue,
            spare_queue,
            spare_capacity,
            spare_count: AtomicU32::new(spare_capacity),
            pool_id: self.pool_id,
            is_per_item_ttl: self.is_per_item_ttl,
            segment_size: self.segment_size,
        })
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::state::State;

    fn create_test_pool() -> MemoryPool {
        MemoryPoolBuilder::new(0)
            .segment_size(64 * 1024) // 64KB
            .heap_size(640 * 1024) // 640KB = 10 segments
            .spare_capacity(0) // No spare capacity for tests
            .build()
            .expect("Failed to create test pool")
    }

    #[test]
    fn test_pool_creation() {
        let pool = create_test_pool();
        assert_eq!(pool.segment_count(), 10);
        assert_eq!(pool.segment_size(), 64 * 1024);
        assert_eq!(pool.pool_id(), 0);
    }

    #[test]
    fn test_reserve_and_release() {
        let pool = create_test_pool();

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
        let pool = create_test_pool();

        let id = pool.reserve().unwrap();
        let segment = pool.get(id).unwrap();

        assert_eq!(segment.state(), State::Reserved);
        assert_eq!(segment.id(), id);
        assert_eq!(segment.pool_id(), 0);

        pool.release(id);
        assert_eq!(segment.state(), State::Free);
    }

    #[test]
    fn test_per_item_ttl_flag() {
        let pool = MemoryPoolBuilder::new(1)
            .per_item_ttl(true)
            .segment_size(64 * 1024)
            .heap_size(128 * 1024)
            .spare_capacity(0) // No spare capacity for tests
            .build()
            .expect("Failed to create per-item TTL pool");

        let id = pool.reserve().unwrap();
        let segment = pool.get(id).unwrap();

        assert!(segment.is_per_item_ttl());
        assert_eq!(segment.pool_id(), 1);
    }

    #[test]
    fn test_get_invalid_id() {
        let pool = create_test_pool();
        assert!(pool.get(999).is_none());
    }

    #[test]
    #[should_panic(expected = "Invalid segment ID")]
    fn test_release_invalid_id() {
        let pool = create_test_pool();
        pool.release(999);
    }

    #[test]
    fn test_double_release_is_idempotent() {
        let pool = create_test_pool();

        let id = pool.reserve().unwrap();
        pool.release(id);

        // Second release should be a no-op (segment already Free)
        pool.release(id);

        // Should still be able to reserve it
        let id2 = pool.reserve();
        assert!(id2.is_some());
    }

    #[test]
    fn test_segment_append() {
        let pool = create_test_pool();
        let id = pool.reserve().unwrap();
        let segment = pool.get(id).unwrap();

        let offset = segment.append_item(b"key", b"value", b"");
        assert!(offset.is_some());
        assert_eq!(segment.live_items(), 1);
    }
}
