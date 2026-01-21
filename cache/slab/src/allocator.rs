//! Slab allocator managing memory pools and slab classes.
//!
//! The allocator coordinates between slab classes, allocating new slabs
//! from the heap when needed and managing eviction when memory is exhausted.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cache_core::{Hashtable, HugepageAllocation, allocate_on_node};
use crossbeam_deque::Injector;

use crate::class::SlabClass;
use crate::config::{EvictionStrategy, HEADER_SIZE, SlabCacheConfig, SlabClasses};
use crate::item::SlabItemHeader;
use crate::location::SlabLocation;
use crate::verifier::SlabVerifier;

/// The main slab allocator.
pub struct SlabAllocator {
    /// Slab classes (indexed by class_id).
    classes: Vec<SlabClass>,
    /// Slab class configuration (slot sizes).
    slab_classes: SlabClasses,
    /// Heap memory allocation.
    heap: HugepageAllocation,
    /// Slab size in bytes.
    slab_size: usize,
    /// Total memory limit.
    memory_limit: usize,
    /// Current memory used (number of slabs allocated * slab_size).
    memory_used: AtomicUsize,
    /// Free slab pages (pointers to unallocated slab memory).
    /// Lock-free: multiple threads can steal slabs concurrently.
    free_slabs: Injector<*mut u8>,
    /// Eviction strategy (twemcache-style).
    eviction_strategy: EvictionStrategy,
}

// Safety: The allocator manages heap memory safely.
unsafe impl Send for SlabAllocator {}
unsafe impl Sync for SlabAllocator {}

impl SlabAllocator {
    /// Create a new slab allocator.
    pub fn new(config: &SlabCacheConfig) -> Result<Self, std::io::Error> {
        // Allocate the heap
        let heap = allocate_on_node(config.heap_size, config.hugepage_size, config.numa_node)?;

        // Generate slab classes from config
        let slab_classes = SlabClasses::from_config(config);

        // Create slab class instances
        let classes: Vec<SlabClass> = slab_classes
            .sizes()
            .iter()
            .enumerate()
            .map(|(i, &slot_size)| SlabClass::new(i as u8, slot_size, config.slab_size))
            .collect();

        // Initialize free slab list
        let free_slabs = Injector::new();
        let slab_count = config.heap_size / config.slab_size;
        let heap_ptr = heap.as_ptr();

        for i in 0..slab_count {
            let slab_ptr = unsafe { heap_ptr.add(i * config.slab_size) };
            free_slabs.push(slab_ptr);
        }

        Ok(Self {
            classes,
            slab_classes,
            heap,
            slab_size: config.slab_size,
            memory_limit: config.heap_size,
            memory_used: AtomicUsize::new(0),
            free_slabs,
            eviction_strategy: config.eviction_strategy,
        })
    }

    /// Select the smallest class that fits an item.
    ///
    /// `item_size` should be `key.len() + value.len() + HEADER_SIZE`.
    #[inline]
    pub fn select_class(&self, item_size: usize) -> Option<u8> {
        self.slab_classes.select_class(item_size)
    }

    /// Get a reference to a slab class.
    #[inline]
    pub fn class(&self, class_id: u8) -> Option<&SlabClass> {
        self.classes.get(class_id as usize)
    }

    /// Allocate a slot for an item.
    ///
    /// Returns `Some((slab_id, slot_index))` if successful.
    /// May allocate a new slab if needed.
    pub fn allocate(&self, class_id: u8) -> Option<(u32, u16)> {
        let class = self.classes.get(class_id as usize)?;

        // Try to allocate from the class's free list
        if let Some(slot) = class.allocate() {
            return Some(slot);
        }

        // Need to allocate a new slab for this class
        self.allocate_slab_for_class(class_id)
    }

    /// Allocate a new slab for a class.
    ///
    /// This is lock-free: multiple threads can concurrently allocate slabs
    /// for different (or even the same) classes. If two threads allocate
    /// slabs for the same class simultaneously, both slabs are added and
    /// used - this is intentional to avoid lock contention.
    fn allocate_slab_for_class(&self, class_id: u8) -> Option<(u32, u16)> {
        let class = self.classes.get(class_id as usize)?;

        // Try to get a free slab from the pool (lock-free steal)
        let slab_ptr = match self.free_slabs.steal() {
            crossbeam_deque::Steal::Success(ptr) => ptr,
            _ => return None, // No free slabs available
        };

        // Update memory usage
        self.memory_used
            .fetch_add(self.slab_size, Ordering::Relaxed);

        // Add the slab to the class (internally synchronized via slabs write lock)
        unsafe {
            class.add_slab(slab_ptr, self.slab_size);
        }

        // Now allocate from the class (lock-free)
        class.allocate()
    }

    /// Deallocate a slot, returning it to the free list.
    pub fn deallocate(&self, class_id: u8, slab_id: u32, slot_index: u16) {
        if let Some(class) = self.classes.get(class_id as usize) {
            class.deallocate(slab_id, slot_index);
        }
    }

    /// Allocate a slot with eviction if needed.
    ///
    /// Tries to allocate a slot. If no free slots or slabs are available,
    /// uses the configured eviction strategy to free up space.
    ///
    /// Eviction strategies are tried in order from highest to lowest bit:
    /// - SLAB_LRC (8) - Evict least recently created slab
    /// - SLAB_LRA (4) - Evict least recently accessed slab
    /// - RANDOM (2) - Evict a random slab
    ///
    /// If no eviction strategy is configured (NONE), returns `None` when full.
    pub fn allocate_with_eviction<H: Hashtable>(
        &self,
        class_id: u8,
        hashtable: &H,
    ) -> Option<(u32, u16)> {
        // First try normal allocation
        if let Some(slot) = self.allocate(class_id) {
            return Some(slot);
        }

        // Check if eviction is disabled
        if self.eviction_strategy.is_none() {
            return None;
        }

        // Try slab-level eviction
        if self.eviction_strategy.has_slab_eviction() {
            if self.try_slab_eviction(hashtable) {
                // Slab evicted - try allocation again
                if let Some(slot) = self.allocate(class_id) {
                    return Some(slot);
                }
            }
        }

        None
    }

    /// Find the least recently accessed slab across all classes.
    ///
    /// Returns `(class_id, slab_id)` of the LRA slab, or `None` if no slabs exist.
    pub fn find_lra_slab(&self) -> Option<(u8, u32)> {
        let mut oldest: Option<(u8, u32, u32)> = None; // (class_id, slab_id, last_accessed)

        for (class_id, class) in self.classes.iter().enumerate() {
            for (slab_id, _created_at, last_accessed) in class.slab_timestamps() {
                match oldest {
                    None => oldest = Some((class_id as u8, slab_id, last_accessed)),
                    Some((_, _, old_ts)) if last_accessed < old_ts => {
                        oldest = Some((class_id as u8, slab_id, last_accessed));
                    }
                    _ => {}
                }
            }
        }

        oldest.map(|(class_id, slab_id, _)| (class_id, slab_id))
    }

    /// Find the least recently created slab across all classes.
    ///
    /// Returns `(class_id, slab_id)` of the LRC slab, or `None` if no slabs exist.
    pub fn find_lrc_slab(&self) -> Option<(u8, u32)> {
        let mut oldest: Option<(u8, u32, u32)> = None; // (class_id, slab_id, created_at)

        for (class_id, class) in self.classes.iter().enumerate() {
            for (slab_id, created_at, _last_accessed) in class.slab_timestamps() {
                match oldest {
                    None => oldest = Some((class_id as u8, slab_id, created_at)),
                    Some((_, _, old_ts)) if created_at < old_ts => {
                        oldest = Some((class_id as u8, slab_id, created_at));
                    }
                    _ => {}
                }
            }
        }

        oldest.map(|(class_id, slab_id, _)| (class_id, slab_id))
    }

    /// Find a random slab across all classes.
    ///
    /// Returns `(class_id, slab_id)` of a random slab, or `None` if no slabs exist.
    pub fn find_random_slab(&self) -> Option<(u8, u32)> {
        // Collect all (class_id, slab_id) pairs
        let mut slabs = Vec::new();
        for (class_id, class) in self.classes.iter().enumerate() {
            let count = class.slab_count();
            for slab_id in 0..count {
                slabs.push((class_id as u8, slab_id as u32));
            }
        }

        if slabs.is_empty() {
            return None;
        }

        // Simple pseudo-random selection using timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as usize)
            .unwrap_or(0);
        let idx = now % slabs.len();
        Some(slabs[idx])
    }

    /// Evict all items from a specific slab.
    ///
    /// Removes all items from the slab, removes them from the hashtable,
    /// and returns the slab memory to the global free pool.
    ///
    /// Returns `true` if successful, `false` if the slab doesn't exist.
    pub fn evict_slab<H: Hashtable>(&self, class_id: u8, slab_id: u32, hashtable: &H) -> bool {
        let class = match self.classes.get(class_id as usize) {
            Some(c) => c,
            None => return false,
        };

        // Evict all items from the slab
        let slab_ptr = unsafe {
            class.evict_slab(slab_id, |key, cid, sid, slot| {
                let location = SlabLocation::new(cid, sid, slot).to_location();
                hashtable.remove(key, location);
            })
        };

        if let Some(ptr) = slab_ptr {
            // Return the slab to the global free pool
            self.free_slabs.push(ptr);

            // Update memory accounting (slab is now free but memory is still allocated)
            // Note: We don't decrement memory_used because the slab is still in our heap,
            // just available for reuse by any class

            true
        } else {
            false
        }
    }

    /// Try slab-level eviction using the configured strategy.
    ///
    /// Tries strategies in order: SLAB_LRC (8), SLAB_LRA (4), RANDOM (2).
    /// Returns `true` if a slab was evicted.
    pub fn try_slab_eviction<H: Hashtable>(&self, hashtable: &H) -> bool {
        let strategy = self.eviction_strategy;

        // Try strategies in order from highest to lowest bit
        // SLAB_LRC (8)
        if strategy.contains(EvictionStrategy::SLAB_LRC) {
            if let Some((class_id, slab_id)) = self.find_lrc_slab() {
                if self.evict_slab(class_id, slab_id, hashtable) {
                    return true;
                }
            }
        }

        // SLAB_LRA (4)
        if strategy.contains(EvictionStrategy::SLAB_LRA) {
            if let Some((class_id, slab_id)) = self.find_lra_slab() {
                if self.evict_slab(class_id, slab_id, hashtable) {
                    return true;
                }
            }
        }

        // RANDOM (2)
        if strategy.contains(EvictionStrategy::RANDOM) {
            if let Some((class_id, slab_id)) = self.find_random_slab() {
                if self.evict_slab(class_id, slab_id, hashtable) {
                    return true;
                }
            }
        }

        false
    }

    /// Get the eviction strategy.
    pub fn eviction_strategy(&self) -> EvictionStrategy {
        self.eviction_strategy
    }

    /// Write an item to a slot.
    ///
    /// # Safety
    ///
    /// The slot must have been allocated and not contain a live item.
    pub unsafe fn write_item(
        &self,
        class_id: u8,
        slab_id: u32,
        slot_index: u16,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) {
        // SAFETY: Caller ensures slot was allocated and doesn't contain live item
        unsafe {
            let class = &self.classes[class_id as usize];
            let ptr = class.slot_ptr(slab_id, slot_index);

            // Initialize header
            SlabItemHeader::init(ptr, key.len(), value.len(), ttl);

            // Copy key
            std::ptr::copy_nonoverlapping(key.as_ptr(), ptr.add(HEADER_SIZE), key.len());

            // Copy value
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                ptr.add(HEADER_SIZE + key.len()),
                value.len(),
            );

            // Update stats
            class.add_bytes(HEADER_SIZE + key.len() + value.len());
            class.add_item();
        }
    }

    /// Get a reference to the header at a location.
    ///
    /// # Safety
    ///
    /// The location must point to a valid item.
    #[inline]
    pub unsafe fn header(&self, location: SlabLocation) -> &SlabItemHeader {
        // SAFETY: Caller ensures location points to valid item
        unsafe {
            let (class_id, slab_id, slot_index) = location.unpack();
            self.classes[class_id as usize].header(slab_id, slot_index)
        }
    }

    /// Get a pointer to the slot at a location.
    ///
    /// # Safety
    ///
    /// The location must be valid.
    #[inline]
    pub unsafe fn slot_ptr(&self, location: SlabLocation) -> *mut u8 {
        // SAFETY: Caller ensures location is valid
        unsafe {
            let (class_id, slab_id, slot_index) = location.unpack();
            self.classes[class_id as usize].slot_ptr(slab_id, slot_index)
        }
    }

    /// Touch a slab for LRA tracking.
    pub fn touch_slab(&self, location: SlabLocation) {
        let (class_id, slab_id, _slot_index) = location.unpack();
        if let Some(class) = self.classes.get(class_id as usize) {
            class.touch_slab(slab_id);
        }
    }

    /// Try to acquire a reference to a slab for reading.
    ///
    /// Returns `true` if the slab is readable and ref_count was incremented.
    /// Returns `false` if the slab is not readable (unallocated or draining).
    ///
    /// Caller must call `release_slab()` when done reading.
    #[inline]
    pub fn acquire_slab(&self, location: SlabLocation) -> bool {
        let (class_id, slab_id, _) = location.unpack();
        if let Some(class) = self.classes.get(class_id as usize) {
            class.acquire_slab(slab_id)
        } else {
            false
        }
    }

    /// Release a reference to a slab after reading.
    ///
    /// Must be called after a successful `acquire_slab()`.
    #[inline]
    pub fn release_slab(&self, location: SlabLocation) {
        let (class_id, slab_id, _) = location.unpack();
        if let Some(class) = self.classes.get(class_id as usize) {
            class.release_slab(slab_id);
        }
    }

    /// Get raw value reference pointers for zero-copy reads.
    ///
    /// Returns `(ref_count_ptr, value_ptr, value_len)` if successful.
    /// The ref_count has already been incremented; caller must decrement on drop
    /// (typically by constructing a `ValueRef`).
    ///
    /// # Safety
    ///
    /// The location must point to a valid item.
    #[inline]
    pub unsafe fn get_value_ref_raw(
        &self,
        location: SlabLocation,
    ) -> Option<(*const std::sync::atomic::AtomicU32, *const u8, usize)> {
        let (class_id, slab_id, slot_index) = location.unpack();
        self.classes
            .get(class_id as usize)?
            .get_value_ref_raw(slab_id, slot_index)
    }

    /// Begin a two-phase write operation for zero-copy receive.
    ///
    /// This method:
    /// 1. Selects the appropriate size class
    /// 2. Allocates a slot (may allocate new slab or trigger eviction)
    /// 3. Initializes the header with key_len, value_len, ttl
    /// 4. Copies the key to slot memory
    /// 5. Returns the location and a pointer to the value area
    ///
    /// The caller then writes the value directly to the returned pointer,
    /// then calls `finalize_write_item` to update statistics.
    ///
    /// # Arguments
    /// * `key` - The key for this item
    /// * `value_len` - Length of the value to be written
    /// * `ttl` - Time-to-live for this item
    /// * `hashtable` - Hashtable for eviction callbacks
    ///
    /// # Returns
    /// `Some((location, value_ptr, item_size))` on success, `None` if allocation fails.
    pub fn begin_write_item<H: Hashtable>(
        &self,
        key: &[u8],
        value_len: usize,
        ttl: Duration,
        hashtable: &H,
    ) -> Option<(SlabLocation, *mut u8, usize)> {
        // Calculate item size
        let item_size = HEADER_SIZE + key.len() + value_len;

        // Select class
        let class_id = self.select_class(item_size)?;
        let class = self.classes.get(class_id as usize)?;

        // Try direct allocation first
        if let Some((slab_id, slot_index, value_ptr, item_size)) =
            class.begin_write_item(key, value_len, ttl)
        {
            let location = SlabLocation::new(class_id, slab_id, slot_index);
            return Some((location, value_ptr, item_size));
        }

        // Need to allocate a new slab or evict
        // First try allocating a new slab
        if self.allocate_slab_for_class(class_id).is_some() {
            // Try allocation again
            if let Some((slab_id, slot_index, value_ptr, item_size)) =
                class.begin_write_item(key, value_len, ttl)
            {
                let location = SlabLocation::new(class_id, slab_id, slot_index);
                return Some((location, value_ptr, item_size));
            }
        }

        // Check if eviction is enabled
        if self.eviction_strategy.is_none() {
            return None;
        }

        // Try slab-level eviction
        if self.eviction_strategy.has_slab_eviction() && self.try_slab_eviction(hashtable) {
            // Eviction freed some slots, try allocation again
            if let Some((slab_id, slot_index, value_ptr, item_size)) =
                class.begin_write_item(key, value_len, ttl)
            {
                let location = SlabLocation::new(class_id, slab_id, slot_index);
                return Some((location, value_ptr, item_size));
            }
        }

        None
    }

    /// Finalize a two-phase write operation.
    ///
    /// Called after the value has been written to the pointer returned by
    /// `begin_write_item`. Updates statistics (bytes_used, item_count).
    ///
    /// # Arguments
    /// * `location` - The location returned by `begin_write_item`
    /// * `item_size` - The item_size returned by `begin_write_item`
    pub fn finalize_write_item(&self, location: SlabLocation, item_size: usize) {
        let (class_id, _slab_id, _slot_index) = location.unpack();
        if let Some(class) = self.classes.get(class_id as usize) {
            class.finalize_write_item(item_size);
        }
    }

    /// Cancel a two-phase write operation.
    ///
    /// Called if the write cannot be completed (e.g., connection closed).
    /// Marks the item as deleted and returns the slot to the free list.
    ///
    /// # Arguments
    /// * `location` - The location returned by `begin_write_item`
    pub fn cancel_write_item(&self, location: SlabLocation) {
        let (class_id, slab_id, slot_index) = location.unpack();
        if let Some(class) = self.classes.get(class_id as usize) {
            class.cancel_write_item(slab_id, slot_index);
        }
    }

    /// Get the total memory used.
    pub fn memory_used(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Get the memory limit.
    pub fn memory_limit(&self) -> usize {
        self.memory_limit
    }

    /// Get the slab size.
    pub fn slab_size(&self) -> usize {
        self.slab_size
    }

    /// Get the number of slab classes.
    pub fn num_classes(&self) -> usize {
        self.classes.len()
    }

    /// Get statistics for a class.
    pub fn class_stats(&self, class_id: u8) -> Option<ClassStats> {
        let class = self.classes.get(class_id as usize)?;
        Some(ClassStats {
            class_id,
            slot_size: class.slot_size(),
            slab_count: class.slab_count(),
            item_count: class.item_count(),
            bytes_used: class.bytes_used(),
        })
    }

    /// Create a verifier for this allocator.
    pub fn verifier(&self) -> SlabVerifier<'_> {
        SlabVerifier::new(self)
    }

    /// Create a verifier that allows expired items.
    ///
    /// Used for lazy cleanup of expired items.
    pub fn verifier_allowing_expired(&self) -> SlabVerifier<'_> {
        SlabVerifier::allowing_expired(self)
    }

    /// Reset the entire allocator, returning all memory to the free pool.
    ///
    /// This resets all slab classes and returns their slabs to the global
    /// free list. Used during flush operations.
    pub fn reset_all(&self) {
        // First, drain the existing free slab list
        loop {
            match self.free_slabs.steal() {
                crossbeam_deque::Steal::Empty => break,
                crossbeam_deque::Steal::Retry => continue,
                crossbeam_deque::Steal::Success(_) => continue,
            }
        }

        // Reset each class and collect all slab data pointers
        let mut all_slab_ptrs = Vec::new();
        for class in &self.classes {
            let ptrs = class.reset();
            all_slab_ptrs.extend(ptrs);
        }

        // Return all slabs to the free pool
        for ptr in all_slab_ptrs {
            self.free_slabs.push(ptr);
        }

        // Also add back the unused heap memory
        // (Re-build from scratch based on heap layout)
        let slab_count = self.memory_limit / self.slab_size;
        let heap_ptr = self.heap.as_ptr();

        // Clear and rebuild - push all slab addresses
        loop {
            match self.free_slabs.steal() {
                crossbeam_deque::Steal::Empty => break,
                crossbeam_deque::Steal::Retry => continue,
                crossbeam_deque::Steal::Success(_) => continue,
            }
        }

        for i in 0..slab_count {
            let slab_ptr = unsafe { heap_ptr.add(i * self.slab_size) };
            self.free_slabs.push(slab_ptr);
        }

        // Reset memory tracking
        self.memory_used.store(0, Ordering::Release);
    }
}

/// Statistics for a slab class.
#[derive(Debug, Clone)]
pub struct ClassStats {
    /// Class ID.
    pub class_id: u8,
    /// Slot size in bytes.
    pub slot_size: usize,
    /// Number of allocated slabs.
    pub slab_count: usize,
    /// Number of items.
    pub item_count: u64,
    /// Bytes used by items.
    pub bytes_used: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cache_core::HugepageSize;

    fn test_config() -> SlabCacheConfig {
        SlabCacheConfig {
            heap_size: 4 * 1024 * 1024, // 4MB
            slab_size: 64 * 1024,       // 64KB slabs
            min_slot_size: 64,
            growth_factor: 1.25,
            hugepage_size: HugepageSize::None,
            ..Default::default()
        }
    }

    #[test]
    fn test_allocator_creation() {
        let config = test_config();
        let allocator = SlabAllocator::new(&config).unwrap();
        // Number of classes depends on slab_size and growth_factor
        assert!(allocator.num_classes() > 0);
        assert_eq!(allocator.slab_size(), 64 * 1024);
    }

    #[test]
    fn test_allocator_select_class() {
        let config = test_config();
        let allocator = SlabAllocator::new(&config).unwrap();

        // Small item -> class 0 (64 bytes)
        assert_eq!(allocator.select_class(50), Some(0));

        // Exact fit
        assert_eq!(allocator.select_class(64), Some(0));

        // Just over 64 -> next class
        let class = allocator.select_class(65);
        assert!(class.is_some());
        assert!(class.unwrap() > 0);
    }

    #[test]
    fn test_allocator_allocate() {
        let config = test_config();
        let allocator = SlabAllocator::new(&config).unwrap();

        // Allocate from class 0
        let slot = allocator.allocate(0);
        assert!(slot.is_some());
        let (slab_id, slot_index) = slot.unwrap();
        assert_eq!(slab_id, 0);
        assert_eq!(slot_index, 0);
    }

    #[test]
    fn test_allocator_write_item() {
        let config = test_config();
        let allocator = SlabAllocator::new(&config).unwrap();

        let key = b"test_key";
        let value = b"test_value";
        let item_size = HEADER_SIZE + key.len() + value.len();
        let class_id = allocator.select_class(item_size).unwrap();

        let (slab_id, slot_index) = allocator.allocate(class_id).unwrap();

        unsafe {
            allocator.write_item(
                class_id,
                slab_id,
                slot_index,
                key,
                value,
                Duration::from_secs(3600),
            );

            let location = SlabLocation::new(class_id, slab_id, slot_index);
            let header = allocator.header(location);
            assert_eq!(header.key(), key);
            assert_eq!(header.value(), value);
        }
    }

    #[test]
    fn test_allocator_large_slab_size() {
        // Test with 16MB slab size - should have classes up to 16MB
        let config = SlabCacheConfig {
            heap_size: 64 * 1024 * 1024, // 64MB
            slab_size: 16 * 1024 * 1024, // 16MB slabs
            min_slot_size: 64,
            growth_factor: 1.25,
            hugepage_size: HugepageSize::None,
            ..Default::default()
        };
        let allocator = SlabAllocator::new(&config).unwrap();

        // Should be able to select a class for a 10MB item
        let class_id = allocator.select_class(10 * 1024 * 1024);
        assert!(class_id.is_some(), "Should have class for 10MB item");
    }
}
