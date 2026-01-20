//! Slab allocator managing memory pools and slab classes.
//!
//! The allocator coordinates between slab classes, allocating new slabs
//! from the heap when needed and managing eviction when memory is exhausted.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cache_core::{Hashtable, HugepageAllocation, HugepageSize, allocate_on_node};
use crossbeam_deque::Injector;
use parking_lot::Mutex;

use crate::class::SlabClass;
use crate::config::{HEADER_SIZE, SLAB_CLASSES, SlabCacheConfig, select_class};
use crate::item::SlabItemHeader;
use crate::location::SlabLocation;
use crate::verifier::SlabVerifier;

/// The main slab allocator.
pub struct SlabAllocator {
    /// Slab classes (indexed by class_id).
    classes: Vec<SlabClass>,
    /// Heap memory allocation.
    heap: HugepageAllocation,
    /// Slab size in bytes.
    slab_size: usize,
    /// Total memory limit.
    memory_limit: usize,
    /// Current memory used (number of slabs allocated * slab_size).
    memory_used: AtomicUsize,
    /// Free slab pages (pointers to unallocated slab memory).
    free_slabs: Injector<*mut u8>,
    /// Lock for allocating new slabs (prevents races).
    alloc_lock: Mutex<()>,
}

// Safety: The allocator manages heap memory safely.
unsafe impl Send for SlabAllocator {}
unsafe impl Sync for SlabAllocator {}

impl SlabAllocator {
    /// Create a new slab allocator.
    pub fn new(config: &SlabCacheConfig) -> Result<Self, std::io::Error> {
        // Allocate the heap
        let heap = allocate_on_node(config.heap_size, config.hugepage_size, config.numa_node)?;

        // Create slab classes
        let classes: Vec<SlabClass> = SLAB_CLASSES
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
            heap,
            slab_size: config.slab_size,
            memory_limit: config.heap_size,
            memory_used: AtomicUsize::new(0),
            free_slabs,
            alloc_lock: Mutex::new(()),
        })
    }

    /// Select the smallest class that fits an item.
    ///
    /// `item_size` should be `key.len() + value.len() + HEADER_SIZE`.
    #[inline]
    pub fn select_class(&self, item_size: usize) -> Option<u8> {
        select_class(item_size)
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
    fn allocate_slab_for_class(&self, class_id: u8) -> Option<(u32, u16)> {
        let _lock = self.alloc_lock.lock();

        let class = self.classes.get(class_id as usize)?;

        // Double-check: maybe another thread already allocated
        if let Some(slot) = class.allocate() {
            return Some(slot);
        }

        // Try to get a free slab from the pool
        let slab_ptr = match self.free_slabs.steal() {
            crossbeam_deque::Steal::Success(ptr) => ptr,
            _ => return None, // No free slabs available
        };

        // Update memory usage
        self.memory_used
            .fetch_add(self.slab_size, Ordering::Relaxed);

        // Add the slab to the class
        unsafe {
            class.add_slab(slab_ptr, self.slab_size);
        }

        // Now allocate from the class
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
    /// evicts the LRU item from the same class.
    pub fn allocate_with_eviction<H: Hashtable>(
        &self,
        class_id: u8,
        hashtable: &H,
    ) -> Option<(u32, u16)> {
        // First try normal allocation
        if let Some(slot) = self.allocate(class_id) {
            return Some(slot);
        }

        // Need to evict
        self.evict_from_class(class_id, hashtable)?;

        // Try allocation again
        self.allocate(class_id)
    }

    /// Evict the LRU item from a class.
    ///
    /// Returns `Some(())` if an item was evicted, `None` if the class is empty.
    pub fn evict_from_class<H: Hashtable>(&self, class_id: u8, hashtable: &H) -> Option<()> {
        let class = self.classes.get(class_id as usize)?;

        // Pop the LRU tail
        let (slab_id, slot_index) = class.lru_pop_tail()?;

        // Get the key and remove from hashtable
        let location = SlabLocation::new(class_id, slab_id, slot_index).to_location();

        unsafe {
            let header = class.header(slab_id, slot_index);
            let key = header.key();

            // Remove from hashtable
            hashtable.remove(key, location);

            // Mark as deleted
            header.mark_deleted();

            // Update bytes used
            class.sub_bytes(header.item_size());
        }

        // Return slot to free list
        class.deallocate(slab_id, slot_index);

        Some(())
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

            // Update bytes used
            class.add_bytes(HEADER_SIZE + key.len() + value.len());

            // Insert at LRU head
            class.lru_insert_head(slab_id, slot_index);
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

    /// Touch an item in the LRU (move to head).
    pub fn lru_touch(&self, location: SlabLocation) {
        let (class_id, slab_id, slot_index) = location.unpack();
        if let Some(class) = self.classes.get(class_id as usize) {
            class.lru_touch(slab_id, slot_index);
        }
    }

    /// Remove an item from the LRU list.
    pub fn lru_remove(&self, location: SlabLocation) {
        let (class_id, slab_id, slot_index) = location.unpack();
        if let Some(class) = self.classes.get(class_id as usize) {
            class.lru_remove(slab_id, slot_index);
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

    fn test_config() -> SlabCacheConfig {
        SlabCacheConfig {
            heap_size: 4 * 1024 * 1024, // 4MB
            slab_size: 64 * 1024,       // 64KB slabs
            hugepage_size: HugepageSize::None,
            ..Default::default()
        }
    }

    #[test]
    fn test_allocator_creation() {
        let config = test_config();
        let allocator = SlabAllocator::new(&config).unwrap();
        assert_eq!(allocator.num_classes(), SLAB_CLASSES.len());
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

        // Just over 64 -> class 1 (80 bytes)
        assert_eq!(allocator.select_class(65), Some(1));

        // 1KB item -> class 12
        assert_eq!(allocator.select_class(1024), Some(12));
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
}
