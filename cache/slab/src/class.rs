//! Slab class management with free list.
//!
//! Each slab class manages slots of a fixed size. Slabs are allocated from
//! a shared heap and divided into equal-sized slots.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crossbeam_deque::Injector;
use parking_lot::RwLock;

use crate::config::HEADER_SIZE;
use crate::item::{SlabItemHeader, now_secs, pack_slot_ref, unpack_slot_ref};

/// A single slab of memory divided into fixed-size slots.
pub struct Slab {
    /// Pointer to the slab memory.
    data: *mut u8,
    /// Slab size in bytes.
    size: usize,
    /// Active readers (prevents deallocation).
    ref_count: AtomicU32,
    /// Creation timestamp (seconds since epoch).
    created_at: u32,
    /// Last access timestamp (seconds since epoch, updated on item access).
    last_accessed: AtomicU32,
    /// Class ID this slab belongs to.
    class_id: u8,
    /// Slab ID within the class.
    slab_id: u32,
}

impl Slab {
    /// Create a new slab from allocated memory.
    ///
    /// # Safety
    ///
    /// The caller must ensure `data` points to valid memory of at least `size` bytes.
    pub unsafe fn new(data: *mut u8, size: usize, class_id: u8, slab_id: u32) -> Self {
        let now = now_secs();
        Self {
            data,
            size,
            ref_count: AtomicU32::new(0),
            created_at: now,
            last_accessed: AtomicU32::new(now),
            class_id,
            slab_id,
        }
    }

    /// Get the slab data pointer.
    #[inline]
    pub fn data(&self) -> *mut u8 {
        self.data
    }

    /// Get the slab size.
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Increment the reference count.
    #[inline]
    pub fn acquire(&self) {
        self.ref_count.fetch_add(1, Ordering::Acquire);
    }

    /// Decrement the reference count.
    #[inline]
    pub fn release(&self) {
        self.ref_count.fetch_sub(1, Ordering::Release);
    }

    /// Get the current reference count.
    #[inline]
    pub fn ref_count(&self) -> u32 {
        self.ref_count.load(Ordering::Relaxed)
    }

    /// Get a pointer to a specific slot.
    ///
    /// # Safety
    ///
    /// The caller must ensure `slot_index * slot_size < self.size`.
    #[inline]
    pub unsafe fn slot_ptr(&self, slot_index: u16, slot_size: usize) -> *mut u8 {
        // SAFETY: Caller ensures slot_index * slot_size < self.size
        unsafe { self.data.add(slot_index as usize * slot_size) }
    }

    /// Get the item header at a specific slot.
    ///
    /// # Safety
    ///
    /// The caller must ensure the slot contains a valid item.
    #[inline]
    pub unsafe fn header(&self, slot_index: u16, slot_size: usize) -> &SlabItemHeader {
        // SAFETY: Caller ensures slot contains valid item
        unsafe { SlabItemHeader::from_ptr(self.slot_ptr(slot_index, slot_size)) }
    }

    /// Get the creation timestamp (seconds since epoch).
    #[inline]
    pub fn created_at(&self) -> u32 {
        self.created_at
    }

    /// Get the last access timestamp (seconds since epoch).
    #[inline]
    pub fn last_accessed(&self) -> u32 {
        self.last_accessed.load(Ordering::Relaxed)
    }

    /// Update the last access timestamp to now.
    #[inline]
    pub fn touch(&self) {
        self.last_accessed.store(now_secs(), Ordering::Relaxed);
    }

    /// Get the class ID this slab belongs to.
    #[inline]
    pub fn class_id(&self) -> u8 {
        self.class_id
    }

    /// Get the slab ID within the class.
    #[inline]
    pub fn slab_id(&self) -> u32 {
        self.slab_id
    }
}

// Safety: Slab just contains raw pointers to heap memory which is stable.
unsafe impl Send for Slab {}
unsafe impl Sync for Slab {}

/// A slab class manages all slabs of a particular slot size.
pub struct SlabClass {
    /// Class ID (index in the SLAB_CLASSES array).
    class_id: u8,
    /// Slot size for this class.
    slot_size: usize,
    /// Slots per slab (slab_size / slot_size).
    slots_per_slab: usize,
    /// Allocated slabs.
    slabs: RwLock<Vec<Slab>>,
    /// Free slot stack: packed as (slab_id << 16 | slot_index).
    free_slots: Injector<u32>,
    /// Number of items in this class.
    item_count: AtomicU64,
    /// Total bytes used by items in this class.
    bytes_used: AtomicU64,
}

impl SlabClass {
    /// Create a new slab class.
    pub fn new(class_id: u8, slot_size: usize, slab_size: usize) -> Self {
        Self {
            class_id,
            slot_size,
            slots_per_slab: slab_size / slot_size,
            slabs: RwLock::new(Vec::new()),
            free_slots: Injector::new(),
            item_count: AtomicU64::new(0),
            bytes_used: AtomicU64::new(0),
        }
    }

    /// Get the class ID.
    #[inline]
    pub fn class_id(&self) -> u8 {
        self.class_id
    }

    /// Get the slot size for this class.
    #[inline]
    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    /// Get the number of slots per slab.
    #[inline]
    pub fn slots_per_slab(&self) -> usize {
        self.slots_per_slab
    }

    /// Get the number of allocated slabs.
    pub fn slab_count(&self) -> usize {
        self.slabs.read().len()
    }

    /// Touch a slab to update its last_accessed timestamp.
    ///
    /// Call this when accessing an item in the slab.
    #[inline]
    pub fn touch_slab(&self, slab_id: u32) {
        let slabs = self.slabs.read();
        if let Some(slab) = slabs.get(slab_id as usize) {
            slab.touch();
        }
    }

    /// Get slab timestamps for LRA/LRC selection.
    ///
    /// Returns (slab_id, created_at, last_accessed) for each slab.
    pub fn slab_timestamps(&self) -> Vec<(u32, u32, u32)> {
        let slabs = self.slabs.read();
        slabs
            .iter()
            .enumerate()
            .map(|(id, slab)| (id as u32, slab.created_at(), slab.last_accessed()))
            .collect()
    }

    /// Get the number of items in this class.
    #[inline]
    pub fn item_count(&self) -> u64 {
        self.item_count.load(Ordering::Relaxed)
    }

    /// Get the total bytes used by items.
    #[inline]
    pub fn bytes_used(&self) -> u64 {
        self.bytes_used.load(Ordering::Relaxed)
    }

    /// Add a new slab to this class.
    ///
    /// # Safety
    ///
    /// The caller must ensure `data` points to valid memory of at least `slab_size` bytes.
    pub unsafe fn add_slab(&self, data: *mut u8, slab_size: usize) -> u32 {
        // SAFETY: Caller ensures data points to valid memory
        unsafe {
            let mut slabs = self.slabs.write();
            let slab_id = slabs.len() as u32;

            // Create the slab with class_id and slab_id for tracking
            let slab = Slab::new(data, slab_size, self.class_id, slab_id);
            slabs.push(slab);

            // Add all slots to the free list
            for slot_index in 0..self.slots_per_slab {
                let packed = pack_slot_ref(slab_id, slot_index as u16);
                self.free_slots.push(packed);
            }

            slab_id
        }
    }

    /// Try to allocate a slot from the free list.
    ///
    /// Returns `Some((slab_id, slot_index))` if successful, `None` if no free slots.
    pub fn allocate(&self) -> Option<(u32, u16)> {
        match self.free_slots.steal() {
            crossbeam_deque::Steal::Success(packed) => {
                let (slab_id, slot_index) = unpack_slot_ref(packed);
                Some((slab_id, slot_index))
            }
            _ => None,
        }
    }

    /// Return a slot to the free list.
    pub fn deallocate(&self, slab_id: u32, slot_index: u16) {
        let packed = pack_slot_ref(slab_id, slot_index);
        self.free_slots.push(packed);
    }

    /// Get a slab by ID.
    pub fn get_slab(&self, slab_id: u32) -> Option<SlabRef<'_>> {
        let slabs = self.slabs.read();
        if (slab_id as usize) < slabs.len() {
            // We need to acquire the ref before dropping the read lock
            slabs[slab_id as usize].acquire();
            Some(SlabRef {
                class: self,
                slab_id,
            })
        } else {
            None
        }
    }

    /// Get the pointer to a slot (without reference counting).
    ///
    /// # Safety
    ///
    /// Caller must ensure proper synchronization and that the slab exists.
    pub unsafe fn slot_ptr(&self, slab_id: u32, slot_index: u16) -> *mut u8 {
        // SAFETY: Caller ensures slab exists
        unsafe {
            let slabs = self.slabs.read();
            slabs[slab_id as usize].slot_ptr(slot_index, self.slot_size)
        }
    }

    /// Get the item header at a specific location.
    ///
    /// # Safety
    ///
    /// Caller must ensure the slot contains a valid item.
    pub unsafe fn header(&self, slab_id: u32, slot_index: u16) -> &SlabItemHeader {
        // SAFETY: Caller ensures slot contains valid item
        unsafe {
            let ptr = self.slot_ptr(slab_id, slot_index);
            SlabItemHeader::from_ptr(ptr)
        }
    }

    /// Increment the item count.
    pub fn add_item(&self) {
        self.item_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the item count.
    pub fn remove_item(&self) {
        self.item_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Evict all items from a specific slab.
    ///
    /// Calls the provided callback for each evicted item with (key, class_id, slab_id, slot_index).
    /// The callback should remove the item from the hashtable.
    ///
    /// Returns the slab's data pointer so it can be returned to the global free pool,
    /// or `None` if the slab doesn't exist.
    ///
    /// # Safety
    ///
    /// The returned pointer must only be used to return the slab to the allocator's
    /// free pool. The slab should not be used by this class after eviction.
    pub unsafe fn evict_slab<F>(&self, slab_id: u32, mut on_evict: F) -> Option<*mut u8>
    where
        F: FnMut(&[u8], u8, u32, u16),
    {
        let slabs = self.slabs.read();

        let slab = slabs.get(slab_id as usize)?;
        let slab_ptr = slab.data();

        // Iterate through all slots in the slab
        for slot_index in 0..self.slots_per_slab {
            let slot_index = slot_index as u16;

            // SAFETY: We're iterating valid slot indices
            unsafe {
                let header = self.header(slab_id, slot_index);

                // Skip deleted/empty slots
                if header.is_deleted() {
                    continue;
                }

                // Get the key for hashtable removal
                let key = header.key();

                // Call the callback to remove from hashtable
                on_evict(key, self.class_id, slab_id, slot_index);

                // Update stats
                self.sub_bytes(header.item_size());
                self.remove_item();

                // Mark as deleted
                header.mark_deleted();
            }
        }

        // Return all slots to free list
        for slot_index in 0..self.slots_per_slab {
            let packed = pack_slot_ref(slab_id, slot_index as u16);
            self.free_slots.push(packed);
        }

        Some(slab_ptr)
    }

    /// Get the data pointer for a slab (for returning to global pool).
    pub fn slab_data_ptr(&self, slab_id: u32) -> Option<*mut u8> {
        let slabs = self.slabs.read();
        slabs.get(slab_id as usize).map(|s| s.data())
    }

    /// Check if the class has any items.
    pub fn is_empty(&self) -> bool {
        self.item_count.load(Ordering::Relaxed) == 0
    }

    /// Update bytes used when an item is added.
    pub fn add_bytes(&self, bytes: usize) {
        self.bytes_used.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Update bytes used when an item is removed.
    pub fn sub_bytes(&self, bytes: usize) {
        self.bytes_used.fetch_sub(bytes as u64, Ordering::Relaxed);
    }

    /// Get the max item size for this class (slot size - header).
    pub fn max_item_size(&self) -> usize {
        self.slot_size.saturating_sub(HEADER_SIZE)
    }

    /// Reset this slab class, returning all slab data pointers.
    ///
    /// This clears all slabs and returns their data pointers so they can
    /// be returned to the global free pool.
    pub fn reset(&self) -> Vec<*mut u8> {
        // Clear the free slots queue
        loop {
            match self.free_slots.steal() {
                crossbeam_deque::Steal::Empty => break,
                crossbeam_deque::Steal::Retry => continue,
                crossbeam_deque::Steal::Success(_) => continue,
            }
        }

        // Get all slab data pointers and clear the slabs list
        let mut slabs = self.slabs.write();
        let data_ptrs: Vec<*mut u8> = slabs.iter().map(|s| s.data()).collect();
        slabs.clear();

        // Reset counters
        self.item_count.store(0, Ordering::Release);
        self.bytes_used.store(0, Ordering::Release);

        data_ptrs
    }
}

/// RAII guard for slab access.
pub struct SlabRef<'a> {
    class: &'a SlabClass,
    slab_id: u32,
}

impl<'a> SlabRef<'a> {
    /// Get the slab ID.
    #[inline]
    pub fn slab_id(&self) -> u32 {
        self.slab_id
    }

    /// Get a pointer to a slot.
    ///
    /// # Safety
    ///
    /// Caller must ensure the slot index is valid.
    #[inline]
    pub unsafe fn slot_ptr(&self, slot_index: u16) -> *mut u8 {
        // SAFETY: Caller ensures slot index is valid
        unsafe { self.class.slot_ptr(self.slab_id, slot_index) }
    }

    /// Get the header at a slot.
    ///
    /// # Safety
    ///
    /// Caller must ensure the slot contains a valid item.
    #[inline]
    pub unsafe fn header(&self, slot_index: u16) -> &SlabItemHeader {
        // SAFETY: Caller ensures slot contains valid item
        unsafe { self.class.header(self.slab_id, slot_index) }
    }
}

impl Drop for SlabRef<'_> {
    fn drop(&mut self) {
        let slabs = self.class.slabs.read();
        if (self.slab_id as usize) < slabs.len() {
            slabs[self.slab_id as usize].release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slab_class_creation() {
        let class = SlabClass::new(0, 64, 1024 * 1024);
        assert_eq!(class.class_id(), 0);
        assert_eq!(class.slot_size(), 64);
        assert_eq!(class.slots_per_slab(), 1024 * 1024 / 64);
    }

    #[test]
    fn test_slab_class_add_slab() {
        let class = SlabClass::new(0, 64, 1024);

        // Allocate a small test slab
        let mut buffer = vec![0u8; 1024];
        unsafe {
            let slab_id = class.add_slab(buffer.as_mut_ptr(), 1024);
            assert_eq!(slab_id, 0);
            assert_eq!(class.slab_count(), 1);

            // Should have slots available
            let slot = class.allocate();
            assert!(slot.is_some());
        }
    }

    #[test]
    fn test_slab_class_allocate_deallocate() {
        let class = SlabClass::new(0, 64, 1024);

        let mut buffer = vec![0u8; 1024];
        unsafe {
            class.add_slab(buffer.as_mut_ptr(), 1024);
        }

        // Allocate all slots
        let slots_per_slab = 1024 / 64;
        let mut allocated = Vec::new();
        for _ in 0..slots_per_slab {
            let slot = class.allocate();
            assert!(slot.is_some());
            allocated.push(slot.unwrap());
        }

        // No more slots
        assert!(class.allocate().is_none());

        // Deallocate one
        let (slab_id, slot_index) = allocated.pop().unwrap();
        class.deallocate(slab_id, slot_index);

        // Can allocate again
        let slot = class.allocate();
        assert!(slot.is_some());
    }

    #[test]
    fn test_slab_timestamps() {
        let class = SlabClass::new(0, 64, 1024);

        let mut buffer = vec![0u8; 1024];
        unsafe {
            class.add_slab(buffer.as_mut_ptr(), 1024);
        }

        let timestamps = class.slab_timestamps();
        assert_eq!(timestamps.len(), 1);

        let (slab_id, created_at, last_accessed) = timestamps[0];
        assert_eq!(slab_id, 0);
        assert!(created_at > 0);
        assert_eq!(created_at, last_accessed); // Initially equal

        // Touch the slab
        std::thread::sleep(std::time::Duration::from_millis(10));
        class.touch_slab(0);

        let timestamps = class.slab_timestamps();
        let (_, _, new_last_accessed) = timestamps[0];
        // last_accessed should be >= created_at
        assert!(new_last_accessed >= created_at);
    }

    #[test]
    fn test_item_count() {
        let class = SlabClass::new(0, 64, 1024);

        assert_eq!(class.item_count(), 0);

        class.add_item();
        assert_eq!(class.item_count(), 1);

        class.add_item();
        assert_eq!(class.item_count(), 2);

        class.remove_item();
        assert_eq!(class.item_count(), 1);
    }
}
