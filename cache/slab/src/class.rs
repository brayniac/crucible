//! Slab class management with free list and LRU tracking.
//!
//! Each slab class manages slots of a fixed size. Slabs are allocated from
//! a shared heap and divided into equal-sized slots.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crossbeam_deque::Injector;
use parking_lot::{Mutex, RwLock};

use crate::config::HEADER_SIZE;
use crate::item::{LRU_NONE, SlabItemHeader, pack_lru_link, unpack_lru_link};

/// A single slab of memory divided into fixed-size slots.
pub struct Slab {
    /// Pointer to the slab memory.
    data: *mut u8,
    /// Slab size in bytes.
    size: usize,
    /// Active readers (prevents deallocation).
    ref_count: AtomicU32,
}

impl Slab {
    /// Create a new slab from allocated memory.
    ///
    /// # Safety
    ///
    /// The caller must ensure `data` points to valid memory of at least `size` bytes.
    pub unsafe fn new(data: *mut u8, size: usize) -> Self {
        Self {
            data,
            size,
            ref_count: AtomicU32::new(0),
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
    /// LRU head (most recently used), packed slot reference.
    lru_head: AtomicU32,
    /// LRU tail (least recently used), packed slot reference.
    lru_tail: AtomicU32,
    /// LRU modification lock.
    lru_lock: Mutex<()>,
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
            lru_head: AtomicU32::new(LRU_NONE),
            lru_tail: AtomicU32::new(LRU_NONE),
            lru_lock: Mutex::new(()),
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

            // Create the slab
            let slab = Slab::new(data, slab_size);
            slabs.push(slab);

            // Add all slots to the free list
            for slot_index in 0..self.slots_per_slab {
                let packed = pack_lru_link(slab_id, slot_index as u16);
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
                let (slab_id, slot_index) = unpack_lru_link(packed);
                Some((slab_id, slot_index))
            }
            _ => None,
        }
    }

    /// Return a slot to the free list.
    pub fn deallocate(&self, slab_id: u32, slot_index: u16) {
        let packed = pack_lru_link(slab_id, slot_index);
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

    /// Insert a slot at the LRU head (most recently used).
    pub fn lru_insert_head(&self, slab_id: u32, slot_index: u16) {
        let _lock = self.lru_lock.lock();
        let new_packed = pack_lru_link(slab_id, slot_index);

        // Get the current head
        let old_head = self.lru_head.load(Ordering::Acquire);

        // Set the new item's links
        unsafe {
            let header = self.header(slab_id, slot_index);
            header.set_lru_prev(LRU_NONE);
            header.set_lru_next(old_head);
        }

        // Update the old head's prev link
        if old_head != LRU_NONE {
            let (old_slab, old_slot) = unpack_lru_link(old_head);
            unsafe {
                let old_header = self.header(old_slab, old_slot);
                old_header.set_lru_prev(new_packed);
            }
        } else {
            // List was empty, this is also the tail
            self.lru_tail.store(new_packed, Ordering::Release);
        }

        // Update head to point to new item
        self.lru_head.store(new_packed, Ordering::Release);

        // Update stats
        self.item_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove a slot from the LRU list.
    pub fn lru_remove(&self, slab_id: u32, slot_index: u16) {
        let _lock = self.lru_lock.lock();
        let packed = pack_lru_link(slab_id, slot_index);

        unsafe {
            let header = self.header(slab_id, slot_index);
            let prev = header.lru_prev();
            let next = header.lru_next();

            // Update previous item's next link
            if prev != LRU_NONE {
                let (prev_slab, prev_slot) = unpack_lru_link(prev);
                let prev_header = self.header(prev_slab, prev_slot);
                prev_header.set_lru_next(next);
            } else {
                // This was the head
                self.lru_head.store(next, Ordering::Release);
            }

            // Update next item's prev link
            if next != LRU_NONE {
                let (next_slab, next_slot) = unpack_lru_link(next);
                let next_header = self.header(next_slab, next_slot);
                next_header.set_lru_prev(prev);
            } else {
                // This was the tail
                self.lru_tail.store(prev, Ordering::Release);
            }

            // Clear this item's links
            header.set_lru_prev(LRU_NONE);
            header.set_lru_next(LRU_NONE);
        }

        // Update stats
        self.item_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Move a slot to the LRU head (touch on access).
    pub fn lru_touch(&self, slab_id: u32, slot_index: u16) {
        let _lock = self.lru_lock.lock();
        let packed = pack_lru_link(slab_id, slot_index);

        // If already at head, nothing to do
        if self.lru_head.load(Ordering::Acquire) == packed {
            return;
        }

        unsafe {
            let header = self.header(slab_id, slot_index);
            let prev = header.lru_prev();
            let next = header.lru_next();

            // Remove from current position
            if prev != LRU_NONE {
                let (prev_slab, prev_slot) = unpack_lru_link(prev);
                let prev_header = self.header(prev_slab, prev_slot);
                prev_header.set_lru_next(next);
            }

            if next != LRU_NONE {
                let (next_slab, next_slot) = unpack_lru_link(next);
                let next_header = self.header(next_slab, next_slot);
                next_header.set_lru_prev(prev);
            } else {
                // This was the tail, update tail
                self.lru_tail.store(prev, Ordering::Release);
            }

            // Insert at head
            let old_head = self.lru_head.load(Ordering::Acquire);
            header.set_lru_prev(LRU_NONE);
            header.set_lru_next(old_head);

            if old_head != LRU_NONE {
                let (old_slab, old_slot) = unpack_lru_link(old_head);
                let old_header = self.header(old_slab, old_slot);
                old_header.set_lru_prev(packed);
            }

            self.lru_head.store(packed, Ordering::Release);
        }
    }

    /// Pop the LRU tail (evict oldest item).
    ///
    /// Returns `Some((slab_id, slot_index))` if the list is not empty.
    pub fn lru_pop_tail(&self) -> Option<(u32, u16)> {
        let _lock = self.lru_lock.lock();

        let tail = self.lru_tail.load(Ordering::Acquire);
        if tail == LRU_NONE {
            return None;
        }

        let (slab_id, slot_index) = unpack_lru_link(tail);

        unsafe {
            let header = self.header(slab_id, slot_index);
            let prev = header.lru_prev();

            // Update the previous item to be the new tail
            if prev != LRU_NONE {
                let (prev_slab, prev_slot) = unpack_lru_link(prev);
                let prev_header = self.header(prev_slab, prev_slot);
                prev_header.set_lru_next(LRU_NONE);
            } else {
                // List is now empty
                self.lru_head.store(LRU_NONE, Ordering::Release);
            }

            self.lru_tail.store(prev, Ordering::Release);

            // Clear the evicted item's links
            header.set_lru_prev(LRU_NONE);
            header.set_lru_next(LRU_NONE);
        }

        // Update stats
        self.item_count.fetch_sub(1, Ordering::Relaxed);

        Some((slab_id, slot_index))
    }

    /// Check if the LRU list is empty.
    pub fn is_empty(&self) -> bool {
        self.lru_head.load(Ordering::Acquire) == LRU_NONE
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
    fn test_slab_class_lru() {
        let class = SlabClass::new(0, 64, 1024);

        // Add a slab
        let mut buffer = vec![0u8; 1024];
        unsafe {
            class.add_slab(buffer.as_mut_ptr(), 1024);
        }

        // Allocate some slots and add to LRU
        let (slab_id1, slot1) = class.allocate().unwrap();
        let (slab_id2, slot2) = class.allocate().unwrap();
        let (slab_id3, slot3) = class.allocate().unwrap();

        // Initialize headers
        unsafe {
            use std::time::Duration;
            let ptr1 = class.slot_ptr(slab_id1, slot1);
            let ptr2 = class.slot_ptr(slab_id2, slot2);
            let ptr3 = class.slot_ptr(slab_id3, slot3);
            SlabItemHeader::init(ptr1, 4, 10, Duration::from_secs(100));
            SlabItemHeader::init(ptr2, 4, 10, Duration::from_secs(100));
            SlabItemHeader::init(ptr3, 4, 10, Duration::from_secs(100));
        }

        // Insert in order: 1, 2, 3
        class.lru_insert_head(slab_id1, slot1);
        class.lru_insert_head(slab_id2, slot2);
        class.lru_insert_head(slab_id3, slot3);

        // Head should be 3, tail should be 1
        assert_eq!(class.item_count(), 3);

        // Pop tail should give us 1
        let popped = class.lru_pop_tail();
        assert_eq!(popped, Some((slab_id1, slot1)));
        assert_eq!(class.item_count(), 2);

        // Pop tail should give us 2
        let popped = class.lru_pop_tail();
        assert_eq!(popped, Some((slab_id2, slot2)));

        // Pop tail should give us 3
        let popped = class.lru_pop_tail();
        assert_eq!(popped, Some((slab_id3, slot3)));

        // List should be empty
        assert!(class.is_empty());
        assert_eq!(class.lru_pop_tail(), None);
    }

    #[test]
    fn test_slab_class_lru_touch() {
        let class = SlabClass::new(0, 64, 1024);

        let mut buffer = vec![0u8; 1024];
        unsafe {
            class.add_slab(buffer.as_mut_ptr(), 1024);
        }

        let (slab_id1, slot1) = class.allocate().unwrap();
        let (slab_id2, slot2) = class.allocate().unwrap();

        unsafe {
            use std::time::Duration;
            let ptr1 = class.slot_ptr(slab_id1, slot1);
            let ptr2 = class.slot_ptr(slab_id2, slot2);
            SlabItemHeader::init(ptr1, 4, 10, Duration::from_secs(100));
            SlabItemHeader::init(ptr2, 4, 10, Duration::from_secs(100));
        }

        // Insert 1, then 2 (head is 2)
        class.lru_insert_head(slab_id1, slot1);
        class.lru_insert_head(slab_id2, slot2);

        // Touch 1 (move to head)
        class.lru_touch(slab_id1, slot1);

        // Now head should be 1, tail should be 2
        let popped = class.lru_pop_tail();
        assert_eq!(popped, Some((slab_id2, slot2)));
    }
}
