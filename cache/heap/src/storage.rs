//! Slot storage with lock-free free list management.
//!
//! Uses a Treiber stack for the free list, providing lock-free
//! allocation and deallocation of slots.

use crate::location::SlotLocation;
use crate::slot::Slot;
use crate::sync::{AtomicU32, Ordering, spin_loop};

/// Sentinel value indicating an empty free list.
pub const EMPTY_FREE_LIST: u32 = u32::MAX;

/// Storage for cache slots with lock-free free list management.
///
/// # Thread Safety
///
/// Uses a lock-free stack (Treiber stack) for the free list.
/// Under very high contention, the single head pointer can become a bottleneck.
pub struct SlotStorage {
    /// All slots.
    slots: Vec<Slot>,
    /// Head of the free list. Stores slot index, or EMPTY_FREE_LIST if empty.
    free_head: AtomicU32,
    /// Number of currently occupied slots.
    occupied_count: AtomicU32,
}

impl SlotStorage {
    /// Create a new slot storage with the given capacity.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        assert!(
            capacity <= u32::MAX as usize,
            "capacity exceeds maximum slot index"
        );

        let mut slots = Vec::with_capacity(capacity);

        // Initialize all slots and build free list
        for i in 0..capacity {
            slots.push(Slot::new());
            // Link each slot to the next, last slot points to EMPTY
            let next = if i + 1 < capacity {
                (i + 1) as u32
            } else {
                EMPTY_FREE_LIST
            };
            slots[i].set_next_free(next);
        }

        Self {
            slots,
            free_head: AtomicU32::new(0),
            occupied_count: AtomicU32::new(0),
        }
    }

    /// Allocate a slot from the free list.
    ///
    /// Returns the slot location (index + generation) or None if exhausted.
    ///
    /// # Thread Safety
    ///
    /// Uses compare-and-swap on the free list head. May retry under contention.
    pub fn allocate(&self) -> Option<SlotLocation> {
        loop {
            let head = self.free_head.load(Ordering::Acquire);
            if head == EMPTY_FREE_LIST {
                return None; // No free slots
            }

            let slot = &self.slots[head as usize];
            let next = slot.get_next_free();

            // Try to CAS the head to the next free slot
            match self.free_head.compare_exchange_weak(
                head,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Successfully allocated - clear the free list pointer
                    // so the slot is ready for store()
                    slot.clear_for_store();
                    self.occupied_count.fetch_add(1, Ordering::Relaxed);
                    let generation = slot.generation();
                    return Some(SlotLocation::new(head, generation));
                }
                Err(_) => {
                    // CAS failed, another thread modified the list, retry
                    spin_loop();
                }
            }
        }
    }

    /// Return a slot to the free list.
    ///
    /// # Thread Safety
    ///
    /// Clears the slot (waiting for readers) then pushes to the free list.
    pub fn deallocate(&self, loc: SlotLocation) {
        let idx = loc.slot_index();
        if idx as usize >= self.slots.len() {
            return; // Invalid index, ignore
        }

        let slot = &self.slots[idx as usize];

        // Clear the entry (waits for readers, increments generation)
        slot.clear();

        // Decrement occupied count
        self.occupied_count.fetch_sub(1, Ordering::Relaxed);

        // Push onto free list (lock-free stack push)
        loop {
            let head = self.free_head.load(Ordering::Acquire);
            slot.set_next_free(head);

            match self.free_head.compare_exchange_weak(
                head,
                idx,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => spin_loop(),
            }
        }
    }

    /// Get a slot by index.
    #[inline]
    pub fn get(&self, idx: u32) -> Option<&Slot> {
        self.slots.get(idx as usize)
    }

    /// Check if a slot is currently occupied (has a valid entry).
    #[inline]
    pub fn is_slot_occupied(&self, idx: u32) -> bool {
        self.slots
            .get(idx as usize)
            .is_some_and(|slot| slot.is_occupied())
    }

    /// Get the number of currently occupied slots.
    #[inline]
    pub fn occupied(&self) -> u32 {
        self.occupied_count.load(Ordering::Relaxed)
    }

    /// Get the total capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Reset all slots and rebuild the free list.
    ///
    /// This clears all occupied slots (freeing their entries) and rebuilds
    /// the free list to its initial state. Used during flush operations.
    ///
    /// # Safety
    ///
    /// This should only be called when no concurrent operations are accessing
    /// the storage (e.g., after the hashtable has been cleared).
    pub fn reset_all(&self) {
        // Clear all occupied slots
        for slot in &self.slots {
            if slot.is_occupied() {
                slot.clear();
            }
        }

        // Rebuild the free list: link all slots sequentially
        let capacity = self.slots.len();
        for (i, slot) in self.slots.iter().enumerate() {
            let next = if i + 1 < capacity {
                (i + 1) as u32
            } else {
                EMPTY_FREE_LIST
            };
            slot.set_next_free(next);
        }

        // Reset counters
        self.free_head.store(0, Ordering::Release);
        self.occupied_count.store(0, Ordering::Release);
    }
}

// Ensure SlotStorage is Send + Sync
unsafe impl Send for SlotStorage {}
unsafe impl Sync for SlotStorage {}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::entry::HeapEntry;
    use std::time::Duration;

    #[test]
    fn test_allocate_deallocate() {
        let storage = SlotStorage::new(10);

        // Allocate all slots
        let mut locations = Vec::new();
        for _ in 0..10 {
            let loc = storage.allocate().expect("should have free slots");
            locations.push(loc);
        }

        // Should be exhausted
        assert!(storage.allocate().is_none());
        assert_eq!(storage.occupied(), 10);

        // Deallocate one
        storage.deallocate(locations.pop().unwrap());
        assert_eq!(storage.occupied(), 9);

        // Should be able to allocate again
        let loc = storage.allocate().expect("should have free slot");

        // Generation should have incremented
        assert_eq!(loc.generation(), 1);
    }

    #[test]
    fn test_slot_storage_with_entries() {
        let storage = SlotStorage::new(5);

        let loc = storage.allocate().expect("allocation failed");
        let entry = HeapEntry::allocate(b"key", b"value", Duration::ZERO, 1).unwrap();

        let slot = storage.get(loc.slot_index()).unwrap();
        slot.store(entry);

        assert!(storage.is_slot_occupied(loc.slot_index()));

        storage.deallocate(loc);
        assert!(!storage.is_slot_occupied(loc.slot_index()));
    }

    #[test]
    fn test_capacity() {
        let storage = SlotStorage::new(100);
        assert_eq!(storage.capacity(), 100);
        assert_eq!(storage.occupied(), 0);
    }
}
