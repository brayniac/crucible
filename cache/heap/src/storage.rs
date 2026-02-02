//! Slot storage with lock-free free list management.
//!
//! Uses a Treiber stack for the free list, providing lock-free
//! allocation and deallocation of slots.
//!
//! # ABA Protection
//!
//! The free list head uses a tagged pointer (index + version) to prevent ABA problems.
//! Without this, the following race can corrupt the free list:
//!
//! 1. Thread A: loads head=X, reads next=Y from slot[X]
//! 2. Thread B: allocates X, stores entry in X (entry_ptr is now a pointer)
//! 3. Thread B: deallocates X, head becomes X again
//! 4. Thread A: CAS(X, Y) succeeds (ABA - head is X again)
//! 5. Now head=Y, but slot[Y] may be occupied, causing get_next_free() to return garbage
//!
//! The version counter ensures that even if the index cycles back, the CAS will fail.

use crate::location::SlotLocation;
use crate::slot::Slot;
use crate::sync::{AtomicU32, AtomicU64, Ordering, spin_loop};

/// Sentinel value indicating an empty free list.
pub const EMPTY_FREE_LIST: u32 = u32::MAX;

/// Pack index and version into a u64 tagged pointer.
#[inline]
fn pack_head(index: u32, version: u32) -> u64 {
    ((version as u64) << 32) | (index as u64)
}

/// Unpack index and version from a u64 tagged pointer.
#[inline]
fn unpack_head(packed: u64) -> (u32, u32) {
    let index = packed as u32;
    let version = (packed >> 32) as u32;
    (index, version)
}

/// Storage for cache slots with lock-free free list management.
///
/// # Thread Safety
///
/// Uses a lock-free stack (Treiber stack) for the free list with a tagged
/// pointer (index + version) to prevent ABA problems.
/// Under very high contention, the single head pointer can become a bottleneck.
pub struct SlotStorage {
    /// All slots.
    slots: Vec<Slot>,
    /// Head of the free list. Lower 32 bits = slot index (or EMPTY_FREE_LIST),
    /// upper 32 bits = version counter for ABA protection.
    free_head: AtomicU64,
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
            free_head: AtomicU64::new(pack_head(0, 0)),
            occupied_count: AtomicU32::new(0),
        }
    }

    /// Allocate a slot from the free list.
    ///
    /// Returns the slot location (index + generation) or None if exhausted.
    ///
    /// # Thread Safety
    ///
    /// Uses compare-and-swap on the free list head with a version tag to
    /// prevent ABA problems. May retry under contention.
    pub fn allocate(&self) -> Option<SlotLocation> {
        loop {
            let packed = self.free_head.load(Ordering::Acquire);
            let (head, version) = unpack_head(packed);
            if head == EMPTY_FREE_LIST {
                return None; // No free slots
            }

            let slot = &self.slots[head as usize];

            // Read the next-free pointer. If the slot was concurrently allocated
            // (another thread won the race and stored an entry), this returns None
            // and we must retry.
            let next = match slot.get_next_free() {
                Some(n) => n,
                None => {
                    // Slot was concurrently modified, retry
                    spin_loop();
                    continue;
                }
            };

            // Try to CAS the head to the next free slot, incrementing version
            let new_packed = pack_head(next, version.wrapping_add(1));
            match self.free_head.compare_exchange_weak(
                packed,
                new_packed,
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
    /// Clears the slot (waiting for readers) then pushes to the free list
    /// with version tag to prevent ABA problems.
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

        // Push onto free list (lock-free stack push with version tag)
        loop {
            let packed = self.free_head.load(Ordering::Acquire);
            let (head, version) = unpack_head(packed);
            slot.set_next_free(head);

            // Increment version to prevent ABA
            let new_packed = pack_head(idx, version.wrapping_add(1));
            match self.free_head.compare_exchange_weak(
                packed,
                new_packed,
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

        // Reset counters - preserve version to avoid potential ABA with in-flight operations
        let packed = self.free_head.load(Ordering::Acquire);
        let (_, version) = unpack_head(packed);
        self.free_head
            .store(pack_head(0, version.wrapping_add(1)), Ordering::Release);
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

/// Loom tests for verifying the Treiber stack ABA protection.
#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use super::*;
    use crate::sync::Arc;
    use loom::thread;

    /// Test that concurrent allocations don't corrupt the free list.
    ///
    /// This test verifies that the version-tagged free list head prevents
    /// ABA problems that could cause out-of-bounds panics.
    #[test]
    fn test_concurrent_allocate() {
        loom::model(|| {
            let storage = Arc::new(SlotStorage::new(3));

            let s1 = storage.clone();
            let s2 = storage.clone();

            let t1 = thread::spawn(move || s1.allocate());

            let t2 = thread::spawn(move || s2.allocate());

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Both should get valid slots or None, no panic
            if let (Some(loc1), Some(loc2)) = (r1, r2) {
                // Different slots must be allocated
                assert_ne!(loc1.slot_index(), loc2.slot_index());
            }
        });
    }

    /// Test the ABA scenario: allocate, deallocate, allocate cycles.
    ///
    /// This is the key scenario that caused the original panic:
    /// 1. Thread A loads head=X, reads next=Y
    /// 2. Thread B allocates X, deallocates X (head becomes X again)
    /// 3. Thread A's CAS should fail due to version mismatch
    #[test]
    fn test_aba_protection() {
        loom::model(|| {
            let storage = Arc::new(SlotStorage::new(2));

            let s1 = storage.clone();
            let s2 = storage.clone();

            // Thread 1: slow allocator (simulates getting preempted)
            let t1 = thread::spawn(move || s1.allocate());

            // Thread 2: fast allocate + deallocate cycle
            let t2 = thread::spawn(move || {
                if let Some(loc) = s2.allocate() {
                    // Immediately deallocate
                    s2.deallocate(loc);
                    // And allocate again
                    s2.allocate()
                } else {
                    None
                }
            });

            // Neither thread should panic, and results should be valid
            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Both results should be valid (Some with valid index, or None)
            if let Some(loc) = r1 {
                assert!((loc.slot_index() as usize) < storage.capacity());
            }
            if let Some(loc) = r2 {
                assert!((loc.slot_index() as usize) < storage.capacity());
            }
        });
    }
}
