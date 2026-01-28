//! Slot for holding heap-allocated entries with reader tracking.
//!
//! # Thread Safety
//!
//! Uses a reader count to ensure safe memory reclamation. This is a Dekker-like
//! pattern that requires SeqCst fences between write and read on each side:
//!
//! - Readers: `readers.fetch_add(SeqCst)` → `fence(SeqCst)` → `entry_ptr.load(SeqCst)`
//! - Writers: `entry_ptr.swap(SeqCst)` → `fence(SeqCst)` → `readers.load(SeqCst)`
//!
//! The fences ensure that:
//! - If reader sees the old pointer, the writer will see the reader's increment
//! - If writer sees readers=0, the reader will see the swapped (null) pointer
//!
//! Without the fences, even SeqCst atomics can be reordered in a way that
//! breaks the Dekker invariant (verified via loom testing).
//!
//! Generation is incremented after clearing to provide ABA protection for
//! stale SlotLocation values.
//!
//! # Entry Pointer Encoding
//!
//! The `entry_ptr` field uses the low bit to distinguish between states:
//! - Bit 0 = 0: Either null (0) or a valid HeapEntry pointer (aligned, so low bits are 0)
//! - Bit 0 = 1: Free list link (remaining bits are next-free index << 1)

use std::ptr::NonNull;

use crate::entry::HeapEntry;
use crate::sync::{AtomicU32, AtomicU64, Ordering, fence, spin_loop, yield_now};

/// A single slot in the heap cache.
pub struct Slot {
    /// Number of concurrent readers accessing this slot.
    /// Writers must wait for this to reach 0 before dropping the entry.
    readers: AtomicU32,

    /// Generation counter (12 bits used). Incremented when slot is cleared.
    /// Provides ABA protection - stale SlotLocations will have wrong generation.
    generation: AtomicU32,

    /// Pointer to the entry, or free list link if low bit is set.
    /// - 0: Empty slot
    /// - Low bit 0, non-zero: Valid HeapEntry pointer
    /// - Low bit 1: Free list link ((next_index << 1) | 1)
    entry_ptr: AtomicU64,
}

impl Slot {
    /// Create a new empty slot.
    pub fn new() -> Self {
        Self {
            readers: AtomicU32::new(0),
            generation: AtomicU32::new(0),
            entry_ptr: AtomicU64::new(0),
        }
    }

    /// Get the current generation (12-bit value).
    #[inline]
    pub fn generation(&self) -> u16 {
        (self.generation.load(Ordering::Acquire) & 0xFFF) as u16
    }

    /// Increment generation (wraps at 12 bits).
    #[inline]
    fn increment_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Try to read the entry if generation matches.
    ///
    /// Returns None if:
    /// - Slot is empty
    /// - Generation mismatch (stale location)
    /// - Entry is expired (unless allow_expired)
    /// - Entry is deleted (unless allow_deleted)
    ///
    /// # Thread Safety
    ///
    /// Increments reader count before accessing entry_ptr, ensuring the
    /// entry cannot be dropped while we're reading it.
    pub fn get(&self, expected_gen: u16, allow_expired: bool) -> Option<&HeapEntry> {
        self.get_with_flags(expected_gen, allow_expired, false)
    }

    /// Try to read the entry, optionally allowing deleted entries.
    ///
    /// # Parameters
    /// - `expected_gen`: Expected generation number
    /// - `allow_expired`: If true, return expired entries
    /// - `allow_deleted`: If true, return deleted entries
    pub fn get_with_flags(
        &self,
        expected_gen: u16,
        allow_expired: bool,
        allow_deleted: bool,
    ) -> Option<&HeapEntry> {
        // Announce we're reading - prevents writers from dropping entry.
        // Use SeqCst for the increment.
        self.readers.fetch_add(1, Ordering::SeqCst);

        // CRITICAL: Full barrier ensures our increment is visible to other threads
        // BEFORE we load the entry_ptr. This is the Dekker pattern - without this
        // fence, a writer could see readers=0 while we see the old pointer.
        fence(Ordering::SeqCst);

        // Check generation matches
        let current_gen = self.generation();
        if current_gen != expected_gen {
            self.readers.fetch_sub(1, Ordering::Release);
            return None;
        }

        // Load the entry pointer
        let ptr = self.entry_ptr.load(Ordering::SeqCst);
        if !Self::is_entry_pointer(ptr) {
            self.readers.fetch_sub(1, Ordering::Release);
            return None;
        }

        // SAFETY: We've incremented readers, so the writer will wait for us
        // before dropping the entry.
        let entry = unsafe { &*(ptr as *const HeapEntry) };

        // Check if entry is valid
        if (!allow_deleted && entry.is_deleted()) || (!allow_expired && entry.is_expired()) {
            self.readers.fetch_sub(1, Ordering::Release);
            return None;
        }

        Some(entry)
    }

    /// Release a read hold on the entry.
    ///
    /// Must be called after `get()` returns `Some` when done accessing the entry.
    #[inline]
    pub fn release_read(&self) {
        self.readers.fetch_sub(1, Ordering::Release);
    }

    /// Store an entry in this slot.
    ///
    /// # Safety
    ///
    /// Caller must ensure the slot is not currently occupied (was just allocated
    /// from the free list).
    ///
    /// # Returns
    ///
    /// The current generation for this slot.
    pub fn store(&self, entry: NonNull<HeapEntry>) -> u16 {
        debug_assert_eq!(
            self.entry_ptr.load(Ordering::Relaxed),
            0,
            "store called on occupied slot"
        );

        let ptr = entry.as_ptr() as u64;
        self.entry_ptr.store(ptr, Ordering::Release);

        self.generation()
    }

    /// Clear the slot, dropping the entry if present.
    ///
    /// # Thread Safety
    ///
    /// Waits for all readers to finish before dropping the entry.
    /// This may spin briefly under contention.
    pub fn clear(&self) {
        // Swap out the entry pointer
        let ptr = self.entry_ptr.swap(0, Ordering::SeqCst);

        // CRITICAL: Full barrier ensures our swap is visible to other threads
        // BEFORE we load readers. This is the Dekker pattern - without this
        // fence, we could see readers=0 while a reader sees our old pointer.
        fence(Ordering::SeqCst);

        if Self::is_entry_pointer(ptr) {
            // Wait for readers to finish
            let mut spin_count = 0;
            while self.readers.load(Ordering::SeqCst) > 0 {
                spin_count += 1;
                if spin_count < 100 {
                    spin_loop();
                } else {
                    yield_now();
                }
            }

            // SAFETY: All readers have finished, safe to drop
            unsafe {
                let entry_ptr = NonNull::new_unchecked(ptr as *mut HeapEntry);
                HeapEntry::free(entry_ptr);
            }
        }

        // Always increment generation to invalidate stale locations (ABA safety)
        self.increment_generation();
    }

    /// Check if entry_ptr contains an entry pointer (not a free list link).
    #[inline]
    fn is_entry_pointer(ptr: u64) -> bool {
        // Entry pointers are aligned, so low bit is 0
        // Free list links have low bit set to 1
        // 0 is "no pointer"
        ptr != 0 && (ptr & 1) == 0
    }

    /// Check if the slot is currently occupied.
    #[inline]
    pub fn is_occupied(&self) -> bool {
        let ptr = self.entry_ptr.load(Ordering::Acquire);
        Self::is_entry_pointer(ptr)
    }

    /// Get the raw entry pointer if occupied.
    ///
    /// # Safety
    ///
    /// Caller must ensure proper synchronization (e.g., reader count increment).
    #[allow(dead_code)]
    #[inline]
    pub unsafe fn entry_ptr(&self) -> Option<NonNull<HeapEntry>> {
        let ptr = self.entry_ptr.load(Ordering::Acquire);
        if Self::is_entry_pointer(ptr) {
            NonNull::new(ptr as *mut HeapEntry)
        } else {
            None
        }
    }

    /// Get the next-free index from entry_ptr.
    /// Only valid when slot is in the free list.
    #[inline]
    pub fn get_next_free(&self) -> u32 {
        let val = self.entry_ptr.load(Ordering::Acquire);
        // Decode: remove the low bit flag, shift back
        ((val >> 1) & 0xFFFF_FFFF) as u32
    }

    /// Set the next free pointer (encodes with low bit = 1).
    /// Only valid when slot is not occupied.
    #[inline]
    pub fn set_next_free(&self, next: u32) {
        // Encode: shift left and set low bit to mark as free list link
        let encoded = ((next as u64) << 1) | 1;
        self.entry_ptr.store(encoded, Ordering::Release);
    }

    /// Clear the entry pointer to make room for store().
    /// Used after allocating from free list.
    #[inline]
    pub fn clear_for_store(&self) {
        self.entry_ptr.store(0, Ordering::Release);
    }
}

impl Default for Slot {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        // No need to wait for readers in drop - we have exclusive access
        let ptr = self.entry_ptr.load(Ordering::Relaxed);
        if Self::is_entry_pointer(ptr) {
            unsafe {
                let entry_ptr = NonNull::new_unchecked(ptr as *mut HeapEntry);
                HeapEntry::free(entry_ptr);
            }
        }
    }
}

// SAFETY: Slot is Send because:
// - All fields use atomic operations
// - HeapEntry pointers are only accessed with proper synchronization
unsafe impl Send for Slot {}

// SAFETY: Slot is Sync because:
// - All access is through atomic operations
// - Reader counting ensures safe concurrent access
unsafe impl Sync for Slot {}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_new_slot() {
        let slot = Slot::new();
        assert_eq!(slot.generation(), 0);
        assert!(!slot.is_occupied());
    }

    #[test]
    fn test_store_and_get() {
        let slot = Slot::new();
        let entry = HeapEntry::allocate(b"key", b"value", Duration::ZERO, 1).unwrap();

        let generation = slot.store(entry);
        assert_eq!(generation, 0);
        assert!(slot.is_occupied());

        let retrieved = slot.get(generation, false);
        assert!(retrieved.is_some());
        let entry_ref = retrieved.unwrap();
        assert_eq!(entry_ref.key(), b"key");
        assert_eq!(entry_ref.value(), b"value");
        slot.release_read();

        // Wrong generation should fail
        assert!(slot.get(generation + 1, false).is_none());
    }

    #[test]
    fn test_clear_increments_generation() {
        let slot = Slot::new();
        let entry = HeapEntry::allocate(b"key", b"value", Duration::ZERO, 1).unwrap();

        let gen0 = slot.store(entry);
        assert_eq!(gen0, 0);

        slot.clear();
        assert!(!slot.is_occupied());

        // Generation should have incremented
        assert_eq!(slot.generation(), 1);
    }

    #[test]
    fn test_free_list_encoding() {
        let slot = Slot::new();

        slot.set_next_free(12345);
        assert_eq!(slot.get_next_free(), 12345);

        slot.set_next_free(u32::MAX);
        assert_eq!(slot.get_next_free(), u32::MAX);
    }
}

/// Loom tests for verifying the Dekker-like reader/writer synchronization.
///
/// These tests verify that the SeqCst ordering is correct and that:
/// - Readers who see the old pointer are waited for by the writer
/// - Writers who swap the pointer see all readers who got the old pointer
#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use crate::sync::{Arc, AtomicU32, AtomicU64, Ordering, fence};
    use loom::thread;

    /// Test the core Dekker-like synchronization pattern with fences.
    ///
    /// This test verifies that if a reader sees a non-zero pointer,
    /// the writer will see the reader count > 0 (and must wait).
    ///
    /// The key is the SeqCst fence between the write and read on each side:
    /// - Reader: increment → fence → load ptr
    /// - Writer: swap → fence → load readers
    ///
    /// This ensures that if reader sees old ptr, writer sees the increment.
    ///
    /// NOTE: The reader does NOT decrement in this test - we're testing
    /// the invariant at the moment when both sides have done their operations.
    /// In real code, the writer would wait for readers to become 0.
    #[test]
    fn test_reader_writer_synchronization() {
        loom::model(|| {
            let readers = Arc::new(AtomicU32::new(0));
            let ptr_value = Arc::new(AtomicU64::new(0x1000));
            // Shared state to record what each thread saw
            let reader_result = Arc::new(AtomicU64::new(0));
            let writer_result = Arc::new(AtomicU32::new(0));

            let r_readers = readers.clone();
            let r_ptr = ptr_value.clone();
            let r_result = reader_result.clone();

            // Reader: announce, fence, load ptr (NO decrement - simulating "in use")
            let reader = thread::spawn(move || {
                // Step 1: Announce reading
                r_readers.fetch_add(1, Ordering::SeqCst);

                // Step 2: FENCE - ensures increment is visible before we load ptr
                fence(Ordering::SeqCst);

                // Step 3: Load pointer
                let ptr = r_ptr.load(Ordering::SeqCst);

                // Record what we saw (no decrement - we're "holding" the entry)
                r_result.store(ptr, Ordering::SeqCst);
            });

            let w_readers = readers.clone();
            let w_ptr = ptr_value.clone();
            let w_result = writer_result.clone();

            // Writer: swap, fence, load readers
            let writer = thread::spawn(move || {
                // Step 1: Swap pointer to 0
                w_ptr.swap(0, Ordering::SeqCst);

                // Step 2: FENCE - ensures swap is visible before we load readers
                fence(Ordering::SeqCst);

                // Step 3: Check readers
                let reader_count = w_readers.load(Ordering::SeqCst);

                // Record what we saw
                w_result.store(reader_count, Ordering::SeqCst);
            });

            reader.join().unwrap();
            writer.join().unwrap();

            let reader_saw = reader_result.load(Ordering::SeqCst);
            let writer_saw_readers = writer_result.load(Ordering::SeqCst);

            // The Dekker invariant:
            // If reader saw ptr (0x1000), the reader's increment happened before
            // the reader's load. The fence ensures this increment is visible
            // to the writer before the writer's load (since writer's swap is
            // before writer's load due to writer's fence).
            //
            // If writer saw readers=0, it means the reader's increment hasn't
            // happened yet. But then the writer's swap (which is before the
            // writer's load of readers) is also visible, so the reader's load
            // (which is after reader's increment by reader's fence) will see 0.
            //
            // Therefore: (reader sees 0x1000) AND (writer sees readers=0) is impossible.

            if reader_saw != 0 && writer_saw_readers == 0 {
                panic!(
                    "Dekker violation: reader saw ptr=0x{:x} but writer saw readers=0",
                    reader_saw
                );
            }

            // In all valid outcomes:
            // - reader_saw == 0 (reader sees writer's swap): SAFE, reader won't use ptr
            // - writer_saw_readers > 0 (writer sees reader): SAFE, writer waits
            // - Both: reader saw 0 AND writer saw readers > 0 is possible and safe
        });
    }

    /// Test generation counter prevents stale reads.
    ///
    /// A reader with an old generation should fail to read even if the slot
    /// is reused with a new entry.
    #[test]
    fn test_generation_prevents_stale_read() {
        loom::model(|| {
            let generation = Arc::new(AtomicU32::new(0));
            let entry_ptr = Arc::new(AtomicU64::new(0x1000));

            let r_gen = generation.clone();
            let r_ptr = entry_ptr.clone();

            // Reader: tries to read with expected_gen = 0
            let reader = thread::spawn(move || {
                let expected_gen = 0u32;
                let current_gen = r_gen.load(Ordering::Acquire);

                if current_gen != expected_gen {
                    return None; // Generation mismatch - stale location
                }

                let ptr = r_ptr.load(Ordering::SeqCst);
                if ptr != 0 { Some(ptr) } else { None }
            });

            let w_gen = generation.clone();
            let w_ptr = entry_ptr.clone();

            // Writer: clears and increments generation
            let writer = thread::spawn(move || {
                w_ptr.swap(0, Ordering::SeqCst);
                w_gen.fetch_add(1, Ordering::Release);
            });

            let read_result = reader.join().unwrap();
            writer.join().unwrap();

            // Valid outcomes:
            // - Reader got Some(0x1000) if it read before writer
            // - Reader got None if writer cleared first or generation mismatched
            // Either way, generation should be 1 now
            assert_eq!(generation.load(Ordering::Acquire), 1);
            let _ = read_result;
        });
    }

    /// Test concurrent free list operations (Treiber stack pop).
    #[test]
    fn test_free_list_concurrent_pop() {
        loom::model(|| {
            // Simple free list: head -> slot0 -> slot1 -> EMPTY
            let free_head = Arc::new(AtomicU32::new(0));
            let slot0_next = Arc::new(AtomicU32::new(1));
            let _slot1_next = Arc::new(AtomicU32::new(u32::MAX)); // EMPTY sentinel

            let fh1 = free_head.clone();
            let s0_1 = slot0_next.clone();

            // Thread 1: tries to pop
            let t1 = thread::spawn(move || {
                let head = fh1.load(Ordering::Acquire);
                if head == u32::MAX {
                    return None;
                }
                let next = if head == 0 {
                    s0_1.load(Ordering::Acquire)
                } else {
                    u32::MAX
                };
                match fh1.compare_exchange(head, next, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => Some(head),
                    Err(_) => None, // Lost race
                }
            });

            let fh2 = free_head.clone();
            let s0_2 = slot0_next.clone();

            // Thread 2: also tries to pop
            let t2 = thread::spawn(move || {
                let head = fh2.load(Ordering::Acquire);
                if head == u32::MAX {
                    return None;
                }
                let next = if head == 0 {
                    s0_2.load(Ordering::Acquire)
                } else {
                    u32::MAX
                };
                match fh2.compare_exchange(head, next, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => Some(head),
                    Err(_) => None,
                }
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // At most one should succeed with slot 0
            if let (Some(a), Some(b)) = (r1, r2) {
                assert_ne!(a, b, "both got same slot");
            }
        });
    }
}
