//! Slab class management with free list.
//!
//! Each slab class manages slots of a fixed size. Slabs are allocated from
//! a shared heap and divided into equal-sized slots.

use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};

use crossbeam_deque::Injector;
use parking_lot::RwLock;

use crate::config::HEADER_SIZE;
use crate::item::{SlabItemHeader, now_secs, pack_slot_ref, unpack_slot_ref};

/// Maximum number of slabs per class for lock-free pointer array.
/// With 1MB slabs and 64GB heap, total slabs = 65536. Since slabs can
/// concentrate in popular size classes, we need to support the full count.
/// Memory overhead: 12 bytes per slot (8 byte pointer + 4 byte state).
/// At 65536 slots = 768KB per class, acceptable for large caches.
const MAX_SLABS_PER_CLASS: usize = 65536;

/// Slab state for the state machine.
///
/// Packed with ref_count into a single AtomicU32:
/// - Bits 0-23: ref_count (max 16M concurrent readers)
/// - Bits 24-31: state
///
/// # State Transitions
///
/// ```text
///                  +------------------+
///        +-------->|   Unallocated    |<-----------------+
///        |         +--------+---------+                  |
///        |                  | add_slab()                 |
///        |                  v                            |
///        |         +------------------+                  |
///        |         |      Live        |                  |
///        |         +--------+---------+                  |
///        |                  | eviction starts            |
///        |                  v                            |
///        |         +------------------+                  |
///   (abort)        |    Draining      |                  |
///        |         +--------+---------+                  |
///        |                  | ref_count == 0             |
///        |                  v                            |
///        |         +------------------+                  |
///        +---------|     Locked       |------------------+
///                  +------------------+
///                           | eviction complete
///                           v
///                  (return to global pool)
/// ```
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlabState {
    /// Slab slot is not allocated (null pointer, available for reuse).
    Unallocated = 0,
    /// Normal operation - can be read and written.
    Live = 1,
    /// Being evicted - new readers rejected, waiting for ref_count → 0.
    Draining = 2,
    /// Eviction locked - ref_count is 0, processing items for eviction.
    /// All access is rejected during this phase.
    Locked = 3,
}

impl SlabState {
    #[inline]
    fn from_u8(v: u8) -> Self {
        match v {
            0 => SlabState::Unallocated,
            1 => SlabState::Live,
            2 => SlabState::Draining,
            3 => SlabState::Locked,
            _ => SlabState::Unallocated,
        }
    }

    /// Check if the slab is readable (allows get operations).
    #[inline]
    pub fn is_readable(self) -> bool {
        matches!(self, SlabState::Live)
    }
}

/// Packed state + ref_count operations.
/// Format: [state: 8 bits][ref_count: 24 bits]
mod packed_state {
    use super::SlabState;
    use std::sync::atomic::{AtomicU32, Ordering};

    const REF_MASK: u32 = 0x00FF_FFFF;
    const STATE_SHIFT: u32 = 24;

    #[inline]
    pub fn pack(state: SlabState, ref_count: u32) -> u32 {
        ((state as u32) << STATE_SHIFT) | (ref_count & REF_MASK)
    }

    #[inline]
    pub fn unpack(packed: u32) -> (SlabState, u32) {
        let state = SlabState::from_u8((packed >> STATE_SHIFT) as u8);
        let ref_count = packed & REF_MASK;
        (state, ref_count)
    }

    /// Try to acquire a reference (increment ref_count) if state is Live.
    /// Returns true if successful, false if slab is not readable.
    #[inline]
    pub fn try_acquire(atom: &AtomicU32) -> bool {
        loop {
            let current = atom.load(Ordering::Acquire);
            let (state, ref_count) = unpack(current);

            if state != SlabState::Live {
                return false;
            }

            let new = pack(state, ref_count + 1);
            match atom.compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    /// Release a reference (decrement ref_count).
    #[inline]
    pub fn release(atom: &AtomicU32) {
        loop {
            let current = atom.load(Ordering::Acquire);
            let (state, ref_count) = unpack(current);
            debug_assert!(ref_count > 0, "release called with zero ref_count");

            let new = pack(state, ref_count.saturating_sub(1));
            match atom.compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(_) => continue,
            }
        }
    }

    /// Try to transition from Live to Draining.
    /// Returns true if successful.
    #[inline]
    pub fn try_start_drain(atom: &AtomicU32) -> bool {
        loop {
            let current = atom.load(Ordering::Acquire);
            let (state, ref_count) = unpack(current);

            if state != SlabState::Live {
                return false;
            }

            let new = pack(SlabState::Draining, ref_count);
            match atom.compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    /// Check if draining is complete (state == Draining && ref_count == 0).
    #[inline]
    pub fn is_drain_complete(atom: &AtomicU32) -> bool {
        let current = atom.load(Ordering::Acquire);
        let (state, ref_count) = unpack(current);
        state == SlabState::Draining && ref_count == 0
    }

    /// Try to transition from Draining to Locked (when ref_count == 0).
    /// Returns true if successful.
    #[inline]
    pub fn try_lock(atom: &AtomicU32) -> bool {
        let expected = pack(SlabState::Draining, 0);
        let new = pack(SlabState::Locked, 0);
        atom.compare_exchange(expected, new, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Get current ref_count.
    #[inline]
    pub fn ref_count(atom: &AtomicU32) -> u32 {
        let (_, ref_count) = unpack(atom.load(Ordering::Acquire));
        ref_count
    }

    /// Reset from Draining to Live state (abort eviction).
    /// Returns true if successful.
    #[inline]
    pub fn abort_drain(atom: &AtomicU32) -> bool {
        loop {
            let current = atom.load(Ordering::Acquire);
            let (state, ref_count) = unpack(current);

            if state != SlabState::Draining {
                return false;
            }

            let new = pack(SlabState::Live, ref_count);
            match atom.compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    /// Reset to Live state with zero ref_count (for reuse after eviction).
    #[inline]
    pub fn reset_to_live(atom: &AtomicU32) {
        atom.store(pack(SlabState::Live, 0), Ordering::Release);
    }

    /// Set to Live state (for newly added slabs).
    #[inline]
    pub fn set_live(atom: &AtomicU32) {
        atom.store(pack(SlabState::Live, 0), Ordering::Release);
    }

    /// Set to Unallocated state (when slab is removed from class).
    #[inline]
    pub fn set_unallocated(atom: &AtomicU32) {
        atom.store(pack(SlabState::Unallocated, 0), Ordering::Release);
    }
}

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
    /// Allocated slabs (holds metadata, protected by RwLock).
    slabs: RwLock<Vec<Slab>>,
    /// Lock-free slab pointer array for hot path reads.
    /// Indexed by slab_id, stores the data pointer for each slab.
    /// Null pointer means slab not yet allocated.
    slab_ptrs: Box<[AtomicPtr<u8>]>,
    /// Lock-free packed state + ref_count for each slab.
    /// Format: [state: 8 bits][ref_count: 24 bits]
    /// Used for safe concurrent access and eviction coordination.
    slab_states: Box<[AtomicU32]>,
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
        // Pre-allocate lock-free arrays
        let slab_ptrs: Box<[AtomicPtr<u8>]> = (0..MAX_SLABS_PER_CLASS)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect();

        // Initialize all states to Unallocated (0)
        let slab_states: Box<[AtomicU32]> = (0..MAX_SLABS_PER_CLASS)
            .map(|_| AtomicU32::new(0))
            .collect();

        Self {
            class_id,
            slot_size,
            slots_per_slab: slab_size / slot_size,
            slabs: RwLock::new(Vec::new()),
            slab_ptrs,
            slab_states,
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

            // Check we haven't exceeded the lock-free array capacity
            assert!(
                (slab_id as usize) < MAX_SLABS_PER_CLASS,
                "exceeded maximum slabs per class ({})",
                MAX_SLABS_PER_CLASS
            );

            // Store the pointer in the lock-free array BEFORE adding to slabs Vec.
            // This ensures the pointer is visible to readers before the slab_id is used.
            self.slab_ptrs[slab_id as usize].store(data, Ordering::Release);

            // Set state to Live so readers can acquire references
            packed_state::set_live(&self.slab_states[slab_id as usize]);

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

    /// Try to acquire a reference to a slab for reading.
    ///
    /// Returns `true` if the slab is readable and ref_count was incremented.
    /// Returns `false` if the slab is not readable (unallocated or draining).
    ///
    /// Caller must call `release_slab()` when done reading.
    #[inline]
    pub fn acquire_slab(&self, slab_id: u32) -> bool {
        if (slab_id as usize) >= MAX_SLABS_PER_CLASS {
            return false;
        }
        packed_state::try_acquire(&self.slab_states[slab_id as usize])
    }

    /// Release a reference to a slab after reading.
    ///
    /// Must be called after a successful `acquire_slab()`.
    #[inline]
    pub fn release_slab(&self, slab_id: u32) {
        debug_assert!((slab_id as usize) < MAX_SLABS_PER_CLASS);
        packed_state::release(&self.slab_states[slab_id as usize]);
    }

    /// Get raw value reference pointers for zero-copy reads.
    ///
    /// Returns `(ref_count_ptr, value_ptr, value_len)` if successful.
    /// The ref_count has already been incremented; caller must decrement on drop
    /// (typically by constructing a `ValueRef`).
    ///
    /// Returns `None` if the slab is not readable (draining or unallocated).
    ///
    /// # Safety
    ///
    /// Caller must ensure the slot contains a valid item.
    #[inline]
    pub unsafe fn get_value_ref_raw(
        &self,
        slab_id: u32,
        slot_index: u16,
    ) -> Option<(*const AtomicU32, *const u8, usize)> {
        if (slab_id as usize) >= MAX_SLABS_PER_CLASS {
            return None;
        }

        // Acquire slab reference (increments ref_count if Live)
        if !packed_state::try_acquire(&self.slab_states[slab_id as usize]) {
            return None;
        }

        // Get value pointer and length
        let header = self.header(slab_id, slot_index);
        let value_ptr = header.value_ptr();
        let value_len = header.value_len();

        // Return pointer to the packed state (ref_count is in low 24 bits)
        // ValueRef::drop will call fetch_sub(1), which decrements only the ref_count
        let ref_count_ptr = &self.slab_states[slab_id as usize] as *const AtomicU32;

        Some((ref_count_ptr, value_ptr, value_len))
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
    /// This is lock-free on the hot path - uses the atomic slab pointer array.
    ///
    /// # Safety
    ///
    /// Caller must ensure proper synchronization and that the slab exists.
    #[inline]
    pub unsafe fn slot_ptr(&self, slab_id: u32, slot_index: u16) -> *mut u8 {
        // Lock-free read from the atomic pointer array
        let slab_ptr = self.slab_ptrs[slab_id as usize].load(Ordering::Acquire);
        debug_assert!(!slab_ptr.is_null(), "slab {} not initialized", slab_id);
        slab_ptr.add(slot_index as usize * self.slot_size)
    }

    /// Get the item header at a specific location.
    ///
    /// This is lock-free on the hot path.
    ///
    /// # Safety
    ///
    /// Caller must ensure the slot contains a valid item.
    #[inline]
    pub unsafe fn header(&self, slab_id: u32, slot_index: u16) -> &SlabItemHeader {
        // SAFETY: Caller ensures slot contains valid item
        let ptr = self.slot_ptr(slab_id, slot_index);
        SlabItemHeader::from_ptr(ptr)
    }

    /// Increment the item count.
    pub fn add_item(&self) {
        self.item_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the item count.
    pub fn remove_item(&self) {
        self.item_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Maximum iterations to wait for readers to drain before aborting eviction.
    /// At ~1µs per iteration, this is roughly 100ms max wait time.
    const MAX_DRAIN_ITERATIONS: usize = 100_000;

    /// Evict all items from a specific slab.
    ///
    /// Calls the provided callback for each evicted item with (key, class_id, slab_id, slot_index).
    /// The callback should remove the item from the hashtable.
    ///
    /// Returns the slab's data pointer so it can be returned to the global free pool,
    /// or `None` if:
    /// - The slab doesn't exist
    /// - The slab is already being drained
    /// - The drain timed out waiting for readers (eviction aborted)
    ///
    /// # Safety
    ///
    /// The returned pointer must only be used to return the slab to the allocator's
    /// free pool. The slab should not be used by this class after eviction.
    pub unsafe fn evict_slab<F>(&self, slab_id: u32, mut on_evict: F) -> Option<*mut u8>
    where
        F: FnMut(&[u8], u8, u32, u16),
    {
        if (slab_id as usize) >= MAX_SLABS_PER_CLASS {
            return None;
        }

        // Phase 1: Transition Live → Draining (blocks new readers)
        let state_atom = &self.slab_states[slab_id as usize];
        if !packed_state::try_start_drain(state_atom) {
            // Slab is not Live (already draining, locked, or unallocated)
            return None;
        }

        // Phase 2: Wait for all readers to finish (bounded wait)
        // Readers should be fast (just copying data), but we don't want to
        // spin forever if there's a livelock situation.
        let mut iterations = 0;
        while !packed_state::is_drain_complete(state_atom) {
            iterations += 1;

            if iterations > Self::MAX_DRAIN_ITERATIONS {
                // Timeout: abort eviction and reset to Live state.
                // The caller can try a different slab.
                packed_state::abort_drain(state_atom);
                return None;
            }

            if iterations % 1000 == 0 {
                // Yield periodically to avoid burning CPU
                std::thread::yield_now();
            }
            std::hint::spin_loop();
        }

        // Phase 3: Transition Draining → Locked
        // This ensures no readers can sneak in during item processing.
        if !packed_state::try_lock(state_atom) {
            // Should not happen if state machine is correct, but be safe
            return None;
        }

        // Phase 4: Get slab pointer (lock-free)
        let slab_ptr = self.slab_ptrs[slab_id as usize].load(Ordering::Acquire);
        if slab_ptr.is_null() {
            // Should not happen if state machine is correct
            packed_state::set_unallocated(state_atom);
            return None;
        }

        // Phase 5: Process each slot without holding the slabs lock.
        // Safe because we're in Locked state with exclusive access.
        for slot_index in 0..self.slots_per_slab {
            let slot_index = slot_index as u16;

            // SAFETY: slot_index is valid, slab memory is stable, no concurrent readers
            unsafe {
                let slot_ptr = slab_ptr.add(slot_index as usize * self.slot_size);
                let header = SlabItemHeader::from_ptr(slot_ptr);

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

        // Phase 6: Mark slab as removed from this class (Locked → Unallocated).
        // The slab will be returned to the global pool and may be assigned
        // to a different class. Old slot refs for this slab will fail at
        // acquire_slab() because the state is no longer Live.
        //
        // NOTE: We do NOT return slots to free_slots because the slab is
        // leaving this class entirely. The slot refs would point to memory
        // that may be reused by a different class.
        self.slab_ptrs[slab_id as usize].store(std::ptr::null_mut(), Ordering::Release);
        packed_state::set_unallocated(state_atom);

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

    /// Begin a two-phase write operation for zero-copy receive.
    ///
    /// This method:
    /// 1. Allocates a slot from the free list
    /// 2. Initializes the header with key_len, value_len, ttl
    /// 3. Copies the key to slot memory
    /// 4. Returns the location and a pointer to the value area
    ///
    /// After this call, the caller writes the value directly to the returned
    /// pointer, then calls `finalize_write_item` to update statistics.
    ///
    /// Returns `None` if no free slots are available.
    ///
    /// # Safety
    ///
    /// The returned pointer is valid until `finalize_write_item` or
    /// `cancel_write_item` is called. The caller must not hold the pointer
    /// after those calls.
    pub fn begin_write_item(
        &self,
        key: &[u8],
        value_len: usize,
        ttl: std::time::Duration,
    ) -> Option<(u32, u16, *mut u8, usize)> {
        // Allocate a slot
        let (slab_id, slot_index) = self.allocate()?;

        // Get slot pointer (lock-free)
        let slot_ptr = unsafe { self.slot_ptr(slab_id, slot_index) };

        // Initialize header
        unsafe {
            SlabItemHeader::init(slot_ptr, key.len(), value_len, ttl);
        }

        // Copy key to slot memory (immediately after header)
        unsafe {
            std::ptr::copy_nonoverlapping(key.as_ptr(), slot_ptr.add(HEADER_SIZE), key.len());
        }

        // Calculate value pointer (after header + key)
        let value_ptr = unsafe { slot_ptr.add(HEADER_SIZE + key.len()) };

        // Calculate total item size
        let item_size = HEADER_SIZE + key.len() + value_len;

        Some((slab_id, slot_index, value_ptr, item_size))
    }

    /// Finalize a two-phase write operation.
    ///
    /// Called after the value has been written to the pointer returned by
    /// `begin_write_item`. Updates statistics (bytes_used, item_count).
    pub fn finalize_write_item(&self, item_size: usize) {
        self.add_bytes(item_size);
        self.add_item();
    }

    /// Cancel a two-phase write operation.
    ///
    /// Called if the write cannot be completed (e.g., connection closed).
    /// Marks the item as deleted and returns the slot to the free list.
    pub fn cancel_write_item(&self, slab_id: u32, slot_index: u16) {
        // Mark as deleted
        unsafe {
            let slot_ptr = self.slot_ptr(slab_id, slot_index);
            let header = SlabItemHeader::from_ptr(slot_ptr);
            header.mark_deleted();
        }

        // Return slot to free list
        self.deallocate(slab_id, slot_index);
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
