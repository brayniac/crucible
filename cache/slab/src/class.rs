//! Slab class management with free list.
//!
//! Each slab class manages slots of a fixed size. Slabs are allocated from
//! a shared heap and divided into equal-sized slots.

use std::ptr;
use std::time::Duration;

use crate::sync::{AtomicPtr, AtomicU32, AtomicU64, Ordering};

use crossbeam_deque::Injector;
use parking_lot::RwLock;

use crate::config::HEADER_SIZE;
use crate::item::{SlabItemHeader, now_secs};

/// Maximum number of slabs per class. Sizes the lock-free `slab_ptrs` and
/// `slab_states` arrays and must equal `MAX_SLAB_ID + 1`, the 16-bit slab_id
/// field of a location. A 64GB heap of 1MB slabs is 65,536 slabs, all of
/// which can land in one class. Each class pre-allocates 24 bytes per entry
/// (8-byte slab pointer, 4-byte state, 8-byte slot-pin array pointer, 4-byte
/// generation): 1.5MB per class, 96MB for 64 classes.
const MAX_SLABS_PER_CLASS: usize = 65536;
const _: () = assert!(MAX_SLABS_PER_CLASS == crate::location::MAX_SLAB_ID as usize + 1);

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
    #[allow(dead_code)]
    #[inline]
    pub fn is_readable(self) -> bool {
        matches!(self, SlabState::Live)
    }
}

/// Packed state + ref_count operations.
/// Format: [state: 8 bits][ref_count: 24 bits]
pub(crate) mod packed_state {
    use super::SlabState;
    use crate::sync::{AtomicU32, Ordering};

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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
#[allow(dead_code)]
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
    /// Position in the order slabs were added, process-wide. LRC orders by
    /// it, LRA breaks last-access ties with it, and `evict_slab` compares it
    /// to reject a victim whose id has been reused. The slab id cannot serve,
    /// because evicted ids are reused.
    sequence: u64,
    /// Class ID this slab belongs to.
    class_id: u8,
    /// Slab ID within the class.
    slab_id: u32,
}

/// Source of `Slab::sequence`.
static SLAB_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[allow(dead_code)]
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
            sequence: SLAB_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
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
    pub unsafe fn slot_ptr(&self, slot_index: u32, slot_size: usize) -> *mut u8 {
        // SAFETY: Caller ensures slot_index * slot_size < self.size
        unsafe { self.data.add(slot_index as usize * slot_size) }
    }

    /// Get the item header at a specific slot.
    ///
    /// # Safety
    ///
    /// The caller must ensure the slot contains a valid item.
    #[inline]
    pub unsafe fn header(&self, slot_index: u32, slot_size: usize) -> &SlabItemHeader {
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

/// Per-slot pin word.
///
/// A reader pins the slot it reads as well as holding a slab reference. A
/// freed slot goes back on the free list only once no pins remain, so a
/// pinned slot is not rewritten. The low 29 bits count pins; the high three
/// bits are the state.
///
/// ```text
/// on free list:  FREED | ON_FREE_LIST   allocate claims it
/// writing:       WRITING                written, then published
/// live:          0                      readers pin and unpin
/// retired:       FREED                  pushed by whoever drops count to 0
/// ```
///
/// The left column is the state bits. Outside `live`, a nonzero count comes
/// from a `pin_slot` that saw the state and is about to unpin.
///
/// A reader that pins a slot in `WRITING` or `FREED` state unpins at once.
/// Exactly one thread moves a retired slot to `ON_FREE_LIST` and pushes it:
/// the freer if no reader holds it, otherwise the last reader to unpin.
pub(crate) mod slot_pin {
    /// Readers holding the slot.
    pub const COUNT_MASK: u32 = (1 << 29) - 1;
    /// Claimed by a writer and not yet published.
    pub const WRITING: u32 = 1 << 29;
    /// Retired: `pin_slot` fails; pushed when the count reaches zero.
    pub const FREED: u32 = 1 << 30;
    /// Pushed to the class free list.
    pub const ON_FREE_LIST: u32 = 1 << 31;
}

/// Pack a free-slot entry: slab id (16 bits), the slab's generation when the
/// entry was pushed (low 28 bits), slot index (20 bits).
#[inline]
fn pack_free_slot(slab_id: u32, generation: u32, slot_index: u32) -> u64 {
    ((slab_id as u64) << 48)
        | (((generation & FREE_SLOT_GENERATION_MASK) as u64) << 20)
        | slot_index as u64
}

/// Unpack a free-slot entry into (slab id, generation, slot index).
#[inline]
fn unpack_free_slot(packed: u64) -> (u32, u32, u32) {
    (
        (packed >> 48) as u32,
        ((packed >> 20) as u32) & FREE_SLOT_GENERATION_MASK,
        (packed as u32) & crate::location::MAX_SLOT_INDEX,
    )
}

const FREE_SLOT_GENERATION_MASK: u32 = (1 << 28) - 1;
const _: () = assert!(crate::location::SLOT_INDEX_BITS == 20);

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
    /// Per-slab array of `slots_per_slab` slot pin words (see `slot_pin`),
    /// from `Box::into_raw`. Null when the slab is not in this class.
    slot_pins: Box<[AtomicPtr<AtomicU32>]>,
    /// Free slot queue; entries are packed by `pack_free_slot`.
    free_slots: Injector<u64>,
    /// Ids of slabs evicted from this class, for `add_slab` to reuse.
    free_slab_ids: parking_lot::Mutex<Vec<u32>>,
    /// Bumped each time a slab id is (re)used by `add_slab`. A free-slot
    /// entry carries the generation it was pushed under, and `allocate`
    /// discards an entry left over from an evicted slab whose id was reused.
    /// The slot pin already stops such an entry handing a slot out twice;
    /// discarding it keeps every popped entry naming a free slot, which
    /// `claim_slot` asserts.
    slab_generations: Box<[AtomicU32]>,
    /// Number of allocated slabs (atomic for lock-free reads).
    slab_count: AtomicU32,
    /// Number of items in this class.
    item_count: AtomicU64,
    /// Total bytes used by items in this class.
    bytes_used: AtomicU64,
}

/// One live item handed to `SlabClass::evict_slab`'s callback.
///
/// Borrows directly out of the slab, which is sound only because the drain
/// has already taken the slab to `Locked` with `ref_count == 0`.
pub struct EvictedItem<'a> {
    /// The item's key.
    pub key: &'a [u8],
    /// The item's value.
    pub value: &'a [u8],
    /// Time left before expiry, or `None` if it has already expired.
    pub remaining_ttl: Option<Duration>,
    /// Slab class this item lives in.
    pub class_id: u8,
    /// Slab within the class.
    pub slab_id: u32,
    /// Slot within the slab.
    pub slot_index: u32,
}

impl SlabClass {
    /// Create a new slab class.
    pub fn new(class_id: u8, slot_size: usize, slab_size: usize) -> Self {
        assert!(
            slab_size / slot_size <= crate::location::MAX_SLOT_INDEX as usize + 1,
            "{} slots per slab exceeds the {} a location can address",
            slab_size / slot_size,
            crate::location::MAX_SLOT_INDEX as usize + 1
        );

        // Pre-allocate lock-free arrays
        let slab_ptrs: Box<[AtomicPtr<u8>]> = (0..MAX_SLABS_PER_CLASS)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect();

        // Initialize all states to Unallocated (0)
        let slab_states: Box<[AtomicU32]> = (0..MAX_SLABS_PER_CLASS)
            .map(|_| AtomicU32::new(0))
            .collect();

        let slot_pins: Box<[AtomicPtr<AtomicU32>]> = (0..MAX_SLABS_PER_CLASS)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect();

        let slab_generations: Box<[AtomicU32]> = (0..MAX_SLABS_PER_CLASS)
            .map(|_| AtomicU32::new(0))
            .collect();

        Self {
            class_id,
            slot_size,
            slots_per_slab: slab_size / slot_size,
            slabs: RwLock::new(Vec::new()),
            slab_ptrs,
            slab_states,
            slot_pins,
            free_slots: Injector::new(),
            free_slab_ids: parking_lot::Mutex::new(Vec::new()),
            slab_generations,
            slab_count: AtomicU32::new(0),
            item_count: AtomicU64::new(0),
            bytes_used: AtomicU64::new(0),
        }
    }

    /// Get the class ID.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    #[inline]
    pub fn slots_per_slab(&self) -> usize {
        self.slots_per_slab
    }

    /// Get the number of allocated slabs.
    #[inline]
    pub fn slab_count(&self) -> usize {
        self.slab_count.load(Ordering::Relaxed) as usize
    }

    /// Check if a slab is in Live state (readable).
    ///
    /// This is a quick check that doesn't acquire a reference. Use this
    /// before calling methods that access slab data (like `header()` or
    /// `slot_ptr()`) to avoid reading from evicted slabs.
    ///
    /// Note: This check is racy - the slab could be evicted immediately after
    /// this returns true. For safe access, use `acquire_slab()` instead.
    #[inline]
    pub fn is_slab_live(&self, slab_id: u32) -> bool {
        if (slab_id as usize) >= MAX_SLABS_PER_CLASS {
            return false;
        }
        let state_packed = self.slab_states[slab_id as usize].load(Ordering::Acquire);
        let (state, _) = packed_state::unpack(state_packed);
        state == SlabState::Live
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
    /// Returns (slab_id, last_accessed, sequence) for each Live slab, where
    /// `sequence` is the slab's place in the order slabs were added.
    /// Evicted slabs (state != Live) are filtered out.
    pub fn slab_timestamps(&self) -> Vec<(u32, u32, u64)> {
        let slabs = self.slabs.read();
        slabs
            .iter()
            .enumerate()
            .filter(|(id, _)| {
                // Only include slabs that are still Live (not evicted)
                let state_packed = self.slab_states[*id].load(Ordering::Acquire);
                let (state, _) = packed_state::unpack(state_packed);
                state == SlabState::Live
            })
            .map(|(id, slab)| (id as u32, slab.last_accessed(), slab.sequence))
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
            // Reuse the id of an evicted slab before taking a new one, so ids
            // stay below MAX_SLABS_PER_CLASS however often slabs turn over.
            let reused = self.free_slab_ids.lock().pop();
            let slab_id = reused.unwrap_or(slabs.len() as u32);

            // Check we haven't exceeded the lock-free array capacity
            assert!(
                (slab_id as usize) < MAX_SLABS_PER_CLASS,
                "exceeded maximum slabs per class ({})",
                MAX_SLABS_PER_CLASS
            );
            let generation = self.slab_generations[slab_id as usize]
                .fetch_add(1, Ordering::AcqRel)
                .wrapping_add(1);

            // Initialize all slot headers as deleted BEFORE making the slab visible.
            // This ensures evict_slab() won't read uninitialized memory if it runs
            // before all slots have been allocated and written to.
            for slot_index in 0..self.slots_per_slab {
                let slot_ptr = data.add(slot_index * self.slot_size);
                SlabItemHeader::init_deleted(slot_ptr);
            }

            // Store the pointer in the lock-free array BEFORE adding to slabs Vec.
            // This ensures the pointer is visible to readers before the slab_id is used.
            self.slab_ptrs[slab_id as usize].store(data, Ordering::Release);

            // Every slot starts on the free list.
            let pins: Box<[AtomicU32]> = (0..self.slots_per_slab)
                .map(|_| AtomicU32::new(slot_pin::FREED | slot_pin::ON_FREE_LIST))
                .collect();
            let old = self.slot_pins[slab_id as usize]
                .swap(Box::into_raw(pins) as *mut AtomicU32, Ordering::Release);
            self.drop_pins_ptr(old);

            // Set state to Live so readers can acquire references
            packed_state::set_live(&self.slab_states[slab_id as usize]);

            // Create the slab with class_id and slab_id for tracking
            let slab = Slab::new(data, slab_size, self.class_id, slab_id);
            if reused.is_some() {
                slabs[slab_id as usize] = slab;
            } else {
                slabs.push(slab);
            }

            // Update the atomic slab count
            self.slab_count.fetch_add(1, Ordering::Release);

            // Add all slots to the free list
            for slot_index in 0..self.slots_per_slab {
                let packed = pack_free_slot(slab_id, generation, slot_index as u32);
                self.free_slots.push(packed);
            }

            slab_id
        }
    }

    /// Try to allocate a slot from the free list for writing.
    ///
    /// Returns `Some((slab_id, slot_index))` if successful, `None` if no free slots.
    /// Increments the slab's ref_count and claims the slot (`WRITING`). The
    /// caller publishes it with `publish_slot` or frees it with `free_slot`,
    /// then calls `release_slab`.
    ///
    /// The free_slots queue can hold entries for slabs that have since been
    /// evicted: entries for an `Unallocated` slab, or entries carrying the
    /// generation of an evicted slab whose id has been reused. Those are
    /// discarded.
    pub fn allocate(&self) -> Option<(u32, u32)> {
        loop {
            match self.free_slots.steal() {
                crossbeam_deque::Steal::Success(packed) => {
                    let (slab_id, generation, slot_index) = unpack_free_slot(packed);

                    // Try to acquire a reference to the slab atomically.
                    // This both checks that the slab is Live AND increments ref_count,
                    // preventing eviction from proceeding while we write to the slot.
                    // If the slab is not Live (evicted or draining), try_acquire fails.
                    if !packed_state::try_acquire(&self.slab_states[slab_id as usize]) {
                        // Slab is not Live (evicted or draining), discard and try again
                        continue;
                    }
                    // An entry from an evicted slab whose id has been reused.
                    if generation != self.slab_generation(slab_id) {
                        self.release_slab(slab_id);
                        continue;
                    }
                    if self.claim_slot(slab_id, slot_index) {
                        return Some((slab_id, slot_index));
                    }
                    // Not a free slot: discard it.
                    self.release_slab(slab_id);
                    continue;
                }
                crossbeam_deque::Steal::Empty => return None,
                crossbeam_deque::Steal::Retry => continue,
            }
        }
    }

    /// The generation of the slab currently using `slab_id`, as packed into
    /// free-slot entries.
    #[inline]
    fn slab_generation(&self, slab_id: u32) -> u32 {
        self.slab_generations[slab_id as usize].load(Ordering::Acquire) & FREE_SLOT_GENERATION_MASK
    }

    /// The pin word for a slot, or `None` if the slab is not in this class.
    ///
    /// The caller must hold a reference on the slab, which keeps the pin
    /// array from being freed.
    #[inline]
    fn pin_word(&self, slab_id: u32, slot_index: u32) -> Option<&AtomicU32> {
        let pins = self
            .slot_pins
            .get(slab_id as usize)?
            .load(Ordering::Acquire);
        if pins.is_null() || slot_index as usize >= self.slots_per_slab {
            return None;
        }
        // SAFETY: `pins` points to `slots_per_slab` words, kept alive by the
        // caller's slab reference.
        Some(unsafe { &*pins.add(slot_index as usize) })
    }

    /// Take a slot popped from the free list for writing. Returns `false`
    /// if the slot is not on the free list.
    fn claim_slot(&self, slab_id: u32, slot_index: u32) -> bool {
        let Some(pin) = self.pin_word(slab_id, slot_index) else {
            return false;
        };
        let free = slot_pin::FREED | slot_pin::ON_FREE_LIST;
        let mut cur = pin.load(Ordering::Acquire);
        loop {
            if cur & !slot_pin::COUNT_MASK != free {
                debug_assert!(
                    false,
                    "slot {slab_id}/{slot_index} on free list in state {cur:#x}"
                );
                return false;
            }
            // A nonzero count is a `pin_slot` that saw `FREED` and will
            // unpin without reading. Keeping it in the word lets that unpin
            // see `WRITING` and do nothing.
            let new = slot_pin::WRITING | (cur & slot_pin::COUNT_MASK);
            match pin.compare_exchange_weak(cur, new, Ordering::Acquire, Ordering::Acquire) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Make a written slot readable. Called after the item is fully written
    /// and before its location is published in the hashtable.
    pub fn publish_slot(&self, slab_id: u32, slot_index: u32) {
        if let Some(pin) = self.pin_word(slab_id, slot_index) {
            let prev = pin.fetch_and(!slot_pin::WRITING, Ordering::Release);
            debug_assert!(
                prev & slot_pin::WRITING != 0,
                "publishing an unclaimed slot"
            );
        }
    }

    /// Hold a live slot for reading. Returns `false` if the slot is being
    /// written or has been freed.
    ///
    /// The caller must hold a reference on the slab, and must call
    /// `unpin_slot` before releasing it. A pinned slot is not reused, but it
    /// may hold a different key than the one the caller looked up.
    pub fn pin_slot(&self, slab_id: u32, slot_index: u32) -> bool {
        let Some(pin) = self.pin_word(slab_id, slot_index) else {
            return false;
        };
        let prev = pin.fetch_add(1, Ordering::Acquire);
        debug_assert!(prev & slot_pin::COUNT_MASK < slot_pin::COUNT_MASK);
        if prev & (slot_pin::WRITING | slot_pin::FREED) != 0 {
            self.unpin_slot(slab_id, slot_index);
            return false;
        }
        true
    }

    /// Release a hold taken by `pin_slot`. If the slot was freed while held
    /// and this was the last hold, pushes it to the free list.
    pub fn unpin_slot(&self, slab_id: u32, slot_index: u32) {
        let Some(pin) = self.pin_word(slab_id, slot_index) else {
            return;
        };
        let prev = pin.fetch_sub(1, Ordering::Release);
        if prev & slot_pin::COUNT_MASK == 1
            && prev & (slot_pin::FREED | slot_pin::ON_FREE_LIST) == slot_pin::FREED
        {
            self.push_freed_slot(pin, slab_id, slot_index);
        }
    }

    /// Retire a slot whose item is no longer reachable from the hashtable.
    /// It goes on the free list once no reader holds it.
    ///
    /// The caller must hold a reference on the slab, and calls this at most
    /// once per `allocate` of the slot.
    pub fn free_slot(&self, slab_id: u32, slot_index: u32) {
        let Some(pin) = self.pin_word(slab_id, slot_index) else {
            return;
        };
        let mut cur = pin.load(Ordering::Relaxed);
        loop {
            debug_assert!(cur & slot_pin::FREED == 0, "slot freed twice");
            let new = (cur & !slot_pin::WRITING) | slot_pin::FREED;
            match pin.compare_exchange_weak(cur, new, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => {
                    if new & slot_pin::COUNT_MASK == 0 {
                        self.push_freed_slot(pin, slab_id, slot_index);
                    }
                    return;
                }
                Err(actual) => cur = actual,
            }
        }
    }

    /// Push a retired slot with no readers, unless another thread already
    /// has or a reader has since pinned it (that reader's unpin pushes it).
    fn push_freed_slot(&self, pin: &AtomicU32, slab_id: u32, slot_index: u32) {
        if pin
            .compare_exchange(
                slot_pin::FREED,
                slot_pin::FREED | slot_pin::ON_FREE_LIST,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.free_slots.push(pack_free_slot(
                slab_id,
                self.slab_generation(slab_id),
                slot_index,
            ));
        }
    }

    /// Free a pin array taken from `slot_pins`.
    fn drop_pins_ptr(&self, pins: *mut AtomicU32) {
        if !pins.is_null() {
            // SAFETY: `pins` came from `Box::into_raw` of a slice of
            // `slots_per_slab` words in `add_slab`. Callers swap it out of
            // `slot_pins` once the slab is `Locked` with no references
            // (`evict_slab`) or the class is being dropped. `reset` also
            // frees it, and requires that no read or write is in flight.
            drop(unsafe {
                Box::from_raw(ptr::slice_from_raw_parts_mut(pins, self.slots_per_slab))
            });
        }
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

    /// The packed state + ref_count word of a slab.
    #[inline]
    pub fn state_word(&self, slab_id: u32) -> &AtomicU32 {
        &self.slab_states[slab_id as usize]
    }

    /// Get a slab by ID.
    #[allow(dead_code)]
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
    pub unsafe fn slot_ptr(&self, slab_id: u32, slot_index: u32) -> *mut u8 {
        unsafe {
            // Lock-free read from the atomic pointer array
            let slab_ptr = self.slab_ptrs[slab_id as usize].load(Ordering::Acquire);
            debug_assert!(!slab_ptr.is_null(), "slab {} not initialized", slab_id);
            slab_ptr.add(slot_index as usize * self.slot_size)
        }
    }

    /// Get the item header at a specific location.
    ///
    /// This is lock-free on the hot path.
    ///
    /// # Safety
    ///
    /// Caller must ensure the slot contains a valid item.
    #[inline]
    pub unsafe fn header(&self, slab_id: u32, slot_index: u32) -> &SlabItemHeader {
        unsafe {
            // SAFETY: Caller ensures slot contains valid item
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

    /// How long a drain waits for the slab's references to be released
    /// before it is aborted. References are held by readers copying or
    /// holding a value (a zero-copy send to a slow client holds one for the
    /// whole send) and by writers filling a slot, so eviction does not wait
    /// for them; the caller tries another slab.
    const DRAIN_WAIT: std::time::Duration = std::time::Duration::from_micros(20);

    /// Evict all items from a specific slab.
    ///
    /// Calls the provided callback for each evicted item with (key, class_id, slab_id, slot_index).
    /// The callback should remove the item from the hashtable.
    ///
    /// Returns the slab's data pointer so it can be returned to the global free pool,
    /// or `None` if:
    /// - The slab doesn't exist
    /// - The slab is already being drained
    /// - `sequence` is given and the slab now using `slab_id` has a different
    ///   `Slab::sequence`
    /// - A reference taken before the drain began was still held after
    ///   `DRAIN_WAIT` (drain aborted)
    ///
    /// # Safety
    ///
    /// The returned pointer must only be used to return the slab to the allocator's
    /// free pool. The slab should not be used by this class after eviction.
    /// Drain a slab, calling `on_evict` for each live item before it is
    /// delete-marked.
    ///
    /// The callback receives the item's value and remaining TTL as well as its
    /// coordinates, so a caller with a disk tier can demote rather than
    /// discard. `remaining_ttl` is `None` for an item that has already expired.
    ///
    /// The callback runs in `Locked` state, after phase 2 has waited for
    /// `ref_count` to reach zero, so there are no concurrent readers and the
    /// borrowed `key`/`value` slices are stable for its duration.
    ///
    /// With `sequence`, evicts only if the slab now using `slab_id` has that
    /// `Slab::sequence`: a victim chosen earlier whose id has since been
    /// given to a new slab is left alone, without starting a drain on it.
    pub unsafe fn evict_slab<F>(
        &self,
        slab_id: u32,
        sequence: Option<u64>,
        mut on_evict: F,
    ) -> Option<*mut u8>
    where
        F: FnMut(EvictedItem<'_>),
    {
        if (slab_id as usize) >= MAX_SLABS_PER_CLASS {
            return None;
        }

        // Phase 1: Transition Live → Draining (blocks new readers)
        let state_atom = &self.slab_states[slab_id as usize];
        {
            // `add_slab` sets a reused id Live and replaces `slabs[slab_id]`
            // under the write lock, so while this read lock is held a Live
            // state belongs to the slab whose sequence is compared here. The
            // check comes before the drain: a drain started on the wrong slab
            // and then aborted would make `allocate` discard that slab's
            // queued free slots.
            let slabs = self.slabs.read();
            if let Some(sequence) = sequence
                && slabs.get(slab_id as usize).map(|slab| slab.sequence) != Some(sequence)
            {
                return None;
            }
            if !packed_state::try_start_drain(state_atom) {
                // Slab is not Live (already draining, locked, or unallocated)
                return None;
            }
        }

        // Phase 2: Wait up to `DRAIN_WAIT` for references taken before the
        // drain began to be released. If one is still held, the drain is
        // aborted and the slab returns to Live. Free-slot entries `allocate`
        // popped meanwhile are lost, and items superseded meanwhile stay
        // unretired, until the slab is evicted.
        let deadline = std::time::Instant::now() + Self::DRAIN_WAIT;
        let mut spins = 0u32;
        while !packed_state::is_drain_complete(state_atom) {
            spins = spins.wrapping_add(1);
            if spins.is_multiple_of(64) && std::time::Instant::now() >= deadline {
                packed_state::abort_drain(state_atom);
                return None;
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
            let slot_index = slot_index as u32;

            // SAFETY: slot_index is valid, slab memory is stable, no concurrent readers
            unsafe {
                let slot_ptr = slab_ptr.add(slot_index as usize * self.slot_size);
                let header = SlabItemHeader::from_ptr(slot_ptr);

                // Skip deleted/empty slots
                if header.is_deleted() {
                    continue;
                }

                // Borrowed for the callback's duration only. Safe: we hold
                // the slab in Locked state with ref_count already drained to
                // zero, so nothing can rewrite these bytes under us.
                on_evict(EvictedItem {
                    key: header.key(),
                    value: header.value(),
                    remaining_ttl: header.remaining_ttl(),
                    class_id: self.class_id,
                    slab_id,
                    slot_index,
                });

                // Update stats
                self.sub_bytes(header.item_size());
                self.remove_item();

                // Mark as deleted
                header.mark_deleted();
            }
        }

        // Phase 6: Mark slab as removed from this class (Locked → Unallocated).
        // The slab will be returned to the global pool and may be assigned
        // to a different class, and its id given to the next slab this
        // class adds. Locations in this slab can reach the slab that reuses
        // the id by three routes: an `ItemPin` or write ref, which this
        // eviction waited out; a queued free-slot entry, which `allocate`
        // discards by generation; and an eviction victim, which `evict_slab`
        // rejects by `sequence`.
        //
        // NOTE: We do NOT return slots to free_slots because the slab is
        // leaving this class entirely. The slot refs would point to memory
        // that may be reused by a different class.
        self.slab_ptrs[slab_id as usize].store(std::ptr::null_mut(), Ordering::Release);
        let pins = self.slot_pins[slab_id as usize].swap(ptr::null_mut(), Ordering::AcqRel);
        self.drop_pins_ptr(pins);
        packed_state::set_unallocated(state_atom);
        self.free_slab_ids.lock().push(slab_id);

        // Decrement slab count since this slab is leaving the class
        self.slab_count.fetch_sub(1, Ordering::Release);

        Some(slab_ptr)
    }

    /// Get the data pointer for a slab (for returning to global pool).
    #[allow(dead_code)]
    pub fn slab_data_ptr(&self, slab_id: u32) -> Option<*mut u8> {
        let slabs = self.slabs.read();
        slabs.get(slab_id as usize).map(|s| s.data())
    }

    /// Check if the class has any items.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    /// pointer, then calls `finalize_write_item` to update statistics and make
    /// the slot readable.
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
    ) -> Option<(u32, u32, *mut u8, usize)> {
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
    /// `begin_write_item`. Updates statistics (bytes_used, item_count) and
    /// makes the slot readable. The write ref taken by `allocate` is still
    /// held; the caller releases it after publishing the location.
    pub fn finalize_write_item(&self, slab_id: u32, slot_index: u32, item_size: usize) {
        self.add_bytes(item_size);
        self.add_item();
        self.publish_slot(slab_id, slot_index);
    }

    /// Cancel a two-phase write operation before `finalize_write_item`.
    ///
    /// Called if the write cannot be completed (e.g., connection closed).
    /// Marks the item as deleted, frees the slot, and releases the write
    /// ref acquired during allocation.
    pub fn cancel_write_item(&self, slab_id: u32, slot_index: u32) {
        unsafe {
            let slot_ptr = self.slot_ptr(slab_id, slot_index);
            let header = SlabItemHeader::from_ptr(slot_ptr);
            header.mark_deleted();
        }
        self.free_slot(slab_id, slot_index);
        self.release_slab(slab_id);
    }

    /// Reset this slab class, returning all slab data pointers.
    ///
    /// This clears all slabs and returns their data pointers so they can
    /// be returned to the global free pool. No read, write or `ValueRef`
    /// may be outstanding: the slot pin arrays are freed here.
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
        self.free_slab_ids.lock().clear();
        for slab_id in 0..slabs.len() {
            let pins = self.slot_pins[slab_id].swap(ptr::null_mut(), Ordering::AcqRel);
            self.drop_pins_ptr(pins);
        }
        slabs.clear();

        // Reset counters
        self.slab_count.store(0, Ordering::Release);
        self.item_count.store(0, Ordering::Release);
        self.bytes_used.store(0, Ordering::Release);

        data_ptrs
    }
}

impl Drop for SlabClass {
    fn drop(&mut self) {
        for i in 0..self.slot_pins.len() {
            let pins = self.slot_pins[i].swap(ptr::null_mut(), Ordering::Relaxed);
            self.drop_pins_ptr(pins);
        }
    }
}

/// RAII guard for slab access.
pub struct SlabRef<'a> {
    class: &'a SlabClass,
    slab_id: u32,
}

#[allow(dead_code)]
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
    pub unsafe fn slot_ptr(&self, slot_index: u32) -> *mut u8 {
        // SAFETY: Caller ensures slot index is valid
        unsafe { self.class.slot_ptr(self.slab_id, slot_index) }
    }

    /// Get the header at a slot.
    ///
    /// # Safety
    ///
    /// Caller must ensure the slot contains a valid item.
    #[inline]
    pub unsafe fn header(&self, slot_index: u32) -> &SlabItemHeader {
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
    fn test_slab_class_allocate_free() {
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

        // Free one; the write ref from `allocate` is still held.
        let (slab_id, slot_index) = allocated.pop().unwrap();
        class.free_slot(slab_id, slot_index);

        // Can allocate again
        let slot = class.allocate();
        assert!(slot.is_some());
    }

    /// A free-slot entry left queued by an evicted slab is not taken for a
    /// slot of the slab that reuses its id, and every slot is handed out
    /// once. Without the generation check the stale entry claims the slot,
    /// the new slab's own entry for it then finds it taken, and the debug
    /// assertion in `claim_slot` (every popped slot is free) fires.
    #[test]
    #[cfg(debug_assertions)]
    fn a_stale_free_slot_entry_is_discarded_after_id_reuse() {
        let class = SlabClass::new(0, 64, 1024);
        let slots_per_slab = 1024 / 64;
        let mut first = vec![0u8; 1024];
        let mut second = vec![0u8; 1024];

        let slab_id = unsafe { class.add_slab(first.as_mut_ptr(), 1024) };
        let taken: Vec<_> = (0..slots_per_slab)
            .map(|_| class.allocate().expect("slot"))
            .collect();
        // Free one slot, queueing its entry, and release every write ref.
        class.free_slot(taken[0].0, taken[0].1);
        for _ in &taken {
            class.release_slab(slab_id);
        }

        assert!(unsafe { class.evict_slab(slab_id, None, |_| {}) }.is_some());
        let reused = unsafe { class.add_slab(second.as_mut_ptr(), 1024) };
        assert_eq!(reused, slab_id, "the evicted id is reused");

        let mut seen = std::collections::HashSet::new();
        while let Some((id, slot)) = class.allocate() {
            assert_eq!(id, reused);
            assert!(seen.insert(slot), "slot {slot} handed out twice");
        }
        assert_eq!(seen.len(), slots_per_slab);
    }

    /// An eviction of a victim whose id has been reused leaves the new slab
    /// `Live` throughout. A drain started and then aborted on it would let a
    /// concurrent `allocate` discard its queued free slots.
    #[test]
    fn a_stale_victim_never_drains_the_slab_that_reused_its_id() {
        let class = SlabClass::new(0, 64, 1024);
        let mut first = vec![0u8; 1024];
        let mut second = vec![0u8; 1024];
        let slab_id = unsafe { class.add_slab(first.as_mut_ptr(), 1024) };
        let (_, _, sequence) = class.slab_timestamps()[0];
        assert!(unsafe { class.evict_slab(slab_id, None, |_| {}) }.is_some());
        assert_eq!(
            unsafe { class.add_slab(second.as_mut_ptr(), 1024) },
            slab_id
        );

        // Holding the write lock stops the stale eviction at its sequence
        // check; the state seen meanwhile shows whether it began a drain.
        let state = class.state_word(slab_id);
        let slabs = class.slabs.write();
        let (seen, evicted) = std::thread::scope(|s| {
            let evict =
                s.spawn(|| unsafe { class.evict_slab(slab_id, Some(sequence), |_| {}) }.is_some());
            std::thread::sleep(std::time::Duration::from_millis(50));
            let (seen, _) = packed_state::unpack(state.load(Ordering::Acquire));
            drop(slabs);
            (seen, evict.join().unwrap())
        });
        assert_eq!(seen, SlabState::Live, "the stale eviction began a drain");
        assert!(!evicted);
        let mut slots = 0;
        while class.allocate().is_some() {
            slots += 1;
        }
        assert_eq!(slots, 1024 / 64);
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

        let (slab_id, last_accessed, _sequence) = timestamps[0];
        assert_eq!(slab_id, 0);
        assert!(last_accessed > 0);

        // Touch the slab
        std::thread::sleep(std::time::Duration::from_millis(10));
        class.touch_slab(0);

        let timestamps = class.slab_timestamps();
        let (_, new_last_accessed, _) = timestamps[0];
        assert!(new_last_accessed >= last_accessed);
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

/// Loom concurrency tests for the slab state machine and concurrent operations.
#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use crate::sync::{AtomicU32, Ordering};
    use loom::sync::Arc;
    use loom::thread;

    // Use the shared packed_state implementation (now uses loom atomics when feature enabled)
    use super::SlabState;
    use super::packed_state;

    /// Test concurrent acquire operations (multiple readers).
    ///
    /// Multiple threads can successfully acquire references to the same slab.
    #[test]
    fn test_concurrent_acquire() {
        loom::model(|| {
            // Start in Live state with 0 refs
            let state = Arc::new(AtomicU32::new(packed_state::pack(SlabState::Live, 0)));

            let s1 = state.clone();
            let s2 = state.clone();

            // Thread 1: acquire
            let t1 = thread::spawn(move || packed_state::try_acquire(&s1));

            // Thread 2: acquire
            let t2 = thread::spawn(move || packed_state::try_acquire(&s2));

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Both should succeed
            assert!(r1, "Thread 1 acquire should succeed");
            assert!(r2, "Thread 2 acquire should succeed");

            // Ref count should be 2
            let (final_state, ref_count) = packed_state::unpack(state.load(Ordering::Acquire));
            assert_eq!(final_state, SlabState::Live);
            assert_eq!(ref_count, 2);
        });
    }

    /// Test acquire racing with release.
    ///
    /// One thread acquires while another releases - both should complete correctly.
    #[test]
    fn test_acquire_release_race() {
        loom::model(|| {
            // Start in Live state with 1 ref
            let state = Arc::new(AtomicU32::new(packed_state::pack(SlabState::Live, 1)));

            let s1 = state.clone();
            let s2 = state.clone();

            // Thread 1: release existing ref
            let t1 = thread::spawn(move || {
                packed_state::release(&s1);
            });

            // Thread 2: acquire new ref
            let t2 = thread::spawn(move || packed_state::try_acquire(&s2));

            t1.join().unwrap();
            let acquired = t2.join().unwrap();

            // Acquire should always succeed (state is Live)
            assert!(acquired, "Acquire should succeed on Live slab");

            // Final ref count should be 1 (started with 1, -1 release, +1 acquire)
            let (final_state, ref_count) = packed_state::unpack(state.load(Ordering::Acquire));
            assert_eq!(final_state, SlabState::Live);
            assert_eq!(ref_count, 1);
        });
    }

    /// Test acquire failing when draining.
    ///
    /// Once draining starts, new acquires should fail.
    #[test]
    fn test_acquire_fails_when_draining() {
        loom::model(|| {
            // Start in Live state
            let state = Arc::new(AtomicU32::new(packed_state::pack(SlabState::Live, 0)));

            let s1 = state.clone();
            let s2 = state.clone();

            // Thread 1: start drain
            let t1 = thread::spawn(move || packed_state::try_start_drain(&s1));

            // Thread 2: try to acquire
            let t2 = thread::spawn(move || packed_state::try_acquire(&s2));

            let drain_started = t1.join().unwrap();
            let _acquired = t2.join().unwrap();

            // Drain should succeed
            assert!(drain_started, "Drain should start");

            // The acquire result depends on ordering:
            // - If acquire runs first: succeeds, then drain fails (state has refs)
            // - If drain runs first: acquire fails
            // Since we assert drain_started is true, acquire must have failed
            // OR acquire completed before drain and drain observed the ref

            let (final_state, ref_count) = packed_state::unpack(state.load(Ordering::Acquire));

            // Valid final states:
            // 1. Draining with 0 refs (drain first, acquire failed)
            // 2. Draining with 1 ref (acquire first, then drain - acquire succeeded before drain)
            assert!(
                final_state == SlabState::Draining,
                "Should be in Draining state"
            );
            assert!(ref_count <= 1, "Ref count should be 0 or 1");
        });
    }

    /// Test drain racing with multiple releases.
    ///
    /// When draining, releases should eventually bring ref_count to 0.
    #[test]
    fn test_drain_with_releases() {
        loom::model(|| {
            // Start in Live state with 2 refs
            let state = Arc::new(AtomicU32::new(packed_state::pack(SlabState::Live, 2)));

            let s1 = state.clone();
            let s2 = state.clone();
            let s3 = state.clone();

            // Thread 1: start drain
            let t1 = thread::spawn(move || packed_state::try_start_drain(&s1));

            // Thread 2: release
            let t2 = thread::spawn(move || packed_state::release(&s2));

            // Thread 3: release
            let t3 = thread::spawn(move || packed_state::release(&s3));

            let drain_started = t1.join().unwrap();
            t2.join().unwrap();
            t3.join().unwrap();

            assert!(drain_started, "Drain should start");

            // Final state should be Draining with 0 refs
            let (final_state, ref_count) = packed_state::unpack(state.load(Ordering::Acquire));
            assert_eq!(final_state, SlabState::Draining);
            assert_eq!(ref_count, 0);

            // Now try_lock should succeed
            assert!(
                packed_state::try_lock(&state),
                "Lock should succeed after drain complete"
            );
        });
    }

    /// Test try_lock only succeeds when ref_count is 0.
    ///
    /// try_lock should fail if there are outstanding references.
    #[test]
    fn test_try_lock_requires_zero_refs() {
        loom::model(|| {
            // Start in Draining state with 1 ref
            let state = Arc::new(AtomicU32::new(packed_state::pack(SlabState::Draining, 1)));

            let s1 = state.clone();
            let s2 = state.clone();

            // Thread 1: try to lock (should fail - refs > 0)
            let t1 = thread::spawn(move || packed_state::try_lock(&s1));

            // Thread 2: release the ref
            let t2 = thread::spawn(move || packed_state::release(&s2));

            let lock1 = t1.join().unwrap();
            t2.join().unwrap();

            // First lock attempt depends on ordering
            // If lock runs before release: fails (refs = 1)
            // If lock runs after release: succeeds (refs = 0)

            let (final_state, ref_count) = packed_state::unpack(state.load(Ordering::Acquire));

            if lock1 {
                // Lock succeeded - we're now in Locked state
                assert_eq!(final_state, SlabState::Locked);
                assert_eq!(ref_count, 0);
            } else {
                // Lock failed - state should still be Draining with 0 refs
                // (release completed but lock already failed)
                assert_eq!(final_state, SlabState::Draining);
                assert_eq!(ref_count, 0);
            }
        });
    }

    /// Test abort_drain racing with release.
    ///
    /// Eviction can be aborted while refs are being released.
    #[test]
    fn test_abort_drain_race() {
        loom::model(|| {
            // Start in Draining state with 1 ref
            let state = Arc::new(AtomicU32::new(packed_state::pack(SlabState::Draining, 1)));

            let s1 = state.clone();
            let s2 = state.clone();

            // Thread 1: abort drain
            let t1 = thread::spawn(move || packed_state::abort_drain(&s1));

            // Thread 2: release
            let t2 = thread::spawn(move || packed_state::release(&s2));

            let aborted = t1.join().unwrap();
            t2.join().unwrap();

            assert!(aborted, "Abort should succeed from Draining state");

            // Final state should be Live with 0 refs
            let (final_state, ref_count) = packed_state::unpack(state.load(Ordering::Acquire));
            assert_eq!(final_state, SlabState::Live);
            assert_eq!(ref_count, 0);
        });
    }

    /// Test drain followed by lock after release.
    ///
    /// This simpler test avoids unbounded loops that cause loom to exceed branch limits.
    /// It tests the critical path: drain started, release happens, then lock succeeds.
    #[test]
    fn test_drain_then_release_then_lock() {
        loom::model(|| {
            // Start in Draining state with 1 ref (drain already started, reader active)
            let state = Arc::new(AtomicU32::new(packed_state::pack(SlabState::Draining, 1)));

            let s1 = state.clone();
            let s2 = state.clone();

            // Thread 1 (reader): release ref
            let t1 = thread::spawn(move || {
                packed_state::release(&s1);
            });

            // Thread 2 (evictor): try to lock (may fail if release hasn't happened)
            let t2 = thread::spawn(move || packed_state::try_lock(&s2));

            t1.join().unwrap();
            let lock_result = t2.join().unwrap();

            let (final_state, ref_count) = packed_state::unpack(state.load(Ordering::Acquire));

            // Either lock succeeded (final state is Locked) or it failed (still Draining with 0 refs)
            if lock_result {
                assert_eq!(final_state, SlabState::Locked);
            } else {
                // Lock failed, but release completed, so we can try again
                assert_eq!(final_state, SlabState::Draining);
                assert_eq!(ref_count, 0);
                // Second try should succeed
                assert!(packed_state::try_lock(&state));
            }
        });
    }

    /// Test concurrent start_drain attempts.
    ///
    /// Only one thread should successfully start drain.
    #[test]
    fn test_concurrent_drain_start() {
        loom::model(|| {
            let state = Arc::new(AtomicU32::new(packed_state::pack(SlabState::Live, 0)));

            let s1 = state.clone();
            let s2 = state.clone();

            // Two threads try to start drain
            let t1 = thread::spawn(move || packed_state::try_start_drain(&s1));
            let t2 = thread::spawn(move || packed_state::try_start_drain(&s2));

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Exactly one should succeed
            assert!(
                (r1 && !r2) || (!r1 && r2),
                "Exactly one drain should succeed: r1={}, r2={}",
                r1,
                r2
            );

            let (final_state, _) = packed_state::unpack(state.load(Ordering::Acquire));
            assert_eq!(final_state, SlabState::Draining);
        });
    }
}
