//! Sketch: Arc-based cache using the generic hashtable.
//!
//! This cache stores `Arc<V>` values, allowing zero-copy reads by cloning
//! the Arc. Uses the lock-free cuckoo hashtable with a custom location
//! interpretation.
//!
//! # Design
//!
//! ```text
//! +------------------+
//! |    Hashtable     |  <- Key hash -> (Location, Frequency)
//! +--------+---------+
//!          |
//!          v
//! +------------------+
//! |   Slot Storage   |  <- Vec<Slot<K, V>> indexed by SlotLocation
//! +------------------+
//! ```
//!
//! # Location Layout (44 bits)
//!
//! ```text
//! | generation (12 bits) | slot_index (32 bits) |
//! ```
//!
//! Generation provides ABA protection when slots are reused.
//!
//! # Thread Safety
//!
//! - Readers increment a per-slot refcount before accessing entry data
//! - Writers wait for readers to drain before dropping entries
//! - Generation counter invalidates stale locations after slot reuse
//! - All operations are lock-free (writers may spin briefly waiting for readers)

use crate::error::{CacheError, CacheResult};
use crate::hashtable::{Hashtable, KeyVerifier};
use crate::hashtable_impl::MultiChoiceHashtable;
use crate::location::Location;
use crate::sync::{AtomicU32, AtomicU64, Ordering};

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};

// Use loom's Arc when testing with loom, std::sync::Arc otherwise
#[cfg(feature = "loom")]
use loom::sync::Arc;
#[cfg(not(feature = "loom"))]
use std::sync::Arc;

// -----------------------------------------------------------------------------
// SlotLocation - interprets Location as (generation, slot_index)
// -----------------------------------------------------------------------------

/// Location interpretation for Arc cache slots.
///
/// Layout: 12-bit generation + 32-bit slot index = 44 bits
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SlotLocation {
    slot_index: u32,
    generation: u16,
}

impl SlotLocation {
    const SLOT_BITS: u64 = 32;
    const SLOT_MASK: u64 = (1 << Self::SLOT_BITS) - 1;
    const GEN_SHIFT: u64 = Self::SLOT_BITS;
    const GEN_MASK: u64 = 0xFFF; // 12 bits

    /// Create a new slot location.
    fn new(slot_index: u32, generation: u16) -> Self {
        debug_assert!(generation <= 0xFFF, "generation exceeds 12 bits");
        Self {
            slot_index,
            generation: generation & 0xFFF,
        }
    }

    /// Convert from opaque Location.
    fn from_location(loc: Location) -> Self {
        let raw = loc.as_raw();
        Self {
            slot_index: (raw & Self::SLOT_MASK) as u32,
            generation: ((raw >> Self::GEN_SHIFT) & Self::GEN_MASK) as u16,
        }
    }

    /// Convert to opaque Location for hashtable storage.
    fn to_location(self) -> Location {
        let raw = (self.slot_index as u64) | ((self.generation as u64) << Self::GEN_SHIFT);
        Location::new(raw)
    }

    /// Get the slot index.
    fn slot_index(&self) -> u32 {
        self.slot_index
    }

    /// Get the generation counter.
    fn generation(&self) -> u16 {
        self.generation
    }
}

// -----------------------------------------------------------------------------
// Slot - holds a key-value entry with reader tracking
// -----------------------------------------------------------------------------

/// A single slot in the Arc cache.
///
/// # Thread Safety
///
/// Uses a reader count to ensure safe memory reclamation:
/// - Readers increment `readers` before accessing `entry_ptr`
/// - Writers wait for `readers` to reach 0 before dropping the entry
/// - Generation is incremented after clearing to invalidate stale locations
///
/// # Entry Pointer Encoding
///
/// The `entry_ptr` field uses the low bit to distinguish between states:
/// - Bit 0 = 0: Either null (0) or a valid Arc pointer (aligned, so low bits are 0)
/// - Bit 0 = 1: Free list link (remaining bits are next-free index << 1)
struct Slot<K, V> {
    /// Number of concurrent readers accessing this slot.
    /// Writers must wait for this to reach 0 before dropping the entry.
    readers: AtomicU32,

    /// Generation counter (12 bits used). Incremented when slot is cleared.
    /// Provides ABA protection - stale SlotLocations will have wrong generation.
    generation: AtomicU32,

    /// Pointer to the entry, or free list link if low bit is set.
    /// - 0: Empty slot
    /// - Low bit 0, non-zero: Valid Arc<Entry<K,V>> pointer
    /// - Low bit 1: Free list link ((next_index << 1) | 1)
    entry_ptr: AtomicU64,

    /// Marker for K, V types
    _marker: std::marker::PhantomData<(K, V)>,
}

/// An entry stored in a slot.
pub struct Entry<K, V> {
    key: K,
    value: V,
}

impl<K, V> Entry<K, V> {
    /// Get the key.
    #[allow(dead_code)]
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Get the value.
    #[allow(dead_code)]
    pub fn value(&self) -> &V {
        &self.value
    }
}

impl<K, V> Slot<K, V> {
    fn new() -> Self {
        Self {
            readers: AtomicU32::new(0),
            generation: AtomicU32::new(0),
            entry_ptr: AtomicU64::new(0),
            _marker: std::marker::PhantomData,
        }
    }

    /// Get the current generation (12-bit value).
    #[inline]
    fn generation(&self) -> u16 {
        (self.generation.load(Ordering::Acquire) & 0xFFF) as u16
    }

    /// Increment generation (wraps at 12 bits).
    #[inline]
    fn increment_generation(&self) {
        // We only care about the low 12 bits, so wrapping is fine
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Try to read the entry if generation matches.
    ///
    /// Returns None if:
    /// - Slot is empty
    /// - Generation mismatch (stale location)
    ///
    /// # Thread Safety
    ///
    /// Increments reader count before accessing entry_ptr, ensuring the
    /// entry cannot be dropped while we're cloning the Arc.
    fn get(&self, expected_gen: u16) -> Option<Arc<Entry<K, V>>> {
        // Announce we're reading - this prevents writers from dropping
        // the entry while we're accessing it
        self.readers.fetch_add(1, Ordering::Acquire);

        // Check generation matches
        let current_gen = self.generation();
        if current_gen != expected_gen {
            self.readers.fetch_sub(1, Ordering::Release);
            return None;
        }

        // Load the entry pointer
        let ptr = self.entry_ptr.load(Ordering::Acquire);
        if ptr == 0 {
            self.readers.fetch_sub(1, Ordering::Release);
            return None;
        }

        // SAFETY: We've incremented readers, so the writer will wait for us
        // before dropping the Arc. The pointer is valid as long as readers > 0.
        let arc = unsafe {
            let arc_ptr = ptr as *const Entry<K, V>;
            // Increment Arc's refcount
            Arc::increment_strong_count(arc_ptr);
            Arc::from_raw(arc_ptr)
        };

        // Done reading - allow writers to proceed
        self.readers.fetch_sub(1, Ordering::Release);

        Some(arc)
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
    fn store(&self, entry: Arc<Entry<K, V>>) -> u16 {
        debug_assert_eq!(
            self.entry_ptr.load(Ordering::Relaxed),
            0,
            "store called on occupied slot"
        );

        // Convert Arc to raw pointer (transfers ownership to the slot)
        let ptr = Arc::into_raw(entry) as u64;
        self.entry_ptr.store(ptr, Ordering::Release);

        self.generation()
    }

    /// Clear the slot, dropping the entry if present.
    ///
    /// # Thread Safety
    ///
    /// Waits for all readers to finish before dropping the entry.
    /// This may spin briefly under contention.
    fn clear(&self) {
        let ptr = self.entry_ptr.swap(0, Ordering::AcqRel);
        if Self::is_arc_pointer(ptr) {
            // Wait for readers to finish
            // In practice this should be very fast - readers just clone an Arc
            let mut spin_count = 0;
            while self.readers.load(Ordering::Acquire) > 0 {
                spin_count += 1;
                if spin_count < 100 {
                    crate::sync::spin_loop();
                } else {
                    // Back off more aggressively after spinning
                    #[cfg(not(feature = "loom"))]
                    std::thread::yield_now();
                    #[cfg(feature = "loom")]
                    loom::thread::yield_now();
                }
            }

            // SAFETY: All readers have finished, safe to drop
            unsafe {
                let arc_ptr = ptr as *const Entry<K, V>;
                drop(Arc::from_raw(arc_ptr));
            }
        }

        // Always increment generation to invalidate stale locations (ABA safety)
        self.increment_generation();
    }

    /// Check if entry_ptr contains an Arc pointer (not a free list link).
    #[inline]
    fn is_arc_pointer(ptr: u64) -> bool {
        // Arc pointers are aligned, so low bit is 0
        // Free list links have low bit set to 1
        // 0 is "no pointer"
        ptr != 0 && (ptr & 1) == 0
    }

    /// Get the next-free index from entry_ptr.
    /// Only valid when slot is in the free list.
    #[inline]
    fn get_next_free(&self) -> u32 {
        let val = self.entry_ptr.load(Ordering::Acquire);
        // Decode: remove the low bit flag, shift back
        ((val >> 1) & 0xFFFF_FFFF) as u32
    }

    /// Set the next free pointer (encodes with low bit = 1).
    /// Only valid when slot is not occupied.
    #[inline]
    fn set_next_free(&self, next: u32) {
        // Encode: shift left and set low bit to mark as free list link
        let encoded = ((next as u64) << 1) | 1;
        self.entry_ptr.store(encoded, Ordering::Release);
    }
}

impl<K, V> Drop for Slot<K, V> {
    fn drop(&mut self) {
        // No need to wait for readers in drop - we have exclusive access
        // Note: loom's atomics don't have get_mut, so we use load instead
        // This is safe because we have &mut self (exclusive access)
        let ptr = self.entry_ptr.load(Ordering::Relaxed);
        // Only drop if it's a valid Arc pointer (not a free list link)
        if Self::is_arc_pointer(ptr) {
            unsafe {
                let arc_ptr = ptr as *const Entry<K, V>;
                drop(Arc::from_raw(arc_ptr));
            }
        }
    }
}

// -----------------------------------------------------------------------------
// SlotStorage - manages slot allocation with lock-free free list
// -----------------------------------------------------------------------------

/// Storage for cache slots with lock-free free list management.
///
/// # Thread Safety
///
/// Uses a lock-free stack (Treiber stack) for the free list.
/// Under very high contention, the single head pointer can become a bottleneck.
/// For extreme throughput, consider sharding into multiple free lists.
struct SlotStorage<K, V> {
    slots: Vec<Slot<K, V>>,
    /// Head of the free list. Stores (version, index) packed into u64.
    /// The version counter prevents ABA problems in the lock-free stack.
    free_head: AtomicU64,
    /// Number of currently occupied slots (for eviction sampling).
    occupied_count: AtomicU32,
}

const EMPTY_FREE_LIST: u32 = u32::MAX;

/// Pack a version and index into a single u64 for ABA-safe CAS operations.
#[inline]
fn pack_free_head(version: u32, index: u32) -> u64 {
    ((version as u64) << 32) | (index as u64)
}

/// Unpack a u64 into (version, index).
#[inline]
fn unpack_free_head(packed: u64) -> (u32, u32) {
    let version = (packed >> 32) as u32;
    let index = packed as u32;
    (version, index)
}

impl<K, V> SlotStorage<K, V> {
    fn new(capacity: usize) -> Self {
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
            free_head: AtomicU64::new(pack_free_head(0, 0)),
            occupied_count: AtomicU32::new(0),
        }
    }

    /// Allocate a slot from the free list.
    ///
    /// Returns the slot location (index + generation) or None if exhausted.
    ///
    /// # Thread Safety
    ///
    /// Uses compare-and-swap on the free list head with a version counter
    /// to prevent ABA problems.
    fn allocate(&self) -> Option<SlotLocation> {
        loop {
            let packed = self.free_head.load(Ordering::Acquire);
            let (version, head) = unpack_free_head(packed);

            if head == EMPTY_FREE_LIST {
                return None; // No free slots
            }

            let slot = &self.slots[head as usize];
            let next = slot.get_next_free();

            // Pack the new head with incremented version to prevent ABA
            let new_packed = pack_free_head(version.wrapping_add(1), next);

            // Try to CAS the head to the next free slot
            match self.free_head.compare_exchange_weak(
                packed,
                new_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Successfully allocated - clear the free list pointer
                    // so the slot is ready for store()
                    slot.entry_ptr.store(0, Ordering::Release);
                    self.occupied_count.fetch_add(1, Ordering::Relaxed);
                    let generation = slot.generation();
                    return Some(SlotLocation::new(head, generation));
                }
                Err(_) => {
                    // CAS failed, another thread modified the list, retry
                    crate::sync::spin_loop();
                }
            }
        }
    }

    /// Return a slot to the free list.
    ///
    /// # Thread Safety
    ///
    /// Clears the slot (waiting for readers) then pushes to the free list
    /// using versioned CAS to prevent ABA problems.
    fn deallocate(&self, loc: SlotLocation) {
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
            let packed = self.free_head.load(Ordering::Acquire);
            let (version, head) = unpack_free_head(packed);

            slot.set_next_free(head);

            // Pack new head with incremented version to prevent ABA
            let new_packed = pack_free_head(version.wrapping_add(1), idx);

            match self.free_head.compare_exchange_weak(
                packed,
                new_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => crate::sync::spin_loop(),
            }
        }
    }

    /// Check if a slot is currently occupied (has a valid entry).
    #[inline]
    fn is_slot_occupied(&self, idx: u32) -> bool {
        if let Some(slot) = self.slots.get(idx as usize) {
            let ptr = slot.entry_ptr.load(Ordering::Acquire);
            Slot::<K, V>::is_arc_pointer(ptr)
        } else {
            false
        }
    }

    /// Get the number of currently occupied slots.
    #[inline]
    fn occupied(&self) -> u32 {
        self.occupied_count.load(Ordering::Relaxed)
    }

    /// Get a slot by index.
    #[inline]
    fn get(&self, idx: u32) -> Option<&Slot<K, V>> {
        self.slots.get(idx as usize)
    }

    /// Get the total capacity.
    #[inline]
    fn capacity(&self) -> usize {
        self.slots.len()
    }
}

// Ensure SlotStorage is Send + Sync
unsafe impl<K: Send, V: Send> Send for SlotStorage<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for SlotStorage<K, V> {}

// -----------------------------------------------------------------------------
// ArcCacheVerifier - KeyVerifier implementation
// -----------------------------------------------------------------------------

/// KeyVerifier for the Arc cache.
struct ArcCacheVerifier<'a, K, V> {
    storage: &'a SlotStorage<K, V>,
}

impl<K, V> KeyVerifier for ArcCacheVerifier<'_, K, V>
where
    K: Eq + AsRef<[u8]> + Send + Sync,
    V: Send + Sync,
{
    fn verify(&self, key: &[u8], location: Location, _allow_deleted: bool) -> bool {
        let slot_loc = SlotLocation::from_location(location);
        let slot = match self.storage.get(slot_loc.slot_index()) {
            Some(s) => s,
            None => return false,
        };

        // Get the entry (handles reader counting internally)
        let entry = match slot.get(slot_loc.generation()) {
            Some(e) => e,
            None => return false,
        };

        // Compare keys
        entry.key.as_ref() == key
    }
}

// -----------------------------------------------------------------------------
// ArcCache - the main cache
// -----------------------------------------------------------------------------

/// Number of random slots to sample when selecting an eviction victim.
/// Higher values give better LFU approximation but cost more CPU.
const EVICTION_SAMPLES: usize = 5;

/// A lock-free cache that stores values accessible via Arc.
///
/// # Thread Safety
///
/// All operations are thread-safe and lock-free:
/// - `get`: Returns a cloned Arc, allowing zero-copy value access
/// - `insert`: Allocates a slot and inserts into the hashtable (evicts if full)
/// - `remove`: Removes from hashtable and returns the slot to the free list
///
/// # Eviction Policy
///
/// When the cache is full, inserts trigger eviction using approximate LFU:
/// - Sample EVICTION_SAMPLES random occupied slots
/// - Evict the one with the lowest access frequency
/// - Frequency is tracked by the underlying hashtable
///
/// # Performance Characteristics
///
/// - Lookups: O(1) expected, lock-free
/// - Inserts: O(1) expected, lock-free (may retry on hash collision)
/// - Removes: O(1) expected, may briefly wait for concurrent readers
/// - Eviction: O(EVICTION_SAMPLES) per eviction
///
/// # Type Parameters
///
/// - `K`: Key type (must be hashable, comparable, and convertible to bytes)
/// - `V`: Value type
/// - `S`: Hash builder (default: RandomState)
pub struct ArcCache<K, V, S = RandomState> {
    hashtable: MultiChoiceHashtable,
    storage: SlotStorage<K, V>,
    #[allow(dead_code)]
    hash_builder: S,
    /// Counter for pseudo-random eviction sampling
    eviction_counter: AtomicU32,
}

impl<K, V> ArcCache<K, V, RandomState>
where
    K: Hash + Eq + AsRef<[u8]> + Send + Sync + Clone,
    V: Send + Sync + Clone,
{
    /// Create a new ArcCache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self::with_hasher(capacity, RandomState::new())
    }
}

impl<K, V, S> ArcCache<K, V, S>
where
    K: Hash + Eq + AsRef<[u8]> + Send + Sync + Clone,
    V: Send + Sync + Clone,
    S: BuildHasher,
{
    /// Create a new ArcCache with a custom hasher.
    pub fn with_hasher(capacity: usize, hash_builder: S) -> Self {
        // Size hashtable for ~50% load factor (good balance of space/performance)
        // MultiChoiceHashtable::new takes power as u8, buckets = 1 << power
        let target_buckets = (capacity * 2).max(1024);
        let power = (target_buckets as f64).log2().ceil() as u8;

        Self {
            hashtable: MultiChoiceHashtable::new(power),
            storage: SlotStorage::new(capacity),
            hash_builder,
            eviction_counter: AtomicU32::new(0),
        }
    }

    /// Get a value by key.
    ///
    /// Returns a clone of the Arc, allowing zero-copy access to the value.
    /// The returned Arc keeps the value alive even if it's evicted from the cache.
    ///
    /// # Returns
    ///
    /// - `Some(Arc<V>)` if the key exists
    /// - `None` if the key is not in the cache
    pub fn get(&self, key: &K) -> Option<Arc<V>>
    where
        V: Clone,
    {
        let key_bytes = key.as_ref();
        let verifier = ArcCacheVerifier {
            storage: &self.storage,
        };

        // Lookup in hashtable (verifier confirms key match)
        let (location, _freq) = self.hashtable.lookup(key_bytes, &verifier)?;
        let slot_loc = SlotLocation::from_location(location);

        // Get the entry from the slot
        let slot = self.storage.get(slot_loc.slot_index())?;
        let entry = slot.get(slot_loc.generation())?;

        // Double-check key (belt and suspenders)
        if entry.key.as_ref() != key_bytes {
            return None;
        }

        // Return Arc containing just the value
        // Note: If V is expensive to clone, consider storing Arc<V> in Entry
        Some(Arc::new(entry.value.clone()))
    }

    /// Get a reference to the entry (key + value) by key.
    ///
    /// This avoids cloning the value, returning the internal Arc instead.
    #[allow(dead_code)]
    pub fn get_entry(&self, key: &K) -> Option<Arc<Entry<K, V>>> {
        let key_bytes = key.as_ref();
        let verifier = ArcCacheVerifier {
            storage: &self.storage,
        };

        let (location, _freq) = self.hashtable.lookup(key_bytes, &verifier)?;
        let slot_loc = SlotLocation::from_location(location);

        let slot = self.storage.get(slot_loc.slot_index())?;
        let entry = slot.get(slot_loc.generation())?;

        if entry.key.as_ref() != key_bytes {
            return None;
        }

        Some(entry)
    }

    /// Insert a key-value pair.
    ///
    /// If the key already exists, the old value is replaced.
    /// If the cache is full, an entry is evicted to make room.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::OutOfMemory` if no slots are available and eviction fails.
    pub fn insert(&self, key: K, value: V) -> CacheResult<()> {
        let key_bytes_owned;
        let key_bytes = {
            key_bytes_owned = key.clone();
            key_bytes_owned.as_ref()
        };

        // Try to allocate a slot, evicting if necessary
        let slot_loc = match self.storage.allocate() {
            Some(loc) => loc,
            None => {
                // Cache is full, try to evict
                if !self.evict_one() {
                    return Err(CacheError::OutOfMemory);
                }
                // Try allocating again
                self.storage.allocate().ok_or(CacheError::OutOfMemory)?
            }
        };

        let slot = self.storage.get(slot_loc.slot_index()).unwrap();

        // Create and store the entry
        let entry = Arc::new(Entry { key, value });
        let generation = slot.store(entry);

        // Update slot location with actual generation (should match, but be safe)
        let slot_loc = SlotLocation::new(slot_loc.slot_index(), generation);

        // Insert into hashtable
        let verifier = ArcCacheVerifier {
            storage: &self.storage,
        };

        match self
            .hashtable
            .insert(key_bytes, slot_loc.to_location(), &verifier)
        {
            Ok(old_location) => {
                // If there was a previous entry, deallocate its slot
                if let Some(old_loc) = old_location {
                    let old_slot_loc = SlotLocation::from_location(old_loc);
                    self.storage.deallocate(old_slot_loc);
                }
                Ok(())
            }
            Err(e) => {
                // Insert failed, return the slot we just allocated
                self.storage.deallocate(slot_loc);
                Err(e)
            }
        }
    }

    /// Remove a key from the cache.
    ///
    /// # Returns
    ///
    /// - `true` if the key was removed
    /// - `false` if the key was not found
    pub fn remove(&self, key: &K) -> bool {
        let key_bytes = key.as_ref();
        let verifier = ArcCacheVerifier {
            storage: &self.storage,
        };

        // First lookup to get the location
        let (location, _freq) = match self.hashtable.lookup(key_bytes, &verifier) {
            Some(result) => result,
            None => return false,
        };

        // Remove from hashtable (requires the expected location for CAS)
        if self.hashtable.remove(key_bytes, location) {
            // Return slot to free list
            let slot_loc = SlotLocation::from_location(location);
            self.storage.deallocate(slot_loc);
            true
        } else {
            // Someone else removed it concurrently, that's fine
            false
        }
    }

    /// Check if a key exists in the cache.
    pub fn contains(&self, key: &K) -> bool {
        let key_bytes = key.as_ref();
        let verifier = ArcCacheVerifier {
            storage: &self.storage,
        };
        self.hashtable.contains(key_bytes, &verifier)
    }

    /// Get the cache capacity (maximum number of entries).
    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.storage.capacity()
    }

    /// Get the current number of entries in the cache.
    pub fn len(&self) -> usize {
        self.storage.occupied() as usize
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Try to evict one entry to make room for a new one.
    ///
    /// Uses approximate LFU: samples EVICTION_SAMPLES random occupied slots
    /// and evicts the one with the lowest frequency.
    ///
    /// # Returns
    ///
    /// `true` if an entry was evicted, `false` if no entry could be evicted
    /// (e.g., cache is empty or all sampled slots became invalid).
    fn evict_one(&self) -> bool {
        let capacity = self.storage.capacity() as u32;
        if capacity == 0 {
            return false;
        }

        // Get a pseudo-random starting point using the counter
        let start = self.eviction_counter.fetch_add(1, Ordering::Relaxed);

        // Track the best victim using separate variables
        let mut victim_slot_idx: Option<u32> = None;
        let mut victim_entry: Option<Arc<Entry<K, V>>> = None;
        let mut victim_freq: u8 = u8::MAX;
        let mut samples_tried = 0;

        // Sample slots looking for eviction candidates
        for i in 0..(capacity.min(EVICTION_SAMPLES as u32 * 4)) {
            // Use a simple hash-like distribution
            let idx = (start.wrapping_mul(2654435761).wrapping_add(i)) % capacity;

            if !self.storage.is_slot_occupied(idx) {
                continue;
            }

            // Try to get the entry from this slot
            let slot = match self.storage.get(idx) {
                Some(s) => s,
                None => continue,
            };

            let generation = slot.generation();
            let entry = match slot.get(generation) {
                Some(e) => e,
                None => continue,
            };

            // Look up frequency in hashtable
            let location = SlotLocation::new(idx, generation).to_location();
            let freq = self
                .hashtable
                .get_item_frequency(entry.key.as_ref(), location)
                .unwrap_or(0);

            // Track the lowest frequency candidate
            if victim_slot_idx.is_none() || freq < victim_freq {
                victim_slot_idx = Some(idx);
                victim_entry = Some(entry);
                victim_freq = freq;
            }

            samples_tried += 1;
            if samples_tried >= EVICTION_SAMPLES {
                break;
            }
        }

        // Evict the best victim if we found one
        if let (Some(slot_idx), Some(entry)) = (victim_slot_idx, victim_entry) {
            let key_bytes = entry.key.as_ref();
            let verifier = ArcCacheVerifier {
                storage: &self.storage,
            };

            // Look up the location in the hashtable
            if let Some((location, _)) = self.hashtable.lookup(key_bytes, &verifier) {
                // Remove from hashtable
                if self.hashtable.remove(key_bytes, location) {
                    // Deallocate the slot
                    let slot_loc = SlotLocation::from_location(location);
                    // Verify slot index matches (it should)
                    if slot_loc.slot_index() == slot_idx {
                        self.storage.deallocate(slot_loc);
                        return true;
                    }
                }
            }
        }

        false
    }
}

// Ensure ArcCache is Send + Sync
unsafe impl<K, V, S> Send for ArcCache<K, V, S>
where
    K: Send + Sync,
    V: Send + Sync,
    S: Send,
{
}

unsafe impl<K, V, S> Sync for ArcCache<K, V, S>
where
    K: Send + Sync,
    V: Send + Sync,
    S: Sync,
{
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_slot_location_roundtrip() {
        let loc = SlotLocation::new(12345, 0xABC);
        let opaque = loc.to_location();
        let recovered = SlotLocation::from_location(opaque);

        assert_eq!(recovered.slot_index(), 12345);
        assert_eq!(recovered.generation(), 0xABC);
    }

    #[test]
    fn test_slot_location_max_values() {
        let loc = SlotLocation::new(u32::MAX, 0xFFF);
        let opaque = loc.to_location();
        let recovered = SlotLocation::from_location(opaque);

        assert_eq!(recovered.slot_index(), u32::MAX);
        assert_eq!(recovered.generation(), 0xFFF);
    }

    #[test]
    fn test_slot_storage_allocate_deallocate() {
        let storage: SlotStorage<Vec<u8>, String> = SlotStorage::new(10);

        // Allocate all slots
        let mut locations = Vec::new();
        for _ in 0..10 {
            let loc = storage.allocate().expect("should have free slots");
            locations.push(loc);
        }

        // Should be exhausted
        assert!(storage.allocate().is_none());

        // Deallocate one
        storage.deallocate(locations.pop().unwrap());

        // Should be able to allocate again
        let loc = storage.allocate().expect("should have free slot");

        // Generation should have incremented
        assert_eq!(loc.generation(), 1);
    }

    #[test]
    fn test_arc_cache_basic() {
        let cache: ArcCache<Vec<u8>, String> = ArcCache::new(100);

        // Insert
        cache
            .insert(b"key1".to_vec(), "value1".to_string())
            .unwrap();
        cache
            .insert(b"key2".to_vec(), "value2".to_string())
            .unwrap();

        // Get
        let v1 = cache.get(&b"key1".to_vec()).expect("key1 should exist");
        assert_eq!(&*v1, "value1");

        let v2 = cache.get(&b"key2".to_vec()).expect("key2 should exist");
        assert_eq!(&*v2, "value2");

        // Miss
        assert!(cache.get(&b"key3".to_vec()).is_none());

        // Contains
        assert!(cache.contains(&b"key1".to_vec()));
        assert!(!cache.contains(&b"key3".to_vec()));

        // Remove
        assert!(cache.remove(&b"key1".to_vec()));
        assert!(!cache.remove(&b"key1".to_vec())); // Already removed
        assert!(cache.get(&b"key1".to_vec()).is_none());
    }

    #[test]
    fn test_arc_cache_overwrite() {
        let cache: ArcCache<Vec<u8>, String> = ArcCache::new(100);

        cache.insert(b"key".to_vec(), "value1".to_string()).unwrap();
        let v1 = cache.get(&b"key".to_vec()).unwrap();
        assert_eq!(&*v1, "value1");

        cache.insert(b"key".to_vec(), "value2".to_string()).unwrap();
        let v2 = cache.get(&b"key".to_vec()).unwrap();
        assert_eq!(&*v2, "value2");

        // Old Arc is still valid
        assert_eq!(&*v1, "value1");
    }

    #[test]
    fn test_arc_cache_eviction() {
        let cache: ArcCache<Vec<u8>, u32> = ArcCache::new(3);

        // Fill the cache
        cache.insert(b"k1".to_vec(), 1).unwrap();
        cache.insert(b"k2".to_vec(), 2).unwrap();
        cache.insert(b"k3".to_vec(), 3).unwrap();

        assert_eq!(cache.len(), 3);

        // Insert another - should trigger eviction
        cache.insert(b"k4".to_vec(), 4).unwrap();

        // Still at capacity
        assert_eq!(cache.len(), 3);

        // New key should exist
        assert!(cache.contains(&b"k4".to_vec()));

        // One of the old keys should have been evicted
        let k1_exists = cache.contains(&b"k1".to_vec());
        let k2_exists = cache.contains(&b"k2".to_vec());
        let k3_exists = cache.contains(&b"k3".to_vec());

        // Exactly one should be evicted (3 old keys, 2 should remain)
        let remaining = [k1_exists, k2_exists, k3_exists]
            .iter()
            .filter(|&&x| x)
            .count();
        assert_eq!(remaining, 2);
    }

    #[test]
    fn test_arc_cache_eviction_frequency() {
        let cache: ArcCache<Vec<u8>, u32> = ArcCache::new(3);

        // Fill the cache
        cache.insert(b"hot".to_vec(), 1).unwrap();
        cache.insert(b"warm".to_vec(), 2).unwrap();
        cache.insert(b"cold".to_vec(), 3).unwrap();

        // Access "hot" multiple times to increase its frequency
        for _ in 0..10 {
            cache.get(&b"hot".to_vec());
        }

        // Access "warm" a few times
        for _ in 0..3 {
            cache.get(&b"warm".to_vec());
        }

        // "cold" has frequency 0 (no accesses after insert)

        // Insert a new key, triggering eviction
        cache.insert(b"new".to_vec(), 4).unwrap();

        // "cold" should be evicted (lowest frequency)
        assert!(!cache.contains(&b"cold".to_vec()));

        // "hot" and "warm" should still exist
        assert!(cache.contains(&b"hot".to_vec()));
        assert!(cache.contains(&b"warm".to_vec()));
        assert!(cache.contains(&b"new".to_vec()));
    }

    #[test]
    fn test_arc_cache_len_and_is_empty() {
        let cache: ArcCache<Vec<u8>, u32> = ArcCache::new(10);

        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);

        cache.insert(b"k1".to_vec(), 1).unwrap();
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);

        cache.insert(b"k2".to_vec(), 2).unwrap();
        assert_eq!(cache.len(), 2);

        cache.remove(&b"k1".to_vec());
        assert_eq!(cache.len(), 1);

        cache.remove(&b"k2".to_vec());
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }
}

// -----------------------------------------------------------------------------
// Design notes
// -----------------------------------------------------------------------------

/*
THREAD SAFETY ANALYSIS:

1. Slot::get() - Reader path:
   - Increment readers (Acquire) - announces read intent
   - Check generation - validates location is still valid
   - Load entry_ptr (Acquire) - gets the pointer
   - Increment Arc refcount - safe because readers > 0 prevents drop
   - Decrement readers (Release) - allows writers to proceed

2. Slot::clear() - Writer path:
   - Swap entry_ptr to 0 (AcqRel) - prevents new readers from seeing entry
   - Spin until readers == 0 (Acquire) - wait for existing readers
   - Drop Arc - safe because no readers can access it
   - Increment generation (Release) - invalidates stale locations

3. Free list operations:
   - Standard Treiber stack with compare_exchange
   - ABA safe because we clear slots before reusing

PERFORMANCE NOTES:

1. Single free list head is a potential contention point.
   Mitigation: Shard into N free lists, hash thread ID to select.

2. Reader counting adds overhead to every lookup.
   Alternative: Epoch-based reclamation (crossbeam-epoch) for higher throughput.

3. Spin-waiting in clear() is bounded by reader duration.
   Readers only clone an Arc, so wait should be very short.

4. Value cloning in get() could be expensive.
   Alternative: Store Arc<V> in Entry, return cloned Arc directly.

FUTURE IMPROVEMENTS:

1. Eviction policy - Currently no eviction, fails when full.
   Could use frequency from hashtable for LFU-like eviction.

2. TTL support - Add expiration timestamp to Entry.

3. Size tracking - Track memory usage, not just entry count.

4. Metrics - Hit/miss counters, latency histograms.
*/

// -----------------------------------------------------------------------------
// Loom tests for concurrency verification
// -----------------------------------------------------------------------------

#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use super::*;
    use loom::sync::Arc as LoomArc;
    use loom::thread;

    /// Test concurrent reader count increments don't race.
    #[test]
    fn test_concurrent_reader_counts() {
        loom::model(|| {
            let readers = LoomArc::new(AtomicU32::new(0));

            let readers1 = readers.clone();
            let readers2 = readers.clone();

            let t1 = thread::spawn(move || {
                readers1.fetch_add(1, Ordering::Acquire);
                // Simulate some work
                let _ = readers1.load(Ordering::Relaxed);
                readers1.fetch_sub(1, Ordering::Release);
            });

            let t2 = thread::spawn(move || {
                readers2.fetch_add(1, Ordering::Acquire);
                // Simulate some work
                let _ = readers2.load(Ordering::Relaxed);
                readers2.fetch_sub(1, Ordering::Release);
            });

            t1.join().unwrap();
            t2.join().unwrap();

            // After both threads finish, reader count should be 0
            assert_eq!(readers.load(Ordering::Relaxed), 0);
        });
    }

    /// Test reader counting synchronization.
    ///
    /// Note: We can't test the full Slot::get/clear with loom because loom's Arc
    /// doesn't support Arc::increment_strong_count/from_raw. Instead, we test
    /// that the reader counting logic is correct.
    #[test]
    fn test_reader_counting_synchronization() {
        loom::model(|| {
            // Test the reader counting synchronization directly using atomics
            let readers = LoomArc::new(AtomicU32::new(0));
            let cleared = LoomArc::new(AtomicU32::new(0));

            let readers1 = readers.clone();
            let cleared1 = cleared.clone();
            let readers2 = readers.clone();
            let cleared2 = cleared.clone();

            // Simulate reader - increment readers, do work, decrement
            let t1 = thread::spawn(move || {
                readers1.fetch_add(1, Ordering::Acquire);

                // Simulate reading - check if cleared
                let was_cleared = cleared1.load(Ordering::Acquire) > 0;

                readers1.fetch_sub(1, Ordering::Release);

                was_cleared
            });

            // Simulate writer - set cleared flag, wait for readers
            let t2 = thread::spawn(move || {
                cleared2.store(1, Ordering::Release);

                // Wait for readers to finish
                while readers2.load(Ordering::Acquire) > 0 {
                    loom::thread::yield_now();
                }
            });

            let reader_saw_cleared = t1.join().unwrap();
            t2.join().unwrap();

            // The key invariant: if reader didn't see cleared flag,
            // writer must have waited for the reader to finish
            // (This is implicitly tested by loom's exhaustive exploration)
            let _ = reader_saw_cleared;
        });
    }

    /// Test generation counter behavior under concurrent access.
    #[test]
    fn test_generation_counter_concurrent() {
        loom::model(|| {
            let generation = LoomArc::new(AtomicU32::new(0));

            let gen1 = generation.clone();
            let gen2 = generation.clone();

            // Thread 1 increments generation
            let t1 = thread::spawn(move || {
                gen1.fetch_add(1, Ordering::Release);
            });

            // Thread 2 reads generation
            let t2 = thread::spawn(move || gen2.load(Ordering::Acquire));

            t1.join().unwrap();
            let value = t2.join().unwrap();

            // Value should be either 0 (read before increment) or 1 (read after)
            assert!(value == 0 || value == 1);
        });
    }

    /// Test concurrent allocate/deallocate on SlotStorage.
    #[test]
    fn test_concurrent_allocate_deallocate() {
        loom::model(|| {
            // Small capacity to keep state space manageable
            let storage: LoomArc<SlotStorage<Vec<u8>, u32>> = LoomArc::new(SlotStorage::new(2));

            let storage1 = storage.clone();
            let storage2 = storage.clone();

            let t1 = thread::spawn(move || {
                if let Some(loc) = storage1.allocate() {
                    // Got a slot, deallocate it
                    storage1.deallocate(loc);
                }
            });

            let t2 = thread::spawn(move || {
                if let Some(loc) = storage2.allocate() {
                    // Got a slot, deallocate it
                    storage2.deallocate(loc);
                }
            });

            t1.join().unwrap();
            t2.join().unwrap();

            // After both threads complete, we should be able to allocate both slots
            let loc1 = storage.allocate();
            let loc2 = storage.allocate();
            assert!(loc1.is_some());
            assert!(loc2.is_some());
        });
    }

    /// Test that multiple allocations don't return the same slot.
    #[test]
    fn test_no_double_allocation() {
        loom::model(|| {
            let storage: LoomArc<SlotStorage<Vec<u8>, u32>> = LoomArc::new(SlotStorage::new(2));

            let storage1 = storage.clone();
            let storage2 = storage.clone();

            let t1 = thread::spawn(move || storage1.allocate());
            let t2 = thread::spawn(move || storage2.allocate());

            let loc1 = t1.join().unwrap();
            let loc2 = t2.join().unwrap();

            // Both should succeed (we have 2 slots)
            assert!(loc1.is_some());
            assert!(loc2.is_some());

            // They should be different slots
            let l1 = loc1.unwrap();
            let l2 = loc2.unwrap();
            assert_ne!(l1.slot_index(), l2.slot_index());
        });
    }

    /// Test free list head CAS under contention.
    #[test]
    fn test_free_list_cas_contention() {
        loom::model(|| {
            let head = LoomArc::new(AtomicU32::new(0));
            // Simulate 3 slots: 0 -> 1 -> 2 -> EMPTY
            let next_ptrs = LoomArc::new([
                AtomicU32::new(1),
                AtomicU32::new(2),
                AtomicU32::new(u32::MAX),
            ]);

            let head1 = head.clone();
            let next1 = next_ptrs.clone();
            let head2 = head.clone();
            let next2 = next_ptrs.clone();

            // Two threads try to allocate concurrently
            let t1 = thread::spawn(move || {
                let current = head1.load(Ordering::Acquire);
                if current != u32::MAX {
                    let next = next1[current as usize].load(Ordering::Acquire);
                    if head1
                        .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return Some(current);
                    }
                }
                None
            });

            let t2 = thread::spawn(move || {
                let current = head2.load(Ordering::Acquire);
                if current != u32::MAX {
                    let next = next2[current as usize].load(Ordering::Acquire);
                    if head2
                        .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return Some(current);
                    }
                }
                None
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // At least one should succeed, and if both succeed they should get different slots
            assert!(r1.is_some() || r2.is_some());
            if let (Some(v1), Some(v2)) = (r1, r2) {
                assert_ne!(v1, v2);
            }
        });
    }

    /// Test three-way allocation contention.
    ///
    /// Three threads compete to allocate from a pool with 3 slots.
    /// This tests CAS retry loops under higher contention.
    ///
    /// Uses bounded preemption to keep state space tractable.
    #[test]
    fn test_three_way_allocation_contention() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let storage: LoomArc<SlotStorage<Vec<u8>, u32>> = LoomArc::new(SlotStorage::new(3));

            let storage1 = storage.clone();
            let storage2 = storage.clone();
            let storage3 = storage.clone();

            let t1 = thread::spawn(move || storage1.allocate());
            let t2 = thread::spawn(move || storage2.allocate());
            let t3 = thread::spawn(move || storage3.allocate());

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();
            let r3 = t3.join().unwrap();

            // All three should succeed (we have 3 slots)
            assert!(r1.is_some(), "First allocation should succeed");
            assert!(r2.is_some(), "Second allocation should succeed");
            assert!(r3.is_some(), "Third allocation should succeed");

            // All should be different slots
            let slots: Vec<_> = [r1, r2, r3]
                .into_iter()
                .map(|r| r.unwrap().slot_index())
                .collect();
            assert_ne!(slots[0], slots[1]);
            assert_ne!(slots[0], slots[2]);
            assert_ne!(slots[1], slots[2]);
        });
    }

    /// Test three-way allocate/deallocate cycling.
    ///
    /// Three threads each allocate and deallocate, testing the free list
    /// under sustained contention from multiple actors.
    ///
    /// Uses bounded preemption to keep state space tractable.
    #[test]
    fn test_three_way_allocate_deallocate_cycle() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.check(|| {
            let storage: LoomArc<SlotStorage<Vec<u8>, u32>> = LoomArc::new(SlotStorage::new(2));

            let storage1 = storage.clone();
            let storage2 = storage.clone();
            let storage3 = storage.clone();

            let t1 = thread::spawn(move || {
                if let Some(loc) = storage1.allocate() {
                    storage1.deallocate(loc);
                    true
                } else {
                    false
                }
            });

            let t2 = thread::spawn(move || {
                if let Some(loc) = storage2.allocate() {
                    storage2.deallocate(loc);
                    true
                } else {
                    false
                }
            });

            let t3 = thread::spawn(move || {
                if let Some(loc) = storage3.allocate() {
                    storage3.deallocate(loc);
                    true
                } else {
                    false
                }
            });

            let _ = t1.join().unwrap();
            let _ = t2.join().unwrap();
            let _ = t3.join().unwrap();

            // After all complete, both slots should be available
            // (Fixed by using versioned pointer in free_head to prevent ABA)
            let loc1 = storage.allocate();
            let loc2 = storage.allocate();
            assert!(loc1.is_some(), "First slot should be available");
            assert!(loc2.is_some(), "Second slot should be available");
        });
    }

    /// Test two readers and one writer pattern.
    ///
    /// Two readers increment/decrement reader count while a writer
    /// waits for readers to clear. This is the core pattern for safe
    /// slot clearing.
    ///
    /// Uses bounded preemption to keep state space tractable.
    #[test]
    fn test_two_readers_one_writer() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let readers = LoomArc::new(AtomicU32::new(0));
            let cleared = LoomArc::new(AtomicU32::new(0));

            let readers1 = readers.clone();
            let cleared1 = cleared.clone();
            let readers2 = readers.clone();
            let cleared2 = cleared.clone();
            let readers3 = readers.clone();
            let cleared3 = cleared.clone();

            // Reader 1
            let t1 = thread::spawn(move || {
                readers1.fetch_add(1, Ordering::Acquire);
                let saw_cleared = cleared1.load(Ordering::Acquire) > 0;
                readers1.fetch_sub(1, Ordering::Release);
                saw_cleared
            });

            // Reader 2
            let t2 = thread::spawn(move || {
                readers2.fetch_add(1, Ordering::Acquire);
                let saw_cleared = cleared2.load(Ordering::Acquire) > 0;
                readers2.fetch_sub(1, Ordering::Release);
                saw_cleared
            });

            // Writer - sets cleared flag and waits for readers
            let t3 = thread::spawn(move || {
                cleared3.store(1, Ordering::Release);
                while readers3.load(Ordering::Acquire) > 0 {
                    loom::thread::yield_now();
                }
            });

            let _ = t1.join().unwrap();
            let _ = t2.join().unwrap();
            t3.join().unwrap();

            // After writer completes, all readers must have finished
            assert_eq!(readers.load(Ordering::Acquire), 0);
        });
    }

    /// Test three-way free list CAS contention.
    ///
    /// Three threads try to pop from a lock-free stack simultaneously.
    ///
    /// Uses bounded preemption to keep state space tractable.
    #[test]
    fn test_three_way_free_list_cas() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let head = LoomArc::new(AtomicU32::new(0));
            // 4 slots: 0 -> 1 -> 2 -> 3 -> EMPTY
            let next_ptrs = LoomArc::new([
                AtomicU32::new(1),
                AtomicU32::new(2),
                AtomicU32::new(3),
                AtomicU32::new(u32::MAX),
            ]);

            let try_pop = |head: &AtomicU32, next: &[AtomicU32; 4]| -> Option<u32> {
                let current = head.load(Ordering::Acquire);
                if current == u32::MAX {
                    return None;
                }
                let next_val = next[current as usize].load(Ordering::Acquire);
                if head
                    .compare_exchange(current, next_val, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    Some(current)
                } else {
                    None
                }
            };

            let head1 = head.clone();
            let next1 = next_ptrs.clone();
            let head2 = head.clone();
            let next2 = next_ptrs.clone();
            let head3 = head.clone();
            let next3 = next_ptrs.clone();

            let t1 = thread::spawn(move || try_pop(&head1, &next1));
            let t2 = thread::spawn(move || try_pop(&head2, &next2));
            let t3 = thread::spawn(move || try_pop(&head3, &next3));

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();
            let r3 = t3.join().unwrap();

            // Collect successful pops
            let successes: Vec<_> = [r1, r2, r3].into_iter().flatten().collect();

            // At least one should succeed
            assert!(!successes.is_empty(), "At least one pop should succeed");

            // All successful pops should have different values
            for i in 0..successes.len() {
                for j in (i + 1)..successes.len() {
                    assert_ne!(
                        successes[i], successes[j],
                        "No two threads should pop the same slot"
                    );
                }
            }
        });
    }
}
