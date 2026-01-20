//! Heap-allocated cache using system allocator with MultiChoiceHashtable.
//!
//! This crate provides a high-performance cache that uses the system allocator
//! for item storage, with the MultiChoiceHashtable for fast key lookups.
//!
//! # Architecture
//!
//! ```text
//! +------------------------------------------+
//! |              HeapCache                   |
//! |                                          |
//! |  +------------------------------------+  |
//! |  | MultiChoiceHashtable                    |  |
//! |  | - Key -> (Location, Frequency)     |  |
//! |  +------------------------------------+  |
//! |        |                                 |
//! |        v                                 |
//! |  +------------------------------------+  |
//! |  | SlotStorage                        |  |
//! |  | - Vec<Slot>                        |  |
//! |  | - Free list (Treiber stack)        |  |
//! |  +------------------------------------+  |
//! |        |                                 |
//! |        v                                 |
//! |  +------------------------------------+  |
//! |  | HeapEntry (heap-allocated)         |  |
//! |  | [header][key][value]               |  |
//! |  +------------------------------------+  |
//! +------------------------------------------+
//! ```
//!
//! # Memory Management
//!
//! The cache tracks allocated bytes and evicts items when approaching the
//! memory limit. A fragmentation ratio is periodically calibrated by querying
//! the allocator (if available) to account for external fragmentation.
//!
//! # Example
//!
//! ```ignore
//! use heap_cache::HeapCache;
//! use std::time::Duration;
//!
//! let cache = HeapCache::builder()
//!     .memory_limit(1024 * 1024 * 1024)  // 1GB
//!     .build();
//!
//! // Store an item
//! cache.set(b"key", b"value", Some(Duration::from_secs(3600))).unwrap();
//!
//! // Retrieve an item
//! if let Some(value) = cache.get(b"key") {
//!     println!("Value: {:?}", value.value());
//! }
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

mod entry;
mod fifo_queue;
mod location;
mod s3fifo_policy;
mod slot;
mod storage;
mod sync;
mod verifier;

use std::time::Duration;

// Use loom-compatible atomics for some fields, std for others
use sync::{Arc, AtomicU32, AtomicU64, Ordering};

// AtomicUsize is only used for bytes_used tracking, not for synchronization
// that loom needs to model, so we use std directly
use std::sync::atomic::AtomicUsize;

use cache_core::{
    Cache, CacheError, CacheResult, DEFAULT_TTL, Hashtable, MultiChoiceHashtable, OwnedGuard,
    ValueRef,
};

use entry::HeapEntry;
use location::SlotLocation;
use storage::SlotStorage;
use verifier::HeapCacheVerifier;

/// Number of random slots to sample when selecting an eviction victim.
const EVICTION_SAMPLES: usize = 5;

/// Default fragmentation ratio (120 = 1.2x overhead).
const DEFAULT_FRAGMENTATION_RATIO: u32 = 120;

/// Number of operations between fragmentation ratio calibrations.
const CALIBRATION_INTERVAL: u64 = 10000;

/// Heap-allocated cache using the system allocator.
///
/// Uses the MultiChoiceHashtable for key lookups and SlotStorage for
/// managing heap-allocated entries. Tracks memory usage and evicts
/// based on byte limits rather than item count.
pub struct HeapCache {
    /// The hashtable for key lookups.
    hashtable: Arc<MultiChoiceHashtable>,
    /// Storage for slots containing heap-allocated entries.
    storage: SlotStorage,
    /// Default TTL for items.
    default_ttl: Duration,
    /// Counter for pseudo-random eviction sampling.
    eviction_counter: AtomicU32,
    /// Global CAS counter for unique tokens.
    cas_counter: AtomicU64,

    // Memory tracking
    /// Tracked bytes allocated for entries (our accounting).
    bytes_used: AtomicUsize,
    /// Memory limit in bytes.
    bytes_limit: usize,
    /// Fragmentation ratio as percentage (100 = 1.0x, 120 = 1.2x).
    /// Applied to bytes_used to estimate actual memory consumption.
    fragmentation_ratio: AtomicU32,
    /// Operations counter for periodic calibration.
    ops_counter: AtomicU64,
}

impl HeapCache {
    /// Create a new builder for HeapCache.
    pub fn builder() -> HeapCacheBuilder {
        HeapCacheBuilder::new()
    }

    /// Calculate the estimated actual memory usage including fragmentation.
    #[inline]
    fn estimated_memory(&self) -> usize {
        let tracked = self.bytes_used.load(Ordering::Relaxed);
        let ratio = self.fragmentation_ratio.load(Ordering::Relaxed) as usize;
        tracked * ratio / 100
    }

    /// Check if we should calibrate the fragmentation ratio.
    #[inline]
    fn maybe_calibrate(&self) {
        let ops = self.ops_counter.fetch_add(1, Ordering::Relaxed);
        if ops % CALIBRATION_INTERVAL == 0 {
            self.calibrate_fragmentation();
        }
    }

    /// Calibrate the fragmentation ratio by querying the allocator.
    ///
    /// This is called periodically to adjust for actual heap fragmentation.
    /// If allocator stats aren't available, keeps the current ratio.
    pub fn calibrate_fragmentation(&self) {
        let tracked = self.bytes_used.load(Ordering::Relaxed);
        if tracked == 0 {
            return;
        }

        // Try to get actual memory usage from allocator
        if let Some(actual) = Self::query_allocator_memory() {
            // Calculate ratio as percentage (with bounds)
            let ratio = ((actual as u128 * 100) / tracked as u128) as u32;
            // Clamp to reasonable range (100-300%)
            let ratio = ratio.clamp(100, 300);
            self.fragmentation_ratio.store(ratio, Ordering::Relaxed);
        }
    }

    /// Query the allocator for actual memory usage.
    ///
    /// Returns None if allocator stats aren't available.
    #[cfg(all(target_os = "linux", feature = "jemalloc"))]
    fn query_allocator_memory() -> Option<usize> {
        // With jemalloc, we can get accurate stats
        // This would use jemalloc_ctl crate
        None // TODO: implement when jemalloc feature is added
    }

    #[cfg(not(all(target_os = "linux", feature = "jemalloc")))]
    fn query_allocator_memory() -> Option<usize> {
        // Without jemalloc, try reading from /proc on Linux
        #[cfg(target_os = "linux")]
        {
            Self::read_proc_statm_rss()
        }
        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    }

    /// Read RSS from /proc/self/statm on Linux.
    #[cfg(target_os = "linux")]
    fn read_proc_statm_rss() -> Option<usize> {
        use std::fs;
        let statm = fs::read_to_string("/proc/self/statm").ok()?;
        let rss_pages: usize = statm.split_whitespace().nth(1)?.parse().ok()?;
        Some(rss_pages * 4096)
    }

    /// Store an item only if the key doesn't exist (ADD semantics).
    fn add_item(&self, key: &[u8], value: &[u8], ttl: Duration) -> CacheResult<()> {
        // Validate key length
        if key.len() > u16::MAX as usize {
            return Err(CacheError::KeyTooLong);
        }

        // Check if key already exists
        let verifier = HeapCacheVerifier::new(&self.storage);
        if self.hashtable.contains(key, &verifier) {
            return Err(CacheError::KeyExists);
        }

        // Calculate item size for memory tracking
        let item_size = entry::item_size(key.len(), value.len());

        // Check memory and evict if necessary
        self.ensure_memory_available(item_size)?;

        // Periodic calibration check
        self.maybe_calibrate();

        // Get a unique CAS token
        let cas_token = self.cas_counter.fetch_add(1, Ordering::Relaxed);

        // Allocate the heap entry
        let entry =
            HeapEntry::allocate(key, value, ttl, cas_token).ok_or(CacheError::OutOfMemory)?;

        // Try to allocate a slot
        let slot_loc = match self.storage.allocate() {
            Some(loc) => loc,
            None => {
                // No free slots, try to evict one
                if !self.evict_one() {
                    unsafe { HeapEntry::free(entry) };
                    return Err(CacheError::OutOfMemory);
                }
                match self.storage.allocate() {
                    Some(loc) => loc,
                    None => {
                        unsafe { HeapEntry::free(entry) };
                        return Err(CacheError::OutOfMemory);
                    }
                }
            }
        };

        // Store the entry in the slot
        let slot = self.storage.get(slot_loc.slot_index()).unwrap();
        let generation = slot.store(entry);

        // Update slot location with actual generation
        let slot_loc = SlotLocation::new(slot_loc.slot_index(), generation);

        // Insert into hashtable using insert_if_absent
        let verifier = HeapCacheVerifier::new(&self.storage);

        match self
            .hashtable
            .insert_if_absent(key, slot_loc.to_location(), &verifier)
        {
            Ok(()) => {
                // Track new bytes
                self.bytes_used.fetch_add(item_size, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                // Insert failed (key already exists or hashtable full)
                self.storage.deallocate(slot_loc);
                Err(e)
            }
        }
    }

    /// Update an existing item only (REPLACE semantics).
    fn replace_item(&self, key: &[u8], value: &[u8], ttl: Duration) -> CacheResult<()> {
        // Validate key length
        if key.len() > u16::MAX as usize {
            return Err(CacheError::KeyTooLong);
        }

        // Check if key exists
        let verifier = HeapCacheVerifier::new(&self.storage);
        if !self.hashtable.contains(key, &verifier) {
            return Err(CacheError::KeyNotFound);
        }

        // Calculate item size for memory tracking
        let item_size = entry::item_size(key.len(), value.len());

        // Check memory and evict if necessary
        self.ensure_memory_available(item_size)?;

        // Periodic calibration check
        self.maybe_calibrate();

        // Get a unique CAS token
        let cas_token = self.cas_counter.fetch_add(1, Ordering::Relaxed);

        // Allocate the heap entry
        let entry =
            HeapEntry::allocate(key, value, ttl, cas_token).ok_or(CacheError::OutOfMemory)?;

        // Try to allocate a slot
        let slot_loc = match self.storage.allocate() {
            Some(loc) => loc,
            None => {
                // No free slots, try to evict one
                if !self.evict_one() {
                    unsafe { HeapEntry::free(entry) };
                    return Err(CacheError::OutOfMemory);
                }
                match self.storage.allocate() {
                    Some(loc) => loc,
                    None => {
                        unsafe { HeapEntry::free(entry) };
                        return Err(CacheError::OutOfMemory);
                    }
                }
            }
        };

        // Store the entry in the slot
        let slot = self.storage.get(slot_loc.slot_index()).unwrap();
        let generation = slot.store(entry);

        // Update slot location with actual generation
        let slot_loc = SlotLocation::new(slot_loc.slot_index(), generation);

        // Update in hashtable using update_if_present
        let verifier = HeapCacheVerifier::new(&self.storage);

        match self
            .hashtable
            .update_if_present(key, slot_loc.to_location(), &verifier)
        {
            Ok(old_location) => {
                // Track new bytes
                self.bytes_used.fetch_add(item_size, Ordering::Relaxed);

                // Deallocate old slot
                let old_slot_loc = SlotLocation::from_location(old_location);
                self.deallocate_and_track(old_slot_loc);
                Ok(())
            }
            Err(e) => {
                // Update failed (key doesn't exist anymore)
                self.storage.deallocate(slot_loc);
                Err(e)
            }
        }
    }

    /// Store an item in the cache.
    fn set_item(&self, key: &[u8], value: &[u8], ttl: Duration) -> CacheResult<()> {
        // Validate key length
        if key.len() > u16::MAX as usize {
            return Err(CacheError::KeyTooLong);
        }

        // Calculate item size for memory tracking
        let item_size = entry::item_size(key.len(), value.len());

        // Check memory and evict if necessary
        self.ensure_memory_available(item_size)?;

        // Periodic calibration check
        self.maybe_calibrate();

        // Get a unique CAS token
        let cas_token = self.cas_counter.fetch_add(1, Ordering::Relaxed);

        // Allocate the heap entry
        let entry =
            HeapEntry::allocate(key, value, ttl, cas_token).ok_or(CacheError::OutOfMemory)?;

        // Try to allocate a slot
        let slot_loc = match self.storage.allocate() {
            Some(loc) => loc,
            None => {
                // No free slots, try to evict one
                if !self.evict_one() {
                    unsafe { HeapEntry::free(entry) };
                    return Err(CacheError::OutOfMemory);
                }
                match self.storage.allocate() {
                    Some(loc) => loc,
                    None => {
                        unsafe { HeapEntry::free(entry) };
                        return Err(CacheError::OutOfMemory);
                    }
                }
            }
        };

        // Store the entry in the slot
        let slot = self.storage.get(slot_loc.slot_index()).unwrap();
        let generation = slot.store(entry);

        // Update slot location with actual generation
        let slot_loc = SlotLocation::new(slot_loc.slot_index(), generation);

        // Insert into hashtable
        let verifier = HeapCacheVerifier::new(&self.storage);

        match self
            .hashtable
            .insert(key, slot_loc.to_location(), &verifier)
        {
            Ok(old_location) => {
                // Track new bytes
                self.bytes_used.fetch_add(item_size, Ordering::Relaxed);

                // If there was a previous entry, deallocate its slot
                if let Some(old_loc) = old_location {
                    let old_slot_loc = SlotLocation::from_location(old_loc);
                    self.deallocate_and_track(old_slot_loc);
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

    /// Ensure enough memory is available for an allocation of `needed` bytes.
    ///
    /// Evicts items until there's room or we can't evict anymore.
    fn ensure_memory_available(&self, needed: usize) -> CacheResult<()> {
        let mut attempts = 0;
        const MAX_EVICTION_ATTEMPTS: usize = 100;

        loop {
            let estimated = self.estimated_memory();
            if estimated + needed <= self.bytes_limit {
                return Ok(());
            }

            // Need to evict
            if !self.evict_one() {
                // Can't evict, check if we're really over limit
                // (might have been concurrent evictions)
                let estimated = self.estimated_memory();
                if estimated + needed <= self.bytes_limit {
                    return Ok(());
                }
                return Err(CacheError::OutOfMemory);
            }

            attempts += 1;
            if attempts >= MAX_EVICTION_ATTEMPTS {
                return Err(CacheError::OutOfMemory);
            }
        }
    }

    /// Deallocate a slot and track the freed bytes.
    fn deallocate_and_track(&self, slot_loc: SlotLocation) {
        // Get the entry size before deallocating
        if let Some(slot) = self.storage.get(slot_loc.slot_index()) {
            if let Some(entry) = slot.get_with_flags(slot_loc.generation(), true, true) {
                let size = entry.total_size();
                slot.release_read();
                self.bytes_used.fetch_sub(size, Ordering::Relaxed);
            }
        }
        self.storage.deallocate(slot_loc);
    }

    /// Retrieve an item's value from the cache.
    fn get_item(&self, key: &[u8]) -> Option<Vec<u8>> {
        let verifier = HeapCacheVerifier::new(&self.storage);
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;

        let slot_loc = SlotLocation::from_location(location);
        let slot = self.storage.get(slot_loc.slot_index())?;

        let entry = slot.get(slot_loc.generation(), false)?;
        let value = entry.value().to_vec();
        slot.release_read();

        Some(value)
    }

    /// Access an item without copying.
    fn with_item<F, R>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let verifier = HeapCacheVerifier::new(&self.storage);
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;

        let slot_loc = SlotLocation::from_location(location);
        let slot = self.storage.get(slot_loc.slot_index())?;

        let entry = slot.get(slot_loc.generation(), false)?;
        let result = f(entry.value());
        slot.release_read();

        Some(result)
    }

    /// Delete an item from the cache.
    fn delete_item(&self, key: &[u8]) -> bool {
        let verifier = HeapCacheVerifier::new(&self.storage);

        let (location, _freq) = match self.hashtable.lookup(key, &verifier) {
            Some(result) => result,
            None => return false,
        };

        if !self.hashtable.remove(key, location) {
            return false;
        }

        let slot_loc = SlotLocation::from_location(location);
        self.deallocate_and_track(slot_loc);

        true
    }

    /// Check if a key exists.
    fn contains_key(&self, key: &[u8]) -> bool {
        let verifier = HeapCacheVerifier::new(&self.storage);
        self.hashtable.contains(key, &verifier)
    }

    /// Try to evict one entry to make room for a new one.
    ///
    /// Uses approximate LFU: samples EVICTION_SAMPLES random occupied slots
    /// and evicts the one with the lowest frequency.
    fn evict_one(&self) -> bool {
        let capacity = self.storage.capacity() as u32;
        if capacity == 0 {
            return false;
        }

        let start = self.eviction_counter.fetch_add(1, Ordering::Relaxed);

        let mut victim_slot_idx: Option<u32> = None;
        let mut victim_key: Option<Vec<u8>> = None;
        let mut victim_freq: u8 = u8::MAX;
        let mut samples_tried = 0;

        for i in 0..(capacity.min(EVICTION_SAMPLES as u32 * 4)) {
            let idx = (start.wrapping_mul(2654435761).wrapping_add(i)) % capacity;

            if !self.storage.is_slot_occupied(idx) {
                continue;
            }

            let slot = match self.storage.get(idx) {
                Some(s) => s,
                None => continue,
            };

            let generation = slot.generation();
            let entry = match slot.get(generation, false) {
                Some(e) => e,
                None => continue,
            };

            let key = entry.key().to_vec();
            slot.release_read();

            let location = SlotLocation::new(idx, generation).to_location();
            let freq = self
                .hashtable
                .get_item_frequency(&key, location)
                .unwrap_or(0);

            if victim_slot_idx.is_none() || freq < victim_freq {
                victim_slot_idx = Some(idx);
                victim_key = Some(key);
                victim_freq = freq;
            }

            samples_tried += 1;
            if samples_tried >= EVICTION_SAMPLES {
                break;
            }
        }

        if let (Some(_slot_idx), Some(key)) = (victim_slot_idx, victim_key) {
            let verifier = HeapCacheVerifier::new(&self.storage);

            if let Some((location, _)) = self.hashtable.lookup(&key, &verifier) {
                if self.hashtable.remove(&key, location) {
                    let slot_loc = SlotLocation::from_location(location);
                    self.deallocate_and_track(slot_loc);
                    return true;
                }
            }
        }

        false
    }

    /// Get the current number of entries.
    pub fn len(&self) -> usize {
        self.storage.occupied() as usize
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the slot capacity.
    pub fn capacity(&self) -> usize {
        self.storage.capacity()
    }

    /// Get the memory limit in bytes.
    pub fn memory_limit(&self) -> usize {
        self.bytes_limit
    }

    /// Get the tracked bytes used (not including fragmentation estimate).
    pub fn bytes_used(&self) -> usize {
        self.bytes_used.load(Ordering::Relaxed)
    }

    /// Get the estimated actual memory usage (including fragmentation).
    pub fn estimated_memory_used(&self) -> usize {
        self.estimated_memory()
    }

    /// Get the current fragmentation ratio as a percentage (100 = 1.0x).
    pub fn fragmentation_ratio(&self) -> u32 {
        self.fragmentation_ratio.load(Ordering::Relaxed)
    }

    /// Manually set the fragmentation ratio (for testing or manual tuning).
    pub fn set_fragmentation_ratio(&self, ratio: u32) {
        self.fragmentation_ratio
            .store(ratio.clamp(100, 300), Ordering::Relaxed);
    }
}

impl Cache for HeapCache {
    fn get(&self, key: &[u8]) -> Option<OwnedGuard> {
        self.get_item(key).map(OwnedGuard::new)
    }

    fn with_value<F, R>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        self.with_item(key, f)
    }

    fn get_value_ref(&self, key: &[u8]) -> Option<ValueRef> {
        let verifier = HeapCacheVerifier::new(&self.storage);
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;

        let slot_loc = SlotLocation::from_location(location);
        let slot = self.storage.get(slot_loc.slot_index())?;

        let entry = slot.get(slot_loc.generation(), false)?;
        let value = entry.value();

        let ref_count = Box::leak(Box::new(AtomicU32::new(1)));
        let value_copy = value.to_vec().leak();

        slot.release_read();

        unsafe {
            Some(ValueRef::new(
                ref_count as *const AtomicU32,
                value_copy.as_ptr(),
                value_copy.len(),
            ))
        }
    }

    fn set(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let ttl = ttl.unwrap_or(self.default_ttl);
        self.set_item(key, value, ttl)
    }

    fn delete(&self, key: &[u8]) -> bool {
        self.delete_item(key)
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.contains_key(key)
    }

    fn flush(&self) {
        // No-op for now
    }

    fn add(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let ttl = ttl.unwrap_or(self.default_ttl);
        self.add_item(key, value, ttl)
    }

    fn replace(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let ttl = ttl.unwrap_or(self.default_ttl);
        self.replace_item(key, value, ttl)
    }

    fn get_with_cas(&self, key: &[u8]) -> Option<(Vec<u8>, u64)> {
        let verifier = HeapCacheVerifier::new(&self.storage);
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;

        let slot_loc = SlotLocation::from_location(location);
        let slot = self.storage.get(slot_loc.slot_index())?;

        let entry = slot.get(slot_loc.generation(), false)?;
        let value = entry.value().to_vec();
        let cas_token = entry.cas_token();
        slot.release_read();

        Some((value, cas_token))
    }

    fn with_value_cas<F, R>(&self, key: &[u8], f: F) -> Option<(R, u64)>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let verifier = HeapCacheVerifier::new(&self.storage);
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;

        let slot_loc = SlotLocation::from_location(location);
        let slot = self.storage.get(slot_loc.slot_index())?;

        let entry = slot.get(slot_loc.generation(), false)?;
        let result = f(entry.value());
        let cas_token = entry.cas_token();
        slot.release_read();

        Some((result, cas_token))
    }

    fn cas(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
        expected_cas: u64,
    ) -> Result<bool, CacheError> {
        let verifier = HeapCacheVerifier::new(&self.storage);

        let (location, _freq) = self
            .hashtable
            .lookup(key, &verifier)
            .ok_or(CacheError::KeyNotFound)?;

        let slot_loc = SlotLocation::from_location(location);
        let slot = self
            .storage
            .get(slot_loc.slot_index())
            .ok_or(CacheError::KeyNotFound)?;

        let entry = slot
            .get(slot_loc.generation(), false)
            .ok_or(CacheError::KeyNotFound)?;
        let current_cas = entry.cas_token();
        slot.release_read();

        if current_cas != expected_cas {
            return Ok(false);
        }

        let ttl = ttl.unwrap_or(self.default_ttl);
        self.set_item(key, value, ttl)?;

        Ok(true)
    }
}

/// Builder for [`HeapCache`].
#[derive(Debug, Clone)]
pub struct HeapCacheBuilder {
    memory_limit: usize,
    hashtable_power: u8,
    default_ttl: Duration,
    initial_fragmentation_ratio: u32,
}

impl Default for HeapCacheBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HeapCacheBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            memory_limit: 1024 * 1024 * 1024, // 1GB default
            hashtable_power: 20,
            default_ttl: DEFAULT_TTL,
            initial_fragmentation_ratio: DEFAULT_FRAGMENTATION_RATIO,
        }
    }

    /// Set the memory limit in bytes.
    ///
    /// The cache will evict items to stay under this limit, accounting
    /// for estimated allocator fragmentation.
    pub fn memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit = bytes;
        self
    }

    /// Set the hashtable power (2^power buckets).
    ///
    /// This determines both the hashtable size and the maximum number of
    /// items the cache can hold (slot capacity = 2^power).
    pub fn hashtable_power(mut self, power: u8) -> Self {
        self.hashtable_power = power;
        self
    }

    /// Set the default TTL for items.
    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Set the initial fragmentation ratio as a percentage.
    ///
    /// 100 = no overhead (1.0x), 120 = 20% overhead (1.2x).
    /// Will be calibrated automatically over time.
    pub fn initial_fragmentation_ratio(mut self, ratio: u32) -> Self {
        self.initial_fragmentation_ratio = ratio.clamp(100, 300);
        self
    }

    /// Build the HeapCache.
    pub fn build(self) -> HeapCache {
        // Slot capacity is derived from hashtable power - the hashtable
        // constrains max items, so slot storage matches that limit.
        // MultiChoiceHashtable has 8 items per bucket, so max items = 2^power * 8.
        let slot_capacity = (1usize << self.hashtable_power) * 8;

        HeapCache {
            hashtable: Arc::new(MultiChoiceHashtable::new(self.hashtable_power)),
            storage: SlotStorage::new(slot_capacity),
            default_ttl: self.default_ttl,
            eviction_counter: AtomicU32::new(0),
            cas_counter: AtomicU64::new(1),
            bytes_used: AtomicUsize::new(0),
            bytes_limit: self.memory_limit,
            fragmentation_ratio: AtomicU32::new(self.initial_fragmentation_ratio),
            ops_counter: AtomicU64::new(0),
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    fn create_test_cache() -> HeapCache {
        HeapCacheBuilder::new()
            .memory_limit(1024 * 1024) // 1MB
            .hashtable_power(10) // 2^10 = 1024 slots
            .initial_fragmentation_ratio(100) // No fragmentation for predictable tests
            .build()
    }

    #[test]
    fn test_cache_creation() {
        let cache = create_test_cache();
        assert_eq!(cache.capacity(), 8192); // 2^10 * 8 (8 items per bucket)
        assert!(cache.is_empty());
        assert_eq!(cache.bytes_used(), 0);
    }

    #[test]
    fn test_set_and_get() {
        let cache = create_test_cache();

        cache
            .set(b"key1", b"value1", Some(Duration::from_secs(3600)))
            .unwrap();
        cache
            .set(b"key2", b"value2", Some(Duration::from_secs(3600)))
            .unwrap();

        let v1 = cache.get(b"key1").expect("key1 should exist");
        assert_eq!(v1.value(), b"value1");

        let v2 = cache.get(b"key2").expect("key2 should exist");
        assert_eq!(v2.value(), b"value2");

        assert!(cache.get(b"key3").is_none());

        // Check bytes are tracked
        assert!(cache.bytes_used() > 0);
    }

    #[test]
    fn test_bytes_tracking() {
        let cache = create_test_cache();

        let initial = cache.bytes_used();
        assert_eq!(initial, 0);

        cache.set(b"key", b"value", None).unwrap();
        let after_set = cache.bytes_used();
        assert!(after_set > 0);

        cache.delete(b"key");
        let after_delete = cache.bytes_used();
        assert_eq!(after_delete, 0);
    }

    #[test]
    fn test_memory_limit_eviction() {
        // Create a cache with a small memory limit
        let cache = HeapCacheBuilder::new()
            .memory_limit(1024) // 1KB limit
            .hashtable_power(7) // 2^7 = 128 slots (plenty for this test)
            .initial_fragmentation_ratio(100)
            .build();

        // Fill with items until we hit the memory limit
        let value = vec![b'x'; 100]; // 100 byte values
        for i in 0..20 {
            let key = format!("key_{:02}", i);
            let _ = cache.set(key.as_bytes(), &value, None);
        }

        // Should have evicted some items to stay under limit
        assert!(cache.bytes_used() <= 1024);
        assert!(cache.len() < 20);
    }

    #[test]
    fn test_update() {
        let cache = create_test_cache();

        cache.set(b"key", b"value1", None).unwrap();
        let bytes1 = cache.bytes_used();

        cache.set(b"key", b"value2", None).unwrap();
        let bytes2 = cache.bytes_used();

        // Bytes should be similar (same size value)
        assert_eq!(bytes1, bytes2);
        assert_eq!(cache.get(b"key").unwrap().value(), b"value2");
    }

    #[test]
    fn test_delete() {
        let cache = create_test_cache();

        cache.set(b"key", b"value", None).unwrap();
        assert!(cache.contains(b"key"));
        assert!(cache.bytes_used() > 0);

        let deleted = cache.delete(b"key");
        assert!(deleted);
        assert!(!cache.contains(b"key"));
        assert_eq!(cache.bytes_used(), 0);
    }

    #[test]
    fn test_fragmentation_ratio() {
        let cache = HeapCacheBuilder::new()
            .memory_limit(10000)
            .hashtable_power(7) // 2^7 = 128 slots
            .initial_fragmentation_ratio(150) // 50% overhead
            .build();

        assert_eq!(cache.fragmentation_ratio(), 150);

        // With 150% ratio, estimated memory is 1.5x tracked
        cache.set(b"key", b"value", None).unwrap();
        let tracked = cache.bytes_used();
        let estimated = cache.estimated_memory_used();
        assert_eq!(estimated, tracked * 150 / 100);
    }

    #[test]
    fn test_with_value() {
        let cache = create_test_cache();

        cache.set(b"key", b"hello", None).unwrap();

        let len = cache.with_value(b"key", |v| v.len());
        assert_eq!(len, Some(5));

        let missing = cache.with_value(b"missing", |v| v.len());
        assert!(missing.is_none());
    }

    #[test]
    fn test_cas_operations() {
        let cache = create_test_cache();

        cache.set(b"key", b"value1", None).unwrap();

        let (value, cas_token) = cache.get_with_cas(b"key").unwrap();
        assert_eq!(value, b"value1");

        let result = cache.cas(b"key", b"value2", None, cas_token).unwrap();
        assert!(result);

        let (new_value, new_cas) = cache.get_with_cas(b"key").unwrap();
        assert_eq!(new_value, b"value2");
        assert_ne!(new_cas, cas_token);

        let result = cache.cas(b"key", b"value3", None, cas_token).unwrap();
        assert!(!result);

        assert_eq!(cache.get(b"key").unwrap().value(), b"value2");
    }

    #[test]
    fn test_cas_not_found() {
        let cache = create_test_cache();

        let result = cache.cas(b"nonexistent", b"value", None, 1);
        assert!(matches!(result, Err(CacheError::KeyNotFound)));
    }

    #[test]
    fn test_len_and_is_empty() {
        let cache = create_test_cache();

        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);

        cache.set(b"key1", b"value1", None).unwrap();
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);

        cache.set(b"key2", b"value2", None).unwrap();
        assert_eq!(cache.len(), 2);

        cache.delete(b"key1");
        assert_eq!(cache.len(), 1);

        cache.delete(b"key2");
        assert!(cache.is_empty());
    }
}
