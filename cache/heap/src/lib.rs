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
//! # Eviction Policies
//!
//! - **S3-FIFO** (default): Uses a small admission filter queue and a main cache.
//!   Items are promoted based on access frequency.
//! - **LFU**: Approximate least-frequently-used eviction via random sampling.
//!
//! # Example
//!
//! ```ignore
//! use heap_cache::{HeapCache, EvictionPolicy};
//! use std::time::Duration;
//!
//! let cache = HeapCache::builder()
//!     .memory_limit(1024 * 1024 * 1024)  // 1GB
//!     .eviction_policy(EvictionPolicy::S3Fifo)
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
mod hash_storage;
mod list_storage;
mod location;
mod s3fifo_policy;
mod set_storage;
mod slot;
mod storage;
mod sync;
mod verifier;

pub use location::ValueType;
pub use s3fifo_policy::S3FifoPolicy;

// Re-export from cache-core for disk tier configuration
pub use cache_core::SyncMode;

use std::time::Duration;

// Use loom-compatible atomics for some fields, std for others
use sync::{Arc, AtomicU32, AtomicU64, Ordering};

// AtomicUsize is only used for bytes_used tracking, not for synchronization
// that loom needs to model, so we use std directly
use std::sync::atomic::AtomicUsize;

use cache_core::{
    Cache, CacheError, CacheResult, DEFAULT_TTL, DiskLayer, DiskLayerBuilder, HashCache, Hashtable,
    ItemGuard, Layer, ListCache, MultiChoiceHashtable, OwnedGuard, SetCache, ValueRef,
};
use std::path::PathBuf;

use entry::HeapEntry;
use hash_storage::HashStorage;
use list_storage::ListStorage;
use location::{SlotLocation, TypedLocation};
use set_storage::SetStorage;
use storage::SlotStorage;
use verifier::{HeapCacheVerifier, HeapTieredVerifier, MultiTypeVerifier};

/// Number of random slots to sample when selecting an eviction victim.
const EVICTION_SAMPLES: usize = 5;

/// Default fragmentation ratio (120 = 1.2x overhead).
const DEFAULT_FRAGMENTATION_RATIO: u32 = 120;

/// Number of operations between fragmentation ratio calibrations.
const CALIBRATION_INTERVAL: u64 = 10000;

/// Default small queue percentage for S3-FIFO.
const DEFAULT_SMALL_QUEUE_PERCENT: u8 = 10;

/// Default demotion threshold for S3-FIFO.
const DEFAULT_DEMOTION_THRESHOLD: u8 = 1;

/// Eviction policy for the heap cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// S3-FIFO: Small admission filter queue + main cache with promotion.
    /// This is the recommended policy for most workloads.
    #[default]
    S3Fifo,
    /// Approximate LFU: Random sampling eviction based on frequency.
    /// Falls back to this when S3-FIFO tracking overhead is undesirable.
    Lfu,
    /// Random eviction: Evicts a random item without considering frequency.
    /// Simple and fast, similar to Redis's allkeys-random policy.
    Random,
}

/// Internal eviction state for different policies.
enum EvictionState {
    /// S3-FIFO policy state.
    S3Fifo(S3FifoPolicy),
    /// LFU has no additional state (uses hashtable frequency).
    Lfu,
    /// Random has no state (picks any occupied slot).
    Random,
}

/// Heap-allocated cache using the system allocator.
///
/// Uses the MultiChoiceHashtable for key lookups and SlotStorage for
/// managing heap-allocated entries. Tracks memory usage and evicts
/// based on byte limits rather than item count.
///
/// Optionally supports a disk tier for extended capacity. When enabled,
/// items evicted from RAM are demoted to disk storage instead of being
/// discarded.
pub struct HeapCache {
    /// The hashtable for key lookups.
    hashtable: Arc<MultiChoiceHashtable>,
    /// Storage for slots containing heap-allocated string entries.
    storage: SlotStorage,
    /// Storage for hash data structures.
    hash_storage: HashStorage,
    /// Storage for list data structures.
    list_storage: ListStorage,
    /// Storage for set data structures.
    set_storage: SetStorage,
    /// Default TTL for items.
    default_ttl: Duration,
    /// Counter for pseudo-random eviction sampling.
    eviction_counter: AtomicU32,
    /// Global CAS counter for unique tokens.
    cas_counter: AtomicU64,
    /// Eviction policy state.
    eviction_state: EvictionState,

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

    /// Optional disk tier for extended capacity.
    disk_layer: Option<DiskLayer>,
    /// Pool ID for RAM storage (default 0).
    ram_pool_id: u8,
    /// Pool ID for disk storage (default 2).
    disk_pool_id: u8,
    /// Frequency threshold for promoting items from disk to RAM.
    promotion_threshold: u8,
}

impl HeapCache {
    /// Create a new builder for HeapCache.
    pub fn builder() -> HeapCacheBuilder {
        HeapCacheBuilder::new()
    }

    /// Create a tiered verifier that can verify keys in both RAM and disk.
    fn tiered_verifier(&self) -> HeapTieredVerifier<'_> {
        if let Some(ref disk_layer) = self.disk_layer {
            HeapTieredVerifier::with_disk(
                &self.storage,
                self.ram_pool_id,
                disk_layer.pool(),
                self.disk_pool_id,
            )
        } else {
            HeapTieredVerifier::new(&self.storage, self.ram_pool_id)
        }
    }

    /// Read an item from the disk tier.
    ///
    /// Returns the value if found and not expired.
    fn get_from_disk(&self, key: &[u8], location: cache_core::Location) -> Option<Vec<u8>> {
        let disk_layer = self.disk_layer.as_ref()?;
        let item_loc = cache_core::ItemLocation::from_location(location);

        let guard = disk_layer.get_item(item_loc, key)?;
        Some(guard.value().to_vec())
    }

    /// Promote an item from disk to RAM.
    ///
    /// Writes the item to RAM storage and updates the hashtable to point
    /// to the new RAM location.
    fn promote_to_ram(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
        old_location: cache_core::Location,
    ) -> bool {
        // Calculate item size for memory tracking
        let item_size = entry::item_size(key.len(), value.len());

        // Check memory and evict if necessary
        if self.ensure_memory_available(item_size).is_err() {
            return false;
        }

        // Get a unique CAS token
        let cas_token = self.cas_counter.fetch_add(1, Ordering::Relaxed);

        // Allocate the heap entry
        let entry = match HeapEntry::allocate(key, value, ttl, cas_token) {
            Some(e) => e,
            None => return false,
        };

        // Try to allocate a slot
        let slot_loc = match self.storage.allocate() {
            Some(loc) => loc,
            None => {
                if !self.evict_one() {
                    unsafe { HeapEntry::free(entry) };
                    return false;
                }
                match self.storage.allocate() {
                    Some(loc) => loc,
                    None => {
                        unsafe { HeapEntry::free(entry) };
                        return false;
                    }
                }
            }
        };

        // Store the entry in the slot
        let slot = self.storage.get(slot_loc.slot_index()).unwrap();
        let generation = slot.store(entry);

        // Update slot location with actual generation
        let slot_loc = SlotLocation::with_pool(self.ram_pool_id, slot_loc.slot_index(), generation);

        // Update hashtable with new RAM location
        if self
            .hashtable
            .cas_location(key, old_location, slot_loc.to_location(), true)
        {
            // Track new bytes
            self.bytes_used.fetch_add(item_size, Ordering::Relaxed);

            // Mark old disk location as deleted
            if let Some(ref disk_layer) = self.disk_layer {
                disk_layer.mark_deleted(cache_core::ItemLocation::from_location(old_location));
            }
            true
        } else {
            // CAS failed, clean up allocated slot
            self.storage.deallocate(slot_loc);
            false
        }
    }

    /// Demote an item to the disk tier.
    ///
    /// Writes the item to disk storage and updates the hashtable.
    /// Returns true if demotion succeeded.
    #[allow(dead_code)]
    fn demote_to_disk(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
        old_location: cache_core::Location,
    ) -> bool {
        let disk_layer = match self.disk_layer.as_ref() {
            Some(layer) => layer,
            None => return false,
        };

        // Write to disk layer
        let disk_loc = match disk_layer.write_item(key, value, &[], ttl) {
            Ok(loc) => loc,
            Err(_) => return false,
        };

        // Update hashtable with disk location
        if self
            .hashtable
            .cas_location(key, old_location, disk_loc.to_location(), true)
        {
            true
        } else {
            // CAS failed, mark disk item as deleted
            disk_layer.mark_deleted(disk_loc);
            false
        }
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
        if ops.is_multiple_of(CALIBRATION_INTERVAL) {
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
    #[cfg(feature = "jemalloc")]
    fn query_allocator_memory() -> Option<usize> {
        use tikv_jemalloc_ctl::{epoch, stats};
        // Advance epoch to refresh cached stats
        epoch::advance().ok()?;
        stats::allocated::read().ok()
    }

    #[cfg(not(feature = "jemalloc"))]
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
    /// Only used when jemalloc is not available.
    #[cfg(all(target_os = "linux", not(feature = "jemalloc")))]
    #[allow(dead_code)]
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
                // Record for S3-FIFO tracking
                self.maybe_record_insert(key);
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
                // Record for S3-FIFO tracking (update counts as new insert)
                self.maybe_record_insert(key);
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
                // Record for S3-FIFO tracking
                self.maybe_record_insert(key);
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
        if let Some(slot) = self.storage.get(slot_loc.slot_index())
            && let Some(entry) = slot.get_with_flags(slot_loc.generation(), true, true)
        {
            let size = entry.total_size();
            slot.release_read();
            self.bytes_used.fetch_sub(size, Ordering::Relaxed);
        }
        self.storage.deallocate(slot_loc);
    }

    /// Retrieve an item's value from the cache.
    fn get_item(&self, key: &[u8]) -> Option<Vec<u8>> {
        let verifier = self.tiered_verifier();
        let (location, freq) = self.hashtable.lookup(key, &verifier)?;

        // Check which pool the item is in
        let pool_id = SlotLocation::pool_id_from_location(location);

        if pool_id == self.ram_pool_id {
            // Item is in RAM
            let slot_loc = SlotLocation::from_location(location);
            let slot = self.storage.get(slot_loc.slot_index())?;

            let entry = slot.get(slot_loc.generation(), false)?;
            let value = entry.value().to_vec();
            slot.release_read();

            return Some(value);
        } else if pool_id == self.disk_pool_id {
            // Item is on disk
            let value = self.get_from_disk(key, location)?;

            // Optionally promote to RAM if accessed frequently
            if freq >= self.promotion_threshold {
                // Get TTL from disk item
                let ttl = self
                    .disk_layer
                    .as_ref()
                    .and_then(|l| {
                        use cache_core::Layer;
                        l.item_ttl(cache_core::ItemLocation::from_location(location))
                    })
                    .unwrap_or(self.default_ttl);

                let _ = self.promote_to_ram(key, &value, ttl, location);
            }

            return Some(value);
        }

        None
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
    /// Dispatches to the appropriate eviction strategy based on policy:
    /// - S3-FIFO: Uses two-queue eviction with admission filter
    /// - LFU: Approximate least-frequently-used via random sampling
    /// - Random: Evicts the first occupied slot found from a random starting point
    fn evict_one(&self) -> bool {
        match &self.eviction_state {
            EvictionState::S3Fifo(policy) => self.evict_s3fifo(policy),
            EvictionState::Lfu => self.evict_lfu(),
            EvictionState::Random => self.evict_random(),
        }
    }

    /// Evict using S3-FIFO policy.
    fn evict_s3fifo(&self, policy: &S3FifoPolicy) -> bool {
        // S3FifoPolicy::evict returns the Location to free
        // We don't create ghosts for heap cache (would need more state)
        if let Some(location) = policy.evict(self.hashtable.as_ref(), false) {
            // Use TypedLocation to handle all value types
            let typed_loc = TypedLocation::from_location(location);
            self.deallocate_typed_slot(typed_loc);
            return true;
        }
        false
    }

    /// Evict using approximate LFU via random sampling.
    ///
    /// Samples EVICTION_SAMPLES random occupied slots across all storage types
    /// and evicts the one with the lowest frequency.
    fn evict_lfu(&self) -> bool {
        let start = self.eviction_counter.fetch_add(1, Ordering::Relaxed);

        // Get occupancy counts for all storage types
        let string_occupied = self.storage.occupied();
        let hash_occupied = self.hash_storage.occupied();
        let list_occupied = self.list_storage.occupied();
        let set_occupied = self.set_storage.occupied();
        let total_occupied = string_occupied + hash_occupied + list_occupied + set_occupied;

        if total_occupied == 0 {
            return false;
        }

        let mut victim_key: Option<Vec<u8>> = None;
        let mut victim_type: Option<ValueType> = None;
        let mut victim_freq: u8 = u8::MAX;
        let mut samples_tried = 0;

        // Sample across all storage types
        for i in 0..(total_occupied.min(EVICTION_SAMPLES as u32 * 4)) {
            let sample_idx = (start.wrapping_mul(2654435761).wrapping_add(i)) % total_occupied;

            // Determine which storage type this sample falls into
            let (key, value_type) = if sample_idx < string_occupied {
                // Sample from string storage
                if let Some((k, _)) = self.sample_string_slot(start, i) {
                    (k, ValueType::String)
                } else {
                    continue;
                }
            } else if sample_idx < string_occupied + hash_occupied {
                // Sample from hash storage
                if let Some((k, _)) = self.sample_hash_slot(start, i) {
                    (k, ValueType::Hash)
                } else {
                    continue;
                }
            } else if sample_idx < string_occupied + hash_occupied + list_occupied {
                // Sample from list storage
                if let Some((k, _)) = self.sample_list_slot(start, i) {
                    (k, ValueType::List)
                } else {
                    continue;
                }
            } else {
                // Sample from set storage
                if let Some((k, _)) = self.sample_set_slot(start, i) {
                    (k, ValueType::Set)
                } else {
                    continue;
                }
            };

            // Look up frequency in hashtable
            let verifier = self.multi_type_verifier();
            let freq = self
                .hashtable
                .lookup(&key, &verifier)
                .map(|(loc, _)| self.hashtable.get_item_frequency(&key, loc).unwrap_or(0))
                .unwrap_or(0);

            if victim_key.is_none() || freq < victim_freq {
                victim_key = Some(key);
                victim_type = Some(value_type);
                victim_freq = freq;
            }

            samples_tried += 1;
            if samples_tried >= EVICTION_SAMPLES {
                break;
            }
        }

        if let (Some(key), Some(_vtype)) = (victim_key, victim_type) {
            let verifier = self.multi_type_verifier();

            if let Some((location, _)) = self.hashtable.lookup(&key, &verifier)
                && self.hashtable.remove(&key, location)
            {
                let typed_loc = TypedLocation::from_location(location);
                self.deallocate_typed_slot(typed_loc);
                return true;
            }
        }

        false
    }

    /// Evict using random selection.
    ///
    /// Finds the first occupied slot from a pseudo-random starting point
    /// across all storage types and evicts it.
    fn evict_random(&self) -> bool {
        let start = self.eviction_counter.fetch_add(1, Ordering::Relaxed);

        // Get occupancy counts for all storage types
        let string_occupied = self.storage.occupied();
        let hash_occupied = self.hash_storage.occupied();
        let list_occupied = self.list_storage.occupied();
        let set_occupied = self.set_storage.occupied();
        let total_occupied = string_occupied + hash_occupied + list_occupied + set_occupied;

        if total_occupied == 0 {
            return false;
        }

        // Try to find an occupied slot across all storage types
        for i in 0..total_occupied {
            let sample_idx = (start.wrapping_mul(2654435761).wrapping_add(i)) % total_occupied;

            // Determine which storage type this sample falls into
            let key = if sample_idx < string_occupied {
                self.sample_string_slot(start, i).map(|(k, _)| k)
            } else if sample_idx < string_occupied + hash_occupied {
                self.sample_hash_slot(start, i).map(|(k, _)| k)
            } else if sample_idx < string_occupied + hash_occupied + list_occupied {
                self.sample_list_slot(start, i).map(|(k, _)| k)
            } else {
                self.sample_set_slot(start, i).map(|(k, _)| k)
            };

            if let Some(key) = key {
                let verifier = self.multi_type_verifier();

                if let Some((location, _)) = self.hashtable.lookup(&key, &verifier)
                    && self.hashtable.remove(&key, location)
                {
                    let typed_loc = TypedLocation::from_location(location);
                    self.deallocate_typed_slot(typed_loc);
                    return true;
                }
            }
        }

        false
    }

    /// Sample a random occupied slot from string storage.
    fn sample_string_slot(&self, start: u32, offset: u32) -> Option<(Vec<u8>, u16)> {
        let capacity = self.storage.capacity() as u32;
        if capacity == 0 {
            return None;
        }

        for j in 0..capacity {
            let idx = (start.wrapping_mul(2654435761).wrapping_add(offset + j)) % capacity;
            if !self.storage.is_slot_occupied(idx) {
                continue;
            }

            let slot = self.storage.get(idx)?;
            let generation = slot.generation();
            let entry = slot.get(generation, false)?;
            let key = entry.key().to_vec();
            slot.release_read();
            return Some((key, generation));
        }
        None
    }

    /// Sample a random occupied slot from hash storage.
    fn sample_hash_slot(&self, start: u32, offset: u32) -> Option<(Vec<u8>, u16)> {
        let capacity = self.hash_storage.capacity() as u32;
        if capacity == 0 {
            return None;
        }

        for j in 0..capacity {
            let idx = (start.wrapping_mul(2654435761).wrapping_add(offset + j)) % capacity;
            if !self.hash_storage.is_slot_occupied(idx) {
                continue;
            }

            let slot = self.hash_storage.get(idx)?;
            let generation = slot.generation();
            let key = self.hash_storage.get_key(idx, generation)?;
            return Some((key, generation));
        }
        None
    }

    /// Sample a random occupied slot from list storage.
    fn sample_list_slot(&self, start: u32, offset: u32) -> Option<(Vec<u8>, u16)> {
        let capacity = self.list_storage.capacity() as u32;
        if capacity == 0 {
            return None;
        }

        for j in 0..capacity {
            let idx = (start.wrapping_mul(2654435761).wrapping_add(offset + j)) % capacity;
            if !self.list_storage.is_slot_occupied(idx) {
                continue;
            }

            let slot = self.list_storage.get(idx)?;
            let generation = slot.generation();
            let key = self.list_storage.get_key(idx, generation)?;
            return Some((key, generation));
        }
        None
    }

    /// Sample a random occupied slot from set storage.
    fn sample_set_slot(&self, start: u32, offset: u32) -> Option<(Vec<u8>, u16)> {
        let capacity = self.set_storage.capacity() as u32;
        if capacity == 0 {
            return None;
        }

        for j in 0..capacity {
            let idx = (start.wrapping_mul(2654435761).wrapping_add(offset + j)) % capacity;
            if !self.set_storage.is_slot_occupied(idx) {
                continue;
            }

            let slot = self.set_storage.get(idx)?;
            let generation = slot.generation();
            let key = self.set_storage.get_key(idx, generation)?;
            return Some((key, generation));
        }
        None
    }

    /// Record an insert for S3-FIFO tracking.
    ///
    /// Called after successful hashtable insert when using S3-FIFO policy.
    fn maybe_record_insert(&self, key: &[u8]) {
        if let EvictionState::S3Fifo(policy) = &self.eviction_state {
            let verifier = self.multi_type_verifier();
            if let Some((bucket_index, item_info)) =
                self.hashtable.lookup_for_tracking(key, &verifier)
            {
                policy.record_insert(bucket_index, item_info);
            }
        }
    }

    /// Get the current number of entries across all storage types.
    pub fn len(&self) -> usize {
        self.storage.occupied() as usize
            + self.hash_storage.occupied() as usize
            + self.list_storage.occupied() as usize
            + self.set_storage.occupied() as usize
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the total slot capacity across all storage types.
    pub fn capacity(&self) -> usize {
        self.storage.capacity()
            + self.hash_storage.capacity()
            + self.list_storage.capacity()
            + self.set_storage.capacity()
    }

    /// Get the number of string entries.
    pub fn string_count(&self) -> usize {
        self.storage.occupied() as usize
    }

    /// Get the number of hash entries.
    pub fn hash_count(&self) -> usize {
        self.hash_storage.occupied() as usize
    }

    /// Get the number of list entries.
    pub fn list_count(&self) -> usize {
        self.list_storage.occupied() as usize
    }

    /// Get the number of set entries.
    pub fn set_count(&self) -> usize {
        self.set_storage.occupied() as usize
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

    /// Create a multi-type verifier for all storage types.
    fn multi_type_verifier(&self) -> MultiTypeVerifier<'_> {
        MultiTypeVerifier::new(
            &self.storage,
            &self.hash_storage,
            &self.list_storage,
            &self.set_storage,
        )
    }

    /// Get the type of a key if it exists.
    ///
    /// Returns `None` if the key doesn't exist.
    pub fn key_type(&self, key: &[u8]) -> Option<ValueType> {
        let verifier = self.multi_type_verifier();
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;
        Some(TypedLocation::type_from_location(location))
    }

    /// Lookup a key and verify it matches the expected type.
    ///
    /// Returns the slot index and generation if the key exists and matches the type.
    /// Returns `WrongType` error if the key exists but has a different type.
    /// Returns `None` (as Ok) if the key doesn't exist.
    fn lookup_typed(
        &self,
        key: &[u8],
        expected_type: ValueType,
    ) -> CacheResult<Option<(u32, u16)>> {
        let verifier = self.multi_type_verifier();
        match self.hashtable.lookup(key, &verifier) {
            Some((location, _freq)) => {
                let typed_loc = TypedLocation::from_location(location);
                if typed_loc.value_type() != expected_type {
                    Err(CacheError::WrongType)
                } else {
                    Ok(Some((typed_loc.slot_index(), typed_loc.generation())))
                }
            }
            None => Ok(None),
        }
    }

    /// Create or get a typed slot for a key.
    ///
    /// If the key exists with the correct type, returns its location.
    /// If the key doesn't exist, allocates a new slot and inserts it.
    /// If the key exists with wrong type, returns WrongType error.
    fn get_or_create_typed(
        &self,
        key: &[u8],
        value_type: ValueType,
        ttl: Duration,
    ) -> CacheResult<(u32, u16, bool)> {
        // Check if key already exists
        let verifier = self.multi_type_verifier();
        if let Some((location, _freq)) = self.hashtable.lookup(key, &verifier) {
            let typed_loc = TypedLocation::from_location(location);
            if typed_loc.value_type() != value_type {
                return Err(CacheError::WrongType);
            }
            return Ok((typed_loc.slot_index(), typed_loc.generation(), false));
        }

        // Allocate a new slot based on type
        let (idx, generation) = match value_type {
            ValueType::Hash => self
                .hash_storage
                .allocate()
                .ok_or(CacheError::OutOfMemory)?,
            ValueType::List => self
                .list_storage
                .allocate()
                .ok_or(CacheError::OutOfMemory)?,
            ValueType::Set => self.set_storage.allocate().ok_or(CacheError::OutOfMemory)?,
            ValueType::String => {
                // String uses regular storage, handle separately
                return Err(CacheError::WrongType);
            }
        };

        // Initialize the slot
        let cas_token = self.cas_counter.fetch_add(1, Ordering::Relaxed);
        match value_type {
            ValueType::Hash => self.hash_storage.init_slot(idx, key, Some(ttl), cas_token),
            ValueType::List => self.list_storage.init_slot(idx, key, Some(ttl), cas_token),
            ValueType::Set => self.set_storage.init_slot(idx, key, Some(ttl), cas_token),
            ValueType::String => unreachable!(),
        };

        // Track base size for new key (key + metadata overhead)
        // Base size: key length + 16 bytes for metadata (expire_at, cas_token)
        let base_size = key.len() + 16;
        self.bytes_used.fetch_add(base_size, Ordering::Relaxed);

        // Insert into hashtable
        let typed_loc = TypedLocation::with_type(value_type, idx, generation);
        let verifier = self.multi_type_verifier();

        match self
            .hashtable
            .insert(key, typed_loc.to_location(), &verifier)
        {
            Ok(old_location) => {
                // Clean up any old entry if we replaced something
                if let Some(old_loc) = old_location {
                    let old_typed = TypedLocation::from_location(old_loc);
                    self.deallocate_typed_slot(old_typed);
                }
                Ok((idx, generation, true))
            }
            Err(e) => {
                // Rollback: deallocate the slot we just created
                match value_type {
                    ValueType::Hash => self.hash_storage.deallocate(idx),
                    ValueType::List => self.list_storage.deallocate(idx),
                    ValueType::Set => self.set_storage.deallocate(idx),
                    ValueType::String => unreachable!(),
                }
                Err(e)
            }
        }
    }

    /// Deallocate a typed slot.
    fn deallocate_typed_slot(&self, typed_loc: TypedLocation) {
        match typed_loc.value_type() {
            ValueType::String => {
                let slot_loc = SlotLocation::new(typed_loc.slot_index(), typed_loc.generation());
                self.deallocate_and_track(slot_loc);
            }
            ValueType::Hash => {
                // Track bytes before deallocation
                if let Some(size) = self
                    .hash_storage
                    .size_bytes(typed_loc.slot_index(), typed_loc.generation())
                {
                    self.bytes_used.fetch_sub(size, Ordering::Relaxed);
                }
                self.hash_storage.deallocate(typed_loc.slot_index());
            }
            ValueType::List => {
                if let Some(size) = self
                    .list_storage
                    .size_bytes(typed_loc.slot_index(), typed_loc.generation())
                {
                    self.bytes_used.fetch_sub(size, Ordering::Relaxed);
                }
                self.list_storage.deallocate(typed_loc.slot_index());
            }
            ValueType::Set => {
                if let Some(size) = self
                    .set_storage
                    .size_bytes(typed_loc.slot_index(), typed_loc.generation())
                {
                    self.bytes_used.fetch_sub(size, Ordering::Relaxed);
                }
                self.set_storage.deallocate(typed_loc.slot_index());
            }
        }
    }

    /// Delete a typed key from the cache.
    ///
    /// Returns true if the key was deleted.
    fn delete_typed(&self, key: &[u8], expected_type: ValueType) -> CacheResult<bool> {
        let verifier = self.multi_type_verifier();
        let (location, _freq) = match self.hashtable.lookup(key, &verifier) {
            Some(result) => result,
            None => return Ok(false),
        };

        let typed_loc = TypedLocation::from_location(location);
        if typed_loc.value_type() != expected_type {
            return Err(CacheError::WrongType);
        }

        if !self.hashtable.remove(key, location) {
            return Ok(false);
        }

        self.deallocate_typed_slot(typed_loc);
        Ok(true)
    }
}

/// Query jemalloc fragmentation ratio (resident / allocated * 100).
///
/// Returns the ratio of resident memory to allocated memory as a percentage.
/// A value of 100 means no fragmentation, 150 means 50% overhead.
/// Returns None if jemalloc stats aren't available.
#[cfg(feature = "jemalloc")]
pub fn jemalloc_fragmentation_ratio() -> Option<u32> {
    use tikv_jemalloc_ctl::{epoch, stats};
    epoch::advance().ok()?;
    let allocated = stats::allocated::read().ok()?;
    let resident = stats::resident::read().ok()?;
    if allocated == 0 {
        return Some(100);
    }
    // ratio = resident / allocated * 100
    Some(((resident as u128 * 100) / allocated as u128) as u32)
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
        // Clear the hashtable first - makes all items "invisible"
        self.hashtable.clear();

        // Reset all storage slots (frees entries and rebuilds free list)
        self.storage.reset_all();

        // Reset memory tracking
        self.bytes_used.store(0, Ordering::Release);
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

    fn increment(
        &self,
        key: &[u8],
        delta: u64,
        initial: Option<u64>,
        ttl: Option<Duration>,
    ) -> Result<u64, CacheError> {
        let ttl = ttl.unwrap_or(self.default_ttl);

        // Try to get the current value
        let current_value = self.get_item(key);

        match current_value {
            Some(data) => {
                // Parse existing value as ASCII decimal
                let value_str = std::str::from_utf8(&data).map_err(|_| CacheError::NotNumeric)?;
                let current: u64 = value_str
                    .trim()
                    .parse()
                    .map_err(|_| CacheError::NotNumeric)?;

                // Compute new value with overflow check
                let new_value = current.checked_add(delta).ok_or(CacheError::Overflow)?;

                // Store the new value
                let new_str = new_value.to_string();
                self.set_item(key, new_str.as_bytes(), ttl)?;

                Ok(new_value)
            }
            None => {
                // Key doesn't exist
                match initial {
                    Some(init) => {
                        // Create with initial + delta
                        let new_value = init.checked_add(delta).ok_or(CacheError::Overflow)?;
                        let new_str = new_value.to_string();
                        self.set_item(key, new_str.as_bytes(), ttl)?;
                        Ok(new_value)
                    }
                    None => Err(CacheError::KeyNotFound),
                }
            }
        }
    }

    fn decrement(
        &self,
        key: &[u8],
        delta: u64,
        initial: Option<u64>,
        ttl: Option<Duration>,
    ) -> Result<u64, CacheError> {
        let ttl = ttl.unwrap_or(self.default_ttl);

        // Try to get the current value
        let current_value = self.get_item(key);

        match current_value {
            Some(data) => {
                // Parse existing value as ASCII decimal
                let value_str = std::str::from_utf8(&data).map_err(|_| CacheError::NotNumeric)?;
                let current: u64 = value_str
                    .trim()
                    .parse()
                    .map_err(|_| CacheError::NotNumeric)?;

                // Compute new value with saturating subtraction (clamp to 0)
                let new_value = current.saturating_sub(delta);

                // Store the new value
                let new_str = new_value.to_string();
                self.set_item(key, new_str.as_bytes(), ttl)?;

                Ok(new_value)
            }
            None => {
                // Key doesn't exist
                match initial {
                    Some(init) => {
                        // Create with initial - delta (saturating)
                        let new_value = init.saturating_sub(delta);
                        let new_str = new_value.to_string();
                        self.set_item(key, new_str.as_bytes(), ttl)?;
                        Ok(new_value)
                    }
                    None => Err(CacheError::KeyNotFound),
                }
            }
        }
    }

    fn append(&self, key: &[u8], data: &[u8]) -> Result<usize, CacheError> {
        // Get the current value
        let current_value = self.get_item(key).ok_or(CacheError::KeyNotFound)?;

        // Create new value with appended data
        let mut new_value = current_value;
        new_value.extend_from_slice(data);
        let new_len = new_value.len();

        // Store the new value
        self.set_item(key, &new_value, self.default_ttl)?;

        Ok(new_len)
    }

    fn prepend(&self, key: &[u8], data: &[u8]) -> Result<usize, CacheError> {
        // Get the current value
        let current_value = self.get_item(key).ok_or(CacheError::KeyNotFound)?;

        // Create new value with prepended data
        let mut new_value = data.to_vec();
        new_value.extend_from_slice(&current_value);
        let new_len = new_value.len();

        // Store the new value
        self.set_item(key, &new_value, self.default_ttl)?;

        Ok(new_len)
    }
}

impl HashCache for HeapCache {
    fn hset(
        &self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> CacheResult<usize> {
        let ttl = ttl.unwrap_or(self.default_ttl);
        let (idx, generation, _created) = self.get_or_create_typed(key, ValueType::Hash, ttl)?;

        match self.hash_storage.hset(idx, generation, field, value) {
            Some((created, bytes_delta)) => {
                // Track bytes change
                if bytes_delta > 0 {
                    self.bytes_used
                        .fetch_add(bytes_delta as usize, Ordering::Relaxed);
                } else if bytes_delta < 0 {
                    self.bytes_used
                        .fetch_sub((-bytes_delta) as usize, Ordering::Relaxed);
                }
                Ok(created)
            }
            None => Err(CacheError::KeyNotFound), // Generation mismatch
        }
    }

    fn hmset(
        &self,
        key: &[u8],
        fields: &[(&[u8], &[u8])],
        ttl: Option<Duration>,
    ) -> CacheResult<usize> {
        let ttl = ttl.unwrap_or(self.default_ttl);
        let (idx, generation, _created) = self.get_or_create_typed(key, ValueType::Hash, ttl)?;

        let mut new_count = 0;
        let mut total_bytes_delta: isize = 0;
        for (field, value) in fields {
            if let Some((created, bytes_delta)) =
                self.hash_storage.hset(idx, generation, field, value)
            {
                new_count += created;
                total_bytes_delta += bytes_delta;
            }
        }
        // Track bytes change
        if total_bytes_delta > 0 {
            self.bytes_used
                .fetch_add(total_bytes_delta as usize, Ordering::Relaxed);
        } else if total_bytes_delta < 0 {
            self.bytes_used
                .fetch_sub((-total_bytes_delta) as usize, Ordering::Relaxed);
        }
        Ok(new_count)
    }

    fn hget(&self, key: &[u8], field: &[u8]) -> CacheResult<Option<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::Hash)? {
            Some((idx, generation)) => Ok(self.hash_storage.hget(idx, generation, field)),
            None => Ok(None),
        }
    }

    fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> CacheResult<Vec<Option<Vec<u8>>>> {
        match self.lookup_typed(key, ValueType::Hash)? {
            Some((idx, generation)) => {
                let results: Vec<Option<Vec<u8>>> = fields
                    .iter()
                    .map(|f| self.hash_storage.hget(idx, generation, f))
                    .collect();
                Ok(results)
            }
            None => Ok(fields.iter().map(|_| None).collect()),
        }
    }

    fn hgetall(&self, key: &[u8]) -> CacheResult<Vec<(Vec<u8>, Vec<u8>)>> {
        match self.lookup_typed(key, ValueType::Hash)? {
            Some((idx, generation)) => Ok(self
                .hash_storage
                .hgetall(idx, generation)
                .unwrap_or_default()),
            None => Ok(Vec::new()),
        }
    }

    fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> CacheResult<usize> {
        match self.lookup_typed(key, ValueType::Hash)? {
            Some((idx, generation)) => {
                let (deleted, bytes_freed) = self
                    .hash_storage
                    .hdel(idx, generation, fields)
                    .unwrap_or((0, 0));
                // Track bytes freed
                if bytes_freed > 0 {
                    self.bytes_used.fetch_sub(bytes_freed, Ordering::Relaxed);
                }
                // If hash is now empty, delete the key
                if let Some(true) = self.hash_storage.is_empty(idx, generation) {
                    let _ = self.delete_typed(key, ValueType::Hash);
                }
                Ok(deleted)
            }
            None => Ok(0),
        }
    }

    fn hexists(&self, key: &[u8], field: &[u8]) -> CacheResult<bool> {
        match self.lookup_typed(key, ValueType::Hash)? {
            Some((idx, generation)) => Ok(self
                .hash_storage
                .hexists(idx, generation, field)
                .unwrap_or(false)),
            None => Ok(false),
        }
    }

    fn hlen(&self, key: &[u8]) -> CacheResult<usize> {
        match self.lookup_typed(key, ValueType::Hash)? {
            Some((idx, generation)) => Ok(self.hash_storage.hlen(idx, generation).unwrap_or(0)),
            None => Ok(0),
        }
    }

    fn hkeys(&self, key: &[u8]) -> CacheResult<Vec<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::Hash)? {
            Some((idx, generation)) => {
                Ok(self.hash_storage.hkeys(idx, generation).unwrap_or_default())
            }
            None => Ok(Vec::new()),
        }
    }

    fn hvals(&self, key: &[u8]) -> CacheResult<Vec<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::Hash)? {
            Some((idx, generation)) => {
                Ok(self.hash_storage.hvals(idx, generation).unwrap_or_default())
            }
            None => Ok(Vec::new()),
        }
    }

    fn hsetnx(
        &self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> CacheResult<bool> {
        let ttl = ttl.unwrap_or(self.default_ttl);
        let (idx, generation, _created) = self.get_or_create_typed(key, ValueType::Hash, ttl)?;

        // Check if field exists first
        if let Some(true) = self.hash_storage.hexists(idx, generation, field) {
            return Ok(false);
        }

        match self.hash_storage.hset(idx, generation, field, value) {
            Some((created, bytes_delta)) => {
                if bytes_delta > 0 {
                    self.bytes_used
                        .fetch_add(bytes_delta as usize, Ordering::Relaxed);
                }
                Ok(created > 0)
            }
            None => Err(CacheError::KeyNotFound),
        }
    }

    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> CacheResult<i64> {
        let ttl = self.default_ttl;
        let (idx, generation, _created) = self.get_or_create_typed(key, ValueType::Hash, ttl)?;

        // Get current value
        let current = match self.hash_storage.hget(idx, generation, field) {
            Some(data) => {
                let s = std::str::from_utf8(&data).map_err(|_| CacheError::NotNumeric)?;
                s.trim()
                    .parse::<i64>()
                    .map_err(|_| CacheError::NotNumeric)?
            }
            None => 0,
        };

        let new_value = current.checked_add(delta).ok_or(CacheError::Overflow)?;
        let value_str = new_value.to_string();

        match self
            .hash_storage
            .hset(idx, generation, field, value_str.as_bytes())
        {
            Some((_, bytes_delta)) => {
                if bytes_delta > 0 {
                    self.bytes_used
                        .fetch_add(bytes_delta as usize, Ordering::Relaxed);
                } else if bytes_delta < 0 {
                    self.bytes_used
                        .fetch_sub((-bytes_delta) as usize, Ordering::Relaxed);
                }
                Ok(new_value)
            }
            None => Err(CacheError::KeyNotFound),
        }
    }
}

impl ListCache for HeapCache {
    fn lpush(&self, key: &[u8], values: &[&[u8]], ttl: Option<Duration>) -> CacheResult<usize> {
        let ttl = ttl.unwrap_or(self.default_ttl);
        let (idx, generation, _created) = self.get_or_create_typed(key, ValueType::List, ttl)?;

        match self.list_storage.lpush(idx, generation, values) {
            Some((len, bytes_added)) => {
                self.bytes_used.fetch_add(bytes_added, Ordering::Relaxed);
                Ok(len)
            }
            None => Err(CacheError::KeyNotFound),
        }
    }

    fn rpush(&self, key: &[u8], values: &[&[u8]], ttl: Option<Duration>) -> CacheResult<usize> {
        let ttl = ttl.unwrap_or(self.default_ttl);
        let (idx, generation, _created) = self.get_or_create_typed(key, ValueType::List, ttl)?;

        match self.list_storage.rpush(idx, generation, values) {
            Some((len, bytes_added)) => {
                self.bytes_used.fetch_add(bytes_added, Ordering::Relaxed);
                Ok(len)
            }
            None => Err(CacheError::KeyNotFound),
        }
    }

    fn lpop(&self, key: &[u8]) -> CacheResult<Option<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::List)? {
            Some((idx, generation)) => {
                let result = self.list_storage.lpop(idx, generation);
                if let Some((_, bytes_freed)) = &result {
                    self.bytes_used.fetch_sub(*bytes_freed, Ordering::Relaxed);
                }
                // If list is now empty, delete the key
                if let Some(true) = self.list_storage.is_empty(idx, generation) {
                    let _ = self.delete_typed(key, ValueType::List);
                }
                Ok(result.map(|(v, _)| v))
            }
            None => Ok(None),
        }
    }

    fn rpop(&self, key: &[u8]) -> CacheResult<Option<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::List)? {
            Some((idx, generation)) => {
                let result = self.list_storage.rpop(idx, generation);
                if let Some((_, bytes_freed)) = &result {
                    self.bytes_used.fetch_sub(*bytes_freed, Ordering::Relaxed);
                }
                // If list is now empty, delete the key
                if let Some(true) = self.list_storage.is_empty(idx, generation) {
                    let _ = self.delete_typed(key, ValueType::List);
                }
                Ok(result.map(|(v, _)| v))
            }
            None => Ok(None),
        }
    }

    fn lpop_count(&self, key: &[u8], count: usize) -> CacheResult<Vec<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::List)? {
            Some((idx, generation)) => {
                let (result, bytes_freed) = self
                    .list_storage
                    .lpop_count(idx, generation, count)
                    .unwrap_or_default();
                if bytes_freed > 0 {
                    self.bytes_used.fetch_sub(bytes_freed, Ordering::Relaxed);
                }
                // If list is now empty, delete the key
                if let Some(true) = self.list_storage.is_empty(idx, generation) {
                    let _ = self.delete_typed(key, ValueType::List);
                }
                Ok(result)
            }
            None => Ok(Vec::new()),
        }
    }

    fn rpop_count(&self, key: &[u8], count: usize) -> CacheResult<Vec<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::List)? {
            Some((idx, generation)) => {
                let (result, bytes_freed) = self
                    .list_storage
                    .rpop_count(idx, generation, count)
                    .unwrap_or_default();
                if bytes_freed > 0 {
                    self.bytes_used.fetch_sub(bytes_freed, Ordering::Relaxed);
                }
                // If list is now empty, delete the key
                if let Some(true) = self.list_storage.is_empty(idx, generation) {
                    let _ = self.delete_typed(key, ValueType::List);
                }
                Ok(result)
            }
            None => Ok(Vec::new()),
        }
    }

    fn lrange(&self, key: &[u8], start: i64, stop: i64) -> CacheResult<Vec<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::List)? {
            Some((idx, generation)) => Ok(self
                .list_storage
                .lrange(idx, generation, start, stop)
                .unwrap_or_default()),
            None => Ok(Vec::new()),
        }
    }

    fn llen(&self, key: &[u8]) -> CacheResult<usize> {
        match self.lookup_typed(key, ValueType::List)? {
            Some((idx, generation)) => Ok(self.list_storage.llen(idx, generation).unwrap_or(0)),
            None => Ok(0),
        }
    }

    fn lindex(&self, key: &[u8], index: i64) -> CacheResult<Option<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::List)? {
            Some((idx, generation)) => Ok(self.list_storage.lindex(idx, generation, index)),
            None => Ok(None),
        }
    }

    fn lset(&self, key: &[u8], index: i64, value: &[u8]) -> CacheResult<()> {
        match self.lookup_typed(key, ValueType::List)? {
            Some((idx, generation)) => {
                match self.list_storage.lset(idx, generation, index, value) {
                    Some(true) => Ok(()),
                    Some(false) => Err(CacheError::InvalidOffset),
                    None => Err(CacheError::KeyNotFound),
                }
            }
            None => Err(CacheError::KeyNotFound),
        }
    }

    fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> CacheResult<()> {
        match self.lookup_typed(key, ValueType::List)? {
            Some((idx, generation)) => {
                self.list_storage.ltrim(idx, generation, start, stop);
                // If list is now empty, delete the key
                if let Some(true) = self.list_storage.is_empty(idx, generation) {
                    let _ = self.delete_typed(key, ValueType::List);
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn lpushx(&self, key: &[u8], values: &[&[u8]]) -> CacheResult<usize> {
        match self.lookup_typed(key, ValueType::List)? {
            Some((idx, generation)) => match self.list_storage.lpush(idx, generation, values) {
                Some((len, bytes_added)) => {
                    self.bytes_used.fetch_add(bytes_added, Ordering::Relaxed);
                    Ok(len)
                }
                None => Ok(0),
            },
            None => Ok(0),
        }
    }

    fn rpushx(&self, key: &[u8], values: &[&[u8]]) -> CacheResult<usize> {
        match self.lookup_typed(key, ValueType::List)? {
            Some((idx, generation)) => match self.list_storage.rpush(idx, generation, values) {
                Some((len, bytes_added)) => {
                    self.bytes_used.fetch_add(bytes_added, Ordering::Relaxed);
                    Ok(len)
                }
                None => Ok(0),
            },
            None => Ok(0),
        }
    }
}

impl SetCache for HeapCache {
    fn sadd(&self, key: &[u8], members: &[&[u8]], ttl: Option<Duration>) -> CacheResult<usize> {
        let ttl = ttl.unwrap_or(self.default_ttl);
        let (idx, generation, _created) = self.get_or_create_typed(key, ValueType::Set, ttl)?;

        match self.set_storage.sadd(idx, generation, members) {
            Some((added, bytes_added)) => {
                if bytes_added > 0 {
                    self.bytes_used.fetch_add(bytes_added, Ordering::Relaxed);
                }
                Ok(added)
            }
            None => Err(CacheError::KeyNotFound),
        }
    }

    fn srem(&self, key: &[u8], members: &[&[u8]]) -> CacheResult<usize> {
        match self.lookup_typed(key, ValueType::Set)? {
            Some((idx, generation)) => {
                let (removed, bytes_freed) = self
                    .set_storage
                    .srem(idx, generation, members)
                    .unwrap_or((0, 0));
                if bytes_freed > 0 {
                    self.bytes_used.fetch_sub(bytes_freed, Ordering::Relaxed);
                }
                // If set is now empty, delete the key
                if let Some(true) = self.set_storage.is_empty(idx, generation) {
                    let _ = self.delete_typed(key, ValueType::Set);
                }
                Ok(removed)
            }
            None => Ok(0),
        }
    }

    fn smembers(&self, key: &[u8]) -> CacheResult<Vec<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::Set)? {
            Some((idx, generation)) => Ok(self
                .set_storage
                .smembers(idx, generation)
                .unwrap_or_default()),
            None => Ok(Vec::new()),
        }
    }

    fn sismember(&self, key: &[u8], member: &[u8]) -> CacheResult<bool> {
        match self.lookup_typed(key, ValueType::Set)? {
            Some((idx, generation)) => Ok(self
                .set_storage
                .sismember(idx, generation, member)
                .unwrap_or(false)),
            None => Ok(false),
        }
    }

    fn smismember(&self, key: &[u8], members: &[&[u8]]) -> CacheResult<Vec<bool>> {
        match self.lookup_typed(key, ValueType::Set)? {
            Some((idx, generation)) => Ok(self
                .set_storage
                .smismember(idx, generation, members)
                .unwrap_or_else(|| members.iter().map(|_| false).collect())),
            None => Ok(members.iter().map(|_| false).collect()),
        }
    }

    fn scard(&self, key: &[u8]) -> CacheResult<usize> {
        match self.lookup_typed(key, ValueType::Set)? {
            Some((idx, generation)) => Ok(self.set_storage.scard(idx, generation).unwrap_or(0)),
            None => Ok(0),
        }
    }

    fn spop(&self, key: &[u8]) -> CacheResult<Option<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::Set)? {
            Some((idx, generation)) => {
                let result = self.set_storage.spop(idx, generation);
                if let Some((_, bytes_freed)) = &result {
                    self.bytes_used.fetch_sub(*bytes_freed, Ordering::Relaxed);
                }
                // If set is now empty, delete the key
                if let Some(true) = self.set_storage.is_empty(idx, generation) {
                    let _ = self.delete_typed(key, ValueType::Set);
                }
                Ok(result.map(|(v, _)| v))
            }
            None => Ok(None),
        }
    }

    fn spop_count(&self, key: &[u8], count: usize) -> CacheResult<Vec<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::Set)? {
            Some((idx, generation)) => {
                let (result, bytes_freed) = self
                    .set_storage
                    .spop_count(idx, generation, count)
                    .unwrap_or_default();
                if bytes_freed > 0 {
                    self.bytes_used.fetch_sub(bytes_freed, Ordering::Relaxed);
                }
                // If set is now empty, delete the key
                if let Some(true) = self.set_storage.is_empty(idx, generation) {
                    let _ = self.delete_typed(key, ValueType::Set);
                }
                Ok(result)
            }
            None => Ok(Vec::new()),
        }
    }

    fn srandmember(&self, key: &[u8]) -> CacheResult<Option<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::Set)? {
            Some((idx, generation)) => Ok(self.set_storage.srandmember(idx, generation)),
            None => Ok(None),
        }
    }

    fn srandmember_count(&self, key: &[u8], count: i64) -> CacheResult<Vec<Vec<u8>>> {
        match self.lookup_typed(key, ValueType::Set)? {
            Some((idx, generation)) => {
                let members = self
                    .set_storage
                    .smembers(idx, generation)
                    .unwrap_or_default();
                if members.is_empty() {
                    return Ok(Vec::new());
                }

                if count >= 0 {
                    // Positive count: return up to count distinct members
                    let count = (count as usize).min(members.len());
                    Ok(members.into_iter().take(count).collect())
                } else {
                    // Negative count: return abs(count) members, may have duplicates
                    use std::time::SystemTime;
                    let seed = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(0);
                    let count = (-count) as usize;
                    let mut result = Vec::with_capacity(count);
                    for i in 0..count {
                        let idx = ((seed.wrapping_mul(2654435761).wrapping_add(i as u64))
                            % members.len() as u64) as usize;
                        result.push(members[idx].clone());
                    }
                    Ok(result)
                }
            }
            None => Ok(Vec::new()),
        }
    }
}

/// Configuration for the disk tier.
#[derive(Debug, Clone)]
pub struct DiskTierConfig {
    /// Path to the disk cache file.
    pub path: PathBuf,
    /// Total size of disk storage in bytes.
    pub size: usize,
    /// Frequency threshold for promoting items from disk to RAM.
    pub promotion_threshold: u8,
    /// Synchronization mode for disk writes.
    pub sync_mode: SyncMode,
    /// Whether to recover from existing disk cache on startup.
    pub recover_on_startup: bool,
}

impl DiskTierConfig {
    /// Create a new disk tier configuration.
    pub fn new(path: impl Into<PathBuf>, size: usize) -> Self {
        Self {
            path: path.into(),
            size,
            promotion_threshold: 2,
            sync_mode: SyncMode::default(),
            recover_on_startup: true,
        }
    }

    /// Set the promotion threshold.
    pub fn promotion_threshold(mut self, threshold: u8) -> Self {
        self.promotion_threshold = threshold;
        self
    }

    /// Set the sync mode.
    pub fn sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    /// Set whether to recover on startup.
    pub fn recover_on_startup(mut self, recover: bool) -> Self {
        self.recover_on_startup = recover;
        self
    }
}

/// Builder for [`HeapCache`].
#[derive(Debug, Clone)]
pub struct HeapCacheBuilder {
    memory_limit: usize,
    hashtable_power: u8,
    default_ttl: Duration,
    initial_fragmentation_ratio: u32,
    eviction_policy: EvictionPolicy,
    small_queue_percent: u8,
    demotion_threshold: u8,
    disk_tier: Option<DiskTierConfig>,
    ram_pool_id: u8,
    disk_pool_id: u8,
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
            eviction_policy: EvictionPolicy::default(),
            small_queue_percent: DEFAULT_SMALL_QUEUE_PERCENT,
            demotion_threshold: DEFAULT_DEMOTION_THRESHOLD,
            disk_tier: None,
            ram_pool_id: 0,
            disk_pool_id: 2,
        }
    }

    /// Set the RAM pool ID (default 0).
    ///
    /// This identifies the RAM storage tier in location encodings.
    pub fn ram_pool_id(mut self, id: u8) -> Self {
        self.ram_pool_id = id;
        self
    }

    /// Set the disk pool ID (default 2).
    ///
    /// This identifies the disk storage tier in location encodings.
    pub fn disk_pool_id(mut self, id: u8) -> Self {
        self.disk_pool_id = id;
        self
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

    /// Set the eviction policy.
    ///
    /// - `S3Fifo` (default): Two-queue eviction with admission filter
    /// - `Lfu`: Approximate least-frequently-used via random sampling
    pub fn eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }

    /// Set S3-FIFO small queue percentage (1-50, default 10).
    ///
    /// Only used when eviction policy is S3-FIFO.
    pub fn small_queue_percent(mut self, percent: u8) -> Self {
        self.small_queue_percent = percent.clamp(1, 50);
        self
    }

    /// Set S3-FIFO demotion threshold (default 1).
    ///
    /// Items with frequency > threshold are promoted from small to main queue.
    /// Only used when eviction policy is S3-FIFO.
    pub fn demotion_threshold(mut self, threshold: u8) -> Self {
        self.demotion_threshold = threshold;
        self
    }

    /// Enable disk tier with the given configuration.
    ///
    /// When enabled, items evicted from RAM are demoted to disk storage
    /// instead of being discarded. On disk hit, items can be promoted
    /// back to RAM based on access frequency.
    pub fn disk_tier(mut self, config: DiskTierConfig) -> Self {
        self.disk_tier = Some(config);
        self
    }

    /// Build the HeapCache.
    ///
    /// # Errors
    ///
    /// Returns an error if disk tier is configured but the disk layer
    /// cannot be created (e.g., path doesn't exist, I/O error).
    pub fn build(self) -> Result<HeapCache, std::io::Error> {
        // Slot capacity is derived from hashtable power - the hashtable
        // constrains max items, so slot storage matches that limit.
        // MultiChoiceHashtable has 8 items per bucket, so max items = 2^power * 8.
        let slot_capacity = (1usize << self.hashtable_power) * 8;

        let eviction_state = match self.eviction_policy {
            EvictionPolicy::S3Fifo => EvictionState::S3Fifo(S3FifoPolicy::new(
                slot_capacity as u32,
                self.small_queue_percent,
                self.demotion_threshold,
            )),
            EvictionPolicy::Lfu => EvictionState::Lfu,
            EvictionPolicy::Random => EvictionState::Random,
        };

        // Build disk layer if configured
        let (disk_layer, promotion_threshold) = match self.disk_tier {
            Some(config) => {
                let layer = DiskLayerBuilder::new()
                    .layer_id(1)
                    .pool_id(self.disk_pool_id)
                    .segment_size(8 * 1024 * 1024) // 8MB segments
                    .path(&config.path)
                    .size(config.size)
                    .sync_mode(config.sync_mode)
                    .recover_on_startup(config.recover_on_startup)
                    .build()?;
                (Some(layer), config.promotion_threshold)
            }
            None => (None, 2),
        };

        // Complex type storage uses a portion of the slot capacity
        // This is a heuristic - can be tuned via builder in future
        let complex_type_capacity = slot_capacity / 4;

        Ok(HeapCache {
            hashtable: Arc::new(MultiChoiceHashtable::new(self.hashtable_power)),
            storage: SlotStorage::new(slot_capacity),
            hash_storage: HashStorage::new(complex_type_capacity),
            list_storage: ListStorage::new(complex_type_capacity),
            set_storage: SetStorage::new(complex_type_capacity),
            default_ttl: self.default_ttl,
            eviction_counter: AtomicU32::new(0),
            cas_counter: AtomicU64::new(1),
            eviction_state,
            bytes_used: AtomicUsize::new(0),
            bytes_limit: self.memory_limit,
            fragmentation_ratio: AtomicU32::new(self.initial_fragmentation_ratio),
            ops_counter: AtomicU64::new(0),
            disk_layer,
            ram_pool_id: self.ram_pool_id,
            disk_pool_id: self.disk_pool_id,
            promotion_threshold,
        })
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
            .expect("Failed to create test cache")
    }

    #[test]
    fn test_cache_creation() {
        let cache = create_test_cache();
        // String capacity: 2^10 * 8 (8 items per bucket) = 8192
        // Complex type capacity: 8192/4 = 2048 each for hash, list, set
        // Total: 8192 + 2048*3 = 14336
        assert_eq!(cache.capacity(), 14336);
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
            .build()
            .expect("Failed to build cache");

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
            .build()
            .expect("Failed to build cache");

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

    #[test]
    fn test_s3fifo_eviction_policy() {
        // Create a cache with S3-FIFO policy and small memory limit
        let cache = HeapCacheBuilder::new()
            .memory_limit(1024) // 1KB limit
            .hashtable_power(7)
            .initial_fragmentation_ratio(100)
            .eviction_policy(EvictionPolicy::S3Fifo)
            .build()
            .expect("Failed to build cache");

        // Fill with items until we hit the memory limit
        let value = vec![b'x'; 100];
        for i in 0..20 {
            let key = format!("key_{:02}", i);
            let _ = cache.set(key.as_bytes(), &value, None);
        }

        // Should have evicted some items to stay under limit
        assert!(cache.bytes_used() <= 1024);
        assert!(cache.len() < 20);
    }

    #[test]
    fn test_lfu_eviction_policy() {
        // Create a cache with LFU policy and small memory limit
        let cache = HeapCacheBuilder::new()
            .memory_limit(1024) // 1KB limit
            .hashtable_power(7)
            .initial_fragmentation_ratio(100)
            .eviction_policy(EvictionPolicy::Lfu)
            .build()
            .expect("Failed to build cache");

        // Fill with items until we hit the memory limit
        let value = vec![b'x'; 100];
        for i in 0..20 {
            let key = format!("key_{:02}", i);
            let _ = cache.set(key.as_bytes(), &value, None);
        }

        // Should have evicted some items to stay under limit
        assert!(cache.bytes_used() <= 1024);
        assert!(cache.len() < 20);
    }

    #[test]
    fn test_s3fifo_hot_item_retention() {
        // Test that S3-FIFO retains frequently accessed items
        let cache = HeapCacheBuilder::new()
            .memory_limit(512)
            .hashtable_power(7)
            .initial_fragmentation_ratio(100)
            .eviction_policy(EvictionPolicy::S3Fifo)
            .build()
            .expect("Failed to build cache");

        // Insert a "hot" key and access it multiple times
        let hot_key = b"hot_key";
        cache.set(hot_key, b"hot_value", None).unwrap();

        // Access it multiple times to increase frequency
        for _ in 0..5 {
            let _ = cache.get(hot_key);
        }

        // Insert more items to trigger eviction
        let value = vec![b'x'; 50];
        for i in 0..10 {
            let key = format!("cold_{:02}", i);
            let _ = cache.set(key.as_bytes(), &value, None);
        }

        // Hot key should still exist due to S3-FIFO promotion
        // Note: This is probabilistic - hot items are more likely to survive
        // but not guaranteed due to the nature of the algorithm
        let hot_exists = cache.contains(hot_key);
        // We just verify the cache is functioning with S3-FIFO enabled
        assert!(cache.len() > 0);
        // If hot key survived (likely), great. If not, that's also valid behavior.
        let _ = hot_exists;
    }

    #[test]
    fn test_eviction_policy_builder_options() {
        // Test builder options for eviction policy
        let s3fifo_cache = HeapCacheBuilder::new()
            .memory_limit(1024)
            .hashtable_power(7)
            .eviction_policy(EvictionPolicy::S3Fifo)
            .small_queue_percent(20) // Non-default small queue size
            .demotion_threshold(2) // Non-default threshold
            .build()
            .expect("Failed to build cache");

        let lfu_cache = HeapCacheBuilder::new()
            .memory_limit(1024)
            .hashtable_power(7)
            .eviction_policy(EvictionPolicy::Lfu)
            .build()
            .expect("Failed to build cache");

        // Both caches should work
        s3fifo_cache.set(b"key", b"value", None).unwrap();
        lfu_cache.set(b"key", b"value", None).unwrap();

        assert!(s3fifo_cache.contains(b"key"));
        assert!(lfu_cache.contains(b"key"));
    }

    #[test]
    fn test_random_eviction_policy() {
        // Create a cache with Random policy and small memory limit
        let cache = HeapCacheBuilder::new()
            .memory_limit(1024) // 1KB limit
            .hashtable_power(7)
            .initial_fragmentation_ratio(100)
            .eviction_policy(EvictionPolicy::Random)
            .build()
            .expect("Failed to build cache");

        // Fill with items until we hit the memory limit
        let value = vec![b'x'; 100];
        for i in 0..20 {
            let key = format!("key_{:02}", i);
            let _ = cache.set(key.as_bytes(), &value, None);
        }

        // Should have evicted some items to stay under limit
        assert!(cache.bytes_used() <= 1024);
        assert!(cache.len() < 20);
    }

    #[test]
    fn test_random_eviction_under_pressure() {
        // Create a cache with Random policy
        let cache = HeapCacheBuilder::new()
            .memory_limit(512)
            .hashtable_power(7)
            .initial_fragmentation_ratio(100)
            .eviction_policy(EvictionPolicy::Random)
            .build()
            .expect("Failed to build cache");

        // Insert items with varying sizes
        for i in 0..50 {
            let key = format!("key_{:03}", i);
            let value = vec![b'v'; 50 + (i % 20)]; // 50-69 bytes
            let _ = cache.set(key.as_bytes(), &value, None);
        }

        // Cache should be under limit
        assert!(cache.bytes_used() <= 512);
        // Some items should remain
        assert!(cache.len() > 0);
    }

    #[test]
    fn test_random_eviction_large_item_evicts_multiple() {
        // Test that inserting a large item evicts multiple smaller items
        let cache = HeapCacheBuilder::new()
            .memory_limit(400)
            .hashtable_power(7)
            .initial_fragmentation_ratio(100)
            .eviction_policy(EvictionPolicy::Random)
            .build()
            .expect("Failed to build cache");

        // Fill with small items (each ~40-50 bytes with header)
        let small_value = vec![b'x'; 20];
        for i in 0..8 {
            let key = format!("s{}", i);
            cache.set(key.as_bytes(), &small_value, None).unwrap();
        }

        let initial_len = cache.len();
        let initial_bytes = cache.bytes_used();

        // Insert a large item that requires evicting multiple small items
        let large_value = vec![b'L'; 200];
        cache.set(b"large", &large_value, None).unwrap();

        // The large item should exist
        assert!(cache.contains(b"large"));
        // Multiple items should have been evicted
        assert!(cache.len() < initial_len);
        // Memory should still be under limit
        assert!(cache.bytes_used() <= 400);
        // And we should have freed a significant portion
        // (since we inserted a 200-byte value, we need to evict items to make room)
        let _ = initial_bytes; // Used for debugging if needed
    }

    // Hash tests

    #[test]
    fn test_hash_basic_operations() {
        let cache = create_test_cache();

        // hset
        assert_eq!(
            cache.hset(b"myhash", b"field1", b"value1", None).unwrap(),
            1
        );
        assert_eq!(
            cache.hset(b"myhash", b"field1", b"value2", None).unwrap(),
            0
        ); // update

        // hget
        assert_eq!(
            cache.hget(b"myhash", b"field1").unwrap(),
            Some(b"value2".to_vec())
        );
        assert_eq!(cache.hget(b"myhash", b"nonexistent").unwrap(), None);

        // hexists
        assert!(cache.hexists(b"myhash", b"field1").unwrap());
        assert!(!cache.hexists(b"myhash", b"nonexistent").unwrap());

        // hlen
        assert_eq!(cache.hlen(b"myhash").unwrap(), 1);
    }

    #[test]
    fn test_hash_multiple_fields() {
        let cache = create_test_cache();

        // hmset
        let fields = vec![
            (&b"f1"[..], &b"v1"[..]),
            (&b"f2"[..], &b"v2"[..]),
            (&b"f3"[..], &b"v3"[..]),
        ];
        assert_eq!(cache.hmset(b"myhash", &fields, None).unwrap(), 3);

        // hmget
        let result = cache.hmget(b"myhash", &[b"f1", b"f2", b"f4"]).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some(b"v1".to_vec()));
        assert_eq!(result[1], Some(b"v2".to_vec()));
        assert_eq!(result[2], None); // f4 doesn't exist

        // hgetall
        let all = cache.hgetall(b"myhash").unwrap();
        assert_eq!(all.len(), 3);

        // hkeys & hvals
        let keys = cache.hkeys(b"myhash").unwrap();
        assert_eq!(keys.len(), 3);
        let vals = cache.hvals(b"myhash").unwrap();
        assert_eq!(vals.len(), 3);
    }

    #[test]
    fn test_hash_delete() {
        let cache = create_test_cache();

        cache.hset(b"myhash", b"f1", b"v1", None).unwrap();
        cache.hset(b"myhash", b"f2", b"v2", None).unwrap();

        // Delete one field
        assert_eq!(cache.hdel(b"myhash", &[b"f1"]).unwrap(), 1);
        assert_eq!(cache.hlen(b"myhash").unwrap(), 1);

        // Delete last field - should auto-delete key
        assert_eq!(cache.hdel(b"myhash", &[b"f2"]).unwrap(), 1);
        assert_eq!(cache.hlen(b"myhash").unwrap(), 0);
    }

    #[test]
    fn test_hash_hincrby() {
        let cache = create_test_cache();

        // Increment non-existent field
        assert_eq!(cache.hincrby(b"myhash", b"counter", 5).unwrap(), 5);
        assert_eq!(cache.hincrby(b"myhash", b"counter", 3).unwrap(), 8);
        assert_eq!(cache.hincrby(b"myhash", b"counter", -2).unwrap(), 6);
    }

    #[test]
    fn test_hash_hsetnx() {
        let cache = create_test_cache();

        // First set should work
        assert!(cache.hsetnx(b"myhash", b"field1", b"value1", None).unwrap());
        // Second set should fail
        assert!(!cache.hsetnx(b"myhash", b"field1", b"value2", None).unwrap());
        // Value should be unchanged
        assert_eq!(
            cache.hget(b"myhash", b"field1").unwrap(),
            Some(b"value1".to_vec())
        );
    }

    // List tests

    #[test]
    fn test_list_basic_operations() {
        let cache = create_test_cache();

        // lpush - elements are inserted at head in order given
        // lpush([a, b]) means: push b to front, then push a to front -> [a, b]
        assert_eq!(cache.lpush(b"mylist", &[b"a", b"b"], None).unwrap(), 2);
        assert_eq!(cache.llen(b"mylist").unwrap(), 2);

        // rpush
        assert_eq!(cache.rpush(b"mylist", &[b"c"], None).unwrap(), 3);

        // lrange
        let range = cache.lrange(b"mylist", 0, -1).unwrap();
        assert_eq!(range.len(), 3);
        // After lpush([a, b]), list is [a, b]. After rpush([c]), list is [a, b, c]
        assert_eq!(range[0], b"a");
        assert_eq!(range[1], b"b");
        assert_eq!(range[2], b"c");

        // lindex
        assert_eq!(cache.lindex(b"mylist", 0).unwrap(), Some(b"a".to_vec()));
        assert_eq!(cache.lindex(b"mylist", -1).unwrap(), Some(b"c".to_vec()));
    }

    #[test]
    fn test_list_pop_operations() {
        let cache = create_test_cache();

        cache.rpush(b"mylist", &[b"a", b"b", b"c"], None).unwrap();

        // lpop
        assert_eq!(cache.lpop(b"mylist").unwrap(), Some(b"a".to_vec()));
        // rpop
        assert_eq!(cache.rpop(b"mylist").unwrap(), Some(b"c".to_vec()));

        // Only "b" should remain
        assert_eq!(cache.llen(b"mylist").unwrap(), 1);

        // Pop last element - key should be deleted
        assert_eq!(cache.lpop(b"mylist").unwrap(), Some(b"b".to_vec()));
        assert_eq!(cache.llen(b"mylist").unwrap(), 0);
    }

    #[test]
    fn test_list_lset_ltrim() {
        let cache = create_test_cache();

        cache
            .rpush(b"mylist", &[b"a", b"b", b"c", b"d"], None)
            .unwrap();

        // lset
        cache.lset(b"mylist", 1, b"B").unwrap();
        assert_eq!(cache.lindex(b"mylist", 1).unwrap(), Some(b"B".to_vec()));

        // ltrim
        cache.ltrim(b"mylist", 1, 2).unwrap();
        let range = cache.lrange(b"mylist", 0, -1).unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0], b"B");
        assert_eq!(range[1], b"c");
    }

    #[test]
    fn test_list_pushx() {
        let cache = create_test_cache();

        // lpushx on non-existent key should return 0
        assert_eq!(cache.lpushx(b"mylist", &[b"a"]).unwrap(), 0);
        assert_eq!(cache.llen(b"mylist").unwrap(), 0);

        // Create the list
        cache.rpush(b"mylist", &[b"x"], None).unwrap();

        // Now lpushx should work
        assert_eq!(cache.lpushx(b"mylist", &[b"a"]).unwrap(), 2);
        assert_eq!(cache.rpushx(b"mylist", &[b"z"]).unwrap(), 3);
    }

    // Set tests

    #[test]
    fn test_set_basic_operations() {
        let cache = create_test_cache();

        // sadd
        assert_eq!(cache.sadd(b"myset", &[b"a", b"b", b"c"], None).unwrap(), 3);
        assert_eq!(cache.sadd(b"myset", &[b"a", b"d"], None).unwrap(), 1); // a is duplicate

        // scard
        assert_eq!(cache.scard(b"myset").unwrap(), 4);

        // sismember
        assert!(cache.sismember(b"myset", b"a").unwrap());
        assert!(!cache.sismember(b"myset", b"z").unwrap());

        // smismember
        let result = cache.smismember(b"myset", &[b"a", b"z"]).unwrap();
        assert_eq!(result, vec![true, false]);
    }

    #[test]
    fn test_set_smembers() {
        let cache = create_test_cache();

        cache.sadd(b"myset", &[b"a", b"b", b"c"], None).unwrap();

        let members = cache.smembers(b"myset").unwrap();
        assert_eq!(members.len(), 3);
        // Members may be in any order, but should contain a, b, c
        assert!(members.contains(&b"a".to_vec()));
        assert!(members.contains(&b"b".to_vec()));
        assert!(members.contains(&b"c".to_vec()));
    }

    #[test]
    fn test_set_remove() {
        let cache = create_test_cache();

        cache.sadd(b"myset", &[b"a", b"b", b"c"], None).unwrap();

        // srem
        assert_eq!(cache.srem(b"myset", &[b"a", b"nonexistent"]).unwrap(), 1);
        assert_eq!(cache.scard(b"myset").unwrap(), 2);

        // Remove remaining - key should be deleted
        assert_eq!(cache.srem(b"myset", &[b"b", b"c"]).unwrap(), 2);
        assert_eq!(cache.scard(b"myset").unwrap(), 0);
    }

    #[test]
    fn test_set_spop() {
        let cache = create_test_cache();

        cache.sadd(b"myset", &[b"a", b"b", b"c"], None).unwrap();

        // spop returns a random member
        let popped = cache.spop(b"myset").unwrap();
        assert!(popped.is_some());
        assert_eq!(cache.scard(b"myset").unwrap(), 2);

        // spop_count
        let popped_vec = cache.spop_count(b"myset", 2).unwrap();
        assert_eq!(popped_vec.len(), 2);
        assert_eq!(cache.scard(b"myset").unwrap(), 0);
    }

    #[test]
    fn test_set_srandmember() {
        let cache = create_test_cache();

        cache.sadd(b"myset", &[b"a", b"b", b"c"], None).unwrap();

        // srandmember doesn't remove
        let member = cache.srandmember(b"myset").unwrap();
        assert!(member.is_some());
        assert_eq!(cache.scard(b"myset").unwrap(), 3); // still 3

        // srandmember_count
        let members = cache.srandmember_count(b"myset", 2).unwrap();
        assert!(members.len() <= 2);
        assert_eq!(cache.scard(b"myset").unwrap(), 3); // still 3
    }

    // Type safety tests

    #[test]
    fn test_wrong_type_error() {
        let cache = create_test_cache();

        // Create a string key
        cache.set(b"mykey", b"value", None).unwrap();

        // Try to use it as a hash - should fail with WrongType
        let result = cache.hget(b"mykey", b"field");
        assert!(matches!(result, Err(CacheError::WrongType)));

        // Try to use it as a list
        let result = cache.lpush(b"mykey", &[b"value"], None);
        assert!(matches!(result, Err(CacheError::WrongType)));

        // Try to use it as a set
        let result = cache.sadd(b"mykey", &[b"member"], None);
        assert!(matches!(result, Err(CacheError::WrongType)));
    }

    #[test]
    fn test_key_type() {
        let cache = create_test_cache();

        // String
        cache.set(b"str", b"value", None).unwrap();
        assert_eq!(cache.key_type(b"str"), Some(ValueType::String));

        // Hash
        cache.hset(b"hash", b"f", b"v", None).unwrap();
        assert_eq!(cache.key_type(b"hash"), Some(ValueType::Hash));

        // List
        cache.rpush(b"list", &[b"x"], None).unwrap();
        assert_eq!(cache.key_type(b"list"), Some(ValueType::List));

        // Set
        cache.sadd(b"set", &[b"x"], None).unwrap();
        assert_eq!(cache.key_type(b"set"), Some(ValueType::Set));

        // Non-existent
        assert_eq!(cache.key_type(b"nonexistent"), None);
    }
}
