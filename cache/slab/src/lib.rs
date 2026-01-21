//! Memcached-style slab allocator cache with slab-level eviction.
//!
//! This crate provides a high-performance cache using a traditional slab
//! allocator design with slab-level eviction strategies (LRA, LRC, or random).
//!
//! # Architecture
//!
//! ```text
//! +-------------------------------------------+
//! |              SlabCache                    |
//! |                                           |
//! |  +-------------------------------------+  |
//! |  | MultiChoiceHashtable                     |  |
//! |  | - Key -> (Location, Frequency)      |  |
//! |  +-------------------------------------+  |
//! |        |                                  |
//! |        v                                  |
//! |  +-------------------------------------+  |
//! |  | SlabAllocator                       |  |
//! |  | +--------------------------------+  |  |
//! |  | | SlabClass 0 (64B slots)        |  |  |
//! |  | | - Free list (lock-free)        |  |  |
//! |  | | - Slab timestamps (LRA/LRC)    |  |  |
//! |  | +--------------------------------+  |  |
//! |  | | SlabClass 1 (80B slots)        |  |  |
//! |  | +--------------------------------+  |  |
//! |  | | ...                            |  |  |
//! |  | +--------------------------------+  |  |
//! |  | | SlabClass 42 (1MB slots)       |  |  |
//! |  | +--------------------------------+  |  |
//! |  +-------------------------------------+  |
//! +-------------------------------------------+
//! ```
//!
//! # Advantages
//!
//! - O(1) allocation/deallocation (pop/push from free list)
//! - Slab-level eviction (LRA, LRC, random)
//! - Compact 12-byte item headers
//! - No merge/compaction overhead
//! - Better for high-churn workloads
//!
//! # Trade-offs
//!
//! - Internal fragmentation (items smaller than slot waste space)
//! - Slab class imbalance possible
//! - More complex per-class management
//!
//! # Example
//!
//! ```ignore
//! use slab_cache::{SlabCache, SlabCacheBuilder};
//! use std::time::Duration;
//!
//! let cache = SlabCacheBuilder::new()
//!     .heap_size(64 * 1024 * 1024)   // 64MB total
//!     .slab_size(1024 * 1024)         // 1MB slabs
//!     .build()?;
//!
//! // Store an item
//! cache.set(b"key", b"value", Duration::from_secs(3600))?;
//!
//! // Retrieve an item
//! if let Some(value) = cache.get(b"key") {
//!     println!("Value: {:?}", value);
//! }
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

mod allocator;
mod class;
mod config;
mod item;
mod location;
mod reservation;
mod sync;
mod verifier;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cache_core::{
    Cache, CacheError, CacheResult, DiskLayer, DiskLayerBuilder, Hashtable, ItemGuard, Layer,
    MultiChoiceHashtable, OwnedGuard, ValueRef,
};

pub use cache_core::HugepageSize;
pub use cache_core::SyncMode;
pub use config::{EvictionStrategy, HEADER_SIZE, SlabCacheConfig, SlabClasses};
pub use reservation::SlabReservation;

use allocator::SlabAllocator;
use location::SlabLocation;
use verifier::SlabTieredVerifier;

/// Memcached-style slab allocator cache.
///
/// Uses a traditional slab allocator with slab-level eviction.
///
/// Optionally supports a disk tier for extended capacity. When enabled,
/// items evicted from RAM are demoted to disk storage instead of being
/// discarded.
pub struct SlabCache {
    /// The hashtable for key lookups.
    hashtable: Arc<MultiChoiceHashtable>,
    /// The slab allocator.
    allocator: SlabAllocator,
    /// Default TTL for items.
    default_ttl: Duration,
    /// Enable ghost entries.
    enable_ghosts: bool,
    /// Optional disk tier for extended capacity.
    disk_layer: Option<DiskLayer>,
    /// Pool ID for RAM storage (default 0).
    ram_pool_id: u8,
    /// Pool ID for disk storage (default 2).
    disk_pool_id: u8,
    /// Frequency threshold for promoting items from disk to RAM.
    promotion_threshold: u8,
}

impl SlabCache {
    /// Create a new builder for SlabCache.
    pub fn builder() -> SlabCacheBuilder {
        SlabCacheBuilder::new()
    }

    /// Create a tiered verifier that can verify keys in both RAM and disk.
    fn tiered_verifier(&self) -> SlabTieredVerifier<'_> {
        if let Some(ref disk_layer) = self.disk_layer {
            SlabTieredVerifier::with_disk(
                &self.allocator,
                self.ram_pool_id,
                disk_layer.pool(),
                self.disk_pool_id,
            )
        } else {
            SlabTieredVerifier::new(&self.allocator, self.ram_pool_id)
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
        // Calculate item size
        let item_size = HEADER_SIZE + key.len() + value.len();

        // Select class
        let class_id = match self.allocator.select_class(item_size) {
            Some(id) => id,
            None => return false,
        };

        // Allocate slot (may evict other items)
        let (slab_id, slot_index) = match self
            .allocator
            .allocate_with_eviction(class_id, &*self.hashtable)
        {
            Some(ids) => ids,
            None => return false,
        };

        // Write item
        unsafe {
            self.allocator
                .write_item(class_id, slab_id, slot_index, key, value, ttl);
        }

        // Update hashtable with new RAM location
        let new_location =
            SlabLocation::with_pool(self.ram_pool_id, class_id, slab_id, slot_index).to_location();

        if self
            .hashtable
            .cas_location(key, old_location, new_location, true)
        {
            // Mark old disk location as deleted
            if let Some(ref disk_layer) = self.disk_layer {
                disk_layer.mark_deleted(cache_core::ItemLocation::from_location(old_location));
            }
            true
        } else {
            // CAS failed, clean up allocated slot
            unsafe {
                let loc = SlabLocation::with_pool(self.ram_pool_id, class_id, slab_id, slot_index);
                let header = self.allocator.header(loc);
                if let Some(class) = self.allocator.class(class_id) {
                    class.sub_bytes(header.item_size());
                    class.remove_item();
                }
                header.mark_deleted();
            }
            self.allocator.deallocate(class_id, slab_id, slot_index);
            false
        }
    }

    /// Demote an item to the disk tier.
    ///
    /// Writes the item to disk storage and updates the hashtable.
    /// Returns true if demotion succeeded.
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

    /// Store an item only if the key doesn't exist (ADD semantics).
    pub fn add_item(&self, key: &[u8], value: &[u8], ttl: Duration) -> CacheResult<()> {
        // Validate key length
        if key.len() > 255 {
            return Err(CacheError::KeyTooLong);
        }

        // Check if key already exists
        let verifier = self.allocator.verifier();
        if self.hashtable.contains(key, &verifier) {
            return Err(CacheError::KeyExists);
        }

        // Calculate item size
        let item_size = HEADER_SIZE + key.len() + value.len();

        // Select class
        let class_id = self
            .allocator
            .select_class(item_size)
            .ok_or(CacheError::ValueTooLong)?;

        // Allocate slot (may evict)
        let (slab_id, slot_index) = self
            .allocator
            .allocate_with_eviction(class_id, &*self.hashtable)
            .ok_or(CacheError::OutOfMemory)?;

        // Write item
        unsafe {
            self.allocator
                .write_item(class_id, slab_id, slot_index, key, value, ttl);
        }

        // Insert into hashtable using insert_if_absent
        let location = SlabLocation::new(class_id, slab_id, slot_index).to_location();
        let verifier = self.allocator.verifier();

        match self.hashtable.insert_if_absent(key, location, &verifier) {
            Ok(()) => Ok(()),
            Err(e) => {
                // Failed to insert, clean up the allocated slot
                let loc = SlabLocation::new(class_id, slab_id, slot_index);
                unsafe {
                    let header = self.allocator.header(loc);
                    if let Some(class) = self.allocator.class(class_id) {
                        class.sub_bytes(header.item_size());
                        class.remove_item();
                    }
                    header.mark_deleted();
                }
                self.allocator.deallocate(class_id, slab_id, slot_index);
                Err(e)
            }
        }
    }

    /// Update an existing item only (REPLACE semantics).
    pub fn replace_item(&self, key: &[u8], value: &[u8], ttl: Duration) -> CacheResult<()> {
        // Validate key length
        if key.len() > 255 {
            return Err(CacheError::KeyTooLong);
        }

        // Check if key exists
        let verifier = self.allocator.verifier();
        if !self.hashtable.contains(key, &verifier) {
            return Err(CacheError::KeyNotFound);
        }

        // Calculate item size
        let item_size = HEADER_SIZE + key.len() + value.len();

        // Select class
        let class_id = self
            .allocator
            .select_class(item_size)
            .ok_or(CacheError::ValueTooLong)?;

        // Allocate slot (may evict)
        let (slab_id, slot_index) = self
            .allocator
            .allocate_with_eviction(class_id, &*self.hashtable)
            .ok_or(CacheError::OutOfMemory)?;

        // Write item
        unsafe {
            self.allocator
                .write_item(class_id, slab_id, slot_index, key, value, ttl);
        }

        // Update in hashtable using update_if_present
        let location = SlabLocation::new(class_id, slab_id, slot_index).to_location();
        let verifier = self.allocator.verifier();

        match self.hashtable.update_if_present(key, location, &verifier) {
            Ok(old_loc) => {
                // Deallocate old slot
                let old = SlabLocation::from_location(old_loc);
                let (old_class, old_slab, old_slot) = old.unpack();

                // Mark deleted and update stats
                unsafe {
                    let header = self.allocator.header(old);
                    if let Some(class) = self.allocator.class(old_class) {
                        class.sub_bytes(header.item_size());
                        class.remove_item();
                    }
                    header.mark_deleted();
                }
                self.allocator.deallocate(old_class, old_slab, old_slot);
                Ok(())
            }
            Err(e) => {
                // Failed to update, clean up the allocated slot
                let loc = SlabLocation::new(class_id, slab_id, slot_index);
                unsafe {
                    let header = self.allocator.header(loc);
                    if let Some(class) = self.allocator.class(class_id) {
                        class.sub_bytes(header.item_size());
                        class.remove_item();
                    }
                    header.mark_deleted();
                }
                self.allocator.deallocate(class_id, slab_id, slot_index);
                Err(e)
            }
        }
    }

    /// Store an item in the cache.
    pub fn set_item(&self, key: &[u8], value: &[u8], ttl: Duration) -> CacheResult<()> {
        // Validate key length
        if key.len() > 255 {
            return Err(CacheError::KeyTooLong);
        }

        // Calculate item size
        let item_size = HEADER_SIZE + key.len() + value.len();

        // Select class
        let class_id = self
            .allocator
            .select_class(item_size)
            .ok_or(CacheError::ValueTooLong)?;

        // Allocate slot (may evict)
        let (slab_id, slot_index) = self
            .allocator
            .allocate_with_eviction(class_id, &*self.hashtable)
            .ok_or(CacheError::OutOfMemory)?;

        // Write item
        unsafe {
            self.allocator
                .write_item(class_id, slab_id, slot_index, key, value, ttl);
        }

        // Insert into hashtable
        let location = SlabLocation::new(class_id, slab_id, slot_index).to_location();
        let verifier = self.allocator.verifier();

        match self.hashtable.insert(key, location, &verifier) {
            Ok(Some(old_loc)) => {
                // Deallocate old slot
                let old = SlabLocation::from_location(old_loc);
                let (old_class, old_slab, old_slot) = old.unpack();

                // Mark deleted and update stats
                unsafe {
                    let header = self.allocator.header(old);
                    if let Some(class) = self.allocator.class(old_class) {
                        class.sub_bytes(header.item_size());
                        class.remove_item();
                    }
                    header.mark_deleted();
                }
                self.allocator.deallocate(old_class, old_slab, old_slot);
            }
            Ok(None) => {
                // New entry, nothing to deallocate
            }
            Err(e) => {
                // Hashtable insert failed, clean up the allocated slot
                let loc = SlabLocation::new(class_id, slab_id, slot_index);
                unsafe {
                    let header = self.allocator.header(loc);
                    if let Some(class) = self.allocator.class(class_id) {
                        class.sub_bytes(header.item_size());
                        class.remove_item();
                    }
                    header.mark_deleted();
                }
                self.allocator.deallocate(class_id, slab_id, slot_index);
                return Err(e);
            }
        }

        Ok(())
    }

    /// Retrieve an item's value from the cache.
    pub fn get_item(&self, key: &[u8]) -> Option<Vec<u8>> {
        let verifier = self.tiered_verifier();

        if let Some((location, freq)) = self.hashtable.lookup(key, &verifier) {
            // Check which pool the item is in
            let pool_id = SlabLocation::pool_id_from_location(location);

            if pool_id == self.ram_pool_id {
                // Item is in RAM
                let slab_loc = SlabLocation::from_location(location);

                // Acquire slab reference to prevent eviction during read
                if !self.allocator.acquire_slab(slab_loc) {
                    // Slab is being evicted, treat as miss
                    return None;
                }

                // Touch slab for LRA tracking
                self.allocator.touch_slab(slab_loc);

                // Copy value while holding slab reference
                let value = unsafe {
                    let header = self.allocator.header(slab_loc);
                    header.value().to_vec()
                };

                // Release slab reference
                self.allocator.release_slab(slab_loc);

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
        }

        // Lookup failed - try to clean up expired item if present
        self.cleanup_expired(key);

        None
    }

    /// Clean up an expired item if present.
    ///
    /// This is called after a failed lookup to lazily remove expired items
    /// from the cache, freeing their slot for reuse.
    fn cleanup_expired(&self, key: &[u8]) {
        // Use a verifier that allows expired items to find the entry
        let verifier = self.allocator.verifier_allowing_expired();

        let (location, _freq) = match self.hashtable.lookup(key, &verifier) {
            Some(result) => result,
            None => return, // Key truly doesn't exist
        };

        let slab_loc = SlabLocation::from_location(location);

        // Check if it's actually expired (not just deleted or wrong key)
        let is_expired = unsafe {
            let header = self.allocator.header(slab_loc);
            header.is_expired()
        };

        if !is_expired {
            return; // Not expired, some other reason for the failed lookup
        }

        // Remove from hashtable
        if !self.hashtable.remove(key, location) {
            return; // Someone else already removed it
        }

        let (class_id, slab_id, slot_index) = slab_loc.unpack();

        // Update stats and mark deleted
        unsafe {
            let header = self.allocator.header(slab_loc);
            if let Some(class) = self.allocator.class(class_id) {
                class.sub_bytes(header.item_size());
                class.remove_item();
            }
            header.mark_deleted();
        }

        // Return to free list
        self.allocator.deallocate(class_id, slab_id, slot_index);
    }

    /// Access an item without copying.
    pub fn with_item<F, R>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let verifier = self.allocator.verifier();

        if let Some((location, _freq)) = self.hashtable.lookup(key, &verifier) {
            let slab_loc = SlabLocation::from_location(location);

            // Acquire slab reference to prevent eviction during read
            if !self.allocator.acquire_slab(slab_loc) {
                return None;
            }

            // Touch slab for LRA tracking
            self.allocator.touch_slab(slab_loc);

            // Access value while holding slab reference
            let result = unsafe {
                let header = self.allocator.header(slab_loc);
                f(header.value())
            };

            // Release slab reference
            self.allocator.release_slab(slab_loc);

            return Some(result);
        }

        // Lookup failed - try to clean up expired item if present
        self.cleanup_expired(key);

        None
    }

    /// Delete an item from the cache.
    pub fn delete_item(&self, key: &[u8]) -> bool {
        let verifier = self.allocator.verifier();

        // Lookup to get location
        let (location, _freq) = match self.hashtable.lookup(key, &verifier) {
            Some(result) => result,
            None => return false,
        };

        // Remove from hashtable
        if !self.hashtable.remove(key, location) {
            return false;
        }

        let slab_loc = SlabLocation::from_location(location);
        let (class_id, slab_id, slot_index) = slab_loc.unpack();

        // Update stats and mark deleted
        unsafe {
            let header = self.allocator.header(slab_loc);
            if let Some(class) = self.allocator.class(class_id) {
                class.sub_bytes(header.item_size());
                class.remove_item();
            }
            header.mark_deleted();
        }

        // Return to free list
        self.allocator.deallocate(class_id, slab_id, slot_index);

        true
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let verifier = self.allocator.verifier();
        if self.hashtable.contains(key, &verifier) {
            return true;
        }

        // Key not found - try to clean up expired item if present
        self.cleanup_expired(key);

        false
    }

    /// Get the remaining TTL for an item.
    pub fn ttl(&self, key: &[u8]) -> Option<Duration> {
        let verifier = self.allocator.verifier();
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;

        let slab_loc = SlabLocation::from_location(location);

        // Acquire slab reference
        if !self.allocator.acquire_slab(slab_loc) {
            return None;
        }

        let result = unsafe {
            let header = self.allocator.header(slab_loc);
            header.remaining_ttl()
        };

        self.allocator.release_slab(slab_loc);
        result
    }

    /// Get the frequency counter for an item.
    pub fn frequency(&self, key: &[u8]) -> Option<u8> {
        let verifier = self.allocator.verifier();
        self.hashtable.get_frequency(key, &verifier)
    }

    /// Begin a two-phase SET for zero-copy receive.
    ///
    /// Reserves space in a slab slot and returns a `SlabReservation`
    /// with a mutable pointer to the value area. The caller writes the value
    /// directly to slab memory, then calls `commit_slab_set` to finalize.
    ///
    /// # Zero-Copy Receive Flow
    ///
    /// ```ignore
    /// // 1. Reserve slab slot
    /// let mut reservation = cache.begin_slab_set(key, value_len, ttl)?;
    ///
    /// // 2. Receive value directly into slab memory
    /// socket.recv_exact(reservation.value_mut())?;
    ///
    /// // 3. Commit to finalize and update hashtable
    /// cache.commit_slab_set(reservation)?;
    /// ```
    ///
    /// # Cancellation
    ///
    /// If the reservation is dropped without committing (e.g., connection
    /// closed during receive), call `cancel_slab_set` to return the slot
    /// to the free list.
    pub fn begin_slab_set(
        &self,
        key: &[u8],
        value_len: usize,
        ttl: Duration,
    ) -> CacheResult<SlabReservation> {
        // Validate key length
        if key.len() > 255 {
            return Err(CacheError::KeyTooLong);
        }

        // Reserve slot in slab
        let (location, value_ptr, item_size) = self
            .allocator
            .begin_write_item(key, value_len, ttl, &*self.hashtable)
            .ok_or(CacheError::OutOfMemory)?;

        // Create the reservation
        // SAFETY: value_ptr points to valid slab memory that will remain
        // valid until the reservation is committed or cancelled
        Ok(unsafe {
            SlabReservation::new(location, value_ptr, value_len, key.to_vec(), ttl, item_size)
        })
    }

    /// Commit a two-phase SET operation.
    ///
    /// Finalizes the slab write and inserts the item into the hashtable.
    /// The reservation is consumed.
    pub fn commit_slab_set(&self, mut reservation: SlabReservation) -> CacheResult<()> {
        let location = reservation.location();
        let item_size = reservation.item_size();

        // Finalize the slab write (update stats)
        self.allocator.finalize_write_item(location, item_size);

        // Insert into hashtable
        let loc = location.to_location();
        let verifier = self.allocator.verifier();

        match self.hashtable.insert(reservation.key(), loc, &verifier) {
            Ok(Some(old_loc)) => {
                // Deallocate old slot
                let old = SlabLocation::from_location(old_loc);
                let (old_class, old_slab, old_slot) = old.unpack();

                // Mark deleted and update stats
                unsafe {
                    let header = self.allocator.header(old);
                    if let Some(class) = self.allocator.class(old_class) {
                        class.sub_bytes(header.item_size());
                        class.remove_item();
                    }
                    header.mark_deleted();
                }
                self.allocator.deallocate(old_class, old_slab, old_slot);
            }
            Ok(None) => {
                // New entry, nothing to deallocate
            }
            Err(e) => {
                // Hashtable insert failed, cancel the reservation
                self.allocator.cancel_write_item(location);
                return Err(e);
            }
        }

        reservation.mark_committed();
        Ok(())
    }

    /// Cancel a two-phase SET operation.
    ///
    /// Marks the reserved slot as deleted and returns it to the free list.
    /// Called when a receive operation fails (e.g., connection closed during
    /// value receive).
    pub fn cancel_slab_set(&self, reservation: SlabReservation) {
        if reservation.is_committed() {
            return;
        }
        self.allocator.cancel_write_item(reservation.location());
    }

    /// Get the total memory used.
    pub fn memory_used(&self) -> usize {
        self.allocator.memory_used()
    }

    /// Get the memory limit.
    pub fn memory_limit(&self) -> usize {
        self.allocator.memory_limit()
    }

    /// Get statistics for a slab class.
    pub fn class_stats(&self, class_id: u8) -> Option<allocator::ClassStats> {
        self.allocator.class_stats(class_id)
    }
}

impl Cache for SlabCache {
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
        let verifier = self.allocator.verifier();
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;

        let slab_loc = SlabLocation::from_location(location);

        // Touch slab for LRA tracking
        self.allocator.touch_slab(slab_loc);

        // Get zero-copy value reference (acquires slab ref internally)
        let (ref_count_ptr, value_ptr, value_len) =
            unsafe { self.allocator.get_value_ref_raw(slab_loc)? };

        // Construct ValueRef - it will decrement ref_count on drop
        Some(unsafe { ValueRef::new(ref_count_ptr, value_ptr, value_len) })
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

        // Reset the allocator (returns all slabs to free pool)
        self.allocator.reset_all();
    }

    fn add(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let ttl = ttl.unwrap_or(self.default_ttl);
        self.add_item(key, value, ttl)
    }

    fn replace(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let ttl = ttl.unwrap_or(self.default_ttl);
        self.replace_item(key, value, ttl)
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

/// Builder for [`SlabCache`].
#[derive(Debug, Clone)]
pub struct SlabCacheBuilder {
    config: SlabCacheConfig,
    disk_tier: Option<DiskTierConfig>,
    ram_pool_id: u8,
    disk_pool_id: u8,
}

impl Default for SlabCacheBuilder {
    fn default() -> Self {
        Self {
            config: SlabCacheConfig::default(),
            disk_tier: None,
            ram_pool_id: 0,
            disk_pool_id: 2,
        }
    }
}

impl SlabCacheBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            config: SlabCacheConfig::default(),
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

    /// Set the total heap size in bytes.
    pub fn heap_size(mut self, bytes: usize) -> Self {
        self.config.heap_size = bytes;
        self
    }

    /// Set the slab size in bytes.
    ///
    /// This also determines the maximum item size (items cannot be larger
    /// than the slab size). Classes will be generated up to this size.
    pub fn slab_size(mut self, bytes: usize) -> Self {
        self.config.slab_size = bytes;
        self
    }

    /// Set the minimum slot size (smallest slab class).
    ///
    /// Default is 64 bytes. Items smaller than this will use this class.
    pub fn min_slot_size(mut self, bytes: usize) -> Self {
        self.config.min_slot_size = bytes;
        self
    }

    /// Set the growth factor between slab classes.
    ///
    /// Default is 1.25 (~20% worst-case fragmentation).
    /// Higher values mean fewer classes but more fragmentation.
    /// Must be > 1.0.
    pub fn growth_factor(mut self, factor: f64) -> Self {
        self.config.growth_factor = factor;
        self
    }

    /// Set the hashtable power (2^power buckets).
    pub fn hashtable_power(mut self, power: u8) -> Self {
        self.config.hashtable_power = power;
        self
    }

    /// Set the hugepage size preference.
    pub fn hugepage_size(mut self, size: HugepageSize) -> Self {
        self.config.hugepage_size = size;
        self
    }

    /// Set the NUMA node to bind memory to (Linux only).
    pub fn numa_node(mut self, node: u32) -> Self {
        self.config.numa_node = Some(node);
        self
    }

    /// Set the default TTL for items.
    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.config.default_ttl = ttl;
        self
    }

    /// Enable ghost entries for evicted items.
    pub fn enable_ghosts(mut self, enabled: bool) -> Self {
        self.config.enable_ghosts = enabled;
        self
    }

    /// Set the eviction strategy (twemcache-style).
    ///
    /// Strategies can be combined using `|`:
    /// - `EvictionStrategy::NONE` - No eviction (return error when full)
    /// - `EvictionStrategy::RANDOM` - Random slab eviction
    /// - `EvictionStrategy::SLAB_LRA` - Least recently accessed slab (default)
    /// - `EvictionStrategy::SLAB_LRC` - Least recently created slab
    ///
    /// Example: `EvictionStrategy::SLAB_LRA | EvictionStrategy::RANDOM`
    /// tries slab LRA first, falls back to random.
    pub fn eviction_strategy(mut self, strategy: EvictionStrategy) -> Self {
        self.config.eviction_strategy = strategy;
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

    /// Build the SlabCache.
    pub fn build(self) -> Result<SlabCache, std::io::Error> {
        // Create allocator
        let allocator = SlabAllocator::new(&self.config)?;

        // Create hashtable
        let hashtable = Arc::new(MultiChoiceHashtable::new(self.config.hashtable_power));

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

        Ok(SlabCache {
            hashtable,
            allocator,
            default_ttl: self.config.default_ttl,
            enable_ghosts: self.config.enable_ghosts,
            disk_layer,
            ram_pool_id: self.ram_pool_id,
            disk_pool_id: self.disk_pool_id,
            promotion_threshold,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_cache() -> SlabCache {
        SlabCacheBuilder::new()
            .heap_size(4 * 1024 * 1024) // 4MB
            .slab_size(64 * 1024) // 64KB slabs
            .hashtable_power(10) // 1K buckets
            .build()
            .expect("Failed to create test cache")
    }

    #[test]
    fn test_cache_creation() {
        let _cache = create_test_cache();
    }

    #[test]
    fn test_set_and_get() {
        let cache = create_test_cache();

        let key = b"test_key";
        let value = b"test_value";
        let ttl = Duration::from_secs(3600);

        cache.set_item(key, value, ttl).expect("Failed to set");

        let result = cache.get_item(key);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), value);
    }

    #[test]
    fn test_delete() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set_item(b"key", b"value", ttl).unwrap();
        assert!(cache.contains_key(b"key"));

        let deleted = cache.delete_item(b"key");
        assert!(deleted);
        assert!(!cache.contains_key(b"key"));
    }

    #[test]
    fn test_update() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set_item(b"key", b"value1", ttl).unwrap();
        assert_eq!(cache.get_item(b"key"), Some(b"value1".to_vec()));

        cache.set_item(b"key", b"value2", ttl).unwrap();
        assert_eq!(cache.get_item(b"key"), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_multiple_items() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        // Set multiple items
        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            cache
                .set_item(key.as_bytes(), value.as_bytes(), ttl)
                .unwrap();
        }

        // Verify all items exist
        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            assert!(cache.contains_key(key.as_bytes()));
            assert_eq!(cache.get_item(key.as_bytes()), Some(value.into_bytes()));
        }
    }

    #[test]
    fn test_cache_trait() {
        let cache = create_test_cache();

        // Use Cache trait methods
        Cache::set(&cache, b"key", b"value", Some(Duration::from_secs(3600))).unwrap();

        let result = Cache::get(&cache, b"key");
        assert!(result.is_some());
        assert_eq!(result.unwrap().value(), b"value");

        assert!(Cache::contains(&cache, b"key"));

        let deleted = Cache::delete(&cache, b"key");
        assert!(deleted);
        assert!(!Cache::contains(&cache, b"key"));
    }

    #[test]
    fn test_with_value() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set_item(b"key", b"hello", ttl).unwrap();

        let len = cache.with_item(b"key", |v| v.len());
        assert_eq!(len, Some(5));

        let missing = cache.with_item(b"missing", |v| v.len());
        assert!(missing.is_none());
    }

    #[test]
    fn test_frequency() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set_item(b"key", b"value", ttl).unwrap();

        // Initial frequency is 1
        let freq = cache.frequency(b"key");
        assert_eq!(freq, Some(1));

        // Access increments frequency
        let _ = cache.get_item(b"key");
        let freq = cache.frequency(b"key");
        assert_eq!(freq, Some(2));
    }

    #[test]
    fn test_builder_defaults() {
        let builder = SlabCacheBuilder::new();
        assert_eq!(builder.config.heap_size, config::DEFAULT_HEAP_SIZE);
        assert_eq!(builder.config.slab_size, config::DEFAULT_SLAB_SIZE);
    }

    #[test]
    fn test_eviction() {
        // Create a small cache that will need to evict
        // 64KB heap, 32KB slabs = 2 slabs
        // Using 200 byte values + ~10 byte key + 12 byte header = ~222 bytes
        // Fits in 256 byte class, so 32KB/256 = 128 slots per slab
        // Total capacity: ~256 items
        let cache = SlabCacheBuilder::new()
            .heap_size(64 * 1024) // 64KB
            .slab_size(32 * 1024) // 32KB slabs
            .hashtable_power(8)
            .build()
            .expect("Failed to create cache");

        let ttl = Duration::from_secs(3600);
        let value = vec![b'x'; 200]; // 200 byte values

        // Fill the cache with more items than can fit
        for i in 0..500 {
            let key = format!("key_{:04}", i);
            let _ = cache.set_item(key.as_bytes(), &value, ttl);
        }

        // Some items should exist
        let mut found = 0;
        for i in 0..500 {
            let key = format!("key_{:04}", i);
            if cache.contains_key(key.as_bytes()) {
                found += 1;
            }
        }

        // Should have some items but not all (due to eviction)
        // With ~256 capacity and 500 inserts, we should have evicted some
        assert!(found > 0);
        assert!(found < 500, "Expected eviction but found {} items", found);
    }

    #[test]
    fn test_expiration_lazy_cleanup() {
        let cache = create_test_cache();

        // Set an item with a short TTL (1 second minimum since we use second precision)
        let key = b"expire_me";
        let value = b"temporary_value";
        cache.set_item(key, value, Duration::from_secs(1)).unwrap();

        // Should exist initially
        assert!(cache.contains_key(key));

        // Wait for expiration (add buffer for timing)
        std::thread::sleep(Duration::from_millis(1500));

        // Should not exist after expiration
        assert!(!cache.contains_key(key));

        // The expired item should have been cleaned up by the lookup
        // Verify by checking that we can still insert and the old slot was freed
        cache
            .set_item(key, b"new_value", Duration::from_secs(3600))
            .unwrap();
        assert!(cache.contains_key(key));
        assert_eq!(cache.get_item(key), Some(b"new_value".to_vec()));
    }

    #[test]
    fn test_eviction_strategy_none() {
        // With NONE strategy, should return error when full
        let cache = SlabCacheBuilder::new()
            .heap_size(64 * 1024) // 64KB - very small
            .slab_size(32 * 1024) // 32KB slabs
            .hashtable_power(8)
            .eviction_strategy(EvictionStrategy::NONE)
            .build()
            .expect("Failed to create cache");

        let ttl = Duration::from_secs(3600);
        let value = vec![b'x'; 200];

        // Fill the cache
        let mut inserted = 0;
        for i in 0..500 {
            let key = format!("key_{:04}", i);
            if cache.set_item(key.as_bytes(), &value, ttl).is_ok() {
                inserted += 1;
            }
        }

        // Should have inserted some but not all
        assert!(inserted > 0);
        assert!(inserted < 500, "Expected some failures with NONE strategy");
    }

    #[test]
    fn test_eviction_strategy_slab_lrc() {
        // With SLAB_LRC, should evict oldest created slab
        let cache = SlabCacheBuilder::new()
            .heap_size(64 * 1024) // 64KB
            .slab_size(32 * 1024) // 32KB slabs = 2 slabs total
            .hashtable_power(8)
            .eviction_strategy(EvictionStrategy::SLAB_LRC)
            .build()
            .expect("Failed to create cache");

        let ttl = Duration::from_secs(3600);
        let value = vec![b'x'; 100];

        // Fill the cache (will use both slabs)
        for i in 0..200 {
            let key = format!("key_{:04}", i);
            let _ = cache.set_item(key.as_bytes(), &value, ttl);
        }

        // Continue inserting - should trigger slab eviction
        for i in 200..400 {
            let key = format!("key_{:04}", i);
            let _ = cache.set_item(key.as_bytes(), &value, ttl);
        }

        // Some items should still exist
        let mut found = 0;
        for i in 0..400 {
            let key = format!("key_{:04}", i);
            if cache.contains_key(key.as_bytes()) {
                found += 1;
            }
        }

        assert!(
            found > 0,
            "Expected some items to remain after slab eviction"
        );
    }

    #[test]
    fn test_eviction_strategy_combined() {
        // With SLAB_LRA | RANDOM, should try slab LRA eviction first, then random
        let cache = SlabCacheBuilder::new()
            .heap_size(64 * 1024)
            .slab_size(32 * 1024)
            .hashtable_power(8)
            .eviction_strategy(EvictionStrategy::SLAB_LRA | EvictionStrategy::RANDOM)
            .build()
            .expect("Failed to create cache");

        let ttl = Duration::from_secs(3600);
        let value = vec![b'x'; 100];

        // Fill and overfill
        for i in 0..300 {
            let key = format!("key_{:04}", i);
            let _ = cache.set_item(key.as_bytes(), &value, ttl);
        }

        // Some items should exist
        let mut found = 0;
        for i in 0..300 {
            let key = format!("key_{:04}", i);
            if cache.contains_key(key.as_bytes()) {
                found += 1;
            }
        }

        assert!(found > 0, "Expected some items after combined eviction");
    }

    #[test]
    fn test_two_phase_set_basic() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        // Begin a reservation
        let mut reservation = cache
            .begin_slab_set(b"key", 5, ttl)
            .expect("Failed to begin slab set");

        // Write value directly to slab memory
        reservation.value_mut().copy_from_slice(b"hello");

        // Commit the reservation
        cache
            .commit_slab_set(reservation)
            .expect("Failed to commit slab set");

        // Verify item is in cache
        let result = cache.get_item(b"key");
        assert_eq!(result, Some(b"hello".to_vec()));
    }

    #[test]
    fn test_two_phase_set_cancel() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        // Begin a reservation
        let reservation = cache
            .begin_slab_set(b"key", 5, ttl)
            .expect("Failed to begin slab set");

        // Cancel the reservation
        cache.cancel_slab_set(reservation);

        // Item should not be in cache
        assert!(!cache.contains_key(b"key"));
    }

    #[test]
    fn test_two_phase_set_overwrite() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        // Set initial value using regular method
        cache.set_item(b"key", b"first", ttl).unwrap();
        assert_eq!(cache.get_item(b"key"), Some(b"first".to_vec()));

        // Overwrite using two-phase set
        let mut reservation = cache
            .begin_slab_set(b"key", 6, ttl)
            .expect("Failed to begin slab set");
        reservation.value_mut().copy_from_slice(b"second");
        cache
            .commit_slab_set(reservation)
            .expect("Failed to commit slab set");

        // Verify new value
        assert_eq!(cache.get_item(b"key"), Some(b"second".to_vec()));
    }

    #[test]
    fn test_two_phase_set_reservation_properties() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        let reservation = cache
            .begin_slab_set(b"testkey", 100, ttl)
            .expect("Failed to begin slab set");

        // Check reservation properties
        assert_eq!(reservation.key(), b"testkey");
        assert_eq!(reservation.value_len(), 100);
        assert_eq!(reservation.ttl(), ttl);
        assert!(!reservation.is_committed());

        // Clean up
        cache.cancel_slab_set(reservation);
    }

    #[test]
    fn test_two_phase_set_large_value() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);
        let value_size = 10_000;
        let expected: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();

        // Begin reservation for large value
        let mut reservation = cache
            .begin_slab_set(b"large", value_size, ttl)
            .expect("Failed to begin slab set");

        // Write pattern to slab memory
        reservation.value_mut().copy_from_slice(&expected);

        // Commit
        cache
            .commit_slab_set(reservation)
            .expect("Failed to commit slab set");

        // Verify
        let result = cache.get_item(b"large");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), expected);
    }
}
