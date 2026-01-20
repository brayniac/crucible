//! Memcached-style slab allocator cache with per-object LRU eviction.
//!
//! This crate provides a high-performance cache using a traditional slab
//! allocator design with per-object LRU eviction, similar to memcached.
//!
//! # Architecture
//!
//! ```text
//! +-------------------------------------------+
//! |              SlabCache                    |
//! |                                           |
//! |  +-------------------------------------+  |
//! |  | CuckooHashtable                     |  |
//! |  | - Key -> (Location, Frequency)      |  |
//! |  +-------------------------------------+  |
//! |        |                                  |
//! |        v                                  |
//! |  +-------------------------------------+  |
//! |  | SlabAllocator                       |  |
//! |  | +--------------------------------+  |  |
//! |  | | SlabClass 0 (64B slots)        |  |  |
//! |  | | - Free list (lock-free)        |  |  |
//! |  | | - LRU list (per-class)         |  |  |
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
//! - Per-object LRU eviction (traditional memcached semantics)
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
mod verifier;

use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use cache_core::{
    Cache, CacheError, CacheResult, CuckooHashtable, Hashtable, OwnedGuard, ValueRef,
};

pub use cache_core::HugepageSize;
pub use config::{HEADER_SIZE, SLAB_CLASSES, SlabCacheConfig};

use allocator::SlabAllocator;
use location::SlabLocation;
use verifier::SlabVerifier;

/// Memcached-style slab allocator cache.
///
/// Uses a traditional slab allocator with per-object LRU eviction.
pub struct SlabCache {
    /// The hashtable for key lookups.
    hashtable: Arc<CuckooHashtable>,
    /// The slab allocator.
    allocator: SlabAllocator,
    /// Default TTL for items.
    default_ttl: Duration,
    /// Enable ghost entries.
    enable_ghosts: bool,
}

impl SlabCache {
    /// Create a new builder for SlabCache.
    pub fn builder() -> SlabCacheBuilder {
        SlabCacheBuilder::new()
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

                // Remove from LRU and mark deleted
                self.allocator.lru_remove(old);
                unsafe {
                    let header = self.allocator.header(old);
                    if let Some(class) = self.allocator.class(old_class) {
                        class.sub_bytes(header.item_size());
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
                self.allocator.lru_remove(loc);
                unsafe {
                    let header = self.allocator.header(loc);
                    if let Some(class) = self.allocator.class(class_id) {
                        class.sub_bytes(header.item_size());
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
        let verifier = self.allocator.verifier();
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;

        let slab_loc = SlabLocation::from_location(location);

        // Touch LRU
        self.allocator.lru_touch(slab_loc);

        // Copy value
        unsafe {
            let header = self.allocator.header(slab_loc);
            Some(header.value().to_vec())
        }
    }

    /// Access an item without copying.
    pub fn with_item<F, R>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let verifier = self.allocator.verifier();
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;

        let slab_loc = SlabLocation::from_location(location);

        // Touch LRU
        self.allocator.lru_touch(slab_loc);

        // Access value
        unsafe {
            let header = self.allocator.header(slab_loc);
            Some(f(header.value()))
        }
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

        // Remove from LRU
        self.allocator.lru_remove(slab_loc);

        // Update stats and mark deleted
        unsafe {
            let header = self.allocator.header(slab_loc);
            if let Some(class) = self.allocator.class(class_id) {
                class.sub_bytes(header.item_size());
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
        self.hashtable.contains(key, &verifier)
    }

    /// Get the remaining TTL for an item.
    pub fn ttl(&self, key: &[u8]) -> Option<Duration> {
        let verifier = self.allocator.verifier();
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;

        let slab_loc = SlabLocation::from_location(location);
        unsafe {
            let header = self.allocator.header(slab_loc);
            header.remaining_ttl()
        }
    }

    /// Get the frequency counter for an item.
    pub fn frequency(&self, key: &[u8]) -> Option<u8> {
        let verifier = self.allocator.verifier();
        self.hashtable.get_frequency(key, &verifier)
    }

    /// Evict the LRU item from a specific class.
    pub fn evict_from_class(&self, class_id: u8) -> bool {
        self.allocator
            .evict_from_class(class_id, &*self.hashtable)
            .is_some()
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

        // Touch LRU
        self.allocator.lru_touch(slab_loc);

        // For slab cache, we don't have segment-level ref counting.
        // We need to copy the value for safety.
        // A proper zero-copy implementation would require per-slot ref counting.
        // For now, we allocate and use a dummy ref counter.
        unsafe {
            let header = self.allocator.header(slab_loc);
            let value = header.value();

            // Create a leaked AtomicU32 for the ref count
            // This is a limitation of the slab design - true zero-copy would need
            // more infrastructure
            let ref_count = Box::leak(Box::new(AtomicU32::new(1)));

            // Copy the value to a stable location
            let value_copy = value.to_vec().leak();

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
        // No-op: SlabCache doesn't support flushing
        // A full implementation would iterate and delete all items
    }
}

/// Builder for [`SlabCache`].
#[derive(Debug, Clone)]
pub struct SlabCacheBuilder {
    config: SlabCacheConfig,
}

impl Default for SlabCacheBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SlabCacheBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            config: SlabCacheConfig::default(),
        }
    }

    /// Set the total heap size in bytes.
    pub fn heap_size(mut self, bytes: usize) -> Self {
        self.config.heap_size = bytes;
        self
    }

    /// Set the slab size in bytes.
    pub fn slab_size(mut self, bytes: usize) -> Self {
        self.config.slab_size = bytes;
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

    /// Build the SlabCache.
    pub fn build(self) -> Result<SlabCache, std::io::Error> {
        // Create allocator
        let allocator = SlabAllocator::new(&self.config)?;

        // Create hashtable
        let hashtable = Arc::new(CuckooHashtable::new(self.config.hashtable_power));

        Ok(SlabCache {
            hashtable,
            allocator,
            default_ttl: self.config.default_ttl,
            enable_ghosts: self.config.enable_ghosts,
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
        // Using 200 byte values + ~10 byte key + 24 byte header = ~234 bytes
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
}
