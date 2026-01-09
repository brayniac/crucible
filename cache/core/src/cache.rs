//! TieredCache - orchestrating cache with multiple layers.
//!
//! [`TieredCache`] manages a hierarchy of cache layers, providing:
//! - Unified write path (all writes go to Layer 0)
//! - Ghost-aware insertion (preserves frequency for re-inserted keys)
//! - Synchronous eviction when space is needed
//! - Layer-based read with proper frequency tracking

use crate::config::LayerConfig;
use crate::error::{CacheError, CacheResult};
use crate::hashtable::{Hashtable, KeyVerifier};
use crate::item::ItemGuard;
use crate::item_location::ItemLocation;
use crate::layer::{FifoLayer, Layer, TtlLayer};
use crate::location::Location;
use crate::memory_pool::MemoryPool;
use crate::pool::RamPool;
use crate::slice_segment::SliceSegment;
use std::sync::Arc;
use std::time::Duration;

/// Cache layer type enumeration.
///
/// Since the Layer trait has GAT (not object-safe), we use an enum
/// to support multiple layer types in the cache hierarchy.
pub enum CacheLayer {
    /// FIFO-organized layer (for S3FIFO admission queue).
    Fifo(FifoLayer),
    /// TTL bucket-organized layer (for main cache storage).
    Ttl(TtlLayer),
}

impl CacheLayer {
    /// Get the layer's configuration.
    pub fn config(&self) -> &LayerConfig {
        match self {
            CacheLayer::Fifo(layer) => layer.config(),
            CacheLayer::Ttl(layer) => layer.config(),
        }
    }

    /// Get the layer ID.
    pub fn layer_id(&self) -> u8 {
        match self {
            CacheLayer::Fifo(layer) => layer.layer_id(),
            CacheLayer::Ttl(layer) => layer.layer_id(),
        }
    }

    /// Write an item to this layer.
    pub fn write_item(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        ttl: Duration,
    ) -> CacheResult<ItemLocation> {
        match self {
            CacheLayer::Fifo(layer) => layer.write_item(key, value, optional, ttl),
            CacheLayer::Ttl(layer) => layer.write_item(key, value, optional, ttl),
        }
    }

    /// Get an item from this layer and call the provided function with it.
    ///
    /// Returns the result of the function, or None if item not found.
    pub fn with_item<F, R>(&self, location: ItemLocation, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&dyn ItemGuard<'_>) -> R,
    {
        match self {
            CacheLayer::Fifo(layer) => layer.get_item(location, key).map(|guard| f(&guard)),
            CacheLayer::Ttl(layer) => layer.get_item(location, key).map(|guard| f(&guard)),
        }
    }

    /// Get value bytes from this layer (convenience method).
    pub fn get_value(&self, location: ItemLocation, key: &[u8]) -> Option<Vec<u8>> {
        self.with_item(location, key, |guard| guard.value().to_vec())
    }

    /// Mark an item as deleted.
    pub fn mark_deleted(&self, location: ItemLocation) {
        match self {
            CacheLayer::Fifo(layer) => layer.mark_deleted(location),
            CacheLayer::Ttl(layer) => layer.mark_deleted(location),
        }
    }

    /// Get the remaining TTL for an item.
    pub fn item_ttl(&self, location: ItemLocation) -> Option<Duration> {
        match self {
            CacheLayer::Fifo(layer) => layer.item_ttl(location),
            CacheLayer::Ttl(layer) => layer.item_ttl(location),
        }
    }

    /// Try to evict a segment from this layer.
    pub fn evict<H: Hashtable>(&self, hashtable: &H) -> bool {
        match self {
            CacheLayer::Fifo(layer) => layer.evict(hashtable),
            CacheLayer::Ttl(layer) => layer.evict(hashtable),
        }
    }

    /// Try to expire segments in this layer.
    pub fn expire<H: Hashtable>(&self, hashtable: &H) -> usize {
        match self {
            CacheLayer::Fifo(layer) => layer.expire(hashtable),
            CacheLayer::Ttl(layer) => layer.expire(hashtable),
        }
    }

    /// Get the number of free segments.
    pub fn free_segment_count(&self) -> usize {
        match self {
            CacheLayer::Fifo(layer) => layer.free_segment_count(),
            CacheLayer::Ttl(layer) => layer.free_segment_count(),
        }
    }

    /// Get the total number of segments.
    pub fn total_segment_count(&self) -> usize {
        match self {
            CacheLayer::Fifo(layer) => layer.total_segment_count(),
            CacheLayer::Ttl(layer) => layer.total_segment_count(),
        }
    }

    /// Get the number of segments in use.
    pub fn used_segment_count(&self) -> usize {
        match self {
            CacheLayer::Fifo(layer) => layer.used_segment_count(),
            CacheLayer::Ttl(layer) => layer.used_segment_count(),
        }
    }

    /// Get a segment from this layer's pool by segment ID.
    pub fn get_segment(&self, segment_id: u32) -> Option<&SliceSegment<'static>> {
        match self {
            CacheLayer::Fifo(layer) => layer.pool().get(segment_id),
            CacheLayer::Ttl(layer) => layer.pool().get(segment_id),
        }
    }

    /// Get the memory pool for this layer.
    ///
    /// This provides direct access to the pool, which can be used to create
    /// a `PoolVerifier` for fast key verification without layer indirection.
    pub fn pool(&self) -> &MemoryPool {
        match self {
            CacheLayer::Fifo(layer) => layer.pool(),
            CacheLayer::Ttl(layer) => layer.pool(),
        }
    }

    /// Get the pool ID for this layer.
    pub fn pool_id(&self) -> u8 {
        match self {
            CacheLayer::Fifo(layer) => layer.pool().pool_id(),
            CacheLayer::Ttl(layer) => layer.pool().pool_id(),
        }
    }
}

/// A tiered cache with multiple layers and a shared hashtable.
///
/// # Architecture
///
/// ```text
/// +------------------+
/// |    Hashtable     |  <- Key -> (Location, Frequency)
/// +--------+---------+
///          |
///          v
/// +------------------+
/// |     Layer 0      |  <- Admission queue (FIFO, per-item TTL)
/// | (FifoLayer/RAM)  |
/// +--------+---------+
///          | evict (demote hot items)
///          v
/// +------------------+
/// |     Layer 1      |  <- Main cache (TTL buckets, segment-level TTL)
/// | (TtlLayer/RAM)   |
/// +--------+---------+
///          | evict
///          v
/// +------------------+
/// |     Layer 2      |  <- (Optional) Disk tier
/// | (TtlLayer/Disk)  |
/// +------------------+
/// ```
///
/// # Write Path
///
/// 1. All writes go to Layer 0
/// 2. If Layer 0 is full, evict a segment (demoting hot items to Layer 1)
/// 3. Insert into hashtable, preserving ghost frequency if present
///
/// # Read Path
///
/// 1. Look up key in hashtable to get location
/// 2. Read from the appropriate layer based on location's pool_id
/// 3. Increment frequency counter
pub struct TieredCache<H: Hashtable> {
    /// Shared hashtable for all layers.
    hashtable: Arc<H>,

    /// Cache layers (index 0 is the admission layer).
    layers: Vec<CacheLayer>,

    /// Pre-computed pool_id -> layer index mapping for O(1) lookup.
    /// Index is pool_id (0-3), value is index into layers vec.
    pool_map: [Option<usize>; 4],

    /// Minimum free segments in Layer 0 before eviction.
    eviction_threshold: usize,

    /// Maximum eviction attempts per write.
    max_eviction_attempts: usize,
}

impl<H: Hashtable> TieredCache<H> {
    /// Create a new tiered cache builder.
    pub fn builder(hashtable: Arc<H>) -> TieredCacheBuilder<H> {
        TieredCacheBuilder::new(hashtable)
    }

    /// Get the hashtable.
    pub fn hashtable(&self) -> &Arc<H> {
        &self.hashtable
    }

    /// Get the number of layers.
    pub fn layer_count(&self) -> usize {
        self.layers.len()
    }

    /// Get a layer by index.
    pub fn layer(&self, index: usize) -> Option<&CacheLayer> {
        self.layers.get(index)
    }

    /// Get mutable access to a layer by index.
    pub fn layer_mut(&mut self, index: usize) -> Option<&mut CacheLayer> {
        self.layers.get_mut(index)
    }

    /// Store an item in the cache.
    ///
    /// The item is always written to Layer 0 (admission queue).
    /// If space is needed, segments are evicted first.
    ///
    /// # Ghost Handling
    ///
    /// If a ghost entry exists for this key, its frequency is preserved
    /// when inserting. This implements "second chance" semantics where
    /// recently-evicted items get a higher initial frequency.
    pub fn set(&self, key: &[u8], value: &[u8], optional: &[u8], ttl: Duration) -> CacheResult<()> {
        // Ensure we have space in Layer 0
        self.ensure_space()?;

        // Write to Layer 0
        let layer = self.layers.first().ok_or(CacheError::OutOfMemory)?;
        let location = layer.write_item(key, value, optional, ttl)?;

        // Create key verifier for hashtable operations
        let verifier = self.create_key_verifier();

        // Insert into hashtable (preserves ghost frequency if present)
        match self
            .hashtable
            .insert(key, location.to_location(), &verifier)
        {
            Ok(Some(old_location)) => {
                // Key existed, mark old location as deleted
                self.mark_deleted_at(old_location);
            }
            Ok(None) => {
                // New key or ghost resurrection
            }
            Err(e) => {
                // Hashtable full - mark item as deleted
                layer.mark_deleted(location);
                return Err(e);
            }
        }

        Ok(())
    }

    /// Store an item only if the key doesn't exist (ADD semantics).
    ///
    /// Returns error if key already exists.
    pub fn add(&self, key: &[u8], value: &[u8], optional: &[u8], ttl: Duration) -> CacheResult<()> {
        // Check if key exists first
        let verifier = self.create_key_verifier();
        if self.hashtable.contains(key, &verifier) {
            return Err(CacheError::KeyExists);
        }

        // Ensure we have space in Layer 0
        self.ensure_space()?;

        // Write to Layer 0
        let layer = self.layers.first().ok_or(CacheError::OutOfMemory)?;
        let location = layer.write_item(key, value, optional, ttl)?;

        // Insert into hashtable (ADD semantics)
        match self
            .hashtable
            .insert_if_absent(key, location.to_location(), &verifier)
        {
            Ok(()) => Ok(()),
            Err(e) => {
                // Failed to insert, mark item as deleted
                layer.mark_deleted(location);
                Err(e)
            }
        }
    }

    /// Update an existing item (REPLACE semantics).
    ///
    /// Returns error if key doesn't exist.
    pub fn replace(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        ttl: Duration,
    ) -> CacheResult<()> {
        let verifier = self.create_key_verifier();

        // Check if key exists
        if !self.hashtable.contains(key, &verifier) {
            return Err(CacheError::KeyNotFound);
        }

        // Ensure we have space in Layer 0
        self.ensure_space()?;

        // Write to Layer 0
        let layer = self.layers.first().ok_or(CacheError::OutOfMemory)?;
        let location = layer.write_item(key, value, optional, ttl)?;

        // Update in hashtable
        match self
            .hashtable
            .update_if_present(key, location.to_location(), &verifier)
        {
            Ok(old_location) => {
                self.mark_deleted_at(old_location);
                Ok(())
            }
            Err(e) => {
                layer.mark_deleted(location);
                Err(e)
            }
        }
    }

    /// Get an item from the cache.
    ///
    /// Returns the value as a `Vec<u8>`, or None if not found.
    /// This increments the item's frequency counter.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let verifier = self.create_key_verifier();

        // Lookup in hashtable
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;
        let item_loc = ItemLocation::from_location(location);

        // Find the layer containing this item
        let layer_idx = self.layer_for_pool(item_loc.pool_id())?;
        let layer = self.layers.get(layer_idx)?;

        // Get value from layer
        layer.get_value(item_loc, key)
    }

    /// Get an item with full details via callback.
    ///
    /// Calls the provided function with access to the item guard,
    /// allowing access to key, value, and optional data.
    pub fn with_item<F, R>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&dyn ItemGuard<'_>) -> R,
    {
        let verifier = self.create_key_verifier();

        // Lookup in hashtable
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;
        let item_loc = ItemLocation::from_location(location);

        // Find the layer containing this item
        let layer_idx = self.layer_for_pool(item_loc.pool_id())?;
        let layer = self.layers.get(layer_idx)?;

        // Call function with item
        layer.with_item(item_loc, key, f)
    }

    /// Delete an item from the cache.
    ///
    /// Returns true if the item was found and deleted.
    pub fn delete(&self, key: &[u8]) -> bool {
        let verifier = self.create_key_verifier();

        // Lookup in hashtable
        let Some((location, _freq)) = self.hashtable.lookup(key, &verifier) else {
            return false;
        };

        // Remove from hashtable
        if !self.hashtable.remove(key, location) {
            return false;
        }

        // Mark as deleted in the layer
        self.mark_deleted_at(location);

        true
    }

    /// Check if a key exists in the cache.
    ///
    /// Does not increment frequency counter.
    pub fn contains(&self, key: &[u8]) -> bool {
        let verifier = self.create_key_verifier();
        self.hashtable.contains(key, &verifier)
    }

    /// Get the remaining TTL for an item.
    pub fn ttl(&self, key: &[u8]) -> Option<Duration> {
        let verifier = self.create_key_verifier();

        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;
        let item_loc = ItemLocation::from_location(location);
        let layer_idx = self.layer_for_pool(item_loc.pool_id())?;
        let layer = self.layers.get(layer_idx)?;

        layer.item_ttl(item_loc)
    }

    /// Get the frequency counter for an item.
    pub fn frequency(&self, key: &[u8]) -> Option<u8> {
        let verifier = self.create_key_verifier();
        self.hashtable.get_frequency(key, &verifier)
    }

    /// Run expiration on all layers.
    ///
    /// Returns total number of segments expired.
    pub fn expire(&self) -> usize {
        let mut total = 0;
        for layer in &self.layers {
            total += layer.expire(self.hashtable.as_ref());
        }
        total
    }

    /// Force eviction from a specific layer.
    ///
    /// Returns true if a segment was evicted.
    pub fn evict_from(&self, layer_idx: usize) -> bool {
        if let Some(layer) = self.layers.get(layer_idx) {
            layer.evict(self.hashtable.as_ref())
        } else {
            false
        }
    }

    /// Ensure Layer 0 has space for a new item.
    fn ensure_space(&self) -> CacheResult<()> {
        let layer = self.layers.first().ok_or(CacheError::OutOfMemory)?;

        // Check if we need to evict
        if layer.free_segment_count() > self.eviction_threshold {
            return Ok(());
        }

        // Try to evict until we have enough space
        for _ in 0..self.max_eviction_attempts {
            if !layer.evict(self.hashtable.as_ref()) {
                // Can't evict from Layer 0, try Layer 1 if it exists
                if self.layers.len() > 1 {
                    self.layers[1].evict(self.hashtable.as_ref());
                }
            }

            if layer.free_segment_count() > self.eviction_threshold {
                return Ok(());
            }
        }

        // Still no space after max attempts
        Err(CacheError::OutOfMemory)
    }

    /// Mark an item as deleted at the given location.
    fn mark_deleted_at(&self, location: Location) {
        let item_loc = ItemLocation::from_location(location);
        if let Some(layer_idx) = self.layer_for_pool(item_loc.pool_id())
            && let Some(layer) = self.layers.get(layer_idx)
        {
            layer.mark_deleted(item_loc);
        }
    }

    /// Find the layer index for a given pool_id.
    ///
    /// Uses pre-computed O(1) lookup instead of linear scan.
    #[inline]
    fn layer_for_pool(&self, pool_id: u8) -> Option<usize> {
        if pool_id < 4 {
            self.pool_map[pool_id as usize]
        } else {
            None
        }
    }

    /// Create a key verifier for hashtable operations.
    fn create_key_verifier(&self) -> CacheKeyVerifier<'_, H> {
        CacheKeyVerifier { cache: self }
    }
}

/// Key verifier for TieredCache.
///
/// Uses pre-computed pool_map for O(1) pool lookup and goes directly
/// to the pool without layer abstraction overhead.
struct CacheKeyVerifier<'a, H: Hashtable> {
    cache: &'a TieredCache<H>,
}

impl<H: Hashtable> KeyVerifier for CacheKeyVerifier<'_, H> {
    #[inline]
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let item_loc = ItemLocation::from_location(location);
        let pool_id = item_loc.pool_id();

        // Fast path: use pre-computed pool_map for O(1) lookup
        if pool_id >= 4 {
            return false;
        }

        let Some(layer_idx) = self.cache.pool_map[pool_id as usize] else {
            return false;
        };

        let Some(layer) = self.cache.layers.get(layer_idx) else {
            return false;
        };

        // Go directly to pool to avoid layer enum matching overhead
        let pool = layer.pool();
        let Some(segment) = pool.get(item_loc.segment_id()) else {
            return false;
        };

        // Branch once at pool level instead of per-segment
        if pool.is_per_item_ttl() {
            segment
                .verify_key_with_ttl_header(item_loc.offset(), key, allow_deleted)
                .is_some()
        } else {
            segment
                .verify_key_with_basic_header(item_loc.offset(), key, allow_deleted)
                .is_some()
        }
    }
}

/// Builder for [`TieredCache`].
pub struct TieredCacheBuilder<H: Hashtable> {
    hashtable: Arc<H>,
    layers: Vec<CacheLayer>,
    pool_map: [Option<usize>; 4],
    eviction_threshold: usize,
    max_eviction_attempts: usize,
}

impl<H: Hashtable> TieredCacheBuilder<H> {
    /// Create a new builder with the given hashtable.
    pub fn new(hashtable: Arc<H>) -> Self {
        Self {
            hashtable,
            layers: Vec::new(),
            pool_map: [None; 4],
            eviction_threshold: 1,
            max_eviction_attempts: 10,
        }
    }

    /// Add a FIFO layer (admission queue).
    pub fn with_fifo_layer(mut self, layer: FifoLayer) -> Self {
        let pool_id = layer.pool().pool_id();
        let layer_idx = self.layers.len();
        self.layers.push(CacheLayer::Fifo(layer));
        if pool_id < 4 {
            self.pool_map[pool_id as usize] = Some(layer_idx);
        }
        self
    }

    /// Add a TTL bucket layer (main cache).
    pub fn with_ttl_layer(mut self, layer: TtlLayer) -> Self {
        let pool_id = layer.pool().pool_id();
        let layer_idx = self.layers.len();
        self.layers.push(CacheLayer::Ttl(layer));
        if pool_id < 4 {
            self.pool_map[pool_id as usize] = Some(layer_idx);
        }
        self
    }

    /// Add a layer (generic).
    pub fn with_layer(mut self, layer: CacheLayer) -> Self {
        let pool_id = layer.pool_id();
        let layer_idx = self.layers.len();
        self.layers.push(layer);
        if pool_id < 4 {
            self.pool_map[pool_id as usize] = Some(layer_idx);
        }
        self
    }

    /// Set the eviction threshold (minimum free segments before eviction).
    pub fn eviction_threshold(mut self, threshold: usize) -> Self {
        self.eviction_threshold = threshold;
        self
    }

    /// Set the maximum eviction attempts per write.
    pub fn max_eviction_attempts(mut self, attempts: usize) -> Self {
        self.max_eviction_attempts = attempts;
        self
    }

    /// Build the tiered cache.
    pub fn build(self) -> TieredCache<H> {
        TieredCache {
            hashtable: self.hashtable,
            layers: self.layers,
            pool_map: self.pool_map,
            eviction_threshold: self.eviction_threshold,
            max_eviction_attempts: self.max_eviction_attempts,
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::hashtable_impl::CuckooHashtable;
    use crate::layer::{FifoLayerBuilder, TtlLayerBuilder};

    fn create_test_cache() -> TieredCache<CuckooHashtable> {
        let hashtable = Arc::new(CuckooHashtable::new(10)); // 2^10 = 1024 buckets

        let fifo_layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(256 * 1024)
            .build()
            .expect("Failed to create FIFO layer");

        let ttl_layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(64 * 1024)
            .heap_size(512 * 1024)
            .build()
            .expect("Failed to create TTL layer");

        TieredCacheBuilder::new(hashtable)
            .with_fifo_layer(fifo_layer)
            .with_ttl_layer(ttl_layer)
            .eviction_threshold(1)
            .build()
    }

    #[test]
    fn test_cache_creation() {
        let cache = create_test_cache();
        assert_eq!(cache.layer_count(), 2);
    }

    #[test]
    fn test_set_and_get() {
        let cache = create_test_cache();

        let key = b"test_key";
        let value = b"test_value";

        // Set item
        cache
            .set(key, value, b"", Duration::from_secs(3600))
            .unwrap();

        // Get item
        let result = cache.get(key);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), value);
    }

    #[test]
    fn test_delete() {
        let cache = create_test_cache();

        let key = b"delete_me";
        cache
            .set(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        assert!(cache.contains(key));
        assert!(cache.delete(key));
        assert!(!cache.contains(key));
    }

    #[test]
    fn test_add_existing_key() {
        let cache = create_test_cache();

        let key = b"unique_key";
        cache
            .set(key, b"value1", b"", Duration::from_secs(3600))
            .unwrap();

        // ADD should fail for existing key
        let result = cache.add(key, b"value2", b"", Duration::from_secs(3600));
        assert!(matches!(result, Err(CacheError::KeyExists)));
    }

    #[test]
    fn test_replace_nonexistent_key() {
        let cache = create_test_cache();

        let key = b"nonexistent";

        // REPLACE should fail for nonexistent key
        let result = cache.replace(key, b"value", b"", Duration::from_secs(3600));
        assert!(matches!(result, Err(CacheError::KeyNotFound)));
    }

    #[test]
    fn test_replace_existing_key() {
        let cache = create_test_cache();

        let key = b"replace_me";
        cache
            .set(key, b"value1", b"", Duration::from_secs(3600))
            .unwrap();

        // REPLACE should succeed
        cache
            .replace(key, b"value2", b"", Duration::from_secs(3600))
            .unwrap();

        let result = cache.get(key);
        assert_eq!(result.unwrap(), b"value2");
    }

    #[test]
    fn test_contains() {
        let cache = create_test_cache();

        let key = b"exists";
        assert!(!cache.contains(key));

        cache
            .set(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();
        assert!(cache.contains(key));
    }

    #[test]
    fn test_multiple_items() {
        let cache = create_test_cache();

        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            cache
                .set(
                    key.as_bytes(),
                    value.as_bytes(),
                    b"",
                    Duration::from_secs(3600),
                )
                .unwrap();
        }

        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            let result = cache.get(key.as_bytes());
            assert!(result.is_some(), "Key {} not found", key);
            assert_eq!(result.unwrap(), value.as_bytes());
        }
    }

    #[test]
    fn test_ttl() {
        let cache = create_test_cache();

        let key = b"ttl_test";
        cache
            .set(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        let ttl = cache.ttl(key);
        assert!(ttl.is_some());
        // TTL should be approximately 3600 seconds (within bucket granularity)
    }

    #[test]
    fn test_with_item() {
        let cache = create_test_cache();

        let key = b"with_item_test";
        let value = b"test_value";
        let optional = b"opt";

        cache
            .set(key, value, optional, Duration::from_secs(3600))
            .unwrap();

        let result = cache.with_item(key, |guard| {
            (
                guard.key().to_vec(),
                guard.value().to_vec(),
                guard.optional().to_vec(),
            )
        });

        assert!(result.is_some());
        let (k, v, o) = result.unwrap();
        assert_eq!(k, key);
        assert_eq!(v, value);
        assert_eq!(o, optional);
    }

    #[test]
    fn test_builder_pattern() {
        let hashtable = Arc::new(CuckooHashtable::new(10));
        let cache = TieredCache::builder(hashtable.clone())
            .eviction_threshold(2)
            .max_eviction_attempts(5)
            .build();

        assert_eq!(cache.layer_count(), 0);
        assert!(Arc::ptr_eq(&cache.hashtable, &hashtable));
    }

    #[test]
    fn test_hashtable_accessor() {
        let cache = create_test_cache();
        let _ht = cache.hashtable();
    }

    #[test]
    fn test_layer_accessors() {
        let cache = create_test_cache();

        // Test layer()
        assert!(cache.layer(0).is_some());
        assert!(cache.layer(1).is_some());
        assert!(cache.layer(2).is_none());
    }

    #[test]
    fn test_layer_mut_accessor() {
        let mut cache = create_test_cache();

        // Test layer_mut()
        assert!(cache.layer_mut(0).is_some());
        assert!(cache.layer_mut(1).is_some());
        assert!(cache.layer_mut(2).is_none());
    }

    #[test]
    fn test_cache_layer_config() {
        let cache = create_test_cache();

        let layer0 = cache.layer(0).unwrap();
        let _config = layer0.config();
    }

    #[test]
    fn test_cache_layer_id() {
        let cache = create_test_cache();

        let layer0 = cache.layer(0).unwrap();
        assert_eq!(layer0.layer_id(), 0);

        let layer1 = cache.layer(1).unwrap();
        assert_eq!(layer1.layer_id(), 1);
    }

    #[test]
    fn test_cache_layer_segment_counts() {
        let cache = create_test_cache();

        let layer0 = cache.layer(0).unwrap();
        let total = layer0.total_segment_count();
        let free = layer0.free_segment_count();
        let used = layer0.used_segment_count();

        assert!(total > 0);
        assert_eq!(total, free + used);
    }

    #[test]
    fn test_cache_layer_pool_id() {
        let cache = create_test_cache();

        let layer0 = cache.layer(0).unwrap();
        assert_eq!(layer0.pool_id(), 0);

        let layer1 = cache.layer(1).unwrap();
        assert_eq!(layer1.pool_id(), 1);
    }

    #[test]
    fn test_evict_from() {
        let cache = create_test_cache();

        // Add enough items to use segments
        for i in 0..50 {
            let key = format!("evict_key_{}", i);
            let value = format!("evict_value_{}", i);
            cache
                .set(
                    key.as_bytes(),
                    value.as_bytes(),
                    b"",
                    Duration::from_secs(3600),
                )
                .unwrap();
        }

        // Try evicting from layer 0
        let _ = cache.evict_from(0);

        // Try evicting from layer 1
        let _ = cache.evict_from(1);

        // Try evicting from nonexistent layer
        assert!(!cache.evict_from(99));
    }

    #[test]
    fn test_expire() {
        let cache = create_test_cache();

        // Add some items
        for i in 0..10 {
            let key = format!("expire_key_{}", i);
            cache
                .set(key.as_bytes(), b"value", b"", Duration::from_secs(3600))
                .unwrap();
        }

        // Run expiration (shouldn't expire anything with 1 hour TTL)
        let expired = cache.expire();
        assert_eq!(expired, 0);
    }

    #[test]
    fn test_frequency() {
        let cache = create_test_cache();

        let key = b"freq_test";
        cache
            .set(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        // Access to increment frequency
        let _ = cache.get(key);
        let _ = cache.get(key);

        let freq = cache.frequency(key);
        assert!(freq.is_some());
    }

    #[test]
    fn test_get_nonexistent() {
        let cache = create_test_cache();
        assert!(cache.get(b"nonexistent").is_none());
    }

    #[test]
    fn test_delete_nonexistent() {
        let cache = create_test_cache();
        assert!(!cache.delete(b"nonexistent"));
    }

    #[test]
    fn test_ttl_nonexistent() {
        let cache = create_test_cache();
        assert!(cache.ttl(b"nonexistent").is_none());
    }

    #[test]
    fn test_frequency_nonexistent() {
        let cache = create_test_cache();
        assert!(cache.frequency(b"nonexistent").is_none());
    }

    #[test]
    fn test_with_item_nonexistent() {
        let cache = create_test_cache();
        let result: Option<()> = cache.with_item(b"nonexistent", |_| ());
        assert!(result.is_none());
    }

    #[test]
    fn test_add_new_key() {
        let cache = create_test_cache();

        let key = b"add_new";
        let result = cache.add(key, b"value", b"", Duration::from_secs(3600));
        assert!(result.is_ok());

        assert!(cache.contains(key));
    }

    #[test]
    fn test_set_overwrites() {
        let cache = create_test_cache();

        let key = b"overwrite";
        cache
            .set(key, b"value1", b"", Duration::from_secs(3600))
            .unwrap();
        cache
            .set(key, b"value2", b"", Duration::from_secs(3600))
            .unwrap();

        let result = cache.get(key);
        assert_eq!(result.unwrap(), b"value2");
    }

    #[test]
    fn test_builder_with_layer() {
        let hashtable = Arc::new(CuckooHashtable::new(10));

        let fifo_layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(256 * 1024)
            .build()
            .expect("Failed to create FIFO layer");

        let cache = TieredCacheBuilder::new(hashtable)
            .with_layer(CacheLayer::Fifo(fifo_layer))
            .build();

        assert_eq!(cache.layer_count(), 1);
    }

    // TTL layer specific tests to improve coverage

    fn create_ttl_only_cache() -> TieredCache<CuckooHashtable> {
        let hashtable = Arc::new(CuckooHashtable::new(10));

        let ttl_layer = TtlLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(256 * 1024)
            .build()
            .expect("Failed to create TTL layer");

        TieredCacheBuilder::new(hashtable)
            .with_ttl_layer(ttl_layer)
            .eviction_threshold(1)
            .build()
    }

    #[test]
    fn test_ttl_layer_write_item() {
        let cache = create_ttl_only_cache();

        let key = b"ttl_key";
        cache
            .set(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        assert!(cache.contains(key));
    }

    #[test]
    fn test_ttl_layer_get_item() {
        let cache = create_ttl_only_cache();

        let key = b"ttl_get";
        cache
            .set(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        let result = cache.get(key);
        assert!(result.is_some());
    }

    #[test]
    fn test_ttl_layer_with_item() {
        let cache = create_ttl_only_cache();

        let key = b"ttl_with";
        cache
            .set(key, b"value", b"opt", Duration::from_secs(3600))
            .unwrap();

        let result = cache.with_item(key, |guard| guard.value().to_vec());
        assert_eq!(result.unwrap(), b"value");
    }

    #[test]
    fn test_ttl_layer_mark_deleted() {
        let cache = create_ttl_only_cache();

        let key = b"ttl_delete";
        cache
            .set(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        assert!(cache.delete(key));
        assert!(!cache.contains(key));
    }

    #[test]
    fn test_ttl_layer_item_ttl() {
        let cache = create_ttl_only_cache();

        let key = b"ttl_check";
        cache
            .set(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        let ttl = cache.ttl(key);
        assert!(ttl.is_some());
    }

    #[test]
    fn test_ttl_layer_segment_counts() {
        let cache = create_ttl_only_cache();

        let layer = cache.layer(0).unwrap();
        let total = layer.total_segment_count();
        let free = layer.free_segment_count();
        let used = layer.used_segment_count();

        assert!(total > 0);
        assert_eq!(total, free + used);
    }

    #[test]
    fn test_ttl_layer_evict() {
        let cache = create_ttl_only_cache();

        // Fill up with items
        for i in 0..100 {
            let key = format!("ttl_evict_{}", i);
            let _ = cache.set(key.as_bytes(), b"value", b"", Duration::from_secs(3600));
        }

        // Try evict
        let _ = cache.evict_from(0);
    }

    #[test]
    fn test_ttl_layer_expire() {
        let cache = create_ttl_only_cache();

        for i in 0..10 {
            let key = format!("ttl_expire_{}", i);
            cache
                .set(key.as_bytes(), b"value", b"", Duration::from_secs(3600))
                .unwrap();
        }

        let expired = cache.expire();
        assert_eq!(expired, 0);
    }

    #[test]
    fn test_ttl_layer_config() {
        let cache = create_ttl_only_cache();

        let layer = cache.layer(0).unwrap();
        let _config = layer.config();
    }

    #[test]
    fn test_ttl_layer_pool_id() {
        let cache = create_ttl_only_cache();

        let layer = cache.layer(0).unwrap();
        assert_eq!(layer.pool_id(), 0);
    }

    #[test]
    fn test_ttl_layer_get_segment() {
        let cache = create_ttl_only_cache();

        // Add item to allocate a segment
        cache
            .set(b"seg_test", b"value", b"", Duration::from_secs(3600))
            .unwrap();

        let layer = cache.layer(0).unwrap();
        // Segment 0 should exist after writing
        let _segment = layer.get_segment(0);
    }
}
