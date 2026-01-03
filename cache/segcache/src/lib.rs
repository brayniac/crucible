//! Segment-based cache with TTL buckets and configurable eviction.
//!
//! This crate provides a high-performance cache using segment-based storage
//! with TTL buckets for efficient expiration. Unlike S3-FIFO, SegCache uses
//! a single-tier architecture with configurable eviction policies.
//!
//! # Architecture
//!
//! ```text
//! +-------------------------------------------+
//! |              SegCache                     |
//! |                                           |
//! |  +-------------------------------------+  |
//! |  | TtlLayer (RAM)                      |  |
//! |  | - TTL bucket organization           |  |
//! |  | - Segment-level TTL                 |  |
//! |  | - Configurable eviction policy      |  |
//! |  |   (Random, FIFO, CTE, Merge)        |  |
//! |  +-------------------------------------+  |
//! |        ^                                  |
//! |        | all items                        |
//! +-------------------------------------------+
//! ```
//!
//! # Example
//!
//! ```ignore
//! use segcache::{SegCacheBuilder, CacheError};
//! use std::time::Duration;
//!
//! let cache = SegCacheBuilder::new()
//!     .heap_size(64 * 1024 * 1024)   // 64MB total
//!     .segment_size(1024 * 1024)      // 1MB segments
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

use cache_core::{
    CacheLayer, CuckooHashtable, ItemGuard, LayerConfig, TieredCache, TieredCacheBuilder,
    TtlLayerBuilder,
};
use std::sync::Arc;
use std::time::Duration;

// Re-export common types from cache-core
pub use cache_core::{
    AtomicCounters, BasicItemGuard, Cache, CacheError, CacheMetrics, CacheResult, CounterSnapshot,
    DEFAULT_TTL, EvictionStrategy, FrequencyDecay, HugepageSize, ItemLocation, LayerMetrics,
    MergeConfig, OwnedGuard, PoolMetrics,
};

/// Segment-based cache with single-tier architecture.
///
/// All items are stored directly in a TTL-organized layer with
/// configurable eviction policy.
pub struct SegCache {
    inner: TieredCache<CuckooHashtable>,
}

impl SegCache {
    /// Create a new builder for SegCache.
    pub fn builder() -> SegCacheBuilder {
        SegCacheBuilder::new()
    }

    /// Store an item in the cache, replacing any existing item with the same key.
    #[inline]
    pub fn set(&self, key: &[u8], value: &[u8], ttl: Duration) -> CacheResult<()> {
        self.inner.set(key, value, b"", ttl)
    }

    /// Store an item in the cache with optional metadata.
    ///
    /// The optional field can be used for application-specific metadata.
    #[inline]
    pub fn set_with_optional(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        ttl: Duration,
    ) -> CacheResult<()> {
        self.inner.set(key, value, optional, ttl)
    }

    /// Store an item only if the key does not already exist.
    ///
    /// Returns `Err(CacheError::KeyExists)` if the key already exists.
    #[inline]
    pub fn add(&self, key: &[u8], value: &[u8], ttl: Duration) -> CacheResult<()> {
        self.inner.add(key, value, b"", ttl)
    }

    /// Store an item only if the key already exists.
    ///
    /// Returns `Err(CacheError::KeyNotFound)` if the key doesn't exist.
    #[inline]
    pub fn replace(&self, key: &[u8], value: &[u8], ttl: Duration) -> CacheResult<()> {
        self.inner.replace(key, value, b"", ttl)
    }

    /// Retrieve an item's value from the cache.
    ///
    /// Returns a copy of the value as `Vec<u8>`, or `None` if not found.
    /// Accessing an item increments its frequency counter.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.get(key)
    }

    /// Execute a closure with the item if it exists.
    ///
    /// This is useful when you need to access key, value, and optional metadata
    /// without copying the value.
    #[inline]
    pub fn with_item<F, R>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&dyn ItemGuard<'_>) -> R,
    {
        self.inner.with_item(key, f)
    }

    /// Delete an item from the cache.
    ///
    /// Returns `true` if the item was found and deleted.
    #[inline]
    pub fn delete(&self, key: &[u8]) -> bool {
        self.inner.delete(key)
    }

    /// Check if a key exists in the cache.
    ///
    /// Does not increment the frequency counter.
    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.inner.contains(key)
    }

    /// Get the remaining TTL for an item.
    ///
    /// Returns `None` if the key doesn't exist.
    #[inline]
    pub fn ttl(&self, key: &[u8]) -> Option<Duration> {
        self.inner.ttl(key)
    }

    /// Get the frequency counter for an item.
    ///
    /// Returns `None` if the key doesn't exist (or is a ghost).
    #[inline]
    pub fn frequency(&self, key: &[u8]) -> Option<u8> {
        self.inner.frequency(key)
    }

    /// Trigger expiration scanning.
    ///
    /// Returns the number of segments expired.
    pub fn expire(&self) -> usize {
        self.inner.expire()
    }

    /// Evict a segment from the cache.
    ///
    /// Uses the configured eviction policy.
    pub fn evict(&self) -> bool {
        self.inner.evict_from(0)
    }

    /// Get metrics for the cache.
    pub fn metrics(&self) -> CacheMetrics {
        let layer_metrics = LayerMetrics::new(
            0,
            PoolMetrics::new(
                self.inner
                    .layer(0)
                    .map(|l| l.total_segment_count() as u64)
                    .unwrap_or(0),
                self.inner
                    .layer(0)
                    .map(|l| l.used_segment_count() as u64)
                    .unwrap_or(0),
                1024 * 1024, // Default segment size
            ),
        );

        CacheMetrics::new(vec![layer_metrics])
    }
}

/// Builder for [`SegCache`].
///
/// # Example
///
/// ```ignore
/// use segcache_v2::SegCacheBuilder;
///
/// let cache = SegCacheBuilder::new()
///     .heap_size(128 * 1024 * 1024)  // 128MB
///     .segment_size(1024 * 1024)      // 1MB segments
///     .hashtable_power(18)            // 256K buckets
///     .build()
///     .expect("Failed to build cache");
/// ```
pub struct SegCacheBuilder {
    /// Total heap size in bytes.
    heap_size: usize,

    /// Segment size in bytes.
    segment_size: usize,

    /// Hashtable power (2^power buckets).
    hashtable_power: u8,

    /// Hugepage size preference.
    hugepage_size: HugepageSize,

    /// Enable ghost entries for evicted items.
    enable_ghosts: bool,

    /// NUMA node to bind memory to (Linux only).
    numa_node: Option<u32>,
}

impl Default for SegCacheBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SegCacheBuilder {
    /// Create a new builder with default settings.
    ///
    /// Defaults:
    /// - Heap: 64MB with 1MB segments
    /// - Hashtable: 2^16 = 64K buckets
    /// - Ghosts: disabled
    pub fn new() -> Self {
        Self {
            heap_size: 64 * 1024 * 1024, // 64MB
            segment_size: 1024 * 1024,   // 1MB
            hashtable_power: 16,         // 64K buckets
            hugepage_size: HugepageSize::None,
            enable_ghosts: false,
            numa_node: None,
        }
    }

    /// Set the total heap size in bytes.
    ///
    /// The number of segments is calculated as `heap_size / segment_size`.
    pub fn heap_size(mut self, bytes: usize) -> Self {
        self.heap_size = bytes;
        self
    }

    /// Set the segment size in bytes (default: 1MB).
    ///
    /// Smaller segments allow finer-grained eviction but increase metadata overhead.
    pub fn segment_size(mut self, bytes: usize) -> Self {
        self.segment_size = bytes;
        self
    }

    /// Set the hashtable power (2^power buckets).
    ///
    /// Each bucket holds 7 items:
    /// - power 16 = 64K buckets = 448K items
    /// - power 18 = 256K buckets = 1.8M items
    /// - power 20 = 1M buckets = 7M items
    pub fn hashtable_power(mut self, power: u8) -> Self {
        self.hashtable_power = power;
        self
    }

    /// Set the hugepage size preference.
    ///
    /// - `HugepageSize::None` - Use regular 4KB pages (default)
    /// - `HugepageSize::TwoMegabyte` - Try 2MB hugepages
    /// - `HugepageSize::OneGigabyte` - Try 1GB hugepages
    pub fn hugepage_size(mut self, size: HugepageSize) -> Self {
        self.hugepage_size = size;
        self
    }

    /// Enable ghost entries for evicted items.
    ///
    /// Ghost entries preserve frequency for recently evicted items,
    /// allowing "second chance" semantics on re-insertion.
    pub fn enable_ghosts(mut self, enabled: bool) -> Self {
        self.enable_ghosts = enabled;
        self
    }

    /// Set the NUMA node to bind cache memory to (Linux only).
    ///
    /// When set, the allocated memory will be bound to the specified NUMA node.
    pub fn numa_node(mut self, node: u32) -> Self {
        self.numa_node = Some(node);
        self
    }

    /// Build the SegCache.
    ///
    /// # Errors
    ///
    /// Returns an error if memory allocation fails or configuration is invalid.
    pub fn build(self) -> Result<SegCache, std::io::Error> {
        // Create TtlLayer
        let layer_config = LayerConfig::new().with_ghosts(self.enable_ghosts);

        let mut layer_builder = TtlLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .config(layer_config)
            .segment_size(self.segment_size)
            .heap_size(self.heap_size)
            .hugepage_size(self.hugepage_size);

        if let Some(node) = self.numa_node {
            layer_builder = layer_builder.numa_node(node);
        }

        let layer = layer_builder.build()?;

        // Create hashtable
        let hashtable = Arc::new(CuckooHashtable::new(self.hashtable_power));

        // Build the tiered cache (with just one layer)
        let inner = TieredCacheBuilder::new(hashtable)
            .with_layer(CacheLayer::Ttl(layer))
            .build();

        Ok(SegCache { inner })
    }
}

impl Cache for SegCache {
    fn get(&self, key: &[u8]) -> Option<OwnedGuard> {
        self.inner.get(key).map(OwnedGuard::new)
    }

    fn set(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let ttl = ttl.unwrap_or(DEFAULT_TTL);
        self.inner.set(key, value, b"", ttl)
    }

    fn delete(&self, key: &[u8]) -> bool {
        self.inner.delete(key)
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.inner.contains(key)
    }

    fn flush(&self) {
        // No-op: SegCache doesn't support flushing individual items
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_cache() -> SegCache {
        SegCacheBuilder::new()
            .heap_size(1024 * 1024) // 1MB total
            .segment_size(64 * 1024) // 64KB segments
            .hashtable_power(10) // 1K buckets
            .build()
            .expect("Failed to create test cache")
    }

    #[test]
    fn test_cache_creation() {
        let cache = create_test_cache();
        let metrics = cache.metrics();
        assert_eq!(metrics.layers.len(), 1);
    }

    #[test]
    fn test_set_and_get() {
        let cache = create_test_cache();

        let key = b"test_key";
        let value = b"test_value";
        let ttl = Duration::from_secs(3600);

        cache.set(key, value, ttl).expect("Failed to set");

        let result = cache.get(key);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), value);
    }

    #[test]
    fn test_add_existing_key() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set(b"key", b"value1", ttl).unwrap();

        let result = cache.add(b"key", b"value2", ttl);
        assert!(matches!(result, Err(CacheError::KeyExists)));
    }

    #[test]
    fn test_replace_nonexistent() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        let result = cache.replace(b"nonexistent", b"value", ttl);
        assert!(matches!(result, Err(CacheError::KeyNotFound)));
    }

    #[test]
    fn test_delete() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set(b"key", b"value", ttl).unwrap();
        assert!(cache.contains(b"key"));

        let deleted = cache.delete(b"key");
        assert!(deleted);
        assert!(!cache.contains(b"key"));
    }

    #[test]
    fn test_frequency() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set(b"key", b"value", ttl).unwrap();

        // Initial frequency is 1
        let freq = cache.frequency(b"key");
        assert_eq!(freq, Some(1));

        // Access increments frequency
        let _ = cache.get(b"key");
        let freq = cache.frequency(b"key");
        assert_eq!(freq, Some(2));
    }

    #[test]
    fn test_with_item() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set(b"key", b"hello", ttl).unwrap();

        let len = cache.with_item(b"key", |guard| guard.value().len());
        assert_eq!(len, Some(5));

        let missing = cache.with_item(b"missing", |guard| guard.value().len());
        assert!(missing.is_none());
    }

    #[test]
    fn test_builder_defaults() {
        let builder = SegCacheBuilder::new();

        assert_eq!(builder.heap_size, 64 * 1024 * 1024);
        assert_eq!(builder.segment_size, 1024 * 1024);
        assert_eq!(builder.hashtable_power, 16);
        assert!(!builder.enable_ghosts);
    }

    #[test]
    fn test_ghosts_enabled() {
        let cache = SegCacheBuilder::new()
            .heap_size(1024 * 1024)
            .segment_size(64 * 1024)
            .hashtable_power(10)
            .enable_ghosts(true)
            .build()
            .expect("Failed to create cache");

        // Just verify it builds successfully
        assert!(cache.metrics().layers.len() == 1);
    }

    #[test]
    fn test_builder_static_method() {
        let cache = SegCache::builder()
            .heap_size(1024 * 1024)
            .segment_size(64 * 1024)
            .hashtable_power(10)
            .build()
            .expect("Failed to create cache");

        assert!(cache.get(b"nonexistent").is_none());
    }

    #[test]
    fn test_builder_default() {
        let builder = SegCacheBuilder::default();
        assert_eq!(builder.heap_size, 64 * 1024 * 1024);
        assert_eq!(builder.segment_size, 1024 * 1024);
    }

    #[test]
    fn test_builder_hugepage_size() {
        let cache = SegCacheBuilder::new()
            .heap_size(1024 * 1024)
            .segment_size(64 * 1024)
            .hashtable_power(10)
            .hugepage_size(HugepageSize::None)
            .build()
            .expect("Failed to create cache");

        assert!(cache.metrics().layers.len() == 1);
    }

    #[test]
    fn test_set_with_optional() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache
            .set_with_optional(b"key", b"value", b"optional_data", ttl)
            .expect("Failed to set with optional");

        // Verify the item exists
        assert!(cache.contains(b"key"));

        // Verify we can access the optional data through with_item
        let optional = cache.with_item(b"key", |guard| guard.optional().to_vec());
        assert_eq!(optional, Some(b"optional_data".to_vec()));
    }

    #[test]
    fn test_add_new_key() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        // Add should succeed for new key
        let result = cache.add(b"new_key", b"value", ttl);
        assert!(result.is_ok());

        // Verify the value was stored
        let value = cache.get(b"new_key");
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[test]
    fn test_replace_existing() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        // First set an item
        cache.set(b"key", b"value1", ttl).unwrap();

        // Replace should succeed for existing key
        let result = cache.replace(b"key", b"value2", ttl);
        assert!(result.is_ok());

        // Verify the value was updated
        let value = cache.get(b"key");
        assert_eq!(value, Some(b"value2".to_vec()));
    }

    #[test]
    fn test_ttl() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set(b"key", b"value", ttl).unwrap();

        // Check TTL is approximately what we set (allow some tolerance)
        let remaining = cache.ttl(b"key");
        assert!(remaining.is_some());
        let secs = remaining.unwrap().as_secs();
        // TTL buckets may round, so allow a wider range
        assert!(secs > 0 && secs <= 3600);

        // Nonexistent key should return None
        assert!(cache.ttl(b"nonexistent").is_none());
    }

    #[test]
    fn test_expire() {
        let cache = create_test_cache();

        // Expire on empty/fresh cache should return 0
        let expired = cache.expire();
        assert_eq!(expired, 0);
    }

    #[test]
    fn test_evict() {
        let cache = create_test_cache();

        // Evict on empty cache should return false
        let evicted = cache.evict();
        assert!(!evicted);
    }

    #[test]
    fn test_evict_with_items() {
        let cache = SegCacheBuilder::new()
            .heap_size(128 * 1024) // Small cache
            .segment_size(32 * 1024) // Small segments
            .hashtable_power(8)
            .build()
            .expect("Failed to create cache");

        let ttl = Duration::from_secs(3600);
        let value = vec![b'x'; 1024];

        // Fill the cache with items
        for i in 0..50 {
            let key = format!("key_{}", i);
            let _ = cache.set(key.as_bytes(), &value, ttl);
        }

        // Now eviction should work
        let evicted = cache.evict();
        assert!(evicted);
    }

    #[test]
    fn test_delete_nonexistent() {
        let cache = create_test_cache();

        // Delete nonexistent key should return false
        let deleted = cache.delete(b"nonexistent");
        assert!(!deleted);
    }

    #[test]
    fn test_frequency_nonexistent() {
        let cache = create_test_cache();

        // Frequency of nonexistent key should return None
        let freq = cache.frequency(b"nonexistent");
        assert!(freq.is_none());
    }

    #[test]
    fn test_contains_nonexistent() {
        let cache = create_test_cache();

        assert!(!cache.contains(b"nonexistent"));
    }

    // Tests for the Cache trait implementation

    #[test]
    fn test_cache_trait_get() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set(b"key", b"value", ttl).unwrap();

        // Use Cache trait's get method
        let result = Cache::get(&cache, b"key");
        assert!(result.is_some());
        let owned = result.unwrap();
        assert_eq!(owned.value(), b"value");

        // Nonexistent key
        let result = Cache::get(&cache, b"nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_trait_set_with_ttl() {
        let cache = create_test_cache();

        // Use Cache trait's set with explicit TTL
        let result = Cache::set(&cache, b"key", b"value", Some(Duration::from_secs(3600)));
        assert!(result.is_ok());

        assert!(cache.contains(b"key"));
    }

    #[test]
    fn test_cache_trait_set_default_ttl() {
        let cache = create_test_cache();

        // Use Cache trait's set with None TTL (uses DEFAULT_TTL)
        let result = Cache::set(&cache, b"key", b"value", None);
        assert!(result.is_ok());

        assert!(cache.contains(b"key"));

        // The TTL should be DEFAULT_TTL (typically a large value)
        let ttl = cache.ttl(b"key");
        assert!(ttl.is_some());
    }

    #[test]
    fn test_cache_trait_delete() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set(b"key", b"value", ttl).unwrap();

        // Use Cache trait's delete
        let deleted = Cache::delete(&cache, b"key");
        assert!(deleted);
        assert!(!cache.contains(b"key"));
    }

    #[test]
    fn test_cache_trait_contains() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        assert!(!Cache::contains(&cache, b"key"));

        cache.set(b"key", b"value", ttl).unwrap();

        assert!(Cache::contains(&cache, b"key"));
    }

    #[test]
    fn test_cache_trait_flush() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set(b"key", b"value", ttl).unwrap();

        // flush is a no-op for SegCache, but shouldn't panic
        Cache::flush(&cache);

        // Item should still exist (flush is no-op)
        assert!(cache.contains(b"key"));
    }

    #[test]
    fn test_metrics_structure() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        // Write some items
        for i in 0..10 {
            let key = format!("key_{}", i);
            cache.set(key.as_bytes(), b"value", ttl).unwrap();
        }

        let metrics = cache.metrics();
        assert_eq!(metrics.layers.len(), 1);

        let layer = &metrics.layers[0];
        assert_eq!(layer.layer_id, 0);
        assert!(layer.pool.total_segments > 0);
    }

    #[test]
    fn test_multiple_operations() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        // Set multiple items
        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            cache.set(key.as_bytes(), value.as_bytes(), ttl).unwrap();
        }

        // Verify all items exist
        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            assert!(cache.contains(key.as_bytes()));
            assert_eq!(cache.get(key.as_bytes()), Some(value.into_bytes()));
        }

        // Delete some items
        for i in 0..50 {
            let key = format!("key_{}", i);
            assert!(cache.delete(key.as_bytes()));
        }

        // Verify deletions
        for i in 0..50 {
            let key = format!("key_{}", i);
            assert!(!cache.contains(key.as_bytes()));
        }

        // Remaining items should still exist
        for i in 50..100 {
            let key = format!("key_{}", i);
            assert!(cache.contains(key.as_bytes()));
        }
    }
}
