//! S3-FIFO cache with segment-based storage.
//!
//! This crate provides a high-performance cache implementing the S3-FIFO
//! eviction policy. S3-FIFO uses a small admission queue (FIFO) to filter
//! one-hit wonders before promoting items to the main cache.
//!
//! # Architecture
//!
//! ```text
//! +-----------------------------------------------------------+
//! |                    S3FifoCache                            |
//! |                                                           |
//! |  +-------------------+      +-------------------------+   |
//! |  | Layer 0 (FIFO)    | ---> | Layer 1 (TTL/Merge)     |   |
//! |  | Small Queue       |      | Main Cache              |   |
//! |  | - Per-item TTL    |      | - Segment-level TTL     |   |
//! |  | - Ghost creation  |      | - Adaptive merge        |   |
//! |  +-------------------+      +-------------------------+   |
//! |        ^                                                  |
//! |        | new items                                        |
//! +-----------------------------------------------------------+
//! ```
//!
//! # Example
//!
//! ```ignore
//! use s3fifo::{S3FifoCacheBuilder, CacheError};
//! use std::time::Duration;
//!
//! let cache = S3FifoCacheBuilder::new()
//!     .ram_size(64 * 1024 * 1024)   // 64MB total
//!     .segment_size(1024 * 1024)     // 1MB segments
//!     .small_queue_percent(10)       // 10% for admission
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
    CuckooHashtable, FifoLayerBuilder, ItemGuard, LayerConfig, TieredCache, TieredCacheBuilder,
    TtlLayerBuilder,
};
use std::sync::Arc;
use std::time::Duration;

// Re-export common types from cache-core
pub use cache_core::{
    AtomicCounters, BasicItemGuard, Cache, CacheError, CacheMetrics, CacheResult, CounterSnapshot,
    DEFAULT_TTL, FrequencyDecay, HugepageSize, ItemLocation, LayerMetrics, OwnedGuard, PoolMetrics,
    ValueRef,
};

/// S3-FIFO cache with two-layer architecture.
///
/// Layer 0 (small queue) acts as an admission filter using FIFO eviction.
/// Items with frequency > 1 are promoted to Layer 1 (main cache) on eviction.
/// Items with frequency <= 1 become ghost entries for admission control.
///
/// Layer 1 (main cache) uses TTL-organized buckets with adaptive merge eviction.
pub struct S3FifoCache {
    inner: TieredCache<CuckooHashtable>,
}

impl S3FifoCache {
    /// Create a new builder for S3FifoCache.
    pub fn builder() -> S3FifoCacheBuilder {
        S3FifoCacheBuilder::new()
    }

    /// Store an item in the cache, replacing any existing item with the same key.
    ///
    /// Items are first written to the small queue (Layer 0). On eviction from
    /// the small queue, items with frequency > 1 are promoted to the main cache.
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

    /// Trigger expiration scanning on all layers.
    ///
    /// Returns the total number of segments expired.
    pub fn expire(&self) -> usize {
        self.inner.expire()
    }

    /// Evict segments from the small queue (Layer 0).
    ///
    /// Items with frequency > 1 are promoted to the main cache.
    /// Items with frequency <= 1 become ghost entries.
    pub fn evict_small_queue(&self) -> bool {
        self.inner.evict_from(0)
    }

    /// Evict segments from the main cache (Layer 1).
    ///
    /// Uses adaptive merge eviction based on frequency counters.
    pub fn evict_main_cache(&self) -> bool {
        self.inner.evict_from(1)
    }

    /// Get metrics for the cache.
    pub fn metrics(&self) -> CacheMetrics {
        // Build metrics from layer information
        let layer0_metrics = LayerMetrics::new(
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

        let layer1_metrics = LayerMetrics::new(
            1,
            PoolMetrics::new(
                self.inner
                    .layer(1)
                    .map(|l| l.total_segment_count() as u64)
                    .unwrap_or(0),
                self.inner
                    .layer(1)
                    .map(|l| l.used_segment_count() as u64)
                    .unwrap_or(0),
                1024 * 1024, // Default segment size
            ),
        );

        CacheMetrics::new(vec![layer0_metrics, layer1_metrics])
    }
}

/// Builder for [`S3FifoCache`].
///
/// # Example
///
/// ```ignore
/// use s3fifo_v2::S3FifoCacheBuilder;
///
/// let cache = S3FifoCacheBuilder::new()
///     .ram_size(128 * 1024 * 1024)  // 128MB
///     .segment_size(1024 * 1024)     // 1MB segments
///     .small_queue_percent(10)       // 10% for admission filter
///     .hashtable_power(18)           // 256K buckets
///     .build()
///     .expect("Failed to build cache");
/// ```
pub struct S3FifoCacheBuilder {
    /// Total RAM size in bytes.
    ram_size: usize,

    /// Segment size in bytes.
    segment_size: usize,

    /// Percentage of RAM for small queue (0-100).
    small_queue_percent: u8,

    /// Hashtable power (2^power buckets).
    hashtable_power: u8,

    /// Hugepage size preference.
    hugepage_size: HugepageSize,

    /// Demotion threshold for small queue items.
    demotion_threshold: u8,

    /// NUMA node to bind memory to (Linux only).
    numa_node: Option<u32>,
}

impl Default for S3FifoCacheBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl S3FifoCacheBuilder {
    /// Create a new builder with default S3-FIFO settings.
    ///
    /// Defaults:
    /// - RAM: 64MB with 1MB segments
    /// - Small queue: 10% of RAM
    /// - Hashtable: 2^16 = 64K buckets
    /// - Demotion threshold: 1 (items must be accessed to be promoted)
    pub fn new() -> Self {
        Self {
            ram_size: 64 * 1024 * 1024, // 64MB
            segment_size: 1024 * 1024,  // 1MB
            small_queue_percent: 10,    // 10%
            hashtable_power: 16,        // 64K buckets
            hugepage_size: HugepageSize::None,
            demotion_threshold: 1, // freq > 1 to promote
            numa_node: None,
        }
    }

    /// Set the total RAM size in bytes.
    ///
    /// The number of segments is calculated as `ram_size / segment_size`.
    pub fn ram_size(mut self, bytes: usize) -> Self {
        self.ram_size = bytes;
        self
    }

    /// Set the segment size in bytes (default: 1MB).
    ///
    /// Smaller segments allow finer-grained eviction but increase metadata overhead.
    pub fn segment_size(mut self, bytes: usize) -> Self {
        self.segment_size = bytes;
        self
    }

    /// Set the percentage of RAM for the small queue (default: 10%).
    ///
    /// The small queue acts as an admission filter. Items must be accessed
    /// multiple times to be promoted to the main cache.
    pub fn small_queue_percent(mut self, percent: u8) -> Self {
        debug_assert!(percent <= 100, "percent must be <= 100");
        self.small_queue_percent = percent.min(100);
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

    /// Set the demotion threshold for small queue items (default: 1).
    ///
    /// Items with frequency > threshold are promoted to main cache.
    /// Items with frequency <= threshold become ghost entries.
    pub fn demotion_threshold(mut self, threshold: u8) -> Self {
        self.demotion_threshold = threshold;
        self
    }

    /// Set the NUMA node to bind cache memory to (Linux only).
    ///
    /// When set, the allocated memory will be bound to the specified NUMA node.
    pub fn numa_node(mut self, node: u32) -> Self {
        self.numa_node = Some(node);
        self
    }

    /// Build the S3-FIFO cache.
    ///
    /// # Errors
    ///
    /// Returns an error if memory allocation fails or configuration is invalid.
    pub fn build(self) -> Result<S3FifoCache, std::io::Error> {
        // Calculate segment counts
        let total_segments = self.ram_size / self.segment_size;
        let small_queue_segments =
            ((total_segments * self.small_queue_percent as usize) / 100).max(1);
        let main_cache_segments = total_segments.saturating_sub(small_queue_segments);

        if main_cache_segments == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Not enough segments for main cache",
            ));
        }

        let small_queue_size = small_queue_segments * self.segment_size;
        let main_cache_size = main_cache_segments * self.segment_size;

        // Create Layer 0: FIFO small queue
        let layer0_config = LayerConfig::new()
            .with_ghosts(true)
            .with_next_layer(1)
            .with_demotion_threshold(self.demotion_threshold);

        let mut layer0_builder = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .config(layer0_config)
            .segment_size(self.segment_size)
            .heap_size(small_queue_size)
            .hugepage_size(self.hugepage_size);

        if let Some(node) = self.numa_node {
            layer0_builder = layer0_builder.numa_node(node);
        }

        let layer0 = layer0_builder.build()?;

        // Create Layer 1: TTL-organized main cache
        let layer1_config = LayerConfig::new().with_ghosts(true);

        let mut layer1_builder = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .config(layer1_config)
            .segment_size(self.segment_size)
            .heap_size(main_cache_size)
            .hugepage_size(self.hugepage_size);

        if let Some(node) = self.numa_node {
            layer1_builder = layer1_builder.numa_node(node);
        }

        let layer1 = layer1_builder.build()?;

        // Create hashtable
        let hashtable = Arc::new(CuckooHashtable::new(self.hashtable_power));

        // Build the tiered cache
        let inner = TieredCacheBuilder::new(hashtable)
            .with_fifo_layer(layer0)
            .with_ttl_layer(layer1)
            .build();

        Ok(S3FifoCache { inner })
    }
}

impl Cache for S3FifoCache {
    fn get(&self, key: &[u8]) -> Option<OwnedGuard> {
        self.inner.get(key).map(OwnedGuard::new)
    }

    fn with_value<F, R>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        self.inner.with_item(key, |guard| f(guard.value()))
    }

    fn get_value_ref(&self, key: &[u8]) -> Option<ValueRef> {
        self.inner.get_value_ref(key)
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
        // No-op: S3FifoCache doesn't support flushing individual items
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_cache() -> S3FifoCache {
        S3FifoCacheBuilder::new()
            .ram_size(1024 * 1024) // 1MB total
            .segment_size(64 * 1024) // 64KB segments
            .small_queue_percent(20) // 20% for small queue
            .hashtable_power(10) // 1K buckets
            .build()
            .expect("Failed to create test cache")
    }

    #[test]
    fn test_cache_creation() {
        let cache = create_test_cache();
        let metrics = cache.metrics();
        assert_eq!(metrics.layers.len(), 2);
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
        let builder = S3FifoCacheBuilder::new();

        assert_eq!(builder.ram_size, 64 * 1024 * 1024);
        assert_eq!(builder.segment_size, 1024 * 1024);
        assert_eq!(builder.small_queue_percent, 10);
        assert_eq!(builder.hashtable_power, 16);
        assert_eq!(builder.demotion_threshold, 1);
    }

    #[test]
    fn test_builder_static_method() {
        let cache = S3FifoCache::builder()
            .ram_size(1024 * 1024)
            .segment_size(64 * 1024)
            .small_queue_percent(20)
            .hashtable_power(10)
            .build()
            .expect("Failed to create cache");

        assert!(cache.get(b"nonexistent").is_none());
    }

    #[test]
    fn test_builder_default_trait() {
        let builder = S3FifoCacheBuilder::default();
        assert_eq!(builder.ram_size, 64 * 1024 * 1024);
        assert_eq!(builder.segment_size, 1024 * 1024);
    }

    #[test]
    fn test_builder_hugepage_size() {
        let cache = S3FifoCacheBuilder::new()
            .ram_size(1024 * 1024)
            .segment_size(64 * 1024)
            .small_queue_percent(20)
            .hashtable_power(10)
            .hugepage_size(HugepageSize::None)
            .build()
            .expect("Failed to create cache");

        assert_eq!(cache.metrics().layers.len(), 2);
    }

    #[test]
    fn test_builder_demotion_threshold() {
        let cache = S3FifoCacheBuilder::new()
            .ram_size(1024 * 1024)
            .segment_size(64 * 1024)
            .small_queue_percent(20)
            .hashtable_power(10)
            .demotion_threshold(2)
            .build()
            .expect("Failed to create cache");

        assert_eq!(cache.metrics().layers.len(), 2);
    }

    #[test]
    fn test_builder_invalid_config() {
        // Small queue takes 100%, leaving nothing for main cache
        let result = S3FifoCacheBuilder::new()
            .ram_size(64 * 1024) // Very small
            .segment_size(64 * 1024) // Same as total = only 1 segment
            .small_queue_percent(100) // All for small queue
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_set_with_optional() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache
            .set_with_optional(b"key", b"value", b"optional_data", ttl)
            .expect("Failed to set with optional");

        assert!(cache.contains(b"key"));

        let optional = cache.with_item(b"key", |guard| guard.optional().to_vec());
        assert_eq!(optional, Some(b"optional_data".to_vec()));
    }

    #[test]
    fn test_add_new_key() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        let result = cache.add(b"new_key", b"value", ttl);
        assert!(result.is_ok());

        let value = cache.get(b"new_key");
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[test]
    fn test_replace_existing() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set(b"key", b"value1", ttl).unwrap();

        let result = cache.replace(b"key", b"value2", ttl);
        assert!(result.is_ok());

        let value = cache.get(b"key");
        assert_eq!(value, Some(b"value2".to_vec()));
    }

    #[test]
    fn test_ttl() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set(b"key", b"value", ttl).unwrap();

        let remaining = cache.ttl(b"key");
        assert!(remaining.is_some());
        let secs = remaining.unwrap().as_secs();
        assert!(secs > 0 && secs <= 3600);

        assert!(cache.ttl(b"nonexistent").is_none());
    }

    #[test]
    fn test_expire() {
        let cache = create_test_cache();

        let expired = cache.expire();
        assert_eq!(expired, 0);
    }

    #[test]
    fn test_evict_small_queue_empty() {
        let cache = create_test_cache();

        let evicted = cache.evict_small_queue();
        assert!(!evicted);
    }

    #[test]
    fn test_evict_main_cache_empty() {
        let cache = create_test_cache();

        let evicted = cache.evict_main_cache();
        assert!(!evicted);
    }

    #[test]
    fn test_evict_small_queue_with_items() {
        let cache = S3FifoCacheBuilder::new()
            .ram_size(128 * 1024) // Small cache
            .segment_size(16 * 1024) // Small segments (8 total)
            .small_queue_percent(50) // 50% small queue (4 segments)
            .hashtable_power(8)
            .build()
            .expect("Failed to create cache");

        let ttl = Duration::from_secs(3600);
        let value = vec![b'x'; 2048]; // Larger values to fill faster

        // Fill the small queue with many items
        for i in 0..100 {
            let key = format!("key_{}", i);
            let _ = cache.set(key.as_bytes(), &value, ttl);
        }

        // Try eviction - may or may not succeed depending on state
        let _ = cache.evict_small_queue();
        // Just verify it doesn't panic
    }

    #[test]
    fn test_delete_nonexistent() {
        let cache = create_test_cache();

        let deleted = cache.delete(b"nonexistent");
        assert!(!deleted);
    }

    #[test]
    fn test_frequency_nonexistent() {
        let cache = create_test_cache();

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

        let result = Cache::get(&cache, b"key");
        assert!(result.is_some());
        let owned = result.unwrap();
        assert_eq!(owned.value(), b"value");

        let result = Cache::get(&cache, b"nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_trait_set_with_ttl() {
        let cache = create_test_cache();

        let result = Cache::set(&cache, b"key", b"value", Some(Duration::from_secs(3600)));
        assert!(result.is_ok());

        assert!(cache.contains(b"key"));
    }

    #[test]
    fn test_cache_trait_set_default_ttl() {
        let cache = create_test_cache();

        let result = Cache::set(&cache, b"key", b"value", None);
        assert!(result.is_ok());

        assert!(cache.contains(b"key"));

        let ttl = cache.ttl(b"key");
        assert!(ttl.is_some());
    }

    #[test]
    fn test_cache_trait_delete() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        cache.set(b"key", b"value", ttl).unwrap();

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

        // flush is a no-op for S3FifoCache
        Cache::flush(&cache);

        // Item should still exist
        assert!(cache.contains(b"key"));
    }

    #[test]
    fn test_metrics_structure() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        for i in 0..10 {
            let key = format!("key_{}", i);
            cache.set(key.as_bytes(), b"value", ttl).unwrap();
        }

        let metrics = cache.metrics();
        assert_eq!(metrics.layers.len(), 2);

        let layer0 = &metrics.layers[0];
        assert_eq!(layer0.layer_id, 0);
        assert!(layer0.pool.total_segments > 0);

        let layer1 = &metrics.layers[1];
        assert_eq!(layer1.layer_id, 1);
        assert!(layer1.pool.total_segments > 0);
    }

    #[test]
    fn test_multiple_operations() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        // Set multiple items
        for i in 0..50 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            cache.set(key.as_bytes(), value.as_bytes(), ttl).unwrap();
        }

        // Verify all items exist
        for i in 0..50 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            assert!(cache.contains(key.as_bytes()));
            assert_eq!(cache.get(key.as_bytes()), Some(value.into_bytes()));
        }

        // Delete some items
        for i in 0..25 {
            let key = format!("key_{}", i);
            assert!(cache.delete(key.as_bytes()));
        }

        // Verify deletions
        for i in 0..25 {
            let key = format!("key_{}", i);
            assert!(!cache.contains(key.as_bytes()));
        }

        // Remaining items should still exist
        for i in 25..50 {
            let key = format!("key_{}", i);
            assert!(cache.contains(key.as_bytes()));
        }
    }
}
