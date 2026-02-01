//! Shared cache using SegCache (allocation-free, lock-free).
//!
//! A single cache instance is shared across all worker threads. SegCache uses
//! lock-free data structures internally, so concurrent access from multiple
//! threads is safe and efficient. The cache uses pre-allocated segments, so
//! after initialization there are no allocations on the hot path.

use crate::config::{CacheConfig, EvictionPolicy};
use segcache::{EvictionPolicy as SegEviction, MergeConfig, SegCache, SegCacheBuilder};
use std::time::Duration;

/// Shared cache using SegCache (allocation-free, lock-free).
///
/// This cache is designed to be wrapped in `Arc` and shared across all worker
/// threads. All operations are thread-safe without explicit locking.
pub struct SharedCache {
    inner: Option<SegCache>,
    default_ttl: Duration,
}

impl SharedCache {
    /// Create a new shared cache from configuration.
    ///
    /// If caching is disabled, returns a no-op cache.
    pub fn new(config: &CacheConfig) -> Self {
        if !config.enabled {
            return Self {
                inner: None,
                default_ttl: Duration::from_millis(config.ttl_ms),
            };
        }

        let eviction = match config.eviction {
            EvictionPolicy::Random => SegEviction::Random,
            EvictionPolicy::Fifo => SegEviction::Fifo,
            EvictionPolicy::Cte => SegEviction::Cte,
            EvictionPolicy::Merge => SegEviction::Merge(MergeConfig::default()),
            EvictionPolicy::S3fifo => SegEviction::S3Fifo {
                small_queue_percent: 10,
                demotion_threshold: 1,
            },
        };

        let cache = SegCacheBuilder::new()
            .heap_size(config.heap_size)
            .segment_size(config.segment_size)
            .hashtable_power(config.hashtable_power)
            .eviction_policy(eviction)
            .build()
            .expect("failed to build segcache");

        Self {
            inner: Some(cache),
            default_ttl: Duration::from_millis(config.ttl_ms),
        }
    }

    /// Access a cached value without copying (zero-copy).
    ///
    /// Calls the provided function with the cached RESP response bytes.
    /// Returns `Some(R)` if the key exists, `None` otherwise.
    #[inline]
    pub fn with_value<F, R>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        self.inner
            .as_ref()?
            .with_item(key, |guard| f(guard.value()))
    }

    /// Cache a value with the default TTL.
    #[inline]
    pub fn set(&self, key: &[u8], value: &[u8]) {
        if let Some(cache) = &self.inner {
            let _ = cache.set(key, value, self.default_ttl);
        }
    }

    /// Cache a value with a specific TTL.
    #[inline]
    pub fn set_with_ttl(&self, key: &[u8], value: &[u8], ttl: Duration) {
        if let Some(cache) = &self.inner {
            let _ = cache.set(key, value, ttl);
        }
    }

    /// Delete a key from the cache.
    #[inline]
    pub fn delete(&self, key: &[u8]) {
        if let Some(cache) = &self.inner {
            cache.delete(key);
        }
    }

    /// Expire stale segments. Call periodically.
    ///
    /// Returns the number of segments expired.
    #[inline]
    pub fn expire(&self) -> usize {
        self.inner.as_ref().map(|c| c.expire()).unwrap_or(0)
    }

    /// Is caching enabled?
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> CacheConfig {
        CacheConfig {
            enabled: true,
            ttl_ms: 60_000,
            heap_size: 1024 * 1024,  // 1MB
            segment_size: 64 * 1024, // 64KB
            eviction: EvictionPolicy::Random,
            hashtable_power: 10,
        }
    }

    /// Helper to get a value from cache as Vec<u8> for testing.
    fn get_value(cache: &SharedCache, key: &[u8]) -> Option<Vec<u8>> {
        cache.with_value(key, |v| v.to_vec())
    }

    #[test]
    fn test_cache_disabled() {
        let config = CacheConfig {
            enabled: false,
            ..test_config()
        };
        let cache = SharedCache::new(&config);

        assert!(!cache.is_enabled());
        assert!(get_value(&cache, b"key").is_none());

        // These should be no-ops
        cache.set(b"key", b"value");
        cache.delete(b"key");
        assert_eq!(cache.expire(), 0);
    }

    #[test]
    fn test_cache_set_get() {
        let cache = SharedCache::new(&test_config());

        assert!(cache.is_enabled());
        assert!(get_value(&cache, b"key").is_none());

        cache.set(b"key", b"value");
        let value = get_value(&cache, b"key");
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[test]
    fn test_cache_delete() {
        let cache = SharedCache::new(&test_config());

        cache.set(b"key", b"value");
        assert!(get_value(&cache, b"key").is_some());

        cache.delete(b"key");
        assert!(get_value(&cache, b"key").is_none());
    }

    #[test]
    fn test_cache_overwrite() {
        let cache = SharedCache::new(&test_config());

        cache.set(b"key", b"value1");
        assert_eq!(get_value(&cache, b"key"), Some(b"value1".to_vec()));

        cache.set(b"key", b"value2");
        assert_eq!(get_value(&cache, b"key"), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_eviction_policies() {
        for policy in [
            EvictionPolicy::Random,
            EvictionPolicy::Fifo,
            EvictionPolicy::Cte,
            EvictionPolicy::Merge,
            EvictionPolicy::S3fifo,
        ] {
            // S3Fifo requires a larger cache for its two-tier architecture
            let heap_size = if matches!(policy, EvictionPolicy::S3fifo) {
                4 * 1024 * 1024 // 4MB for S3Fifo
            } else {
                1024 * 1024 // 1MB for others
            };

            let config = CacheConfig {
                eviction: policy,
                heap_size,
                ..test_config()
            };
            let cache = SharedCache::new(&config);
            assert!(cache.is_enabled());

            cache.set(b"key", b"value");
            assert_eq!(get_value(&cache, b"key"), Some(b"value".to_vec()));
        }
    }
}
