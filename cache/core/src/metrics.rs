//! Cache metrics and statistics.
//!
//! This module provides metrics collection and export for cache components:
//!
//! - [`PoolMetrics`] - Segment pool statistics
//! - [`LayerMetrics`] - Layer-level statistics
//! - [`CacheMetrics`] - Overall cache statistics
//! - [`MetricsExport`] - Trait for exporting metrics from components

use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics for a segment pool.
#[derive(Debug, Default)]
pub struct PoolMetrics {
    /// Total number of segments in the pool.
    pub total_segments: u64,
    /// Number of segments currently in use.
    pub used_segments: u64,
    /// Number of free segments available.
    pub free_segments: u64,
    /// Total capacity in bytes.
    pub total_bytes: u64,
    /// Bytes currently in use (approximate).
    pub used_bytes: u64,
    /// Segment size in bytes.
    pub segment_size: u64,
}

impl PoolMetrics {
    /// Create new pool metrics.
    pub fn new(total_segments: u64, used_segments: u64, segment_size: u64) -> Self {
        Self {
            total_segments,
            used_segments,
            free_segments: total_segments.saturating_sub(used_segments),
            total_bytes: total_segments * segment_size,
            used_bytes: used_segments * segment_size,
            segment_size,
        }
    }

    /// Get utilization as a percentage (0.0 - 100.0).
    pub fn utilization(&self) -> f64 {
        if self.total_segments == 0 {
            0.0
        } else {
            (self.used_segments as f64 / self.total_segments as f64) * 100.0
        }
    }
}

/// Metrics for a cache layer.
#[derive(Debug, Default)]
pub struct LayerMetrics {
    /// Layer identifier.
    pub layer_id: u8,
    /// Pool metrics for this layer.
    pub pool: PoolMetrics,
    /// Number of items stored (approximate).
    pub item_count: u64,
    /// Number of segments evicted.
    pub segments_evicted: u64,
    /// Number of segments expired.
    pub segments_expired: u64,
    /// Number of items converted to ghosts.
    pub ghosts_created: u64,
    /// Number of items demoted to next layer.
    pub items_demoted: u64,
}

impl LayerMetrics {
    /// Create new layer metrics.
    pub fn new(layer_id: u8, pool: PoolMetrics) -> Self {
        Self {
            layer_id,
            pool,
            ..Default::default()
        }
    }
}

/// Metrics for the entire cache.
#[derive(Debug, Default)]
pub struct CacheMetrics {
    /// Metrics for each layer.
    pub layers: Vec<LayerMetrics>,
    /// Total GET operations.
    pub gets: u64,
    /// GET operations that found the key (hits).
    pub get_hits: u64,
    /// GET operations that didn't find the key (misses).
    pub get_misses: u64,
    /// Total SET operations.
    pub sets: u64,
    /// SET operations that succeeded.
    pub set_successes: u64,
    /// SET operations that failed.
    pub set_failures: u64,
    /// Total DELETE operations.
    pub deletes: u64,
    /// DELETE operations that found the key.
    pub delete_hits: u64,
    /// DELETE operations that didn't find the key.
    pub delete_misses: u64,
    /// Total evictions triggered.
    pub evictions: u64,
    /// Total expirations.
    pub expirations: u64,
    /// Current number of items in cache (approximate).
    pub item_count: u64,
    /// Current bytes used (approximate).
    pub bytes_used: u64,
}

impl CacheMetrics {
    /// Create new cache metrics with the given layer metrics.
    pub fn new(layers: Vec<LayerMetrics>) -> Self {
        let bytes_used = layers.iter().map(|l| l.pool.used_bytes).sum();
        Self {
            layers,
            bytes_used,
            ..Default::default()
        }
    }

    /// Get overall hit rate as a percentage (0.0 - 100.0).
    pub fn hit_rate(&self) -> f64 {
        let total = self.get_hits + self.get_misses;
        if total == 0 {
            0.0
        } else {
            (self.get_hits as f64 / total as f64) * 100.0
        }
    }

    /// Get total number of segments across all layers.
    pub fn total_segments(&self) -> u64 {
        self.layers.iter().map(|l| l.pool.total_segments).sum()
    }

    /// Get total used segments across all layers.
    pub fn used_segments(&self) -> u64 {
        self.layers.iter().map(|l| l.pool.used_segments).sum()
    }

    /// Get overall utilization as a percentage (0.0 - 100.0).
    pub fn utilization(&self) -> f64 {
        let total = self.total_segments();
        if total == 0 {
            0.0
        } else {
            (self.used_segments() as f64 / total as f64) * 100.0
        }
    }
}

/// Trait for exporting metrics from cache components.
pub trait MetricsExport {
    /// The type of metrics this component exports.
    type Metrics;

    /// Export current metrics snapshot.
    fn metrics(&self) -> Self::Metrics;
}

/// Atomic counters for tracking cache operations.
///
/// These counters can be shared across threads for lock-free updates.
#[derive(Debug, Default)]
pub struct AtomicCounters {
    /// GET operations.
    pub gets: AtomicU64,
    /// GET hits.
    pub get_hits: AtomicU64,
    /// GET misses.
    pub get_misses: AtomicU64,
    /// SET operations.
    pub sets: AtomicU64,
    /// SET successes.
    pub set_successes: AtomicU64,
    /// SET failures.
    pub set_failures: AtomicU64,
    /// DELETE operations.
    pub deletes: AtomicU64,
    /// DELETE hits.
    pub delete_hits: AtomicU64,
    /// DELETE misses.
    pub delete_misses: AtomicU64,
    /// Evictions.
    pub evictions: AtomicU64,
    /// Expirations.
    pub expirations: AtomicU64,
}

impl AtomicCounters {
    /// Create new atomic counters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a GET operation.
    #[inline]
    pub fn record_get(&self, hit: bool) {
        self.gets.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.get_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.get_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a SET operation.
    #[inline]
    pub fn record_set(&self, success: bool) {
        self.sets.fetch_add(1, Ordering::Relaxed);
        if success {
            self.set_successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.set_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a DELETE operation.
    #[inline]
    pub fn record_delete(&self, hit: bool) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.delete_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.delete_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record an eviction.
    #[inline]
    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an expiration.
    #[inline]
    pub fn record_expiration(&self) {
        self.expirations.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot the current counter values.
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            gets: self.gets.load(Ordering::Relaxed),
            get_hits: self.get_hits.load(Ordering::Relaxed),
            get_misses: self.get_misses.load(Ordering::Relaxed),
            sets: self.sets.load(Ordering::Relaxed),
            set_successes: self.set_successes.load(Ordering::Relaxed),
            set_failures: self.set_failures.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            delete_hits: self.delete_hits.load(Ordering::Relaxed),
            delete_misses: self.delete_misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            expirations: self.expirations.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.gets.store(0, Ordering::Relaxed);
        self.get_hits.store(0, Ordering::Relaxed);
        self.get_misses.store(0, Ordering::Relaxed);
        self.sets.store(0, Ordering::Relaxed);
        self.set_successes.store(0, Ordering::Relaxed);
        self.set_failures.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.delete_hits.store(0, Ordering::Relaxed);
        self.delete_misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.expirations.store(0, Ordering::Relaxed);
    }
}

/// Snapshot of counter values at a point in time.
#[derive(Debug, Clone, Copy, Default)]
pub struct CounterSnapshot {
    /// GET operations.
    pub gets: u64,
    /// GET hits.
    pub get_hits: u64,
    /// GET misses.
    pub get_misses: u64,
    /// SET operations.
    pub sets: u64,
    /// SET successes.
    pub set_successes: u64,
    /// SET failures.
    pub set_failures: u64,
    /// DELETE operations.
    pub deletes: u64,
    /// DELETE hits.
    pub delete_hits: u64,
    /// DELETE misses.
    pub delete_misses: u64,
    /// Evictions.
    pub evictions: u64,
    /// Expirations.
    pub expirations: u64,
}

impl CounterSnapshot {
    /// Get hit rate as a percentage (0.0 - 100.0).
    pub fn hit_rate(&self) -> f64 {
        let total = self.get_hits + self.get_misses;
        if total == 0 {
            0.0
        } else {
            (self.get_hits as f64 / total as f64) * 100.0
        }
    }

    /// Get SET success rate as a percentage (0.0 - 100.0).
    pub fn set_success_rate(&self) -> f64 {
        if self.sets == 0 {
            0.0
        } else {
            (self.set_successes as f64 / self.sets as f64) * 100.0
        }
    }

    /// Compute the difference between two snapshots (self - other).
    ///
    /// Useful for computing rates over an interval.
    pub fn diff(&self, other: &CounterSnapshot) -> CounterSnapshot {
        CounterSnapshot {
            gets: self.gets.saturating_sub(other.gets),
            get_hits: self.get_hits.saturating_sub(other.get_hits),
            get_misses: self.get_misses.saturating_sub(other.get_misses),
            sets: self.sets.saturating_sub(other.sets),
            set_successes: self.set_successes.saturating_sub(other.set_successes),
            set_failures: self.set_failures.saturating_sub(other.set_failures),
            deletes: self.deletes.saturating_sub(other.deletes),
            delete_hits: self.delete_hits.saturating_sub(other.delete_hits),
            delete_misses: self.delete_misses.saturating_sub(other.delete_misses),
            evictions: self.evictions.saturating_sub(other.evictions),
            expirations: self.expirations.saturating_sub(other.expirations),
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_pool_metrics() {
        let metrics = PoolMetrics::new(100, 60, 1024 * 1024);

        assert_eq!(metrics.total_segments, 100);
        assert_eq!(metrics.used_segments, 60);
        assert_eq!(metrics.free_segments, 40);
        assert_eq!(metrics.total_bytes, 100 * 1024 * 1024);
        assert_eq!(metrics.used_bytes, 60 * 1024 * 1024);
        assert!((metrics.utilization() - 60.0).abs() < 0.001);
    }

    #[test]
    fn test_pool_metrics_empty() {
        let metrics = PoolMetrics::new(0, 0, 1024);
        assert_eq!(metrics.utilization(), 0.0);
    }

    #[test]
    fn test_layer_metrics() {
        let pool = PoolMetrics::new(10, 5, 64 * 1024);
        let layer = LayerMetrics::new(0, pool);

        assert_eq!(layer.layer_id, 0);
        assert_eq!(layer.pool.total_segments, 10);
        assert_eq!(layer.pool.used_segments, 5);
    }

    #[test]
    fn test_cache_metrics() {
        let layer0 = LayerMetrics::new(0, PoolMetrics::new(10, 5, 1024));
        let layer1 = LayerMetrics::new(1, PoolMetrics::new(20, 10, 1024));

        let mut metrics = CacheMetrics::new(vec![layer0, layer1]);
        metrics.get_hits = 80;
        metrics.get_misses = 20;

        assert_eq!(metrics.layers.len(), 2);
        assert_eq!(metrics.total_segments(), 30);
        assert_eq!(metrics.used_segments(), 15);
        assert!((metrics.utilization() - 50.0).abs() < 0.001);
        assert!((metrics.hit_rate() - 80.0).abs() < 0.001);
    }

    #[test]
    fn test_cache_metrics_empty() {
        let metrics = CacheMetrics::default();
        assert_eq!(metrics.hit_rate(), 0.0);
        assert_eq!(metrics.utilization(), 0.0);
    }

    #[test]
    fn test_atomic_counters() {
        let counters = AtomicCounters::new();

        counters.record_get(true);
        counters.record_get(true);
        counters.record_get(false);
        counters.record_set(true);
        counters.record_set(false);
        counters.record_delete(true);
        counters.record_eviction();
        counters.record_expiration();

        let snapshot = counters.snapshot();

        assert_eq!(snapshot.gets, 3);
        assert_eq!(snapshot.get_hits, 2);
        assert_eq!(snapshot.get_misses, 1);
        assert_eq!(snapshot.sets, 2);
        assert_eq!(snapshot.set_successes, 1);
        assert_eq!(snapshot.set_failures, 1);
        assert_eq!(snapshot.deletes, 1);
        assert_eq!(snapshot.delete_hits, 1);
        assert_eq!(snapshot.evictions, 1);
        assert_eq!(snapshot.expirations, 1);
    }

    #[test]
    fn test_atomic_counters_reset() {
        let counters = AtomicCounters::new();

        counters.record_get(true);
        counters.record_set(true);

        counters.reset();

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.gets, 0);
        assert_eq!(snapshot.sets, 0);
    }

    #[test]
    fn test_counter_snapshot_hit_rate() {
        let snapshot = CounterSnapshot {
            get_hits: 75,
            get_misses: 25,
            ..Default::default()
        };

        assert!((snapshot.hit_rate() - 75.0).abs() < 0.001);
    }

    #[test]
    fn test_counter_snapshot_diff() {
        let before = CounterSnapshot {
            gets: 100,
            get_hits: 80,
            get_misses: 20,
            ..Default::default()
        };

        let after = CounterSnapshot {
            gets: 150,
            get_hits: 120,
            get_misses: 30,
            ..Default::default()
        };

        let diff = after.diff(&before);

        assert_eq!(diff.gets, 50);
        assert_eq!(diff.get_hits, 40);
        assert_eq!(diff.get_misses, 10);
    }

    #[test]
    fn test_counter_snapshot_set_success_rate() {
        let snapshot = CounterSnapshot {
            sets: 100,
            set_successes: 95,
            set_failures: 5,
            ..Default::default()
        };

        assert!((snapshot.set_success_rate() - 95.0).abs() < 0.001);
    }

    #[test]
    fn test_counter_snapshot_empty_rates() {
        let snapshot = CounterSnapshot::default();

        assert_eq!(snapshot.hit_rate(), 0.0);
        assert_eq!(snapshot.set_success_rate(), 0.0);
    }
}
