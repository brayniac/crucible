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
    CasToken, DiskLayerBuilder, FifoLayerBuilder, IoUringDiskLayerBuilder, ItemGuard, LayerConfig,
    MultiChoiceHashtable, TieredCache, TieredCacheBuilder, TtlLayerBuilder,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

// Re-export common types from cache-core
pub use cache_core::{
    AtomicCounters, BasicHeader, BasicItemGuard, Cache, CacheError, CacheInternalStats, CacheLayer,
    CacheMetrics, CacheResult, CounterSnapshot, DEFAULT_TTL, EvictionStrategy, FrequencyDecay,
    HugepageSize, ItemLocation, LayerMetrics, LookupResult, MergeConfig, OwnedGuard, PoolMetrics,
    SyncMode, ValueRef,
};

/// Eviction policy for the segmented cache.
///
/// This determines the overall cache architecture and eviction strategy.
#[derive(Debug, Clone, Default)]
pub enum EvictionPolicy {
    /// Pick a bucket at random, weighted by segment count, and evict its
    /// oldest segment (default).
    ///
    /// Weights eviction toward the TTL ranges holding the most memory while
    /// preserving the TTL distribution. Before #156 this was the only segment
    /// policy implemented, and `Random`, `Fifo` and `Cte` were all aliases
    /// for it.
    #[default]
    RandomFifo,

    /// Evict a segment chosen uniformly at random.
    ///
    /// Simple and fast, but blind to item value. The only policy here that
    /// can take a segment from the middle of a bucket chain.
    Random,

    /// Strict FIFO segment eviction.
    ///
    /// Evicts the oldest segment in the cache, across all TTL buckets. Since
    /// segments are append-only this behaves like LRU at segment granularity.
    Fifo,

    /// Closest to expiration eviction.
    ///
    /// Evicts the segment whose items expire soonest.
    Cte,

    /// Adaptive merge eviction.
    ///
    /// Prunes low-frequency items from segments before full eviction.
    /// Can reclaim space more efficiently when segments have mixed-frequency items.
    Merge(MergeConfig),

    /// S3-FIFO: Two-tier admission filtering architecture.
    ///
    /// Uses a small FIFO queue as an admission filter (~10% of capacity)
    /// and a main cache with merge eviction (~90% of capacity).
    ///
    /// Items must be accessed multiple times to be promoted to the main cache.
    /// Provides better hit rates for workloads with one-hit wonders.
    S3Fifo {
        /// Percentage of total capacity for the small queue (1-50, default: 10).
        small_queue_percent: u8,
        /// Frequency threshold for promotion (default: 1).
        demotion_threshold: u8,
    },
}

/// Segment-based cache with single-tier architecture.
///
/// All items are stored directly in a TTL-organized layer with
/// configurable eviction policy.
pub struct SegCache {
    inner: TieredCache<MultiChoiceHashtable>,
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

    /// Get a cache layer by index.
    ///
    /// Layer 0 is the admission queue (FIFO), layer 1 is the main cache (TTL),
    /// and layer 2 (if present) is the disk tier.
    pub fn layer(&self, index: usize) -> Option<&CacheLayer> {
        self.inner.layer(index)
    }

    /// Get the number of cache layers.
    pub fn layer_count(&self) -> usize {
        self.inner.layer_count()
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

/// Configuration for the disk tier.
#[derive(Debug, Clone)]
pub struct DiskTierConfig {
    /// Path to the disk cache file.
    pub path: PathBuf,
    /// Total size of disk storage in bytes.
    pub size: usize,
    /// Frequency threshold for promoting items from disk to RAM.
    /// Items with frequency > threshold are promoted on read.
    pub promotion_threshold: u8,
    /// Synchronization mode for disk writes.
    pub sync_mode: SyncMode,
    /// Whether to recover from existing disk cache on startup.
    pub recover_on_startup: bool,
}

/// Configuration for the io_uring-based disk tier (Direct I/O).
///
/// Unlike [`DiskTierConfig`] which uses mmap, this tier performs I/O through
/// io_uring with O_DIRECT, bypassing the page cache entirely.
/// Segment metadata is managed in-memory by [`cache_core::IoUringDiskLayer`].
#[derive(Debug, Clone)]
pub struct IoUringDiskTierConfig {
    /// Number of disk segments.
    pub segment_count: usize,
    /// I/O block size (default 4096).
    pub block_size: u32,
    /// Frequency threshold for promoting items from disk to RAM.
    pub promotion_threshold: u8,
    /// Number of write buffers for staging segment data before disk flush.
    /// Under pressure, demotion degrades to discard. Default: 16.
    pub write_buffer_count: usize,
}

impl Default for IoUringDiskTierConfig {
    fn default() -> Self {
        Self {
            segment_count: 128,
            block_size: 4096,
            promotion_threshold: 2,
            write_buffer_count: 16,
        }
    }
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
    hashtable_seed: Option<[u64; 4]>,
    eviction_seed: Option<u64>,
    main_eviction: EvictionStrategy,

    /// Hugepage size preference.
    hugepage_size: HugepageSize,

    /// Enable ghost entries for evicted items.
    enable_ghosts: bool,

    /// NUMA node to bind memory to (Linux only).
    numa_node: Option<u32>,

    /// Eviction policy for the cache.
    eviction_policy: EvictionPolicy,

    /// Disk tier configuration (optional, mmap-based).
    disk_tier: Option<DiskTierConfig>,

    /// io_uring disk tier configuration (optional, Direct I/O).
    io_uring_disk_tier: Option<IoUringDiskTierConfig>,
}

/// The layer strategy a single-layer policy runs.
///
/// Lifted out of `build_single_layer` and pinned by
/// `every_single_layer_policy_maps_to_its_own_strategy`, because #156 was
/// exactly this mapping collapsing: `fifo`, `random` and `cte` were three
/// accepted names that all reached one behaviour, and nothing said so.
///
/// # Panics
///
/// On [`EvictionPolicy::S3Fifo`], which is a two-layer topology and never
/// reaches the single-layer builder.
fn single_layer_strategy(policy: &EvictionPolicy) -> EvictionStrategy {
    match policy {
        EvictionPolicy::RandomFifo => EvictionStrategy::RandomFifo,
        EvictionPolicy::Random => EvictionStrategy::Random,
        EvictionPolicy::Fifo => EvictionStrategy::Fifo,
        EvictionPolicy::Cte => EvictionStrategy::Cte,
        EvictionPolicy::Merge(config) => EvictionStrategy::Merge(*config),
        EvictionPolicy::S3Fifo { .. } => {
            unreachable!("s3fifo builds two layers, not one")
        }
    }
}

/// [`LayerConfig::new`] with the builder's eviction seed applied, if it set
/// one.
///
/// Every layer a `SegCacheBuilder` constructs goes through here, so a seeded
/// build seeds the disk tiers and the S3-FIFO admission queue too, not just
/// the main cache.
fn seeded_layer_config(seed: Option<u64>) -> LayerConfig {
    match seed {
        Some(seed) => LayerConfig::new().with_eviction_seed(seed),
        None => LayerConfig::new(),
    }
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
    /// - Eviction: Random
    /// - Disk tier: disabled
    pub fn new() -> Self {
        Self {
            heap_size: 64 * 1024 * 1024, // 64MB
            segment_size: 1024 * 1024,   // 1MB
            hashtable_power: 16,         // 64K buckets
            hashtable_seed: None,
            eviction_seed: None,
            main_eviction: EvictionStrategy::Merge(MergeConfig::default()),
            hugepage_size: HugepageSize::None,
            enable_ghosts: false,
            numa_node: None,
            eviction_policy: EvictionPolicy::Random,
            disk_tier: None,
            io_uring_disk_tier: None,
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

    /// Pin the hashtable's hash seed so key placement is reproducible.
    ///
    /// Unseeded, the table seeds from the OS and bucket occupancy differs run
    /// to run. That variance is the noise floor a measured eviction difference
    /// has to clear, so a benchmark pins it to make one run repeatable and
    /// varies it deliberately to size the floor. Leave unset in production.
    pub fn hashtable_seed(mut self, seed: [u64; 4]) -> Self {
        self.hashtable_seed = Some(seed);
        self
    }

    /// Seed every layer's eviction randomness.
    ///
    /// Which bucket `RandomFifo` picks, and which segment `Random` picks.
    /// Unset, the layers use [`DEFAULT_EVICTION_SEED`], which is fixed -- so
    /// a run is reproducible either way. Set it to sweep: a policy difference
    /// that survives a spread of seeds is a policy difference, one that does
    /// not is the luck of a single sequence. The same reason
    /// [`hashtable_seed`](Self::hashtable_seed) is exposed.
    ///
    /// [`DEFAULT_EVICTION_SEED`]: cache_core::DEFAULT_EVICTION_SEED
    pub fn eviction_seed(mut self, seed: u64) -> Self {
        self.eviction_seed = Some(seed);
        self
    }

    /// The eviction seed this builder will apply, if any.
    ///
    /// A readback, so a caller that plumbs the seed through from a config
    /// file can assert the plumbing without building a cache. A seed that
    /// parses but never reaches the builder makes a sweep over seeds report a
    /// spread of zero, which is the one failure the seed exists to rule out.
    pub fn configured_eviction_seed(&self) -> Option<u64> {
        self.eviction_seed
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

    /// Set the eviction policy.
    ///
    /// - `EvictionPolicy::Random` - Random segment selection (default)
    /// - `EvictionPolicy::Fifo` - Strict FIFO segment selection
    /// - `EvictionPolicy::Cte` - Closest to expiration
    /// - `EvictionPolicy::Merge(config)` - Adaptive merge eviction
    /// - `EvictionPolicy::S3Fifo { .. }` - Two-tier S3-FIFO architecture
    pub fn eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }

    /// Choose how the S3-FIFO main cache (layer 1) reclaims segments.
    ///
    /// Defaults to adaptive merge. [`EvictionStrategy::Clock`] runs the same
    /// machinery with the chain pinned to one segment and the prune threshold
    /// pinned above the insert baseline, which is the comparison in
    /// `docs/superpowers/specs/2026-09-18-s3fifo-main-pool-experiment-design.md`.
    ///
    /// Ignored unless the eviction policy is `S3Fifo`; the single-layer
    /// policies configure their own layer directly.
    pub fn main_eviction(mut self, strategy: EvictionStrategy) -> Self {
        self.main_eviction = strategy;
        self
    }

    /// Use S3-FIFO eviction policy with default settings.
    ///
    /// S3-FIFO uses a small FIFO queue (10% of capacity) as an admission filter
    /// and promotes frequently accessed items to a main cache (90% of capacity).
    pub fn s3fifo(mut self) -> Self {
        self.eviction_policy = EvictionPolicy::S3Fifo {
            small_queue_percent: 10,
            demotion_threshold: 1,
        };
        self
    }

    /// Enable disk tier with the given configuration.
    ///
    /// When enabled, items evicted from RAM are demoted to disk storage
    /// instead of being discarded. On disk hit, items can be promoted
    /// back to RAM based on access frequency.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use segcache::{SegCacheBuilder, DiskTierConfig};
    ///
    /// let cache = SegCacheBuilder::new()
    ///     .heap_size(4 * 1024 * 1024 * 1024)  // 4GB RAM
    ///     .disk_tier(DiskTierConfig::new("/var/cache/crucible/disk.dat", 100 * 1024 * 1024 * 1024))  // 100GB disk
    ///     .build()?;
    /// ```
    pub fn disk_tier(mut self, config: DiskTierConfig) -> Self {
        self.disk_tier = Some(config);
        self
    }

    /// Enable io_uring-based disk tier with Direct I/O.
    ///
    /// Unlike [`disk_tier`](Self::disk_tier) which uses mmap, this tier
    /// performs all I/O through io_uring with O_DIRECT. Segment metadata
    /// is managed in-memory; the server handler submits reads/writes via
    /// ringline's Direct I/O API.
    pub fn io_uring_disk_tier(mut self, config: IoUringDiskTierConfig) -> Self {
        self.io_uring_disk_tier = Some(config);
        self
    }

    /// Build the SegCache.
    ///
    /// # Errors
    ///
    /// Returns an error if memory allocation fails or configuration is invalid.
    pub fn build(self) -> Result<SegCache, std::io::Error> {
        // Create hashtable
        let hashtable = Arc::new(match self.hashtable_seed {
            Some(seed) => MultiChoiceHashtable::with_seeds(self.hashtable_power, 2, seed),
            None => MultiChoiceHashtable::new(self.hashtable_power),
        });

        // Build based on eviction policy
        let inner = match self.eviction_policy {
            EvictionPolicy::S3Fifo {
                small_queue_percent,
                demotion_threshold,
            } => self.build_s3fifo(hashtable, small_queue_percent, demotion_threshold)?,

            _ => self.build_single_layer(hashtable)?,
        };

        Ok(SegCache { inner })
    }

    /// Build a single-layer cache architecture.
    fn build_single_layer(
        self,
        hashtable: Arc<MultiChoiceHashtable>,
    ) -> Result<TieredCache<MultiChoiceHashtable>, std::io::Error> {
        let eviction_seed = self.eviction_seed;
        let eviction_strategy = single_layer_strategy(&self.eviction_policy);

        // If disk tier is enabled, configure demotion to disk layer (layer 1)
        let mut layer_config = seeded_layer_config(eviction_seed)
            .with_ghosts(self.enable_ghosts)
            .with_eviction_strategy(eviction_strategy);

        let has_disk = self.disk_tier.is_some() || self.io_uring_disk_tier.is_some();
        if has_disk {
            // Demote to disk layer (layer 1) with demotion threshold of 1
            // (demote items that have been accessed at least once)
            layer_config = layer_config.with_next_layer(1).with_demotion_threshold(1);
        }

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

        let mut builder = TieredCacheBuilder::new(hashtable).with_layer(CacheLayer::Ttl(layer));

        // Add disk layer if configured
        if let Some(disk_config) = self.disk_tier {
            let disk_layer_config = seeded_layer_config(eviction_seed)
                .with_ghosts(self.enable_ghosts)
                .with_eviction_strategy(EvictionStrategy::RandomFifo);

            let disk_layer = DiskLayerBuilder::new()
                .layer_id(1)
                .pool_id(2) // Use pool_id 2 for disk
                .config(disk_layer_config)
                .segment_size(self.segment_size)
                .path(disk_config.path)
                .size(disk_config.size)
                .sync_mode(disk_config.sync_mode)
                .build()?;

            builder = builder.with_disk_layer(disk_layer);
        } else if let Some(io_uring_config) = self.io_uring_disk_tier {
            let disk_layer_config = seeded_layer_config(eviction_seed)
                .with_ghosts(self.enable_ghosts)
                .with_eviction_strategy(EvictionStrategy::RandomFifo);

            let io_uring_layer = IoUringDiskLayerBuilder::new()
                .layer_id(1)
                .pool_id(2)
                .config(disk_layer_config)
                .segment_size(self.segment_size)
                .segment_count(io_uring_config.segment_count)
                .block_size(io_uring_config.block_size)
                .write_buffer_count(io_uring_config.write_buffer_count)
                .build();

            builder = builder.with_io_uring_disk_layer(io_uring_layer);
        }

        Ok(builder.build())
    }

    /// Build a two-layer S3-FIFO cache architecture.
    fn build_s3fifo(
        self,
        hashtable: Arc<MultiChoiceHashtable>,
        small_queue_percent: u8,
        demotion_threshold: u8,
    ) -> Result<TieredCache<MultiChoiceHashtable>, std::io::Error> {
        let eviction_seed = self.eviction_seed;

        // Calculate segment counts
        let total_segments = self.heap_size / self.segment_size;
        let small_percent = small_queue_percent.clamp(1, 50) as usize;
        let small_queue_segments = ((total_segments * small_percent) / 100).max(1);
        let main_cache_segments = total_segments.saturating_sub(small_queue_segments);

        if main_cache_segments == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Not enough segments for main cache",
            ));
        }

        let small_queue_size = small_queue_segments * self.segment_size;
        let main_cache_size = main_cache_segments * self.segment_size;

        // Layer 0: FIFO small queue with ghosts and demotion to layer 1
        let layer0_config = seeded_layer_config(eviction_seed)
            .with_ghosts(true)
            .with_next_layer(1)
            .with_demotion_threshold(demotion_threshold);

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

        // Layer 1: TTL-organized main cache with merge eviction
        // If disk tier is enabled, configure demotion to disk layer (layer 2)
        let mut layer1_config = seeded_layer_config(eviction_seed)
            .with_ghosts(true)
            .with_eviction_strategy(self.main_eviction);

        let has_disk = self.disk_tier.is_some() || self.io_uring_disk_tier.is_some();
        if has_disk {
            // Demote to disk layer (layer 2) with demotion threshold of 1
            layer1_config = layer1_config.with_next_layer(2).with_demotion_threshold(1);
        }

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

        let mut builder = TieredCacheBuilder::new(hashtable)
            .with_fifo_layer(layer0)
            .with_ttl_layer(layer1);

        // Add disk layer if configured
        if let Some(disk_config) = self.disk_tier {
            let disk_layer_config = seeded_layer_config(eviction_seed)
                .with_ghosts(true)
                .with_eviction_strategy(EvictionStrategy::RandomFifo);

            let disk_layer = DiskLayerBuilder::new()
                .layer_id(2)
                .pool_id(2) // Use pool_id 2 for disk
                .config(disk_layer_config)
                .segment_size(self.segment_size)
                .path(disk_config.path)
                .size(disk_config.size)
                .sync_mode(disk_config.sync_mode)
                .build()?;

            builder = builder.with_disk_layer(disk_layer);
        } else if let Some(io_uring_config) = self.io_uring_disk_tier {
            let disk_layer_config = seeded_layer_config(eviction_seed)
                .with_ghosts(true)
                .with_eviction_strategy(EvictionStrategy::RandomFifo);

            let io_uring_layer = IoUringDiskLayerBuilder::new()
                .layer_id(2)
                .pool_id(2)
                .config(disk_layer_config)
                .segment_size(self.segment_size)
                .segment_count(io_uring_config.segment_count)
                .block_size(io_uring_config.block_size)
                .write_buffer_count(io_uring_config.write_buffer_count)
                .build();

            builder = builder.with_io_uring_disk_layer(io_uring_layer);
        }

        Ok(builder.build())
    }
}

impl Cache for SegCache {
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

    fn lookup(&self, key: &[u8]) -> LookupResult {
        self.inner.lookup(key)
    }

    fn internal_stats(&self) -> Option<CacheInternalStats> {
        Some(CacheInternalStats {
            resident_items: self.inner.resident_items(),
            free_segments: self.inner.ram_free_segment_count(),
            total_segments: self.inner.ram_total_segment_count(),
            ..self.inner.stats().snapshot()
        })
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
        self.inner.flush();
    }

    fn take_flush_queue(&self) -> Vec<cache_core::FlushRequest> {
        self.inner.take_flush_queue()
    }

    fn complete_flush(&self, segment_id: u32) {
        self.inner.complete_flush(segment_id);
    }

    fn release_disk_read(&self, segment_id: u32, pool_id: u8) {
        self.inner.release_disk_read(segment_id, pool_id);
    }

    fn add(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let ttl = ttl.unwrap_or(DEFAULT_TTL);
        self.inner.add(key, value, b"", ttl)
    }

    fn replace(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let ttl = ttl.unwrap_or(DEFAULT_TTL);
        self.inner.replace(key, value, b"", ttl)
    }

    fn begin_segment_set(
        &self,
        key: &[u8],
        value_len: usize,
        ttl: Option<Duration>,
    ) -> Result<cache_core::SegmentReservation, CacheError> {
        let ttl = ttl.unwrap_or(DEFAULT_TTL);
        self.inner.begin_segment_set(key, value_len, ttl)
    }

    fn commit_segment_set(
        &self,
        reservation: cache_core::SegmentReservation,
    ) -> Result<(), CacheError> {
        self.inner.commit_segment_set(reservation)
    }

    fn cancel_segment_set(&self, reservation: cache_core::SegmentReservation) {
        self.inner.cancel_segment_set(reservation);
    }

    fn get_with_cas(&self, key: &[u8]) -> Option<(Vec<u8>, u64)> {
        self.inner
            .get_with_cas(key)
            .map(|(value, cas_token)| (value, cas_token.as_raw()))
    }

    fn with_value_cas<F, R>(&self, key: &[u8], f: F) -> Option<(R, u64)>
    where
        F: FnOnce(&[u8]) -> R,
    {
        self.inner
            .with_value_cas(key, f)
            .map(|(result, cas_token)| (result, cas_token.as_raw()))
    }

    fn cas(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
        cas: u64,
    ) -> Result<bool, CacheError> {
        let ttl = ttl.unwrap_or(DEFAULT_TTL);
        let cas_token = CasToken::from_raw(cas);
        self.inner.cas(key, value, b"", ttl, cas_token)
    }

    fn increment(
        &self,
        key: &[u8],
        delta: u64,
        initial: Option<u64>,
        ttl: Option<Duration>,
    ) -> Result<u64, CacheError> {
        let ttl = ttl.unwrap_or(DEFAULT_TTL);
        self.inner.increment(key, delta, initial, ttl)
    }

    fn decrement(
        &self,
        key: &[u8],
        delta: u64,
        initial: Option<u64>,
        ttl: Option<Duration>,
    ) -> Result<u64, CacheError> {
        let ttl = ttl.unwrap_or(DEFAULT_TTL);
        self.inner.decrement(key, delta, initial, ttl)
    }

    fn append(&self, key: &[u8], data: &[u8]) -> Result<usize, CacheError> {
        self.inner.append(key, data)
    }

    fn prepend(&self, key: &[u8], data: &[u8]) -> Result<usize, CacheError> {
        self.inner.prepend(key, data)
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
    fn internal_stats_reports_ram_segment_fill() {
        let cache = create_test_cache();

        let before = cache.internal_stats().expect("segcache reports stats");
        assert!(
            before.total_segments > 0,
            "expected total_segments > 0 for a built cache, got {}",
            before.total_segments
        );
        assert!(
            before.free_segments <= before.total_segments,
            "free_segments ({}) must not exceed total_segments ({})",
            before.free_segments,
            before.total_segments
        );

        // Fill layer 0 (12 * 64KB segments at this heap/segment size) so at
        // least one segment moves from free to used, proving the field
        // reflects live pool state rather than a value fixed at build time.
        let ttl = Duration::from_secs(3600);
        let value = vec![b'v'; 32 * 1024];
        for i in 0..8u32 {
            let key = format!("fill_{i}");
            let _ = cache.set(key.as_bytes(), &value, ttl);
        }

        let after = cache.internal_stats().expect("segcache reports stats");
        assert_eq!(
            after.total_segments, before.total_segments,
            "total_segments should not change from writes alone"
        );
        assert!(
            after.free_segments < before.free_segments,
            "expected free_segments to drop after filling segments \
             (before={}, after={})",
            before.free_segments,
            after.free_segments
        );
    }

    #[test]
    fn internal_stats_total_segments_sums_every_layer_not_just_one() {
        // `create_test_cache()` (used above) builds a single-layer cache by
        // default -- an implementation that only ever read layer 0 would
        // still pass that test. Build explicitly with `.s3fifo()` to get the
        // FIFO admission + TTL main two-layer topology, so a wiring that
        // summed only one layer is caught here.
        let cache = SegCacheBuilder::new()
            .heap_size(1024 * 1024)
            .segment_size(64 * 1024)
            .hashtable_power(10)
            .s3fifo()
            .build()
            .expect("failed to build s3fifo test cache");

        assert_eq!(cache.layer_count(), 2);
        let expected_total = cache.layer(0).unwrap().total_segment_count() as u64
            + cache.layer(1).unwrap().total_segment_count() as u64;

        let stats = cache.internal_stats().expect("segcache reports stats");
        assert_eq!(
            stats.total_segments, expected_total,
            "total_segments must sum every RAM layer, not just one"
        );
        assert!(
            expected_total > cache.layer(0).unwrap().total_segment_count() as u64,
            "fixture must have a nonzero layer 1 for this test to be \
             meaningful"
        );
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
    fn a_pinned_hashtable_seed_reaches_the_table_it_builds() {
        // The behavioural consequence -- how much run-to-run spread the seed
        // accounts for -- is what the noise-floor sweep measures. What is
        // asserted here is only that the knob is wired, because a setter that
        // silently dropped its argument would make that sweep report a floor
        // of zero and every policy difference look significant.
        let builder = SegCacheBuilder::new().hashtable_seed([9, 8, 7, 6]);

        assert_eq!(builder.hashtable_seed, Some([9, 8, 7, 6]));
    }

    #[test]
    fn an_unseeded_builder_leaves_the_table_seeded_from_the_os() {
        assert_eq!(SegCacheBuilder::new().hashtable_seed, None);
    }

    /// The eviction seed has to reach every layer the builder makes, not just
    /// the main cache: a sweep over seeds that left the admission queue and
    /// the disk tier pinned would under-report the spread it is measuring.
    /// One name, one strategy, and no two names sharing one. This is the
    /// claim #156 found to be false.
    #[test]
    fn every_single_layer_policy_maps_to_its_own_strategy() {
        let cases = [
            (EvictionPolicy::RandomFifo, EvictionStrategy::RandomFifo),
            (EvictionPolicy::Random, EvictionStrategy::Random),
            (EvictionPolicy::Fifo, EvictionStrategy::Fifo),
            (EvictionPolicy::Cte, EvictionStrategy::Cte),
        ];
        for (policy, expected) in &cases {
            assert_eq!(
                single_layer_strategy(policy),
                *expected,
                "{policy:?} reached the wrong strategy"
            );
        }

        // And no two of them reach the same one -- the property that actually
        // failed, which per-case equality alone would not catch if two
        // expectations were wrong together.
        let mapped: Vec<EvictionStrategy> = cases
            .iter()
            .map(|(p, _)| single_layer_strategy(p))
            .collect();
        for i in 0..mapped.len() {
            for j in (i + 1)..mapped.len() {
                assert_ne!(
                    mapped[i], mapped[j],
                    "{:?} and {:?} resolve to the same strategy",
                    cases[i].0, cases[j].0
                );
            }
        }
    }

    #[test]
    fn a_pinned_eviction_seed_reaches_every_layer_config_the_builder_makes() {
        assert_eq!(
            seeded_layer_config(Some(42)).eviction_seed,
            42,
            "the builder's eviction seed must reach the layer config"
        );
        assert_eq!(
            seeded_layer_config(None).eviction_seed,
            cache_core::DEFAULT_EVICTION_SEED,
            "an unset seed must leave the fixed default, not zero"
        );
        assert_eq!(SegCacheBuilder::new().eviction_seed, None);
        assert_eq!(
            SegCacheBuilder::new().eviction_seed(7).eviction_seed,
            Some(7)
        );
    }

    #[test]
    fn a_seeded_builder_still_builds_a_working_cache() {
        let cache = SegCacheBuilder::new()
            .heap_size(1024 * 1024)
            .segment_size(64 * 1024)
            .hashtable_power(10)
            .hashtable_seed([1, 2, 3, 4])
            .build()
            .expect("a seeded cache must build");

        cache.set(b"k", b"v", Duration::from_secs(60)).unwrap();
        assert_eq!(cache.get(b"k").as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn the_main_cache_reclaims_with_merge_unless_told_otherwise() {
        assert_eq!(
            SegCacheBuilder::new().main_eviction,
            EvictionStrategy::Merge(MergeConfig::default())
        );
    }

    #[test]
    fn a_chosen_main_eviction_strategy_reaches_the_layer_it_builds() {
        // A setter that dropped its argument would silently run both arms of
        // the merge-vs-CLOCK comparison as merge and report that the policies
        // agree.
        let builder = SegCacheBuilder::new().main_eviction(EvictionStrategy::Clock);

        assert_eq!(builder.main_eviction, EvictionStrategy::Clock);
    }

    #[test]
    fn a_clock_main_cache_still_builds_a_working_s3fifo() {
        // 64 KiB segments so layer 0 gets 12 of them. At 1 MiB segments this
        // heap gives layer 0 a single segment, where S3-FIFO wedges and every
        // write returns OutOfMemory -- the sub-3-segment cliff.
        let cache = SegCacheBuilder::new()
            .heap_size(8 * 1024 * 1024)
            .segment_size(64 * 1024)
            .hashtable_power(12)
            .s3fifo()
            .main_eviction(EvictionStrategy::Clock)
            .build()
            .expect("a clock-main s3fifo must build");

        cache.set(b"k", b"v", Duration::from_secs(60)).unwrap();
        assert_eq!(cache.get(b"k").as_deref(), Some(&b"v"[..]));
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
        // Need enough segments for eviction to work:
        // - min_free_segments = 3 reserved in free_queue
        // - At least 2 segments in a bucket for eviction to succeed
        // - So we need at least 5 segments (256KB / 32KB = 8 gives us headroom)
        let cache = SegCacheBuilder::new()
            .heap_size(256 * 1024) // 8 segments with 32KB each
            .segment_size(32 * 1024)
            .hashtable_power(8)
            .build()
            .expect("Failed to create cache");

        let ttl = Duration::from_secs(3600);
        let value = vec![b'x'; 1024];

        // Fill the cache with enough items to use multiple segments
        // Each segment holds ~31 items (32KB / 1KB)
        // We need at least 2 segments worth to enable eviction
        for i in 0..100 {
            let key = format!("key_{}", i);
            let _ = cache.set(key.as_bytes(), &value, ttl);
        }

        // Now eviction should work (bucket has 2+ segments)
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
        assert!(cache.contains(b"key"));

        // flush clears all items from the cache
        Cache::flush(&cache);

        // Item should no longer exist
        assert!(!cache.contains(b"key"));
    }

    /// A flushed cache must still accept writes.
    ///
    /// `flush()` resets the pools but historically left each layer's chain
    /// naming the segments it had just freed, so the next chain link failed and
    /// was reported as `OutOfMemory` with every segment free. `flush()` backs
    /// FLUSHALL, so this left a running server serving misses forever.
    #[test]
    fn test_cache_accepts_writes_after_flush() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);
        let value = vec![b'x'; 4096];

        // Fill past the point where eviction has to run, so the chain is deep.
        for i in 0..300 {
            let key = format!("pre{i}");
            cache
                .set(key.as_bytes(), &value, ttl)
                .expect("pre-flush set");
        }

        Cache::flush(&cache);

        // The same workload must succeed again. Failing here means the layer
        // kept organization state pointing at freed segments.
        for i in 0..300 {
            let key = format!("post{i}");
            cache
                .set(key.as_bytes(), &value, ttl)
                .unwrap_or_else(|e| panic!("set {i} after flush failed: {e:?}"));
        }

        assert!(cache.contains(b"post299"));
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
    fn internal_stats_reports_resident_items_within_inserted_bound() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        let num_items = 20;
        for i in 0..num_items {
            let key = format!("resident_{i}");
            cache.set(key.as_bytes(), b"v", ttl).unwrap();
        }

        let stats = cache
            .internal_stats()
            .expect("internal_stats should be Some for segcache");

        assert!(
            stats.resident_items > 0,
            "expected resident_items > 0 after inserting {num_items} items, got {}",
            stats.resident_items
        );
        assert!(
            stats.resident_items <= num_items as u64,
            "expected resident_items <= {num_items} inserted items, got {}",
            stats.resident_items
        );
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
