//! TieredCache - orchestrating cache with multiple layers.
//!
//! [`TieredCache`] manages a hierarchy of cache layers, providing:
//! - Unified write path (all writes go to Layer 0)
//! - Ghost-aware insertion (preserves frequency for re-inserted keys)
//! - Synchronous eviction when space is needed
//! - Layer-based read with proper frequency tracking

use crate::cache_trait::CacheInternalStats;
use crate::cas::CasToken;
use crate::config::LayerConfig;
use crate::disk::{DiskLayer, FilePool, IoUringDiskLayer, IoUringPool};
use crate::error::{CacheError, CacheResult};
use crate::hashtable::{Hashtable, KeyVerifier};
use crate::item::ItemGuard;
use crate::item_location::ItemLocation;
use crate::layer::{EvictResult, FifoLayer, Layer, TtlLayer};
use crate::location::Location;
use crate::memory_pool::MemoryPool;
use crate::pool::RamPool;
use crate::segment::{Segment, SegmentKeyVerify};
use crate::slice_segment::SliceSegment;
use crate::sync::{AtomicU64, Ordering};
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
    /// Disk-backed layer (for extended capacity beyond RAM, mmap-based).
    Disk(DiskLayer),
    /// io_uring disk-backed layer (for extended capacity beyond RAM).
    IoUringDisk(IoUringDiskLayer),
}

/// Dispatches a method call to the inner layer for all CacheLayer variants.
macro_rules! dispatch {
    ($self:expr, $method:ident ( $($arg:expr),* $(,)? )) => {
        match $self {
            CacheLayer::Fifo(layer) => layer.$method($($arg),*),
            CacheLayer::Ttl(layer) => layer.$method($($arg),*),
            CacheLayer::Disk(layer) => layer.$method($($arg),*),
            CacheLayer::IoUringDisk(layer) => layer.$method($($arg),*),
        }
    };
}

impl CacheLayer {
    /// Get the layer's configuration.
    pub fn config(&self) -> &LayerConfig {
        dispatch!(self, config())
    }

    /// Set this layer's demotion target.
    pub fn set_next_layer(&mut self, layer_id: crate::config::LayerId) {
        match self {
            CacheLayer::Fifo(l) => l.set_next_layer(layer_id),
            CacheLayer::Ttl(l) => l.set_next_layer(layer_id),
            CacheLayer::Disk(l) => l.set_next_layer(layer_id),
            CacheLayer::IoUringDisk(l) => l.set_next_layer(layer_id),
        }
    }

    /// Get the layer ID.
    pub fn layer_id(&self) -> u8 {
        dispatch!(self, layer_id())
    }

    /// Get the location layout of this layer's pool.
    ///
    /// A location naming this layer's pool must be decoded with it: pools with
    /// different segment sizes or alignment factors split the bits differently.
    pub fn layout(&self) -> &crate::location_layout::LocationLayout {
        match self {
            CacheLayer::Fifo(layer) => layer.pool().layout(),
            CacheLayer::Ttl(layer) => layer.pool().layout(),
            CacheLayer::Disk(layer) => layer.pool().layout(),
            CacheLayer::IoUringDisk(layer) => layer.pool().layout(),
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
        dispatch!(self, write_item(key, value, optional, ttl))
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
            CacheLayer::Disk(layer) => layer.get_item(location, key).map(|guard| f(&guard)),
            CacheLayer::IoUringDisk(layer) => layer.get_item(location, key).map(|guard| f(&guard)),
        }
    }

    /// Get value bytes from this layer (convenience method).
    pub fn get_value(&self, location: ItemLocation, key: &[u8]) -> Option<Vec<u8>> {
        self.with_item(location, key, |guard| guard.value().to_vec())
    }

    /// Get raw value reference pointers for zero-copy scatter-gather I/O.
    ///
    /// Returns the raw components needed to construct a `ValueRef`:
    /// - `ref_count_ptr`: Pointer to segment's ref_count (already incremented)
    /// - `value_ptr`: Pointer to value bytes in segment memory
    /// - `value_len`: Length of the value
    /// - `metadata_ptr`: Pointer to segment's packed metadata
    /// - `free_queue_ptr`: Pointer to pool's free queue
    /// - `segment_id`: Segment ID for free queue return
    ///
    /// Returns `None` if the item is not found or not accessible.
    /// Note: For disk layers, this returns pointers to mmap'd memory.
    pub fn get_value_ref_raw(
        &self,
        location: ItemLocation,
        key: &[u8],
    ) -> Option<crate::slice_segment::ValueRefRaw> {
        // Helper: shared logic for layers backed by SliceSegment pools.
        macro_rules! value_ref_from_pool {
            ($layer:expr) => {{
                let pool = $layer.pool();
                if location.pool_id() != pool.pool_id() {
                    return None;
                }
                let (_, segment_id, _, offset) = location.unpack(pool.layout());
                let segment = pool.get(segment_id)?;
                let state = segment.state();
                // Condemned: hashtable entries are gone, so this location is
                // stale and the answer is a miss (#127).
                if !state.is_readable() || state.is_condemned() {
                    return None;
                }
                segment.get_value_ref_raw(offset, key).ok()
            }};
        }

        match self {
            CacheLayer::Fifo(layer) => value_ref_from_pool!(layer),
            CacheLayer::Ttl(layer) => value_ref_from_pool!(layer),
            CacheLayer::Disk(layer) => value_ref_from_pool!(layer),
            // io_uring disk layer doesn't support direct value ref for committed
            // segments; use read_from_buffer instead.
            CacheLayer::IoUringDisk(_) => None,
        }
    }

    /// Mark an item as deleted.
    pub fn mark_deleted(&self, location: ItemLocation) {
        dispatch!(self, mark_deleted(location))
    }

    /// Mark an item as deleted and attempt segment compaction.
    ///
    /// This is like `mark_deleted` but additionally attempts to compact the
    /// segment with its predecessor when the deletion creates enough free space.
    pub fn mark_deleted_and_compact<H: Hashtable>(
        &self,
        location: ItemLocation,
        hashtable: &H,
    ) -> bool {
        dispatch!(self, mark_deleted_and_compact(location, hashtable))
    }

    /// Mark an item deleted and free its segment if that emptied it.
    pub fn mark_deleted_and_free_empty(&self, location: ItemLocation) {
        dispatch!(self, mark_deleted_and_free_empty(location))
    }

    /// Get the remaining TTL for an item.
    pub fn item_ttl(&self, location: ItemLocation) -> Option<Duration> {
        dispatch!(self, item_ttl(location))
    }

    /// Try to evict a segment from this layer.
    pub fn evict<H: Hashtable>(&self, hashtable: &H) -> bool {
        dispatch!(self, evict(hashtable))
    }

    /// Try to evict a segment with a demotion callback.
    ///
    /// For items that should be demoted, the callback is called with the item data
    /// to allow writing to a lower layer (e.g., disk).
    pub fn evict_with_demoter<H, F>(&self, hashtable: &H, demoter: F) -> bool
    where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, Location),
    {
        dispatch!(self, evict_with_demoter(hashtable, demoter))
    }

    /// Try to evict without blocking on ref_count.
    pub fn evict_nonblocking<H: Hashtable>(&self, hashtable: &H) -> EvictResult {
        match self {
            CacheLayer::Fifo(layer) => layer.evict_nonblocking(hashtable),
            CacheLayer::Ttl(layer) => layer.evict_nonblocking(hashtable),
            // Disk layers: fall back to regular evict (disk segments rarely have readers)
            CacheLayer::Disk(layer) => {
                if layer.evict(hashtable) {
                    EvictResult::Freed
                } else {
                    EvictResult::NoCandidate
                }
            }
            CacheLayer::IoUringDisk(layer) => {
                if layer.evict(hashtable) {
                    EvictResult::Freed
                } else {
                    EvictResult::NoCandidate
                }
            }
        }
    }

    /// Try to evict without blocking, with demotion callback.
    pub fn evict_nonblocking_with_demoter<H, F>(&self, hashtable: &H, demoter: F) -> EvictResult
    where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, Location),
    {
        match self {
            CacheLayer::Fifo(layer) => layer.evict_nonblocking_with_demoter(hashtable, demoter),
            CacheLayer::Ttl(layer) => layer.evict_nonblocking_with_demoter(hashtable, demoter),
            CacheLayer::Disk(layer) => {
                if layer.evict_with_demoter(hashtable, demoter) {
                    EvictResult::Freed
                } else {
                    EvictResult::NoCandidate
                }
            }
            CacheLayer::IoUringDisk(layer) => {
                if layer.evict_with_demoter(hashtable, demoter) {
                    EvictResult::Freed
                } else {
                    EvictResult::NoCandidate
                }
            }
        }
    }

    /// Emergency eviction: find any segment with ref_count == 0 to free.
    pub fn emergency_evict<H: Hashtable>(&self, hashtable: &H) -> bool {
        match self {
            CacheLayer::Fifo(layer) => layer.try_emergency_evict(hashtable),
            CacheLayer::Ttl(layer) => layer.try_emergency_evict(hashtable),
            // Disk layers don't support emergency eviction
            CacheLayer::Disk(_) | CacheLayer::IoUringDisk(_) => false,
        }
    }

    /// Try to expire segments in this layer.
    pub fn expire<H: Hashtable>(&self, hashtable: &H) -> usize {
        dispatch!(self, expire(hashtable))
    }

    /// Get the number of free segments.
    pub fn free_segment_count(&self) -> usize {
        dispatch!(self, free_segment_count())
    }

    /// Get the total number of segments.
    pub fn total_segment_count(&self) -> usize {
        dispatch!(self, total_segment_count())
    }

    /// Get the number of segments in use.
    pub fn used_segment_count(&self) -> usize {
        dispatch!(self, used_segment_count())
    }

    /// Get a segment from this layer's pool by segment ID (RAM layers only).
    ///
    /// For disk layers, use `disk_pool()` instead.
    /// Returns `None` for disk layers.
    pub fn get_segment(&self, segment_id: u32) -> Option<&SliceSegment<'static>> {
        match self {
            CacheLayer::Fifo(layer) => layer.pool().get(segment_id),
            CacheLayer::Ttl(layer) => layer.pool().get(segment_id),
            CacheLayer::Disk(_) | CacheLayer::IoUringDisk(_) => None,
        }
    }

    /// Get the memory pool for this layer (RAM layers only).
    ///
    /// # Panics
    ///
    /// Panics if called on a disk layer. Use `disk_pool()` instead.
    pub fn pool(&self) -> &MemoryPool {
        match self {
            CacheLayer::Fifo(layer) => layer.pool(),
            CacheLayer::Ttl(layer) => layer.pool(),
            CacheLayer::Disk(_) | CacheLayer::IoUringDisk(_) => {
                panic!("Cannot get MemoryPool from disk layer; use disk_pool()")
            }
        }
    }

    /// Get the disk pool for this layer (disk layers only).
    ///
    /// Returns `None` for RAM layers and IoUringDisk layers.
    pub fn disk_pool(&self) -> Option<&FilePool> {
        match self {
            CacheLayer::Fifo(_) | CacheLayer::Ttl(_) | CacheLayer::IoUringDisk(_) => None,
            CacheLayer::Disk(layer) => Some(layer.pool()),
        }
    }

    /// Get the io_uring disk pool for this layer.
    ///
    /// Returns `None` for RAM layers and mmap disk layers.
    pub fn io_uring_disk_pool(&self) -> Option<&IoUringPool> {
        match self {
            CacheLayer::IoUringDisk(layer) => Some(layer.pool()),
            _ => None,
        }
    }

    /// Check if this is a disk layer.
    pub fn is_disk(&self) -> bool {
        matches!(self, CacheLayer::Disk(_) | CacheLayer::IoUringDisk(_))
    }

    /// Get the pool ID for this layer.
    pub fn pool_id(&self) -> u8 {
        match self {
            CacheLayer::Fifo(layer) => layer.pool().pool_id(),
            CacheLayer::Ttl(layer) => layer.pool().pool_id(),
            CacheLayer::Disk(layer) => layer.pool().pool_id(),
            CacheLayer::IoUringDisk(layer) => layer.pool().pool_id(),
        }
    }

    /// Begin a two-phase write operation for zero-copy receive.
    pub fn begin_write_item(
        &self,
        key: &[u8],
        value_len: usize,
        optional: &[u8],
        ttl: Duration,
    ) -> CacheResult<(ItemLocation, *mut u8, u32)> {
        dispatch!(self, begin_write_item(key, value_len, optional, ttl))
    }

    /// Finalize a two-phase write operation.
    pub fn finalize_write_item(&self, location: ItemLocation, item_size: u32) {
        dispatch!(self, finalize_write_item(location, item_size))
    }

    /// Cancel a two-phase write operation.
    pub fn cancel_write_item(&self, location: ItemLocation) {
        dispatch!(self, cancel_write_item(location))
    }

    /// Reset this layer to its freshly built state.
    ///
    /// Backs [`TieredCache::flush`]. This is the whole layer, not just its
    /// pool: chains and bucket lists are cleared, cached write segments are
    /// dropped, any pending disk flush is discarded, and only then is every
    /// segment returned to the free queue.
    ///
    /// This was once `reset_all_segments`, which reset the pool alone. That
    /// left each layer's organization state naming segments the pool had just
    /// recycled, so the next write's chain link failed -- surfacing as
    /// `OutOfMemory` with every segment free. Anything added here must reset
    /// the layer completely; a partial reset reproduces that bug.
    ///
    /// # Preconditions
    ///
    /// Only valid when no concurrent operation is touching the layer, and only
    /// after the hashtable has been cleared.
    pub fn reset(&self) {
        match self {
            CacheLayer::Fifo(layer) => layer.reset(),
            CacheLayer::Ttl(layer) => layer.reset(),
            CacheLayer::Disk(layer) => layer.reset(),
            CacheLayer::IoUringDisk(layer) => layer.reset(),
        }
    }
}

/// Atomic counters for cache-internal events (demotions, evictions).
pub struct CacheStats {
    /// Items demoted from one layer to another.
    pub demotions: AtomicU64,
    /// Segments evicted entirely (items discarded).
    pub evictions: AtomicU64,
    /// Items that failed to demote (staging pool exhausted, discarded instead).
    pub demotion_failures: AtomicU64,
    /// Segments reclaimed by expiry rather than eviction.
    ///
    /// The distinction the whole TTL-bucket design exists for: an expired
    /// segment is reclaimed whole, costing nothing and destroying nothing
    /// live, while an eviction pass copies survivors and discards the rest.
    /// A cache doing the first needs less of the second, so reporting only
    /// evictions makes a working expiry look like good luck.
    pub expirations: AtomicU64,

    /// Compaction passes that actually ran.
    ///
    /// `try_compact_segment` declines far more often than it fires: it
    /// needs a sealed predecessor whose combined live set fits in 90% of
    /// one segment. A cache can therefore be configured for compaction,
    /// report nothing unusual, and never compact once -- which is
    /// indistinguishable from compaction running and not helping unless
    /// this is counted.
    pub compactions: AtomicU64,
    /// Duration of eviction passes. See [`CacheInternalStats::eviction_latency`].
    pub eviction_latency: crate::latency::LatencyHistogram,
}

impl CacheStats {
    /// Create a new zeroed stats instance.
    pub fn new() -> Self {
        Self {
            demotions: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            demotion_failures: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
            expirations: AtomicU64::new(0),
            eviction_latency: crate::latency::LatencyHistogram::new(),
        }
    }

    /// Snapshot the current values as a `CacheInternalStats`.
    ///
    /// `CacheStats` holds only atomic counters and has no access to the
    /// layers, so it cannot report `resident_items` (a gauge over live
    /// segments, not a counter) -- callers with layer access (e.g.
    /// `SegCache::internal_stats`) must fill that field in themselves via
    /// `TieredCache::resident_items()`.
    pub fn snapshot(&self) -> CacheInternalStats {
        CacheInternalStats {
            demotions: self.demotions.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            demotion_failures: self.demotion_failures.load(Ordering::Relaxed),
            compactions: self.compactions.load(Ordering::Relaxed),
            expirations: self.expirations.load(Ordering::Relaxed),
            eviction_latency: self.eviction_latency.snapshot(),
            ..Default::default()
        }
    }
}

impl Default for CacheStats {
    fn default() -> Self {
        Self::new()
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

    /// What an overwrite does with the superseded copy's bytes.
    overwrite_reclaim: crate::config::OverwriteReclaim,

    /// Atomic counters for demotion and eviction events.
    stats: CacheStats,
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

    /// Get the cache stats (demotions, evictions).
    pub fn stats(&self) -> &CacheStats {
        &self.stats
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

    /// Sum live item counts across every segment of every layer.
    ///
    /// Segment IDs within a layer's pool are 0-based (see
    /// `MemoryPool::get`, which indexes `segments` directly by `id as
    /// usize`), so this iterates `0..total_segment_count()`. Disk layers
    /// have no addressable `SliceSegment` (`CacheLayer::get_segment` always
    /// returns `None` for them), so they contribute nothing here and this
    /// counts RAM-resident items only.
    ///
    /// This is a gauge, not a counter: it is read without pinning against
    /// concurrent writers/evictors, so a segment can be double- or
    /// under-counted mid-transition. See [`CacheInternalStats::resident_items`]
    /// for why an approximate figure is still worth reporting.
    pub fn resident_items(&self) -> u64 {
        let mut total = 0u64;
        for layer in &self.layers {
            for segment_id in 0..layer.total_segment_count() as u32 {
                if let Some(segment) = layer.get_segment(segment_id)
                    && !Self::is_freed(segment)
                {
                    total += segment.live_items() as u64;
                }
            }
        }
        total
    }

    /// Whether a segment is sitting in the free queue.
    ///
    /// A freed segment keeps its `live_items` and `live_bytes` until it is
    /// reserved again -- `try_reserve` zeroes them, `try_release` does not --
    /// so a walk over every addressable segment counts contents that were
    /// correctly evicted. About two segments' worth at any moment, which is
    /// 1.6% of residency on a 128-segment cache and 25.6% on a 16-segment
    /// one.
    ///
    /// Filtered here rather than zeroed at release, because zeroing after
    /// the release CAS races a concurrent `try_reserve` that has already
    /// reset the counters and begun appending, and zeroing before it would
    /// clear a segment whose CAS then fails. A free segment holds nothing
    /// by definition, so declining to count it is correct without any new
    /// ordering requirement.
    fn is_freed(segment: &SliceSegment<'_>) -> bool {
        segment.state() == crate::state::State::Free
    }

    /// Live item bytes and the segment bytes they sit in, across RAM layers.
    ///
    /// Returns `(live_bytes, written_bytes, capacity_bytes)` over RAM layers.
    ///
    /// Three numbers rather than one, because "why does this engine hold
    /// fewer items in the same heap" has two different answers that a single
    /// ratio cannot separate:
    ///
    /// - `written / capacity` is how full the segments are. Low means the
    ///   bytes are sitting there unused -- a packing problem.
    /// - `live / written` is how much of what was written is still live.
    ///   Low means the segments are full of superseded or expired items that
    ///   nothing has reclaimed -- a reclamation problem.
    ///
    /// Dividing heap size by resident items conflates those with a third
    /// possibility, that the policy simply retained fewer items on purpose,
    /// and the three have different fixes.
    ///
    /// A gauge, read without pinning exactly as `resident_items` is, so the
    /// same caveat about mid-transition segments applies.
    pub fn resident_bytes(&self) -> (u64, u64, u64) {
        let mut live = 0u64;
        let mut written = 0u64;
        let mut capacity = 0u64;
        for layer in &self.layers {
            for segment_id in 0..layer.total_segment_count() as u32 {
                if let Some(segment) = layer.get_segment(segment_id)
                    && !Self::is_freed(segment)
                {
                    live += segment.live_bytes() as u64;
                    written += segment.write_offset() as u64;
                    capacity += segment.capacity() as u64;
                }
            }
        }
        (live, written, capacity)
    }

    /// Per-segment live occupancy, as counts in ten deciles.
    ///
    /// Bucket `i` counts non-free RAM segments whose `live_bytes` falls in
    /// `[i*10%, (i+1)*10%)` of segment capacity; a completely full segment
    /// lands in bucket 9.
    ///
    /// The cache-wide `live / capacity` ratio cannot answer whether
    /// compaction is reachable, because compaction is a decision about
    /// *pairs* of adjacent segments, not about the mean.
    /// [`TtlLayer::try_compact_segment`] merges two sealed segments into one
    /// spare only when their combined live bytes fit in 90% of a single
    /// segment -- an average occupancy of 45% across the pair. A cache
    /// sitting at 76% live overall can still hold a compactable tail, or
    /// none at all, and the mean does not distinguish those.
    ///
    /// `live_bytes` is charged the full `item_stride`, so each count already
    /// includes header and alignment overhead rather than just payload.
    ///
    /// A gauge, read without pinning exactly as [`resident_bytes`] is.
    ///
    /// [`resident_bytes`]: Self::resident_bytes
    pub fn segment_occupancy(&self) -> [u64; 10] {
        let mut deciles = [0u64; 10];
        for layer in &self.layers {
            for segment_id in 0..layer.total_segment_count() as u32 {
                if let Some(segment) = layer.get_segment(segment_id)
                    && !Self::is_freed(segment)
                {
                    let capacity = segment.capacity();
                    if capacity == 0 {
                        continue;
                    }
                    let live = segment.live_bytes() as usize;
                    let decile = (live * 10 / capacity).min(9);
                    deciles[decile] += 1;
                }
            }
        }
        deciles
    }

    /// Sum of free segments across RAM layers only.
    ///
    /// Skips disk-backed layers the same way `resident_items` does (see its
    /// doc comment): a disk tier's fill dynamic is a different question from
    /// "did the in-memory cache reach capacity", which is what
    /// [`crate::cache_trait::CacheInternalStats::free_segments`] and
    /// [`total_segments`](crate::cache_trait::CacheInternalStats::total_segments)
    /// exist to answer.
    pub fn ram_free_segment_count(&self) -> u64 {
        self.layers
            .iter()
            .filter(|layer| !layer.is_disk())
            .map(|layer| layer.free_segment_count() as u64)
            .sum()
    }

    /// Sum of total segments across RAM layers only. See
    /// [`ram_free_segment_count`](Self::ram_free_segment_count) for why disk
    /// layers are excluded.
    pub fn ram_total_segment_count(&self) -> u64 {
        self.layers
            .iter()
            .filter(|layer| !layer.is_disk())
            .map(|layer| layer.total_segment_count() as u64)
            .sum()
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
                self.supersede_at(old_location);
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

    /// Begin a two-phase SET for zero-copy receive.
    ///
    /// Reserves space in segment memory and returns a `SegmentReservation`
    /// with a mutable pointer to the value area. The caller writes the value
    /// directly to segment memory, then calls `commit_segment_set` to finalize.
    ///
    /// # Zero-Copy Receive Flow
    ///
    /// ```ignore
    /// // 1. Reserve segment space
    /// let mut reservation = cache.begin_segment_set(key, value_len, ttl)?;
    ///
    /// // 2. Receive value directly into segment memory
    /// socket.recv_exact(reservation.value_mut())?;
    ///
    /// // 3. Commit to finalize and update hashtable
    /// cache.commit_segment_set(reservation)?;
    /// ```
    ///
    /// # Cancellation
    ///
    /// If the reservation is dropped without committing (e.g., connection
    /// closed during receive), the reserved space is marked as deleted.
    pub fn begin_segment_set(
        &self,
        key: &[u8],
        value_len: usize,
        ttl: Duration,
    ) -> CacheResult<crate::SegmentReservation> {
        // Ensure we have space in Layer 0
        self.ensure_space()?;

        // Reserve space in Layer 0
        let layer = self.layers.first().ok_or(CacheError::OutOfMemory)?;
        let (location, value_ptr, item_size) = layer.begin_write_item(key, value_len, &[], ttl)?;

        // Create the reservation
        // SAFETY: value_ptr points to valid segment memory that will remain
        // valid until the reservation is committed or cancelled
        Ok(unsafe {
            crate::SegmentReservation::new(
                location,
                value_ptr,
                value_len,
                key.to_vec(),
                ttl,
                item_size,
            )
        })
    }

    /// Commit a two-phase SET operation.
    ///
    /// Finalizes the segment write and inserts the item into the hashtable.
    /// The reservation is consumed.
    pub fn commit_segment_set(
        &self,
        mut reservation: crate::SegmentReservation,
    ) -> CacheResult<()> {
        let location = reservation.location();
        let item_size = reservation.item_size();

        // Finalize the segment write
        let layer = self.layers.first().ok_or(CacheError::OutOfMemory)?;
        layer.finalize_write_item(location, item_size);

        // Create key verifier for hashtable operations
        let verifier = self.create_key_verifier();

        // Insert into hashtable (preserves ghost frequency if present)
        match self
            .hashtable
            .insert(reservation.key(), location.to_location(), &verifier)
        {
            Ok(Some(old_location)) => {
                // Key existed, mark old location as deleted
                self.supersede_at(old_location);
            }
            Ok(None) => {
                // New key or ghost resurrection
            }
            Err(e) => {
                // Hashtable full - mark item as deleted
                layer.cancel_write_item(location);
                return Err(e);
            }
        }

        reservation.mark_committed();
        Ok(())
    }

    /// Cancel a two-phase SET operation.
    ///
    /// Marks the reserved space as deleted. Called when a receive operation
    /// fails (e.g., connection closed during value receive).
    pub fn cancel_segment_set(&self, reservation: crate::SegmentReservation) {
        if reservation.is_committed() {
            return;
        }

        if let Some(layer) = self.layers.first() {
            layer.cancel_write_item(reservation.location());
        }
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
                self.supersede_at(old_location);
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

    /// Get a zero-copy reference to a cached value.
    ///
    /// Returns a [`crate::ValueRef`] that holds a reference to the value directly
    /// in cache memory. The segment's ref_count is incremented to prevent
    /// eviction while the reference is held.
    ///
    /// This is the most efficient way to read values for scatter-gather I/O.
    pub fn get_value_ref(&self, key: &[u8]) -> Option<crate::cache_trait::ValueRef> {
        let verifier = self.create_key_verifier();

        // Lookup in hashtable
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;
        let item_loc = ItemLocation::from_location(location);

        // Find the layer containing this item
        let layer_idx = self.layer_for_pool(item_loc.pool_id())?;
        let layer = self.layers.get(layer_idx)?;

        // Get raw pointers from layer
        let (ref_count_ptr, value_ptr, value_len, metadata_ptr, free_queue_ptr, segment_id) =
            layer.get_value_ref_raw(item_loc, key)?;

        // Safety: The layer's get_value_ref_raw has incremented ref_count and
        // validated that the pointers are valid. ValueRef will decrement ref_count on drop.
        Some(unsafe {
            crate::cache_trait::ValueRef::new(
                ref_count_ptr,
                value_ptr,
                value_len,
                metadata_ptr,
                free_queue_ptr,
                segment_id,
            )
        })
    }

    /// Look up a key, returning either an immediate hit or disk read params.
    ///
    /// For items in RAM (or in a disk segment's write buffer), returns
    /// [`crate::cache_trait::LookupResult::Hit`] with a zero-copy `ValueRef`.
    /// For items on committed disk segments, returns
    /// [`crate::cache_trait::LookupResult::DiskRead`] with parameters for
    /// submitting an io_uring read.
    /// Returns [`crate::cache_trait::LookupResult::Miss`] if not found.
    pub fn lookup(&self, key: &[u8]) -> crate::cache_trait::LookupResult {
        use crate::cache_trait::LookupResult;

        let verifier = self.create_key_verifier();

        // Lookup in hashtable
        let Some((location, _freq)) = self.hashtable.lookup(key, &verifier) else {
            return LookupResult::Miss;
        };
        let item_loc = ItemLocation::from_location(location);

        // Find the layer containing this item
        let Some(layer_idx) = self.layer_for_pool(item_loc.pool_id()) else {
            return LookupResult::Miss;
        };
        let Some(layer) = self.layers.get(layer_idx) else {
            return LookupResult::Miss;
        };

        match layer {
            CacheLayer::IoUringDisk(disk_layer) => {
                // Try synchronous read from write buffer first
                if let Some((
                    ref_count_ptr,
                    value_ptr,
                    value_len,
                    metadata_ptr,
                    free_queue_ptr,
                    segment_id,
                )) = disk_layer.read_from_buffer(item_loc, key)
                {
                    let vr = unsafe {
                        crate::cache_trait::ValueRef::new(
                            ref_count_ptr,
                            value_ptr,
                            value_len,
                            metadata_ptr,
                            free_queue_ptr,
                            segment_id,
                        )
                    };
                    return LookupResult::Hit(vr);
                }
                // Need async disk read
                match disk_layer.prepare_read(item_loc, disk_layer.pool().block_size()) {
                    Some(params) => LookupResult::DiskRead(params),
                    None => LookupResult::Miss,
                }
            }
            _ => {
                // RAM layers (and mmap disk): existing zero-copy path
                match layer.get_value_ref_raw(item_loc, key) {
                    Some((
                        ref_count_ptr,
                        value_ptr,
                        value_len,
                        metadata_ptr,
                        free_queue_ptr,
                        segment_id,
                    )) => {
                        let vr = unsafe {
                            crate::cache_trait::ValueRef::new(
                                ref_count_ptr,
                                value_ptr,
                                value_len,
                                metadata_ptr,
                                free_queue_ptr,
                                segment_id,
                            )
                        };
                        LookupResult::Hit(vr)
                    }
                    None => LookupResult::Miss,
                }
            }
        }
    }

    /// Get an item from the cache with a CAS token.
    ///
    /// Returns the value as a `Vec<u8>` along with a CAS token that can be
    /// used for subsequent CAS operations. The token combines the item's
    /// location with the segment's generation counter to detect modifications.
    ///
    /// This is used for memcached GETS command.
    pub fn get_with_cas(&self, key: &[u8]) -> Option<(Vec<u8>, CasToken)> {
        let verifier = self.create_key_verifier();

        // Lookup in hashtable
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;
        let item_loc = ItemLocation::from_location(location);

        // Find the layer containing this item
        let layer_idx = self.layer_for_pool(item_loc.pool_id())?;
        let layer = self.layers.get(layer_idx)?;

        // Get the segment to retrieve its generation
        let segment = layer.get_segment(item_loc.segment_id(layer.layout()))?;
        let generation = segment.generation();

        // Get value from layer
        let value = layer.get_value(item_loc, key)?;

        // Build CAS token from location + generation
        let cas_token = CasToken::new(location, generation);

        Some((value, cas_token))
    }

    /// Get an item with CAS token via callback (zero-copy).
    ///
    /// Calls the provided function with access to the value bytes,
    /// returning the result along with the CAS token.
    ///
    /// This is more efficient than `get_with_cas` when you only need
    /// to read or copy the value to another buffer.
    pub fn with_value_cas<F, R>(&self, key: &[u8], f: F) -> Option<(R, CasToken)>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let verifier = self.create_key_verifier();

        // Lookup in hashtable
        let (location, _freq) = self.hashtable.lookup(key, &verifier)?;
        let item_loc = ItemLocation::from_location(location);

        // Find the layer containing this item
        let layer_idx = self.layer_for_pool(item_loc.pool_id())?;
        let layer = self.layers.get(layer_idx)?;

        // Get the segment to retrieve its generation
        let segment = layer.get_segment(item_loc.segment_id(layer.layout()))?;
        let generation = segment.generation();

        // Call function with value
        let result = layer.with_item(item_loc, key, |guard| f(guard.value()))?;

        // Build CAS token from location + generation
        let cas_token = CasToken::new(location, generation);

        Some((result, cas_token))
    }

    /// Compare-and-swap: update an item only if the CAS token matches.
    ///
    /// This implements memcached CAS semantics:
    /// - If the key doesn't exist, returns `Err(CacheError::KeyNotFound)`
    /// - If the CAS token doesn't match (item was modified), returns `Ok(false)`
    /// - If the CAS token matches, updates the item and returns `Ok(true)`
    ///
    /// # Arguments
    /// * `key` - The key to update
    /// * `value` - The new value
    /// * `optional` - Optional metadata (e.g., flags)
    /// * `ttl` - Time-to-live for the new item
    /// * `cas_token` - The CAS token from a previous GETS operation
    pub fn cas(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        ttl: Duration,
        cas_token: CasToken,
    ) -> CacheResult<bool> {
        let verifier = self.create_key_verifier();

        // Lookup current item
        let Some((current_location, _freq)) = self.hashtable.lookup(key, &verifier) else {
            return Err(CacheError::KeyNotFound);
        };

        let item_loc = ItemLocation::from_location(current_location);

        // Find the layer containing this item
        let layer_idx = self
            .layer_for_pool(item_loc.pool_id())
            .ok_or(CacheError::KeyNotFound)?;
        let layer = self.layers.get(layer_idx).ok_or(CacheError::KeyNotFound)?;

        // Get the segment to check generation
        let segment = layer
            .get_segment(item_loc.segment_id(layer.layout()))
            .ok_or(CacheError::KeyNotFound)?;
        let current_generation = segment.generation();

        // Build current CAS token and compare
        let current_cas = CasToken::new(current_location, current_generation);
        if current_cas != cas_token {
            // CAS mismatch - item was modified
            return Ok(false);
        }

        // CAS matches - perform the update (same as replace but we've already verified)
        self.ensure_space()?;

        // Write to Layer 0
        let write_layer = self.layers.first().ok_or(CacheError::OutOfMemory)?;
        let new_location = write_layer.write_item(key, value, optional, ttl)?;

        // Publish by swapping the slot only if it still holds the location we
        // checked the token against — this is the linearization point. A
        // plain replace (update_if_present) would overwrite whatever entry is
        // current, silently losing a write that raced in between the token
        // check and the publish.
        if self
            .hashtable
            .cas_location(key, current_location, new_location.to_location(), true)
        {
            self.supersede_at(current_location);
            return Ok(true);
        }

        // `cas_location` retries the slot itself while it keeps publishing
        // `current_location`, so a failure here means the entry genuinely
        // stopped being the one whose token we checked — somebody else
        // published over it. This used to be a retry loop guarding against
        // spurious failures from a concurrent reader's frequency bump, which
        // `try_cas_in_bucket` now absorbs.
        write_layer.mark_deleted(new_location);
        Ok(false)
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

        // Mark as deleted in the layer and try to compact
        self.mark_deleted_at_with_compact(location);

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
        self.stats
            .expirations
            .fetch_add(total as u64, Ordering::Relaxed);
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

    /// Atomically increment a numeric value stored as ASCII decimal.
    ///
    /// Reads the current value with its CAS token and publishes the new
    /// value via [`Self::cas`], retrying if a concurrent write lands in
    /// between — concurrent increments cannot lose updates. Each retry
    /// re-reads the value (and counts as a hit for frequency purposes).
    ///
    /// If the key doesn't exist and `initial` is provided, creates the key
    /// with `initial + delta` (insert-if-absent; a concurrent creation
    /// triggers a retry). If `initial` is `None` and the key doesn't exist,
    /// returns `Err(CacheError::KeyNotFound)`.
    ///
    /// # Returns
    /// * `Ok(new_value)` - The value after incrementing
    /// * `Err(CacheError::KeyNotFound)` - Key doesn't exist and no initial provided
    /// * `Err(CacheError::NotNumeric)` - Value exists but isn't a valid ASCII number
    /// * `Err(CacheError::Overflow)` - Operation would overflow u64
    pub fn increment(
        &self,
        key: &[u8],
        delta: u64,
        initial: Option<u64>,
        ttl: Duration,
    ) -> CacheResult<u64> {
        loop {
            // Read the current value and its CAS token in one step,
            // parsing as ASCII decimal under the item guard.
            let current = self.with_value_cas(key, |v| {
                std::str::from_utf8(v)
                    .ok()
                    .and_then(|s| s.trim().parse::<u64>().ok())
            });

            match current {
                Some((Some(current), token)) => {
                    let new_value = current.checked_add(delta).ok_or(CacheError::Overflow)?;
                    match self.cas(key, new_value.to_string().as_bytes(), &[], ttl, token) {
                        Ok(true) => return Ok(new_value),
                        // Raced with another writer, or the key was deleted
                        // in between; re-read and retry.
                        Ok(false) | Err(CacheError::KeyNotFound) => continue,
                        Err(e) => return Err(e),
                    }
                }
                Some((None, _)) => return Err(CacheError::NotNumeric),
                None => {
                    // with_value_cas returns None both when the key is absent
                    // and when the item is transiently unreadable (e.g. its
                    // segment is mid-migration). Only treat the key as absent
                    // if the hashtable agrees; otherwise retry.
                    let verifier = self.create_key_verifier();
                    if self.hashtable.contains(key, &verifier) {
                        continue;
                    }
                    match initial {
                        Some(init) => {
                            let new_value = init.checked_add(delta).ok_or(CacheError::Overflow)?;
                            match self.add(key, new_value.to_string().as_bytes(), &[], ttl) {
                                Ok(()) => return Ok(new_value),
                                // Lost the creation race; re-read and retry.
                                Err(CacheError::KeyExists) => continue,
                                Err(e) => return Err(e),
                            }
                        }
                        None => return Err(CacheError::KeyNotFound),
                    }
                }
            }
        }
    }

    /// Atomically decrement a numeric value stored as ASCII decimal.
    ///
    /// Reads the current value with its CAS token and publishes the new
    /// value via [`Self::cas`], retrying if a concurrent write lands in
    /// between — concurrent decrements cannot lose updates. Each retry
    /// re-reads the value (and counts as a hit for frequency purposes).
    ///
    /// If the key doesn't exist and `initial` is provided, creates the key
    /// with `initial.saturating_sub(delta)` (insert-if-absent; a concurrent
    /// creation triggers a retry). If `initial` is `None` and the key
    /// doesn't exist, returns `Err(CacheError::KeyNotFound)`.
    ///
    /// Underflow clamps to 0 (saturating subtraction) per memcache semantics.
    ///
    /// # Returns
    /// * `Ok(new_value)` - The value after decrementing (clamped to 0 on underflow)
    /// * `Err(CacheError::KeyNotFound)` - Key doesn't exist and no initial provided
    /// * `Err(CacheError::NotNumeric)` - Value exists but isn't a valid ASCII number
    pub fn decrement(
        &self,
        key: &[u8],
        delta: u64,
        initial: Option<u64>,
        ttl: Duration,
    ) -> CacheResult<u64> {
        loop {
            // Read the current value and its CAS token in one step,
            // parsing as ASCII decimal under the item guard.
            let current = self.with_value_cas(key, |v| {
                std::str::from_utf8(v)
                    .ok()
                    .and_then(|s| s.trim().parse::<u64>().ok())
            });

            match current {
                Some((Some(current), token)) => {
                    // Saturating subtraction (clamp to 0)
                    let new_value = current.saturating_sub(delta);
                    match self.cas(key, new_value.to_string().as_bytes(), &[], ttl, token) {
                        Ok(true) => return Ok(new_value),
                        // Raced with another writer, or the key was deleted
                        // in between; re-read and retry.
                        Ok(false) | Err(CacheError::KeyNotFound) => continue,
                        Err(e) => return Err(e),
                    }
                }
                Some((None, _)) => return Err(CacheError::NotNumeric),
                None => {
                    // with_value_cas returns None both when the key is absent
                    // and when the item is transiently unreadable (e.g. its
                    // segment is mid-migration). Only treat the key as absent
                    // if the hashtable agrees; otherwise retry.
                    let verifier = self.create_key_verifier();
                    if self.hashtable.contains(key, &verifier) {
                        continue;
                    }
                    match initial {
                        Some(init) => {
                            let new_value = init.saturating_sub(delta);
                            match self.add(key, new_value.to_string().as_bytes(), &[], ttl) {
                                Ok(()) => return Ok(new_value),
                                // Lost the creation race; re-read and retry.
                                Err(CacheError::KeyExists) => continue,
                                Err(e) => return Err(e),
                            }
                        }
                        None => return Err(CacheError::KeyNotFound),
                    }
                }
            }
        }
    }

    /// Append data to an existing value.
    ///
    /// Concatenates `data` to the end of the existing value for `key`.
    /// If the key doesn't exist, returns `Err(CacheError::KeyNotFound)`.
    ///
    /// # Returns
    /// * `Ok(new_length)` - The length of the value after appending
    /// * `Err(CacheError::KeyNotFound)` - Key doesn't exist
    pub fn append(&self, key: &[u8], data: &[u8]) -> CacheResult<usize> {
        // Get the current value and TTL
        let verifier = self.create_key_verifier();

        let (location, _freq) = self
            .hashtable
            .lookup(key, &verifier)
            .ok_or(CacheError::KeyNotFound)?;
        let item_loc = ItemLocation::from_location(location);

        // Find the layer containing this item
        let layer_idx = self
            .layer_for_pool(item_loc.pool_id())
            .ok_or(CacheError::KeyNotFound)?;
        let layer = self.layers.get(layer_idx).ok_or(CacheError::KeyNotFound)?;

        // Get current value and remaining TTL
        let (current_value, ttl) = layer
            .with_item(item_loc, key, |guard| {
                let value = guard.value().to_vec();
                let remaining_ttl = layer
                    .item_ttl(item_loc)
                    .unwrap_or(Duration::from_secs(3600));
                (value, remaining_ttl)
            })
            .ok_or(CacheError::KeyNotFound)?;

        // Create new value with appended data
        let mut new_value = current_value;
        new_value.extend_from_slice(data);
        let new_len = new_value.len();

        // Store the new value, preserving TTL
        self.set(key, &new_value, &[], ttl)?;

        Ok(new_len)
    }

    /// Prepend data to an existing value.
    ///
    /// Concatenates `data` to the beginning of the existing value for `key`.
    /// If the key doesn't exist, returns `Err(CacheError::KeyNotFound)`.
    ///
    /// # Returns
    /// * `Ok(new_length)` - The length of the value after prepending
    /// * `Err(CacheError::KeyNotFound)` - Key doesn't exist
    pub fn prepend(&self, key: &[u8], data: &[u8]) -> CacheResult<usize> {
        // Get the current value and TTL
        let verifier = self.create_key_verifier();

        let (location, _freq) = self
            .hashtable
            .lookup(key, &verifier)
            .ok_or(CacheError::KeyNotFound)?;
        let item_loc = ItemLocation::from_location(location);

        // Find the layer containing this item
        let layer_idx = self
            .layer_for_pool(item_loc.pool_id())
            .ok_or(CacheError::KeyNotFound)?;
        let layer = self.layers.get(layer_idx).ok_or(CacheError::KeyNotFound)?;

        // Get current value and remaining TTL
        let (current_value, ttl) = layer
            .with_item(item_loc, key, |guard| {
                let value = guard.value().to_vec();
                let remaining_ttl = layer
                    .item_ttl(item_loc)
                    .unwrap_or(Duration::from_secs(3600));
                (value, remaining_ttl)
            })
            .ok_or(CacheError::KeyNotFound)?;

        // Create new value with prepended data
        let mut new_value = data.to_vec();
        new_value.extend_from_slice(&current_value);
        let new_len = new_value.len();

        // Store the new value, preserving TTL
        self.set(key, &new_value, &[], ttl)?;

        Ok(new_len)
    }

    /// Ensure Layer 0 has space for a new item.
    ///
    /// Uses cascading eviction: before evicting from a layer that demotes to a
    /// downstream layer, ensure the downstream layer has free space first.
    /// This works bottom-up: disk → Layer 1 → Layer 0.
    fn ensure_space(&self) -> CacheResult<()> {
        let layer = self.layers.first().ok_or(CacheError::OutOfMemory)?;

        // Check if we need to evict
        if layer.free_segment_count() > self.eviction_threshold {
            return Ok(());
        }

        // Find disk layer index
        let disk_layer_idx = self.layers.iter().position(|l| l.is_disk());

        // Timed from here, after the free-segment early return above, so the
        // histogram holds eviction passes rather than every `set`. Timing the
        // early return too would bury a rare multi-millisecond stall under
        // millions of ~0 ns samples.
        //
        // `std::time::Instant`, deliberately, and NOT `clocksource::coarse`
        // which this crate uses for every TTL: coarse is 1-second resolution,
        // so a sub-millisecond pass would measure as 0 or as exactly one
        // second -- a plausible-looking number that is pure artifact.
        let started = std::time::Instant::now();

        // Expiry first, because it is free and eviction is not.
        //
        // An expired segment is reclaimed whole: nothing is copied and
        // nothing live is discarded. An eviction pass copies the survivors
        // of a chain and throws the rest away. Reaching for the second
        // while the first would have sufficed destroys live data to make
        // room that dead data was already holding -- which is precisely the
        // advantage TTL-bucketed segments exist to provide, and it was
        // unreachable before this: `expire()` is a public method nothing
        // called internally, so unless pressure triggers it, proactive
        // expiration never runs at all. Both engines measured in this
        // program had it switched off for that reason.
        //
        // Cheap to attempt: `try_expire_segments` walks bucket heads and
        // stops at the first unexpired one, so on a cache with nothing
        // expired it costs a comparison per bucket and no segment work.
        if self.expire() > 0 && layer.free_segment_count() > self.eviction_threshold {
            self.stats
                .eviction_latency
                .record(started.elapsed().as_nanos() as u64);
            return Ok(());
        }

        // Try to evict until we have enough space
        for _ in 0..self.max_eviction_attempts {
            // Cascading: ensure downstream layers have space (bottom-up)
            // 1. Evict from disk if full (frees disk segments)
            if let Some(disk_idx) = disk_layer_idx
                && let Some(disk) = self.layers.get(disk_idx)
                && disk.free_segment_count() == 0
            {
                self.evict_from_layer(disk_idx);
            }

            // 2. Evict from Layer 1 if full, so layer 0's demotions have
            //    somewhere to land.
            //
            //    This was `len() > 2`, which asked whether layer 1 has a disk
            //    tier to demote *into*. That is the wrong question: the reason
            //    to reclaim layer 1 is that layer 0 demotes into *it*, which is
            //    true in every tiered topology. Gated on a disk tier, the
            //    two-layer S3-FIFO default never reclaimed its main cache at
            //    all -- layer 1 filled once and froze, `write_item` failed for
            //    every subsequent promotion, and the demoter discarded each one
            //    (80-92% of them under pressure). Nothing failed loudly: the
            //    admission filter kept selecting hot items and the cache kept
            //    throwing them away.
            //
            //    A terminal layer 1 is fine here -- `evict_from_layer` falls
            //    through to its discard path when there is no next layer.
            if self.layers.len() > 1
                && let Some(layer1) = self.layers.get(1)
                && layer1.free_segment_count() == 0
            {
                self.evict_from_layer(1);
            }

            // 3. Evict from Layer 0 (demotes to Layer 1)
            let evicted = self.evict_from_layer(0);

            if !evicted {
                // Layer 0 eviction failed, try Layer 1 directly
                if self.layers.len() > 1 {
                    self.evict_from_layer(1);
                }
            }

            if layer.free_segment_count() > self.eviction_threshold {
                self.stats
                    .eviction_latency
                    .record(started.elapsed().as_nanos() as u64);
                return Ok(());
            }
        }

        // Still no space after max attempts. Timed too: a pass that ran the
        // full attempt budget and failed is the longest stall a `set` can
        // absorb, and excluding it would understate the tail precisely where
        // it is worst.
        self.stats
            .eviction_latency
            .record(started.elapsed().as_nanos() as u64);
        Err(CacheError::OutOfMemory)
    }

    /// Evict from a specific layer, demoting items to the layer's configured `next_layer`.
    ///
    /// Uses non-blocking eviction: if the policy-selected segment has active readers,
    /// it is deferred (AwaitingRelease) and an emergency eviction of a different
    /// ref_count==0 segment is attempted instead.
    fn evict_from_layer(&self, layer_idx: usize) -> bool {
        let layer = match self.layers.get(layer_idx) {
            Some(l) => l,
            None => return false,
        };

        // Use the layer's configured next_layer as demotion target
        if let Some(next_idx) = layer.config().next_layer
            && let Some(target_layer) = self.layers.get(next_idx as usize)
        {
            let hashtable = self.hashtable.as_ref();

            // Create demoter callback that writes to target layer and updates hashtable
            let stats = &self.stats;
            let demoter = |key: &[u8],
                           value: &[u8],
                           optional: &[u8],
                           ttl: Duration,
                           old_location: Location| {
                // Write item to target layer
                if let Ok(new_location) = target_layer.write_item(key, value, optional, ttl) {
                    // Atomically update hashtable to point to new location
                    // preserve_freq=true to keep the frequency counter
                    hashtable.cas_location(key, old_location, new_location.to_location(), true);
                    stats.demotions.fetch_add(1, Ordering::Relaxed);
                } else {
                    // Target write failed (staging pool exhausted), discard item
                    stats.demotion_failures.fetch_add(1, Ordering::Relaxed);
                    hashtable.remove(key, old_location);
                }
            };

            return match layer.evict_nonblocking_with_demoter(hashtable, demoter) {
                EvictResult::Freed => true,
                EvictResult::Deferred => {
                    // Segment was pinned by readers — try emergency evict of a different segment
                    layer.emergency_evict(hashtable)
                }
                EvictResult::NoCandidate => false,
            };
        }

        // No next layer — just evict (discard items)
        let result = match layer.evict_nonblocking(self.hashtable.as_ref()) {
            EvictResult::Freed => true,
            EvictResult::Deferred => {
                // Segment was pinned by readers — try emergency evict
                layer.emergency_evict(self.hashtable.as_ref())
            }
            EvictResult::NoCandidate => false,
        };
        if result {
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Mark an item as deleted at the given location.
    /// Supersede an item, reclaiming its space according to the configured
    /// policy.
    ///
    /// The four overwrite paths -- `set`, `replace`, `cas` and a committed
    /// streaming set -- all route through here so they cannot drift apart
    /// from each other, which is how the deferred behaviour came to differ
    /// from `delete` in the first place.
    fn supersede_at(&self, location: Location) {
        match self.overwrite_reclaim {
            crate::config::OverwriteReclaim::Deferred => self.mark_deleted_at(location),
            crate::config::OverwriteReclaim::FreeEmpty => self.mark_deleted_at_free_empty(location),
            crate::config::OverwriteReclaim::Compact => self.mark_deleted_at_with_compact(location),
        }
    }

    /// Mark deleted and free the segment if that emptied it, without
    /// attempting predecessor compaction.
    fn mark_deleted_at_free_empty(&self, location: Location) {
        let item_loc = ItemLocation::from_location(location);
        if let Some(layer_idx) = self.layer_for_pool(item_loc.pool_id())
            && let Some(layer) = self.layers.get(layer_idx)
        {
            layer.mark_deleted_and_free_empty(item_loc);
        }
    }

    fn mark_deleted_at(&self, location: Location) {
        let item_loc = ItemLocation::from_location(location);
        if let Some(layer_idx) = self.layer_for_pool(item_loc.pool_id())
            && let Some(layer) = self.layers.get(layer_idx)
        {
            layer.mark_deleted(item_loc);
        }
    }

    /// Mark an item as deleted and attempt segment compaction.
    ///
    /// This is called from `delete` to allow eager segment reclamation.
    fn mark_deleted_at_with_compact(&self, location: Location) {
        let item_loc = ItemLocation::from_location(location);
        if let Some(layer_idx) = self.layer_for_pool(item_loc.pool_id())
            && let Some(layer) = self.layers.get(layer_idx)
        {
            if layer.mark_deleted_and_compact(item_loc, self.hashtable.as_ref()) {
                self.stats.compactions.fetch_add(1, Ordering::Relaxed);
            }
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
    fn create_key_verifier(&self) -> CacheKeyVerifier<'_> {
        // Pre-compute direct pool references indexed by pool_id
        // This avoids layer lookup and enum matching in the hot path
        let mut pools: [Option<(PoolRef<'_>, bool)>; 4] = [None; 4];

        for (pool_id, layer_idx) in self.pool_map.iter().enumerate() {
            if let Some(idx) = layer_idx
                && let Some(layer) = self.layers.get(*idx)
            {
                match layer {
                    CacheLayer::Fifo(l) => {
                        let pool = l.pool();
                        pools[pool_id] = Some((PoolRef::Memory(pool), pool.is_per_item_ttl()));
                    }
                    CacheLayer::Ttl(l) => {
                        let pool = l.pool();
                        pools[pool_id] = Some((PoolRef::Memory(pool), pool.is_per_item_ttl()));
                    }
                    CacheLayer::Disk(l) => {
                        let pool = l.pool();
                        // Disk pools always use segment-level TTL
                        pools[pool_id] = Some((PoolRef::Disk(pool), false));
                    }
                    CacheLayer::IoUringDisk(l) => {
                        let pool = l.pool();
                        // IoUringDisk pools always use segment-level TTL
                        pools[pool_id] = Some((PoolRef::IoUring(pool), false));
                    }
                }
            }
        }

        CacheKeyVerifier { pools }
    }

    /// Drain the io_uring disk tier's flush queue.
    ///
    /// Returns all pending flush requests. The server submits each as an
    /// io_uring write and calls `complete_flush` when the write completes.
    pub fn take_flush_queue(&self) -> Vec<crate::FlushRequest> {
        for layer in &self.layers {
            if let CacheLayer::IoUringDisk(disk_layer) = layer {
                return disk_layer.take_flush_queue();
            }
        }
        Vec::new()
    }

    /// Signal that a disk flush completed for the given segment.
    ///
    /// Detaches the write buffer and returns it to the buffer pool.
    pub fn complete_flush(&self, segment_id: u32) {
        for layer in &self.layers {
            if let CacheLayer::IoUringDisk(disk_layer) = layer {
                disk_layer.complete_flush(segment_id);
                return;
            }
        }
    }

    /// Release a disk segment's ref_count after an async read completes.
    pub fn release_disk_read(&self, segment_id: u32, pool_id: u8) {
        let Some(layer_idx) = self.layer_for_pool(pool_id) else {
            return;
        };
        let Some(CacheLayer::IoUringDisk(disk_layer)) = self.layers.get(layer_idx) else {
            return;
        };
        disk_layer.release_read(segment_id);
    }

    /// Flush all items from the cache.
    ///
    /// This clears the hashtable and resets all segments to their initial state.
    /// After calling flush, the cache will be empty.
    pub fn flush(&self) {
        // Clear the hashtable first - this makes all items "invisible"
        self.hashtable.clear();

        // Reset each layer completely -- organization state and pool both, or
        // the chains keep naming segments the pool has just recycled.
        for layer in &self.layers {
            layer.reset();
        }
    }
}

/// Reference to either a memory pool or a file pool.
///
/// Used by CacheKeyVerifier to support both RAM and disk layers.
#[derive(Clone, Copy)]
enum PoolRef<'a> {
    Memory(&'a MemoryPool),
    Disk(&'a FilePool),
    IoUring(&'a IoUringPool),
}

/// Key verifier for TieredCache.
///
/// Stores pre-computed direct pool references indexed by pool_id,
/// avoiding layer lookup and enum matching in the hot verify path.
struct CacheKeyVerifier<'a> {
    /// Direct pool references with is_per_item_ttl flag, indexed by pool_id.
    pools: [Option<(PoolRef<'a>, bool)>; 4],
}

impl KeyVerifier for CacheKeyVerifier<'_> {
    #[inline(always)]
    fn prefetch(&self, location: Location) {
        // pool_id sits at a fixed position and decodes without a layout; the
        // rest of the split is per-pool, so resolve the pool first and let its
        // layout decode segment_id and offset.
        let item_loc = ItemLocation::from_location(location);
        let pool_id = item_loc.pool_id();

        // Direct pool lookup - pool_id is 2 bits (0-3), pools is [_; 4]
        // SAFETY: pool_id is extracted from ItemLocation which stores it in 2 bits
        let Some((pool_ref, _)) = (unsafe { *self.pools.get_unchecked(pool_id as usize) }) else {
            return;
        };

        // Get data pointer based on pool type
        let ptr = match pool_ref {
            PoolRef::Memory(pool) => {
                let (_, segment_id, incarnation, offset) = item_loc.unpack(pool.layout());
                let Some(segment) = pool.get(segment_id) else {
                    return;
                };
                // Do not prefetch off a stale location either: the address it
                // names belongs to a later incarnation's item.
                //
                // Deliberately NOT under a read guard, unlike `verify`. A
                // prefetch reads no bytes -- it computes an address and issues
                // a hardware hint -- and pool memory stays mapped for the
                // pool's lifetime, so a recycled segment's address is still
                // valid to hint on. Taking a guard here would add three atomics
                // to an operation whose whole point is being nearly free.
                if segment.incarnation() != incarnation {
                    return;
                }
                unsafe { segment.data_ptr().add(offset as usize) }
            }
            PoolRef::Disk(pool) => {
                let (_, segment_id, incarnation, offset) = item_loc.unpack(pool.layout());
                let Some(segment) = pool.get(segment_id) else {
                    return;
                };
                if segment.incarnation() != incarnation {
                    return;
                }
                unsafe { segment.data_ptr().add(offset as usize) }
            }
            PoolRef::IoUring(pool) => {
                let (_, segment_id, incarnation, offset) = item_loc.unpack(pool.layout());
                let Some(meta) = pool.get_meta(segment_id) else {
                    return;
                };
                if meta.incarnation() != incarnation {
                    return;
                }
                let Some(data_ptr) = meta.write_buffer_ptr() else {
                    return;
                };
                unsafe { data_ptr.add(offset as usize) }
            }
        };

        // Use platform-specific prefetch intrinsics
        #[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
        unsafe {
            // PREFETCHT0 - prefetch to all cache levels
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(ptr as *const i8);
        }

        // `not(miri)`: as in `prefetch_bucket` -- inline asm Miri cannot run,
        // and a prefetch has no semantic effect.
        #[cfg(all(target_arch = "aarch64", not(miri)))]
        unsafe {
            // PRFM PLDL1KEEP - prefetch for load, L1 cache, keep in cache
            std::arch::asm!(
                "prfm pldl1keep, [{ptr}]",
                ptr = in(reg) ptr,
                options(nostack, preserves_flags)
            );
        }

        // On other platforms, this is a no-op
        // `miri` lands here too: both prefetch arms are compiled out under it.
        #[cfg(any(
            miri,
            not(any(
                all(target_arch = "x86_64", target_feature = "sse"),
                target_arch = "aarch64"
            ))
        ))]
        let _ = ptr;
    }

    #[inline(always)]
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        // pool_id sits at a fixed position and decodes without a layout; the
        // rest of the split is per-pool, so resolve the pool first and let its
        // layout decode segment_id and offset.
        let item_loc = ItemLocation::from_location(location);
        let pool_id = item_loc.pool_id();

        // Direct pool lookup - pool_id is 2 bits (0-3), pools is [_; 4]
        // SAFETY: pool_id is extracted from ItemLocation which stores it in 2 bits
        let Some((pool_ref, _is_per_item_ttl)) =
            (unsafe { *self.pools.get_unchecked(pool_id as usize) })
        else {
            return false;
        };

        // Branch based on pool type
        // Use verify_key_at_offset from SegmentKeyVerify trait
        match pool_ref {
            PoolRef::Memory(pool) => {
                let (_, segment_id, incarnation, offset) = item_loc.unpack(pool.layout());
                let Some(segment) = pool.get(segment_id) else {
                    return false;
                };
                // Guard, then check the incarnation, then read -- see
                // `SegmentKeyVerify::verify_key_guarded`. The guard stops the
                // segment being recycled underneath the byte reads (#109).
                segment.verify_key_guarded(offset, key, allow_deleted, incarnation)
            }
            PoolRef::Disk(pool) => {
                let (_, segment_id, incarnation, offset) = item_loc.unpack(pool.layout());
                let Some(segment) = pool.get(segment_id) else {
                    return false;
                };
                // Guard, then check the incarnation, then read -- see
                // `SegmentKeyVerify::verify_key_guarded`. The guard stops the
                // segment being recycled underneath the byte reads (#109).
                segment.verify_key_guarded(offset, key, allow_deleted, incarnation)
            }
            PoolRef::IoUring(pool) => {
                let (_, segment_id, incarnation, offset) = item_loc.unpack(pool.layout());
                let Some(meta) = pool.get_meta(segment_id) else {
                    return false;
                };
                // Guard, then check the incarnation, then read -- see
                // `SegmentKeyVerify::verify_key_guarded`. The guard stops the
                // segment being recycled underneath the byte reads (#109).
                meta.verify_key_guarded(offset, key, allow_deleted, incarnation)
            }
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
    overwrite_reclaim: crate::config::OverwriteReclaim,
}

impl<H: Hashtable> TieredCacheBuilder<H> {
    /// Create a new builder with the given hashtable.
    pub fn new(hashtable: Arc<H>) -> Self {
        Self {
            hashtable,
            layers: Vec::new(),
            pool_map: [None; 4],
            overwrite_reclaim: crate::config::OverwriteReclaim::default(),
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

    /// Add a disk layer (extended capacity tier).
    pub fn with_disk_layer(mut self, layer: DiskLayer) -> Self {
        let pool_id = layer.pool().pool_id();
        let layer_idx = self.layers.len();
        self.layers.push(CacheLayer::Disk(layer));
        if pool_id < 4 {
            self.pool_map[pool_id as usize] = Some(layer_idx);
        }
        self
    }

    /// Add an io_uring disk layer (extended capacity tier).
    pub fn with_io_uring_disk_layer(mut self, layer: IoUringDiskLayer) -> Self {
        let pool_id = layer.pool().pool_id();
        let layer_idx = self.layers.len();
        self.layers.push(CacheLayer::IoUringDisk(layer));
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

    /// What an overwrite does with the superseded copy's bytes.
    ///
    /// Defaults to `Deferred`, which is the behaviour every overwrite path
    /// has always had: mark the old copy deleted and leave its bytes until a
    /// merge pass sweeps the segment. `delete` alone reclaims eagerly.
    pub fn overwrite_reclaim(mut self, policy: crate::config::OverwriteReclaim) -> Self {
        self.overwrite_reclaim = policy;
        self
    }

    /// Build the tiered cache.
    pub fn build(mut self) -> TieredCache<H> {
        // Wire each layer to demote into the next one added, unless the caller
        // already chose a target.
        //
        // `LayerConfig::next_layer` defaults to `None`, and
        // `determine_item_fate` only demotes when it is set. Leaving it unset on
        // a multi-layer cache produces one where the lower tiers are
        // unreachable by demotion: every eviction discards its items instead of
        // promoting hot ones, while `layer_count()` and the metrics still report
        // every layer and `demotions` stays 0 forever. A tier nothing can reach
        // is indistinguishable at runtime from one that is merely cold, which is
        // why this defaults rather than being left to each caller (#116, and the
        // cause of the flake in #105).
        //
        // The last layer keeps `None`: there is nothing below it.
        for i in 0..self.layers.len().saturating_sub(1) {
            if self.layers[i].config().next_layer.is_none() {
                self.layers[i].set_next_layer((i + 1) as crate::config::LayerId);
            }
        }

        TieredCache {
            hashtable: self.hashtable,
            layers: self.layers,
            pool_map: self.pool_map,
            eviction_threshold: self.eviction_threshold,
            max_eviction_attempts: self.max_eviction_attempts,
            overwrite_reclaim: self.overwrite_reclaim,
            stats: CacheStats::new(),
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::config::{EvictionStrategy, MergeConfig, OverwriteReclaim};
    use crate::hashtable_impl::MultiChoiceHashtable;
    use crate::layer::{FifoLayerBuilder, TtlLayerBuilder};

    /// A tiered cache must wire a demotion path by default.
    ///
    /// `determine_item_fate` only demotes when `config.next_layer.is_some()`,
    /// and `LayerConfig::next_layer` defaults to `None`. A caller who adds two
    /// layers and does not set it by hand gets a cache where layer 1 is
    /// unreachable by demotion: every eviction from layer 0 discards its items
    /// instead of promoting hot ones. It is a FIFO-with-discard cache that
    /// presents as S3-FIFO -- `layer_count()` says 2, the metrics show two
    /// layers, and `demotions` simply stays 0 forever.
    ///
    /// That is not hypothetical: it is what made #105 flaky.
    #[test]
    fn test_builder_wires_a_demotion_path_between_adjacent_layers() {
        let cache = create_test_cache_without_explicit_demotion();

        assert_eq!(
            cache.layers[0].config().next_layer,
            Some(1),
            "layer 0 must demote into layer 1 by default, or layer 1 is dead weight"
        );
        assert_eq!(
            cache.layers[1].config().next_layer,
            None,
            "the last layer has nowhere to demote to"
        );
    }

    /// An explicit `with_next_layer` must win over the default wiring.
    #[test]
    fn test_builder_does_not_override_an_explicit_demotion_target() {
        let hashtable = Arc::new(MultiChoiceHashtable::new(10));
        let fifo = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(256 * 1024)
            .spare_capacity(0)
            // Deliberately points past the adjacent layer.
            .config(LayerConfig::new().with_next_layer(0))
            .build()
            .expect("fifo layer");
        let ttl = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(64 * 1024)
            .heap_size(512 * 1024)
            .spare_capacity(0)
            .build()
            .expect("ttl layer");

        let cache = TieredCacheBuilder::new(hashtable)
            .with_fifo_layer(fifo)
            .with_ttl_layer(ttl)
            .build();

        assert_eq!(
            cache.layers[0].config().next_layer,
            Some(0),
            "an explicit demotion target must not be overwritten"
        );
    }

    /// A two-layer cache built without touching `LayerConfig`.
    fn create_test_cache_without_explicit_demotion() -> TieredCache<MultiChoiceHashtable> {
        let hashtable = Arc::new(MultiChoiceHashtable::new(10));
        let fifo = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(256 * 1024)
            .spare_capacity(0)
            .build()
            .expect("fifo layer");
        let ttl = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(64 * 1024)
            .heap_size(512 * 1024)
            .spare_capacity(0)
            .build()
            .expect("ttl layer");

        TieredCacheBuilder::new(hashtable)
            .with_fifo_layer(fifo)
            .with_ttl_layer(ttl)
            .build()
    }

    /// Every layer type must still accept writes after `flush()`.
    ///
    /// `flush()` backs FLUSHALL. It used to reset the pools alone, leaving
    /// each layer's organization state -- the FIFO chain, the TTL bucket
    /// chains, the cached write segments -- naming segments the pool had just
    /// recycled. The next write could not link onto that stale tail, and
    /// `allocate_segment` reports the chain failure as `OutOfMemory`, so a
    /// flushed server served misses forever with every segment free.
    ///
    /// Written against the layers directly rather than through `set`, which
    /// only ever reaches layer 0: each `CacheLayer::reset` arm needs its own
    /// proof, and three of the four are otherwise unexercised.
    /// Not run under Miri: building a `DiskLayer` maps a file, and Miri does
    /// not support file-backed memory mappings. The `--skip disk::` filters in
    /// CI do not cover this test because it lives in `cache::tests`, so the
    /// exclusion has to be stated here.
    #[test]
    #[cfg_attr(miri, ignore = "file-backed mmap is unsupported under Miri")]
    fn test_flush_leaves_every_layer_type_writable() {
        use crate::disk::{DiskLayerBuilder, IoUringDiskLayerBuilder};

        let dir = tempfile::tempdir().expect("temp dir");

        let fifo_layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(640 * 1024)
            .spare_capacity(0)
            .build()
            .expect("fifo layer");
        let ttl_layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(64 * 1024)
            .heap_size(640 * 1024)
            .spare_capacity(0)
            .build()
            .expect("ttl layer");
        let disk_layer = DiskLayerBuilder::new()
            .layer_id(2)
            .pool_id(2)
            .segment_size(64 * 1024)
            .path(dir.path().join("disk.dat"))
            .size(640 * 1024)
            .build()
            .expect("disk layer");
        let io_uring_layer = IoUringDiskLayerBuilder::new()
            .layer_id(3)
            .pool_id(3)
            .segment_size(64 * 1024)
            .segment_count(10)
            .build();

        let cache: TieredCache<MultiChoiceHashtable> =
            TieredCacheBuilder::new(Arc::new(MultiChoiceHashtable::new(10)))
                .with_fifo_layer(fifo_layer)
                .with_ttl_layer(ttl_layer)
                .with_disk_layer(disk_layer)
                .with_io_uring_disk_layer(io_uring_layer)
                .build();

        let ttl = Duration::from_secs(3600);
        let value = vec![b'v'; 4096];

        // Deep enough that each layer chains more than one segment, so the
        // tail a reset must clear is not also the head.
        let fill = |phase: &str| {
            for layer in &cache.layers {
                for i in 0..24 {
                    let key = format!("{phase}{i:03}");
                    let written = match layer {
                        CacheLayer::Fifo(l) => l.write_item(key.as_bytes(), &value, b"", ttl),
                        CacheLayer::Ttl(l) => l.write_item(key.as_bytes(), &value, b"", ttl),
                        CacheLayer::Disk(l) => l.write_item(key.as_bytes(), &value, b"", ttl),
                        CacheLayer::IoUringDisk(l) => {
                            l.write_item_with_buffers(key.as_bytes(), &value, b"", ttl)
                        }
                    };
                    written.unwrap_or_else(|e| {
                        panic!(
                            "layer {} ({phase}) write {i} failed: {e:?}",
                            layer.layer_id()
                        )
                    });
                }
            }
        };

        fill("pre");

        for layer in &cache.layers {
            let chained = match layer {
                CacheLayer::Fifo(l) => l.chain().segment_count(),
                CacheLayer::Ttl(l) => l.buckets().total_segment_count(),
                CacheLayer::Disk(l) => l.buckets().total_segment_count(),
                CacheLayer::IoUringDisk(l) => l.buckets().total_segment_count(),
            };
            assert!(
                chained > 1,
                "layer {} must chain more than one segment for this to test the link",
                layer.layer_id()
            );
        }

        cache.flush();

        for layer in &cache.layers {
            let chained = match layer {
                CacheLayer::Fifo(l) => l.chain().segment_count(),
                CacheLayer::Ttl(l) => l.buckets().total_segment_count(),
                CacheLayer::Disk(l) => l.buckets().total_segment_count(),
                CacheLayer::IoUringDisk(l) => l.buckets().total_segment_count(),
            };
            assert_eq!(
                chained,
                0,
                "layer {} kept organization state naming segments flush freed",
                layer.layer_id()
            );
        }

        // The same workload again. A failure here is the flushed-server bug.
        fill("post");
    }

    /// The disk arms of the hot-path verifier reject a stale tag too.
    ///
    /// `test_stale_location_is_rejected_after_recycle` drives the Memory arm
    /// through a real recycle; the two disk arms need a disk pool, which the
    /// cache's own `set` never writes to directly. Here the recycle is staged
    /// on the pool and the location built from the live one, so the key really
    /// is at that offset and only the tag distinguishes them.
    /// Not run under Miri: building a `DiskLayer` maps a file, and Miri does
    /// not support file-backed memory mappings. The `--skip disk::` filters in
    /// CI do not cover this test because it lives in `cache::tests`, so the
    /// exclusion has to be stated here.
    #[test]
    #[cfg_attr(miri, ignore = "file-backed mmap is unsupported under Miri")]
    fn test_stale_location_is_rejected_on_the_disk_arms() {
        use crate::disk::{DiskLayerBuilder, IoUringDiskLayerBuilder};
        use crate::pool::RamPool;
        use crate::segment::Segment;
        use crate::state::State;

        let dir = tempfile::tempdir().expect("temp dir");

        let fifo_layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(256 * 1024)
            .spare_capacity(0)
            .build()
            .expect("fifo layer");
        let disk_layer = DiskLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(64 * 1024)
            .path(dir.path().join("disk.dat"))
            .size(256 * 1024)
            .build()
            .expect("disk layer");
        let io_uring_layer = IoUringDiskLayerBuilder::new()
            .layer_id(2)
            .pool_id(2)
            .segment_size(64 * 1024)
            .segment_count(4)
            .build();

        // Age every disk segment through a used incarnation before anything is
        // written, so the tag a write stamps is non-zero.
        for _ in 0..disk_layer.pool().segment_count() {
            let id = disk_layer.pool().reserve().expect("free segment");
            let segment = disk_layer.pool().get(id).expect("segment");
            assert!(segment.cas_metadata(State::Reserved, State::Locked, None, None));
            disk_layer.pool().release(id);
        }
        for _ in 0..io_uring_layer.pool().segment_count() {
            let id = io_uring_layer.pool().reserve().expect("free segment");
            let segment = io_uring_layer.pool().get(id).expect("segment");
            assert!(segment.cas_metadata(State::Reserved, State::Locked, None, None));
            io_uring_layer.pool().release(id);
        }

        let ttl = Duration::from_secs(3600);
        let disk_live = disk_layer
            .write_item(b"key", b"value", b"", ttl)
            .expect("disk write");
        let io_uring_live = io_uring_layer
            .write_item_with_buffers(b"key", b"value", b"", ttl)
            .expect("io_uring write");

        let disk_layout = *disk_layer.pool().layout();
        let io_uring_layout = *io_uring_layer.pool().layout();

        let cache: TieredCache<MultiChoiceHashtable> =
            TieredCacheBuilder::new(Arc::new(MultiChoiceHashtable::new(10)))
                .with_fifo_layer(fifo_layer)
                .with_disk_layer(disk_layer)
                .with_io_uring_disk_layer(io_uring_layer)
                .build();

        for (label, live, layout) in [
            ("Disk", disk_live, disk_layout),
            ("IoUring", io_uring_live, io_uring_layout),
        ] {
            let (pool_id, segment_id, tag, offset) = live.unpack(&layout);
            assert_ne!(
                tag, 0,
                "{label}: the segment must be past its first incarnation"
            );
            let stale = ItemLocation::new(&layout, pool_id, segment_id, tag - 1, offset);

            assert!(
                cache
                    .create_key_verifier()
                    .verify(b"key", live.to_location(), false),
                "{label}: the current incarnation must resolve"
            );
            assert!(
                !cache
                    .create_key_verifier()
                    .verify(b"key", stale.to_location(), false),
                "{label}: a location from a previous incarnation must not resolve"
            );
        }
    }

    /// A cache whose FIFO layer holds one big item per segment, so writing the
    /// same key repeatedly cycles segments and eventually reuses one.
    fn create_recycling_test_cache() -> TieredCache<MultiChoiceHashtable> {
        let hashtable = Arc::new(MultiChoiceHashtable::new(10));

        let fifo_layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(1024)
            .heap_size(4 * 1024) // 4 segments
            .spare_capacity(0)
            .build()
            .expect("Failed to create FIFO layer");

        TieredCacheBuilder::new(hashtable)
            .with_fifo_layer(fifo_layer)
            .eviction_threshold(1)
            .build()
    }

    /// A location held across a segment recycle must stop resolving.
    ///
    /// This is the crucible#88 hazard, and the shape matters: segments are
    /// append-only from a fixed start, so with uniform item sizes the n-th item
    /// of every incarnation lands at the same offset. A stale location does not
    /// merely point at free space -- it points at a *different live item*, and
    /// here at one with the very same key. The key compare alone would resolve
    /// it. Only the incarnation tag can tell the two apart.
    #[test]
    fn test_stale_location_is_rejected_after_recycle() {
        let cache = create_recycling_test_cache();
        let ttl = Duration::from_secs(60);

        // One item per segment: 600 bytes of value leaves no room for a second.
        let value = vec![b'v'; 600];

        cache.set(b"key", &value, b"", ttl).unwrap();
        let (stale_raw, _) = cache
            .hashtable
            .lookup(b"key", &cache.create_key_verifier())
            .expect("key should be present");
        let stale = ItemLocation::from_location(stale_raw);

        // Sanity: it resolves now, so a later rejection means something.
        assert!(
            cache.create_key_verifier().verify(b"key", stale_raw, false),
            "the location must resolve before any recycle"
        );

        let pool = match &cache.layers[0] {
            CacheLayer::Fifo(layer) => layer.pool(),
            _ => unreachable!("layer 0 is the FIFO layer"),
        };
        let (_, stale_segment, stale_tag, stale_offset) = stale.unpack(pool.layout());

        // Rewrite the same key until the pool cycles back to that segment.
        // Four segments and one item each, so a handful of rounds suffices;
        // the loop is bounded generously and the guards below prove it ran.
        let mut recycled = false;
        for _ in 0..64 {
            cache.set(b"key", &value, b"", ttl).unwrap();

            let segment = pool
                .get(stale_segment)
                .expect("segment id came from a location");
            if segment.incarnation() != stale_tag
                && segment.verify_key_at_offset(stale_offset, b"key", false)
            {
                recycled = true;
                break;
            }
        }

        // Vacuity guards. Without the first, a loop that never recycled the
        // segment would pass while proving nothing. Without the second, the
        // rejection could come from the key compare rather than the tag.
        assert!(
            recycled,
            "no recycle placed a matching key back at that offset; the test is vacuous"
        );
        let segment = pool.get(stale_segment).expect("segment");
        assert_ne!(
            segment.incarnation(),
            stale_tag,
            "the segment was not recycled; the test is vacuous"
        );
        assert!(
            segment.verify_key_at_offset(stale_offset, b"key", false),
            "the offset must hold a live matching key, or the key compare alone \
             would reject and this test proves nothing"
        );

        assert!(
            !cache.create_key_verifier().verify(b"key", stale_raw, false),
            "a location from a previous incarnation must not resolve"
        );

        // ...and the live location still does.
        let live = ItemLocation::new(
            pool.layout(),
            pool.pool_id(),
            stale_segment,
            segment.incarnation(),
            stale_offset,
        );
        assert!(
            cache
                .create_key_verifier()
                .verify(b"key", live.to_location(), false),
            "the current incarnation must still resolve"
        );
    }

    fn create_test_cache_with_reclaim(
        policy: OverwriteReclaim,
    ) -> TieredCache<MultiChoiceHashtable> {
        let hashtable = Arc::new(MultiChoiceHashtable::new(10));
        let fifo_config = LayerConfig::new()
            .with_next_layer(1)
            .with_demotion_threshold(1);
        let fifo_layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(256 * 1024)
            .spare_capacity(0)
            .config(fifo_config)
            .build()
            .expect("fifo layer");
        let ttl_layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(64 * 1024)
            .heap_size(512 * 1024)
            .spare_capacity(0)
            .build()
            .expect("ttl layer");
        TieredCacheBuilder::new(hashtable)
            .with_fifo_layer(fifo_layer)
            .with_ttl_layer(ttl_layer)
            .eviction_threshold(1)
            .overwrite_reclaim(policy)
            .build()
    }

    fn create_test_cache() -> TieredCache<MultiChoiceHashtable> {
        let hashtable = Arc::new(MultiChoiceHashtable::new(10)); // 2^10 = 1024 buckets

        // Layer 0 must name layer 1 as its demotion target, matching how
        // `SegCacheBuilder` wires the s3fifo topology. Without it,
        // `determine_item_fate` can never demote -- it requires
        // `next_layer.is_some()` -- so eviction discards every item instead
        // of promoting hot ones, making this a FIFO-with-discard cache that
        // only looks like S3-FIFO.
        let fifo_config = LayerConfig::new()
            .with_next_layer(1)
            .with_demotion_threshold(1);

        let fifo_layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(256 * 1024)
            .spare_capacity(0) // No spare for tests
            .config(fifo_config)
            .build()
            .expect("Failed to create FIFO layer");

        let ttl_layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(64 * 1024)
            .heap_size(512 * 1024)
            .spare_capacity(0) // No spare for tests
            .build()
            .expect("Failed to create TTL layer");

        TieredCacheBuilder::new(hashtable)
            .with_fifo_layer(fifo_layer)
            .with_ttl_layer(ttl_layer)
            .eviction_threshold(1)
            .build()
    }

    /// Drive a two-layer cache well past capacity, reading each key once so it
    /// qualifies for demotion, and report what the cascade did.
    ///
    /// Reads matter: `determine_item_fate` demotes only at
    /// `freq >= demotion_threshold`, and a write leaves frequency at zero, so
    /// a write-only workload ghosts everything and exercises nothing.
    fn drive_past_capacity(cache: &TieredCache<MultiChoiceHashtable>) -> CacheInternalStats {
        let value = vec![0xABu8; 1024];
        for i in 0..3000u32 {
            let key = format!("key-{i:08}");
            if cache
                .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .is_ok()
            {
                // Raise the frequency so this item is worth promoting.
                let _ = cache.get(key.as_bytes());
            }
        }
        cache.stats().snapshot()
    }

    /// The stall crucible#152 is about is the one a `set` absorbs, so the
    /// histogram times the eviction path rather than every `ensure_space`
    /// call. Most calls return immediately on the free-segment check; timing
    /// those would bury the stall under millions of ~0 ns samples and make
    /// every percentile below the maximum meaningless.
    #[test]
    fn an_eviction_pass_is_timed() {
        let cache = create_test_cache();

        let stats = drive_past_capacity(&cache);

        let samples = stats.eviction_latency.count();
        assert!(
            samples > 0,
            "no eviction pass was timed despite {} demotions and {} evictions",
            stats.demotions,
            stats.evictions,
        );
        assert!(
            stats.eviction_latency.max_ns().is_some(),
            "a timed pass must produce a readable maximum"
        );
    }

    /// The control: a cache under no pressure must record nothing, or the
    /// histogram is measuring `set` rather than eviction and its percentiles
    /// describe the fast path.
    #[test]
    fn a_cache_that_never_evicted_records_no_eviction_latency() {
        let cache = create_test_cache();

        cache
            .set(b"k", b"v", b"", Duration::from_secs(3600))
            .unwrap();

        let stats = cache.stats().snapshot();
        assert_eq!(stats.eviction_latency.count(), 0);
        assert_eq!(stats.eviction_latency.max_ns(), None);
    }

    #[test]
    fn a_two_layer_cache_evicts_from_its_main_layer_once_that_layer_fills() {
        let cache = create_test_cache();

        let stats = drive_past_capacity(&cache);

        // `stats.evictions` only counts a layer with no demotion target, which
        // in this topology is layer 1 alone. Zero means layer 1 was never
        // evicted from at all: it filled once and froze, and every later
        // promotion had nowhere to land.
        assert!(
            stats.evictions > 0,
            "layer 1 never evicted (evictions={}, demotions={}, failures={}); \
             a main cache that stops reclaiming is not a policy, it is a stall",
            stats.evictions,
            stats.demotions,
            stats.demotion_failures,
        );
    }

    #[test]
    fn most_items_chosen_for_demotion_reach_the_main_layer() {
        let cache = create_test_cache();

        let stats = drive_past_capacity(&cache);

        // The demoter discards an item whose target write fails. If the main
        // layer is never reclaimed, that is most of them -- the admission
        // filter keeps selecting hot items and the cache keeps throwing them
        // away, which looks like a working S3-FIFO from the outside.
        assert!(
            stats.demotions > stats.demotion_failures,
            "more demotions were discarded than landed \
             (demotions={}, failures={})",
            stats.demotions,
            stats.demotion_failures,
        );
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
        let hashtable = Arc::new(MultiChoiceHashtable::new(10));
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

    /// Every reclamation policy must preserve the live set.
    ///
    /// This pins correctness, not benefit. The benefit is a capacity effect
    /// that needs a realistic segment count to appear: at this fixture's
    /// eight segments in layer 1, all three policies return identical
    /// survivor and free-segment counts, because merge already reclaims
    /// everything the workload frees. The measurement that motivated the
    /// policy ran 128 segments on a real trace, and belongs on the rig
    /// rather than here -- a unit test tuned until it showed a difference
    /// would be measuring the tuning.
    #[test]
    fn every_reclamation_policy_preserves_the_live_set() {
        fn survivors(policy: OverwriteReclaim) -> u64 {
            let cache = create_test_cache_with_reclaim(policy);
            let value = vec![0xABu8; 1024];
            for i in 0..200u32 {
                let key = format!("key-{i:08}");
                if cache
                    .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                    .is_ok()
                {
                    let _ = cache.get(key.as_bytes());
                }
            }
            // Rewrite repeatedly: every one of these supersedes a copy that
            // is still resident, which is the path that never reclaimed.
            let bigger = vec![0xCDu8; 1024];
            for _round in 0..20 {
                for i in 0..200u32 {
                    let key = format!("key-{i:08}");
                    let _ = cache.set(key.as_bytes(), &bigger, b"", Duration::from_secs(3600));
                }
            }
            let mut alive = 0u64;
            for i in 0..200u32 {
                let key = format!("key-{i:08}");
                if let Some(v) = cache.get(key.as_bytes()) {
                    // The survivor must be the copy written last, or a
                    // policy could "preserve" the live set by serving a
                    // superseded copy.
                    let bytes: &[u8] = &v;
                    assert_eq!(bytes[0], 0xCD, "{policy:?} served a superseded copy");
                    alive += 1;
                }
            }
            alive
        }

        let deferred = survivors(OverwriteReclaim::Deferred);
        for policy in [OverwriteReclaim::FreeEmpty, OverwriteReclaim::Compact] {
            assert_eq!(
                survivors(policy),
                deferred,
                "{policy:?} lost live items the deferred policy kept"
            );
        }
    }

    /// Compaction must relocate items without touching their frequency.
    ///
    /// The counterpart to `a_merge_pass_resets_the_frequency_of_the_items_it_keeps`.
    /// A merge pass judges items and resets what it keeps, which is
    /// Segcache's substitute for aging. Compaction judges nothing -- it
    /// relocates every live item to reclaim dead bytes -- so resetting
    /// there would charge items for a maintenance pass, repeatedly
    /// flattening the frequencies the eviction policy depends on, and more
    /// often the more fragmented the cache is.
    ///
    /// Added because mutating the compaction path to reset left all 607
    /// other tests passing: the distinction was asserted only in a comment.
    #[test]
    fn compaction_relocates_items_without_resetting_their_frequency() {
        // A purpose-built cache rather than the shared fixture. Compaction
        // needs three segments in one TTL bucket, a sealed predecessor, a
        // spare to copy into, and a combined live set under 90% of one
        // segment -- and the shared fixture's eight spare-less segments
        // never satisfy all four, so the first version of this test passed
        // without compacting once.
        let hashtable = Arc::new(MultiChoiceHashtable::new(12));
        let fifo_layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(16 * 1024)
            .heap_size(64 * 1024)
            .spare_capacity(0)
            .config(
                LayerConfig::new()
                    .with_next_layer(1)
                    .with_demotion_threshold(1),
            )
            .build()
            .expect("fifo layer");
        let ttl_layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(16 * 1024)
            .heap_size(1024 * 1024)
            .spare_capacity(4)
            .build()
            .expect("ttl layer");
        let cache = TieredCacheBuilder::new(hashtable)
            .with_fifo_layer(fifo_layer)
            .with_ttl_layer(ttl_layer)
            .eviction_threshold(1)
            .overwrite_reclaim(OverwriteReclaim::Compact)
            .build();
        let value = vec![0xABu8; 512];
        let mut keys = Vec::new();
        for i in 0..400u32 {
            let key = format!("comp-{i:08}");
            if cache
                .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .is_ok()
            {
                keys.push(key);
            }
        }
        assert!(!keys.is_empty(), "the fixture must store something");

        // Warm every key well clear of 1. `get` bumps the counter, so the
        // reads are the warming.
        for key in &keys {
            for _ in 0..6 {
                let _ = cache.get(key.as_bytes());
            }
        }

        // Delete most of them. `delete` runs the compaction path directly,
        // and draining segments this hard is what puts adjacent pairs under
        // the combined-live bound so a pass can actually fire.
        let survivors: Vec<&String> = keys.iter().step_by(8).collect();
        for key in &keys {
            if !survivors.contains(&key) {
                cache.delete(key.as_bytes());
            }
        }

        // The survivors are what compaction relocated. Their frequency must
        // be whatever the warming reads left it at, never reset to 1. Read
        // through `frequency`, not `get`, which would bump it.
        let mut checked = 0;
        let mut at_floor = 0;
        for key in survivors {
            if let Some(freq) = cache.frequency(key.as_bytes()) {
                checked += 1;
                if freq <= 1 {
                    at_floor += 1;
                }
            }
        }
        // Without this the test is vacuous: if no compaction ran, nothing
        // was relocated and every frequency is trivially intact. The
        // mutation that resets frequency during compaction passed the first
        // version of this test for exactly that reason.
        assert!(
            cache.stats().snapshot().compactions > 0,
            "no compaction ran, so this fixture proves nothing about what \
             compaction does to frequencies"
        );
        assert!(checked > 0, "some untouched keys must still be resident");
        assert_eq!(
            at_floor, 0,
            "{at_floor} of {checked} untouched items came back at frequency \
             1 or below: compaction reset what it merely relocated"
        );
    }

    /// The compaction counter must count compactions, not delete calls.
    ///
    /// `delete` runs the compaction path on every call regardless of
    /// `OverwriteReclaim`, so "a delete happened" and "a compaction ran" are
    /// easy to conflate -- and a counter incremented once per delete passes
    /// every test that only asks whether it is above zero. That matters
    /// because this counter is what answers "is compaction reachable on
    /// this workload at all", where a false yes is worse than no counter.
    ///
    /// The control is a working set too small to fill three segments.
    /// `try_compact_segment` needs a segment with a sealed *predecessor*
    /// that is not the bucket tail, so it returns early when
    /// `bucket.segment_count() < 3` -- while the deletes still run exactly
    /// as they do in the other arm.
    ///
    /// An earlier version used a layer with no spare capacity, on the
    /// assumption that compaction could not reserve a destination. It can:
    /// the pool hands out free segments, and that arm compacted nine times.
    #[test]
    fn the_compaction_counter_counts_passes_not_deletes() {
        let build = || {
            let hashtable = Arc::new(MultiChoiceHashtable::new(12));
            let fifo_layer = FifoLayerBuilder::new()
                .layer_id(0)
                .pool_id(0)
                .segment_size(16 * 1024)
                .heap_size(64 * 1024)
                .spare_capacity(0)
                .config(
                    LayerConfig::new()
                        .with_next_layer(1)
                        .with_demotion_threshold(1),
                )
                .build()
                .expect("fifo layer");
            let ttl_layer = TtlLayerBuilder::new()
                .layer_id(1)
                .pool_id(1)
                .segment_size(16 * 1024)
                .heap_size(1024 * 1024)
                .spare_capacity(4)
                .build()
                .expect("ttl layer");
            TieredCacheBuilder::new(hashtable)
                .with_fifo_layer(fifo_layer)
                .with_ttl_layer(ttl_layer)
                .eviction_threshold(1)
                .overwrite_reclaim(OverwriteReclaim::Compact)
                .build()
        };

        // (compactions, deletes issued, segments released)
        let run = |n_items: u32| -> (u64, u64, u64) {
            let cache = build();
            let value = vec![0xABu8; 512];
            let mut keys = Vec::new();
            for i in 0..n_items {
                let key = format!("cnt-{i:08}");
                if cache
                    .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                    .is_ok()
                {
                    keys.push(key);
                }
            }
            for key in &keys {
                for _ in 0..6 {
                    let _ = cache.get(key.as_bytes());
                }
            }
            let survivors: Vec<&String> = keys.iter().step_by(8).collect();
            let free_before = cache.ram_free_segment_count();
            let mut deletes = 0u64;
            for key in &keys {
                if !survivors.contains(&key) {
                    cache.delete(key.as_bytes());
                    deletes += 1;
                }
            }
            let released = cache.ram_free_segment_count().saturating_sub(free_before);
            (cache.stats().snapshot().compactions, deletes, released)
        };

        let (many, deletes, released) = run(400);
        let (few, deletes_control, _) = run(16);

        assert!(deletes > 0 && deletes_control > 0, "both arms must delete");
        assert!(
            many > 0,
            "the large arm must actually compact, or the control proves nothing"
        );
        assert_eq!(
            few, 0,
            "a working set under three segments cannot compact -- {few} were \
             counted across {deletes_control} deletes, so the counter is \
             tracking delete calls rather than compaction passes"
        );
        // A real pass retires two segments and takes one spare, so it nets
        // one release. Deletes release segments too, by emptying them, which
        // only makes this a looser bound -- and it still catches a count
        // that reports a pass whenever it merely reaches the call site,
        // which is what "compaction happened" degrades into otherwise.
        assert!(
            many <= released,
            "each compaction nets one released segment, so {many} passes \
             cannot be reconciled with {released} segments released across \
             {deletes} deletes -- the counter is reporting attempts, not passes"
        );
    }

    /// Every allocating write must reach expiry, not just `set`.
    ///
    /// `ensure_space` is where expiry is attempted, and it is called by
    /// `set`, `begin_segment_set`, `add`, `replace` and `cas` -- with
    /// `append` and `prepend` reaching it through `set`. That list is easy
    /// to read off the code and easy to be wrong about: a write path added
    /// later that allocates without calling `ensure_space` would evict
    /// live data while expired data sat there, and nothing would say so.
    ///
    /// So this exercises the paths rather than reading them.
    #[test]
    fn every_allocating_write_path_expires_before_it_evicts() {
        type Cache = TieredCache<MultiChoiceHashtable>;
        // (name, prepare, exercise). Anything the op needs in place runs in
        // `prepare`, before the TTL lapses, so the only write happening
        // against an all-expired cache is the one under test. Doing that
        // setup afterwards consumes the expired supply itself, and the op is
        // then measured evicting for space its own preamble used -- which is
        // what the first version did, and it read as `append` bypassing
        // expiry entirely.
        type Op = (
            &'static str,
            fn(&Cache, &[u8], &[u8]),
            fn(&Cache, &[u8], &[u8]) -> bool,
        );
        let ops: &[Op] = &[
            (
                "set",
                |_, _, _| {},
                |c, k, v| c.set(k, v, b"", Duration::from_secs(3600)).is_ok(),
            ),
            (
                "add",
                |_, _, _| {},
                |c, k, v| c.add(k, v, b"", Duration::from_secs(3600)).is_ok(),
            ),
            (
                "append",
                |c, k, v| {
                    let _ = c.set(k, v, b"", Duration::from_secs(3600));
                },
                |c, k, v| c.append(k, v).is_ok(),
            ),
            (
                "replace",
                |c, k, v| {
                    let _ = c.set(k, v, b"", Duration::from_secs(3600));
                },
                |c, k, v| c.replace(k, v, b"", Duration::from_secs(3600)).is_ok(),
            ),
        ];

        for (name, prepare, exercise) in ops {
            let clock = crate::clock::TestClock::start();
            let hashtable = Arc::new(MultiChoiceHashtable::new(12));
            let ttl_layer = TtlLayerBuilder::new()
                .layer_id(0)
                .pool_id(0)
                .segment_size(16 * 1024)
                .heap_size(256 * 1024)
                .spare_capacity(2)
                .build()
                .expect("ttl layer");
            let cache = TieredCacheBuilder::new(hashtable)
                .with_ttl_layer(ttl_layer)
                .eviction_threshold(1)
                .overwrite_reclaim(OverwriteReclaim::Deferred)
                .build();
            let value = vec![0xEEu8; 512];

            for i in 0..2000u32 {
                let key = format!("{name}-{i:08}");
                let _ = cache.set(key.as_bytes(), &value, b"", Duration::from_secs(60));
            }
            // Whatever the op needs present, written while nothing has
            // expired yet.
            for i in 0..200u32 {
                let key = format!("{name}-after-{i:08}");
                prepare(&cache, key.as_bytes(), &value);
            }

            let evictions_before = cache.stats().snapshot().evictions;
            let expirations_before = cache.stats().snapshot().expirations;
            for _ in 0..600 {
                clock.tick();
            }

            let mut applied = 0;
            for i in 0..200u32 {
                let key = format!("{name}-after-{i:08}");
                if exercise(&cache, key.as_bytes(), &value) {
                    applied += 1;
                }
            }
            assert!(applied > 0, "{name} never succeeded, so it proved nothing");

            let after = cache.stats().snapshot();
            assert!(
                after.expirations > expirations_before,
                "{name} made space without reaching expiry: expirations stayed \
                 at {expirations_before}"
            );
            assert_eq!(
                after.evictions, evictions_before,
                "{name} evicted live data while expired data was available"
            );
        }
    }

    /// Memory pressure must try expiry before it evicts.
    ///
    /// Reclaiming an expired segment costs nothing and destroys nothing
    /// live; an eviction pass copies survivors and discards the rest. So a
    /// cache whose contents have all expired should make room by expiring,
    /// and evict not at all. Doing it the other way round throws away live
    /// data to make space that dead data was already holding.
    ///
    /// This is also what makes proactive expiration reachable at all in a
    /// benchmark. `expire()` is a public method neither engine calls
    /// internally, so unless pressure triggers it, the TTL-bucket design's
    /// whole advantage is switched off in every measurement.
    #[test]
    fn memory_pressure_expires_before_it_evicts() {
        let clock = crate::clock::TestClock::start();
        // A single TTL layer, matching `policy = "merge"`. The shared
        // tiered fixture will not do: its layer 0 is a FIFO layer whose
        // `expire` returns 0 by construction ("items are checked on read"),
        // and items only reach the TTL layer by demotion, which needs a
        // frequency above the threshold. Nothing here is ever read, so in
        // the tiered shape every item stays where expiry cannot see it.
        let hashtable = Arc::new(MultiChoiceHashtable::new(12));
        let ttl_layer = TtlLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(16 * 1024)
            .heap_size(256 * 1024)
            .spare_capacity(2)
            .build()
            .expect("ttl layer");
        let cache = TieredCacheBuilder::new(hashtable)
            .with_ttl_layer(ttl_layer)
            .eviction_threshold(1)
            .overwrite_reclaim(OverwriteReclaim::Deferred)
            .build();
        let value = vec![0xEEu8; 512];

        // Enough to fill the heap several times over. A fixture that merely
        // fits never calls the space-making path at all, so expiry is never
        // reached and the test reads as "expiry does not run" when what it
        // showed was "there was nothing to reclaim".
        let mut stored = 0u32;
        for i in 0..2000u32 {
            let key = format!("exp-{i:08}");
            if cache
                .set(key.as_bytes(), &value, b"", Duration::from_secs(60))
                .is_ok()
            {
                stored += 1;
            }
        }
        assert!(stored > 0, "the fixture must store something");

        let evictions_before = cache.stats().snapshot().evictions;

        // Everything is now dead. The next write needs space, and there is
        // a segment's worth of expired bytes to take it from.
        for _ in 0..600 {
            clock.tick();
        }
        // A batch, not a single write: whether any one `set` meets pressure
        // depends on where the fill happened to leave the free count, and a
        // write that simply fits never reaches the space-making path at all.
        for i in 0..200u32 {
            let key = format!("after-{i:08}");
            cache
                .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .expect("a write must succeed when every resident item has expired");
        }

        let after = cache.stats().snapshot();
        assert!(
            after.expirations > 0,
            "pressure against a cache of expired items must reclaim by \
             expiry: {} expirations",
            after.expirations
        );
        assert_eq!(
            after.evictions, evictions_before,
            "and must not evict to do it: evictions went from \
             {evictions_before} to {}",
            after.evictions
        );
    }

    /// Occupancy must be reported per segment, not as a cache-wide mean.
    ///
    /// Compaction pairs two adjacent sealed segments, so what decides
    /// whether it can fire is where individual segments sit, not the
    /// average. A version of this that returned `live / capacity` over the
    /// whole cache would report the same single number for a cache of
    /// uniformly half-full segments and for one holding full segments
    /// beside empty ones -- and only the second has pairs to compact.
    ///
    /// Not covered here: the `is_freed` filter. Nothing in this fixture
    /// reaches the free queue, so a version counting freed segments passes.
    /// That filter is exercised by the resident-byte tests instead.
    #[test]
    fn segment_occupancy_locates_segments_rather_than_averaging_them() {
        let cache = create_test_cache_with_reclaim(OverwriteReclaim::Deferred);
        let value = vec![0xCDu8; 1024];
        let mut stored = Vec::new();
        for i in 0..150u32 {
            let key = format!("occ-{i:08}");
            if cache
                .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .is_ok()
            {
                stored.push(key);
            }
        }
        assert!(!stored.is_empty(), "the fixture must store something");

        let full = cache.segment_occupancy();
        let occupied: u64 = full.iter().sum();
        assert!(occupied > 0, "a filled cache has non-free segments");
        assert_eq!(
            occupied,
            full.iter().sum::<u64>(),
            "every counted segment lands in exactly one decile"
        );
        // Freshly written segments are densely packed, so the mass sits high.
        let high: u64 = full[5..].iter().sum();
        assert!(
            high > 0,
            "freshly filled segments should sit in the upper deciles: {full:?}"
        );

        // Items are appended in order, so deleting a prefix of the keys
        // empties the earliest segments and leaves the later ones untouched.
        // That is deliberately bimodal: drained segments at the bottom, full
        // ones at the top, and nothing in between.
        for key in stored.iter().take(stored.len() / 2) {
            cache.delete(key.as_bytes());
        }
        let sparse = cache.segment_occupancy();

        // The load-bearing assertion. Any cache-wide ratio -- however it is
        // computed -- is a single number, so it can only ever populate a
        // single bucket. Two modes several deciles apart cannot come from a
        // mean, which is what makes this reject the averaged version rather
        // than merely agreeing with it.
        let occupied: Vec<usize> = (0..10).filter(|&i| sparse[i] > 0).collect();
        let span = occupied.last().unwrap() - occupied.first().unwrap();
        assert!(
            span >= 5,
            "draining a prefix leaves emptied segments far below untouched \
             ones, and a mean could not show that spread: {full:?} became \
             {sparse:?}"
        );
        // ...and the spread has to be caused by the deletions, not merely
        // present beforehand. The partially-filled tail segment already sits
        // well below the sealed ones, so a version reading `write_offset`
        // instead of `live_bytes` would satisfy the span check while being
        // blind to every delete.
        assert!(
            sparse[0] > full[0],
            "emptied segments must fall into the bottom decile, which bytes \
             written rather than bytes live would never show: {full:?} \
             became {sparse:?}"
        );
    }

    /// The three byte figures must tell apart the two things that look
    /// identical in a resident-item count.
    ///
    /// Overwriting the same keys leaves superseded copies behind: the
    /// segments stay just as full of written bytes, but fewer of those bytes
    /// are live. A resident-item count cannot see that -- the live set is
    /// unchanged -- while `live / written` drops. That is the distinction
    /// the metric exists to make.
    #[test]
    fn written_and_live_bytes_separate_packing_from_reclamation() {
        let cache = create_test_cache_with_reclaim(OverwriteReclaim::Deferred);
        let value = vec![0xABu8; 1024];
        for i in 0..150u32 {
            let key = format!("key-{i:08}");
            if cache
                .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .is_ok()
            {
                let _ = cache.get(key.as_bytes());
            }
        }
        let (live_before, written_before, capacity) = cache.resident_bytes();
        assert!(capacity > 0, "a built cache has segment capacity");
        assert!(
            live_before > 0 && written_before >= live_before,
            "live {live_before} cannot exceed written {written_before}"
        );

        // Same keys, same sizes: the live set does not grow, but each write
        // appends a new copy and strands the previous one.
        for _round in 0..12 {
            for i in 0..150u32 {
                let key = format!("key-{i:08}");
                let _ = cache.set(key.as_bytes(), &value, b"", Duration::from_secs(3600));
            }
        }
        let (live_after, written_after, _) = cache.resident_bytes();

        let before = live_before as f64 / written_before as f64;
        let after = live_after as f64 / written_after as f64;
        assert!(
            after < before,
            "overwriting should lower the live share of written bytes: \
             {after:.3} against {before:.3}"
        );
    }

    /// Every resident item must be reachable through the hashtable.
    ///
    /// A lookup that loses an item it still holds produces exactly the
    /// signature that is otherwise hard to explain: residency unchanged,
    /// miss ratio higher. The item stays live in its segment, counted by
    /// `resident_items`, while reads for it miss -- so capacity looks fine
    /// and hit ratio does not.
    ///
    /// Sized to fit without eviction, so any shortfall is the index losing
    /// track rather than the policy discarding.
    #[test]
    fn every_resident_item_is_reachable_by_lookup() {
        let cache = create_test_cache();
        let value = vec![0xABu8; 256];

        let mut stored = 0u64;
        for i in 0..400u32 {
            let key = format!("reach-{i:08}");
            if cache
                .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .is_ok()
            {
                stored += 1;
            }
        }

        let mut found = 0u64;
        for i in 0..400u32 {
            let key = format!("reach-{i:08}");
            if cache.get(key.as_bytes()).is_some() {
                found += 1;
            }
        }

        let resident = cache.resident_items();
        assert_eq!(
            found,
            resident,
            "{resident} items are resident but only {found} read back; \
             {} are held but unreachable",
            resident.saturating_sub(found)
        );
        assert_eq!(found, stored, "{stored} stored, {found} read back");
    }

    /// The same invariant after items have been relocated.
    ///
    /// CURRENTLY FAILS for the same reason as
    /// `every_resident_item_should_be_reachable_after_merge`, which
    /// isolates it to merge rather than to demotion: the single-layer case
    /// with zero demotions strands items too. Ignored so it does not break
    /// the suite while the bug stands.
    ///
    /// Merge copies survivors into a new segment and demotion moves them
    /// between layers; both must re-point the index. A relocation that
    /// updated the segment but not the hashtable would leave the item live
    /// and counted while reads for it miss, and the unpressured case above
    /// would never catch it because nothing moves there.
    #[test]
    fn resident_items_stay_reachable_after_eviction_and_demotion() {
        let cache = create_test_cache();
        let value = vec![0xABu8; 1024];

        // Well past capacity, reading each key so it carries a frequency
        // and demotes rather than being discarded out of layer 0.
        for i in 0..3000u32 {
            let key = format!("moved-{i:08}");
            if cache
                .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .is_ok()
            {
                let _ = cache.get(key.as_bytes());
            }
        }

        let stats = cache.stats().snapshot();
        assert!(
            stats.evictions > 0,
            "the fixture must actually evict for this to test anything"
        );

        let mut found = 0u64;
        for i in 0..3000u32 {
            let key = format!("moved-{i:08}");
            if cache.get(key.as_bytes()).is_some() {
                found += 1;
            }
        }
        let resident = cache.resident_items();

        assert_eq!(
            found,
            resident,
            "after {} evictions and {} demotions: {resident} resident but {found} \
             reachable, so {} are held and unreachable",
            stats.evictions,
            stats.demotions,
            resident.saturating_sub(found)
        );
    }

    /// Every item the cache reports as resident should be reachable.
    ///
    /// CURRENTLY FAILS -- this is a bug report, not a guard. Ignored so it
    /// does not break the suite while it stands.
    ///
    /// Single layer with merge eviction and no demotion, which is the shape
    /// `policy = "merge"` builds. After eviction a fixed set of items stays
    /// live in its segments and counted by `resident_items` while lookups
    /// for it miss. About 126 items, roughly two segments' worth, largely
    /// independent of heap size: 25.6% of residency on a 16-segment cache,
    /// 1.6% on a 128-segment one. It persists -- further traffic leaves the
    /// shortfall at exactly the same absolute number.
    ///
    /// Two consequences. Reads for those items miss even though the bytes
    /// are held, which inflates miss ratio at unchanged capacity. And
    /// `resident_items` overstates the useful contents, so any cross-engine
    /// residency comparison using it is biased toward crucible by that
    /// margin.
    ///
    /// See `report_unreachable_resident_items_by_heap_size` for the numbers.
    #[test]
    fn every_resident_item_should_be_reachable_after_merge() {
        let hashtable = Arc::new(MultiChoiceHashtable::new(16));
        let layer = TtlLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(8 * 1024 * 1024)
            .spare_capacity(4)
            .config(
                LayerConfig::new()
                    .with_eviction_strategy(EvictionStrategy::Merge(MergeConfig::default())),
            )
            .build()
            .expect("ttl layer");
        let cache = TieredCacheBuilder::new(hashtable)
            .with_ttl_layer(layer)
            .eviction_threshold(1)
            .build();

        let value = vec![0xABu8; 1024];
        for i in 0..24000u32 {
            let key = format!("k-{i:08}");
            if cache
                .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .is_ok()
            {
                let _ = cache.get(key.as_bytes());
            }
        }
        let mut found = 0u64;
        for i in 0..24000u32 {
            let key = format!("k-{i:08}");
            if cache.get(key.as_bytes()).is_some() {
                found += 1;
            }
        }
        let resident = cache.resident_items();
        assert_eq!(
            found,
            resident,
            "{resident} resident but {found} reachable: {} items held outside the index",
            resident.saturating_sub(found)
        );
    }

    /// Does capacity drift away over many eviction cycles?
    ///
    /// The freed-segment counters were an accounting fault, not a leak --
    /// those segments are in the free queue and get reused. This checks
    /// that claim the only way that matters: run far more eviction cycles
    /// than the cache has segments and see whether usable capacity, or the
    /// items it holds, decays.
    #[test]
    #[ignore = "diagnostic: run explicitly with --ignored"]
    fn report_capacity_drift_over_eviction_cycles() {
        let hashtable = Arc::new(MultiChoiceHashtable::new(16));
        let layer = TtlLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(8 * 1024 * 1024)
            .spare_capacity(4)
            .config(
                LayerConfig::new()
                    .with_eviction_strategy(EvictionStrategy::Merge(MergeConfig::default())),
            )
            .build()
            .expect("ttl layer");
        let cache = TieredCacheBuilder::new(hashtable)
            .with_ttl_layer(layer)
            .eviction_threshold(1)
            .build();

        let value = vec![0xABu8; 1024];
        let mut written = 0u32;
        eprintln!(
            "  {:>9}{:>12}{:>11}{:>12}{:>10}",
            "writes", "evictions", "resident", "live MiB", "free segs"
        );
        for round in 1..=6u32 {
            for _ in 0..20000 {
                let key = format!("k-{written:08}");
                if cache
                    .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                    .is_ok()
                {
                    let _ = cache.get(key.as_bytes());
                }
                written += 1;
            }
            let (live, _w, _c) = cache.resident_bytes();
            eprintln!(
                "  {:>9}{:>12}{:>11}{:>12.2}{:>10}",
                round * 20000,
                cache.stats().snapshot().evictions,
                cache.resident_items(),
                live as f64 / (1024.0 * 1024.0),
                cache.ram_free_segment_count()
            );
        }
    }

    /// Where the unreachable items actually live, by segment state.
    ///
    /// `resident_items` walks every segment the pool addresses, so anything
    /// holding live items counts regardless of whether the chain still
    /// points at it. This says which state the strays are parked in, which
    /// is the difference between "pending release" and "leaked".
    #[test]
    #[ignore = "diagnostic: run explicitly with --ignored"]
    fn report_stranded_items_by_segment_state() {
        let hashtable = Arc::new(MultiChoiceHashtable::new(16));
        let layer = TtlLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(8 * 1024 * 1024)
            .spare_capacity(4)
            .config(
                LayerConfig::new()
                    .with_eviction_strategy(EvictionStrategy::Merge(MergeConfig::default())),
            )
            .build()
            .expect("ttl layer");
        let cache = TieredCacheBuilder::new(hashtable)
            .with_ttl_layer(layer)
            .eviction_threshold(1)
            .build();

        let value = vec![0xABu8; 1024];
        for i in 0..24000u32 {
            let key = format!("k-{i:08}");
            if cache
                .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .is_ok()
            {
                let _ = cache.get(key.as_bytes());
            }
        }
        let mut found = 0u64;
        for i in 0..24000u32 {
            let key = format!("k-{i:08}");
            if cache.get(key.as_bytes()).is_some() {
                found += 1;
            }
        }

        let layer = cache.layer(0).expect("layer 0");
        let mut by_state: std::collections::BTreeMap<String, (u32, u32)> =
            std::collections::BTreeMap::new();
        for id in 0..layer.total_segment_count() as u32 {
            if let Some(seg) = layer.get_segment(id) {
                let items = seg.live_items();
                if items == 0 {
                    continue;
                }
                let e = by_state
                    .entry(format!("{:?}", seg.state()))
                    .or_insert((0, 0));
                e.0 += 1;
                e.1 += items;
            }
        }
        eprintln!(
            "  resident {} / reachable {found} -> {} unreachable",
            cache.resident_items(),
            cache.resident_items().saturating_sub(found)
        );
        for (state, (segs, items)) in by_state {
            eprintln!("    {state:<16} {segs:>4} segments, {items:>7} live items");
        }
    }

    /// How many resident items are unreachable, across heap sizes.
    ///
    /// Single layer with merge eviction and no demotion -- the shape
    /// `policy = "merge"` actually builds. Reports rather than asserts,
    /// because the question being answered is whether the shortfall scales
    /// with the segment count or is an artifact of a cramped fixture.
    #[test]
    #[ignore = "diagnostic: run explicitly with --ignored"]
    fn report_unreachable_resident_items_by_heap_size() {
        for (heap_mb, power) in [(1usize, 12u8), (4, 14), (8, 16)] {
            let hashtable = Arc::new(MultiChoiceHashtable::new(power));
            let layer = TtlLayerBuilder::new()
                .layer_id(0)
                .pool_id(0)
                .segment_size(64 * 1024)
                .heap_size(heap_mb * 1024 * 1024)
                .spare_capacity(4)
                .config(
                    LayerConfig::new()
                        .with_eviction_strategy(EvictionStrategy::Merge(MergeConfig::default())),
                )
                .build()
                .expect("ttl layer");
            let cache = TieredCacheBuilder::new(hashtable)
                .with_ttl_layer(layer)
                .eviction_threshold(1)
                .build();

            let value = vec![0xABu8; 1024];
            let n = heap_mb as u32 * 3000;
            for i in 0..n {
                let key = format!("k-{i:08}");
                if cache
                    .set(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                    .is_ok()
                {
                    let _ = cache.get(key.as_bytes());
                }
            }
            let mut found = 0u64;
            for i in 0..n {
                let key = format!("k-{i:08}");
                if cache.get(key.as_bytes()).is_some() {
                    found += 1;
                }
            }
            let resident = cache.resident_items();
            let evictions = cache.stats().snapshot().evictions;
            let segments = heap_mb * 1024 / 64;
            eprintln!(
                "  {heap_mb} MiB ({segments} segments, {evictions} evictions): \
                 {resident} resident, {found} reachable, {} unreachable ({:.1}%)",
                resident.saturating_sub(found),
                100.0 * resident.saturating_sub(found) as f64 / resident.max(1) as f64
            );
        }
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
        let hashtable = Arc::new(MultiChoiceHashtable::new(10));

        let fifo_layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(256 * 1024)
            .spare_capacity(0) // No spare for tests
            .build()
            .expect("Failed to create FIFO layer");

        let cache = TieredCacheBuilder::new(hashtable)
            .with_layer(CacheLayer::Fifo(fifo_layer))
            .build();

        assert_eq!(cache.layer_count(), 1);
    }

    // TTL layer specific tests to improve coverage

    fn create_ttl_only_cache() -> TieredCache<MultiChoiceHashtable> {
        let hashtable = Arc::new(MultiChoiceHashtable::new(10));

        let ttl_layer = TtlLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(256 * 1024)
            .spare_capacity(0) // No spare for tests
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

    #[test]
    fn resident_items_counts_inserted_small_items_without_overcounting() {
        let cache = create_ttl_only_cache();

        let num_items = 20;
        for i in 0..num_items {
            let key = format!("resident_{i}");
            cache
                .set(key.as_bytes(), b"v", b"", Duration::from_secs(3600))
                .unwrap();
        }

        let resident = cache.resident_items();
        assert!(
            resident > 0,
            "expected resident_items > 0 after inserting {num_items} items, got {resident}"
        );
        assert!(
            resident <= num_items as u64,
            "expected resident_items <= {num_items} inserted items, got {resident}"
        );
    }

    #[test]
    fn ram_segment_counts_report_free_and_total_across_ram_layers() {
        let cache = create_test_cache();

        let total = cache.ram_total_segment_count();
        let free = cache.ram_free_segment_count();

        assert!(total > 0);
        assert!(free <= total);
        assert_eq!(
            total,
            cache.layer(0).unwrap().total_segment_count() as u64
                + cache.layer(1).unwrap().total_segment_count() as u64
        );

        // Write until layer 0 has a used segment, so free < total proves the
        // method reads live state rather than a value cached at construction.
        cache
            .set(b"k", b"v", b"", Duration::from_secs(3600))
            .unwrap();
        assert!(
            cache.ram_free_segment_count() < total,
            "expected a write to reduce free segments below the total"
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "file-backed mmap is unsupported under Miri")]
    fn ram_segment_counts_exclude_disk_layers() {
        use crate::disk::DiskLayerBuilder;

        let dir = tempfile::tempdir().expect("temp dir");

        let fifo_layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(256 * 1024) // 4 segments
            .spare_capacity(0)
            .build()
            .expect("fifo layer");
        let ttl_layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(64 * 1024)
            .heap_size(512 * 1024) // 8 segments
            .spare_capacity(0)
            .build()
            .expect("ttl layer");
        // Deliberately far larger than the RAM total (12 segments): if the
        // disk layer leaked into the RAM-only sum, this test would fail by a
        // large margin rather than an easily-missed small one.
        let disk_layer = DiskLayerBuilder::new()
            .layer_id(2)
            .pool_id(2)
            .segment_size(64 * 1024)
            .path(dir.path().join("disk.dat"))
            .size(64 * 64 * 1024) // 64 segments
            .build()
            .expect("disk layer");

        let cache: TieredCache<MultiChoiceHashtable> =
            TieredCacheBuilder::new(Arc::new(MultiChoiceHashtable::new(10)))
                .with_fifo_layer(fifo_layer)
                .with_ttl_layer(ttl_layer)
                .with_disk_layer(disk_layer)
                .build();

        let ram_total = cache.ram_total_segment_count();
        let expected_ram_total = cache.layer(0).unwrap().total_segment_count() as u64
            + cache.layer(1).unwrap().total_segment_count() as u64;
        let disk_total = cache.layer(2).unwrap().total_segment_count() as u64;

        assert_eq!(ram_total, expected_ram_total);
        assert!(
            ram_total < disk_total,
            "fixture must make the disk layer's segment count dwarf the RAM \
             total for this test to be meaningful (ram={ram_total}, disk={disk_total})"
        );
    }

    #[test]
    fn test_cas_token_lifecycle() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        // Missing key fails with KeyNotFound
        assert_eq!(
            cache.cas(b"counter", b"v1", b"", ttl, CasToken::from_raw(0)),
            Err(CacheError::KeyNotFound)
        );

        cache.set(b"counter", b"v1", b"", ttl).unwrap();
        let (value, token) = cache.get_with_cas(b"counter").unwrap();
        assert_eq!(value, b"v1");

        // A fresh token round-trips
        assert_eq!(cache.cas(b"counter", b"v2", b"", ttl, token), Ok(true));
        assert_eq!(cache.get(b"counter").unwrap(), b"v2");

        // The consumed token is now stale and must not match
        assert_eq!(cache.cas(b"counter", b"v3", b"", ttl, token), Ok(false));
        assert_eq!(cache.get(b"counter").unwrap(), b"v2");

        // A token invalidated by an intervening set fails and leaves the
        // newer value in place
        let (_, token) = cache.get_with_cas(b"counter").unwrap();
        cache.set(b"counter", b"v4", b"", ttl).unwrap();
        assert_eq!(cache.cas(b"counter", b"v5", b"", ttl, token), Ok(false));
        assert_eq!(cache.get(b"counter").unwrap(), b"v4");
    }

    #[test]
    fn test_increment_decrement_semantics() {
        let cache = create_test_cache();
        let ttl = Duration::from_secs(3600);

        // Missing key, no initial
        assert_eq!(
            cache.increment(b"n", 1, None, ttl),
            Err(CacheError::KeyNotFound)
        );
        assert_eq!(
            cache.decrement(b"n", 1, None, ttl),
            Err(CacheError::KeyNotFound)
        );

        // Missing key, initial provided: created with initial + delta
        assert_eq!(cache.increment(b"n", 2, Some(10), ttl), Ok(12));
        assert_eq!(cache.get(b"n").unwrap(), b"12");

        // Existing key: initial is ignored
        assert_eq!(cache.increment(b"n", 3, Some(100), ttl), Ok(15));

        // Decrement, including saturation at zero
        assert_eq!(cache.decrement(b"n", 5, None, ttl), Ok(10));
        assert_eq!(cache.decrement(b"n", 100, None, ttl), Ok(0));
        assert_eq!(cache.get(b"n").unwrap(), b"0");

        // Decrement creation saturates too
        assert_eq!(cache.decrement(b"m", 5, Some(3), ttl), Ok(0));

        // Non-numeric value
        cache.set(b"s", b"not a number", b"", ttl).unwrap();
        assert_eq!(
            cache.increment(b"s", 1, None, ttl),
            Err(CacheError::NotNumeric)
        );

        // Overflow
        cache
            .set(b"max", u64::MAX.to_string().as_bytes(), b"", ttl)
            .unwrap();
        assert_eq!(
            cache.increment(b"max", 1, None, ttl),
            Err(CacheError::Overflow)
        );
    }

    // Concurrent increments must not lose updates. This exercises both the
    // increment retry loop and the cas() publish linearization: with the
    // old get-then-set increment, or with cas() publishing via a plain
    // replace, concurrent updates are lost and the final count comes up
    // short (measured ~30-45% of updates surviving before the fix).
    #[test]
    fn test_increment_concurrent_no_lost_updates() {
        use std::sync::Arc as StdArc;

        const THREADS: usize = 4;
        const PER_THREAD: u64 = 250;

        let cache = StdArc::new(create_test_cache());
        let ttl = Duration::from_secs(3600);

        cache.set(b"counter", b"0", b"", ttl).unwrap();

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let cache = StdArc::clone(&cache);
                std::thread::spawn(move || {
                    for _ in 0..PER_THREAD {
                        cache.increment(b"counter", 1, None, ttl).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Eviction would invalidate the premise: `increment` with no initial
        // value correctly reports KeyNotFound for a key the cache discarded,
        // so an evicted counter surfaces as a confusing KeyNotFound panic in a
        // worker rather than as a wrong total here. Assert the precondition so
        // that failure names itself.
        //
        // This is what made the test flaky: the fixture built two layers but
        // left layer 0's `next_layer` unset, so `determine_item_fate` could
        // never demote and eviction discarded the counter outright.
        assert_eq!(
            cache.stats().evictions.load(Ordering::Relaxed),
            0,
            "the counter was evicted, so this run cannot say anything about lost updates"
        );

        let value = cache.get(b"counter").unwrap();
        let value: u64 = std::str::from_utf8(&value).unwrap().parse().unwrap();
        assert_eq!(value, THREADS as u64 * PER_THREAD);
    }
}
