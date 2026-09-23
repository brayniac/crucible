//! Disk-backed layer implementation.
//!
//! [`DiskLayer`] provides a disk tier for the cache hierarchy,
//! using mmap'd file storage for segments.

use crate::config::{EvictionStrategy, LayerConfig};
use crate::disk::config::DiskConfig;
use crate::disk::file_pool::{FilePool, FilePoolBuilder};
use crate::error::{CacheError, CacheResult};
use crate::eviction::{ItemFate, determine_item_fate};
use crate::hashtable::Hashtable;
use crate::item::{BasicHeader, BasicItemGuard};
use crate::item_location::ItemLocation;
use crate::layer::Layer;
use crate::location::Location;
use crate::organization::TtlBuckets;
use crate::pool::RamPool;
use crate::segment::{Segment, SegmentGuard, SegmentKeyVerify};
use crate::state::State;
use std::path::Path;
use std::time::Duration;

/// A disk-backed layer for extended cache capacity.
///
/// DiskLayer provides a storage tier backed by memory-mapped files,
/// allowing cache sizes larger than available RAM. It uses the same
/// segment organization as RAM layers but with file-backed storage.
///
/// # Use Case
///
/// Layer 2 or lower in a tiered cache hierarchy:
/// - Receives items demoted from RAM layers (Layer 0/1)
/// - Items can be promoted back to RAM on read based on frequency
/// - Provides larger but slower storage tier
///
/// # File Layout
///
/// The disk layer stores segments in a single file:
/// ```text
/// +------------------+
/// | FilePoolHeader   |  64 bytes
/// +------------------+
/// | Segment 0        |  segment_size bytes
/// | Segment 1        |  segment_size bytes
/// | ...              |
/// +------------------+
/// ```
pub struct DiskLayer {
    /// Layer identifier.
    layer_id: u8,

    /// Layer configuration.
    config: LayerConfig,

    /// File-backed segment pool.
    pool: FilePool,

    /// TTL bucket organization.
    buckets: TtlBuckets,

    /// Current write segment ID per bucket.
    current_write_segments: Vec<std::sync::atomic::AtomicU32>,
}

impl DiskLayer {
    /// Create a new disk layer builder.
    pub fn builder() -> DiskLayerBuilder {
        DiskLayerBuilder::new()
    }

    /// Set this layer's demotion target.
    ///
    /// Called by `TieredCacheBuilder::build` to wire each layer to the next one
    /// added, so a tiered cache has a demotion path by default. Without one,
    /// `determine_item_fate` can never demote and eviction discards items
    /// instead of promoting hot ones.
    pub fn set_next_layer(&mut self, layer_id: crate::config::LayerId) {
        self.config.next_layer = Some(layer_id);
    }

    /// Get a reference to the segment pool.
    pub fn pool(&self) -> &FilePool {
        &self.pool
    }

    /// Get the TTL buckets.
    pub fn buckets(&self) -> &TtlBuckets {
        &self.buckets
    }

    /// Sync dirty segments to disk.
    pub fn sync(&self) -> std::io::Result<usize> {
        self.pool.sync()
    }

    /// Async sync dirty segments to disk.
    pub fn sync_async(&self) -> std::io::Result<usize> {
        self.pool.sync_async()
    }

    /// Get current time as coarse seconds.
    fn now_secs() -> u32 {
        clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs()
    }

    /// Reset the layer to its freshly built state: empty TTL buckets, no
    /// cached write segments, and every segment free.
    ///
    /// Backs [`crate::cache::TieredCache::flush`]. Resetting the pool alone is
    /// not enough: the buckets and the per-bucket write-segment cache would
    /// keep naming segments the pool had just recycled, and the next append
    /// onto that stale tail fails -- reported as `OutOfMemory` even with every
    /// segment free.
    ///
    /// Organization state is cleared before the pool so that a reader following
    /// a bucket chain during the window reaches segments that are still live
    /// rather than ones already back on the free queue.
    ///
    /// # Preconditions
    ///
    /// Only valid when no concurrent operation is touching this layer, and only
    /// after the hashtable has been cleared. Same precondition as
    /// [`crate::disk::FileSegment::force_free`], which this reaches through `reset_all`.
    pub fn reset(&self) {
        self.buckets.reset();
        for slot in &self.current_write_segments {
            slot.store(u32::MAX, std::sync::atomic::Ordering::Release);
        }
        self.pool.reset_all();
    }

    /// Allocate a new segment and add it to the specified bucket.
    fn allocate_segment_for_bucket(&self, bucket_index: usize, ttl: Duration) -> CacheResult<u32> {
        let segment_id = self.pool.reserve().ok_or(CacheError::OutOfMemory)?;

        let segment = self.pool.get(segment_id).ok_or(CacheError::OutOfMemory)?;

        // Set segment expiration time
        let expire_at = Self::now_secs().saturating_add(ttl.as_secs() as u32);
        segment.set_expire_at(expire_at);

        // Add to bucket
        let bucket = self.buckets.get_bucket_by_index(bucket_index);
        match bucket.append_segment(segment_id, &self.pool) {
            Ok(()) => {
                if bucket_index < self.current_write_segments.len() {
                    self.current_write_segments[bucket_index]
                        .store(segment_id, std::sync::atomic::Ordering::Release);
                }
                Ok(segment_id)
            }
            Err(_) => {
                self.pool.release(segment_id);
                Err(CacheError::OutOfMemory)
            }
        }
    }

    /// Get or allocate the write segment for a TTL.
    fn get_or_allocate_write_segment(&self, ttl: Duration) -> CacheResult<u32> {
        let bucket_index = self.buckets.get_bucket_index(ttl);
        let bucket = self.buckets.get_bucket_by_index(bucket_index);

        // Check cached write segment first
        if bucket_index < self.current_write_segments.len() {
            let cached_id = self.current_write_segments[bucket_index]
                .load(std::sync::atomic::Ordering::Acquire);
            if cached_id != u32::MAX
                && let Some(segment) = self.pool.get(cached_id)
                && segment.state() == State::Live
            {
                return Ok(cached_id);
            }
        }

        // Check bucket tail
        if let Some(tail_id) = bucket.tail()
            && let Some(segment) = self.pool.get(tail_id)
            && segment.state() == State::Live
        {
            if bucket_index < self.current_write_segments.len() {
                self.current_write_segments[bucket_index]
                    .store(tail_id, std::sync::atomic::Ordering::Release);
            }
            return Ok(tail_id);
        }

        // Need to allocate new segment
        self.allocate_segment_for_bucket(bucket_index, bucket.ttl())
    }

    /// Remove all hashtable entries for items in a segment, without releasing.
    fn drain_segment_from_hashtable<H: Hashtable>(&self, segment_id: u32, hashtable: &H) {
        let segment = match self.pool.get(segment_id) {
            Some(s) => s,
            None => return,
        };

        let mut offset = 0u32;
        let write_offset = segment.write_offset();

        while offset < write_offset {
            if let Some(data) = segment.header_ptr(offset, BasicHeader::SIZE) {
                if let Some(header) = unsafe { BasicHeader::try_from_ptr(data) } {
                    // The segment's stride, not the 8-byte padded body size: a scan
                    // that advances by anything but what the append advanced by
                    // desyncs after the first item on a coarser-aligned pool.
                    let item_size = segment.item_stride(header.padded_size());

                    let key_start =
                        offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
                    let key_len = header.key_len() as usize;

                    if let Some(key) = segment.data_slice(key_start as u32, key_len)
                        && !header.is_deleted()
                    {
                        let location = ItemLocation::new(
                            self.pool.layout(),
                            self.pool.pool_id(),
                            segment_id,
                            segment.incarnation(),
                            offset,
                        );
                        hashtable.remove(key, location.to_location());
                    }

                    offset += item_size;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    /// Process items in an evicted segment.
    fn process_evicted_segment<H: Hashtable>(&self, segment_id: u32, hashtable: &H) {
        let segment = match self.pool.get(segment_id) {
            Some(s) => s,
            None => return,
        };

        // Claim before counting (#133). `Draining` still admits key-verify
        // readers, so reading `ref_count` first and CASing to `Locked` only if
        // it was zero leaves a window for a pin to land in between -- and the
        // clearing loop below then runs under it. `Locked` refuses every fresh
        // reader, so a zero read after the claim is final. See
        // `layer::try_claim_for_clear`.
        let claimed = crate::layer::try_claim_for_clear(segment);
        if !claimed || segment.ref_count_seqcst() > 0 {
            // Drain hashtable entries and defer to the last reference out.
            // The sweep here removes entries outright and consults no
            // verifier, so it is indifferent to the claim.
            self.drain_segment_from_hashtable(segment_id, hashtable);
            let held = if claimed {
                State::Locked
            } else {
                State::Draining
            };
            // Unlike the old code this also runs the race fix, so a last
            // reader that dropped during the condemn window cannot strand the
            // segment in `AwaitingRelease` with `ref_count == 0`.
            crate::layer::condemn_and_reclaim(segment, held);
            return;
        }

        // Process each item in the segment
        let mut offset = 0u32;
        let write_offset = segment.write_offset();

        while offset < write_offset {
            if let Some(data) = segment.header_ptr(offset, BasicHeader::SIZE) {
                if let Some(header) = unsafe { BasicHeader::try_from_ptr(data) } {
                    // The segment's stride, not the 8-byte padded body size: a scan
                    // that advances by anything but what the append advanced by
                    // desyncs after the first item on a coarser-aligned pool.
                    let item_size = segment.item_stride(header.padded_size());

                    let key_start =
                        offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
                    let key_len = header.key_len() as usize;

                    if let Some(key) = segment.data_slice(key_start as u32, key_len)
                        && !header.is_deleted()
                    {
                        let location = ItemLocation::new(
                            self.pool.layout(),
                            self.pool.pool_id(),
                            segment_id,
                            segment.incarnation(),
                            offset,
                        );

                        // Location-matched, not verifier-backed (#138). This
                        // arm runs under the `Locked` claim taken above, and
                        // `State::admits_verify_reader` refuses `Locked` --
                        // so `get_frequency`, which resolves the key through
                        // `SinglePoolVerifier`, can only ever return `None`
                        // here and the `unwrap_or(0)` turned that refusal into
                        // a plausible-looking zero. Nothing failed and nothing
                        // logged; the frequency was simply always 0.
                        //
                        // `get_item_frequency` matches on the location alone,
                        // needs no verifier and is honest under any segment
                        // state. Its `None` now means what it says -- no live
                        // entry names this slot -- and 0 is the right answer
                        // for that, because both arms below are themselves
                        // location-matched and no-op on exactly the same
                        // condition.
                        let freq = hashtable
                            .get_item_frequency(key, location.to_location())
                            .unwrap_or(0);

                        let fate = determine_item_fate(freq, &self.config);

                        match fate {
                            ItemFate::Ghost => {
                                hashtable.convert_to_ghost(key, location.to_location());
                            }
                            ItemFate::Demote | ItemFate::Discard => {
                                // Disk is the last tier, so demote = discard
                                hashtable.remove(key, location.to_location());
                            }
                        }
                    }

                    offset += item_size;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        segment.cas_metadata(State::Locked, State::Reserved, None, None);
        self.pool.release(segment_id);
    }

    /// Try to evict expired segments.
    fn try_expire_segments<H: Hashtable>(&self, hashtable: &H) -> usize {
        let now = Self::now_secs();
        let mut expired_count = 0;

        for bucket in self.buckets.iter() {
            if bucket.segment_count() < 2 {
                continue;
            }

            if let Some(head_id) = bucket.head()
                && let Some(segment) = self.pool.get(head_id)
            {
                let expire_at = segment.expire_at();
                if expire_at > 0
                    && now >= expire_at
                    && let Ok(evicted_id) = bucket.evict_head_segment(&self.pool)
                {
                    self.process_evicted_segment(evicted_id, hashtable);
                    expired_count += 1;
                }
            }
        }

        expired_count
    }

    /// Default eviction: weighted random bucket selection.
    fn evict_randomfifo<H: Hashtable>(&self, hashtable: &H) -> bool {
        let (_, bucket) = match self.buckets.select_bucket_for_eviction() {
            Some(b) => b,
            None => return false,
        };

        match bucket.evict_head_segment(&self.pool) {
            Ok(segment_id) => {
                self.process_evicted_segment(segment_id, hashtable);
                true
            }
            Err(_) => false,
        }
    }
}

impl Layer for DiskLayer {
    type Guard<'a> = BasicItemGuard<'a>;

    fn config(&self) -> &LayerConfig {
        &self.config
    }

    fn layer_id(&self) -> u8 {
        self.layer_id
    }

    fn write_item(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        ttl: Duration,
    ) -> CacheResult<ItemLocation> {
        if key.len() > BasicHeader::MAX_KEY_LEN {
            return Err(CacheError::KeyTooLong);
        }
        if optional.len() > BasicHeader::MAX_OPTIONAL_LEN {
            return Err(CacheError::OptionalTooLong);
        }

        loop {
            let segment_id = self.get_or_allocate_write_segment(ttl)?;

            if let Some(segment) = self.pool.get(segment_id) {
                if let Some(offset) = segment.append_item(key, value, optional) {
                    return Ok(ItemLocation::new(
                        self.pool.layout(),
                        self.pool.pool_id(),
                        segment_id,
                        segment.incarnation(),
                        offset,
                    ));
                }

                // Segment is full
                let bucket_index = self.buckets.get_bucket_index(ttl);
                if bucket_index < self.current_write_segments.len() {
                    self.current_write_segments[bucket_index]
                        .store(u32::MAX, std::sync::atomic::Ordering::Release);
                }
            }

            // Allocate a new segment
            let bucket_index = self.buckets.get_bucket_index(ttl);
            let bucket = self.buckets.get_bucket_by_index(bucket_index);
            self.allocate_segment_for_bucket(bucket_index, bucket.ttl())?;
        }
    }

    fn get_item(&self, location: ItemLocation, key: &[u8]) -> Option<Self::Guard<'_>> {
        if location.pool_id() != self.pool.pool_id() {
            return None;
        }

        let (_, segment_id, _, offset) = location.unpack(self.pool.layout());
        let segment = self.pool.get(segment_id)?;

        let state = segment.state();
        // Condemned: stale location, answer is a miss (#127).
        if !state.is_readable() || state.is_condemned() {
            return None;
        }

        // Check segment-level TTL
        let now = Self::now_secs();
        let expire_at = segment.expire_at();
        if expire_at > 0 && now >= expire_at {
            return None;
        }

        // Verify key matches
        let header_info = segment.verify_key_unexpired(offset, key, now)?;

        segment.get_item_verified(offset, header_info).ok()
    }

    fn mark_deleted(&self, location: ItemLocation) {
        if location.pool_id() != self.pool.pool_id() {
            return;
        }

        let (_, segment_id, _, offset) = location.unpack(self.pool.layout());
        if let Some(segment) = self.pool.get(segment_id)
            && let Some(data) = segment.header_ptr(offset, BasicHeader::SIZE)
            && let Some(header) = unsafe { BasicHeader::try_from_ptr(data) }
        {
            let key_start = offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
            let key_len = header.key_len() as usize;
            if let Some(key) = segment.data_slice(key_start as u32, key_len) {
                let _ = segment.mark_deleted(offset, key);
            }
        }
    }

    fn item_ttl(&self, location: ItemLocation) -> Option<Duration> {
        if location.pool_id() != self.pool.pool_id() {
            return None;
        }

        let segment = self.pool.get(location.segment_id(self.pool.layout()))?;
        let now = Self::now_secs();
        segment.segment_ttl(now)
    }

    fn evict<H: Hashtable>(&self, hashtable: &H) -> bool {
        if self.try_expire_segments(hashtable) > 0 {
            return true;
        }

        match &self.config.eviction_strategy {
            EvictionStrategy::Merge(merge_config) => {
                // For disk tier, use simpler eviction (merge adds complexity)
                let _ = merge_config;
                self.evict_randomfifo(hashtable)
            }
            _ => self.evict_randomfifo(hashtable),
        }
    }

    fn evict_with_demoter<H, F>(&self, hashtable: &H, _demoter: F) -> bool
    where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, Location),
    {
        // Disk is the last tier, so there's nowhere to demote to.
        // Just use normal eviction.
        self.evict(hashtable)
    }

    fn expire<H: Hashtable>(&self, hashtable: &H) -> usize {
        self.try_expire_segments(hashtable)
    }

    fn free_segment_count(&self) -> usize {
        self.pool.free_count()
    }

    fn total_segment_count(&self) -> usize {
        self.pool.segment_count()
    }

    fn begin_write_item(
        &self,
        key: &[u8],
        value_len: usize,
        optional: &[u8],
        ttl: Duration,
    ) -> CacheResult<(ItemLocation, *mut u8, u32)> {
        if key.len() > BasicHeader::MAX_KEY_LEN {
            return Err(CacheError::KeyTooLong);
        }
        if optional.len() > BasicHeader::MAX_OPTIONAL_LEN {
            return Err(CacheError::OptionalTooLong);
        }

        loop {
            let segment_id = self.get_or_allocate_write_segment(ttl)?;

            if let Some(segment) = self.pool.get(segment_id) {
                if let Some((offset, item_size, value_ptr)) =
                    segment.begin_append(key, value_len, optional)
                {
                    let location = ItemLocation::new(
                        self.pool.layout(),
                        self.pool.pool_id(),
                        segment_id,
                        segment.incarnation(),
                        offset,
                    );
                    return Ok((location, value_ptr, item_size));
                }

                let bucket_index = self.buckets.get_bucket_index(ttl);
                if bucket_index < self.current_write_segments.len() {
                    self.current_write_segments[bucket_index]
                        .store(u32::MAX, std::sync::atomic::Ordering::Release);
                }
            }

            let bucket_index = self.buckets.get_bucket_index(ttl);
            let bucket = self.buckets.get_bucket_by_index(bucket_index);
            self.allocate_segment_for_bucket(bucket_index, bucket.ttl())?;
        }
    }

    fn finalize_write_item(&self, location: ItemLocation, item_size: u32) {
        if location.pool_id() != self.pool.pool_id() {
            return;
        }

        if let Some(segment) = self.pool.get(location.segment_id(self.pool.layout())) {
            segment.finalize_append(item_size);
        }
    }

    fn cancel_write_item(&self, location: ItemLocation) {
        if location.pool_id() != self.pool.pool_id() {
            return;
        }

        let (_, segment_id, _, offset) = location.unpack(self.pool.layout());
        if let Some(segment) = self.pool.get(segment_id) {
            segment.mark_deleted_at_offset(offset);
        }
    }

    fn mark_deleted_and_compact<H: Hashtable>(
        &self,
        location: ItemLocation,
        _hashtable: &H,
    ) -> bool {
        // Disk layer doesn't do compaction - just mark deleted
        self.mark_deleted(location);
        // A disk layer reclaims by rewriting whole segments, not by pairing them.
        false
    }

    fn mark_deleted_and_free_empty(&self, location: ItemLocation) {
        // Nor does it reclaim a file region early: space here is recovered
        // when the whole region is rewritten, so there is nothing an
        // overwrite can free on its own.
        self.mark_deleted(location);
    }
}

/// Builder for [`DiskLayer`].
pub struct DiskLayerBuilder {
    layer_id: u8,
    config: LayerConfig,
    pool_id: u8,
    segment_size: usize,
    path: std::path::PathBuf,
    size: usize,
    recover_on_startup: bool,
    sync_mode: crate::disk::SyncMode,
}

impl DiskLayerBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self {
            layer_id: 2,
            config: LayerConfig::new()
                .with_ghosts(true)
                .with_promotion_threshold(2),
            pool_id: 2,
            segment_size: 8 * 1024 * 1024, // 8MB
            path: std::path::PathBuf::from("/var/cache/crucible/disk.dat"),
            size: 10 * 1024 * 1024 * 1024, // 10GB
            recover_on_startup: true,
            sync_mode: crate::disk::SyncMode::default(),
        }
    }

    /// Set the layer ID.
    pub fn layer_id(mut self, id: u8) -> Self {
        self.layer_id = id;
        self
    }

    /// Set the layer configuration.
    pub fn config(mut self, config: LayerConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the pool ID (0-3).
    pub fn pool_id(mut self, id: u8) -> Self {
        self.pool_id = id;
        self
    }

    /// Set the segment size in bytes.
    pub fn segment_size(mut self, size: usize) -> Self {
        self.segment_size = size;
        self
    }

    /// Set the path for the disk cache file.
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        self.path = path.as_ref().to_path_buf();
        self
    }

    /// Set the total disk storage size in bytes.
    pub fn size(mut self, size: usize) -> Self {
        self.size = size;
        self
    }

    /// Set whether to recover from existing disk cache on startup.
    pub fn recover_on_startup(mut self, recover: bool) -> Self {
        self.recover_on_startup = recover;
        self
    }

    /// Set the sync mode for disk writes.
    pub fn sync_mode(mut self, mode: crate::disk::SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    /// Build from disk config.
    pub fn from_config(config: &DiskConfig, default_segment_size: usize) -> Self {
        let segment_size = config.segment_size.unwrap_or(default_segment_size);
        Self {
            layer_id: 2,
            config: LayerConfig::new()
                .with_ghosts(true)
                .with_promotion_threshold(config.promotion_threshold),
            pool_id: 2,
            segment_size,
            path: config.path.clone(),
            size: config.size,
            recover_on_startup: config.recover_on_startup,
            sync_mode: config.sync_mode,
        }
    }

    /// Build the disk layer.
    pub fn build(self) -> std::io::Result<DiskLayer> {
        let pool = FilePoolBuilder::new(self.pool_id)
            .per_item_ttl(false) // Disk layer uses segment-level TTL
            .segment_size(self.segment_size)
            .path(&self.path)
            .size(self.size)
            .create_new(!self.recover_on_startup)
            .sync_mode(self.sync_mode)
            .build()?;

        // Initialize cached write segments
        let bucket_count = crate::organization::MAX_TTL_BUCKETS;
        let current_write_segments: Vec<_> = (0..bucket_count)
            .map(|_| std::sync::atomic::AtomicU32::new(u32::MAX))
            .collect();

        let buckets = TtlBuckets::with_seed(self.config.eviction_seed);

        Ok(DiskLayer {
            layer_id: self.layer_id,
            config: self.config,
            pool,
            buckets,
            current_write_segments,
        })
    }
}

impl Default for DiskLayerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::hashtable::KeyVerifier;
    use crate::hashtable_impl::MultiChoiceHashtable;
    use crate::item::ItemGuard;
    use tempfile::tempdir;

    /// Resolve a key against this layer's pool, the way `TieredCache`'s own
    /// verifier would.
    ///
    /// This lives in the tests since #138. It used to sit beside the layer
    /// because `process_evicted_segment`'s exclusive arm looked frequencies up
    /// through it -- under the `Locked` claim, where
    /// `State::admits_verify_reader` refuses it, so every one of those lookups
    /// silently returned `None`. That call is location-matched now, and no
    /// production path in this layer resolves a key: `TieredCache` owns the
    /// hashtable and brings its own verifier.
    struct SinglePoolVerifier<'a> {
        pool: &'a FilePool,
    }

    impl KeyVerifier for SinglePoolVerifier<'_> {
        fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
            let item_loc = ItemLocation::from_location(location);
            let (_, segment_id, incarnation, offset) = item_loc.unpack(self.pool.layout());
            if let Some(segment) = self.pool.get(segment_id) {
                // Guard, then check the incarnation, then read -- see
                // `SegmentKeyVerify::verify_key_guarded`. The guard stops the
                // segment being recycled underneath the byte reads (#109).
                segment.verify_key_guarded(offset, key, allow_deleted, incarnation)
            } else {
                false
            }
        }
    }

    fn create_test_layer() -> (tempfile::TempDir, DiskLayer) {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_disk_layer.dat");

        let layer = DiskLayerBuilder::new()
            .layer_id(2)
            .pool_id(2)
            .segment_size(64 * 1024) // 64KB
            .path(&path)
            .size(640 * 1024) // 640KB = 10 segments
            .config(LayerConfig::new().with_ghosts(true))
            .build()
            .expect("Failed to create test layer");

        (dir, layer)
    }

    /// Fill one segment with `ITEMS` items and index them, returning the keys
    /// and the segment they share.
    fn fill_one_segment(layer: &DiskLayer, hashtable: &MultiChoiceHashtable) -> (Vec<String>, u32) {
        let verifier = SinglePoolVerifier { pool: &layer.pool };
        let ttl = Duration::from_secs(3600);

        const ITEMS: usize = 5;
        let mut keys = Vec::new();
        let mut segment_id = None;
        for i in 0..ITEMS {
            let key = format!("key_{i:02}");
            let location = layer
                .write_item(key.as_bytes(), b"value", b"", ttl)
                .expect("write");
            hashtable
                .insert(key.as_bytes(), location.to_location(), &verifier)
                .expect("insert");
            let id = location.segment_id(layer.pool.layout());
            assert_eq!(
                *segment_id.get_or_insert(id),
                id,
                "all items must share one segment for this to test the walk"
            );
            keys.push(key);
        }
        (keys, segment_id.expect("at least one item"))
    }

    fn assert_all_removed(layer: &DiskLayer, hashtable: &MultiChoiceHashtable, keys: &[String]) {
        let verifier = SinglePoolVerifier { pool: &layer.pool };
        for key in keys {
            assert!(
                hashtable.lookup(key.as_bytes(), &verifier).is_none(),
                "{key} survived the walk -- it stopped short of that item"
            );
        }
    }

    /// The demotion threshold the frequency tests configure their layer with.
    const DEMOTION_THRESHOLD: u8 = 4;

    /// A layer wired to a tier *below* it.
    ///
    /// This is what makes the frequency the exclusive eviction arm reads
    /// observable at all. With no `next_layer`, `determine_item_fate` never
    /// reaches its frequency comparison, so the arm's answer is the same for
    /// every value -- which is why the silent zero of #138 was latent, and why
    /// a test built on the default layer cannot tell a correct frequency from
    /// a constant. `set_next_layer` is the same call `TieredCacheBuilder::build`
    /// makes when a layer is added below this one.
    fn create_demoting_test_layer() -> (tempfile::TempDir, DiskLayer) {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_disk_layer.dat");

        let mut layer = DiskLayerBuilder::new()
            .layer_id(2)
            .pool_id(2)
            .segment_size(64 * 1024)
            .path(&path)
            .size(640 * 1024)
            .config(
                LayerConfig::new()
                    .with_ghosts(true)
                    .with_demotion_threshold(DEMOTION_THRESHOLD),
            )
            .build()
            .expect("Failed to create test layer");
        layer.set_next_layer(3);
        assert!(
            layer.config.next_layer.is_some(),
            "without a next layer `determine_item_fate` ignores the frequency \
             and this test proves nothing"
        );

        (dir, layer)
    }

    /// Write one item, read it back `reads` times to build frequency, then run
    /// it through the exclusive arm of `process_evicted_segment`.
    ///
    /// Returns the frequency the hashtable held going in, and the ghost
    /// frequency left behind afterwards -- `None` meaning the entry was
    /// removed outright, which on this layer is what demotion looks like
    /// (disk is the last tier, so `Demote` and `Discard` share an arm).
    ///
    /// One item, one hashtable: `get_ghost_frequency` matches on the 12-bit
    /// tag alone, so a second key in the table could answer for the first.
    fn evict_one_item(layer: &DiskLayer, reads: usize) -> (u8, Option<u8>) {
        let hashtable = MultiChoiceHashtable::new(10);
        let verifier = SinglePoolVerifier { pool: &layer.pool };
        let key = b"solo";

        let location = layer
            .write_item(key, b"value", b"", Duration::from_secs(3600))
            .expect("write");
        hashtable
            .insert(key, location.to_location(), &verifier)
            .expect("insert");

        // Reads go through the verifier, which is fine here -- the segment is
        // still `Live`, so `admits_verify_reader` allows it.
        // once however many there are. Each read gets its own second.
        // smoothed counter), so reads inside one second raise the frequency
        // The counter is rate-limited to one increment per epoch (Segcache's
        let _tick_clock = crate::clock::TestClock::start();
        for _ in 0..reads {
            _tick_clock.tick();
            assert!(
                hashtable.lookup(key, &verifier).is_some(),
                "the warming read must hit, or no frequency accrues"
            );
        }
        let freq = hashtable
            .get_item_frequency(key, location.to_location())
            .expect("the item must be indexed before eviction");

        // The evictor hands `process_evicted_segment` a segment already
        // unlinked into `Draining`.
        let segment_id = location.segment_id(layer.pool.layout());
        let segment = layer.pool.get(segment_id).expect("segment");
        let state = segment.state();
        assert!(segment.cas_metadata(state, State::Draining, None, None));

        layer.process_evicted_segment(segment_id, &hashtable);

        assert!(
            hashtable
                .get_item_frequency(key, location.to_location())
                .is_none(),
            "the item must not still be indexed as live at a segment that has \
             been recycled"
        );
        (freq, hashtable.get_ghost_frequency(key))
    }

    /// An item hot enough to demote must be *removed*, not ghosted.
    ///
    /// This is #138's red proof. The exclusive arm runs under the `Locked`
    /// claim it took a few lines earlier, and `State::admits_verify_reader`
    /// refuses `Locked` -- so `get_frequency`, which resolves the key through
    /// `SinglePoolVerifier`, returns `None` there every single time and the
    /// `unwrap_or(0)` that followed it turned the refusal into a
    /// plausible-looking zero. A zero is below any threshold, so a hot item
    /// took the ghost arm instead of the demote arm.
    ///
    /// Red proof: restore
    /// `hashtable.get_frequency(key, &SinglePoolVerifier { pool: &self.pool })`
    /// in place of the location-matched lookup and this fails with a ghost.
    #[test]
    fn eviction_demotes_an_item_whose_frequency_clears_the_threshold() {
        let (_dir, layer) = create_demoting_test_layer();

        let (freq, ghost) = evict_one_item(&layer, DEMOTION_THRESHOLD as usize + 2);

        assert!(
            freq >= DEMOTION_THRESHOLD,
            "the warming reads must carry the item past the threshold for this \
             to test the demote arm, got {freq}"
        );
        assert_eq!(
            ghost, None,
            "a hot item was ghosted instead of demoted -- the exclusive arm \
             read its frequency through the key verifier, which its own \
             `Locked` claim refuses, and `unwrap_or(0)` turned that into a \
             zero (#138)"
        );
    }

    /// The control: a cold item must still be ghosted.
    ///
    /// Without this the test above passes for any fix that reports every item
    /// as hot -- including replacing the lookup with a constant 255.
    #[test]
    fn eviction_ghosts_an_item_whose_frequency_is_below_the_threshold() {
        let (_dir, layer) = create_demoting_test_layer();

        let (freq, ghost) = evict_one_item(&layer, 0);

        assert!(
            freq < DEMOTION_THRESHOLD,
            "an unread item must sit below the threshold for this to test the \
             ghost arm, got {freq}"
        );
        assert_eq!(
            ghost,
            Some(freq),
            "a cold item must become a ghost, carrying its frequency with it"
        );
    }

    /// Tests that drive a race by hand through `segment::interpose`.
    ///
    /// Gated off under the model checkers for the same reason the hook itself
    /// is: a `std` thread-local inside a loom or shuttle execution is state
    /// the checker cannot see.
    #[cfg(all(not(feature = "loom"), not(feature = "shuttle")))]
    mod interposed {
        use super::*;
        use crate::segment::interpose;
        use std::cell::Cell;
        use std::rc::Rc;

        /// A segment pinned during the evictor's claim window must not be
        /// recycled -- the disk tier's copy of #133.
        ///
        /// Red proof: swap the two lines in
        /// `DiskLayer::process_evicted_segment` so the count is read before
        /// the claim.
        #[test]
        fn eviction_does_not_recycle_a_segment_pinned_while_it_was_claiming() {
            let (_dir, layer) = create_test_layer();
            let hashtable = MultiChoiceHashtable::new(10);
            let ttl = Duration::from_secs(3600);
            let value = vec![b'v'; 4096];
            for i in 0..24 {
                let key = format!("k{i:03}");
                let loc = layer
                    .write_item(key.as_bytes(), &value, b"", ttl)
                    .expect("write");
                let verifier = SinglePoolVerifier { pool: &layer.pool };
                let _ = hashtable.insert(key.as_bytes(), loc.to_location(), &verifier);
            }

            let pinned = Rc::new(Cell::new(None));
            {
                let flag = Rc::clone(&pinned);
                let pool_ptr: *const FilePool = &layer.pool;
                let _hook = interpose::install(Box::new(move |phase| {
                    if phase != interpose::CLAIM_BEFORE_CAS || flag.get().is_some() {
                        return;
                    }
                    // SAFETY: `layer` outlives this hook guard.
                    let pool = unsafe { &*pool_ptr };
                    for id in 0..pool.segment_count() as u32 {
                        if let Some(seg) = pool.get(id)
                            && seg.state() == State::Draining
                        {
                            assert!(seg.try_acquire_read());
                            flag.set(Some(id));
                            return;
                        }
                    }
                    panic!("the claim window must be entered with the segment still Draining");
                }));

                layer.evict(&hashtable);
            }

            let id = pinned.get().expect("the hook must have run");
            let seg = layer.pool.get(id).expect("segment");
            assert_eq!(seg.ref_count(), 1, "the reader is still pinned");
            assert_eq!(
                seg.state(),
                State::AwaitingRelease,
                "the disk evictor recycled a segment pinned during its claim -- its \
                 count was read before the claim, so the arrival was invisible (#133)"
            );
            seg.release_read();
            assert_eq!(
                seg.state(),
                State::Free,
                "the last reference out owes the AwaitingRelease -> Free handoff -- \
                 before #133 this path condemned without a race fix at all, so a reader \
                 that left during the condemn window stranded the segment"
            );
        }
    }

    /// A layer must accept writes again after `reset()`.
    ///
    /// `reset()` backs FLUSHALL. Resetting the pool alone leaves the TTL
    /// buckets -- and the per-bucket write-segment cache -- naming segments
    /// that are now `Free`, so `append_segment` cannot link onto that stale
    /// tail and the failure is reported as `OutOfMemory` even though every
    /// segment is free.
    #[test]
    fn test_layer_accepts_writes_after_reset() {
        let (_dir, layer) = create_test_layer();
        let ttl = Duration::from_secs(3600);
        let value = vec![b'v'; 4096];

        // Deep enough that the bucket chain spans several segments, so the
        // tail the reset must clear is not also the head.
        for i in 0..24 {
            let key = format!("pre{i:03}");
            layer
                .write_item(key.as_bytes(), &value, b"", ttl)
                .expect("pre-reset write");
        }
        assert!(
            layer.buckets.total_segment_count() > 1,
            "the fill must chain more than one segment for this to test the link"
        );

        layer.reset();

        assert_eq!(
            layer.buckets.total_segment_count(),
            0,
            "every bucket chain must be emptied"
        );
        assert!(
            layer
                .current_write_segments
                .iter()
                .all(|s| s.load(std::sync::atomic::Ordering::Acquire) == u32::MAX),
            "no bucket may keep a cached write segment across a reset"
        );

        for i in 0..24 {
            let key = format!("post{i:03}");
            layer
                .write_item(key.as_bytes(), &value, b"", ttl)
                .unwrap_or_else(|e| panic!("write {i} after reset failed: {e:?}"));
        }
    }

    /// Draining a pinned segment must visit every item, at the pool's stride.
    ///
    /// Disk pools align items to 512 bytes while `BasicHeader::padded_size()`
    /// rounds to 8. A walk that advances by the latter lands mid-item on its
    /// second iteration, reads a garbage header and stops -- silently, since
    /// both quantities are `u32`. Hashtable entries left behind after the
    /// segment is recycled is what that failure looks like from the outside.
    #[test]
    fn test_draining_a_segment_walks_every_item_at_the_disk_stride() {
        let (_dir, layer) = create_test_layer();
        assert_eq!(
            layer.pool.layout().align_bytes(),
            512,
            "disk pools align to a sector"
        );

        let hashtable = MultiChoiceHashtable::new(10);
        let (keys, segment_id) = fill_one_segment(&layer, &hashtable);

        layer.drain_segment_from_hashtable(segment_id, &hashtable);
        assert_all_removed(&layer, &hashtable, &keys);
    }

    /// The same for the unpinned path, which walks the segment itself rather
    /// than deferring to `drain_segment_from_hashtable`.
    #[test]
    fn test_evicting_a_segment_walks_every_item_at_the_disk_stride() {
        let (_dir, layer) = create_test_layer();

        let hashtable = MultiChoiceHashtable::new(10);
        let (keys, segment_id) = fill_one_segment(&layer, &hashtable);

        // The eviction path leaves its victim in `Draining`, which is the
        // state `process_evicted_segment` expects.
        let segment = layer.pool.get(segment_id).expect("segment");
        let state = segment.state();
        assert!(segment.cas_metadata(state, State::Draining, None, None));

        layer.process_evicted_segment(segment_id, &hashtable);
        assert_all_removed(&layer, &hashtable, &keys);
    }

    /// A location from a previous incarnation must not resolve, even though
    /// the key really is at that offset.
    ///
    /// That is the whole hazard: segments are append-only from a fixed start,
    /// so the n-th item of the new incarnation lands where the n-th item of the
    /// old one was. The key compare alone says yes; only the tag says no.
    #[test]
    fn test_verifier_rejects_a_stale_incarnation() {
        let (_dir, layer) = create_test_layer();
        let pool = &layer.pool;

        // Age every segment through a used incarnation, so the tag the write
        // stamps is non-zero and a predecessor tag exists to test against.
        let ids: Vec<u32> = (0..pool.segment_count())
            .map(|_| pool.reserve().expect("pool must have a free segment"))
            .collect();
        for &id in &ids {
            let segment = pool.get(id).expect("segment id came from the pool");
            assert!(segment.cas_metadata(State::Reserved, State::Locked, None, None));
            pool.release(id);
        }

        let live = layer
            .write_item(b"key", b"value", b"", Duration::from_secs(3600))
            .expect("write");
        let layout = pool.layout();
        let (pool_id, segment_id, tag, offset) = live.unpack(layout);
        assert_ne!(tag, 0, "the segment must be past its first incarnation");

        // Same segment, same offset, previous tag.
        let stale = ItemLocation::new(layout, pool_id, segment_id, tag - 1, offset);
        let verifier = SinglePoolVerifier { pool };

        assert!(
            verifier.verify(b"key", live.to_location(), false),
            "the current incarnation must resolve"
        );
        assert!(
            !verifier.verify(b"key", stale.to_location(), false),
            "a location from a previous incarnation must not resolve"
        );
    }

    #[test]
    fn test_layer_creation() {
        let (_dir, layer) = create_test_layer();
        assert_eq!(layer.layer_id(), 2);
        assert_eq!(layer.total_segment_count(), 10);
        assert_eq!(layer.free_segment_count(), 10);
    }

    #[test]
    fn test_write_and_get_item() {
        let (_dir, layer) = create_test_layer();

        let key = b"test_key";
        let value = b"test_value";
        let ttl = Duration::from_secs(3600);

        let location = layer.write_item(key, value, b"", ttl).unwrap();
        assert_eq!(location.pool_id(), 2);

        let guard = layer.get_item(location, key);
        assert!(guard.is_some());

        let guard = guard.unwrap();
        assert_eq!(guard.key(), key);
        assert_eq!(guard.value(), value);
    }

    #[test]
    fn test_mark_deleted() {
        let (_dir, layer) = create_test_layer();

        let key = b"delete_me";
        let location = layer
            .write_item(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        layer.mark_deleted(location);

        // Item may no longer be retrievable (depending on implementation)
    }

    #[test]
    fn test_sync() {
        let (_dir, layer) = create_test_layer();

        let _location = layer
            .write_item(b"key", b"value", b"", Duration::from_secs(3600))
            .unwrap();

        let synced = layer.sync().expect("Sync should succeed");
        assert!(synced >= 1);
    }

    #[test]
    fn test_builder_defaults() {
        let builder = DiskLayerBuilder::default();
        assert_eq!(builder.layer_id, 2);
        assert_eq!(builder.pool_id, 2);
    }

    #[test]
    fn test_builder_static_method() {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("builder_test.dat");

        let layer = DiskLayer::builder()
            .layer_id(3)
            .pool_id(3)
            .segment_size(64 * 1024)
            .path(&path)
            .size(128 * 1024)
            .build()
            .expect("Should build");

        assert_eq!(layer.layer_id(), 3);
        assert_eq!(layer.pool().pool_id(), 3);
    }

    #[test]
    fn test_evict_empty_layer() {
        use crate::hashtable_impl::MultiChoiceHashtable;

        let (_dir, layer) = create_test_layer();
        let hashtable = MultiChoiceHashtable::new(10);

        let evicted = layer.evict(&hashtable);
        assert!(!evicted);
    }
}
