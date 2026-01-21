//! Disk-backed layer implementation.
//!
//! [`DiskLayer`] provides a disk tier for the cache hierarchy,
//! using mmap'd file storage for segments.

use crate::config::{EvictionStrategy, LayerConfig};
use crate::disk::config::DiskConfig;
use crate::disk::file_pool::{FilePool, FilePoolBuilder};
use crate::error::{CacheError, CacheResult};
use crate::eviction::{ItemFate, determine_item_fate};
use crate::hashtable::{Hashtable, KeyVerifier};
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

/// Helper struct for verifying keys in segments.
struct SinglePoolVerifier<'a> {
    pool: &'a FilePool,
}

impl KeyVerifier for SinglePoolVerifier<'_> {
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let item_loc = ItemLocation::from_location(location);
        if let Some(segment) = self.pool.get(item_loc.segment_id()) {
            segment.verify_key_at_offset(item_loc.offset(), key, allow_deleted)
        } else {
            false
        }
    }
}

impl DiskLayer {
    /// Create a new disk layer builder.
    pub fn builder() -> DiskLayerBuilder {
        DiskLayerBuilder::new()
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

    /// Allocate a new segment and add it to the specified bucket.
    fn allocate_segment_for_bucket(&self, bucket_index: usize, ttl: Duration) -> CacheResult<u32> {
        let segment_id = self.pool.reserve().ok_or(CacheError::OutOfMemory)?;

        let segment = self.pool.get(segment_id).ok_or(CacheError::OutOfMemory)?;

        // Set segment expiration time
        let expire_at = Self::now_secs() + ttl.as_secs() as u32;
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

    /// Process items in an evicted segment.
    fn process_evicted_segment<H: Hashtable>(&self, segment_id: u32, hashtable: &H) {
        let segment = match self.pool.get(segment_id) {
            Some(s) => s,
            None => return,
        };

        // Wait for readers to finish
        while segment.ref_count() > 0 {
            std::hint::spin_loop();
        }

        // Transition to Locked for clearing
        segment.cas_metadata(State::Draining, State::Locked, None, None);

        // Process each item in the segment
        let mut offset = 0u32;
        let write_offset = segment.write_offset();

        while offset < write_offset {
            if let Some(data) = segment.data_slice(offset, BasicHeader::SIZE) {
                if let Some(header) = BasicHeader::try_from_bytes(data) {
                    let item_size = header.padded_size() as u32;

                    let key_start =
                        offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
                    let key_len = header.key_len() as usize;

                    if let Some(key) = segment.data_slice(key_start as u32, key_len)
                        && !header.is_deleted()
                    {
                        let location = ItemLocation::new(self.pool.pool_id(), segment_id, offset);

                        let verifier = SinglePoolVerifier { pool: &self.pool };
                        let freq = hashtable.get_frequency(key, &verifier).unwrap_or(0);

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
                if expire_at > 0 && now >= expire_at {
                    if let Ok(evicted_id) = bucket.evict_head_segment(&self.pool) {
                        self.process_evicted_segment(evicted_id, hashtable);
                        expired_count += 1;
                    }
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
                    return Ok(ItemLocation::new(self.pool.pool_id(), segment_id, offset));
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

        let segment = self.pool.get(location.segment_id())?;

        let state = segment.state();
        if !state.is_readable() {
            return None;
        }

        // Check segment-level TTL
        let now = Self::now_secs();
        let expire_at = segment.expire_at();
        if expire_at > 0 && now >= expire_at {
            return None;
        }

        // Verify key matches
        let header_info = segment.verify_key_unexpired(location.offset(), key, now)?;

        segment
            .get_item_verified(location.offset(), header_info)
            .ok()
    }

    fn mark_deleted(&self, location: ItemLocation) {
        if location.pool_id() != self.pool.pool_id() {
            return;
        }

        if let Some(segment) = self.pool.get(location.segment_id()) {
            let offset = location.offset();
            if let Some(data) = segment.data_slice(offset, BasicHeader::SIZE)
                && let Some(header) = BasicHeader::try_from_bytes(data)
            {
                let key_start =
                    offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
                let key_len = header.key_len() as usize;
                if let Some(key) = segment.data_slice(key_start as u32, key_len) {
                    let _ = segment.mark_deleted(offset, key);
                }
            }
        }
    }

    fn item_ttl(&self, location: ItemLocation) -> Option<Duration> {
        if location.pool_id() != self.pool.pool_id() {
            return None;
        }

        let segment = self.pool.get(location.segment_id())?;
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
                    let location = ItemLocation::new(self.pool.pool_id(), segment_id, offset);
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

        if let Some(segment) = self.pool.get(location.segment_id()) {
            segment.finalize_append(item_size);
        }
    }

    fn cancel_write_item(&self, location: ItemLocation) {
        if location.pool_id() != self.pool.pool_id() {
            return;
        }

        if let Some(segment) = self.pool.get(location.segment_id()) {
            segment.mark_deleted_at_offset(location.offset());
        }
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

        Ok(DiskLayer {
            layer_id: self.layer_id,
            config: self.config,
            pool,
            buckets: TtlBuckets::new(),
            current_write_segments,
        })
    }
}

impl Default for DiskLayerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::item::ItemGuard;
    use tempfile::tempdir;

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
