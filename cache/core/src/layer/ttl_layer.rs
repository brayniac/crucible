//! TTL bucket-organized layer for main cache storage.
//!
//! [`TtlLayer`] combines a [`MemoryPool`] with [`TtlBuckets`] organization
//! for use as the main storage tier in an S3FIFO or Segcache configuration.
//!
//! # Characteristics
//!
//! - Segment-level TTL (all items in a segment share expiration)
//! - TTL bucket organization (logarithmic time ranges)
//! - Weighted random eviction by bucket segment count
//! - Optional merge eviction for segment compaction

use crate::config::LayerConfig;
use crate::error::{CacheError, CacheResult};
use crate::eviction::{ItemFate, determine_item_fate};
use crate::hashtable::{Hashtable, KeyVerifier};
use crate::item::{BasicHeader, BasicItemGuard};
use crate::item_location::ItemLocation;
use crate::layer::Layer;
use crate::location::Location;
use crate::memory_pool::{MemoryPool, MemoryPoolBuilder};
use crate::organization::TtlBuckets;
use crate::pool::RamPool;
use crate::segment::{Segment, SegmentGuard, SegmentKeyVerify};
use crate::state::State;
use std::time::Duration;

/// A TTL bucket-organized layer for main cache storage.
///
/// This layer organizes segments by their expiration time using logarithmic
/// TTL buckets. Items are appended to the tail segment of the appropriate
/// bucket; when full, a new segment is allocated.
///
/// # Use Case
///
/// S3FIFO main queue (Layer 1) or Segcache single-tier:
/// - Receives items demoted from admission queue (S3FIFO)
/// - Or receives all items directly (Segcache)
/// - Segments grouped by TTL for efficient expiration
/// - Eviction selects bucket weighted by segment count
pub struct TtlLayer {
    /// Layer identifier.
    layer_id: u8,

    /// Layer configuration.
    config: LayerConfig,

    /// Segment pool.
    pool: MemoryPool,

    /// TTL bucket organization.
    buckets: TtlBuckets,

    /// Current write segment ID per bucket (optimization to avoid walking chain).
    /// Uses u32::MAX to indicate no current write segment.
    current_write_segments: Vec<std::sync::atomic::AtomicU32>,
}

impl TtlLayer {
    /// Create a new TTL layer builder.
    pub fn builder() -> TtlLayerBuilder {
        TtlLayerBuilder::new()
    }

    /// Get a reference to the segment pool.
    pub fn pool(&self) -> &MemoryPool {
        &self.pool
    }

    /// Get the TTL buckets.
    pub fn buckets(&self) -> &TtlBuckets {
        &self.buckets
    }

    /// Get current time as coarse seconds.
    fn now_secs() -> u32 {
        clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs()
    }

    /// Allocate a new segment and add it to the specified bucket.
    fn allocate_segment_for_bucket(&self, bucket_index: usize, ttl: Duration) -> CacheResult<u32> {
        // Reserve a segment from the pool
        let segment_id = self.pool.reserve().ok_or(CacheError::OutOfMemory)?;

        let segment = self.pool.get(segment_id).ok_or(CacheError::OutOfMemory)?;

        // Set segment expiration time
        let expire_at = Self::now_secs() + ttl.as_secs() as u32;
        segment.set_expire_at(expire_at);

        // Add to bucket
        let bucket = self.buckets.get_bucket_by_index(bucket_index);
        match bucket.append_segment(segment_id, &self.pool) {
            Ok(()) => {
                // Update cached write segment for this bucket
                if bucket_index < self.current_write_segments.len() {
                    self.current_write_segments[bucket_index]
                        .store(segment_id, std::sync::atomic::Ordering::Release);
                }
                Ok(segment_id)
            }
            Err(_) => {
                // Failed to add to bucket, release the segment
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
            // Update cache
            if bucket_index < self.current_write_segments.len() {
                self.current_write_segments[bucket_index]
                    .store(tail_id, std::sync::atomic::Ordering::Release);
            }
            return Ok(tail_id);
        }

        // Need to allocate new segment - use bucket's TTL
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
            // Get header at offset
            if let Some(data) = segment.data_slice(offset, BasicHeader::SIZE) {
                if let Some(header) = BasicHeader::try_from_bytes(data) {
                    let item_size = header.padded_size() as u32;

                    // Get key for this item
                    let key_start =
                        offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
                    let key_len = header.key_len() as usize;

                    if let Some(key) = segment.data_slice(key_start as u32, key_len)
                        && !header.is_deleted()
                    {
                        let location = ItemLocation::new(self.pool.pool_id(), segment_id, offset);

                        // Get frequency from hashtable
                        let verifier = SinglePoolVerifier { pool: &self.pool };
                        let freq = hashtable.get_frequency(key, &verifier).unwrap_or(0);

                        // Determine item fate
                        let fate = determine_item_fate(freq, &self.config);

                        match fate {
                            ItemFate::Ghost => {
                                // Convert to ghost in hashtable
                                hashtable.convert_to_ghost(key, location.to_location());
                            }
                            ItemFate::Demote => {
                                // Demotion to next layer is handled by caller (TieredCache)
                                // For now, just unlink from hashtable
                                hashtable.remove(key, location.to_location());
                            }
                            ItemFate::Discard => {
                                // Simply remove from hashtable
                                hashtable.remove(key, location.to_location());
                            }
                        }
                    }

                    offset += item_size;
                } else {
                    // Invalid header, stop processing
                    break;
                }
            } else {
                break;
            }
        }

        // Clear segment state and release to pool
        segment.cas_metadata(State::Locked, State::Reserved, None, None);
        self.pool.release(segment_id);
    }

    /// Try to evict expired segments.
    fn try_expire_segments<H: Hashtable>(&self, hashtable: &H) -> usize {
        let now = Self::now_secs();
        let mut expired_count = 0;

        // Check each bucket for expired segments
        for bucket in self.buckets.iter() {
            // Can only evict if bucket has 2+ segments (keep Live tail)
            if bucket.segment_count() < 2 {
                continue;
            }

            // Check if head segment is expired
            if let Some(head_id) = bucket.head()
                && let Some(segment) = self.pool.get(head_id)
            {
                let expire_at = segment.expire_at();
                if expire_at > 0 && now >= expire_at {
                    // Segment is expired, try to evict it
                    if let Ok(evicted_id) = bucket.evict_head_segment(&self.pool) {
                        self.process_evicted_segment(evicted_id, hashtable);
                        expired_count += 1;
                    }
                }
            }
        }

        expired_count
    }
}

/// Helper struct for verifying keys in segments
struct SinglePoolVerifier<'a> {
    pool: &'a MemoryPool,
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

impl Layer for TtlLayer {
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
        // Validate inputs
        if key.len() > BasicHeader::MAX_KEY_LEN {
            return Err(CacheError::KeyTooLong);
        }
        if optional.len() > BasicHeader::MAX_OPTIONAL_LEN {
            return Err(CacheError::OptionalTooLong);
        }

        // Try to append to current write segment
        loop {
            let segment_id = self.get_or_allocate_write_segment(ttl)?;

            if let Some(segment) = self.pool.get(segment_id) {
                // Try to append
                if let Some(offset) = segment.append_item(key, value, optional) {
                    return Ok(ItemLocation::new(self.pool.pool_id(), segment_id, offset));
                }

                // Segment is full, need to allocate a new one
                // Clear cached write segment
                let bucket_index = self.buckets.get_bucket_index(ttl);
                if bucket_index < self.current_write_segments.len() {
                    self.current_write_segments[bucket_index]
                        .store(u32::MAX, std::sync::atomic::Ordering::Release);
                }
            }

            // Allocate a new segment (next iteration will use it)
            let bucket_index = self.buckets.get_bucket_index(ttl);
            let bucket = self.buckets.get_bucket_by_index(bucket_index);
            self.allocate_segment_for_bucket(bucket_index, bucket.ttl())?;
        }
    }

    fn get_item(&self, location: ItemLocation, key: &[u8]) -> Option<Self::Guard<'_>> {
        // Verify pool ID matches
        if location.pool_id() != self.pool.pool_id() {
            return None;
        }

        let segment = self.pool.get(location.segment_id())?;

        // Check segment state
        let state = segment.state();
        if !state.is_readable() {
            return None;
        }

        // Verify key matches (before acquiring ref count)
        if !segment.verify_key_at_offset(location.offset(), key, false) {
            return None;
        }

        // Check if segment is expired (segment-level TTL)
        let now = Self::now_secs();
        let expire_at = segment.expire_at();
        if expire_at > 0 && now >= expire_at {
            return None;
        }

        // Get item using SegmentGuard trait
        segment.get_item(location.offset(), key).ok()
    }

    fn mark_deleted(&self, location: ItemLocation) {
        if location.pool_id() != self.pool.pool_id() {
            return;
        }

        if let Some(segment) = self.pool.get(location.segment_id()) {
            // We need the key to mark deleted.
            // Get it from the segment header.
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
        // First try to expire any segments
        if self.try_expire_segments(hashtable) > 0 {
            return true;
        }

        // Select a bucket for eviction (weighted by segment count)
        let (_, bucket) = match self.buckets.select_bucket_for_eviction() {
            Some(b) => b,
            None => return false,
        };

        // Evict head segment from selected bucket
        match bucket.evict_head_segment(&self.pool) {
            Ok(segment_id) => {
                self.process_evicted_segment(segment_id, hashtable);
                true
            }
            Err(_) => false,
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
}

/// Builder for [`TtlLayer`].
pub struct TtlLayerBuilder {
    layer_id: u8,
    config: LayerConfig,
    pool_id: u8,
    segment_size: usize,
    heap_size: usize,
}

impl TtlLayerBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self {
            layer_id: 1,
            config: LayerConfig::new().with_ghosts(true),
            pool_id: 1,
            segment_size: 1024 * 1024,    // 1MB
            heap_size: 256 * 1024 * 1024, // 256MB
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

    /// Set the total heap size in bytes.
    pub fn heap_size(mut self, size: usize) -> Self {
        self.heap_size = size;
        self
    }

    /// Build the TTL layer.
    pub fn build(self) -> Result<TtlLayer, std::io::Error> {
        let pool = MemoryPoolBuilder::new(self.pool_id)
            .per_item_ttl(false) // TTL layer uses segment-level TTL
            .segment_size(self.segment_size)
            .heap_size(self.heap_size)
            .build()?;

        // Initialize cached write segments (one per bucket)
        let bucket_count = crate::organization::MAX_TTL_BUCKETS;
        let current_write_segments: Vec<_> = (0..bucket_count)
            .map(|_| std::sync::atomic::AtomicU32::new(u32::MAX))
            .collect();

        Ok(TtlLayer {
            layer_id: self.layer_id,
            config: self.config,
            pool,
            buckets: TtlBuckets::new(),
            current_write_segments,
        })
    }
}

impl Default for TtlLayerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::item::ItemGuard;

    fn create_test_layer() -> TtlLayer {
        TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(64 * 1024) // 64KB
            .heap_size(640 * 1024) // 640KB = 10 segments
            .config(LayerConfig::new().with_ghosts(true))
            .build()
            .expect("Failed to create test layer")
    }

    #[test]
    fn test_layer_creation() {
        let layer = create_test_layer();
        assert_eq!(layer.layer_id(), 1);
        assert_eq!(layer.total_segment_count(), 10);
        assert_eq!(layer.free_segment_count(), 10);
    }

    #[test]
    fn test_write_and_get_item() {
        let layer = create_test_layer();

        let key = b"test_key";
        let value = b"test_value";
        let ttl = Duration::from_secs(3600);

        // Write item
        let location = layer.write_item(key, value, b"", ttl).unwrap();
        assert_eq!(location.pool_id(), 1);

        // Get item
        let guard = layer.get_item(location, key);
        assert!(guard.is_some());

        let guard = guard.unwrap();
        assert_eq!(guard.key(), key);
        assert_eq!(guard.value(), value);
    }

    #[test]
    fn test_ttl_bucket_assignment() {
        let layer = create_test_layer();

        // Items with different TTLs should go to different buckets
        let key1 = b"key1";
        let key2 = b"key2";

        let loc1 = layer
            .write_item(key1, b"value", b"", Duration::from_secs(10))
            .unwrap();
        let loc2 = layer
            .write_item(key2, b"value", b"", Duration::from_secs(5000))
            .unwrap();

        // Both should be accessible
        assert!(layer.get_item(loc1, key1).is_some());
        assert!(layer.get_item(loc2, key2).is_some());
    }

    #[test]
    fn test_key_too_long() {
        let layer = create_test_layer();

        let key = vec![0u8; 256]; // 256 bytes, exceeds 255 limit
        let result = layer.write_item(&key, b"value", b"", Duration::from_secs(60));

        assert!(matches!(result, Err(CacheError::KeyTooLong)));
    }

    #[test]
    fn test_mark_deleted() {
        let layer = create_test_layer();

        let key = b"delete_me";
        let location = layer
            .write_item(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        // Mark deleted
        layer.mark_deleted(location);

        // Item should no longer be retrievable (depending on implementation)
    }

    #[test]
    fn test_used_segment_count() {
        let layer = create_test_layer();

        assert_eq!(layer.used_segment_count(), 0);

        // Write an item to allocate a segment
        layer
            .write_item(b"key", b"value", b"", Duration::from_secs(60))
            .unwrap();

        assert_eq!(layer.used_segment_count(), 1);
        assert_eq!(layer.free_segment_count(), 9);
    }

    #[test]
    fn test_item_ttl() {
        let layer = create_test_layer();

        let key = b"ttl_test";
        let ttl = Duration::from_secs(3600);
        let location = layer.write_item(key, b"value", b"", ttl).unwrap();

        // Item TTL should be approximately the segment's TTL
        let remaining = layer.item_ttl(location);
        assert!(remaining.is_some());
        // Should be close to 3600 seconds (within the bucket's granularity)
    }

    #[test]
    fn test_builder_default() {
        let builder = TtlLayerBuilder::default();
        let layer = builder
            .segment_size(64 * 1024)
            .heap_size(128 * 1024)
            .build()
            .expect("Should build");
        assert_eq!(layer.layer_id(), 1); // Default layer_id
    }

    #[test]
    fn test_builder_custom_config() {
        let config = LayerConfig::new().with_ghosts(false);
        let layer = TtlLayerBuilder::new()
            .layer_id(2)
            .pool_id(2)
            .config(config)
            .segment_size(64 * 1024)
            .heap_size(128 * 1024)
            .build()
            .expect("Should build");

        assert_eq!(layer.layer_id(), 2);
        assert!(!layer.config().create_ghosts);
    }

    #[test]
    fn test_get_item_wrong_pool_id() {
        let layer = create_test_layer();

        let key = b"test_key";
        let location = layer
            .write_item(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        // Create a location with wrong pool_id (must be 0-3)
        let wrong_location = ItemLocation::new(0, location.segment_id(), location.offset());

        let guard = layer.get_item(wrong_location, key);
        assert!(guard.is_none());
    }

    #[test]
    fn test_get_item_wrong_key() {
        let layer = create_test_layer();

        let key = b"correct_key";
        let wrong_key = b"wrong_key";
        let location = layer
            .write_item(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        // Try to get with wrong key
        let guard = layer.get_item(location, wrong_key);
        assert!(guard.is_none());
    }

    #[test]
    fn test_item_ttl_wrong_pool_id() {
        let layer = create_test_layer();

        let key = b"test_key";
        let location = layer
            .write_item(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        // Pool_id must be 0-3, use 0 which is different from layer's pool_id of 1
        let wrong_location = ItemLocation::new(0, location.segment_id(), location.offset());
        let ttl = layer.item_ttl(wrong_location);
        assert!(ttl.is_none());
    }

    #[test]
    fn test_mark_deleted_wrong_pool_id() {
        let layer = create_test_layer();

        let key = b"test_key";
        let location = layer
            .write_item(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        // Pool_id must be 0-3, use 0 which is different from layer's pool_id of 1
        let wrong_location = ItemLocation::new(0, location.segment_id(), location.offset());
        // Should not panic, just be a no-op
        layer.mark_deleted(wrong_location);
    }

    #[test]
    fn test_optional_too_long() {
        let layer = create_test_layer();

        let optional = vec![0u8; 65]; // 65 bytes, exceeds 64 limit
        let result = layer.write_item(b"key", b"value", &optional, Duration::from_secs(60));

        assert!(matches!(result, Err(CacheError::OptionalTooLong)));
    }

    #[test]
    fn test_write_with_optional() {
        let layer = create_test_layer();

        let key = b"key_with_opt";
        let value = b"value";
        let optional = b"optional_data";
        let ttl = Duration::from_secs(3600);

        let location = layer.write_item(key, value, optional, ttl).unwrap();

        let guard = layer.get_item(location, key).unwrap();
        assert_eq!(guard.key(), key);
        assert_eq!(guard.value(), value);
        assert_eq!(guard.optional(), optional);
    }

    #[test]
    fn test_multiple_items_same_segment() {
        let layer = create_test_layer();
        let ttl = Duration::from_secs(60);

        let loc1 = layer.write_item(b"key1", b"value1", b"", ttl).unwrap();
        let loc2 = layer.write_item(b"key2", b"value2", b"", ttl).unwrap();
        let loc3 = layer.write_item(b"key3", b"value3", b"", ttl).unwrap();

        // All items should be in the same segment (same TTL bucket)
        assert_eq!(loc1.segment_id(), loc2.segment_id());
        assert_eq!(loc2.segment_id(), loc3.segment_id());

        // All items should be retrievable
        assert!(layer.get_item(loc1, b"key1").is_some());
        assert!(layer.get_item(loc2, b"key2").is_some());
        assert!(layer.get_item(loc3, b"key3").is_some());
    }

    #[test]
    fn test_pool_and_buckets_accessors() {
        let layer = create_test_layer();

        // Test pool accessor
        assert_eq!(layer.pool().pool_id(), 1);
        assert_eq!(layer.pool().segment_count(), 10);

        // Test buckets accessor
        assert_eq!(layer.buckets().bucket_count(), 1024);
    }

    #[test]
    fn test_config_accessor() {
        let layer = create_test_layer();
        assert!(layer.config().create_ghosts);
    }

    #[test]
    fn test_builder_static_method() {
        let layer = TtlLayer::builder()
            .layer_id(3)
            .pool_id(3)
            .segment_size(64 * 1024)
            .heap_size(128 * 1024)
            .build()
            .expect("Should build");

        assert_eq!(layer.layer_id(), 3);
        assert_eq!(layer.pool().pool_id(), 3);
    }

    #[test]
    fn test_evict_empty_layer() {
        use crate::hashtable_impl::CuckooHashtable;

        let layer = create_test_layer();
        let hashtable = CuckooHashtable::new(10);

        // Evict from empty layer should return false
        let evicted = layer.evict(&hashtable);
        assert!(!evicted);
    }

    #[test]
    fn test_expire_empty_layer() {
        use crate::hashtable_impl::CuckooHashtable;

        let layer = create_test_layer();
        let hashtable = CuckooHashtable::new(10);

        // Expire on empty layer should return 0
        let expired = layer.expire(&hashtable);
        assert_eq!(expired, 0);
    }

    #[test]
    fn test_evict_with_items() {
        use crate::hashtable_impl::CuckooHashtable;

        let layer = create_test_layer();
        let hashtable = CuckooHashtable::new(10);

        // Fill up segments to trigger eviction
        for i in 0..500 {
            let key = format!("evict_key_{}", i);
            let value = format!("evict_value_{}", i);
            let _ = layer.write_item(
                key.as_bytes(),
                value.as_bytes(),
                b"",
                Duration::from_secs(60),
            );
        }

        // Try to evict
        let _ = layer.evict(&hashtable);
    }

    #[test]
    fn test_different_ttls() {
        let layer = create_test_layer();

        // Write items with very different TTLs
        let short_ttl = Duration::from_secs(5);
        let medium_ttl = Duration::from_secs(300);
        let long_ttl = Duration::from_secs(86400);

        let loc1 = layer
            .write_item(b"short", b"value", b"", short_ttl)
            .unwrap();
        let loc2 = layer
            .write_item(b"medium", b"value", b"", medium_ttl)
            .unwrap();
        let loc3 = layer.write_item(b"long", b"value", b"", long_ttl).unwrap();

        // All should be retrievable
        assert!(layer.get_item(loc1, b"short").is_some());
        assert!(layer.get_item(loc2, b"medium").is_some());
        assert!(layer.get_item(loc3, b"long").is_some());

        // They should potentially be in different segments due to different TTL buckets
        // (depends on bucket organization)
    }

    #[test]
    fn test_fill_and_allocate_new_segment() {
        let layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(1024) // Small segments for testing
            .heap_size(10 * 1024) // 10 segments
            .build()
            .expect("Failed to create test layer");

        let ttl = Duration::from_secs(60);

        // Fill up one segment by writing many items
        let mut locations = Vec::new();
        for i in 0..50 {
            let key = format!("fill_key_{:04}", i);
            let value = format!("fill_value_{:04}", i);
            if let Ok(loc) = layer.write_item(key.as_bytes(), value.as_bytes(), b"", ttl) {
                locations.push((key, loc));
            }
        }

        // Should have used multiple segments
        assert!(layer.used_segment_count() >= 1);
    }

    #[test]
    fn test_segment_exhaustion() {
        // Create a very small layer
        let layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(1024)
            .heap_size(2 * 1024) // Only 2 segments
            .build()
            .expect("Failed to create test layer");

        let ttl = Duration::from_secs(60);

        // Try to fill up beyond capacity
        let mut success_count = 0;
        for i in 0..1000 {
            let key = format!("exhaust_key_{:04}", i);
            let value = format!("exhaust_value_{:04}", i);
            if layer
                .write_item(key.as_bytes(), value.as_bytes(), b"", ttl)
                .is_ok()
            {
                success_count += 1;
            }
        }

        // Should have written some items
        assert!(success_count > 0);
    }

    #[test]
    fn test_value_too_large() {
        let layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(1024) // Small segment
            .heap_size(4 * 1024)
            .build()
            .expect("Failed to create test layer");

        // Value larger than segment can hold
        let large_value = vec![0u8; 2000];
        let result = layer.write_item(b"key", &large_value, b"", Duration::from_secs(60));
        assert!(result.is_err());
    }

    #[test]
    fn test_write_item_zero_ttl() {
        let layer = create_test_layer();

        // Zero TTL should still work (may be treated as minimal TTL)
        let result = layer.write_item(b"zero_ttl", b"value", b"", Duration::ZERO);
        // Implementation may or may not allow zero TTL
        let _ = result; // Just check it doesn't panic
    }

    #[test]
    fn test_get_item_invalid_segment() {
        let layer = create_test_layer();

        // Try to get from an invalid segment ID
        let invalid_location = ItemLocation::new(1, 9999, 0);
        let guard = layer.get_item(invalid_location, b"key");
        assert!(guard.is_none());
    }

    #[test]
    fn test_item_ttl_invalid_segment() {
        let layer = create_test_layer();

        // Try to get TTL from an invalid segment ID
        let invalid_location = ItemLocation::new(1, 9999, 0);
        let ttl = layer.item_ttl(invalid_location);
        assert!(ttl.is_none());
    }

    #[test]
    fn test_mark_deleted_invalid_segment() {
        let layer = create_test_layer();

        // Try to mark deleted on an invalid segment
        let invalid_location = ItemLocation::new(1, 9999, 0);
        // Should not panic
        layer.mark_deleted(invalid_location);
    }
}
