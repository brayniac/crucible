//! FIFO-organized layer for admission queues.
//!
//! [`FifoLayer`] combines a [`MemoryPool`] with [`FifoChain`] organization
//! for use as an admission filter (S3FIFO small queue).
//!
//! # Characteristics
//!
//! - FIFO eviction order (oldest segment first)
//! - Per-item TTL storage (uses [`TtlHeader`])
//! - Ghost creation for evicted cold items
//! - Demotion of hot items to next layer

use crate::config::LayerConfig;
use crate::error::{CacheError, CacheResult};
use crate::eviction::{ItemFate, determine_item_fate};
use crate::hashtable::{Hashtable, KeyVerifier};
use crate::item::{BasicItemGuard, TtlHeader};
use crate::item_location::ItemLocation;
use crate::layer::Layer;
use crate::location::Location;
use crate::memory_pool::{MemoryPool, MemoryPoolBuilder};
use crate::organization::FifoChain;
use crate::pool::RamPool;
use crate::segment::{Segment, SegmentGuard, SegmentKeyVerify};
use crate::state::State;
use std::time::Duration;

/// A FIFO-organized layer for admission filtering.
///
/// This layer uses a simple FIFO chain for segment organization.
/// Items are appended to the tail segment; when full, a new segment
/// is allocated. Eviction removes the head (oldest) segment.
///
/// # Use Case
///
/// S3FIFO small queue (Layer 0):
/// - All new items enter here first
/// - Items with freq > threshold are demoted to main cache
/// - Items with freq <= threshold become ghosts
pub struct FifoLayer {
    /// Layer identifier.
    layer_id: u8,

    /// Layer configuration.
    config: LayerConfig,

    /// Segment pool.
    pool: MemoryPool,

    /// FIFO segment chain.
    chain: FifoChain,
}

impl FifoLayer {
    /// Create a new FIFO layer builder.
    pub fn builder() -> FifoLayerBuilder {
        FifoLayerBuilder::new()
    }

    /// Get a reference to the segment pool.
    pub fn pool(&self) -> &MemoryPool {
        &self.pool
    }

    /// Try to allocate a new segment and add it to the chain.
    fn allocate_segment(&self) -> CacheResult<u32> {
        // Try to reserve a segment from the pool
        let segment_id = self.pool.reserve().ok_or(CacheError::OutOfMemory)?;

        // Push onto the chain
        match self.chain.push(segment_id, &self.pool) {
            Ok(()) => Ok(segment_id),
            Err(_) => {
                // Failed to push, release the segment
                self.pool.release(segment_id);
                Err(CacheError::OutOfMemory)
            }
        }
    }

    /// Get the current write segment, allocating if needed.
    fn get_or_allocate_write_segment(&self) -> CacheResult<u32> {
        // Check if we have a tail segment that's Live
        if let Some(tail_id) = self.chain.tail()
            && let Some(segment) = self.pool.get(tail_id)
            && segment.state() == State::Live
        {
            return Ok(tail_id);
        }

        // Need to allocate a new segment
        self.allocate_segment()
    }

    /// Get current time as coarse seconds.
    fn now_secs() -> u32 {
        clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs()
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
        let now = Self::now_secs();
        let mut offset = 0u32;
        let write_offset = segment.write_offset();

        while offset < write_offset {
            // Try to read header at offset
            if let Some(data) = segment.data_slice(offset, TtlHeader::SIZE) {
                if let Some(header) = TtlHeader::try_from_bytes(data) {
                    let item_size = header.padded_size() as u32;

                    // Skip if already deleted or expired
                    if !header.is_deleted() && !header.is_expired(now) {
                        // Get key for this item
                        let key_start =
                            offset as usize + TtlHeader::SIZE + header.optional_len() as usize;
                        let key_len = header.key_len() as usize;

                        if let Some(key) = segment.data_slice(key_start as u32, key_len) {
                            let location =
                                ItemLocation::new(self.pool.pool_id(), segment_id, offset);

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
                                    // Demotion is handled by the caller (TieredCache)
                                    // For now, just unlink from hashtable
                                    hashtable.remove(key, location.to_location());
                                }
                                ItemFate::Discard => {
                                    // Simply remove from hashtable
                                    hashtable.remove(key, location.to_location());
                                }
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

impl Layer for FifoLayer {
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
        if key.len() > 255 {
            return Err(CacheError::KeyTooLong);
        }
        if optional.len() > 64 {
            return Err(CacheError::OptionalTooLong);
        }

        // Try to append to current write segment
        loop {
            let segment_id = self.get_or_allocate_write_segment()?;

            if let Some(segment) = self.pool.get(segment_id) {
                // Calculate expiration
                let expire_at = Self::now_secs() + ttl.as_secs() as u32;

                // Try to append with per-item TTL
                if let Some(offset) = segment.append_item_with_ttl(key, value, optional, expire_at)
                {
                    return Ok(ItemLocation::new(self.pool.pool_id(), segment_id, offset));
                }

                // Segment is full, need to allocate a new one
                // The next iteration will allocate
            }

            // Allocate a new segment
            self.allocate_segment()?;
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

        // Check if item is expired (per-item TTL) before getting guard
        let now = Self::now_secs();
        if let Some(remaining) = segment.item_ttl(location.offset(), now) {
            if remaining.is_zero() {
                return None;
            }
        } else {
            // No TTL means expired
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
            // We need the key to mark deleted, but we don't have it here.
            // This is a limitation - mark_deleted needs to be called with key.
            // For now, we skip the key verification by getting it from the segment.
            let offset = location.offset();
            if let Some(data) = segment.data_slice(offset, TtlHeader::SIZE)
                && let Some(header) = TtlHeader::try_from_bytes(data)
            {
                let key_start = offset as usize + TtlHeader::SIZE + header.optional_len() as usize;
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
        segment.item_ttl(location.offset(), now)
    }

    fn evict<H: Hashtable>(&self, hashtable: &H) -> bool {
        // Try to pop head segment from chain
        match self.chain.pop(&self.pool) {
            Ok(segment_id) => {
                // Process items in the evicted segment
                self.process_evicted_segment(segment_id, hashtable);
                true
            }
            Err(_) => false,
        }
    }

    fn expire<H: Hashtable>(&self, _hashtable: &H) -> usize {
        // FIFO layer uses per-item TTL, so segment-level expiration
        // doesn't apply. Items are checked on read.
        // We could scan for fully-expired segments here.
        0
    }

    fn free_segment_count(&self) -> usize {
        self.pool.free_count()
    }

    fn total_segment_count(&self) -> usize {
        self.pool.segment_count()
    }
}

/// Builder for [`FifoLayer`].
pub struct FifoLayerBuilder {
    layer_id: u8,
    config: LayerConfig,
    pool_id: u8,
    segment_size: usize,
    heap_size: usize,
    numa_node: Option<u32>,
}

impl FifoLayerBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self {
            layer_id: 0,
            config: LayerConfig::new().with_ghosts(true),
            pool_id: 0,
            segment_size: 1024 * 1024,   // 1MB
            heap_size: 64 * 1024 * 1024, // 64MB
            numa_node: None,
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

    /// Set the NUMA node to bind memory to (Linux only).
    pub fn numa_node(mut self, node: u32) -> Self {
        self.numa_node = Some(node);
        self
    }

    /// Build the FIFO layer.
    pub fn build(self) -> Result<FifoLayer, std::io::Error> {
        let mut builder = MemoryPoolBuilder::new(self.pool_id)
            .per_item_ttl(true) // FIFO layer uses per-item TTL
            .segment_size(self.segment_size)
            .heap_size(self.heap_size);

        if let Some(node) = self.numa_node {
            builder = builder.numa_node(node);
        }

        let pool = builder.build()?;

        Ok(FifoLayer {
            layer_id: self.layer_id,
            config: self.config,
            pool,
            chain: FifoChain::new(),
        })
    }
}

impl Default for FifoLayerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::item::ItemGuard;

    fn create_test_layer() -> FifoLayer {
        FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024) // 64KB
            .heap_size(640 * 1024) // 640KB = 10 segments
            .config(LayerConfig::new().with_ghosts(true))
            .build()
            .expect("Failed to create test layer")
    }

    #[test]
    fn test_layer_creation() {
        let layer = create_test_layer();
        assert_eq!(layer.layer_id(), 0);
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
        assert_eq!(location.pool_id(), 0);

        // Get item
        let guard = layer.get_item(location, key);
        assert!(guard.is_some());

        let guard = guard.unwrap();
        assert_eq!(guard.key(), key);
        assert_eq!(guard.value(), value);
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

        // Item should still be retrievable but marked as deleted
        // (the header flag is set, but get_item doesn't check it)
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
    fn test_builder_default() {
        let builder = FifoLayerBuilder::default();
        let layer = builder
            .segment_size(64 * 1024)
            .heap_size(128 * 1024)
            .build()
            .expect("Should build");
        assert_eq!(layer.layer_id(), 0); // Default layer_id
    }

    #[test]
    fn test_builder_custom_config() {
        let config = LayerConfig::new().with_ghosts(false);
        let layer = FifoLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .config(config)
            .segment_size(64 * 1024)
            .heap_size(128 * 1024)
            .build()
            .expect("Should build");

        assert_eq!(layer.layer_id(), 1);
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
        let wrong_location = ItemLocation::new(1, location.segment_id(), location.offset());

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
    fn test_item_ttl() {
        let layer = create_test_layer();

        let key = b"ttl_test";
        let ttl = Duration::from_secs(3600);
        let location = layer.write_item(key, b"value", b"", ttl).unwrap();

        // Item TTL should be approximately the requested TTL
        let remaining = layer.item_ttl(location);
        assert!(remaining.is_some());
        // Should be close to 3600 seconds
        let secs = remaining.unwrap().as_secs();
        assert!((3590..=3600).contains(&secs));
    }

    #[test]
    fn test_item_ttl_wrong_pool_id() {
        let layer = create_test_layer();

        let key = b"test_key";
        let location = layer
            .write_item(key, b"value", b"", Duration::from_secs(3600))
            .unwrap();

        // Pool_id must be 0-3, use 1 which is different from layer's pool_id of 0
        let wrong_location = ItemLocation::new(1, location.segment_id(), location.offset());
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

        // Pool_id must be 0-3, use 1 which is different from layer's pool_id of 0
        let wrong_location = ItemLocation::new(1, location.segment_id(), location.offset());
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

        // All items should be in the same segment (FIFO chain)
        assert_eq!(loc1.segment_id(), loc2.segment_id());
        assert_eq!(loc2.segment_id(), loc3.segment_id());

        // All items should be retrievable
        assert!(layer.get_item(loc1, b"key1").is_some());
        assert!(layer.get_item(loc2, b"key2").is_some());
        assert!(layer.get_item(loc3, b"key3").is_some());
    }

    #[test]
    fn test_pool_accessor() {
        let layer = create_test_layer();

        assert_eq!(layer.pool().pool_id(), 0);
        assert_eq!(layer.pool().segment_count(), 10);
    }

    #[test]
    fn test_config_accessor() {
        let layer = create_test_layer();
        assert!(layer.config().create_ghosts);
    }

    #[test]
    fn test_different_ttls() {
        let layer = create_test_layer();

        // Write items with different TTLs
        let loc1 = layer
            .write_item(b"key1", b"value1", b"", Duration::from_secs(60))
            .unwrap();
        let loc2 = layer
            .write_item(b"key2", b"value2", b"", Duration::from_secs(3600))
            .unwrap();

        // Both should be accessible
        assert!(layer.get_item(loc1, b"key1").is_some());
        assert!(layer.get_item(loc2, b"key2").is_some());

        // Check individual TTLs
        let ttl1 = layer.item_ttl(loc1).unwrap();
        let ttl2 = layer.item_ttl(loc2).unwrap();

        // TTLs should differ significantly
        assert!(ttl2.as_secs() > ttl1.as_secs() + 1000);
    }

    #[test]
    fn test_builder_static_method() {
        let layer = FifoLayer::builder()
            .layer_id(3)
            .pool_id(3)
            .segment_size(64 * 1024)
            .heap_size(128 * 1024)
            .build()
            .expect("Should build");

        assert_eq!(layer.layer_id(), 3);
    }

    #[test]
    fn test_evict_empty_layer() {
        use crate::hashtable_impl::CuckooHashtable;

        let layer = create_test_layer();
        let hashtable = CuckooHashtable::new(10);

        // Evict on empty layer should return false
        let evicted = layer.evict(&hashtable);
        assert!(!evicted);
    }

    #[test]
    fn test_expire_returns_zero() {
        use crate::hashtable_impl::CuckooHashtable;

        let layer = create_test_layer();
        let hashtable = CuckooHashtable::new(10);

        // FIFO layer expire always returns 0 (no-op)
        let expired = layer.expire(&hashtable);
        assert_eq!(expired, 0);
    }

    #[test]
    fn test_evict_with_items() {
        use crate::hashtable_impl::CuckooHashtable;

        // Create layer with small segments to trigger eviction
        let layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(4 * 1024) // 4KB segments
            .heap_size(16 * 1024) // Only 4 segments
            .build()
            .expect("Failed to create layer");

        let hashtable = CuckooHashtable::new(10);

        // Fill segments with items
        let value = vec![b'x'; 512];
        for i in 0..20 {
            let key = format!("evict_key_{}", i);
            let _ = layer.write_item(key.as_bytes(), &value, b"", Duration::from_secs(60));
        }

        // Evict should work
        let evicted = layer.evict(&hashtable);
        assert!(evicted);
    }

    #[test]
    fn test_fill_and_allocate_new_segment() {
        let layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(4 * 1024) // 4KB segments
            .heap_size(20 * 1024) // 5 segments
            .build()
            .expect("Failed to create layer");

        let initial_free = layer.free_segment_count();

        // Fill the first segment
        let value = vec![b'x'; 512];
        for i in 0..10 {
            let key = format!("fill_key_{}", i);
            let _ = layer.write_item(key.as_bytes(), &value, b"", Duration::from_secs(60));
        }

        // Should have allocated at least one segment
        assert!(layer.used_segment_count() >= 1);
        assert!(layer.free_segment_count() < initial_free);
    }

    #[test]
    fn test_segment_exhaustion() {
        // Create layer with minimal segments
        let layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(1024) // 1KB segments
            .heap_size(2048) // Only 2 segments
            .build()
            .expect("Failed to create layer");

        // Fill both segments with large items
        let value = vec![b'x'; 512];
        let mut count = 0;
        for i in 0..10 {
            let key = format!("exhaust_key_{}", i);
            if layer
                .write_item(key.as_bytes(), &value, b"", Duration::from_secs(60))
                .is_ok()
            {
                count += 1;
            }
        }

        // Should have written at least some items
        assert!(count > 0);
    }

    #[test]
    fn test_get_item_invalid_segment() {
        let layer = create_test_layer();

        // Create a location with an invalid segment ID
        let invalid_location = ItemLocation::new(0, 999, 0);
        let guard = layer.get_item(invalid_location, b"key");
        assert!(guard.is_none());
    }

    #[test]
    fn test_item_ttl_invalid_segment() {
        let layer = create_test_layer();

        // Create a location with an invalid segment ID
        let invalid_location = ItemLocation::new(0, 999, 0);
        let ttl = layer.item_ttl(invalid_location);
        assert!(ttl.is_none());
    }

    #[test]
    fn test_mark_deleted_invalid_segment() {
        let layer = create_test_layer();

        // Create a location with an invalid segment ID
        let invalid_location = ItemLocation::new(0, 999, 0);
        // Should not panic, just be a no-op
        layer.mark_deleted(invalid_location);
    }

    #[test]
    fn test_value_too_large_for_segment() {
        let layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(1024) // 1KB segments
            .heap_size(4096) // 4 segments
            .build()
            .expect("Failed to create layer");

        // Try to write a value that's too large for a segment
        let key = b"big_key";
        let value = vec![b'x'; 2048]; // 2KB value, larger than segment size
        let result = layer.write_item(key, &value, b"", Duration::from_secs(60));

        // This should eventually fail when all segments are exhausted
        // or return an error
        assert!(result.is_err());
    }

    #[test]
    fn test_write_item_zero_ttl() {
        let layer = create_test_layer();

        let key = b"zero_ttl";
        let location = layer
            .write_item(key, b"value", b"", Duration::from_secs(0))
            .unwrap();

        // Item with zero TTL should be immediately expired
        let guard = layer.get_item(location, key);
        assert!(guard.is_none());
    }
}
