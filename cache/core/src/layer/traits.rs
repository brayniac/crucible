//! Layer trait definition.

use crate::config::LayerConfig;
use crate::error::CacheResult;
use crate::hashtable::Hashtable;
use crate::item::ItemGuard;
use crate::item_location::ItemLocation;
use std::time::Duration;

/// A layer in the cache hierarchy.
///
/// Layers combine a segment pool, organization strategy, and configuration
/// to provide a complete storage tier. Each layer can:
///
/// - Store items in segments
/// - Retrieve items by location
/// - Evict segments when space is needed
/// - Apply ghost/demotion policies
///
/// # Thread Safety
///
/// Layers must be `Send + Sync` for use in concurrent caches.
pub trait Layer: Send + Sync {
    /// The item guard type returned by this layer.
    type Guard<'a>: ItemGuard<'a>
    where
        Self: 'a;

    /// Get the layer's configuration.
    fn config(&self) -> &LayerConfig;

    /// Get the layer ID (for debugging/metrics).
    fn layer_id(&self) -> u8;

    /// Write an item to this layer.
    ///
    /// Allocates space in the current write segment (or a new segment if needed),
    /// appends the item, and returns the location.
    ///
    /// # Arguments
    /// * `key` - Item key (max 255 bytes)
    /// * `value` - Item value
    /// * `optional` - Optional metadata (max 64 bytes)
    /// * `ttl` - Time to live
    ///
    /// # Returns
    /// * `Ok(location)` - Item was stored successfully
    /// * `Err(CacheError)` - Storage failed (out of memory, etc.)
    fn write_item(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        ttl: Duration,
    ) -> CacheResult<ItemLocation>;

    /// Get an item from this layer.
    ///
    /// Returns a guard that provides access to the item's key, value, and optional data.
    /// The guard holds a reference count on the segment to prevent eviction while reading.
    ///
    /// # Arguments
    /// * `location` - Item location from hashtable lookup
    /// * `key` - Expected key (for verification)
    ///
    /// # Returns
    /// * `Some(guard)` - Item found and key matches
    /// * `None` - Item not found, expired, or key mismatch
    fn get_item(&self, location: ItemLocation, key: &[u8]) -> Option<Self::Guard<'_>>;

    /// Mark an item as deleted.
    ///
    /// Sets the deleted flag in the item header. The item will be skipped
    /// during iteration and won't be copied during merge eviction.
    ///
    /// # Arguments
    /// * `location` - Item location
    fn mark_deleted(&self, location: ItemLocation);

    /// Get the remaining TTL for an item.
    ///
    /// # Arguments
    /// * `location` - Item location
    ///
    /// # Returns
    /// * `Some(duration)` - Remaining TTL
    /// * `None` - Item not found or expired
    fn item_ttl(&self, location: ItemLocation) -> Option<Duration>;

    /// Try to evict a segment to free space.
    ///
    /// Attempts to evict one segment using the layer's eviction strategy.
    /// Items in the evicted segment are processed according to the layer config:
    /// - Hot items may be demoted to the next layer
    /// - Cold items become ghosts or are discarded
    ///
    /// # Arguments
    /// * `hashtable` - Hashtable for unlinking items and creating ghosts
    ///
    /// # Returns
    /// * `true` - A segment was evicted
    /// * `false` - No segment could be evicted
    fn evict<H: Hashtable>(&self, hashtable: &H) -> bool;

    /// Try to evict a segment with a callback for item demotion.
    ///
    /// Like `evict`, but calls the provided callback for items that should be
    /// demoted to the next layer. The callback receives:
    /// - `key`: Item key
    /// - `value`: Item value
    /// - `optional`: Optional metadata
    /// - `ttl`: Remaining time to live
    /// - `old_location`: Current location (to update hashtable)
    ///
    /// The callback should:
    /// 1. Write the item to the disk layer
    /// 2. Update the hashtable with the new disk location
    ///
    /// # Arguments
    /// * `hashtable` - Hashtable for unlinking items and creating ghosts
    /// * `demoter` - Callback for items that should be demoted
    ///
    /// # Returns
    /// * `true` - A segment was evicted
    /// * `false` - No segment could be evicted
    fn evict_with_demoter<H, F>(&self, hashtable: &H, demoter: F) -> bool
    where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, crate::location::Location);

    /// Try to expire segments whose TTL has passed.
    ///
    /// # Arguments
    /// * `hashtable` - Hashtable for unlinking expired items
    ///
    /// # Returns
    /// Number of segments expired
    fn expire<H: Hashtable>(&self, hashtable: &H) -> usize;

    /// Get the number of free segments in the pool.
    fn free_segment_count(&self) -> usize;

    /// Get the total number of segments in the pool.
    fn total_segment_count(&self) -> usize;

    /// Get the number of segments currently in use.
    fn used_segment_count(&self) -> usize {
        self.total_segment_count() - self.free_segment_count()
    }

    /// Begin a two-phase write operation for zero-copy receive.
    ///
    /// Reserves space in a segment for the item and returns a pointer to the
    /// value area. The caller writes the value directly to segment memory,
    /// then calls `finalize_write_item` to complete the operation.
    ///
    /// # Arguments
    /// * `key` - Item key (max 255 bytes)
    /// * `value_len` - Size of the value to be written
    /// * `optional` - Optional metadata (max 64 bytes)
    /// * `ttl` - Time to live
    ///
    /// # Returns
    /// * `Ok((location, value_ptr, item_size))` - Space reserved successfully
    /// * `Err(CacheError)` - Reservation failed
    ///
    /// # Safety
    ///
    /// The returned `value_ptr` points into segment memory. The caller must:
    /// - Write exactly `value_len` bytes to the pointer
    /// - Call `finalize_write_item` with the same `item_size`
    /// - Not hold the pointer beyond the `finalize_write_item` call
    fn begin_write_item(
        &self,
        key: &[u8],
        value_len: usize,
        optional: &[u8],
        ttl: Duration,
    ) -> CacheResult<(ItemLocation, *mut u8, u32)>;

    /// Complete a two-phase write operation.
    ///
    /// Called after writing the value data to the pointer returned by
    /// `begin_write_item`. Updates segment statistics (live_items, live_bytes).
    ///
    /// # Arguments
    /// * `location` - Item location from `begin_write_item`
    /// * `item_size` - The item_size returned by `begin_write_item`
    fn finalize_write_item(&self, location: ItemLocation, item_size: u32);

    /// Cancel a two-phase write operation.
    ///
    /// Called if the write cannot be completed (e.g., connection closed).
    /// Marks the reserved space as deleted.
    ///
    /// # Arguments
    /// * `location` - Item location from `begin_write_item`
    fn cancel_write_item(&self, location: ItemLocation);

    /// Mark an item as deleted and attempt segment compaction.
    ///
    /// This is like `mark_deleted` but additionally attempts to compact the
    /// segment with its predecessor when the deletion creates enough free space.
    /// This eagerly reclaims fragmented space rather than waiting for eviction.
    ///
    /// # Arguments
    /// * `location` - Item location
    /// * `hashtable` - Hashtable for updating item locations during compaction
    fn mark_deleted_and_compact<H: Hashtable>(&self, location: ItemLocation, hashtable: &H);
}
