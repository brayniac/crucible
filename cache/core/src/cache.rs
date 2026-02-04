//! TieredCache - orchestrating cache with multiple layers.
//!
//! [`TieredCache`] manages a hierarchy of cache layers, providing:
//! - Unified write path (all writes go to Layer 0)
//! - Ghost-aware insertion (preserves frequency for re-inserted keys)
//! - Synchronous eviction when space is needed
//! - Layer-based read with proper frequency tracking

use crate::cas::CasToken;
use crate::config::LayerConfig;
use crate::disk::{DiskLayer, FilePool};
use crate::error::{CacheError, CacheResult};
use crate::hashtable::{Hashtable, KeyVerifier};
use crate::item::ItemGuard;
use crate::item_location::ItemLocation;
use crate::layer::{FifoLayer, Layer, TtlLayer};
use crate::location::Location;
use crate::memory_pool::MemoryPool;
use crate::pool::RamPool;
use crate::segment::{Segment, SegmentKeyVerify};
use crate::slice_segment::SliceSegment;
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
    /// Disk-backed layer (for extended capacity beyond RAM).
    Disk(DiskLayer),
}

impl CacheLayer {
    /// Get the layer's configuration.
    pub fn config(&self) -> &LayerConfig {
        match self {
            CacheLayer::Fifo(layer) => layer.config(),
            CacheLayer::Ttl(layer) => layer.config(),
            CacheLayer::Disk(layer) => layer.config(),
        }
    }

    /// Get the layer ID.
    pub fn layer_id(&self) -> u8 {
        match self {
            CacheLayer::Fifo(layer) => layer.layer_id(),
            CacheLayer::Ttl(layer) => layer.layer_id(),
            CacheLayer::Disk(layer) => layer.layer_id(),
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
        match self {
            CacheLayer::Fifo(layer) => layer.write_item(key, value, optional, ttl),
            CacheLayer::Ttl(layer) => layer.write_item(key, value, optional, ttl),
            CacheLayer::Disk(layer) => layer.write_item(key, value, optional, ttl),
        }
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
    ///
    /// Returns `None` if the item is not found or not accessible.
    /// Note: For disk layers, this returns pointers to mmap'd memory.
    pub fn get_value_ref_raw(
        &self,
        location: ItemLocation,
        key: &[u8],
    ) -> Option<(*const crate::sync::AtomicU32, *const u8, usize)> {
        match self {
            CacheLayer::Fifo(layer) => {
                let pool = layer.pool();
                if location.pool_id() != pool.pool_id() {
                    return None;
                }
                let segment = pool.get(location.segment_id())?;
                if !segment.state().is_readable() {
                    return None;
                }
                segment.get_value_ref_raw(location.offset(), key).ok()
            }
            CacheLayer::Ttl(layer) => {
                let pool = layer.pool();
                if location.pool_id() != pool.pool_id() {
                    return None;
                }
                let segment = pool.get(location.segment_id())?;
                if !segment.state().is_readable() {
                    return None;
                }
                segment.get_value_ref_raw(location.offset(), key).ok()
            }
            CacheLayer::Disk(layer) => {
                let pool = layer.pool();
                if location.pool_id() != pool.pool_id() {
                    return None;
                }
                let segment = pool.get(location.segment_id())?;
                if !segment.state().is_readable() {
                    return None;
                }
                segment.get_value_ref_raw(location.offset(), key).ok()
            }
        }
    }

    /// Mark an item as deleted.
    pub fn mark_deleted(&self, location: ItemLocation) {
        match self {
            CacheLayer::Fifo(layer) => layer.mark_deleted(location),
            CacheLayer::Ttl(layer) => layer.mark_deleted(location),
            CacheLayer::Disk(layer) => layer.mark_deleted(location),
        }
    }

    /// Mark an item as deleted and attempt segment compaction.
    ///
    /// This is like `mark_deleted` but additionally attempts to compact the
    /// segment with its predecessor when the deletion creates enough free space.
    pub fn mark_deleted_and_compact<H: Hashtable>(&self, location: ItemLocation, hashtable: &H) {
        match self {
            CacheLayer::Fifo(layer) => layer.mark_deleted_and_compact(location, hashtable),
            CacheLayer::Ttl(layer) => layer.mark_deleted_and_compact(location, hashtable),
            CacheLayer::Disk(layer) => layer.mark_deleted_and_compact(location, hashtable),
        }
    }

    /// Get the remaining TTL for an item.
    pub fn item_ttl(&self, location: ItemLocation) -> Option<Duration> {
        match self {
            CacheLayer::Fifo(layer) => layer.item_ttl(location),
            CacheLayer::Ttl(layer) => layer.item_ttl(location),
            CacheLayer::Disk(layer) => layer.item_ttl(location),
        }
    }

    /// Try to evict a segment from this layer.
    pub fn evict<H: Hashtable>(&self, hashtable: &H) -> bool {
        match self {
            CacheLayer::Fifo(layer) => layer.evict(hashtable),
            CacheLayer::Ttl(layer) => layer.evict(hashtable),
            CacheLayer::Disk(layer) => layer.evict(hashtable),
        }
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
        match self {
            CacheLayer::Fifo(layer) => layer.evict_with_demoter(hashtable, demoter),
            CacheLayer::Ttl(layer) => layer.evict_with_demoter(hashtable, demoter),
            CacheLayer::Disk(layer) => layer.evict_with_demoter(hashtable, demoter),
        }
    }

    /// Try to expire segments in this layer.
    pub fn expire<H: Hashtable>(&self, hashtable: &H) -> usize {
        match self {
            CacheLayer::Fifo(layer) => layer.expire(hashtable),
            CacheLayer::Ttl(layer) => layer.expire(hashtable),
            CacheLayer::Disk(layer) => layer.expire(hashtable),
        }
    }

    /// Get the number of free segments.
    pub fn free_segment_count(&self) -> usize {
        match self {
            CacheLayer::Fifo(layer) => layer.free_segment_count(),
            CacheLayer::Ttl(layer) => layer.free_segment_count(),
            CacheLayer::Disk(layer) => layer.free_segment_count(),
        }
    }

    /// Get the total number of segments.
    pub fn total_segment_count(&self) -> usize {
        match self {
            CacheLayer::Fifo(layer) => layer.total_segment_count(),
            CacheLayer::Ttl(layer) => layer.total_segment_count(),
            CacheLayer::Disk(layer) => layer.total_segment_count(),
        }
    }

    /// Get the number of segments in use.
    pub fn used_segment_count(&self) -> usize {
        match self {
            CacheLayer::Fifo(layer) => layer.used_segment_count(),
            CacheLayer::Ttl(layer) => layer.used_segment_count(),
            CacheLayer::Disk(layer) => layer.used_segment_count(),
        }
    }

    /// Get a segment from this layer's pool by segment ID (RAM layers only).
    ///
    /// For disk layers, use `disk_pool()` instead.
    /// Returns `None` for disk layers.
    pub fn get_segment(&self, segment_id: u32) -> Option<&SliceSegment<'static>> {
        match self {
            CacheLayer::Fifo(layer) => layer.pool().get(segment_id),
            CacheLayer::Ttl(layer) => layer.pool().get(segment_id),
            CacheLayer::Disk(_) => None, // Disk segments have different type
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
            CacheLayer::Disk(_) => panic!("Cannot get MemoryPool from disk layer; use disk_pool()"),
        }
    }

    /// Get the disk pool for this layer (disk layers only).
    ///
    /// Returns `None` for RAM layers.
    pub fn disk_pool(&self) -> Option<&FilePool> {
        match self {
            CacheLayer::Fifo(_) | CacheLayer::Ttl(_) => None,
            CacheLayer::Disk(layer) => Some(layer.pool()),
        }
    }

    /// Check if this is a disk layer.
    pub fn is_disk(&self) -> bool {
        matches!(self, CacheLayer::Disk(_))
    }

    /// Get the pool ID for this layer.
    pub fn pool_id(&self) -> u8 {
        match self {
            CacheLayer::Fifo(layer) => layer.pool().pool_id(),
            CacheLayer::Ttl(layer) => layer.pool().pool_id(),
            CacheLayer::Disk(layer) => layer.pool().pool_id(),
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
        match self {
            CacheLayer::Fifo(layer) => layer.begin_write_item(key, value_len, optional, ttl),
            CacheLayer::Ttl(layer) => layer.begin_write_item(key, value_len, optional, ttl),
            CacheLayer::Disk(layer) => layer.begin_write_item(key, value_len, optional, ttl),
        }
    }

    /// Finalize a two-phase write operation.
    pub fn finalize_write_item(&self, location: ItemLocation, item_size: u32) {
        match self {
            CacheLayer::Fifo(layer) => layer.finalize_write_item(location, item_size),
            CacheLayer::Ttl(layer) => layer.finalize_write_item(location, item_size),
            CacheLayer::Disk(layer) => layer.finalize_write_item(location, item_size),
        }
    }

    /// Cancel a two-phase write operation.
    pub fn cancel_write_item(&self, location: ItemLocation) {
        match self {
            CacheLayer::Fifo(layer) => layer.cancel_write_item(location),
            CacheLayer::Ttl(layer) => layer.cancel_write_item(location),
            CacheLayer::Disk(layer) => layer.cancel_write_item(location),
        }
    }

    /// Reset all segments in this layer to Free state.
    ///
    /// This is used during flush operations to reset the entire layer.
    pub fn reset_all_segments(&self) {
        match self {
            CacheLayer::Fifo(layer) => layer.pool().reset_all(),
            CacheLayer::Ttl(layer) => layer.pool().reset_all(),
            CacheLayer::Disk(layer) => layer.pool().reset_all(),
        }
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
                self.mark_deleted_at(old_location);
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
                self.mark_deleted_at(old_location);
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
                self.mark_deleted_at(old_location);
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
        let (ref_count_ptr, value_ptr, value_len) = layer.get_value_ref_raw(item_loc, key)?;

        // Safety: The layer's get_value_ref_raw has incremented ref_count and
        // validated that the pointers are valid. ValueRef will decrement ref_count on drop.
        Some(unsafe { crate::cache_trait::ValueRef::new(ref_count_ptr, value_ptr, value_len) })
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
        let segment = layer.get_segment(item_loc.segment_id())?;
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
        let segment = layer.get_segment(item_loc.segment_id())?;
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
            .get_segment(item_loc.segment_id())
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

        // Update in hashtable
        match self
            .hashtable
            .update_if_present(key, new_location.to_location(), &verifier)
        {
            Ok(old_location) => {
                self.mark_deleted_at(old_location);
                Ok(true)
            }
            Err(e) => {
                write_layer.mark_deleted(new_location);
                Err(e)
            }
        }
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
    /// If the key doesn't exist and `initial` is provided, creates the key
    /// with `initial + delta`. If `initial` is `None` and the key doesn't exist,
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
        // Try to get the current value
        let current_value = self.get(key);

        match current_value {
            Some(data) => {
                // Parse existing value as ASCII decimal
                let value_str = std::str::from_utf8(&data).map_err(|_| CacheError::NotNumeric)?;
                let current: u64 = value_str
                    .trim()
                    .parse()
                    .map_err(|_| CacheError::NotNumeric)?;

                // Compute new value with overflow check
                let new_value = current.checked_add(delta).ok_or(CacheError::Overflow)?;

                // Store the new value
                let new_str = new_value.to_string();
                self.set(key, new_str.as_bytes(), &[], ttl)?;

                Ok(new_value)
            }
            None => {
                // Key doesn't exist
                match initial {
                    Some(init) => {
                        // Create with initial + delta
                        let new_value = init.checked_add(delta).ok_or(CacheError::Overflow)?;
                        let new_str = new_value.to_string();
                        self.set(key, new_str.as_bytes(), &[], ttl)?;
                        Ok(new_value)
                    }
                    None => Err(CacheError::KeyNotFound),
                }
            }
        }
    }

    /// Atomically decrement a numeric value stored as ASCII decimal.
    ///
    /// If the key doesn't exist and `initial` is provided, creates the key
    /// with `initial.saturating_sub(delta)`. If `initial` is `None` and the key
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
        // Try to get the current value
        let current_value = self.get(key);

        match current_value {
            Some(data) => {
                // Parse existing value as ASCII decimal
                let value_str = std::str::from_utf8(&data).map_err(|_| CacheError::NotNumeric)?;
                let current: u64 = value_str
                    .trim()
                    .parse()
                    .map_err(|_| CacheError::NotNumeric)?;

                // Compute new value with saturating subtraction (clamp to 0)
                let new_value = current.saturating_sub(delta);

                // Store the new value
                let new_str = new_value.to_string();
                self.set(key, new_str.as_bytes(), &[], ttl)?;

                Ok(new_value)
            }
            None => {
                // Key doesn't exist
                match initial {
                    Some(init) => {
                        // Create with initial - delta (saturating)
                        let new_value = init.saturating_sub(delta);
                        let new_str = new_value.to_string();
                        self.set(key, new_str.as_bytes(), &[], ttl)?;
                        Ok(new_value)
                    }
                    None => Err(CacheError::KeyNotFound),
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
    fn ensure_space(&self) -> CacheResult<()> {
        let layer = self.layers.first().ok_or(CacheError::OutOfMemory)?;

        // Check if we need to evict
        if layer.free_segment_count() > self.eviction_threshold {
            return Ok(());
        }

        // Find disk layer for demotion (if any)
        let disk_layer_idx = self.layers.iter().position(|l| l.is_disk());

        // Try to evict until we have enough space
        for _ in 0..self.max_eviction_attempts {
            let evicted = self.evict_from_layer(0, disk_layer_idx);

            if !evicted {
                // Can't evict from Layer 0, try Layer 1 if it exists
                if self.layers.len() > 1 {
                    self.evict_from_layer(1, disk_layer_idx);
                }
            }

            if layer.free_segment_count() > self.eviction_threshold {
                return Ok(());
            }
        }

        // Still no space after max attempts
        Err(CacheError::OutOfMemory)
    }

    /// Evict from a specific layer, optionally demoting to disk.
    fn evict_from_layer(&self, layer_idx: usize, disk_layer_idx: Option<usize>) -> bool {
        let layer = match self.layers.get(layer_idx) {
            Some(l) => l,
            None => return false,
        };

        // If we have a disk layer and this isn't the disk layer, use demotion
        if let Some(disk_idx) = disk_layer_idx
            && layer_idx != disk_idx
            && let Some(disk_layer) = self.layers.get(disk_idx)
        {
            let hashtable = self.hashtable.as_ref();

            // Create demoter callback that writes to disk and updates hashtable
            let demoter = |key: &[u8],
                           value: &[u8],
                           optional: &[u8],
                           ttl: Duration,
                           old_location: Location| {
                // Write item to disk layer
                if let Ok(new_location) = disk_layer.write_item(key, value, optional, ttl) {
                    // Atomically update hashtable to point to disk location
                    // preserve_freq=true to keep the frequency counter
                    hashtable.cas_location(key, old_location, new_location.to_location(), true);
                } else {
                    // Disk write failed, just remove from hashtable
                    hashtable.remove(key, old_location);
                }
            };

            return layer.evict_with_demoter(hashtable, demoter);
        }

        // No disk layer or this is the disk layer - use regular eviction
        layer.evict(self.hashtable.as_ref())
    }

    /// Mark an item as deleted at the given location.
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
            layer.mark_deleted_and_compact(item_loc, self.hashtable.as_ref());
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
                }
            }
        }

        CacheKeyVerifier { pools }
    }

    /// Flush all items from the cache.
    ///
    /// This clears the hashtable and resets all segments to their initial state.
    /// After calling flush, the cache will be empty.
    pub fn flush(&self) {
        // Clear the hashtable first - this makes all items "invisible"
        self.hashtable.clear();

        // Reset all segments in all layers
        for layer in &self.layers {
            layer.reset_all_segments();
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
        // Unpack location to get segment and offset
        let (pool_id, segment_id, offset) = ItemLocation::from_location(location).unpack();

        // Direct pool lookup - pool_id is 2 bits (0-3), pools is [_; 4]
        // SAFETY: pool_id is extracted from ItemLocation which stores it in 2 bits
        let Some((pool_ref, _)) = (unsafe { *self.pools.get_unchecked(pool_id as usize) }) else {
            return;
        };

        // Get data pointer based on pool type
        let ptr = match pool_ref {
            PoolRef::Memory(pool) => {
                let Some(segment) = pool.get(segment_id) else {
                    return;
                };
                unsafe { segment.data_ptr().add(offset as usize) }
            }
            PoolRef::Disk(pool) => {
                let Some(segment) = pool.get(segment_id) else {
                    return;
                };
                unsafe { segment.data_ptr().add(offset as usize) }
            }
        };

        // Use platform-specific prefetch intrinsics
        #[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
        unsafe {
            // PREFETCHT0 - prefetch to all cache levels
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(ptr as *const i8);
        }

        #[cfg(target_arch = "aarch64")]
        unsafe {
            // PRFM PLDL1KEEP - prefetch for load, L1 cache, keep in cache
            std::arch::asm!(
                "prfm pldl1keep, [{ptr}]",
                ptr = in(reg) ptr,
                options(nostack, preserves_flags)
            );
        }

        // On other platforms, this is a no-op
        #[cfg(not(any(
            all(target_arch = "x86_64", target_feature = "sse"),
            target_arch = "aarch64"
        )))]
        let _ = ptr;
    }

    #[inline(always)]
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        // Unpack all location fields in one pass
        let (pool_id, segment_id, offset) = ItemLocation::from_location(location).unpack();

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
                let Some(segment) = pool.get(segment_id) else {
                    return false;
                };
                segment.verify_key_at_offset(offset, key, allow_deleted)
            }
            PoolRef::Disk(pool) => {
                let Some(segment) = pool.get(segment_id) else {
                    return false;
                };
                segment.verify_key_at_offset(offset, key, allow_deleted)
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
}

impl<H: Hashtable> TieredCacheBuilder<H> {
    /// Create a new builder with the given hashtable.
    pub fn new(hashtable: Arc<H>) -> Self {
        Self {
            hashtable,
            layers: Vec::new(),
            pool_map: [None; 4],
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

    /// Build the tiered cache.
    pub fn build(self) -> TieredCache<H> {
        TieredCache {
            hashtable: self.hashtable,
            layers: self.layers,
            pool_map: self.pool_map,
            eviction_threshold: self.eviction_threshold,
            max_eviction_attempts: self.max_eviction_attempts,
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::hashtable_impl::MultiChoiceHashtable;
    use crate::layer::{FifoLayerBuilder, TtlLayerBuilder};

    fn create_test_cache() -> TieredCache<MultiChoiceHashtable> {
        let hashtable = Arc::new(MultiChoiceHashtable::new(10)); // 2^10 = 1024 buckets

        let fifo_layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(64 * 1024)
            .heap_size(256 * 1024)
            .build()
            .expect("Failed to create FIFO layer");

        let ttl_layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(64 * 1024)
            .heap_size(512 * 1024)
            .build()
            .expect("Failed to create TTL layer");

        TieredCacheBuilder::new(hashtable)
            .with_fifo_layer(fifo_layer)
            .with_ttl_layer(ttl_layer)
            .eviction_threshold(1)
            .build()
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
}
