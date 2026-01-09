//! Segment trait for cache storage.
//!
//! A segment is a fixed-size memory region that stores cache items sequentially.
//! Items are appended to the segment until it's full, then the segment is sealed
//! and a new one is allocated.
//!
//! This module provides:
//! - [`Segment`] - Core trait for segment operations
//! - [`SegmentGuard`] - Extension trait for zero-copy access
//! - [`SegmentCopy`] - Extension trait for tier migration
//! - [`SegmentPrune`] - Extension trait for merge eviction

use crate::error::CacheError;
use crate::item::ItemGuard;
use crate::state::State;
use std::time::Duration;

/// Minimal trait for key verification, used by hashtables.
///
/// This is the only segment functionality the hashtable needs. By using this
/// minimal trait, types that don't provide full segment access (like SSD-backed
/// segments that require async I/O) can still work with the hashtable.
pub trait SegmentKeyVerify {
    /// Verify that the key at the given offset matches.
    ///
    /// Used by hashtables to verify tag matches against actual keys.
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `key`: The key to compare against
    /// - `allow_deleted`: If `true`, matches deleted items
    ///
    /// # Returns
    /// `true` if the key matches (and item is not deleted unless `allow_deleted`).
    fn verify_key_at_offset(&self, offset: u32, key: &[u8], allow_deleted: bool) -> bool;

    /// Verify key and return parsed header info if matched.
    ///
    /// This avoids double-parsing when the caller needs header information
    /// after verification (e.g., for computing item size in delete operations).
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `key`: The key to compare against
    /// - `allow_deleted`: If `true`, matches deleted items
    ///
    /// # Returns
    /// `Some((key_len, optional_len, value_len))` if key matches, `None` otherwise.
    fn verify_key_with_header(
        &self,
        offset: u32,
        key: &[u8],
        allow_deleted: bool,
    ) -> Option<(u8, u8, u32)>;

    /// Verify key and check expiration in one parse.
    ///
    /// For segments with per-item TTL (TtlHeader), this checks the item's
    /// expiration time. For segment-level TTL (BasicHeader), this just
    /// verifies the key since TTL is checked at the segment level.
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `key`: The key to compare against
    /// - `now`: Current time as coarse seconds since epoch
    ///
    /// # Returns
    /// `Some((key_len, optional_len, value_len))` if key matches and item
    /// is not expired, `None` otherwise.
    fn verify_key_unexpired(&self, offset: u32, key: &[u8], now: u32) -> Option<(u8, u8, u32)>;
}

/// Trait for a cache segment.
///
/// Segments are the fundamental storage unit in segment-based caches.
/// They provide:
/// - Sequential item storage with atomic append
/// - State machine for concurrent access control
/// - Chain pointers for linked list organization (TTL buckets, FIFO, etc.)
/// - Reference counting for safe concurrent reads
///
/// # Concurrency
///
/// Segment operations use atomic primitives and CAS loops for thread safety.
/// The state machine ensures that:
/// - Only `Live` segments accept writes
/// - Only `Live`, `Sealed`, and `Relinking` segments allow reads
/// - Reference counting prevents clearing segments with active readers
///
/// # TTL Handling
///
/// Segments support two TTL models:
/// - **Segment-level TTL**: All items share `expire_at()`, stored in segment metadata
/// - **Per-item TTL**: Each item has individual TTL in `TtlHeader`
///
/// Use `item_ttl()` to get the TTL of a specific item, which will return the
/// appropriate TTL based on how the segment stores TTL information.
pub trait Segment: SegmentKeyVerify + Send + Sync {
    // ========== Identity ==========

    /// Get the segment ID within its pool.
    fn id(&self) -> u32;

    /// Get the pool ID this segment belongs to.
    ///
    /// Useful when multiple pools exist (e.g., RAM and SSD tiers).
    fn pool_id(&self) -> u8;

    /// Get the generation counter.
    ///
    /// Incremented each time the segment is reused. Combined with
    /// (pool_id, segment_id, offset), forms a unique identifier that
    /// prevents ABA problems.
    fn generation(&self) -> u16;

    /// Increment the generation counter (wraps at u16::MAX).
    fn increment_generation(&self);

    // ========== Capacity ==========

    /// Get the total data capacity in bytes.
    fn capacity(&self) -> usize;

    /// Get the current write offset (next write position).
    fn write_offset(&self) -> u32;

    /// Get free space remaining in the segment.
    fn free_space(&self) -> usize {
        self.capacity().saturating_sub(self.write_offset() as usize)
    }

    // ========== Statistics ==========

    /// Get the count of live (non-deleted) items.
    fn live_items(&self) -> u32;

    /// Get the total bytes used by live items.
    fn live_bytes(&self) -> u32;

    /// Get the current reference count (active readers).
    fn ref_count(&self) -> u32;

    // ========== State Machine ==========

    /// Get the current state.
    fn state(&self) -> State;

    /// Attempt to transition from `Free` to `Reserved`.
    ///
    /// Resets all segment statistics for reuse.
    ///
    /// # Returns
    /// `true` if successful, `false` if not in `Free` state.
    fn try_reserve(&self) -> bool;

    /// Attempt to transition back to `Free` state.
    ///
    /// Valid from `Reserved`, `Linking`, or `Locked` states.
    ///
    /// # Returns
    /// `true` if successful, `false` if already `Free` (idempotent).
    ///
    /// # Panics
    /// May panic if in an invalid state for release (`Live`, `Sealed`, etc.).
    fn try_release(&self) -> bool;

    /// Atomically update state and chain pointers.
    ///
    /// # Parameters
    /// - `expected_state`: The state we expect the segment to be in
    /// - `new_state`: The state to transition to
    /// - `new_next`: New next pointer, or `None` to preserve current
    /// - `new_prev`: New prev pointer, or `None` to preserve current
    ///
    /// # Returns
    /// `true` if the CAS succeeded, `false` if expected state didn't match.
    fn cas_metadata(
        &self,
        expected_state: State,
        new_state: State,
        new_next: Option<u32>,
        new_prev: Option<u32>,
    ) -> bool;

    // ========== Chain Pointers ==========

    /// Get the next segment ID in the chain, or `None` if this is the tail.
    fn next(&self) -> Option<u32>;

    /// Get the previous segment ID in the chain, or `None` if this is the head.
    fn prev(&self) -> Option<u32>;

    // ========== TTL ==========

    /// Get the segment-level expiration time (coarse seconds since epoch).
    ///
    /// For segments using segment-level TTL (BasicHeader), all items inherit
    /// this expiration time.
    fn expire_at(&self) -> u32;

    /// Set the segment-level expiration time.
    fn set_expire_at(&self, expire_at: u32);

    /// Get the remaining TTL for the entire segment.
    ///
    /// # Parameters
    /// - `now`: Current time as coarse seconds since epoch
    ///
    /// # Returns
    /// `None` if the segment is expired, otherwise the remaining duration.
    fn segment_ttl(&self, now: u32) -> Option<Duration> {
        let expire_at = self.expire_at();
        if now >= expire_at {
            None
        } else {
            Some(Duration::from_secs((expire_at - now) as u64))
        }
    }

    /// Get the TTL for a specific item.
    ///
    /// This method abstracts over the TTL model:
    /// - For segment-level TTL: Returns `segment_ttl(now)`
    /// - For per-item TTL: Reads the item's header to get its specific TTL
    ///
    /// Used when moving items between layers to reconstruct their TTL.
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `now`: Current time as coarse seconds since epoch
    ///
    /// # Returns
    /// `None` if the item is expired or offset is invalid, otherwise remaining TTL.
    fn item_ttl(&self, offset: u32, now: u32) -> Option<Duration>;

    /// Get the TTL bucket ID this segment belongs to, or `None` if not in a bucket.
    fn bucket_id(&self) -> Option<u16>;

    /// Set the TTL bucket ID.
    fn set_bucket_id(&self, bucket_id: u16);

    /// Clear the TTL bucket ID (mark as not in a bucket).
    fn clear_bucket_id(&self);

    // ========== Data Access ==========

    /// Get a raw slice of segment data at the given offset and length.
    ///
    /// # Safety
    /// The caller must ensure:
    /// - `offset + len <= capacity()`
    /// - The segment is in a readable state
    ///
    /// # Returns
    /// `Some(&[u8])` if the range is valid, `None` otherwise.
    fn data_slice(&self, offset: u32, len: usize) -> Option<&[u8]>;

    // ========== Item Operations ==========

    /// Append an item to the segment (segment-level TTL).
    ///
    /// Atomically reserves space and writes the item using BasicHeader.
    /// The segment should be in `Live` state.
    ///
    /// # Parameters
    /// - `key`: The item's key
    /// - `value`: The item's value
    /// - `optional`: Optional metadata (e.g., flags, CAS tag)
    ///
    /// # Returns
    /// `Some(offset)` where the item was written, `None` if segment is full.
    fn append_item(&self, key: &[u8], value: &[u8], optional: &[u8]) -> Option<u32>;

    /// Append an item to the segment with per-item TTL.
    ///
    /// Atomically reserves space and writes the item using TtlHeader.
    /// The segment should be in `Live` state.
    ///
    /// # Parameters
    /// - `key`: The item's key
    /// - `value`: The item's value
    /// - `optional`: Optional metadata (e.g., flags, CAS tag)
    /// - `expire_at`: Expiration time as coarse seconds since epoch
    ///
    /// # Returns
    /// `Some(offset)` where the item was written, `None` if segment is full.
    fn append_item_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        expire_at: u32,
    ) -> Option<u32>;

    /// Mark an item as deleted.
    ///
    /// Decrements `live_items` and `live_bytes` if successful.
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `key`: Expected key (for verification)
    ///
    /// # Returns
    /// - `Ok(true)`: Item was successfully marked as deleted
    /// - `Ok(false)`: Item was already deleted
    /// - `Err(CacheError)`: Key mismatch or segment in invalid state
    fn mark_deleted(&self, offset: u32, key: &[u8]) -> Result<bool, CacheError>;

    // ========== Maintenance ==========

    /// Get the merge count (times this segment was a merge destination).
    fn merge_count(&self) -> u16;

    /// Increment the merge count (saturates at u16::MAX).
    fn increment_merge_count(&self);

    /// Reset the segment for reuse.
    ///
    /// Clears all data and statistics. Should only be called when
    /// the segment is in `Locked` state with ref_count == 0.
    fn reset(&self);
}

/// Extension trait for segments that support zero-copy access via guards.
pub trait SegmentGuard: Segment {
    /// The guard type returned by `get_item`.
    type Guard<'a>: ItemGuard<'a>
    where
        Self: 'a;

    /// Get a zero-copy guard for an item.
    ///
    /// The guard holds a reference to the segment and increments the
    /// reference count, preventing the segment from being cleared while
    /// the guard exists.
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `key`: Expected key (for verification)
    ///
    /// # Returns
    /// A guard providing access to key, value, and optional data.
    fn get_item(&self, offset: u32, key: &[u8]) -> Result<Self::Guard<'_>, CacheError>;

    /// Get a zero-copy guard for an item that has already been verified.
    ///
    /// This is an optimized path that skips header parsing and key verification,
    /// using pre-parsed header info from a prior `verify_key_unexpired` call.
    /// The caller must ensure the item was recently verified and is valid.
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `header_info`: Pre-parsed `(key_len, optional_len, value_len)` from verification
    ///
    /// # Returns
    /// A guard providing access to key, value, and optional data.
    ///
    /// # Safety
    /// The caller must ensure:
    /// - The header_info matches the actual item at offset
    /// - The item was verified (key match, not deleted, not expired) recently
    fn get_item_verified(
        &self,
        offset: u32,
        header_info: (u8, u8, u32),
    ) -> Result<Self::Guard<'_>, CacheError>;
}

/// Extension trait for segments that support copying items.
///
/// Used for tier migration (demotion/promotion) and merge eviction.
pub trait SegmentCopy: Segment {
    /// Copy items matching a predicate to a destination segment.
    ///
    /// Items are copied from this segment to the destination. The predicate
    /// receives the item's frequency and returns `true` if it should be copied.
    ///
    /// # Type Parameters
    /// - `S`: Destination segment type
    /// - `F`: Predicate function `(frequency) -> should_copy`
    /// - `L`: Location update callback `(old_offset, new_offset) -> ()`
    ///
    /// # Parameters
    /// - `dest`: Destination segment
    /// - `predicate`: Returns `true` for items that should be copied
    /// - `on_copy`: Called for each copied item with old and new offsets
    ///
    /// # Returns
    /// Number of items copied, or `None` if destination is full.
    fn copy_into<S, F, L>(&self, dest: &S, predicate: F, on_copy: L) -> Option<u32>
    where
        S: Segment,
        F: Fn(u8) -> bool,
        L: FnMut(u32, u32);
}

/// Result type for `SegmentPrune::prune_collecting`.
///
/// Contains `(items_retained, items_pruned, bytes_retained, bytes_pruned, items_to_demote)`
/// where `items_to_demote` is a Vec of `(key, value, optional, ttl_secs)` tuples.
pub type PruneCollectingResult = (u32, u32, u32, u32, Vec<(Vec<u8>, Vec<u8>, Vec<u8>, u32)>);

/// Extension trait for segments that support pruning low-frequency items.
///
/// Used in merge eviction to remove cold items.
pub trait SegmentPrune: Segment {
    /// Prune items with frequency below threshold.
    ///
    /// Items with frequency at or below the threshold are marked as deleted.
    /// Their data remains until the segment is cleared, but they are removed
    /// from the hashtable (or converted to ghosts).
    ///
    /// # Type Parameters
    /// - `F`: Frequency lookup function `(key) -> Option<frequency>`
    ///
    /// # Parameters
    /// - `threshold`: Maximum frequency to prune (items with freq <= threshold are pruned)
    /// - `get_frequency`: Returns the frequency for a key
    ///
    /// # Returns
    /// `(items_retained, items_pruned, bytes_retained, bytes_pruned)`
    fn prune<F>(&self, threshold: u8, get_frequency: F) -> (u32, u32, u32, u32)
    where
        F: Fn(&[u8]) -> Option<u8>;

    /// Prune with collection of demoted items.
    ///
    /// Similar to `prune`, but instead of just marking items deleted,
    /// collects them for demotion to another tier.
    ///
    /// # Type Parameters
    /// - `F`: Frequency lookup function `(key) -> Option<frequency>`
    ///
    /// # Parameters
    /// - `threshold`: Maximum frequency to prune
    /// - `get_frequency`: Returns the frequency for a key
    ///
    /// # Returns
    /// A `PruneCollectingResult` containing the pruned items and statistics.
    fn prune_collecting<F>(&self, threshold: u8, get_frequency: F) -> PruneCollectingResult
    where
        F: Fn(&[u8]) -> Option<u8>;
}

/// Extension trait for iterating over items in a segment.
///
/// Used during eviction, migration, and debugging.
pub trait SegmentIter: Segment {
    /// Iterate over all items in the segment.
    ///
    /// The callback receives `(offset, key, value, optional, is_deleted)` for each item.
    /// Returns early if the callback returns `false`.
    ///
    /// # Type Parameters
    /// - `F`: Callback function returning `true` to continue, `false` to stop
    ///
    /// # Parameters
    /// - `callback`: Called for each item in the segment
    fn for_each_item<F>(&self, callback: F)
    where
        F: FnMut(u32, &[u8], &[u8], &[u8], bool) -> bool;

    /// Count items by their properties.
    ///
    /// # Returns
    /// `(total_items, deleted_items, total_bytes, deleted_bytes)`
    fn count_items(&self) -> (u32, u32, u32, u32) {
        let mut total_items = 0u32;
        let mut deleted_items = 0u32;
        let mut total_bytes = 0u32;
        let mut deleted_bytes = 0u32;

        self.for_each_item(|_offset, key, value, optional, is_deleted| {
            let item_size = key.len() + value.len() + optional.len();
            total_items += 1;
            total_bytes += item_size as u32;
            if is_deleted {
                deleted_items += 1;
                deleted_bytes += item_size as u32;
            }
            true
        });

        (total_items, deleted_items, total_bytes, deleted_bytes)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    // Tests will be added when we have a concrete Segment implementation
    // For now, just verify the trait is object-safe where applicable

    #[test]
    fn test_segment_key_verify_is_object_safe() {
        fn _takes_dyn(_: &dyn SegmentKeyVerify) {}
    }

    // Note: Segment trait is not fully object-safe due to associated types
    // in extension traits, but the base Segment trait operations work with
    // concrete types via generics.
}
