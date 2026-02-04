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

use crate::config::{EvictionStrategy, LayerConfig};
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
use crate::segment::{Segment, SegmentGuard, SegmentKeyVerify, SegmentPrune};
use crate::slice_segment::SliceSegment;
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

        // Track which bucket this segment belongs to
        segment.set_bucket_id(bucket_index as u16);

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

    /// Try to compact a segment with its predecessor using a spare segment.
    ///
    /// If the segment (src_b) and its predecessor (src_a) can fit their combined
    /// live items in one segment, reserves a spare segment, copies all live items
    /// from both sources to the spare, updates the hashtable, and replaces the
    /// two sources in the chain with the spare.
    ///
    /// This uses the spare segment approach (like SSD garbage collection) to avoid
    /// trying to fill gaps in sequential segments.
    ///
    /// # Arguments
    /// * `segment_id` - The segment where deletion occurred (src_b)
    /// * `hashtable` - Hashtable for updating item locations during compaction
    ///
    /// # Returns
    /// `true` if compaction occurred, `false` otherwise.
    fn try_compact_segment<H: Hashtable>(&self, segment_id: u32, hashtable: &H) -> bool {
        // src_b = segment where deletion occurred
        let src_b = match self.pool.get(segment_id) {
            Some(s) => s,
            None => return false,
        };

        // Source must be Sealed (not Live - can't compact active segment)
        if src_b.state() != State::Sealed {
            return false;
        }

        // Get the bucket this segment belongs to
        let bucket_id = match src_b.bucket_id() {
            Some(id) => id as usize,
            None => return false,
        };

        let bucket = self.buckets.get_bucket_by_index(bucket_id);

        // Need at least 3 segments: head + src_a + src_b + tail (or more)
        // Because we need src_b to have a predecessor (src_a) and src_b can't be tail
        if bucket.segment_count() < 3 {
            return false;
        }

        // src_b cannot be the tail (tail is Live)
        if bucket.tail() == Some(segment_id) {
            return false;
        }

        // Get predecessor (src_a)
        let src_a_id = match src_b.prev() {
            Some(id) => id,
            None => return false,
        };

        let src_a = match self.pool.get(src_a_id) {
            Some(s) => s,
            None => return false,
        };

        // src_a must also be Sealed
        if src_a.state() != State::Sealed {
            return false;
        }

        // Check if combined live bytes fit in one segment
        let src_a_live = src_a.live_bytes();
        let src_b_live = src_b.live_bytes();
        let segment_capacity = self.pool.segment_size() as u32;

        // Conservative check: live_bytes doesn't include headers, so we need some margin
        // Use 90% of capacity to account for header overhead
        let max_combined = (segment_capacity * 9) / 10;
        if src_a_live + src_b_live > max_combined {
            return false;
        }

        // Try to reserve a spare segment (gracefully degrade if none available)
        let spare_id = match self.pool.reserve() {
            Some(id) => id,
            None => return false,
        };

        let spare = match self.pool.get(spare_id) {
            Some(s) => s,
            None => {
                self.pool.release(spare_id);
                return false;
            }
        };

        // Set up spare segment with the earlier expire_at (min of src_a and src_b)
        let src_a_expire = src_a.expire_at();
        let src_b_expire = src_b.expire_at();
        let min_expire = src_a_expire.min(src_b_expire);
        spare.set_expire_at(min_expire);

        // Copy bucket_id to spare
        if let Some(bucket_idx) = src_a.bucket_id() {
            spare.set_bucket_id(bucket_idx);
        }

        // Transition src_a and src_b to Relinking (allows reads, signals modification)
        if !src_a.cas_metadata(State::Sealed, State::Relinking, None, None) {
            self.pool.release(spare_id);
            return false;
        }
        if !src_b.cas_metadata(State::Sealed, State::Relinking, None, None) {
            // Rollback src_a
            src_a.cas_metadata(State::Relinking, State::Sealed, None, None);
            self.pool.release(spare_id);
            return false;
        }

        // Helper to copy items from a source segment to the spare
        let copy_items = |src: &SliceSegment<'static>, src_id: u32| {
            let header_size = BasicHeader::SIZE;
            let mut offset = 0u32;
            let write_offset = src.write_offset();

            while offset < write_offset {
                if let Some(data) = src.data_slice(offset, header_size) {
                    if let Some(header) = BasicHeader::try_from_bytes(data) {
                        let item_size = header.padded_size() as u32;

                        // Skip deleted items
                        if !header.is_deleted() {
                            let optional_start = offset as usize + header_size;
                            let optional_len = header.optional_len() as usize;
                            let key_start = optional_start + optional_len;
                            let key_len = header.key_len() as usize;
                            let value_start = key_start + key_len;
                            let value_len = header.value_len() as usize;

                            if let (Some(key), Some(value), Some(optional)) = (
                                src.data_slice(key_start as u32, key_len),
                                src.data_slice(value_start as u32, value_len),
                                src.data_slice(optional_start as u32, optional_len),
                            ) {
                                if let Some(new_offset) = spare.append_item(key, value, optional) {
                                    let old_loc =
                                        ItemLocation::new(self.pool.pool_id(), src_id, offset);
                                    let new_loc = ItemLocation::new(
                                        self.pool.pool_id(),
                                        spare_id,
                                        new_offset,
                                    );

                                    // Update hashtable (preserve frequency)
                                    if !hashtable.cas_location(
                                        key,
                                        old_loc.to_location(),
                                        new_loc.to_location(),
                                        true,
                                    ) {
                                        // CAS failed, mark spare copy as deleted
                                        spare.mark_deleted_at_offset(new_offset);
                                    }
                                }
                                // If spare is full, just stop (partial copy is fine)
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
        };

        // Copy items from src_a first (older), then src_b
        copy_items(src_a, src_a_id);
        copy_items(src_b, segment_id);

        // Replace src_a and src_b with spare in the chain
        // This transitions src_a and src_b to AwaitingRelease
        if bucket
            .replace_adjacent_segments(src_a_id, segment_id, spare_id, &self.pool)
            .is_err()
        {
            // Rollback: restore states and release spare
            src_a.cas_metadata(State::Relinking, State::Sealed, None, None);
            src_b.cas_metadata(State::Relinking, State::Sealed, None, None);
            self.pool.release(spare_id);
            return false;
        }

        // Compaction successful!
        // src_a and src_b are now in AwaitingRelease state.
        // They will be returned to the free pool when their last reader drops.
        true
    }

    /// Try to free a segment if it has no live items.
    ///
    /// This is called after `mark_deleted` to eagerly reclaim segments that
    /// become empty due to overwrites or deletes, rather than waiting for
    /// the next eviction cycle.
    ///
    /// Returns `true` if the segment was freed.
    fn try_free_empty_segment(&self, segment_id: u32) -> bool {
        let segment = match self.pool.get(segment_id) {
            Some(s) => s,
            None => return false,
        };

        // Only proceed if segment is truly empty
        if segment.live_items() > 0 {
            return false;
        }

        // Segment must be Sealed to be removed (Live segments are still being written to)
        if segment.state() != State::Sealed {
            return false;
        }

        // Get the bucket this segment belongs to
        let bucket_id = match segment.bucket_id() {
            Some(id) => id as usize,
            None => return false,
        };

        let bucket = self.buckets.get_bucket_by_index(bucket_id);

        // Need at least 2 segments to remove one (keep the Live tail)
        if bucket.segment_count() < 2 {
            return false;
        }

        // Try to remove from chain (handles head vs non-head)
        let removed_id = if bucket.head() == Some(segment_id) {
            bucket.evict_head_segment(&self.pool).ok()
        } else {
            bucket.remove_segment(segment_id, &self.pool).ok()
        };

        if let Some(id) = removed_id {
            // Wait for readers then release
            if let Some(seg) = self.pool.get(id) {
                while seg.ref_count() > 0 {
                    std::hint::spin_loop();
                }
                seg.cas_metadata(State::Draining, State::Locked, None, None);
                seg.cas_metadata(State::Locked, State::Reserved, None, None);
            }
            self.pool.release(id);
            return true;
        }

        false
    }

    /// Process items in an evicted segment with a demotion callback.
    ///
    /// For items that should be demoted, the callback is called with the item data
    /// instead of removing from hashtable. This allows TieredCache to write to disk.
    fn process_evicted_segment_with_demoter<H, F>(
        &self,
        segment_id: u32,
        hashtable: &H,
        mut demoter: F,
    ) where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, Location),
    {
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

        // Get segment TTL for demotion
        let now = Self::now_secs();
        let expire_at = segment.expire_at();
        let remaining_secs = expire_at.saturating_sub(now);
        let segment_ttl = Duration::from_secs(remaining_secs as u64);

        // Process each item in the segment
        let mut offset = 0u32;
        let write_offset = segment.write_offset();

        while offset < write_offset {
            // Get header at offset
            if let Some(data) = segment.data_slice(offset, BasicHeader::SIZE) {
                if let Some(header) = BasicHeader::try_from_bytes(data) {
                    let item_size = header.padded_size() as u32;

                    // Calculate offsets for key, value, optional
                    let optional_start = offset as usize + BasicHeader::SIZE;
                    let optional_len = header.optional_len() as usize;
                    let key_start = optional_start + optional_len;
                    let key_len = header.key_len() as usize;
                    let value_start = key_start + key_len;
                    let value_len = header.value_len() as usize;

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
                                // Extract item data for demotion
                                let optional = segment
                                    .data_slice(optional_start as u32, optional_len)
                                    .unwrap_or(&[]);
                                let value = segment
                                    .data_slice(value_start as u32, value_len)
                                    .unwrap_or(&[]);

                                // Call demoter callback (which will write to disk and update hashtable)
                                demoter(key, value, optional, segment_ttl, location.to_location());
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

    /// Default eviction: weighted random bucket selection, evict head segment
    fn evict_randomfifo<H: Hashtable>(&self, hashtable: &H) -> bool {
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

    /// Default eviction with demoter callback
    fn evict_randomfifo_with_demoter<H, F>(&self, hashtable: &H, demoter: F) -> bool
    where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, Location),
    {
        // Select a bucket for eviction (weighted by segment count)
        let (_, bucket) = match self.buckets.select_bucket_for_eviction() {
            Some(b) => b,
            None => return false,
        };

        // Evict head segment from selected bucket
        match bucket.evict_head_segment(&self.pool) {
            Ok(segment_id) => {
                self.process_evicted_segment_with_demoter(segment_id, hashtable, demoter);
                true
            }
            Err(_) => false,
        }
    }

    /// Merge eviction: select oldest segments and prune low-frequency items
    fn try_merge_eviction<H: Hashtable>(
        &self,
        merge_config: &crate::config::MergeConfig,
        hashtable: &H,
    ) -> bool {
        // Collect candidate segments from all buckets (oldest first)
        let mut candidates: Vec<(usize, u32)> = Vec::new();

        for (bucket_idx, bucket) in self.buckets.iter().enumerate() {
            // Skip buckets with only 1 segment (need to keep the write segment)
            if bucket.segment_count() < 2 {
                continue;
            }

            // Add head segment as candidate
            if let Some(head_id) = bucket.head() {
                candidates.push((bucket_idx, head_id));
            }
        }

        // Need at least min_segments to proceed
        if candidates.len() < merge_config.min_segments {
            // Fall back to random eviction
            return self.evict_randomfifo(hashtable);
        }

        // Prune each candidate segment
        let verifier = SinglePoolVerifier { pool: &self.pool };
        let threshold = self.config.demotion_threshold;

        for (_bucket_idx, segment_id) in candidates.iter().take(merge_config.min_segments) {
            if let Some(segment) = self.pool.get(*segment_id) {
                // Use SegmentPrune to prune low-frequency items
                let (retained, pruned, _bytes_retained, _bytes_pruned) =
                    segment.prune(threshold, |key| hashtable.get_frequency(key, &verifier));

                // Remove pruned items from hashtable
                if pruned > 0 {
                    // Iterate segment again to find and remove deleted items
                    let mut offset = 0u32;
                    let write_offset = segment.write_offset();

                    while offset < write_offset {
                        if let Some(data) = segment.data_slice(offset, BasicHeader::SIZE) {
                            if let Some(header) = BasicHeader::try_from_bytes(data) {
                                let item_size = header.padded_size() as u32;

                                if header.is_deleted() {
                                    // Get key and remove from hashtable
                                    let key_start = offset as usize
                                        + BasicHeader::SIZE
                                        + header.optional_len() as usize;
                                    let key_len = header.key_len() as usize;
                                    if let Some(key) = segment.data_slice(key_start as u32, key_len)
                                    {
                                        let location = ItemLocation::new(
                                            self.pool.pool_id(),
                                            *segment_id,
                                            offset,
                                        );
                                        if self.config.create_ghosts {
                                            hashtable.convert_to_ghost(key, location.to_location());
                                        } else {
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
                }

                // If segment is empty after pruning, reclaim it
                if retained == 0 {
                    self.try_free_empty_segment(*segment_id);
                }
            }
        }

        true
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

        // Check segment-level TTL first (cheap atomic read)
        let now = Self::now_secs();
        let expire_at = segment.expire_at();
        if expire_at > 0 && now >= expire_at {
            return None;
        }

        // Verify key matches (before acquiring ref count)
        let header_info = segment.verify_key_unexpired(location.offset(), key, now)?;

        // Get item using pre-verified header info (single parse path)
        segment
            .get_item_verified(location.offset(), header_info)
            .ok()
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

                    // Try to free segment if now empty
                    self.try_free_empty_segment(location.segment_id());
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

        // Check eviction strategy from config
        match &self.config.eviction_strategy {
            EvictionStrategy::Merge(merge_config) => {
                self.try_merge_eviction(merge_config, hashtable)
            }
            _ => self.evict_randomfifo(hashtable),
        }
    }

    fn evict_with_demoter<H, F>(&self, hashtable: &H, demoter: F) -> bool
    where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, Location),
    {
        // First try to expire any segments (no demotion for expired items)
        if self.try_expire_segments(hashtable) > 0 {
            return true;
        }

        // For now, only support randomfifo with demoter
        // Merge eviction with demoter can be added if needed
        self.evict_randomfifo_with_demoter(hashtable, demoter)
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
        // Validate inputs
        if key.len() > BasicHeader::MAX_KEY_LEN {
            return Err(CacheError::KeyTooLong);
        }
        if optional.len() > BasicHeader::MAX_OPTIONAL_LEN {
            return Err(CacheError::OptionalTooLong);
        }

        // Try to reserve space in current write segment
        loop {
            let segment_id = self.get_or_allocate_write_segment(ttl)?;

            if let Some(segment) = self.pool.get(segment_id) {
                // Try to reserve space for the item (no per-item TTL)
                if let Some((offset, item_size, value_ptr)) =
                    segment.begin_append(key, value_len, optional)
                {
                    let location = ItemLocation::new(self.pool.pool_id(), segment_id, offset);
                    return Ok((location, value_ptr, item_size));
                }

                // Segment is full, clear cached write segment
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

    fn mark_deleted_and_compact<H: Hashtable>(&self, location: ItemLocation, hashtable: &H) {
        if location.pool_id() != self.pool.pool_id() {
            return;
        }

        if let Some(segment) = self.pool.get(location.segment_id()) {
            // Mark the item as deleted (same as mark_deleted)
            let offset = location.offset();
            if let Some(data) = segment.data_slice(offset, BasicHeader::SIZE)
                && let Some(header) = BasicHeader::try_from_bytes(data)
            {
                let key_start =
                    offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
                let key_len = header.key_len() as usize;
                if let Some(key) = segment.data_slice(key_start as u32, key_len) {
                    let _ = segment.mark_deleted(offset, key);

                    // Try to free segment if now empty
                    if self.try_free_empty_segment(location.segment_id()) {
                        return;
                    }

                    // Try compaction with predecessor
                    self.try_compact_segment(location.segment_id(), hashtable);
                }
            }
        }
    }
}

/// Builder for [`TtlLayer`].
pub struct TtlLayerBuilder {
    layer_id: u8,
    config: LayerConfig,
    pool_id: u8,
    segment_size: usize,
    heap_size: usize,
    numa_node: Option<u32>,
    hugepage_size: crate::hugepage::HugepageSize,
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
            numa_node: None,
            hugepage_size: crate::hugepage::HugepageSize::None,
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

    /// Set the hugepage size preference.
    pub fn hugepage_size(mut self, size: crate::hugepage::HugepageSize) -> Self {
        self.hugepage_size = size;
        self
    }

    /// Build the TTL layer.
    pub fn build(self) -> Result<TtlLayer, std::io::Error> {
        let mut builder = MemoryPoolBuilder::new(self.pool_id)
            .per_item_ttl(false) // TTL layer uses segment-level TTL
            .segment_size(self.segment_size)
            .heap_size(self.heap_size)
            .hugepage_size(self.hugepage_size);

        if let Some(node) = self.numa_node {
            builder = builder.numa_node(node);
        }

        let pool = builder.build()?;

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
        use crate::hashtable_impl::MultiChoiceHashtable;

        let layer = create_test_layer();
        let hashtable = MultiChoiceHashtable::new(10);

        // Evict from empty layer should return false
        let evicted = layer.evict(&hashtable);
        assert!(!evicted);
    }

    #[test]
    fn test_expire_empty_layer() {
        use crate::hashtable_impl::MultiChoiceHashtable;

        let layer = create_test_layer();
        let hashtable = MultiChoiceHashtable::new(10);

        // Expire on empty layer should return 0
        let expired = layer.expire(&hashtable);
        assert_eq!(expired, 0);
    }

    #[test]
    fn test_evict_with_items() {
        use crate::hashtable_impl::MultiChoiceHashtable;

        let layer = create_test_layer();
        let hashtable = MultiChoiceHashtable::new(10);

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

    #[test]
    fn test_free_empty_segment_on_delete() {
        // Create layer with small segments so we can fill and empty them
        let layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(1024) // Small segment
            .heap_size(5 * 1024) // 5 segments
            .build()
            .expect("Failed to create test layer");

        let ttl = Duration::from_secs(3600);

        // Write items to fill a segment, then add a second segment
        let mut locations = Vec::new();
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            if let Ok(loc) = layer.write_item(key.as_bytes(), b"v", b"", ttl) {
                locations.push((key, loc));
            }
        }

        // Need at least 2 segments in the bucket for freeing to work
        // Force a second segment by filling the first
        for i in 10..100 {
            let key = format!("key_{:03}", i);
            if let Ok(loc) = layer.write_item(key.as_bytes(), b"value_padding", b"", ttl) {
                locations.push((key, loc));
            }
        }

        let initial_used = layer.used_segment_count();
        assert!(initial_used >= 2, "Need at least 2 segments");

        // Delete all items in the first segment
        // Find items from the first segment (should all have segment_id of the first allocated)
        let first_segment_id = locations[0].1.segment_id();
        let items_in_first: Vec<_> = locations
            .iter()
            .filter(|(_, loc)| loc.segment_id() == first_segment_id)
            .collect();

        // Delete all items in first segment
        for (_, loc) in &items_in_first {
            layer.mark_deleted(*loc);
        }

        // The empty segment should have been freed
        let final_used = layer.used_segment_count();
        assert!(
            final_used < initial_used,
            "Expected segment to be freed: initial={}, final={}",
            initial_used,
            final_used
        );
    }

    #[test]
    fn test_single_segment_bucket_not_freed() {
        let layer = create_test_layer();
        let ttl = Duration::from_secs(60);

        // Write a single item - creates one segment in the bucket
        let location = layer.write_item(b"only_key", b"value", b"", ttl).unwrap();

        assert_eq!(layer.used_segment_count(), 1);

        // Mark deleted - segment should NOT be freed (it's the only one in bucket)
        layer.mark_deleted(location);

        // Segment is still in use (can't free the only segment in a bucket)
        // The segment count stays the same because we can't remove a Live segment
        assert_eq!(layer.used_segment_count(), 1);
    }
}
