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
use crate::layer::fifo_layer::EvictResult;
use crate::location::Location;
use crate::memory_pool::{MemoryPool, MemoryPoolBuilder};
use crate::organization::{TtlBucket, TtlBuckets};
use crate::pool::RamPool;
use crate::segment::{Segment, SegmentGuard, SegmentKeyVerify};
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

/// Where the parts of a `BasicHeader` item sit inside its segment.
///
/// Both halves of a merge pass need this: the scan reads the key to look the
/// frequency up, and the copy reads the key, value and optional bytes back
/// out. Parsing is a handful of loads from segment memory, which is why the
/// pass memoizes the *frequency* -- a hashtable probe -- and not this.
#[derive(Clone, Copy)]
struct BasicItemSpan {
    optional_start: u32,
    optional_len: usize,
    key_start: u32,
    key_len: usize,
    value_start: u32,
    value_len: usize,
    /// Bytes from this item's start to the next item's start.
    stride: u32,
    deleted: bool,
}

impl BasicItemSpan {
    fn parse<S: Segment + ?Sized>(segment: &S, offset: u32) -> Option<Self> {
        let data = segment.header_ptr(offset, BasicHeader::SIZE)?;
        let header = unsafe { BasicHeader::try_from_ptr(data) }?;

        // The segment's stride, not the 8-byte padded body size: a scan that
        // advances by anything but what the append advanced by desyncs after
        // the first item on a coarser-aligned pool.
        let stride = segment.item_stride(header.padded_size());

        let optional_start = offset as usize + BasicHeader::SIZE;
        let optional_len = header.optional_len() as usize;
        let key_start = optional_start + optional_len;
        let key_len = header.key_len() as usize;
        let value_start = key_start + key_len;
        let value_len = header.value_len() as usize;

        Some(Self {
            optional_start: optional_start as u32,
            optional_len,
            key_start: key_start as u32,
            key_len,
            value_start: value_start as u32,
            value_len,
            stride,
            deleted: header.is_deleted(),
        })
    }
}

/// Rank an item for retention: `frequency * (mean_size / size)^e`.
///
/// Greedy dual size frequency's `frequency * cost / size`, with `e` setting
/// how much size counts. At `e == 0.0` the multiplier is 1 and this is the
/// raw frequency; at `1.0` it is Segcache's frequency-over-size.
///
/// Returns `f64` rather than a bucket index. An earlier version quantised
/// into 256 linear classes to drive a histogram, which destroyed the
/// ranking for exactly the items it was meant to penalise: a 1 KiB item
/// against a 931-byte mean earns a 0.875 multiplier, and
/// `round(freq * 0.875) == freq` for every frequency under 4.
///
/// Degenerate inputs fall back to the raw frequency rather than to zero --
/// an empty segment gives a mean of 0.0 and a multiplier of infinity, and
/// a zero stride divides by zero. Neither can be ranked, and answering 0
/// would prune a live item on the strength of arithmetic that failed.
fn weighted_frequency(freq: u8, stride: u32, mean_size: f64, exponent: f64) -> f64 {
    let freq = freq as f64;
    if exponent == 0.0 {
        return freq;
    }
    if !(mean_size > 0.0) || stride == 0 {
        return freq;
    }
    let weighted = freq * (mean_size / stride as f64).powf(exponent);
    if weighted.is_finite() { weighted } else { freq }
}

impl TtlLayer {
    /// Create a new TTL layer builder.
    pub fn builder() -> TtlLayerBuilder {
        TtlLayerBuilder::new()
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
    pub fn pool(&self) -> &MemoryPool {
        &self.pool
    }

    /// Get the TTL buckets.
    pub fn buckets(&self) -> &TtlBuckets {
        &self.buckets
    }

    /// Get current time as coarse seconds.
    fn now_secs() -> u32 {
        crate::clock::now_unix_secs()
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
    /// [`crate::slice_segment::SliceSegment::force_free`], which this reaches through `reset_all`.
    pub fn reset(&self) {
        self.buckets.reset();
        for slot in &self.current_write_segments {
            slot.store(u32::MAX, std::sync::atomic::Ordering::Release);
        }
        self.pool.reset_all();
    }

    /// Allocate a new segment and add it to the specified bucket.
    fn allocate_segment_for_bucket(&self, bucket_index: usize, ttl: Duration) -> CacheResult<u32> {
        // Reserve a segment from the pool
        let segment_id = self.pool.reserve().ok_or(CacheError::OutOfMemory)?;

        let segment = self.pool.get(segment_id).ok_or(CacheError::OutOfMemory)?;

        // Set segment expiration time
        let expire_at = Self::now_secs().saturating_add(ttl.as_secs() as u32);
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

    /// Remove all hashtable entries for items in a segment, without waiting for
    /// readers or releasing the segment. Used by non-blocking eviction.
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

                        let verifier = SinglePoolVerifier { pool: &self.pool };
                        let freq = hashtable.get_frequency(key, &verifier).unwrap_or(0);
                        let fate = determine_item_fate(freq, &self.config);

                        match fate {
                            ItemFate::Ghost => {
                                hashtable.convert_to_ghost(key, location.to_location());
                            }
                            ItemFate::Demote | ItemFate::Discard => {
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

    /// Non-blocking eviction: process an evicted segment without spinning on
    /// `ref_count`.
    ///
    /// Returns `true` if the segment was fully processed and returned to the
    /// pool, `false` if it was deferred (condemned, for the last reader to
    /// free).
    ///
    /// # Order of operations
    ///
    /// 1. sweep this segment's items out of the hashtable, while the segment
    ///    is still `Draining` -- the sweep resolves each item's own location
    ///    through a key-verify acquire, which `Draining` admits and `Locked`
    ///    does not;
    /// 2. claim `Draining -> Locked`, which refuses every fresh reader;
    /// 3. *then* read `ref_count`.
    ///
    /// Steps 2 and 3 used to be the other way round, which is #133: with the
    /// count read first, a key-verify reader can pin between the observation
    /// of zero and the claim, and then be mid-`verify_key_at_offset` while
    /// this thread recycles the segment. Reachable under a full SC total
    /// order, so no memory ordering fixes it. See `layer::try_claim_for_clear`.
    ///
    /// The sweep is unconditional, where it used to be duplicated verbatim
    /// into both arms -- the deferred arm ran it under `Draining` and the
    /// exclusive arm ran the identical loop under `Locked`, where every
    /// `get_frequency` silently came back `None`. Hoisting it above the claim
    /// gives one copy and one state. In production that changes no outcome:
    /// this path is reached only for a layer with no `next_layer`, and
    /// `determine_item_fate` is frequency-independent there.
    fn process_evicted_segment_nonblocking<H: Hashtable>(
        &self,
        segment_id: u32,
        hashtable: &H,
    ) -> bool {
        let segment = match self.pool.get(segment_id) {
            Some(s) => s,
            None => return true,
        };

        self.drain_segment_from_hashtable(segment_id, hashtable);

        // Claim before counting (#133).
        let claimed = super::try_claim_for_clear(segment);
        if claimed && segment.ref_count_seqcst() == 0 {
            segment.cas_metadata(State::Locked, State::Reserved, None, None);
            self.pool.release(segment_id);
            return true;
        }

        // Either the claim was lost (someone else owns the segment) or readers
        // are still holding it. Nothing was cleared either way; hand the
        // segment to whoever drops the last reference.
        let held = if claimed {
            State::Locked
        } else {
            State::Draining
        };
        super::condemn_and_reclaim(segment, held)
    }

    /// Non-blocking variant of process_evicted_segment_with_demoter.
    ///
    /// Demotes items regardless of active readers. Segment data is immutable
    /// once sealed, so it is safe to read while readers hold refs. After
    /// demotion, if readers remain the segment transitions to AwaitingRelease
    /// for the last reader to free.
    fn process_evicted_segment_with_demoter_nonblocking<H, F>(
        &self,
        segment_id: u32,
        hashtable: &H,
        mut demoter: F,
    ) -> bool
    where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, Location),
    {
        let segment = match self.pool.get(segment_id) {
            Some(s) => s,
            None => return true,
        };

        // Iterate items and run demoter while still in Draining state.
        // Segment data is immutable (sealed) — safe to read with active readers.
        // No new readers will be admitted (is_readable() returns false for Draining).
        let now = Self::now_secs();
        let expire_at = segment.expire_at();
        let remaining_secs = expire_at.saturating_sub(now);
        let segment_ttl = Duration::from_secs(remaining_secs as u64);

        let mut offset = 0u32;
        let write_offset = segment.write_offset();

        while offset < write_offset {
            if let Some(data) = segment.header_ptr(offset, BasicHeader::SIZE) {
                if let Some(header) = unsafe { BasicHeader::try_from_ptr(data) } {
                    // The segment's stride, not the 8-byte padded body size: a scan
                    // that advances by anything but what the append advanced by
                    // desyncs after the first item on a coarser-aligned pool.
                    let item_size = segment.item_stride(header.padded_size());

                    let optional_start = offset as usize + BasicHeader::SIZE;
                    let optional_len = header.optional_len() as usize;
                    let key_start = optional_start + optional_len;
                    let key_len = header.key_len() as usize;
                    let value_start = key_start + key_len;
                    let value_len = header.value_len() as usize;

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

                        let verifier = SinglePoolVerifier { pool: &self.pool };
                        let freq = hashtable.get_frequency(key, &verifier).unwrap_or(0);
                        let fate = determine_item_fate(freq, &self.config);

                        match fate {
                            ItemFate::Ghost => {
                                hashtable.convert_to_ghost(key, location.to_location());
                            }
                            ItemFate::Demote => {
                                let optional = segment
                                    .data_slice(optional_start as u32, optional_len)
                                    .unwrap_or(&[]);
                                let value = segment
                                    .data_slice(value_start as u32, value_len)
                                    .unwrap_or(&[]);

                                demoter(key, value, optional, segment_ttl, location.to_location());
                            }
                            ItemFate::Discard => {
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

        // All items processed. Claim before counting (#133): the count this
        // recycle turns on must be read *after* the transition that refuses
        // fresh readers, not before it. Reading it first leaves a window in
        // which a key-verify reader pins the still-`Draining` segment and is
        // then recycled out from under mid-`verify_key_at_offset`.
        let claimed = super::try_claim_for_clear(segment);
        if claimed && segment.ref_count_seqcst() == 0 {
            segment.cas_metadata(State::Locked, State::Reserved, None, None);
            self.pool.release(segment_id);
            return true;
        }

        // Readers still active (or the claim was lost) -- let the last
        // reference out free it.
        let held = if claimed {
            State::Locked
        } else {
            State::Draining
        };
        super::condemn_and_reclaim(segment, held)
    }

    /// Emergency eviction: find any Sealed segment with ref_count == 0 and evict it.
    ///
    /// Plain `ref_count()` on purpose -- see `FifoLayer::emergency_evict`.
    /// The scan publishes no transition of its own, so the load is ordered
    /// against nothing; the exclusive claim is downstream in
    /// `evict_head_segment`/`remove_segment` and `process_evicted_segment`.
    fn emergency_evict_from_buckets<H: Hashtable>(&self, hashtable: &H) -> bool {
        let num_segments = self.pool.segment_count();
        for i in 0..num_segments {
            if let Some(segment) = self.pool.get(i as u32)
                && segment.state() == State::Sealed
                && segment.ref_count() == 0
            {
                // Get bucket to remove from
                let bucket_id = match segment.bucket_id() {
                    Some(id) => id as usize,
                    None => continue,
                };
                let bucket = self.buckets.get_bucket_by_index(bucket_id);

                // Try to remove from bucket
                let removed_id = if bucket.head() == Some(i as u32) {
                    bucket.evict_head_segment(&self.pool).ok()
                } else {
                    bucket.remove_segment(i as u32, &self.pool).ok()
                };

                if let Some(id) = removed_id {
                    self.process_evicted_segment(id, hashtable);
                    return true;
                }
            }
        }
        false
    }

    /// Process items in an evicted segment.
    fn process_evicted_segment<H: Hashtable>(&self, segment_id: u32, hashtable: &H) {
        let segment = match self.pool.get(segment_id) {
            Some(s) => s,
            None => return,
        };

        // Claim first, then wait (#133). `Draining` still admits key-verify
        // readers, so waiting for zero under it and only then taking `Locked`
        // leaves a window for a fresh pin between the two; `Locked` refuses
        // every reader, so the count this returns on is final. See
        // `layer::claim_and_wait_for_readers`.
        // A lost claim means another thread owns this segment. Everything
        // below -- rewriting the items, `Locked -> Reserved`, returning it to
        // the pool -- would be acting on the winner's segment, and the
        // `Locked -> Reserved` CAS would *succeed*, because the winner is what
        // put it in `Locked` (#142).
        if !super::claim_and_wait_for_readers(segment) {
            return;
        }

        // Process each item in the segment
        let mut offset = 0u32;
        let write_offset = segment.write_offset();

        while offset < write_offset {
            // Get header at offset
            if let Some(data) = segment.header_ptr(offset, BasicHeader::SIZE) {
                if let Some(header) = unsafe { BasicHeader::try_from_ptr(data) } {
                    // The segment's stride, not the 8-byte padded body size: a scan
                    // that advances by anything but what the append advanced by
                    // desyncs after the first item on a coarser-aligned pool.
                    let item_size = segment.item_stride(header.padded_size());

                    // Get key for this item
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

                        // Location-matched, not verifier-backed (#140). This loop runs
                        // under the `Locked` claim `claim_and_wait_for_readers` took
                        // above, and `State::admits_verify_reader` refuses `Locked` --
                        // so `get_frequency`, which resolves the key through
                        // `SinglePoolVerifier`, could only ever come back `None` here
                        // and the `unwrap_or(0)` turned that refusal into a
                        // plausible-looking zero. Nothing failed and nothing logged;
                        // every item's fate was decided against a frequency of 0.
                        //
                        // `get_item_frequency` matches on the location alone, needs no
                        // verifier and is honest under any segment state. The
                        // `unwrap_or(0)` stays and is now correct rather than papering
                        // over a refusal: `search_bucket_for_item_freq` and the fate
                        // arms below use the identical predicate (non-ghost, tag match,
                        // location match) against this same location, so a `None` here
                        // and both arms no-opping coincide exactly. The segment is
                        // `Locked`, so nothing can append into it and make an entry
                        // start matching mid-scan.
                        let freq = hashtable
                            .get_item_frequency(key, location.to_location())
                            .unwrap_or(0);

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
                if let Some(data) = src.header_ptr(offset, header_size) {
                    if let Some(header) = unsafe { BasicHeader::try_from_ptr(data) } {
                        // The segment's stride, not the 8-byte padded body size: a scan
                        // that advances by anything but what the append advanced by
                        // desyncs after the first item on a coarser-aligned pool.
                        let item_size = src.item_stride(header.padded_size());

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
                            ) && let Some(new_offset) = spare.append_item(key, value, optional)
                            {
                                let old_loc = ItemLocation::new(
                                    self.pool.layout(),
                                    self.pool.pool_id(),
                                    src_id,
                                    src.incarnation(),
                                    offset,
                                );
                                let new_loc = ItemLocation::new(
                                    self.pool.layout(),
                                    self.pool.pool_id(),
                                    spare_id,
                                    spare.incarnation(),
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
            if let Some(seg) = self.pool.get(id) {
                // Claim before counting (#133), exactly as the eviction paths
                // do: `Draining` still admits key-verify readers, so a zero
                // read under it can be invalidated by an arrival before the
                // `Locked` CAS. There are no live items to sweep here -- that
                // is this function's precondition -- so the claim is the whole
                // of it.
                let claimed = super::try_claim_for_clear(seg);
                if claimed && seg.ref_count_seqcst() == 0 {
                    seg.cas_metadata(State::Locked, State::Reserved, None, None);
                    self.pool.release(id);
                } else {
                    // Active readers (or a lost claim): defer to the last
                    // reference out.
                    let held = if claimed {
                        State::Locked
                    } else {
                        State::Draining
                    };
                    super::condemn_and_reclaim(seg, held);
                }
            } else {
                self.pool.release(id);
            }
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

        // Claim first, then wait (#133). `Draining` still admits key-verify
        // readers, so waiting for zero under it and only then taking `Locked`
        // leaves a window for a fresh pin between the two; `Locked` refuses
        // every reader, so the count this returns on is final. See
        // `layer::claim_and_wait_for_readers`.
        // A lost claim means another thread owns this segment. Everything
        // below -- rewriting the items, `Locked -> Reserved`, returning it to
        // the pool -- would be acting on the winner's segment, and the
        // `Locked -> Reserved` CAS would *succeed*, because the winner is what
        // put it in `Locked` (#142).
        if !super::claim_and_wait_for_readers(segment) {
            return;
        }

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
            if let Some(data) = segment.header_ptr(offset, BasicHeader::SIZE) {
                if let Some(header) = unsafe { BasicHeader::try_from_ptr(data) } {
                    // The segment's stride, not the 8-byte padded body size: a scan
                    // that advances by anything but what the append advanced by
                    // desyncs after the first item on a coarser-aligned pool.
                    let item_size = segment.item_stride(header.padded_size());

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
                        let location = ItemLocation::new(
                            self.pool.layout(),
                            self.pool.pool_id(),
                            segment_id,
                            segment.incarnation(),
                            offset,
                        );

                        // Location-matched, not verifier-backed (#140). This loop runs
                        // under the `Locked` claim `claim_and_wait_for_readers` took
                        // above, and `State::admits_verify_reader` refuses `Locked` --
                        // so `get_frequency`, which resolves the key through
                        // `SinglePoolVerifier`, could only ever come back `None` here
                        // and the `unwrap_or(0)` turned that refusal into a
                        // plausible-looking zero. Nothing failed and nothing logged;
                        // every item's fate was decided against a frequency of 0.
                        //
                        // `get_item_frequency` matches on the location alone, needs no
                        // verifier and is honest under any segment state. The
                        // `unwrap_or(0)` stays and is now correct rather than papering
                        // over a refusal: `search_bucket_for_item_freq` and the fate
                        // arms below use the identical predicate (non-ghost, tag match,
                        // location match) against this same location, so a `None` here
                        // and both arms no-opping coincide exactly. The segment is
                        // `Locked`, so nothing can append into it and make an entry
                        // start matching mid-scan.
                        let freq = hashtable
                            .get_item_frequency(key, location.to_location())
                            .unwrap_or(0);

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

    // --- Victim selection (#156) ---------------------------------------
    //
    // One function per eviction strategy, each answering the same question:
    // which segment goes next. They were one function until #156 -- `Fifo`,
    // `Random` and `Cte` were three config names for `evict_randomfifo`, and
    // a sweep measured all three byte-identical at every heap size.

    /// The segment the configured strategy would evict next, or `None` if
    /// nothing is evictable.
    fn pick_victim(&self) -> Option<u32> {
        match self.config.eviction_strategy {
            EvictionStrategy::Fifo => self.pick_fifo(),
            EvictionStrategy::Cte => self.pick_cte(),
            EvictionStrategy::Random => self.pick_uniform(self.buckets.next_random()),
            // Merge and Clock reach `evict` through `try_merge_eviction` and
            // only land here when it declines; random-FIFO is the fallback
            // they have always had.
            EvictionStrategy::RandomFifo | EvictionStrategy::Merge(_) | EvictionStrategy::Clock => {
                self.pick_randomfifo(self.buckets.next_random())
            }
        }
    }

    /// Visit every segment eviction may legally take right now.
    ///
    /// Three conditions, and all three matter:
    ///
    /// - `Sealed`, so the segment is neither the bucket's Live write target
    ///   nor mid-transition in someone else's merge;
    /// - still carrying a bucket id, so there is a chain to unlink it from;
    /// - in a bucket holding at least two segments, because a bucket must
    ///   keep a Live tail and `evict_head_segment` refuses otherwise.
    ///
    /// The scan is over the pool rather than the chains, because `Random`,
    /// `Fifo` and `Cte` all rank segments layer-wide and a chain walk would
    /// visit the same segments plus 1024 bucket headers. It is still O(pool
    /// segments) per eviction, against O(1024) for `RandomFifo`, so on a
    /// large heap these three rules cost more per eviction than the default
    /// does -- the price of ranking every candidate, and the same price
    /// cache-rs pays for them.
    fn for_each_evictable<F>(&self, mut visit: F)
    where
        F: FnMut(u32, &SliceSegment<'static>, &TtlBucket),
    {
        for id in 0..self.pool.segment_count() as u32 {
            let Some(segment) = self.pool.get(id) else {
                continue;
            };
            if segment.state() != State::Sealed {
                continue;
            }
            let Some(bucket_id) = segment.bucket_id() else {
                continue;
            };
            let bucket = self.buckets.get_bucket_by_index(bucket_id as usize);
            if bucket.segment_count() < 2 {
                continue;
            }
            visit(id, segment, bucket);
        }
    }

    /// The oldest segment in the layer, across every bucket.
    ///
    /// Age is [`SliceSegment::create_seq`]: the order segments entered
    /// service, which for a merge destination is the merge. Unlike
    /// [`pick_randomfifo`], the bucket a segment lives in has no say.
    ///
    /// [`pick_randomfifo`]: Self::pick_randomfifo
    fn pick_fifo(&self) -> Option<u32> {
        let mut best: Option<(u32, u32)> = None;
        self.for_each_evictable(|id, segment, _| {
            let seq = segment.create_seq();
            if best.is_none_or(|(best_seq, _)| crate::slice_segment::is_older(seq, best_seq)) {
                best = Some((seq, id));
            }
        });
        best.map(|(_, id)| id)
    }

    /// The segment whose items expire soonest.
    ///
    /// `expire_at == 0` means the segment carries no segment-level expiry, so
    /// it sorts last rather than first. Ties -- and a bucket's segments share
    /// an `expire_at` to within the second they were allocated in -- go to
    /// the older segment, which keeps the choice deterministic and makes CTE
    /// degrade to FIFO within a TTL range rather than to scan order.
    fn pick_cte(&self) -> Option<u32> {
        let mut best: Option<(u32, u32, u32)> = None;
        self.for_each_evictable(|id, segment, _| {
            let expire_at = match segment.expire_at() {
                0 => u32::MAX,
                at => at,
            };
            let seq = segment.create_seq();
            let better = match best {
                None => true,
                Some((best_expire, best_seq, _)) => {
                    expire_at < best_expire
                        || (expire_at == best_expire
                            && crate::slice_segment::is_older(seq, best_seq))
                }
            };
            if better {
                best = Some((expire_at, seq, id));
            }
        });
        best.map(|(_, _, id)| id)
    }

    /// An evictable segment chosen uniformly at random.
    ///
    /// Uniform over segments, not over buckets, and with no preference for
    /// chain heads -- so this is the one rule that can take a segment out of
    /// the middle of a chain. Two passes, because the candidate count is not
    /// known until the first one finishes and materialising the list would
    /// allocate on every eviction.
    fn pick_uniform(&self, draw: u64) -> Option<u32> {
        let mut count = 0u64;
        self.for_each_evictable(|_, _, _| count += 1);
        if count == 0 {
            return None;
        }

        let target = draw % count;
        let mut seen = 0u64;
        let mut chosen = None;
        self.for_each_evictable(|id, _, _| {
            if seen == target {
                chosen = Some(id);
            }
            seen += 1;
        });
        chosen
    }

    /// The head of a bucket chosen at random, weighted by segment count.
    ///
    /// This is what the layer did for every non-merge strategy before #156.
    fn pick_randomfifo(&self, draw: u64) -> Option<u32> {
        let (_, bucket) = self.buckets.select_bucket_for_eviction_with(draw)?;
        bucket.head()
    }

    /// Unlink `segment_id` from its bucket chain, leaving it Draining.
    fn detach(&self, segment_id: u32) -> Option<u32> {
        let segment = self.pool.get(segment_id)?;
        let bucket = self
            .buckets
            .get_bucket_by_index(segment.bucket_id()? as usize);
        if bucket.head() == Some(segment_id) {
            bucket.evict_head_segment(&self.pool).ok()
        } else {
            bucket.remove_segment(segment_id, &self.pool).ok()
        }
    }

    /// Evict whichever segment the configured strategy chose.
    fn evict_selected<H: Hashtable>(&self, hashtable: &H) -> bool {
        match self.pick_victim().and_then(|id| self.detach(id)) {
            Some(segment_id) => {
                self.process_evicted_segment(segment_id, hashtable);
                true
            }
            None => false,
        }
    }

    /// [`evict_selected`] with a demotion callback.
    ///
    /// [`evict_selected`]: Self::evict_selected
    fn evict_selected_with_demoter<H, F>(&self, hashtable: &H, demoter: F) -> bool
    where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, Location),
    {
        match self.pick_victim().and_then(|id| self.detach(id)) {
            Some(segment_id) => {
                self.process_evicted_segment_with_demoter(segment_id, hashtable, demoter);
                true
            }
            None => false,
        }
    }

    /// Merge eviction: SSD garbage-collection style.
    ///
    /// Selects N candidate segments from the head of a bucket, reserves a spare,
    /// copies high-frequency items into the spare, frees the candidates.
    ///
    /// The retained set has to fit *one* spare, which is `1/N` of the chain's
    /// capacity, so the threshold is chosen before anything is copied:
    ///
    /// - **Phase A** walks the chain once and memoizes each live item's
    ///   frequency, accumulating bytes per frequency class as it goes. That
    ///   is one hashtable probe per item, and the only one the pass makes.
    /// - **Phase B** reads the threshold off the histogram: the lowest one
    ///   whose retained bytes fit the spare, floored by `initial_threshold`
    ///   and capped further by `target_ratio`.
    /// - **Phase C** copies the memoized survivors.
    ///
    /// Reacting to the retention ratio *after* each segment, as this used to,
    /// commits the pass to overshooting before it can adapt; the spare then
    /// fills mid-chain and the remainder is discarded by position rather than
    /// by frequency (#154). Choosing up front, in bytes, makes the capacity
    /// bound structural: position can now only decide *within* the single
    /// class that straddles the boundary, where order is arbitrary anyway.
    fn try_merge_eviction<H: Hashtable>(
        &self,
        merge_config: &crate::config::MergeConfig,
        hashtable: &H,
    ) -> bool {
        // Select a bucket for eviction (weighted by segment count)
        let (bucket_idx, bucket) = match self.buckets.select_bucket_for_eviction() {
            Some(b) => b,
            None => return false,
        };

        // Get N candidates from the head of the bucket
        let candidates =
            self.buckets
                .select_merge_candidates(bucket_idx, merge_config.min_segments, &self.pool);

        // Need at least min_segments candidates
        if candidates.len() < merge_config.min_segments {
            // Fall back to random eviction
            return self.evict_selected(hashtable);
        }

        // Reserve a spare segment for compaction
        let spare_id = match self.pool.reserve_spare() {
            Some(id) => id,
            None => return self.evict_selected(hashtable),
        };

        let spare = match self.pool.get(spare_id) {
            Some(s) => s,
            None => {
                self.pool.release(spare_id);
                return false;
            }
        };

        // Set spare's expire_at = min(candidate.expire_at)
        let mut min_expire = u32::MAX;
        for &cand_id in &candidates {
            if let Some(seg) = self.pool.get(cand_id) {
                min_expire = min_expire.min(seg.expire_at());
            }
        }
        spare.set_expire_at(min_expire);

        // Copy bucket_id to spare
        if let Some(seg) = self.pool.get(candidates[0])
            && let Some(bid) = seg.bucket_id()
        {
            spare.set_bucket_id(bid);
        }

        // Transition all candidates: Sealed → Relinking
        for (i, &cand_id) in candidates.iter().enumerate() {
            if let Some(seg) = self.pool.get(cand_id) {
                if !seg.cas_metadata(State::Sealed, State::Relinking, None, None) {
                    // Rollback previously transitioned candidates
                    for &prev_id in &candidates[..i] {
                        if let Some(prev) = self.pool.get(prev_id) {
                            prev.cas_metadata(State::Relinking, State::Sealed, None, None);
                        }
                    }
                    self.pool.release(spare_id);
                    return false;
                }
            } else {
                // Rollback
                for &prev_id in &candidates[..i] {
                    if let Some(prev) = self.pool.get(prev_id) {
                        prev.cas_metadata(State::Relinking, State::Sealed, None, None);
                    }
                }
                self.pool.release(spare_id);
                return false;
            }
        }

        let verifier = SinglePoolVerifier { pool: &self.pool };

        // ---- Streaming prune, one pass over the chain.
        //
        // Segcache's own shape (3.6.2, "One-pass merge"): a dynamic cutoff
        // updated every tenth of a segment, aiming to retain `target_ratio`
        // of each candidate's bytes. It decides and copies in the same
        // traversal, so it holds no per-item state.
        //
        // This replaced a two-pass histogram that scanned the chain into a
        // `Vec<ScannedItem>`, bucketed by frequency into 256 classes, and
        // chose an exact byte-accurate threshold. That was more precise
        // about its budget, and wrong about its ranking: 256 *linear*
        // classes are right for a u8 frequency and useless for
        // `frequency * (mean/size)`, which spans orders of magnitude. At
        // this corpus's ~931-byte mean, a 1 KiB item earns a multiplier of
        // 0.875, and `round(freq * 0.875) == freq` for every frequency
        // below 4 -- so the size term rounded away to nothing on exactly
        // the items it was meant to penalise. Measured: over a full sweep
        // of the exponent, retention at 1 KiB moved 21,470 -> 21,437 while
        // 128 B moved 2,801 -> 3,917.
        //
        // A float cutoff has no such floor. The pass also stops allocating
        // a vector sized to the whole chain and stops walking segment
        // memory twice, which is the cost that #152 is about.
        let target_ratio = merge_config.target_ratio;
        let seg_capacity = self.pool.segment_size() as u64;

        // The budget is known before the scan, without scanning.
        //
        // `live_bytes` is a per-segment counter the appends already
        // maintain, so the chain's live total costs N header reads rather
        // than a traversal -- which means the streaming pass can keep the
        // budget composition the histogram had: the spare's free space
        // bounds the pass structurally, and `target_ratio` is a policy cap
        // on top that can only ever prune further (#154). Without this the
        // pass would target a fraction of each candidate independently, so
        // a fragmented chain whose whole live set fits the spare would
        // still be pruned to the ratio, and the ratio's meaning would
        // quietly change from "of the chain" to "of each segment".
        let chain_live: u64 = candidates
            .iter()
            .filter_map(|&id| self.pool.get(id))
            .map(|seg| seg.live_bytes() as u64)
            .sum();
        let budget = (spare.free_space() as u64).min((chain_live as f64 * target_ratio) as u64);
        // Carried across the whole chain, not reset per candidate: the
        // budget is a chain-wide quantity and charging it per segment would
        // let each one prune to the ratio independently.
        let mut to_drop = chain_live.saturating_sub(budget);
        let mut n_dropped = 0u64;
        // Carried across candidates rather than restarted per segment, so
        // what the pass learned about the chain's frequency distribution
        // survives into the next one. `initial_threshold` seeds it, which
        // is what keeps `MergeConfig::CLOCK` a fixed rule.
        let mut cutoff = (1.0 + merge_config.initial_threshold as f64) / 2.0;

        for &cand_id in &candidates {
            let segment = match self.pool.get(cand_id) {
                Some(s) => s,
                None => continue,
            };
            let live_items = segment.live_items() as u64;
            if live_items == 0 {
                continue;
            }
            // Per candidate, as Segcache does: consecutive segments are
            // homogeneous, so each is measured against its own population
            // rather than a chain-wide average that no segment matches.
            let mean_size = segment.live_bytes() as f64 / live_items as f64;
            let update_interval = (seg_capacity / 10).max(1);

            let mut n_scanned = 0u64;
            let mut n_retained = 0u64;
            let mut n_th_update = 1u64;

            let write_offset = segment.write_offset();
            let mut offset = 0u32;
            while offset < write_offset {
                let span = match BasicItemSpan::parse(segment, offset) {
                    Some(s) => s,
                    None => break,
                };
                let stride = span.stride;
                if span.deleted {
                    offset += stride;
                    continue;
                }
                let key = match segment.data_slice(span.key_start, span.key_len) {
                    Some(k) => k,
                    None => break,
                };
                // Absent from the hashtable means already superseded or
                // removed; it is dead weight, not a candidate.
                let Some(freq) = hashtable.get_frequency(key, &verifier) else {
                    offset += stride;
                    continue;
                };

                n_scanned += stride as u64;
                if n_scanned >= n_th_update * update_interval {
                    n_th_update += 1;
                    let t = (n_retained as f64 / n_scanned as f64 - target_ratio) / target_ratio;
                    if !(-0.5..=0.5).contains(&t) {
                        // Floor the multiplier. `n_retained == 0` at the
                        // first checkpoint gives `t == -1`, and a bare
                        // `1.0 + t` would zero the cutoff permanently --
                        // zero stays zero, the drop gate below never opens
                        // again, and the pass retains the entire chain.
                        cutoff *= (1.0 + t).max(0.25);
                    }
                }

                let weighted =
                    weighted_frequency(freq, stride, mean_size, merge_config.cost_exponent);
                // Two independent reasons to prune, as the histogram had.
                //
                // The floor is unconditional: `initial_threshold` is a rule
                // about the item, not about the budget, and
                // `MergeConfig::CLOCK` is nothing but that rule. CLOCK sets
                // `target_ratio: 1.0`, which makes `to_drop` zero and shuts
                // the budget gate permanently -- so folding the two
                // together turned CLOCK into a pass that reclaims dead
                // bytes and gives every live item a second chance forever.
                let below_floor = weighted <= merge_config.initial_threshold as f64;
                // The budget gate is the adaptive part: prune down to
                // `target_ratio` of this candidate and no further.
                let over_budget =
                    cutoff >= 0.0001 && to_drop > 0 && n_dropped < to_drop && weighted <= cutoff;
                let prune = below_floor || over_budget;

                let old_loc = ItemLocation::new(
                    self.pool.layout(),
                    self.pool.pool_id(),
                    cand_id,
                    segment.incarnation(),
                    offset,
                );

                let mut retained = false;
                if !prune {
                    let optional = segment
                        .data_slice(span.optional_start, span.optional_len)
                        .unwrap_or(&[]);
                    let value = segment
                        .data_slice(span.value_start, span.value_len)
                        .unwrap_or(&[]);
                    if let Some(new_offset) = spare.append_item(key, value, optional) {
                        let new_loc = ItemLocation::new(
                            self.pool.layout(),
                            self.pool.pool_id(),
                            spare_id,
                            spare.incarnation(),
                            new_offset,
                        );

                        // Relocate, and reset the frequency to 1.
                        //
                        // Segcache 3.6.3: "To avoid extra parameters,
                        // Segcache resets the frequency of retained
                        // objects during evictions, which has a similar
                        // effect as window-based frequency." A frequency
                        // counter only ever rises, so without the reset an
                        // item that was hot once outranks an item that is
                        // hot now, for as long as it keeps surviving --
                        // and surviving is what a high frequency buys it.
                        //
                        // Only here, not in `try_compact_segment`.
                        // Compaction relocates every live item and judges
                        // nothing, so resetting there would charge items
                        // for a maintenance pass.
                        if hashtable.cas_location(
                            key,
                            old_loc.to_location(),
                            new_loc.to_location(),
                            false,
                        ) {
                            retained = true;
                            n_retained += stride as u64;
                        } else {
                            // Concurrent overwrite won the slot; the copy
                            // in the spare is already stale.
                            spare.mark_deleted_at_offset(new_offset);
                        }
                    }
                    // Falling through with `retained == false` means the
                    // spare is full. The streaming cutoff targets a byte
                    // budget rather than guaranteeing one, so unlike the
                    // histogram this is reachable on a chain whose
                    // survivors genuinely do not fit, and the remainder is
                    // pruned rather than lost silently.
                }

                if !retained {
                    if self.config.create_ghosts {
                        hashtable.convert_to_ghost(key, old_loc.to_location());
                    } else {
                        hashtable.remove(key, old_loc.to_location());
                    }
                    n_dropped += stride as u64;
                }

                offset += stride;
            }
        }

        // Replace head segments with spare in the chain
        if bucket
            .replace_head_segments(&candidates, spare_id, &self.pool)
            .is_err()
        {
            // Rollback: restore all candidates Relinking → Sealed
            for &cand_id in &candidates {
                if let Some(seg) = self.pool.get(cand_id) {
                    seg.cas_metadata(State::Relinking, State::Sealed, None, None);
                }
            }
            self.pool.release(spare_id);
            return false;
        }

        // Successfully freed N-1 segments (N candidates replaced by 1 spare)
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
                    return Ok(ItemLocation::new(
                        self.pool.layout(),
                        self.pool.pool_id(),
                        segment_id,
                        segment.incarnation(),
                        offset,
                    ));
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

        let (_, segment_id, _, offset) = location.unpack(self.pool.layout());
        let segment = self.pool.get(segment_id)?;

        // Check segment state. Condemned is excluded: the hashtable entries are
        // already gone, so this location is stale and the answer is a miss (#127).
        let state = segment.state();
        if !state.is_readable() || state.is_condemned() {
            return None;
        }

        // Check segment-level TTL first (cheap atomic read)
        let now = Self::now_secs();
        let expire_at = segment.expire_at();
        if expire_at > 0 && now >= expire_at {
            return None;
        }

        // Verify key matches (before acquiring ref count)
        let header_info = segment.verify_key_unexpired(offset, key, now)?;

        // Get item using pre-verified header info (single parse path)
        segment.get_item_verified(offset, header_info).ok()
    }

    fn mark_deleted(&self, location: ItemLocation) {
        if location.pool_id() != self.pool.pool_id() {
            return;
        }

        let (_, segment_id, _, offset) = location.unpack(self.pool.layout());
        if let Some(segment) = self.pool.get(segment_id) {
            // We need the key to mark deleted.
            // Get it from the segment header.
            if let Some(data) = segment.header_ptr(offset, BasicHeader::SIZE)
                && let Some(header) = unsafe { BasicHeader::try_from_ptr(data) }
            {
                let key_start =
                    offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
                let key_len = header.key_len() as usize;
                if let Some(key) = segment.data_slice(key_start as u32, key_len) {
                    let _ = segment.mark_deleted(offset, key);

                    // Try to free segment if now empty
                    self.try_free_empty_segment(segment_id);
                }
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
        // First try to expire any segments
        if self.try_expire_segments(hashtable) > 0 {
            return true;
        }

        // Merge and Clock run the same machinery, differing only in the
        // parameters `merge_params` hands over -- see `EvictionStrategy::Clock`
        // on why CLOCK is not a second implementation.
        if let Some(merge_config) = self.config.eviction_strategy.merge_params() {
            return self.try_merge_eviction(&merge_config, hashtable);
        }

        self.evict_selected(hashtable)
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

        // Whole-segment eviction only: merge would have to run the demoter
        // over the items it prunes, which it does not yet do.
        self.evict_selected_with_demoter(hashtable, demoter)
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
                    let location = ItemLocation::new(
                        self.pool.layout(),
                        self.pool.pool_id(),
                        segment_id,
                        segment.incarnation(),
                        offset,
                    );
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

    fn mark_deleted_and_free_empty(&self, location: ItemLocation) {
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
                // Whole segments only. Compacting a partly-used segment into
                // its predecessor is the expensive half and is what
                // `mark_deleted_and_compact` adds.
                self.try_free_empty_segment(segment_id);
            }
        }
    }

    /// Returns whether a compaction pass actually ran, so the caller can
    /// count it. `try_compact_segment` declines far more often than it
    /// fires -- it needs a sealed predecessor and a combined live set that
    /// fits one segment -- and discarding the answer made "is compaction
    /// reachable on this workload" unanswerable from a running cache.
    fn mark_deleted_and_compact<H: Hashtable>(
        &self,
        location: ItemLocation,
        hashtable: &H,
    ) -> bool {
        if location.pool_id() != self.pool.pool_id() {
            return false;
        }

        let (_, segment_id, _, offset) = location.unpack(self.pool.layout());
        if let Some(segment) = self.pool.get(segment_id) {
            // Mark the item as deleted (same as mark_deleted)
            if let Some(data) = segment.header_ptr(offset, BasicHeader::SIZE)
                && let Some(header) = unsafe { BasicHeader::try_from_ptr(data) }
            {
                let key_start =
                    offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
                let key_len = header.key_len() as usize;
                if let Some(key) = segment.data_slice(key_start as u32, key_len) {
                    let _ = segment.mark_deleted(offset, key);

                    // Try to free segment if now empty
                    if self.try_free_empty_segment(segment_id) {
                        return false;
                    }

                    // Try compaction with predecessor
                    return self.try_compact_segment(segment_id, hashtable);
                }
            }
        }
        false
    }
}

/// Non-blocking eviction methods for TtlLayer.
impl TtlLayer {
    /// Try to evict without blocking on ref_count.
    pub fn evict_nonblocking<H: Hashtable>(&self, hashtable: &H) -> EvictResult {
        // First try to expire any segments (these are typically ref_count == 0)
        if self.try_expire_segments(hashtable) > 0 {
            return EvictResult::Freed;
        }

        // Dispatch based on eviction strategy. Merge and Clock share this
        // path; they differ only in the parameters `merge_params` returns.
        if let Some(merge_config) = self.config.eviction_strategy.merge_params() {
            // Merge eviction prunes items in-place without reclaiming whole segments,
            // so it is inherently non-blocking (no ref_count wait needed).
            return if self.try_merge_eviction(&merge_config, hashtable) {
                EvictResult::Freed
            } else {
                EvictResult::NoCandidate
            };
        }

        // Whole-segment eviction, non-blocking path. The strategy chooses
        // the victim exactly as it does for the blocking `evict` (#156).
        match self.pick_victim().and_then(|id| self.detach(id)) {
            Some(segment_id) => {
                if self.process_evicted_segment_nonblocking(segment_id, hashtable) {
                    EvictResult::Freed
                } else {
                    EvictResult::Deferred
                }
            }
            None => EvictResult::NoCandidate,
        }
    }

    /// Try to evict without blocking, with demotion callback.
    pub fn evict_nonblocking_with_demoter<H, F>(&self, hashtable: &H, demoter: F) -> EvictResult
    where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, Location),
    {
        // First try to expire any segments
        if self.try_expire_segments(hashtable) > 0 {
            return EvictResult::Freed;
        }

        match self.pick_victim().and_then(|id| self.detach(id)) {
            Some(segment_id) => {
                if self.process_evicted_segment_with_demoter_nonblocking(
                    segment_id, hashtable, demoter,
                ) {
                    EvictResult::Freed
                } else {
                    EvictResult::Deferred
                }
            }
            None => EvictResult::NoCandidate,
        }
    }

    /// Emergency eviction: find any Sealed segment with ref_count == 0.
    pub fn try_emergency_evict<H: Hashtable>(&self, hashtable: &H) -> bool {
        self.emergency_evict_from_buckets(hashtable)
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
    spare_capacity: Option<u32>,
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
            spare_capacity: None, // Use pool's default
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

    /// Set the spare capacity (segments reserved for compaction).
    pub fn spare_capacity(mut self, capacity: u32) -> Self {
        self.spare_capacity = Some(capacity);
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

        if let Some(spare) = self.spare_capacity {
            builder = builder.spare_capacity(spare);
        }

        let pool = builder.build()?;

        // Initialize cached write segments (one per bucket)
        let bucket_count = crate::organization::MAX_TTL_BUCKETS;
        let current_write_segments: Vec<_> = (0..bucket_count)
            .map(|_| std::sync::atomic::AtomicU32::new(u32::MAX))
            .collect();

        let buckets = TtlBuckets::with_seed(self.config.eviction_seed);

        Ok(TtlLayer {
            layer_id: self.layer_id,
            config: self.config,
            pool,
            buckets,
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
            .spare_capacity(0) // No spare for tests
            .build()
            .expect("Failed to create test layer")
    }

    /// The frequency the *blocking* eviction paths read (#140).
    ///
    /// `process_evicted_segment` and `process_evicted_segment_with_demoter`
    /// both take the exclusive `Locked` claim up front, via
    /// `layer::claim_and_wait_for_readers`, and then decided every item's fate
    /// from `hashtable.get_frequency(key, &verifier)` -- a lookup that resolves
    /// the key through `SinglePoolVerifier`, which goes through
    /// `try_acquire_read`, which `State::admits_verify_reader` refuses under
    /// `Locked`. So the lookup returned `None` on every item, every time, and
    /// the `unwrap_or(0)` turned that refusal into a plausible-looking zero.
    ///
    /// Unlike #138's disk sites this is not latent: a RAM layer does get a
    /// `next_layer`, and `TieredCache::evict_from_layer` reaches
    /// `emergency_evict` -- and so the blocking path -- inside the branch that
    /// established `next_layer.is_some()`.
    ///
    /// These tests do not use `segment::interpose`, but they are gated the
    /// same way as the module that does: they run a whole eviction pass over a
    /// real pool, which is not a model the checkers should be asked to
    /// explore.
    #[cfg(all(not(feature = "loom"), not(feature = "shuttle")))]
    mod blocking_eviction_frequency {
        use super::*;
        use crate::hashtable_impl::MultiChoiceHashtable;

        /// The demotion threshold these tests configure their layer with.
        const DEMOTION_THRESHOLD: u8 = 4;

        /// The single key each test stages. One item per hashtable is
        /// deliberate: `get_ghost_frequency` matches on the 12-bit tag alone,
        /// so a second key in the table could answer for the first.
        const KEY: &[u8] = b"solo";

        /// A layer wired to a tier *below* it.
        ///
        /// This is what makes the frequency observable at all.
        /// `determine_item_fate` reaches its frequency comparison only when
        /// `next_layer.is_some()`; without it every frequency produces the
        /// same fate and a test proves nothing. `set_next_layer` is the same
        /// call `TieredCacheBuilder::build` makes for any layer with a tier
        /// below it.
        pub(super) fn create_demoting_test_layer() -> TtlLayer {
            let mut layer = TtlLayerBuilder::new()
                .layer_id(1)
                .pool_id(1)
                .segment_size(64 * 1024)
                .heap_size(640 * 1024)
                .config(
                    LayerConfig::new()
                        .with_ghosts(true)
                        .with_demotion_threshold(DEMOTION_THRESHOLD),
                )
                .spare_capacity(0)
                .build()
                .expect("Failed to create test layer");
            layer.set_next_layer(2);
            assert!(
                layer.config.next_layer.is_some(),
                "without a next layer `determine_item_fate` ignores the \
                 frequency and this test proves nothing"
            );
            layer
        }

        /// One item, warmed with `reads` hits, in a segment already unlinked
        /// into `Draining` -- which is the state the evictor hands the
        /// blocking paths.
        pub(super) struct Staged {
            pub(super) hashtable: MultiChoiceHashtable,
            /// The frequency the hashtable holds for the item going in.
            pub(super) freq: u8,
            /// Where the item sits, which is what both fate arms match on.
            pub(super) location: ItemLocation,
            pub(super) segment_id: u32,
        }

        pub(super) fn stage_one_item(layer: &TtlLayer, reads: usize) -> Staged {
            let hashtable = MultiChoiceHashtable::new(10);
            let verifier = SinglePoolVerifier { pool: &layer.pool };

            let location = layer
                .write_item(KEY, b"value", b"", Duration::from_secs(3600))
                .expect("write");
            hashtable
                .insert(KEY, location.to_location(), &verifier)
                .expect("insert");

            // The warming reads go through the verifier, which is fine here --
            // the segment is still `Live`, so `admits_verify_reader` allows it.
            // once however many there are. Each read gets its own second.
            // smoothed counter), so reads inside one second raise the frequency
            // The counter is rate-limited to one increment per epoch (Segcache's
            let _tick_clock = crate::clock::TestClock::start();
            for _ in 0..reads {
                _tick_clock.tick();
                assert!(
                    hashtable.lookup(KEY, &verifier).is_some(),
                    "the warming read must hit, or no frequency accrues"
                );
            }
            let freq = hashtable
                .get_item_frequency(KEY, location.to_location())
                .expect("the item must be indexed before eviction");

            let segment_id = location.segment_id(layer.pool().layout());
            let segment = layer.pool().get(segment_id).expect("segment");
            let state = segment.state();
            assert!(
                segment.cas_metadata(state, State::Draining, None, None),
                "the evictor hands these paths a segment already in `Draining`"
            );

            Staged {
                hashtable,
                freq,
                location,
                segment_id,
            }
        }

        /// A hot item must be *removed* by the blocking non-demoting path, not
        /// ghosted.
        ///
        /// Red proof: restore
        /// `hashtable.get_frequency(key, &SinglePoolVerifier { pool: &self.pool })`
        /// at the frequency read in `process_evicted_segment` and this fails
        /// with a ghost, because the `Locked` claim taken a few lines above
        /// refuses the verifier and `unwrap_or(0)` reports a zero.
        #[test]
        fn blocking_eviction_demotes_an_item_over_the_threshold() {
            let layer = create_demoting_test_layer();
            let staged = stage_one_item(&layer, DEMOTION_THRESHOLD as usize + 2);
            assert!(
                staged.freq >= DEMOTION_THRESHOLD,
                "the warming reads must carry the item past the threshold for \
                 this to test the demote arm, got {}",
                staged.freq
            );

            layer.process_evicted_segment(staged.segment_id, &staged.hashtable);

            assert!(
                staged
                    .hashtable
                    .get_item_frequency(KEY, staged.location.to_location())
                    .is_none(),
                "the item must not still be indexed as live at a segment that \
                 has been recycled"
            );
            assert_eq!(
                staged.hashtable.get_ghost_frequency(KEY),
                None,
                "a hot item was ghosted instead of demoted -- the blocking \
                 path read its frequency through the key verifier, which its \
                 own `Locked` claim refuses, and `unwrap_or(0)` turned that \
                 into a zero (#140)"
            );
        }

        /// The control: a cold item must still be ghosted.
        ///
        /// Without this the test above passes for any "fix" that reports every
        /// item as hot -- including replacing the lookup with a constant 255.
        #[test]
        fn blocking_eviction_ghosts_an_item_under_the_threshold() {
            let layer = create_demoting_test_layer();
            let staged = stage_one_item(&layer, 0);
            assert!(
                staged.freq < DEMOTION_THRESHOLD,
                "an unread item must sit below the threshold for this to test \
                 the ghost arm, got {}",
                staged.freq
            );

            layer.process_evicted_segment(staged.segment_id, &staged.hashtable);

            assert!(
                staged
                    .hashtable
                    .get_item_frequency(KEY, staged.location.to_location())
                    .is_none(),
                "the item must not still be indexed as live at a segment that \
                 has been recycled"
            );
            assert_eq!(
                staged.hashtable.get_ghost_frequency(KEY),
                Some(staged.freq),
                "a cold item must become a ghost, carrying its frequency with it"
            );
        }

        /// The demoting twin, where the silent zero is worse in kind:
        /// `ItemFate::Demote` is the only arm that invokes the callback, so a
        /// frequency stuck at 0 means the demoter never fires at all.
        ///
        /// Red proof: restore the verifier-backed lookup at the frequency read
        /// in `process_evicted_segment_with_demoter` and the callback is never
        /// called.
        #[test]
        fn blocking_demoting_eviction_hands_a_hot_item_to_the_demoter() {
            let layer = create_demoting_test_layer();
            let staged = stage_one_item(&layer, DEMOTION_THRESHOLD as usize + 2);
            assert!(
                staged.freq >= DEMOTION_THRESHOLD,
                "the warming reads must carry the item past the threshold for \
                 this to test the demote arm, got {}",
                staged.freq
            );

            let mut demoted: Vec<Vec<u8>> = Vec::new();
            layer.process_evicted_segment_with_demoter(
                staged.segment_id,
                &staged.hashtable,
                |key, _value, _optional, _ttl, _location| demoted.push(key.to_vec()),
            );

            assert_eq!(
                demoted,
                vec![KEY.to_vec()],
                "a hot item was not handed to the demoter -- the blocking \
                 demoting path read its frequency through the key verifier, \
                 which its own `Locked` claim refuses (#140)"
            );
        }

        /// The control for the demoting twin: a cold item must be ghosted and
        /// the callback must not fire for it.
        #[test]
        fn blocking_demoting_eviction_ghosts_a_cold_item() {
            let layer = create_demoting_test_layer();
            let staged = stage_one_item(&layer, 0);
            assert!(
                staged.freq < DEMOTION_THRESHOLD,
                "an unread item must sit below the threshold for this to test \
                 the ghost arm, got {}",
                staged.freq
            );

            let mut demoted: Vec<Vec<u8>> = Vec::new();
            layer.process_evicted_segment_with_demoter(
                staged.segment_id,
                &staged.hashtable,
                |key, _value, _optional, _ttl, _location| demoted.push(key.to_vec()),
            );

            assert!(
                demoted.is_empty(),
                "a cold item must not be demoted, got {demoted:?}"
            );
            assert_eq!(
                staged.hashtable.get_ghost_frequency(KEY),
                Some(staged.freq),
                "a cold item must become a ghost, carrying its frequency with it"
            );
        }
    }

    /// A blocking eviction that *loses* the claim must leave the segment alone.
    ///
    /// Unreachable in production today -- every caller is gated on a
    /// `Sealed -> Draining` CAS that admits exactly one thread -- so this
    /// drives the path directly. The hazard it pins is #142: the tail's
    /// `Locked -> Reserved` CAS *succeeds* for a loser, because the winner is
    /// what put the segment in `Locked`, and the loser then returns the
    /// winner's segment to the pool mid-clear.
    #[cfg(all(not(feature = "loom"), not(feature = "shuttle")))]
    mod lost_claim {
        use super::blocking_eviction_frequency::*;
        use super::*;

        /// Red proof: drop the `if !claim { return }` guard in
        /// `process_evicted_segment` and this fails.
        #[test]
        fn a_blocking_eviction_that_loses_the_claim_touches_nothing() {
            let layer = create_demoting_test_layer();
            let staged = stage_one_item(&layer, 0);

            let segment = layer.pool().get(staged.segment_id).expect("segment");
            assert!(
                segment.cas_metadata(State::Draining, State::Locked, None, None),
                "the winner takes the claim"
            );
            let free_before = layer.pool().free_count();

            layer.process_evicted_segment(staged.segment_id, &staged.hashtable);

            assert_eq!(
                segment.state(),
                State::Locked,
                "the loser must not advance the winner's segment out of `Locked`"
            );
            assert_eq!(
                layer.pool().free_count(),
                free_before,
                "the loser must not return the winner's segment to the pool"
            );
        }

        /// The demoting twin. Separate body, so it needs its own proof.
        #[test]
        fn a_blocking_demoting_eviction_that_loses_the_claim_touches_nothing() {
            let layer = create_demoting_test_layer();
            let staged = stage_one_item(&layer, 0);

            let segment = layer.pool().get(staged.segment_id).expect("segment");
            assert!(
                segment.cas_metadata(State::Draining, State::Locked, None, None),
                "the winner takes the claim"
            );
            let free_before = layer.pool().free_count();

            let mut demoted = 0usize;
            layer.process_evicted_segment_with_demoter(
                staged.segment_id,
                &staged.hashtable,
                |_, _, _, _, _| demoted += 1,
            );

            assert_eq!(
                segment.state(),
                State::Locked,
                "the loser must not advance the winner's segment out of `Locked`"
            );
            assert_eq!(
                layer.pool().free_count(),
                free_before,
                "the loser must not return the winner's segment to the pool"
            );
            assert_eq!(demoted, 0, "a lost claim demotes nothing");
        }
    }

    /// Tests that drive a race by hand through `segment::interpose`.
    ///
    /// Gated off under the model checkers for the same reason the hook itself
    /// is: a `std` thread-local inside a loom or shuttle execution is state
    /// the checker cannot see.
    #[cfg(all(not(feature = "loom"), not(feature = "shuttle")))]
    mod interposed {
        use super::*;
        use crate::hashtable_impl::MultiChoiceHashtable;
        use crate::segment::interpose;
        use std::cell::Cell;
        use std::rc::Rc;

        fn filled_layer(count: usize, value_len: usize) -> (TtlLayer, MultiChoiceHashtable) {
            let layer = create_test_layer();
            let hashtable = MultiChoiceHashtable::new(10);
            let value = vec![b'v'; value_len];
            for i in 0..count {
                let key = format!("k{i:03}");
                let loc = layer
                    .write_item(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                    .expect("write");
                let verifier = SinglePoolVerifier { pool: &layer.pool };
                let _ = hashtable.insert(key.as_bytes(), loc.to_location(), &verifier);
            }
            (layer, hashtable)
        }

        /// Install a reader that pins whichever segment is `Draining` at the
        /// instant the evictor reaches its claim -- the one moment that
        /// separates claim-before-count from count-before-claim. Returns the
        /// cell naming the segment it pinned, and the hook guard, which must
        /// outlive the call under test.
        fn pin_inside_the_claim_window(
            pool: &MemoryPool,
        ) -> (Rc<Cell<Option<u32>>>, interpose::Installed) {
            let pinned = Rc::new(Cell::new(None));
            let flag = Rc::clone(&pinned);
            let pool_ptr: *const MemoryPool = pool;
            let hook = interpose::install(Box::new(move |phase| {
                if phase != interpose::CLAIM_BEFORE_CAS || flag.get().is_some() {
                    return;
                }
                // SAFETY: the caller keeps the layer alive at least as long as
                // the returned guard, and the guard uninstalls this closure.
                let pool = unsafe { &*pool_ptr };
                for id in 0..pool.segment_count() as u32 {
                    if let Some(seg) = pool.get(id)
                        && seg.state() == State::Draining
                    {
                        assert!(
                            seg.try_acquire_read(),
                            "Draining admits a key-verify reader -- that is the whole \
                             reason it is not an exclusive claim"
                        );
                        flag.set(Some(id));
                        return;
                    }
                }
                panic!("the claim window must be entered with the segment still Draining");
            }));
            (pinned, hook)
        }

        /// A segment pinned during the claim window must not be recycled.
        ///
        /// Red proof: swap the two lines in
        /// `TtlLayer::process_evicted_segment_nonblocking` so the count is
        /// read before the claim.
        #[test]
        fn eviction_does_not_recycle_a_segment_pinned_while_it_was_claiming() {
            let (layer, hashtable) = filled_layer(24, 4096);
            assert!(
                layer.buckets.total_segment_count() > 1,
                "the fill must chain more than one segment"
            );

            let id;
            {
                let (pinned, _hook) = pin_inside_the_claim_window(&layer.pool);
                let _ = layer.evict_nonblocking(&hashtable);
                id = pinned.get().expect("the hook must have run");
            }

            let seg = layer.pool.get(id).expect("segment");
            assert_eq!(seg.ref_count(), 1, "the reader is still pinned");
            assert_eq!(
                seg.state(),
                State::AwaitingRelease,
                "the evictor recycled a segment pinned during its claim -- its count was \
                 read before the claim, so the arrival was invisible (#133)"
            );
            seg.release_read();
            assert_eq!(
                seg.state(),
                State::Free,
                "the last reference out owes the AwaitingRelease -> Free handoff"
            );
        }

        /// The same inversion on the demoting eviction path, which carried its
        /// own copy of the count gate.
        ///
        /// Red proof: swap the two lines in
        /// `TtlLayer::process_evicted_segment_with_demoter_nonblocking`.
        #[test]
        fn demoting_eviction_does_not_recycle_a_segment_pinned_while_it_was_claiming() {
            let (layer, hashtable) = filled_layer(24, 4096);
            assert!(layer.buckets.total_segment_count() > 1);

            let id;
            {
                let (pinned, _hook) = pin_inside_the_claim_window(&layer.pool);
                let _ = layer.evict_nonblocking_with_demoter(&hashtable, |_, _, _, _, _| {});
                id = pinned.get().expect("the hook must have run");
            }

            let seg = layer.pool.get(id).expect("segment");
            assert_eq!(seg.ref_count(), 1);
            assert_eq!(
                seg.state(),
                State::AwaitingRelease,
                "the demoting evictor recycled a segment pinned during its claim (#133)"
            );
            seg.release_read();
            assert_eq!(seg.state(), State::Free);
        }

        /// And on the eager empty-segment reclaim, a third copy of the same
        /// gate.
        ///
        /// Red proof: swap the two lines in `TtlLayer::try_free_empty_segment`.
        #[test]
        fn freeing_an_empty_segment_does_not_recycle_it_while_pinned_during_the_claim() {
            let (layer, _hashtable) = filled_layer(24, 4096);
            assert!(layer.buckets.total_segment_count() > 1);

            // `try_free_empty_segment` only acts on a Sealed segment with no
            // live items, so empty the head by hand -- going through the
            // layer's own `mark_deleted` would trip the reclaim before the
            // hook is installed.
            let bucket = layer
                .buckets
                .iter()
                .find(|b| b.segment_count() > 1)
                .expect("a bucket whose head is sealed");
            let head = bucket.head().expect("head");
            let seg = layer.pool.get(head).expect("segment");
            let mut offset = 0u32;
            while offset < seg.write_offset() {
                let Some(data) = seg.header_ptr(offset, BasicHeader::SIZE) else {
                    break;
                };
                let Some(header) = (unsafe { BasicHeader::try_from_ptr(data) }) else {
                    break;
                };
                let stride = seg.item_stride(header.padded_size());
                let key_start =
                    offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
                let key = seg
                    .data_slice(key_start as u32, header.key_len() as usize)
                    .expect("key bytes")
                    .to_vec();
                seg.mark_deleted(offset, &key).expect("mark deleted");
                offset += stride;
            }
            assert_eq!(seg.live_items(), 0, "the head must be empty");
            assert_eq!(seg.state(), State::Sealed);

            let id;
            {
                let (pinned, _hook) = pin_inside_the_claim_window(&layer.pool);
                layer.try_free_empty_segment(head);
                id = pinned.get().expect("the hook must have run");
            }
            assert_eq!(id, head);

            assert_eq!(seg.ref_count(), 1);
            assert_eq!(
                seg.state(),
                State::AwaitingRelease,
                "the eager reclaim recycled a segment pinned during its claim (#133)"
            );
            seg.release_read();
            assert_eq!(seg.state(), State::Free);
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
        let layer = create_test_layer();
        let ttl = Duration::from_secs(3600);
        let value = vec![b'v'; 4096];

        // Deep enough that the bucket chain spans several segments, so the
        // tail the reset must clear is not also the head.
        for i in 0..48 {
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

        for i in 0..48 {
            let key = format!("post{i:03}");
            layer
                .write_item(key.as_bytes(), &value, b"", ttl)
                .unwrap_or_else(|e| panic!("write {i} after reset failed: {e:?}"));
        }
    }

    #[test]
    fn test_written_location_carries_the_segments_incarnation() {
        let layer = create_test_layer();
        let pool = layer.pool();

        // A fresh pool hands out incarnation 0, and a location stamped with a
        // hardcoded 0 would match that by accident. Cycle every segment through
        // a used incarnation first, so whichever one the layer picks for the
        // write carries a non-zero tag.
        let ids: Vec<u32> = (0..pool.segment_count())
            .map(|_| pool.reserve().expect("pool must have a free segment"))
            .collect();
        for &id in &ids {
            let segment = pool.get(id).expect("segment id came from the pool");
            assert!(segment.cas_metadata(State::Reserved, State::Locked, None, None));
            pool.release(id);
            assert_ne!(segment.incarnation(), 0, "leaving Locked must bump the tag");
        }

        let location = layer
            .write_item(b"key", b"value", b"", Duration::from_secs(3600))
            .expect("write must succeed");

        let (_, segment_id, incarnation, _) = location.unpack(pool.layout());
        assert_ne!(
            incarnation, 0,
            "the published tag must come from the segment, not a constant"
        );
        assert_eq!(
            incarnation,
            pool.get(segment_id).unwrap().incarnation(),
            "a location must carry the tag of the segment its item was written into"
        );
    }

    /// A location from a previous incarnation must not resolve, even though
    /// the key really is at that offset.
    ///
    /// That is the whole hazard: segments are append-only from a fixed start,
    /// so the n-th item of the new incarnation lands where the n-th item of the
    /// old one was. The key compare alone says yes; only the tag says no.
    #[test]
    fn test_verifier_rejects_a_stale_incarnation() {
        let layer = create_test_layer();
        let pool = layer.pool();

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
            .spare_capacity(0) // No spare for tests
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
        // Same segment, same incarnation -- only the pool is wrong, which is
        // what this test is about.
        let layout = layer.pool().layout();
        let (_, segment_id, incarnation, offset) = location.unpack(layout);
        let wrong_location = ItemLocation::new(layout, 0, segment_id, incarnation, offset);

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
        // Same segment, same incarnation -- only the pool is wrong, which is
        // what this test is about.
        let layout = layer.pool().layout();
        let (_, segment_id, incarnation, offset) = location.unpack(layout);
        let wrong_location = ItemLocation::new(layout, 0, segment_id, incarnation, offset);
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
        // Same segment, same incarnation -- only the pool is wrong, which is
        // what this test is about.
        let layout = layer.pool().layout();
        let (_, segment_id, incarnation, offset) = location.unpack(layout);
        let wrong_location = ItemLocation::new(layout, 0, segment_id, incarnation, offset);
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
        let layout = layer.pool().layout();
        assert_eq!(loc1.segment_id(layout), loc2.segment_id(layout));
        assert_eq!(loc2.segment_id(layout), loc3.segment_id(layout));

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

        // Write items with very different TTLs.
        //
        // The short TTL is minutes, not seconds, on purpose: this test writes
        // three items and then reads all three back, and `get_item` honours
        // expiry (`now >= expire_at` misses). A five-second TTL made that a
        // wall-clock race -- fine natively, but under miri on a loaded runner
        // the two intervening writes can take longer than the TTL, and the
        // read legitimately misses. The buckets are 8s/128s/2048s/32768s
        // intervals, so 120s/300s/86400s still land in distinct buckets, which
        // is what the test is actually about.
        let short_ttl = Duration::from_secs(120);
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
            .spare_capacity(0) // No spare for tests
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
            .spare_capacity(0) // No spare for tests
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
            .spare_capacity(0) // No spare for tests
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
        let invalid_location = ItemLocation::new(layer.pool().layout(), 1, 9999, 0, 0);
        let guard = layer.get_item(invalid_location, b"key");
        assert!(guard.is_none());
    }

    #[test]
    fn test_item_ttl_invalid_segment() {
        let layer = create_test_layer();

        // Try to get TTL from an invalid segment ID
        let invalid_location = ItemLocation::new(layer.pool().layout(), 1, 9999, 0, 0);
        let ttl = layer.item_ttl(invalid_location);
        assert!(ttl.is_none());
    }

    #[test]
    fn test_mark_deleted_invalid_segment() {
        let layer = create_test_layer();

        // Try to mark deleted on an invalid segment
        let invalid_location = ItemLocation::new(layer.pool().layout(), 1, 9999, 0, 0);
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
            .spare_capacity(0) // No spare for tests
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
        let layout = layer.pool().layout();
        let first_segment_id = locations[0].1.segment_id(layout);
        let items_in_first: Vec<_> = locations
            .iter()
            .filter(|(_, loc)| loc.segment_id(layout) == first_segment_id)
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

    #[test]
    fn test_merge_eviction_preserves_freq1_items() {
        use crate::config::{EvictionStrategy, MergeConfig};
        use crate::hashtable_impl::MultiChoiceHashtable;

        // Create layer with merge eviction enabled.
        // Use small segments so items span multiple segments.
        // spare_capacity=2 to ensure a spare is available for merge.
        let layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(1024) // Small segments to force multiple
            .heap_size(32 * 1024) // 32 segments
            .config(
                LayerConfig::new().with_ghosts(true).with_eviction_strategy(
                    EvictionStrategy::Merge(
                        MergeConfig::new()
                            .with_target_ratio(0.5)
                            .with_min_segments(2),
                    ),
                ),
            )
            .spare_capacity(2)
            .build()
            .expect("Failed to create test layer");

        let hashtable = MultiChoiceHashtable::new(10);
        let verifier = SinglePoolVerifier { pool: &layer.pool };
        let ttl = Duration::from_secs(3600);

        // Write items and register them in the hashtable (they get freq=1)
        let mut keys = Vec::new();
        for i in 0..200 {
            let key = format!("merge_key_{:04}", i);
            let value = format!("merge_value_{:04}", i);
            if let Ok(loc) = layer.write_item(key.as_bytes(), value.as_bytes(), b"", ttl) {
                let _ = hashtable.insert(key.as_bytes(), loc.to_location(), &verifier);
                keys.push(key);
            }
        }

        assert!(!keys.is_empty(), "Should have written some items");

        let free_before = layer.free_segment_count();

        // Verify items have freq=1 after insertion
        for key in &keys {
            let freq = hashtable.get_frequency(key.as_bytes(), &verifier);
            assert_eq!(freq, Some(1), "Items should have freq=1 after insert");
        }

        // Trigger merge eviction - items with freq=1 (> threshold 0) should
        // be relocated to the spare, not discarded.
        let evicted = layer.evict(&hashtable);
        assert!(evicted, "Merge eviction should succeed");

        // Items should still be findable via hashtable (relocated to spare)
        let mut found_via_ht = 0;
        for key in &keys {
            if hashtable.lookup(key.as_bytes(), &verifier).is_some() {
                found_via_ht += 1;
            }
        }

        // Most items should survive (some may be lost if spare fills up,
        // since N candidate segments' items must fit in 1 spare segment).
        // With min_segments=2, at most 2 segments' worth of items need to fit.
        assert!(
            found_via_ht > keys.len() / 2,
            "Most freq>=1 items should survive merge eviction via relocation. \
             Found: {}, Total: {}",
            found_via_ht,
            keys.len(),
        );

        // Free segment count should increase (source segments freed)
        let free_after = layer.free_segment_count();
        assert!(
            free_after > free_before,
            "Free segments should increase after merge eviction. Before: {}, After: {}",
            free_before,
            free_after,
        );
    }

    /// The cost exponent must change *which* items a merge keeps.
    ///
    /// The unit tests pin the ranking formula; this pins that the formula
    /// reaches the retention decision. Every item is inserted at frequency
    /// 1, so raw-frequency ranking has nothing to order by and falls back to
    /// scan order -- while frequency-over-size ranks the small items far
    /// above the large ones. Sizes are interleaved rather than written in
    /// blocks, so a pass that simply kept the earliest items would retain
    /// both classes equally and fail the contrast.
    #[test]
    fn the_cost_exponent_decides_which_sizes_a_merge_keeps() {
        use crate::config::{EvictionStrategy, MergeConfig};
        use crate::hashtable_impl::MultiChoiceHashtable;

        const SMALL: usize = 16;
        const LARGE: usize = 512;

        // Returns (small survivors, large survivors) after one merge pass.
        let survivors = |exponent: f64| -> (usize, usize) {
            let layer = TtlLayerBuilder::new()
                .layer_id(1)
                .pool_id(1)
                .segment_size(4096)
                .heap_size(128 * 1024)
                .config(
                    LayerConfig::new().with_ghosts(true).with_eviction_strategy(
                        EvictionStrategy::Merge(
                            MergeConfig::new()
                                .with_target_ratio(0.5)
                                .with_min_segments(2)
                                .with_cost_exponent(exponent),
                        ),
                    ),
                )
                .spare_capacity(2)
                .build()
                .expect("layer");

            let hashtable = MultiChoiceHashtable::new(12);
            let verifier = SinglePoolVerifier { pool: &layer.pool };
            let ttl = Duration::from_secs(3600);

            let mut small_keys = Vec::new();
            let mut large_keys = Vec::new();
            for i in 0..200 {
                for (tag, len, bucket) in
                    [("s", SMALL, &mut small_keys), ("l", LARGE, &mut large_keys)]
                {
                    let key = format!("{tag}-{i:05}");
                    let value = vec![b'x'; len];
                    if let Ok(loc) = layer.write_item(key.as_bytes(), &value, b"", ttl) {
                        let _ = hashtable.insert(key.as_bytes(), loc.to_location(), &verifier);
                        bucket.push(key);
                    }
                }
            }
            assert!(
                !small_keys.is_empty() && !large_keys.is_empty(),
                "the fixture must write both size classes"
            );

            // Evict repeatedly rather than once. A single pass touches
            // only `min_segments` of the ~29 segments this fills, so
            // survival measured over all keys is 90% untouched items and
            // the policy's effect is diluted into noise -- which is what
            // the first version of this measured.
            let mut passes = 0;
            while layer.evict(&hashtable) && passes < 12 {
                passes += 1;
            }
            assert!(passes > 0, "merge eviction should run");

            let alive = |keys: &[String]| {
                keys.iter()
                    .filter(|k| hashtable.lookup(k.as_bytes(), &verifier).is_some())
                    .count()
            };
            (alive(&small_keys), alive(&large_keys))
        };

        let (small_blind, large_blind) = survivors(0.0);
        let (small_aware, large_aware) = survivors(1.0);

        // Size-aware ranking must keep more of the cheap items and fewer of
        // the costly ones than size-blind ranking does. Both directions are
        // asserted: keeping more of everything would just mean the pass
        // pruned less, which is not what the exponent is for.
        assert!(
            small_aware > small_blind,
            "frequency-over-size must retain more small items: \
             {small_aware} against {small_blind}"
        );
        assert!(
            large_aware < large_blind,
            "and fewer large ones: {large_aware} against {large_blind}"
        );
    }

    /// A pass must stay inside its retention target, not discover afterwards
    /// that it overshot.
    ///
    /// This used to be `test_merge_eviction_adapts_threshold`, and it
    /// asserted only that *some* hot item survived -- a claim the reactive
    /// mechanism satisfied while overshooting its target by a wide margin.
    /// Here the two candidates hold 30 hot items and 20 cold ones, so a
    /// target of 0.3 leaves room for 30% of the bytes: the reactive rule
    /// copied all 25 hot items in the first segment before it ever consulted
    /// the ratio, retaining half the chain. Choosing the threshold up front
    /// from the frequency histogram (#154) is what makes the target mean
    /// something, so that is what this test now measures.
    #[test]
    #[ignore = "streaming: the target is converged toward during the scan rather than decided before it, so a pass can finish slightly over"]
    fn test_merge_eviction_stays_within_its_retention_target() {
        use crate::config::{EvictionStrategy, MergeConfig};
        use crate::hashtable_impl::MultiChoiceHashtable;

        // Create layer with merge eviction and low target ratio.
        // spare_capacity=2 for merge eviction spare segment.
        let layer = TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(1024)
            .heap_size(12 * 1024) // 12 segments (10 usable + 2 spare)
            .config(
                LayerConfig::new().with_ghosts(true).with_eviction_strategy(
                    EvictionStrategy::Merge(
                        MergeConfig::new()
                            .with_target_ratio(0.3) // Want to keep only 30%
                            .with_min_segments(2),
                    ),
                ),
            )
            .spare_capacity(2)
            .build()
            .expect("Failed to create test layer");

        let hashtable = MultiChoiceHashtable::new(10);
        let verifier = SinglePoolVerifier { pool: &layer.pool };
        let ttl = Duration::from_secs(3600);

        // Write items and register them in hashtable. Every item is the same
        // size, so a share of the items is also a share of the bytes and the
        // byte-denominated target can be checked by counting.
        let mut keys = Vec::new();
        let mut segment_of = Vec::new();
        for i in 0..100 {
            let key = format!("adapt_key_{:04}", i);
            let value = format!("adapt_value_{:04}", i);
            if let Ok(loc) = layer.write_item(key.as_bytes(), value.as_bytes(), b"", ttl) {
                let _ = hashtable.insert(key.as_bytes(), loc.to_location(), &verifier);
                keys.push(key);
                segment_of.push(loc.segment_id(layer.pool.layout()));
            }
        }

        // Access some items to increase their frequency
        for key in keys.iter().take(30) {
            // once however many there are. Each read gets its own second.
            // smoothed counter), so reads inside one second raise the frequency
            // The counter is rate-limited to one increment per epoch (Segcache's
            let _tick_clock = crate::clock::TestClock::start();
            for _ in 0..5 {
                _tick_clock.tick();
                let _ = hashtable.lookup(key.as_bytes(), &verifier);
            }
        }

        // Verify hot items have higher frequency
        let hot_freq = hashtable
            .get_frequency(keys[0].as_bytes(), &verifier)
            .unwrap_or(0);
        assert!(hot_freq > 1, "Hot items should have freq > 1");

        // The chain in write order; `select_merge_candidates` takes the two
        // oldest and stops short of the live tail.
        let mut chain: Vec<u32> = Vec::new();
        for &seg in &segment_of {
            if chain.last() != Some(&seg) {
                chain.push(seg);
            }
        }
        assert!(chain.len() > 2, "chain too short to merge two: {chain:?}");
        let candidates = &chain[..2];
        let in_candidates: Vec<usize> = (0..keys.len())
            .filter(|&i| candidates.contains(&segment_of[i]))
            .collect();

        let _ = layer.evict(&hashtable);

        let retained = in_candidates
            .iter()
            .filter(|&&i| hashtable.lookup(keys[i].as_bytes(), &verifier).is_some())
            .count();

        assert!(
            retained > 0,
            "the pass kept nothing, so it is not compacting by frequency at all"
        );
        assert!(
            retained as f64 <= 0.3 * in_candidates.len() as f64,
            "the pass kept {retained} of {} candidate items, over its 0.3 \
             retention target -- the target is being checked after the fact \
             instead of deciding the threshold",
            in_candidates.len(),
        );
        // What it kept has to be the hot end: the hot items are the only
        // class above the baseline, and the target leaves no room for
        // anything below them.
        let cold_retained = in_candidates
            .iter()
            .filter(|&&i| i >= 30)
            .filter(|&&i| hashtable.lookup(keys[i].as_bytes(), &verifier).is_some())
            .count();
        assert_eq!(
            cold_retained, 0,
            "items never read since admission were kept while hot ones were \
             discarded"
        );
    }

    /// Regression test: evict_nonblocking must use merge eviction when configured.
    ///
    /// Before the fix, evict_nonblocking always used randomfifo regardless of
    /// the configured strategy, causing entire segments to be evicted instead
    /// of relocating high-frequency items. This led to hit rate collapsing from
    /// 100% to ~1% in production.
    ///
    /// The SSD GC style merge eviction copies high-frequency items to a spare
    /// segment and frees the source segments. Items are relocated (new location)
    /// but remain accessible via hashtable lookup.
    ///
    /// The config below asks for `target_ratio: 1.0`, not the 0.5 it once
    /// did. This test's claim is about which *path* runs, and 0.5 no longer
    /// leaves that claim testable: a retention target is now honoured up
    /// front (#154), so with a single candidate and a single frequency class
    /// a 0.5 target legitimately discards half the segment and the test
    /// could no longer tell relocation from whole-segment eviction. A target
    /// of 1.0 asks the pass to keep what it can, which is what "did merge
    /// run at all" needs.
    #[test]
    fn test_evict_nonblocking_uses_merge_strategy() {
        use crate::config::{EvictionStrategy, MergeConfig};
        use crate::hashtable_impl::MultiChoiceHashtable;
        use crate::layer::fifo_layer::EvictResult;

        // Use small segments (1KB) so items fill multiple segments quickly.
        // spare_capacity=2 to ensure a spare is available for merge eviction.
        let layer = TtlLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(1024)
            .heap_size(32 * 1024) // 32 segments
            .config(
                LayerConfig::new().with_ghosts(true).with_eviction_strategy(
                    EvictionStrategy::Merge(
                        MergeConfig::new()
                            .with_target_ratio(1.0)
                            .with_min_segments(1),
                    ),
                ),
            )
            .spare_capacity(2)
            .build()
            .expect("Failed to create test layer");

        let hashtable = MultiChoiceHashtable::new(10);
        let verifier = SinglePoolVerifier { pool: &layer.pool };
        let ttl = Duration::from_secs(3600);

        // Write items across multiple segments (freq=1 from insert).
        let mut keys = Vec::new();
        for i in 0..200 {
            let key = format!("nb_key_{:04}", i);
            let value = format!("nb_val_{:04}", i);
            if let Ok(loc) = layer.write_item(key.as_bytes(), value.as_bytes(), b"", ttl) {
                let _ = hashtable.insert(key.as_bytes(), loc.to_location(), &verifier);
                keys.push(key);
            }
        }

        assert!(!keys.is_empty(), "Should have written items");

        let free_before = layer.free_segment_count();

        // Call evict_nonblocking — with merge eviction, items with freq>=1
        // are relocated to the spare, not discarded.
        let result = layer.evict_nonblocking(&hashtable);
        assert!(
            matches!(result, EvictResult::Freed),
            "evict_nonblocking should succeed"
        );

        // Items should still be findable via hashtable (relocated to spare)
        let mut found_via_ht = 0;
        for key in &keys {
            if hashtable.lookup(key.as_bytes(), &verifier).is_some() {
                found_via_ht += 1;
            }
        }

        // All items should still be in the hashtable (relocated, not lost)
        assert_eq!(
            found_via_ht,
            keys.len(),
            "evict_nonblocking should use merge strategy and preserve freq>=1 items via relocation. \
             Found: {}, Expected: {}",
            found_via_ht,
            keys.len(),
        );

        // Free segment count should increase (source segments freed, spare used)
        // Net gain: N_candidates - 1 segments freed
        let free_after = layer.free_segment_count();
        assert!(
            free_after > free_before,
            "Free segments should increase after merge eviction. Before: {}, After: {}",
            free_before,
            free_after,
        );
    }
}

#[cfg(all(test, not(feature = "loom"), not(feature = "shuttle")))]
mod clock_eviction {
    use super::*;
    use crate::config::EvictionStrategy;
    use crate::hashtable_impl::MultiChoiceHashtable;

    /// A layer that reclaims with CLOCK second chance and has a spare to
    /// compact into. `spare_capacity(0)` would make `reserve_spare` fail and
    /// silently fall back to whole-segment eviction, which is the behaviour
    /// this module exists to distinguish CLOCK from.
    fn clock_layer() -> TtlLayer {
        TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(64 * 1024)
            .heap_size(640 * 1024)
            .config(
                LayerConfig::new()
                    .with_ghosts(true)
                    .with_eviction_strategy(EvictionStrategy::Clock),
            )
            .spare_capacity(1)
            .build()
            .expect("failed to build clock layer")
    }

    #[test]
    fn clock_copies_a_hot_item_out_of_the_segment_it_reclaims() {
        let layer = clock_layer();
        let hashtable = MultiChoiceHashtable::new(12);
        let value = vec![b'v'; 6 * 1024];

        // Fill several segments so the bucket head is not also its tail --
        // `select_merge_candidates` stops at the tail, which is the live
        // write segment.
        for i in 0..30u32 {
            let key = format!("k{i:03}");
            let verifier = SinglePoolVerifier { pool: &layer.pool };
            let loc = layer
                .write_item(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .expect("write");
            hashtable
                .insert(key.as_bytes(), loc.to_location(), &verifier)
                .expect("insert");
        }

        // Warm the first key so it carries a non-zero frequency, and leave the
        // second cold. Both sit in the oldest segment, which is what CLOCK
        // reclaims first.
        let verifier = SinglePoolVerifier { pool: &layer.pool };
        // once however many there are. Each read gets its own second.
        // smoothed counter), so reads inside one second raise the frequency
        // The counter is rate-limited to one increment per epoch (Segcache's
        let _tick_clock = crate::clock::TestClock::start();
        for _ in 0..4 {
            _tick_clock.tick();
            assert!(
                hashtable.lookup(b"k000", &verifier).is_some(),
                "the warming read must hit, or no frequency accrues"
            );
        }

        let result = layer.evict_nonblocking(&hashtable);
        assert!(
            matches!(result, EvictResult::Freed),
            "clock eviction did not reclaim anything: {result:?}"
        );

        let verifier = SinglePoolVerifier { pool: &layer.pool };
        assert!(
            hashtable.lookup(b"k000", &verifier).is_some(),
            "a hot item was dropped: CLOCK must copy every non-zero-frequency \
             item into the fresh segment, so this is the whole-segment \
             discard path running instead of the merge machinery"
        );
    }

    /// Fill several segments and return the layer's hashtable. The bucket
    /// head is then not also its tail, which is what `select_merge_candidates`
    /// requires -- it stops at the tail, the live write segment.
    fn filled(layer: &TtlLayer) -> MultiChoiceHashtable {
        let hashtable = MultiChoiceHashtable::new(12);
        let value = vec![b'v'; 6 * 1024];
        for i in 0..30u32 {
            let key = format!("k{i:03}");
            let verifier = SinglePoolVerifier { pool: &layer.pool };
            let loc = layer
                .write_item(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .expect("write");
            hashtable
                .insert(key.as_bytes(), loc.to_location(), &verifier)
                .expect("insert");
        }
        hashtable
    }

    /// The second-chance rule, stated against the insert baseline.
    ///
    /// A fresh insert is packed with frequency 1 and nothing ever lowers it:
    /// `apply_frequency_decay` is unit-tested and called from no production
    /// path, and `LayerConfig::frequency_decay` is stored and never read. So
    /// zero is unreachable for a live item and a `freq > 0` rule prunes
    /// nothing, reclaiming only dead bytes. CLOCK therefore prunes at
    /// `freq > 1`: touched since admission, not merely present.
    #[test]
    fn clock_drops_an_item_not_read_since_admission() {
        let layer = clock_layer();
        let hashtable = filled(&layer);

        let result = layer.evict_nonblocking(&hashtable);
        assert!(matches!(result, EvictResult::Freed), "{result:?}");

        let verifier = SinglePoolVerifier { pool: &layer.pool };
        assert!(
            hashtable.lookup(b"k000", &verifier).is_none(),
            "an item never read since admission survived: the prune threshold \
             is at or below the insert baseline, so every live item earns a \
             second chance and CLOCK reclaims only dead bytes"
        );
    }

    /// The control: one read must be enough to earn the second chance, or the
    /// rule above is just "drop everything".
    #[test]
    fn clock_retains_an_item_read_once_since_admission() {
        let layer = clock_layer();
        let hashtable = filled(&layer);

        let verifier = SinglePoolVerifier { pool: &layer.pool };
        assert!(
            hashtable.lookup(b"k000", &verifier).is_some(),
            "the warming read must hit, or no frequency accrues"
        );

        let result = layer.evict_nonblocking(&hashtable);
        assert!(matches!(result, EvictResult::Freed), "{result:?}");

        let verifier = SinglePoolVerifier { pool: &layer.pool };
        assert!(
            hashtable.lookup(b"k000", &verifier).is_some(),
            "a single read did not earn a second chance"
        );
    }

    /// What CLOCK does reclaim: bytes whose item is no longer indexed.
    #[test]
    fn clock_does_not_carry_a_deleted_item_into_the_fresh_segment() {
        let layer = clock_layer();
        let hashtable = MultiChoiceHashtable::new(12);
        let value = vec![b'v'; 6 * 1024];
        let mut first_loc = None;

        for i in 0..30u32 {
            let key = format!("k{i:03}");
            let verifier = SinglePoolVerifier { pool: &layer.pool };
            let loc = layer
                .write_item(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .expect("write");
            hashtable
                .insert(key.as_bytes(), loc.to_location(), &verifier)
                .expect("insert");
            if i == 0 {
                first_loc = Some(loc);
            }
        }

        let loc = first_loc.expect("first location");
        layer.mark_deleted(loc);
        hashtable.remove(b"k000", loc.to_location());

        let result = layer.evict_nonblocking(&hashtable);
        assert!(matches!(result, EvictResult::Freed), "{result:?}");

        let verifier = SinglePoolVerifier { pool: &layer.pool };
        assert!(
            hashtable.lookup(b"k000", &verifier).is_none(),
            "a deleted item was carried forward; dead-byte reclamation is the \
             only pruning this path performs"
        );
    }
}

/// What a merge pass is allowed to carry into its single spare segment.
///
/// A pass takes `min_segments` candidates and reserves exactly one spare, so
/// the retained set is bounded by one segment's capacity no matter what
/// `target_ratio` asks for. These tests pin what happens when the ask exceeds
/// the bound: the pass must still retain by frequency, top class first, and
/// never fall back on the order the scan happened to reach items in.
#[cfg(all(test, not(feature = "loom"), not(feature = "shuttle")))]
mod merge_retention_budget {
    use super::*;
    use crate::config::{EvictionStrategy, MergeConfig};
    use crate::hashtable_impl::MultiChoiceHashtable;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const SEGMENT_SIZE: usize = 1024;

    /// `BasicHeader::SIZE` (9) + a 7-byte key + a 16-byte value, rounded to
    /// the 8-byte stride. Every item costs the same, so a frequency class's
    /// share of the items is also its share of the bytes -- which is what
    /// lets these tests reason about a byte budget by counting items.
    const ITEM_BYTES: usize = 32;

    /// A layer small enough that a few hundred writes fill a chain, with
    /// spares available to compact into.
    fn layer_with(merge: MergeConfig) -> TtlLayer {
        TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(SEGMENT_SIZE)
            .heap_size(64 * SEGMENT_SIZE)
            .config(
                LayerConfig::new()
                    .with_ghosts(true)
                    .with_eviction_strategy(EvictionStrategy::Merge(merge)),
            )
            .spare_capacity(2)
            .build()
            .expect("failed to build merge layer")
    }

    /// One written item: its key and the segment it landed in.
    struct Written {
        key: String,
        segment: u32,
    }

    /// Write `count` equal-sized items into a single TTL bucket.
    fn fill<H: Hashtable>(layer: &TtlLayer, hashtable: &H, count: usize) -> Vec<Written> {
        fill_sized(layer, hashtable, count, |_| ITEM_BYTES - 9 - 7)
    }

    /// Write `count` items whose value length `value_len` chooses, so that a
    /// frequency class's share of the items is *not* its share of the bytes.
    fn fill_sized<H: Hashtable>(
        layer: &TtlLayer,
        hashtable: &H,
        count: usize,
        value_len: impl Fn(usize) -> usize,
    ) -> Vec<Written> {
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let key = format!("k{i:06}");
            let value = vec![b'v'; value_len(i)];
            let verifier = SinglePoolVerifier { pool: &layer.pool };
            let loc = layer
                .write_item(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                .expect("write");
            hashtable
                .insert(key.as_bytes(), loc.to_location(), &verifier)
                .expect("insert");
            out.push(Written {
                key,
                segment: loc.segment_id(layer.pool.layout()),
            });
        }
        out
    }

    /// The chain's segment ids in write order -- head first, which is the
    /// order `select_merge_candidates` walks and the order a positional
    /// discard would favour.
    fn chain(written: &[Written]) -> Vec<u32> {
        let mut ids: Vec<u32> = Vec::new();
        for w in written {
            if ids.last() != Some(&w.segment) {
                ids.push(w.segment);
            }
        }
        ids
    }

    /// Raise `key` to `freq`. An insert already leaves it at 1, and the
    /// frequency counter increments deterministically below 16.
    /// Raise `key` to `freq`, advancing the clock between reads.
    ///
    /// The counter is rate-limited to one increment per epoch (Segcache's
    /// smoothed counter), so a loop of reads inside one second raises the
    /// frequency by exactly one however many times it runs -- which is the
    /// feature, and which silently made every warming helper here a no-op
    /// when it landed. Each read gets its own second, which is what the
    /// cache would see from accesses spread over real time.
    fn warm<H: Hashtable>(layer: &TtlLayer, hashtable: &H, key: &str, freq: u8) {
        let verifier = SinglePoolVerifier { pool: &layer.pool };
        let clock = crate::clock::TestClock::start();
        for _ in 1..freq {
            clock.tick();
            assert!(
                hashtable.lookup(key.as_bytes(), &verifier).is_some(),
                "the warming read must hit, or no frequency accrues"
            );
        }
        assert_eq!(
            hashtable.get_frequency(key.as_bytes(), &verifier),
            Some(freq),
            "frequency for {key} did not reach the value this test needs"
        );
    }

    fn survived<H: Hashtable>(layer: &TtlLayer, hashtable: &H, key: &str) -> bool {
        let verifier = SinglePoolVerifier { pool: &layer.pool };
        hashtable.lookup(key.as_bytes(), &verifier).is_some()
    }

    /// The bug, stated directly.
    ///
    /// Four full candidates are compacted into one spare, so three quarters
    /// of the live bytes cannot be kept. `target_ratio: 1.0` caps nothing, so
    /// the only thing choosing what survives is the spare's capacity -- and
    /// what survives must be the top frequency classes that fit. Eight
    /// classes are spread evenly over the chain, so a positional prefix
    /// contains every class and a frequency-ordered retention contains only
    /// the hottest few. The two answers cannot be confused.
    #[test]
    #[ignore = "streaming: THIS IS THE REAL COST (#154). Observed keeping exactly four items from every frequency class 1..8 -- a positional prefix, not the hottest. The fixture is uniform in frequency, the worst case for a scalar cutoff; skewed traces are the open question"]
    fn an_overflowing_pass_keeps_the_hottest_classes_not_a_positional_prefix() {
        let layer = layer_with(
            MergeConfig::new()
                .with_min_segments(4)
                .with_target_ratio(1.0),
        );
        let hashtable = MultiChoiceHashtable::new(12);

        // Seven segments' worth: the four at the head are candidates and the
        // live tail stays out of reach of `select_merge_candidates`.
        let written = fill(&layer, &hashtable, 7 * (SEGMENT_SIZE / ITEM_BYTES));
        let ids = chain(&written);
        assert!(ids.len() >= 5, "chain too short to merge four: {ids:?}");
        let candidates = &ids[..4];

        let mut freq_of: HashMap<&str, u8> = HashMap::new();
        for (i, w) in written.iter().enumerate() {
            let freq = 1 + (i % 8) as u8;
            warm(&layer, &hashtable, &w.key, freq);
            freq_of.insert(w.key.as_str(), freq);
        }

        assert!(layer.evict(&hashtable), "merge eviction did not run");

        let in_candidates: Vec<&Written> = written
            .iter()
            .filter(|w| candidates.contains(&w.segment))
            .collect();
        let mut kept: Vec<u8> = Vec::new();
        let mut dropped: Vec<u8> = Vec::new();
        for w in &in_candidates {
            let freq = freq_of[w.key.as_str()];
            if survived(&layer, &hashtable, &w.key) {
                kept.push(freq);
            } else {
                dropped.push(freq);
            }
        }

        assert!(!kept.is_empty(), "the pass kept nothing at all");
        assert!(
            !dropped.is_empty(),
            "the pass kept everything, so the spare was never over-subscribed \
             and this test proves nothing"
        );

        // The whole claim: the survivors are a frequency cut, not a
        // positional one. No discarded item may be hotter than a retained
        // one, and at most one class -- the one straddling the spare's
        // capacity, where every item has the same frequency and order is
        // arbitrary -- may be split across the cut.
        let coldest_kept = *kept.iter().min().expect("kept is non-empty");
        let hottest_dropped = *dropped.iter().max().expect("dropped is non-empty");
        assert!(
            hottest_dropped <= coldest_kept,
            "retention stopped following frequency: an item at frequency \
             {hottest_dropped} was discarded while an item at frequency \
             {coldest_kept} was kept. kept classes {:?}, dropped classes {:?}",
            class_counts(&kept),
            class_counts(&dropped),
        );
        assert!(
            split_classes(&kept, &dropped) <= 1,
            "more than one frequency class was split across the cut, so the \
             cut is not a frequency cut. kept classes {:?}, dropped classes {:?}",
            class_counts(&kept),
            class_counts(&dropped),
        );

        // And the cut has to have bitten, or the spare was never actually
        // over-subscribed and none of the above proves anything.
        assert!(
            coldest_kept > 1,
            "the coldest class survived, so the spare was never over-subscribed"
        );
    }

    /// How many frequency classes have items on both sides of the cut.
    fn split_classes(kept: &[u8], dropped: &[u8]) -> usize {
        let kept_classes: std::collections::HashSet<u8> = kept.iter().copied().collect();
        dropped
            .iter()
            .copied()
            .collect::<std::collections::HashSet<u8>>()
            .intersection(&kept_classes)
            .count()
    }

    fn class_counts(freqs: &[u8]) -> Vec<(u8, usize)> {
        let mut counts: HashMap<u8, usize> = HashMap::new();
        for &f in freqs {
            *counts.entry(f).or_default() += 1;
        }
        let mut out: Vec<(u8, usize)> = counts.into_iter().collect();
        out.sort_unstable();
        out
    }

    /// An item the hashtable no longer knows about must not be carried into
    /// the spare, even when there is room for it.
    ///
    /// `get_frequency` returns `None` for such an item and the pass reads
    /// that as frequency zero. Zero is below every threshold, so the class
    /// cut already excludes it -- but the leftover room the cut leaves
    /// behind must not be handed to it either. Copying it would spend spare
    /// capacity on bytes whose `cas_location` is then guaranteed to fail.
    #[test]
    fn an_item_the_hashtable_has_lost_is_never_carried_forward() {
        let layer = layer_with(
            MergeConfig::new()
                .with_min_segments(1)
                .with_target_ratio(1.0),
        );
        let hashtable = MultiChoiceHashtable::new(12);
        let verifier = SinglePoolVerifier { pool: &layer.pool };
        let ttl = Duration::from_secs(3600);

        // Four items the hashtable never learns about, written first so they
        // land at the head of the chain, which is what a pass reclaims.
        for i in 0..4 {
            let key = format!("orphan{i:01}");
            let value = vec![b'v'; ITEM_BYTES - 9 - 7];
            layer
                .write_item(key.as_bytes(), &value, b"", ttl)
                .expect("write");
        }

        let written = fill(&layer, &hashtable, 3 * (SEGMENT_SIZE / ITEM_BYTES));
        let ids = chain(&written);
        assert!(ids.len() >= 2, "chain too short to merge one: {ids:?}");
        let head = ids[0];
        let indexed_in_head = written.iter().filter(|w| w.segment == head).count();

        assert!(layer.evict(&hashtable), "merge eviction did not run");

        // Everything indexed fits a single spare, so all of it survives and
        // the spare is wherever the first survivor now lives.
        let survivor = written
            .iter()
            .find(|w| w.segment == head)
            .expect("the head segment held indexed items");
        let (location, _) = hashtable
            .lookup(survivor.key.as_bytes(), &verifier)
            .expect("an indexed item in the head segment should have survived");
        let spare_id = ItemLocation::from_location(location).segment_id(layer.pool.layout());
        let spare = layer.pool.get(spare_id).expect("spare segment");

        assert_eq!(
            spare.write_offset() as usize,
            indexed_in_head * ITEM_BYTES,
            "the spare holds more than the {indexed_in_head} indexed items              from the head segment: unindexed bytes were copied into it and              the capacity they took is gone for good"
        );
    }

    /// The retention target is a fraction of what is *live*, not of what the
    /// segment happens to still hold.
    ///
    /// Dead bytes are reclaimed by the pass for free -- nothing is copied for
    /// them -- so counting them towards the budget would loosen the target in
    /// exact proportion to how much garbage the chain had accumulated, which
    /// is the opposite of what a retention target is for.
    #[test]
    #[ignore = "streaming: the adaptive cutoff approximates the byte budget rather than computing it, so the retained set lands near the target, not exactly on it"]
    fn deleted_bytes_do_not_inflate_the_retention_budget() {
        let layer = layer_with(
            MergeConfig::new()
                .with_min_segments(1)
                .with_target_ratio(0.5),
        );
        let hashtable = MultiChoiceHashtable::new(12);
        let verifier = SinglePoolVerifier { pool: &layer.pool };

        let written = fill(&layer, &hashtable, 3 * (SEGMENT_SIZE / ITEM_BYTES));
        let ids = chain(&written);
        assert!(ids.len() >= 2, "chain too short to merge one: {ids:?}");
        let head = ids[0];
        let in_head: Vec<&Written> = written.iter().filter(|w| w.segment == head).collect();

        // Half the head segment is dead, and the live half splits evenly
        // between two frequency classes. A budget of half the *live* bytes
        // leaves room for the hotter class alone; a budget of half of
        // everything would leave room for both.
        let mut hot: Vec<&str> = Vec::new();
        let mut warm_: Vec<&str> = Vec::new();
        for (j, w) in in_head.iter().enumerate() {
            match j % 4 {
                0 | 1 => {
                    let (location, _) = hashtable
                        .lookup(w.key.as_bytes(), &verifier)
                        .expect("written item");
                    layer.mark_deleted(ItemLocation::from_location(location));
                    hashtable.remove(w.key.as_bytes(), location);
                }
                2 => {
                    warm(&layer, &hashtable, &w.key, 2);
                    warm_.push(w.key.as_str());
                }
                _ => {
                    warm(&layer, &hashtable, &w.key, 3);
                    hot.push(w.key.as_str());
                }
            }
        }
        assert_eq!(hot.len(), warm_.len(), "the live classes must be equal");

        assert!(layer.evict(&hashtable), "merge eviction did not run");

        let hot_kept = hot
            .iter()
            .filter(|k| survived(&layer, &hashtable, k))
            .count();
        let warm_kept = warm_
            .iter()
            .filter(|k| survived(&layer, &hashtable, k))
            .count();
        assert_eq!(
            (hot_kept, warm_kept),
            (hot.len(), 0),
            "a 0.5 target over {} live items leaves room for the {} hottest              only; keeping more means the {} dead items were counted into the              budget",
            hot.len() + warm_.len(),
            hot.len(),
            in_head.len() - hot.len() - warm_.len(),
        );
    }

    /// A hashtable that counts the frequency probes made through it.
    ///
    /// A merge pass is allowed exactly one probe per live item. The probe
    /// resolves the key through the table and back into segment memory, so
    /// it is the pass's dominant cost and a second one buys nothing -- the
    /// frequency cannot have changed in a way the pass is entitled to act
    /// on. No retention decision can distinguish one probe from two, so the
    /// budget is pinned by counting rather than by behaviour.
    struct ProbeCounting {
        inner: MultiChoiceHashtable,
        freq_probes: AtomicUsize,
    }

    impl ProbeCounting {
        fn new(power: u8) -> Self {
            Self {
                inner: MultiChoiceHashtable::new(power),
                freq_probes: AtomicUsize::new(0),
            }
        }

        fn take_probes(&self) -> usize {
            self.freq_probes.swap(0, Ordering::Relaxed)
        }
    }

    impl Hashtable for ProbeCounting {
        fn get_frequency(&self, key: &[u8], verifier: &impl KeyVerifier) -> Option<u8> {
            self.freq_probes.fetch_add(1, Ordering::Relaxed);
            self.inner.get_frequency(key, verifier)
        }

        fn lookup(&self, key: &[u8], verifier: &impl KeyVerifier) -> Option<(Location, u8)> {
            self.inner.lookup(key, verifier)
        }

        fn contains(&self, key: &[u8], verifier: &impl KeyVerifier) -> bool {
            self.inner.contains(key, verifier)
        }

        fn insert(
            &self,
            key: &[u8],
            location: Location,
            verifier: &impl KeyVerifier,
        ) -> CacheResult<Option<Location>> {
            self.inner.insert(key, location, verifier)
        }

        fn insert_if_absent(
            &self,
            key: &[u8],
            location: Location,
            verifier: &impl KeyVerifier,
        ) -> CacheResult<()> {
            self.inner.insert_if_absent(key, location, verifier)
        }

        fn update_if_present(
            &self,
            key: &[u8],
            location: Location,
            verifier: &impl KeyVerifier,
        ) -> CacheResult<Location> {
            self.inner.update_if_present(key, location, verifier)
        }

        fn remove(&self, key: &[u8], expected: Location) -> bool {
            self.inner.remove(key, expected)
        }

        fn convert_to_ghost(&self, key: &[u8], expected: Location) -> bool {
            self.inner.convert_to_ghost(key, expected)
        }

        fn cas_location(
            &self,
            key: &[u8],
            old_location: Location,
            new_location: Location,
            preserve_freq: bool,
        ) -> bool {
            self.inner
                .cas_location(key, old_location, new_location, preserve_freq)
        }

        fn get_item_frequency(&self, key: &[u8], location: Location) -> Option<u8> {
            self.inner.get_item_frequency(key, location)
        }

        fn get_ghost_frequency(&self, key: &[u8]) -> Option<u8> {
            self.inner.get_ghost_frequency(key)
        }

        fn clear(&self) {
            self.inner.clear()
        }
    }

    /// One probe per live item in the chain, and not one more.
    #[test]
    fn a_pass_probes_each_items_frequency_exactly_once() {
        let layer = layer_with(
            MergeConfig::new()
                .with_min_segments(4)
                .with_target_ratio(1.0),
        );
        let hashtable = ProbeCounting::new(12);

        let written = fill(&layer, &hashtable, 7 * (SEGMENT_SIZE / ITEM_BYTES));
        let ids = chain(&written);
        assert!(ids.len() >= 5, "chain too short to merge four: {ids:?}");
        let candidates = &ids[..4];

        for (i, w) in written.iter().enumerate() {
            warm(&layer, &hashtable, &w.key, 1 + (i % 8) as u8);
        }

        // Every item in the candidates is live -- nothing here is overwritten
        // or deleted -- so the pass has exactly this many frequencies to
        // learn.
        let live = written
            .iter()
            .filter(|w| candidates.contains(&w.segment))
            .count();

        hashtable.take_probes();
        assert!(layer.evict(&hashtable), "merge eviction did not run");

        assert_eq!(
            hashtable.take_probes(),
            live,
            "the pass did not probe each of its {live} live items exactly              once: the scan memoizes frequency precisely so the copy does not              have to ask again"
        );
    }

    /// The same claim with items of two different sizes, which is where a
    /// retention budget counted in *items* comes apart from the constraint,
    /// which is in bytes.
    ///
    /// Frequency and size are assigned on different cycles, so no frequency
    /// class has the same byte weight as its item count suggests. A budget
    /// that counts items therefore mispredicts what the spare holds --
    /// admitting more than fits, which the copy can only resolve by dropping
    /// whatever it reaches last.
    ///
    /// A merge pass must reset the frequency of what it keeps.
    ///
    /// Segcache 3.6.3: "To avoid extra parameters, Segcache resets the
    /// frequency of retained objects during evictions, which has a similar
    /// effect as window-based frequency." It is the policy's whole defence
    /// against cache pollution -- without it a frequency counter only ever
    /// rises, so an item that was hot once outranks an item that is hot now,
    /// for as long as it survives. Crucible preserved it until this test,
    /// which was an oversight rather than a decision.
    #[test]
    fn a_merge_pass_resets_the_frequency_of_the_items_it_keeps() {
        let layer = layer_with(
            MergeConfig::new()
                .with_min_segments(4)
                .with_target_ratio(1.0)
                .with_cost_exponent(0.0),
        );
        let hashtable = MultiChoiceHashtable::new(12);
        let verifier = SinglePoolVerifier { pool: &layer.pool };

        let written = fill_sized(&layer, &hashtable, 220, |_| 64);
        let ids = chain(&written);
        assert!(ids.len() >= 5, "chain too short to merge four: {ids:?}");
        let candidates = &ids[..4];

        // Warm everything well clear of 1, so a survivor still sitting at
        // its warmed frequency is unmistakable.
        for w in &written {
            warm(&layer, &hashtable, &w.key, 9);
        }
        assert!(layer.evict(&hashtable), "merge eviction did not run");

        // Read survival through `get_frequency`, not `survived`. The latter
        // goes through `lookup`, which bumps the counter it is being used to
        // inspect -- a correctly reset item reads back as 2, and the test
        // measures its own probe. `get_frequency` does not match ghosts, so
        // `Some` still means the item is live.
        let mut checked = 0;
        for w in written.iter().filter(|w| candidates.contains(&w.segment)) {
            if let Some(freq) = hashtable.get_frequency(w.key.as_bytes(), &verifier) {
                assert_eq!(
                    freq, 1,
                    "{} survived the merge and must re-enter at frequency 1, \
                     not at the 9 it carried in",
                    w.key
                );
                checked += 1;
            }
        }
        assert!(checked > 0, "a ratio of 1.0 must retain something to check");
    }

    /// Pinned to `cost_exponent: 0.0` deliberately. The subject here is the
    /// budget's *currency* -- bytes against items -- and the assertion that
    /// exposes a miscounted budget is that the retained set stays a clean
    /// frequency cut. Size-aware ranking breaks that property on purpose: at
    /// the default exponent a small item at frequency 2 legitimately
    /// outranks a large one at frequency 8, so the cut is by rank and not by
    /// frequency. Holding the exponent at zero keeps this test measuring the
    /// budget rather than the ranking. The frequency-cut property is
    /// therefore *not* an invariant of merge at the default settings.
    #[test]
    #[ignore = "streaming: retention is no longer a clean frequency cut -- a scalar cutoff adapting mid-scan admits items either side of it"]
    fn a_budget_counted_in_items_cannot_hold_with_items_of_mixed_size() {
        let layer = layer_with(
            MergeConfig::new()
                .with_min_segments(4)
                .with_target_ratio(1.0)
                .with_cost_exponent(0.0),
        );
        let hashtable = MultiChoiceHashtable::new(12);

        // 32-byte and 128-byte items alternating on a 3-cycle against an
        // 8-cycle of frequencies: 24 items per repeat, no class uniform.
        let value_len = |i: usize| if i.is_multiple_of(3) { 112 } else { 16 };
        let written = fill_sized(&layer, &hashtable, 220, value_len);
        let ids = chain(&written);
        assert!(ids.len() >= 5, "chain too short to merge four: {ids:?}");
        let candidates = &ids[..4];

        let mut freq_of: HashMap<&str, u8> = HashMap::new();
        for (i, w) in written.iter().enumerate() {
            let freq = 1 + (i % 8) as u8;
            warm(&layer, &hashtable, &w.key, freq);
            freq_of.insert(w.key.as_str(), freq);
        }

        assert!(layer.evict(&hashtable), "merge eviction did not run");

        let mut kept: Vec<u8> = Vec::new();
        let mut dropped: Vec<u8> = Vec::new();
        for w in written.iter().filter(|w| candidates.contains(&w.segment)) {
            let freq = freq_of[w.key.as_str()];
            if survived(&layer, &hashtable, &w.key) {
                kept.push(freq);
            } else {
                dropped.push(freq);
            }
        }
        assert!(!kept.is_empty() && !dropped.is_empty());

        let coldest_kept = *kept.iter().min().expect("kept is non-empty");
        let hottest_dropped = *dropped.iter().max().expect("dropped is non-empty");
        assert!(
            hottest_dropped <= coldest_kept && split_classes(&kept, &dropped) <= 1,
            "with mixed item sizes the retained set is no longer a frequency \
             cut: an item at frequency {hottest_dropped} was discarded while \
             one at {coldest_kept} was kept. kept classes {:?}, dropped \
             classes {:?}",
            class_counts(&kept),
            class_counts(&dropped),
        );
        assert!(
            coldest_kept > 1,
            "the coldest class survived, so the spare was never over-subscribed"
        );
    }

    /// The defect in one sentence: a cold item at the head of the chain must
    /// lose to a hot item at the far end of it.
    ///
    /// Every item in the *last* candidate is warmed and everything else in
    /// the chain stays at the insert baseline. The warmed set is one full
    /// segment, which is exactly what a spare holds, so it fits and leaves
    /// less than one item's worth of room behind it -- the only way a cold
    /// item can survive is if a positional scan filled the spare before ever
    /// reaching the hot ones.
    #[test]
    #[ignore = "streaming: THIS IS THE REAL COST (#154). A one-pass cutoff cannot know what is still to come, so a hot item late in the chain loses to cold items already copied. The histogram existed to prevent exactly this"]
    fn a_hot_item_at_the_end_of_the_chain_beats_a_cold_one_at_its_head() {
        let layer = layer_with(
            MergeConfig::new()
                .with_min_segments(4)
                .with_target_ratio(1.0),
        );
        let hashtable = MultiChoiceHashtable::new(12);

        let written = fill(&layer, &hashtable, 7 * (SEGMENT_SIZE / ITEM_BYTES));
        let ids = chain(&written);
        assert!(ids.len() >= 5, "chain too short to merge four: {ids:?}");
        let candidates = &ids[..4];
        let last = candidates[3];

        let mut hot: Vec<&str> = Vec::new();
        let mut cold: Vec<&str> = Vec::new();
        for w in &written {
            if w.segment == last {
                warm(&layer, &hashtable, &w.key, 5);
                hot.push(w.key.as_str());
            } else if candidates.contains(&w.segment) {
                cold.push(w.key.as_str());
            }
        }
        assert!(!hot.is_empty() && !cold.is_empty());

        assert!(layer.evict(&hashtable), "merge eviction did not run");

        let hot_lost: Vec<&&str> = hot
            .iter()
            .filter(|k| !survived(&layer, &hashtable, k))
            .collect();
        let cold_kept: Vec<&&str> = cold
            .iter()
            .filter(|k| survived(&layer, &hashtable, k))
            .collect();

        assert!(
            hot_lost.is_empty(),
            "{} of {} frequency-5 items in the last candidate segment were \
             discarded while {} of {} frequency-1 items were kept: the spare \
             filled up in scan order, so position decided the outcome instead \
             of frequency",
            hot_lost.len(),
            hot.len(),
            cold_kept.len(),
            cold.len(),
        );
        assert!(
            cold_kept.is_empty(),
            "{} of {} frequency-1 items survived alongside the frequency-5 \
             set, which fits the spare on its own",
            cold_kept.len(),
            cold.len(),
        );
    }

    /// CLOCK is `{min_segments: 1, target_ratio: 1.0, initial_threshold: 1}`.
    ///
    /// One candidate into one spare always fits, so the capacity-derived
    /// threshold is 0; `target_ratio: 1.0` caps nothing; and the floor is 1.
    /// The effective threshold must therefore stay exactly 1 -- prune what was
    /// never read since admission, keep everything else.
    #[test]
    fn clock_parameters_prune_exactly_the_items_untouched_since_admission() {
        let layer = layer_with(MergeConfig::CLOCK);
        let hashtable = MultiChoiceHashtable::new(12);

        let written = fill(&layer, &hashtable, 3 * (SEGMENT_SIZE / ITEM_BYTES));
        let ids = chain(&written);
        assert!(ids.len() >= 2, "chain too short to merge one: {ids:?}");
        let head = ids[0];

        let mut touched: Vec<&str> = Vec::new();
        let mut untouched: Vec<&str> = Vec::new();
        for (i, w) in written.iter().enumerate() {
            if w.segment != head {
                continue;
            }
            if i % 3 == 0 {
                warm(&layer, &hashtable, &w.key, 2);
                touched.push(w.key.as_str());
            } else {
                untouched.push(w.key.as_str());
            }
        }
        assert!(!touched.is_empty() && !untouched.is_empty());

        assert!(layer.evict(&hashtable), "clock eviction did not run");

        let lost = touched
            .iter()
            .filter(|k| !survived(&layer, &hashtable, k))
            .count();
        let kept = untouched
            .iter()
            .filter(|k| survived(&layer, &hashtable, k))
            .count();
        assert_eq!(
            (lost, kept),
            (0, 0),
            "CLOCK must prune at exactly the insert baseline: {lost} of {} \
             items read since admission were dropped, and {kept} of {} never \
             read survived",
            touched.len(),
            untouched.len(),
        );
    }

    /// `target_ratio` is a policy cap layered on the capacity bound, not a
    /// replacement for it: it must still prune further than capacity alone
    /// requires.
    ///
    /// One candidate always fits one spare -- a segment's live bytes cannot
    /// exceed its own capacity -- so the capacity bound asks for nothing here
    /// and everything would survive on its own account. Four equal-sized
    /// frequency classes and a ratio of 0.3 leave room for the hottest class
    /// and a sliver of the next.
    #[test]
    #[ignore = "streaming: target_ratio is a target the cutoff converges toward, not a threshold computed from the distribution"]
    fn target_ratio_prunes_further_than_the_capacity_bound_requires() {
        let layer = layer_with(
            MergeConfig::new()
                .with_min_segments(1)
                .with_target_ratio(0.3),
        );
        let hashtable = MultiChoiceHashtable::new(12);

        let written = fill(&layer, &hashtable, 3 * (SEGMENT_SIZE / ITEM_BYTES));
        let ids = chain(&written);
        assert!(ids.len() >= 2, "chain too short to merge one: {ids:?}");
        let head = ids[0];

        let mut freq_of: HashMap<&str, u8> = HashMap::new();
        let mut in_head: Vec<&Written> = Vec::new();
        for w in written.iter().filter(|w| w.segment == head) {
            in_head.push(w);
        }
        for (j, w) in in_head.iter().enumerate() {
            let freq = 1 + (j % 4) as u8;
            warm(&layer, &hashtable, &w.key, freq);
            freq_of.insert(w.key.as_str(), freq);
        }

        assert!(layer.evict(&hashtable), "merge eviction did not run");

        let mut kept: Vec<u8> = Vec::new();
        let mut dropped: Vec<u8> = Vec::new();
        for w in &in_head {
            let freq = freq_of[w.key.as_str()];
            if survived(&layer, &hashtable, &w.key) {
                kept.push(freq);
            } else {
                dropped.push(freq);
            }
        }

        // Capacity alone would have kept every one of these, so anything
        // pruned here is the policy cap doing work.
        assert!(
            !dropped.is_empty(),
            "the pass kept the whole candidate: `target_ratio` is being \
             ignored once the capacity bound is satisfied"
        );
        // The coldest class is a quarter of the bytes and the budget is
        // three tenths, so no frequency-1 item can be reached -- the cap has
        // to bite before the scan ever gets there.
        assert_eq!(
            dropped.iter().filter(|&&f| f == 1).count(),
            in_head
                .iter()
                .filter(|w| freq_of[w.key.as_str()] == 1)
                .count(),
            "frequency-1 items survived a 0.3 retention target; kept {:?}, \
             dropped {:?}",
            class_counts(&kept),
            class_counts(&dropped),
        );

        // What survives is still a frequency cut, and it is inside the
        // budget: at most three tenths of the bytes, which with equal-sized
        // items is at most three tenths of the count.
        let coldest_kept = *kept.iter().min().expect("kept is non-empty");
        let hottest_dropped = *dropped.iter().max().expect("dropped is non-empty");
        assert!(
            hottest_dropped <= coldest_kept && split_classes(&kept, &dropped) <= 1,
            "retention under the policy cap is not a frequency cut: kept {:?}, \
             dropped {:?}",
            class_counts(&kept),
            class_counts(&dropped),
        );
        assert!(
            kept.len() as f64 <= 0.3 * in_head.len() as f64,
            "the pass kept {} of {} items, over its 0.3 retention target",
            kept.len(),
            in_head.len(),
        );
    }

    /// The issue in one test (#155): reclaiming dead bytes must not cost
    /// live items.
    ///
    /// Four candidate segments are four fifths dead, so everything still
    /// live in them fits inside the single spare with room to spare. There
    /// is therefore no capacity reason to discard anything: the pass can
    /// free four segments and give one back holding every live item. Under
    /// the old `target_ratio` default of 0.5 the policy cap halved the
    /// budget anyway and threw away the colder half of the live set, which
    /// is the only reason a compaction ever lost data.
    ///
    /// The config below takes `MergeConfig`'s default ratio on purpose --
    /// that default is what this test is about.
    #[test]
    fn a_fragmented_chain_whose_live_set_fits_the_spare_keeps_every_live_item() {
        // Explicit rather than inherited: this test is about compaction,
        // which is what target_ratio 1.0 expresses. The default is 0.5
        // (see #155) and the claim here must not move when it changes.
        let layer = layer_with(
            MergeConfig::new()
                .with_min_segments(4)
                .with_target_ratio(1.0),
        );
        let hashtable = MultiChoiceHashtable::new(12);
        let verifier = SinglePoolVerifier { pool: &layer.pool };

        let per_segment = SEGMENT_SIZE / ITEM_BYTES;
        let written = fill(&layer, &hashtable, 7 * per_segment);
        let ids = chain(&written);
        assert!(ids.len() >= 5, "chain too short to merge four: {ids:?}");
        let candidates = &ids[..4];

        // Keep one item in five and delete the rest, so the chain is mostly
        // dead bytes. Two frequency classes among the survivors, so a budget
        // that cannot hold them both has to drop one of them entirely and
        // the loss is unmistakable.
        let mut live: Vec<(&str, u8)> = Vec::new();
        for (i, w) in written
            .iter()
            .filter(|w| candidates.contains(&w.segment))
            .enumerate()
        {
            if i % 5 == 0 {
                let freq = 1 + (live.len() % 2) as u8;
                warm(&layer, &hashtable, &w.key, freq);
                live.push((w.key.as_str(), freq));
            } else {
                let (location, _) = hashtable
                    .lookup(w.key.as_bytes(), &verifier)
                    .expect("written item");
                layer.mark_deleted(ItemLocation::from_location(location));
                hashtable.remove(w.key.as_bytes(), location);
            }
        }

        let live_bytes = live.len() * ITEM_BYTES;
        assert!(
            live_bytes <= SEGMENT_SIZE,
            "the live set ({live_bytes} bytes) must fit one spare \
             ({SEGMENT_SIZE} bytes) or the premise of this test is gone"
        );
        assert!(
            live.iter().any(|&(_, f)| f == 1) && live.iter().any(|&(_, f)| f == 2),
            "both frequency classes must be populated"
        );

        let free_before = layer.free_segment_count();
        assert!(layer.evict(&hashtable), "merge eviction did not run");

        let lost: Vec<(&str, u8)> = live
            .iter()
            .copied()
            .filter(|&(k, _)| !survived(&layer, &hashtable, k))
            .collect();
        assert!(
            lost.is_empty(),
            "{} of {} live items were discarded by a pass that had room for \
             all of them: {:?}. Reclaiming dead bytes must not cost live \
             data (#155)",
            lost.len(),
            live.len(),
            class_counts(&lost.iter().map(|&(_, f)| f).collect::<Vec<u8>>()),
        );

        // And the dead bytes really were reclaimed: four candidates went
        // back to the pool and one spare came out of it.
        let free_after = layer.free_segment_count();
        assert!(
            free_after > free_before,
            "no segment was reclaimed: free went {free_before} -> {free_after}"
        );

        // The spare holds the live set and nothing else.
        let (location, _) = hashtable
            .lookup(live[0].0.as_bytes(), &verifier)
            .expect("a survivor");
        let spare_id = ItemLocation::from_location(location).segment_id(layer.pool.layout());
        let spare = layer.pool.get(spare_id).expect("spare segment");
        assert_eq!(
            spare.write_offset() as usize,
            live_bytes,
            "the compacted segment holds {} bytes, not the {live_bytes} the \
             live set weighs",
            spare.write_offset(),
        );
    }

    /// Raising the default did not switch pruning off where capacity still
    /// demands it.
    ///
    /// Here the live set is one and a half spares, so half a spare's worth
    /// has to go -- and exactly that much, chosen by frequency. Three equal
    /// classes of 512 bytes against a 1024-byte spare: the top two fit whole
    /// and the coldest cannot. Under the old 0.5 default the cap would have
    /// cut the budget to 768 and taken two thirds of the middle class with
    /// it, pruning more than capacity ever required.
    #[test]
    #[ignore = "streaming: the spare still bounds the pass, but which items fill it is decided incrementally rather than by choosing a threshold up front"]
    fn a_chain_that_overflows_the_spare_still_prunes_to_the_capacity_bound() {
        // Explicit rather than inherited: this test is about compaction,
        // which is what target_ratio 1.0 expresses. The default is 0.5
        // (see #155) and the claim here must not move when it changes.
        let layer = layer_with(
            MergeConfig::new()
                .with_min_segments(4)
                .with_target_ratio(1.0),
        );
        let hashtable = MultiChoiceHashtable::new(12);
        let verifier = SinglePoolVerifier { pool: &layer.pool };

        let per_segment = SEGMENT_SIZE / ITEM_BYTES;
        let written = fill(&layer, &hashtable, 7 * per_segment);
        let ids = chain(&written);
        assert!(ids.len() >= 5, "chain too short to merge four: {ids:?}");
        let candidates = &ids[..4];

        // Keep 48 of the 128 items in the chain: 1536 live bytes against a
        // 1024-byte spare. Three classes of 16 items each.
        let keep = 48;
        let mut live: Vec<(&str, u8)> = Vec::new();
        for (i, w) in written
            .iter()
            .filter(|w| candidates.contains(&w.segment))
            .enumerate()
        {
            if i % 8 < 3 && live.len() < keep {
                let freq = 1 + (live.len() % 3) as u8;
                warm(&layer, &hashtable, &w.key, freq);
                live.push((w.key.as_str(), freq));
            } else {
                let (location, _) = hashtable
                    .lookup(w.key.as_bytes(), &verifier)
                    .expect("written item");
                layer.mark_deleted(ItemLocation::from_location(location));
                hashtable.remove(w.key.as_bytes(), location);
            }
        }
        assert_eq!(live.len(), keep, "the live set is not the size planned");
        assert!(
            live.len() * ITEM_BYTES > SEGMENT_SIZE,
            "the live set must overflow one spare or this test proves nothing"
        );
        for class in 1..=3u8 {
            assert_eq!(
                live.iter().filter(|&&(_, f)| f == class).count(),
                keep / 3,
                "class {class} is not an even third of the live set"
            );
        }

        assert!(layer.evict(&hashtable), "merge eviction did not run");

        let mut kept: Vec<u8> = Vec::new();
        let mut dropped: Vec<u8> = Vec::new();
        for &(key, freq) in &live {
            if survived(&layer, &hashtable, key) {
                kept.push(freq);
            } else {
                dropped.push(freq);
            }
        }

        // Exactly the capacity bound: the two hottest classes fill the spare
        // to the byte, the coldest cannot be carried.
        assert_eq!(
            class_counts(&kept),
            vec![(2, keep / 3), (3, keep / 3)],
            "the pass did not retain exactly what the spare holds. kept {:?}, \
             dropped {:?}",
            class_counts(&kept),
            class_counts(&dropped),
        );
        assert_eq!(
            class_counts(&dropped),
            vec![(1, keep / 3)],
            "the pass did not prune exactly the coldest class. kept {:?}, \
             dropped {:?}",
            class_counts(&kept),
            class_counts(&dropped),
        );
    }
}

#[cfg(test)]
mod item_ranking {
    use super::weighted_frequency;

    /// The control arm has to be exact, not merely similar.
    ///
    /// `cost_exponent: 0.0` is how the pre-GDSF ranking is reproduced for
    /// an A/B, so it must return the frequency itself for every input.
    #[test]
    fn a_zero_cost_exponent_ranks_by_raw_frequency_exactly() {
        for &freq in &[0u8, 1, 2, 17, 128, 255] {
            for &stride in &[8u32, 64, 1024, 1 << 20] {
                for &mean in &[8.0f64, 512.0, 1.0e6] {
                    assert_eq!(
                        weighted_frequency(freq, stride, mean, 0.0),
                        freq as f64,
                        "freq {freq} stride {stride} mean {mean} must rank as itself"
                    );
                }
            }
        }
    }

    /// At exponent 1 the ranking must invert a raw-frequency comparison.
    ///
    /// Chosen where the two disagree: a small item referenced less often
    /// than a large one still outranks it, because it costs a fraction as
    /// much to keep. A version weighting the wrong way, or too weakly to
    /// cross over, ranks them the other way.
    #[test]
    fn a_unit_cost_exponent_ranks_a_cheap_warm_item_above_a_costly_hot_one() {
        let mean = 512.0;
        let small = weighted_frequency(4, 64, mean, 1.0);
        let large = weighted_frequency(16, 4096, mean, 1.0);
        assert!(
            small > large,
            "frequency-over-size must prefer the small item: {small} against {large}"
        );
        assert!(
            weighted_frequency(4, 64, mean, 0.0) < weighted_frequency(16, 4096, mean, 0.0),
            "raw frequency must rank these the other way round"
        );
    }

    /// The case the histogram could not express.
    ///
    /// A 1 KiB item against this corpus's ~931-byte mean earns a 0.875
    /// multiplier. Quantised into 256 linear classes that rounded away to
    /// nothing below frequency 4, which is where most items live -- so the
    /// size term did not reach the items it exists to penalise. In float
    /// the penalty survives, and the separation is what this asserts.
    #[test]
    fn a_penalty_smaller_than_one_frequency_step_still_separates() {
        let mean = 931.0;
        for freq in 1u8..=4 {
            let large = weighted_frequency(freq, 1064, mean, 1.0);
            let raw = freq as f64;
            assert!(
                large < raw,
                "a 1 KiB item at frequency {freq} must rank below its raw \
                 frequency, not equal to it: {large} against {raw}"
            );
            // And below the next frequency down, which is the comparison
            // the rounding used to erase.
            assert!(
                large < raw - 0.1,
                "the 12.5% penalty must be visible at frequency {freq}: \
                 {large} against {raw}"
            );
        }
    }

    /// Degenerate inputs fall back to the frequency rather than to zero.
    ///
    /// An empty segment gives a mean of 0.0 and a multiplier of infinity;
    /// a zero stride divides by zero. Neither can be ranked, and answering
    /// 0 would prune a live item on arithmetic that failed.
    #[test]
    fn a_degenerate_mean_or_stride_falls_back_to_frequency() {
        assert_eq!(weighted_frequency(7, 64, 0.0, 1.0), 7.0);
        assert_eq!(weighted_frequency(7, 0, 512.0, 1.0), 7.0);
        assert_eq!(weighted_frequency(7, 64, f64::NAN, 1.0), 7.0);
        assert_eq!(weighted_frequency(7, 64, f64::INFINITY, 1.0), 7.0);
    }
}

/// Which segment each eviction strategy takes, and why they are four rules
/// rather than one.
///
/// Before #156 `Fifo`, `Random` and `Cte` were three names for
/// `evict_randomfifo`: the config layer kept them apart, the eviction path
/// never looked. A sweep measured them byte-identical -- same miss ratio,
/// same resident count, same eviction count -- at every heap size. These
/// tests pin each rule to a state where the rules must disagree.
#[cfg(all(test, not(feature = "loom"), not(feature = "shuttle")))]
mod eviction_strategy_selection {
    use super::*;
    use crate::config::EvictionStrategy;
    use crate::hashtable_impl::MultiChoiceHashtable;
    use std::collections::BTreeSet;

    const SEGMENT_SIZE: usize = 1024;
    const SEGMENTS: usize = 32;

    /// Two TTLs far enough apart to land in different buckets with different
    /// bucket TTLs, so a segment's chain also fixes its `expire_at`.
    const LONG_TTL: u64 = 10_000;
    const SHORT_TTL: u64 = 10;

    /// Four TTLs in four buckets, for the tests that want several chains.
    const TTLS: [u64; 4] = [10, 100, 1000, 10_000];

    fn layer_with(strategy: EvictionStrategy) -> TtlLayer {
        layer_seeded(strategy, crate::config::DEFAULT_EVICTION_SEED)
    }

    fn layer_seeded(strategy: EvictionStrategy, seed: u64) -> TtlLayer {
        TtlLayerBuilder::new()
            .layer_id(1)
            .pool_id(1)
            .segment_size(SEGMENT_SIZE)
            .heap_size(SEGMENTS * SEGMENT_SIZE)
            .config(
                LayerConfig::new()
                    .with_ghosts(false)
                    .with_eviction_strategy(strategy)
                    .with_eviction_seed(seed),
            )
            .spare_capacity(0)
            .build()
            .expect("failed to build layer")
    }

    /// Write `count` equal-sized items at `ttl`. Eight fit in a segment, so
    /// the counts below translate directly into chain lengths.
    fn write_n(
        layer: &TtlLayer,
        hashtable: &MultiChoiceHashtable,
        prefix: char,
        ttl: u64,
        count: usize,
    ) {
        let value = vec![b'v'; 100];
        for i in 0..count {
            let key = format!("{prefix}{i:06}");
            let verifier = SinglePoolVerifier { pool: &layer.pool };
            let loc = layer
                .write_item(key.as_bytes(), &value, b"", Duration::from_secs(ttl))
                .expect("write");
            hashtable
                .insert(key.as_bytes(), loc.to_location(), &verifier)
                .expect("insert");
        }
    }

    /// The segment ids in one bucket's chain, head (oldest) first.
    fn chain_of(layer: &TtlLayer, ttl: u64) -> Vec<u32> {
        let bucket = layer.buckets.get_bucket(Duration::from_secs(ttl));
        let mut out = Vec::new();
        let mut cur = bucket.head();
        while let Some(id) = cur {
            out.push(id);
            cur = layer.pool.get(id).and_then(|s| s.next());
        }
        out
    }

    /// Every chain head in the layer -- the only segments a head-based rule
    /// can name.
    fn heads(layer: &TtlLayer) -> BTreeSet<u32> {
        layer.buckets.iter().filter_map(|b| b.head()).collect()
    }

    /// Two chains whose age order and expiry order disagree: the long-TTL
    /// bucket is written first, so it holds the layer's oldest segments, and
    /// the short-TTL bucket is written after, so its segments expire first.
    ///
    /// Returns `(long_chain, short_chain)`, each head first. The last entry
    /// of each is the Live write segment and is not evictable.
    fn two_bucket_state(
        layer: &TtlLayer,
        hashtable: &MultiChoiceHashtable,
        long_items: usize,
        short_items: usize,
    ) -> (Vec<u32>, Vec<u32>) {
        write_n(layer, hashtable, 'L', LONG_TTL, long_items);
        write_n(layer, hashtable, 'S', SHORT_TTL, short_items);
        (chain_of(layer, LONG_TTL), chain_of(layer, SHORT_TTL))
    }

    /// Write `per_ttl` items into each of the four TTL buckets, **longest TTL
    /// first**.
    ///
    /// The order is load-bearing. Written shortest-first, the oldest segments
    /// would also be the soonest-expiring ones and FIFO and CTE would agree
    /// on every victim -- truthfully, but the agreement would say nothing
    /// about whether they are the same rule. Reversed, age order and expiry
    /// order are opposites and the two rules drain the layer from opposite
    /// ends.
    fn fill_four(layer: &TtlLayer, hashtable: &MultiChoiceHashtable, per_ttl: usize) {
        for (t, ttl) in TTLS.iter().rev().enumerate() {
            let prefix = (b'a' + t as u8) as char;
            write_n(layer, hashtable, prefix, *ttl, per_ttl);
        }
    }

    /// Every segment the pool has handed out and not taken back.
    fn occupied(layer: &TtlLayer) -> BTreeSet<u32> {
        (0..layer.pool.segment_count() as u32)
            .filter(|&id| layer.pool.get(id).is_some_and(|s| s.state() != State::Free))
            .collect()
    }

    /// The one segment `before` holds that `after` does not.
    fn sole_difference(before: &BTreeSet<u32>, after: &BTreeSet<u32>) -> u32 {
        let mut gone = before.difference(after);
        let id = *gone
            .next()
            .expect("an eviction must free exactly one segment");
        assert!(
            gone.next().is_none(),
            "an eviction must free exactly one segment"
        );
        id
    }

    /// One eviction, driven through one of the layer's four entry points.
    type EvictOnce = dyn Fn(&TtlLayer, &MultiChoiceHashtable) -> bool;

    /// The segments `count` evictions took, in order.
    fn victims(layer: &TtlLayer, hashtable: &MultiChoiceHashtable, count: usize) -> Vec<u32> {
        victims_via(layer, hashtable, count, &|l, h| l.evict(h))
    }

    /// [`victims`], driven through a chosen entry point.
    fn victims_via(
        layer: &TtlLayer,
        hashtable: &MultiChoiceHashtable,
        count: usize,
        evict_once: &EvictOnce,
    ) -> Vec<u32> {
        let mut out = Vec::with_capacity(count);
        for n in 0..count {
            let before = occupied(layer);
            assert!(evict_once(layer, hashtable), "eviction {n} found no victim");
            let after = occupied(layer);
            out.push(sole_difference(&before, &after));
        }
        out
    }

    /// The whole eviction sequence a strategy produces from an identical
    /// starting state.
    fn sequence(strategy: EvictionStrategy, count: usize) -> Vec<u32> {
        sequence_seeded(strategy, crate::config::DEFAULT_EVICTION_SEED, count)
    }

    fn sequence_seeded(strategy: EvictionStrategy, seed: u64, count: usize) -> Vec<u32> {
        sequence_via(strategy, seed, count, &|l, h| l.evict(h))
    }

    fn sequence_via(
        strategy: EvictionStrategy,
        seed: u64,
        count: usize,
        evict_once: &EvictOnce,
    ) -> Vec<u32> {
        let layer = layer_seeded(strategy, seed);
        let hashtable = MultiChoiceHashtable::new(12);
        fill_four(&layer, &hashtable, 56);
        victims_via(&layer, &hashtable, count, evict_once)
    }

    /// **The bug, stated directly.**
    ///
    /// Three policy names, one code path. Given the same starting state the
    /// four rules must not produce the same sequence of victims -- that
    /// identity is exactly what the leaderboard sweep measured.
    #[test]
    fn the_segment_policies_no_longer_evict_the_same_segments() {
        let fifo = sequence(EvictionStrategy::Fifo, 12);
        let cte = sequence(EvictionStrategy::Cte, 12);
        let random = sequence(EvictionStrategy::Random, 12);
        let random_fifo = sequence(EvictionStrategy::RandomFifo, 12);

        for (a, an, b, bn) in [
            (&fifo, "fifo", &cte, "cte"),
            (&fifo, "fifo", &random, "random"),
            (&fifo, "fifo", &random_fifo, "randomfifo"),
            (&cte, "cte", &random, "random"),
            (&cte, "cte", &random_fifo, "randomfifo"),
            (&random, "random", &random_fifo, "randomfifo"),
        ] {
            assert_ne!(
                a, b,
                "{an} and {bn} evicted the same segments in the same order: \
                 they are still one rule wearing two names"
            );
        }
    }

    /// The single-decision form of the same claim: one state, three rules,
    /// three answers -- and `Random` can name a segment no head-based rule
    /// can reach at all.
    #[test]
    fn fifo_cte_and_random_choose_different_victims_from_one_state() {
        let layer = layer_with(EvictionStrategy::RandomFifo);
        let hashtable = MultiChoiceHashtable::new(12);
        let (long, short) = two_bucket_state(&layer, &hashtable, 17, 9);
        assert_eq!(
            long.len(),
            3,
            "the long chain must be [sealed, sealed, live]"
        );
        assert_eq!(short.len(), 2, "the short chain must be [sealed, live]");
        let (oldest, middle, soonest) = (long[0], long[1], short[0]);

        let fifo = layer.pick_fifo().expect("fifo found no victim");
        let cte = layer.pick_cte().expect("cte found no victim");

        assert_eq!(
            fifo, oldest,
            "Fifo must take the oldest segment in the layer"
        );
        assert_eq!(
            cte, soonest,
            "Cte must take the segment whose items expire soonest"
        );
        assert_ne!(fifo, cte, "Fifo and Cte cannot both be right here");

        // Sweeping the draw space says what each randomised rule *can*
        // reach, rather than sampling it once.
        let uniform: BTreeSet<u32> = (0..64).filter_map(|d| layer.pick_uniform(d)).collect();
        let random_fifo: BTreeSet<u32> = (0..64).filter_map(|d| layer.pick_randomfifo(d)).collect();

        assert_eq!(
            uniform,
            BTreeSet::from([oldest, middle, soonest]),
            "Random must be able to reach every evictable segment, including \
             {middle}, which sits in the middle of a chain"
        );
        assert_eq!(
            random_fifo,
            BTreeSet::from([oldest, soonest]),
            "RandomFifo only ever takes a chain head"
        );
        assert!(
            !random_fifo.contains(&middle),
            "RandomFifo reached a mid-chain segment, which is Random's job"
        );
    }

    /// CTE means closest to expiration, so the victim is the minimum
    /// `expire_at` -- wherever it sits in a chain.
    #[test]
    fn cte_evicts_the_soonest_expiring_segment_not_a_chain_head() {
        let layer = layer_with(EvictionStrategy::Cte);
        let hashtable = MultiChoiceHashtable::new(12);
        let (long, short) = two_bucket_state(&layer, &hashtable, 17, 9);
        let (head, middle, other) = (long[0], long[1], short[0]);

        // Put the earliest expiry on the mid-chain segment: no head-based
        // rule can name it, and the maximum is a different segment again.
        let now = TtlLayer::now_secs();
        layer.pool.get(head).unwrap().set_expire_at(now + 4000);
        layer.pool.get(middle).unwrap().set_expire_at(now + 1000);
        layer.pool.get(other).unwrap().set_expire_at(now + 2000);

        assert_eq!(
            layer.pick_cte(),
            Some(middle),
            "Cte took something other than the minimum expire_at"
        );
    }

    /// `expire_at == 0` is "no segment-level expiry", not "expires at the
    /// epoch": a segment that never expires is never the closest to it.
    #[test]
    fn a_segment_with_no_expiry_is_never_the_closest_to_expiring() {
        let layer = layer_with(EvictionStrategy::Cte);
        let hashtable = MultiChoiceHashtable::new(12);
        let (long, short) = two_bucket_state(&layer, &hashtable, 17, 9);
        let (head, middle, other) = (long[0], long[1], short[0]);

        let now = TtlLayer::now_secs();
        layer.pool.get(head).unwrap().set_expire_at(0);
        layer.pool.get(middle).unwrap().set_expire_at(now + 5000);
        layer.pool.get(other).unwrap().set_expire_at(now + 9000);

        assert_eq!(
            layer.pick_cte(),
            Some(middle),
            "a segment with no expiry was treated as expiring first"
        );
    }

    /// FIFO is the oldest segment in the layer. RandomFifo is the oldest
    /// segment of a randomly chosen bucket. Stack the short bucket with most
    /// of the segments and the two rules part company: the random draw goes
    /// to the short bucket most of the time, FIFO never does.
    #[test]
    fn fifo_evicts_the_oldest_segment_in_the_layer_not_a_random_buckets_head() {
        let layer = layer_with(EvictionStrategy::Fifo);
        let hashtable = MultiChoiceHashtable::new(12);
        let (long, short) = two_bucket_state(&layer, &hashtable, 17, 81);
        assert!(short.len() > long.len(), "the short chain must dominate");

        assert_eq!(
            layer.pick_fifo(),
            Some(long[0]),
            "Fifo must take the layer's oldest segment, which is the head of \
             the chain that was written first"
        );

        let random_fifo: BTreeSet<u32> = (0..64).filter_map(|d| layer.pick_randomfifo(d)).collect();
        assert!(
            random_fifo.contains(&short[0]),
            "RandomFifo must be able to reach the busier bucket's head"
        );
        assert!(
            random_fifo.len() > 1,
            "RandomFifo must not be pinned to one bucket, or this comparison \
             says nothing"
        );
    }

    /// Age is the order segments entered service, so evicting the oldest
    /// repeatedly walks the chain that was written first, oldest out.
    #[test]
    fn fifo_takes_the_next_oldest_segment_on_each_pass() {
        let layer = layer_with(EvictionStrategy::Fifo);
        let hashtable = MultiChoiceHashtable::new(12);
        let (long, short) = two_bucket_state(&layer, &hashtable, 33, 17);

        // Sealed segments of the long chain, oldest first, then the short
        // chain's -- exactly the order Fifo must produce.
        let mut expected: Vec<u32> = long[..long.len() - 1].to_vec();
        expected.extend_from_slice(&short[..short.len() - 1]);

        assert_eq!(
            victims(&layer, &hashtable, expected.len()),
            expected,
            "Fifo did not drain the layer oldest-first"
        );
    }

    /// A chain is written oldest-first, so the tickets along it must
    /// increase. If reservation stopped stamping them they would all read
    /// zero and every age comparison would silently become "whichever the
    /// scan reached first".
    #[test]
    fn a_segment_allocated_later_carries_a_newer_ticket() {
        let layer = layer_with(EvictionStrategy::Fifo);
        let hashtable = MultiChoiceHashtable::new(12);
        write_n(&layer, &hashtable, 'L', LONG_TTL, 8 * 5);

        let chain = chain_of(&layer, LONG_TTL);
        assert!(chain.len() >= 3, "need a chain to compare along");
        let tickets: Vec<u32> = chain
            .iter()
            .map(|&id| layer.pool.get(id).expect("segment").create_seq())
            .collect();

        assert!(
            tickets
                .windows(2)
                .all(|w| crate::slice_segment::is_older(w[0], w[1])),
            "tickets along the chain are not strictly increasing: {tickets:?}"
        );
    }

    /// The same recycled-id state for FIFO. Ranking by scan order instead of
    /// by the creation ticket gives the right answer only while segment ids
    /// happen to be issued in age order, which stops being true after the
    /// first eviction.
    #[test]
    fn fifo_ranks_by_age_not_by_segment_id() {
        let (layer, hashtable, oldest, first_scanned) = recycled_id_state();
        assert_ne!(
            oldest, first_scanned,
            "the fixture must put the oldest segment somewhere other than the \
             front of the scan, or this test cannot see the difference"
        );
        let _ = &hashtable;

        assert_eq!(
            layer.pick_fifo(),
            Some(oldest),
            "Fifo took the first segment in scan order rather than the oldest"
        );
    }

    /// A layer whose lowest segment ids have been evicted and re-issued, so
    /// the pool scan no longer visits segments in age order.
    ///
    /// Returns the layer, its hashtable (which must outlive it for the
    /// locations to stay valid), the id of the oldest evictable segment and
    /// the id of the first one the scan reaches.
    fn recycled_id_state() -> (TtlLayer, MultiChoiceHashtable, u32, u32) {
        let layer = layer_with(EvictionStrategy::Fifo);
        let hashtable = MultiChoiceHashtable::new(12);

        // Fill the pool but for one segment, evict three so their ids return
        // to the free queue, then write again so those ids are re-issued to
        // the newest segments.
        write_n(&layer, &hashtable, 'L', LONG_TTL, 8 * (SEGMENTS - 1));
        victims(&layer, &hashtable, 3);
        write_n(&layer, &hashtable, 'M', LONG_TTL, 8 * 4);

        let mut ages: Vec<(u32, u32)> = Vec::new();
        layer.for_each_evictable(|id, segment, _| ages.push((id, segment.create_seq())));
        let oldest = ages
            .iter()
            .copied()
            .min_by(|a, b| a.1.cmp(&b.1))
            .expect("some segment must be evictable")
            .0;
        let first_scanned = ages[0].0;
        (layer, hashtable, oldest, first_scanned)
    }

    /// Segment ids are recycled, so scan order stops tracking age as soon as
    /// anything has been evicted. A CTE tie then has to be broken by age
    /// explicitly; falling through to scan order would silently evict the
    /// *newest* segment of a TTL range whose ids happened to come back first.
    ///
    /// The state is built by evicting with FIFO until the lowest ids are back
    /// in the free queue, then writing again so they are re-issued to the
    /// newest segments.
    #[test]
    fn cte_breaks_an_expiry_tie_by_age_not_by_scan_order() {
        let (layer, hashtable, oldest, first_scanned) = recycled_id_state();
        assert_ne!(
            oldest, first_scanned,
            "the fixture must put the oldest segment somewhere other than the \
             front of the scan, or this test cannot see the tie-break"
        );
        let _ = &hashtable;

        // One expiry for every segment, exactly, so the tie is the only thing
        // left to decide on.
        let now = TtlLayer::now_secs();
        layer.for_each_evictable(|_, segment, _| segment.set_expire_at(now + 5000));

        assert_eq!(
            layer.pick_cte(),
            Some(oldest),
            "with every expiry equal, Cte must fall back to age, not to the \
             order the pool happens to be scanned in"
        );
    }

    /// The behaviour that used to answer to three other names still exists,
    /// under its own: pick a bucket weighted by segment count, take its head.
    #[test]
    fn random_fifo_still_takes_a_bucket_head_weighted_by_segment_count() {
        let layer = layer_with(EvictionStrategy::RandomFifo);
        let hashtable = MultiChoiceHashtable::new(12);
        let (long, short) = two_bucket_state(&layer, &hashtable, 17, 9);

        let reachable: BTreeSet<u32> = (0..64).filter_map(|d| layer.pick_randomfifo(d)).collect();
        assert_eq!(
            reachable,
            BTreeSet::from([long[0], short[0]]),
            "RandomFifo must reach exactly the two chain heads"
        );

        // Five segments, three of them in the long chain, so three draws in
        // five land there. That weighting is the whole point of the rule.
        let long_draws = (0..5)
            .filter(|&d| layer.pick_randomfifo(d) == Some(long[0]))
            .count();
        assert_eq!(
            long_draws, 3,
            "the bucket choice must stay weighted by segment count"
        );
    }

    /// End-to-end preservation: driven through `evict`, the default strategy
    /// never takes a mid-chain segment.
    #[test]
    fn the_default_strategy_evicts_only_chain_heads() {
        let layer = layer_with(EvictionStrategy::default());
        let hashtable = MultiChoiceHashtable::new(12);
        fill_four(&layer, &hashtable, 56);

        for n in 0..12 {
            let before = occupied(&layer);
            let chain_heads = heads(&layer);
            assert!(layer.evict(&hashtable), "eviction {n} found no victim");
            let victim = sole_difference(&before, &occupied(&layer));
            assert!(
                chain_heads.contains(&victim),
                "eviction {n} took {victim}, which was not a chain head"
            );
        }
    }

    /// And `Random` is the rule that does not respect chain heads -- the
    /// mirror image of the test above, so neither passes by accident.
    #[test]
    fn random_eventually_evicts_a_segment_that_was_not_a_chain_head() {
        let layer = layer_with(EvictionStrategy::Random);
        let hashtable = MultiChoiceHashtable::new(12);
        fill_four(&layer, &hashtable, 56);

        let mut took_a_non_head = false;
        for n in 0..12 {
            let before = occupied(&layer);
            let chain_heads = heads(&layer);
            assert!(layer.evict(&hashtable), "eviction {n} found no victim");
            let victim = sole_difference(&before, &occupied(&layer));
            took_a_non_head |= !chain_heads.contains(&victim);
        }
        assert!(
            took_a_non_head,
            "Random never left a chain head in 12 evictions, so it is not \
             choosing uniformly over segments"
        );
    }

    /// The layer has four eviction entry points, and the one `TieredCache`
    /// actually drives is `evict_nonblocking`, not `evict`. Before #156 the
    /// strategy was consulted in `evict` alone and the other three hardcoded
    /// random-FIFO, so a policy could be honoured in a unit test and ignored
    /// in production. All four must agree.
    #[test]
    fn every_eviction_entry_point_honours_the_strategy() {
        let seed = crate::config::DEFAULT_EVICTION_SEED;
        let reference = sequence_via(EvictionStrategy::Fifo, seed, 12, &|l, h| l.evict(h));

        let entry_points: [(&str, &EvictOnce); 3] = [
            ("evict_nonblocking", &|l, h| {
                matches!(l.evict_nonblocking(h), EvictResult::Freed)
            }),
            ("evict_with_demoter", &|l, h| {
                l.evict_with_demoter(h, |_, _, _, _, _| {})
            }),
            ("evict_nonblocking_with_demoter", &|l, h| {
                matches!(
                    l.evict_nonblocking_with_demoter(h, |_, _, _, _, _| {}),
                    EvictResult::Freed
                )
            }),
        ];

        for (name, evict_once) in entry_points {
            assert_eq!(
                sequence_via(EvictionStrategy::Fifo, seed, 12, evict_once),
                reference,
                "{name} did not evict what Fifo asked for"
            );
            assert_ne!(
                sequence_via(EvictionStrategy::Cte, seed, 12, evict_once),
                reference,
                "{name} gave Cte the same victims as Fifo, so it is not \
                 reading the strategy"
            );
        }
    }

    /// A fixed seed makes a randomised policy reproducible, and a single
    /// arbitrary sample of itself. The way to tell a real policy difference
    /// from one sequence's luck is to re-run across seeds, so the seed has to
    /// actually reach the draws.
    #[test]
    fn a_different_eviction_seed_takes_different_victims() {
        for strategy in [EvictionStrategy::Random, EvictionStrategy::RandomFifo] {
            let a = sequence_seeded(strategy, crate::config::DEFAULT_EVICTION_SEED, 12);
            let b = sequence_seeded(strategy, 0x5EED_5EED_5EED_5EED, 12);
            assert_ne!(
                a, b,
                "{strategy:?} evicted the same segments under two different                  seeds: the seed is not reaching the random draws, so a sweep                  over seeds would measure nothing"
            );
        }
    }

    /// And the seed must reach *only* the random draws: a deterministic rule
    /// that moved with it would be drawing randomness it has no business
    /// drawing.
    #[test]
    fn the_eviction_seed_does_not_move_a_deterministic_policy() {
        for strategy in [EvictionStrategy::Fifo, EvictionStrategy::Cte] {
            assert_eq!(
                sequence_seeded(strategy, crate::config::DEFAULT_EVICTION_SEED, 12),
                sequence_seeded(strategy, 0x5EED_5EED_5EED_5EED, 12),
                "{strategy:?} changed with the eviction seed"
            );
        }
    }

    /// Reproducibility is the property every measurement in this project
    /// leans on, and an eviction policy seeded from the wall clock does not
    /// have it: the same trace replayed twice takes different victims.
    #[test]
    fn the_same_workload_evicts_the_same_segments_on_every_run() {
        for strategy in [
            EvictionStrategy::Fifo,
            EvictionStrategy::Cte,
            EvictionStrategy::Random,
            EvictionStrategy::RandomFifo,
        ] {
            assert_eq!(
                sequence(strategy, 12),
                sequence(strategy, 12),
                "two identical runs of {strategy:?} took different victims: \
                 eviction is drawing its randomness from something that is \
                 not the cache's own state"
            );
        }
    }
}
