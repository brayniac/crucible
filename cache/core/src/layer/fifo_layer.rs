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

    /// Get a reference to the FIFO segment chain.
    pub fn chain(&self) -> &FifoChain {
        &self.chain
    }

    /// Reset the layer to its freshly built state: an empty chain and every
    /// segment free.
    ///
    /// Backs [`crate::cache::TieredCache::flush`]. Resetting the pool alone is
    /// not enough: the chain would keep naming segments the pool had just
    /// recycled, and the next `push` onto that stale tail fails -- reported as
    /// `OutOfMemory` even with every segment free.
    ///
    /// The chain is cleared before the pool so that a reader following the
    /// chain during the window reaches segments that are still live rather than
    /// ones already back on the free queue.
    ///
    /// # Preconditions
    ///
    /// Only valid when no concurrent operation is touching this layer, and only
    /// after the hashtable has been cleared. Same precondition as
    /// [`crate::slice_segment::SliceSegment::force_free`], which this reaches
    /// through `reset_all`.
    pub fn reset(&self) {
        self.chain.reset();
        self.pool.reset_all();
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
        crate::clock::now_unix_secs()
    }

    /// Remove all hashtable entries for items in a segment, without waiting for
    /// readers or releasing the segment. Used by non-blocking eviction to "condemn"
    /// a segment that still has active readers.
    fn drain_segment_from_hashtable<H: Hashtable>(&self, segment_id: u32, hashtable: &H) {
        let segment = match self.pool.get(segment_id) {
            Some(s) => s,
            None => return,
        };

        let now = Self::now_secs();
        let mut offset = 0u32;
        let write_offset = segment.write_offset();

        while offset < write_offset {
            if let Some(data) = segment.header_ptr(offset, TtlHeader::SIZE) {
                if let Some(header) = unsafe { TtlHeader::try_from_ptr(data) } {
                    // The segment's stride, not the 8-byte padded body size: a scan
                    // that advances by anything but what the append advanced by
                    // desyncs after the first item on a coarser-aligned pool.
                    let item_size = segment.item_stride(header.padded_size());

                    if !header.is_deleted() && !header.is_expired(now) {
                        let key_start =
                            offset as usize + TtlHeader::SIZE + header.optional_len() as usize;
                        let key_len = header.key_len() as usize;

                        if let Some(key) = segment.data_slice(key_start as u32, key_len) {
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

    /// Emergency eviction: find any Sealed segment with ref_count == 0 and evict it.
    ///
    /// This is called when the policy-selected segment was deferred due to active readers.
    /// Scans the pool for an alternative segment that can be freed immediately.
    ///
    /// Plain `ref_count()` on purpose. This scan publishes no state transition
    /// of its own, so there is nothing for the load to be ordered against --
    /// it only picks a candidate. The exclusive claim happens downstream, in
    /// `try_remove`'s `Sealed -> Draining` CAS and `process_evicted_segment`'s
    /// `wait_for_readers`, which is where the Dekker pair actually lives. A
    /// stale answer here costs at most one wasted candidate.
    fn emergency_evict<H: Hashtable>(&self, hashtable: &H) -> bool {
        let num_segments = self.pool.segment_count();
        for i in 0..num_segments {
            if let Some(segment) = self.pool.get(i as u32)
                && segment.state() == State::Sealed
                && segment.ref_count() == 0
            {
                // Try to remove this specific segment from the chain
                if self.chain.try_remove(i as u32, &self.pool).is_ok() {
                    self.process_evicted_segment(i as u32, hashtable);
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
        let now = Self::now_secs();
        let mut offset = 0u32;
        let write_offset = segment.write_offset();

        while offset < write_offset {
            // Try to read header at offset
            if let Some(data) = segment.header_ptr(offset, TtlHeader::SIZE) {
                if let Some(header) = unsafe { TtlHeader::try_from_ptr(data) } {
                    // The segment's stride, not the 8-byte padded body size: a scan
                    // that advances by anything but what the append advanced by
                    // desyncs after the first item on a coarser-aligned pool.
                    let item_size = segment.item_stride(header.padded_size());

                    // Skip if already deleted or expired
                    if !header.is_deleted() && !header.is_expired(now) {
                        // Get key for this item
                        let key_start =
                            offset as usize + TtlHeader::SIZE + header.optional_len() as usize;
                        let key_len = header.key_len() as usize;

                        if let Some(key) = segment.data_slice(key_start as u32, key_len) {
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

        // Process each item in the segment
        let now = Self::now_secs();
        let mut offset = 0u32;
        let write_offset = segment.write_offset();

        while offset < write_offset {
            // Try to read header at offset
            if let Some(data) = segment.header_ptr(offset, TtlHeader::SIZE) {
                if let Some(header) = unsafe { TtlHeader::try_from_ptr(data) } {
                    // The segment's stride, not the 8-byte padded body size: a scan
                    // that advances by anything but what the append advanced by
                    // desyncs after the first item on a coarser-aligned pool.
                    let item_size = segment.item_stride(header.padded_size());

                    // Skip if already deleted or expired
                    if !header.is_deleted() && !header.is_expired(now) {
                        // Calculate offsets for key, value, optional
                        let optional_start = offset as usize + TtlHeader::SIZE;
                        let optional_len = header.optional_len() as usize;
                        let key_start = optional_start + optional_len;
                        let key_len = header.key_len() as usize;
                        let value_start = key_start + key_len;
                        let value_len = header.value_len() as usize;

                        if let Some(key) = segment.data_slice(key_start as u32, key_len) {
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

                                    // Calculate remaining TTL
                                    let expire_at = header.expire_at();
                                    let remaining_secs = expire_at.saturating_sub(now);
                                    let ttl = Duration::from_secs(remaining_secs as u64);

                                    // Call demoter callback (which will write to disk and update hashtable)
                                    demoter(key, value, optional, ttl, location.to_location());
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

    /// Non-blocking variant of process_evicted_segment_with_demoter.
    ///
    /// Returns `true` if fully processed, `false` if deferred (AwaitingRelease).
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
        let mut offset = 0u32;
        let write_offset = segment.write_offset();

        while offset < write_offset {
            if let Some(data) = segment.header_ptr(offset, TtlHeader::SIZE) {
                if let Some(header) = unsafe { TtlHeader::try_from_ptr(data) } {
                    // The segment's stride, not the 8-byte padded body size: a scan
                    // that advances by anything but what the append advanced by
                    // desyncs after the first item on a coarser-aligned pool.
                    let item_size = segment.item_stride(header.padded_size());

                    if !header.is_deleted() && !header.is_expired(now) {
                        let optional_start = offset as usize + TtlHeader::SIZE;
                        let optional_len = header.optional_len() as usize;
                        let key_start = optional_start + optional_len;
                        let key_len = header.key_len() as usize;
                        let value_start = key_start + key_len;
                        let value_len = header.value_len() as usize;

                        if let Some(key) = segment.data_slice(key_start as u32, key_len) {
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

                                    let expire_at = header.expire_at();
                                    let remaining_secs = expire_at.saturating_sub(now);
                                    let ttl = Duration::from_secs(remaining_secs as u64);

                                    demoter(key, value, optional, ttl, location.to_location());
                                }
                                ItemFate::Discard => {
                                    hashtable.remove(key, location.to_location());
                                }
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
                let expire_at = Self::now_secs().saturating_add(ttl.as_secs() as u32);

                // Try to append with per-item TTL
                if let Some(offset) = segment.append_item_with_ttl(key, value, optional, expire_at)
                {
                    return Ok(ItemLocation::new(
                        self.pool.layout(),
                        self.pool.pool_id(),
                        segment_id,
                        segment.incarnation(),
                        offset,
                    ));
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

        let (_, segment_id, _, offset) = location.unpack(self.pool.layout());
        let segment = self.pool.get(segment_id)?;

        // Check segment state. Condemned is excluded: the hashtable entries are
        // already gone, so this location is stale and the answer is a miss (#127).
        let state = segment.state();
        if !state.is_readable() || state.is_condemned() {
            return None;
        }

        // Verify key matches and check expiration in one parse (before acquiring ref count)
        let now = Self::now_secs();
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
            // We need the key to mark deleted, but we don't have it here.
            // This is a limitation - mark_deleted needs to be called with key.
            // For now, we skip the key verification by getting it from the segment.
            if let Some(data) = segment.header_ptr(offset, TtlHeader::SIZE)
                && let Some(header) = unsafe { TtlHeader::try_from_ptr(data) }
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

        let (_, segment_id, _, offset) = location.unpack(self.pool.layout());
        let segment = self.pool.get(segment_id)?;
        let now = Self::now_secs();
        segment.item_ttl(offset, now)
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

    fn evict_with_demoter<H, F>(&self, hashtable: &H, demoter: F) -> bool
    where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, Location),
    {
        // Try to pop head segment from chain
        match self.chain.pop(&self.pool) {
            Ok(segment_id) => {
                // Process items with demoter callback
                self.process_evicted_segment_with_demoter(segment_id, hashtable, demoter);
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

    fn begin_write_item(
        &self,
        key: &[u8],
        value_len: usize,
        optional: &[u8],
        ttl: Duration,
    ) -> CacheResult<(ItemLocation, *mut u8, u32)> {
        // Validate inputs
        if key.len() > 255 {
            return Err(CacheError::KeyTooLong);
        }
        if optional.len() > 64 {
            return Err(CacheError::OptionalTooLong);
        }

        // Try to reserve space in current write segment
        loop {
            let segment_id = self.get_or_allocate_write_segment()?;

            if let Some(segment) = self.pool.get(segment_id) {
                // Calculate expiration
                let expire_at = Self::now_secs().saturating_add(ttl.as_secs() as u32);

                // Try to reserve space for the item
                if let Some((offset, item_size, value_ptr)) =
                    segment.begin_append_with_ttl(key, value_len, optional, expire_at)
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

                // Segment is full, need to allocate a new one
            }

            // Allocate a new segment
            self.allocate_segment()?;
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
        // FIFO layer doesn't do compaction - just mark deleted
        self.mark_deleted(location);
        // The admission queue has no compaction path: it is FIFO-organised, so pairing neighbours would reorder the queue it exists to preserve.
        false
    }

    fn mark_deleted_and_free_empty(&self, location: ItemLocation) {
        // Same reason: this layer reclaims by evicting whole segments in
        // order, so there is no partial reclamation to do here.
        self.mark_deleted(location);
    }
}

/// Result of a non-blocking eviction attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictResult {
    /// Segment was freed immediately (ref_count was 0).
    Freed,
    /// Segment was deferred (ref_count > 0, transitioned to AwaitingRelease).
    Deferred,
    /// No segment could be evicted at all.
    NoCandidate,
}

/// Non-blocking eviction methods for FifoLayer.
impl FifoLayer {
    /// Try to evict without blocking on ref_count.
    ///
    /// Returns `EvictResult::Freed` if a segment was freed,
    /// `EvictResult::Deferred` if the segment was condemned but not yet freed,
    /// `EvictResult::NoCandidate` if no segment could be evicted.
    pub fn evict_nonblocking<H: Hashtable>(&self, hashtable: &H) -> EvictResult {
        match self.chain.pop(&self.pool) {
            Ok(segment_id) => {
                if self.process_evicted_segment_nonblocking(segment_id, hashtable) {
                    EvictResult::Freed
                } else {
                    EvictResult::Deferred
                }
            }
            Err(_) => EvictResult::NoCandidate,
        }
    }

    /// Try to evict without blocking, with demotion callback.
    pub fn evict_nonblocking_with_demoter<H, F>(&self, hashtable: &H, demoter: F) -> EvictResult
    where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, Location),
    {
        match self.chain.pop(&self.pool) {
            Ok(segment_id) => {
                if self.process_evicted_segment_with_demoter_nonblocking(
                    segment_id, hashtable, demoter,
                ) {
                    EvictResult::Freed
                } else {
                    EvictResult::Deferred
                }
            }
            Err(_) => EvictResult::NoCandidate,
        }
    }

    /// Emergency eviction: find any Sealed segment with ref_count == 0.
    pub fn try_emergency_evict<H: Hashtable>(&self, hashtable: &H) -> bool {
        self.emergency_evict(hashtable)
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
    hugepage_size: crate::hugepage::HugepageSize,
    spare_capacity: Option<u32>,
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

    /// Build the FIFO layer.
    pub fn build(self) -> Result<FifoLayer, std::io::Error> {
        let mut builder = MemoryPoolBuilder::new(self.pool_id)
            .per_item_ttl(true) // FIFO layer uses per-item TTL
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
        pub(super) fn create_demoting_test_layer() -> FifoLayer {
            let mut layer = FifoLayerBuilder::new()
                .layer_id(0)
                .pool_id(0)
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
            layer.set_next_layer(1);
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

        pub(super) fn stage_one_item(layer: &FifoLayer, reads: usize) -> Staged {
            let hashtable = MultiChoiceHashtable::new(10);
            let verifier = SinglePoolVerifier { pool: layer.pool() };

            let location = layer
                .write_item(KEY, b"value", b"", Duration::from_secs(3600))
                .expect("write");
            hashtable
                .insert(KEY, location.to_location(), &verifier)
                .expect("insert");

            // The warming reads go through the verifier, which is fine here --
            // the segment is still `Live`, so `admits_verify_reader` allows it.
            for _ in 0..reads {
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
    /// Unreachable in production today -- every caller is gated on
    /// `chain.try_remove`, whose `Sealed -> Draining` CAS admits exactly one
    /// thread -- so this drives the path directly. The hazard it pins is #142:
    /// the tail's `Locked -> Reserved` CAS *succeeds* for a loser, because the
    /// winner is what put the segment in `Locked`, and the loser then returns
    /// the winner's segment to the pool mid-clear.
    #[cfg(all(not(feature = "loom"), not(feature = "shuttle")))]
    mod lost_claim {
        use super::blocking_eviction_frequency::*;
        use super::*;

        /// Red proof: drop the `if !claim { return }` guard in
        /// `process_evicted_segment` and this fails -- the segment lands in
        /// `Reserved` and the pool gains a segment that is still being cleared.
        #[test]
        fn a_blocking_eviction_that_loses_the_claim_touches_nothing() {
            let layer = create_demoting_test_layer();
            let staged = stage_one_item(&layer, 0);

            let segment = layer.pool().get(staged.segment_id).expect("segment");

            // Stand in for the thread that won: it holds `Locked` and is
            // partway through clearing.
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

        /// The demoting twin. Same hazard, separate body, so it needs its own
        /// proof -- deleting either guard alone must go red.
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

        // ---- claim before count, and the deferral tail (#133 / #134) --------

        /// A `FifoLayer` holding `count` items keyed `k{i:03}`, all registered in
        /// the returned hashtable.
        fn filled_layer(count: usize, value_len: usize) -> (FifoLayer, MultiChoiceHashtable) {
            let layer = create_test_layer();
            let hashtable = MultiChoiceHashtable::new(10);
            let value = vec![b'v'; value_len];
            for i in 0..count {
                let key = format!("k{i:03}");
                let loc = layer
                    .write_item(key.as_bytes(), &value, b"", Duration::from_secs(3600))
                    .expect("write");
                let verifier = SinglePoolVerifier { pool: layer.pool() };
                let _ = hashtable.insert(key.as_bytes(), loc.to_location(), &verifier);
            }
            (layer, hashtable)
        }

        /// The recycle must be gated on a count read *after* the exclusive claim,
        /// never before it (#133).
        ///
        /// The reader here arrives at the one instant that separates the two
        /// orders: after the evictor has decided to claim, before the claim is
        /// published. The segment is still `Draining`, which admits a key-verify
        /// reader on purpose, so the pin succeeds -- and a count read taken
        /// *earlier* cannot see it.
        ///
        /// Claim first, and the count that follows does see the pin, so the
        /// evictor defers. Count first, and the evictor believes the segment is
        /// unreferenced, takes `Locked`, and recycles it while the reader is
        /// inside `verify_key_at_offset`.
        ///
        /// Red proof: swap the two lines in
        /// `process_evicted_segment_nonblocking` to
        ///
        /// ```ignore
        /// let unpinned = segment.ref_count_seqcst() == 0;
        /// let claimed = super::try_claim_for_clear(segment);
        /// if claimed && unpinned {
        /// ```
        ///
        /// and the segment is recycled out from under the pin.
        #[test]
        fn eviction_does_not_recycle_a_segment_pinned_while_it_was_claiming() {
            let (layer, hashtable) = filled_layer(24, 4096);
            assert!(
                layer.chain.segment_count() > 1,
                "the fill must chain more than one segment, or there is nothing to evict"
            );
            let head = layer.chain.head().expect("a chain head to evict");

            let pinned = std::rc::Rc::new(std::cell::Cell::new(false));
            {
                let flag = std::rc::Rc::clone(&pinned);
                let pool_ptr: *const MemoryPool = layer.pool();
                let _hook = interpose::install(Box::new(move |phase| {
                    if phase == interpose::CLAIM_BEFORE_CAS && !flag.get() {
                        // SAFETY: `layer` outlives the hook guard, which is
                        // dropped at the end of this block.
                        let seg = unsafe { &*pool_ptr }.get(head).expect("head segment");
                        assert_eq!(
                            seg.state(),
                            State::Draining,
                            "the claim window is entered with the segment still Draining"
                        );
                        assert!(
                            seg.try_acquire_read(),
                            "Draining admits a key-verify reader -- that is the whole \
                             reason it is not an exclusive claim"
                        );
                        flag.set(true);
                    }
                }));

                let _ = layer.evict_nonblocking(&hashtable);
            }

            assert!(pinned.get(), "the hook must have run");

            let seg = layer.pool().get(head).expect("head segment");
            assert_eq!(seg.ref_count(), 1, "the reader is still pinned");
            assert_ne!(
                seg.state(),
                State::Reserved,
                "the evictor recycled a segment while a reader was pinned -- its count was \
                 read before the claim, so the reader's arrival was invisible (#133)"
            );
            assert_ne!(
                seg.state(),
                State::Free,
                "the evictor published a pinned segment on the free queue (#133)"
            );
            assert_eq!(
                seg.state(),
                State::AwaitingRelease,
                "the evictor must defer to the last reference out instead"
            );

            // And the deferral completes when the reader leaves.
            seg.release_read();
            assert_eq!(seg.ref_count(), 0);
            assert_eq!(
                seg.state(),
                State::Free,
                "the last reference out owes the AwaitingRelease -> Free handoff"
            );
        }

        /// The same inversion on the demoting eviction path.
        ///
        /// `evict_nonblocking_with_demoter` is the path `TieredCache` uses for
        /// any layer with a `next_layer` -- the one that produces the disk
        /// tier's demotions -- and it carried its own copy of the count gate.
        ///
        /// Red proof: swap the two lines in
        /// `process_evicted_segment_with_demoter_nonblocking`.
        #[test]
        fn demoting_eviction_does_not_recycle_a_segment_pinned_while_it_was_claiming() {
            let (layer, hashtable) = filled_layer(24, 4096);
            assert!(layer.chain.segment_count() > 1, "need something to evict");
            let head = layer.chain.head().expect("head");

            let pinned = std::rc::Rc::new(std::cell::Cell::new(false));
            {
                let flag = std::rc::Rc::clone(&pinned);
                let pool_ptr: *const MemoryPool = layer.pool();
                let _hook = interpose::install(Box::new(move |phase| {
                    if phase == interpose::CLAIM_BEFORE_CAS && !flag.get() {
                        // SAFETY: `layer` outlives this hook guard.
                        let seg = unsafe { &*pool_ptr }.get(head).expect("head segment");
                        assert_eq!(seg.state(), State::Draining);
                        assert!(seg.try_acquire_read());
                        flag.set(true);
                    }
                }));

                let _ = layer.evict_nonblocking_with_demoter(&hashtable, |_, _, _, _, _| {});
            }

            assert!(pinned.get(), "the hook must have run");
            let seg = layer.pool().get(head).expect("head segment");
            assert_eq!(seg.ref_count(), 1, "the reader is still pinned");
            assert_eq!(
                seg.state(),
                State::AwaitingRelease,
                "the demoting evictor recycled a segment pinned during its claim (#133)"
            );
            seg.release_read();
            assert_eq!(seg.state(), State::Free);
        }

        /// The state the blocking paths are in while they wait out readers.
        ///
        /// `wait_for_readers` fires `WAIT_BEFORE_POLL` from inside itself, so
        /// the probe moves with the wait: whatever the caller does before
        /// calling it is already published when this fires. `Locked` means the
        /// claim came first. `Draining` means the wait came first, which is
        /// #133 on the blocking path -- `Draining` still admits key-verify
        /// readers, so the zero the spin converges on can be invalidated by an
        /// arrival before the `Locked` CAS lands.
        ///
        /// Red proof: put `wait_for_readers` back in front of the claim in
        /// `layer::claim_and_wait_for_readers`.
        #[test]
        fn blocking_eviction_holds_the_claim_while_it_waits() {
            for demoting in [false, true] {
                let (layer, hashtable) = filled_layer(24, 4096);
                assert!(layer.chain.segment_count() > 1);

                let observed = std::rc::Rc::new(std::cell::Cell::new(None));
                {
                    let cell = std::rc::Rc::clone(&observed);
                    let pool_ptr: *const MemoryPool = layer.pool();
                    let _hook = interpose::install(Box::new(move |phase| {
                        if phase == interpose::WAIT_BEFORE_POLL && cell.get().is_none() {
                            // SAFETY: `layer` outlives this hook guard.
                            let pool = unsafe { &*pool_ptr };
                            for id in 0..pool.segment_count() as u32 {
                                if let Some(seg) = pool.get(id)
                                    && matches!(seg.state(), State::Draining | State::Locked)
                                {
                                    cell.set(Some(seg.state()));
                                    return;
                                }
                            }
                        }
                    }));

                    if demoting {
                        layer.evict_with_demoter(&hashtable, |_, _, _, _, _| {});
                    } else {
                        layer.evict(&hashtable);
                    }
                }

                assert_eq!(
                    observed.get(),
                    Some(State::Locked),
                    "the blocking eviction path (demoting={demoting}) waited for readers \
                     while the segment was still admitting them -- it must claim first \
                     (#133)"
                );
            }
        }

        /// The exclusive claim must refuse every class of fresh reader.
        ///
        /// This is what makes a post-claim count read final: `Draining` admits
        /// key-verify readers, so only `Locked` can carry the claim.
        #[test]
        fn the_clear_claim_refuses_every_fresh_reader() {
            let (layer, _hashtable) = filled_layer(4, 64);
            let head = layer.chain.head().expect("head");
            let seg = layer.pool().get(head).expect("segment");

            assert!(seg.cas_metadata(State::Live, State::Sealed, None, None));
            assert!(seg.cas_metadata(State::Sealed, State::Draining, None, None));
            assert!(
                seg.try_acquire_read(),
                "Draining admits the key-verify reader the demoter needs"
            );
            seg.release_read();

            assert!(crate::layer::try_claim_for_clear(seg), "the claim must win");
            assert_eq!(seg.state(), State::Locked);
            assert!(
                !seg.try_acquire_read(),
                "a claimed segment must admit no fresh key-verify reader -- otherwise the \
                 count read after the claim can still go stale"
            );
            assert!(
                seg.get_item(0, b"k000").is_err(),
                "nor a fresh guard reader"
            );
            assert_eq!(seg.ref_count(), 0, "a refused acquire leaks no reference");
        }

        /// The deferral tail must reclaim a segment whose last reader left during
        /// the condemn window, rather than stranding it.
        ///
        /// `AwaitingRelease` with `ref_count == 0` is a permanent leak: nothing
        /// sweeps it, and the reader that would have discharged the handoff saw a
        /// state that was not yet `AwaitingRelease` and declined.
        ///
        /// This is the one call a test can make directly, which is the point of
        /// hoisting the tail out of its four copies: on `main` the branch was only
        /// reachable through a race.
        ///
        /// Red proof: replace the second line of `layer::condemn_and_reclaim` with
        /// `false` -- the "delete the evictor's post-CAS race fix" mutation -- and
        /// the segment strands.
        #[test]
        fn condemn_and_reclaim_discharges_the_handoff_when_the_last_reader_already_left() {
            let (layer, _hashtable) = filled_layer(4, 64);
            let head = layer.chain.head().expect("head");
            let seg = layer.pool().get(head).expect("segment");

            assert!(seg.cas_metadata(State::Live, State::Sealed, None, None));
            assert!(seg.cas_metadata(State::Sealed, State::Draining, None, None));
            assert_eq!(seg.ref_count(), 0, "the reader has already gone");

            assert!(
                crate::layer::condemn_and_reclaim(seg, State::Draining),
                "with no reader left, the condemner itself owes the free"
            );
            assert_eq!(
                seg.state(),
                State::Free,
                "the segment stranded in AwaitingRelease with ref_count == 0 -- nothing \
                 sweeps that state, so this is a permanent leak of one segment"
            );
        }

        /// The same tail, entered from the `Locked` claim rather than `Draining`.
        ///
        /// #133's order makes this the common deferral: the evictor claims, finds
        /// readers, and hands the segment back from `Locked`. The transition must
        /// still publish `AwaitingRelease`, and must **not** advance the
        /// incarnation on the way -- the readers' pins are still good and nothing
        /// has been cleared, so the incarnation has not ended.
        ///
        /// Red proof: restore `cas_metadata`'s old bump rule
        /// (`state == Locked && new_state != Locked`).
        #[test]
        fn condemning_from_a_claim_does_not_end_the_incarnation() {
            let (layer, _hashtable) = filled_layer(4, 64);
            let head = layer.chain.head().expect("head");
            let seg = layer.pool().get(head).expect("segment");

            assert!(seg.cas_metadata(State::Live, State::Sealed, None, None));
            assert!(seg.cas_metadata(State::Sealed, State::Draining, None, None));
            assert!(seg.try_acquire_read(), "pin it before the claim");
            let incarnation = seg.incarnation();

            assert!(crate::layer::try_claim_for_clear(seg));
            assert!(
                !crate::layer::condemn_and_reclaim(seg, State::Locked),
                "a reader is still in, so the last reference out owes the free"
            );
            assert_eq!(seg.state(), State::AwaitingRelease);
            assert_eq!(
                seg.incarnation(),
                incarnation,
                "the deferral hands a claim back; it is not the end of the segment's life. \
                 Bumping here would make every location the still-pinned readers hold stop \
                 resolving, and would burn the 6-bit tag twice per lifetime"
            );

            seg.release_read();
            assert_eq!(seg.state(), State::Free);
            assert_ne!(
                seg.incarnation(),
                incarnation,
                "the incarnation ends at AwaitingRelease -> Free, exactly once"
            );
        }

        /// The blocking path must claim before it waits.
        ///
        /// Waiting under `Draining` converges on a zero a fresh key-verify pin can
        /// invalidate before the `Locked` CAS lands; waiting under `Locked` cannot,
        /// because no fresh pin is admitted.
        ///
        /// Red proof: put `wait_for_readers` back in front of the claim in
        /// `layer::claim_and_wait_for_readers`.
        #[test]
        fn claim_and_wait_holds_the_claim_while_it_waits() {
            let (layer, _hashtable) = filled_layer(4, 64);
            let head = layer.chain.head().expect("head");
            let seg = layer.pool().get(head).expect("segment");

            assert!(seg.cas_metadata(State::Live, State::Sealed, None, None));
            assert!(seg.cas_metadata(State::Sealed, State::Draining, None, None));

            // A reader is admitted right up to the claim...
            assert!(seg.try_acquire_read());
            seg.release_read();

            assert!(crate::layer::claim_and_wait_for_readers(seg));
            assert_eq!(
                seg.state(),
                State::Locked,
                "the wait must run with the claim already published, or the zero it \
                 returns on can be invalidated by an arrival (#133)"
            );
            assert!(
                !seg.try_acquire_read(),
                "...and refused from the claim onwards, for the whole of the wait"
            );

            // A second claimant finds the segment already taken and must not
            // proceed to clear it.
            assert!(
                !crate::layer::claim_and_wait_for_readers(seg),
                "the claim is exclusive -- exactly one caller may clear"
            );
        }
    }

    /// A layer must accept writes again after `reset()`.
    ///
    /// `reset()` backs FLUSHALL. Resetting the pool alone leaves the FIFO
    /// chain naming segments that are now `Free`, so `FifoChain::push` cannot
    /// link onto that stale tail -- and `allocate_segment` reports the failure
    /// as `OutOfMemory` even though every segment is free.
    #[test]
    fn test_layer_accepts_writes_after_reset() {
        let layer = create_test_layer();
        let ttl = Duration::from_secs(3600);
        let value = vec![b'v'; 4096];

        // Deep enough that the chain spans several segments, so the tail the
        // reset must clear is not also the head.
        for i in 0..48 {
            let key = format!("pre{i:03}");
            layer
                .write_item(key.as_bytes(), &value, b"", ttl)
                .expect("pre-reset write");
        }
        assert!(
            layer.chain.segment_count() > 1,
            "the fill must chain more than one segment for this to test the link"
        );

        layer.reset();

        assert_eq!(layer.chain.segment_count(), 0, "chain must be emptied");
        assert!(
            layer.chain.tail().is_none(),
            "tail must not name a free segment"
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
            .spare_capacity(0) // No spare for tests
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
        // Same segment, same incarnation -- only the pool is wrong, which is
        // what this test is about.
        let layout = layer.pool().layout();
        let (_, segment_id, incarnation, offset) = location.unpack(layout);
        let wrong_location = ItemLocation::new(layout, 1, segment_id, incarnation, offset);

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
        // Same segment, same incarnation -- only the pool is wrong, which is
        // what this test is about.
        let layout = layer.pool().layout();
        let (_, segment_id, incarnation, offset) = location.unpack(layout);
        let wrong_location = ItemLocation::new(layout, 1, segment_id, incarnation, offset);
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
        // Same segment, same incarnation -- only the pool is wrong, which is
        // what this test is about.
        let layout = layer.pool().layout();
        let (_, segment_id, incarnation, offset) = location.unpack(layout);
        let wrong_location = ItemLocation::new(layout, 1, segment_id, incarnation, offset);
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
        let layout = layer.pool().layout();
        assert_eq!(loc1.segment_id(layout), loc2.segment_id(layout));
        assert_eq!(loc2.segment_id(layout), loc3.segment_id(layout));

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
        use crate::hashtable_impl::MultiChoiceHashtable;

        let layer = create_test_layer();
        let hashtable = MultiChoiceHashtable::new(10);

        // Evict on empty layer should return false
        let evicted = layer.evict(&hashtable);
        assert!(!evicted);
    }

    #[test]
    fn test_expire_returns_zero() {
        use crate::hashtable_impl::MultiChoiceHashtable;

        let layer = create_test_layer();
        let hashtable = MultiChoiceHashtable::new(10);

        // FIFO layer expire always returns 0 (no-op)
        let expired = layer.expire(&hashtable);
        assert_eq!(expired, 0);
    }

    #[test]
    fn test_evict_with_items() {
        use crate::hashtable_impl::MultiChoiceHashtable;

        // Create layer with small segments to trigger eviction
        let layer = FifoLayerBuilder::new()
            .layer_id(0)
            .pool_id(0)
            .segment_size(4 * 1024) // 4KB segments
            .heap_size(16 * 1024) // Only 4 segments
            .spare_capacity(0) // No spare for tests
            .build()
            .expect("Failed to create layer");

        let hashtable = MultiChoiceHashtable::new(10);

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
            .spare_capacity(0) // No spare for tests
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
            .spare_capacity(0) // No spare for tests
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
        let invalid_location = ItemLocation::new(layer.pool().layout(), 0, 999, 0, 0);
        let guard = layer.get_item(invalid_location, b"key");
        assert!(guard.is_none());
    }

    #[test]
    fn test_item_ttl_invalid_segment() {
        let layer = create_test_layer();

        // Create a location with an invalid segment ID
        let invalid_location = ItemLocation::new(layer.pool().layout(), 0, 999, 0, 0);
        let ttl = layer.item_ttl(invalid_location);
        assert!(ttl.is_none());
    }

    #[test]
    fn test_mark_deleted_invalid_segment() {
        let layer = create_test_layer();

        // Create a location with an invalid segment ID
        let invalid_location = ItemLocation::new(layer.pool().layout(), 0, 999, 0, 0);
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
            .spare_capacity(0) // No spare for tests
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
