//! io_uring-based disk layer implementation.
//!
//! [`IoUringDiskLayer`] replaces the mmap-based [`DiskLayer`] with an
//! io_uring-driven approach:
//!
//! - **Writes**: Items are written to RAM write buffers. When a segment
//!   is sealed, it's queued for flushing to disk via io_uring.
//! - **Reads**: If the segment's write buffer is still attached (not yet
//!   flushed), reads go to RAM (synchronous). Otherwise, the caller must
//!   perform an io_uring read from disk.
//!
//! # Integration
//!
//! The server handler calls:
//! - `write_item()` to write demoted items
//! - `read_from_buffer()` for synchronous reads from write buffers
//! - `prepare_read()` to get parameters for io_uring disk reads
//! - `take_flush_queue()` on each tick to submit pending flushes
//! - `complete_flush()` when io_uring write completes
//! - `release_read()` after a disk read completes

use crate::config::LayerConfig;
use crate::disk::DiskSegmentMeta;
use crate::error::{CacheError, CacheResult};
use crate::eviction::{ItemFate, determine_item_fate};
use crate::hashtable::{Hashtable, KeyVerifier};
use crate::item::BasicHeader;
use crate::item_location::ItemLocation;
use crate::layer::Layer;
use crate::location::Location;
use crate::organization::TtlBuckets;
use crate::pool::RamPool;
use crate::segment::{Segment, SegmentKeyVerify};
use crate::slice_segment::ValueRefRaw;
use crate::state::State;
use crate::sync::*;
use std::sync::Mutex;
use std::time::Duration;

use super::aligned_buffer::AlignedBufferPool;
use super::io_uring_pool::IoUringPool;

/// Parameters for submitting an io_uring read of a disk segment item.
///
/// Returned by [`IoUringDiskLayer::prepare_read`] when an item is on a
/// committed disk segment (write buffer already detached).
#[derive(Debug)]
pub struct DiskReadParams {
    /// Block-aligned byte offset to read from on the device/file.
    pub disk_offset: u64,
    /// Block-aligned read length in bytes.
    pub read_len: u32,
    /// Item's byte offset within the read buffer after the read completes.
    pub item_offset: u32,
    /// Segment ID (for releasing ref_count after read).
    pub segment_id: u32,
    /// Pool ID.
    pub pool_id: u8,
}

/// A sealed segment that needs to be flushed to disk via io_uring.
///
/// Created when a write segment fills up and is sealed. The server handler
/// picks these up on each tick and submits io_uring writes.
pub struct FlushRequest {
    /// Segment ID being flushed.
    pub segment_id: u32,
    /// Block-aligned byte offset on the device/file.
    pub disk_offset: u64,
    /// Number of bytes written to the segment (write_offset at seal time).
    pub data_len: u32,
    /// Pointer to the segment's write buffer data.
    pub buffer_ptr: *const u8,
    /// Block-aligned length for the I/O operation.
    pub buffer_len: u32,
}

// SAFETY: FlushRequest contains a raw pointer that points to a stable
// AlignedBuffer allocation. The buffer remains valid until complete_flush()
// is called, which detaches and returns it to the pool.
unsafe impl Send for FlushRequest {}

/// Helper struct for verifying keys in IoUringPool segments.
struct IoUringPoolVerifier<'a> {
    pool: &'a IoUringPool,
}

impl KeyVerifier for IoUringPoolVerifier<'_> {
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

/// io_uring-based disk layer for the cache hierarchy.
///
/// Replaces the mmap-based `DiskLayer` with explicit I/O operations:
/// - Items are written to RAM write buffers (synchronous)
/// - Sealed segments are flushed to disk via io_uring (async)
/// - Reads from committed segments require io_uring reads (async)
/// - Reads from segments with write buffers are synchronous
pub struct IoUringDiskLayer {
    /// Layer identifier.
    layer_id: u8,

    /// Layer configuration.
    config: LayerConfig,

    /// Pool of disk segment metadata.
    pool: IoUringPool,

    /// TTL bucket organization.
    buckets: TtlBuckets,

    /// Current write segment ID per bucket.
    current_write_segments: Vec<AtomicU32>,

    /// Queue of sealed segments pending flush to disk.
    flush_queue: Mutex<Vec<FlushRequest>>,

    /// Pool of page-aligned write buffers for staging segment data.
    buffer_pool: Mutex<AlignedBufferPool>,
}

impl IoUringDiskLayer {
    /// Create a new builder for IoUringDiskLayer.
    pub fn builder() -> IoUringDiskLayerBuilder {
        IoUringDiskLayerBuilder::new()
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
    pub fn pool(&self) -> &IoUringPool {
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

    /// Write an item to the disk layer.
    ///
    /// Writes to the current write segment's RAM buffer. If the segment
    /// is full, seals it (pushes to flush queue) and allocates a new one.
    /// Write buffers are allocated from the internal buffer pool.
    pub fn write_item_with_buffers(
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

                // Segment is full — seal it and queue for flush
                self.seal_and_queue_flush(segment_id);

                // Reset cached write segment for this bucket
                let bucket_index = self.buckets.get_bucket_index(ttl);
                if bucket_index < self.current_write_segments.len() {
                    self.current_write_segments[bucket_index].store(u32::MAX, Ordering::Release);
                }
            }

            // Loop will allocate a new segment on next iteration
            let bucket_index = self.buckets.get_bucket_index(ttl);
            let bucket = self.buckets.get_bucket_by_index(bucket_index);
            self.allocate_segment_for_bucket(bucket_index, bucket.ttl())?;
        }
    }

    /// Try to read an item synchronously from a segment's write buffer.
    ///
    /// Returns the raw value reference components if the segment has a
    /// write buffer attached (i.e., it hasn't been flushed to disk yet).
    /// Returns `None` if the segment has no write buffer (data is on disk).
    pub fn read_from_buffer(&self, location: ItemLocation, key: &[u8]) -> Option<ValueRefRaw> {
        if location.pool_id() != self.pool.pool_id() {
            return None;
        }

        let (_, segment_id, _, offset) = location.unpack(self.pool.layout());
        let segment = self.pool.get(segment_id)?;

        let state = segment.state();
        // Condemned: hashtable entries are gone, so this location is stale and
        // the answer is a miss (#127).
        if !state.is_readable() || state.is_condemned() {
            return None;
        }

        // Only works if write buffer is still attached
        if !segment.has_write_buffer() {
            return None;
        }

        // Check segment-level TTL
        let now = Self::now_secs();
        let expire_at = segment.expire_at();
        if expire_at > 0 && now as u64 >= expire_at as u64 {
            return None;
        }

        // Pin the segment and resolve its staging buffer. Both back-outs in
        // here return the reference; see `pin_for_buffer_read`.
        let ref_count_ptr = segment.ref_count_ptr();
        let data_ptr = self.pin_for_buffer_read(segment)?;

        // Parse header
        if offset as usize + BasicHeader::SIZE > segment.capacity() {
            self.release_segment_ref(segment);
            return None;
        }

        let header = unsafe { BasicHeader::from_ptr(data_ptr.add(offset as usize)) };

        if header.is_deleted() {
            self.release_segment_ref(segment);
            return None;
        }

        // A bounds check on the item's actual bytes, not a stride: the padding
        // past `padded_size()` is dead space the last item in a segment need
        // not own, so checking the stride here would reject a valid read.
        let item_size = header.padded_size();
        if offset as usize + item_size > segment.capacity() {
            self.release_segment_ref(segment);
            return None;
        }

        // Verify key
        let key_start = offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + header.key_len() as usize;
        if key_end > segment.capacity() {
            self.release_segment_ref(segment);
            return None;
        }

        let stored_key = unsafe {
            std::slice::from_raw_parts(data_ptr.add(key_start), header.key_len() as usize)
        };
        if stored_key != key {
            self.release_segment_ref(segment);
            return None;
        }

        // Build ValueRefRaw
        let value_start = key_end;
        let value_len = header.value_len() as usize;
        let value_ptr = unsafe { data_ptr.add(value_start) };

        Some((
            ref_count_ptr,
            value_ptr,
            value_len,
            segment.metadata_ptr(),
            segment.free_queue_ptr(),
            segment.id(),
        ))
    }

    /// Prepare parameters for an io_uring disk read.
    ///
    /// If the segment has a write buffer, returns `None` (caller should
    /// use `read_from_buffer()` instead). If the segment is committed to
    /// disk, increments ref_count and returns the disk read parameters.
    ///
    /// # Parameters
    /// - `location`: Item location from hashtable
    /// - `read_size`: How many bytes to read (typically one block)
    pub fn prepare_read(&self, location: ItemLocation, read_size: u32) -> Option<DiskReadParams> {
        if location.pool_id() != self.pool.pool_id() {
            return None;
        }

        let (_, segment_id, _, offset) = location.unpack(self.pool.layout());
        let segment = self.pool.get(segment_id)?;

        let state = segment.state();
        // Condemned: hashtable entries are gone, so this location is stale and
        // the answer is a miss (#127).
        if !state.is_readable() || state.is_condemned() {
            return None;
        }

        // If write buffer is present, caller should use read_from_buffer()
        if segment.has_write_buffer() {
            return None;
        }

        // Check segment-level TTL
        let now = Self::now_secs();
        let expire_at = segment.expire_at();
        if expire_at > 0 && now as u64 >= expire_at as u64 {
            return None;
        }

        // Pin the segment so it cannot be evicted while the read is in
        // flight; `release_read` drops this again on completion.
        if !self.pin_for_read(segment) {
            return None;
        }

        // Compute block-aligned read range
        let (disk_offset, read_len, item_offset) =
            self.pool.item_disk_range(segment_id, offset, read_size);

        Some(DiskReadParams {
            disk_offset,
            read_len,
            item_offset,
            segment_id,
            pool_id: self.pool.pool_id(),
        })
    }

    /// Reset the layer to its freshly built state: empty TTL buckets, no
    /// cached write segments, no pending flushes, and every segment free.
    ///
    /// Backs [`crate::cache::TieredCache::flush`]. Resetting the pool alone is
    /// not enough, for two reasons:
    ///
    /// - The buckets and the per-bucket write-segment cache would keep naming
    ///   segments the pool had just recycled, and the next append onto that
    ///   stale tail fails -- reported as `OutOfMemory` even with every segment
    ///   free.
    /// - A queued [`FlushRequest`] names a segment id and the disk offset that
    ///   segment owned when it was sealed. Submitting it after the flush would
    ///   write a recycled segment's bytes at the old segment's offset, so the
    ///   queue is dropped rather than carried across.
    ///
    /// Every write buffer is detached and returned to the buffer pool; the
    /// buffers behind the dropped flush requests are exactly those, and
    /// `DiskSegmentMeta::reset` does not release them.
    ///
    /// Organization state is cleared before the pool so that a reader following
    /// a bucket chain during the window reaches segments that are still live
    /// rather than ones already back on the free queue.
    ///
    /// # Preconditions
    ///
    /// Only valid when no concurrent operation is touching this layer, no
    /// in-flight io_uring write still references a write buffer, and only after
    /// the hashtable has been cleared.
    pub fn reset(&self) {
        // Drop pending flushes before anything can renumber the segments they
        // name. Their buffers are released with the rest below.
        self.flush_queue.lock().unwrap().clear();

        self.buckets.reset();
        for slot in &self.current_write_segments {
            slot.store(u32::MAX, Ordering::Release);
        }

        // `DiskSegmentMeta::reset` leaves the write buffer attached, so
        // reclaim them here or the buffer pool drains one flush at a time.
        for id in 0..self.pool.segment_count() as u32 {
            if let Some(segment) = self.pool.get(id)
                && let Some(buf) = segment.detach_write_buffer()
            {
                self.buffer_pool.lock().unwrap().release(buf);
            }
        }

        self.pool.reset_all();
    }

    /// Drain the flush queue, returning all pending flush requests.
    ///
    /// Called by the server handler on each tick to submit io_uring writes.
    pub fn take_flush_queue(&self) -> Vec<FlushRequest> {
        let mut queue = self.flush_queue.lock().unwrap();
        std::mem::take(&mut *queue)
    }

    /// Complete a flush operation.
    ///
    /// Called when an io_uring write completes. Detaches the write buffer
    /// from the segment and returns it to the buffer pool.
    pub fn complete_flush(&self, segment_id: u32) {
        if let Some(segment) = self.pool.get(segment_id)
            && let Some(buf) = segment.detach_write_buffer()
        {
            self.buffer_pool.lock().unwrap().release(buf);
        }
    }

    /// Pin a segment against eviction for a read, re-validating the state
    /// after the increment.
    ///
    /// Returns `false` -- reference already dropped -- if the segment stopped
    /// being readable, or was condemned, in the window between the caller's
    /// first look at the state and the increment.
    ///
    /// The protocol itself lives in [`crate::segment::try_acquire_pin`], where
    /// every other acquire in the crate already went (#134). This was the last
    /// hand-rolled copy in the tree -- its own `fetch_add` followed by its own
    /// `state_seqcst()` re-check, with no interposition point, which is exactly
    /// why that re-check could be deleted and the whole suite stay green.
    /// Routing through the shared body puts this site behind
    /// `interpose::ACQUIRE_AFTER_INCREMENT`, so a test can condemn the segment
    /// inside the window by hand; see
    /// `a_pin_that_loses_the_condemn_race_completes_the_free`.
    ///
    /// [`State::admits_guard_reader`] is the predicate, not the wider
    /// key-verify one: these are value reads, so `Draining` must refuse them.
    /// It is the same `is_readable() && !is_condemned()` pair this used to
    /// spell out by hand.
    ///
    /// The back-out goes through `release_segment_ref` rather than a bare
    /// `fetch_sub`, because a back-out that drops the last reference on an
    /// already-condemned segment owes it the `AwaitingRelease -> Free`
    /// handoff -- the condemner that saw this reference has already declined
    /// it (#131).
    fn pin_for_read(&self, segment: &DiskSegmentMeta) -> bool {
        // SAFETY: both pointers name this segment's own atomics, which live as
        // long as the pool and therefore outlive the call.
        unsafe {
            crate::segment::try_acquire_pin(
                &*segment.ref_count_ptr(),
                &*segment.metadata_ptr(),
                State::admits_guard_reader,
                || self.release_segment_ref(segment),
            )
        }
    }

    /// Pin a segment and resolve the staging buffer a RAM read will be served
    /// out of.
    ///
    /// Returns `None` -- reference already dropped -- when the segment stopped
    /// being readable, and equally when the buffer went away: `complete_flush`
    /// detaches it, and can land between the caller's `has_write_buffer()`
    /// check and this resolve.
    ///
    /// That second back-out is why the pin and the resolve are one call. Both
    /// callers used to resolve the pointer themselves with `?`, returning with
    /// the reference still held (#130), and a segment that loses one never
    /// reads `ref_count() == 0` again -- the evictor's gate never opens for it
    /// again, so it is pinned for the life of the process: never evicted,
    /// never condemned, never recycled. Handing the reference back through
    /// `release_segment_ref` rather than a bare `fetch_sub` is the other half:
    /// a back-out that drops the last reference on an already-condemned
    /// segment owes it the free handoff (#131).
    fn pin_for_buffer_read(&self, segment: &DiskSegmentMeta) -> Option<*const u8> {
        if !self.pin_for_read(segment) {
            return None;
        }

        fence(Ordering::Acquire);

        match segment.write_buffer_ptr() {
            Some(data_ptr) => Some(data_ptr),
            None => {
                self.release_segment_ref(segment);
                None
            }
        }
    }

    /// Drop one reference taken by a synchronous read path, completing the
    /// condemned handoff if we were the last reader.
    ///
    /// A back-out can now land on a condemned segment (see #127); the last one
    /// out has to release it or nobody will.
    fn release_segment_ref(&self, segment: &DiskSegmentMeta) {
        // SeqCst -- the release half of the handoff Dekker pair; see
        // `SliceSegment::release_ref` and `Segment::ref_count_seqcst` (#129).
        let prev = unsafe { (*segment.ref_count_ptr()).fetch_sub(1, Ordering::SeqCst) };
        if prev == 1 {
            self.free_condemned_returning_buffer(segment);
        }
    }

    /// The condemned handoff for a disk segment, returning its staging buffer
    /// to the pool on the way out.
    ///
    /// The buffer return rides in `try_free_condemned`'s `on_freed` hook
    /// rather than in a pre-check of its own, and that placement is the whole
    /// point. The hook runs only for the caller whose CAS won -- so the buffer
    /// is returned exactly once, only for a segment that really was condemned
    /// and really had no references left, and only after the same
    /// `prev == 1` re-validation the free itself rests on. A back-out from a
    /// live segment must not detach the buffer it is still writing into, and a
    /// reader that pinned the segment again since the caller's decrement must
    /// not have its `write_buffer_ptr()` pulled out from under it.
    ///
    /// The hook also runs *before* the push to the free queue, so no thread
    /// can reserve the segment and attach a fresh buffer in between and have
    /// that one returned instead.
    ///
    /// Returns `true` iff this call performed the `AwaitingRelease -> Free`
    /// transition, which is what lets it stand in for
    /// `Segment::release_condemned` as `layer::condemn_and_reclaim_with`'s
    /// reclaim.
    fn free_condemned_returning_buffer(&self, segment: &DiskSegmentMeta) -> bool {
        // SAFETY: the pointers come from this segment and outlive the call;
        // `free_queue_ptr` is the pool's queue and `id` names this segment.
        unsafe {
            crate::segment::try_free_condemned(
                &*segment.ref_count_ptr(),
                &*segment.metadata_ptr(),
                segment.free_queue_ptr(),
                segment.id(),
                || {
                    if let Some(buf) = segment.detach_write_buffer() {
                        self.buffer_pool.lock().unwrap().release(buf);
                    }
                },
            )
        }
    }

    /// Release a segment's ref_count after a disk read completes.
    ///
    /// Called when the server handler finishes processing a disk read.
    pub fn release_read(&self, segment_id: u32) {
        if let Some(segment) = self.pool.get(segment_id) {
            let ref_count_ptr = segment.ref_count_ptr();
            // SeqCst -- the release half of the handoff Dekker pair; see
            // `SliceSegment::release_ref` and `Segment::ref_count_seqcst`.
            let prev = unsafe { (*ref_count_ptr).fetch_sub(1, Ordering::SeqCst) };

            // Check if this was the last reader and segment is condemned.
            // The state check, the `prev == 1` re-validation and the buffer
            // return all live in `free_condemned_returning_buffer`; this used
            // to detach the buffer on `prev == 1` alone, without even checking
            // the state.
            if prev == 1 {
                self.free_condemned_returning_buffer(segment);
            }
        }
    }

    /// Seal a segment and queue it for flushing to disk.
    fn seal_and_queue_flush(&self, segment_id: u32) {
        let segment = match self.pool.get(segment_id) {
            Some(s) => s,
            None => return,
        };

        // Transition Live -> Sealed
        if !segment.cas_metadata(State::Live, State::Sealed, None, None) {
            return; // Already sealed or in wrong state
        }

        // Create flush request
        if let Some(buf_ptr) = segment.write_buffer_ptr() {
            let write_offset = segment.write_offset();
            let block_size = self.pool.block_size() as u64;

            // Align the write length up to block boundary
            let aligned_len = ((write_offset as u64 + block_size - 1) & !(block_size - 1)) as u32;

            let request = FlushRequest {
                segment_id,
                disk_offset: segment.disk_offset(),
                data_len: write_offset,
                buffer_ptr: buf_ptr,
                buffer_len: aligned_len,
            };

            let mut queue = self.flush_queue.lock().unwrap();
            queue.push(request);
        }
    }

    /// Allocate a new segment and add it to the specified bucket.
    fn allocate_segment_for_bucket(&self, bucket_index: usize, ttl: Duration) -> CacheResult<u32> {
        let segment_id = self.pool.reserve().ok_or(CacheError::OutOfMemory)?;

        let segment = match self.pool.get(segment_id) {
            Some(s) => s,
            None => {
                self.pool.release(segment_id);
                return Err(CacheError::OutOfMemory);
            }
        };

        // Allocate a write buffer from the dedicated buffer pool
        let buf = match self.buffer_pool.lock().unwrap().allocate() {
            Some(buf) => buf,
            None => {
                self.pool.release(segment_id);
                return Err(CacheError::OutOfMemory);
            }
        };
        segment.attach_write_buffer(buf);

        // Set segment expiration time
        let expire_at = Self::now_secs().saturating_add(ttl.as_secs() as u32);
        segment.set_expire_at(expire_at);

        // Add to bucket
        let bucket = self.buckets.get_bucket_by_index(bucket_index);
        match bucket.append_segment(segment_id, &self.pool) {
            Ok(()) => {
                if bucket_index < self.current_write_segments.len() {
                    self.current_write_segments[bucket_index].store(segment_id, Ordering::Release);
                }
                Ok(segment_id)
            }
            Err(_) => {
                // Return write buffer and release segment
                if let Some(buf) = segment.detach_write_buffer() {
                    self.buffer_pool.lock().unwrap().release(buf);
                }
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
            let cached_id = self.current_write_segments[bucket_index].load(Ordering::Acquire);
            if cached_id != u32::MAX
                && let Some(segment) = self.pool.get(cached_id)
                && segment.state() == State::Live
                && segment.has_write_buffer()
            {
                return Ok(cached_id);
            }
        }

        // Check bucket tail
        if let Some(tail_id) = bucket.tail()
            && let Some(segment) = self.pool.get(tail_id)
            && segment.state() == State::Live
            && segment.has_write_buffer()
        {
            if bucket_index < self.current_write_segments.len() {
                self.current_write_segments[bucket_index].store(tail_id, Ordering::Release);
            }
            return Ok(tail_id);
        }

        // Need to allocate new segment
        self.allocate_segment_for_bucket(bucket_index, bucket.ttl())
    }

    /// Remove all hashtable entries for items in a segment.
    fn drain_segment_from_hashtable<H: Hashtable>(&self, segment_id: u32, hashtable: &H) {
        let segment = match self.pool.get(segment_id) {
            Some(s) => s,
            None => return,
        };

        // Can only drain if write buffer is present (need data access)
        if !segment.has_write_buffer() {
            return;
        }

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
    ///
    /// # Claim, then count (#133)
    ///
    /// The claim `Draining -> Locked` comes first and the `ref_count` read
    /// second. The other way round -- which is what this was -- leaves a
    /// window: `Draining` deliberately still admits key-verify readers, so a
    /// reader can pin between the observation of zero and the claim, and this
    /// thread then rewrites the segment's bytes and recycles it while that
    /// reader is inside `verify_key_at_offset`. Every access involved is
    /// already `SeqCst` and the bad outcome is still reachable under a full SC
    /// total order, because it is a TOCTOU and not a Dekker pair. See
    /// [`crate::layer::try_claim_for_clear`].
    ///
    /// Either failure -- losing the claim, or winning it and finding readers
    /// -- takes the deferral arm, which clears nothing and hands the segment
    /// to whoever drops the last reference.
    fn process_evicted_segment<H: Hashtable>(&self, segment_id: u32, hashtable: &H) {
        let segment = match self.pool.get(segment_id) {
            Some(s) => s,
            None => return,
        };

        // Remove any pending flush request for this segment. Since the
        // staging buffer will be released below, the FlushRequest's pointer
        // would become stale if left in the queue.
        {
            let mut queue = self.flush_queue.lock().unwrap();
            queue.retain(|req| req.segment_id != segment_id);
        }

        // Claim before counting (#133). `try_claim_for_clear` publishes
        // `Locked`, the only state that refuses every class of fresh reader;
        // only then is a `ref_count` read final, because from here the count
        // can only fall. `ref_count_seqcst` because the claim CAS is the store
        // half of the Dekker pair this load completes (#129).
        let claimed = crate::layer::try_claim_for_clear(segment);
        if !claimed || segment.ref_count_seqcst() > 0 {
            self.drain_segment_from_hashtable(segment_id, hashtable);

            // Nothing was cleared on this arm, so the segment's bytes are
            // intact and the readers still in it stay valid. The condemn, its
            // race fix and the staging-buffer return all live in
            // `condemn_and_reclaim_with`; see its note for why the buffer
            // release has to ride inside `try_free_condemned`'s `on_freed`
            // hook rather than sit on either side of the condemn.
            let held = if claimed {
                State::Locked
            } else {
                State::Draining
            };
            crate::layer::condemn_and_reclaim_with(segment, held, |s| {
                self.free_condemned_returning_buffer(s)
            });
            return;
        }

        // Process each item
        if segment.has_write_buffer() {
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

                            let verifier = IoUringPoolVerifier { pool: &self.pool };
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

        // Return write buffer before releasing segment
        if let Some(buf) = segment.detach_write_buffer() {
            self.buffer_pool.lock().unwrap().release(buf);
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

impl Layer for IoUringDiskLayer {
    type Guard<'a> = crate::item::BasicItemGuard<'a>;

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
        self.write_item_with_buffers(key, value, optional, ttl)
    }

    fn get_item(&self, location: ItemLocation, key: &[u8]) -> Option<Self::Guard<'_>> {
        // For IoUringDiskLayer, synchronous get is only possible from write buffer.
        // The full async path is handled by the server handler via prepare_read().
        if location.pool_id() != self.pool.pool_id() {
            return None;
        }

        let (_, segment_id, _, offset) = location.unpack(self.pool.layout());
        let segment = self.pool.get(segment_id)?;
        let state = segment.state();
        // Condemned: hashtable entries are gone, so this location is stale and
        // the answer is a miss (#127).
        if !state.is_readable() || state.is_condemned() || !segment.has_write_buffer() {
            return None;
        }

        let now = Self::now_secs();
        let expire_at = segment.expire_at();
        if expire_at > 0 && now as u64 >= expire_at as u64 {
            return None;
        }

        let header_info = segment.verify_key_unexpired(offset, key, now)?;

        // Build a BasicItemGuard from the write buffer data. Both back-outs
        // in here return the reference; see `pin_for_buffer_read`.
        let ref_count_ptr = segment.ref_count_ptr();
        let data_ptr = self.pin_for_buffer_read(segment)?;

        let (key_len, optional_len, value_len) = header_info;

        let optional_start = offset as usize + BasicHeader::SIZE;
        let optional_end = optional_start + optional_len as usize;
        let key_start = optional_end;
        let key_end = key_start + key_len as usize;
        let value_start = key_end;
        let value_end = value_start + value_len as usize;

        if value_end > segment.capacity() {
            self.release_segment_ref(segment);
            return None;
        }

        unsafe {
            let key_slice = std::slice::from_raw_parts(data_ptr.add(key_start), key_len as usize);
            let value_slice =
                std::slice::from_raw_parts(data_ptr.add(value_start), value_len as usize);
            let optional_slice =
                std::slice::from_raw_parts(data_ptr.add(optional_start), optional_len as usize);

            Some(crate::item::BasicItemGuard::new(
                &*ref_count_ptr,
                key_slice,
                value_slice,
                optional_slice,
                &*segment.metadata_ptr(),
                segment.free_queue_ptr(),
                segment.id(),
            ))
        }
    }

    fn mark_deleted(&self, location: ItemLocation) {
        if location.pool_id() != self.pool.pool_id() {
            return;
        }

        let (_, segment_id, _, offset) = location.unpack(self.pool.layout());
        if let Some(segment) = self.pool.get(segment_id) {
            // Can only mark deleted if write buffer is present
            if segment.has_write_buffer() {
                segment.mark_deleted_at_offset(offset);
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
        self.evict_randomfifo(hashtable)
    }

    fn evict_with_demoter<H, F>(&self, hashtable: &H, _demoter: F) -> bool
    where
        H: Hashtable,
        F: FnMut(&[u8], &[u8], &[u8], Duration, Location),
    {
        // Disk is the last tier — no demotion.
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
        _key: &[u8],
        _value_len: usize,
        _optional: &[u8],
        _ttl: Duration,
    ) -> CacheResult<(ItemLocation, *mut u8, u32)> {
        // Streaming writes not supported for disk layer
        Err(CacheError::Unsupported)
    }

    fn finalize_write_item(&self, _location: ItemLocation, _item_size: u32) {}
    fn cancel_write_item(&self, _location: ItemLocation) {}

    fn mark_deleted_and_compact<H: Hashtable>(&self, location: ItemLocation, _hashtable: &H) {
        // Disk layer doesn't compact
        self.mark_deleted(location);
    }
}

/// Builder for [`IoUringDiskLayer`].
pub struct IoUringDiskLayerBuilder {
    layer_id: u8,
    config: LayerConfig,
    pool_id: u8,
    segment_size: usize,
    segment_count: usize,
    block_size: u32,
    write_buffer_count: usize,
}

impl IoUringDiskLayerBuilder {
    /// Create a new builder with defaults.
    pub fn new() -> Self {
        Self {
            layer_id: 2,
            config: LayerConfig::new()
                .with_ghosts(true)
                .with_demotion_threshold(2),
            pool_id: 2,
            segment_size: 8 * 1024 * 1024, // 8MB
            segment_count: 128,
            block_size: 4096,
            write_buffer_count: 16,
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

    /// Set the pool ID.
    pub fn pool_id(mut self, id: u8) -> Self {
        self.pool_id = id;
        self
    }

    /// Set the segment size in bytes.
    pub fn segment_size(mut self, size: usize) -> Self {
        self.segment_size = size;
        self
    }

    /// Set the number of segments.
    pub fn segment_count(mut self, count: usize) -> Self {
        self.segment_count = count;
        self
    }

    /// Set the I/O block size.
    pub fn block_size(mut self, size: u32) -> Self {
        self.block_size = size;
        self
    }

    /// Set the number of write buffers for staging segment data before flush.
    ///
    /// Under pressure, demotion degrades to discard. Default: 16.
    pub fn write_buffer_count(mut self, count: usize) -> Self {
        self.write_buffer_count = count;
        self
    }

    /// Build the IoUringDiskLayer.
    pub fn build(self) -> IoUringDiskLayer {
        let pool = IoUringPool::new(
            self.pool_id,
            self.segment_count,
            self.segment_size,
            self.block_size,
        );

        let num_buckets = crate::organization::MAX_TTL_BUCKETS;
        let buckets = TtlBuckets::new();

        let current_write_segments = (0..num_buckets).map(|_| AtomicU32::new(u32::MAX)).collect();

        let buffer_pool = AlignedBufferPool::new(self.write_buffer_count, self.segment_size, 4096);

        IoUringDiskLayer {
            layer_id: self.layer_id,
            config: self.config,
            pool,
            buckets,
            current_write_segments,
            flush_queue: Mutex::new(Vec::new()),
            buffer_pool: Mutex::new(buffer_pool),
        }
    }
}

impl Default for IoUringDiskLayerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::hashtable_impl::MultiChoiceHashtable;

    /// Fill one segment with items and index them, returning keys, their
    /// locations and the segment id.
    ///
    /// The locations are what lets a test ask whether an entry is still in the
    /// hashtable *without* going through the verifier:
    /// `Hashtable::get_item_frequency` matches on the location alone, so its
    /// answer does not depend on the segment's state. A `lookup` would be
    /// refused outright once the segment is `Locked` or `AwaitingRelease`, and
    /// an assertion built on one is vacuous exactly when it matters.
    fn fill_one_segment(
        layer: &IoUringDiskLayer,
        hashtable: &MultiChoiceHashtable,
    ) -> (Vec<String>, Vec<ItemLocation>, u32) {
        let verifier = IoUringPoolVerifier { pool: &layer.pool };
        let ttl = Duration::from_secs(3600);

        const ITEMS: usize = 5;
        let mut keys = Vec::new();
        let mut locations = Vec::new();
        let mut segment_id = None;
        for i in 0..ITEMS {
            let key = format!("key_{i:02}");
            let location = layer
                .write_item_with_buffers(key.as_bytes(), b"value", b"", ttl)
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
            locations.push(location);
        }
        (keys, locations, segment_id.expect("at least one item"))
    }

    /// Assert that none of `locations` is still indexed.
    ///
    /// Location-matched, so unlike [`assert_all_removed`] this stays honest on
    /// a segment whose state the key verifier refuses.
    ///
    /// Only `mod interposed` uses this, and that module is compiled out under
    /// the model checkers, so the helper has to carry the same gate or it is
    /// dead code there.
    #[cfg(all(not(feature = "loom"), not(feature = "shuttle")))]
    fn assert_none_indexed(
        hashtable: &MultiChoiceHashtable,
        keys: &[String],
        locations: &[ItemLocation],
    ) {
        for (key, location) in keys.iter().zip(locations) {
            assert!(
                hashtable
                    .get_item_frequency(key.as_bytes(), location.to_location())
                    .is_none(),
                "{key} is still indexed at a segment that is being taken away -- \
                 the sweep did not run"
            );
        }
    }

    fn assert_all_removed(
        layer: &IoUringDiskLayer,
        hashtable: &MultiChoiceHashtable,
        keys: &[String],
    ) {
        let verifier = IoUringPoolVerifier { pool: &layer.pool };
        for key in keys {
            assert!(
                hashtable.lookup(key.as_bytes(), &verifier).is_none(),
                "{key} survived the walk -- it stopped short of that item"
            );
        }
    }

    fn test_layer() -> IoUringDiskLayer {
        IoUringDiskLayerBuilder::new()
            .pool_id(2)
            .segment_size(64 * 1024)
            .segment_count(4)
            .build()
    }

    /// A layer must accept writes again after `reset()`, with nothing left
    /// pointing at the segments it just recycled.
    ///
    /// `reset()` backs FLUSHALL. Three things go stale at once here:
    ///
    /// - the TTL buckets and the per-bucket write-segment cache, which would
    ///   name `Free` segments and make the next append fail as `OutOfMemory`;
    /// - the flush queue, whose requests pair a segment id with the disk offset
    ///   that segment held when it was sealed -- submitting one after a flush
    ///   writes a recycled segment's bytes over the old segment's extent;
    /// - the write buffers, which `DiskSegmentMeta::reset` leaves attached, so
    ///   the buffer pool drains a segment's worth per flush.
    #[test]
    fn test_layer_accepts_writes_after_reset() {
        let layer = test_layer();
        let ttl = Duration::from_secs(3600);
        let value = vec![b'v'; 4096];
        let buffers_total = layer.buffer_pool.lock().unwrap().total();

        // Deep enough to fill and seal at least one segment, so the chain has
        // more than one link and the flush queue is not empty.
        for i in 0..30 {
            let key = format!("pre{i:03}");
            layer
                .write_item_with_buffers(key.as_bytes(), &value, b"", ttl)
                .expect("pre-reset write");
        }
        assert!(
            layer.buckets.total_segment_count() > 1,
            "the fill must chain more than one segment for this to test the link"
        );
        assert!(
            !layer.flush_queue.lock().unwrap().is_empty(),
            "the fill must seal a segment for this to test the flush queue"
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
                .all(|s| s.load(Ordering::Acquire) == u32::MAX),
            "no bucket may keep a cached write segment across a reset"
        );
        assert!(
            layer.take_flush_queue().is_empty(),
            "a queued flush names a recycled segment and the offset the old one \
             owned -- submitting it would corrupt that extent"
        );
        assert_eq!(
            layer.buffer_pool.lock().unwrap().available(),
            buffers_total,
            "every write buffer must return to the pool, or repeated flushes \
             starve it"
        );

        for i in 0..30 {
            let key = format!("post{i:03}");
            layer
                .write_item_with_buffers(key.as_bytes(), &value, b"", ttl)
                .unwrap_or_else(|e| panic!("write {i} after reset failed: {e:?}"));
        }
    }

    /// `release_read` must complete the condemned handoff, and the staging
    /// buffer must come back only on the path that actually frees the
    /// segment (#129).
    ///
    /// Three things are pinned here, all of which used to be hand-rolled in
    /// this file and now ride on `segment::try_free_condemned`:
    ///
    /// - the evictor's `release_condemned` race fix declines while a
    ///   reference is live, so the segment is not recycled under a reader;
    /// - the buffer is *not* returned on that declining path -- a live reader
    ///   is still resolving `write_buffer_ptr()` into it;
    /// - the last reference out frees the segment and returns the buffer.
    ///
    /// Before this, `release_read` detached the buffer on `prev == 1` alone,
    /// without even checking the state.
    #[test]
    fn release_read_completes_the_handoff_and_returns_the_buffer_once() {
        let layer = test_layer();
        let hashtable = MultiChoiceHashtable::new(10);
        let (_keys, _locations, segment_id) = fill_one_segment(&layer, &hashtable);

        let segment = layer.pool.get(segment_id).expect("segment");
        assert!(
            segment.has_write_buffer(),
            "the item is still staged in RAM"
        );
        let buffers_free_before = layer.buffer_pool.lock().unwrap().available();

        // A reader pins the segment, the way a buffer read does.
        unsafe { (*segment.ref_count_ptr()).fetch_add(1, Ordering::SeqCst) };

        // The evictor seals, claims and condemns it.
        let state = segment.state();
        assert!(segment.cas_metadata(state, State::Sealed, None, None));
        assert!(segment.cas_metadata(State::Sealed, State::Draining, None, None));
        assert!(segment.cas_metadata(State::Draining, State::AwaitingRelease, None, None));

        // Its race fix must decline: the reader is still in.
        assert!(
            !segment.release_condemned(),
            "recycled a disk segment out from under a live reference"
        );
        assert_eq!(segment.state(), State::AwaitingRelease);
        assert!(
            segment.has_write_buffer(),
            "the staging buffer must not be returned while a reader is              resolving write_buffer_ptr() into it"
        );
        assert_eq!(
            layer.buffer_pool.lock().unwrap().available(),
            buffers_free_before
        );

        // The last reference out owes the free.
        layer.release_read(segment_id);
        assert_eq!(
            segment.state(),
            State::Free,
            "release_read must complete the handoff, not strand the segment"
        );
        assert!(!segment.has_write_buffer());
        assert_eq!(
            layer.buffer_pool.lock().unwrap().available(),
            buffers_free_before + 1,
            "the staging buffer returns to the pool exactly once"
        );
    }

    /// A RAM read that loses the race with the flush must hand its reference
    /// back (#130).
    ///
    /// `read_from_buffer` and `get_item` check `has_write_buffer()`, pin the
    /// segment, and only then resolve the pointer -- and `complete_flush`
    /// detaches the buffer, so it can land inside that window. Both used to
    /// resolve it with `?` and return with the pin still held. A segment that
    /// loses a reference that way never reads `ref_count() == 0` again, so the
    /// evictor's gate never opens for it: not evicted, not condemned, not
    /// recycled, for the life of the process.
    ///
    /// The window itself cannot be opened from a single thread -- it takes a
    /// concurrent detach -- so the proof is on `pin_for_buffer_read`, the one
    /// call both entry points now make for the pin and the resolve together.
    #[test]
    fn a_buffer_read_that_loses_the_flush_race_hands_its_reference_back() {
        let layer = test_layer();
        let hashtable = MultiChoiceHashtable::new(10);
        let (_keys, _locations, segment_id) = fill_one_segment(&layer, &hashtable);
        let segment = layer.pool.get(segment_id).expect("segment");

        // What the flush does when it lands in the window: the buffer goes
        // away while the segment is still live and perfectly readable.
        let buf = segment
            .detach_write_buffer()
            .expect("the item is staged in RAM");
        assert!(segment.state().is_readable());
        assert_eq!(segment.ref_count(), 0, "nothing holds this segment yet");

        assert!(
            layer.pin_for_buffer_read(segment).is_none(),
            "there is no buffer left to read out of"
        );
        assert_eq!(
            segment.ref_count(),
            0,
            "the backed-out read kept its reference -- this segment can never \
             satisfy the evictor's ref_count == 0 gate again"
        );

        layer.buffer_pool.lock().unwrap().release(buf);
    }

    /// A location from a previous incarnation must not resolve, even though
    /// the key really is at that offset.
    ///
    /// That is the whole hazard: segments are append-only from a fixed start,
    /// so the n-th item of the new incarnation lands where the n-th item of the
    /// old one was. The key compare alone says yes; only the tag says no.
    #[test]
    fn test_verifier_rejects_a_stale_incarnation() {
        let layer = test_layer();
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
            .write_item_with_buffers(b"key", b"value", b"", Duration::from_secs(3600))
            .expect("write");
        let layout = pool.layout();
        let (pool_id, segment_id, tag, offset) = live.unpack(layout);
        assert_ne!(tag, 0, "the segment must be past its first incarnation");

        // Same segment, same offset, previous tag.
        let stale = ItemLocation::new(layout, pool_id, segment_id, tag - 1, offset);
        let verifier = IoUringPoolVerifier { pool };

        assert!(
            verifier.verify(b"key", live.to_location(), false),
            "the current incarnation must resolve"
        );
        assert!(
            !verifier.verify(b"key", stale.to_location(), false),
            "a location from a previous incarnation must not resolve"
        );
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
        let layer = test_layer();
        assert_eq!(
            layer.pool.layout().align_bytes(),
            512,
            "disk pools align to a sector"
        );

        let hashtable = MultiChoiceHashtable::new(10);
        let (keys, _locations, segment_id) = fill_one_segment(&layer, &hashtable);

        layer.drain_segment_from_hashtable(segment_id, &hashtable);
        assert_all_removed(&layer, &hashtable, &keys);
    }

    /// The same for the unpinned path, which walks the segment itself rather
    /// than deferring to `drain_segment_from_hashtable`.
    #[test]
    fn test_evicting_a_segment_walks_every_item_at_the_disk_stride() {
        let layer = test_layer();
        let hashtable = MultiChoiceHashtable::new(10);
        let (keys, _locations, segment_id) = fill_one_segment(&layer, &hashtable);

        // The eviction path leaves its victim in `Draining`, which is the
        // state `process_evicted_segment` expects.
        let segment = layer.pool.get(segment_id).expect("segment");
        let state = segment.state();
        assert!(segment.cas_metadata(state, State::Draining, None, None));

        layer.process_evicted_segment(segment_id, &hashtable);
        assert_all_removed(&layer, &hashtable, &keys);
    }

    /// `pin_for_read` is a *value* read, so it must refuse `Draining`.
    ///
    /// `Draining` is exactly the state the two acquire predicates disagree
    /// about: it still admits key-verify readers, which is what lets the
    /// demoter resolve keys on a segment it is draining, and it refuses guard
    /// readers, because the evictor is about to rewrite the bytes. Widening
    /// this site to [`State::admits_verify_reader`] would let a RAM read hold
    /// a reference across the clear.
    ///
    /// Red proof: swap `State::admits_guard_reader` for
    /// `State::admits_verify_reader` in `pin_for_read`.
    #[test]
    fn pin_for_read_refuses_a_draining_segment() {
        let layer = test_layer();
        let hashtable = MultiChoiceHashtable::new(10);
        let (_keys, _locations, segment_id) = fill_one_segment(&layer, &hashtable);
        let segment = layer.pool.get(segment_id).expect("segment");

        let state = segment.state();
        assert!(segment.cas_metadata(state, State::Sealed, None, None));
        assert!(
            layer.pin_for_read(segment),
            "a sealed segment is still readable"
        );
        layer.release_segment_ref(segment);

        assert!(segment.cas_metadata(State::Sealed, State::Draining, None, None));
        assert!(
            !layer.pin_for_read(segment),
            "a draining segment is being processed for eviction -- only the \
             key-verify acquire may hold it, never a value read"
        );
        assert_eq!(segment.ref_count(), 0, "a refused pin leaks no reference");
    }

    /// A read that backs out of a *live* segment must leave its staging
    /// buffer alone.
    ///
    /// `free_condemned_returning_buffer` is reached from every `prev == 1`
    /// decrement, condemned or not -- `release_segment_ref` runs it on the
    /// back-outs in `read_from_buffer` and `get_item`, which happen on live
    /// segments the writer is still appending to. That is why the buffer
    /// return has to sit inside `try_free_condemned`'s `on_freed` hook and not
    /// in a line before the call: detaching unconditionally hands a live
    /// segment's staging buffer back to the pool, which then issues it to
    /// another segment while the first is still writing into it.
    ///
    /// Red proof, in production: move the detach out of `on_freed` to ahead of
    /// `try_free_condemned`.
    #[test]
    fn a_read_that_backs_out_of_a_live_segment_keeps_its_staging_buffer() {
        let layer = test_layer();
        let hashtable = MultiChoiceHashtable::new(10);
        let (_keys, _locations, segment_id) = fill_one_segment(&layer, &hashtable);
        let segment = layer.pool.get(segment_id).expect("segment");
        let buffers_free_before = layer.buffer_pool.lock().unwrap().available();

        assert!(
            layer.pin_for_read(segment),
            "a live segment admits a value read"
        );
        assert_eq!(segment.ref_count(), 1);
        layer.release_segment_ref(segment);

        assert_eq!(segment.ref_count(), 0, "the reference came back");
        assert_eq!(
            segment.state(),
            State::Live,
            "nothing condemned this segment"
        );
        assert!(
            segment.has_write_buffer(),
            "the last reader leaving a live segment took its staging buffer -- the \
             writer is still appending into that allocation"
        );
        assert_eq!(
            layer.buffer_pool.lock().unwrap().available(),
            buffers_free_before,
            "a live segment's buffer must not be handed back to the pool"
        );
    }

    /// Tests that drive a race by hand through `segment::interpose`.
    ///
    /// Every window in this file is only *entered* when the other side moves
    /// inside it, which no single-threaded test can arrange and no
    /// multi-threaded one can arrange deterministically. This tier has no
    /// coverage on Linux-only paths from macOS either, so the hook is the only
    /// route to these branches at all.
    ///
    /// Gated off under the model checkers for the same reason the hook itself
    /// is: a `std` thread-local inside a loom or shuttle execution is state the
    /// checker cannot see.
    #[cfg(all(not(feature = "loom"), not(feature = "shuttle")))]
    mod interposed {
        use super::*;
        use crate::segment::interpose;

        /// A read whose pin lands *inside* the condemn window must hand its
        /// reference back, and complete the free if it was the last one out.
        ///
        /// This is the post-increment re-check, and until this test it was the
        /// last deletable-and-green guard in the tree. The version this
        /// replaces condemned the segment *before* calling `pin_for_read`, so
        /// the pre-increment check answered and the re-check never ran -- a
        /// pre-check test wearing a post-check name, which is precisely how
        /// #134's mutation survived. The hook now condemns at
        /// `ACQUIRE_AFTER_INCREMENT`: the reference is already taken, the
        /// pre-check has already passed on a `Live` segment, and only the
        /// re-check can still see the condemn.
        ///
        /// Red proofs, both in production:
        ///
        /// - delete the post-increment re-check in `segment::try_acquire_pin`
        ///   -- the pin is granted on a condemned segment and never released;
        /// - back out with a bare `fetch_sub` instead of
        ///   `release_segment_ref` -- the condemner already declined the free
        ///   on seeing this reference, so the segment strands in
        ///   `AwaitingRelease` forever (#131).
        #[test]
        fn a_pin_that_loses_the_condemn_race_completes_the_free() {
            let layer = test_layer();
            let hashtable = MultiChoiceHashtable::new(10);
            let (_keys, _locations, segment_id) = fill_one_segment(&layer, &hashtable);
            let segment = layer.pool.get(segment_id).expect("segment");
            let buffers_free_before = layer.buffer_pool.lock().unwrap().available();
            assert_eq!(
                segment.state(),
                State::Live,
                "the pre-increment check must pass, or the window is never entered"
            );

            let condemned = std::rc::Rc::new(std::cell::Cell::new(false));
            {
                let flag = std::rc::Rc::clone(&condemned);
                let seg_ptr: *const DiskSegmentMeta = segment;
                let _hook = interpose::install(Box::new(move |phase| {
                    if phase == interpose::ACQUIRE_AFTER_INCREMENT && !flag.get() {
                        // SAFETY: `layer` owns the pool this segment lives in
                        // and outlives the hook guard, which is dropped at the
                        // end of this block.
                        let seg = unsafe { &*seg_ptr };
                        assert!(seg.cas_metadata(State::Live, State::Sealed, None, None));
                        assert!(seg.cas_metadata(State::Sealed, State::Draining, None, None));
                        assert!(seg.cas_metadata(
                            State::Draining,
                            State::AwaitingRelease,
                            None,
                            None
                        ));
                        flag.set(true);
                    }
                }));

                assert!(
                    !layer.pin_for_read(segment),
                    "the segment was condemned between the increment and the re-check; \
                     its hashtable entries are gone, so the read is a miss"
                );
            }

            assert!(condemned.get(), "the hook must have run");
            assert_eq!(
                segment.ref_count(),
                0,
                "the backed-out read kept its reference -- this segment can never \
                 satisfy the evictor's ref_count == 0 gate again"
            );
            assert_eq!(
                segment.state(),
                State::Free,
                "the last reference out owes the condemned segment its free"
            );
            assert_eq!(
                layer.buffer_pool.lock().unwrap().available(),
                buffers_free_before + 1,
                "the staging buffer rides back out with the free"
            );
        }

        /// The recycle must be gated on a count read taken *after* the
        /// exclusive claim, never before it (#133).
        ///
        /// The reader here arrives at the one instant that separates the two
        /// orders: after the evictor has decided to claim, before the claim is
        /// published. The segment is still `Draining`, which admits a
        /// key-verify reader on purpose, so the pin succeeds -- and a count
        /// read taken *earlier* cannot see it.
        ///
        /// Claim first and the count that follows does see the pin, so the
        /// evictor defers. Count first and the evictor believes the segment is
        /// unreferenced, takes `Locked`, rewrites the segment's bytes and
        /// hands it back to the pool while the reader is inside
        /// `verify_key_at_offset`.
        ///
        /// Red proof, in production: put the count back in front of the claim
        /// in `process_evicted_segment` --
        ///
        /// ```ignore
        /// let unpinned = segment.ref_count_seqcst() == 0;
        /// let claimed = crate::layer::try_claim_for_clear(segment);
        /// if !claimed || !unpinned {
        /// ```
        ///
        /// -- and the segment is recycled out from under the pin.
        #[test]
        fn eviction_does_not_recycle_a_segment_pinned_while_it_was_claiming() {
            let layer = test_layer();
            let hashtable = MultiChoiceHashtable::new(10);
            let (keys, locations, segment_id) = fill_one_segment(&layer, &hashtable);
            let segment = layer.pool.get(segment_id).expect("segment");
            let buffers_free_before = layer.buffer_pool.lock().unwrap().available();

            // The eviction path leaves its victim `Draining`, which is the
            // state `process_evicted_segment` expects.
            let state = segment.state();
            assert!(segment.cas_metadata(state, State::Draining, None, None));

            let pinned = std::rc::Rc::new(std::cell::Cell::new(false));
            {
                let flag = std::rc::Rc::clone(&pinned);
                let seg_ptr: *const DiskSegmentMeta = segment;
                let _hook = interpose::install(Box::new(move |phase| {
                    if phase == interpose::CLAIM_BEFORE_CAS && !flag.get() {
                        // SAFETY: `layer` outlives the hook guard.
                        let seg = unsafe { &*seg_ptr };
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

                layer.process_evicted_segment(segment_id, &hashtable);
            }

            assert!(pinned.get(), "the hook must have run");

            // The deferred arm owes the sweep: the segment is going away and
            // every location naming it has to come out of the hashtable first,
            // or the entries outlive the incarnation they point into.
            assert_none_indexed(&hashtable, &keys, &locations);

            assert_eq!(segment.ref_count(), 1, "the reader is still pinned");
            assert_ne!(
                segment.state(),
                State::Reserved,
                "the evictor recycled a segment while a reader was pinned -- its count \
                 was read before the claim, so the reader's arrival was invisible (#133)"
            );
            assert_ne!(
                segment.state(),
                State::Free,
                "the evictor published a pinned segment on the free queue (#133)"
            );
            assert_eq!(
                segment.state(),
                State::AwaitingRelease,
                "the evictor must defer to the last reference out instead"
            );
            assert!(
                segment.has_write_buffer(),
                "the staging buffer must not be returned while a reader may still \
                 resolve write_buffer_ptr() into it"
            );
            assert_eq!(
                layer.buffer_pool.lock().unwrap().available(),
                buffers_free_before,
                "nothing was freed, so nothing may go back to the buffer pool"
            );

            // And the deferral completes when the reader leaves, buffer and all.
            layer.release_read(segment_id);
            assert_eq!(
                segment.state(),
                State::Free,
                "the last reference out owes the AwaitingRelease -> Free handoff"
            );
            assert!(!segment.has_write_buffer());
            assert_eq!(
                layer.buffer_pool.lock().unwrap().available(),
                buffers_free_before + 1,
                "the staging buffer returns to the pool exactly once"
            );
        }

        /// The staging buffer must be back in the pool *before* the freed
        /// segment is published to the free queue.
        ///
        /// This is the ordering constraint no other condemn site in the tree
        /// has. Once the segment is on the free queue it is anybody's: another
        /// worker reserves it and stages a fresh buffer in it. A detach that
        /// runs after that point takes the *new* owner's buffer and hands it
        /// back to the pool, which will then issue it to a third segment while
        /// the second is still writing into it -- and the original buffer is
        /// leaked, because nothing ever detaches it.
        ///
        /// `free_condemned_returning_buffer` puts the release inside
        /// `segment::try_free_condemned`'s `on_freed` hook, which runs after
        /// the winning CAS and before the push, which is the only instant that
        /// is both exactly-once and still exclusive.
        ///
        /// Red proof, in production: move the release out of the hook to after
        /// the call --
        ///
        /// ```ignore
        /// let freed = unsafe { crate::segment::try_free_condemned(.., || {}) };
        /// if freed && let Some(buf) = segment.detach_write_buffer() {
        ///     self.buffer_pool.lock().unwrap().release(buf);
        /// }
        /// freed
        /// ```
        ///
        /// -- and the segment comes out of this test with no buffer at all.
        #[test]
        fn the_staging_buffer_is_returned_before_the_segment_is_published() {
            let layer = test_layer();
            let hashtable = MultiChoiceHashtable::new(10);
            let (_keys, _locations, segment_id) = fill_one_segment(&layer, &hashtable);
            let segment = layer.pool.get(segment_id).expect("segment");
            assert!(segment.has_write_buffer(), "the items are staged in RAM");

            // Empty the free queue, so the only id the racing reserve below
            // can pick up is the one this free is about to publish.
            let mut drained = Vec::new();
            while let Some(id) = layer.pool.reserve() {
                assert_ne!(id, segment_id, "this segment is not free yet");
                drained.push(id);
            }
            assert!(!drained.is_empty(), "the pool had other free segments");

            // A reader holds the segment while the evictor condemns it, so the
            // free is deferred to `release_read` below.
            unsafe { (*segment.ref_count_ptr()).fetch_add(1, Ordering::SeqCst) };
            let state = segment.state();
            assert!(segment.cas_metadata(state, State::Sealed, None, None));
            assert!(segment.cas_metadata(State::Sealed, State::Draining, None, None));
            assert!(segment.cas_metadata(State::Draining, State::AwaitingRelease, None, None));

            let buffers_free_before = layer.buffer_pool.lock().unwrap().available();

            // The instant the free queue accepts the segment, another worker
            // reserves it and stages a fresh buffer in it -- which is exactly
            // what a late detach would then hand back to the pool.
            let restaged = std::rc::Rc::new(std::cell::Cell::new(std::ptr::null::<u8>()));
            {
                let cell = std::rc::Rc::clone(&restaged);
                let layer_ptr: *const IoUringDiskLayer = &layer;
                let _hook = interpose::install(Box::new(move |phase| {
                    if phase == interpose::FREE_AFTER_PUBLISH && cell.get().is_null() {
                        // SAFETY: `layer` outlives the hook guard.
                        let layer = unsafe { &*layer_ptr };
                        let id = layer
                            .pool
                            .reserve()
                            .expect("the segment was just published to the free queue");
                        assert_eq!(id, segment_id, "the queue was drained to this one id");
                        let buf = layer
                            .buffer_pool
                            .lock()
                            .unwrap()
                            .allocate()
                            .expect("a spare staging buffer");
                        cell.set(buf.as_ptr());
                        layer
                            .pool
                            .get(id)
                            .expect("segment")
                            .attach_write_buffer(buf);
                    }
                }));

                // The last reference out frees the segment.
                layer.release_read(segment_id);
            }

            let fresh = restaged.get();
            assert!(!fresh.is_null(), "the hook must have run");
            assert!(
                segment.has_write_buffer(),
                "the free returned the buffer after publishing the segment, so it took \
                 the buffer its next owner had already staged"
            );
            assert_eq!(
                segment.write_buffer_ptr(),
                Some(fresh),
                "the segment must still hold the buffer its new owner attached"
            );
            assert_eq!(
                layer.buffer_pool.lock().unwrap().available(),
                buffers_free_before,
                "one buffer out to the new owner, one back from the free"
            );
        }
    }
}
