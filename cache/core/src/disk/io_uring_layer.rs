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
    fn pin_for_read(&self, segment: &DiskSegmentMeta) -> bool {
        // SeqCst, both halves -- the reader side of the Dekker pair with the
        // condemner's (CAS the state, load `ref_count`). Acquire/release lets
        // both loads come back stale and both sides proceed. See
        // `Segment::ref_count_seqcst` (#129).
        unsafe { (*segment.ref_count_ptr()).fetch_add(1, Ordering::SeqCst) };

        // Double-check state after increment
        let state_after = segment.state_seqcst();
        if !state_after.is_readable() || state_after.is_condemned() {
            self.release_segment_ref(segment);
            return false;
        }
        true
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
    fn free_condemned_returning_buffer(&self, segment: &DiskSegmentMeta) {
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
            );
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

        // `ref_count_seqcst`: this thread reached here through a
        // `Sealed -> Draining` CAS, the store half of the Dekker pair this
        // load completes -- see `Segment::ref_count_seqcst` (#129).
        if segment.ref_count_seqcst() > 0 {
            self.drain_segment_from_hashtable(segment_id, hashtable);
            segment.cas_metadata(State::Draining, State::AwaitingRelease, None, None);

            // Re-check ref_count after CAS to handle race. SeqCst so the load
            // is ordered after that CAS in the total order; otherwise it can
            // miss a decrement already published and the segment strands in
            // AwaitingRelease with ref_count == 0.
            if segment.ref_count_seqcst() == 0 {
                if let Some(buf) = segment.detach_write_buffer() {
                    self.buffer_pool.lock().unwrap().release(buf);
                }
                segment.release_condemned();
            }
            return;
        }

        segment.cas_metadata(State::Draining, State::Locked, None, None);

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

    /// Fill one segment with items and index them, returning keys + segment id.
    fn fill_one_segment(
        layer: &IoUringDiskLayer,
        hashtable: &MultiChoiceHashtable,
    ) -> (Vec<String>, u32) {
        let verifier = IoUringPoolVerifier { pool: &layer.pool };
        let ttl = Duration::from_secs(3600);

        const ITEMS: usize = 5;
        let mut keys = Vec::new();
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
        }
        (keys, segment_id.expect("at least one item"))
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
        let (_keys, segment_id) = fill_one_segment(&layer, &hashtable);

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

    /// A read whose pin lands after the condemner must hand its reference
    /// back, and complete the free if it was the last one out.
    ///
    /// This is the other back-out inside the shared pin, and the one the
    /// audit for #130 had to leave alone: it was already correct. It is worth
    /// a test all the same, because a bare `fetch_sub` here would strand the
    /// segment in `AwaitingRelease` forever -- the condemner already declined
    /// the free on seeing this reference, so nobody else is coming (#131).
    #[test]
    fn a_pin_that_loses_the_condemn_race_completes_the_free() {
        let layer = test_layer();
        let hashtable = MultiChoiceHashtable::new(10);
        let (_keys, segment_id) = fill_one_segment(&layer, &hashtable);
        let segment = layer.pool.get(segment_id).expect("segment");
        let buffers_free_before = layer.buffer_pool.lock().unwrap().available();

        // The condemner gets there first: sealed, drained, awaiting release.
        let state = segment.state();
        assert!(segment.cas_metadata(state, State::Sealed, None, None));
        assert!(segment.cas_metadata(State::Sealed, State::Draining, None, None));
        assert!(segment.cas_metadata(State::Draining, State::AwaitingRelease, None, None));

        // The reader's increment lands after that, so its re-check sees the
        // condemned state and backs out -- owing the segment its free.
        assert!(
            !layer.pin_for_read(segment),
            "a condemned segment's entries are gone; the read is a miss"
        );
        assert_eq!(
            segment.ref_count(),
            0,
            "the backed-out read kept its reference"
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
        let (_keys, segment_id) = fill_one_segment(&layer, &hashtable);
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
        let (keys, segment_id) = fill_one_segment(&layer, &hashtable);

        layer.drain_segment_from_hashtable(segment_id, &hashtable);
        assert_all_removed(&layer, &hashtable, &keys);
    }

    /// The same for the unpinned path, which walks the segment itself rather
    /// than deferring to `drain_segment_from_hashtable`.
    #[test]
    fn test_evicting_a_segment_walks_every_item_at_the_disk_stride() {
        let layer = test_layer();
        let hashtable = MultiChoiceHashtable::new(10);
        let (keys, segment_id) = fill_one_segment(&layer, &hashtable);

        // The eviction path leaves its victim in `Draining`, which is the
        // state `process_evicted_segment` expects.
        let segment = layer.pool.get(segment_id).expect("segment");
        let state = segment.state();
        assert!(segment.cas_metadata(state, State::Draining, None, None));

        layer.process_evicted_segment(segment_id, &hashtable);
        assert_all_removed(&layer, &hashtable, &keys);
    }
}
