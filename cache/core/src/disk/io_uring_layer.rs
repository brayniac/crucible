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
        if let Some(segment) = self.pool.get(item_loc.segment_id()) {
            segment.verify_key_at_offset(item_loc.offset(), key, allow_deleted)
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

    /// Write buffer pool for staging segment data before flush.
    buffer_pool: Mutex<AlignedBufferPool>,

    /// Maximum read size for disk reads.
    ///
    /// When `prepare_read()` is called, this determines how many bytes
    /// to read from disk starting at the item's position. Must be large
    /// enough to cover the largest item (header + key + value + padding).
    /// Defaults to `segment_size`.
    max_read_size: u32,
}

impl IoUringDiskLayer {
    /// Create a new builder for IoUringDiskLayer.
    pub fn builder() -> IoUringDiskLayerBuilder {
        IoUringDiskLayerBuilder::new()
    }

    /// Get a reference to the segment pool.
    pub fn pool(&self) -> &IoUringPool {
        &self.pool
    }

    /// Get the TTL buckets.
    pub fn buckets(&self) -> &TtlBuckets {
        &self.buckets
    }

    /// Maximum bytes to read per disk read operation.
    ///
    /// This determines the `read_size` parameter for `prepare_read()`.
    /// The server must allocate read buffers at least this large
    /// (plus one block for alignment).
    pub fn max_read_size(&self) -> u32 {
        self.max_read_size
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
    ///
    /// The `buffer_pool` is used to allocate write buffers for new segments.
    pub fn write_item_with_buffers(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        ttl: Duration,
        buffer_pool: &mut AlignedBufferPool,
    ) -> CacheResult<ItemLocation> {
        if key.len() > BasicHeader::MAX_KEY_LEN {
            return Err(CacheError::KeyTooLong);
        }
        if optional.len() > BasicHeader::MAX_OPTIONAL_LEN {
            return Err(CacheError::OptionalTooLong);
        }

        loop {
            let segment_id = self.get_or_allocate_write_segment(ttl, buffer_pool)?;

            if let Some(segment) = self.pool.get(segment_id) {
                if let Some(offset) = segment.append_item(key, value, optional) {
                    return Ok(ItemLocation::new(self.pool.pool_id(), segment_id, offset));
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
            self.allocate_segment_for_bucket(bucket_index, bucket.ttl(), buffer_pool)?;
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

        let segment = self.pool.get(location.segment_id())?;

        if !segment.state().is_readable() {
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

        // Increment ref_count before reading
        let ref_count_ptr = segment.ref_count_ptr();
        unsafe { (*ref_count_ptr).fetch_add(1, Ordering::Acquire) };

        // Double-check state after increment
        if !segment.state().is_readable() {
            unsafe { (*ref_count_ptr).fetch_sub(1, Ordering::Release) };
            return None;
        }

        fence(Ordering::Acquire);

        let data_ptr = segment.write_buffer_ptr()?;
        let offset = location.offset();

        // Parse header
        if offset as usize + BasicHeader::SIZE > segment.capacity() {
            unsafe { (*ref_count_ptr).fetch_sub(1, Ordering::Release) };
            return None;
        }

        let header_bytes =
            unsafe { std::slice::from_raw_parts(data_ptr.add(offset as usize), BasicHeader::SIZE) };
        let header = BasicHeader::from_bytes(header_bytes);

        if header.is_deleted() {
            unsafe { (*ref_count_ptr).fetch_sub(1, Ordering::Release) };
            return None;
        }

        let item_size = header.padded_size();
        if offset as usize + item_size > segment.capacity() {
            unsafe { (*ref_count_ptr).fetch_sub(1, Ordering::Release) };
            return None;
        }

        // Verify key
        let key_start = offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + header.key_len() as usize;
        if key_end > segment.capacity() {
            unsafe { (*ref_count_ptr).fetch_sub(1, Ordering::Release) };
            return None;
        }

        let stored_key = unsafe {
            std::slice::from_raw_parts(data_ptr.add(key_start), header.key_len() as usize)
        };
        if stored_key != key {
            unsafe { (*ref_count_ptr).fetch_sub(1, Ordering::Release) };
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

        let segment = self.pool.get(location.segment_id())?;

        if !segment.state().is_readable() {
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

        // Increment ref_count to prevent eviction during async read
        let ref_count_ptr = segment.ref_count_ptr();
        unsafe { (*ref_count_ptr).fetch_add(1, Ordering::Acquire) };

        // Double-check state after increment
        if !segment.state().is_readable() {
            unsafe { (*ref_count_ptr).fetch_sub(1, Ordering::Release) };
            return None;
        }

        // Compute block-aligned read range
        let (disk_offset, read_len, item_offset) =
            self.pool
                .item_disk_range(location.segment_id(), location.offset(), read_size);

        Some(DiskReadParams {
            disk_offset,
            read_len,
            item_offset,
            segment_id: location.segment_id(),
            pool_id: self.pool.pool_id(),
        })
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
    /// from the segment and returns it to the internal buffer pool.
    pub fn complete_flush(&self, segment_id: u32) {
        if let Some(segment) = self.pool.get(segment_id)
            && let Some(buf) = segment.detach_write_buffer()
        {
            self.buffer_pool.lock().unwrap().release(buf);
        }
    }

    /// Release a segment's ref_count after a disk read completes.
    ///
    /// Called when the server handler finishes processing a disk read.
    pub fn release_read(&self, segment_id: u32) {
        if let Some(segment) = self.pool.get(segment_id) {
            let ref_count_ptr = segment.ref_count_ptr();
            let prev = unsafe { (*ref_count_ptr).fetch_sub(1, Ordering::Release) };

            // Check if this was the last reader and segment is condemned
            if prev == 1 {
                fence(Ordering::Acquire);
                segment.release_condemned();
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
    fn allocate_segment_for_bucket(
        &self,
        bucket_index: usize,
        ttl: Duration,
        buffer_pool: &mut AlignedBufferPool,
    ) -> CacheResult<u32> {
        let segment_id = self.pool.reserve().ok_or(CacheError::OutOfMemory)?;

        let segment = self.pool.get(segment_id).ok_or(CacheError::OutOfMemory)?;

        // Attach a write buffer
        let buf = buffer_pool.allocate().ok_or(CacheError::OutOfMemory)?;
        segment.attach_write_buffer(buf);

        // Set segment expiration time
        let expire_at = Self::now_secs() + ttl.as_secs() as u32;
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
                    buffer_pool.release(buf);
                }
                self.pool.release(segment_id);
                Err(CacheError::OutOfMemory)
            }
        }
    }

    /// Get or allocate the write segment for a TTL.
    fn get_or_allocate_write_segment(
        &self,
        ttl: Duration,
        buffer_pool: &mut AlignedBufferPool,
    ) -> CacheResult<u32> {
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
        self.allocate_segment_for_bucket(bucket_index, bucket.ttl(), buffer_pool)
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
            if let Some(data) = segment.data_slice(offset, BasicHeader::SIZE) {
                if let Some(header) = BasicHeader::try_from_bytes(data) {
                    let item_size = header.padded_size() as u32;

                    let key_start =
                        offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
                    let key_len = header.key_len() as usize;

                    if let Some(key) = segment.data_slice(key_start as u32, key_len)
                        && !header.is_deleted()
                    {
                        let location = ItemLocation::new(self.pool.pool_id(), segment_id, offset);
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

        if segment.ref_count() > 0 {
            self.drain_segment_from_hashtable(segment_id, hashtable);
            segment.cas_metadata(State::Draining, State::AwaitingRelease, None, None);

            // Re-check ref_count after CAS to handle race
            if segment.ref_count() == 0 {
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
                if let Some(data) = segment.data_slice(offset, BasicHeader::SIZE) {
                    if let Some(header) = BasicHeader::try_from_bytes(data) {
                        let item_size = header.padded_size() as u32;

                        let key_start =
                            offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
                        let key_len = header.key_len() as usize;

                        if let Some(key) = segment.data_slice(key_start as u32, key_len)
                            && !header.is_deleted()
                        {
                            let location =
                                ItemLocation::new(self.pool.pool_id(), segment_id, offset);

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
        let mut pool = self.buffer_pool.lock().unwrap();
        self.write_item_with_buffers(key, value, optional, ttl, &mut pool)
    }

    fn get_item(&self, location: ItemLocation, key: &[u8]) -> Option<Self::Guard<'_>> {
        // For IoUringDiskLayer, synchronous get is only possible from write buffer.
        // The full async path is handled by the server handler via prepare_read().
        if location.pool_id() != self.pool.pool_id() {
            return None;
        }

        let segment = self.pool.get(location.segment_id())?;
        if !segment.state().is_readable() || !segment.has_write_buffer() {
            return None;
        }

        let now = Self::now_secs();
        let expire_at = segment.expire_at();
        if expire_at > 0 && now as u64 >= expire_at as u64 {
            return None;
        }

        let header_info = segment.verify_key_unexpired(location.offset(), key, now)?;

        // Build a BasicItemGuard from the write buffer data
        let ref_count_ptr = segment.ref_count_ptr();
        unsafe { (*ref_count_ptr).fetch_add(1, Ordering::Acquire) };

        // Double-check state after increment
        if !segment.state().is_readable() {
            unsafe { (*ref_count_ptr).fetch_sub(1, Ordering::Release) };
            return None;
        }

        let data_ptr = segment.write_buffer_ptr()?;
        let offset = location.offset();

        let (key_len, optional_len, value_len) = header_info;

        let optional_start = offset as usize + BasicHeader::SIZE;
        let optional_end = optional_start + optional_len as usize;
        let key_start = optional_end;
        let key_end = key_start + key_len as usize;
        let value_start = key_end;
        let value_end = value_start + value_len as usize;

        if value_end > segment.capacity() {
            unsafe { (*ref_count_ptr).fetch_sub(1, Ordering::Release) };
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

        if let Some(segment) = self.pool.get(location.segment_id()) {
            // Can only mark deleted if write buffer is present
            if segment.has_write_buffer() {
                segment.mark_deleted_at_offset(location.offset());
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
    write_buffer_count: Option<usize>,
    max_read_size: Option<u32>,
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
            write_buffer_count: None,
            max_read_size: None,
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

    /// Set the number of write buffers.
    ///
    /// Defaults to `min(segment_count, 32).max(4)`.
    pub fn write_buffer_count(mut self, count: usize) -> Self {
        self.write_buffer_count = Some(count);
        self
    }

    /// Set the maximum read size for disk reads.
    ///
    /// This determines how many bytes are read from disk for each item.
    /// Must be large enough to cover the largest possible item
    /// (header + key + value + padding). Defaults to `segment_size`.
    ///
    /// The server must allocate read buffers at least `max_read_size + block_size`
    /// bytes to account for block alignment.
    pub fn max_read_size(mut self, size: u32) -> Self {
        self.max_read_size = Some(size);
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

        let write_buffer_count = self
            .write_buffer_count
            .unwrap_or_else(|| self.segment_count.clamp(4, 32));
        let buffer_pool =
            AlignedBufferPool::new(write_buffer_count, self.segment_size, self.block_size as usize);

        let max_read_size = self
            .max_read_size
            .unwrap_or(self.segment_size as u32);

        IoUringDiskLayer {
            layer_id: self.layer_id,
            config: self.config,
            pool,
            buckets,
            current_write_segments,
            flush_queue: Mutex::new(Vec::new()),
            buffer_pool: Mutex::new(buffer_pool),
            max_read_size,
        }
    }
}

impl Default for IoUringDiskLayerBuilder {
    fn default() -> Self {
        Self::new()
    }
}
