//! In-RAM metadata for disk-backed segments.
//!
//! [`DiskSegmentMeta`] stores the same atomic state machine fields as
//! [`SliceSegment`], but the segment data lives on disk (or in a temporary
//! write buffer). This enables the io_uring disk layer to manage segment
//! lifecycle (state transitions, chain pointers, TTL) without keeping
//! full segment data in RAM.

use crate::error::CacheError;
use crate::item::BasicHeader;
use crate::segment::{Segment, SegmentKeyVerify};
use crate::state::{INVALID_SEGMENT_ID, Metadata, State};
use crate::sync::*;
use std::cell::UnsafeCell;
use std::time::Duration;

use super::aligned_buffer::AlignedBuffer;

/// In-RAM metadata for a segment whose data lives on disk.
///
/// Contains the full segment state machine (state, chain pointers, ref_count,
/// TTL, statistics) but does NOT own the segment data. Data access is only
/// available when a write buffer is attached (for recently-written segments
/// that haven't been flushed to disk yet).
///
/// # Write Buffer Lifecycle
///
/// 1. When a segment is reserved for writing, a write buffer is attached
/// 2. Items are written to the write buffer (synchronous, like RAM segments)
/// 3. When the segment is sealed, it's queued for flushing to disk
/// 4. After the io_uring write completes, the write buffer is detached
/// 5. Subsequent reads go to disk via io_uring
#[repr(C, align(64))]
pub struct DiskSegmentMeta {
    /// Packed metadata: [8 unused][8 state][24 prev][24 next]
    metadata: AtomicU64,

    /// Next write position in the segment.
    write_offset: AtomicU32,

    /// Count of non-deleted items.
    live_items: AtomicU32,

    /// Bytes used by non-deleted items.
    live_bytes: AtomicU32,

    /// Reference count for active readers.
    ref_count: AtomicU32,

    /// Segment ID within its pool.
    id: u32,

    /// Total data capacity in bytes.
    capacity: u32,

    /// Pool ID (0-3).
    pool_id: u8,

    /// Segment-level expiration time (coarse seconds since epoch).
    expire_at: AtomicU32,

    /// TTL bucket ID (0xFFFF = not in bucket).
    bucket_id: AtomicU16,

    /// Number of times this segment was a merge destination.
    merge_count: AtomicU16,

    /// Generation counter - incremented on reuse to prevent ABA.
    generation: AtomicU16,

    /// Byte offset of this segment's data on the disk device/file.
    disk_offset: u64,

    /// Pointer to the pool's main free queue for guard-based release.
    free_queue: *const crossbeam_deque::Injector<u32>,

    /// RAM write buffer: present while the segment is being written to
    /// or while a flush is in-flight. Once the flush completes, this is
    /// detached (set to None) and reads go to disk via io_uring.
    write_buffer: UnsafeCell<Option<AlignedBuffer>>,
}

// SAFETY: All mutable state is managed through atomics.
// The write_buffer is only accessed by a single writer thread at a time
// (the worker that owns this pool). UnsafeCell is needed for interior
// mutability but access is serialized by the caller.
unsafe impl Send for DiskSegmentMeta {}
unsafe impl Sync for DiskSegmentMeta {}

impl DiskSegmentMeta {
    const INVALID_BUCKET_ID: u16 = 0xFFFF;

    /// Create a new disk segment metadata entry.
    ///
    /// # Parameters
    /// - `pool_id`: Pool ID (0-3)
    /// - `id`: Segment ID within the pool
    /// - `capacity`: Segment data capacity in bytes
    /// - `disk_offset`: Byte offset on the disk device/file
    /// - `free_queue`: Pointer to the pool's free queue
    pub fn new(
        pool_id: u8,
        id: u32,
        capacity: u32,
        disk_offset: u64,
        free_queue: *const crossbeam_deque::Injector<u32>,
    ) -> Self {
        debug_assert!(pool_id <= 3, "pool_id {} exceeds 2-bit limit", pool_id);

        let initial_meta = Metadata::new(State::Free);

        Self {
            metadata: AtomicU64::new(initial_meta.pack()),
            write_offset: AtomicU32::new(0),
            live_items: AtomicU32::new(0),
            live_bytes: AtomicU32::new(0),
            ref_count: AtomicU32::new(0),
            id,
            capacity,
            pool_id,
            expire_at: AtomicU32::new(0),
            bucket_id: AtomicU16::new(Self::INVALID_BUCKET_ID),
            merge_count: AtomicU16::new(0),
            generation: AtomicU16::new(0),
            disk_offset,
            free_queue,
            write_buffer: UnsafeCell::new(None),
        }
    }

    /// Get the byte offset of this segment on the disk device/file.
    #[inline]
    pub fn disk_offset(&self) -> u64 {
        self.disk_offset
    }

    /// Check if a write buffer is currently attached.
    #[inline]
    pub fn has_write_buffer(&self) -> bool {
        // SAFETY: Read-only check, caller serializes mutations.
        unsafe { (*self.write_buffer.get()).is_some() }
    }

    /// Attach a write buffer to this segment.
    ///
    /// # Safety
    ///
    /// Must be called from the owning worker thread only.
    pub fn attach_write_buffer(&self, buf: AlignedBuffer) {
        unsafe {
            debug_assert!(
                (*self.write_buffer.get()).is_none(),
                "segment {} already has a write buffer",
                self.id
            );
            *self.write_buffer.get() = Some(buf);
        }
    }

    /// Detach and return the write buffer.
    ///
    /// # Safety
    ///
    /// Must be called from the owning worker thread only.
    pub fn detach_write_buffer(&self) -> Option<AlignedBuffer> {
        unsafe { (*self.write_buffer.get()).take() }
    }

    /// Get a pointer to the write buffer data (if present).
    ///
    /// Returns `None` if no write buffer is attached.
    pub fn write_buffer_ptr(&self) -> Option<*const u8> {
        unsafe { (*self.write_buffer.get()).as_ref().map(|buf| buf.as_ptr()) }
    }

    /// Get a mutable pointer to the write buffer data (if present).
    ///
    /// Returns `None` if no write buffer is attached.
    pub fn write_buffer_mut_ptr(&self) -> Option<*mut u8> {
        unsafe {
            (*self.write_buffer.get())
                .as_mut()
                .map(|buf| buf.as_mut_ptr())
        }
    }

    /// Get the pointer to the metadata atomic for ValueRef construction.
    #[inline]
    pub fn metadata_ptr(&self) -> *const AtomicU64 {
        &self.metadata as *const AtomicU64
    }

    /// Get the pointer to the ref_count atomic for ValueRef construction.
    #[inline]
    pub fn ref_count_ptr(&self) -> *const AtomicU32 {
        &self.ref_count as *const AtomicU32
    }

    /// Get the pointer to the free queue for ValueRef construction.
    #[inline]
    pub fn free_queue_ptr(&self) -> *const crossbeam_deque::Injector<u32> {
        self.free_queue
    }
}

impl SegmentKeyVerify for DiskSegmentMeta {
    fn verify_key_at_offset(&self, offset: u32, key: &[u8], allow_deleted: bool) -> bool {
        // If write buffer is detached (segment flushed to disk), we can't verify
        // the key synchronously. Return true (optimistic match) — the actual key
        // will be verified after the io_uring disk read completes.
        let Some(data_ptr) = self.write_buffer_ptr() else {
            return self.state().is_readable();
        };

        if offset as usize + BasicHeader::SIZE > self.capacity as usize {
            return false;
        }

        let header_bytes =
            unsafe { std::slice::from_raw_parts(data_ptr.add(offset as usize), BasicHeader::SIZE) };

        let header = BasicHeader::from_bytes(header_bytes);

        if !allow_deleted && header.is_deleted() {
            return false;
        }

        let key_start = offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + header.key_len() as usize;
        if key_end > self.capacity as usize {
            return false;
        }

        let stored_key = unsafe { std::slice::from_raw_parts(data_ptr.add(key_start), key.len()) };
        stored_key == key
    }

    fn verify_key_with_header(
        &self,
        offset: u32,
        key: &[u8],
        allow_deleted: bool,
    ) -> Option<(u8, u8, u32)> {
        let data_ptr = self.write_buffer_ptr()?;

        if offset as usize + BasicHeader::SIZE > self.capacity as usize {
            return None;
        }

        let header_bytes =
            unsafe { std::slice::from_raw_parts(data_ptr.add(offset as usize), BasicHeader::SIZE) };

        let header = BasicHeader::from_bytes(header_bytes);

        if !allow_deleted && header.is_deleted() {
            return None;
        }

        let key_start = offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + header.key_len() as usize;
        if key_end > self.capacity as usize {
            return None;
        }

        let stored_key = unsafe { std::slice::from_raw_parts(data_ptr.add(key_start), key.len()) };
        if stored_key != key {
            return None;
        }

        Some((header.key_len(), header.optional_len(), header.value_len()))
    }

    fn verify_key_unexpired(&self, offset: u32, key: &[u8], now: u32) -> Option<(u8, u8, u32)> {
        // Check segment-level expiration first
        let expire_at = self.expire_at.load(Ordering::Acquire);
        if expire_at > 0 && now as u64 >= expire_at as u64 {
            return None;
        }

        self.verify_key_with_header(offset, key, false)
    }
}

impl Segment for DiskSegmentMeta {
    #[inline]
    fn id(&self) -> u32 {
        self.id
    }

    #[inline]
    fn pool_id(&self) -> u8 {
        self.pool_id
    }

    #[inline]
    fn generation(&self) -> u16 {
        self.generation.load(Ordering::Acquire)
    }

    fn increment_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.capacity as usize
    }

    #[inline]
    fn write_offset(&self) -> u32 {
        self.write_offset.load(Ordering::Acquire)
    }

    #[inline]
    fn live_items(&self) -> u32 {
        self.live_items.load(Ordering::Relaxed)
    }

    #[inline]
    fn live_bytes(&self) -> u32 {
        self.live_bytes.load(Ordering::Relaxed)
    }

    #[inline]
    fn ref_count(&self) -> u32 {
        self.ref_count.load(Ordering::Acquire)
    }

    fn state(&self) -> State {
        let packed = self.metadata.load(Ordering::Acquire);
        Metadata::unpack(packed).state
    }

    fn try_reserve(&self) -> bool {
        let packed = self.metadata.load(Ordering::Acquire);
        let meta = Metadata::unpack(packed);
        if meta.state != State::Free {
            return false;
        }

        let new_meta = Metadata::new(State::Reserved);
        self.metadata
            .compare_exchange(packed, new_meta.pack(), Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
            .then(|| {
                // Reset statistics for reuse
                self.write_offset.store(0, Ordering::Release);
                self.live_items.store(0, Ordering::Relaxed);
                self.live_bytes.store(0, Ordering::Relaxed);
                self.ref_count.store(0, Ordering::Relaxed);
                self.expire_at.store(0, Ordering::Relaxed);
                self.merge_count.store(0, Ordering::Relaxed);
                self.increment_generation();
            })
            .is_some()
    }

    fn try_release(&self) -> bool {
        let packed = self.metadata.load(Ordering::Acquire);
        let meta = Metadata::unpack(packed);
        match meta.state {
            State::Free => false, // Already free (idempotent)
            State::Reserved | State::Linking | State::Locked => {
                let new_meta = Metadata::new(State::Free);
                self.metadata
                    .compare_exchange(packed, new_meta.pack(), Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            }
            other => panic!("try_release called in invalid state: {:?}", other),
        }
    }

    fn cas_metadata(
        &self,
        expected_state: State,
        new_state: State,
        new_next: Option<u32>,
        new_prev: Option<u32>,
    ) -> bool {
        let packed = self.metadata.load(Ordering::Acquire);
        let meta = Metadata::unpack(packed);

        if meta.state != expected_state {
            return false;
        }

        let next = new_next.unwrap_or(meta.next);
        let prev = new_prev.unwrap_or(meta.prev);

        let new_meta = Metadata {
            next,
            prev,
            state: new_state,
        };

        self.metadata
            .compare_exchange(packed, new_meta.pack(), Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    fn release_condemned(&self) -> bool {
        let packed = self.metadata.load(Ordering::Acquire);
        let meta = Metadata::unpack(packed);

        if meta.state != State::AwaitingRelease {
            return false;
        }

        if self.ref_count.load(Ordering::Acquire) != 0 {
            return false;
        }

        let new_meta = Metadata::new(State::Free);
        if self
            .metadata
            .compare_exchange(packed, new_meta.pack(), Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            // Push back to free queue
            unsafe {
                (*self.free_queue).push(self.id);
            }
            true
        } else {
            false
        }
    }

    fn next(&self) -> Option<u32> {
        let packed = self.metadata.load(Ordering::Acquire);
        let meta = Metadata::unpack(packed);
        if meta.next == INVALID_SEGMENT_ID {
            None
        } else {
            Some(meta.next)
        }
    }

    fn prev(&self) -> Option<u32> {
        let packed = self.metadata.load(Ordering::Acquire);
        let meta = Metadata::unpack(packed);
        if meta.prev == INVALID_SEGMENT_ID {
            None
        } else {
            Some(meta.prev)
        }
    }

    #[inline]
    fn expire_at(&self) -> u32 {
        self.expire_at.load(Ordering::Acquire)
    }

    fn set_expire_at(&self, expire_at: u32) {
        self.expire_at.store(expire_at, Ordering::Release);
    }

    fn item_ttl(&self, _offset: u32, now: u32) -> Option<Duration> {
        // Disk segments use segment-level TTL only
        self.segment_ttl(now)
    }

    fn bucket_id(&self) -> Option<u16> {
        let id = self.bucket_id.load(Ordering::Acquire);
        if id == Self::INVALID_BUCKET_ID {
            None
        } else {
            Some(id)
        }
    }

    fn set_bucket_id(&self, bucket_id: u16) {
        self.bucket_id.store(bucket_id, Ordering::Release);
    }

    fn clear_bucket_id(&self) {
        self.bucket_id
            .store(Self::INVALID_BUCKET_ID, Ordering::Release);
    }

    fn data_slice(&self, offset: u32, len: usize) -> Option<&[u8]> {
        // Only available when write buffer is present
        let data_ptr = self.write_buffer_ptr()?;

        if offset as usize + len > self.capacity as usize {
            return None;
        }

        Some(unsafe { std::slice::from_raw_parts(data_ptr.add(offset as usize), len) })
    }

    fn append_item(&self, key: &[u8], value: &[u8], optional: &[u8]) -> Option<u32> {
        let data_ptr = self.write_buffer_mut_ptr()?;

        let header = BasicHeader::new(key.len() as u8, optional.len() as u8, value.len() as u32);
        let header_size = BasicHeader::SIZE;

        let item_size = header_size + optional.len() + key.len() + value.len();
        let padded_size = (item_size + 7) & !7; // 8-byte alignment

        // Reserve space atomically
        let offset = self.reserve_space(padded_size as u32)?;

        // Write the item to the write buffer
        unsafe {
            let mut ptr = data_ptr.add(offset as usize);

            // Write header
            let header_buf = std::slice::from_raw_parts_mut(ptr, header_size);
            header.to_bytes(header_buf);
            ptr = ptr.add(header_size);

            // Write optional
            if !optional.is_empty() {
                std::ptr::copy_nonoverlapping(optional.as_ptr(), ptr, optional.len());
                ptr = ptr.add(optional.len());
            }

            // Write key
            std::ptr::copy_nonoverlapping(key.as_ptr(), ptr, key.len());
            ptr = ptr.add(key.len());

            // Write value
            if !value.is_empty() {
                std::ptr::copy_nonoverlapping(value.as_ptr(), ptr, value.len());
            }
        }

        fence(Ordering::Release);

        self.live_items.fetch_add(1, Ordering::Relaxed);
        self.live_bytes
            .fetch_add(padded_size as u32, Ordering::Relaxed);

        Some(offset)
    }

    fn append_item_with_ttl(
        &self,
        _key: &[u8],
        _value: &[u8],
        _optional: &[u8],
        _expire_at: u32,
    ) -> Option<u32> {
        // Disk segments use segment-level TTL, not per-item
        None
    }

    fn begin_append_with_ttl(
        &self,
        _key: &[u8],
        _value_len: usize,
        _optional: &[u8],
        _expire_at: u32,
    ) -> Option<(u32, u32, *mut u8)> {
        // Disk segments use segment-level TTL, not per-item
        None
    }

    fn begin_append(
        &self,
        key: &[u8],
        value_len: usize,
        optional: &[u8],
    ) -> Option<(u32, u32, *mut u8)> {
        let data_ptr = self.write_buffer_mut_ptr()?;

        let header = BasicHeader::new(key.len() as u8, optional.len() as u8, value_len as u32);
        let header_size = BasicHeader::SIZE;

        let item_size = header_size + optional.len() + key.len() + value_len;
        let padded_size = (item_size + 7) & !7;

        let offset = self.reserve_space(padded_size as u32)?;

        unsafe {
            let mut ptr = data_ptr.add(offset as usize);

            // Write header
            let header_buf = std::slice::from_raw_parts_mut(ptr, header_size);
            header.to_bytes(header_buf);
            ptr = ptr.add(header_size);

            // Write optional
            if !optional.is_empty() {
                std::ptr::copy_nonoverlapping(optional.as_ptr(), ptr, optional.len());
                ptr = ptr.add(optional.len());
            }

            // Write key
            std::ptr::copy_nonoverlapping(key.as_ptr(), ptr, key.len());
            ptr = ptr.add(key.len());

            // Return pointer to value area
            Some((offset, padded_size as u32, ptr))
        }
    }

    fn finalize_append(&self, item_size: u32) {
        fence(Ordering::Release);
        self.live_items.fetch_add(1, Ordering::Relaxed);
        self.live_bytes.fetch_add(item_size, Ordering::Relaxed);
    }

    fn mark_deleted_at_offset(&self, offset: u32) {
        let Some(data_ptr) = self.write_buffer_mut_ptr() else {
            return;
        };

        if offset as usize + BasicHeader::SIZE > self.capacity as usize {
            return;
        }

        // Set deleted flag in the header
        unsafe {
            let flags_ptr = data_ptr.add(offset as usize + 1);
            let flags = *flags_ptr;
            *flags_ptr = flags | 0x40; // Set is_deleted bit
        }
    }

    fn mark_deleted(&self, offset: u32, key: &[u8]) -> Result<bool, CacheError> {
        let Some(data_ptr) = self.write_buffer_mut_ptr() else {
            return Err(CacheError::SegmentNotAccessible);
        };

        if offset as usize + BasicHeader::SIZE > self.capacity as usize {
            return Err(CacheError::InvalidOffset);
        }

        let header_bytes =
            unsafe { std::slice::from_raw_parts(data_ptr.add(offset as usize), BasicHeader::SIZE) };
        let header = BasicHeader::from_bytes(header_bytes);

        if header.is_deleted() {
            return Ok(false); // Already deleted
        }

        // Verify key
        let key_start = offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + header.key_len() as usize;
        if key_end > self.capacity as usize {
            return Err(CacheError::InvalidOffset);
        }

        let stored_key = unsafe { std::slice::from_raw_parts(data_ptr.add(key_start), key.len()) };
        if stored_key != key {
            return Err(CacheError::KeyMismatch);
        }

        // Set deleted flag
        unsafe {
            let flags_ptr = data_ptr.add(offset as usize + 1);
            let flags = *flags_ptr;
            *flags_ptr = flags | 0x40;
        }

        let padded_size = header.padded_size() as u32;
        self.live_items.fetch_sub(1, Ordering::Relaxed);
        self.live_bytes.fetch_sub(padded_size, Ordering::Relaxed);

        Ok(true)
    }

    #[inline]
    fn merge_count(&self) -> u16 {
        self.merge_count.load(Ordering::Relaxed)
    }

    fn increment_merge_count(&self) {
        self.merge_count.fetch_add(1, Ordering::Relaxed);
    }

    fn reset(&self) {
        self.write_offset.store(0, Ordering::Release);
        self.live_items.store(0, Ordering::Relaxed);
        self.live_bytes.store(0, Ordering::Relaxed);
        self.expire_at.store(0, Ordering::Relaxed);
        self.bucket_id
            .store(Self::INVALID_BUCKET_ID, Ordering::Release);
        self.merge_count.store(0, Ordering::Relaxed);

        let new_meta = Metadata::new(State::Free);
        self.metadata.store(new_meta.pack(), Ordering::Release);
    }
}

impl DiskSegmentMeta {
    /// Atomically reserve space and return the start offset.
    fn reserve_space(&self, size: u32) -> Option<u32> {
        let mut attempts = 0u32;

        loop {
            let current = self.write_offset.load(Ordering::Acquire);
            let new_offset = current.checked_add(size)?;

            if new_offset > self.capacity {
                return None;
            }

            match self.write_offset.compare_exchange(
                current,
                new_offset,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(current),
                Err(_) => {
                    attempts += 1;
                    if attempts >= 16 {
                        return None;
                    }
                    crate::sync::spin_loop();
                }
            }
        }
    }
}
