//! In-RAM metadata for disk-backed segments.
//!
//! [`DiskSegmentMeta`] stores the same atomic state machine fields as
//! [`SliceSegment`], but the segment data lives on disk (or in a temporary
//! write buffer). This enables the io_uring disk layer to manage segment
//! lifecycle (state transitions, chain pointers, TTL) without keeping
//! full segment data in RAM.

use crate::disk::AlignedBuffer;
use crate::error::CacheError;
use crate::item::BasicHeader;
use crate::segment::{Segment, SegmentKeyVerify};
use crate::state::{INVALID_SEGMENT_ID, Metadata, State};
use crate::sync::*;
use std::cell::UnsafeCell;
use std::time::Duration;

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
    /// Packed metadata: [2 unused][6 incarnation][8 state][24 prev][24 next]
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

    /// `log2` of the owning pool's offset alignment factor.
    ///
    /// Appends stride by `1 << align_shift`, because a `Location` stores
    /// `offset / align_bytes` and cannot name a finer offset.
    align_shift: u8,

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
        align_bytes: usize,
    ) -> Self {
        debug_assert!(pool_id <= 3, "pool_id {} exceeds 2-bit limit", pool_id);
        debug_assert!(
            align_bytes.is_power_of_two() && align_bytes >= 8,
            "align_bytes {align_bytes} must be a power of two and at least 8"
        );

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
            align_shift: align_bytes.trailing_zeros() as u8,
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

    /// Get the capacity of the attached write buffer (if present).
    pub fn write_buffer_capacity(&self) -> Option<usize> {
        unsafe {
            (*self.write_buffer.get())
                .as_ref()
                .map(|buf| buf.capacity())
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
        // When write buffer has been flushed to disk, we can't verify the key
        // in RAM. Trust the hashtable tag match — the server will verify the
        // actual key after completing the async disk read.
        let Some(data_ptr) = self.write_buffer_ptr() else {
            return self.state().is_readable();
        };

        if offset as usize + BasicHeader::SIZE > self.capacity as usize {
            return false;
        }

        let header = unsafe { BasicHeader::from_ptr(data_ptr.add(offset as usize)) };

        if !allow_deleted && header.is_deleted() {
            return false;
        }

        // The stored key must be the same LENGTH before its bytes are worth
        // comparing. The bounds check below is computed from
        // `header.key_len()` but the slice is built with the caller's
        // `key.len()`, so without this the two can disagree: a longer caller
        // key reads past the bound that was checked, and a shorter one
        // compares against a prefix and reports a match for a key this
        // segment does not hold. `SliceSegment` has always had this guard.
        if header.key_len() as usize != key.len() {
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

        let header = unsafe { BasicHeader::from_ptr(data_ptr.add(offset as usize)) };

        if !allow_deleted && header.is_deleted() {
            return None;
        }

        // The stored key must be the same LENGTH before its bytes are worth
        // comparing. The bounds check below is computed from
        // `header.key_len()` but the slice is built with the caller's
        // `key.len()`, so without this the two can disagree: a longer caller
        // key reads past the bound that was checked, and a shorter one
        // compares against a prefix and reports a match for a key this
        // segment does not hold. `SliceSegment` has always had this guard.
        if header.key_len() as usize != key.len() {
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
    fn incarnation(&self) -> u8 {
        Metadata::unpack(self.metadata.load(Ordering::Acquire)).incarnation
    }

    #[inline]
    fn align_bytes(&self) -> u32 {
        1 << self.align_shift
    }

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

        // Preserve the incarnation: reserving does not end one, and zeroing
        // here would wipe the tag the release that freed this segment set.
        let new_meta = meta
            .with_state(State::Reserved)
            .with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID);
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
                // Leaving `Locked` ends a used incarnation: the segment has
                // been drained and cleared. `Reserved` and `Linking` must NOT
                // bump -- `FilePool::release` returns never-used segments
                // through here, and bumping there would advance a 6-bit tag at
                // a rate decoupled from segment lifecycles.
                let ends_incarnation = meta.state == State::Locked;

                let new_meta = meta
                    .with_state(State::Free)
                    .with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID);
                let new_meta = if ends_incarnation {
                    new_meta.bump_incarnation()
                } else {
                    new_meta
                };
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

        let new_meta = meta.with_state(new_state).with_chain_ids(next, prev);

        // *Leaving* `Locked` ends a used incarnation -- not merely being
        // `Locked`. The layers recycle a drained segment as
        // `Locked -> Reserved` before releasing it `Reserved -> Free`. Bumping
        // inside the same CAS that publishes the new state means no thread can
        // observe the new state paired with the old tag.
        //
        // The `new_state != Locked` half is load-bearing: a chain-pointer-only
        // rewrite that stays in `Locked` is mid-life, and eleven identity CASes
        // in `organization/` take exactly that shape. Keying on the source
        // state alone would advance the tag twice in one lifetime, halving the
        // space the collision argument rests on. Every other transition is
        // mid-life and preserves the tag.
        let new_meta = if meta.state == State::Locked && new_state != State::Locked {
            new_meta.bump_incarnation()
        } else {
            new_meta
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

        // `AwaitingRelease -> Free` is unconditionally the end of a used
        // incarnation: the segment was condemned while live and its last reader
        // has just dropped. Always bump.
        let new_meta = meta
            .with_state(State::Free)
            .with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID)
            .bump_incarnation();
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

    fn header_ptr(&self, offset: u32, len: usize) -> Option<*const u8> {
        // Only available when write buffer is present
        let data_ptr = self.write_buffer_ptr()?;

        if offset as usize + len > self.capacity as usize {
            return None;
        }

        Some(unsafe { data_ptr.add(offset as usize) })
    }

    fn append_item(&self, key: &[u8], value: &[u8], optional: &[u8]) -> Option<u32> {
        let data_ptr = self.write_buffer_mut_ptr()?;

        let header = BasicHeader::new(key.len() as u8, optional.len() as u8, value.len() as u32);
        let header_size = BasicHeader::SIZE;

        let item_size = header_size + optional.len() + key.len() + value.len();
        // The pool's stride, not a hardcoded 8: every scan advances by the same
        // quantity, and a `Location` cannot name a finer offset.
        let padded_size = self.item_stride(item_size) as usize;

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
        let padded_size = self.item_stride(item_size) as usize;

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

        // Set the deleted flag atomically. This byte is read concurrently by
        // every header decode (`item::read_flags`), so a plain
        // read-modify-write here is a data race with live readers.
        let flags_ptr = unsafe { data_ptr.add(offset as usize + 1) };

        #[cfg(not(feature = "loom"))]
        {
            // SAFETY: `flags_ptr` addresses the header's flags byte, which is
            // in bounds (checked above). `AtomicU8` matches `u8` in layout and
            // alignment, and every other writer of this byte uses the same
            // atomic view.
            let flags_atomic = unsafe { &*(flags_ptr as *const AtomicU8) };
            flags_atomic.fetch_or(0x40, Ordering::Release);
        }

        #[cfg(feature = "loom")]
        {
            // loom does not re-export `AtomicU8` and its atomics cannot be
            // built from a raw pointer, so the loom build pairs volatile
            // access with the volatile readers, as `SliceSegment` does.
            fence(Ordering::Release);
            unsafe {
                let old = std::ptr::read_volatile(flags_ptr);
                std::ptr::write_volatile(flags_ptr, old | 0x40);
            }
        }
    }

    fn mark_deleted(&self, offset: u32, key: &[u8]) -> Result<bool, CacheError> {
        let Some(data_ptr) = self.write_buffer_mut_ptr() else {
            return Err(CacheError::SegmentNotAccessible);
        };

        if offset as usize + BasicHeader::SIZE > self.capacity as usize {
            return Err(CacheError::InvalidOffset);
        }

        let header = unsafe { BasicHeader::from_ptr(data_ptr.add(offset as usize)) };

        // NOTE: the is_deleted check that used to live here has moved below
        // the flag write. Checking first and acting later let two concurrent
        // deletes of the same item both observe "live" and both decrement the
        // live counters, wrapping `live_items` past zero.

        // Verify key. Length first: the bounds check below is computed from
        // `header.key_len()` while the slice is built with the caller's
        // `key.len()`, so without this a shorter key matches a prefix of a
        // longer stored one and this tombstones an item the caller never
        // named.
        if header.key_len() as usize != key.len() {
            return Err(CacheError::KeyMismatch);
        }

        let key_start = offset as usize + BasicHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + header.key_len() as usize;
        if key_end > self.capacity as usize {
            return Err(CacheError::InvalidOffset);
        }

        let stored_key = unsafe { std::slice::from_raw_parts(data_ptr.add(key_start), key.len()) };
        if stored_key != key {
            return Err(CacheError::KeyMismatch);
        }

        // Set the deleted flag atomically and let the returned value decide
        // who owns the delete. The flag byte is read concurrently by every
        // header decode, so a plain read-modify-write here both races those
        // readers and loses the race between two deleters: the winner is
        // whoever observes the bit still clear, and only that caller may
        // adjust the live counters.
        let flags_ptr = unsafe { data_ptr.add(offset as usize + 1) };

        #[cfg(not(feature = "loom"))]
        let old_flags = {
            // SAFETY: as in `mark_deleted_at_offset` above.
            let flags_atomic = unsafe { &*(flags_ptr as *const AtomicU8) };
            flags_atomic.fetch_or(0x40, Ordering::Release)
        };

        #[cfg(feature = "loom")]
        let old_flags = {
            fence(Ordering::Release);
            unsafe {
                let old = std::ptr::read_volatile(flags_ptr);
                std::ptr::write_volatile(flags_ptr, old | 0x40);
                old
            }
        };

        if (old_flags & 0x40) != 0 {
            return Ok(false); // Someone else marked it deleted first.
        }

        // Must be the stride the append charged to `live_bytes`, not the
        // 8-padded body size, or the accounting drifts on a coarser pool.
        let padded_size = self.item_stride(header.padded_size());
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

        // Stores unconditionally, so load first to advance the incarnation
        // rather than clobber it -- exactly as `SliceSegment::force_free` does.
        // A bulk reset recycles the segment, so locations issued before it must
        // stop resolving.
        let new_meta = Metadata::unpack(self.metadata.load(Ordering::Acquire))
            .with_state(State::Free)
            .with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID)
            .bump_incarnation();
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

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::disk::aligned_buffer::AlignedBufferPool;

    /// Build a segment holding one item: `key` then `value`, at offset 0.
    ///
    /// Returns the pool and injector alongside it because both must outlive
    /// the segment — the segment holds a raw pointer to the injector.
    fn segment_with_item(
        key: &[u8],
        value: &[u8],
    ) -> (
        DiskSegmentMeta,
        AlignedBufferPool,
        Box<crossbeam_deque::Injector<u32>>,
    ) {
        let capacity = 4096usize;
        let mut pool = AlignedBufferPool::new(1, capacity, 512);
        let buf = pool.allocate().expect("one slot must be available");
        let injector = Box::new(crossbeam_deque::Injector::new());

        let seg = DiskSegmentMeta::new(0, 0, capacity as u32, 0, &*injector, 512);
        seg.attach_write_buffer(buf);

        let header = BasicHeader::new(key.len() as u8, 0, value.len() as u32);
        let ptr = seg.write_buffer_mut_ptr().expect("write buffer attached");
        // SAFETY: the buffer is `capacity` bytes and the item fits well inside.
        unsafe {
            let all = std::slice::from_raw_parts_mut(ptr, capacity);
            all.fill(0);
            header.to_bytes(&mut all[..BasicHeader::SIZE]);
            all[BasicHeader::SIZE..BasicHeader::SIZE + key.len()].copy_from_slice(key);
            all[BasicHeader::SIZE + key.len()..BasicHeader::SIZE + key.len() + value.len()]
                .copy_from_slice(value);
        }

        (seg, pool, injector)
    }

    /// Build an empty segment with a given capacity and offset alignment.
    ///
    /// Separate from `segment_with_item` because these tests append through the
    /// real path rather than writing a header by hand.
    fn segment_with_alignment(
        capacity: usize,
        align: usize,
    ) -> (
        DiskSegmentMeta,
        AlignedBufferPool,
        Box<crossbeam_deque::Injector<u32>>,
    ) {
        let mut pool = AlignedBufferPool::new(1, capacity, 512);
        let buf = pool.allocate().expect("one slot must be available");
        let injector = Box::new(crossbeam_deque::Injector::new());

        let seg = DiskSegmentMeta::new(0, 0, capacity as u32, 0, &*injector, align);
        seg.attach_write_buffer(buf);
        (seg, pool, injector)
    }

    /// Appends must stride by the pool's alignment, and a scan must agree.
    ///
    /// If appends pad to 512 and scans still advance by `header.padded_size()`
    /// (which rounds to 8), the walk desyncs after the first item and silently
    /// reads garbage. Offsets must also be representable: an unaligned offset
    /// is truncated by `LocationLayout::pack` in release.
    #[test]
    fn test_disk_appends_stride_by_the_pools_alignment() {
        let (meta, _pool, _injector) = segment_with_alignment(64 * 1024, 512);
        assert!(meta.try_reserve());

        // Three small items, each far below the alignment factor.
        let mut offsets = Vec::new();
        for i in 0..3u8 {
            let key = [b'k', i];
            let offset = meta.append_item(&key, b"v", &[]).expect("append");
            offsets.push(offset);
        }

        for (i, &offset) in offsets.iter().enumerate() {
            assert_eq!(
                offset % 512,
                0,
                "item {i} at offset {offset} is not 512-aligned"
            );
        }
        assert_eq!(offsets, vec![0, 512, 1024], "stride must be the alignment");

        // A scan must land on exactly those offsets, not 8-byte ones.
        let mut walked = Vec::new();
        let mut offset = 0u32;
        while offset < meta.write_offset() {
            let ptr = meta.header_ptr(offset, BasicHeader::SIZE).expect("header");
            let header = unsafe { BasicHeader::try_from_ptr(ptr) }.expect("valid header");
            walked.push(offset);
            offset += meta.item_stride(header.padded_size());
        }
        assert_eq!(
            walked, offsets,
            "the scan stride disagrees with the append stride"
        );
    }

    /// Memory-style 8-byte alignment must be unaffected.
    #[test]
    fn test_disk_alignment_of_eight_is_unchanged() {
        let (meta, _pool, _injector) = segment_with_alignment(64 * 1024, 8);
        assert!(meta.try_reserve());
        let a = meta.append_item(b"k1", b"v", &[]).expect("append");
        let b = meta.append_item(b"k2", b"v", &[]).expect("append");
        assert_eq!(a, 0);

        // Derived, not hardcoded: `BasicHeader::SIZE` differs by feature, and
        // the point is that the stride follows the item, not the disk default.
        let expected = (BasicHeader::SIZE + b"k2".len() + b"v".len()).next_multiple_of(8) as u32;
        assert_eq!(
            b, expected,
            "an 8-aligned pool must still pack items at 8 bytes"
        );
        assert!(
            b < 512,
            "an 8-aligned pool must not inherit the 512-byte disk stride"
        );
    }

    /// The disk twin of the slice-segment bump table: the incarnation advances
    /// on exactly the transitions that end a *used* incarnation, and no others.
    ///
    /// The eviction path recycles a drained segment as `Locked -> Reserved` and
    /// only then releases it `Reserved -> Free`, so keying the bump on
    /// `Locked -> Free` -- which the layers never drive -- would leave the tag
    /// at 0 forever with the whole suite still green. Keying on *leaving
    /// Locked* is what makes it fire.
    ///
    /// The exclusions matter just as much: `FilePool::release` returns
    /// never-used segments through `try_release`, and a bump there would drain
    /// the 6-bit tag's collision hardness with no item lifecycle at all.
    #[test]
    fn test_disk_incarnation_bumps_on_exactly_the_used_incarnation_transitions() {
        let (seg, _pool, _injector) = segment_with_item(b"k", b"v");
        assert_eq!(seg.incarnation(), 0);

        // Free -> Reserved starts an incarnation. Must NOT bump.
        assert!(seg.try_reserve());
        assert_eq!(seg.incarnation(), 0, "try_reserve must not bump");

        // Reserved -> Free: reserved but never used. Must NOT bump.
        assert!(seg.try_release());
        assert_eq!(
            seg.incarnation(),
            0,
            "Reserved -> Free is a never-used release and must not bump"
        );

        // Linking -> Free: lost a chain-extension election. Must NOT bump.
        assert!(seg.try_reserve());
        assert!(seg.cas_metadata(State::Reserved, State::Linking, None, None));
        assert!(seg.try_release());
        assert_eq!(
            seg.incarnation(),
            0,
            "Linking -> Free is a lost election and must not bump"
        );

        // Locked -> Reserved: THE REAL RECYCLE PATH. Must bump.
        assert!(seg.try_reserve());
        assert!(seg.cas_metadata(State::Reserved, State::Locked, None, None));
        assert!(seg.cas_metadata(State::Locked, State::Reserved, None, None));
        assert_eq!(
            seg.incarnation(),
            1,
            "Locked -> Reserved is how a drained segment is recycled and must bump"
        );

        // The Reserved -> Free that follows must not bump again -- one
        // incarnation ending is one bump, not two.
        assert!(seg.try_release());
        assert_eq!(
            seg.incarnation(),
            1,
            "the release following a recycle must not double-bump"
        );

        // Locked -> Free: the other way out of Locked. Must bump.
        assert!(seg.try_reserve());
        assert!(seg.cas_metadata(State::Reserved, State::Locked, None, None));
        assert!(seg.try_release());
        assert_eq!(
            seg.incarnation(),
            2,
            "Locked -> Free also ends a used incarnation and must bump"
        );

        // AwaitingRelease -> Free: condemned. Must bump.
        assert!(seg.try_reserve());
        assert!(seg.cas_metadata(State::Reserved, State::AwaitingRelease, None, None));
        assert!(seg.release_condemned());
        assert_eq!(
            seg.incarnation(),
            3,
            "AwaitingRelease -> Free ends a used incarnation and must bump"
        );

        // Bulk reset recycles the segment. Must bump.
        assert!(seg.try_reserve());
        seg.reset();
        assert_eq!(
            seg.incarnation(),
            4,
            "reset recycles the segment and must bump"
        );
        assert_eq!(seg.state(), State::Free);

        // Sealed -> Draining and Draining -> Locked are mid-life. Must NOT bump.
        assert!(seg.try_reserve());
        assert!(seg.cas_metadata(State::Reserved, State::Sealed, None, None));
        assert!(seg.cas_metadata(State::Sealed, State::Draining, None, None));
        assert!(seg.cas_metadata(State::Draining, State::Locked, None, None));
        assert_eq!(
            seg.incarnation(),
            4,
            "transitions into Locked are mid-life and must not bump"
        );

        // A chain-pointer-only rewrite that stays in Locked is mid-life, not
        // the end of an incarnation. Eleven identity CASes in organization/
        // take this shape; if one ever ran against a Locked neighbour, keying
        // the bump on the source state alone would advance the tag twice in
        // one lifetime.
        let before = seg.incarnation();
        assert!(seg.cas_metadata(State::Locked, State::Locked, Some(7), None));
        assert_eq!(
            seg.incarnation(),
            before,
            "a Locked -> Locked chain rewrite must not bump"
        );
        assert_eq!(
            seg.next(),
            Some(7),
            "the chain pointer must still be written"
        );
        assert!(seg.try_release());
        assert_eq!(
            seg.incarnation(),
            before + 1,
            "leaving Locked must still bump exactly once"
        );
    }

    /// Every disk-segment transition must carry the incarnation forward.
    ///
    /// `Metadata::new` zeroes it and compiles clean, so nothing but this test
    /// stands between a refactor and a silently reset tag -- which resurrects
    /// stale locations rather than failing loudly. This is the disk twin of
    /// `slice_segment.rs`'s `test_segment_transitions_preserve_incarnation`;
    /// its absence is why these sites went unnoticed.
    ///
    /// This pins *preservation* for the transitions that do not end an
    /// incarnation. `release_condemned` and `reset` do end one and now bump;
    /// they are still checked here, relative to the seeded tag, because a site
    /// that rebuilt `Metadata` from scratch and then bumped would land on 1
    /// rather than `TAG + 1`. Which transitions bump is pinned separately by
    /// `slice_segment.rs`'s
    /// `test_incarnation_bumps_on_exactly_the_used_incarnation_transitions`.
    #[test]
    fn test_disk_segment_transitions_preserve_incarnation() {
        const TAG: u8 = 42;

        let (seg, _pool, _injector) = segment_with_item(b"k", b"v");
        assert_eq!(seg.incarnation(), 0, "a brand-new segment starts at 0");

        // Seed a non-zero tag directly, the way a prior incarnation's release
        // would have left it.
        let seeded = Metadata::unpack(seg.metadata.load(Ordering::Acquire)).with_state(State::Free);
        seg.metadata.store(
            Metadata {
                incarnation: TAG,
                ..seeded
            }
            .pack(),
            Ordering::Release,
        );

        // Free -> Reserved
        assert!(seg.try_reserve());
        assert_eq!(seg.incarnation(), TAG, "try_reserve reset the tag");

        // Reserved -> Free
        assert!(seg.try_release());
        assert_eq!(seg.incarnation(), TAG, "try_release reset the tag");

        // Reserved -> AwaitingRelease -> Free
        assert!(seg.try_reserve());
        assert!(seg.cas_metadata(State::Reserved, State::AwaitingRelease, None, None));
        assert_eq!(seg.incarnation(), TAG, "cas_metadata reset the tag");
        // `release_condemned` ends a used incarnation, so it bumps; what is
        // pinned here is that it bumps from the seeded tag rather than
        // rebuilding the word from scratch, which would land on 1.
        assert!(seg.release_condemned());
        assert_eq!(
            seg.incarnation(),
            TAG + 1,
            "release_condemned did not carry the tag forward"
        );

        // Unconditional store back to Free. Also an incarnation-ending
        // transition, so it bumps rather than preserves.
        assert!(seg.try_reserve());
        seg.reset();
        assert_eq!(seg.incarnation(), TAG + 2, "reset cleared the tag");
    }

    /// A key LONGER than the stored one must not match, even when the bytes
    /// that follow the stored key happen to continue it.
    ///
    /// The bounds check is computed from `header.key_len()`, but the slice was
    /// built with the caller's `key.len()`, with nothing requiring the two to
    /// agree. So a lookup for `b"abXY"` against a stored `b"ab"` compared the
    /// stored key PLUS the first two bytes of the value — reading past the
    /// bound that was actually checked, and reporting a match for a key the
    /// segment does not hold.
    ///
    /// A stale hashtable location pointing at a shorter item is exactly how a
    /// caller arrives here with a mismatched length.
    #[test]
    fn a_longer_key_does_not_match_a_shorter_stored_key() {
        // The value's first bytes continue the stored key, so a comparison
        // that overruns `key_len` sees "abXY" and reports a match.
        let (seg, _pool, _injector) = segment_with_item(b"ab", b"XYZ");

        assert!(
            !seg.verify_key_at_offset(0, b"abXY", false),
            "a longer key matched a shorter stored key"
        );
        assert!(
            seg.verify_key_with_header(0, b"abXY", false).is_none(),
            "a longer key matched a shorter stored key"
        );

        // The stored key itself must still match, both ways.
        assert!(seg.verify_key_at_offset(0, b"ab", false));
        assert!(seg.verify_key_with_header(0, b"ab", false).is_some());
    }

    /// `mark_deleted` must not tombstone an item whose key merely starts with
    /// the caller's.
    ///
    /// This site had the same mismatch as the two verify paths, and the worst
    /// consequence of the three: a false-positive match here delete-marks a
    /// DIFFERENT item than the caller asked for, which is data loss rather
    /// than a wrong answer to a read.
    #[test]
    fn mark_deleted_does_not_tombstone_an_item_with_a_longer_key() {
        let (seg, _pool, _injector) = segment_with_item(b"abcd", b"V");

        assert!(
            matches!(seg.mark_deleted(0, b"ab"), Err(CacheError::KeyMismatch)),
            "mark_deleted matched a prefix and would have tombstoned the wrong item"
        );

        // And it still deletes the item it was actually asked for.
        assert_eq!(seg.mark_deleted(0, b"abcd").ok(), Some(true));
    }

    /// A key SHORTER than the stored one must not match a prefix of it.
    #[test]
    fn a_shorter_key_does_not_match_a_prefix_of_a_longer_stored_key() {
        let (seg, _pool, _injector) = segment_with_item(b"abcd", b"V");

        assert!(
            !seg.verify_key_at_offset(0, b"ab", false),
            "a prefix matched a longer stored key"
        );
        assert!(
            seg.verify_key_with_header(0, b"ab", false).is_none(),
            "a prefix matched a longer stored key"
        );
    }
}
