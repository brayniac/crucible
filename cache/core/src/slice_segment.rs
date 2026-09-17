//! In-memory segment implementation backed by a byte slice.
//!
//! [`SliceSegment`] is the core segment implementation for RAM-based caching.
//! It stores items sequentially in a contiguous memory region with atomic
//! state management for concurrent access.

use crate::error::CacheError;
use crate::item::{BasicHeader, BasicItemGuard, TtlHeader};
use crate::segment::{Segment, SegmentGuard, SegmentKeyVerify};
use crate::state::{INVALID_SEGMENT_ID, Metadata, State};
use crate::sync::*;
use std::ptr::NonNull;
use std::time::Duration;

/// Raw value reference components for constructing a `ValueRef`.
///
/// Fields: (ref_count_ptr, value_ptr, value_len, metadata_ptr, free_queue_ptr, segment_id)
pub type ValueRefRaw = (
    *const AtomicU32,
    *const u8,
    usize,
    *const AtomicU64,
    *const crossbeam_deque::Injector<u32>,
    u32,
);

/// Retry configuration for CAS operations.
struct CasRetryConfig {
    max_attempts: u32,
}

impl Default for CasRetryConfig {
    fn default() -> Self {
        Self { max_attempts: 16 }
    }
}

/// A segment backed by an in-memory byte slice.
///
/// This is the primary segment implementation for RAM-based caching.
/// Items are appended sequentially, and the segment tracks live items,
/// bytes, and reference counts for safe concurrent access.
///
/// # Memory Layout
///
/// ```text
/// +---------------------------------------------------+
/// | Item 1 | Item 2 | Item 3 | ... | [unused space]   |
/// +---------------------------------------------------+
/// ^                                ^                   ^
/// 0                           write_offset        capacity
/// ```
///
/// # Thread Safety
///
/// All mutable state is managed through atomic operations:
/// - `metadata`: Packed state + chain pointers (AtomicU64)
/// - `write_offset`: Next write position (AtomicU32)
/// - `live_items`, `live_bytes`: Statistics (AtomicU32)
/// - `ref_count`: Active readers (AtomicU32)
///
/// # TTL Models
///
/// SliceSegment supports two TTL modes controlled by `is_per_item_ttl`:
/// - `false`: Segment-level TTL using [`BasicHeader`] (all items share segment's expire_at)
/// - `true`: Per-item TTL using [`TtlHeader`] (each item has individual expire_at)
#[repr(C, align(64))]
pub struct SliceSegment<'a> {
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

    /// Total data capacity (in bytes).
    capacity: u32,

    /// Pointer to segment data.
    data: NonNull<u8>,

    /// Segment-level expiration time (coarse seconds since epoch).
    /// Only used when `is_per_item_ttl` is false.
    expire_at: AtomicU32,

    /// TTL bucket ID (0xFFFF = not in bucket).
    bucket_id: AtomicU16,

    /// Packed: [2 bits pool_id][1 bit is_per_item_ttl][5 bits reserved]
    pool_flags: u8,

    /// `log2` of the owning pool's offset alignment factor.
    ///
    /// Appends stride by `1 << align_shift`, because a `Location` stores
    /// `offset / align_bytes` and cannot name a finer offset.
    align_shift: u8,

    /// Number of times this segment was a merge destination.
    merge_count: AtomicU16,

    /// Generation counter - incremented on reuse to prevent ABA.
    generation: AtomicU16,

    /// Pointer to the pool's main free queue for guard-based release.
    /// When a segment in AwaitingRelease state has its last reader drop,
    /// the guard pushes the segment to this queue.
    free_queue: *const crossbeam_deque::Injector<u32>,

    _lifetime: std::marker::PhantomData<&'a u8>,
}

// SAFETY: Segment synchronizes access via atomics
unsafe impl<'a> Send for SliceSegment<'a> {}
unsafe impl<'a> Sync for SliceSegment<'a> {}

impl<'a> SliceSegment<'a> {
    const INVALID_BUCKET_ID: u16 = 0xFFFF;
    const POOL_ID_MASK: u8 = 0x03;
    const PER_ITEM_TTL_BIT: u8 = 0x04;

    /// Create a new segment from a data pointer.
    ///
    /// # Safety
    ///
    /// - `data` must point to at least `len` bytes of valid memory
    /// - The memory must remain valid for the lifetime `'a`
    /// - The memory must be properly aligned (8-byte minimum)
    ///
    /// # Parameters
    /// - `pool_id`: Pool ID (0-3)
    /// - `is_per_item_ttl`: If true, use per-item TTL headers
    /// - `id`: Segment ID within the pool
    /// - `data`: Pointer to segment memory
    /// - `len`: Size of segment memory in bytes
    /// - `free_queue`: Pointer to pool's main free queue for async release
    /// - `align_bytes`: The owning pool's offset alignment factor. Items are
    ///   appended at multiples of it, so it must match the `LocationLayout`
    ///   the pool packs locations with. A power of two, at least 8.
    pub unsafe fn new(
        pool_id: u8,
        is_per_item_ttl: bool,
        id: u32,
        data: *mut u8,
        len: usize,
        free_queue: *const crossbeam_deque::Injector<u32>,
        align_bytes: usize,
    ) -> Self {
        debug_assert!(pool_id <= 3, "pool_id {} exceeds 2-bit limit", pool_id);
        debug_assert!(len <= u32::MAX as usize, "segment too large");
        debug_assert!(
            align_bytes.is_power_of_two() && align_bytes >= 8,
            "align_bytes {align_bytes} must be a power of two and at least 8"
        );

        let pool_flags = (pool_id & Self::POOL_ID_MASK)
            | if is_per_item_ttl {
                Self::PER_ITEM_TTL_BIT
            } else {
                0
            };

        // A brand-new segment: this is the one site that legitimately starts
        // the incarnation counter at zero.
        let initial_meta = Metadata::new(State::Free);

        Self {
            metadata: AtomicU64::new(initial_meta.pack()),
            write_offset: AtomicU32::new(0),
            live_items: AtomicU32::new(0),
            live_bytes: AtomicU32::new(0),
            ref_count: AtomicU32::new(0),
            id,
            capacity: len as u32,
            data: unsafe { NonNull::new_unchecked(data) },
            expire_at: AtomicU32::new(0),
            bucket_id: AtomicU16::new(Self::INVALID_BUCKET_ID),
            pool_flags,
            align_shift: align_bytes.trailing_zeros() as u8,
            merge_count: AtomicU16::new(0),
            generation: AtomicU16::new(0),
            free_queue,
            _lifetime: std::marker::PhantomData,
        }
    }

    /// Check if this segment uses per-item TTL.
    #[inline]
    pub fn is_per_item_ttl(&self) -> bool {
        self.pool_flags & Self::PER_ITEM_TTL_BIT != 0
    }

    /// Get the raw data pointer.
    #[inline]
    pub fn data_ptr(&self) -> *mut u8 {
        self.data.as_ptr()
    }

    /// Reserve space atomically and return the start offset.
    fn reserve_space(&self, size: u32) -> Option<u32> {
        let config = CasRetryConfig::default();
        let mut attempts = 0;

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
                    if attempts >= config.max_attempts {
                        return None;
                    }
                    crate::sync::spin_loop();
                }
            }
        }
    }

    /// Common logic for appending an item.
    fn append_with_header(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        header_bytes: &[u8],
        header_size: usize,
    ) -> Option<u32> {
        let item_size = header_size + optional.len() + key.len() + value.len();
        // The pool's stride, not a hardcoded 8: every scan advances by the same
        // quantity, and a `Location` cannot name a finer offset.
        let padded_size = self.item_stride(item_size) as usize;

        let offset = self.reserve_space(padded_size as u32)?;

        // Write the item
        unsafe {
            let mut ptr = self.data.as_ptr().add(offset as usize);

            // Write header
            std::ptr::copy_nonoverlapping(header_bytes.as_ptr(), ptr, header_size);
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

        // Update statistics
        self.live_items.fetch_add(1, Ordering::Relaxed);
        self.live_bytes
            .fetch_add(padded_size as u32, Ordering::Relaxed);

        Some(offset)
    }

    /// Get item guard for segment-level TTL segments.
    fn get_item_guard_basic(
        &self,
        offset: u32,
        key: &[u8],
    ) -> Result<BasicItemGuard<'_>, CacheError> {
        // Check segment expiration
        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        let expire_at = self.expire_at.load(Ordering::Acquire);
        if expire_at > 0 && now >= expire_at {
            self.release_ref();
            return Err(CacheError::Expired);
        }

        // Validate offset
        if offset as usize + BasicHeader::SIZE > self.capacity as usize {
            self.release_ref();
            return Err(CacheError::InvalidOffset);
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

        // Parse header

        #[cfg(feature = "validation")]
        let header = unsafe { BasicHeader::try_from_ptr(data_ptr) };
        #[cfg(not(feature = "validation"))]
        let header = Some(unsafe { BasicHeader::from_ptr_unchecked(data_ptr) });

        let header = match header {
            Some(h) => h,
            None => {
                self.release_ref();
                return Err(CacheError::Corrupted);
            }
        };

        if header.is_deleted() {
            self.release_ref();
            return Err(CacheError::ItemDeleted);
        }

        let item_size = header.padded_size();
        if offset as usize + item_size > self.capacity as usize {
            self.release_ref();
            return Err(CacheError::InvalidOffset);
        }

        // Slice the item's BODY only. A slice spanning the header would
        // include the flags byte, and `mark_deleted` writes that byte through
        // an atomic after the item is published -- so merely creating such a
        // reference is a read that races the write. Key, value and optional
        // bytes are written before publication and never mutated, so shared
        // references over them are honest.
        let optional_start = BasicHeader::SIZE;
        let optional_end = optional_start + header.optional_len() as usize;
        let key_start = optional_end;
        let key_end = key_start + header.key_len() as usize;
        let value_start = key_end;
        let value_end = value_start + header.value_len() as usize;

        let stored_key =
            unsafe { std::slice::from_raw_parts(data_ptr.add(key_start), key_end - key_start) };
        let stored_value = unsafe {
            std::slice::from_raw_parts(data_ptr.add(value_start), value_end - value_start)
        };
        let stored_optional = unsafe {
            std::slice::from_raw_parts(data_ptr.add(optional_start), optional_end - optional_start)
        };

        if stored_key != key {
            self.release_ref();
            return Err(CacheError::KeyMismatch);
        }

        Ok(BasicItemGuard::new(
            &self.ref_count,
            stored_key,
            stored_value,
            stored_optional,
            &self.metadata,
            self.free_queue,
            self.id,
        ))
    }

    /// Get item guard for per-item TTL segments.
    fn get_item_guard_ttl(
        &self,
        offset: u32,
        key: &[u8],
    ) -> Result<BasicItemGuard<'_>, CacheError> {
        // Validate offset
        if offset as usize + TtlHeader::SIZE > self.capacity as usize {
            self.release_ref();
            return Err(CacheError::InvalidOffset);
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

        // Parse header

        #[cfg(feature = "validation")]
        let header = unsafe { TtlHeader::try_from_ptr(data_ptr) };
        #[cfg(not(feature = "validation"))]
        let header = Some(unsafe { TtlHeader::from_ptr_unchecked(data_ptr) });

        let header = match header {
            Some(h) => h,
            None => {
                self.release_ref();
                return Err(CacheError::Corrupted);
            }
        };

        if header.is_deleted() {
            self.release_ref();
            return Err(CacheError::ItemDeleted);
        }

        // Check per-item expiration
        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        if header.is_expired(now) {
            self.release_ref();
            return Err(CacheError::Expired);
        }

        let item_size = header.padded_size();
        if offset as usize + item_size > self.capacity as usize {
            self.release_ref();
            return Err(CacheError::InvalidOffset);
        }

        // Slice the item's BODY only. A slice spanning the header would
        // include the flags byte, and `mark_deleted` writes that byte through
        // an atomic after the item is published -- so merely creating such a
        // reference is a read that races the write. Key, value and optional
        // bytes are written before publication and never mutated, so shared
        // references over them are honest.
        let optional_start = TtlHeader::SIZE;
        let optional_end = optional_start + header.optional_len() as usize;
        let key_start = optional_end;
        let key_end = key_start + header.key_len() as usize;
        let value_start = key_end;
        let value_end = value_start + header.value_len() as usize;

        let stored_key =
            unsafe { std::slice::from_raw_parts(data_ptr.add(key_start), key_end - key_start) };
        let stored_value = unsafe {
            std::slice::from_raw_parts(data_ptr.add(value_start), value_end - value_start)
        };
        let stored_optional = unsafe {
            std::slice::from_raw_parts(data_ptr.add(optional_start), optional_end - optional_start)
        };

        if stored_key != key {
            self.release_ref();
            return Err(CacheError::KeyMismatch);
        }

        Ok(BasicItemGuard::new(
            &self.ref_count,
            stored_key,
            stored_value,
            stored_optional,
            &self.metadata,
            self.free_queue,
            self.id,
        ))
    }

    /// Get raw value reference for zero-copy scatter-gather I/O.
    ///
    /// Returns the raw pointers needed to construct a `ValueRef`:
    /// - `ref_count_ptr`: Pointer to this segment's ref_count (already incremented)
    /// - `value_ptr`: Pointer to the value bytes in segment memory
    /// - `value_len`: Length of the value
    ///
    /// The caller is responsible for creating a `ValueRef` that will decrement
    /// the ref_count on drop.
    ///
    /// # Safety
    ///
    /// If this method returns `Ok`, the ref_count has been incremented and
    /// the caller must ensure it is decremented when done (typically by
    /// constructing a `ValueRef` from the returned pointers).
    pub fn get_value_ref_raw(&self, offset: u32, key: &[u8]) -> Result<ValueRefRaw, CacheError> {
        // The two-phase pin, with the guard predicate: a condemned segment is
        // readable only for a reference already held, so a fresh acquire here
        // is a stale location and must miss (#127). One body for every acquire
        // site in the crate -- see `segment::try_acquire_pin` for why the
        // post-increment re-check is the load-bearing half and how a test
        // reaches the window between the two (#134).
        if !crate::segment::try_acquire_pin(
            &self.ref_count,
            &self.metadata,
            State::admits_guard_reader,
            || self.release_ref(),
        ) {
            return Err(CacheError::SegmentNotAccessible);
        }

        fence(Ordering::Acquire);

        if self.is_per_item_ttl() {
            self.get_value_ref_raw_ttl(offset, key)
        } else {
            self.get_value_ref_raw_basic(offset, key)
        }
    }

    /// Get raw value reference for BasicHeader segments.
    fn get_value_ref_raw_basic(&self, offset: u32, key: &[u8]) -> Result<ValueRefRaw, CacheError> {
        // Check segment expiration
        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        let expire_at = self.expire_at.load(Ordering::Acquire);
        if expire_at > 0 && now >= expire_at {
            self.release_ref();
            return Err(CacheError::Expired);
        }

        // Validate offset
        if offset as usize + BasicHeader::SIZE > self.capacity as usize {
            self.release_ref();
            return Err(CacheError::InvalidOffset);
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

        // Parse header

        #[cfg(feature = "validation")]
        let header = unsafe { BasicHeader::try_from_ptr(data_ptr) };
        #[cfg(not(feature = "validation"))]
        let header = Some(unsafe { BasicHeader::from_ptr_unchecked(data_ptr) });

        let header = match header {
            Some(h) => h,
            None => {
                self.release_ref();
                return Err(CacheError::Corrupted);
            }
        };

        if header.is_deleted() {
            self.release_ref();
            return Err(CacheError::ItemDeleted);
        }

        let item_size = header.padded_size();
        if offset as usize + item_size > self.capacity as usize {
            self.release_ref();
            return Err(CacheError::InvalidOffset);
        }

        // Compute value location
        let key_start = BasicHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + header.key_len() as usize;
        let value_start = key_end;
        let value_len = header.value_len() as usize;

        // Verify key
        let key_bytes =
            unsafe { std::slice::from_raw_parts(data_ptr.add(key_start), key_end - key_start) };
        if key_bytes != key {
            self.release_ref();
            return Err(CacheError::KeyMismatch);
        }

        let ref_count_ptr = &self.ref_count as *const AtomicU32;
        let value_ptr = unsafe { data_ptr.add(value_start) };
        let metadata_ptr = &self.metadata as *const AtomicU64;
        let free_queue_ptr = self.free_queue;

        Ok((
            ref_count_ptr,
            value_ptr,
            value_len,
            metadata_ptr,
            free_queue_ptr,
            self.id,
        ))
    }

    /// Get raw value reference for TtlHeader segments.
    fn get_value_ref_raw_ttl(&self, offset: u32, key: &[u8]) -> Result<ValueRefRaw, CacheError> {
        // Validate offset
        if offset as usize + TtlHeader::SIZE > self.capacity as usize {
            self.release_ref();
            return Err(CacheError::InvalidOffset);
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

        // Parse header

        #[cfg(feature = "validation")]
        let header = unsafe { TtlHeader::try_from_ptr(data_ptr) };
        #[cfg(not(feature = "validation"))]
        let header = Some(unsafe { TtlHeader::from_ptr_unchecked(data_ptr) });

        let header = match header {
            Some(h) => h,
            None => {
                self.release_ref();
                return Err(CacheError::Corrupted);
            }
        };

        if header.is_deleted() {
            self.release_ref();
            return Err(CacheError::ItemDeleted);
        }

        // Check item-level TTL
        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        if header.is_expired(now) {
            self.release_ref();
            return Err(CacheError::Expired);
        }

        let item_size = header.padded_size();
        if offset as usize + item_size > self.capacity as usize {
            self.release_ref();
            return Err(CacheError::InvalidOffset);
        }

        // Compute value location
        let key_start = TtlHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + header.key_len() as usize;
        let value_start = key_end;
        let value_len = header.value_len() as usize;

        // Verify key
        let key_bytes =
            unsafe { std::slice::from_raw_parts(data_ptr.add(key_start), key_end - key_start) };
        if key_bytes != key {
            self.release_ref();
            return Err(CacheError::KeyMismatch);
        }

        let ref_count_ptr = &self.ref_count as *const AtomicU32;
        let value_ptr = unsafe { data_ptr.add(value_start) };
        let metadata_ptr = &self.metadata as *const AtomicU64;
        let free_queue_ptr = self.free_queue;

        Ok((
            ref_count_ptr,
            value_ptr,
            value_len,
            metadata_ptr,
            free_queue_ptr,
            self.id,
        ))
    }

    /// Verify key with BasicHeader (segment-level TTL).
    /// Use when you know the pool uses segment-level TTL.
    #[inline(always)]
    pub fn verify_key_with_basic_header(
        &self,
        offset: u32,
        key: &[u8],
        allow_deleted: bool,
    ) -> Option<(u8, u8, u32)> {
        let offset = offset as usize;
        let capacity = self.capacity as usize;

        // First bounds check: can we read the header?
        if offset + BasicHeader::SIZE > capacity {
            return None;
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset) };

        #[cfg(feature = "validation")]
        let header = unsafe { BasicHeader::try_from_ptr(data_ptr) }?;
        #[cfg(not(feature = "validation"))]
        let header = unsafe { BasicHeader::from_ptr_unchecked(data_ptr) };

        if !allow_deleted && header.is_deleted() {
            return None;
        }

        // Early key length check before computing offsets
        if header.key_len() as usize != key.len() {
            return None;
        }

        let key_start = BasicHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + key.len();

        // Single combined bounds check: can we read up to key_end?
        // (This is sufficient since we only need to compare the key)
        if offset + key_end > capacity {
            return None;
        }

        // Create slice only for key bytes, not entire item
        let stored_key = unsafe { std::slice::from_raw_parts(data_ptr.add(key_start), key.len()) };
        if stored_key == key {
            Some((header.key_len(), header.optional_len(), header.value_len()))
        } else {
            None
        }
    }

    /// Verify key with TtlHeader (per-item TTL).
    /// Use when you know the pool uses per-item TTL.
    #[inline(always)]
    pub fn verify_key_with_ttl_header(
        &self,
        offset: u32,
        key: &[u8],
        allow_deleted: bool,
    ) -> Option<(u8, u8, u32)> {
        let offset = offset as usize;
        let capacity = self.capacity as usize;

        // First bounds check: can we read the header?
        if offset + TtlHeader::SIZE > capacity {
            return None;
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset) };

        #[cfg(feature = "validation")]
        let header = unsafe { TtlHeader::try_from_ptr(data_ptr) }?;
        #[cfg(not(feature = "validation"))]
        let header = unsafe { TtlHeader::from_ptr_unchecked(data_ptr) };

        if !allow_deleted && header.is_deleted() {
            return None;
        }

        // Early key length check before computing offsets
        if header.key_len() as usize != key.len() {
            return None;
        }

        let key_start = TtlHeader::SIZE + header.optional_len() as usize;
        let key_end = key_start + key.len();

        // Single combined bounds check: can we read up to key_end?
        // (This is sufficient since we only need to compare the key)
        if offset + key_end > capacity {
            return None;
        }

        // Create slice only for key bytes, not entire item
        let stored_key = unsafe { std::slice::from_raw_parts(data_ptr.add(key_start), key.len()) };
        if stored_key == key {
            Some((header.key_len(), header.optional_len(), header.value_len()))
        } else {
            None
        }
    }
}

// Implement SegmentKeyVerify
impl SegmentKeyVerify for SliceSegment<'_> {
    fn try_acquire_read(&self) -> bool {
        // The two-phase pin, with the key-verify predicate. `Draining` stays
        // admitted on purpose -- that is what lets the demoter verify keys on
        // a segment it is draining, and removing it regressed demotions to
        // zero once already. `AwaitingRelease` does not: the hashtable entries
        // are gone, so an arriving thread holds a stale location, and
        // admitting it would unstick the evictor's `ref_count == 0` gate
        // (#127).
        //
        // Because `Draining` is admitted, `Draining` is *not* an exclusive
        // claim. The evictor's claim is `Locked`, which this predicate
        // refuses, and it takes that claim before it reads `ref_count` --
        // see `layer::try_claim_for_clear` (#133).
        crate::segment::try_acquire_pin(
            &self.ref_count,
            &self.metadata,
            State::admits_verify_reader,
            || self.release_ref(),
        )
    }

    fn release_read(&self) {
        self.release_ref();
    }

    fn incarnation(&self) -> u8 {
        Metadata::unpack(self.metadata.load(Ordering::Acquire)).incarnation
    }

    #[inline(always)]
    fn verify_key_at_offset(&self, offset: u32, key: &[u8], allow_deleted: bool) -> bool {
        self.verify_key_with_header(offset, key, allow_deleted)
            .is_some()
    }

    #[inline(always)]
    fn verify_key_with_header(
        &self,
        offset: u32,
        key: &[u8],
        allow_deleted: bool,
    ) -> Option<(u8, u8, u32)> {
        if self.is_per_item_ttl() {
            self.verify_key_with_ttl_header(offset, key, allow_deleted)
        } else {
            self.verify_key_with_basic_header(offset, key, allow_deleted)
        }
    }

    fn verify_key_unexpired(&self, offset: u32, key: &[u8], now: u32) -> Option<(u8, u8, u32)> {
        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

        if self.is_per_item_ttl() {
            // Per-item TTL header - check item's expire_at
            if offset as usize + TtlHeader::SIZE > self.capacity as usize {
                return None;
            }

            #[cfg(feature = "validation")]
            let header = unsafe { TtlHeader::try_from_ptr(data_ptr) }?;
            #[cfg(not(feature = "validation"))]
            let header = unsafe { TtlHeader::from_ptr_unchecked(data_ptr) };

            if header.is_deleted() {
                return None;
            }

            // Check per-item expiration
            if header.is_expired(now) {
                return None;
            }

            let item_size = header.padded_size();
            if offset as usize + item_size > self.capacity as usize {
                return None;
            }

            let key_start = TtlHeader::SIZE + header.optional_len() as usize;
            let key_end = key_start + header.key_len() as usize;

            // Body only -- see the note in `get_item`: a slice spanning the
            // header would race `mark_deleted`'s write to the flags byte.
            let stored_key =
                unsafe { std::slice::from_raw_parts(data_ptr.add(key_start), key_end - key_start) };
            if key_end <= item_size && stored_key == key {
                Some((header.key_len(), header.optional_len(), header.value_len()))
            } else {
                None
            }
        } else {
            // Segment-level TTL - just verify key, caller checks segment expiration
            self.verify_key_with_header(offset, key, false)
        }
    }
}

// Implement Segment trait
impl Segment for SliceSegment<'_> {
    fn id(&self) -> u32 {
        self.id
    }

    fn pool_id(&self) -> u8 {
        self.pool_flags & Self::POOL_ID_MASK
    }

    fn generation(&self) -> u16 {
        self.generation.load(Ordering::Acquire)
    }

    fn increment_generation(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    fn align_bytes(&self) -> u32 {
        1 << self.align_shift
    }

    fn capacity(&self) -> usize {
        self.capacity as usize
    }

    fn write_offset(&self) -> u32 {
        self.write_offset.load(Ordering::Acquire)
    }

    fn live_items(&self) -> u32 {
        self.live_items.load(Ordering::Relaxed)
    }

    fn live_bytes(&self) -> u32 {
        self.live_bytes.load(Ordering::Relaxed)
    }

    fn ref_count(&self) -> u32 {
        self.ref_count.load(Ordering::Acquire)
    }

    fn ref_count_seqcst(&self) -> u32 {
        self.ref_count.load(Ordering::SeqCst)
    }

    fn state(&self) -> State {
        let packed = self.metadata.load(Ordering::Acquire);
        Metadata::unpack(packed).state
    }

    fn try_reserve(&self) -> bool {
        let current_packed = self.metadata.load(Ordering::Acquire);
        let current_meta = Metadata::unpack(current_packed);

        if current_meta.state != State::Free {
            return false;
        }

        // Preserve the incarnation: reserving does not end one.
        let new_meta = current_meta
            .with_state(State::Reserved)
            .with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID);

        match self.metadata.compare_exchange(
            current_packed,
            new_meta.pack(),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Reset statistics
                self.write_offset.store(0, Ordering::Relaxed);
                self.live_items.store(0, Ordering::Relaxed);
                self.live_bytes.store(0, Ordering::Relaxed);
                self.expire_at.store(0, Ordering::Relaxed);
                self.merge_count.store(0, Ordering::Relaxed);
                self.generation.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(_) => false,
        }
    }

    fn try_release(&self) -> bool {
        loop {
            let current_packed = self.metadata.load(Ordering::Acquire);
            let current_meta = Metadata::unpack(current_packed);

            match current_meta.state {
                State::Reserved | State::Linking | State::Locked => {}
                State::Free => return false,
                _ => {
                    panic!(
                        "Attempt to release segment {} in invalid state {:?}",
                        self.id, current_meta.state
                    );
                }
            }

            // Leaving `Locked` ends a used incarnation: the segment has been
            // drained and cleared, so every `Location` issued against it must
            // stop resolving. `Reserved` and `Linking` must NOT bump --
            // `MemoryPool::release`, `FilePool::release` and lost
            // chain-extension elections all return never-used segments through
            // here, and bumping there would advance a 6-bit tag at a rate
            // decoupled from segment lifecycles.
            let ends_incarnation = current_meta.state == State::Locked;

            let new_meta = current_meta
                .with_state(State::Free)
                .with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID);
            let new_meta = if ends_incarnation {
                new_meta.bump_incarnation()
            } else {
                new_meta
            };

            match self.metadata.compare_exchange(
                current_packed,
                new_meta.pack(),
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.merge_count.store(0, Ordering::Relaxed);
                    return true;
                }
                Err(_) => continue,
            }
        }
    }

    fn release_condemned(&self) -> bool {
        // Delegate to the inherent method
        SliceSegment::release_condemned(self)
    }

    fn cas_metadata(
        &self,
        expected_state: State,
        new_state: State,
        new_next: Option<u32>,
        new_prev: Option<u32>,
    ) -> bool {
        let current = self.metadata.load(Ordering::Acquire);
        let current_meta = Metadata::unpack(current);

        if current_meta.state != expected_state {
            return false;
        }

        let new_meta = current_meta.with_state(new_state).with_chain_ids(
            new_next.unwrap_or(current_meta.next),
            new_prev.unwrap_or(current_meta.prev),
        );

        // *Recycling out of* `Locked` ends a used incarnation -- not merely
        // being `Locked`, and not every way of leaving it. The segment has
        // been drained and cleared, and the layers recycle it as
        // `Locked -> Reserved` before releasing it `Reserved -> Free`. Bumping
        // here, inside the same CAS that publishes the new state, is what
        // makes a stale location stop resolving -- and no thread can observe
        // the new state paired with the old tag.
        //
        // Naming the destinations is load-bearing twice over:
        //
        // * a chain-pointer-only rewrite that stays in `Locked` is mid-life,
        //   and eleven identity CASes in `organization/` take exactly that
        //   shape;
        // * `Locked -> AwaitingRelease` is the #133 deferral -- the evictor
        //   claimed `Locked`, found readers still holding the segment, and is
        //   handing it back. Nothing has been cleared and the readers' pins
        //   are still good, so the incarnation has *not* ended; it ends later,
        //   at `AwaitingRelease -> Free`, where `try_free_condemned` bumps it.
        //
        // Keying on the source state alone would advance the tag twice in one
        // lifetime, halving the space the collision argument rests on. Every
        // other transition is mid-life and carries the tag forward unchanged.
        let new_meta = if current_meta.state == State::Locked
            && matches!(new_state, State::Reserved | State::Free)
        {
            new_meta.bump_incarnation()
        } else {
            new_meta
        };

        // The transitions that *end* reader admission are the condemner half
        // of the Dekker pair with every two-phase acquire: this CAS is the
        // store, and the `ref_count_seqcst()` load the caller makes next is
        // the load. Both have to sit in the SC total order, so the CAS is
        // `SeqCst` for those and `AcqRel` for the rest.
        //
        // `Draining -> AwaitingRelease` shuts out every reader;
        // `Draining -> Locked` shuts out the `holds_valid_data` readers that
        // `Draining` still admitted and is the point past which the evictor
        // rewrites segment bytes. `Sealed -> Draining` is the store the
        // guard-path readers (`is_readable()`, which excludes `Draining`)
        // pair against, and the layers load `ref_count` right after it.
        //
        // Everything else -- chain-pointer rewrites, `Locked -> Reserved`,
        // `Reserved -> Free`, the `Relinking` shuffles -- is either mid-life
        // or already exclusive, and stays `AcqRel`. Blanket-SeqCst here would
        // be noise on the merge paths' eleven identity CASes.
        let (success, failure) =
            if crate::state::transition_excludes_readers(expected_state, new_state) {
                (Ordering::SeqCst, Ordering::SeqCst)
            } else {
                (Ordering::AcqRel, Ordering::Acquire)
            };

        self.metadata
            .compare_exchange(current, new_meta.pack(), success, failure)
            .is_ok()
    }

    fn next(&self) -> Option<u32> {
        let packed = self.metadata.load(Ordering::Acquire);
        Metadata::unpack(packed).next_id()
    }

    fn prev(&self) -> Option<u32> {
        let packed = self.metadata.load(Ordering::Acquire);
        Metadata::unpack(packed).prev_id()
    }

    fn expire_at(&self) -> u32 {
        self.expire_at.load(Ordering::Acquire)
    }

    fn set_expire_at(&self, expire_at: u32) {
        self.expire_at.store(expire_at, Ordering::Release);
    }

    fn segment_ttl(&self, now: u32) -> Option<Duration> {
        let expire_at = self.expire_at.load(Ordering::Acquire);
        if expire_at == 0 || now >= expire_at {
            None
        } else {
            Some(Duration::from_secs((expire_at - now) as u64))
        }
    }

    fn item_ttl(&self, offset: u32, now: u32) -> Option<Duration> {
        if self.is_per_item_ttl() {
            // Per-item TTL: read from item header
            if offset as usize + TtlHeader::SIZE > self.capacity as usize {
                return None;
            }

            let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

            #[cfg(feature = "validation")]
            let header = unsafe { TtlHeader::try_from_ptr(data_ptr) }?;
            #[cfg(not(feature = "validation"))]
            let header = unsafe { TtlHeader::from_ptr_unchecked(data_ptr) };

            header.remaining_ttl(now)
        } else {
            // Segment-level TTL
            self.segment_ttl(now)
        }
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
        let end = offset as usize + len;
        if end > self.capacity as usize {
            return None;
        }
        Some(unsafe { std::slice::from_raw_parts(self.data.as_ptr().add(offset as usize), len) })
    }

    fn header_ptr(&self, offset: u32, len: usize) -> Option<*const u8> {
        let end = offset as usize + len;
        if end > self.capacity as usize {
            return None;
        }
        // Derived from the segment allocation's own pointer, so it keeps that
        // allocation's provenance rather than being narrowed to read-only.
        Some(unsafe { self.data.as_ptr().add(offset as usize) })
    }

    fn append_item(&self, key: &[u8], value: &[u8], optional: &[u8]) -> Option<u32> {
        assert!(
            !self.is_per_item_ttl(),
            "use append_item_with_ttl for per-item TTL segments"
        );
        assert!(!key.is_empty() && key.len() <= BasicHeader::MAX_KEY_LEN);
        assert!(optional.len() <= BasicHeader::MAX_OPTIONAL_LEN);
        assert!(value.len() <= BasicHeader::MAX_VALUE_LEN);

        let header = BasicHeader::new(key.len() as u8, optional.len() as u8, value.len() as u32);

        let mut header_bytes = [0u8; BasicHeader::SIZE];
        header.to_bytes(&mut header_bytes);

        self.append_with_header(key, value, optional, &header_bytes, BasicHeader::SIZE)
    }

    fn append_item_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        expire_at: u32,
    ) -> Option<u32> {
        assert!(
            self.is_per_item_ttl(),
            "use append_item for segment-level TTL segments"
        );
        assert!(!key.is_empty() && key.len() <= TtlHeader::MAX_KEY_LEN);
        assert!(optional.len() <= TtlHeader::MAX_OPTIONAL_LEN);
        assert!(value.len() <= TtlHeader::MAX_VALUE_LEN);

        let header = TtlHeader::new(
            key.len() as u8,
            optional.len() as u8,
            value.len() as u32,
            expire_at,
        );

        let mut header_bytes = [0u8; TtlHeader::SIZE];
        header.to_bytes(&mut header_bytes);

        self.append_with_header(key, value, optional, &header_bytes, TtlHeader::SIZE)
    }

    fn begin_append_with_ttl(
        &self,
        key: &[u8],
        value_len: usize,
        optional: &[u8],
        expire_at: u32,
    ) -> Option<(u32, u32, *mut u8)> {
        assert!(
            self.is_per_item_ttl(),
            "begin_append_with_ttl requires per-item TTL segments"
        );
        assert!(!key.is_empty() && key.len() <= TtlHeader::MAX_KEY_LEN);
        assert!(optional.len() <= TtlHeader::MAX_OPTIONAL_LEN);
        assert!(value_len <= TtlHeader::MAX_VALUE_LEN);

        let header = TtlHeader::new(
            key.len() as u8,
            optional.len() as u8,
            value_len as u32,
            expire_at,
        );

        let mut header_bytes = [0u8; TtlHeader::SIZE];
        header.to_bytes(&mut header_bytes);

        let item_size = TtlHeader::SIZE + optional.len() + key.len() + value_len;
        let padded_size = self.item_stride(item_size) as usize;

        let offset = self.reserve_space(padded_size as u32)?;

        // Write header + optional + key (but NOT value)
        unsafe {
            let mut ptr = self.data.as_ptr().add(offset as usize);

            // Write header
            std::ptr::copy_nonoverlapping(header_bytes.as_ptr(), ptr, TtlHeader::SIZE);
            ptr = ptr.add(TtlHeader::SIZE);

            // Write optional
            if !optional.is_empty() {
                std::ptr::copy_nonoverlapping(optional.as_ptr(), ptr, optional.len());
                ptr = ptr.add(optional.len());
            }

            // Write key
            std::ptr::copy_nonoverlapping(key.as_ptr(), ptr, key.len());
            ptr = ptr.add(key.len());

            // Return (offset, item_size, value_ptr)
            Some((offset, padded_size as u32, ptr))
        }
    }

    fn begin_append(
        &self,
        key: &[u8],
        value_len: usize,
        optional: &[u8],
    ) -> Option<(u32, u32, *mut u8)> {
        assert!(
            !self.is_per_item_ttl(),
            "begin_append requires non-per-item-TTL segments"
        );
        assert!(!key.is_empty() && key.len() <= BasicHeader::MAX_KEY_LEN);
        assert!(optional.len() <= BasicHeader::MAX_OPTIONAL_LEN);
        assert!(value_len <= BasicHeader::MAX_VALUE_LEN);

        let header = BasicHeader::new(key.len() as u8, optional.len() as u8, value_len as u32);

        let mut header_bytes = [0u8; BasicHeader::SIZE];
        header.to_bytes(&mut header_bytes);

        let item_size = BasicHeader::SIZE + optional.len() + key.len() + value_len;
        let padded_size = self.item_stride(item_size) as usize;

        let offset = self.reserve_space(padded_size as u32)?;

        // Write header + optional + key (but NOT value)
        unsafe {
            let mut ptr = self.data.as_ptr().add(offset as usize);

            // Write header
            std::ptr::copy_nonoverlapping(header_bytes.as_ptr(), ptr, BasicHeader::SIZE);
            ptr = ptr.add(BasicHeader::SIZE);

            // Write optional
            if !optional.is_empty() {
                std::ptr::copy_nonoverlapping(optional.as_ptr(), ptr, optional.len());
                ptr = ptr.add(optional.len());
            }

            // Write key
            std::ptr::copy_nonoverlapping(key.as_ptr(), ptr, key.len());
            ptr = ptr.add(key.len());

            // Return (offset, item_size, value_ptr)
            Some((offset, padded_size as u32, ptr))
        }
    }

    fn finalize_append(&self, item_size: u32) {
        fence(Ordering::Release);

        // Update statistics
        self.live_items.fetch_add(1, Ordering::Relaxed);
        self.live_bytes.fetch_add(item_size, Ordering::Relaxed);
    }

    fn mark_deleted_at_offset(&self, offset: u32) {
        let header_size = if self.is_per_item_ttl() {
            TtlHeader::SIZE
        } else {
            BasicHeader::SIZE
        };

        if offset as usize + header_size > self.capacity as usize {
            return;
        }

        // Set deleted flag (byte 1, bit 6) without key verification
        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };
        let flags_ptr = unsafe { data_ptr.add(1) };

        #[cfg(not(feature = "loom"))]
        {
            let flags_atomic = unsafe { &*(flags_ptr as *const AtomicU8) };
            flags_atomic.fetch_or(0x40, Ordering::Release);
        }

        #[cfg(feature = "loom")]
        {
            fence(Ordering::Release);
            let old_val = unsafe { std::ptr::read_volatile(flags_ptr) };
            unsafe {
                std::ptr::write_volatile(flags_ptr, old_val | 0x40);
            }
        }

        // Note: We don't update live_items/live_bytes here because we don't
        // know the full item size. The segment will be cleaned up eventually
        // through normal eviction.
    }

    fn mark_deleted(&self, offset: u32, key: &[u8]) -> Result<bool, CacheError> {
        let current_state = self.state();
        match current_state {
            State::Free
            | State::Reserved
            | State::Linking
            | State::Live
            | State::Sealed
            | State::Relinking => {}
            // Reject new operations on segments being cleared or awaiting release
            State::Draining | State::Locked | State::AwaitingRelease => return Ok(false),
        }

        let header_size = if self.is_per_item_ttl() {
            TtlHeader::SIZE
        } else {
            BasicHeader::SIZE
        };

        if offset as usize + header_size > self.capacity as usize {
            return Ok(false);
        }

        // Verify key and get header info in one parse
        let (key_len, optional_len, value_len) =
            match self.verify_key_with_header(offset, key, true) {
                Some(info) => info,
                None => return Err(CacheError::KeyMismatch),
            };

        // Must be the stride the append charged to `live_bytes`, not the
        // 8-padded body size, or the accounting drifts on a coarser pool.
        let item_size = self.item_stride(
            header_size + optional_len as usize + key_len as usize + value_len as usize,
        );

        // Set deleted flag (byte 1, bit 6)
        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };
        let flags_ptr = unsafe { data_ptr.add(1) };

        #[cfg(not(feature = "loom"))]
        let old_flags = {
            let flags_atomic = unsafe { &*(flags_ptr as *const AtomicU8) };
            flags_atomic.fetch_or(0x40, Ordering::Release)
        };

        #[cfg(feature = "loom")]
        let old_flags = {
            let old_val = unsafe { std::ptr::read_volatile(flags_ptr) };
            if (old_val & 0x40) == 0 {
                fence(Ordering::Release);
                unsafe {
                    std::ptr::write_volatile(flags_ptr, old_val | 0x40);
                }
            }
            old_val
        };

        if (old_flags & 0x40) != 0 {
            return Ok(false); // Already deleted
        }

        // Update statistics
        self.live_items.fetch_sub(1, Ordering::Relaxed);
        self.live_bytes.fetch_sub(item_size, Ordering::Relaxed);

        Ok(true)
    }

    fn merge_count(&self) -> u16 {
        self.merge_count.load(Ordering::Acquire)
    }

    fn increment_merge_count(&self) {
        let _ = self.merge_count.fetch_add(1, Ordering::AcqRel);
    }

    fn reset(&self) {
        self.write_offset.store(0, Ordering::Relaxed);
        self.live_items.store(0, Ordering::Relaxed);
        self.live_bytes.store(0, Ordering::Relaxed);
        self.ref_count.store(0, Ordering::Relaxed);
        self.expire_at.store(0, Ordering::Relaxed);
        self.bucket_id
            .store(Self::INVALID_BUCKET_ID, Ordering::Relaxed);
        self.merge_count.store(0, Ordering::Relaxed);
    }
}

impl SliceSegment<'_> {
    /// Force reset this segment to Free state.
    ///
    /// This is used during flush operations to reset all segments regardless
    /// of their current state. It resets all data fields and sets the state
    /// to Free.
    ///
    /// # Safety
    ///
    /// This should only be called when the cache is being flushed and no
    /// concurrent operations are accessing the segments (e.g., after the
    /// hashtable has been cleared).
    ///
    /// The flush-only requirement is stronger than it looks now that this ends
    /// an incarnation: the bump goes through a plain load-modify-`store`, not a
    /// CAS. Racing a concurrent `BasicItemGuard::drop`, which bumps through a
    /// CAS, would drop or duplicate one of the two bumps.
    pub fn force_free(&self) {
        // Reset all data fields
        self.write_offset.store(0, Ordering::Relaxed);
        self.live_items.store(0, Ordering::Relaxed);
        self.live_bytes.store(0, Ordering::Relaxed);
        self.ref_count.store(0, Ordering::Relaxed);
        self.expire_at.store(0, Ordering::Relaxed);
        self.bucket_id
            .store(Self::INVALID_BUCKET_ID, Ordering::Relaxed);
        self.merge_count.store(0, Ordering::Relaxed);

        // Set state to Free with invalid chain pointers, advancing the
        // incarnation: a flush recycles every segment at once, so locations
        // issued before it must stop resolving just as they would after any
        // other end-of-life transition.
        let free_meta = Metadata::unpack(self.metadata.load(Ordering::Acquire))
            .with_state(State::Free)
            .with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID)
            .bump_incarnation();
        self.metadata.store(free_meta.pack(), Ordering::Release);
    }

    /// Drop one reference, freeing the segment if we were the last reader and it
    /// was condemned while we held it.
    ///
    /// This is the same handoff `BasicItemGuard::drop` performs. A back-out can
    /// now land on a condemned segment (see #127), and the last one out has to
    /// release it or nobody will.
    fn release_ref(&self) {
        // SeqCst: the release side of the handoff is the other Dekker pair.
        // This thread stores `ref_count` then loads the state; the condemner
        // stores `AwaitingRelease` then loads `ref_count`. If both loads may
        // go stale, each side concludes the other will free the segment and
        // neither does -- the `AwaitingRelease` strand of #129. The
        // `fetch_sub`'s store half is what has to be in the total order, so
        // `Release` is not enough. See `Segment::ref_count_seqcst`.
        let prev = self.ref_count.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            fence(Ordering::SeqCst);
            if Metadata::unpack(self.metadata.load(Ordering::SeqCst)).state
                == State::AwaitingRelease
            {
                self.release_condemned();
            }
        }
    }

    /// Release a condemned segment to the free queue.
    ///
    /// Called when the last reader drops its guard on a segment in
    /// AwaitingRelease state. Transitions to Free and pushes to the
    /// pool's free queue.
    ///
    /// Returns true if the segment was released, false if it wasn't in
    /// AwaitingRelease state *or* a reader has pinned it again since the
    /// caller's decrement. The whole transition, including that
    /// re-validation and why it is needed, lives in
    /// `segment::try_free_condemned` -- this is the `SliceSegment`
    /// entry point to it, which additionally zeroes `merge_count`.
    pub fn release_condemned(&self) -> bool {
        // SAFETY: `free_queue` is the pool's queue, valid for the pool's
        // lifetime, and `self.id` is this segment's id within it.
        unsafe {
            crate::segment::try_free_condemned(
                &self.ref_count,
                &self.metadata,
                self.free_queue,
                self.id,
                || self.merge_count.store(0, Ordering::Relaxed),
            )
        }
    }
}

// Implement SegmentGuard for zero-copy access
impl SegmentGuard for SliceSegment<'_> {
    type Guard<'a>
        = BasicItemGuard<'a>
    where
        Self: 'a;

    fn get_item(&self, offset: u32, key: &[u8]) -> Result<Self::Guard<'_>, CacheError> {
        // The two-phase pin, with the guard predicate: a condemned segment is
        // readable only for a reference already held, so a fresh acquire here
        // is a stale location and must miss (#127). One body for every acquire
        // site in the crate -- see `segment::try_acquire_pin` for why the
        // post-increment re-check is the load-bearing half and how a test
        // reaches the window between the two (#134).
        if !crate::segment::try_acquire_pin(
            &self.ref_count,
            &self.metadata,
            State::admits_guard_reader,
            || self.release_ref(),
        ) {
            return Err(CacheError::SegmentNotAccessible);
        }

        fence(Ordering::Acquire);

        if self.is_per_item_ttl() {
            self.get_item_guard_ttl(offset, key)
        } else {
            self.get_item_guard_basic(offset, key)
        }
    }

    fn get_item_verified(
        &self,
        offset: u32,
        header_info: (u8, u8, u32),
    ) -> Result<Self::Guard<'_>, CacheError> {
        // The two-phase pin, with the guard predicate: a condemned segment is
        // readable only for a reference already held, so a fresh acquire here
        // is a stale location and must miss (#127). One body for every acquire
        // site in the crate -- see `segment::try_acquire_pin` for why the
        // post-increment re-check is the load-bearing half and how a test
        // reaches the window between the two (#134).
        if !crate::segment::try_acquire_pin(
            &self.ref_count,
            &self.metadata,
            State::admits_guard_reader,
            || self.release_ref(),
        ) {
            return Err(CacheError::SegmentNotAccessible);
        }

        fence(Ordering::Acquire);

        let (key_len, optional_len, value_len) = header_info;
        let header_size = if self.is_per_item_ttl() {
            TtlHeader::SIZE
        } else {
            BasicHeader::SIZE
        };

        // Compute padded item size
        let item_size =
            (header_size + optional_len as usize + key_len as usize + value_len as usize + 7) & !7;

        // Validate offset bounds
        if offset as usize + item_size > self.capacity as usize {
            self.release_ref();
            return Err(CacheError::InvalidOffset);
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

        // Compute slice boundaries from header info
        let optional_start = header_size;
        let optional_end = optional_start + optional_len as usize;
        let key_start = optional_end;
        let key_end = key_start + key_len as usize;
        let value_start = key_end;
        let value_end = value_start + value_len as usize;

        // Body only -- see the note in `get_item`: a slice spanning the header
        // would include the flags byte that `mark_deleted` writes after
        // publication, and creating it races that write.
        let stored_key =
            unsafe { std::slice::from_raw_parts(data_ptr.add(key_start), key_end - key_start) };
        let stored_value = unsafe {
            std::slice::from_raw_parts(data_ptr.add(value_start), value_end - value_start)
        };
        let stored_optional = unsafe {
            std::slice::from_raw_parts(data_ptr.add(optional_start), optional_end - optional_start)
        };

        Ok(BasicItemGuard::new(
            &self.ref_count,
            stored_key,
            stored_value,
            stored_optional,
            &self.metadata,
            self.free_queue,
            self.id,
        ))
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::item::ItemGuard;
    use std::alloc::{Layout, alloc, dealloc};

    /// Dummy free queue for tests - segments won't actually be released back.
    static TEST_FREE_QUEUE: std::sync::LazyLock<crossbeam_deque::Injector<u32>> =
        std::sync::LazyLock::new(crossbeam_deque::Injector::new);

    /// `SliceSegment::release_condemned` must decline while a reader is
    /// pinned, and the pinned reader's own drop must then complete the
    /// handoff (#129).
    ///
    /// This is the end-to-end wiring of `segment::try_free_condemned`'s
    /// re-validation, driven through the real entry points: a real guard from
    /// `get_item` holds the reference, the evictor's race-fix call is the one
    /// that must decline, and `BasicItemGuard::drop` is what actually frees.
    /// Deleting the re-validation makes the first assertion fail.
    #[test]
    fn release_condemned_declines_while_a_guard_is_held() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 64 * 1024);
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Live, None, None));
        segment.append_item(b"key", b"value", &[]).expect("append");

        let guard = segment.get_item(0, b"key").expect("guard");
        assert_eq!(segment.ref_count(), 1, "the guard holds a reference");

        // The evictor's sequence: seal, claim, condemn.
        assert!(segment.cas_metadata(State::Live, State::Sealed, None, None));
        assert!(segment.cas_metadata(State::Sealed, State::Draining, None, None));
        assert!(segment.cas_metadata(State::Draining, State::AwaitingRelease, None, None));

        // The race fix, running while the reader is still in. `prev == 1` is
        // not what it checks -- but every caller that reaches here believed
        // it, which is exactly the hazard.
        assert!(
            !segment.release_condemned(),
            "released a condemned segment out from under a live guard"
        );
        assert_eq!(segment.state(), State::AwaitingRelease);
        assert_eq!(segment.ref_count(), 1);

        // The reader leaves: its drop is the one that owes the free.
        drop(guard);
        assert_eq!(segment.ref_count(), 0);
        assert_eq!(
            segment.state(),
            State::Free,
            "the last guard drop must complete the handoff, not strand the segment"
        );

        unsafe { free_test_segment(ptr, layout) };
    }

    /// The same gate reached through `SliceSegment::release_ref`, the
    /// back-out path -- a reference taken and dropped while another reader is
    /// pinned must not free the segment.
    #[test]
    fn a_back_out_does_not_free_a_segment_another_reader_holds() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 64 * 1024);
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Live, None, None));
        segment.append_item(b"key", b"value", &[]).expect("append");

        let guard = segment.get_item(0, b"key").expect("guard");
        assert!(segment.cas_metadata(State::Live, State::Sealed, None, None));
        assert!(segment.cas_metadata(State::Sealed, State::Draining, None, None));
        assert!(segment.cas_metadata(State::Draining, State::AwaitingRelease, None, None));

        // A second reader arrives, is refused by the condemned gate, and
        // backs its transient reference out through `release_ref`.
        assert!(segment.get_item(0, b"key").is_err());
        assert_eq!(segment.ref_count(), 1, "only the first guard remains");
        assert_eq!(
            segment.state(),
            State::AwaitingRelease,
            "the back-out must not free a segment the first guard still holds"
        );

        drop(guard);
        assert_eq!(segment.state(), State::Free);

        unsafe { free_test_segment(ptr, layout) };
    }

    /// Tests that drive a race by hand through `segment::interpose`.
    ///
    /// Gated off under the model checkers for the same reason the hook itself
    /// is: a `std` thread-local inside a loom or shuttle execution is state
    /// the checker cannot see.
    #[cfg(all(not(feature = "loom"), not(feature = "shuttle")))]
    mod interposed {
        use super::*;
        use crate::segment::interpose;

        // ---- the two-phase acquire's post-increment re-check (#134) ----------
        //
        // The window these cover is between `try_acquire_pin`'s `fetch_add` and
        // its re-check. No single-threaded caller enters it and no multi-threaded
        // one enters it *deterministically*, which is why the check was
        // deletable-and-green on `main`: every existing condemned-refusal test
        // above is satisfied by the *pre*-increment check alone.
        //
        // `segment::interpose` puts the racing transition inside the window by
        // hand. The call under test is the real production entry point, so these
        // pin the shipped body, not a mirror of it.
        //
        // `Rc` only so the hook closure can own a handle to the segment (the hook
        // slot is `'static`); the whole test runs on one thread.

        /// A guard acquire must refuse a condemn published inside its own
        /// increment/re-check window -- and, being the last reference out of a
        /// segment that is now condemned, must complete the handoff rather than
        /// strand it.
        ///
        /// Red proof: delete the `if !admits(...)` re-check in
        /// `segment::try_acquire_pin` and the acquire returns a live guard on an
        /// `AwaitingRelease` segment.
        #[test]
        fn guard_acquire_refuses_a_condemn_published_inside_its_window() {
            use std::cell::Cell;
            use std::rc::Rc;

            let (segment, ptr, layout) = create_test_segment(0, false, 0, 64 * 1024);
            let segment = Rc::new(segment);
            assert!(segment.try_reserve());
            assert!(segment.cas_metadata(State::Reserved, State::Live, None, None));
            segment.append_item(b"key", b"value", &[]).expect("append");
            assert!(segment.cas_metadata(State::Live, State::Sealed, None, None));

            // The evictor, run by hand at the one instant that matters: the
            // reader has taken its transient reference and has not yet re-checked.
            let condemned = Rc::new(Cell::new(false));
            let observed_count = Rc::new(Cell::new(u32::MAX));
            {
                let seg = Rc::clone(&segment);
                let flag = Rc::clone(&condemned);
                let count = Rc::clone(&observed_count);
                let _hook = interpose::install(Box::new(move |phase| {
                    if phase == interpose::ACQUIRE_AFTER_INCREMENT {
                        count.set(seg.ref_count());
                        assert!(seg.cas_metadata(
                            State::Sealed,
                            State::AwaitingRelease,
                            None,
                            None
                        ));
                        flag.set(true);
                    }
                }));

                assert!(
                    segment.get_item(0, b"key").is_err(),
                    "the acquire pinned a segment condemned inside its own window -- \
                     only the post-increment re-check can see that transition"
                );
            }

            assert!(condemned.get(), "the hook must have run");
            assert_eq!(
                observed_count.get(),
                1,
                "the window is entered with the transient reference already taken"
            );
            assert_eq!(segment.ref_count(), 0, "the back-out must leave no pin");
            assert_eq!(
                segment.state(),
                State::Free,
                "the back-out removed the LAST reference from a condemned segment, so it \
                 owes the AwaitingRelease -> Free handoff; nothing sweeps AwaitingRelease"
            );

            drop(segment);
            unsafe { free_test_segment(ptr, layout) };
        }

        /// The same window, reached through the *key-verify* acquire and closed by
        /// the evictor's exclusive claim rather than by a condemn.
        ///
        /// This is the pairing #133 turns on: `Draining` deliberately admits this
        /// reader, so `Locked` is the claim that shuts it out, and the re-check is
        /// the only read on the reader's side that can see it.
        ///
        /// Red proof: delete `try_acquire_pin`'s re-check and the verify reader
        /// pins a segment whose bytes the evictor is about to rewrite.
        #[test]
        fn verify_acquire_refuses_a_clear_claim_published_inside_its_window() {
            use std::cell::Cell;
            use std::rc::Rc;

            let (segment, ptr, layout) = create_test_segment(0, false, 0, 64 * 1024);
            let segment = Rc::new(segment);
            assert!(segment.try_reserve());
            assert!(segment.cas_metadata(State::Reserved, State::Live, None, None));
            segment.append_item(b"key", b"value", &[]).expect("append");
            assert!(segment.cas_metadata(State::Live, State::Sealed, None, None));
            assert!(segment.cas_metadata(State::Sealed, State::Draining, None, None));

            // Sanity: `Draining` admits this reader when nothing races it. That is
            // what the demoter depends on, and what makes `Locked` -- not
            // `Draining` -- the exclusive claim.
            assert!(segment.try_acquire_read(), "Draining must admit a verify");
            segment.release_read();

            let claimed = Rc::new(Cell::new(false));
            {
                let seg = Rc::clone(&segment);
                let flag = Rc::clone(&claimed);
                let _hook = interpose::install(Box::new(move |phase| {
                    if phase == interpose::ACQUIRE_AFTER_INCREMENT {
                        assert!(seg.cas_metadata(State::Draining, State::Locked, None, None));
                        flag.set(true);
                    }
                }));

                assert!(
                    !segment.try_acquire_read(),
                    "the verify acquire pinned a segment claimed for clearing inside its \
                     own window"
                );
            }

            assert!(claimed.get(), "the hook must have run");
            assert_eq!(segment.ref_count(), 0, "the back-out must leave no pin");
            assert_eq!(
                segment.state(),
                State::Locked,
                "a refused verify must leave the evictor's claim alone"
            );

            drop(segment);
            unsafe { free_test_segment(ptr, layout) };
        }

        /// Every acquire entry point must route through the one body, not carry
        /// its own copy of the protocol.
        ///
        /// Four separate copies is how #134 happened: there was no single place a
        /// test could aim at, and each copy was independently deletable. This runs
        /// the same in-window condemn against each entry point in turn; a site
        /// that stops calling `segment::try_acquire_pin` stops being refused.
        #[test]
        fn every_acquire_entry_point_routes_through_the_shared_body() {
            use std::cell::Cell;
            use std::rc::Rc;

            #[derive(Clone, Copy, Debug)]
            enum Entry {
                GetItem,
                GetItemVerified,
                GetValueRefRaw,
                TryAcquireRead,
            }

            for entry in [
                Entry::GetItem,
                Entry::GetItemVerified,
                Entry::GetValueRefRaw,
                Entry::TryAcquireRead,
            ] {
                let (segment, ptr, layout) = create_test_segment(0, false, 0, 64 * 1024);
                let segment = Rc::new(segment);
                assert!(segment.try_reserve());
                assert!(segment.cas_metadata(State::Reserved, State::Live, None, None));
                segment.append_item(b"key", b"value", &[]).expect("append");
                assert!(segment.cas_metadata(State::Live, State::Sealed, None, None));

                let fired = Rc::new(Cell::new(false));
                {
                    let seg = Rc::clone(&segment);
                    let flag = Rc::clone(&fired);
                    let _hook = interpose::install(Box::new(move |phase| {
                        if phase == interpose::ACQUIRE_AFTER_INCREMENT {
                            assert!(seg.cas_metadata(
                                State::Sealed,
                                State::AwaitingRelease,
                                None,
                                None
                            ));
                            flag.set(true);
                        }
                    }));

                    let admitted = match entry {
                        Entry::GetItem => segment.get_item(0, b"key").is_ok(),
                        // key_len 3, optional_len 0, value_len 5 -- "key"/"value".
                        Entry::GetItemVerified => segment.get_item_verified(0, (3, 0, 5)).is_ok(),
                        Entry::GetValueRefRaw => segment.get_value_ref_raw(0, b"key").is_ok(),
                        Entry::TryAcquireRead => segment.try_acquire_read(),
                    };
                    assert!(
                        !admitted,
                        "{entry:?} admitted a condemn published inside the \
                         increment/re-check window -- it is not going through \
                         segment::try_acquire_pin"
                    );
                }

                assert!(
                    fired.get(),
                    "the hook must have run for {entry:?} -- if it did not, the entry \
                     point never reached the shared acquire body at all"
                );
                assert_eq!(segment.ref_count(), 0, "{entry:?} left a pin behind");

                drop(segment);
                unsafe { free_test_segment(ptr, layout) };
            }
        }
    }

    /// A verify must hold a reference for the duration of its byte reads.
    ///
    /// Without it, a reader can pass the incarnation check and then be
    /// preempted while the segment is drained and refilled, so its reads race
    /// `append_with_header`'s `copy_nonoverlapping` (crucible#109). The
    /// reference is what stops the recycle: eviction checks `ref_count() == 0`
    /// before condemning.
    #[test]
    fn test_read_guard_is_taken_and_released_around_a_verify() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 64 * 1024);
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Live, None, None));
        segment.append_item(b"key", b"value", &[]).expect("append");

        assert_eq!(segment.ref_count(), 0, "starts unreferenced");
        assert!(segment.verify_key_guarded(0, b"key", false, segment.incarnation()));
        assert_eq!(
            segment.ref_count(),
            0,
            "the guard must be released, whatever the verify answered"
        );

        // A mismatching key still releases.
        assert!(!segment.verify_key_guarded(0, b"other", false, segment.incarnation()));
        assert_eq!(segment.ref_count(), 0, "released on the reject path too");

        unsafe { free_test_segment(ptr, layout) };
    }

    /// The guard must refuse a segment whose bytes are not valid to read.
    ///
    /// `Locked` is mid-clear and `Free`/`Reserved` hold no published item, so a
    /// verify against one must fail rather than read whatever is there.
    /// `Draining` must still be accepted: the drain reads items through the
    /// verifier while the segment sits in that state, and excluding it stopped
    /// demotion outright.
    #[test]
    fn test_read_guard_accepts_exactly_the_states_holding_valid_data() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 64 * 1024);
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Live, None, None));
        segment.append_item(b"key", b"value", &[]).expect("append");
        let inc = segment.incarnation();

        // Live: readable.
        assert!(segment.verify_key_guarded(0, b"key", false, inc));

        // Sealed and Draining both still hold the item's bytes.
        assert!(segment.cas_metadata(State::Live, State::Sealed, None, None));
        assert!(segment.verify_key_guarded(0, b"key", false, inc));
        assert!(segment.cas_metadata(State::Sealed, State::Draining, None, None));
        assert!(
            segment.verify_key_guarded(0, b"key", false, inc),
            "Draining must be verifiable -- the drain itself reads through here"
        );

        // Locked is mid-clear: refuse.
        assert!(segment.cas_metadata(State::Draining, State::Locked, None, None));
        assert!(
            !segment.verify_key_guarded(0, b"key", false, inc),
            "a segment being cleared must not be read"
        );
        assert_eq!(segment.ref_count(), 0, "a refused guard leaks no reference");

        unsafe { free_test_segment(ptr, layout) };
    }

    /// Build a Sealed segment holding one item, then condemn it.
    ///
    /// This is the state a segment is in after the evictor has drained its
    /// hashtable entries and is waiting on its last reader: the item's bytes
    /// are still intact, but no fresh lookup can reach it any more.
    fn condemned_segment_with_one_item(id: u32) -> (SliceSegment<'static>, *mut u8, Layout) {
        let (segment, ptr, layout) = create_test_segment(0, false, id, 64 * 1024);
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Live, None, None));
        segment.append_item(b"key", b"value", &[]).expect("append");
        assert!(segment.cas_metadata(State::Live, State::Sealed, None, None));
        assert!(segment.cas_metadata(State::Sealed, State::AwaitingRelease, None, None));
        (segment, ptr, layout)
    }

    /// `get_value_ref_raw` must miss on a condemned segment (#127).
    #[test]
    fn test_condemned_segment_refuses_get_value_ref_raw() {
        let (segment, ptr, layout) = condemned_segment_with_one_item(10);

        assert!(
            segment.get_value_ref_raw(0, b"key").is_err(),
            "a fresh zero-copy read must not resolve against a condemned segment"
        );
        assert_eq!(
            segment.ref_count(),
            0,
            "a refused acquire must not raise the reference count -- the evictor's \
             ref_count == 0 gate depends on it staying put"
        );
        assert_eq!(
            segment.state(),
            State::AwaitingRelease,
            "a refused read must leave the state machine alone -- gated only on the \
             post-increment re-check, the refused acquire would take a reference, find \
             itself the last one out, and recycle the segment itself"
        );

        unsafe { free_test_segment(ptr, layout) };
    }

    /// `get_item` must miss on a condemned segment (#127).
    #[test]
    fn test_condemned_segment_refuses_get_item() {
        let (segment, ptr, layout) = condemned_segment_with_one_item(11);

        assert!(
            segment.get_item(0, b"key").is_err(),
            "a fresh guarded read must not resolve against a condemned segment"
        );
        assert_eq!(segment.ref_count(), 0);
        assert_eq!(
            segment.state(),
            State::AwaitingRelease,
            "a refused read must leave the state machine alone -- gated only on the \
             post-increment re-check, the refused acquire would take a reference, find \
             itself the last one out, and recycle the segment itself"
        );

        unsafe { free_test_segment(ptr, layout) };
    }

    /// `get_item_verified` must miss on a condemned segment (#127).
    ///
    /// This is the layers' hot path: they parse the header themselves and then
    /// ask for the guard, so the gate has to be here too and not only in
    /// `get_item`.
    #[test]
    fn test_condemned_segment_refuses_get_item_verified() {
        let (segment, ptr, layout) = create_test_segment(0, false, 12, 64 * 1024);
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Live, None, None));
        segment.append_item(b"key", b"value", &[]).expect("append");
        let header_info = segment
            .verify_key_with_header(0, b"key", false)
            .expect("header parses while the segment is live");
        assert!(segment.cas_metadata(State::Live, State::Sealed, None, None));
        assert!(segment.cas_metadata(State::Sealed, State::AwaitingRelease, None, None));

        assert!(
            segment.get_item_verified(0, header_info).is_err(),
            "a pre-verified read must not resolve against a condemned segment"
        );
        assert_eq!(segment.ref_count(), 0);
        assert_eq!(
            segment.state(),
            State::AwaitingRelease,
            "a refused read must leave the state machine alone -- gated only on the \
             post-increment re-check, the refused acquire would take a reference, find \
             itself the last one out, and recycle the segment itself"
        );

        unsafe { free_test_segment(ptr, layout) };
    }

    /// The verify guard must refuse a condemned segment but keep admitting a
    /// draining one.
    ///
    /// `Draining` staying in is load-bearing: the drain reads items through the
    /// verifier while the segment sits in that state, and excluding it once
    /// stopped demotion outright. `AwaitingRelease` is the one to exclude
    /// (#127) -- its hashtable entries are already gone.
    #[test]
    fn test_read_guard_refuses_a_condemned_segment_but_admits_a_draining_one() {
        let (segment, ptr, layout) = create_test_segment(0, false, 13, 64 * 1024);
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Live, None, None));
        segment.append_item(b"key", b"value", &[]).expect("append");
        let inc = segment.incarnation();

        assert!(segment.cas_metadata(State::Live, State::Sealed, None, None));
        assert!(segment.cas_metadata(State::Sealed, State::Draining, None, None));
        assert!(
            segment.try_acquire_read(),
            "Draining must still be admitted -- the demoter verifies keys through here"
        );
        segment.release_read();
        assert_eq!(segment.ref_count(), 0);

        assert!(segment.cas_metadata(State::Draining, State::AwaitingRelease, None, None));
        assert!(
            !segment.try_acquire_read(),
            "a condemned segment must not admit a fresh reference"
        );
        assert!(
            !segment.verify_key_guarded(0, b"key", false, inc),
            "and the verify built on it must answer no"
        );
        assert_eq!(segment.ref_count(), 0, "a refused guard leaks no reference");
        assert_eq!(
            segment.state(),
            State::AwaitingRelease,
            "a refused read must leave the state machine alone -- gated only on the \
             post-increment re-check, the refused acquire would take a reference, find \
             itself the last one out, and recycle the segment itself"
        );

        unsafe { free_test_segment(ptr, layout) };
    }

    /// The last reference out of a condemned segment must free it (#127 part 2).
    ///
    /// Part 1 makes this path reachable: a back-out can now land on
    /// `AwaitingRelease`, and if it just decrements, the segment is stranded
    /// there with no reader left to hand it on.
    #[test]
    fn test_release_ref_frees_a_condemned_segment_when_last_out() {
        let (segment, ptr, layout) = condemned_segment_with_one_item(14);

        // Stand in for a reference taken before the condemn.
        segment.ref_count.fetch_add(1, Ordering::Acquire);
        segment.release_ref();

        assert_eq!(segment.ref_count(), 0);
        assert_eq!(
            segment.state(),
            State::Free,
            "the last reference out of a condemned segment must release it, not \
             strand it in AwaitingRelease"
        );

        unsafe { free_test_segment(ptr, layout) };
    }

    /// ...but only the *last* one out, and only when condemned.
    #[test]
    fn test_release_ref_does_not_free_early_or_spuriously() {
        let (segment, ptr, layout) = condemned_segment_with_one_item(15);

        segment.ref_count.fetch_add(2, Ordering::Acquire);
        segment.release_ref();
        assert_eq!(segment.ref_count(), 1);
        assert_eq!(
            segment.state(),
            State::AwaitingRelease,
            "a reference still outstanding means the segment is not free yet"
        );
        segment.release_ref();
        assert_eq!(segment.state(), State::Free);

        unsafe { free_test_segment(ptr, layout) };

        // A live segment is left alone.
        let (segment, ptr, layout) = create_test_segment(0, false, 16, 64 * 1024);
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Live, None, None));
        segment.ref_count.fetch_add(1, Ordering::Acquire);
        segment.release_ref();
        assert_eq!(segment.ref_count(), 0);
        assert_eq!(segment.state(), State::Live);

        unsafe { free_test_segment(ptr, layout) };
    }

    fn create_test_segment(
        pool_id: u8,
        is_per_item_ttl: bool,
        id: u32,
        size: usize,
    ) -> (SliceSegment<'static>, *mut u8, Layout) {
        let layout = Layout::from_size_align(size, 64).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null());
        unsafe {
            std::ptr::write_bytes(ptr, 0, size);
        }
        let free_queue_ptr: *const crossbeam_deque::Injector<u32> = &*TEST_FREE_QUEUE;
        let segment = unsafe {
            SliceSegment::new(pool_id, is_per_item_ttl, id, ptr, size, free_queue_ptr, 8)
        };
        (segment, ptr, layout)
    }

    unsafe fn free_test_segment(ptr: *mut u8, layout: Layout) {
        unsafe {
            dealloc(ptr, layout);
        }
    }

    /// The production header-decode path, end to end, over an allocation Miri
    /// can reason about.
    ///
    /// This is the sequence #88 was about: publish an item, decode its header
    /// (which reads the flags byte through an `AtomicU8`), delete-mark it —
    /// the one header write that happens after publication — and decode again.
    /// While the decoders took `&[u8]`, running this under Miri reported a
    /// `SharedReadWrite` retag of `&AtomicU8` from a `SharedReadOnly` parent.
    ///
    /// Deliberately clock-free: `clocksource` issues a syscall Miri does not
    /// support, so anything touching expiry cannot be checked this way.
    #[test]
    fn decoding_a_published_header_does_not_alias_segment_memory() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();

        let offset = segment.append_item(b"mykey", b"myvalue", b"").unwrap();

        // Decode through the production path.
        assert!(segment.verify_key_at_offset(offset, b"mykey", false));
        assert!(!segment.verify_key_at_offset(offset, b"otherkey", false));

        // The one header byte written after publication.
        assert!(segment.mark_deleted(offset, b"mykey").unwrap());

        // Decode again: the flags byte now reads back as deleted.
        assert!(!segment.verify_key_at_offset(offset, b"mykey", false));
        assert!(segment.verify_key_at_offset(offset, b"mykey", true));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    /// `header_ptr` is where the bounds check for header decoding lives now
    /// that `try_from_ptr` has no length to check.
    #[test]
    fn header_ptr_rejects_a_range_past_the_end_of_the_segment() {
        let (segment, ptr, layout) = create_test_segment(0, false, 1, 1024);
        let capacity = segment.capacity() as u32;

        assert!(segment.header_ptr(0, BasicHeader::SIZE).is_some());
        assert!(
            segment
                .header_ptr(capacity - BasicHeader::SIZE as u32, BasicHeader::SIZE)
                .is_some()
        );

        assert!(
            segment
                .header_ptr(capacity - BasicHeader::SIZE as u32 + 1, BasicHeader::SIZE)
                .is_none(),
            "a header running one byte past the end must be rejected"
        );
        assert!(segment.header_ptr(capacity, 1).is_none());

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_creation() {
        let (segment, ptr, layout) = create_test_segment(0, false, 42, 1024);
        assert_eq!(segment.id(), 42);
        assert_eq!(segment.pool_id(), 0);
        assert!(!segment.is_per_item_ttl());
        assert_eq!(segment.capacity(), 1024);
        assert_eq!(segment.state(), State::Free);
        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_per_item_ttl_flag() {
        let (segment, ptr, layout) = create_test_segment(2, true, 0, 1024);
        assert_eq!(segment.pool_id(), 2);
        assert!(segment.is_per_item_ttl());
        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_reserve_release() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);
        assert_eq!(segment.state(), State::Free);
        assert!(segment.try_reserve());
        assert_eq!(segment.state(), State::Reserved);
        assert!(segment.try_release());
        assert_eq!(segment.state(), State::Free);
        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    // Every Free -> Reserved transition must bump the generation counter:
    // CasToken ABA protection depends on a recycled segment never serving
    // items under a generation that outstanding tokens were issued against.
    #[test]
    fn test_segment_generation_bumps_on_recycle() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);
        assert_eq!(segment.generation(), 0);

        // First allocation
        assert!(segment.try_reserve());
        assert_eq!(segment.generation(), 1);

        // Recycle: back to Free, then reserved again
        assert!(segment.try_release());
        assert!(segment.try_reserve());
        assert_eq!(segment.generation(), 2);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    /// The incarnation advances on exactly the transitions that end a *used*
    /// incarnation, and on no others.
    ///
    /// This is not a tidiness test, and the table is not the obvious one. The
    /// eviction path recycles a drained segment as `Locked -> Reserved` and
    /// only then releases it `Reserved -> Free`, so keying the bump on
    /// `Locked -> Free` -- which the layers never drive -- leaves the tag at 0
    /// forever with the whole suite still green. Keying on *leaving Locked* is
    /// what makes it fire.
    ///
    /// The exclusions matter just as much. `MemoryPool::release` and
    /// `FilePool::release` both return never-used segments through
    /// `try_release`. If the bump fired there too, a burst of reserve/release
    /// with no item lifecycle at all would drain the 6-bit tag's collision
    /// hardness for free. The last arm covers the other half of the rule: a
    /// chain rewrite that stays in `Locked` is not a departure from it.
    #[test]
    fn test_incarnation_bumps_on_exactly_the_used_incarnation_transitions() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        assert_eq!(segment.incarnation(), 0);

        // Free -> Reserved starts an incarnation. Must NOT bump.
        assert!(segment.try_reserve());
        assert_eq!(segment.incarnation(), 0, "try_reserve must not bump");

        // Reserved -> Free: reserved but never used. Must NOT bump.
        assert!(segment.try_release());
        assert_eq!(
            segment.incarnation(),
            0,
            "Reserved -> Free is a never-used release and must not bump"
        );

        // Linking -> Free: lost a chain-extension election. Must NOT bump.
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Linking, None, None));
        assert!(segment.try_release());
        assert_eq!(
            segment.incarnation(),
            0,
            "Linking -> Free is a lost election and must not bump"
        );

        // Locked -> Reserved: THE REAL RECYCLE PATH. Must bump.
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Locked, None, None));
        assert!(segment.cas_metadata(State::Locked, State::Reserved, None, None));
        assert_eq!(
            segment.incarnation(),
            1,
            "Locked -> Reserved is how a drained segment is recycled and must bump"
        );

        // The Reserved -> Free that follows must not bump again -- one
        // incarnation ending is one bump, not two.
        assert!(segment.try_release());
        assert_eq!(
            segment.incarnation(),
            1,
            "the release following a recycle must not double-bump"
        );

        // Locked -> Free: the other way out of Locked. Must bump.
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Locked, None, None));
        assert!(segment.try_release());
        assert_eq!(
            segment.incarnation(),
            2,
            "Locked -> Free also ends a used incarnation and must bump"
        );

        // AwaitingRelease -> Free: condemned. Must bump.
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::AwaitingRelease, None, None));
        assert!(SliceSegment::release_condemned(&segment));
        assert_eq!(
            segment.incarnation(),
            3,
            "AwaitingRelease -> Free ends a used incarnation and must bump"
        );

        // Sealed -> Draining and Draining -> Locked are mid-life. Must NOT bump.
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Sealed, None, None));
        assert!(segment.cas_metadata(State::Sealed, State::Draining, None, None));
        assert!(segment.cas_metadata(State::Draining, State::Locked, None, None));
        assert_eq!(
            segment.incarnation(),
            3,
            "transitions into Locked are mid-life and must not bump"
        );

        // A chain-pointer-only rewrite that stays in Locked is mid-life, not
        // the end of an incarnation. Eleven identity CASes in organization/
        // take this shape; if one ever ran against a Locked neighbour, keying
        // the bump on the source state alone would advance the tag twice in
        // one lifetime.
        let before = segment.incarnation();
        assert!(segment.cas_metadata(State::Locked, State::Locked, Some(7), None));
        assert_eq!(
            segment.incarnation(),
            before,
            "a Locked -> Locked chain rewrite must not bump"
        );
        assert_eq!(
            segment.next(),
            Some(7),
            "the chain pointer must still be written"
        );
        assert!(segment.try_release());
        assert_eq!(
            segment.incarnation(),
            before + 1,
            "leaving Locked must still bump exactly once"
        );

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    /// Flush recycles every segment at once, so locations issued before it must
    /// stop resolving: `force_free` ends an incarnation like any other exit.
    #[test]
    fn test_force_free_bumps_the_incarnation() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        assert!(segment.try_reserve());
        let before = segment.incarnation();
        segment.force_free();
        assert_eq!(segment.incarnation(), (before + 1) & 0x3F);
        assert_eq!(segment.state(), State::Free);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    /// The tag is 6 bits: it must wrap rather than spill into the state byte.
    #[test]
    fn test_incarnation_wraps_at_64_and_stays_in_range() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        for _ in 0..70 {
            assert!(segment.try_reserve());
            assert!(segment.cas_metadata(State::Reserved, State::Locked, None, None));
            assert!(segment.cas_metadata(State::Locked, State::Reserved, None, None));
            assert!(segment.try_release());
            assert!(segment.incarnation() < 64, "tag must stay within 6 bits");
        }
        assert_eq!(segment.incarnation(), 70 % 64);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    /// Every metadata transition must carry the incarnation forward.
    ///
    /// Seeds a nonzero tag and drives each transition site. A site that rebuilt
    /// `Metadata` from scratch would silently reset the tag to 0, and stale
    /// `Location`s naming this segment would start resolving again -- exactly
    /// the aliasing the tag exists to prevent. Nothing bumps the tag yet, so a
    /// reset is invisible without seeding one first.
    ///
    /// This pins *preservation* for the transitions that do not end an
    /// incarnation. `release_condemned` and `force_free` do end one and now
    /// bump; they are still checked here, relative to the seeded tag, because a
    /// site that rebuilt `Metadata` from scratch and then bumped would land on
    /// 1 rather than `TAG + 1`. Which transitions bump is pinned separately by
    /// `test_incarnation_bumps_on_exactly_the_used_incarnation_transitions`.
    #[test]
    fn test_segment_transitions_preserve_incarnation() {
        const TAG: u8 = 0x2A;

        fn seed(seg: &SliceSegment<'_>, tag: u8) {
            let cur = Metadata::unpack(seg.metadata.load(Ordering::Acquire));
            seg.metadata.store(
                Metadata {
                    incarnation: tag,
                    ..cur
                }
                .pack(),
                Ordering::Release,
            );
        }

        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);
        assert_eq!(segment.incarnation(), 0, "a fresh segment starts at 0");

        seed(&segment, TAG);

        // Free -> Reserved
        assert!(segment.try_reserve());
        assert_eq!(segment.incarnation(), TAG, "try_reserve reset the tag");

        // Reserved -> Linking, with chain pointers rewritten
        assert!(segment.cas_metadata(State::Reserved, State::Linking, Some(7), Some(9)));
        assert_eq!(segment.incarnation(), TAG, "cas_metadata reset the tag");

        // Linking -> Free
        assert!(segment.try_release());
        assert_eq!(segment.incarnation(), TAG, "try_release reset the tag");

        // AwaitingRelease -> Free. This one ends a used incarnation, so it
        // bumps; what is pinned here is that it bumps *from the seeded tag*
        // rather than rebuilding the word from scratch, which would land on 1.
        assert!(segment.cas_metadata(State::Free, State::AwaitingRelease, None, None));
        assert!(SliceSegment::release_condemned(&segment));
        assert_eq!(
            segment.incarnation(),
            TAG + 1,
            "release_condemned did not carry the tag forward"
        );

        // Bulk pool reset. Also incarnation-ending, so it bumps again.
        segment.force_free();
        assert_eq!(
            segment.incarnation(),
            TAG + 2,
            "force_free did not carry the tag forward"
        );

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_append_basic() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();

        let offset = segment.append_item(b"test_key", b"test_value", b"");
        assert!(offset.is_some());
        assert_eq!(offset.unwrap(), 0);
        assert_eq!(segment.live_items(), 1);
        assert!(segment.live_bytes() > 0);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_append_per_item_ttl() {
        let (segment, ptr, layout) = create_test_segment(0, true, 0, 4096);
        segment.try_reserve();

        let expire_at = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs()
            + 3600;

        let offset = segment.append_item_with_ttl(b"test_key", b"test_value", b"", expire_at);
        assert!(offset.is_some());
        assert_eq!(offset.unwrap(), 0);
        assert_eq!(segment.live_items(), 1);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_verify_key() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();

        let offset = segment.append_item(b"mykey", b"myvalue", b"").unwrap();

        assert!(segment.verify_key_at_offset(offset, b"mykey", false));
        assert!(!segment.verify_key_at_offset(offset, b"wrongkey", false));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_get_item() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();

        // Set far-future expiration
        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        segment.set_expire_at(now + 3600);

        // Transition to Live for reads
        segment.cas_metadata(State::Reserved, State::Live, None, None);

        let offset = segment.append_item(b"mykey", b"myvalue", b"opts").unwrap();

        let guard = segment.get_item(offset, b"mykey").unwrap();
        assert_eq!(guard.key(), b"mykey");
        assert_eq!(guard.value(), b"myvalue");
        assert_eq!(guard.optional(), b"opts");

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_mark_deleted() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();

        let offset = segment.append_item(b"key1", b"value1", b"").unwrap();
        assert_eq!(segment.live_items(), 1);

        let result = segment.mark_deleted(offset, b"key1");
        assert_eq!(result, Ok(true));
        assert_eq!(segment.live_items(), 0);

        // Second delete should return Ok(false)
        let result2 = segment.mark_deleted(offset, b"key1");
        assert_eq!(result2, Ok(false));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_item_ttl() {
        let (segment, ptr, layout) = create_test_segment(0, true, 0, 4096);
        segment.try_reserve();

        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        let expire_at = now + 3600;

        let offset = segment
            .append_item_with_ttl(b"key", b"value", b"", expire_at)
            .unwrap();

        let ttl = segment.item_ttl(offset, now);
        assert!(ttl.is_some());
        let ttl_secs = ttl.unwrap().as_secs();
        assert!((3599..=3600).contains(&ttl_secs));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_next_prev() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);
        // Initially should have no next/prev
        assert!(segment.next().is_none());
        assert!(segment.prev().is_none());

        // Set up chain links via cas_metadata
        segment.try_reserve();
        segment.cas_metadata(State::Reserved, State::Live, Some(42), Some(41));

        assert_eq!(segment.next(), Some(42));
        assert_eq!(segment.prev(), Some(41));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_bucket_id() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);

        // Initially no bucket
        assert!(segment.bucket_id().is_none());

        // Set bucket
        segment.set_bucket_id(5);
        assert_eq!(segment.bucket_id(), Some(5));

        // Clear bucket
        segment.clear_bucket_id();
        assert!(segment.bucket_id().is_none());

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_generation() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);

        let gen0 = segment.generation();
        assert_eq!(gen0, 0);

        segment.increment_generation();
        assert_eq!(segment.generation(), 1);

        segment.increment_generation();
        assert_eq!(segment.generation(), 2);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_merge_count() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);

        assert_eq!(segment.merge_count(), 0);

        segment.increment_merge_count();
        assert_eq!(segment.merge_count(), 1);

        segment.increment_merge_count();
        assert_eq!(segment.merge_count(), 2);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_reset() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();

        // Write some data
        segment.append_item(b"key", b"value", b"");
        assert!(segment.live_items() > 0);
        segment.set_bucket_id(3);
        segment.increment_merge_count();

        // Reset should clear everything
        segment.reset();
        assert_eq!(segment.write_offset(), 0);
        assert_eq!(segment.live_items(), 0);
        assert_eq!(segment.live_bytes(), 0);
        assert_eq!(segment.ref_count(), 0);
        assert_eq!(segment.expire_at(), 0);
        assert!(segment.bucket_id().is_none());
        assert_eq!(segment.merge_count(), 0);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_ttl_methods() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);

        // No expire_at set yet
        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        assert!(segment.segment_ttl(now).is_none());

        // Set far future expiration
        segment.set_expire_at(now + 3600);
        assert_eq!(segment.expire_at(), now + 3600);

        let ttl = segment.segment_ttl(now);
        assert!(ttl.is_some());
        assert!(ttl.unwrap().as_secs() >= 3599);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_segment_data_slice() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);

        // Valid slice
        let slice = segment.data_slice(0, 100);
        assert!(slice.is_some());
        assert_eq!(slice.unwrap().len(), 100);

        // Invalid slice (beyond capacity)
        let slice = segment.data_slice(900, 200);
        assert!(slice.is_none());

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_try_reserve_when_not_free() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);

        // First reserve succeeds
        assert!(segment.try_reserve());
        assert_eq!(segment.state(), State::Reserved);

        // Second reserve should fail (not in Free state)
        assert!(!segment.try_reserve());
        assert_eq!(segment.state(), State::Reserved);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_try_release_when_already_free() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);

        // Already Free, should return false
        assert!(!segment.try_release());
        assert_eq!(segment.state(), State::Free);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_cas_metadata_wrong_state() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);
        segment.try_reserve();

        // Try to transition from wrong expected state
        let result = segment.cas_metadata(State::Live, State::Sealed, None, None);
        assert!(!result); // Should fail because current state is Reserved

        // Correct transition should work
        let result = segment.cas_metadata(State::Reserved, State::Live, None, None);
        assert!(result);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_get_item_segment_not_readable() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        // Segment is in Free state, should not be readable
        let result = segment.get_item(0, b"key");
        assert!(result.is_err());
        assert!(matches!(result, Err(CacheError::SegmentNotAccessible)));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_get_item_key_mismatch() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();
        segment.set_expire_at(
            clocksource::coarse::UnixInstant::now()
                .duration_since(clocksource::coarse::UnixInstant::EPOCH)
                .as_secs()
                + 3600,
        );
        segment.cas_metadata(State::Reserved, State::Live, None, None);

        let offset = segment.append_item(b"correct_key", b"value", b"").unwrap();

        let result = segment.get_item(offset, b"wrong_key");
        assert!(result.is_err());
        assert!(matches!(result, Err(CacheError::KeyMismatch)));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_get_item_expired() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();
        // Set expiration in the past
        segment.set_expire_at(1);
        segment.cas_metadata(State::Reserved, State::Live, None, None);

        let offset = segment.append_item(b"key", b"value", b"").unwrap();

        let result = segment.get_item(offset, b"key");
        assert!(result.is_err());
        assert!(matches!(result, Err(CacheError::Expired)));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_get_item_deleted() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();
        segment.set_expire_at(
            clocksource::coarse::UnixInstant::now()
                .duration_since(clocksource::coarse::UnixInstant::EPOCH)
                .as_secs()
                + 3600,
        );
        segment.cas_metadata(State::Reserved, State::Live, None, None);

        let offset = segment.append_item(b"key", b"value", b"").unwrap();
        segment.mark_deleted(offset, b"key").unwrap();

        let result = segment.get_item(offset, b"key");
        assert!(result.is_err());
        assert!(matches!(result, Err(CacheError::ItemDeleted)));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_get_item_invalid_offset() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();
        segment.set_expire_at(
            clocksource::coarse::UnixInstant::now()
                .duration_since(clocksource::coarse::UnixInstant::EPOCH)
                .as_secs()
                + 3600,
        );
        segment.cas_metadata(State::Reserved, State::Live, None, None);

        // Offset beyond capacity
        let result = segment.get_item(5000, b"key");
        assert!(result.is_err());
        assert!(matches!(result, Err(CacheError::InvalidOffset)));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_get_item_per_item_ttl() {
        let (segment, ptr, layout) = create_test_segment(0, true, 0, 4096);
        segment.try_reserve();
        segment.cas_metadata(State::Reserved, State::Live, None, None);

        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        let expire_at = now + 3600;

        let offset = segment
            .append_item_with_ttl(b"key", b"value", b"opt", expire_at)
            .unwrap();

        let guard = segment.get_item(offset, b"key").unwrap();
        assert_eq!(guard.key(), b"key");
        assert_eq!(guard.value(), b"value");
        assert_eq!(guard.optional(), b"opt");

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_get_item_per_item_ttl_expired() {
        let (segment, ptr, layout) = create_test_segment(0, true, 0, 4096);
        segment.try_reserve();
        segment.cas_metadata(State::Reserved, State::Live, None, None);

        // Expire in the past
        let offset = segment
            .append_item_with_ttl(b"key", b"value", b"", 1)
            .unwrap();

        let result = segment.get_item(offset, b"key");
        assert!(result.is_err());
        assert!(matches!(result, Err(CacheError::Expired)));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_mark_deleted_key_mismatch() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();

        let offset = segment.append_item(b"correct_key", b"value", b"").unwrap();

        let result = segment.mark_deleted(offset, b"wrong_key");
        assert!(result.is_err());
        assert!(matches!(result, Err(CacheError::KeyMismatch)));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_mark_deleted_invalid_offset() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();

        // Offset beyond capacity
        let result = segment.mark_deleted(5000, b"key");
        assert!(result.is_ok());
        assert!(!result.unwrap());

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_mark_deleted_draining_state() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();

        let offset = segment.append_item(b"key", b"value", b"").unwrap();

        // Transition to Draining
        segment.cas_metadata(State::Reserved, State::Draining, None, None);

        let result = segment.mark_deleted(offset, b"key");
        assert!(result.is_ok());
        assert!(!result.unwrap());

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_mark_deleted_per_item_ttl() {
        let (segment, ptr, layout) = create_test_segment(0, true, 0, 4096);
        segment.try_reserve();

        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();

        let offset = segment
            .append_item_with_ttl(b"key", b"value", b"", now + 3600)
            .unwrap();

        let result = segment.mark_deleted(offset, b"key");
        assert!(result.is_ok());
        assert!(result.unwrap());

        // Second delete should return false
        let result2 = segment.mark_deleted(offset, b"key");
        assert!(result2.is_ok());
        assert!(!result2.unwrap());

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_verify_key_per_item_ttl_invalid_offset() {
        let (segment, ptr, layout) = create_test_segment(0, true, 0, 1024);

        // Offset beyond capacity for per-item TTL
        assert!(!segment.verify_key_at_offset(5000, b"key", false));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_verify_key_basic_invalid_offset() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);

        // Offset beyond capacity for basic header
        assert!(!segment.verify_key_at_offset(5000, b"key", false));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_verify_key_allow_deleted() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();

        let offset = segment.append_item(b"key", b"value", b"").unwrap();
        segment.mark_deleted(offset, b"key").unwrap();

        // With allow_deleted=false, should fail
        assert!(!segment.verify_key_at_offset(offset, b"key", false));

        // With allow_deleted=true, should succeed
        assert!(segment.verify_key_at_offset(offset, b"key", true));

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_item_ttl_segment_level() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();

        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        segment.set_expire_at(now + 3600);

        let offset = segment.append_item(b"key", b"value", b"").unwrap();

        // For segment-level TTL, item_ttl returns segment TTL
        let ttl = segment.item_ttl(offset, now);
        assert!(ttl.is_some());
        assert!(ttl.unwrap().as_secs() >= 3599);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_item_ttl_per_item_invalid_offset() {
        let (segment, ptr, layout) = create_test_segment(0, true, 0, 1024);

        // Invalid offset for per-item TTL
        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        let ttl = segment.item_ttl(5000, now);
        assert!(ttl.is_none());

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_data_ptr() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 1024);
        assert_eq!(segment.data_ptr(), ptr);
        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_multiple_items_and_write_offset() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 4096);
        segment.try_reserve();

        assert_eq!(segment.write_offset(), 0);

        let offset1 = segment.append_item(b"key1", b"value1", b"").unwrap();
        assert_eq!(offset1, 0);
        let wo1 = segment.write_offset();
        assert!(wo1 > 0);

        let offset2 = segment.append_item(b"key2", b"value2", b"").unwrap();
        assert_eq!(offset2, wo1);
        let wo2 = segment.write_offset();
        assert!(wo2 > wo1);

        assert_eq!(segment.live_items(), 2);

        unsafe {
            free_test_segment(ptr, layout);
        }
    }

    #[test]
    fn test_capacity_exhausted() {
        let (segment, ptr, layout) = create_test_segment(0, false, 0, 64);
        segment.try_reserve();

        // Try to write a large value that won't fit
        let large_value = vec![b'x'; 100];
        let result = segment.append_item(b"key", &large_value, b"");
        assert!(result.is_none());

        unsafe {
            free_test_segment(ptr, layout);
        }
    }
}

// -----------------------------------------------------------------------------
// SegmentPrune implementation
// -----------------------------------------------------------------------------

use crate::segment::{PruneCollectingResult, SegmentPrune};

impl SegmentPrune for SliceSegment<'_> {
    fn prune<F>(&self, threshold: u8, get_frequency: F) -> (u32, u32, u32, u32)
    where
        F: Fn(&[u8]) -> Option<u8>,
    {
        let mut items_retained = 0u32;
        let mut items_pruned = 0u32;
        let mut bytes_retained = 0u32;
        let mut bytes_pruned = 0u32;

        let header_size = if self.is_per_item_ttl() {
            TtlHeader::SIZE
        } else {
            BasicHeader::SIZE
        };

        let mut offset = 0u32;
        let write_offset = self.write_offset();

        while offset < write_offset {
            // Check bounds for header
            if offset as usize + header_size > self.capacity as usize {
                break;
            }

            let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

            // Parse header based on TTL mode
            let (key_len, optional_len, value_len, is_deleted) = if self.is_per_item_ttl() {
                match unsafe { TtlHeader::try_from_ptr(data_ptr) } {
                    Some(h) => (h.key_len(), h.optional_len(), h.value_len(), h.is_deleted()),
                    None => break,
                }
            } else {
                match unsafe { BasicHeader::try_from_ptr(data_ptr) } {
                    Some(h) => (h.key_len(), h.optional_len(), h.value_len(), h.is_deleted()),
                    None => break,
                }
            };

            // Calculate the item's stride -- what the append advanced the
            // write offset by. Advancing by anything else desyncs this walk.
            let item_size =
                header_size + optional_len as usize + key_len as usize + value_len as usize;
            let padded_size = self.item_stride(item_size);

            // Skip if already deleted
            if is_deleted {
                offset += padded_size;
                continue;
            }

            // Extract key
            let key_start = offset as usize + header_size + optional_len as usize;
            let key_end = key_start + key_len as usize;
            if key_end > self.capacity as usize {
                break;
            }
            let key = unsafe {
                std::slice::from_raw_parts(self.data.as_ptr().add(key_start), key_len as usize)
            };

            // Get frequency for this key
            let freq = get_frequency(key).unwrap_or(0);

            if freq <= threshold {
                // Mark as deleted - use mark_deleted to properly update live_items/live_bytes
                let _ = self.mark_deleted(offset, key);
                items_pruned += 1;
                bytes_pruned += padded_size;
            } else {
                items_retained += 1;
                bytes_retained += padded_size;
            }

            offset += padded_size;
        }

        (items_retained, items_pruned, bytes_retained, bytes_pruned)
    }

    fn prune_collecting<F>(&self, threshold: u8, get_frequency: F) -> PruneCollectingResult
    where
        F: Fn(&[u8]) -> Option<u8>,
    {
        let mut items_retained = 0u32;
        let mut items_pruned = 0u32;
        let mut bytes_retained = 0u32;
        let mut bytes_pruned = 0u32;
        let mut items_to_demote = Vec::new();

        let header_size = if self.is_per_item_ttl() {
            TtlHeader::SIZE
        } else {
            BasicHeader::SIZE
        };

        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();

        let mut offset = 0u32;
        let write_offset = self.write_offset();

        while offset < write_offset {
            // Check bounds for header
            if offset as usize + header_size > self.capacity as usize {
                break;
            }

            let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

            // Parse header based on TTL mode
            let (key_len, optional_len, value_len, is_deleted, expire_at) =
                if self.is_per_item_ttl() {
                    match unsafe { TtlHeader::try_from_ptr(data_ptr) } {
                        Some(h) => (
                            h.key_len(),
                            h.optional_len(),
                            h.value_len(),
                            h.is_deleted(),
                            h.expire_at(),
                        ),
                        None => break,
                    }
                } else {
                    match unsafe { BasicHeader::try_from_ptr(data_ptr) } {
                        Some(h) => (
                            h.key_len(),
                            h.optional_len(),
                            h.value_len(),
                            h.is_deleted(),
                            self.expire_at.load(Ordering::Acquire),
                        ),
                        None => break,
                    }
                };

            // Calculate the item's stride -- what the append advanced the
            // write offset by. Advancing by anything else desyncs this walk.
            let item_size =
                header_size + optional_len as usize + key_len as usize + value_len as usize;
            let padded_size = self.item_stride(item_size);

            // Skip if already deleted
            if is_deleted {
                offset += padded_size;
                continue;
            }

            // Extract key, value, optional
            let optional_start = offset as usize + header_size;
            let key_start = optional_start + optional_len as usize;
            let value_start = key_start + key_len as usize;
            let value_end = value_start + value_len as usize;

            if value_end > self.capacity as usize {
                break;
            }

            let optional = if optional_len > 0 {
                unsafe {
                    std::slice::from_raw_parts(
                        self.data.as_ptr().add(optional_start),
                        optional_len as usize,
                    )
                }
            } else {
                &[]
            };
            let key = unsafe {
                std::slice::from_raw_parts(self.data.as_ptr().add(key_start), key_len as usize)
            };
            let value = unsafe {
                std::slice::from_raw_parts(self.data.as_ptr().add(value_start), value_len as usize)
            };

            // Get frequency for this key
            let freq = get_frequency(key).unwrap_or(0);

            if freq <= threshold {
                // Collect item for demotion
                let ttl_secs = expire_at.saturating_sub(now);
                items_to_demote.push((key.to_vec(), value.to_vec(), optional.to_vec(), ttl_secs));

                // Mark as deleted
                let _ = self.mark_deleted(offset, key);
                items_pruned += 1;
                bytes_pruned += padded_size;
            } else {
                items_retained += 1;
                bytes_retained += padded_size;
            }

            offset += padded_size;
        }

        (
            items_retained,
            items_pruned,
            bytes_retained,
            bytes_pruned,
            items_to_demote,
        )
    }
}

// -----------------------------------------------------------------------------
// SegmentIter implementation
// -----------------------------------------------------------------------------

use crate::segment::SegmentIter;

impl SegmentIter for SliceSegment<'_> {
    fn for_each_item<F>(&self, mut callback: F)
    where
        F: FnMut(u32, &[u8], &[u8], &[u8], bool) -> bool,
    {
        let header_size = if self.is_per_item_ttl() {
            TtlHeader::SIZE
        } else {
            BasicHeader::SIZE
        };

        let mut offset = 0u32;
        let write_offset = self.write_offset();

        while offset < write_offset {
            // Check bounds for header
            if offset as usize + header_size > self.capacity as usize {
                break;
            }

            let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

            // Parse header based on TTL mode
            let (key_len, optional_len, value_len, is_deleted) = if self.is_per_item_ttl() {
                match unsafe { TtlHeader::try_from_ptr(data_ptr) } {
                    Some(h) => (h.key_len(), h.optional_len(), h.value_len(), h.is_deleted()),
                    None => break,
                }
            } else {
                match unsafe { BasicHeader::try_from_ptr(data_ptr) } {
                    Some(h) => (h.key_len(), h.optional_len(), h.value_len(), h.is_deleted()),
                    None => break,
                }
            };

            // Calculate the item's stride -- what the append advanced the
            // write offset by. Advancing by anything else desyncs this walk.
            let item_size =
                header_size + optional_len as usize + key_len as usize + value_len as usize;
            let padded_size = self.item_stride(item_size);

            // Extract key, value, optional
            let optional_start = offset as usize + header_size;
            let key_start = optional_start + optional_len as usize;
            let value_start = key_start + key_len as usize;
            let value_end = value_start + value_len as usize;

            if value_end > self.capacity as usize {
                break;
            }

            let optional = if optional_len > 0 {
                unsafe {
                    std::slice::from_raw_parts(
                        self.data.as_ptr().add(optional_start),
                        optional_len as usize,
                    )
                }
            } else {
                &[]
            };
            let key = unsafe {
                std::slice::from_raw_parts(self.data.as_ptr().add(key_start), key_len as usize)
            };
            let value = unsafe {
                std::slice::from_raw_parts(self.data.as_ptr().add(value_start), value_len as usize)
            };

            // Call callback
            if !callback(offset, key, value, optional, is_deleted) {
                break;
            }

            offset += padded_size;
        }
    }
}

// -----------------------------------------------------------------------------
// Loom concurrency tests
// -----------------------------------------------------------------------------

#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use crate::state::{INVALID_SEGMENT_ID, Metadata, State};
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use loom::thread;

    /// Test concurrent state transitions using CAS on packed metadata.
    #[test]
    fn test_concurrent_state_transition() {
        loom::model(|| {
            // Simulate segment metadata atomic
            let metadata = Arc::new(AtomicU64::new(Metadata::new(State::Sealed).pack()));

            let m1 = metadata.clone();
            let t1 = thread::spawn(move || {
                // Try to transition from Sealed -> Draining
                let current = Metadata::new(State::Sealed).pack();
                let new = Metadata::new(State::Draining).pack();
                m1.compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            });

            let m2 = metadata.clone();
            let t2 = thread::spawn(move || {
                // Try same transition concurrently
                let current = Metadata::new(State::Sealed).pack();
                let new = Metadata::new(State::Draining).pack();
                m2.compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Exactly one should succeed
            assert_eq!(
                [r1, r2].iter().filter(|&&x| x).count(),
                1,
                "Exactly one state transition should succeed"
            );

            // Final state should be Draining
            let final_meta = Metadata::unpack(metadata.load(Ordering::Acquire));
            assert_eq!(final_meta.state, State::Draining);
        });
    }

    /// Test concurrent ref_count increment/decrement.
    #[test]
    fn test_concurrent_ref_count() {
        loom::model(|| {
            let ref_count = Arc::new(AtomicU32::new(0));

            let rc1 = ref_count.clone();
            let t1 = thread::spawn(move || {
                rc1.fetch_add(1, Ordering::AcqRel);
            });

            let rc2 = ref_count.clone();
            let t2 = thread::spawn(move || {
                rc2.fetch_add(1, Ordering::AcqRel);
            });

            t1.join().unwrap();
            t2.join().unwrap();

            // Both increments should have happened
            assert_eq!(ref_count.load(Ordering::Acquire), 2);

            // Now decrement
            let rc3 = ref_count.clone();
            let t3 = thread::spawn(move || {
                rc3.fetch_sub(1, Ordering::AcqRel);
            });

            let rc4 = ref_count.clone();
            let t4 = thread::spawn(move || {
                rc4.fetch_sub(1, Ordering::AcqRel);
            });

            t3.join().unwrap();
            t4.join().unwrap();

            // Should be back to 0
            assert_eq!(ref_count.load(Ordering::Acquire), 0);
        });
    }

    /// Test concurrent write_offset CAS (simulating reserve_space).
    #[test]
    fn test_concurrent_write_offset_cas() {
        loom::model(|| {
            let write_offset = Arc::new(AtomicU32::new(0));
            let capacity: u32 = 1000;

            let wo1 = write_offset.clone();
            let t1 = thread::spawn(move || {
                let size = 100u32;
                loop {
                    let current = wo1.load(Ordering::Acquire);
                    let new_offset = current + size;
                    if new_offset > capacity {
                        return None;
                    }
                    match wo1.compare_exchange(
                        current,
                        new_offset,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return Some(current),
                        Err(_) => continue,
                    }
                }
            });

            let wo2 = write_offset.clone();
            let t2 = thread::spawn(move || {
                let size = 100u32;
                loop {
                    let current = wo2.load(Ordering::Acquire);
                    let new_offset = current + size;
                    if new_offset > capacity {
                        return None;
                    }
                    match wo2.compare_exchange(
                        current,
                        new_offset,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return Some(current),
                        Err(_) => continue,
                    }
                }
            });

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Both should succeed with different offsets
            assert!(r1.is_some());
            assert!(r2.is_some());
            assert_ne!(r1, r2);

            // Final offset should be 200
            assert_eq!(write_offset.load(Ordering::Acquire), 200);
        });
    }

    /// Test reader-writer pattern (ref_count check before state transition).
    #[test]
    fn test_reader_writer_pattern() {
        loom::model(|| {
            let ref_count = Arc::new(AtomicU32::new(0));
            let metadata = Arc::new(AtomicU64::new(Metadata::new(State::Live).pack()));

            // Reader: increment ref_count if state is readable
            let rc1 = ref_count.clone();
            let m1 = metadata.clone();
            let t1 = thread::spawn(move || {
                let meta = Metadata::unpack(m1.load(Ordering::Acquire));
                if meta.state.is_readable() {
                    rc1.fetch_add(1, Ordering::AcqRel);
                    true
                } else {
                    false
                }
            });

            // Writer: transition state if ref_count is 0
            let rc2 = ref_count.clone();
            let m2 = metadata.clone();
            let t2 = thread::spawn(move || {
                // Check ref_count first
                if rc2.load(Ordering::Acquire) == 0 {
                    let current = Metadata::new(State::Live).pack();
                    let new = Metadata::new(State::Sealed).pack();
                    m2.compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                } else {
                    false
                }
            });

            let reader_acquired = t1.join().unwrap();
            let _writer_transitioned = t2.join().unwrap();

            // Due to race, we can have various outcomes:
            // 1. Reader acquired ref, writer failed (ref_count > 0)
            // 2. Writer transitioned first, reader failed (state not readable)
            // 3. Writer transitioned while reader was checking (data race, but safe)
            // The key is that we don't have undefined behavior
            let final_ref = ref_count.load(Ordering::Acquire);
            let final_meta = Metadata::unpack(metadata.load(Ordering::Acquire));

            // If reader acquired, ref_count should be 1
            if reader_acquired {
                assert_eq!(final_ref, 1);
            }

            // State should be either Live or Sealed
            assert!(final_meta.state == State::Live || final_meta.state == State::Sealed);
        });
    }

    /// Test metadata pack/unpack atomicity.
    #[test]
    fn test_metadata_atomic_update() {
        loom::model(|| {
            let metadata = Arc::new(AtomicU64::new(
                Metadata::with_chain(State::Live, Some(10), Some(20)).pack(),
            ));

            // Thread 1: Update next pointer
            let m1 = metadata.clone();
            let t1 = thread::spawn(move || {
                loop {
                    let current = m1.load(Ordering::Acquire);
                    let mut meta = Metadata::unpack(current);
                    meta.next = 30;
                    let new = meta.pack();
                    if m1
                        .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        break;
                    }
                }
            });

            // Thread 2: Update prev pointer
            let m2 = metadata.clone();
            let t2 = thread::spawn(move || {
                loop {
                    let current = m2.load(Ordering::Acquire);
                    let mut meta = Metadata::unpack(current);
                    meta.prev = 40;
                    let new = meta.pack();
                    if m2
                        .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        break;
                    }
                }
            });

            t1.join().unwrap();
            t2.join().unwrap();

            // Both updates should have been applied (possibly with retries)
            let final_meta = Metadata::unpack(metadata.load(Ordering::Acquire));
            assert_eq!(final_meta.next, 30);
            assert_eq!(final_meta.prev, 40);
            assert_eq!(final_meta.state, State::Live);
        });
    }

    /// Test three threads trying the same state transition.
    ///
    /// Only one should succeed in transitioning from Sealed -> Draining.
    #[test]
    fn test_three_way_state_transition() {
        loom::model(|| {
            let metadata = Arc::new(AtomicU64::new(Metadata::new(State::Sealed).pack()));

            let m1 = metadata.clone();
            let m2 = metadata.clone();
            let m3 = metadata.clone();

            let try_transition = |m: Arc<AtomicU64>| -> bool {
                let current = Metadata::new(State::Sealed).pack();
                let new = Metadata::new(State::Draining).pack();
                m.compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            };

            let t1 = thread::spawn(move || try_transition(m1));
            let t2 = thread::spawn(move || try_transition(m2));
            let t3 = thread::spawn(move || try_transition(m3));

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();
            let r3 = t3.join().unwrap();

            // Exactly one should succeed
            let successes = [r1, r2, r3].iter().filter(|&&x| x).count();
            assert_eq!(successes, 1, "Exactly one state transition should succeed");

            // Final state should be Draining
            let final_meta = Metadata::unpack(metadata.load(Ordering::Acquire));
            assert_eq!(final_meta.state, State::Draining);
        });
    }

    /// Test three threads reserving space concurrently.
    ///
    /// All should get non-overlapping offsets.
    #[test]
    fn test_three_way_write_offset_cas() {
        loom::model(|| {
            let write_offset = Arc::new(AtomicU32::new(0));
            const CAPACITY: u32 = 1000;

            let reserve_space = |wo: Arc<AtomicU32>, size: u32| -> Option<u32> {
                loop {
                    let current = wo.load(Ordering::Acquire);
                    let new_offset = current + size;
                    if new_offset > CAPACITY {
                        return None;
                    }
                    match wo.compare_exchange(
                        current,
                        new_offset,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return Some(current),
                        Err(_) => continue,
                    }
                }
            };

            let wo1 = write_offset.clone();
            let wo2 = write_offset.clone();
            let wo3 = write_offset.clone();

            let t1 = thread::spawn(move || reserve_space(wo1, 100));
            let t2 = thread::spawn(move || reserve_space(wo2, 100));
            let t3 = thread::spawn(move || reserve_space(wo3, 100));

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();
            let r3 = t3.join().unwrap();

            // All should succeed
            assert!(r1.is_some());
            assert!(r2.is_some());
            assert!(r3.is_some());

            // All should have different starting offsets
            let offsets = [r1.unwrap(), r2.unwrap(), r3.unwrap()];
            assert_ne!(offsets[0], offsets[1]);
            assert_ne!(offsets[0], offsets[2]);
            assert_ne!(offsets[1], offsets[2]);

            // Final offset should be 300
            assert_eq!(write_offset.load(Ordering::Acquire), 300);
        });
    }

    /// Test three concurrent ref_count operations.
    ///
    /// Tests increment/decrement under higher contention.
    #[test]
    fn test_three_way_ref_count() {
        loom::model(|| {
            let ref_count = Arc::new(AtomicU32::new(0));

            let rc1 = ref_count.clone();
            let rc2 = ref_count.clone();
            let rc3 = ref_count.clone();

            // All three threads increment then decrement
            let t1 = thread::spawn(move || {
                rc1.fetch_add(1, Ordering::AcqRel);
                rc1.fetch_sub(1, Ordering::AcqRel);
            });

            let t2 = thread::spawn(move || {
                rc2.fetch_add(1, Ordering::AcqRel);
                rc2.fetch_sub(1, Ordering::AcqRel);
            });

            let t3 = thread::spawn(move || {
                rc3.fetch_add(1, Ordering::AcqRel);
                rc3.fetch_sub(1, Ordering::AcqRel);
            });

            t1.join().unwrap();
            t2.join().unwrap();
            t3.join().unwrap();

            // Should be back to 0
            assert_eq!(ref_count.load(Ordering::Acquire), 0);
        });
    }

    /// Test two readers and one writer with state transition.
    ///
    /// Two readers check state and increment ref_count if readable.
    /// One writer transitions state after checking ref_count.
    ///
    /// Uses bounded preemption to keep state space tractable.
    #[test]
    fn test_two_readers_one_writer_segment() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let ref_count = Arc::new(AtomicU32::new(0));
            let metadata = Arc::new(AtomicU64::new(Metadata::new(State::Live).pack()));

            let rc1 = ref_count.clone();
            let m1 = metadata.clone();
            let rc2 = ref_count.clone();
            let m2 = metadata.clone();
            let rc3 = ref_count.clone();
            let m3 = metadata.clone();

            // Reader 1: check state, increment ref if readable
            let t1 = thread::spawn(move || {
                let meta = Metadata::unpack(m1.load(Ordering::Acquire));
                if meta.state.is_readable() {
                    rc1.fetch_add(1, Ordering::AcqRel);
                    // Simulate reading
                    let _ = m1.load(Ordering::Acquire);
                    rc1.fetch_sub(1, Ordering::AcqRel);
                    true
                } else {
                    false
                }
            });

            // Reader 2: same pattern
            let t2 = thread::spawn(move || {
                let meta = Metadata::unpack(m2.load(Ordering::Acquire));
                if meta.state.is_readable() {
                    rc2.fetch_add(1, Ordering::AcqRel);
                    // Simulate reading
                    let _ = m2.load(Ordering::Acquire);
                    rc2.fetch_sub(1, Ordering::AcqRel);
                    true
                } else {
                    false
                }
            });

            // Writer: transition to Sealed if no readers
            let t3 = thread::spawn(move || {
                if rc3.load(Ordering::Acquire) == 0 {
                    let current = Metadata::new(State::Live).pack();
                    let new = Metadata::new(State::Sealed).pack();
                    m3.compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                } else {
                    false
                }
            });

            let _ = t1.join().unwrap();
            let _ = t2.join().unwrap();
            let _ = t3.join().unwrap();

            // After all complete, ref_count should be 0
            assert_eq!(ref_count.load(Ordering::Acquire), 0);

            // State should be either Live or Sealed
            let final_meta = Metadata::unpack(metadata.load(Ordering::Acquire));
            assert!(final_meta.state == State::Live || final_meta.state == State::Sealed);
        });
    }

    /// The counterpart of `shuttle_tests::shuttle_seqcst_store_buffering_forbidden`,
    /// kept here so the claim that loom cannot model the `SeqCst` total order
    /// is checkable rather than asserted.
    ///
    /// This is the textbook SB litmus with **every access `SeqCst`**. Real
    /// hardware and the C++/Rust model both forbid `r1 == 0 && r2 == 0`.
    /// loom reports it anyway: it models `SeqCst` as acquire/release and has
    /// no notion of the single total order. That is why every loom model in
    /// this file asserts only SC-independent properties, and why the shuttle
    /// suite exists.
    ///
    /// `#[ignore]` because it is *expected* to fail -- it documents a tool
    /// limitation, not a defect. Run it deliberately:
    ///
    /// ```text
    /// cargo test -p cache-core --features loom -- --ignored loom_seqcst
    /// ```
    ///
    /// If it ever *passes*, loom has gained SC modelling and the division of
    /// labour between the two suites should be revisited.
    #[test]
    #[ignore = "documents that loom cannot model the SeqCst total order; expected to fail"]
    fn loom_seqcst_store_buffering_is_reported_anyway() {
        loom::model(|| {
            let x = Arc::new(AtomicU32::new(0));
            let y = Arc::new(AtomicU32::new(0));

            let t1 = {
                let x = x.clone();
                let y = y.clone();
                thread::spawn(move || {
                    x.store(1, Ordering::SeqCst);
                    y.load(Ordering::SeqCst)
                })
            };
            let t2 = {
                let x = x.clone();
                let y = y.clone();
                thread::spawn(move || {
                    y.store(1, Ordering::SeqCst);
                    x.load(Ordering::SeqCst)
                })
            };

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();
            assert!(
                !(r1 == 0 && r2 == 0),
                "store-buffering outcome observed under SeqCst"
            );
        });
    }

    /// Item publication: the `Release` on the state store is what makes the
    /// item bytes written before it visible to a reader that `Acquire`-loads
    /// the state.
    ///
    /// This is the half shuttle structurally cannot check. Shuttle executes
    /// sequentially consistently, so it treats a `Relaxed` store exactly like
    /// a `SeqCst` one and stays green however weak the orderings get; loom
    /// models the release/acquire edge and reports the stale read. Degrading
    /// either the writer's `Release` store or the reader's `Acquire` load to
    /// `Relaxed` turns this model red, which is the standing demonstration
    /// that the loom suite still covers ordering *strength*.
    ///
    /// `payload` stands for the item bytes an append writes into segment
    /// memory before `cas_metadata` publishes the new state; the reader is
    /// any acquire site, which gates on the state before touching bytes.
    #[test]
    fn test_publication_release_makes_item_bytes_visible() {
        loom::model(|| {
            let payload = Arc::new(AtomicU32::new(0));
            let metadata = Arc::new(AtomicU64::new(Metadata::new(State::Reserved).pack()));

            let writer = {
                let p = payload.clone();
                let m = metadata.clone();
                thread::spawn(move || {
                    // write the item bytes ...
                    p.store(0xC0FFEE, Ordering::Relaxed);
                    // ... then publish the state that makes them reachable
                    m.store(Metadata::new(State::Live).pack(), Ordering::Release);
                })
            };

            let reader = {
                let p = payload.clone();
                let m = metadata.clone();
                thread::spawn(move || {
                    if Metadata::unpack(m.load(Ordering::Acquire)).state == State::Live {
                        assert_eq!(
                            p.load(Ordering::Relaxed),
                            0xC0FFEE,
                            "a reader admitted by the published state read item bytes \
                             that were not yet visible -- the publication store's \
                             Release (or the reader's Acquire) is too weak"
                        );
                    }
                })
            };

            writer.join().unwrap();
            reader.join().unwrap();
        });
    }

    /// Mirror of `SliceSegment::release_ref` -- the #127 back-out handoff.
    fn model_release_ref(rc: &AtomicU32, m: &AtomicU64) -> u32 {
        let prev = rc.fetch_sub(1, Ordering::Release);
        if prev == 1 {
            loom::sync::atomic::fence(Ordering::Acquire);
            if Metadata::unpack(m.load(Ordering::Acquire)).state == State::AwaitingRelease {
                model_release_condemned(m);
            }
        }
        prev
    }

    /// Mirror of `SliceSegment::release_condemned` -- state-only check, one CAS.
    fn model_release_condemned(m: &AtomicU64) -> bool {
        let current = m.load(Ordering::Acquire);
        let current_meta = Metadata::unpack(current);
        if current_meta.state != State::AwaitingRelease {
            return false;
        }
        let new_meta = current_meta
            .with_state(State::Free)
            .with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID)
            .bump_incarnation();
        m.compare_exchange(
            current,
            new_meta.pack(),
            Ordering::Release,
            Ordering::Acquire,
        )
        .is_ok()
    }

    /// Mirror of the fresh-acquire gate every read-entry site now uses.
    fn model_admits_fresh_reader(state: State) -> bool {
        state.is_readable() && !state.is_condemned()
    }

    /// Part 2 of #127: a reader turned away by the condemned gate *after* it
    /// has taken its reference must hand the segment on, not strand it.
    ///
    /// Part 1 makes this path reachable. Before it, `AwaitingRelease` passed
    /// the post-increment re-check, so a back-out there could not happen and a
    /// bare `ref_count.fetch_sub` was harmless. Now the back-out is the common
    /// case, and the last reference out is the only one that can free the
    /// segment -- `SliceSegment::release_ref` is where that happens.
    ///
    /// The evictor here deliberately does **not** run its
    /// `ref_count() == 0 && release_condemned()` race fix: the point is to pin
    /// the reader's obligation, so nothing else may discharge it.
    ///
    /// Scope, deliberately narrow. This does not also assert "a reader holding
    /// a reference never sees `Free`/`Locked`", because modelling the evictor's
    /// `ref_count` read alongside the reader's state re-check reopens the
    /// store-buffer question from crucible#109: reader stores `ref_count` then
    /// loads state, evictor stores state then loads `ref_count`, and loom lets
    /// both loads come back stale (with `SeqCst` throughout, too). That window
    /// predates #127 and is not what this change is about; pinning it here
    /// would make the test unadjudicable for the property it exists to check.
    #[test]
    fn test_condemned_backout_hands_the_segment_on() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let ref_count = Arc::new(AtomicU32::new(0));
            let metadata = Arc::new(AtomicU64::new(Metadata::new(State::Sealed).pack()));

            // Reader: the fresh-acquire gate every read-entry site now uses,
            // backed out through `release_ref`.
            let rc1 = ref_count.clone();
            let m1 = metadata.clone();
            let reader = thread::spawn(move || {
                let state = Metadata::unpack(m1.load(Ordering::Acquire)).state;
                if !model_admits_fresh_reader(state) {
                    return false;
                }
                rc1.fetch_add(1, Ordering::Acquire);
                let state_after = Metadata::unpack(m1.load(Ordering::Acquire)).state;
                if !model_admits_fresh_reader(state_after) {
                    let prev = model_release_ref(&rc1, &m1);
                    return state_after.is_condemned() && prev == 1;
                }
                model_release_ref(&rc1, &m1);
                false
            });

            // Evictor: drain the hashtable (a no-op here -- no fresh lookup can
            // find the segment afterwards) and condemn. No race fix.
            let m2 = metadata.clone();
            let evictor = thread::spawn(move || {
                let current = m2.load(Ordering::Acquire);
                let current_meta = Metadata::unpack(current);
                if current_meta.state != State::Sealed {
                    return;
                }
                let _ = m2.compare_exchange(
                    current,
                    current_meta.with_state(State::AwaitingRelease).pack(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            });

            let backed_out_as_last = reader.join().unwrap();
            evictor.join().unwrap();

            assert_eq!(ref_count.load(Ordering::Acquire), 0);

            if backed_out_as_last {
                let final_state = Metadata::unpack(metadata.load(Ordering::Acquire)).state;
                assert_ne!(
                    final_state,
                    State::AwaitingRelease,
                    "the last reference out backed off a condemned segment and left it \
                     stranded in AwaitingRelease with ref_count == 0 -- the back-out did \
                     not complete the handoff (#127 part 2)"
                );
            }
        });
    }

    /// #133's claim path, in the half loom can adjudicate: a verify reader
    /// turned away by the evictor's **`Locked` claim** must still complete the
    /// `AwaitingRelease` handoff when it turns out to be the last reference
    /// out.
    ///
    /// # Why this replaces `test_release_condemned_gate_respects_readers`
    ///
    /// That model was **vacuous**: it started in `AwaitingRelease`, where
    /// #127's `is_condemned()` gate turns the reader away at its *first*
    /// check, so `observed` was always `None` and the assertion body never
    /// ran. (Confirmed by replacing the body with a `panic!` and watching it
    /// still pass.) It was kept as a "regression guard"; a test that cannot
    /// fail is not one. Its property -- a pinned reader never coexists with a
    /// committed drain -- is SC-dependent and belongs to
    /// `shuttle_tests::shuttle_reader_never_coexists_with_committed_drain`,
    /// which loom structurally cannot assert.
    ///
    /// This model is live in the same place the old one was dead. It starts in
    /// `Draining`, which `State::admits_verify_reader` really does admit, so
    /// the reader reaches its `fetch_add`; the evictor then claims `Locked`
    /// and, seeing the pin, condemns from `Locked` -- the transition #133's
    /// claim-before-count order introduced. The reader's re-check sees
    /// `AwaitingRelease`, backs out as the last reference, and owes the free.
    ///
    /// # What is asserted, and what is not
    ///
    /// Only the SC-independent half: *if* the reader reports backing out as
    /// the last reference from a condemned segment, the segment must not be
    /// left in `AwaitingRelease`. Nothing sweeps that state, so a strand is a
    /// permanent leak of one segment. The evictor deliberately does **not**
    /// run its race fix, so nothing else can discharge the obligation.
    ///
    /// Not asserted: that a pinned reader never sees the claim committed.
    /// Modelling the evictor's `ref_count` read alongside the reader's
    /// re-check reopens the store-buffering question, and loom reports that
    /// outcome even with `SeqCst` throughout.
    #[test]
    fn test_claim_refused_backout_hands_the_segment_on() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let ref_count = Arc::new(AtomicU32::new(0));
            let metadata = Arc::new(AtomicU64::new(Metadata::new(State::Draining).pack()));

            // Reader: the key-verify acquire, through the production
            // predicate. `Draining` is admitted here and not by
            // `model_admits_fresh_reader` -- that width is exactly why the
            // evictor's claim has to be `Locked`.
            let rc1 = ref_count.clone();
            let m1 = metadata.clone();
            let reader = thread::spawn(move || {
                let state = Metadata::unpack(m1.load(Ordering::Acquire)).state;
                if !state.admits_verify_reader() {
                    return false;
                }
                rc1.fetch_add(1, Ordering::SeqCst);
                let state_after = Metadata::unpack(m1.load(Ordering::SeqCst)).state;
                if !state_after.admits_verify_reader() {
                    let prev = model_release_ref(&rc1, &m1);
                    return state_after.is_condemned() && prev == 1;
                }
                model_release_ref(&rc1, &m1);
                false
            });

            // Evictor: claim `Draining -> Locked`, and on finding the segment
            // pinned hand it back as `Locked -> AwaitingRelease`. No race fix,
            // so the reader's obligation is the only one that can discharge.
            let rc2 = ref_count.clone();
            let m2 = metadata.clone();
            let evictor = thread::spawn(move || {
                let current = m2.load(Ordering::Acquire);
                let current_meta = Metadata::unpack(current);
                if current_meta.state != State::Draining {
                    return;
                }
                if m2
                    .compare_exchange(
                        current,
                        current_meta.with_state(State::Locked).pack(),
                        Ordering::SeqCst,
                        Ordering::Acquire,
                    )
                    .is_err()
                {
                    return;
                }
                // Claim held: only now is the count read.
                if rc2.load(Ordering::SeqCst) == 0 {
                    return;
                }
                let current = m2.load(Ordering::Acquire);
                let current_meta = Metadata::unpack(current);
                if current_meta.state != State::Locked {
                    return;
                }
                let _ = m2.compare_exchange(
                    current,
                    current_meta.with_state(State::AwaitingRelease).pack(),
                    Ordering::SeqCst,
                    Ordering::Acquire,
                );
            });

            let backed_out_as_last = reader.join().unwrap();
            evictor.join().unwrap();

            assert_eq!(ref_count.load(Ordering::Acquire), 0);

            if backed_out_as_last {
                let final_state = Metadata::unpack(metadata.load(Ordering::Acquire)).state;
                assert_ne!(
                    final_state,
                    State::AwaitingRelease,
                    "the last reference out backed off a segment condemned from the \
                     evictor's claim and left it stranded in AwaitingRelease with \
                     ref_count == 0 -- the back-out did not complete the handoff"
                );
            }
        });
    }
}

/// Shuttle models: the sequentially-consistent complement to `loom_tests`.
///
/// The two suites are deliberately complementary and neither subsumes the
/// other.
///
/// **loom** models weak memory, so it is the only check that no ordering in
/// this file is *too weak* (degrade a `Release` to `Relaxed` and loom notices).
/// But it over-approximates `SeqCst`: it reports the store-buffering outcome
/// even for a pure-`SeqCst` litmus, which is why the loom models in this file
/// carry NOTEs saying the Dekker invariants are unassertable there.
///
/// **shuttle** executes sequentially consistently -- every ordering is treated
/// as `SeqCst` -- so exactly those SC-total-order invariants become assertable,
/// and asserting them is this module's entire purpose. The trade is symmetric:
/// shuttle can *never* catch an ordering that is too weak, because it cannot
/// tell `AcqRel` from `SeqCst`. That is the whole reason #129's ordering change
/// has no red proof in either tool, and why these models are justified as
/// *invariant* coverage, not as evidence for the ordering.
///
/// Keep both green. Every model here must be demonstrable red by some *logic*
/// mutation (neuter the condemned gate, delete a post-CAS re-check); a model
/// that no mutation can redden is vacuous.
///
/// # These models mirror the protocol; they do not execute it
///
/// Like the loom models above, and unlike cache-rs's shuttle suite, the models
/// here are hand-written mirrors against the checker's own atomics -- not
/// instantiations of `SliceSegment`. That is what lets them exist at all
/// (shuttle cannot instrument `std` atomics, and rebuilding the whole crate
/// against `shuttle::sync` would drag every `static` in `metrics.rs` into its
/// thread-local state), but it has a sharp consequence worth stating plainly:
///
/// **reddening a model by mutating a mirror proves a property of the mirror,
/// not of the shipped code.** A mutation has to be applied to production and
/// the *whole* suite re-run for the claim "this check is load-bearing and
/// covered" to mean anything. The exceptions are the few mirrors that call
/// production predicates directly -- `admits_guard_reader` here goes through
/// `State::is_readable`/`is_condemned`, so neutering those is a real
/// production mutation.
///
/// The deterministic counterparts that *do* pin production live in
/// `segment::tests` (`condemned_segment_is_not_freed_while_referenced` and
/// friends, over `segment::try_free_condemned`, the single commit point every
/// condemned-free path routes through), in
/// `slice_segment::tests::release_condemned_declines_while_a_guard_is_held`,
/// and in
/// `disk::io_uring_layer::tests::release_read_completes_the_handoff_and_returns_the_buffer_once`.
#[cfg(all(test, feature = "shuttle", not(feature = "loom")))]
mod shuttle_tests {
    use crate::state::{INVALID_SEGMENT_ID, Metadata, State};
    use crate::sync::shuttle_iters;
    use shuttle::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};
    use shuttle::thread;
    use std::sync::Arc;

    // ---- mirrors of the production protocol ------------------------------

    /// Mirror of the fresh-acquire gate the guard paths use
    /// (`get_item`, `get_item_verified`, `get_value_ref_raw`).
    fn admits_guard_reader(state: State) -> bool {
        state.is_readable() && !state.is_condemned()
    }

    /// Mirror of `SliceSegment::release_condemned`, re-validation included.
    fn model_release_condemned(rc: &AtomicU32, m: &AtomicU64) -> bool {
        let current = m.load(Ordering::SeqCst);
        let current_meta = Metadata::unpack(current);
        if current_meta.state != State::AwaitingRelease {
            return false;
        }
        // The caller's `prev == 1` only says the count *was* zero-after; a
        // reader may have re-pinned on a still-Sealed word since.
        //
        // Deleting this line reddens this model -- but that is a property of
        // the mirror, not of the shipped check. The production one lives in
        // `segment::try_free_condemned` and is pinned by
        // `segment::tests::condemned_segment_is_not_freed_while_referenced`.
        if rc.load(Ordering::SeqCst) != 0 {
            return false;
        }
        let new_meta = current_meta
            .with_state(State::Free)
            .with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID)
            .bump_incarnation();
        m.compare_exchange(current, new_meta.pack(), Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Mirror of `SliceSegment::release_ref` -- the last-reader handoff.
    /// Returns `true` if *this* call freed the condemned segment.
    fn model_release_ref(rc: &AtomicU32, m: &AtomicU64) -> bool {
        let prev = rc.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            fence(Ordering::SeqCst);
            if Metadata::unpack(m.load(Ordering::SeqCst)).state == State::AwaitingRelease {
                return model_release_condemned(rc, m);
            }
        }
        false
    }

    /// Mirror of the two-phase guard acquire: check the gate, `fetch_add`,
    /// re-check. Returns `true` if the pin is held on return.
    ///
    /// Still a mirror, so reddening it by deleting the re-check here proves a
    /// property of the mirror. What changed since #134 is that the production
    /// side is no longer uncovered: all five acquire entry points now route
    /// through the single `segment::try_acquire_pin`, whose window a test
    /// enters directly via `segment::interpose`. Deleting the re-check *in
    /// production* reddens
    /// `slice_segment::tests::{guard_acquire_refuses_a_condemn_published_inside_its_window,
    /// verify_acquire_refuses_a_clear_claim_published_inside_its_window,
    /// every_acquire_entry_point_routes_through_the_shared_body}`.
    fn model_try_acquire_guard(rc: &AtomicU32, m: &AtomicU64) -> bool {
        let state = Metadata::unpack(m.load(Ordering::SeqCst)).state;
        if !admits_guard_reader(state) {
            return false;
        }
        rc.fetch_add(1, Ordering::SeqCst);
        let state_after = Metadata::unpack(m.load(Ordering::SeqCst)).state;
        if !admits_guard_reader(state_after) {
            model_release_ref(rc, m);
            return false;
        }
        true
    }

    /// The same two phases with the *key-verify* predicate, straight off
    /// `State::admits_verify_reader` -- production, not a copy of it.
    ///
    /// The difference from [`model_try_acquire_guard`] is one state:
    /// `Draining` is admitted, because the demoter verifies keys on a segment
    /// it is draining. That is what makes `Draining` unusable as an exclusive
    /// claim and what #133 is about.
    fn model_try_acquire_verify(rc: &AtomicU32, m: &AtomicU64) -> bool {
        let state = Metadata::unpack(m.load(Ordering::SeqCst)).state;
        if !state.admits_verify_reader() {
            return false;
        }
        rc.fetch_add(1, Ordering::SeqCst);
        let state_after = Metadata::unpack(m.load(Ordering::SeqCst)).state;
        if !state_after.admits_verify_reader() {
            model_release_ref(rc, m);
            return false;
        }
        true
    }

    /// Mirror of `SliceSegment::cas_metadata` for a state-only transition.
    fn model_cas_state(m: &AtomicU64, expected: State, new: State) -> bool {
        let current = m.load(Ordering::SeqCst);
        let current_meta = Metadata::unpack(current);
        if current_meta.state != expected {
            return false;
        }
        m.compare_exchange(
            current,
            current_meta.with_state(new).pack(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
        .is_ok()
    }

    // ---- the positive control -------------------------------------------

    /// The tool premise, checked rather than assumed: shuttle forbids the
    /// store-buffering outcome for `SeqCst` accesses. **loom fails this exact
    /// litmus** -- it cannot model the SC total order, which is precisely why
    /// its models in this file assert only SC-independent halves.
    ///
    /// If this test ever fails, shuttle's execution model has regressed and
    /// every strong assertion below loses its foundation. Treat that as
    /// disabling the module, not as a bug in the models.
    #[test]
    fn shuttle_seqcst_store_buffering_forbidden() {
        shuttle::check_random(
            || {
                let x = Arc::new(AtomicU32::new(0));
                let y = Arc::new(AtomicU32::new(0));

                let t1 = {
                    let x = Arc::clone(&x);
                    let y = Arc::clone(&y);
                    thread::spawn(move || {
                        x.store(1, Ordering::SeqCst);
                        y.load(Ordering::SeqCst)
                    })
                };
                let t2 = {
                    let x = Arc::clone(&x);
                    let y = Arc::clone(&y);
                    thread::spawn(move || {
                        y.store(1, Ordering::SeqCst);
                        x.load(Ordering::SeqCst)
                    })
                };

                let r1 = t1.join().unwrap();
                let r2 = t2.join().unwrap();
                assert!(
                    !(r1 == 0 && r2 == 0),
                    "store-buffering outcome observed under SeqCst"
                );
            },
            shuttle_iters(50_000),
        );
    }

    // ---- #129 invariant (a) ---------------------------------------------

    /// **A reader holding a reference never coexists with a committed drain.**
    ///
    /// This is the first assertion #129 reports and the one the loom twin
    /// (`test_condemned_backout_hands_the_segment_on`) explicitly refuses to
    /// make: "modelling the evictor's `ref_count` read alongside the reader's
    /// state re-check reopens the store-buffer question ... loom lets both
    /// loads come back stale (with `SeqCst` throughout, too)". Under shuttle's
    /// SC execution it is assertable, and it holds.
    ///
    /// The evictor is `FifoLayer::process_evicted_segment_nonblocking` in
    /// full, in #133's order: claim `Sealed -> Draining`, sweep, claim
    /// `Draining -> Locked`, and only *then* load `ref_count` -- rewriting the
    /// segment's bytes if it is zero and condemning from `Locked` if it is
    /// not. `committed` stands for that rewrite. The soundness argument is
    /// that under the SC total order at most one of the two rechecks can be
    /// stale: a reader whose re-check saw an admitting state ordered its
    /// increment before the claim CAS, so the evictor's load cannot come back
    /// zero.
    ///
    /// Also asserts, at the end, that the segment did not strand in
    /// `AwaitingRelease` with `ref_count == 0` -- invariant (b) in the shape
    /// the layers actually produce it.
    #[test]
    fn shuttle_reader_never_coexists_with_committed_drain() {
        shuttle::check_random(
            || {
                let ref_count = Arc::new(AtomicU32::new(0));
                let metadata = Arc::new(AtomicU64::new(Metadata::new(State::Sealed).pack()));
                let committed = Arc::new(AtomicU32::new(0));
                let freed = Arc::new(AtomicU32::new(0));

                let readers: Vec<_> = (0..2)
                    .map(|_| {
                        let rc = Arc::clone(&ref_count);
                        let m = Arc::clone(&metadata);
                        let c = Arc::clone(&committed);
                        let f = Arc::clone(&freed);
                        thread::spawn(move || {
                            if !model_try_acquire_guard(&rc, &m) {
                                return;
                            }
                            // Pin held. This is where the caller reads item
                            // bytes out of the segment.
                            assert_eq!(
                                c.load(Ordering::SeqCst),
                                0,
                                "a pinned reader observed a COMMITTED drain -- the \
                                 evictor took Draining -> Locked and rewrote segment \
                                 bytes under a live reference (#129 invariant a)"
                            );
                            // `Locked` is deliberately not excluded: since
                            // #133 it means *claimed*, not *being cleared*.
                            // The evictor takes it before it knows whether
                            // anyone is pinned, and hands it back as
                            // `AwaitingRelease` when someone is -- so a pinned
                            // reader seeing `Locked` is precisely the case in
                            // which nothing gets rewritten. `committed` above
                            // is the invariant with teeth.
                            let observed = Metadata::unpack(m.load(Ordering::SeqCst)).state;
                            assert_ne!(
                                observed,
                                State::Free,
                                "a pinned reader observed the segment back on the free \
                                 queue while a reference was held"
                            );
                            if model_release_ref(&rc, &m) {
                                f.fetch_add(1, Ordering::SeqCst);
                            }
                        })
                    })
                    .collect();

                let evictor = {
                    let rc = Arc::clone(&ref_count);
                    let m = Arc::clone(&metadata);
                    let c = Arc::clone(&committed);
                    let f = Arc::clone(&freed);
                    thread::spawn(move || {
                        if !model_cas_state(&m, State::Sealed, State::Draining) {
                            return;
                        }
                        // Claim before counting (#133): `Draining -> Locked`
                        // is the store, and the `ref_count` load below is the
                        // load, of the condemner half of the Dekker pair.
                        if !model_cas_state(&m, State::Draining, State::Locked) {
                            return;
                        }
                        if rc.load(Ordering::SeqCst) == 0 {
                            c.store(1, Ordering::SeqCst);
                        } else {
                            model_cas_state(&m, State::Locked, State::AwaitingRelease);
                            // race fix
                            if rc.load(Ordering::SeqCst) == 0 && model_release_condemned(&rc, &m) {
                                f.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    })
                };

                for r in readers {
                    r.join().unwrap();
                }
                evictor.join().unwrap();

                assert_eq!(ref_count.load(Ordering::SeqCst), 0);
                assert!(
                    freed.load(Ordering::SeqCst) <= 1,
                    "the condemned segment was freed twice"
                );
                let final_state = Metadata::unpack(metadata.load(Ordering::SeqCst)).state;
                assert_ne!(
                    final_state,
                    State::AwaitingRelease,
                    "the segment stranded in AwaitingRelease with ref_count == 0 -- \
                     neither the last reader nor the evictor's race fix discharged \
                     the handoff (#129 invariant b)"
                );
            },
            shuttle_iters(50_000),
        );
    }

    // ---- #129 invariant (b) ---------------------------------------------

    /// **Exactly one of {evictor, last reader's guard drop} frees a condemned
    /// segment.**
    ///
    /// "At most once" is CAS uniqueness, which loom already covers. "At least
    /// once" -- no leak -- is the SC-dependent half: the evictor stores
    /// `AwaitingRelease` then loads `ref_count`, the reader stores `ref_count`
    /// then loads the state, and under the SC total order at most one of those
    /// loads can be stale, so at least one side sees the other and acts. That
    /// is the direction loom reports false violations for.
    ///
    /// Starts where `process_evicted_segment_nonblocking`'s deferred branch
    /// starts: `Draining` with one outstanding pin.
    #[test]
    fn shuttle_awaiting_release_exactly_one_free() {
        shuttle::check_random(
            || {
                let ref_count = Arc::new(AtomicU32::new(1));
                let metadata = Arc::new(AtomicU64::new(Metadata::new(State::Draining).pack()));
                let freed = Arc::new(AtomicU32::new(0));

                let evictor = {
                    let rc = Arc::clone(&ref_count);
                    let m = Arc::clone(&metadata);
                    let f = Arc::clone(&freed);
                    thread::spawn(move || {
                        assert!(model_cas_state(&m, State::Draining, State::AwaitingRelease));
                        // The race fix: the pin may have dropped before the CAS.
                        if rc.load(Ordering::SeqCst) == 0 && model_release_condemned(&rc, &m) {
                            f.fetch_add(1, Ordering::SeqCst);
                        }
                    })
                };

                let reader = {
                    let rc = Arc::clone(&ref_count);
                    let m = Arc::clone(&metadata);
                    let f = Arc::clone(&freed);
                    thread::spawn(move || {
                        if model_release_ref(&rc, &m) {
                            f.fetch_add(1, Ordering::SeqCst);
                        }
                    })
                };

                evictor.join().unwrap();
                reader.join().unwrap();

                assert_eq!(
                    freed.load(Ordering::SeqCst),
                    1,
                    "exactly one side must free the condemned segment: 0 is the \
                     AwaitingRelease strand loom cannot rule out, 2 is a double-free"
                );
                assert_eq!(
                    Metadata::unpack(metadata.load(Ordering::SeqCst)).state,
                    State::Free
                );
                assert_eq!(ref_count.load(Ordering::SeqCst), 0);
            },
            shuttle_iters(50_000),
        );
    }

    /// **A pinned reader never coexists with a committed drain, in the
    /// blocking claimer's shape.**
    ///
    /// The other production drain path: `process_evicted_segment` claims
    /// `Sealed -> Draining`, then takes `Draining -> Locked`, and only then
    /// *waits* -- `layer::claim_and_wait_for_readers`, which spins on
    /// `ref_count_seqcst()` with the claim already published -- before
    /// rewriting the segment's bytes. `committed` stands for that rewrite.
    ///
    /// The claim has to come first here too (#133). `Draining` still admits
    /// key-verify readers, so a spin that converges to zero under it says
    /// nothing about the instant after: a fresh pin can land between the
    /// observation and the `Locked` CAS. Under `Locked` the count is
    /// monotonically non-increasing, so the spin terminates on a zero that
    /// stays true.
    ///
    /// Repeating the load does not rescue a weaker ordering here: the property
    /// needed is that an *observation of zero* cannot coexist with a reader
    /// whose re-check saw an admitting state, and only the SC total order
    /// gives that. Shuttle is the tool that can assert it; loom cannot.
    #[test]
    fn shuttle_readers_vs_waiting_drain_claimer() {
        shuttle::check_random(
            || {
                let ref_count = Arc::new(AtomicU32::new(0));
                let metadata = Arc::new(AtomicU64::new(Metadata::new(State::Sealed).pack()));
                let committed = Arc::new(AtomicU32::new(0));

                let readers: Vec<_> = (0..2)
                    .map(|_| {
                        let rc = Arc::clone(&ref_count);
                        let m = Arc::clone(&metadata);
                        let c = Arc::clone(&committed);
                        thread::spawn(move || {
                            if !model_try_acquire_guard(&rc, &m) {
                                return;
                            }
                            assert_eq!(
                                c.load(Ordering::SeqCst),
                                0,
                                "a pinned reader coexisted with a COMMITTED drain -- \
                                 wait_for_readers returned while a reference was live"
                            );
                            model_release_ref(&rc, &m);
                        })
                    })
                    .collect();

                let claimer = {
                    let rc = Arc::clone(&ref_count);
                    let m = Arc::clone(&metadata);
                    let c = Arc::clone(&committed);
                    thread::spawn(move || {
                        if !model_cas_state(&m, State::Sealed, State::Draining) {
                            return;
                        }
                        // layer::claim_and_wait_for_readers: claim, then wait.
                        assert!(model_cas_state(&m, State::Draining, State::Locked));
                        while rc.load(Ordering::SeqCst) > 0 {
                            thread::yield_now();
                        }
                        c.store(1, Ordering::SeqCst);
                    })
                };

                for r in readers {
                    r.join().unwrap();
                }
                claimer.join().unwrap();

                assert_eq!(ref_count.load(Ordering::SeqCst), 0);
            },
            shuttle_iters(20_000),
        );
    }

    /// **A key-verify reader never coexists with a committed clear (#133).**
    ///
    /// The other three models here use `model_try_acquire_guard`, whose
    /// predicate excludes `Draining` -- so none of them can express #133 at
    /// all. This one uses `State::admits_verify_reader`, the production
    /// predicate the demoter's verifier goes through, which admits `Draining`
    /// deliberately. That width is the entire bug: `Draining` is not an
    /// exclusive claim, so an evictor that reads `ref_count` while holding
    /// only `Draining` is reading a number that can still go up.
    ///
    /// The evictor is `process_evicted_segment_nonblocking`'s tail in the
    /// order this branch introduced: claim `Draining -> Locked`, *then* load
    /// `ref_count`, and either clear (`committed`) or condemn from `Locked`.
    ///
    /// Not vacuous, and not green by construction: reverse the two evictor
    /// steps in this model --
    ///
    /// ```ignore
    /// let unpinned = rc.load(Ordering::SeqCst) == 0;
    /// let claimed = model_cas_state(&m, State::Draining, State::Locked);
    /// if claimed && unpinned { ... }
    /// ```
    ///
    /// -- and shuttle reports a pinned reader observing `committed == 1`
    /// within a few thousand schedules. That is a *mirror* mutation and proves
    /// a property of the mirror; the production red proof for the same
    /// inversion is
    /// `fifo_layer::tests::eviction_does_not_recycle_a_segment_pinned_while_it_was_claiming`.
    #[test]
    fn shuttle_verify_reader_never_coexists_with_a_committed_clear() {
        shuttle::check_random(
            || {
                let ref_count = Arc::new(AtomicU32::new(0));
                let metadata = Arc::new(AtomicU64::new(Metadata::new(State::Draining).pack()));
                let committed = Arc::new(AtomicU32::new(0));

                let readers: Vec<_> = (0..2)
                    .map(|_| {
                        let rc = Arc::clone(&ref_count);
                        let m = Arc::clone(&metadata);
                        let c = Arc::clone(&committed);
                        thread::spawn(move || {
                            if !model_try_acquire_verify(&rc, &m) {
                                return;
                            }
                            // Pin held. This is where `verify_key_at_offset`
                            // reads the item's bytes.
                            assert_eq!(
                                c.load(Ordering::SeqCst),
                                0,
                                "a pinned key-verify reader observed a COMMITTED clear -- \
                                 the evictor read ref_count while it held only `Draining`, \
                                 which still admits this reader (#133)"
                            );
                            // NOT asserted: that the observed state is never
                            // `Locked`. Since #133 `Locked` means *claimed*,
                            // not *being cleared* -- the evictor takes it
                            // before it knows whether anyone is pinned, and
                            // backs out to `AwaitingRelease` when someone is.
                            // A pinned reader can therefore see it, and that
                            // is exactly the case in which nothing is
                            // rewritten. `committed` above is the real
                            // invariant.
                            let observed = Metadata::unpack(m.load(Ordering::SeqCst)).state;
                            assert_ne!(
                                observed,
                                State::Free,
                                "a pinned key-verify reader observed the segment back on \
                                 the free queue"
                            );
                            model_release_ref(&rc, &m);
                        })
                    })
                    .collect();

                let evictor = {
                    let rc = Arc::clone(&ref_count);
                    let m = Arc::clone(&metadata);
                    let c = Arc::clone(&committed);
                    thread::spawn(move || {
                        // Claim before counting.
                        if !model_cas_state(&m, State::Draining, State::Locked) {
                            return;
                        }
                        if rc.load(Ordering::SeqCst) == 0 {
                            c.store(1, Ordering::SeqCst);
                        } else {
                            model_cas_state(&m, State::Locked, State::AwaitingRelease);
                            if rc.load(Ordering::SeqCst) == 0 {
                                model_release_condemned(&rc, &m);
                            }
                        }
                    })
                };

                for r in readers {
                    r.join().unwrap();
                }
                evictor.join().unwrap();

                assert_eq!(ref_count.load(Ordering::SeqCst), 0);
                assert_ne!(
                    Metadata::unpack(metadata.load(Ordering::SeqCst)).state,
                    State::AwaitingRelease,
                    "the segment stranded in AwaitingRelease with ref_count == 0"
                );
            },
            shuttle_iters(50_000),
        );
    }
}
