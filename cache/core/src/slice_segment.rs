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

    /// Number of times this segment was a merge destination.
    merge_count: AtomicU16,

    /// Generation counter - incremented on reuse to prevent ABA.
    generation: AtomicU16,

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
    pub unsafe fn new(
        pool_id: u8,
        is_per_item_ttl: bool,
        id: u32,
        data: *mut u8,
        len: usize,
    ) -> Self {
        debug_assert!(pool_id <= 3, "pool_id {} exceeds 2-bit limit", pool_id);
        debug_assert!(len <= u32::MAX as usize, "segment too large");

        let pool_flags = (pool_id & Self::POOL_ID_MASK)
            | if is_per_item_ttl {
                Self::PER_ITEM_TTL_BIT
            } else {
                0
            };

        let initial_meta = Metadata {
            next: INVALID_SEGMENT_ID,
            prev: INVALID_SEGMENT_ID,
            state: State::Free,
        };

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
            merge_count: AtomicU16::new(0),
            generation: AtomicU16::new(0),
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
        let padded_size = (item_size + 7) & !7; // 8-byte alignment

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
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::Expired);
        }

        // Validate offset
        if offset as usize + BasicHeader::SIZE > self.capacity as usize {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::InvalidOffset);
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

        // Parse header
        let header_bytes = unsafe { std::slice::from_raw_parts(data_ptr, BasicHeader::SIZE) };

        #[cfg(feature = "validation")]
        let header = BasicHeader::try_from_bytes(header_bytes);
        #[cfg(not(feature = "validation"))]
        let header = Some(BasicHeader::from_bytes_unchecked(header_bytes));

        let header = match header {
            Some(h) => h,
            None => {
                self.ref_count.fetch_sub(1, Ordering::Release);
                return Err(CacheError::Corrupted);
            }
        };

        if header.is_deleted() {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::ItemDeleted);
        }

        let item_size = header.padded_size();
        if offset as usize + item_size > self.capacity as usize {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::InvalidOffset);
        }

        let raw = unsafe { std::slice::from_raw_parts(data_ptr, item_size) };

        let optional_start = BasicHeader::SIZE;
        let optional_end = optional_start + header.optional_len() as usize;
        let key_start = optional_end;
        let key_end = key_start + header.key_len() as usize;
        let value_start = key_end;
        let value_end = value_start + header.value_len() as usize;

        if &raw[key_start..key_end] != key {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::KeyMismatch);
        }

        Ok(BasicItemGuard::new(
            &self.ref_count,
            &raw[key_start..key_end],
            &raw[value_start..value_end],
            &raw[optional_start..optional_end],
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
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::InvalidOffset);
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

        // Parse header
        let header_bytes = unsafe { std::slice::from_raw_parts(data_ptr, TtlHeader::SIZE) };

        #[cfg(feature = "validation")]
        let header = TtlHeader::try_from_bytes(header_bytes);
        #[cfg(not(feature = "validation"))]
        let header = Some(TtlHeader::from_bytes_unchecked(header_bytes));

        let header = match header {
            Some(h) => h,
            None => {
                self.ref_count.fetch_sub(1, Ordering::Release);
                return Err(CacheError::Corrupted);
            }
        };

        if header.is_deleted() {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::ItemDeleted);
        }

        // Check per-item expiration
        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        if header.is_expired(now) {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::Expired);
        }

        let item_size = header.padded_size();
        if offset as usize + item_size > self.capacity as usize {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::InvalidOffset);
        }

        let raw = unsafe { std::slice::from_raw_parts(data_ptr, item_size) };

        let optional_start = TtlHeader::SIZE;
        let optional_end = optional_start + header.optional_len() as usize;
        let key_start = optional_end;
        let key_end = key_start + header.key_len() as usize;
        let value_start = key_end;
        let value_end = value_start + header.value_len() as usize;

        if &raw[key_start..key_end] != key {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::KeyMismatch);
        }

        Ok(BasicItemGuard::new(
            &self.ref_count,
            &raw[key_start..key_end],
            &raw[value_start..value_end],
            &raw[optional_start..optional_end],
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
    pub fn get_value_ref_raw(
        &self,
        offset: u32,
        key: &[u8],
    ) -> Result<(*const AtomicU32, *const u8, usize), CacheError> {
        // Check state and increment ref count
        let state = self.state();
        if !state.is_readable() {
            return Err(CacheError::SegmentNotAccessible);
        }

        self.ref_count.fetch_add(1, Ordering::Acquire);

        // Double-check state after increment
        let state_after = self.state();
        if !state_after.is_readable() {
            self.ref_count.fetch_sub(1, Ordering::Release);
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
    fn get_value_ref_raw_basic(
        &self,
        offset: u32,
        key: &[u8],
    ) -> Result<(*const AtomicU32, *const u8, usize), CacheError> {
        // Check segment expiration
        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        let expire_at = self.expire_at.load(Ordering::Acquire);
        if expire_at > 0 && now >= expire_at {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::Expired);
        }

        // Validate offset
        if offset as usize + BasicHeader::SIZE > self.capacity as usize {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::InvalidOffset);
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

        // Parse header
        let header_bytes = unsafe { std::slice::from_raw_parts(data_ptr, BasicHeader::SIZE) };

        #[cfg(feature = "validation")]
        let header = BasicHeader::try_from_bytes(header_bytes);
        #[cfg(not(feature = "validation"))]
        let header = Some(BasicHeader::from_bytes_unchecked(header_bytes));

        let header = match header {
            Some(h) => h,
            None => {
                self.ref_count.fetch_sub(1, Ordering::Release);
                return Err(CacheError::Corrupted);
            }
        };

        if header.is_deleted() {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::ItemDeleted);
        }

        let item_size = header.padded_size();
        if offset as usize + item_size > self.capacity as usize {
            self.ref_count.fetch_sub(1, Ordering::Release);
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
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::KeyMismatch);
        }

        let ref_count_ptr = &self.ref_count as *const AtomicU32;
        let value_ptr = unsafe { data_ptr.add(value_start) };

        Ok((ref_count_ptr, value_ptr, value_len))
    }

    /// Get raw value reference for TtlHeader segments.
    fn get_value_ref_raw_ttl(
        &self,
        offset: u32,
        key: &[u8],
    ) -> Result<(*const AtomicU32, *const u8, usize), CacheError> {
        // Validate offset
        if offset as usize + TtlHeader::SIZE > self.capacity as usize {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::InvalidOffset);
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };

        // Parse header
        let header_bytes = unsafe { std::slice::from_raw_parts(data_ptr, TtlHeader::SIZE) };

        #[cfg(feature = "validation")]
        let header = TtlHeader::try_from_bytes(header_bytes);
        #[cfg(not(feature = "validation"))]
        let header = Some(TtlHeader::from_bytes_unchecked(header_bytes));

        let header = match header {
            Some(h) => h,
            None => {
                self.ref_count.fetch_sub(1, Ordering::Release);
                return Err(CacheError::Corrupted);
            }
        };

        if header.is_deleted() {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::ItemDeleted);
        }

        // Check item-level TTL
        let now = clocksource::coarse::UnixInstant::now()
            .duration_since(clocksource::coarse::UnixInstant::EPOCH)
            .as_secs();
        if header.is_expired(now) {
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::Expired);
        }

        let item_size = header.padded_size();
        if offset as usize + item_size > self.capacity as usize {
            self.ref_count.fetch_sub(1, Ordering::Release);
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
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::KeyMismatch);
        }

        let ref_count_ptr = &self.ref_count as *const AtomicU32;
        let value_ptr = unsafe { data_ptr.add(value_start) };

        Ok((ref_count_ptr, value_ptr, value_len))
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
        let header_bytes = unsafe { std::slice::from_raw_parts(data_ptr, BasicHeader::SIZE) };

        #[cfg(feature = "validation")]
        let header = BasicHeader::try_from_bytes(header_bytes)?;
        #[cfg(not(feature = "validation"))]
        let header = BasicHeader::from_bytes_unchecked(header_bytes);

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
        let header_bytes = unsafe { std::slice::from_raw_parts(data_ptr, TtlHeader::SIZE) };

        #[cfg(feature = "validation")]
        let header = TtlHeader::try_from_bytes(header_bytes)?;
        #[cfg(not(feature = "validation"))]
        let header = TtlHeader::from_bytes_unchecked(header_bytes);

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
    fn verify_key_at_offset(&self, offset: u32, key: &[u8], allow_deleted: bool) -> bool {
        self.verify_key_with_header(offset, key, allow_deleted)
            .is_some()
    }

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

            let header_bytes = unsafe { std::slice::from_raw_parts(data_ptr, TtlHeader::SIZE) };

            #[cfg(feature = "validation")]
            let header = TtlHeader::try_from_bytes(header_bytes)?;
            #[cfg(not(feature = "validation"))]
            let header = TtlHeader::from_bytes_unchecked(header_bytes);

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

            let raw = unsafe { std::slice::from_raw_parts(data_ptr, item_size) };
            if key_end <= raw.len() && &raw[key_start..key_end] == key {
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

        let new_meta = Metadata {
            next: INVALID_SEGMENT_ID,
            prev: INVALID_SEGMENT_ID,
            state: State::Reserved,
        };

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

            let new_meta = Metadata {
                next: INVALID_SEGMENT_ID,
                prev: INVALID_SEGMENT_ID,
                state: State::Free,
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

        let new_meta = Metadata {
            state: new_state,
            next: new_next.unwrap_or(current_meta.next),
            prev: new_prev.unwrap_or(current_meta.prev),
        };

        self.metadata
            .compare_exchange(
                current,
                new_meta.pack(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
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
            let header_bytes = unsafe { std::slice::from_raw_parts(data_ptr, TtlHeader::SIZE) };

            #[cfg(feature = "validation")]
            let header = TtlHeader::try_from_bytes(header_bytes)?;
            #[cfg(not(feature = "validation"))]
            let header = TtlHeader::from_bytes_unchecked(header_bytes);

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
        let padded_size = (item_size + 7) & !7; // 8-byte alignment

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
        let padded_size = (item_size + 7) & !7; // 8-byte alignment

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

        // Set deleted flag (byte 4, bit 6) without key verification
        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };
        let flags_ptr = unsafe { data_ptr.add(4) };

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
            State::Draining | State::Locked => return Ok(false),
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

        // Compute padded item size from header info
        let item_size = ((header_size
            + optional_len as usize
            + key_len as usize
            + value_len as usize
            + 7)
            & !7) as u32;

        // Set deleted flag (byte 4, bit 6)
        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };
        let flags_ptr = unsafe { data_ptr.add(4) };

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

// Implement SegmentGuard for zero-copy access
impl SegmentGuard for SliceSegment<'_> {
    type Guard<'a>
        = BasicItemGuard<'a>
    where
        Self: 'a;

    fn get_item(&self, offset: u32, key: &[u8]) -> Result<Self::Guard<'_>, CacheError> {
        // Check state and increment ref count
        let state = self.state();
        if !state.is_readable() {
            return Err(CacheError::SegmentNotAccessible);
        }

        self.ref_count.fetch_add(1, Ordering::Acquire);

        // Double-check state after increment
        let state_after = self.state();
        if !state_after.is_readable() {
            self.ref_count.fetch_sub(1, Ordering::Release);
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
        // Check state and increment ref count
        let state = self.state();
        if !state.is_readable() {
            return Err(CacheError::SegmentNotAccessible);
        }

        self.ref_count.fetch_add(1, Ordering::Acquire);

        // Double-check state after increment
        let state_after = self.state();
        if !state_after.is_readable() {
            self.ref_count.fetch_sub(1, Ordering::Release);
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
            self.ref_count.fetch_sub(1, Ordering::Release);
            return Err(CacheError::InvalidOffset);
        }

        let data_ptr = unsafe { self.data.as_ptr().add(offset as usize) };
        let raw = unsafe { std::slice::from_raw_parts(data_ptr, item_size) };

        // Compute slice boundaries from header info
        let optional_start = header_size;
        let optional_end = optional_start + optional_len as usize;
        let key_start = optional_end;
        let key_end = key_start + key_len as usize;
        let value_start = key_end;
        let value_end = value_start + value_len as usize;

        Ok(BasicItemGuard::new(
            &self.ref_count,
            &raw[key_start..key_end],
            &raw[value_start..value_end],
            &raw[optional_start..optional_end],
        ))
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::item::ItemGuard;
    use std::alloc::{Layout, alloc, dealloc};

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
        let segment = unsafe { SliceSegment::new(pool_id, is_per_item_ttl, id, ptr, size) };
        (segment, ptr, layout)
    }

    unsafe fn free_test_segment(ptr: *mut u8, layout: Layout) {
        unsafe {
            dealloc(ptr, layout);
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
// Loom concurrency tests
// -----------------------------------------------------------------------------

#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use crate::state::{Metadata, State};
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
}
