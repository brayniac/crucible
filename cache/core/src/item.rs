//! Item header types and guards.
//!
//! This module provides:
//! - [`BasicHeader`] - Header for items with segment-level TTL
//! - [`TtlHeader`] - Header for items with per-item TTL
//! - [`ItemGuard`] - Trait for zero-copy item data access

use crate::sync::{AtomicU32, Ordering};
use std::time::Duration;

// ============================================================================
// BasicHeader - Header with segment-level TTL
// ============================================================================

/// Item header for segments with segment-level TTL.
///
/// Items using this header inherit their TTL from the segment's expiration time.
/// This is more space-efficient when all items in a segment share the same TTL.
///
/// Layout with validation feature:
/// ```text
/// [0]    key_len: u8
/// [1]    flags: [is_numeric (1)][is_deleted (1)][optional_len (6)]
/// [2..6] value_len: u32
/// [6]    MAGIC0 (0xCA)
/// [7]    MAGIC1 (0xCE)
/// [8]    checksum
/// ```
///
/// Layout without validation:
/// ```text
/// [0]    key_len: u8
/// [1]    flags: [is_numeric (1)][is_deleted (1)][optional_len (6)]
/// [2..6] value_len: u32
/// ```
#[derive(Debug, Clone)]
pub struct BasicHeader {
    key_len: u8,
    optional_len: u8,
    is_deleted: bool,
    is_numeric: bool,
    value_len: u32,
}

impl BasicHeader {
    /// Magic bytes to detect valid headers (0xCA 0xCE = "CACHE").
    pub const MAGIC0: u8 = 0xCA;
    /// Second magic byte.
    pub const MAGIC1: u8 = 0xCE;

    /// Header size with validation feature.
    #[cfg(feature = "validation")]
    pub const SIZE: usize = 9;

    /// Header size without validation.
    #[cfg(not(feature = "validation"))]
    pub const SIZE: usize = 6;

    /// Maximum key length (8 bits).
    pub const MAX_KEY_LEN: usize = 0xFF;

    /// Maximum optional metadata length (6 bits).
    pub const MAX_OPTIONAL_LEN: usize = 0x3F;

    /// Maximum value length (32 bits).
    pub const MAX_VALUE_LEN: usize = u32::MAX as usize;

    /// Minimum valid item size for bounds checking.
    pub const MIN_ITEM_SIZE: usize = Self::SIZE;

    /// Create a new BasicHeader.
    pub fn new(key_len: u8, optional_len: u8, value_len: u32) -> Self {
        Self {
            key_len,
            optional_len,
            is_deleted: false,
            is_numeric: false,
            value_len,
        }
    }

    /// Create a new BasicHeader with all fields.
    pub fn with_flags(
        key_len: u8,
        optional_len: u8,
        value_len: u32,
        is_deleted: bool,
        is_numeric: bool,
    ) -> Self {
        Self {
            key_len,
            optional_len,
            is_deleted,
            is_numeric,
            value_len,
        }
    }

    /// Compute checksum for validation.
    /// Uses XOR of all content bytes plus magic bytes.
    #[cfg(feature = "validation")]
    #[inline(always)]
    fn compute_checksum(key_len: u8, optional_len: u8, value_len: u32, is_numeric: bool) -> u8 {
        // XOR all bytes together: MAGIC0 ^ MAGIC1 ^ key_len ^ optional_len ^ value_bytes ^ numeric_flag
        // MAGIC0 ^ MAGIC1 = 0xCA ^ 0xCE = 0x04 (constant)
        let mut checksum = 0x04_u8;
        checksum ^= key_len;
        checksum ^= optional_len;
        // XOR all 4 bytes of value_len (32 bits)
        checksum ^= (value_len as u8)
            ^ ((value_len >> 8) as u8)
            ^ ((value_len >> 16) as u8)
            ^ ((value_len >> 24) as u8);
        if is_numeric {
            checksum ^= 0x55;
        }
        checksum
    }

    /// Try to parse header from bytes, returning None if validation fails.
    #[inline]
    pub fn try_from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < Self::SIZE {
            return None;
        }

        let ptr = data.as_ptr();

        // Read key_len, flags, and value_len from new layout
        let key_len = unsafe { *ptr };
        let flags = unsafe { *ptr.add(1) };
        let value_len = unsafe { ptr.add(2).cast::<u32>().read_unaligned() };

        let optional_len = flags & 0x3F;
        let is_deleted = (flags & 0x40) != 0;
        let is_numeric = (flags & 0x80) != 0;

        #[cfg(feature = "validation")]
        {
            // Check magic bytes with single comparison
            let magic = unsafe { ptr.add(6).cast::<u16>().read_unaligned() };
            if magic != u16::from_ne_bytes([Self::MAGIC0, Self::MAGIC1]) {
                return None;
            }

            let stored_checksum = unsafe { *ptr.add(8) };
            let computed_checksum =
                Self::compute_checksum(key_len, optional_len, value_len, is_numeric);
            if stored_checksum != computed_checksum {
                return None;
            }
        }

        Some(Self {
            key_len,
            optional_len,
            is_deleted,
            is_numeric,
            value_len,
        })
    }

    /// Parse header from bytes without validation checks.
    /// Only use when you know the data is valid.
    ///
    /// # Safety assumption
    /// The data pointer must be 8-byte aligned (items are padded to 8-byte boundaries).
    #[inline(always)]
    #[cfg(not(feature = "validation"))]
    pub fn from_bytes_unchecked(data: &[u8]) -> Self {
        debug_assert!(data.len() >= Self::SIZE);
        let ptr = data.as_ptr();

        // Items are 8-byte aligned
        let key_len = unsafe { *ptr };
        let flags = unsafe { *ptr.add(1) };
        // value_len at offset 2 is NOT 4-byte aligned, must use unaligned read
        let value_len = unsafe { ptr.add(2).cast::<u32>().read_unaligned() };

        Self {
            key_len,
            optional_len: flags & 0x3F,
            is_deleted: (flags & 0x40) != 0,
            is_numeric: (flags & 0x80) != 0,
            value_len,
        }
    }

    /// Parse header from bytes, panicking if validation fails.
    pub fn from_bytes(data: &[u8]) -> Self {
        debug_assert!(data.len() >= Self::SIZE);
        Self::try_from_bytes(data).expect("Invalid BasicHeader")
    }

    /// Write header to bytes.
    pub fn to_bytes(&self, data: &mut [u8]) {
        debug_assert!(data.len() >= Self::SIZE);

        let mut flags = self.optional_len;

        if self.is_deleted {
            flags |= 0x40;
        }
        if self.is_numeric {
            flags |= 0x80;
        }

        data[0] = self.key_len;
        data[1] = flags;
        data[2..6].copy_from_slice(&self.value_len.to_ne_bytes());

        #[cfg(feature = "validation")]
        {
            data[6] = Self::MAGIC0;
            data[7] = Self::MAGIC1;
            data[8] = Self::compute_checksum(
                self.key_len,
                self.optional_len,
                self.value_len,
                self.is_numeric,
            );
        }
    }

    /// Calculate padded size rounded to 8-byte boundary.
    ///
    /// Uses checked arithmetic to detect overflow.
    pub fn padded_size(&self) -> usize {
        let size = Self::SIZE
            .checked_add(self.optional_len as usize)
            .and_then(|s| s.checked_add(self.key_len as usize))
            .and_then(|s| s.checked_add(self.value_len as usize))
            .and_then(|s| s.checked_add(7))
            .expect("Item size overflow");

        size & !7
    }

    /// Calculate padded size without overflow checks.
    ///
    /// This is faster than `padded_size()` but assumes the header fields
    /// are valid and won't overflow. Use in hot paths where the header
    /// has already been validated or comes from trusted storage.
    #[inline(always)]
    pub fn padded_size_unchecked(&self) -> usize {
        let size = Self::SIZE
            + self.optional_len as usize
            + self.key_len as usize
            + self.value_len as usize
            + 7;
        size & !7
    }

    /// Check if item is marked as deleted.
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }

    /// Get the key length.
    #[inline]
    pub fn key_len(&self) -> u8 {
        self.key_len
    }

    /// Get the optional data length.
    #[inline]
    pub fn optional_len(&self) -> u8 {
        self.optional_len
    }

    /// Get the value length.
    #[inline]
    pub fn value_len(&self) -> u32 {
        self.value_len
    }

    /// Check if value is numeric.
    #[inline]
    pub fn is_numeric(&self) -> bool {
        self.is_numeric
    }

    /// Get the byte range for the optional data within item data starting at `offset`.
    pub fn optional_range(&self, offset: u32) -> std::ops::Range<usize> {
        let offset = offset as usize;
        let start = offset + Self::SIZE;
        start..(start + self.optional_len as usize)
    }

    /// Get the byte range for the key within item data starting at `offset`.
    pub fn key_range(&self, offset: u32) -> std::ops::Range<usize> {
        let offset = offset as usize;
        let start = offset + Self::SIZE + self.optional_len as usize;
        start..(start + self.key_len as usize)
    }

    /// Get the byte range for the value within item data starting at `offset`.
    pub fn value_range(&self, offset: u32) -> std::ops::Range<usize> {
        let offset = offset as usize;
        let start = offset + Self::SIZE + self.optional_len as usize + self.key_len as usize;
        start..(start + self.value_len as usize)
    }
}

// ============================================================================
// TtlHeader - Header with per-item TTL
// ============================================================================

/// Item header with per-item TTL.
///
/// Used in FIFO layers where items have varying TTLs and cannot share
/// a segment-level expiration time.
///
/// Layout with validation feature:
/// ```text
/// [0]     key_len: u8
/// [1]     flags: [is_numeric (1)][is_deleted (1)][optional_len (6)]
/// [2..6]  value_len: u32
/// [6..10] expire_at: u32 (coarse seconds since epoch)
/// [10]    MAGIC0 (0xCA)
/// [11]    MAGIC1 (0xCE)
/// [12]    checksum
/// ```
///
/// Layout without validation:
/// ```text
/// [0]     key_len: u8
/// [1]     flags: [is_numeric (1)][is_deleted (1)][optional_len (6)]
/// [2..6]  value_len: u32
/// [6..10] expire_at: u32 (coarse seconds since epoch)
/// ```
#[derive(Debug, Clone)]
pub struct TtlHeader {
    key_len: u8,
    optional_len: u8,
    is_deleted: bool,
    is_numeric: bool,
    value_len: u32,
    expire_at: u32,
}

impl TtlHeader {
    /// Magic bytes to detect valid headers.
    pub const MAGIC0: u8 = 0xCA;
    /// Second magic byte.
    pub const MAGIC1: u8 = 0xCE;

    /// Header size with validation feature.
    #[cfg(feature = "validation")]
    pub const SIZE: usize = 13;

    /// Header size without validation.
    #[cfg(not(feature = "validation"))]
    pub const SIZE: usize = 10;

    /// Maximum key length (8 bits).
    pub const MAX_KEY_LEN: usize = 0xFF;

    /// Maximum optional metadata length (6 bits).
    pub const MAX_OPTIONAL_LEN: usize = 0x3F;

    /// Maximum value length (32 bits).
    pub const MAX_VALUE_LEN: usize = u32::MAX as usize;

    /// Create a new TtlHeader.
    pub fn new(key_len: u8, optional_len: u8, value_len: u32, expire_at: u32) -> Self {
        Self {
            key_len,
            optional_len,
            is_deleted: false,
            is_numeric: false,
            value_len,
            expire_at,
        }
    }

    /// Create a new TtlHeader with all fields.
    pub fn with_flags(
        key_len: u8,
        optional_len: u8,
        value_len: u32,
        is_deleted: bool,
        is_numeric: bool,
        expire_at: u32,
    ) -> Self {
        Self {
            key_len,
            optional_len,
            is_deleted,
            is_numeric,
            value_len,
            expire_at,
        }
    }

    /// Compute checksum for validation.
    #[cfg(feature = "validation")]
    #[inline(always)]
    fn compute_checksum(
        key_len: u8,
        optional_len: u8,
        value_len: u32,
        is_numeric: bool,
        expire_at: u32,
    ) -> u8 {
        // MAGIC0 ^ MAGIC1 = 0xCA ^ 0xCE = 0x04
        let mut checksum = 0x04_u8;
        checksum ^= key_len;
        checksum ^= optional_len;
        // XOR all 4 bytes of value_len and expire_at
        checksum ^= (value_len as u8)
            ^ ((value_len >> 8) as u8)
            ^ ((value_len >> 16) as u8)
            ^ ((value_len >> 24) as u8);
        checksum ^= (expire_at as u8)
            ^ ((expire_at >> 8) as u8)
            ^ ((expire_at >> 16) as u8)
            ^ ((expire_at >> 24) as u8);
        if is_numeric {
            checksum ^= 0x55;
        }
        checksum
    }

    /// Try to parse header from bytes, returning None if validation fails.
    #[inline]
    pub fn try_from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < Self::SIZE {
            return None;
        }

        let ptr = data.as_ptr();

        // Read key_len, flags, value_len, and expire_at from new layout
        let key_len = unsafe { *ptr };
        let flags = unsafe { *ptr.add(1) };
        let value_len = unsafe { ptr.add(2).cast::<u32>().read_unaligned() };
        let expire_at = unsafe { ptr.add(6).cast::<u32>().read_unaligned() };

        let optional_len = flags & 0x3F;
        let is_deleted = (flags & 0x40) != 0;
        let is_numeric = (flags & 0x80) != 0;

        #[cfg(feature = "validation")]
        {
            // Check magic bytes with single comparison
            let magic = unsafe { ptr.add(10).cast::<u16>().read_unaligned() };
            if magic != u16::from_ne_bytes([Self::MAGIC0, Self::MAGIC1]) {
                return None;
            }

            let stored_checksum = unsafe { *ptr.add(12) };
            let computed_checksum =
                Self::compute_checksum(key_len, optional_len, value_len, is_numeric, expire_at);
            if stored_checksum != computed_checksum {
                return None;
            }
        }

        Some(Self {
            key_len,
            optional_len,
            is_deleted,
            is_numeric,
            value_len,
            expire_at,
        })
    }

    /// Parse header from bytes, panicking if validation fails.
    pub fn from_bytes(data: &[u8]) -> Self {
        debug_assert!(data.len() >= Self::SIZE);
        Self::try_from_bytes(data).expect("Invalid TtlHeader")
    }

    /// Parse header from bytes without validation checks.
    /// Only use when you know the data is valid.
    ///
    /// # Safety assumption
    /// The data pointer must be 8-byte aligned (items are padded to 8-byte boundaries).
    #[inline(always)]
    #[cfg(not(feature = "validation"))]
    pub fn from_bytes_unchecked(data: &[u8]) -> Self {
        debug_assert!(data.len() >= Self::SIZE);
        let ptr = data.as_ptr();

        // Items are 8-byte aligned
        let key_len = unsafe { *ptr };
        let flags = unsafe { *ptr.add(1) };
        // value_len at offset 2 is NOT 4-byte aligned, must use unaligned read
        let value_len = unsafe { ptr.add(2).cast::<u32>().read_unaligned() };
        // expire_at at offset 6 is NOT 4-byte aligned, must use unaligned read
        let expire_at = unsafe { ptr.add(6).cast::<u32>().read_unaligned() };

        Self {
            key_len,
            optional_len: flags & 0x3F,
            is_deleted: (flags & 0x40) != 0,
            is_numeric: (flags & 0x80) != 0,
            value_len,
            expire_at,
        }
    }

    /// Write header to bytes.
    pub fn to_bytes(&self, data: &mut [u8]) {
        debug_assert!(data.len() >= Self::SIZE);

        let mut flags = self.optional_len;

        if self.is_deleted {
            flags |= 0x40;
        }
        if self.is_numeric {
            flags |= 0x80;
        }

        data[0] = self.key_len;
        data[1] = flags;
        data[2..6].copy_from_slice(&self.value_len.to_ne_bytes());
        data[6..10].copy_from_slice(&self.expire_at.to_ne_bytes());

        #[cfg(feature = "validation")]
        {
            data[10] = Self::MAGIC0;
            data[11] = Self::MAGIC1;
            data[12] = Self::compute_checksum(
                self.key_len,
                self.optional_len,
                self.value_len,
                self.is_numeric,
                self.expire_at,
            );
        }
    }

    /// Calculate padded size rounded to 8-byte boundary.
    ///
    /// Uses checked arithmetic to detect overflow.
    pub fn padded_size(&self) -> usize {
        let size = Self::SIZE
            .checked_add(self.optional_len as usize)
            .and_then(|s| s.checked_add(self.key_len as usize))
            .and_then(|s| s.checked_add(self.value_len as usize))
            .and_then(|s| s.checked_add(7))
            .expect("Item size overflow");

        size & !7
    }

    /// Calculate padded size without overflow checks.
    ///
    /// This is faster than `padded_size()` but assumes the header fields
    /// are valid and won't overflow. Use in hot paths where the header
    /// has already been validated or comes from trusted storage.
    #[inline(always)]
    pub fn padded_size_unchecked(&self) -> usize {
        let size = Self::SIZE
            + self.optional_len as usize
            + self.key_len as usize
            + self.value_len as usize
            + 7;
        size & !7
    }

    /// Check if item is marked as deleted.
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }

    /// Get the key length.
    #[inline]
    pub fn key_len(&self) -> u8 {
        self.key_len
    }

    /// Get the optional data length.
    #[inline]
    pub fn optional_len(&self) -> u8 {
        self.optional_len
    }

    /// Get the value length.
    #[inline]
    pub fn value_len(&self) -> u32 {
        self.value_len
    }

    /// Get the expiration timestamp (coarse seconds).
    #[inline]
    pub fn expire_at(&self) -> u32 {
        self.expire_at
    }

    /// Check if this item is expired.
    #[inline]
    pub fn is_expired(&self, now: u32) -> bool {
        now >= self.expire_at
    }

    /// Get the remaining TTL as a Duration.
    ///
    /// Returns `None` if the item is expired.
    pub fn remaining_ttl(&self, now: u32) -> Option<Duration> {
        if now >= self.expire_at {
            None
        } else {
            Some(Duration::from_secs((self.expire_at - now) as u64))
        }
    }

    /// Check if value is numeric.
    #[inline]
    pub fn is_numeric(&self) -> bool {
        self.is_numeric
    }

    /// Get the byte range for the optional data within item data starting at `offset`.
    pub fn optional_range(&self, offset: u32) -> std::ops::Range<usize> {
        let offset = offset as usize;
        let start = offset + Self::SIZE;
        start..(start + self.optional_len as usize)
    }

    /// Get the byte range for the key within item data starting at `offset`.
    pub fn key_range(&self, offset: u32) -> std::ops::Range<usize> {
        let offset = offset as usize;
        let start = offset + Self::SIZE + self.optional_len as usize;
        start..(start + self.key_len as usize)
    }

    /// Get the byte range for the value within item data starting at `offset`.
    pub fn value_range(&self, offset: u32) -> std::ops::Range<usize> {
        let offset = offset as usize;
        let start = offset + Self::SIZE + self.optional_len as usize + self.key_len as usize;
        start..(start + self.value_len as usize)
    }
}

// ============================================================================
// ItemGuard trait - Zero-copy access to item data
// ============================================================================

/// Trait for zero-copy access to item data.
///
/// Implementations hold a reference to the segment and increment the
/// reference count, preventing the segment from being cleared while
/// the guard exists.
pub trait ItemGuard<'a>: Send {
    /// Get the item's key.
    fn key(&self) -> &[u8];

    /// Get the item's value.
    fn value(&self) -> &[u8];

    /// Get the item's optional metadata (may be empty).
    fn optional(&self) -> &[u8];

    /// Copy value to a new Vec (when you need ownership).
    fn value_to_vec(&self) -> Vec<u8> {
        self.value().to_vec()
    }
}

// ============================================================================
// BasicItemGuard - Concrete guard implementation
// ============================================================================

/// A guard providing zero-copy access to an item's data in a segment.
///
/// The segment's reference count is held while this guard exists,
/// preventing eviction or clearing of the segment.
///
/// When the last reader drops a guard for a segment in `AwaitingRelease` state,
/// the segment is automatically returned to the free queue.
pub struct BasicItemGuard<'a> {
    ref_count: &'a AtomicU32,
    key: &'a [u8],
    value: &'a [u8],
    optional: &'a [u8],
    /// Segment metadata for checking AwaitingRelease state on drop.
    metadata: &'a crate::sync::AtomicU64,
    /// Free queue pointer for releasing condemned segments.
    /// Note: This always points to the main free_queue, not spare_queue.
    /// Balancing to spare_queue happens through normal release() calls.
    free_queue: *const crossbeam_deque::Injector<u32>,
    /// Segment ID for pushing to free queue.
    segment_id: u32,
}

impl<'a> BasicItemGuard<'a> {
    /// Create a new BasicItemGuard.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - The segment's ref_count has been incremented
    /// - The slice references are valid and point into the segment's data
    /// - The metadata, free_queue, and segment_id are valid for the segment
    pub fn new(
        ref_count: &'a AtomicU32,
        key: &'a [u8],
        value: &'a [u8],
        optional: &'a [u8],
        metadata: &'a crate::sync::AtomicU64,
        free_queue: *const crossbeam_deque::Injector<u32>,
        segment_id: u32,
    ) -> Self {
        Self {
            ref_count,
            key,
            value,
            optional,
            metadata,
            free_queue,
            segment_id,
        }
    }
}

impl<'a> ItemGuard<'a> for BasicItemGuard<'a> {
    fn key(&self) -> &[u8] {
        self.key
    }

    fn value(&self) -> &[u8] {
        self.value
    }

    fn optional(&self) -> &[u8] {
        self.optional
    }
}

impl Drop for BasicItemGuard<'_> {
    fn drop(&mut self) {
        use crate::state::{INVALID_SEGMENT_ID, Metadata, State};

        let prev_count = self.ref_count.fetch_sub(1, Ordering::Release);

        // If we were the last reader (prev_count == 1 means new count is 0)
        if prev_count == 1 {
            // Check if segment is condemned and needs release
            let packed = self.metadata.load(Ordering::Acquire);
            let meta = Metadata::unpack(packed);

            if meta.state == State::AwaitingRelease {
                // Transition to Free and push to free queue
                let new_meta = Metadata {
                    next: INVALID_SEGMENT_ID,
                    prev: INVALID_SEGMENT_ID,
                    state: State::Free,
                };

                if self
                    .metadata
                    .compare_exchange(
                        packed,
                        new_meta.pack(),
                        Ordering::Release,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    // Push to free queue
                    // SAFETY: free_queue pointer is valid for the lifetime of the pool
                    unsafe {
                        (*self.free_queue).push(self.segment_id);
                    }
                }
            }
        }
    }
}

// SAFETY: BasicItemGuard only contains references to data that is Send
unsafe impl Send for BasicItemGuard<'_> {}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_basic_header_round_trip() {
        let header = BasicHeader::with_flags(10, 5, 1000, false, true);
        let mut buf = vec![0u8; BasicHeader::SIZE];
        header.to_bytes(&mut buf);

        let parsed = BasicHeader::try_from_bytes(&buf).expect("parse failed");
        assert_eq!(parsed.key_len(), 10);
        assert_eq!(parsed.optional_len(), 5);
        assert_eq!(parsed.value_len(), 1000);
        assert!(!parsed.is_deleted());
        assert!(parsed.is_numeric());
    }

    #[test]
    fn test_basic_header_padded_size() {
        // Header + optional + key + value, rounded up to 8 bytes
        let header = BasicHeader::new(8, 0, 10);
        let size = header.padded_size();
        // SIZE + 0 + 8 + 10 = SIZE + 18, rounded to 8-byte boundary
        assert_eq!(size % 8, 0);
    }

    #[test]
    fn test_ttl_header_round_trip() {
        let header = TtlHeader::with_flags(10, 5, 1000, false, true, 12345);
        let mut buf = vec![0u8; TtlHeader::SIZE];
        header.to_bytes(&mut buf);

        let parsed = TtlHeader::try_from_bytes(&buf).expect("parse failed");
        assert_eq!(parsed.key_len(), 10);
        assert_eq!(parsed.optional_len(), 5);
        assert_eq!(parsed.value_len(), 1000);
        assert_eq!(parsed.expire_at(), 12345);
        assert!(!parsed.is_deleted());
        assert!(parsed.is_numeric());
    }

    #[test]
    fn test_ttl_header_expiration() {
        let header = TtlHeader::new(10, 0, 100, 1000);
        assert!(!header.is_expired(500));
        assert!(!header.is_expired(999));
        assert!(header.is_expired(1000));
        assert!(header.is_expired(2000));
    }

    #[test]
    fn test_ttl_header_remaining_ttl() {
        let header = TtlHeader::new(10, 0, 100, 1000);
        assert_eq!(header.remaining_ttl(500), Some(Duration::from_secs(500)));
        assert_eq!(header.remaining_ttl(999), Some(Duration::from_secs(1)));
        assert_eq!(header.remaining_ttl(1000), None);
        assert_eq!(header.remaining_ttl(2000), None);
    }

    #[test]
    fn test_basic_header_ranges() {
        let header = BasicHeader::new(8, 4, 100);
        let offset = 0u32;

        let opt_range = header.optional_range(offset);
        assert_eq!(opt_range.start, BasicHeader::SIZE);
        assert_eq!(opt_range.len(), 4);

        let key_range = header.key_range(offset);
        assert_eq!(key_range.start, BasicHeader::SIZE + 4);
        assert_eq!(key_range.len(), 8);

        let value_range = header.value_range(offset);
        assert_eq!(value_range.start, BasicHeader::SIZE + 4 + 8);
        assert_eq!(value_range.len(), 100);
    }

    #[cfg(feature = "validation")]
    #[test]
    fn test_basic_header_bad_magic() {
        let header = BasicHeader::new(10, 0, 100);
        let mut buf = vec![0u8; BasicHeader::SIZE];
        header.to_bytes(&mut buf);

        // Corrupt magic byte
        buf[5] = 0xFF;

        assert!(BasicHeader::try_from_bytes(&buf).is_none());
    }

    #[cfg(feature = "validation")]
    #[test]
    fn test_basic_header_bad_checksum() {
        let header = BasicHeader::new(10, 0, 100);
        let mut buf = vec![0u8; BasicHeader::SIZE];
        header.to_bytes(&mut buf);

        // Corrupt checksum
        buf[7] ^= 0xFF;

        assert!(BasicHeader::try_from_bytes(&buf).is_none());
    }

    #[test]
    fn test_basic_header_try_from_bytes_too_short() {
        // Data shorter than SIZE should return None
        let short_data = [0u8; 4];
        assert!(BasicHeader::try_from_bytes(&short_data).is_none());

        let empty_data: [u8; 0] = [];
        assert!(BasicHeader::try_from_bytes(&empty_data).is_none());
    }

    #[test]
    fn test_basic_header_from_bytes() {
        let header = BasicHeader::new(10, 5, 1000);
        let mut buf = vec![0u8; BasicHeader::SIZE];
        header.to_bytes(&mut buf);

        // from_bytes should work for valid data
        let parsed = BasicHeader::from_bytes(&buf);
        assert_eq!(parsed.key_len(), 10);
        assert_eq!(parsed.optional_len(), 5);
        assert_eq!(parsed.value_len(), 1000);
    }

    #[test]
    fn test_basic_header_with_deleted_flag() {
        // Test round-trip with deleted flag
        let header = BasicHeader::with_flags(10, 5, 1000, true, false);
        assert!(header.is_deleted());
        assert!(!header.is_numeric());

        let mut buf = vec![0u8; BasicHeader::SIZE];
        header.to_bytes(&mut buf);

        let parsed = BasicHeader::try_from_bytes(&buf).unwrap();
        assert!(parsed.is_deleted());
        assert!(!parsed.is_numeric());
    }

    #[test]
    fn test_ttl_header_try_from_bytes_too_short() {
        // Data shorter than SIZE should return None
        let short_data = [0u8; 8];
        assert!(TtlHeader::try_from_bytes(&short_data).is_none());

        let empty_data: [u8; 0] = [];
        assert!(TtlHeader::try_from_bytes(&empty_data).is_none());
    }

    #[test]
    fn test_ttl_header_from_bytes() {
        let header = TtlHeader::new(10, 5, 1000, 12345);
        let mut buf = vec![0u8; TtlHeader::SIZE];
        header.to_bytes(&mut buf);

        // from_bytes should work for valid data
        let parsed = TtlHeader::from_bytes(&buf);
        assert_eq!(parsed.key_len(), 10);
        assert_eq!(parsed.optional_len(), 5);
        assert_eq!(parsed.value_len(), 1000);
        assert_eq!(parsed.expire_at(), 12345);
    }

    #[test]
    fn test_ttl_header_with_deleted_flag() {
        let header = TtlHeader::with_flags(10, 5, 1000, true, false, 12345);
        assert!(header.is_deleted());
        assert!(!header.is_numeric());

        let mut buf = vec![0u8; TtlHeader::SIZE];
        header.to_bytes(&mut buf);

        let parsed = TtlHeader::try_from_bytes(&buf).unwrap();
        assert!(parsed.is_deleted());
        assert!(!parsed.is_numeric());
    }

    #[test]
    fn test_basic_header_with_both_flags() {
        let header = BasicHeader::with_flags(10, 5, 1000, true, true);
        assert!(header.is_deleted());
        assert!(header.is_numeric());

        let mut buf = vec![0u8; BasicHeader::SIZE];
        header.to_bytes(&mut buf);

        let parsed = BasicHeader::try_from_bytes(&buf).unwrap();
        assert!(parsed.is_deleted());
        assert!(parsed.is_numeric());
    }

    #[test]
    fn test_ttl_header_with_both_flags() {
        let header = TtlHeader::with_flags(10, 5, 1000, true, true, 12345);
        assert!(header.is_deleted());
        assert!(header.is_numeric());

        let mut buf = vec![0u8; TtlHeader::SIZE];
        header.to_bytes(&mut buf);

        let parsed = TtlHeader::try_from_bytes(&buf).unwrap();
        assert!(parsed.is_deleted());
        assert!(parsed.is_numeric());
    }

    #[test]
    fn test_basic_header_zero_values() {
        let header = BasicHeader::new(0, 0, 0);
        let mut buf = vec![0u8; BasicHeader::SIZE];
        header.to_bytes(&mut buf);

        let parsed = BasicHeader::try_from_bytes(&buf).unwrap();
        assert_eq!(parsed.key_len(), 0);
        assert_eq!(parsed.optional_len(), 0);
        assert_eq!(parsed.value_len(), 0);
    }

    #[test]
    fn test_ttl_header_zero_expire() {
        let header = TtlHeader::new(10, 5, 1000, 0);
        let mut buf = vec![0u8; TtlHeader::SIZE];
        header.to_bytes(&mut buf);

        let parsed = TtlHeader::try_from_bytes(&buf).unwrap();
        assert_eq!(parsed.expire_at(), 0);
        // With expire_at=0, should be expired at any non-zero time
        assert!(parsed.is_expired(1));
    }

    #[test]
    fn test_ttl_header_max_expire() {
        let header = TtlHeader::new(10, 5, 1000, u32::MAX);
        let mut buf = vec![0u8; TtlHeader::SIZE];
        header.to_bytes(&mut buf);

        let parsed = TtlHeader::try_from_bytes(&buf).unwrap();
        assert_eq!(parsed.expire_at(), u32::MAX);
        // Should not be expired yet
        assert!(!parsed.is_expired(1000000));
    }

    #[test]
    fn test_basic_header_padded_size_alignment() {
        // Test that padded_size always returns 8-byte aligned values
        for key_len in [1u8, 5, 10, 255] {
            for opt_len in [0u8, 1, 5, 63] {
                for val_len in [0u32, 1, 100, 10000] {
                    let header = BasicHeader::new(key_len, opt_len, val_len);
                    let padded = header.padded_size();
                    assert_eq!(
                        padded % 8,
                        0,
                        "padded_size not aligned for key_len={}, opt_len={}, val_len={}",
                        key_len,
                        opt_len,
                        val_len
                    );
                }
            }
        }
    }

    #[test]
    fn test_ttl_header_padded_size_alignment() {
        // Test that padded_size always returns 8-byte aligned values
        for key_len in [1u8, 5, 10, 255] {
            for opt_len in [0u8, 1, 5, 63] {
                for val_len in [0u32, 1, 100, 10000] {
                    let header = TtlHeader::new(key_len, opt_len, val_len, 12345);
                    let padded = header.padded_size();
                    assert_eq!(
                        padded % 8,
                        0,
                        "padded_size not aligned for key_len={}, opt_len={}, val_len={}",
                        key_len,
                        opt_len,
                        val_len
                    );
                }
            }
        }
    }
}
