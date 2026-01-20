//! Slab item header and access guards.
//!
//! Each slot in a slab contains:
//! - Fixed header (24 bytes)
//! - Key bytes
//! - Value bytes
//! - Padding to slot size

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::config::HEADER_SIZE;

/// Get current time as coarse seconds since epoch.
#[inline]
fn now_secs() -> u32 {
    clocksource::coarse::UnixInstant::now()
        .duration_since(clocksource::coarse::UnixInstant::EPOCH)
        .as_secs() as u32
}

/// Sentinel value for LRU list head/tail (no link).
pub const LRU_NONE: u32 = u32::MAX;

/// Flag bit: item is deleted.
const FLAG_DELETED: u32 = 1 << 0;

/// Flag bit: item is numeric (for incr/decr).
const FLAG_NUMERIC: u32 = 1 << 1;

/// Slab item header (24 bytes).
///
/// ```text
/// Offset  Size  Field
/// ------  ----  -----
/// 0       4     flags (atomic): deleted, numeric, reserved
/// 4       4     key_len
/// 8       4     value_len
/// 12      4     expire_at (seconds since epoch, 0 = no expiry)
/// 16      4     lru_prev (atomic): previous slot in LRU, LRU_NONE = head
/// 20      4     lru_next (atomic): next slot in LRU, LRU_NONE = tail
/// ```
///
/// After the header:
/// - `[24..24+key_len]`: key bytes
/// - `[24+key_len..24+key_len+value_len]`: value bytes
#[repr(C)]
pub struct SlabItemHeader {
    /// Flags (deleted, numeric, reserved).
    flags: AtomicU32,
    /// Key length in bytes.
    key_len: u32,
    /// Value length in bytes.
    value_len: u32,
    /// Expiration time (seconds since epoch), 0 = no expiry.
    expire_at: u32,
    /// LRU previous slot packed as (slab_id << 16 | slot_index).
    /// LRU_NONE indicates list head.
    lru_prev: AtomicU32,
    /// LRU next slot packed as (slab_id << 16 | slot_index).
    /// LRU_NONE indicates list tail.
    lru_next: AtomicU32,
}

// Compile-time assertion for header size
const _: () = assert!(std::mem::size_of::<SlabItemHeader>() == HEADER_SIZE);

impl SlabItemHeader {
    /// Initialize a new header at the given memory location.
    ///
    /// # Safety
    ///
    /// The caller must ensure `ptr` points to valid, writable memory
    /// of at least `HEADER_SIZE` bytes.
    #[inline]
    pub unsafe fn init(
        ptr: *mut u8,
        key_len: usize,
        value_len: usize,
        ttl: Duration,
    ) -> &'static Self {
        // SAFETY: Caller ensures ptr is valid and points to HEADER_SIZE bytes
        unsafe {
            let header = &mut *(ptr as *mut SlabItemHeader);

            // Calculate expiration time
            let expire_at = if ttl.is_zero() {
                0
            } else {
                now_secs().saturating_add(ttl.as_secs() as u32)
            };

            // Use relaxed stores during initialization since we haven't published
            // the item yet
            header.flags = AtomicU32::new(0);
            header.key_len = key_len as u32;
            header.value_len = value_len as u32;
            header.expire_at = expire_at;
            header.lru_prev = AtomicU32::new(LRU_NONE);
            header.lru_next = AtomicU32::new(LRU_NONE);

            &*(ptr as *const SlabItemHeader)
        }
    }

    /// Get a reference to the header at the given memory location.
    ///
    /// # Safety
    ///
    /// The caller must ensure `ptr` points to a valid SlabItemHeader.
    #[inline]
    pub unsafe fn from_ptr(ptr: *const u8) -> &'static Self {
        // SAFETY: Caller ensures ptr points to a valid SlabItemHeader
        unsafe { &*(ptr as *const SlabItemHeader) }
    }

    /// Check if the item is marked as deleted.
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.flags.load(Ordering::Acquire) & FLAG_DELETED != 0
    }

    /// Mark the item as deleted.
    #[inline]
    pub fn mark_deleted(&self) {
        self.flags.fetch_or(FLAG_DELETED, Ordering::Release);
    }

    /// Check if the item is numeric.
    #[inline]
    pub fn is_numeric(&self) -> bool {
        self.flags.load(Ordering::Acquire) & FLAG_NUMERIC != 0
    }

    /// Set the numeric flag.
    #[inline]
    pub fn set_numeric(&self, numeric: bool) {
        if numeric {
            self.flags.fetch_or(FLAG_NUMERIC, Ordering::Release);
        } else {
            self.flags.fetch_and(!FLAG_NUMERIC, Ordering::Release);
        }
    }

    /// Get the key length.
    #[inline]
    pub fn key_len(&self) -> usize {
        self.key_len as usize
    }

    /// Get the value length.
    #[inline]
    pub fn value_len(&self) -> usize {
        self.value_len as usize
    }

    /// Update the value length (for append/prepend operations).
    #[inline]
    pub fn set_value_len(&mut self, len: usize) {
        self.value_len = len as u32;
    }

    /// Get the total item size (header + key + value).
    #[inline]
    pub fn item_size(&self) -> usize {
        HEADER_SIZE + self.key_len as usize + self.value_len as usize
    }

    /// Check if the item has expired.
    #[inline]
    pub fn is_expired(&self) -> bool {
        let expire = self.expire_at;
        if expire == 0 {
            return false; // No expiry
        }
        now_secs() >= expire
    }

    /// Get the remaining TTL, or None if expired or no expiry set.
    #[inline]
    pub fn remaining_ttl(&self) -> Option<Duration> {
        let expire = self.expire_at;
        if expire == 0 {
            return None; // No expiry
        }
        let now = now_secs();
        if now >= expire {
            return Some(Duration::ZERO); // Expired
        }
        Some(Duration::from_secs((expire - now) as u64))
    }

    /// Get the expiration timestamp.
    #[inline]
    pub fn expire_at(&self) -> u32 {
        self.expire_at
    }

    /// Set the expiration timestamp.
    #[inline]
    pub fn set_expire_at(&mut self, expire_at: u32) {
        self.expire_at = expire_at;
    }

    /// Get the LRU previous link.
    #[inline]
    pub fn lru_prev(&self) -> u32 {
        self.lru_prev.load(Ordering::Acquire)
    }

    /// Set the LRU previous link.
    #[inline]
    pub fn set_lru_prev(&self, prev: u32) {
        self.lru_prev.store(prev, Ordering::Release);
    }

    /// Get the LRU next link.
    #[inline]
    pub fn lru_next(&self) -> u32 {
        self.lru_next.load(Ordering::Acquire)
    }

    /// Set the LRU next link.
    #[inline]
    pub fn set_lru_next(&self, next: u32) {
        self.lru_next.store(next, Ordering::Release);
    }

    /// Get a pointer to the key bytes.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot.
    #[inline]
    pub unsafe fn key_ptr(&self) -> *const u8 {
        // SAFETY: Caller ensures header is at start of valid slot
        unsafe { (self as *const Self as *const u8).add(HEADER_SIZE) }
    }

    /// Get the key as a slice.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot.
    #[inline]
    pub unsafe fn key(&self) -> &[u8] {
        // SAFETY: Caller ensures header is at start of valid slot with key
        unsafe { std::slice::from_raw_parts(self.key_ptr(), self.key_len as usize) }
    }

    /// Get a pointer to the value bytes.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot.
    #[inline]
    pub unsafe fn value_ptr(&self) -> *const u8 {
        // SAFETY: Caller ensures header is at start of valid slot
        unsafe { (self as *const Self as *const u8).add(HEADER_SIZE + self.key_len as usize) }
    }

    /// Get a mutable pointer to the value bytes.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot
    /// and no other references to the value exist.
    #[inline]
    pub unsafe fn value_ptr_mut(&mut self) -> *mut u8 {
        // SAFETY: Caller ensures header is at start of valid slot
        unsafe { (self as *mut Self as *mut u8).add(HEADER_SIZE + self.key_len as usize) }
    }

    /// Get the value as a slice.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot.
    #[inline]
    pub unsafe fn value(&self) -> &[u8] {
        // SAFETY: Caller ensures header is at start of valid slot with value
        unsafe { std::slice::from_raw_parts(self.value_ptr(), self.value_len as usize) }
    }
}

/// Pack slab_id and slot_index into a u32 for LRU links.
///
/// Note: This only works for up to 65536 slabs and 65536 slots per slab.
/// For larger slab counts, we'd need a different encoding or larger links.
#[inline]
pub fn pack_lru_link(slab_id: u32, slot_index: u16) -> u32 {
    debug_assert!(slab_id <= 0xFFFF, "slab_id too large for LRU link");
    ((slab_id as u32) << 16) | (slot_index as u32)
}

/// Unpack a u32 LRU link into (slab_id, slot_index).
#[inline]
pub fn unpack_lru_link(link: u32) -> (u32, u16) {
    ((link >> 16) as u32, (link & 0xFFFF) as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size() {
        assert_eq!(std::mem::size_of::<SlabItemHeader>(), HEADER_SIZE);
    }

    #[test]
    fn test_pack_unpack_lru() {
        let slab_id = 1234u32;
        let slot_index = 5678u16;
        let packed = pack_lru_link(slab_id, slot_index);
        let (unpacked_slab, unpacked_slot) = unpack_lru_link(packed);
        assert_eq!(unpacked_slab, slab_id);
        assert_eq!(unpacked_slot, slot_index);
    }

    #[test]
    fn test_lru_none() {
        let (slab, slot) = unpack_lru_link(LRU_NONE);
        assert_eq!(slab, 0xFFFF);
        assert_eq!(slot, 0xFFFF);
    }

    #[test]
    fn test_header_flags() {
        let mut buf = [0u8; 64];
        unsafe {
            let header = SlabItemHeader::init(buf.as_mut_ptr(), 4, 10, Duration::from_secs(3600));

            assert!(!header.is_deleted());
            assert!(!header.is_numeric());

            header.mark_deleted();
            assert!(header.is_deleted());

            header.set_numeric(true);
            assert!(header.is_numeric());

            header.set_numeric(false);
            assert!(!header.is_numeric());
        }
    }

    #[test]
    fn test_header_key_value() {
        let mut buf = [0u8; 64];
        let key = b"test";
        let value = b"value12345";

        unsafe {
            let header = SlabItemHeader::init(
                buf.as_mut_ptr(),
                key.len(),
                value.len(),
                Duration::from_secs(100),
            );

            // Copy key and value
            std::ptr::copy_nonoverlapping(
                key.as_ptr(),
                buf.as_mut_ptr().add(HEADER_SIZE),
                key.len(),
            );
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                buf.as_mut_ptr().add(HEADER_SIZE + key.len()),
                value.len(),
            );

            assert_eq!(header.key_len(), 4);
            assert_eq!(header.value_len(), 10);
            assert_eq!(header.key(), key);
            assert_eq!(header.value(), value);
            assert_eq!(header.item_size(), HEADER_SIZE + 4 + 10);
        }
    }
}
