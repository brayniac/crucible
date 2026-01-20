//! Slab item header and access guards.
//!
//! Each slot in a slab contains:
//! - Fixed header (12 bytes)
//! - Optional CAS token (8 bytes if has_cas flag set)
//! - Optional data (4 bytes if has_optional flag set)
//! - Key bytes
//! - Value bytes
//! - Padding to slot size

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::config::HEADER_SIZE;
use crate::sync::spin_loop;

/// Base epoch for expiration timestamps (2024-01-01 00:00:00 UTC).
/// Using a recent base allows 28 bits (~8.5 years) to cover times through ~2032.
const EXPIRE_BASE_EPOCH: u64 = 1704067200;

/// Get current time as coarse seconds since our base epoch.
#[inline]
pub(crate) fn now_secs() -> u32 {
    let unix_secs: u64 = clocksource::coarse::UnixInstant::now()
        .duration_since(clocksource::coarse::UnixInstant::EPOCH)
        .as_secs() as u64;
    // Saturating sub handles the case where system clock is before base epoch
    unix_secs.saturating_sub(EXPIRE_BASE_EPOCH) as u32
}

/// Maximum expiration time (~8.5 years from base epoch with 28 bits).
pub const MAX_EXPIRE: u32 = (1 << 28) - 1;

/// Maximum key length (8 bits = 255 bytes).
pub const MAX_KEY_LEN: usize = 255;

/// Maximum value length (24 bits = ~16MB).
pub const MAX_VALUE_LEN: usize = (1 << 24) - 1;

/// Size of optional CAS token.
pub const CAS_SIZE: usize = 8;

/// Size of optional data field.
pub const OPTIONAL_SIZE: usize = 4;

// Flag bits in expire_and_flags (high 4 bits of u32)
const FLAG_DELETED: u32 = 1 << 28;
const FLAG_NUMERIC: u32 = 1 << 29;
const FLAG_HAS_CAS: u32 = 1 << 30;
const FLAG_HAS_OPTIONAL: u32 = 1 << 31;

// Mask for expiration time (low 28 bits)
const EXPIRE_MASK: u32 = (1 << 28) - 1;

// RwLock states in the rwlock field
const RWLOCK_UNLOCKED: u32 = 0;
const RWLOCK_WRITER: u32 = u32::MAX;
// Values 1..u32::MAX-1 represent reader counts

/// Slab item header (12 bytes).
///
/// ```text
/// Offset  Size  Field
/// ------  ----  -----
/// 0       4     rwlock (atomic): reader-writer lock
/// 4       4     kv_lens: key_len (8 bits) | value_len (24 bits)
/// 8       4     expire_and_flags: expire_at (28 bits) | flags (4 bits)
/// ```
///
/// After the header (optional fields determined by flags):
/// - `[12..12+8]`: CAS token (if FLAG_HAS_CAS)
/// - `[12+cas_size..12+cas_size+4]`: Optional data (if FLAG_HAS_OPTIONAL)
/// - Key bytes
/// - Value bytes
#[repr(C)]
pub struct SlabItemHeader {
    /// Reader-writer lock for concurrent access.
    /// 0 = unlocked, 1..MAX-1 = reader count, MAX = writer held.
    rwlock: AtomicU32,
    /// Packed key length (8 bits) and value length (24 bits).
    kv_lens: u32,
    /// Packed expiration time (28 bits) and flags (4 bits).
    expire_and_flags: AtomicU32,
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

            // Calculate expiration time (capped to 28 bits)
            let expire_at = if ttl.is_zero() {
                0
            } else {
                let expire = now_secs().saturating_add(ttl.as_secs() as u32);
                expire.min(MAX_EXPIRE)
            };

            // Pack key_len (8 bits) and value_len (24 bits)
            debug_assert!(key_len <= MAX_KEY_LEN);
            debug_assert!(value_len <= MAX_VALUE_LEN);
            let kv_lens = ((key_len as u32) << 24) | (value_len as u32 & 0x00FF_FFFF);

            // Use relaxed stores during initialization since we haven't published
            // the item yet
            header.rwlock = AtomicU32::new(RWLOCK_UNLOCKED);
            header.kv_lens = kv_lens;
            header.expire_and_flags = AtomicU32::new(expire_at);

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

    /// Get a mutable reference to the header at the given memory location.
    ///
    /// # Safety
    ///
    /// The caller must ensure `ptr` points to a valid SlabItemHeader
    /// and that no other references exist.
    #[inline]
    pub unsafe fn from_ptr_mut(ptr: *mut u8) -> &'static mut Self {
        // SAFETY: Caller ensures ptr points to a valid SlabItemHeader
        unsafe { &mut *(ptr as *mut SlabItemHeader) }
    }

    // ========== RwLock operations ==========

    /// Try to acquire a read lock. Returns true if successful.
    #[inline]
    pub fn try_read_lock(&self) -> bool {
        let current = self.rwlock.load(Ordering::Relaxed);
        if current == RWLOCK_WRITER {
            return false; // Writer held
        }
        // Try to increment reader count
        self.rwlock
            .compare_exchange_weak(current, current + 1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// Acquire a read lock, spinning until successful.
    #[inline]
    pub fn read_lock(&self) {
        loop {
            if self.try_read_lock() {
                return;
            }
            spin_loop();
        }
    }

    /// Release a read lock.
    #[inline]
    pub fn read_unlock(&self) {
        self.rwlock.fetch_sub(1, Ordering::Release);
    }

    /// Try to acquire a write lock. Returns true if successful.
    #[inline]
    pub fn try_write_lock(&self) -> bool {
        self.rwlock
            .compare_exchange(
                RWLOCK_UNLOCKED,
                RWLOCK_WRITER,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Acquire a write lock, spinning until successful.
    #[inline]
    pub fn write_lock(&self) {
        loop {
            if self.try_write_lock() {
                return;
            }
            spin_loop();
        }
    }

    /// Release a write lock.
    #[inline]
    pub fn write_unlock(&self) {
        self.rwlock.store(RWLOCK_UNLOCKED, Ordering::Release);
    }

    // ========== Flag operations ==========

    /// Load expire_and_flags atomically.
    #[inline]
    fn load_expire_and_flags(&self) -> u32 {
        self.expire_and_flags.load(Ordering::Acquire)
    }

    /// Check if the item is marked as deleted.
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.load_expire_and_flags() & FLAG_DELETED != 0
    }

    /// Mark the item as deleted.
    #[inline]
    pub fn mark_deleted(&self) {
        self.expire_and_flags
            .fetch_or(FLAG_DELETED, Ordering::Release);
    }

    /// Check if the item is numeric.
    #[inline]
    pub fn is_numeric(&self) -> bool {
        self.load_expire_and_flags() & FLAG_NUMERIC != 0
    }

    /// Set the numeric flag.
    #[inline]
    pub fn set_numeric(&self, numeric: bool) {
        if numeric {
            self.expire_and_flags
                .fetch_or(FLAG_NUMERIC, Ordering::Release);
        } else {
            self.expire_and_flags
                .fetch_and(!FLAG_NUMERIC, Ordering::Release);
        }
    }

    /// Check if the item has a CAS token.
    #[inline]
    pub fn has_cas(&self) -> bool {
        self.load_expire_and_flags() & FLAG_HAS_CAS != 0
    }

    /// Set the CAS flag.
    #[inline]
    pub fn set_has_cas(&self, has_cas: bool) {
        if has_cas {
            self.expire_and_flags
                .fetch_or(FLAG_HAS_CAS, Ordering::Release);
        } else {
            self.expire_and_flags
                .fetch_and(!FLAG_HAS_CAS, Ordering::Release);
        }
    }

    /// Check if the item has optional data.
    #[inline]
    pub fn has_optional(&self) -> bool {
        self.load_expire_and_flags() & FLAG_HAS_OPTIONAL != 0
    }

    /// Set the optional data flag.
    #[inline]
    pub fn set_has_optional(&self, has_optional: bool) {
        if has_optional {
            self.expire_and_flags
                .fetch_or(FLAG_HAS_OPTIONAL, Ordering::Release);
        } else {
            self.expire_and_flags
                .fetch_and(!FLAG_HAS_OPTIONAL, Ordering::Release);
        }
    }

    // ========== Length accessors ==========

    /// Get the key length.
    #[inline]
    pub fn key_len(&self) -> usize {
        (self.kv_lens >> 24) as usize
    }

    /// Get the value length.
    #[inline]
    pub fn value_len(&self) -> usize {
        (self.kv_lens & 0x00FF_FFFF) as usize
    }

    /// Update the value length (for append/prepend operations).
    ///
    /// # Safety
    ///
    /// Caller must hold the write lock.
    #[inline]
    pub unsafe fn set_value_len(&mut self, len: usize) {
        debug_assert!(len <= MAX_VALUE_LEN);
        self.kv_lens = (self.kv_lens & 0xFF00_0000) | (len as u32 & 0x00FF_FFFF);
    }

    /// Calculate the offset to key/value data (after header and optional fields).
    #[inline]
    fn data_offset(&self) -> usize {
        let mut offset = HEADER_SIZE;
        let flags = self.load_expire_and_flags();
        if flags & FLAG_HAS_CAS != 0 {
            offset += CAS_SIZE;
        }
        if flags & FLAG_HAS_OPTIONAL != 0 {
            offset += OPTIONAL_SIZE;
        }
        offset
    }

    /// Get the total item size (header + optional + key + value).
    #[inline]
    pub fn item_size(&self) -> usize {
        self.data_offset() + self.key_len() + self.value_len()
    }

    // ========== Expiration ==========

    /// Check if the item has expired.
    #[inline]
    pub fn is_expired(&self) -> bool {
        let expire = self.load_expire_and_flags() & EXPIRE_MASK;
        if expire == 0 {
            return false; // No expiry
        }
        now_secs() >= expire
    }

    /// Get the remaining TTL, or None if expired or no expiry set.
    #[inline]
    pub fn remaining_ttl(&self) -> Option<Duration> {
        let expire = self.load_expire_and_flags() & EXPIRE_MASK;
        if expire == 0 {
            return None; // No expiry
        }
        let now = now_secs();
        if now >= expire {
            return Some(Duration::ZERO); // Expired
        }
        Some(Duration::from_secs((expire - now) as u64))
    }

    /// Get the expiration timestamp (28-bit).
    #[inline]
    pub fn expire_at(&self) -> u32 {
        self.load_expire_and_flags() & EXPIRE_MASK
    }

    /// Set the expiration timestamp.
    ///
    /// # Safety
    ///
    /// Caller should hold the write lock for consistency.
    #[inline]
    pub fn set_expire_at(&self, expire_at: u32) {
        let expire = expire_at.min(MAX_EXPIRE);
        loop {
            let old = self.expire_and_flags.load(Ordering::Relaxed);
            let new = (old & !EXPIRE_MASK) | expire;
            if self
                .expire_and_flags
                .compare_exchange_weak(old, new, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    // ========== CAS operations ==========

    /// Get the CAS token pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot
    /// and that the item has a CAS token (has_cas() returns true).
    #[inline]
    pub unsafe fn cas_ptr(&self) -> *const u64 {
        debug_assert!(self.has_cas());
        // CAS is immediately after the header
        // SAFETY: Caller ensures header is at start of valid slot with CAS
        unsafe { (self as *const Self as *const u8).add(HEADER_SIZE) as *const u64 }
    }

    /// Get a mutable CAS token pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot
    /// and that the item has a CAS token. Caller must hold write lock.
    #[inline]
    pub unsafe fn cas_ptr_mut(&mut self) -> *mut u64 {
        debug_assert!(self.has_cas());
        // SAFETY: Caller ensures header is at start of valid slot with CAS
        unsafe { (self as *mut Self as *mut u8).add(HEADER_SIZE) as *mut u64 }
    }

    /// Get the CAS token value.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot
    /// and that the item has a CAS token.
    #[inline]
    pub unsafe fn cas(&self) -> u64 {
        // Note: unaligned read, protected by rwlock
        // SAFETY: Caller ensures header is at start of valid slot with CAS
        unsafe { std::ptr::read_unaligned(self.cas_ptr()) }
    }

    /// Set the CAS token value.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot
    /// and that the item has a CAS token. Caller must hold write lock.
    #[inline]
    pub unsafe fn set_cas(&mut self, cas: u64) {
        // Note: unaligned write, protected by rwlock
        // SAFETY: Caller ensures header is at start of valid slot with CAS
        unsafe { std::ptr::write_unaligned(self.cas_ptr_mut(), cas) }
    }

    // ========== Optional data operations ==========

    /// Get the optional data pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot
    /// and that the item has optional data (has_optional() returns true).
    #[inline]
    pub unsafe fn optional_ptr(&self) -> *const u32 {
        debug_assert!(self.has_optional());
        let mut offset = HEADER_SIZE;
        if self.has_cas() {
            offset += CAS_SIZE;
        }
        // SAFETY: Caller ensures header is at start of valid slot with optional
        unsafe { (self as *const Self as *const u8).add(offset) as *const u32 }
    }

    /// Get a mutable optional data pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot
    /// and that the item has optional data. Caller must hold write lock.
    #[inline]
    pub unsafe fn optional_ptr_mut(&mut self) -> *mut u32 {
        debug_assert!(self.has_optional());
        let mut offset = HEADER_SIZE;
        if self.has_cas() {
            offset += CAS_SIZE;
        }
        // SAFETY: Caller ensures header is at start of valid slot with optional
        unsafe { (self as *mut Self as *mut u8).add(offset) as *mut u32 }
    }

    /// Get the optional data value.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot
    /// and that the item has optional data.
    #[inline]
    pub unsafe fn optional(&self) -> u32 {
        // SAFETY: Caller ensures header is at start of valid slot with optional
        unsafe { std::ptr::read_unaligned(self.optional_ptr()) }
    }

    /// Set the optional data value.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot
    /// and that the item has optional data. Caller must hold write lock.
    #[inline]
    pub unsafe fn set_optional(&mut self, value: u32) {
        // SAFETY: Caller ensures header is at start of valid slot with optional
        unsafe { std::ptr::write_unaligned(self.optional_ptr_mut(), value) }
    }

    // ========== Key/Value accessors ==========

    /// Get a pointer to the key bytes.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot.
    #[inline]
    pub unsafe fn key_ptr(&self) -> *const u8 {
        // SAFETY: Caller ensures header is at start of valid slot
        unsafe { (self as *const Self as *const u8).add(self.data_offset()) }
    }

    /// Get the key as a slice.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot.
    #[inline]
    pub unsafe fn key(&self) -> &[u8] {
        // SAFETY: Caller ensures header is at start of valid slot with key
        unsafe { std::slice::from_raw_parts(self.key_ptr(), self.key_len()) }
    }

    /// Get a pointer to the value bytes.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot.
    #[inline]
    pub unsafe fn value_ptr(&self) -> *const u8 {
        // SAFETY: Caller ensures header is at start of valid slot
        unsafe { (self as *const Self as *const u8).add(self.data_offset() + self.key_len()) }
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
        unsafe { (self as *mut Self as *mut u8).add(self.data_offset() + self.key_len()) }
    }

    /// Get the value as a slice.
    ///
    /// # Safety
    ///
    /// The caller must ensure this header is at the start of a valid slot.
    #[inline]
    pub unsafe fn value(&self) -> &[u8] {
        // SAFETY: Caller ensures header is at start of valid slot with value
        unsafe { std::slice::from_raw_parts(self.value_ptr(), self.value_len()) }
    }
}

/// Pack slab_id and slot_index into a u32.
///
/// Note: This only works for up to 65536 slabs and 65536 slots per slab.
/// For larger slab counts, we'd need a different encoding or larger links.
#[inline]
pub fn pack_slot_ref(slab_id: u32, slot_index: u16) -> u32 {
    debug_assert!(slab_id <= 0xFFFF, "slab_id too large for slot ref");
    ((slab_id as u32) << 16) | (slot_index as u32)
}

/// Unpack a u32 slot reference into (slab_id, slot_index).
#[inline]
pub fn unpack_slot_ref(packed: u32) -> (u32, u16) {
    ((packed >> 16) as u32, (packed & 0xFFFF) as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size() {
        assert_eq!(std::mem::size_of::<SlabItemHeader>(), HEADER_SIZE);
        assert_eq!(HEADER_SIZE, 12);
    }

    #[test]
    fn test_pack_unpack_slot_ref() {
        let slab_id = 1234u32;
        let slot_index = 5678u16;
        let packed = pack_slot_ref(slab_id, slot_index);
        let (unpacked_slab, unpacked_slot) = unpack_slot_ref(packed);
        assert_eq!(unpacked_slab, slab_id);
        assert_eq!(unpacked_slot, slot_index);
    }

    #[test]
    fn test_header_flags() {
        let mut buf = [0u8; 64];
        unsafe {
            let header = SlabItemHeader::init(buf.as_mut_ptr(), 4, 10, Duration::from_secs(3600));

            assert!(!header.is_deleted());
            assert!(!header.is_numeric());
            assert!(!header.has_cas());
            assert!(!header.has_optional());

            header.mark_deleted();
            assert!(header.is_deleted());

            header.set_numeric(true);
            assert!(header.is_numeric());

            header.set_numeric(false);
            assert!(!header.is_numeric());

            header.set_has_cas(true);
            assert!(header.has_cas());

            header.set_has_optional(true);
            assert!(header.has_optional());
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

    #[test]
    fn test_header_key_value_with_cas() {
        let mut buf = [0u8; 128];
        let key = b"test";
        let value = b"value12345";

        unsafe {
            let header = SlabItemHeader::init(
                buf.as_mut_ptr(),
                key.len(),
                value.len(),
                Duration::from_secs(100),
            );

            // Set CAS flag
            header.set_has_cas(true);

            // Write CAS token
            let header_mut = SlabItemHeader::from_ptr_mut(buf.as_mut_ptr());
            header_mut.set_cas(0xDEADBEEF_CAFEBABE);

            // Copy key and value (after CAS)
            let data_offset = HEADER_SIZE + CAS_SIZE;
            std::ptr::copy_nonoverlapping(
                key.as_ptr(),
                buf.as_mut_ptr().add(data_offset),
                key.len(),
            );
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                buf.as_mut_ptr().add(data_offset + key.len()),
                value.len(),
            );

            assert_eq!(header.key_len(), 4);
            assert_eq!(header.value_len(), 10);
            assert_eq!(header.cas(), 0xDEADBEEF_CAFEBABE);
            assert_eq!(header.key(), key);
            assert_eq!(header.value(), value);
            assert_eq!(header.item_size(), HEADER_SIZE + CAS_SIZE + 4 + 10);
        }
    }

    #[test]
    fn test_rwlock() {
        let mut buf = [0u8; 64];
        unsafe {
            let header = SlabItemHeader::init(buf.as_mut_ptr(), 4, 10, Duration::from_secs(3600));

            // Test read lock
            assert!(header.try_read_lock());
            assert!(header.try_read_lock()); // Multiple readers OK
            header.read_unlock();
            header.read_unlock();

            // Test write lock
            assert!(header.try_write_lock());
            assert!(!header.try_read_lock()); // Can't read with writer
            assert!(!header.try_write_lock()); // Can't write with writer
            header.write_unlock();

            // Can lock again after unlock
            assert!(header.try_read_lock());
            header.read_unlock();
        }
    }

    #[test]
    fn test_packed_lengths() {
        let mut buf = [0u8; 64];

        // Test max key length
        unsafe {
            let header = SlabItemHeader::init(buf.as_mut_ptr(), 255, 100, Duration::ZERO);
            assert_eq!(header.key_len(), 255);
            assert_eq!(header.value_len(), 100);
        }

        // Test max value length (24 bits)
        unsafe {
            let header = SlabItemHeader::init(buf.as_mut_ptr(), 10, MAX_VALUE_LEN, Duration::ZERO);
            assert_eq!(header.key_len(), 10);
            assert_eq!(header.value_len(), MAX_VALUE_LEN);
        }
    }

    #[test]
    fn test_expiration() {
        let mut buf = [0u8; 64];

        // Test no expiry
        unsafe {
            let header = SlabItemHeader::init(buf.as_mut_ptr(), 4, 10, Duration::ZERO);
            assert!(!header.is_expired());
            assert_eq!(header.remaining_ttl(), None);
        }

        // Test with TTL
        unsafe {
            let header = SlabItemHeader::init(buf.as_mut_ptr(), 4, 10, Duration::from_secs(3600));
            assert!(!header.is_expired());
            let ttl = header.remaining_ttl();
            assert!(ttl.is_some());
            // Should be close to 3600 seconds (allow for some time passing)
            let secs = ttl.unwrap().as_secs();
            assert!(secs >= 3590 && secs <= 3600);
        }
    }
}

/// Loom concurrency tests for the item header's RwLock.
#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use super::*;
    use crate::sync::{AtomicU32 as LoomAtomicU32, Ordering as LoomOrdering};
    use loom::sync::Arc;
    use loom::thread;

    /// Test concurrent read lock acquisition.
    ///
    /// Multiple readers should be able to hold locks simultaneously.
    #[test]
    fn test_concurrent_readers() {
        loom::model(|| {
            // Create a simple atomic to simulate the rwlock
            let rwlock = Arc::new(LoomAtomicU32::new(RWLOCK_UNLOCKED));

            let r1 = rwlock.clone();
            let r2 = rwlock.clone();

            let t1 = thread::spawn(move || {
                // Try to acquire read lock
                loop {
                    let current = r1.load(LoomOrdering::Relaxed);
                    if current == RWLOCK_WRITER {
                        spin_loop();
                        continue;
                    }
                    if r1
                        .compare_exchange_weak(
                            current,
                            current + 1,
                            LoomOrdering::Acquire,
                            LoomOrdering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                // Hold lock briefly
                r1.fetch_sub(1, LoomOrdering::Release);
            });

            let t2 = thread::spawn(move || {
                // Try to acquire read lock
                loop {
                    let current = r2.load(LoomOrdering::Relaxed);
                    if current == RWLOCK_WRITER {
                        spin_loop();
                        continue;
                    }
                    if r2
                        .compare_exchange_weak(
                            current,
                            current + 1,
                            LoomOrdering::Acquire,
                            LoomOrdering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                // Hold lock briefly
                r2.fetch_sub(1, LoomOrdering::Release);
            });

            t1.join().unwrap();
            t2.join().unwrap();

            // Lock should be unlocked
            assert_eq!(rwlock.load(LoomOrdering::Relaxed), RWLOCK_UNLOCKED);
        });
    }

    /// Test that writer excludes readers.
    ///
    /// When a writer holds the lock, readers should wait.
    #[test]
    fn test_writer_excludes_readers() {
        loom::model(|| {
            let rwlock = Arc::new(LoomAtomicU32::new(RWLOCK_UNLOCKED));

            let w = rwlock.clone();
            let r = rwlock.clone();

            // Writer thread
            let t1 = thread::spawn(move || {
                // Acquire write lock
                loop {
                    if w.compare_exchange(
                        RWLOCK_UNLOCKED,
                        RWLOCK_WRITER,
                        LoomOrdering::Acquire,
                        LoomOrdering::Relaxed,
                    )
                    .is_ok()
                    {
                        break;
                    }
                    spin_loop();
                }
                // Hold lock briefly then release
                w.store(RWLOCK_UNLOCKED, LoomOrdering::Release);
            });

            // Reader thread
            let t2 = thread::spawn(move || {
                loop {
                    let current = r.load(LoomOrdering::Relaxed);
                    if current == RWLOCK_WRITER {
                        spin_loop();
                        continue;
                    }
                    if r.compare_exchange_weak(
                        current,
                        current + 1,
                        LoomOrdering::Acquire,
                        LoomOrdering::Relaxed,
                    )
                    .is_ok()
                    {
                        break;
                    }
                }
                r.fetch_sub(1, LoomOrdering::Release);
            });

            t1.join().unwrap();
            t2.join().unwrap();

            assert_eq!(rwlock.load(LoomOrdering::Relaxed), RWLOCK_UNLOCKED);
        });
    }

    /// Test concurrent flag updates (deleted flag).
    ///
    /// Multiple threads can atomically update flags.
    #[test]
    fn test_concurrent_flag_updates() {
        loom::model(|| {
            let flags = Arc::new(LoomAtomicU32::new(0));

            let f1 = flags.clone();
            let f2 = flags.clone();

            // Thread 1: set deleted flag
            let t1 = thread::spawn(move || {
                f1.fetch_or(FLAG_DELETED, LoomOrdering::Release);
            });

            // Thread 2: set numeric flag
            let t2 = thread::spawn(move || {
                f2.fetch_or(FLAG_NUMERIC, LoomOrdering::Release);
            });

            t1.join().unwrap();
            t2.join().unwrap();

            // Both flags should be set
            let final_flags = flags.load(LoomOrdering::Acquire);
            assert!(final_flags & FLAG_DELETED != 0);
            assert!(final_flags & FLAG_NUMERIC != 0);
        });
    }

    /// Test expiration update CAS loop.
    ///
    /// Concurrent expiration updates should not lose data.
    #[test]
    fn test_concurrent_expire_update() {
        loom::model(|| {
            // Initial value: expire_at = 100
            let expire_and_flags = Arc::new(LoomAtomicU32::new(100));

            let e1 = expire_and_flags.clone();
            let e2 = expire_and_flags.clone();

            // Thread 1: update expiration to 200
            let t1 = thread::spawn(move || {
                loop {
                    let old = e1.load(LoomOrdering::Relaxed);
                    let new = (old & !EXPIRE_MASK) | (200 & EXPIRE_MASK);
                    if e1
                        .compare_exchange_weak(
                            old,
                            new,
                            LoomOrdering::Release,
                            LoomOrdering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
            });

            // Thread 2: set a flag (should preserve expiration)
            let t2 = thread::spawn(move || {
                e2.fetch_or(FLAG_NUMERIC, LoomOrdering::Release);
            });

            t1.join().unwrap();
            t2.join().unwrap();

            let final_val = expire_and_flags.load(LoomOrdering::Acquire);
            // Numeric flag should be set
            assert!(final_val & FLAG_NUMERIC != 0);
            // Expiration should be either 100 or 200 depending on ordering
            let expire = final_val & EXPIRE_MASK;
            assert!(expire == 100 || expire == 200);
        });
    }
}
