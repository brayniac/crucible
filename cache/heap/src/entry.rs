//! Heap-allocated cache entry with inline key and value.
//!
//! Each entry is allocated on the heap with a fixed header followed by
//! variable-length key and value data.
//!
//! # Memory Layout
//!
//! ```text
//! +----------------+
//! |   expire_at    |  4 bytes - seconds since epoch (or 0 for no expiry)
//! +----------------+
//! |     flags      |  4 bytes - atomic flags (deleted, etc.)
//! +----------------+
//! |   cas_token    |  8 bytes - atomic CAS token for compare-and-swap
//! +----------------+
//! |    key_len     |  2 bytes - key length
//! +----------------+
//! |   value_len    |  4 bytes - value length
//! +----------------+
//! |      key       |  key_len bytes
//! +----------------+
//! |     value      |  value_len bytes
//! +----------------+
//! ```

use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

/// Get the actual header size including any padding.
/// This uses the struct layout rather than hardcoding to handle alignment.
#[inline]
fn header_size() -> usize {
    std::mem::size_of::<HeapEntry>()
}

/// Calculate the total size of an entry with the given key and value lengths.
///
/// This is useful for pre-calculating memory requirements before allocation.
#[inline]
pub fn item_size(key_len: usize, value_len: usize) -> usize {
    header_size() + key_len + value_len
}

/// Flag indicating the entry has been deleted.
const FLAG_DELETED: u32 = 1 << 0;

/// Get current time as seconds since Unix epoch.
#[inline]
fn current_time_secs() -> u32 {
    clocksource::coarse::UnixInstant::now()
        .duration_since(clocksource::coarse::UnixInstant::EPOCH)
        .as_secs() as u32
}

/// A heap-allocated cache entry.
///
/// The entry stores key and value data inline after a fixed header.
/// The header contains metadata for TTL, CAS, and deletion tracking.
#[repr(C)]
pub struct HeapEntry {
    /// Expiration time (seconds since epoch, 0 = no expiry)
    expire_at: u32,
    /// Atomic flags (deleted, etc.)
    flags: AtomicU32,
    /// Atomic CAS token for compare-and-swap operations
    cas_token: AtomicU64,
    /// Key length
    key_len: u16,
    /// Value length
    value_len: u32,
    /// Flexible array member - key followed by value
    /// This is a zero-sized marker; actual data is accessed via pointer arithmetic
    _data: [u8; 0],
}

impl HeapEntry {
    /// Allocate a new heap entry with the given key and value.
    ///
    /// # Parameters
    /// - `key`: The key bytes
    /// - `value`: The value bytes
    /// - `ttl`: Time-to-live (0 = no expiry)
    /// - `cas_token`: Initial CAS token value
    ///
    /// # Safety
    ///
    /// The returned pointer must be freed with `HeapEntry::free()`.
    pub fn allocate(
        key: &[u8],
        value: &[u8],
        ttl: Duration,
        cas_token: u64,
    ) -> Option<NonNull<Self>> {
        if key.len() > u16::MAX as usize || value.len() > u32::MAX as usize {
            return None;
        }

        let total_size = header_size() + key.len() + value.len();
        let layout = Layout::from_size_align(total_size, 8).ok()?;

        // Allocate memory
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            return None;
        }

        let entry_ptr = ptr.cast::<HeapEntry>();

        // Calculate expiration time
        let expire_at = if ttl.is_zero() {
            0
        } else {
            current_time_secs() + ttl.as_secs() as u32
        };

        // Initialize the entry
        unsafe {
            // Write header fields
            std::ptr::addr_of_mut!((*entry_ptr).expire_at).write(expire_at);
            std::ptr::addr_of_mut!((*entry_ptr).flags).write(AtomicU32::new(0));
            std::ptr::addr_of_mut!((*entry_ptr).cas_token).write(AtomicU64::new(cas_token));
            std::ptr::addr_of_mut!((*entry_ptr).key_len).write(key.len() as u16);
            std::ptr::addr_of_mut!((*entry_ptr).value_len).write(value.len() as u32);

            // Write key and value data
            let data_ptr = ptr.add(header_size());
            std::ptr::copy_nonoverlapping(key.as_ptr(), data_ptr, key.len());
            std::ptr::copy_nonoverlapping(value.as_ptr(), data_ptr.add(key.len()), value.len());
        }

        NonNull::new(entry_ptr)
    }

    /// Free a heap entry.
    ///
    /// # Safety
    ///
    /// - `ptr` must have been allocated by `HeapEntry::allocate()`
    /// - `ptr` must not be used after this call
    pub unsafe fn free(ptr: NonNull<Self>) {
        unsafe {
            let entry = ptr.as_ptr();
            let key_len = (*entry).key_len as usize;
            let value_len = (*entry).value_len as usize;
            let total_size = header_size() + key_len + value_len;

            let layout = Layout::from_size_align_unchecked(total_size, 8);
            dealloc(ptr.as_ptr().cast(), layout);
        }
    }

    /// Get the key.
    #[inline]
    pub fn key(&self) -> &[u8] {
        unsafe {
            let data_ptr = (self as *const Self).cast::<u8>().add(header_size());
            std::slice::from_raw_parts(data_ptr, self.key_len as usize)
        }
    }

    /// Get the value.
    #[inline]
    pub fn value(&self) -> &[u8] {
        unsafe {
            let data_ptr = (self as *const Self).cast::<u8>().add(header_size());
            let value_ptr = data_ptr.add(self.key_len as usize);
            std::slice::from_raw_parts(value_ptr, self.value_len as usize)
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

    /// Get the total entry size (header + key + value).
    #[inline]
    pub fn total_size(&self) -> usize {
        header_size() + self.key_len as usize + self.value_len as usize
    }

    /// Check if the entry has expired.
    #[inline]
    pub fn is_expired(&self) -> bool {
        let expire_at = self.expire_at;
        if expire_at == 0 {
            return false; // No expiry
        }
        let now = current_time_secs();
        now >= expire_at
    }

    /// Get the remaining TTL.
    #[inline]
    pub fn remaining_ttl(&self) -> Option<Duration> {
        let expire_at = self.expire_at;
        if expire_at == 0 {
            return None; // No expiry
        }
        let now = current_time_secs();
        if now >= expire_at {
            Some(Duration::ZERO)
        } else {
            Some(Duration::from_secs((expire_at - now) as u64))
        }
    }

    /// Check if the entry is marked as deleted.
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.flags.load(Ordering::Acquire) & FLAG_DELETED != 0
    }

    /// Mark the entry as deleted.
    #[inline]
    pub fn mark_deleted(&self) {
        self.flags.fetch_or(FLAG_DELETED, Ordering::Release);
    }

    /// Get the CAS token.
    #[inline]
    pub fn cas_token(&self) -> u64 {
        self.cas_token.load(Ordering::Acquire)
    }

    /// Increment and return the new CAS token.
    #[inline]
    pub fn increment_cas_token(&self) -> u64 {
        self.cas_token.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Compare-and-swap the CAS token.
    ///
    /// Returns `true` if the current token matched and was updated.
    #[inline]
    pub fn cas_token_matches(&self, expected: u64) -> bool {
        self.cas_token.load(Ordering::Acquire) == expected
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_and_free() {
        let key = b"test_key";
        let value = b"test_value";
        let ttl = Duration::from_secs(3600);

        let entry_ptr = HeapEntry::allocate(key, value, ttl, 1).expect("allocation failed");

        unsafe {
            let entry = entry_ptr.as_ref();
            assert_eq!(entry.key(), key);
            assert_eq!(entry.value(), value);
            assert!(!entry.is_expired());
            assert!(!entry.is_deleted());

            HeapEntry::free(entry_ptr);
        }
    }

    #[test]
    fn test_empty_key_value() {
        let entry_ptr =
            HeapEntry::allocate(b"", b"", Duration::ZERO, 1).expect("allocation failed");

        unsafe {
            let entry = entry_ptr.as_ref();
            assert!(entry.key().is_empty());
            assert!(entry.value().is_empty());

            HeapEntry::free(entry_ptr);
        }
    }

    #[test]
    fn test_deleted_flag() {
        let entry_ptr =
            HeapEntry::allocate(b"key", b"value", Duration::ZERO, 1).expect("allocation failed");

        unsafe {
            let entry = entry_ptr.as_ref();
            assert!(!entry.is_deleted());

            entry.mark_deleted();
            assert!(entry.is_deleted());

            HeapEntry::free(entry_ptr);
        }
    }

    #[test]
    fn test_cas_token() {
        let entry_ptr =
            HeapEntry::allocate(b"key", b"value", Duration::ZERO, 1).expect("allocation failed");

        unsafe {
            let entry = entry_ptr.as_ref();
            let initial = entry.cas_token();
            assert_eq!(initial, 1);

            let new_token = entry.increment_cas_token();
            assert_eq!(new_token, 2);
            assert_eq!(entry.cas_token(), 2);

            assert!(entry.cas_token_matches(2));
            assert!(!entry.cas_token_matches(1));

            HeapEntry::free(entry_ptr);
        }
    }

    #[test]
    fn test_total_size() {
        let key = b"hello";
        let value = b"world";
        let entry_ptr =
            HeapEntry::allocate(key, value, Duration::ZERO, 1).expect("allocation failed");

        unsafe {
            let entry = entry_ptr.as_ref();
            assert_eq!(entry.total_size(), header_size() + 5 + 5);
            assert_eq!(entry.key_len(), 5);
            assert_eq!(entry.value_len(), 5);

            HeapEntry::free(entry_ptr);
        }
    }
}
