//! Cache trait for compatibility with server implementations.
//!
//! This module provides a simple `Cache` trait that can be implemented by
//! different cache backends (S3FifoCache, SegCache, etc.) for use with
//! protocol servers like iou-cache and tokio-cache.

use crate::error::CacheError;
use crate::sync::{AtomicU32, Ordering};
use bytes::Bytes;
use std::time::Duration;

/// A simple guard that owns the cached value.
///
/// This is returned by cache get operations and provides access to the value.
#[derive(Debug, Clone)]
pub struct OwnedGuard {
    value: Vec<u8>,
}

/// A zero-copy reference to a cached value.
///
/// This struct holds a reference to the value bytes directly in segment memory,
/// keeping the segment's ref_count incremented to prevent eviction.
///
/// # Safety
///
/// This type is `Send` because:
/// - The ref_count pointer points to an AtomicU32 in a 'static MemoryPool
/// - The value pointer points to memory in that same pool
/// - The pool outlives all ValueRefs (pool is dropped after all refs are released)
///
/// # Usage
///
/// Use this for zero-copy scatter-gather I/O where you need to send the value
/// directly from cache memory without copying to an intermediate buffer.
pub struct ValueRef {
    /// Pointer to the segment's ref_count for proper cleanup.
    ref_count: *const AtomicU32,
    /// Pointer to the value data in segment memory.
    value_ptr: *const u8,
    /// Length of the value in bytes.
    value_len: usize,
}

impl ValueRef {
    /// Create a new ValueRef.
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - `ref_count` points to a valid AtomicU32 that has been incremented
    /// - `value_ptr` and `value_len` describe valid memory that will remain
    ///   valid as long as the ref_count is held
    #[inline]
    pub unsafe fn new(ref_count: *const AtomicU32, value_ptr: *const u8, value_len: usize) -> Self {
        Self {
            ref_count,
            value_ptr,
            value_len,
        }
    }

    /// Get the value as a byte slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: The ref_count being held guarantees the segment memory is valid
        unsafe { std::slice::from_raw_parts(self.value_ptr, self.value_len) }
    }

    /// Get the length of the value.
    #[inline]
    pub fn len(&self) -> usize {
        self.value_len
    }

    /// Check if the value is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.value_len == 0
    }

    /// Get the raw pointer to the value data.
    ///
    /// This is useful for scatter-gather I/O where you need to pass
    /// the buffer directly to the kernel.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.value_ptr
    }

    /// Convert the ValueRef into an owned `Bytes` that keeps the segment ref alive.
    ///
    /// This is useful for zero-copy I/O where the buffer needs to outlive the
    /// current scope (e.g., for io_uring operations that complete asynchronously).
    /// The segment ref count is held until the returned `Bytes` is dropped.
    #[inline]
    pub fn into_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }
}

impl AsRef<[u8]> for ValueRef {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::ops::Deref for ValueRef {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl Drop for ValueRef {
    fn drop(&mut self) {
        // SAFETY: ref_count is a valid AtomicU32 pointer from the segment
        unsafe {
            (*self.ref_count).fetch_sub(1, Ordering::Release);
        }
    }
}

// SAFETY: ValueRef can be sent between threads because:
// 1. The ref_count points to an AtomicU32 in a 'static MemoryPool
// 2. The value memory is in the same pool
// 3. The pool's lifetime exceeds all ValueRefs
unsafe impl Send for ValueRef {}

// SAFETY: ValueRef can be shared between threads because:
// 1. All access to the value is read-only
// 2. The ref_count uses atomic operations
unsafe impl Sync for ValueRef {}

impl OwnedGuard {
    /// Create a new owned guard from a value.
    pub fn new(value: Vec<u8>) -> Self {
        Self { value }
    }

    /// Get the value as a byte slice.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Consume the guard and return the owned value.
    pub fn into_value(self) -> Vec<u8> {
        self.value
    }
}

impl AsRef<[u8]> for OwnedGuard {
    fn as_ref(&self) -> &[u8] {
        &self.value
    }
}

/// Trait for cache implementations.
///
/// This trait defines the core operations that cache backends must support
/// for use with protocol servers.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow concurrent access from
/// multiple threads.
pub trait Cache: Send + Sync + 'static {
    /// Get a value from the cache.
    ///
    /// Returns `Some(guard)` if the key exists, `None` otherwise.
    /// The guard provides access to the value.
    ///
    /// Note: This method copies the value into an owned buffer. For better
    /// performance, use [`with_value`](Self::with_value) to avoid the copy.
    fn get(&self, key: &[u8]) -> Option<OwnedGuard>;

    /// Access a cached value without copying.
    ///
    /// Calls the provided function with the value bytes if the key exists.
    /// The value is read directly from cache memory without copying.
    ///
    /// This is more efficient than [`get`](Self::get) when you only need
    /// to read or copy the value to another buffer (e.g., a response buffer).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Write value directly to response buffer without intermediate copy
    /// cache.with_value(key, |value| {
    ///     response_buf.extend_from_slice(value);
    /// });
    /// ```
    fn with_value<F, R>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R;

    /// Get a zero-copy reference to a cached value.
    ///
    /// Returns a [`ValueRef`] that holds a reference to the value directly
    /// in cache memory, preventing the segment from being evicted while
    /// the reference is held.
    ///
    /// This is the most efficient way to read values for scatter-gather I/O,
    /// as the value bytes can be sent directly to the network without any
    /// intermediate copies.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get zero-copy reference for vectored write
    /// if let Some(value_ref) = cache.get_value_ref(key) {
    ///     // value_ref.as_slice() points directly into cache memory
    ///     driver.send_vectored(conn_id, &[
    ///         IoSlice::new(header.as_bytes()),
    ///         IoSlice::new(value_ref.as_slice()),
    ///         IoSlice::new(b"\r\n"),
    ///     ]);
    ///     // Segment stays pinned until value_ref is dropped
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// - `Some(ValueRef)` if the key exists
    /// - `None` if the key is not found
    ///
    /// # Note
    ///
    /// Holding `ValueRef` prevents segment eviction. For long-lived references
    /// or slow consumers (e.g., network writes to slow clients), prefer
    /// copying the value to avoid blocking cache eviction.
    fn get_value_ref(&self, key: &[u8]) -> Option<ValueRef>;

    /// Set a key-value pair in the cache.
    ///
    /// `ttl` specifies the time-to-live for the entry. If `None`, a default
    /// TTL is used (implementation-dependent).
    fn set(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError>;

    /// Begin a two-phase SET operation for zero-copy receive.
    ///
    /// Returns a reservation with a pre-sized buffer that the caller can
    /// write to directly (e.g., from a network receive). Call `commit_set`
    /// to finalize the operation.
    ///
    /// # Benefits
    ///
    /// - Single allocation of exact size (vs growing coalesce buffer)
    /// - Buffer is correctly sized before receive starts
    /// - Avoids multiple extend() calls during network receive
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Parse header to learn value size
    /// let (key, value_len) = parse_set_header(buffer)?;
    ///
    /// // Reserve space
    /// let mut reservation = cache.begin_set(key, value_len, None)?;
    ///
    /// // Receive directly into reservation buffer
    /// socket.recv_exact(reservation.value_mut())?;
    ///
    /// // Commit
    /// cache.commit_set(reservation)?;
    /// ```
    fn begin_set(
        &self,
        key: &[u8],
        value_len: usize,
        ttl: Option<Duration>,
    ) -> Result<crate::SetReservation, CacheError> {
        let ttl = ttl.unwrap_or(DEFAULT_TTL);
        Ok(crate::SetReservation::new(key, value_len, &[], ttl))
    }

    /// Commit a two-phase SET operation.
    ///
    /// Writes the reservation's data to the cache. The reservation is consumed.
    fn commit_set(&self, reservation: crate::SetReservation) -> Result<(), CacheError> {
        let (key, value, _optional, ttl) = reservation.into_parts();
        self.set(&key, &value, Some(ttl))
    }

    /// Delete a key from the cache.
    ///
    /// Returns `true` if the key was present and deleted, `false` otherwise.
    fn delete(&self, key: &[u8]) -> bool;

    /// Check if a key exists in the cache.
    fn contains(&self, key: &[u8]) -> bool;

    /// Flush all entries from the cache.
    ///
    /// Note: This may be a no-op for some implementations.
    fn flush(&self);

    /// Begin a two-phase SET for zero-copy receive into segment memory.
    ///
    /// Returns a `SegmentReservation` with a mutable pointer to segment memory.
    /// The caller writes the value directly to this memory, then calls
    /// `commit_segment_set` to finalize.
    ///
    /// # Default Implementation
    ///
    /// Returns `CacheError::Unsupported` - only TieredCache supports this.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut reservation = cache.begin_segment_set(key, value_len, ttl)?;
    /// socket.recv_exact(reservation.value_mut())?;
    /// cache.commit_segment_set(reservation)?;
    /// ```
    fn begin_segment_set(
        &self,
        _key: &[u8],
        _value_len: usize,
        _ttl: Option<Duration>,
    ) -> Result<crate::SegmentReservation, CacheError> {
        Err(CacheError::Unsupported)
    }

    /// Commit a segment-based SET operation.
    ///
    /// Finalizes the segment write and updates the hashtable.
    ///
    /// # Default Implementation
    ///
    /// Returns `CacheError::Unsupported` - only TieredCache supports this.
    fn commit_segment_set(
        &self,
        _reservation: crate::SegmentReservation,
    ) -> Result<(), CacheError> {
        Err(CacheError::Unsupported)
    }

    /// Cancel a segment-based SET operation.
    ///
    /// Marks the reserved space as deleted. Call this if the receive fails.
    fn cancel_segment_set(&self, _reservation: crate::SegmentReservation) {}

    /// Get a value with its CAS token for GETS response.
    ///
    /// Returns the value and a CAS token that can be used for subsequent
    /// CAS operations. The token is unique to this version of the item.
    ///
    /// # Default Implementation
    ///
    /// Returns `None` - only TieredCache-based implementations support this.
    fn get_with_cas(&self, _key: &[u8]) -> Option<(Vec<u8>, u64)> {
        None
    }

    /// Zero-copy variant of get_with_cas.
    ///
    /// Calls the provided function with the value bytes and returns
    /// the result along with the CAS token.
    ///
    /// # Default Implementation
    ///
    /// Returns `None` - only TieredCache-based implementations support this.
    fn with_value_cas<F, R>(&self, _key: &[u8], _f: F) -> Option<(R, u64)>
    where
        F: FnOnce(&[u8]) -> R,
    {
        None
    }

    /// Compare-and-swap: update an item only if the CAS token matches.
    ///
    /// This implements memcached CAS semantics:
    /// - If the key doesn't exist, returns `Err(CacheError::KeyNotFound)`
    /// - If the CAS token doesn't match, returns `Ok(false)` (EXISTS response)
    /// - If the CAS token matches, updates the item and returns `Ok(true)` (STORED)
    ///
    /// # Default Implementation
    ///
    /// Returns `Err(CacheError::Unsupported)` - only TieredCache supports this.
    fn cas(
        &self,
        _key: &[u8],
        _value: &[u8],
        _ttl: Option<Duration>,
        _cas: u64,
    ) -> Result<bool, CacheError> {
        Err(CacheError::Unsupported)
    }
}

/// Default TTL used when None is provided (1 hour).
pub const DEFAULT_TTL: Duration = Duration::from_secs(3600);

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_owned_guard_new() {
        let data = vec![1, 2, 3, 4, 5];
        let guard = OwnedGuard::new(data.clone());
        assert_eq!(guard.value(), &data[..]);
    }

    #[test]
    fn test_owned_guard_value() {
        let guard = OwnedGuard::new(vec![10, 20, 30]);
        assert_eq!(guard.value(), &[10, 20, 30]);
    }

    #[test]
    fn test_owned_guard_into_value() {
        let data = vec![1, 2, 3];
        let guard = OwnedGuard::new(data.clone());
        let extracted = guard.into_value();
        assert_eq!(extracted, data);
    }

    #[test]
    fn test_owned_guard_as_ref() {
        let guard = OwnedGuard::new(vec![5, 6, 7]);
        let slice: &[u8] = guard.as_ref();
        assert_eq!(slice, &[5, 6, 7]);
    }

    #[test]
    fn test_owned_guard_clone() {
        let guard1 = OwnedGuard::new(vec![1, 2, 3]);
        let guard2 = guard1.clone();
        assert_eq!(guard1.value(), guard2.value());
    }

    #[test]
    fn test_owned_guard_debug() {
        let guard = OwnedGuard::new(vec![1, 2, 3]);
        let debug_str = format!("{:?}", guard);
        assert!(debug_str.contains("OwnedGuard"));
    }

    #[test]
    fn test_owned_guard_empty() {
        let guard = OwnedGuard::new(vec![]);
        assert!(guard.value().is_empty());
        assert_eq!(guard.into_value().len(), 0);
    }

    #[test]
    fn test_default_ttl() {
        assert_eq!(DEFAULT_TTL, Duration::from_secs(3600));
        assert_eq!(DEFAULT_TTL.as_secs(), 3600);
    }
}
