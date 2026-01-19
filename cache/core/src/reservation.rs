//! Reservation types for two-phase write operations.
//!
//! This module provides the infrastructure for zero-copy receives where:
//! 1. Space is reserved with a known value size
//! 2. The caller writes the value directly into the reserved buffer
//! 3. The reservation is committed to finalize the operation
//!
//! If a reservation is dropped without committing, no cache modification occurs.

use std::time::Duration;

/// A reservation for a SET operation that provides a pre-sized buffer.
///
/// This type is created by `Cache::begin_set()` and provides:
/// - Pre-allocated buffer of exactly the right size (avoids coalescing)
/// - Mutable access for direct network receive
/// - Commit to finalize the operation
/// - Automatic cleanup on drop if not committed
///
/// # Zero-Copy Receive Flow
///
/// ```ignore
/// // 1. Parse header to learn value size
/// let (header, value_len) = parse_set_header(buffer)?;
///
/// // 2. Reserve space (single allocation of correct size)
/// let mut reservation = cache.begin_set(header.key, value_len, optional, ttl)?;
///
/// // 3. Receive value directly into reservation buffer
/// let value_buf = reservation.value_mut();
/// socket.recv_exact(value_buf)?;
///
/// // 4. Commit to write to cache
/// reservation.commit()?;
/// ```
///
/// # Benefits
///
/// - Single allocation of exact size (vs growing coalesce buffer)
/// - Buffer is sized correctly upfront (vs multiple recv + extend)
/// - Same API works for direct segment writes in future
#[derive(Debug)]
pub struct SetReservation {
    /// Pre-allocated buffer for the value.
    value_buffer: Vec<u8>,
    /// The key for this SET operation.
    key: Vec<u8>,
    /// Optional metadata (flags, CAS, etc.).
    optional: Vec<u8>,
    /// TTL for the item.
    ttl: Duration,
}

impl SetReservation {
    /// Create a new reservation with a pre-sized value buffer.
    pub fn new(key: &[u8], value_len: usize, optional: &[u8], ttl: Duration) -> Self {
        Self {
            value_buffer: vec![0u8; value_len],
            key: key.to_vec(),
            optional: optional.to_vec(),
            ttl,
        }
    }

    /// Get the reserved value length.
    #[inline]
    pub fn value_len(&self) -> usize {
        self.value_buffer.len()
    }

    /// Get mutable access to the value buffer.
    ///
    /// The caller can write data directly to this buffer, typically
    /// by using it as the target for a network receive operation.
    #[inline]
    pub fn value_mut(&mut self) -> &mut [u8] {
        &mut self.value_buffer
    }

    /// Get read access to the value buffer.
    #[inline]
    pub fn value(&self) -> &[u8] {
        &self.value_buffer
    }

    /// Get the key for this reservation.
    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Get the optional metadata.
    #[inline]
    pub fn optional(&self) -> &[u8] {
        &self.optional
    }

    /// Get the TTL for this reservation.
    #[inline]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Consume the reservation and return its parts for cache insertion.
    ///
    /// Returns (key, value, optional, ttl) for use with cache.set().
    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>, Vec<u8>, Duration) {
        (self.key, self.value_buffer, self.optional, self.ttl)
    }
}

/// A reservation for a SET operation that writes directly to segment memory.
///
/// This type provides true zero-copy receive by reserving space in a cache
/// segment and allowing direct writes to segment memory. When committed, the
/// item is finalized in place without any copies.
///
/// # Zero-Copy Receive Flow
///
/// ```ignore
/// // 1. Parse header to learn value size
/// let (key, value_len) = parse_set_header(buffer)?;
///
/// // 2. Reserve segment space (writes header + key, returns value pointer)
/// let mut reservation = cache.begin_segment_set(key, value_len, ttl)?;
///
/// // 3. Receive value directly into segment memory
/// socket.recv_exact(reservation.value_mut())?;
///
/// // 4. Commit to finalize segment write and update hashtable
/// cache.commit_segment_set(reservation)?;
/// ```
///
/// # Cancellation
///
/// If the reservation is dropped without calling `commit()` (e.g., connection
/// closed during receive), the reserved space is marked as deleted.
#[derive(Debug)]
pub struct SegmentReservation {
    /// Item location in the segment (pool_id, segment_id, offset).
    location: crate::ItemLocation,
    /// Pointer to the value area in segment memory.
    value_ptr: *mut u8,
    /// Length of the value buffer.
    value_len: usize,
    /// Key copy (needed for hashtable insert on commit).
    key: Vec<u8>,
    /// TTL for this item.
    ttl: Duration,
    /// Padded item size (needed for finalize_append).
    item_size: u32,
    /// Whether this reservation has been committed.
    committed: bool,
}

// SAFETY: SegmentReservation can be sent between threads because:
// 1. The segment memory is thread-safe (atomic operations)
// 2. The caller is responsible for ensuring no concurrent writes
unsafe impl Send for SegmentReservation {}

impl SegmentReservation {
    /// Create a new segment reservation.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `value_ptr` points to valid, writable
    /// memory of at least `value_len` bytes that will remain valid until
    /// the reservation is committed or dropped.
    pub unsafe fn new(
        location: crate::ItemLocation,
        value_ptr: *mut u8,
        value_len: usize,
        key: Vec<u8>,
        ttl: Duration,
        item_size: u32,
    ) -> Self {
        Self {
            location,
            value_ptr,
            value_len,
            key,
            ttl,
            item_size,
            committed: false,
        }
    }

    /// Get the item location in the segment.
    #[inline]
    pub fn location(&self) -> crate::ItemLocation {
        self.location
    }

    /// Get the item size (for finalization).
    #[inline]
    pub fn item_size(&self) -> u32 {
        self.item_size
    }

    /// Get the key for this reservation.
    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Get the TTL for this reservation.
    #[inline]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Get the value length.
    #[inline]
    pub fn value_len(&self) -> usize {
        self.value_len
    }

    /// Get mutable access to the value buffer in segment memory.
    ///
    /// The caller can write data directly to this buffer, typically
    /// by using it as the target for a network receive operation.
    #[inline]
    pub fn value_mut(&mut self) -> &mut [u8] {
        // SAFETY: The caller of `new()` guarantees the pointer is valid
        unsafe { std::slice::from_raw_parts_mut(self.value_ptr, self.value_len) }
    }

    /// Get read access to the value buffer.
    #[inline]
    pub fn value(&self) -> &[u8] {
        // SAFETY: The caller of `new()` guarantees the pointer is valid
        unsafe { std::slice::from_raw_parts(self.value_ptr, self.value_len) }
    }

    /// Check if this reservation has been committed.
    #[inline]
    pub fn is_committed(&self) -> bool {
        self.committed
    }

    /// Mark this reservation as committed.
    #[inline]
    pub(crate) fn mark_committed(&mut self) {
        self.committed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reservation_new() {
        let reservation = SetReservation::new(b"testkey", 1024, b"flags", Duration::from_secs(60));

        assert_eq!(reservation.key(), b"testkey");
        assert_eq!(reservation.value_len(), 1024);
        assert_eq!(reservation.optional(), b"flags");
        assert_eq!(reservation.ttl(), Duration::from_secs(60));
    }

    #[test]
    fn test_reservation_value_mut() {
        let mut reservation = SetReservation::new(b"key", 5, b"", Duration::from_secs(60));

        let buf = reservation.value_mut();
        buf.copy_from_slice(b"hello");

        assert_eq!(reservation.value(), b"hello");
    }

    #[test]
    fn test_reservation_into_parts() {
        let mut reservation = SetReservation::new(b"key", 5, b"opt", Duration::from_secs(30));
        reservation.value_mut().copy_from_slice(b"value");

        let (key, value, optional, ttl) = reservation.into_parts();

        assert_eq!(key, b"key");
        assert_eq!(value, b"value");
        assert_eq!(optional, b"opt");
        assert_eq!(ttl, Duration::from_secs(30));
    }
}
