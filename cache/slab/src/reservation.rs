//! Reservation types for two-phase write operations.
//!
//! This module provides infrastructure for zero-copy receives where:
//! 1. Space is reserved in a slab slot
//! 2. The caller writes the value directly into the slot's value area
//! 3. The reservation is committed to finalize the operation
//!
//! If a reservation is dropped without committing, the slot is returned
//! to the free list.

use std::time::Duration;

use crate::location::SlabLocation;

/// A reservation for a SET operation that writes directly to slab memory.
///
/// This type provides true zero-copy receive by reserving a slot in a slab
/// class and allowing direct writes to slot memory. When committed, the
/// item is finalized in place without any copies.
///
/// # Zero-Copy Receive Flow
///
/// ```ignore
/// // 1. Parse header to learn value size
/// let (key, value_len) = parse_set_header(buffer)?;
///
/// // 2. Reserve slot space (writes header + key, returns value pointer)
/// let mut reservation = cache.begin_slab_set(key, value_len, ttl)?;
///
/// // 3. Receive value directly into slab memory
/// socket.recv_exact(reservation.value_mut())?;
///
/// // 4. Commit to finalize the write and update hashtable
/// cache.commit_slab_set(reservation)?;
/// ```
///
/// # Cancellation
///
/// If the reservation is dropped without calling `commit()` (e.g., connection
/// closed during receive), the reserved slot is returned to the free list.
#[derive(Debug)]
pub struct SlabReservation {
    /// Item location in the slab (class_id, slab_id, slot_index).
    location: SlabLocation,
    /// Pointer to the value area in slot memory.
    value_ptr: *mut u8,
    /// Length of the value buffer.
    value_len: usize,
    /// Key copy (needed for hashtable insert on commit).
    key: Vec<u8>,
    /// TTL for this item.
    ttl: Duration,
    /// Total item size (header + key + value).
    item_size: usize,
    /// Whether this reservation has been committed.
    committed: bool,
}

// SAFETY: SlabReservation can be sent between threads because:
// 1. The slab memory is thread-safe (atomic state machine)
// 2. The caller is responsible for ensuring no concurrent writes
unsafe impl Send for SlabReservation {}

impl SlabReservation {
    /// Create a new slab reservation.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `value_ptr` points to valid, writable
    /// memory of at least `value_len` bytes that will remain valid until
    /// the reservation is committed or dropped.
    pub unsafe fn new(
        location: SlabLocation,
        value_ptr: *mut u8,
        value_len: usize,
        key: Vec<u8>,
        ttl: Duration,
        item_size: usize,
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

    /// Get the item location in the slab.
    #[inline]
    pub fn location(&self) -> SlabLocation {
        self.location
    }

    /// Get the total item size.
    #[inline]
    pub fn item_size(&self) -> usize {
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

    /// Get mutable access to the value buffer in slab memory.
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
    fn test_reservation_value_access() {
        let mut buffer = [0u8; 64];
        let location = SlabLocation::new(0, 0, 0);

        let mut reservation = unsafe {
            SlabReservation::new(
                location,
                buffer.as_mut_ptr(),
                10,
                b"key".to_vec(),
                Duration::from_secs(60),
                25,
            )
        };

        // Write to value
        let val = reservation.value_mut();
        val.copy_from_slice(b"0123456789");

        // Read back
        assert_eq!(reservation.value(), b"0123456789");
        assert_eq!(reservation.value_len(), 10);
    }

    #[test]
    fn test_reservation_properties() {
        let buffer = [0u8; 64];
        let location = SlabLocation::new(1, 2, 3);

        let reservation = unsafe {
            SlabReservation::new(
                location,
                buffer.as_ptr() as *mut u8,
                20,
                b"testkey".to_vec(),
                Duration::from_secs(120),
                45,
            )
        };

        assert_eq!(reservation.key(), b"testkey");
        assert_eq!(reservation.ttl(), Duration::from_secs(120));
        assert_eq!(reservation.item_size(), 45);
        assert_eq!(reservation.value_len(), 20);
        assert!(!reservation.is_committed());

        let (class_id, slab_id, slot_index) = reservation.location().unpack();
        assert_eq!(class_id, 1);
        assert_eq!(slab_id, 2);
        assert_eq!(slot_index, 3);
    }

    #[test]
    fn test_reservation_committed() {
        let buffer = [0u8; 64];
        let location = SlabLocation::new(0, 0, 0);

        let mut reservation = unsafe {
            SlabReservation::new(
                location,
                buffer.as_ptr() as *mut u8,
                10,
                b"key".to_vec(),
                Duration::from_secs(60),
                25,
            )
        };

        assert!(!reservation.is_committed());
        reservation.mark_committed();
        assert!(reservation.is_committed());
    }
}
