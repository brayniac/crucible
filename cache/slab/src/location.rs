//! Slab location encoding for the 44-bit location field.
//!
//! Encodes: pool_id (2 bits) + class_id (6 bits) + slab_id (20 bits) + slot_index (16 bits)
//!
//! Capacity:
//! - 4 storage pools (RAM + up to 3 disk tiers)
//! - 64 slab classes
//! - ~1M slabs per class
//! - 65K slots per slab

use cache_core::Location;

/// Maximum pool ID (2 bits = 0-3).
pub const MAX_POOL_ID: u8 = 3;

/// Maximum class ID (6 bits = 0-63).
pub const MAX_CLASS_ID: u8 = 63;

/// Maximum slab ID (20 bits = 0-1048575).
pub const MAX_SLAB_ID: u32 = (1 << 20) - 1;

/// Maximum slot index (16 bits = 0-65535).
#[allow(dead_code)]
pub const MAX_SLOT_INDEX: u16 = u16::MAX;

/// Slab location encoding.
///
/// ```text
/// 44-bit layout:
/// +--------+--------+----------+-----------+
/// | 43..42 | 41..36 |  35..16  |   15..0   |
/// |pool_id |class_id| slab_id  |slot_index |
/// | 2 bits | 6 bits | 20 bits  |  16 bits  |
/// +--------+--------+----------+-----------+
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlabLocation {
    pool_id: u8,
    class_id: u8,
    slab_id: u32,
    slot_index: u16,
}

impl SlabLocation {
    /// Create a new slab location with default pool_id (0 = RAM).
    ///
    /// # Panics
    ///
    /// Panics in debug mode if class_id exceeds 6 bits or slab_id exceeds 20 bits.
    #[inline]
    pub fn new(class_id: u8, slab_id: u32, slot_index: u16) -> Self {
        Self::with_pool(0, class_id, slab_id, slot_index)
    }

    /// Create a new slab location with explicit pool_id.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - pool_id > 3 (exceeds 2 bits)
    /// - class_id > 63 (exceeds 6 bits) - reduce slab_size or increase growth_factor
    /// - slab_id exceeds 20 bits (debug only)
    #[inline]
    pub fn with_pool(pool_id: u8, class_id: u8, slab_id: u32, slot_index: u16) -> Self {
        // Runtime checks to prevent silent data corruption from bit truncation
        assert!(
            pool_id <= MAX_POOL_ID,
            "pool_id {} exceeds max {}",
            pool_id,
            MAX_POOL_ID
        );
        assert!(
            class_id <= MAX_CLASS_ID,
            "class_id {} exceeds max {} - reduce slab_size or increase growth_factor to generate fewer classes",
            class_id,
            MAX_CLASS_ID
        );
        debug_assert!(slab_id <= MAX_SLAB_ID, "slab_id exceeds 20 bits");
        Self {
            pool_id,
            class_id,
            slab_id,
            slot_index,
        }
    }

    /// Get the pool ID (2 bits).
    #[inline(always)]
    pub fn pool_id(&self) -> u8 {
        self.pool_id
    }

    /// Get the class ID (6 bits).
    #[inline(always)]
    pub fn class_id(&self) -> u8 {
        self.class_id
    }

    /// Get the slab ID (20 bits).
    #[inline(always)]
    pub fn slab_id(&self) -> u32 {
        self.slab_id
    }

    /// Get the slot index (16 bits).
    #[inline(always)]
    pub fn slot_index(&self) -> u16 {
        self.slot_index
    }

    /// Convert to the opaque Location type for hashtable storage.
    #[inline]
    pub fn to_location(self) -> Location {
        let raw = ((self.pool_id as u64) << 42)
            | ((self.class_id as u64) << 36)
            | ((self.slab_id as u64) << 16)
            | (self.slot_index as u64);
        Location::new(raw)
    }

    /// Extract from an opaque Location.
    #[inline]
    pub fn from_location(loc: Location) -> Self {
        let raw = loc.as_raw();
        Self {
            pool_id: ((raw >> 42) & 0b11) as u8,
            class_id: ((raw >> 36) & 0x3F) as u8,
            slab_id: ((raw >> 16) & 0xFFFFF) as u32,
            slot_index: (raw & 0xFFFF) as u16,
        }
    }

    /// Extract pool_id from an opaque Location without full parsing.
    ///
    /// This is useful for quickly determining which storage tier a location
    /// belongs to without the overhead of extracting all fields.
    #[inline]
    pub fn pool_id_from_location(loc: Location) -> u8 {
        ((loc.as_raw() >> 42) & 0b11) as u8
    }

    /// Unpack all fields at once for efficiency.
    ///
    /// Returns `(class_id, slab_id, slot_index)` (not pool_id for backward compatibility).
    #[inline]
    pub fn unpack(&self) -> (u8, u32, u16) {
        (self.class_id, self.slab_id, self.slot_index)
    }

    /// Unpack all fields including pool_id.
    ///
    /// Returns `(pool_id, class_id, slab_id, slot_index)`.
    #[inline]
    pub fn unpack_with_pool(&self) -> (u8, u8, u32, u16) {
        (self.pool_id, self.class_id, self.slab_id, self.slot_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let loc = SlabLocation::new(42, 123456, 789);
        let location = loc.to_location();
        let decoded = SlabLocation::from_location(location);
        assert_eq!(decoded.pool_id(), 0); // default pool_id
        assert_eq!(decoded.class_id(), 42);
        assert_eq!(decoded.slab_id(), 123456);
        assert_eq!(decoded.slot_index(), 789);
    }

    #[test]
    fn test_roundtrip_with_pool() {
        let loc = SlabLocation::with_pool(2, 42, 123456, 789);
        let location = loc.to_location();
        let decoded = SlabLocation::from_location(location);
        assert_eq!(decoded.pool_id(), 2);
        assert_eq!(decoded.class_id(), 42);
        assert_eq!(decoded.slab_id(), 123456);
        assert_eq!(decoded.slot_index(), 789);
    }

    #[test]
    fn test_max_values() {
        let loc = SlabLocation::with_pool(MAX_POOL_ID, MAX_CLASS_ID, MAX_SLAB_ID, MAX_SLOT_INDEX);
        let location = loc.to_location();
        let decoded = SlabLocation::from_location(location);
        assert_eq!(decoded.pool_id(), MAX_POOL_ID);
        assert_eq!(decoded.class_id(), MAX_CLASS_ID);
        assert_eq!(decoded.slab_id(), MAX_SLAB_ID);
        assert_eq!(decoded.slot_index(), MAX_SLOT_INDEX);
    }

    #[test]
    fn test_zero_values() {
        let loc = SlabLocation::new(0, 0, 0);
        let location = loc.to_location();
        assert_eq!(location.as_raw(), 0);
        let decoded = SlabLocation::from_location(location);
        assert_eq!(decoded.pool_id(), 0);
        assert_eq!(decoded.class_id(), 0);
        assert_eq!(decoded.slab_id(), 0);
        assert_eq!(decoded.slot_index(), 0);
    }

    #[test]
    fn test_unpack() {
        let loc = SlabLocation::new(10, 20, 30);
        let (class, slab, slot) = loc.unpack();
        assert_eq!(class, 10);
        assert_eq!(slab, 20);
        assert_eq!(slot, 30);
    }

    #[test]
    fn test_unpack_with_pool() {
        let loc = SlabLocation::with_pool(3, 10, 20, 30);
        let (pool, class, slab, slot) = loc.unpack_with_pool();
        assert_eq!(pool, 3);
        assert_eq!(class, 10);
        assert_eq!(slab, 20);
        assert_eq!(slot, 30);
    }

    #[test]
    fn test_pool_id_from_location() {
        for pool_id in 0..=MAX_POOL_ID {
            let loc = SlabLocation::with_pool(pool_id, 10, 20, 30);
            let location = loc.to_location();
            assert_eq!(SlabLocation::pool_id_from_location(location), pool_id);
        }
    }

    #[test]
    fn test_fits_in_44_bits() {
        let loc = SlabLocation::with_pool(MAX_POOL_ID, MAX_CLASS_ID, MAX_SLAB_ID, MAX_SLOT_INDEX);
        let location = loc.to_location();
        assert!(location.as_raw() <= Location::MAX_RAW);
    }

    #[test]
    fn test_bit_layout() {
        // Verify bit layout: pool_id at 42-43, class_id at 36-41, slab_id at 16-35, slot_index at 0-15
        let loc = SlabLocation::with_pool(0b11, 0b11_1111, 0xFFFFF, 0xFFFF);
        let raw = loc.to_location().as_raw();

        // Pool bits at position 42-43
        assert_eq!((raw >> 42) & 0b11, 0b11);
        // Class bits at position 36-41
        assert_eq!((raw >> 36) & 0x3F, 0b11_1111);
        // Slab bits at position 16-35
        assert_eq!((raw >> 16) & 0xFFFFF, 0xFFFFF);
        // Slot bits at position 0-15
        assert_eq!(raw & 0xFFFF, 0xFFFF);
    }
}
