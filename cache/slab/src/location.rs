//! Slab location encoding for the 44-bit location field.
//!
//! Encodes: class_id (8 bits) + slab_id (20 bits) + slot_index (16 bits)
//!
//! Capacity:
//! - 256 slab classes
//! - ~1M slabs per class
//! - 65K slots per slab

use cache_core::Location;

/// Maximum class ID (8 bits = 0-255).
pub const MAX_CLASS_ID: u8 = 255;

/// Maximum slab ID (20 bits = 0-1048575).
pub const MAX_SLAB_ID: u32 = (1 << 20) - 1;

/// Maximum slot index (16 bits = 0-65535).
pub const MAX_SLOT_INDEX: u16 = u16::MAX;

/// Slab location encoding.
///
/// ```text
/// 44-bit layout:
/// +--------+----------+-----------+
/// | 43..36 |  35..16  |   15..0   |
/// |class_id| slab_id  |slot_index |
/// | 8 bits | 20 bits  |  16 bits  |
/// +--------+----------+-----------+
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlabLocation {
    class_id: u8,
    slab_id: u32,
    slot_index: u16,
}

impl SlabLocation {
    /// Create a new slab location.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if slab_id exceeds 20 bits.
    #[inline]
    pub fn new(class_id: u8, slab_id: u32, slot_index: u16) -> Self {
        debug_assert!(slab_id <= MAX_SLAB_ID, "slab_id exceeds 20 bits");
        Self {
            class_id,
            slab_id,
            slot_index,
        }
    }

    /// Get the class ID (8 bits).
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
    pub fn to_location(&self) -> Location {
        let raw = ((self.class_id as u64) << 36)
            | ((self.slab_id as u64) << 16)
            | (self.slot_index as u64);
        Location::new(raw)
    }

    /// Extract from an opaque Location.
    #[inline]
    pub fn from_location(loc: Location) -> Self {
        let raw = loc.as_raw();
        Self {
            class_id: ((raw >> 36) & 0xFF) as u8,
            slab_id: ((raw >> 16) & 0xFFFFF) as u32,
            slot_index: (raw & 0xFFFF) as u16,
        }
    }

    /// Unpack all fields at once for efficiency.
    #[inline]
    pub fn unpack(&self) -> (u8, u32, u16) {
        (self.class_id, self.slab_id, self.slot_index)
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
        assert_eq!(decoded.class_id(), 42);
        assert_eq!(decoded.slab_id(), 123456);
        assert_eq!(decoded.slot_index(), 789);
    }

    #[test]
    fn test_max_values() {
        let loc = SlabLocation::new(MAX_CLASS_ID, MAX_SLAB_ID, MAX_SLOT_INDEX);
        let location = loc.to_location();
        let decoded = SlabLocation::from_location(location);
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
    fn test_fits_in_44_bits() {
        let loc = SlabLocation::new(MAX_CLASS_ID, MAX_SLAB_ID, MAX_SLOT_INDEX);
        let location = loc.to_location();
        assert!(location.as_raw() <= Location::MAX_RAW);
    }
}
