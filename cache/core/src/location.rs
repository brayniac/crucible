//! Opaque location type for cache storage.
//!
//! `Location` is a 44-bit packed value that identifies where an item is stored.
//! The hashtable treats this as an opaque identifier - storage backends define
//! their own interpretation of the bits.

use std::fmt;

/// Opaque 44-bit location value.
///
/// The hashtable stores this alongside a 12-bit tag and 8-bit frequency,
/// fitting in a single 64-bit atomic. The meaning of the 44 bits is defined
/// by the storage backend:
///
/// - Segment caches: pool_id (2) + segment_id (22) + offset/8 (20)
/// - Arc caches: index (40) + generation (4)
/// - Other backends: custom interpretation
///
/// ```text
/// Hashtable entry layout:
/// +--------+--------+---------------------------+
/// | 63..52 | 51..44 |          43..0            |
/// |  tag   |  freq  |         location          |
/// | 12 bits| 8 bits |         44 bits           |
/// +--------+--------+---------------------------+
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Location(u64);

impl Location {
    /// Maximum raw value (44 bits set).
    pub const MAX_RAW: u64 = 0xFFF_FFFF_FFFF;

    /// Sentinel value indicating a ghost entry (recently evicted).
    /// All 44 location bits set to 1.
    pub const GHOST: Self = Self(Self::MAX_RAW);

    /// Create a location from a raw 44-bit value.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if `raw > MAX_RAW`.
    #[inline]
    pub fn new(raw: u64) -> Self {
        debug_assert!(raw <= Self::MAX_RAW, "location exceeds 44 bits");
        Self(raw)
    }

    /// Get the raw 44-bit value.
    #[inline]
    pub fn as_raw(&self) -> u64 {
        self.0
    }

    /// Construct from raw value, masking to 44 bits.
    #[inline]
    pub fn from_raw(raw: u64) -> Self {
        Self(raw & Self::MAX_RAW)
    }

    /// Check if this is the ghost sentinel.
    #[inline]
    pub fn is_ghost(&self) -> bool {
        *self == Self::GHOST
    }
}

impl fmt::Debug for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_ghost() {
            write!(f, "Location::GHOST")
        } else {
            write!(f, "Location(0x{:011X})", self.0)
        }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_ghost() {
            write!(f, "GHOST")
        } else {
            write!(f, "0x{:011X}", self.0)
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_as_raw() {
        let loc = Location::new(0x123_4567_89AB);
        assert_eq!(loc.as_raw(), 0x123_4567_89AB);
        assert!(!loc.is_ghost());
    }

    #[test]
    fn test_ghost_sentinel() {
        assert!(Location::GHOST.is_ghost());
        assert_eq!(Location::GHOST.as_raw(), Location::MAX_RAW);
    }

    #[test]
    fn test_max_value() {
        let loc = Location::new(Location::MAX_RAW - 1);
        assert!(!loc.is_ghost());
        assert_eq!(loc.as_raw(), Location::MAX_RAW - 1);
    }

    #[test]
    fn test_zero() {
        let loc = Location::new(0);
        assert_eq!(loc.as_raw(), 0);
        assert!(!loc.is_ghost());
    }

    #[test]
    fn test_from_raw_masks() {
        // Should mask off bits above 44
        let loc = Location::from_raw(0xFFFF_FFFF_FFFF_FFFF);
        assert_eq!(loc.as_raw(), Location::MAX_RAW);
        assert!(loc.is_ghost());
    }

    #[test]
    fn test_equality() {
        let a = Location::new(12345);
        let b = Location::new(12345);
        let c = Location::new(12346);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_display() {
        let loc = Location::new(0x123);
        assert_eq!(format!("{}", loc), "0x00000000123");
        assert_eq!(format!("{}", Location::GHOST), "GHOST");
    }

    #[test]
    fn test_debug() {
        let loc = Location::new(0x123);
        let debug_str = format!("{:?}", loc);
        assert!(debug_str.contains("Location"));
        assert!(debug_str.contains("123"));

        let ghost_debug = format!("{:?}", Location::GHOST);
        assert!(ghost_debug.contains("GHOST"));
    }

    #[test]
    #[should_panic(expected = "location exceeds 44 bits")]
    #[cfg(debug_assertions)]
    fn test_overflow_panics() {
        Location::new(Location::MAX_RAW + 1);
    }
}
