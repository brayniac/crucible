//! Location encoding for heap cache slots.
//!
//! Encodes a slot index and generation counter into a 44-bit Location.
//!
//! # Layout
//!
//! ```text
//! | generation (12 bits) | slot_index (32 bits) |
//! |      bits 43-32      |      bits 31-0       |
//! ```
//!
//! Generation provides ABA protection when slots are reused.

use cache_core::Location;

/// Location interpretation for heap cache slots.
///
/// Layout: 12-bit generation + 32-bit slot index = 44 bits
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotLocation {
    slot_index: u32,
    generation: u16,
}

impl SlotLocation {
    const SLOT_BITS: u64 = 32;
    const SLOT_MASK: u64 = (1 << Self::SLOT_BITS) - 1;
    const GEN_SHIFT: u64 = Self::SLOT_BITS;
    const GEN_MASK: u64 = 0xFFF; // 12 bits

    /// Create a new slot location.
    #[inline]
    pub fn new(slot_index: u32, generation: u16) -> Self {
        debug_assert!(generation <= 0xFFF, "generation exceeds 12 bits");
        Self {
            slot_index,
            generation: generation & 0xFFF,
        }
    }

    /// Convert from opaque Location.
    #[inline]
    pub fn from_location(loc: Location) -> Self {
        let raw = loc.as_raw();
        Self {
            slot_index: (raw & Self::SLOT_MASK) as u32,
            generation: ((raw >> Self::GEN_SHIFT) & Self::GEN_MASK) as u16,
        }
    }

    /// Convert to opaque Location for hashtable storage.
    #[inline]
    pub fn to_location(self) -> Location {
        let raw = (self.slot_index as u64) | ((self.generation as u64) << Self::GEN_SHIFT);
        Location::new(raw)
    }

    /// Get the slot index.
    #[inline]
    pub fn slot_index(&self) -> u32 {
        self.slot_index
    }

    /// Get the generation counter.
    #[inline]
    pub fn generation(&self) -> u16 {
        self.generation
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let loc = SlotLocation::new(12345, 0xABC);
        let opaque = loc.to_location();
        let recovered = SlotLocation::from_location(opaque);

        assert_eq!(recovered.slot_index(), 12345);
        assert_eq!(recovered.generation(), 0xABC);
    }

    #[test]
    fn test_max_values() {
        let loc = SlotLocation::new(u32::MAX, 0xFFF);
        let opaque = loc.to_location();
        let recovered = SlotLocation::from_location(opaque);

        assert_eq!(recovered.slot_index(), u32::MAX);
        assert_eq!(recovered.generation(), 0xFFF);
    }

    #[test]
    fn test_zero_values() {
        let loc = SlotLocation::new(0, 0);
        let opaque = loc.to_location();
        let recovered = SlotLocation::from_location(opaque);

        assert_eq!(recovered.slot_index(), 0);
        assert_eq!(recovered.generation(), 0);
    }
}
