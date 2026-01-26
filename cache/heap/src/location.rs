//! Location encoding for heap cache slots.
//!
//! Encodes a pool ID, generation counter, and slot index into a 44-bit Location.
//!
//! # Layout
//!
//! ```text
//! | pool_id (2 bits) | generation (10 bits) | slot_index (32 bits) |
//! |    bits 43-42    |      bits 41-32      |      bits 31-0       |
//! ```
//!
//! - pool_id: Identifies the storage tier (0=RAM, 2=disk tier)
//! - generation: Provides ABA protection when slots are reused
//! - slot_index: Index into the slot storage array

use cache_core::Location;

/// Maximum pool ID (2 bits = 0-3).
pub const MAX_POOL_ID: u8 = 3;

/// Maximum generation value (10 bits = 0-1023).
pub const MAX_GENERATION: u16 = 0x3FF;

/// Location interpretation for heap cache slots.
///
/// Layout: 2-bit pool_id + 10-bit generation + 32-bit slot index = 44 bits
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotLocation {
    pool_id: u8,
    slot_index: u32,
    generation: u16,
}

impl SlotLocation {
    const SLOT_BITS: u64 = 32;
    const SLOT_MASK: u64 = (1 << Self::SLOT_BITS) - 1;
    const GEN_SHIFT: u64 = Self::SLOT_BITS;
    const GEN_MASK: u64 = 0x3FF; // 10 bits
    const POOL_SHIFT: u64 = 42;
    const POOL_MASK: u64 = 0b11;

    /// Create a new slot location with default pool_id (0 = RAM).
    #[inline]
    pub fn new(slot_index: u32, generation: u16) -> Self {
        Self::with_pool(0, slot_index, generation)
    }

    /// Create a new slot location with explicit pool_id.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if:
    /// - pool_id exceeds 2 bits
    /// - generation exceeds 10 bits
    #[inline]
    pub fn with_pool(pool_id: u8, slot_index: u32, generation: u16) -> Self {
        debug_assert!(pool_id <= MAX_POOL_ID, "pool_id exceeds 2 bits");
        debug_assert!(generation <= MAX_GENERATION, "generation exceeds 10 bits");
        Self {
            pool_id,
            slot_index,
            generation: generation & MAX_GENERATION,
        }
    }

    /// Convert from opaque Location.
    #[inline]
    pub fn from_location(loc: Location) -> Self {
        let raw = loc.as_raw();
        Self {
            pool_id: ((raw >> Self::POOL_SHIFT) & Self::POOL_MASK) as u8,
            slot_index: (raw & Self::SLOT_MASK) as u32,
            generation: ((raw >> Self::GEN_SHIFT) & Self::GEN_MASK) as u16,
        }
    }

    /// Convert to opaque Location for hashtable storage.
    #[inline]
    pub fn to_location(self) -> Location {
        let raw = (self.slot_index as u64)
            | ((self.generation as u64) << Self::GEN_SHIFT)
            | ((self.pool_id as u64) << Self::POOL_SHIFT);
        Location::new(raw)
    }

    /// Extract pool_id from an opaque Location without full parsing.
    ///
    /// This is useful for quickly determining which storage tier a location
    /// belongs to without the overhead of extracting all fields.
    #[inline]
    pub fn pool_id_from_location(loc: Location) -> u8 {
        ((loc.as_raw() >> Self::POOL_SHIFT) & Self::POOL_MASK) as u8
    }

    /// Get the pool ID (2 bits).
    #[allow(dead_code)]
    #[inline]
    pub fn pool_id(&self) -> u8 {
        self.pool_id
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
        let loc = SlotLocation::new(12345, 0x2BC);
        let opaque = loc.to_location();
        let recovered = SlotLocation::from_location(opaque);

        assert_eq!(recovered.pool_id(), 0); // default pool_id
        assert_eq!(recovered.slot_index(), 12345);
        assert_eq!(recovered.generation(), 0x2BC);
    }

    #[test]
    fn test_roundtrip_with_pool() {
        let loc = SlotLocation::with_pool(2, 12345, 0x2BC);
        let opaque = loc.to_location();
        let recovered = SlotLocation::from_location(opaque);

        assert_eq!(recovered.pool_id(), 2);
        assert_eq!(recovered.slot_index(), 12345);
        assert_eq!(recovered.generation(), 0x2BC);
    }

    #[test]
    fn test_max_values() {
        let loc = SlotLocation::with_pool(MAX_POOL_ID, u32::MAX, MAX_GENERATION);
        let opaque = loc.to_location();
        let recovered = SlotLocation::from_location(opaque);

        assert_eq!(recovered.pool_id(), MAX_POOL_ID);
        assert_eq!(recovered.slot_index(), u32::MAX);
        assert_eq!(recovered.generation(), MAX_GENERATION);
    }

    #[test]
    fn test_zero_values() {
        let loc = SlotLocation::new(0, 0);
        let opaque = loc.to_location();
        let recovered = SlotLocation::from_location(opaque);

        assert_eq!(recovered.pool_id(), 0);
        assert_eq!(recovered.slot_index(), 0);
        assert_eq!(recovered.generation(), 0);
    }

    #[test]
    fn test_pool_id_from_location() {
        for pool_id in 0..=MAX_POOL_ID {
            let loc = SlotLocation::with_pool(pool_id, 123, 456);
            let opaque = loc.to_location();
            assert_eq!(SlotLocation::pool_id_from_location(opaque), pool_id);
        }
    }

    #[test]
    fn test_fits_in_44_bits() {
        let loc = SlotLocation::with_pool(MAX_POOL_ID, u32::MAX, MAX_GENERATION);
        let opaque = loc.to_location();
        assert!(opaque.as_raw() <= Location::MAX_RAW);
    }

    #[test]
    fn test_bit_layout() {
        // Verify bit layout: pool_id at 42-43, generation at 32-41, slot_index at 0-31
        let loc = SlotLocation::with_pool(0b11, u32::MAX, 0x3FF);
        let raw = loc.to_location().as_raw();

        // Pool bits at position 42-43
        assert_eq!((raw >> 42) & 0b11, 0b11);
        // Generation bits at position 32-41
        assert_eq!((raw >> 32) & 0x3FF, 0x3FF);
        // Slot bits at position 0-31
        assert_eq!(raw & 0xFFFF_FFFF, u32::MAX as u64);
    }
}
