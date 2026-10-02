//! Slab location encoding for the 44-bit location field.
//!
//! Encodes: pool_id (2 bits) + class_id (6 bits) + slab_id (16 bits) + slot_index (20 bits)
//!
//! Capacity:
//! - 4 storage pools (RAM + up to 3 disk tiers)
//! - 64 slab classes
//! - 65,536 slabs per class (`MAX_SLABS_PER_CLASS` in `class.rs`)
//! - 1,048,576 slots per slab, enough for 64-byte slots in a 64MB slab

use cache_core::Location;

/// Maximum pool ID (2 bits = 0-3).
pub const MAX_POOL_ID: u8 = 3;

/// Maximum class ID (6 bits = 0-63).
pub const MAX_CLASS_ID: u8 = 63;

/// Bits of the location word holding the slot index.
pub const SLOT_INDEX_BITS: u32 = 20;

/// Bits of the location word holding the slab ID.
pub const SLAB_ID_BITS: u32 = 16;

const _: () = assert!(SLOT_INDEX_BITS + SLAB_ID_BITS == 36);

/// Maximum slab ID (16 bits = 0-65535).
pub const MAX_SLAB_ID: u32 = (1 << SLAB_ID_BITS) - 1;

/// Maximum slot index (20 bits = 0-1048575).
pub const MAX_SLOT_INDEX: u32 = (1 << SLOT_INDEX_BITS) - 1;

/// Slab location encoding.
///
/// ```text
/// 44-bit layout:
/// +--------+--------+----------+-----------+
/// | 43..42 | 41..36 |  35..20  |   19..0   |
/// |pool_id |class_id| slab_id  |slot_index |
/// | 2 bits | 6 bits | 16 bits  |  20 bits  |
/// +--------+--------+----------+-----------+
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlabLocation {
    pool_id: u8,
    class_id: u8,
    slab_id: u32,
    slot_index: u32,
}

impl SlabLocation {
    /// Create a new slab location with default pool_id (0 = RAM).
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`Self::with_pool`].
    #[inline]
    pub fn new(class_id: u8, slab_id: u32, slot_index: u32) -> Self {
        Self::with_pool(0, class_id, slab_id, slot_index)
    }

    /// Create a new slab location with explicit pool_id.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - pool_id > 3 (exceeds 2 bits)
    /// - class_id > 63 (exceeds 6 bits) - reduce slab_size or increase growth_factor
    /// - slab_id exceeds 16 bits or slot_index exceeds 20 bits
    #[inline]
    pub fn with_pool(pool_id: u8, class_id: u8, slab_id: u32, slot_index: u32) -> Self {
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
        assert!(slab_id <= MAX_SLAB_ID, "slab_id {slab_id} exceeds 16 bits");
        assert!(
            slot_index <= MAX_SLOT_INDEX,
            "slot_index {slot_index} exceeds 20 bits"
        );
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

    /// Get the slab ID (16 bits).
    #[inline(always)]
    pub fn slab_id(&self) -> u32 {
        self.slab_id
    }

    /// Get the slot index (20 bits).
    #[inline(always)]
    pub fn slot_index(&self) -> u32 {
        self.slot_index
    }

    /// Convert to the opaque Location type for hashtable storage.
    #[inline]
    pub fn to_location(self) -> Location {
        let raw = ((self.pool_id as u64) << 42)
            | ((self.class_id as u64) << 36)
            | ((self.slab_id as u64) << SLOT_INDEX_BITS)
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
            slab_id: ((raw >> SLOT_INDEX_BITS) as u32) & MAX_SLAB_ID,
            slot_index: (raw as u32) & MAX_SLOT_INDEX,
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
    pub fn unpack(&self) -> (u8, u32, u32) {
        (self.class_id, self.slab_id, self.slot_index)
    }

    /// Unpack all fields including pool_id.
    ///
    /// Returns `(pool_id, class_id, slab_id, slot_index)`.
    #[inline]
    pub fn unpack_with_pool(&self) -> (u8, u8, u32, u32) {
        (self.pool_id, self.class_id, self.slab_id, self.slot_index)
    }
}

#[cfg(kani)]
mod verification {
    use super::*;

    /// A slab location round-trips every field through the opaque 44-bit word.
    ///
    /// `pool_id` sits at bits 43..42, the same fixed position `ItemLocation`
    /// uses, so a tiered verifier can read it before knowing which backend
    /// owns the location.
    #[kani::proof]
    fn slab_location_roundtrip() {
        let pool_id: u8 = kani::any();
        kani::assume(pool_id <= 3);
        let class_id: u8 = kani::any();
        kani::assume(class_id <= 63);
        let slab_id: u32 = kani::any();
        kani::assume(slab_id <= MAX_SLAB_ID);
        let slot_index: u32 = kani::any();
        kani::assume(slot_index <= MAX_SLOT_INDEX);

        let loc = SlabLocation::with_pool(pool_id, class_id, slab_id, slot_index);
        let back = SlabLocation::from_location(loc.to_location());
        assert_eq!(
            back.unpack_with_pool(),
            (pool_id, class_id, slab_id, slot_index)
        );
        assert_eq!(
            SlabLocation::pool_id_from_location(loc.to_location()),
            pool_id
        );
    }

    /// Distinct coordinates give distinct location words.
    ///
    /// Two live items sharing a location word would make the hashtable
    /// indistinguishable between them.
    #[kani::proof]
    fn slab_location_injective() {
        let (p_a, c_a, s_a, i_a): (u8, u8, u32, u32) =
            (kani::any(), kani::any(), kani::any(), kani::any());
        let (p_b, c_b, s_b, i_b): (u8, u8, u32, u32) =
            (kani::any(), kani::any(), kani::any(), kani::any());
        kani::assume(p_a <= 3 && p_b <= 3);
        kani::assume(c_a <= 63 && c_b <= 63);
        kani::assume(s_a <= MAX_SLAB_ID && s_b <= MAX_SLAB_ID);
        kani::assume(i_a <= MAX_SLOT_INDEX && i_b <= MAX_SLOT_INDEX);

        kani::assume(
            SlabLocation::with_pool(p_a, c_a, s_a, i_a).to_location()
                == SlabLocation::with_pool(p_b, c_b, s_b, i_b).to_location(),
        );
        assert_eq!((p_a, c_a, s_a, i_a), (p_b, c_b, s_b, i_b));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let loc = SlabLocation::new(42, 54321, 789);
        let location = loc.to_location();
        let decoded = SlabLocation::from_location(location);
        assert_eq!(decoded.pool_id(), 0); // default pool_id
        assert_eq!(decoded.class_id(), 42);
        assert_eq!(decoded.slab_id(), 54321);
        assert_eq!(decoded.slot_index(), 789);
    }

    #[test]
    fn test_roundtrip_with_pool() {
        let loc = SlabLocation::with_pool(2, 42, 54321, 789);
        let location = loc.to_location();
        let decoded = SlabLocation::from_location(location);
        assert_eq!(decoded.pool_id(), 2);
        assert_eq!(decoded.class_id(), 42);
        assert_eq!(decoded.slab_id(), 54321);
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
        // Verify bit layout: pool_id at 42-43, class_id at 36-41, slab_id at 20-35, slot_index at 0-19
        let loc = SlabLocation::with_pool(0b11, 0b11_1111, 0xFFFF, 0xF_FFFF);
        let raw = loc.to_location().as_raw();

        // Pool bits at position 42-43
        assert_eq!((raw >> 42) & 0b11, 0b11);
        // Class bits at position 36-41
        assert_eq!((raw >> 36) & 0x3F, 0b11_1111);
        // Slab bits at position 20-35
        assert_eq!((raw >> 20) & 0xFFFF, 0xFFFF);
        // Slot bits at position 0-19
        assert_eq!(raw & 0xF_FFFF, 0xF_FFFF);
    }
}
