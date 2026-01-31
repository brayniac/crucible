//! Location encoding for heap cache slots with value type support.
//!
//! Encodes a value type, generation counter, and slot index into a 44-bit Location.
//!
//! # Layout
//!
//! ```text
//! | type (3 bits) | generation (9 bits) | slot_index (32 bits) |
//! |  bits 43-41   |     bits 40-32      |      bits 31-0       |
//! ```
//!
//! - type: Value type (0=String, 1=Hash, 2=List, 3=Set, 4-7=Reserved)
//! - generation: Provides ABA protection when slots are reused
//! - slot_index: Index into the type-specific storage array
//!
//! Note: The original pool_id encoding is replaced by value type. Pool selection
//! is now implicit based on the value type (all complex types use RAM only initially).

use cache_core::Location;

/// Value types stored in the cache.
///
/// Type IDs are encoded in the top 3 bits of the location.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ValueType {
    /// String value (default, lock-free storage)
    String = 0,
    /// Hash (field -> value mapping, RwLock per slot)
    Hash = 1,
    /// List (ordered collection, Mutex per slot)
    List = 2,
    /// Set (unique members, RwLock per slot)
    Set = 3,
    // 4-7 reserved for future types
}

impl ValueType {
    /// Convert from raw type ID.
    ///
    /// Returns None if the type ID is reserved or invalid.
    #[inline]
    pub fn from_raw(id: u8) -> Option<Self> {
        match id {
            0 => Some(ValueType::String),
            1 => Some(ValueType::Hash),
            2 => Some(ValueType::List),
            3 => Some(ValueType::Set),
            _ => None,
        }
    }

    /// Get the raw type ID.
    #[inline]
    pub fn as_raw(self) -> u8 {
        self as u8
    }

    /// Get Redis TYPE command response string.
    #[inline]
    pub fn as_redis_type_str(self) -> &'static str {
        match self {
            ValueType::String => "string",
            ValueType::Hash => "hash",
            ValueType::List => "list",
            ValueType::Set => "set",
        }
    }
}

/// Maximum generation value (9 bits = 0-511).
pub const MAX_GENERATION: u16 = 0x1FF;

/// Maximum type ID (3 bits = 0-7).
#[allow(dead_code)]
pub const MAX_TYPE_ID: u8 = 7;

/// Location interpretation for heap cache slots with value type encoding.
///
/// Layout: 3-bit type + 9-bit generation + 32-bit slot index = 44 bits
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TypedLocation {
    value_type: ValueType,
    slot_index: u32,
    generation: u16,
}

impl TypedLocation {
    const SLOT_BITS: u64 = 32;
    const SLOT_MASK: u64 = (1 << Self::SLOT_BITS) - 1;
    const GEN_SHIFT: u64 = Self::SLOT_BITS;
    const GEN_MASK: u64 = 0x1FF; // 9 bits
    const TYPE_SHIFT: u64 = 41;
    const TYPE_MASK: u64 = 0b111; // 3 bits

    /// Create a new typed location for a String value (type = 0).
    #[inline]
    pub fn new(slot_index: u32, generation: u16) -> Self {
        Self::with_type(ValueType::String, slot_index, generation)
    }

    /// Create a new typed location with explicit value type.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if generation exceeds 9 bits.
    #[inline]
    pub fn with_type(value_type: ValueType, slot_index: u32, generation: u16) -> Self {
        debug_assert!(generation <= MAX_GENERATION, "generation exceeds 9 bits");
        Self {
            value_type,
            slot_index,
            generation: generation & MAX_GENERATION,
        }
    }

    /// Convert from opaque Location.
    #[inline]
    pub fn from_location(loc: Location) -> Self {
        let raw = loc.as_raw();
        let type_id = ((raw >> Self::TYPE_SHIFT) & Self::TYPE_MASK) as u8;
        Self {
            value_type: ValueType::from_raw(type_id).unwrap_or(ValueType::String),
            slot_index: (raw & Self::SLOT_MASK) as u32,
            generation: ((raw >> Self::GEN_SHIFT) & Self::GEN_MASK) as u16,
        }
    }

    /// Convert to opaque Location for hashtable storage.
    #[inline]
    pub fn to_location(self) -> Location {
        let raw = (self.slot_index as u64)
            | ((self.generation as u64) << Self::GEN_SHIFT)
            | ((self.value_type.as_raw() as u64) << Self::TYPE_SHIFT);
        Location::new(raw)
    }

    /// Extract value type from an opaque Location without full parsing.
    ///
    /// This is useful for quickly determining which storage type a location
    /// belongs to without the overhead of extracting all fields.
    #[inline]
    pub fn type_from_location(loc: Location) -> ValueType {
        let type_id = ((loc.as_raw() >> Self::TYPE_SHIFT) & Self::TYPE_MASK) as u8;
        ValueType::from_raw(type_id).unwrap_or(ValueType::String)
    }

    /// Extract type ID as raw u8 from an opaque Location without full parsing.
    #[inline]
    #[allow(dead_code)]
    pub fn type_id_from_location(loc: Location) -> u8 {
        ((loc.as_raw() >> Self::TYPE_SHIFT) & Self::TYPE_MASK) as u8
    }

    /// Get the value type.
    #[inline]
    pub fn value_type(&self) -> ValueType {
        self.value_type
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

// Legacy compatibility: SlotLocation is an alias for TypedLocation
/// Legacy alias for TypedLocation.
///
/// Provides backwards compatibility with code using the old SlotLocation name.
pub type SlotLocation = TypedLocation;

impl TypedLocation {
    /// Legacy: Create a new slot location with pool_id (maps to type for compatibility).
    ///
    /// This method exists for backwards compatibility. Pool ID 0 maps to String type.
    #[inline]
    pub fn with_pool(pool_id: u8, slot_index: u32, generation: u16) -> Self {
        // For backwards compatibility, pool_id 0 = RAM = String type
        // pool_id 2 = disk tier, but disk tier is not supported for complex types
        let value_type = if pool_id == 0 {
            ValueType::String
        } else {
            // For disk tier (pool_id 2), still use String type
            ValueType::String
        };
        Self::with_type(value_type, slot_index, generation)
    }

    /// Legacy: Extract pool_id from location (always returns 0 for RAM).
    ///
    /// This method exists for backwards compatibility with the tiered verifier.
    #[inline]
    pub fn pool_id_from_location(_loc: Location) -> u8 {
        // All complex types are RAM-only, so always return 0
        // For String type, the existing disk tier logic will handle it
        0
    }

    /// Legacy: Get the pool ID (always 0 for RAM storage).
    #[inline]
    #[allow(dead_code)]
    pub fn pool_id(&self) -> u8 {
        0
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_value_type_from_raw() {
        assert_eq!(ValueType::from_raw(0), Some(ValueType::String));
        assert_eq!(ValueType::from_raw(1), Some(ValueType::Hash));
        assert_eq!(ValueType::from_raw(2), Some(ValueType::List));
        assert_eq!(ValueType::from_raw(3), Some(ValueType::Set));
        assert_eq!(ValueType::from_raw(4), None);
        assert_eq!(ValueType::from_raw(7), None);
    }

    #[test]
    fn test_value_type_redis_str() {
        assert_eq!(ValueType::String.as_redis_type_str(), "string");
        assert_eq!(ValueType::Hash.as_redis_type_str(), "hash");
        assert_eq!(ValueType::List.as_redis_type_str(), "list");
        assert_eq!(ValueType::Set.as_redis_type_str(), "set");
    }

    #[test]
    fn test_roundtrip_string() {
        let loc = TypedLocation::new(12345, 0x1BC);
        let opaque = loc.to_location();
        let recovered = TypedLocation::from_location(opaque);

        assert_eq!(recovered.value_type(), ValueType::String);
        assert_eq!(recovered.slot_index(), 12345);
        assert_eq!(recovered.generation(), 0x1BC);
    }

    #[test]
    fn test_roundtrip_with_type() {
        for vtype in [
            ValueType::String,
            ValueType::Hash,
            ValueType::List,
            ValueType::Set,
        ] {
            let loc = TypedLocation::with_type(vtype, 12345, 0x1BC);
            let opaque = loc.to_location();
            let recovered = TypedLocation::from_location(opaque);

            assert_eq!(recovered.value_type(), vtype);
            assert_eq!(recovered.slot_index(), 12345);
            assert_eq!(recovered.generation(), 0x1BC);
        }
    }

    #[test]
    fn test_max_values() {
        let loc = TypedLocation::with_type(ValueType::Set, u32::MAX, MAX_GENERATION);
        let opaque = loc.to_location();
        let recovered = TypedLocation::from_location(opaque);

        assert_eq!(recovered.value_type(), ValueType::Set);
        assert_eq!(recovered.slot_index(), u32::MAX);
        assert_eq!(recovered.generation(), MAX_GENERATION);
    }

    #[test]
    fn test_zero_values() {
        let loc = TypedLocation::new(0, 0);
        let opaque = loc.to_location();
        let recovered = TypedLocation::from_location(opaque);

        assert_eq!(recovered.value_type(), ValueType::String);
        assert_eq!(recovered.slot_index(), 0);
        assert_eq!(recovered.generation(), 0);
    }

    #[test]
    fn test_type_from_location() {
        for vtype in [
            ValueType::String,
            ValueType::Hash,
            ValueType::List,
            ValueType::Set,
        ] {
            let loc = TypedLocation::with_type(vtype, 123, 456 & MAX_GENERATION);
            let opaque = loc.to_location();
            assert_eq!(TypedLocation::type_from_location(opaque), vtype);
        }
    }

    #[test]
    fn test_fits_in_44_bits() {
        let loc = TypedLocation::with_type(ValueType::Set, u32::MAX, MAX_GENERATION);
        let opaque = loc.to_location();
        assert!(opaque.as_raw() <= Location::MAX_RAW);
    }

    #[test]
    fn test_bit_layout() {
        // Verify bit layout: type at 41-43, generation at 32-40, slot_index at 0-31
        let loc = TypedLocation::with_type(ValueType::Set, u32::MAX, MAX_GENERATION);
        let raw = loc.to_location().as_raw();

        // Type bits at position 41-43 (Set = 3 = 0b011)
        assert_eq!((raw >> 41) & 0b111, 3);
        // Generation bits at position 32-40 (9 bits, MAX_GENERATION = 0x1FF)
        assert_eq!((raw >> 32) & 0x1FF, MAX_GENERATION as u64);
        // Slot bits at position 0-31
        assert_eq!(raw & 0xFFFF_FFFF, u32::MAX as u64);
    }

    #[test]
    fn test_legacy_slot_location_alias() {
        // Ensure SlotLocation works as an alias
        let loc = SlotLocation::new(100, 50);
        assert_eq!(loc.slot_index(), 100);
        assert_eq!(loc.generation(), 50);
        assert_eq!(loc.value_type(), ValueType::String);
    }

    #[test]
    fn test_legacy_pool_id_compatibility() {
        // Legacy pool_id methods should work for backwards compatibility
        let loc = SlotLocation::with_pool(0, 100, 50);
        assert_eq!(loc.pool_id(), 0);
        assert_eq!(loc.value_type(), ValueType::String);

        // pool_id_from_location should return 0 (RAM)
        let opaque = loc.to_location();
        assert_eq!(SlotLocation::pool_id_from_location(opaque), 0);
    }

    #[test]
    fn test_generation_max_is_9_bits() {
        // MAX_GENERATION should be 511 (9 bits)
        assert_eq!(MAX_GENERATION, 0x1FF);
        assert_eq!(MAX_GENERATION, 511);
    }
}
