//! Item location within the cache.
//!
//! `ItemLocation` is a 44-bit packed value that identifies where an item is stored.
//! It fits within a 64-bit hashtable entry alongside a 12-bit tag and 8-bit frequency.

use std::fmt;

/// Packed item location - 44 bits total.
///
/// Bit layout (within the 44-bit field):
/// ```text
/// +--------+--------------+----------------+
/// | 43..42 |    41..20    |     19..0      |
/// |  pool  |    seg_id    |    offset/8    |
/// | 2 bits |   22 bits    |    20 bits     |
/// +--------+--------------+----------------+
/// ```
///
/// - pool_id:    2 bits  -> 4 pools max
/// - segment_id: 22 bits -> 4M segments per pool
/// - offset:     20 bits -> stored as offset/8, actual range 0..8MB
///
/// Total: 44 bits (fits in hashtable entry with 12-bit tag + 8-bit frequency)
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ItemLocation(u64);

impl ItemLocation {
    const POOL_SHIFT: u32 = 42;
    const POOL_MASK: u64 = 0b11 << 42;

    const SEG_SHIFT: u32 = 20;
    const SEG_MASK: u64 = 0x3F_FFFF << 20; // 22 bits

    const OFFSET_MASK: u64 = 0xF_FFFF; // 20 bits (lowest bits)

    /// Alignment for offset encoding (offset stored as offset/8).
    pub const OFFSET_ALIGN: u32 = 8;

    /// Maximum pool ID (2 bits = 0-3).
    pub const MAX_POOL_ID: u8 = 3;

    /// Maximum segment ID (22 bits).
    pub const MAX_SEGMENT_ID: u32 = (1 << 22) - 1;

    /// Maximum raw offset value (20 bits * 8 = ~8MB).
    pub const MAX_OFFSET: u32 = ((1u32 << 20) - 1) * Self::OFFSET_ALIGN;

    /// Sentinel value indicating a ghost entry (recently evicted).
    /// All 44 location bits set to 1.
    pub const GHOST: Self = Self(0xFFF_FFFF_FFFF);

    /// Create a new item location.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if:
    /// - pool_id >= 4
    /// - segment_id >= 2^22
    /// - offset is not 8-byte aligned
    /// - offset/8 >= 2^20
    #[inline]
    pub fn new(pool_id: u8, segment_id: u32, offset: u32) -> Self {
        debug_assert!(pool_id <= Self::MAX_POOL_ID, "pool_id must be 0-3");
        debug_assert!(
            segment_id <= Self::MAX_SEGMENT_ID,
            "segment_id must fit in 22 bits"
        );
        debug_assert!(
            offset.is_multiple_of(Self::OFFSET_ALIGN),
            "offset must be 8-byte aligned"
        );
        debug_assert!(offset <= Self::MAX_OFFSET, "offset/8 must fit in 20 bits");

        let encoded_offset = (offset / Self::OFFSET_ALIGN) as u64;
        Self(
            ((pool_id as u64) << Self::POOL_SHIFT)
                | ((segment_id as u64) << Self::SEG_SHIFT)
                | encoded_offset,
        )
    }

    /// Get the pool ID (0-3).
    #[inline]
    pub fn pool_id(&self) -> u8 {
        ((self.0 & Self::POOL_MASK) >> Self::POOL_SHIFT) as u8
    }

    /// Get the segment ID within the pool.
    #[inline]
    pub fn segment_id(&self) -> u32 {
        ((self.0 & Self::SEG_MASK) >> Self::SEG_SHIFT) as u32
    }

    /// Get the byte offset within the segment.
    #[inline]
    pub fn offset(&self) -> u32 {
        ((self.0 & Self::OFFSET_MASK) as u32) * Self::OFFSET_ALIGN
    }

    /// Check if this is a ghost entry (sentinel location).
    #[inline]
    pub fn is_ghost(&self) -> bool {
        *self == Self::GHOST
    }

    /// Get the raw 44-bit value for packing into hashtable entry.
    #[inline]
    pub fn as_raw(&self) -> u64 {
        self.0
    }

    /// Construct from raw 44-bit value.
    #[inline]
    pub fn from_raw(raw: u64) -> Self {
        Self(raw & 0xFFF_FFFF_FFFF) // Mask to 44 bits
    }
}

impl fmt::Debug for ItemLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_ghost() {
            write!(f, "ItemLocation::GHOST")
        } else {
            f.debug_struct("ItemLocation")
                .field("pool_id", &self.pool_id())
                .field("segment_id", &self.segment_id())
                .field("offset", &self.offset())
                .finish()
        }
    }
}

impl fmt::Display for ItemLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_ghost() {
            write!(f, "GHOST")
        } else {
            write!(
                f,
                "pool{}:seg{}:off{}",
                self.pool_id(),
                self.segment_id(),
                self.offset()
            )
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_accessors() {
        let loc = ItemLocation::new(2, 1000, 4096);
        assert_eq!(loc.pool_id(), 2);
        assert_eq!(loc.segment_id(), 1000);
        assert_eq!(loc.offset(), 4096);
        assert!(!loc.is_ghost());
    }

    #[test]
    fn test_ghost_sentinel() {
        assert!(ItemLocation::GHOST.is_ghost());
        assert_eq!(ItemLocation::GHOST.as_raw(), 0xFFF_FFFF_FFFF);
    }

    #[test]
    fn test_max_values() {
        let loc = ItemLocation::new(3, ItemLocation::MAX_SEGMENT_ID, ItemLocation::MAX_OFFSET);
        assert_eq!(loc.pool_id(), 3);
        assert_eq!(loc.segment_id(), ItemLocation::MAX_SEGMENT_ID);
        assert_eq!(loc.offset(), ItemLocation::MAX_OFFSET);
    }

    #[test]
    fn test_min_values() {
        let loc = ItemLocation::new(0, 0, 0);
        assert_eq!(loc.pool_id(), 0);
        assert_eq!(loc.segment_id(), 0);
        assert_eq!(loc.offset(), 0);
        assert!(!loc.is_ghost());
    }

    #[test]
    fn test_raw_roundtrip() {
        let loc = ItemLocation::new(1, 12345, 8192);
        let raw = loc.as_raw();
        let loc2 = ItemLocation::from_raw(raw);
        assert_eq!(loc, loc2);
    }

    #[test]
    fn test_offset_alignment() {
        // Offset must be 8-byte aligned
        for offset in (0..1024).step_by(8) {
            let loc = ItemLocation::new(0, 0, offset);
            assert_eq!(loc.offset(), offset);
        }
    }

    #[test]
    #[should_panic(expected = "pool_id must be 0-3")]
    #[cfg(debug_assertions)]
    fn test_invalid_pool_id() {
        ItemLocation::new(4, 0, 0);
    }

    #[test]
    #[should_panic(expected = "offset must be 8-byte aligned")]
    #[cfg(debug_assertions)]
    fn test_unaligned_offset() {
        ItemLocation::new(0, 0, 7);
    }

    #[test]
    fn test_bit_packing() {
        // Verify bit layout matches documentation
        let loc = ItemLocation::new(0b11, 0x3F_FFFF, 0x7FFFF8);

        // Pool bits should be at position 42-43
        assert_eq!((loc.as_raw() >> 42) & 0b11, 0b11);

        // Segment bits should be at position 20-41
        assert_eq!((loc.as_raw() >> 20) & 0x3F_FFFF, 0x3F_FFFF);

        // Offset/8 bits should be at position 0-19
        assert_eq!(loc.as_raw() & 0xF_FFFF, 0x7FFFF8 / 8);
    }

    #[test]
    fn test_display() {
        let loc = ItemLocation::new(1, 42, 128);
        assert_eq!(format!("{}", loc), "pool1:seg42:off128");
        assert_eq!(format!("{}", ItemLocation::GHOST), "GHOST");
    }

    #[test]
    fn test_debug() {
        let loc = ItemLocation::new(0, 1, 8);
        let debug_str = format!("{:?}", loc);
        assert!(debug_str.contains("pool_id"));
        assert!(debug_str.contains("segment_id"));
        assert!(debug_str.contains("offset"));

        let ghost_debug = format!("{:?}", ItemLocation::GHOST);
        assert!(ghost_debug.contains("GHOST"));
    }
}
