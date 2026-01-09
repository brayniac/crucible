//! Segment-specific location interpretation.
//!
//! This module provides `ItemLocation`, which interprets the opaque 44-bit
//! `Location` as a (pool_id, segment_id, offset) tuple for segment-based caches.

use crate::hashtable::KeyVerifier;
use crate::location::Location;
use crate::segment::SegmentKeyVerify;
use std::fmt;

/// Segment-specific location interpretation.
///
/// Interprets the 44-bit `Location` as:
/// - pool_id: 2 bits (0-3) - up to 4 storage pools
/// - segment_id: 22 bits - up to 4M segments per pool
/// - offset: 20 bits (stored as offset/8) - up to ~8MB per segment
///
/// ```text
/// +--------+--------------+----------------+
/// | 43..42 |    41..20    |     19..0      |
/// |  pool  |    seg_id    |    offset/8    |
/// | 2 bits |   22 bits    |    20 bits     |
/// +--------+--------------+----------------+
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ItemLocation(Location);

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

    /// Create a new item location from segment coordinates.
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
        let raw = ((pool_id as u64) << Self::POOL_SHIFT)
            | ((segment_id as u64) << Self::SEG_SHIFT)
            | encoded_offset;

        Self(Location::new(raw))
    }

    /// Create from an opaque `Location`.
    ///
    /// Use this when receiving a `Location` from the hashtable.
    #[inline]
    pub fn from_location(location: Location) -> Self {
        Self(location)
    }

    /// Convert to an opaque `Location`.
    ///
    /// Use this when passing to hashtable operations.
    #[inline]
    pub fn to_location(self) -> Location {
        self.0
    }

    /// Get the pool ID (0-3).
    #[inline]
    pub fn pool_id(&self) -> u8 {
        ((self.0.as_raw() & Self::POOL_MASK) >> Self::POOL_SHIFT) as u8
    }

    /// Get the segment ID within the pool.
    #[inline]
    pub fn segment_id(&self) -> u32 {
        ((self.0.as_raw() & Self::SEG_MASK) >> Self::SEG_SHIFT) as u32
    }

    /// Get the byte offset within the segment.
    #[inline]
    pub fn offset(&self) -> u32 {
        ((self.0.as_raw() & Self::OFFSET_MASK) as u32) * Self::OFFSET_ALIGN
    }

    /// Check if this location represents a ghost entry.
    #[inline]
    pub fn is_ghost(&self) -> bool {
        self.0.is_ghost()
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

impl From<Location> for ItemLocation {
    fn from(location: Location) -> Self {
        Self::from_location(location)
    }
}

impl From<ItemLocation> for Location {
    fn from(item_location: ItemLocation) -> Self {
        item_location.to_location()
    }
}

// ============================================================================
// Segment Verifiers
// ============================================================================

/// Single-pool segment verifier.
///
/// Used when there's only one segment pool (pool_id is ignored).
pub struct SinglePoolVerifier<'a, S> {
    segments: &'a [S],
}

impl<'a, S> SinglePoolVerifier<'a, S> {
    /// Create a new single-pool verifier.
    pub fn new(segments: &'a [S]) -> Self {
        Self { segments }
    }
}

impl<S: SegmentKeyVerify + Send + Sync> KeyVerifier for SinglePoolVerifier<'_, S> {
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let item_loc = ItemLocation::from_location(location);
        let segment_id = item_loc.segment_id() as usize;
        let offset = item_loc.offset();

        if let Some(segment) = self.segments.get(segment_id) {
            segment.verify_key_at_offset(offset, key, allow_deleted)
        } else {
            false
        }
    }
}

/// Multi-pool segment verifier with up to 4 pools.
pub struct MultiPoolVerifier<'a, S> {
    pools: [Option<&'a [S]>; 4],
}

impl<'a, S> MultiPoolVerifier<'a, S> {
    /// Create a new multi-pool verifier with no pools.
    pub fn new() -> Self {
        Self { pools: [None; 4] }
    }

    /// Add a pool at the specified ID.
    ///
    /// # Panics
    /// Panics if pool_id >= 4.
    pub fn with_pool(mut self, pool_id: u8, segments: &'a [S]) -> Self {
        assert!(pool_id < 4, "pool_id must be 0-3");
        self.pools[pool_id as usize] = Some(segments);
        self
    }
}

impl<S> Default for MultiPoolVerifier<'_, S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: SegmentKeyVerify + Send + Sync> KeyVerifier for MultiPoolVerifier<'_, S> {
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let item_loc = ItemLocation::from_location(location);
        let pool_id = item_loc.pool_id() as usize;
        let segment_id = item_loc.segment_id() as usize;
        let offset = item_loc.offset();

        if pool_id >= 4 {
            return false;
        }

        if let Some(segments) = self.pools[pool_id]
            && let Some(segment) = segments.get(segment_id)
        {
            return segment.verify_key_at_offset(offset, key, allow_deleted);
        }

        false
    }
}

/// Pool-based key verifier for direct pool access.
///
/// This verifier provides the fastest verification path by directly
/// accessing the pool without layer indirection. Use this when you
/// know which pool contains your items.
///
/// # Performance
///
/// This is significantly faster than `CacheKeyVerifier` which must:
/// 1. Look up layer by pool_id (linear scan)
/// 2. Match on layer type
/// 3. Get segment from layer
///
/// `PoolVerifier` skips all of that and goes directly to the pool.
pub struct PoolVerifier<'a, P> {
    pool: &'a P,
}

impl<'a, P> PoolVerifier<'a, P> {
    /// Create a new pool verifier.
    #[inline]
    pub fn new(pool: &'a P) -> Self {
        Self { pool }
    }
}

impl<P> KeyVerifier for PoolVerifier<'_, P>
where
    P: crate::pool::RamPool,
    P::Segment: SegmentKeyVerify,
{
    #[inline]
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let item_loc = ItemLocation::from_location(location);
        if let Some(segment) = self.pool.get(item_loc.segment_id()) {
            segment.verify_key_at_offset(item_loc.offset(), key, allow_deleted)
        } else {
            false
        }
    }
}

/// Two-pool key verifier for caches with exactly two pools.
///
/// This verifier handles the common case of a two-layer cache (like S3FIFO)
/// where items can be in either pool 0 or pool 1. It uses the pool_id from
/// the location to dispatch to the correct pool.
///
/// # Performance
///
/// This is faster than `MultiPoolVerifier` because it avoids the Option
/// array lookup and uses direct field access.
pub struct TwoPoolVerifier<'a, P0, P1> {
    pool0: &'a P0,
    pool1: &'a P1,
}

impl<'a, P0, P1> TwoPoolVerifier<'a, P0, P1> {
    /// Create a new two-pool verifier.
    #[inline]
    pub fn new(pool0: &'a P0, pool1: &'a P1) -> Self {
        Self { pool0, pool1 }
    }
}

impl<P0, P1> KeyVerifier for TwoPoolVerifier<'_, P0, P1>
where
    P0: crate::pool::RamPool,
    P0::Segment: SegmentKeyVerify,
    P1: crate::pool::RamPool,
    P1::Segment: SegmentKeyVerify,
{
    #[inline]
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let item_loc = ItemLocation::from_location(location);
        let pool_id = item_loc.pool_id();
        let segment_id = item_loc.segment_id();
        let offset = item_loc.offset();

        match pool_id {
            0 => {
                if let Some(segment) = self.pool0.get(segment_id) {
                    segment.verify_key_at_offset(offset, key, allow_deleted)
                } else {
                    false
                }
            }
            1 => {
                if let Some(segment) = self.pool1.get(segment_id) {
                    segment.verify_key_at_offset(offset, key, allow_deleted)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

/// Callback-based segment verifier for flexible integration.
pub struct FnVerifier<F> {
    verify_fn: F,
}

impl<F> FnVerifier<F> {
    /// Create a new function-based verifier.
    pub fn new(verify_fn: F) -> Self {
        Self { verify_fn }
    }
}

impl<F> KeyVerifier for FnVerifier<F>
where
    F: Fn(&[u8], Location, bool) -> bool + Send + Sync,
{
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        (self.verify_fn)(key, location, allow_deleted)
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
    fn test_location_roundtrip() {
        let loc = ItemLocation::new(1, 12345, 8192);
        let raw_location = loc.to_location();
        let loc2 = ItemLocation::from_location(raw_location);
        assert_eq!(loc.pool_id(), loc2.pool_id());
        assert_eq!(loc.segment_id(), loc2.segment_id());
        assert_eq!(loc.offset(), loc2.offset());
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
        assert_eq!((loc.to_location().as_raw() >> 42) & 0b11, 0b11);

        // Segment bits should be at position 20-41
        assert_eq!((loc.to_location().as_raw() >> 20) & 0x3F_FFFF, 0x3F_FFFF);

        // Offset/8 bits should be at position 0-19
        assert_eq!(loc.to_location().as_raw() & 0xF_FFFF, 0x7FFFF8 / 8);
    }

    #[test]
    fn test_display() {
        let loc = ItemLocation::new(1, 42, 128);
        assert_eq!(format!("{}", loc), "pool1:seg42:off128");
    }

    #[test]
    fn test_debug() {
        let loc = ItemLocation::new(0, 1, 8);
        let debug_str = format!("{:?}", loc);
        assert!(debug_str.contains("pool_id"));
        assert!(debug_str.contains("segment_id"));
        assert!(debug_str.contains("offset"));
    }

    #[test]
    fn test_from_into() {
        let item_loc = ItemLocation::new(1, 100, 256);
        let location: Location = item_loc.into();
        let back: ItemLocation = location.into();
        assert_eq!(item_loc.pool_id(), back.pool_id());
        assert_eq!(item_loc.segment_id(), back.segment_id());
        assert_eq!(item_loc.offset(), back.offset());
    }

    // Mock segment for verifier tests
    struct MockSegment {
        keys: Vec<(u32, Vec<u8>, bool)>, // (offset, key, deleted)
    }

    impl SegmentKeyVerify for MockSegment {
        fn verify_key_at_offset(&self, offset: u32, key: &[u8], allow_deleted: bool) -> bool {
            self.keys
                .iter()
                .any(|(off, k, deleted)| *off == offset && k == key && (allow_deleted || !deleted))
        }

        fn verify_key_with_header(
            &self,
            offset: u32,
            key: &[u8],
            allow_deleted: bool,
        ) -> Option<(u8, u8, u32)> {
            if self.verify_key_at_offset(offset, key, allow_deleted) {
                // Return mock header values: (key_len, optional_len, value_len)
                Some((key.len() as u8, 0, 0))
            } else {
                None
            }
        }

        fn verify_key_unexpired(
            &self,
            offset: u32,
            key: &[u8],
            _now: u32,
        ) -> Option<(u8, u8, u32)> {
            // Mock doesn't track TTL, just verify key
            self.verify_key_with_header(offset, key, false)
        }
    }

    #[test]
    fn test_single_pool_verifier() {
        let segments = vec![
            MockSegment {
                keys: vec![(0, b"key0".to_vec(), false)],
            },
            MockSegment {
                keys: vec![(8, b"key1".to_vec(), false)],
            },
        ];
        let verifier = SinglePoolVerifier::new(&segments);

        let loc0 = ItemLocation::new(0, 0, 0);
        let loc1 = ItemLocation::new(0, 1, 8);

        assert!(verifier.verify(b"key0", loc0.to_location(), false));
        assert!(verifier.verify(b"key1", loc1.to_location(), false));
        assert!(!verifier.verify(b"wrong", loc0.to_location(), false));
        assert!(!verifier.verify(b"key0", loc1.to_location(), false));
    }

    #[test]
    fn test_multi_pool_verifier() {
        let pool0 = vec![MockSegment {
            keys: vec![(0, b"pool0_key".to_vec(), false)],
        }];
        let pool2 = vec![
            MockSegment {
                keys: vec![(0, b"pool2_seg0".to_vec(), false)],
            },
            MockSegment {
                keys: vec![(8, b"pool2_seg1".to_vec(), false)],
            },
        ];

        let verifier = MultiPoolVerifier::new()
            .with_pool(0, &pool0)
            .with_pool(2, &pool2);

        let loc0 = ItemLocation::new(0, 0, 0);
        let loc2_0 = ItemLocation::new(2, 0, 0);
        let loc2_1 = ItemLocation::new(2, 1, 8);
        let loc1 = ItemLocation::new(1, 0, 0); // pool 1 not set

        assert!(verifier.verify(b"pool0_key", loc0.to_location(), false));
        assert!(verifier.verify(b"pool2_seg0", loc2_0.to_location(), false));
        assert!(verifier.verify(b"pool2_seg1", loc2_1.to_location(), false));
        assert!(!verifier.verify(b"any", loc1.to_location(), false)); // pool 1 not set
    }

    #[test]
    fn test_fn_verifier() {
        let verifier = FnVerifier::new(|key: &[u8], location: Location, _allow_deleted| {
            let item_loc = ItemLocation::from_location(location);
            key == b"magic" && item_loc.segment_id() == 42
        });

        let loc_match = ItemLocation::new(0, 42, 0);
        let loc_nomatch = ItemLocation::new(0, 99, 0);

        assert!(verifier.verify(b"magic", loc_match.to_location(), false));
        assert!(!verifier.verify(b"magic", loc_nomatch.to_location(), false));
        assert!(!verifier.verify(b"other", loc_match.to_location(), false));
    }

    #[test]
    fn test_deleted_handling() {
        let segments = vec![MockSegment {
            keys: vec![(0, b"deleted_key".to_vec(), true)],
        }];
        let verifier = SinglePoolVerifier::new(&segments);

        let loc = ItemLocation::new(0, 0, 0);

        // allow_deleted=false should not match
        assert!(!verifier.verify(b"deleted_key", loc.to_location(), false));

        // allow_deleted=true should match
        assert!(verifier.verify(b"deleted_key", loc.to_location(), true));
    }
}
