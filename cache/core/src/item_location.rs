//! Segment-specific location interpretation.
//!
//! This module provides `ItemLocation`, which interprets the opaque 44-bit
//! `Location` as a `(pool_id, segment_id, incarnation, offset)` tuple for
//! segment-based caches.
//!
//! Only `pool_id` sits at a fixed position. The rest of the split is per-pool
//! and described by that pool's [`LocationLayout`], so the decoding accessors
//! take one.

use crate::hashtable::KeyVerifier;
use crate::location::Location;
use crate::location_layout::LocationLayout;
use crate::segment::SegmentKeyVerify;
use std::fmt;

/// Segment-specific location interpretation.
///
/// Interprets the 44-bit `Location` as `(pool_id, segment_id, incarnation,
/// offset)`. Only `pool_id` sits at a fixed position; the rest of the split is
/// per-pool and described by that pool's [`LocationLayout`], which is why the
/// accessors take one.
///
/// ```text
/// +--------+------------+-------------+--------------+
/// | 43..42 |    ...     |     ...     |     ...      |
/// |  pool  |  seg_id    | incarnation | offset/align |
/// | 2 bits |  variable  |   6 bits    |   variable   |
/// +--------+------------+-------------+--------------+
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ItemLocation(Location);

impl ItemLocation {
    /// Create a new item location from segment coordinates.
    ///
    /// `incarnation` must be the tag the segment carries *at the moment the
    /// item is written into it* -- read it from the same segment reference the
    /// append used, not from a later lookup.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if `pool_id`, `segment_id` or `offset` is outside
    /// what `layout` can represent. See [`LocationLayout::pack`].
    #[inline]
    pub fn new(
        layout: &LocationLayout,
        pool_id: u8,
        segment_id: u32,
        incarnation: u8,
        offset: u32,
    ) -> Self {
        Self(Location::new(layout.pack(
            pool_id,
            segment_id,
            incarnation,
            offset,
        )))
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
    ///
    /// Layout-independent: this is what a decoder reads first, to find out
    /// which layout decodes the rest.
    #[inline]
    pub fn pool_id(&self) -> u8 {
        LocationLayout::pool_id(self.0.as_raw())
    }

    /// Get the segment ID within the pool.
    #[inline]
    pub fn segment_id(&self, layout: &LocationLayout) -> u32 {
        layout.segment_id(self.0.as_raw())
    }

    /// Get the incarnation tag this location was issued under.
    #[inline]
    pub fn incarnation(&self, layout: &LocationLayout) -> u8 {
        layout.incarnation(self.0.as_raw())
    }

    /// Get the byte offset within the segment.
    #[inline]
    pub fn offset(&self, layout: &LocationLayout) -> u32 {
        layout.offset(self.0.as_raw())
    }

    /// Unpack all fields in a single pass.
    ///
    /// Returns `(pool_id, segment_id, incarnation, offset)`. Cheaper than the
    /// individual accessors when more than one field is needed, since the raw
    /// bits are loaded once.
    #[inline]
    pub fn unpack(&self, layout: &LocationLayout) -> (u8, u32, u8, u32) {
        let raw = self.0.as_raw();
        (
            LocationLayout::pool_id(raw),
            layout.segment_id(raw),
            layout.incarnation(raw),
            layout.offset(raw),
        )
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
            // Decoding requires the owning pool's layout, which a Debug impl
            // does not have. pool_id is the one field at a fixed position.
            f.debug_struct("ItemLocation")
                .field("pool_id", &self.pool_id())
                .field("raw", &format_args!("0x{:011X}", self.0.as_raw()))
                .finish()
        }
    }
}

impl fmt::Display for ItemLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_ghost() {
            write!(f, "GHOST")
        } else {
            write!(f, "pool{}:0x{:011X}", self.pool_id(), self.0.as_raw())
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
/// Used when there's only one segment pool (pool_id is ignored). The layout is
/// carried alongside the segments because decoding a location needs the owning
/// pool's bit split.
pub struct SinglePoolVerifier<'a, S> {
    layout: LocationLayout,
    segments: &'a [S],
}

impl<'a, S> SinglePoolVerifier<'a, S> {
    /// Create a new single-pool verifier.
    ///
    /// `layout` must be the layout of the pool that owns `segments`.
    pub fn new(layout: LocationLayout, segments: &'a [S]) -> Self {
        Self { layout, segments }
    }
}

impl<S: SegmentKeyVerify + Send + Sync> KeyVerifier for SinglePoolVerifier<'_, S> {
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let item_loc = ItemLocation::from_location(location);
        let (_, segment_id, incarnation, offset) = item_loc.unpack(&self.layout);

        if let Some(segment) = self.segments.get(segment_id as usize) {
            // A location naming a previous incarnation of this segment is not ours
            // to resolve: the segment was drained and refilled, and this offset now
            // holds a different item. Checked before touching any item bytes.
            if segment.incarnation() != incarnation {
                return false;
            }
            segment.verify_key_at_offset(offset, key, allow_deleted)
        } else {
            false
        }
    }
}

/// Multi-pool segment verifier with up to 4 pools.
///
/// Each pool carries its own [`LocationLayout`]: pools with different segment
/// sizes or alignment factors split the location bits differently, so the
/// pool_id is read first and its layout decodes the rest.
pub struct MultiPoolVerifier<'a, S> {
    pools: [Option<(LocationLayout, &'a [S])>; 4],
}

impl<'a, S> MultiPoolVerifier<'a, S> {
    /// Create a new multi-pool verifier with no pools.
    pub fn new() -> Self {
        Self { pools: [None; 4] }
    }

    /// Add a pool at the specified ID, with that pool's layout.
    ///
    /// # Panics
    /// Panics if pool_id >= 4.
    pub fn with_pool(mut self, pool_id: u8, layout: LocationLayout, segments: &'a [S]) -> Self {
        assert!(pool_id < 4, "pool_id must be 0-3");
        self.pools[pool_id as usize] = Some((layout, segments));
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

        // pool_id is 2 bits, so this cannot be out of range -- but the index is
        // kept honest rather than unchecked.
        let Some((layout, segments)) = self.pools.get(pool_id).copied().flatten() else {
            return false;
        };

        let (_, segment_id, incarnation, offset) = item_loc.unpack(&layout);

        if let Some(segment) = segments.get(segment_id as usize) {
            // A location naming a previous incarnation of this segment is not ours
            // to resolve: the segment was drained and refilled, and this offset now
            // holds a different item. Checked before touching any item bytes.
            if segment.incarnation() != incarnation {
                return false;
            }
            return segment.verify_key_at_offset(offset, key, allow_deleted);
        }

        false
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

    fn mem_layout() -> LocationLayout {
        LocationLayout::new(1024 * 1024, 8).unwrap()
    }

    #[test]
    fn test_new_and_accessors() {
        let l = mem_layout();
        let loc = ItemLocation::new(&l, 2, 1000, 5, 4096);
        assert_eq!(loc.pool_id(), 2);
        assert_eq!(loc.segment_id(&l), 1000);
        assert_eq!(loc.incarnation(&l), 5);
        assert_eq!(loc.offset(&l), 4096);
        assert!(!loc.is_ghost());
    }

    #[test]
    fn test_unpack_matches_individual_accessors() {
        let l = mem_layout();
        let loc = ItemLocation::new(&l, 1, 12345, 63, 8192);
        assert_eq!(
            loc.unpack(&l),
            (
                loc.pool_id(),
                loc.segment_id(&l),
                loc.incarnation(&l),
                loc.offset(&l)
            )
        );
    }

    #[test]
    fn test_incarnation_survives_a_location_roundtrip() {
        let l = mem_layout();
        let loc = ItemLocation::new(&l, 1, 12345, 42, 8192);
        let back = ItemLocation::from_location(loc.to_location());
        assert_eq!(back.incarnation(&l), 42);
        assert_eq!(back.segment_id(&l), 12345);
        assert_eq!(back.offset(&l), 8192);
    }

    #[test]
    fn test_pool_id_decodes_without_the_layout() {
        // pool_id must be readable before the layout is known -- that is what
        // lets pools with different segment sizes share one location space.
        let mem = LocationLayout::new(1024 * 1024, 8).unwrap();
        let disk = LocationLayout::new(8 * 1024 * 1024, 512).unwrap();
        assert_eq!(ItemLocation::new(&mem, 1, 5, 0, 0).pool_id(), 1);
        assert_eq!(ItemLocation::new(&disk, 3, 5, 0, 0).pool_id(), 3);
    }

    #[test]
    fn test_min_values() {
        let l = mem_layout();
        let loc = ItemLocation::new(&l, 0, 0, 0, 0);
        assert_eq!(loc.pool_id(), 0);
        assert_eq!(loc.segment_id(&l), 0);
        assert_eq!(loc.incarnation(&l), 0);
        assert_eq!(loc.offset(&l), 0);
        assert!(!loc.is_ghost());
    }

    #[test]
    fn test_display() {
        let l = mem_layout();
        let loc = ItemLocation::new(&l, 1, 42, 0, 128);
        let raw = loc.to_location().as_raw();
        assert_eq!(format!("{}", loc), format!("pool1:0x{raw:011X}"));
    }

    #[test]
    fn test_debug() {
        let l = mem_layout();
        let loc = ItemLocation::new(&l, 0, 1, 0, 8);
        let debug_str = format!("{:?}", loc);
        assert!(debug_str.contains("pool_id"));
        assert!(debug_str.contains("raw"));
    }

    #[test]
    fn test_from_into() {
        let l = mem_layout();
        let item_loc = ItemLocation::new(&l, 1, 100, 7, 256);
        let location: Location = item_loc.into();
        let back: ItemLocation = location.into();
        assert_eq!(item_loc.unpack(&l), back.unpack(&l));
    }

    // Mock segment for verifier tests
    struct MockSegment {
        incarnation: u8,
        keys: Vec<(u32, Vec<u8>, bool)>, // (offset, key, deleted)
    }

    impl MockSegment {
        /// A segment on its first incarnation, holding one key at one offset.
        fn new(offset: u32, key: &[u8]) -> Self {
            Self {
                incarnation: 0,
                keys: vec![(offset, key.to_vec(), false)],
            }
        }
    }

    impl SegmentKeyVerify for MockSegment {
        fn incarnation(&self) -> u8 {
            self.incarnation
        }

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
        let l = mem_layout();
        let segments = vec![MockSegment::new(0, b"key0"), MockSegment::new(8, b"key1")];
        let verifier = SinglePoolVerifier::new(l, &segments);

        let loc0 = ItemLocation::new(&l, 0, 0, 0, 0);
        let loc1 = ItemLocation::new(&l, 0, 1, 0, 8);

        assert!(verifier.verify(b"key0", loc0.to_location(), false));
        assert!(verifier.verify(b"key1", loc1.to_location(), false));
        assert!(!verifier.verify(b"wrong", loc0.to_location(), false));
        assert!(!verifier.verify(b"key0", loc1.to_location(), false));
    }

    #[test]
    fn test_multi_pool_verifier_decodes_each_pool_with_its_own_layout() {
        // The two pools have different segment sizes, so their bit splits
        // differ: decoding pool 2's location with pool 0's layout would name a
        // different segment entirely.
        let l0 = LocationLayout::new(1024 * 1024, 8).unwrap();
        let l2 = LocationLayout::new(8 * 1024 * 1024, 512).unwrap();
        assert_ne!(l0.offset_bits(), l2.offset_bits());

        let pool0 = vec![MockSegment::new(0, b"pool0_key")];
        let pool2 = vec![
            MockSegment::new(0, b"pool2_seg0"),
            MockSegment::new(512, b"pool2_seg1"),
        ];

        let verifier = MultiPoolVerifier::new()
            .with_pool(0, l0, &pool0)
            .with_pool(2, l2, &pool2);

        let loc0 = ItemLocation::new(&l0, 0, 0, 0, 0);
        let loc2_0 = ItemLocation::new(&l2, 2, 0, 0, 0);
        let loc2_1 = ItemLocation::new(&l2, 2, 1, 0, 512);
        let loc1 = ItemLocation::new(&l0, 1, 0, 0, 0); // pool 1 not set

        assert!(verifier.verify(b"pool0_key", loc0.to_location(), false));
        assert!(verifier.verify(b"pool2_seg0", loc2_0.to_location(), false));
        assert!(verifier.verify(b"pool2_seg1", loc2_1.to_location(), false));
        assert!(!verifier.verify(b"any", loc1.to_location(), false)); // pool 1 not set
    }

    /// A location from a previous incarnation must not resolve, even though
    /// the key really is at that offset.
    ///
    /// That is the whole hazard: the segment was drained and refilled, and the
    /// n-th item of the new incarnation landed where the n-th item of the old
    /// one was. The key compare alone says yes; only the tag says no.
    #[test]
    fn test_single_pool_verifier_rejects_a_stale_incarnation() {
        let l = mem_layout();
        let segments = vec![MockSegment {
            incarnation: 1,
            keys: vec![(0, b"key".to_vec(), false)],
        }];
        let verifier = SinglePoolVerifier::new(l, &segments);

        let live = ItemLocation::new(&l, 0, 0, 1, 0);
        let stale = ItemLocation::new(&l, 0, 0, 0, 0);

        assert!(
            verifier.verify(b"key", live.to_location(), false),
            "the current incarnation must resolve"
        );
        assert!(
            !verifier.verify(b"key", stale.to_location(), false),
            "a location from a previous incarnation must not resolve"
        );
    }

    /// The same for the multi-pool verifier, whose per-pool layout decodes the
    /// tag before it is compared.
    #[test]
    fn test_multi_pool_verifier_rejects_a_stale_incarnation() {
        let l = mem_layout();
        let pool1 = vec![MockSegment {
            incarnation: 1,
            keys: vec![(0, b"key".to_vec(), false)],
        }];
        let verifier = MultiPoolVerifier::new().with_pool(1, l, &pool1);

        let live = ItemLocation::new(&l, 1, 0, 1, 0);
        let stale = ItemLocation::new(&l, 1, 0, 0, 0);

        assert!(
            verifier.verify(b"key", live.to_location(), false),
            "the current incarnation must resolve"
        );
        assert!(
            !verifier.verify(b"key", stale.to_location(), false),
            "a location from a previous incarnation must not resolve"
        );
    }

    #[test]
    fn test_fn_verifier() {
        let l = mem_layout();
        let verifier = FnVerifier::new(move |key: &[u8], location: Location, _allow_deleted| {
            let item_loc = ItemLocation::from_location(location);
            key == b"magic" && item_loc.segment_id(&l) == 42
        });

        let loc_match = ItemLocation::new(&l, 0, 42, 0, 0);
        let loc_nomatch = ItemLocation::new(&l, 0, 99, 0, 0);

        assert!(verifier.verify(b"magic", loc_match.to_location(), false));
        assert!(!verifier.verify(b"magic", loc_nomatch.to_location(), false));
        assert!(!verifier.verify(b"other", loc_match.to_location(), false));
    }

    #[test]
    fn test_deleted_handling() {
        let l = mem_layout();
        let segments = vec![MockSegment {
            incarnation: 0,
            keys: vec![(0, b"deleted_key".to_vec(), true)],
        }];
        let verifier = SinglePoolVerifier::new(l, &segments);

        let loc = ItemLocation::new(&l, 0, 0, 0, 0);

        // allow_deleted=false should not match
        assert!(!verifier.verify(b"deleted_key", loc.to_location(), false));

        // allow_deleted=true should match
        assert!(verifier.verify(b"deleted_key", loc.to_location(), true));
    }
}
