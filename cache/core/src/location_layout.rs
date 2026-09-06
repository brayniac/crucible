//! Per-pool bit split for `Location`.
//!
//! A `Location` is 44 bits. `pool_id` occupies the top 2 at a fixed position so
//! a decoder can read it before knowing anything else, then select that pool's
//! layout to decode the rest. The remaining 42 bits split three ways:
//!
//! ```text
//! [43..42] pool_id      2 bits, fixed
//! [41..  ] segment_id   42 - 6 - offset_bits
//! [  ..  ] incarnation  6 bits
//! [  ..0 ] offset/align offset_bits
//! ```
//!
//! # Capacity is invariant under `segment_size`
//!
//! With a fixed tag width, offset bits gained are segment bits lost, exactly:
//!
//! ```text
//! capacity/pool = 2^(42 - TAG_BITS) * align_bytes = 2^36 * align_bytes
//! ```
//!
//! `segment_size` does not appear. The alignment factor is the only capacity
//! lever, which is why disk pools default to 512-byte alignment (32 TiB/pool)
//! while memory pools stay at 8 (512 GiB/pool).

use std::fmt;

/// Bits available below `pool_id`.
const PAYLOAD_BITS: u32 = 42;

/// Width of the incarnation tag. Fixed: see the module docs on why widening it
/// would cost capacity rather than segment size.
pub const TAG_BITS: u32 = 6;

/// Mask for a 6-bit incarnation tag.
pub const TAG_MASK: u8 = (1 << TAG_BITS) - 1;

/// Why a pool's location layout could not be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutError {
    /// `align_bytes` was not a power of two.
    AlignNotPowerOfTwo {
        /// The rejected `align_bytes` value.
        align: usize,
    },
    /// `align_bytes` was below the 8-byte item alignment.
    AlignTooSmall {
        /// The rejected `align_bytes` value.
        align: usize,
    },
    /// `align_bytes` exceeded `segment_size`.
    AlignExceedsSegment {
        /// The rejected `align_bytes` value.
        align: usize,
        /// The pool's segment size.
        segment_size: usize,
    },
    /// `segment_size / align_bytes` needs so many offset bits that no segment
    /// ids are left.
    SegmentTooLarge {
        /// The pool's segment size.
        segment_size: usize,
        /// The pool's `align_bytes` value.
        align: usize,
        /// The offset field width this combination would require.
        offset_bits: u32,
    },
    /// `segment_size / align_bytes` yields so few offset units that more than
    /// 31 segment-id bits remain, which a `u32` segment id cannot hold.
    SegmentTooSmall {
        /// The rejected segment size.
        segment_size: usize,
        /// The alignment factor in force.
        align: usize,
        /// Segment-id bits the split would have left.
        seg_bits: u32,
    },
    /// The pool has more segments than the layout can address.
    TooManySegments {
        /// The number of segments the pool needs.
        requested: usize,
        /// The most segments this layout can address.
        max: u32,
    },
}

impl fmt::Display for LayoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlignNotPowerOfTwo { align } => {
                write!(f, "align_bytes ({align}) must be a power of two")
            }
            Self::AlignTooSmall { align } => write!(
                f,
                "align_bytes ({align}) must be at least 8, the item alignment"
            ),
            Self::AlignExceedsSegment {
                align,
                segment_size,
            } => write!(
                f,
                "align_bytes ({align}) must not exceed segment_size ({segment_size})"
            ),
            Self::SegmentTooLarge {
                segment_size,
                align,
                offset_bits,
            } => write!(
                f,
                "segment_size ({segment_size}) at align_bytes ({align}) needs \
                 {offset_bits} offset bits, and offsets past u32 cannot be \
                 represented; reduce segment_size. Raising align_bytes does not \
                 help -- it lowers offset_bits by exactly as much as it raises \
                 the shift"
            ),
            Self::SegmentTooSmall {
                segment_size,
                align,
                seg_bits,
            } => write!(
                f,
                "segment_size ({segment_size}) at align_bytes ({align}) addresses \
                 too few slots, leaving {seg_bits} segment-id bits -- more than the \
                 31 a u32 segment id can hold; raise segment_size or lower align_bytes"
            ),
            Self::TooManySegments { requested, max } => write!(
                f,
                "pool needs {requested} segments but the layout addresses at most \
                 {max}; reduce heap_size, raise segment_size, or raise align_bytes"
            ),
        }
    }
}

impl std::error::Error for LayoutError {}

impl From<LayoutError> for std::io::Error {
    fn from(e: LayoutError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, e)
    }
}

/// How one pool splits the 44 location bits.
///
/// Small and `Copy`: two shifts is all that is stored, everything else derives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocationLayout {
    /// `log2(align_bytes)`.
    align_shift: u8,
    /// Width of the offset field.
    offset_bits: u8,
}

impl LocationLayout {
    /// Position of the 2-bit `pool_id`, identical for every pool.
    pub const POOL_SHIFT: u32 = PAYLOAD_BITS;

    /// Build the layout for a pool.
    pub fn new(segment_size: usize, align_bytes: usize) -> Result<Self, LayoutError> {
        if !align_bytes.is_power_of_two() {
            return Err(LayoutError::AlignNotPowerOfTwo { align: align_bytes });
        }
        if align_bytes < 8 {
            return Err(LayoutError::AlignTooSmall { align: align_bytes });
        }
        if align_bytes > segment_size {
            return Err(LayoutError::AlignExceedsSegment {
                align: align_bytes,
                segment_size,
            });
        }

        // Offset units needed to address the segment. `segment_size` need not be
        // a power of two, so round the unit count up before taking its width.
        let units = segment_size.div_ceil(align_bytes);
        let offset_bits = units.next_power_of_two().trailing_zeros();

        let align_shift = align_bytes.trailing_zeros();

        // Both fields are u32 in every consumer -- `SliceSegment::capacity`,
        // `write_offset` and `ItemLocation::new` for the offset, and `segment_id`
        // for the id -- so a layout that addresses past u32 on either end is
        // meaningless and must be rejected rather than silently truncated.
        //
        // Note the id-space bound (`offset_bits + TAG_BITS >= PAYLOAD_BITS`) is
        // NOT checked: it is unreachable. `align_bytes >= 8` forces
        // `align_shift >= 3`, so the offset ceiling below caps `offset_bits` at
        // 29, well under the 36 that bound would need.
        if offset_bits + align_shift > 32 {
            return Err(LayoutError::SegmentTooLarge {
                segment_size,
                align: align_bytes,
                offset_bits,
            });
        }

        // Safe from underflow only because the ceiling above already capped
        // offset_bits at 29 (align_shift >= 3). This ordering is load-bearing.
        let seg_bits = PAYLOAD_BITS - TAG_BITS - offset_bits;
        if seg_bits > 31 {
            return Err(LayoutError::SegmentTooSmall {
                segment_size,
                align: align_bytes,
                seg_bits,
            });
        }

        Ok(Self {
            align_shift: align_shift as u8,
            offset_bits: offset_bits as u8,
        })
    }

    /// Width of the offset field.
    #[inline]
    pub fn offset_bits(&self) -> u32 {
        self.offset_bits as u32
    }

    /// Width of the segment id field.
    #[inline]
    pub fn seg_bits(&self) -> u32 {
        PAYLOAD_BITS - TAG_BITS - self.offset_bits as u32
    }

    /// Alignment factor in bytes.
    #[inline]
    pub fn align_bytes(&self) -> u32 {
        1 << self.align_shift
    }

    /// How many segments a pool may hold, so ids run `0..max_segment_count()`.
    ///
    /// One below the field's capacity: leaving the top id unissuable is what
    /// keeps `Location::GHOST` (all 44 bits set) unaliasable by construction.
    #[inline]
    pub fn max_segment_count(&self) -> u32 {
        (1u32 << self.seg_bits()) - 1
    }

    /// Reject a pool that has more segments than this layout can address.
    pub fn validate_segment_count(&self, count: usize) -> Result<(), LayoutError> {
        if count > self.max_segment_count() as usize {
            return Err(LayoutError::TooManySegments {
                requested: count,
                max: self.max_segment_count(),
            });
        }
        Ok(())
    }

    #[inline]
    fn seg_shift(&self) -> u32 {
        self.offset_bits as u32 + TAG_BITS
    }

    /// Pack the four fields into raw location bits.
    ///
    /// `incarnation` is masked to 6 bits; the other three are `debug_assert`ed
    /// in range.
    #[inline]
    pub fn pack(&self, pool_id: u8, segment_id: u32, incarnation: u8, offset: u32) -> u64 {
        debug_assert!(pool_id <= 3, "pool_id must be 0-3");
        debug_assert!(
            segment_id < self.max_segment_count(),
            "segment_id {segment_id} is outside the issuable range 0..{}",
            self.max_segment_count()
        );
        debug_assert!(
            offset.is_multiple_of(self.align_bytes()),
            "offset must be {}-byte aligned",
            self.align_bytes()
        );
        debug_assert!(
            (offset >> self.align_shift) < (1u32 << self.offset_bits as u32),
            "offset {offset} exceeds this layout's {}-bit offset field",
            self.offset_bits
        );

        ((pool_id as u64) << Self::POOL_SHIFT)
            | ((segment_id as u64) << self.seg_shift())
            | (((incarnation & TAG_MASK) as u64) << self.offset_bits as u32)
            | ((offset >> self.align_shift) as u64)
    }

    /// Extract `pool_id`. Layout-independent by design: this is what a decoder
    /// reads first, to find out which layout decodes the rest.
    #[inline]
    pub fn pool_id(raw: u64) -> u8 {
        ((raw >> Self::POOL_SHIFT) & 0b11) as u8
    }

    /// Extract `segment_id`.
    #[inline]
    pub fn segment_id(&self, raw: u64) -> u32 {
        ((raw >> self.seg_shift()) & ((1 << self.seg_bits()) - 1)) as u32
    }

    /// Extract the incarnation tag.
    #[inline]
    pub fn incarnation(&self, raw: u64) -> u8 {
        ((raw >> self.offset_bits as u32) as u8) & TAG_MASK
    }

    /// Extract the byte offset.
    #[inline]
    pub fn offset(&self, raw: u64) -> u32 {
        ((raw & ((1u64 << self.offset_bits as u32) - 1)) as u32) << self.align_shift
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn memory_defaults_split_as_documented() {
        // 1 MB segments, 8-byte alignment: offset needs log2(1MB/8) = 17 bits.
        let layout = LocationLayout::new(1024 * 1024, 8).unwrap();
        assert_eq!(layout.offset_bits(), 17);
        assert_eq!(layout.seg_bits(), 19); // 42 - 6 - 17
        // A count, not a max id: ids run 0..max_segment_count(), so the top
        // field value is never issued.
        assert_eq!(layout.max_segment_count(), (1 << 19) - 1);

        // 8 MB segments: offset needs 20 bits.
        let layout = LocationLayout::new(8 * 1024 * 1024, 8).unwrap();
        assert_eq!(layout.offset_bits(), 20);
        assert_eq!(layout.seg_bits(), 16);
    }

    #[test]
    fn disk_default_alignment_buys_capacity() {
        // 8 MB segments at 512-byte alignment: offset needs log2(8MB/512) = 14 bits.
        let layout = LocationLayout::new(8 * 1024 * 1024, 512).unwrap();
        assert_eq!(layout.offset_bits(), 14);
        assert_eq!(layout.seg_bits(), 22);
    }

    #[test]
    fn capacity_is_invariant_under_segment_size() {
        // The design's central claim: offset bits gained are segment bits lost,
        // so capacity depends only on the alignment factor. If this ever fails,
        // the justification for the disk alignment change is gone.
        for segment_size in [
            1024 * 1024,
            8 * 1024 * 1024,
            32 * 1024 * 1024,
            128 * 1024 * 1024,
        ] {
            let layout = LocationLayout::new(segment_size, 8).unwrap();
            let capacity = (1u64 << layout.seg_bits()) * segment_size as u64;
            assert_eq!(
                capacity,
                512 * 1024 * 1024 * 1024,
                "segment_size {segment_size} should still yield 512 GiB"
            );
        }
    }

    #[test]
    fn roundtrips_every_field() {
        for segment_size in [1024 * 1024, 8 * 1024 * 1024, 128 * 1024 * 1024] {
            for align in [8, 64, 512] {
                let layout = LocationLayout::new(segment_size, align).unwrap();
                for &seg in &[0u32, 1, 7, layout.max_segment_count() - 1] {
                    for incarnation in 0u8..64 {
                        for &offset in &[0u32, align as u32, segment_size as u32 - align as u32] {
                            let raw = layout.pack(3, seg, incarnation, offset);
                            assert_eq!(LocationLayout::pool_id(raw), 3);
                            assert_eq!(layout.segment_id(raw), seg);
                            assert_eq!(layout.incarnation(raw), incarnation);
                            assert_eq!(layout.offset(raw), offset);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn ghost_is_unaliasable() {
        // The top segment id is never issued, so no issuable location can
        // collide with Location::GHOST (all 44 bits set).
        let layout = LocationLayout::new(1024 * 1024, 8).unwrap();
        let max_offset = ((1u32 << layout.offset_bits()) - 1) * layout.align_bytes();

        // The highest location any pool can actually issue.
        let highest = layout.pack(3, layout.max_segment_count() - 1, 0x3F, max_offset);
        assert_ne!(highest, crate::location::Location::MAX_RAW);

        // And the reason it cannot: only the unissuable top id reaches GHOST.
        // Built directly, not via pack: this id is out of the layout's field
        // range by construction, which is the whole point.
        let unissuable = (3u64 << LocationLayout::POOL_SHIFT)
            | ((layout.max_segment_count() as u64) << (layout.offset_bits() + TAG_BITS))
            | (0x3Fu64 << layout.offset_bits())
            | ((max_offset / layout.align_bytes()) as u64);
        assert_eq!(unissuable, crate::location::Location::MAX_RAW);
    }

    #[test]
    fn rejects_bad_alignment() {
        assert!(matches!(
            LocationLayout::new(1024 * 1024, 3),
            Err(LayoutError::AlignNotPowerOfTwo { .. })
        ));
        assert!(matches!(
            LocationLayout::new(1024 * 1024, 4),
            Err(LayoutError::AlignTooSmall { .. })
        ));
        assert!(matches!(
            LocationLayout::new(1024, 4096),
            Err(LayoutError::AlignExceedsSegment { .. })
        ));
    }

    #[test]
    fn rejects_segment_too_large_to_address() {
        // Offsets are u32 throughout the crate (`SliceSegment::capacity`,
        // `write_offset`, `ItemLocation::new`), so the real ceiling is a layout
        // whose widest offset still fits in u32: offset_bits + align_shift <= 32.
        // At 8-byte alignment that is a 4 GiB segment, an order of magnitude
        // above the 128 MB the tests exercise.
        assert!(LocationLayout::new(4 * 1024 * 1024 * 1024, 8).is_ok());
        assert!(matches!(
            LocationLayout::new(8 * 1024 * 1024 * 1024, 8),
            Err(LayoutError::SegmentTooLarge { .. })
        ));

        // Coarser alignment does not buy past it: the offset field shifts left
        // by align_shift, so the two trade off exactly.
        assert!(LocationLayout::new(4 * 1024 * 1024 * 1024, 512).is_ok());
        assert!(matches!(
            LocationLayout::new(512 * 1024 * 1024 * 1024, 512),
            Err(LayoutError::SegmentTooLarge { .. })
        ));
    }

    #[test]
    fn every_accepted_layout_can_represent_its_own_widest_offset() {
        // Guards the truncation the u32 bound exists to prevent: if `new`
        // accepts a layout, packing its widest offset must round-trip.
        //
        // The list MUST contain a size past the 4 GiB boundary. 4 GiB is where
        // offset_bits + align_shift == 32 exactly -- still representable -- so a
        // list stopping there never constructs a truncating layout and the test
        // cannot go red when the bound is removed. 8 GiB is the first size that
        // `new` would wrongly accept without it.
        for segment_size in [
            1024 * 1024usize,
            8 * 1024 * 1024,
            128 * 1024 * 1024,
            4 * 1024 * 1024 * 1024,
            8 * 1024 * 1024 * 1024,
        ] {
            for align in [8usize, 512] {
                let Ok(layout) = LocationLayout::new(segment_size, align) else {
                    continue;
                };

                // Computed in u64 on purpose: in u32 this multiply would itself
                // overflow and panic, reddening the test for the wrong reason.
                let widest = ((1u64 << layout.offset_bits()) - 1) * layout.align_bytes() as u64;
                assert!(
                    widest <= u32::MAX as u64,
                    "segment_size {segment_size} align {align} was accepted but \
                     addresses past u32 (widest offset {widest})"
                );

                let widest = widest as u32;
                assert_eq!(
                    layout.offset(layout.pack(0, 0, 0, widest)),
                    widest,
                    "segment_size {segment_size} align {align} truncates its widest offset"
                );
            }
        }
    }

    #[test]
    fn rejects_more_segments_than_fit() {
        let layout = LocationLayout::new(1024 * 1024, 8).unwrap();
        assert!(
            layout
                .validate_segment_count(layout.max_segment_count() as usize)
                .is_ok()
        );
        assert!(
            layout
                .validate_segment_count(layout.max_segment_count() as usize + 1)
                .is_err()
        );
    }

    #[test]
    fn rejects_segments_with_too_few_addressable_slots() {
        // The floor, mirroring the u32 ceiling. `segment_id` is a u32
        // throughout the crate, so a layout leaving more than 31 segment-id
        // bits is meaningless -- and `1u32 << seg_bits` overflows computing
        // `max_segment_count()`. A 4 KiB disk segment at the 512-byte disk
        // default lands here, and 4 KiB is exactly the block size the design
        // names, so this is reachable rather than theoretical.
        assert!(matches!(
            LocationLayout::new(4096, 512),
            Err(LayoutError::SegmentTooSmall { .. })
        ));
        assert!(matches!(
            LocationLayout::new(8192, 512),
            Err(LayoutError::SegmentTooSmall { .. })
        ));
        assert!(LocationLayout::new(16 * 1024, 512).is_ok());

        // The degenerate align == segment_size case is subsumed by the same
        // floor: it derives offset_bits 0, hence seg_bits 36.
        assert!(matches!(
            LocationLayout::new(4 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024),
            Err(LayoutError::SegmentTooSmall { .. })
        ));
    }

    #[test]
    fn every_accepted_layout_has_computable_fields() {
        // Companion to `every_accepted_layout_can_represent_its_own_widest_offset`
        // at the other end: if `new` accepts a layout, every derived quantity
        // must be computable rather than overflowing a shift.
        for segment_size in [
            4096usize,
            8192,
            16 * 1024,
            64 * 1024,
            1024 * 1024,
            8 * 1024 * 1024,
            128 * 1024 * 1024,
            4 * 1024 * 1024 * 1024,
        ] {
            for align in [8usize, 512, 4096] {
                let Ok(layout) = LocationLayout::new(segment_size, align) else {
                    continue;
                };
                assert!(
                    layout.seg_bits() <= 31,
                    "segment_size {segment_size} align {align} was accepted with \
                     {} segment-id bits, which overflows a u32 shift",
                    layout.seg_bits()
                );
                assert!(layout.max_segment_count() > 0);
                assert_eq!(layout.align_bytes() as usize, align);
                assert_eq!(layout.offset(layout.pack(0, 0, 0, 0)), 0);
            }
        }
    }

    #[test]
    fn derives_offset_bits_for_non_power_of_two_segment_sizes() {
        // `segment_size` is an arbitrary usize from the pool builder, and the
        // div_ceil/next_power_of_two derivation exists solely for this case --
        // nothing else pins it. The naive
        // `segment_size.trailing_zeros() - align_shift` gives 16 here and would
        // silently truncate most of the segment.
        let layout = LocationLayout::new(1536 * 1024, 8).unwrap();
        assert_eq!(layout.offset_bits(), 18);

        let last = 1536 * 1024 - 8;
        assert_eq!(layout.offset(layout.pack(0, 0, 0, last)), last);

        // Pins div_ceil specifically: with plain division this is 7 bits, which
        // cannot address the last 9 bytes of the segment.
        assert_eq!(LocationLayout::new(1025, 8).unwrap().offset_bits(), 8);
    }

    #[test]
    fn segment_too_large_names_the_ceiling_that_actually_fired() {
        // The message must not advise raising align_bytes: for a power-of-two
        // segment_size, offset_bits + align_shift is invariant, so coarser
        // alignment provably cannot get past this ceiling. Advising it sends an
        // operator down a dead end.
        let err = LocationLayout::new(8 * 1024 * 1024 * 1024, 8).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("u32"),
            "message should name the real limit: {msg}"
        );
        assert!(
            !msg.contains("raise align_bytes"),
            "message advises a remedy that cannot work: {msg}"
        );
    }

    #[test]
    fn pool_id_decodes_without_a_layout_instance() {
        // The module's premise: pool_id is read to CHOOSE a layout, so reading
        // it must not require already having one.
        let raw = LocationLayout::new(1024 * 1024, 8)
            .unwrap()
            .pack(2, 5, 0, 0);
        assert_eq!(LocationLayout::pool_id(raw), 2);
    }

    #[test]
    #[should_panic(expected = "exceeds this layout's 17-bit offset field")]
    fn pack_rejects_an_offset_past_the_offset_field() {
        // One byte past a 1 MiB segment, and still 8-aligned, so the alignment
        // assert waves it through. Without the range check this silently
        // overflows into the incarnation field and returns a corrupted tag.
        let layout = LocationLayout::new(1024 * 1024, 8).unwrap();
        layout.pack(0, 5, 0, 1024 * 1024);
    }

    #[test]
    #[should_panic(expected = "is outside the issuable range")]
    fn pack_rejects_the_unissuable_top_segment_id() {
        // The id reserved so Location::GHOST cannot be aliased. A pool must
        // never issue it, and pack must say so rather than encode it.
        let layout = LocationLayout::new(1024 * 1024, 8).unwrap();
        layout.pack(0, layout.max_segment_count(), 0, 0);
    }

    #[test]
    fn io_error_conversion_preserves_the_structured_error() {
        let err = LocationLayout::new(4096, 512).unwrap_err();
        let io: std::io::Error = err.into();
        assert_eq!(io.kind(), std::io::ErrorKind::InvalidInput);
        assert!(matches!(
            io.get_ref().and_then(|e| e.downcast_ref::<LayoutError>()),
            Some(LayoutError::SegmentTooSmall { .. })
        ));
    }
}
