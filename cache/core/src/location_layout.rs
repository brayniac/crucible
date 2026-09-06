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
                "segment_size ({segment_size}) needs {offset_bits} offset bits at \
                 align_bytes ({align}), leaving none for segment ids; \
                 reduce segment_size or raise align_bytes"
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
        std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string())
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
    pub const POOL_SHIFT: u32 = 42;

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

        if offset_bits + TAG_BITS >= PAYLOAD_BITS {
            return Err(LayoutError::SegmentTooLarge {
                segment_size,
                align: align_bytes,
                offset_bits,
            });
        }

        Ok(Self {
            align_shift: align_bytes.trailing_zeros() as u8,
            offset_bits: offset_bits as u8,
        })
    }

    /// Width of the offset field.
    #[inline(always)]
    pub fn offset_bits(&self) -> u32 {
        self.offset_bits as u32
    }

    /// Width of the segment id field.
    #[inline(always)]
    pub fn seg_bits(&self) -> u32 {
        PAYLOAD_BITS - TAG_BITS - self.offset_bits as u32
    }

    /// Alignment factor in bytes.
    #[inline(always)]
    pub fn align_bytes(&self) -> u32 {
        1 << self.align_shift
    }

    /// How many segments a pool may hold, so ids run `0..max_segment_count()`.
    ///
    /// One below the field's capacity: leaving the top id unissuable is what
    /// keeps `Location::GHOST` (all 44 bits set) unaliasable by construction.
    #[inline(always)]
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

    #[inline(always)]
    fn seg_shift(&self) -> u32 {
        self.offset_bits as u32 + TAG_BITS
    }

    /// Pack the four fields into raw location bits.
    #[inline(always)]
    pub fn pack(&self, pool_id: u8, segment_id: u32, incarnation: u8, offset: u32) -> u64 {
        debug_assert!(pool_id <= 3, "pool_id must be 0-3");
        // `<=` not `<`: pack is a pure bit function, and the top id is a
        // representable value -- it is `validate_segment_count` that keeps a
        // pool from ever issuing it, which is what protects GHOST.
        debug_assert!(
            segment_id <= self.max_segment_count(),
            "segment_id exceeds this layout"
        );
        debug_assert!(
            offset.is_multiple_of(self.align_bytes()),
            "offset must be {}-byte aligned",
            self.align_bytes()
        );

        ((pool_id as u64) << Self::POOL_SHIFT)
            | ((segment_id as u64) << self.seg_shift())
            | (((incarnation & TAG_MASK) as u64) << self.offset_bits as u32)
            | ((offset >> self.align_shift) as u64)
    }

    /// Extract `pool_id`. Layout-independent by design.
    #[inline(always)]
    pub fn pool_id(&self, raw: u64) -> u8 {
        ((raw >> Self::POOL_SHIFT) & 0b11) as u8
    }

    /// Extract `segment_id`.
    #[inline(always)]
    pub fn segment_id(&self, raw: u64) -> u32 {
        ((raw >> self.seg_shift()) & ((1 << self.seg_bits()) - 1)) as u32
    }

    /// Extract the incarnation tag.
    #[inline(always)]
    pub fn incarnation(&self, raw: u64) -> u8 {
        ((raw >> self.offset_bits as u32) as u8) & TAG_MASK
    }

    /// Extract the byte offset.
    #[inline(always)]
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
                            assert_eq!(layout.pool_id(raw), 3);
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
        let unissuable = layout.pack(3, layout.max_segment_count(), 0x3F, max_offset);
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
        // offset_bits + 6 must leave room for segment ids, so at 8-byte
        // alignment the ceiling is a 512 GiB segment (offset_bits == 36).
        assert!(LocationLayout::new(256 * 1024 * 1024 * 1024, 8).is_ok());
        assert!(matches!(
            LocationLayout::new(512 * 1024 * 1024 * 1024, 8),
            Err(LayoutError::SegmentTooLarge { .. })
        ));
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
}
