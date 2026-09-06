# Incarnation-Tagged Locations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every `ItemLocation` a 6-bit incarnation tag so a stale location naming a recycled segment is rejected before any item bytes are read.

**Architecture:** The 44 location bits are repartitioned per pool. `pool_id` stays at a fixed position (bits 43..42) so a decoder reads it first and then selects that pool's `LocationLayout`, which derives the offset-field width from `segment_size / align_bytes` and gives 6 bits to the tag. The tag itself lives in the unused byte of the segment's packed `Metadata` word, so bumping it is atomic with the state transition that ends an incarnation.

**Tech Stack:** Rust, `cache-core`. Testing: `cargo test -p cache-core`, `cargo test -p cache-core --features loom`, `cargo +nightly miri test -p cache-core --lib`.

**Spec:** `docs/superpowers/specs/2026-09-06-location-incarnation-tag-design.md`

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `cache/core/src/location_layout.rs` | Derive and validate the per-pool bit split | **create** |
| `cache/core/src/state.rs` | `Metadata` carries a 6-bit `incarnation` | modify |
| `cache/core/src/slice_segment.rs` | Bump sites; `incarnation()` accessor | modify |
| `cache/core/src/segment.rs` | `Segment::incarnation()` trait method | modify |
| `cache/core/src/item_location.rs` | Layout-aware pack/unpack; verifier tag check | modify |
| `cache/core/src/memory_pool.rs` | `align_bytes` config, layout construction | modify |
| `cache/core/src/disk/file_pool.rs`, `disk/io_uring_pool.rs` | same, defaulting to 512 B | modify |
| `cache/core/src/pool.rs` | `RamPool::layout()` | modify |
| `cache/core/src/cache.rs` | `CacheKeyVerifier` holds layouts; the tag check | modify |
| `cache/core/src/layer/{fifo,ttl}_layer.rs`, `disk/disk_layer.rs` | Stamp the tag when publishing a location | modify |
| `cache/core/src/loom_oracle.rs` | Model incarnations | modify |

`location_layout.rs` is a new file rather than more code in `item_location.rs` (501 lines) because layout derivation and validation is a self-contained unit with its own error cases, and it is the piece the property test targets.

---

### Task 1: `LocationLayout`

**Files:**
- Create: `cache/core/src/location_layout.rs`
- Modify: `cache/core/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `cache/core/src/location_layout.rs` containing only this test module:

```rust
//! Per-pool bit split for `Location`.

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
                        for &offset in &[0u32, align, segment_size as u32 - align] {
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
                let widest =
                    ((1u64 << layout.offset_bits()) - 1) * layout.align_bytes() as u64;
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
        assert!(layout
            .validate_segment_count(layout.max_segment_count() as usize)
            .is_ok());
        assert!(layout
            .validate_segment_count(layout.max_segment_count() as usize + 1)
            .is_err());
    }
}
```

Register the module. In `cache/core/src/lib.rs`, next to the existing `mod location;` line, add:

```rust
pub mod location_layout;
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p cache-core --lib location_layout`
Expected: FAIL to compile, `cannot find type LocationLayout in this scope`.

- [ ] **Step 3: Write the implementation**

Insert above the test module in `cache/core/src/location_layout.rs`:

```rust
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
    AlignNotPowerOfTwo { align: usize },
    /// `align_bytes` was below the 8-byte item alignment.
    AlignTooSmall { align: usize },
    /// `align_bytes` exceeded `segment_size`.
    AlignExceedsSegment { align: usize, segment_size: usize },
    /// `segment_size / align_bytes` needs so many offset bits that no segment
    /// ids are left.
    SegmentTooLarge {
        segment_size: usize,
        align: usize,
        offset_bits: u32,
    },
    /// The pool has more segments than the layout can address.
    TooManySegments { requested: usize, max: u32 },
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

        let align_shift = align_bytes.trailing_zeros();

        // Two independent ceilings, both reported as SegmentTooLarge:
        //
        // 1. The offset field must leave room for segment ids.
        // 2. The widest offset must fit in u32. Offsets are u32 throughout the
        //    crate -- `SliceSegment::capacity`, `write_offset`, and
        //    `ItemLocation::new` all use it -- so a layout that addresses past
        //    u32 would truncate in `pack`/`offset` rather than error, and the
        //    round trip would silently lose the high bits.
        if offset_bits + TAG_BITS >= PAYLOAD_BITS || offset_bits + align_shift > 32 {
            return Err(LayoutError::SegmentTooLarge {
                segment_size,
                align: align_bytes,
                offset_bits,
            });
        }

        Ok(Self {
            align_shift: align_shift as u8,
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

    #[inline(always)]
    pub fn segment_id(&self, raw: u64) -> u32 {
        ((raw >> self.seg_shift()) & ((1 << self.seg_bits()) - 1)) as u32
    }

    #[inline(always)]
    pub fn incarnation(&self, raw: u64) -> u8 {
        ((raw >> self.offset_bits as u32) as u8) & TAG_MASK
    }

    #[inline(always)]
    pub fn offset(&self, raw: u64) -> u32 {
        ((raw & ((1u64 << self.offset_bits as u32) - 1)) as u32) << self.align_shift
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p cache-core --lib location_layout`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
git add cache/core/src/location_layout.rs cache/core/src/lib.rs
git commit -m "feat(cache-core): add per-pool LocationLayout

Derives the offset field width from segment_size/align_bytes and gives a
fixed 6 bits to the incarnation tag. Capacity per pool is invariant under
segment_size and depends only on the alignment factor -- pinned by
capacity_is_invariant_under_segment_size, since that claim is what
justifies coarsening disk alignment."
```

---

### Task 1b: fixes from the Task 1 code-quality review

The review found a Critical plus four Important issues. C1 is the third instance
of one defect class in this module -- **an unvalidated bound on a derived
shift** -- so it must be closed before Tasks 2-9 wire the type into pools, where
a wrong-but-`Ok` layout surfaces as an inexplicable pool-construction failure
rather than a layout error.

**Files:** Modify `cache/core/src/location_layout.rs`, `cache/core/src/lib.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
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
    }

    #[test]
    fn segment_too_large_names_the_ceiling_that_actually_fired() {
        // The message must not advise raising align_bytes: for a power-of-two
        // segment_size, offset_bits + align_shift is invariant, so coarser
        // alignment provably cannot get past this ceiling. Advising it sends an
        // operator down a dead end.
        let err = LocationLayout::new(8 * 1024 * 1024 * 1024, 8).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("u32"), "message should name the real limit: {msg}");
        assert!(
            !msg.contains("raise align_bytes"),
            "message advises a remedy that cannot work: {msg}"
        );
    }

    #[test]
    fn pool_id_decodes_without_a_layout_instance() {
        // The module's premise: pool_id is read to CHOOSE a layout, so reading
        // it must not require already having one.
        let raw = LocationLayout::new(1024 * 1024, 8).unwrap().pack(2, 5, 0, 0);
        assert_eq!(LocationLayout::pool_id(raw), 2);
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p cache-core --lib location_layout`
Expected: FAIL. `rejects_segments_with_too_few_addressable_slots` and
`every_accepted_layout_has_computable_fields` panic on a shift overflow in
debug (`attempt to shift left with overflow`);
`derives_offset_bits_for_non_power_of_two_segment_sizes` passes already (it
documents existing correct behaviour, and is the regression guard I4 asked
for); `segment_too_large_names_the_ceiling_that_actually_fired` and
`pool_id_decodes_without_a_layout_instance` fail to compile or assert.

- [ ] **Step 3: Fix C1 -- add the floor**

Add the variant to `LayoutError`:

```rust
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
```

and its `Display` arm:

```rust
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
```

In `new()`, replace the ceiling block with both bounds:

```rust
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

        let seg_bits = PAYLOAD_BITS - TAG_BITS - offset_bits;
        if seg_bits > 31 {
            return Err(LayoutError::SegmentTooSmall {
                segment_size,
                align: align_bytes,
                seg_bits,
            });
        }
```

- [ ] **Step 4: Fix I2 -- correct the `SegmentTooLarge` message**

It currently describes the unreachable ceiling and advises a remedy that
provably cannot work. Replace its `Display` arm:

```rust
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
```

- [ ] **Step 5: Fix I3 -- range-check `offset` in `pack`**

An out-of-range offset currently overflows into the incarnation field, silently
corrupting the tag this module exists to protect. Also document the
`incarnation` masking, and express the `segment_id` bound as a field width (M2)
so the assert can catch a caller's off-by-one:

```rust
    /// Pack the four fields into raw location bits.
    ///
    /// `incarnation` is masked to 6 bits; the other three are `debug_assert`ed
    /// in range.
    #[inline]
    pub fn pack(&self, pool_id: u8, segment_id: u32, incarnation: u8, offset: u32) -> u64 {
        debug_assert!(pool_id <= 3, "pool_id must be 0-3");
        debug_assert!(
            segment_id < (1u32 << self.seg_bits()),
            "segment_id {segment_id} exceeds this layout's {}-bit field",
            self.seg_bits()
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
```

`ghost_is_unaliasable` packs the unissuable top segment id deliberately, so it
must construct that value directly rather than through `pack`:

```rust
        // Built directly, not via pack: this id is out of the layout's field
        // range by construction, which is the whole point.
        let unissuable = ((3u64) << LocationLayout::POOL_SHIFT)
            | ((layout.max_segment_count() as u64) << (layout.offset_bits() + 6))
            | ((0x3F as u64) << layout.offset_bits())
            | ((max_offset >> 3) as u64);
        assert_eq!(unissuable, crate::location::Location::MAX_RAW);
```

- [ ] **Step 6: Fix I5 and the minors**

`pool_id` becomes an associated function -- the module's premise is that it is
read *before* a layout is known:

```rust
    /// Extract `pool_id`. Layout-independent by design: this is what a decoder
    /// reads first, to find out which layout decodes the rest.
    #[inline]
    pub fn pool_id(raw: u64) -> u8 {
        ((raw >> Self::POOL_SHIFT) & 0b11) as u8
    }
```

Then:
- **M1:** `pub const POOL_SHIFT: u32 = PAYLOAD_BITS;` so the two cannot drift.
- **M5:** `io::Error::new(ErrorKind::InvalidInput, e)` instead of `e.to_string()`, so pool builders can `downcast_ref::<LayoutError>()`.
- **M6:** follow the crate convention `lib.rs` already uses for `Location` -- `mod location_layout;` plus `pub use location_layout::{LayoutError, LocationLayout, TAG_BITS, TAG_MASK};`.
- **M7:** `#[inline]` rather than `#[inline(always)]` on the accessors. The design doc flags the layout-dependent shifts as an open measurement; forcing inlining removes the compiler's judgment from the exact thing that needs measuring.

- [ ] **Step 7: Verify**

Run: `cargo test -p cache-core --lib location_layout`
Expected: PASS, 14 tests.

Run: `cargo test -p cache-core --lib`, `cargo clippy -p cache-core --all-targets -- -D warnings`, `cargo fmt`
Expected: all clean.

- [ ] **Step 8: Prove the floor red**

Remove the `seg_bits > 31` rejection and confirm BOTH
`rejects_segments_with_too_few_addressable_slots` and
`every_accepted_layout_has_computable_fields` fail. Restore it. Quote the
observed failure text in the commit message.

- [ ] **Step 9: Commit**

```bash
git add cache/core/src/location_layout.rs cache/core/src/lib.rs
git commit -m "fix(cache-core): bound LocationLayout on both ends, not just the ceiling

Third instance of one defect class in this module: an unvalidated bound on a
derived shift. seg_bits is 36 - offset_bits, so a small segment leaves more
than 31 segment-id bits and 1u32 << seg_bits overflows -- reachable with a
4 KiB disk segment at the 512-byte disk default.

Also: the id-space ceiling was dead code (align_bytes >= 8 caps offset_bits
at 29), and its message misdescribed the live ceiling while advising a remedy
that provably cannot work. pack now range-checks offset, which previously
overflowed into the incarnation field -- silently corrupting the very tag this
module exists to protect."
```

---

### Task 1c: pin the guards Task 1b added

The Task 1b re-review approved the fixes but found the most consequential new
guard untested, plus four cheap items. N1 and N4 are the substantive ones.

**Files:** Modify `cache/core/src/location_layout.rs`

- [ ] **Step 1: N1 -- pin the `offset` assert**

Deleting `pack`'s offset range check currently leaves all 14 tests green. It is
the guard against silent incarnation corruption -- the failure this whole
feature exists to prevent -- so it must be proven red like everything else.

```rust
    #[test]
    #[should_panic(expected = "exceeds this layout's 17-bit offset field")]
    fn pack_rejects_an_offset_past_the_offset_field() {
        // One byte past a 1 MiB segment, and still 8-aligned, so the alignment
        // assert waves it through. Without the range check this silently
        // overflows into the incarnation field and returns a corrupted tag.
        let layout = LocationLayout::new(1024 * 1024, 8).unwrap();
        layout.pack(0, 5, 0, 1024 * 1024);
    }
```

- [ ] **Step 2: N4 -- tighten the `segment_id` assert to the issuable range**

`ghost_is_unaliasable` now builds its word directly, so `pack` no longer needs
to admit the top id. As written the assert is value-identical to the old one and
still passes for `max_segment_count()` -- the GHOST-aliasing value. Tighten it to
the range a pool can actually issue, which is the off-by-one Tasks 4-6 are most
likely to make:

```rust
        debug_assert!(
            segment_id < self.max_segment_count(),
            "segment_id {segment_id} is outside the issuable range 0..{}",
            self.max_segment_count()
        );
```

and pin it:

```rust
    #[test]
    #[should_panic(expected = "is outside the issuable range")]
    fn pack_rejects_the_unissuable_top_segment_id() {
        // The id reserved so Location::GHOST cannot be aliased. A pool must
        // never issue it, and pack must say so rather than encode it.
        let layout = LocationLayout::new(1024 * 1024, 8).unwrap();
        layout.pack(0, layout.max_segment_count(), 0, 0);
    }
```

- [ ] **Step 3: N2 -- record the precondition on the new subtraction**

`let seg_bits = PAYLOAD_BITS - TAG_BITS - offset_bits;` cannot underflow only
because the ceiling above caps `offset_bits` at 29. Swapping the two blocks
passes every test and then panics on a 1 TiB input. State it:

```rust
        // Safe from underflow only because the ceiling above already capped
        // offset_bits at 29 (align_shift >= 3). This ordering is load-bearing.
        let seg_bits = PAYLOAD_BITS - TAG_BITS - offset_bits;
```

- [ ] **Step 4: N3 and N5 -- pin `div_ceil`, drop the magic numbers**

`div_ceil` is still unpinned: plain `/` survives every test because
`next_power_of_two` masks it. It diverges only when `align` does not divide
`segment_size` and the plain quotient is already a power of two. Add to
`derives_offset_bits_for_non_power_of_two_segment_sizes`:

```rust
        // Pins div_ceil specifically: with plain division this is 7 bits, which
        // cannot address the last 9 bytes of the segment.
        assert_eq!(LocationLayout::new(1025, 8).unwrap().offset_bits(), 8);
```

In `ghost_is_unaliasable`, replace the literal `+ 6` and `>> 3` with
`TAG_BITS` and the layout's own alignment, so the test tracks the constants it
is meant to pin:

```rust
        let unissuable = (3u64 << LocationLayout::POOL_SHIFT)
            | ((layout.max_segment_count() as u64) << (layout.offset_bits() + TAG_BITS))
            | (0x3Fu64 << layout.offset_bits())
            | ((max_offset / layout.align_bytes()) as u64);
```

- [ ] **Step 5: N7 -- pin the downcast contract**

`From<LayoutError> for io::Error` preserving the structured error is a claimed
capability the pool builders in Task 5 may rely on, and nothing pins it:

```rust
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
```

- [ ] **Step 6: Verify and prove red**

Run: `cargo test -p cache-core --lib location_layout`
Expected: PASS, 17 tests (three new; N2/N3/N5 modify existing tests).

Prove each new guard red: delete `pack`'s offset assert and confirm
`pack_rejects_an_offset_past_the_offset_field` fails; loosen the `segment_id`
assert back to `<= max_segment_count()` and confirm
`pack_rejects_the_unissuable_top_segment_id` fails; replace `div_ceil` with `/`
and confirm the non-power-of-two test fails. Restore all three.

Run: `cargo test -p cache-core --lib`, `cargo clippy -p cache-core --all-targets -- -D warnings`, `cargo fmt`

- [ ] **Step 7: Commit**

```bash
git add cache/core/src/location_layout.rs
git commit -m "test(cache-core): pin the guards Task 1b added

pack's offset range check -- the guard against silent incarnation
corruption -- could be deleted with every test still green. Now pinned,
along with the issuable-id bound, div_ceil, and the io::Error downcast.

Tightens the segment_id assert from the field width to the issuable range,
so it rejects the unissuable top id that keeps GHOST unaliasable rather
than encoding it."
```

---

### Task 2: `Metadata` carries the incarnation

Adding a field to `Metadata` deliberately breaks every struct-literal site at compile time. Do not add a `Default`.

**The compiler does NOT enumerate every site.** It catches struct literals only;
`Metadata::new(state)` calls compile clean and silently zero the tag. Six live
transition sites take that form and are handled in Task 2b, which must land
before Task 3 introduces any bump.

**Files:**
- Modify: `cache/core/src/state.rs:186-260`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `cache/core/src/state.rs`:

```rust
    #[test]
    fn test_metadata_incarnation_roundtrip() {
        for incarnation in 0u8..64 {
            let meta = Metadata {
                next: 123,
                prev: 456,
                state: State::Live,
                incarnation,
            };
            let unpacked = Metadata::unpack(meta.pack());
            assert_eq!(unpacked.incarnation, incarnation);
            assert_eq!(unpacked.next, 123);
            assert_eq!(unpacked.prev, 456);
            assert_eq!(unpacked.state, State::Live);
        }
    }

    #[test]
    fn test_metadata_incarnation_masks_to_six_bits() {
        let meta = Metadata {
            next: 0,
            prev: 0,
            state: State::Free,
            incarnation: 0xFF,
        };
        assert_eq!(Metadata::unpack(meta.pack()).incarnation, 0x3F);
    }

    #[test]
    fn test_metadata_incarnation_does_not_disturb_other_fields() {
        // The tag lives in the byte above `state`; a full-width tag must not
        // bleed into the state or chain pointers.
        let meta = Metadata {
            next: INVALID_SEGMENT_ID,
            prev: INVALID_SEGMENT_ID,
            state: State::AwaitingRelease,
            incarnation: 0x3F,
        };
        let unpacked = Metadata::unpack(meta.pack());
        assert_eq!(unpacked.state, State::AwaitingRelease);
        assert_eq!(unpacked.next, INVALID_SEGMENT_ID);
        assert_eq!(unpacked.prev, INVALID_SEGMENT_ID);
    }

    #[test]
    fn test_metadata_bump_incarnation_wraps_at_six_bits() {
        let meta = Metadata {
            next: 0,
            prev: 0,
            state: State::Locked,
            incarnation: 0x3F,
        };
        assert_eq!(meta.bump_incarnation().incarnation, 0);
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p cache-core --lib state::tests`
Expected: FAIL to compile, `struct Metadata has no field named incarnation`.

- [ ] **Step 3: Write the implementation**

In `cache/core/src/state.rs`, replace the `Metadata` doc comment, struct, and the `new`/`with_chain`/`pack`/`unpack` methods with:

```rust
/// Packed representation of segment metadata in a single AtomicU64.
///
/// Layout: `[2 unused][6 bits incarnation][8 bits state][24 prev][24 next]`
///
/// This packing allows atomic updates to state and chain pointers together,
/// which is essential for lock-free chain operations.
///
/// # Why the incarnation lives here
///
/// The incarnation tag is the low 6 bits of a per-segment counter that rides
/// inside every `Location` issued during that incarnation. It must be bumped
/// *atomically with the state transition that ends the incarnation*. Keeping it
/// in a separate atomic would leave a window where the segment is already
/// `Free` but still carries the old counter, and a thread reserving inside that
/// window would refill under the old tag -- exactly the aliasing the tag exists
/// to prevent.
///
/// This is distinct from `Segment::generation()`, a 16-bit counter bumped on
/// `Free -> Reserved` that feeds `CasToken`. The two answer different questions
/// and have different bump sites on purpose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    /// Next segment ID in the chain (24 bits, INVALID_SEGMENT_ID if none).
    pub next: u32,
    /// Previous segment ID in the chain (24 bits, INVALID_SEGMENT_ID if none).
    pub prev: u32,
    /// Current state of the segment.
    pub state: State,
    /// Incarnation tag (6 bits). See the struct docs.
    pub incarnation: u8,
}

impl Metadata {
    /// Bits of incarnation stored in the packed word.
    const INCARNATION_BITS: u32 = 6;
    const INCARNATION_MASK: u8 = (1 << Self::INCARNATION_BITS) - 1;
    const INCARNATION_SHIFT: u32 = 56;

    /// Create new metadata with the given state, no chain links, incarnation 0.
    ///
    /// Only correct at segment construction. Every *transition* must carry the
    /// incarnation forward -- use `with_state` or `bump_incarnation`.
    pub fn new(state: State) -> Self {
        Self {
            next: INVALID_SEGMENT_ID,
            prev: INVALID_SEGMENT_ID,
            state,
            incarnation: 0,
        }
    }

    /// Create metadata with state and chain pointers, incarnation 0.
    ///
    /// Only correct at segment construction; see `new`.
    pub fn with_chain(state: State, next: Option<u32>, prev: Option<u32>) -> Self {
        Self {
            next: next.unwrap_or(INVALID_SEGMENT_ID),
            prev: prev.unwrap_or(INVALID_SEGMENT_ID),
            state,
            incarnation: 0,
        }
    }

    /// This metadata with a different state, preserving the incarnation.
    ///
    /// The correct helper for a transition that does not end an incarnation.
    #[inline]
    pub fn with_state(self, state: State) -> Self {
        Self { state, ..self }
    }

    /// This metadata with the incarnation advanced by one, wrapping at 6 bits.
    ///
    /// Call this on exactly the transitions that end a *used* incarnation --
    /// `Locked -> Free` and `AwaitingRelease -> Free`. See
    /// `slice_segment.rs`'s bump-site test for why the set matters.
    #[inline]
    pub fn bump_incarnation(self) -> Self {
        Self {
            incarnation: self.incarnation.wrapping_add(1) & Self::INCARNATION_MASK,
            ..self
        }
    }

    /// Pack the metadata into a single u64 for atomic storage.
    #[inline]
    pub fn pack(self) -> u64 {
        // Mask to ensure we only use 24 bits for IDs
        let next_24 = (self.next & 0xFF_FFFF) as u64;
        let prev_24 = (self.prev & 0xFF_FFFF) as u64;
        let state_8 = self.state as u64;
        let inc = ((self.incarnation & Self::INCARNATION_MASK) as u64) << Self::INCARNATION_SHIFT;

        // Pack: [2 unused][6 incarnation][8 state][24 prev][24 next]
        inc | (state_8 << 48) | (prev_24 << 24) | next_24
    }

    /// Unpack metadata from a u64.
    #[inline]
    pub fn unpack(packed: u64) -> Self {
        let state_val = ((packed >> 48) & 0xFF) as u8;
        let state = State::from_u8(state_val);

        Self {
            next: (packed & 0xFF_FFFF) as u32,
            prev: ((packed >> 24) & 0xFF_FFFF) as u32,
            state,
            incarnation: ((packed >> Self::INCARNATION_SHIFT) as u8) & Self::INCARNATION_MASK,
        }
    }
```

Leave the rest of the `impl` (`next_id`, `prev_id`) unchanged.

- [ ] **Step 4: Fix every struct-literal site the compiler now rejects**

Run: `cargo build -p cache-core 2>&1 | grep -A2 "missing field"`

Each site must preserve the incarnation rather than reset it. In
`cache/core/src/state.rs`'s own test module (lines ~323, ~339, ~357) add
`incarnation: 0`. For the six sites in `cache/core/src/slice_segment.rs`, one
in `cache/core/src/item.rs:813`, and one in
`cache/core/src/disk/disk_segment_meta.rs:414`, replace the struct literal with
the `with_state` helper built from the metadata that site already loaded. For
example at `slice_segment.rs:862` (`try_reserve`):

```rust
        // Preserve the incarnation: reserving does not end one.
        let new_meta = current_meta
            .with_state(State::Reserved)
            .with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID);
```

Add the small helper this needs to `Metadata` in `cache/core/src/state.rs`:

```rust
    /// This metadata with different chain pointers, preserving everything else.
    #[inline]
    pub fn with_chain_ids(self, next: u32, prev: u32) -> Self {
        Self { next, prev, ..self }
    }
```

The one site that legitimately starts at zero is `SliceSegment::new`
(`slice_segment.rs:159`), which constructs a brand-new segment: leave it as
`Metadata::new(State::Free)`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p cache-core --lib state::tests`
Expected: PASS, including the four new tests.

Run: `cargo test -p cache-core --lib`
Expected: PASS, no regressions.

- [ ] **Step 6: Commit**

```bash
git add cache/core/src/state.rs cache/core/src/slice_segment.rs cache/core/src/item.rs cache/core/src/disk/disk_segment_meta.rs
git commit -m "feat(cache-core): carry a 6-bit incarnation in Metadata

Puts the tag in the packed word's unused byte so bumping it is atomic with
the state transition. Adding the field deliberately breaks every struct
literal, forcing each transition site to say whether it preserves or bumps;
with_state and with_chain_ids are the preserving forms."
```

---

### Task 2b: the six transition sites the compiler could not catch

Task 2's premise was incomplete. Adding the field breaks struct *literals*, but
`Metadata::new(state)` compiles clean and returns incarnation 0, so six live
transitions silently reset the tag.

This is harmless **only** while nothing bumps. It becomes a real aliasing bug the
moment Task 3 lands, and `disk_segment_meta.rs`'s `try_reserve` is the worst of
them: a `Free -> Reserved` reset would wipe a tag that was just advanced by the
release which freed the segment.

Every one of these sites already loads the metadata it needs on the line above,
so each is a one-line change -- except `reset`, which stores unconditionally and
needs a load added, exactly as `force_free` did in Task 2.

**Files:** Modify `cache/core/src/cache_trait.rs`, `cache/core/src/disk/disk_segment_meta.rs`

- [ ] **Step 1: Write the failing test**

The `SliceSegment` twin of this test already exists from Task 2
(`test_segment_transitions_preserve_incarnation`). The disk path has no
equivalent, which is why these six went unnoticed. Add to the `tests` module in
`cache/core/src/disk/disk_segment_meta.rs`, following that test's shape and
whatever segment-construction helper the neighbouring tests there already use:

```rust
    /// Every disk-segment transition must carry the incarnation forward.
    ///
    /// `Metadata::new` zeroes it and compiles clean, so nothing but this test
    /// stands between a refactor and a silently reset tag -- which resurrects
    /// stale locations rather than failing loudly.
    #[test]
    fn test_disk_segment_transitions_preserve_incarnation() {
        let (meta, _pool) = make_disk_segment_meta(4096);

        // Seed a non-zero tag directly, the way a prior incarnation's release
        // would have left it.
        let seeded = Metadata::unpack(meta.metadata.load(Ordering::Acquire))
            .with_state(State::Free);
        meta.metadata
            .store(Metadata { incarnation: 42, ..seeded }.pack(), Ordering::Release);

        assert!(meta.try_reserve());
        assert_eq!(meta.incarnation_for_test(), 42, "try_reserve reset the tag");

        assert!(meta.try_release());
        assert_eq!(meta.incarnation_for_test(), 42, "try_release reset the tag");

        assert!(meta.try_reserve());
        assert!(meta.cas_metadata(State::Reserved, State::AwaitingRelease, None, None));
        assert!(meta.release_condemned());
        assert_eq!(
            meta.incarnation_for_test(),
            42,
            "release_condemned reset the tag"
        );

        assert!(meta.try_reserve());
        meta.reset();
        assert_eq!(meta.incarnation_for_test(), 42, "reset cleared the tag");
    }
```

`incarnation_for_test` is a private test helper reading
`Metadata::unpack(self.metadata.load(Ordering::Acquire)).incarnation` -- Task 3
adds the real `Segment::incarnation()` trait method, and this task must not
anticipate it.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p cache-core --lib disk_segment_meta::tests::test_disk_segment_transitions_preserve_incarnation`
Expected: FAIL with `try_reserve reset the tag`, left `0`, right `42`.

- [ ] **Step 3: Fix all six sites**

Each replaces `Metadata::new(State::X)` with `.with_state(State::X)` on the
metadata already in hand:

| File | Line | Site | Transition |
|---|---|---|---|
| `cache_trait.rs` | 231 | guard `drop` | `AwaitingRelease -> Free` |
| `disk/disk_segment_meta.rs` | 365 | `try_reserve` | `Free -> Reserved` |
| `disk/disk_segment_meta.rs` | 388 | `try_release` | `Reserved\|Linking\|Locked -> Free` |
| `disk/disk_segment_meta.rs` | 437 | `release_condemned` | `AwaitingRelease -> Free` |
| `disk/disk_segment_meta.rs` | 767 | `reset` | `* -> Free` |

That is five; the sixth is whichever remaining `Metadata::new` call in a
non-test transition path the compiler-independent audit in Step 4 turns up.

For example, `cache_trait.rs:231` already has `meta` from the line above:

```rust
            if meta.state == State::AwaitingRelease {
                // Preserve the incarnation: freeing a condemned segment is
                // Task 3's business, and zeroing here would resurrect stale
                // locations naming this segment.
                let new_meta = meta.with_state(State::Free);
```

and `disk_segment_meta.rs:767` `reset` needs the load added:

```rust
        let current = Metadata::unpack(self.metadata.load(Ordering::Acquire));
        let new_meta = current.with_state(State::Free);
        self.metadata.store(new_meta.pack(), Ordering::Release);
```

- [ ] **Step 4: Audit for any site the table missed**

The compiler cannot find these, so grep and classify every one by hand:

```bash
grep -rn "Metadata::new\|Metadata::with_chain" --include='*.rs' cache/
```

For each hit, decide: is it constructing a brand-new segment (correct to zero),
or seeding a test's metadata word (correct to zero), or a state transition on a
live segment (**must preserve**)? Report the full classified list. There are 23
`Metadata::new` and 2 `Metadata::with_chain` call sites; most are test setup.

- [ ] **Step 5: Sharpen the doc comment so the next reader is warned**

`Metadata::new`'s doc currently says transitions must carry the incarnation
forward, while six call sites did the opposite. Make the hazard explicit:

```rust
    /// Create metadata for a brand-new segment: no chain links, incarnation 0.
    ///
    /// # This is not a transition
    ///
    /// Zeroing the incarnation is correct **only** when constructing a segment
    /// that has never been used, or when seeding a test's metadata word.
    /// Calling this on a live segment silently resets its tag, which
    /// resurrects every stale location naming it -- and unlike a struct
    /// literal, it compiles clean. For a state change use `with_state`.
```

- [ ] **Step 6: Verify and prove red**

Run: `cargo test -p cache-core --lib`
Expected: PASS, 446 (445 plus this test).

Re-break one site -- revert `disk_segment_meta.rs:365` `try_reserve` to
`Metadata::new(State::Reserved)` -- and confirm the new test fails with
`try_reserve reset the tag`. Restore it and quote the failure text.

- [ ] **Step 7: Commit**

```bash
git add cache/core/src/cache_trait.rs cache/core/src/disk/disk_segment_meta.rs
git commit -m "fix(cache-core): preserve the incarnation across disk-segment transitions

Task 2 assumed adding the field made the compiler enumerate every site. It
enumerates struct literals only -- Metadata::new(state) compiles clean and
returns incarnation 0, and six live transitions took that form.

No effect yet, since nothing bumps until Task 3. Landing it first so the
bump cannot be introduced on top of a silent reset."
```

---

### Task 3: Bump sites, and a transition table that pins them

> **Rewritten 2026-09-06.** The original version keyed the bump on
> `Locked -> Free`, a transition that does not occur on crucible's main eviction
> path. Implementing it literally would have left the tag at 0 forever -- the
> feature inert, every test green. See the corrected table in the design doc.

The real recycle path is:

```text
Sealed -> Draining -> Locked -> Reserved      (cas_metadata, in the layers)
                                  |
                                  v
                      pool.release() -> try_release  =>  Reserved -> Free
```

So the bump keys on **leaving `Locked`**, not on a destination state. `Locked` is
reached only after a drain, so any exit from it ends a used incarnation
whichever path took it. Keying on the source state rather than enumerating
destination pairs is what makes this robust to the paths not yet enumerated.

**Files:**
- Modify: `cache/core/src/segment.rs` (trait method)
- Modify: `cache/core/src/slice_segment.rs` (`cas_metadata`, `try_release`, `release_condemned`, `force_free`)
- Modify: `cache/core/src/disk/disk_segment_meta.rs` (same four)
- Modify: `cache/core/src/disk/file_segment.rs` (delegating trait impl)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `cache/core/src/slice_segment.rs`. Adapt to
whatever segment-construction helper the neighbouring tests use.

```rust
    /// The incarnation advances on exactly the transitions that end a *used*
    /// incarnation, and on no others.
    ///
    /// This is not a tidiness test, and the table is not the obvious one. The
    /// eviction path recycles a drained segment as `Locked -> Reserved` and
    /// only then releases it `Reserved -> Free`, so keying the bump on
    /// `Locked -> Free` -- which never happens -- leaves the tag at 0 forever
    /// with the whole suite still green. Keying on *leaving Locked* is what
    /// makes it fire.
    ///
    /// The exclusions matter just as much. `MemoryPool::release_segment`,
    /// `MemoryPool::release` and `FilePool::release` all return never-used
    /// segments through `try_release`. If the bump fired there too, a burst of
    /// reserve/release with no item lifecycle at all would drain the 6-bit
    /// tag's collision hardness for free.
    #[test]
    fn test_incarnation_bumps_on_exactly_the_used_incarnation_transitions() {
        let (segment, _mem) = make_segment(4096);
        assert_eq!(segment.incarnation(), 0);

        // Free -> Reserved starts an incarnation. Must NOT bump.
        assert!(segment.try_reserve());
        assert_eq!(segment.incarnation(), 0, "try_reserve must not bump");

        // Reserved -> Free: reserved but never used. Must NOT bump.
        assert!(segment.try_release());
        assert_eq!(
            segment.incarnation(),
            0,
            "Reserved -> Free is a never-used release and must not bump"
        );

        // Linking -> Free: lost a chain-extension election. Must NOT bump.
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Linking, None, None));
        assert!(segment.try_release());
        assert_eq!(
            segment.incarnation(),
            0,
            "Linking -> Free is a lost election and must not bump"
        );

        // Locked -> Reserved: THE REAL RECYCLE PATH. Must bump.
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Locked, None, None));
        assert!(segment.cas_metadata(State::Locked, State::Reserved, None, None));
        assert_eq!(
            segment.incarnation(),
            1,
            "Locked -> Reserved is how a drained segment is recycled and must bump"
        );

        // The Reserved -> Free that follows must not bump again -- one
        // incarnation ending is one bump, not two.
        assert!(segment.try_release());
        assert_eq!(
            segment.incarnation(),
            1,
            "the release following a recycle must not double-bump"
        );

        // Locked -> Free: the other way out of Locked. Must bump.
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Locked, None, None));
        assert!(segment.try_release());
        assert_eq!(
            segment.incarnation(),
            2,
            "Locked -> Free also ends a used incarnation and must bump"
        );

        // AwaitingRelease -> Free: condemned. Must bump.
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::AwaitingRelease, None, None));
        assert!(segment.release_condemned());
        assert_eq!(
            segment.incarnation(),
            3,
            "AwaitingRelease -> Free ends a used incarnation and must bump"
        );

        // Sealed -> Draining and Draining -> Locked are mid-life. Must NOT bump.
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Sealed, None, None));
        assert!(segment.cas_metadata(State::Sealed, State::Draining, None, None));
        assert!(segment.cas_metadata(State::Draining, State::Locked, None, None));
        assert_eq!(
            segment.incarnation(),
            3,
            "transitions into Locked are mid-life and must not bump"
        );
    }

    #[test]
    fn test_force_free_bumps_the_incarnation() {
        // Flush recycles every segment at once, so locations issued before it
        // must stop resolving: it ends an incarnation like any other.
        let (segment, _mem) = make_segment(4096);
        assert!(segment.try_reserve());
        let before = segment.incarnation();
        segment.force_free();
        assert_eq!(segment.incarnation(), (before + 1) & 0x3F);
        assert_eq!(segment.state(), State::Free);
    }

    #[test]
    fn test_incarnation_wraps_at_64_and_stays_in_range() {
        let (segment, _mem) = make_segment(4096);
        for _ in 0..70 {
            assert!(segment.try_reserve());
            assert!(segment.cas_metadata(State::Reserved, State::Locked, None, None));
            assert!(segment.cas_metadata(State::Locked, State::Reserved, None, None));
            assert!(segment.try_release());
            assert!(segment.incarnation() < 64, "tag must stay within 6 bits");
        }
        assert_eq!(segment.incarnation(), 70 % 64);
    }
```

Add the disk twin of the first test to `disk_segment_meta.rs`, using the
`segment_with_item` helper the tests there already use.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p cache-core --lib test_incarnation_bumps_on_exactly`
Expected: FAIL to compile, `no method named incarnation found`.

- [ ] **Step 3: Add the trait method**

In `cache/core/src/segment.rs`, in `pub trait Segment`, after
`fn increment_generation(&self);`:

```rust
    /// Get the incarnation tag (6 bits) currently stamped on this segment.
    ///
    /// This is the value carried inside every `Location` issued while the
    /// segment holds it. A location whose tag differs names a previous
    /// incarnation and must not be resolved.
    ///
    /// Distinct from `generation()`: that is a 16-bit counter bumped on
    /// `Free -> Reserved` and consumed by `CasToken`. This one is 6 bits and
    /// advances only when a *used* incarnation ends.
    fn incarnation(&self) -> u8;
```

Implement on `SliceSegment` and `DiskSegmentMeta` as
`Metadata::unpack(self.metadata.load(Ordering::Acquire)).incarnation`, and on
`FileSegment` by delegating to `self.inner.incarnation()`. The two `#[cfg(test)]`
`incarnation_for_test` helpers Task 2b added are now redundant -- delete them and
point their callers at the trait method.

- [ ] **Step 4: Bump on leaving `Locked`**

In `cas_metadata` (both `slice_segment.rs` and `disk_segment_meta.rs`), after
building `new_meta` from `current_meta`:

```rust
        // Leaving Locked ends a used incarnation: the segment has been drained
        // and cleared, and the layers recycle it as Locked -> Reserved before
        // releasing it. Bumping here, inside the same CAS that publishes the
        // new state, is what makes a stale location stop resolving -- and no
        // thread can observe the new state with the old tag.
        let new_meta = if current_meta.state == State::Locked {
            new_meta.bump_incarnation()
        } else {
            new_meta
        };
```

In `try_release` (both files), the same condition, since `Locked -> Free` is the
other way out:

```rust
            let ends_incarnation = current_meta.state == State::Locked;
```

and apply `.bump_incarnation()` to `new_meta` when it holds. `Reserved` and
`Linking` must not bump.

In `release_condemned` (both files), always bump -- `AwaitingRelease -> Free` is
unconditionally the end of a used incarnation.

In `force_free` (and `DiskSegmentMeta::reset`), always bump.

- [ ] **Step 5: Update the Task 2b assertions this invalidates**

`test_segment_transitions_preserve_incarnation` and
`test_disk_segment_transitions_preserve_incarnation` currently assert that
`release_condemned` *preserves*. It now bumps. Change those assertions to expect
`43`, and add a comment noting the test pins preservation for the
non-incarnation-ending transitions only. Do not delete the tests -- they still
guard the ten sites that must preserve.

- [ ] **Step 6: Verify and prove red**

Run: `cargo test -p cache-core --lib`
Expected: PASS.

Prove each arm red individually and quote the failure text:
- Change the `cas_metadata` condition to `false`: `Locked -> Reserved is how a drained segment is recycled and must bump` must fail.
- Change it to bump unconditionally: `transitions into Locked are mid-life and must not bump` must fail.
- Change `try_release`'s condition to `true`: `Reserved -> Free is a never-used release and must not bump` must fail.
- Remove `release_condemned`'s bump: its assertion must fail.

That third one is the cache-rs#78 prerequisite and the reason the exclusions are
tested at all -- a bump that fires on never-used releases drains the tag's
hardness at a rate decoupled from segment lifecycles.

- [ ] **Step 7: Commit**

```bash
git add cache/core/src/segment.rs cache/core/src/slice_segment.rs cache/core/src/disk/
git commit -m "feat(cache-core): bump the incarnation when a used incarnation ends

Keys on leaving Locked rather than on a destination state. The layers
recycle a drained segment as Locked -> Reserved and only then release it
Reserved -> Free, so the design's original Locked -> Free never fires --
that table would have left the tag at 0 forever with the suite green.

Reserved|Linking -> Free still must not bump: release_segment and lost
chain elections would otherwise drain a 6-bit tag at a rate decoupled from
segment lifecycles. Pinned as a full transition table, every arm proven red."
```

### Task 3b: the guard-drop path that actually frees condemned segments

Task 3 made `release_condemned` bump. It missed the two sites that perform the
identical `AwaitingRelease -> Free` transition from a guard's `Drop` -- and those
are the **main** path, not the exception:

```rust
// fifo_layer.rs:171
// Draining -> AwaitingRelease (last reader's ValueRef::drop will free it)
segment.cas_metadata(State::Draining, State::AwaitingRelease, None, None);
// Race fix: if the last reader dropped between our ref_count check and the
// CAS above, the segment is now AwaitingRelease with ref_count == 0 and
// nobody will free it. Reclaim it now.
if segment.ref_count() == 0 && segment.release_condemned() { return true; }
```

`release_condemned` here is the **fallback** for a narrow race. Whenever a reader
is actually outstanding -- `ref_count() > 0`, which is the branch this whole code
path exists for -- the segment is freed by the last guard drop instead.

That is precisely the case a stale `Location` is most likely to exist for: a
reader holding a reference across the recycle is the scenario the incarnation tag
was built to defend against. As Task 3 stands, those segments keep their tag and
the stale location still resolves.

This is the same error as the `Locked -> Free` one, in the same shape: the
fallback transition was tagged and the dominant one was not.

**Files:** Modify `cache/core/src/item.rs`, `cache/core/src/cache_trait.rs`

- [ ] **Step 1: Update the two drop tests to expect a bump**

`test_item_guard_drop_preserves_incarnation` and
`test_value_ref_drop_preserves_incarnation` (added in Task 2b) currently assert
the tag is preserved at these sites. Invert them: seed 42, drop the guard, assert
the tag is now **43** and the state is `Free` and the segment reached the free
queue. Rename both from `_preserves_` to `_bumps_the_incarnation`, and keep the
free-queue assertion -- it is what proves the transition actually ran rather than
the CAS silently failing.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p cache-core --lib drop_bumps_the_incarnation`
Expected: FAIL, tag 42 where 43 was expected, at both sites.

- [ ] **Step 3: Bump at both sites**

`item.rs:813` (`BasicItemGuard::drop`) and `cache_trait.rs:231`
(`ValueRef::drop`), replacing the "Task 3 decides" comments:

```rust
                // AwaitingRelease -> Free ends a used incarnation, so the tag
                // advances in the same CAS that publishes Free.
                //
                // This is the *main* condemned-free path, not an edge case: the
                // layers condemn a segment with readers outstanding and leave
                // the last guard drop to free it, calling `release_condemned`
                // only as a fallback when the last reader vanished during the
                // condemn window. A reader holding a location across this
                // recycle is exactly what the tag defends against.
                let new_meta = meta
                    .with_state(State::Free)
                    .with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID)
                    .bump_incarnation();
```

Keep the `with_chain_ids` call. Dropping it would return a freed segment to the
free queue still linked to its neighbours -- the defect caught in the Task 2b
review.

- [ ] **Step 4: Verify and prove red**

Run: `cargo test -p cache-core --lib`
Expected: PASS, 452.

Remove each bump in turn and confirm the matching test reddens. Quote both.

- [ ] **Step 5: Check for any remaining unbumped end-of-incarnation**

Both misses so far were a dominant transition overlooked beside a tagged
fallback. Grep every write that publishes `State::Free` or moves out of
`State::Locked` and confirm each either bumps or is a documented never-used
release:

```bash
grep -rn "State::Free\|State::Locked" --include='*.rs' cache/core/src | grep -v "^.*test"
```

Report the classified list. A site that ends a used incarnation without bumping
is the failure this whole task exists to prevent.

- [ ] **Step 6: Commit**

```bash
git add cache/core/src/item.rs cache/core/src/cache_trait.rs
git commit -m "fix(cache-core): bump the incarnation on the guard-drop free path

Task 3 tagged release_condemned, which is the fallback. The layers condemn
a segment with readers outstanding and leave the last guard drop to free
it, so BasicItemGuard::drop and ValueRef::drop are the main path -- and a
reader holding a location across that recycle is exactly the case the tag
defends against.

Same shape as the Locked -> Free error: the fallback was tagged and the
dominant transition was not."
```

### Task 3c: key the bump on *leaving* Locked, not on being Locked

The Task 3/3b review approved the table but found `cas_metadata` implementing a
broader rule than the spec states.

The design says the bump fires on **leaving** `Locked`. The code says
`if current_meta.state == State::Locked` -- source state alone. Those differ for
a transition that stays in `Locked`, i.e. a chain-pointer-only rewrite.

Eleven call sites use the identity form
`seg.cas_metadata(seg.state(), seg.state(), Some(x), None)` --
`organization/fifo.rs:265,333,348,358` and
`organization/ttl_buckets.rs:438,503,514,611,623,749,763`. If any ever ran
against a `Locked` neighbour, the tag would advance **mid-life**. The reviewer
demonstrated it in a scratch copy:

```text
PROBE-IDCAS Locked identity CAS: 0 -> 1 (state Locked)
PROBE-IDCAS after recycle: 2          # two bumps, one lifetime
```

**It is currently unreachable**, and the reviewer traced why: every unlink
(`fifo::pop_head`/`try_remove`, `TtlBucket::evict_head_segment`/`remove_segment`)
fixes its neighbours' pointers *under `chain_mutex`* before the layer drives
`Draining -> Locked` *outside* it, so a chain member never names a `Locked`
segment.

That invariant is stated nowhere and tested nowhere. It is also exactly the
"tag advancing at a rate decoupled from segment lifecycles" failure the
exclusions exist to prevent -- and it stops being merely latent once Task 7's
read-side check lands, because `process_evicted_segment`'s drain loop builds
`ItemLocation`s *while* the segment is `Locked`.

Fix the rule rather than rely on an unstated invariant.

**Files:** Modify `cache/core/src/slice_segment.rs`, `cache/core/src/disk/disk_segment_meta.rs`

- [ ] **Step 1: Add the failing arm to the transition table test**

In `test_incarnation_bumps_on_exactly_the_used_incarnation_transitions` (and its
disk twin), after the `Locked -> Reserved` arm:

```rust
        // A chain-pointer-only rewrite that stays in Locked is mid-life, not
        // the end of an incarnation. Eleven identity CASes in organization/
        // take this shape; if one ever ran against a Locked neighbour, keying
        // the bump on the source state alone would advance the tag twice in
        // one lifetime.
        assert!(segment.try_reserve());
        assert!(segment.cas_metadata(State::Reserved, State::Locked, None, None));
        let before = segment.incarnation();
        assert!(segment.cas_metadata(State::Locked, State::Locked, Some(7), None));
        assert_eq!(
            segment.incarnation(),
            before,
            "a Locked -> Locked chain rewrite must not bump"
        );
        assert_eq!(segment.next(), Some(7), "the chain pointer must still be written");
        assert!(segment.try_release());
        assert_eq!(
            segment.incarnation(),
            before + 1,
            "leaving Locked must still bump exactly once"
        );
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p cache-core --lib test_incarnation_bumps_on_exactly`
Expected: FAIL, `a Locked -> Locked chain rewrite must not bump`.

- [ ] **Step 3: Tighten the condition in both files**

```rust
        // *Leaving* Locked ends a used incarnation -- not merely being Locked.
        // A chain-pointer rewrite that stays in Locked is mid-life; bumping
        // there would advance the tag twice in one lifetime.
        let new_meta = if current_meta.state == State::Locked && new_state != State::Locked {
            new_meta.bump_incarnation()
        } else {
            new_meta
        };
```

- [ ] **Step 4: Correct the two docs that rest on a dead function**

`MemoryPool::release_segment`'s doc claims it is "called by guard Drop when a
segment in AwaitingRelease state has its last reader drop". It calls
`try_release`, which **panics** on `AwaitingRelease`, and it has no production
callers. Both the `try_release` comment and `state.rs`'s `bump_incarnation` doc
cite it as the justification for the `Reserved|Linking -> Free` exclusion.

The classification is right but the citation is wrong. Point both at the live
callers -- `MemoryPool::release` (`memory_pool.rs:270`), `FilePool::release`
(`file_pool.rs:322`), `IoUringPool::release` (`io_uring_pool.rs:222`) -- and fix
`release_segment`'s own doc to say what it actually does.

- [ ] **Step 5: Note the two asymmetries the review flagged**

Neither is a defect; both now carry semantics they did not before, so record
them where a reader will find them:

- `force_free` bumps through a plain load-modify-`store`, not a CAS. Racing a
  concurrent `BasicItemGuard::drop` CAS could drop or duplicate a bump. Add this
  to its existing `# Safety` note, which already requires flush-only use.
- `Segment::reset()` is asymmetric: `DiskSegmentMeta::reset` publishes `Free`
  and bumps, `SliceSegment::reset` touches only statistics. Say so on the trait
  method, since a bump now hangs off one side of it.

- [ ] **Step 6: Verify and prove red**

Run: `cargo test -p cache-core --lib`, expect 452.

Prove the new arm red by reverting the condition to `state == State::Locked`,
and confirm the rest of the table still passes (the tightening must not disturb
`Locked -> Reserved` or `Locked -> Free`). Quote the failure.

- [ ] **Step 7: Commit**

```bash
git add cache/core/src/slice_segment.rs cache/core/src/disk/disk_segment_meta.rs cache/core/src/memory_pool.rs cache/core/src/state.rs cache/core/src/segment.rs
git commit -m "fix(cache-core): bump on leaving Locked, not on being Locked

cas_metadata keyed on the source state alone, so a chain-pointer-only
rewrite that stays in Locked would bump mid-life -- two bumps in one
lifetime, halving the tag space the collision argument depends on.

Unreachable today: unlinks fix neighbour pointers under chain_mutex before
the layer drives Draining -> Locked outside it, so a chain member never
names a Locked segment. That invariant was unstated and untested, and it
stops being latent once the read-side check lands, since the drain loop
builds locations while the segment is Locked. Fix the rule rather than
depend on it."
```

### Task 4: `ItemLocation` becomes layout-aware

Accessors take the layout rather than `ItemLocation` storing one: the type stays
8 bytes on a hot path that passes it by value, and every call site that decodes
already holds the pool (they all guard on `location.pool_id() != self.pool.pool_id()`
first).

**Files:**
- Modify: `cache/core/src/item_location.rs:26-130`

- [ ] **Step 1: Write the failing test**

Replace the existing bit-packing tests in `cache/core/src/item_location.rs`'s
`tests` module with:

```rust
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p cache-core --lib item_location`
Expected: FAIL to compile, `this function takes 3 arguments but 5 arguments were supplied`.

- [ ] **Step 3: Write the implementation**

In `cache/core/src/item_location.rs`, replace the `ItemLocation` doc comment and
the whole `impl ItemLocation` block (lines ~11-130) with:

```rust
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
    #[inline]
    pub fn new(
        layout: &LocationLayout,
        pool_id: u8,
        segment_id: u32,
        incarnation: u8,
        offset: u32,
    ) -> Self {
        Self(Location::new(
            layout.pack(pool_id, segment_id, incarnation, offset),
        ))
    }

    /// Create from an opaque `Location`.
    #[inline]
    pub fn from_location(location: Location) -> Self {
        Self(location)
    }

    /// Convert to an opaque `Location`.
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
        ((self.0.as_raw() >> LocationLayout::POOL_SHIFT) & 0b11) as u8
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
            ((raw >> LocationLayout::POOL_SHIFT) & 0b11) as u8,
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
```

Delete the now-stale `POOL_SHIFT`/`SEG_SHIFT`/`OFFSET_MASK`/`MAX_SEGMENT_ID`/
`MAX_OFFSET`/`OFFSET_ALIGN` constants. `Debug` and `Display` can no longer
decode without a layout: reduce both to printing `GHOST` or the raw value:

```rust
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
```

Update `test_display` and `test_debug` in the same module to match, and delete
`test_max_values`, `test_offset_alignment`, `test_invalid_pool_id`,
`test_unaligned_offset` and `test_bit_packing` -- Task 1's property test now
covers all of that against the layout, which is where the invariants live.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p cache-core --lib item_location`
Expected: PASS. Note that `cache-core` as a whole will not build yet -- Task 5
and Task 6 fix the call sites.

- [ ] **Step 5: Commit**

```bash
git add cache/core/src/item_location.rs
git commit -m "feat(cache-core): make ItemLocation layout-aware

Accessors take the layout rather than the type storing one, so ItemLocation
stays 8 bytes on a path that passes it by value. Every decoding call site
already holds its pool. pool_id keeps a fixed position so it can be read
before the layout is known."
```

---

### Task 5: Pools own a layout

**Files:**
- Modify: `cache/core/src/pool.rs` (add `RamPool::layout()`)
- Modify: `cache/core/src/memory_pool.rs:340-505`
- Modify: `cache/core/src/disk/file_pool.rs`, `cache/core/src/disk/io_uring_pool.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `cache/core/src/memory_pool.rs`:

```rust
    #[test]
    fn test_pool_exposes_a_layout_matching_its_segment_size() {
        let pool = MemoryPoolBuilder::new(0)
            .heap_size(4 * 1024 * 1024)
            .segment_size(1024 * 1024)
            .build()
            .unwrap();
        assert_eq!(pool.layout().offset_bits(), 17);
        assert_eq!(pool.layout().align_bytes(), 8);
    }

    #[test]
    fn test_pool_rejects_more_segments_than_the_layout_addresses() {
        // 128 MB segments leave 12 segment bits (4095 issuable ids). Ask for
        // more and construction must fail by name rather than alias silently.
        let err = MemoryPoolBuilder::new(0)
            .heap_size(5000 * 128 * 1024 * 1024)
            .segment_size(128 * 1024 * 1024)
            .build()
            .unwrap_err();
        assert!(
            err.to_string().contains("addresses at most"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_pool_rejects_alignment_below_item_alignment() {
        let err = MemoryPoolBuilder::new(0)
            .heap_size(4 * 1024 * 1024)
            .segment_size(1024 * 1024)
            .align_bytes(4)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("at least 8"), "unexpected: {err}");
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p cache-core --lib memory_pool::tests::test_pool_`
Expected: FAIL to compile, `no method named layout`.

- [ ] **Step 3: Write the implementation**

In `cache/core/src/pool.rs`, add to `pub trait RamPool`, after `fn segment_size`:

```rust
    /// Get this pool's location layout.
    ///
    /// Locations naming this pool must be packed and unpacked with it; pools
    /// with different segment sizes or alignment factors have different splits.
    fn layout(&self) -> &crate::location_layout::LocationLayout;
```

In `cache/core/src/memory_pool.rs`, add `align_bytes: usize` to both
`MemoryPool` and `MemoryPoolBuilder`, defaulting the builder's to `8` alongside
`segment_size: 1024 * 1024`, plus a `layout: LocationLayout` field on
`MemoryPool` and this builder method next to `segment_size`:

```rust
    /// Set the offset alignment factor in bytes.
    ///
    /// Locations store `offset / align_bytes`, so this is the only lever on a
    /// pool's addressable capacity: `2^36 * align_bytes`. Memory pools default
    /// to 8, matching the item alignment, because a minimal item is 8 bytes and
    /// coarser alignment would round small items up. Must be a power of two, at
    /// least 8, and no larger than `segment_size`.
    pub fn align_bytes(mut self, align: usize) -> Self {
        self.align_bytes = align;
        self
    }
```

In `MemoryPoolBuilder::build`, immediately after the existing `num_segments == 0`
check:

```rust
        let layout = LocationLayout::new(self.segment_size, self.align_bytes)?;
        layout.validate_segment_count(num_segments)?;
```

`LayoutError` converts into `std::io::Error` via the `From` impl from Task 1, so
`?` works against the existing `Result<MemoryPool, std::io::Error>`. Add
`layout` and `align_bytes` to the returned `MemoryPool { .. }` literal, and
implement the trait method:

```rust
    fn layout(&self) -> &LocationLayout {
        &self.layout
    }
```

Apply the same three changes to `FilePool` and `IoUringPool`, with two
differences: their builders default `align_bytes` to **512**, and their `build`
adds one further check after the layout is built:

```rust
        // Aligning coarser than the I/O block buys nothing on the read path:
        // `item_disk_range` already rounds reads down to a block boundary.
        if layout.align_bytes() as usize > self.block_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "align_bytes ({}) must not exceed block_size ({})",
                    layout.align_bytes(),
                    self.block_size
                ),
            ));
        }
```

Document the 512 default on the disk builders' `align_bytes` method:

```rust
    /// Set the offset alignment factor in bytes.
    ///
    /// Disk pools default to 512 (a sector) rather than 8. The justification is
    /// the read path, not capacity: `item_disk_range` already rounds reads down
    /// to a `block_size` boundary, so an item straddling a block costs an extra
    /// block read today. Sector alignment cuts that, wastes 256 bytes per item
    /// on average, and yields 32 TiB per pool as a side effect.
    pub fn align_bytes(mut self, align: usize) -> Self {
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p cache-core --lib memory_pool::tests`
Expected: PASS, including the three new tests.

- [ ] **Step 5: Commit**

```bash
git add cache/core/src/pool.rs cache/core/src/memory_pool.rs cache/core/src/disk/file_pool.rs cache/core/src/disk/io_uring_pool.rs
git commit -m "feat(cache-core): pools own a LocationLayout and validate capacity

Construction now fails by name when a config needs more segments than the
layout addresses, instead of aliasing them silently. align_bytes is a per-pool
knob defaulting to 8 for memory and 512 for disk."
```

---

### Task 6: Stamp the tag at every publish site

This task is compiler-driven: Tasks 4 and 5 make every stale call site a build
error. Work through them until `cargo build -p cache-core` is clean.

**Files:**
- Modify: `cache/core/src/layer/fifo_layer.rs` (7 sites), `cache/core/src/layer/ttl_layer.rs` (~10 sites), `cache/core/src/disk/disk_layer.rs` (4 sites)

- [ ] **Step 1: List the sites**

Run: `cargo build -p cache-core 2>&1 | grep -E "^error" | sort | uniq -c`

- [ ] **Step 2: Fix each publish site**

Every site already holds the `segment` it just appended to. The tag must come
from *that* reference, so the location cannot be stamped with an incarnation the
item was not written into. For example at `fifo_layer.rs:585`:

```rust
                if let Some(offset) = segment.append_item_with_ttl(key, value, optional, expire_at)
                {
                    return Ok(ItemLocation::new(
                        self.pool.layout(),
                        self.pool.pool_id(),
                        segment_id,
                        segment.incarnation(),
                        offset,
                    ));
                }
```

and at `fifo_layer.rs:723`:

```rust
                if let Some((offset, item_size, value_ptr)) =
                    segment.begin_append_with_ttl(key, value_len, optional, expire_at)
                {
                    let location = ItemLocation::new(
                        self.pool.layout(),
                        self.pool.pool_id(),
                        segment_id,
                        segment.incarnation(),
                        offset,
                    );
                    return Ok((location, value_ptr, item_size));
                }
```

- [ ] **Step 3: Fix each decode site**

Calls like `location.offset()` and `location.segment_id()` now need the layout.
Every one of these sits after a `location.pool_id() != self.pool.pool_id()`
guard, so `self.pool.layout()` is the correct layout. Prefer `unpack` where two
or more fields are read:

```rust
        let (_, segment_id, incarnation, offset) = location.unpack(self.pool.layout());
```

- [ ] **Step 4: Build clean**

Run: `cargo build -p cache-core`
Expected: no errors.

Run: `cargo test -p cache-core`
Expected: PASS. Tests that construct locations by hand (for example
`fifo_layer.rs:1051`'s `wrong_location`) need the layout and an incarnation
argument; pass the real segment's incarnation so those tests keep testing what
they were testing (a wrong *pool*), not accidentally testing the new tag.

- [ ] **Step 5: Commit**

```bash
git add cache/core/src/layer/ cache/core/src/disk/
git commit -m "feat(cache-core): stamp the incarnation when publishing a location

Each publish site reads the tag from the same segment reference the append
used, so a location can never carry an incarnation its item was not written
into."
```

---

### Task 7: The check, and a test that recycles

**Files:**
- Modify: `cache/core/src/cache.rs:1559-1670` (`CacheKeyVerifier`)
- Modify: `cache/core/src/item_location.rs` (`SinglePoolVerifier`, `MultiPoolVerifier`)
- Test: `cache/core/src/cache.rs` tests module

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `cache/core/src/cache.rs`:

```rust
    /// A location held across a segment recycle must stop resolving.
    ///
    /// Without the tag this is the crucible#88 hazard: segments are append-only
    /// from a fixed start, so with uniform item sizes the n-th item of every
    /// incarnation lands at the same offset, and a stale location silently
    /// resolves to a different item at the same address.
    #[test]
    fn test_stale_location_is_rejected_after_recycle() {
        let cache = build_small_test_cache();
        let verifier = cache.key_verifier();

        cache.set(b"key", b"value", None, Duration::from_secs(60)).unwrap();
        let stale = cache.locate(b"key").expect("key should be present");

        // Sanity: it resolves now, so a later rejection means something.
        assert!(
            verifier.verify(b"key", stale.to_location(), false),
            "location must resolve before the recycle"
        );

        let mut rejections = 0usize;
        for _ in 0..64 {
            cache.flush();
            cache.set(b"key", b"value", None, Duration::from_secs(60)).unwrap();
            if !verifier.verify(b"key", stale.to_location(), false) {
                rejections += 1;
            }
        }

        // Non-zero guards against vacuity: if the loop never recycled the
        // segment the stale location names, this test proves nothing.
        assert!(rejections > 0, "no recycle was observed; test is vacuous");
        assert_eq!(
            rejections, 64,
            "every post-recycle resolution of a stale location must be rejected"
        );
    }
```

If `build_small_test_cache`, `locate` or `key_verifier` do not exist under those
names, use the equivalents the neighbouring `cache.rs` tests already use rather
than adding new public API; the shape of the test is what matters.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p cache-core --lib test_stale_location_is_rejected_after_recycle`
Expected: FAIL at the `rejections == 64` assertion (the stale location still
resolves), or at compile time if a helper name differs.

- [ ] **Step 3: Write the implementation**

In `cache/core/src/cache.rs`, add layouts to the verifier:

```rust
struct CacheKeyVerifier<'a> {
    /// Direct pool references with is_per_item_ttl flag, indexed by pool_id.
    pools: [Option<(PoolRef<'a>, bool)>; 4],
    /// Each pool's location layout, indexed by pool_id. Decoding a location
    /// requires the layout of the pool it names, which is why `pool_id` sits at
    /// a fixed position.
    layouts: [Option<LocationLayout>; 4],
}
```

Populate `layouts` wherever `pools` is populated, from each pool's `layout()`.

Rewrite the body of `verify`:

```rust
    #[inline(always)]
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let item_loc = ItemLocation::from_location(location);
        let pool_id = item_loc.pool_id();

        // SAFETY: pool_id is 2 bits, and both arrays are [_; 4].
        let Some((pool_ref, _is_per_item_ttl)) =
            (unsafe { *self.pools.get_unchecked(pool_id as usize) })
        else {
            return false;
        };
        let Some(layout) = (unsafe { *self.layouts.get_unchecked(pool_id as usize) }) else {
            return false;
        };

        let (_, segment_id, incarnation, offset) = item_loc.unpack(&layout);

        match pool_ref {
            PoolRef::Memory(pool) => {
                let Some(segment) = pool.get(segment_id) else {
                    return false;
                };
                // Before any item bytes are touched: a location naming a
                // previous incarnation of this segment is not ours to resolve.
                if segment.incarnation() != incarnation {
                    return false;
                }
                segment.verify_key_at_offset(offset, key, allow_deleted)
            }
            PoolRef::Disk(pool) => {
                let Some(segment) = pool.get(segment_id) else {
                    return false;
                };
                if segment.incarnation() != incarnation {
                    return false;
                }
                segment.verify_key_at_offset(offset, key, allow_deleted)
            }
            PoolRef::IoUring(pool) => {
                let Some(meta) = pool.get_meta(segment_id) else {
                    return false;
                };
                if meta.incarnation() != incarnation {
                    return false;
                }
                meta.verify_key_at_offset(offset, key, allow_deleted)
            }
        }
    }
```

Apply the same decode-and-check to `prefetch` in the same impl (it currently
calls `unpack()` with no layout), and to `SinglePoolVerifier::verify` and
`MultiPoolVerifier::verify` in `cache/core/src/item_location.rs`. Both of those
need a `LocationLayout` field, supplied at construction:

```rust
pub struct SinglePoolVerifier<'a, S> {
    segments: &'a [S],
    layout: LocationLayout,
}

impl<'a, S> SinglePoolVerifier<'a, S> {
    /// Create a new single-pool verifier.
    pub fn new(segments: &'a [S], layout: LocationLayout) -> Self {
        Self { segments, layout }
    }
}
```

and in its `verify`, after resolving the segment and before
`verify_key_at_offset`:

```rust
        if segment.incarnation() != item_loc.incarnation(&self.layout) {
            return false;
        }
```

`MultiPoolVerifier::with_pool` gains a `layout` parameter and stores
`[Option<(&'a [S], LocationLayout)>; 4]`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p cache-core --lib test_stale_location_is_rejected_after_recycle`
Expected: PASS.

Run: `cargo test -p cache-core`
Expected: PASS.

- [ ] **Step 5: Prove the check red**

Temporarily change the memory arm's guard to `if false {`, then run:

Run: `cargo test -p cache-core --lib test_stale_location_is_rejected_after_recycle`
Expected: FAIL on the `rejections == 64` assertion.

Then temporarily replace the loop body's `cache.flush()` with a no-op and
confirm the test fails on `no recycle was observed; test is vacuous` -- this is
the vacuity trap, and it must fire. Revert both.

- [ ] **Step 6: Commit**

```bash
git add cache/core/src/cache.rs cache/core/src/item_location.rs
git commit -m "fix(cache-core): reject stale-incarnation locations before reading bytes

Closes the false positive in the crucible#88 family: a location held across
a segment recycle no longer resolves to whatever now occupies that offset.

Proven red two ways -- neutering the guard fails the rejection assertion,
and removing the recycle fails the vacuity assertion."
```

---

### Task 8: Loom model

`cache/core/src/loom_oracle.rs:42` currently records that the oracle owns all 44
bits with no generation riding in them. That is exactly what blocked the cache-rs
model port, and it is now false.

**Files:**
- Modify: `cache/core/src/loom_oracle.rs`

- [ ] **Step 1: Write the failing model**

Add to `cache/core/src/loom_oracle.rs`, and update the module comment at line 42
to say the oracle now models incarnations:

```rust
    /// A reader holding a location races the recycle of the segment it names.
    ///
    /// Without the incarnation tag the reader can observe the *new* occupant
    /// through the *old* location. The tag makes that resolution fail, so the
    /// reader must see either its own item or nothing -- never a different one.
    #[test]
    fn loom_stale_location_never_resolves_to_the_new_occupant() {
        loom::model(|| {
            let oracle = std::sync::Arc::new(KeyOracle::new());
            let ht = std::sync::Arc::new(MultiChoiceHashtable::new(16));

            oracle.place(SRC, KEY);
            let stale = oracle.location_tagged(SRC, 0);
            ht.insert(KEY, stale, &*oracle).unwrap();

            let recycler = {
                let oracle = oracle.clone();
                loom::thread::spawn(move || {
                    // End the incarnation, then refill the same cell with a
                    // different key under the next tag.
                    oracle.bump_incarnation(SRC);
                    oracle.place(SRC, OTHER);
                })
            };

            let reader = {
                let oracle = oracle.clone();
                let ht = ht.clone();
                loom::thread::spawn(move || ht.get(KEY, &*oracle))
            };

            let observed = reader.join().unwrap();
            recycler.join().unwrap();

            if let Some((loc, _freq)) = observed {
                assert_eq!(
                    loc, stale,
                    "a resolved location must be the one we published"
                );
                assert!(
                    oracle.resolves_to(loc, KEY),
                    "a stale location resolved to the new occupant"
                );
            }
        });
    }
```

Extend `KeyOracle` with the three methods this needs, keeping the existing cells
representation. `bump_incarnation` must be a single atomic read-modify-write for
the same reason `tombstone` is -- a `load` then `store` collapses loom's
exploration and the model silently stops observing the hazard:

```rust
    /// Advance a cell's incarnation.
    ///
    /// # This MUST be one atomic read-modify-write
    ///
    /// Splitting it into a load and a store collapses loom's interleaving
    /// space: the hazard this model exists to find stops being reachable, and
    /// the model goes green while proving nothing. The same trap cost this
    /// fixture 183 hazard observations once already -- see `tombstone`.
    pub(crate) fn bump_incarnation(&self, cell: usize) {
        self.cells[cell].fetch_add(INCARNATION_UNIT, Ordering::AcqRel);
    }

    /// The location naming `cell` under a specific incarnation.
    pub(crate) fn location_tagged(&self, cell: usize, incarnation: u8) -> Location {
        Location::new(((cell as u64) << 8) | (incarnation as u64 & 0x3F))
    }

    /// Whether `loc` still names the cell/incarnation it was issued for, and
    /// that cell currently holds `key`.
    pub(crate) fn resolves_to(&self, loc: Location, key: &[u8]) -> bool {
        let cell = (loc.as_raw() >> 8) as usize;
        let tag = (loc.as_raw() & 0x3F) as u8;
        let current = self.cells[cell].load(Ordering::Acquire);
        Self::incarnation_of(current) == tag && Self::key_of(current) == key
    }
```

- [ ] **Step 2: Run the model to verify it fails**

Run: `cargo test -p cache-core --features loom --lib loom_stale_location`
Expected: FAIL with `a stale location resolved to the new occupant`, if the
oracle is wired so `verify` does not consult the tag.

- [ ] **Step 3: Make it pass**

Route the oracle's `KeyVerifier::verify` through the incarnation comparison, the
same way the real verifiers do in Task 7.

- [ ] **Step 4: Prove it can fail**

Neuter the oracle's incarnation comparison and re-run:

Run: `cargo test -p cache-core --features loom --lib loom_stale_location`
Expected: FAIL. A model that cannot fail proves nothing -- if it stays green,
the interleaving is not being reached and the model must be fixed, not kept.

Then run the full loom suite and confirm the count went 61 -> 62:

Run: `cargo test -p cache-core --features loom 2>&1 | tail -5`

- [ ] **Step 5: Commit**

```bash
git add cache/core/src/loom_oracle.rs
git commit -m "test(cache-core): loom model for stale-incarnation resolution

The oracle can model incarnations now, which is what the module comment
recorded as blocking the cache-rs model port. bump_incarnation is a single
fetch_add for the same reason tombstone is -- a load/store pair collapses
loom's exploration and the model goes green proving nothing.

Proven red by neutering the oracle's tag comparison."
```

---

### Task 9: Full verification and the residual issue

- [ ] **Step 1: Run every gate**

```bash
cargo test --workspace
cargo test -p cache-core --features loom
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

Expected: all PASS. Record the test counts.

- [ ] **Step 2: Run Miri**

```bash
MIRIFLAGS="-Zmiri-disable-isolation" cargo +nightly miri test -p cache-core --lib -- \
  --skip disk::file_pool --skip disk::disk_layer \
  --skip disk::io_uring --skip disk::recovery --skip hugepage::
```

Expected: PASS, 378 tests plus the ones added here.

- [ ] **Step 3: File the residual**

```bash
gh issue create \
  --title "cache-core: a stale location can still race a header write after passing the incarnation check" \
  --body "$(cat <<'BODY'
Split out from the incarnation-tag work so it is not implied fixed.

The 6-bit incarnation tag removes the *false positive* half of #88: a stale
location naming a recycled segment no longer resolves to whatever now occupies
that offset. It does not remove the *data race*.

A reader can pass the tag check and then be preempted before reading item
bytes, and the segment can be drained and refilled in that window.
`append_with_header` publishes an offset via `reserve_space`'s CAS and then
lays the header down with a plain `copy_nonoverlapping`, so the reader's bytes
race that write.

Closing it requires the read to happen under a ref guard taken *before* the
tag check, so the incarnation cannot advance underneath: guard, then check,
then read. The guard establishes "this incarnation cannot be reclaimed while I
hold it"; the tag establishes "this is the incarnation my location names".
Neither alone is sufficient.

Design context: docs/superpowers/specs/2026-09-06-location-incarnation-tag-design.md
BODY
)"
```

- [ ] **Step 4: Open the PR**

Body must state, in the author's own words: what the tag does and does not
close; that capacity per pool is now `2^36 * align_bytes` and therefore
invariant under `segment_size`; that memory pools are unchanged at 8-byte
alignment while disk pools coarsen to 512; that the read-path cost is
**unmeasured** and cache-rs#78's `+0.94 ns` figure does not transfer because
crucible's shifts became layout-dependent loads; and the residual issue number
from Step 3.

---

## Self-Review

**Spec coverage.** Layout and the fixed 6-bit tag: Task 1. Capacity invariance
and per-pool alignment: Tasks 1 and 5. `incarnation` in `Metadata` rather than
`generation`: Task 2. Bump sites and the transition table: Task 3. The check:
Task 7. GHOST unaliasable: Task 1 (`ghost_is_unaliasable`, `max_segment_count`).
Construction validates: Tasks 1 and 5. Memory and disk together: Tasks 5, 6, 7.
Evidence items 1-4: Tasks 7, 8, 3, 1 respectively. Residuals: Task 9 Steps 3-4.

**Known soft spots**, called out rather than hidden:

- Task 7's test uses `build_small_test_cache` / `locate` / `key_verifier`, and
  Task 3's uses `make_segment`. These are the shapes the tests need, not names
  verified to exist. Both steps say to adapt to the neighbouring tests' helpers.
- Task 8's `KeyOracle` extension assumes the cells encode a key and can carry an
  incarnation in spare bits. If the existing encoding has no room, widen the cell
  rather than packing the tag into the key field.
- The read-path cost of turning constant shifts into layout-dependent loads is
  unmeasured, and no task measures it. That is deliberate: measuring it well
  needs the counterbalanced A/B discipline cache-rs#78 used, which is its own
  piece of work and should not be faked inside this one.
