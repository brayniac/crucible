//! Segment state machine and packed metadata.
//!
//! This module defines the segment lifecycle states and provides a packed
//! representation for atomic state transitions.

/// Sentinel value for invalid/unset segment IDs in chain pointers.
/// Uses 24-bit max value since segment IDs are packed into 24 bits.
pub const INVALID_SEGMENT_ID: u32 = 0xFF_FFFF;

/// State of a segment in its lifecycle.
///
/// # State Semantics
///
/// - **Free**: In free queue, available for allocation
/// - **Reserved**: Allocated for use, being prepared for chain insertion
/// - **Linking**: Being added to a chain (next/prev being set)
/// - **Live**: Active tail segment accepting writes and reads
/// - **Sealed**: No more writes accepted, but data readable and chain stable
/// - **Relinking**: Chain pointers being updated during neighbor removal.
///   Data remains readable, only next/prev pointers are being modified.
/// - **Draining**: Segment is being processed (merge eviction or removal).
///   Provides exclusive access - only one thread can hold a segment in Draining.
///   New reads are rejected; must wait for ref_count to drop before modifying data.
/// - **Locked**: Being cleared, all access rejected
///
/// # State Transition Diagram
///
/// ```text
///                  +------------------+
///        +-------->|      Free        |<-----------------+
///        |         +--------+---------+                  |
///        |                  | reserve()                  |
///        |                  v                            |
///        |         +------------------+                  |
///        |         |    Reserved      |                  |
///        |         +--------+---------+                  |
///        |                  | link into chain            |
///        |                  v                            |
///        |         +------------------+                  |
///   release()      |     Linking      |                  |
///        |         +--------+---------+                  |
///        |                  | chain linked               |
///        |                  v                            |
///        |         +------------------+                  |
///        |         |      Live        |<----+            |
///        |         +--------+---------+     |            |
///        |                  | segment full  | relink     |
///        |                  v               |            |
///        |         +------------------+     |            |
///        |         |     Sealed       |-----+            |
///        |         +--------+---------+                  |
///        |                  | begin eviction             |
///        |                  v                            |
///        |         +------------------+                  |
///        |         |    Draining      |                  |
///        |         +--------+---------+                  |
///        |                  | ref_count == 0             |
///        |                  v                            |
///        |         +------------------+                  |
///        +---------|     Locked       |------------------+
///                  +------------------+
///                           | clear() -> Reserved
///                           v
///                  (back to Reserved for reuse)
/// ```
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// In free queue, available for allocation.
    Free = 0,
    /// Allocated for use, being prepared.
    Reserved = 1,
    /// Being added to a chain.
    Linking = 2,
    /// Active segment accepting writes and reads.
    Live = 3,
    /// No more writes, still readable.
    Sealed = 4,
    /// Chain pointers being updated.
    Relinking = 5,
    /// Being evicted, waiting for readers.
    Draining = 6,
    /// Being cleared, all access rejected.
    Locked = 7,
    /// Segment is condemned (removed from chain, hashtable updated).
    /// Data remains valid for in-flight readers. When ref_count hits 0,
    /// the last reader's guard drop will push segment to free queue.
    ///
    /// # AwaitingRelease State Transition Pattern
    ///
    /// This state implements a sophisticated concurrency pattern for safe
    /// segment reclamation during eviction. The pattern works as follows:
    ///
    /// ## State Transition Flow
    ///
    /// ```text
    /// Eviction Thread:                    Reader Thread:
    /// --------------------------------    --------------------------------
    /// 1. Check ref_count == 0
    /// 2. CAS: Draining -> AwaitingRelease
    /// 3. Update hashtable (remove key)
    ///                                    4. Increment ref_count
    ///                                    5. Read data
    ///                                    6. Decrement ref_count in Drop
    ///                                    7. If prev_count == 1:
    ///                                       - Check state == AwaitingRelease
    ///                                       - CAS: AwaitingRelease -> Free
    ///                                       - Push segment to free queue
    /// ```
    ///
    /// ## Why This Pattern?
    ///
    /// The race condition this solves:
    /// - Eviction thread sees ref_count == 0
    /// - Before eviction CAS, a reader increments ref_count
    /// - Eviction thread transitions to AwaitingRelease
    /// - Reader's Drop sees AwaitingRelease and frees the segment
    ///
    /// ## Memory Ordering
    ///
    /// - Release fence when decrementing ref_count (ensures all reads
    ///   complete before checking state)
    /// - Acquire fence when reading state (ensures we see the
    ///   AwaitingRelease written by eviction thread)
    /// - AcqRel for the final CAS (combines acquire/release semantics)
    ///
    /// ## Safety Guarantees
    ///
    /// - Segment memory remains valid as long as ref_count > 0
    /// - The last reader (seeing AwaitingRelease) is responsible for freeing
    /// - Double-free is prevented by the CAS (only one thread succeeds)
    AwaitingRelease = 8,
}

impl State {
    /// Convert from raw u8 value.
    ///
    /// # Panics
    /// Panics if the value is not a valid state (0-7).
    #[inline]
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => State::Free,
            1 => State::Reserved,
            2 => State::Linking,
            3 => State::Live,
            4 => State::Sealed,
            5 => State::Relinking,
            6 => State::Draining,
            7 => State::Locked,
            8 => State::AwaitingRelease,
            _ => panic!("Invalid segment state value: {}", value),
        }
    }

    /// Check if the segment is readable (allows get operations).
    ///
    /// Note: AwaitingRelease is readable for in-flight readers to complete,
    /// but new reads should not be initiated (hashtable points elsewhere).
    #[inline]
    pub fn is_readable(self) -> bool {
        matches!(
            self,
            State::Live | State::Sealed | State::Relinking | State::AwaitingRelease
        )
    }

    /// Check if the segment is writable (allows append operations).
    #[inline]
    pub fn is_writable(self) -> bool {
        matches!(self, State::Live)
    }

    /// Check if the segment can be evicted.
    #[inline]
    pub fn is_evictable(self) -> bool {
        matches!(self, State::Sealed)
    }
}

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
    ///
    /// Shared with `Location`'s tag field so the two cannot drift apart.
    const INCARNATION_BITS: u32 = crate::location_layout::TAG_BITS;
    /// Mask covering [`Self::INCARNATION_BITS`].
    const INCARNATION_MASK: u8 = crate::location_layout::TAG_MASK;
    /// Bit position of the incarnation within the packed word.
    const INCARNATION_SHIFT: u32 = 56;

    /// Create metadata for a brand-new segment: no chain links, incarnation 0.
    ///
    /// # This is not a transition
    ///
    /// Zeroing the incarnation is correct **only** when constructing a segment
    /// that has never been used, or when seeding a test's metadata word.
    /// Calling this on a live segment silently resets its tag, which
    /// resurrects every stale location naming it -- and unlike a struct
    /// literal, it compiles clean. For a state change use `with_state`.
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
    /// Zeroes the incarnation and carries the same hazard as [`Self::new`];
    /// see the warning there. For a state or chain change on a live segment
    /// use `with_state` / `with_chain_ids`.
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

    /// This metadata with different chain pointers, preserving everything else.
    #[inline]
    pub fn with_chain_ids(self, next: u32, prev: u32) -> Self {
        Self { next, prev, ..self }
    }

    /// This metadata with the incarnation advanced by one, wrapping at 6 bits.
    ///
    /// Call this on exactly the transitions that end a *used* incarnation --
    /// `Locked -> Free` and `AwaitingRelease -> Free`.
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

    /// Get the next segment ID, or None if invalid.
    #[inline]
    pub fn next_id(&self) -> Option<u32> {
        if self.next == INVALID_SEGMENT_ID {
            None
        } else {
            Some(self.next)
        }
    }

    /// Get the previous segment ID, or None if invalid.
    #[inline]
    pub fn prev_id(&self) -> Option<u32> {
        if self.prev == INVALID_SEGMENT_ID {
            None
        } else {
            Some(self.prev)
        }
    }
}

/// The incarnation must sit strictly above the state byte (bits 48..56) and
/// still fit inside the 64-bit packed word. Widening `TAG_BITS` past 8 would
/// break this, so fail the build rather than corrupt `state`.
const _: () = assert!(
    Metadata::INCARNATION_SHIFT >= 56
        && Metadata::INCARNATION_SHIFT + Metadata::INCARNATION_BITS <= 64,
    "incarnation tag does not fit above the state byte in the packed metadata word"
);

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_state_from_u8() {
        assert_eq!(State::from_u8(0), State::Free);
        assert_eq!(State::from_u8(1), State::Reserved);
        assert_eq!(State::from_u8(2), State::Linking);
        assert_eq!(State::from_u8(3), State::Live);
        assert_eq!(State::from_u8(4), State::Sealed);
        assert_eq!(State::from_u8(5), State::Relinking);
        assert_eq!(State::from_u8(6), State::Draining);
        assert_eq!(State::from_u8(7), State::Locked);
        assert_eq!(State::from_u8(8), State::AwaitingRelease);
    }

    #[test]
    #[should_panic(expected = "Invalid segment state value")]
    fn test_state_from_u8_invalid() {
        State::from_u8(9);
    }

    #[test]
    fn test_state_predicates() {
        assert!(State::Live.is_readable());
        assert!(State::Sealed.is_readable());
        assert!(State::Relinking.is_readable());
        assert!(State::AwaitingRelease.is_readable());
        assert!(!State::Free.is_readable());
        assert!(!State::Draining.is_readable());

        assert!(State::Live.is_writable());
        assert!(!State::Sealed.is_writable());
        assert!(!State::AwaitingRelease.is_writable());

        assert!(State::Sealed.is_evictable());
        assert!(!State::Live.is_evictable());
        assert!(!State::AwaitingRelease.is_evictable());
    }

    #[test]
    fn test_metadata_new() {
        let meta = Metadata::new(State::Free);
        assert_eq!(meta.state, State::Free);
        assert_eq!(meta.next, INVALID_SEGMENT_ID);
        assert_eq!(meta.prev, INVALID_SEGMENT_ID);
        assert!(meta.next_id().is_none());
        assert!(meta.prev_id().is_none());
    }

    #[test]
    fn test_metadata_with_chain() {
        let meta = Metadata::with_chain(State::Live, Some(100), Some(50));
        assert_eq!(meta.state, State::Live);
        assert_eq!(meta.next_id(), Some(100));
        assert_eq!(meta.prev_id(), Some(50));
    }

    #[test]
    fn test_metadata_pack_unpack() {
        let meta = Metadata {
            next: 123,
            prev: 456,
            state: State::Live,
            incarnation: 0,
        };

        let packed = meta.pack();
        let unpacked = Metadata::unpack(packed);

        assert_eq!(unpacked.next, 123);
        assert_eq!(unpacked.prev, 456);
        assert_eq!(unpacked.state, State::Live);
    }

    #[test]
    fn test_metadata_pack_unpack_invalid_ids() {
        let meta = Metadata {
            next: INVALID_SEGMENT_ID,
            prev: INVALID_SEGMENT_ID,
            state: State::Free,
            incarnation: 0,
        };

        let packed = meta.pack();
        let unpacked = Metadata::unpack(packed);

        assert_eq!(unpacked.next, INVALID_SEGMENT_ID);
        assert_eq!(unpacked.prev, INVALID_SEGMENT_ID);
        assert!(unpacked.next_id().is_none());
        assert!(unpacked.prev_id().is_none());
    }

    #[test]
    fn test_metadata_24bit_masking() {
        // Test that values larger than 24 bits are masked
        let meta = Metadata {
            next: 0xFFFF_FFFF, // 32 bits set
            prev: 0xFFFF_FFFF,
            state: State::Sealed,
            incarnation: 0,
        };

        let packed = meta.pack();
        let unpacked = Metadata::unpack(packed);

        // Should be masked to 24 bits
        assert_eq!(unpacked.next, 0xFF_FFFF);
        assert_eq!(unpacked.prev, 0xFF_FFFF);
    }

    #[test]
    fn test_all_states_pack_unpack() {
        for state_val in 0..9u8 {
            let state = State::from_u8(state_val);
            let meta = Metadata::new(state);
            let packed = meta.pack();
            let unpacked = Metadata::unpack(packed);
            assert_eq!(unpacked.state, state);
        }
    }

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
    fn test_metadata_with_state_and_chain_ids_preserve_incarnation() {
        // These are the helpers every transition site uses. If either dropped
        // the tag, stale locations naming the segment would resolve again.
        let meta = Metadata {
            next: 1,
            prev: 2,
            state: State::Live,
            incarnation: 0x2A,
        };

        let restated = meta.with_state(State::Sealed);
        assert_eq!(restated.incarnation, 0x2A);
        assert_eq!(restated.state, State::Sealed);
        assert_eq!(restated.next, 1);
        assert_eq!(restated.prev, 2);

        let rechained = meta.with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID);
        assert_eq!(rechained.incarnation, 0x2A);
        assert_eq!(rechained.state, State::Live);
        assert_eq!(rechained.next, INVALID_SEGMENT_ID);
        assert_eq!(rechained.prev, INVALID_SEGMENT_ID);
    }

    #[test]
    fn test_metadata_bump_incarnation_advances_and_preserves_fields() {
        let meta = Metadata {
            next: 7,
            prev: 8,
            state: State::AwaitingRelease,
            incarnation: 0,
        };
        let bumped = meta.bump_incarnation();
        assert_eq!(bumped.incarnation, 1);
        assert_eq!(bumped.state, State::AwaitingRelease);
        assert_eq!(bumped.next, 7);
        assert_eq!(bumped.prev, 8);
        // And it survives a pack/unpack round trip.
        assert_eq!(Metadata::unpack(bumped.pack()).incarnation, 1);
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
}
