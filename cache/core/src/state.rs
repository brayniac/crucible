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
            _ => panic!("Invalid segment state value: {}", value),
        }
    }

    /// Check if the segment is readable (allows get operations).
    #[inline]
    pub fn is_readable(self) -> bool {
        matches!(self, State::Live | State::Sealed | State::Relinking)
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
/// Layout: `[8 bits unused][8 bits state][24 bits prev][24 bits next]`
///
/// This packing allows atomic updates to state and chain pointers together,
/// which is essential for lock-free chain operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    /// Next segment ID in the chain (24 bits, INVALID_SEGMENT_ID if none).
    pub next: u32,
    /// Previous segment ID in the chain (24 bits, INVALID_SEGMENT_ID if none).
    pub prev: u32,
    /// Current state of the segment.
    pub state: State,
}

impl Metadata {
    /// Create new metadata with the given state and no chain links.
    pub fn new(state: State) -> Self {
        Self {
            next: INVALID_SEGMENT_ID,
            prev: INVALID_SEGMENT_ID,
            state,
        }
    }

    /// Create metadata with state and chain pointers.
    pub fn with_chain(state: State, next: Option<u32>, prev: Option<u32>) -> Self {
        Self {
            next: next.unwrap_or(INVALID_SEGMENT_ID),
            prev: prev.unwrap_or(INVALID_SEGMENT_ID),
            state,
        }
    }

    /// Pack the metadata into a single u64 for atomic storage.
    #[inline]
    pub fn pack(self) -> u64 {
        // Mask to ensure we only use 24 bits for IDs
        let next_24 = (self.next & 0xFF_FFFF) as u64;
        let prev_24 = (self.prev & 0xFF_FFFF) as u64;
        let state_8 = self.state as u64;

        // Pack: [8 unused][8 state][24 prev][24 next]
        (state_8 << 48) | (prev_24 << 24) | next_24
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
    }

    #[test]
    #[should_panic(expected = "Invalid segment state value")]
    fn test_state_from_u8_invalid() {
        State::from_u8(8);
    }

    #[test]
    fn test_state_predicates() {
        assert!(State::Live.is_readable());
        assert!(State::Sealed.is_readable());
        assert!(State::Relinking.is_readable());
        assert!(!State::Free.is_readable());
        assert!(!State::Draining.is_readable());

        assert!(State::Live.is_writable());
        assert!(!State::Sealed.is_writable());

        assert!(State::Sealed.is_evictable());
        assert!(!State::Live.is_evictable());
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
        };

        let packed = meta.pack();
        let unpacked = Metadata::unpack(packed);

        // Should be masked to 24 bits
        assert_eq!(unpacked.next, 0xFF_FFFF);
        assert_eq!(unpacked.prev, 0xFF_FFFF);
    }

    #[test]
    fn test_all_states_pack_unpack() {
        for state_val in 0..8u8 {
            let state = State::from_u8(state_val);
            let meta = Metadata::new(state);
            let packed = meta.pack();
            let unpacked = Metadata::unpack(packed);
            assert_eq!(unpacked.state, state);
        }
    }
}
