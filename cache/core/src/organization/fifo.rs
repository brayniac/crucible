//! FIFO segment chain organization.
//!
//! This module provides a simple FIFO (First-In-First-Out) segment chain
//! for use in admission queues like S3FIFO's small queue.
//!
//! # Chain Structure
//!
//! Segments form a singly-linked FIFO chain:
//! ```text
//! head (oldest) -> seg1 -> seg2 -> ... -> tail (newest, accepting writes)
//! ```
//!
//! - **Head**: Oldest segment (evicted first)
//! - **Tail**: Newest segment (currently Live, accepting writes)
//!
//! # Thread Safety
//!
//! Chain operations are serialized via a mutex. The atomic head/tail
//! pointers can be read without the lock for fast lookups.

use crate::pool::RamPool;
use crate::segment::Segment;
use crate::state::{INVALID_SEGMENT_ID, State};
use crate::sync::{AtomicU64, Ordering};
use parking_lot::Mutex;

/// Error type for segment chain operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainError {
    /// Segment ID was invalid or not found in pool.
    InvalidSegmentId,
    /// Segment is not in the expected state.
    InvalidState,
    /// Failed to transition segment state.
    StateTransitionFailed,
    /// Chain is empty.
    EmptyChain,
    /// Cannot evict the only segment (need to keep tail Live).
    CannotEvictLiveSegment,
}

/// A FIFO segment chain for organizing segments.
///
/// Maintains a singly-linked list of segments in FIFO order.
/// Used by S3FIFO's small queue (Layer 0) for admission filtering.
pub struct FifoChain {
    /// Packed head and tail segment IDs.
    /// Layout: [32 bits head][32 bits tail]
    head_tail: AtomicU64,

    /// Number of segments in the chain.
    segment_count: crate::sync::AtomicU32,

    /// Mutex for serializing chain modifications.
    chain_mutex: Mutex<()>,
}

impl FifoChain {
    /// Sentinel value for empty chain (both head and tail invalid).
    const EMPTY: u64 = Self::pack(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID);

    /// Create a new empty FIFO chain.
    pub fn new() -> Self {
        Self {
            head_tail: AtomicU64::new(Self::EMPTY),
            segment_count: crate::sync::AtomicU32::new(0),
            chain_mutex: Mutex::new(()),
        }
    }

    /// Pack head and tail into a single u64.
    const fn pack(head: u32, tail: u32) -> u64 {
        ((head as u64) << 32) | (tail as u64)
    }

    /// Unpack head and tail from a u64.
    fn unpack(packed: u64) -> (u32, u32) {
        let head = (packed >> 32) as u32;
        let tail = packed as u32;
        (head, tail)
    }

    /// Get the current head segment ID (oldest segment).
    ///
    /// This is a lock-free read for fast lookups.
    pub fn head(&self) -> Option<u32> {
        let (head, _) = Self::unpack(self.head_tail.load(Ordering::Acquire));
        if head == INVALID_SEGMENT_ID {
            None
        } else {
            Some(head)
        }
    }

    /// Get the current tail segment ID (newest segment, accepting writes).
    ///
    /// This is a lock-free read for fast lookups.
    pub fn tail(&self) -> Option<u32> {
        let (_, tail) = Self::unpack(self.head_tail.load(Ordering::Acquire));
        if tail == INVALID_SEGMENT_ID {
            None
        } else {
            Some(tail)
        }
    }

    /// Check if the chain is empty.
    pub fn is_empty(&self) -> bool {
        self.head_tail.load(Ordering::Acquire) == Self::EMPTY
    }

    /// Get the number of segments in the chain.
    pub fn segment_count(&self) -> usize {
        self.segment_count.load(Ordering::Relaxed) as usize
    }

    /// Push a segment onto the tail of the chain.
    ///
    /// The segment must be in Reserved state. It will be transitioned
    /// through Linking to Live state.
    ///
    /// # Arguments
    /// * `segment_id` - ID of the segment to push
    /// * `pool` - The pool containing the segments
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(ChainError)` if the operation fails
    pub fn push<P: RamPool>(&self, segment_id: u32, pool: &P) -> Result<(), ChainError>
    where
        P::Segment: Segment,
    {
        let _guard = self.chain_mutex.lock();

        let segment = pool.get(segment_id).ok_or(ChainError::InvalidSegmentId)?;

        // Verify segment is Reserved
        if segment.state() != State::Reserved {
            return Err(ChainError::InvalidState);
        }

        let current = self.head_tail.load(Ordering::Acquire);
        let (head, tail) = Self::unpack(current);

        if tail == INVALID_SEGMENT_ID {
            // Empty chain - this segment becomes both head and tail
            // Transition: Reserved -> Linking
            if !segment.cas_metadata(
                State::Reserved,
                State::Linking,
                Some(INVALID_SEGMENT_ID),
                Some(INVALID_SEGMENT_ID),
            ) {
                return Err(ChainError::StateTransitionFailed);
            }

            // Update chain pointers
            let new = Self::pack(segment_id, segment_id);
            self.head_tail.store(new, Ordering::Release);

            // Transition: Linking -> Live
            segment.cas_metadata(State::Linking, State::Live, None, None);
            self.segment_count.fetch_add(1, Ordering::Relaxed);

            Ok(())
        } else {
            // Non-empty chain - append after current tail
            let old_tail = pool.get(tail).ok_or(ChainError::InvalidSegmentId)?;

            // Seal the old tail if it's Live
            if old_tail.state() == State::Live
                && !old_tail.cas_metadata(State::Live, State::Sealed, None, None)
            {
                return Err(ChainError::StateTransitionFailed);
            }

            // Set up the new segment: Reserved -> Linking with prev pointing to old tail
            if !segment.cas_metadata(
                State::Reserved,
                State::Linking,
                Some(INVALID_SEGMENT_ID),
                Some(tail),
            ) {
                return Err(ChainError::StateTransitionFailed);
            }

            // Update old tail's next pointer to point to new segment
            if !old_tail.cas_metadata(State::Sealed, State::Sealed, Some(segment_id), None) {
                // Rollback new segment state
                segment.cas_metadata(State::Linking, State::Reserved, None, None);
                return Err(ChainError::StateTransitionFailed);
            }

            // Update chain tail
            let new = Self::pack(head, segment_id);
            self.head_tail.store(new, Ordering::Release);

            // Transition: Linking -> Live
            segment.cas_metadata(State::Linking, State::Live, None, None);
            self.segment_count.fetch_add(1, Ordering::Relaxed);

            Ok(())
        }
    }

    /// Pop the head segment from the chain for eviction.
    ///
    /// The head segment must be in Sealed state (not the current Live tail).
    /// The segment will be transitioned to Draining state.
    ///
    /// # Arguments
    /// * `pool` - The pool containing the segments
    ///
    /// # Returns
    /// * `Ok(segment_id)` of the popped segment
    /// * `Err(ChainError)` if the operation fails
    pub fn pop<P: RamPool>(&self, pool: &P) -> Result<u32, ChainError>
    where
        P::Segment: Segment,
    {
        let _guard = self.chain_mutex.lock();

        let current = self.head_tail.load(Ordering::Acquire);
        let (head, tail) = Self::unpack(current);

        if head == INVALID_SEGMENT_ID {
            return Err(ChainError::EmptyChain);
        }

        // Don't evict if head == tail (only one segment, which is Live)
        if head == tail {
            return Err(ChainError::CannotEvictLiveSegment);
        }

        let head_segment = pool.get(head).ok_or(ChainError::InvalidSegmentId)?;
        let next = head_segment.next().unwrap_or(INVALID_SEGMENT_ID);

        // Head must be Sealed (not Live)
        let state = head_segment.state();
        if state != State::Sealed {
            return Err(ChainError::InvalidState);
        }

        // Transition: Sealed -> Draining
        if !head_segment.cas_metadata(State::Sealed, State::Draining, None, None) {
            return Err(ChainError::StateTransitionFailed);
        }

        // Update chain head
        let new_head = next;
        let new_tail = if new_head == INVALID_SEGMENT_ID {
            INVALID_SEGMENT_ID
        } else {
            tail
        };

        let new = Self::pack(new_head, new_tail);
        self.head_tail.store(new, Ordering::Release);
        self.segment_count.fetch_sub(1, Ordering::Relaxed);

        // Update new head's prev pointer to INVALID
        if new_head != INVALID_SEGMENT_ID
            && let Some(new_head_segment) = pool.get(new_head)
        {
            new_head_segment.cas_metadata(
                new_head_segment.state(),
                new_head_segment.state(),
                None,
                Some(INVALID_SEGMENT_ID),
            );
        }

        Ok(head)
    }

    /// Remove a specific segment from the chain (for emergency eviction).
    ///
    /// The segment must be in Sealed state. It will be transitioned to Draining.
    /// This walks the chain from head to find and unlink the target segment.
    ///
    /// # Arguments
    /// * `segment_id` - ID of the segment to remove
    /// * `pool` - The pool containing the segments
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(ChainError)` if the operation fails
    pub fn try_remove<P: RamPool>(&self, segment_id: u32, pool: &P) -> Result<(), ChainError>
    where
        P::Segment: Segment,
    {
        let _guard = self.chain_mutex.lock();

        let current = self.head_tail.load(Ordering::Acquire);
        let (head, tail) = Self::unpack(current);

        if head == INVALID_SEGMENT_ID {
            return Err(ChainError::EmptyChain);
        }

        // Don't remove the Live tail
        if segment_id == tail {
            return Err(ChainError::CannotEvictLiveSegment);
        }

        let target = pool.get(segment_id).ok_or(ChainError::InvalidSegmentId)?;
        if target.state() != State::Sealed {
            return Err(ChainError::InvalidState);
        }

        // Transition target: Sealed -> Draining
        if !target.cas_metadata(State::Sealed, State::Draining, None, None) {
            return Err(ChainError::StateTransitionFailed);
        }

        let target_next = target.next().unwrap_or(INVALID_SEGMENT_ID);

        if segment_id == head {
            // Removing head - same as pop
            let new_head = target_next;
            let new_tail = if new_head == INVALID_SEGMENT_ID {
                INVALID_SEGMENT_ID
            } else {
                tail
            };
            self.head_tail
                .store(Self::pack(new_head, new_tail), Ordering::Release);

            // Update new head's prev pointer
            if new_head != INVALID_SEGMENT_ID
                && let Some(new_head_seg) = pool.get(new_head)
            {
                new_head_seg.cas_metadata(
                    new_head_seg.state(),
                    new_head_seg.state(),
                    None,
                    Some(INVALID_SEGMENT_ID),
                );
            }
        } else {
            // Removing from middle - walk from head to find predecessor
            let mut prev_id = head;
            loop {
                let prev_seg = pool.get(prev_id).ok_or(ChainError::InvalidSegmentId)?;
                let next_id = prev_seg.next().unwrap_or(INVALID_SEGMENT_ID);
                if next_id == segment_id {
                    // Found predecessor - unlink target
                    prev_seg.cas_metadata(
                        prev_seg.state(),
                        prev_seg.state(),
                        Some(target_next),
                        None,
                    );
                    // Update successor's prev pointer
                    if target_next != INVALID_SEGMENT_ID
                        && let Some(next_seg) = pool.get(target_next)
                    {
                        next_seg.cas_metadata(
                            next_seg.state(),
                            next_seg.state(),
                            None,
                            Some(prev_id),
                        );
                    }
                    break;
                }
                if next_id == INVALID_SEGMENT_ID || next_id == tail {
                    // Didn't find target in chain - rollback
                    target.cas_metadata(State::Draining, State::Sealed, None, None);
                    return Err(ChainError::InvalidSegmentId);
                }
                prev_id = next_id;
            }
        }

        self.segment_count.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get the write segment (tail) if available and Live.
    ///
    /// Returns the tail segment ID if the tail is in Live state.
    pub fn get_write_segment(&self) -> Option<u32> {
        self.tail()
    }
}

impl Default for FifoChain {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::memory_pool::MemoryPoolBuilder;

    fn create_test_pool() -> crate::memory_pool::MemoryPool {
        MemoryPoolBuilder::new(0)
            .per_item_ttl(true)
            .segment_size(64 * 1024) // 64KB
            .heap_size(640 * 1024) // 640KB = 10 segments
            .spare_capacity(0) // No spare capacity for tests
            .build()
            .expect("Failed to create test pool")
    }

    #[test]
    fn test_pack_unpack() {
        let head = 0x123456;
        let tail = 0x789ABC;
        let packed = FifoChain::pack(head, tail);
        let (h, t) = FifoChain::unpack(packed);
        assert_eq!(h, head);
        assert_eq!(t, tail);
    }

    #[test]
    fn test_empty_chain() {
        let chain = FifoChain::new();
        assert!(chain.is_empty());
        assert_eq!(chain.head(), None);
        assert_eq!(chain.tail(), None);
        assert_eq!(chain.segment_count(), 0);
    }

    #[test]
    fn test_pack_unpack_sentinel() {
        let packed = FifoChain::pack(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID);
        let (h, t) = FifoChain::unpack(packed);
        assert_eq!(h, INVALID_SEGMENT_ID);
        assert_eq!(t, INVALID_SEGMENT_ID);
    }

    #[test]
    fn test_default() {
        let chain = FifoChain::default();
        assert!(chain.is_empty());
        assert_eq!(chain.segment_count(), 0);
    }

    #[test]
    fn test_push_first_segment() {
        let pool = create_test_pool();
        let chain = FifoChain::new();

        let seg_id = pool.reserve().expect("Should reserve segment");
        chain.push(seg_id, &pool).expect("Push should succeed");

        assert!(!chain.is_empty());
        assert_eq!(chain.head(), Some(seg_id));
        assert_eq!(chain.tail(), Some(seg_id));
        assert_eq!(chain.segment_count(), 1);

        // Segment should be Live
        let segment = pool.get(seg_id).unwrap();
        assert_eq!(segment.state(), State::Live);
    }

    #[test]
    fn test_push_multiple_segments() {
        let pool = create_test_pool();
        let chain = FifoChain::new();

        let seg1 = pool.reserve().expect("Should reserve segment 1");
        let seg2 = pool.reserve().expect("Should reserve segment 2");
        let seg3 = pool.reserve().expect("Should reserve segment 3");

        chain.push(seg1, &pool).expect("Push seg1");
        chain.push(seg2, &pool).expect("Push seg2");
        chain.push(seg3, &pool).expect("Push seg3");

        assert_eq!(chain.segment_count(), 3);
        assert_eq!(chain.head(), Some(seg1));
        assert_eq!(chain.tail(), Some(seg3));

        // First segment should be Sealed
        let segment1 = pool.get(seg1).unwrap();
        assert_eq!(segment1.state(), State::Sealed);

        // Second segment should be Sealed
        let segment2 = pool.get(seg2).unwrap();
        assert_eq!(segment2.state(), State::Sealed);

        // Third segment (tail) should be Live
        let segment3 = pool.get(seg3).unwrap();
        assert_eq!(segment3.state(), State::Live);
    }

    #[test]
    fn test_push_invalid_segment_id() {
        let pool = create_test_pool();
        let chain = FifoChain::new();

        let result = chain.push(999, &pool);
        assert_eq!(result, Err(ChainError::InvalidSegmentId));
    }

    #[test]
    fn test_push_wrong_state() {
        let pool = create_test_pool();
        let chain = FifoChain::new();

        // Get a segment but don't reserve it (it's Free)
        let seg_id = pool.reserve().unwrap();
        pool.release(seg_id); // Put it back to Free

        let result = chain.push(seg_id, &pool);
        assert_eq!(result, Err(ChainError::InvalidState));
    }

    #[test]
    fn test_pop_empty_chain() {
        let pool = create_test_pool();
        let chain = FifoChain::new();

        let result = chain.pop(&pool);
        assert_eq!(result, Err(ChainError::EmptyChain));
    }

    #[test]
    fn test_pop_single_segment() {
        let pool = create_test_pool();
        let chain = FifoChain::new();

        let seg_id = pool.reserve().expect("Should reserve segment");
        chain.push(seg_id, &pool).expect("Push should succeed");

        // Cannot pop the only segment (it's Live)
        let result = chain.pop(&pool);
        assert_eq!(result, Err(ChainError::CannotEvictLiveSegment));
    }

    #[test]
    fn test_pop_from_chain() {
        let pool = create_test_pool();
        let chain = FifoChain::new();

        let seg1 = pool.reserve().expect("Should reserve segment 1");
        let seg2 = pool.reserve().expect("Should reserve segment 2");

        chain.push(seg1, &pool).expect("Push seg1");
        chain.push(seg2, &pool).expect("Push seg2");

        // Now seg1 is Sealed and can be popped
        let popped = chain.pop(&pool).expect("Pop should succeed");
        assert_eq!(popped, seg1);
        assert_eq!(chain.segment_count(), 1);
        assert_eq!(chain.head(), Some(seg2));
        assert_eq!(chain.tail(), Some(seg2));

        // Popped segment should be Draining
        let segment = pool.get(seg1).unwrap();
        assert_eq!(segment.state(), State::Draining);
    }

    #[test]
    fn test_pop_multiple() {
        let pool = create_test_pool();
        let chain = FifoChain::new();

        let seg1 = pool.reserve().unwrap();
        let seg2 = pool.reserve().unwrap();
        let seg3 = pool.reserve().unwrap();

        chain.push(seg1, &pool).unwrap();
        chain.push(seg2, &pool).unwrap();
        chain.push(seg3, &pool).unwrap();

        // Pop first
        let popped1 = chain.pop(&pool).expect("Pop 1");
        assert_eq!(popped1, seg1);
        assert_eq!(chain.head(), Some(seg2));

        // Pop second
        let popped2 = chain.pop(&pool).expect("Pop 2");
        assert_eq!(popped2, seg2);
        assert_eq!(chain.head(), Some(seg3));

        // Cannot pop third (it's Live tail)
        let result = chain.pop(&pool);
        assert_eq!(result, Err(ChainError::CannotEvictLiveSegment));
    }

    #[test]
    fn test_get_write_segment() {
        let pool = create_test_pool();
        let chain = FifoChain::new();

        assert_eq!(chain.get_write_segment(), None);

        let seg_id = pool.reserve().unwrap();
        chain.push(seg_id, &pool).unwrap();

        assert_eq!(chain.get_write_segment(), Some(seg_id));
    }

    #[test]
    fn test_chain_error_variants() {
        // Just verify all error variants exist and can be compared
        assert_eq!(ChainError::InvalidSegmentId, ChainError::InvalidSegmentId);
        assert_ne!(ChainError::InvalidSegmentId, ChainError::InvalidState);
        assert_ne!(ChainError::StateTransitionFailed, ChainError::EmptyChain);
        assert_ne!(
            ChainError::CannotEvictLiveSegment,
            ChainError::InvalidSegmentId
        );
    }

    #[test]
    fn test_chain_error_debug() {
        let err = ChainError::InvalidSegmentId;
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("InvalidSegmentId"));
    }
}
