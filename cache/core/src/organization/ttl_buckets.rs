//! TTL-based segment organization with doubly-linked bucket chains.
//!
//! This module organizes segments by their expiration time (TTL) using 1024 buckets
//! with logarithmic time ranges. Each bucket maintains a doubly-linked list of
//! segments ordered by expiration time.
//!
//! # Bucket Structure
//!
//! The 1024 buckets are organized in 4 ranges with different granularities:
//!
//! ```text
//! Range 0: Buckets 0-255   (256 buckets, 8s each)    = TTLs 0-2047s
//! Range 1: Buckets 256-511 (256 buckets, 128s each)  = TTLs 2048-34815s
//! Range 2: Buckets 512-767 (256 buckets, 2048s each) = TTLs 34816-559103s
//! Range 3: Buckets 768-1023(256 buckets, 32768s each)= TTLs 559104-8947711s
//! ```
//!
//! # Segment Chain
//!
//! Each bucket maintains a doubly-linked list of segments:
//!
//! ```text
//! TtlBucket
//! +--------+     +------------+     +------------+     +------------+
//! | head --|---->| Segment A  |<--->| Segment B  |<--->| Segment C  |
//! | tail --|------------------------------- ... ----->| (Live)     |
//! +--------+     | (Sealed)   |     | (Sealed)   |     +------------+
//!                | oldest     |     |            |     | newest     |
//!                +------------+     +------------+     +------------+
//! ```
//!
//! - **Head**: Oldest segment (expires first, evicted first)
//! - **Tail**: Newest segment (currently Live, accepting writes)

use crate::pool::RamPool;
use crate::segment::Segment;
use crate::state::{INVALID_SEGMENT_ID, State};
use crate::sync::{AtomicU32, Ordering};
use parking_lot::Mutex;
use std::time::Duration;

// TTL bucket configuration constants
const N_BUCKET_PER_STEP_N_BIT: usize = 8;
const N_BUCKET_PER_STEP: usize = 1 << N_BUCKET_PER_STEP_N_BIT; // 256 buckets per step

const TTL_BUCKET_INTERVAL_N_BIT_1: usize = 3; // 8 second intervals
const TTL_BUCKET_INTERVAL_N_BIT_2: usize = 7; // 128 second intervals
const TTL_BUCKET_INTERVAL_N_BIT_3: usize = 11; // 2048 second intervals
const TTL_BUCKET_INTERVAL_N_BIT_4: usize = 15; // 32768 second intervals

const TTL_BUCKET_INTERVAL_1: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_1; // 8
const TTL_BUCKET_INTERVAL_2: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_2; // 128
const TTL_BUCKET_INTERVAL_3: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_3; // 2048
const TTL_BUCKET_INTERVAL_4: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_4; // 32768

const TTL_BOUNDARY_1: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_1 + N_BUCKET_PER_STEP_N_BIT); // 2048
const TTL_BOUNDARY_2: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_2 + N_BUCKET_PER_STEP_N_BIT); // 32768
const TTL_BOUNDARY_3: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_3 + N_BUCKET_PER_STEP_N_BIT); // 524288

/// Total number of TTL buckets.
pub const MAX_TTL_BUCKETS: usize = N_BUCKET_PER_STEP * 4; // 1024 total buckets
const MAX_TTL_BUCKET_IDX: usize = MAX_TTL_BUCKETS - 1;

/// Error type for TTL bucket operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtlBucketError {
    /// Segment ID was invalid or not found in pool.
    InvalidSegmentId,
    /// Segment is not in the expected state.
    InvalidState,
    /// Failed to transition segment state.
    StateTransitionFailed,
    /// Bucket is empty.
    EmptyBucket,
    /// Cannot evict the only segment (need to keep tail Live).
    CannotEvictLiveSegment,
    /// Cannot remove head segment directly (use evict_head instead).
    CannotRemoveHead,
    /// Segment has active readers.
    ActiveReaders,
}

/// TTL buckets manager for organizing segments by expiration time.
pub struct TtlBuckets {
    buckets: Box<[TtlBucket]>,
}

impl TtlBuckets {
    /// Create a new set of TTL buckets.
    pub fn new() -> Self {
        let intervals = [
            TTL_BUCKET_INTERVAL_1,
            TTL_BUCKET_INTERVAL_2,
            TTL_BUCKET_INTERVAL_3,
            TTL_BUCKET_INTERVAL_4,
        ];

        let mut buckets = Vec::with_capacity(intervals.len() * N_BUCKET_PER_STEP);

        for (i, interval) in intervals.iter().enumerate() {
            for j in 0..N_BUCKET_PER_STEP {
                let ttl_secs = (interval * j + 1) as u64;
                let ttl = Duration::from_secs(ttl_secs);
                let index = (i * N_BUCKET_PER_STEP + j) as u16;
                let bucket = TtlBucket::new(ttl, index);
                buckets.push(bucket);
            }
        }

        let buckets = buckets.into_boxed_slice();
        Self { buckets }
    }

    /// Get the bucket index for a given TTL duration.
    pub fn get_bucket_index(&self, ttl: Duration) -> usize {
        let ttl_secs = ttl.as_secs() as i32;

        if ttl_secs <= 0 {
            return 0; // Use first bucket for zero/negative TTLs
        }

        // Branchless bucket index calculation
        let idx1 = (ttl_secs >> TTL_BUCKET_INTERVAL_N_BIT_1) as usize;
        let idx2 = (ttl_secs >> TTL_BUCKET_INTERVAL_N_BIT_2) as usize + N_BUCKET_PER_STEP;
        let idx3 = (ttl_secs >> TTL_BUCKET_INTERVAL_N_BIT_3) as usize + N_BUCKET_PER_STEP * 2;
        let idx4 = (ttl_secs >> TTL_BUCKET_INTERVAL_N_BIT_4) as usize + N_BUCKET_PER_STEP * 3;

        let mask1 = ((ttl_secs < TTL_BOUNDARY_1) as usize).wrapping_neg();
        let mask2 = ((TTL_BOUNDARY_1..TTL_BOUNDARY_2).contains(&ttl_secs) as usize).wrapping_neg();
        let mask3 = ((TTL_BOUNDARY_2..TTL_BOUNDARY_3).contains(&ttl_secs) as usize).wrapping_neg();
        let mask4 = ((ttl_secs >= TTL_BOUNDARY_3) as usize).wrapping_neg();

        let result = (idx1 & mask1) | (idx2 & mask2) | (idx3 & mask3) | (idx4 & mask4);
        result.min(MAX_TTL_BUCKET_IDX)
    }

    /// Get the bucket for a given TTL duration.
    pub fn get_bucket(&self, ttl: Duration) -> &TtlBucket {
        let idx = self.get_bucket_index(ttl);
        &self.buckets[idx]
    }

    /// Get a bucket by its index.
    pub fn get_bucket_by_index(&self, index: usize) -> &TtlBucket {
        &self.buckets[index.min(MAX_TTL_BUCKET_IDX)]
    }

    /// Get the number of buckets.
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Iterate over all buckets.
    pub fn iter(&self) -> impl Iterator<Item = &TtlBucket> {
        self.buckets.iter()
    }

    /// Count total segments across all buckets.
    pub fn total_segment_count(&self) -> usize {
        self.buckets.iter().map(|b| b.segment_count()).sum()
    }

    /// Select a bucket for eviction using weighted random selection.
    ///
    /// Buckets with more segments are more likely to be selected.
    /// Returns the bucket index and reference, or None if all buckets are empty.
    pub fn select_bucket_for_eviction(&self) -> Option<(usize, &TtlBucket)> {
        // First pass: count total segments
        let total: usize = self.buckets.iter().map(|b| b.segment_count()).sum();
        if total == 0 {
            return None;
        }

        // Simple random selection weighted by segment count
        // Uses current time as a simple source of randomness
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let random = (now.as_nanos() as usize) % total;

        // Walk through buckets accumulating counts until we hit the random target
        let mut accumulated = 0;
        for (idx, bucket) in self.buckets.iter().enumerate() {
            let count = bucket.segment_count();
            accumulated += count;
            if accumulated > random && count > 0 {
                // Need at least 2 segments to evict (keep tail Live)
                if count >= 2 {
                    return Some((idx, bucket));
                }
            }
        }

        // Fallback: find any bucket with >= 2 segments
        for (idx, bucket) in self.buckets.iter().enumerate() {
            if bucket.segment_count() >= 2 {
                return Some((idx, bucket));
            }
        }

        None
    }

    /// Select merge candidates from a bucket.
    ///
    /// Returns the N oldest segment IDs from the given bucket.
    /// Used for merge eviction which combines multiple segments.
    pub fn select_merge_candidates<P: RamPool>(
        &self,
        bucket_index: usize,
        count: usize,
        pool: &P,
    ) -> Vec<u32>
    where
        P::Segment: Segment,
    {
        let bucket = &self.buckets[bucket_index.min(MAX_TTL_BUCKET_IDX)];
        let mut candidates = Vec::with_capacity(count);

        let mut current = match bucket.head() {
            Some(id) => id,
            None => return candidates,
        };

        let tail = bucket.tail().unwrap_or(INVALID_SEGMENT_ID);

        while candidates.len() < count && current != INVALID_SEGMENT_ID && current != tail {
            candidates.push(current);

            if let Some(segment) = pool.get(current) {
                current = segment.next().unwrap_or(INVALID_SEGMENT_ID);
            } else {
                break;
            }
        }

        candidates
    }
}

impl Default for TtlBuckets {
    fn default() -> Self {
        Self::new()
    }
}

/// A single TTL bucket containing a chain of segments.
pub struct TtlBucket {
    /// The TTL duration for this bucket (minimum TTL for items in this bucket).
    ttl: Duration,
    /// Bucket index (0-1023).
    index: u16,
    /// Head of the segment chain (oldest segment).
    head: AtomicU32,
    /// Tail of the segment chain (newest segment).
    tail: AtomicU32,
    /// Number of segments in this bucket.
    segment_count: AtomicU32,
    /// Mutex for serializing chain modifications.
    chain_mutex: Mutex<()>,
}

impl TtlBucket {
    /// Create a new TTL bucket.
    fn new(ttl: Duration, index: u16) -> Self {
        Self {
            ttl,
            index,
            head: AtomicU32::new(INVALID_SEGMENT_ID),
            tail: AtomicU32::new(INVALID_SEGMENT_ID),
            segment_count: AtomicU32::new(0),
            chain_mutex: Mutex::new(()),
        }
    }

    /// Get the TTL duration for this bucket.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Get the bucket index.
    pub fn index(&self) -> u16 {
        self.index
    }

    /// Get the head segment ID.
    pub fn head(&self) -> Option<u32> {
        let h = self.head.load(Ordering::Acquire);
        if h == INVALID_SEGMENT_ID {
            None
        } else {
            Some(h)
        }
    }

    /// Get the tail segment ID.
    pub fn tail(&self) -> Option<u32> {
        let t = self.tail.load(Ordering::Acquire);
        if t == INVALID_SEGMENT_ID {
            None
        } else {
            Some(t)
        }
    }

    /// Get the number of segments in this bucket.
    pub fn segment_count(&self) -> usize {
        self.segment_count.load(Ordering::Relaxed) as usize
    }

    /// Check if the bucket is empty.
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == INVALID_SEGMENT_ID
    }

    /// Append a segment to the tail of this bucket's chain.
    ///
    /// The segment must be in Reserved state. It will be transitioned
    /// through Linking to Live state.
    pub fn append_segment<P: RamPool>(
        &self,
        segment_id: u32,
        pool: &P,
    ) -> Result<(), TtlBucketError>
    where
        P::Segment: Segment,
    {
        let _guard = self.chain_mutex.lock();

        let segment = pool
            .get(segment_id)
            .ok_or(TtlBucketError::InvalidSegmentId)?;

        // Verify segment is Reserved
        if segment.state() != State::Reserved {
            return Err(TtlBucketError::InvalidState);
        }

        let current_tail = self.tail.load(Ordering::Acquire);

        if current_tail == INVALID_SEGMENT_ID {
            // Empty bucket - this segment becomes both head and tail
            // Transition: Reserved -> Linking
            if !segment.cas_metadata(
                State::Reserved,
                State::Linking,
                Some(INVALID_SEGMENT_ID),
                Some(INVALID_SEGMENT_ID),
            ) {
                return Err(TtlBucketError::StateTransitionFailed);
            }

            self.head.store(segment_id, Ordering::Release);
            self.tail.store(segment_id, Ordering::Release);

            // Transition: Linking -> Live
            segment.cas_metadata(State::Linking, State::Live, None, None);
            self.segment_count.fetch_add(1, Ordering::Relaxed);

            Ok(())
        } else {
            // Non-empty bucket - append after current tail
            let tail_segment = pool
                .get(current_tail)
                .ok_or(TtlBucketError::InvalidSegmentId)?;

            // Seal the old tail if it's Live
            let old_tail_state = tail_segment.state();
            if old_tail_state == State::Live
                && !tail_segment.cas_metadata(State::Live, State::Sealed, Some(segment_id), None)
            {
                return Err(TtlBucketError::StateTransitionFailed);
            }

            // Set up new segment: Reserved -> Linking with prev pointing to old tail
            if !segment.cas_metadata(
                State::Reserved,
                State::Linking,
                Some(INVALID_SEGMENT_ID),
                Some(current_tail),
            ) {
                return Err(TtlBucketError::StateTransitionFailed);
            }

            // Update tail pointer
            self.tail.store(segment_id, Ordering::Release);

            // Transition: Linking -> Live
            segment.cas_metadata(State::Linking, State::Live, None, None);
            self.segment_count.fetch_add(1, Ordering::Relaxed);

            Ok(())
        }
    }

    /// Evict and remove the head segment from this bucket.
    ///
    /// Returns the segment ID in Draining state (ready for clearing).
    /// The caller is responsible for waiting for readers, clearing items,
    /// and transitioning to Free state.
    pub fn evict_head_segment<P: RamPool>(&self, pool: &P) -> Result<u32, TtlBucketError>
    where
        P::Segment: Segment,
    {
        let _guard = self.chain_mutex.lock();

        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        // Can't evict if empty or only one segment (need to keep Live tail)
        if head == INVALID_SEGMENT_ID {
            return Err(TtlBucketError::EmptyBucket);
        }
        if head == tail {
            return Err(TtlBucketError::CannotEvictLiveSegment);
        }

        let segment = pool.get(head).ok_or(TtlBucketError::InvalidSegmentId)?;

        // Can only evict Sealed segments
        if segment.state() != State::Sealed {
            return Err(TtlBucketError::InvalidState);
        }

        // Get next segment before we start modifying state
        let next_id = segment.next().unwrap_or(INVALID_SEGMENT_ID);

        // Transition: Sealed -> Draining
        if !segment.cas_metadata(State::Sealed, State::Draining, None, None) {
            return Err(TtlBucketError::StateTransitionFailed);
        }

        // Update chain: new head is next segment
        if next_id != INVALID_SEGMENT_ID
            && let Some(next_segment) = pool.get(next_id)
        {
            // Update next's prev pointer to INVALID (it's now the head)
            next_segment.cas_metadata(
                next_segment.state(),
                next_segment.state(),
                None,
                Some(INVALID_SEGMENT_ID),
            );
        }

        self.head.store(next_id, Ordering::Release);
        self.segment_count.fetch_sub(1, Ordering::Relaxed);

        // Clear evicted segment's chain links
        segment.cas_metadata(
            State::Draining,
            State::Draining,
            Some(INVALID_SEGMENT_ID),
            Some(INVALID_SEGMENT_ID),
        );

        Ok(head)
    }

    /// Remove a specific segment from this bucket's chain.
    ///
    /// The segment must be in Sealed state and must not be the head.
    /// For head eviction, use `evict_head_segment` instead.
    ///
    /// Returns the segment ID in Draining state.
    pub fn remove_segment<P: RamPool>(
        &self,
        segment_id: u32,
        pool: &P,
    ) -> Result<u32, TtlBucketError>
    where
        P::Segment: Segment,
    {
        let _guard = self.chain_mutex.lock();

        let head = self.head.load(Ordering::Acquire);
        if segment_id == head {
            return Err(TtlBucketError::CannotRemoveHead);
        }

        let segment = pool
            .get(segment_id)
            .ok_or(TtlBucketError::InvalidSegmentId)?;

        // Must be Sealed
        if segment.state() != State::Sealed {
            return Err(TtlBucketError::InvalidState);
        }

        // Get chain links
        let prev_id = segment.prev().unwrap_or(INVALID_SEGMENT_ID);
        let next_id = segment.next().unwrap_or(INVALID_SEGMENT_ID);

        // Transition: Sealed -> Draining
        if !segment.cas_metadata(State::Sealed, State::Draining, None, None) {
            return Err(TtlBucketError::StateTransitionFailed);
        }

        // Update prev's next pointer
        if prev_id != INVALID_SEGMENT_ID
            && let Some(prev_segment) = pool.get(prev_id)
        {
            prev_segment.cas_metadata(
                prev_segment.state(),
                prev_segment.state(),
                Some(next_id),
                None,
            );
        }

        // Update next's prev pointer
        if next_id != INVALID_SEGMENT_ID {
            if let Some(next_segment) = pool.get(next_id) {
                next_segment.cas_metadata(
                    next_segment.state(),
                    next_segment.state(),
                    None,
                    Some(prev_id),
                );
            }
        } else {
            // This was the tail
            self.tail.store(prev_id, Ordering::Release);
        }

        self.segment_count.fetch_sub(1, Ordering::Relaxed);

        // Clear evicted segment's chain links
        segment.cas_metadata(
            State::Draining,
            State::Draining,
            Some(INVALID_SEGMENT_ID),
            Some(INVALID_SEGMENT_ID),
        );

        Ok(segment_id)
    }

    /// Replace two adjacent segments with a single spare segment.
    ///
    /// This is used for segment compaction. The two source segments (src_a and src_b)
    /// must be adjacent in the chain (src_a.next == src_b), and src_b must not be the tail.
    ///
    /// After this operation:
    /// - src_a and src_b are transitioned to AwaitingRelease state
    /// - spare is inserted in their place in the chain with state Sealed
    /// - Chain pointers are updated atomically
    ///
    /// # Arguments
    /// * `src_a_id` - First segment to replace (the older one, closer to head)
    /// * `src_b_id` - Second segment to replace (src_a.next)
    /// * `spare_id` - The spare segment to insert in their place
    /// * `pool` - The segment pool
    ///
    /// # Returns
    /// `Ok(())` on success, `Err` with reason on failure.
    pub fn replace_adjacent_segments<P: RamPool>(
        &self,
        src_a_id: u32,
        src_b_id: u32,
        spare_id: u32,
        pool: &P,
    ) -> Result<(), TtlBucketError>
    where
        P::Segment: Segment,
    {
        let _guard = self.chain_mutex.lock();

        let src_a = pool.get(src_a_id).ok_or(TtlBucketError::InvalidSegmentId)?;
        let src_b = pool.get(src_b_id).ok_or(TtlBucketError::InvalidSegmentId)?;
        let spare = pool.get(spare_id).ok_or(TtlBucketError::InvalidSegmentId)?;

        // Verify src_a and src_b are adjacent
        if src_a.next() != Some(src_b_id) {
            return Err(TtlBucketError::InvalidState);
        }

        // Both must be in Relinking state (set by caller after copying items)
        if src_a.state() != State::Relinking || src_b.state() != State::Relinking {
            return Err(TtlBucketError::InvalidState);
        }

        // src_b cannot be the tail (tail is Live)
        let tail = self.tail.load(Ordering::Acquire);
        if src_b_id == tail {
            return Err(TtlBucketError::InvalidState);
        }

        // Spare must be in Reserved or Sealed state
        let spare_state = spare.state();
        if spare_state != State::Reserved && spare_state != State::Sealed {
            return Err(TtlBucketError::InvalidState);
        }

        // Get chain links
        let prev_of_a = src_a.prev().unwrap_or(INVALID_SEGMENT_ID);
        let next_of_b = src_b.next().unwrap_or(INVALID_SEGMENT_ID);

        // Set up spare's chain pointers and transition to Sealed
        if !spare.cas_metadata(spare_state, State::Sealed, Some(next_of_b), Some(prev_of_a)) {
            return Err(TtlBucketError::StateTransitionFailed);
        }

        // Update prev segment's next pointer (or head if src_a was head)
        let head = self.head.load(Ordering::Acquire);
        if src_a_id == head {
            self.head.store(spare_id, Ordering::Release);
        } else if prev_of_a != INVALID_SEGMENT_ID
            && let Some(prev_segment) = pool.get(prev_of_a)
        {
            prev_segment.cas_metadata(
                prev_segment.state(),
                prev_segment.state(),
                Some(spare_id),
                None,
            );
        }

        // Update next segment's prev pointer
        if next_of_b != INVALID_SEGMENT_ID
            && let Some(next_segment) = pool.get(next_of_b)
        {
            next_segment.cas_metadata(
                next_segment.state(),
                next_segment.state(),
                None,
                Some(spare_id),
            );
        }

        // Transition src_a and src_b to AwaitingRelease
        // Clear their chain pointers
        src_a.cas_metadata(
            State::Relinking,
            State::AwaitingRelease,
            Some(INVALID_SEGMENT_ID),
            Some(INVALID_SEGMENT_ID),
        );
        src_b.cas_metadata(
            State::Relinking,
            State::AwaitingRelease,
            Some(INVALID_SEGMENT_ID),
            Some(INVALID_SEGMENT_ID),
        );

        // Update segment count: removed 2, added 1 = net -1
        self.segment_count.fetch_sub(1, Ordering::Relaxed);

        Ok(())
    }

    /// Walk the chain and count segments (up to max_count).
    pub fn chain_len<P: RamPool>(&self, max_count: usize, pool: &P) -> usize
    where
        P::Segment: Segment,
    {
        let head = match self.head() {
            Some(h) => h,
            None => return 0,
        };

        let mut count = 0;
        let mut current = head;

        while current != INVALID_SEGMENT_ID && count < max_count {
            count += 1;
            if let Some(segment) = pool.get(current) {
                match segment.next() {
                    Some(next) if next != INVALID_SEGMENT_ID => current = next,
                    _ => break,
                }
            } else {
                break;
            }
        }

        count
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::memory_pool::MemoryPoolBuilder;

    fn create_test_pool() -> crate::memory_pool::MemoryPool {
        MemoryPoolBuilder::new(0)
            .per_item_ttl(false)
            .segment_size(64 * 1024) // 64KB
            .heap_size(640 * 1024) // 640KB = 10 segments
            .spare_capacity(0) // No spare capacity for tests
            .build()
            .expect("Failed to create test pool")
    }

    #[test]
    fn test_bucket_index_calculation() {
        let buckets = TtlBuckets::new();

        // Test various TTLs map to expected bucket ranges
        assert!(buckets.get_bucket_index(Duration::from_secs(1)) < 256);
        assert!(buckets.get_bucket_index(Duration::from_secs(100)) < 256);
        assert!(buckets.get_bucket_index(Duration::from_secs(2048)) >= 256);
        assert!(buckets.get_bucket_index(Duration::from_secs(2048)) < 512);
    }

    #[test]
    fn test_ttl_buckets_creation() {
        let buckets = TtlBuckets::new();
        assert_eq!(buckets.bucket_count(), MAX_TTL_BUCKETS);
    }

    #[test]
    fn test_empty_bucket() {
        let bucket = TtlBucket::new(Duration::from_secs(8), 0);
        assert!(bucket.is_empty());
        assert_eq!(bucket.head(), None);
        assert_eq!(bucket.tail(), None);
        assert_eq!(bucket.segment_count(), 0);
    }

    #[test]
    fn test_bucket_ttl_and_index() {
        let bucket = TtlBucket::new(Duration::from_secs(128), 16);
        assert_eq!(bucket.ttl(), Duration::from_secs(128));
        assert_eq!(bucket.index(), 16);
    }

    #[test]
    fn test_bucket_index_boundary_conditions() {
        let buckets = TtlBuckets::new();

        // Zero TTL should map to first bucket
        assert_eq!(buckets.get_bucket_index(Duration::from_secs(0)), 0);

        // Very large TTL should map to last bucket
        let large_ttl = Duration::from_secs(10_000_000);
        assert!(buckets.get_bucket_index(large_ttl) <= MAX_TTL_BUCKET_IDX);
    }

    #[test]
    fn test_total_segment_count_empty() {
        let buckets = TtlBuckets::new();
        assert_eq!(buckets.total_segment_count(), 0);
    }

    #[test]
    fn test_default() {
        let buckets = TtlBuckets::default();
        assert_eq!(buckets.bucket_count(), MAX_TTL_BUCKETS);
    }

    #[test]
    fn test_get_bucket() {
        let buckets = TtlBuckets::new();
        let bucket = buckets.get_bucket(Duration::from_secs(100));
        assert!(bucket.is_empty());
    }

    #[test]
    fn test_get_bucket_by_index() {
        let buckets = TtlBuckets::new();
        let bucket = buckets.get_bucket_by_index(10);
        assert_eq!(bucket.index(), 10);
    }

    #[test]
    fn test_get_bucket_by_index_clamped() {
        let buckets = TtlBuckets::new();
        // Index beyond range should be clamped
        let bucket = buckets.get_bucket_by_index(9999);
        assert_eq!(bucket.index() as usize, MAX_TTL_BUCKET_IDX);
    }

    #[test]
    fn test_iter() {
        let buckets = TtlBuckets::new();
        let count = buckets.iter().count();
        assert_eq!(count, MAX_TTL_BUCKETS);
    }

    #[test]
    fn test_append_first_segment() {
        let pool = create_test_pool();
        let bucket = TtlBucket::new(Duration::from_secs(100), 12);

        let seg_id = pool.reserve().expect("Should reserve segment");
        bucket
            .append_segment(seg_id, &pool)
            .expect("Append should succeed");

        assert!(!bucket.is_empty());
        assert_eq!(bucket.head(), Some(seg_id));
        assert_eq!(bucket.tail(), Some(seg_id));
        assert_eq!(bucket.segment_count(), 1);

        // Segment should be Live
        let segment = pool.get(seg_id).unwrap();
        assert_eq!(segment.state(), State::Live);
    }

    #[test]
    fn test_append_multiple_segments() {
        let pool = create_test_pool();
        let bucket = TtlBucket::new(Duration::from_secs(100), 12);

        let seg1 = pool.reserve().unwrap();
        let seg2 = pool.reserve().unwrap();
        let seg3 = pool.reserve().unwrap();

        bucket.append_segment(seg1, &pool).unwrap();
        bucket.append_segment(seg2, &pool).unwrap();
        bucket.append_segment(seg3, &pool).unwrap();

        assert_eq!(bucket.segment_count(), 3);
        assert_eq!(bucket.head(), Some(seg1));
        assert_eq!(bucket.tail(), Some(seg3));

        // First should be Sealed
        let segment1 = pool.get(seg1).unwrap();
        assert_eq!(segment1.state(), State::Sealed);

        // Tail should be Live
        let segment3 = pool.get(seg3).unwrap();
        assert_eq!(segment3.state(), State::Live);
    }

    #[test]
    fn test_append_invalid_segment_id() {
        let pool = create_test_pool();
        let bucket = TtlBucket::new(Duration::from_secs(100), 12);

        let result = bucket.append_segment(999, &pool);
        assert_eq!(result, Err(TtlBucketError::InvalidSegmentId));
    }

    #[test]
    fn test_append_wrong_state() {
        let pool = create_test_pool();
        let bucket = TtlBucket::new(Duration::from_secs(100), 12);

        let seg_id = pool.reserve().unwrap();
        pool.release(seg_id); // Put back to Free

        let result = bucket.append_segment(seg_id, &pool);
        assert_eq!(result, Err(TtlBucketError::InvalidState));
    }

    #[test]
    fn test_evict_head_empty_bucket() {
        let pool = create_test_pool();
        let bucket = TtlBucket::new(Duration::from_secs(100), 12);

        let result = bucket.evict_head_segment(&pool);
        assert_eq!(result, Err(TtlBucketError::EmptyBucket));
    }

    #[test]
    fn test_evict_head_single_segment() {
        let pool = create_test_pool();
        let bucket = TtlBucket::new(Duration::from_secs(100), 12);

        let seg_id = pool.reserve().unwrap();
        bucket.append_segment(seg_id, &pool).unwrap();

        // Cannot evict single segment (it's Live)
        let result = bucket.evict_head_segment(&pool);
        assert_eq!(result, Err(TtlBucketError::CannotEvictLiveSegment));
    }

    #[test]
    fn test_evict_head_segment() {
        let pool = create_test_pool();
        let bucket = TtlBucket::new(Duration::from_secs(100), 12);

        let seg1 = pool.reserve().unwrap();
        let seg2 = pool.reserve().unwrap();

        bucket.append_segment(seg1, &pool).unwrap();
        bucket.append_segment(seg2, &pool).unwrap();

        let evicted = bucket
            .evict_head_segment(&pool)
            .expect("Evict should succeed");
        assert_eq!(evicted, seg1);
        assert_eq!(bucket.segment_count(), 1);
        assert_eq!(bucket.head(), Some(seg2));
        assert_eq!(bucket.tail(), Some(seg2));

        // Evicted segment should be Draining
        let segment = pool.get(seg1).unwrap();
        assert_eq!(segment.state(), State::Draining);
    }

    #[test]
    fn test_chain_len() {
        let pool = create_test_pool();
        let bucket = TtlBucket::new(Duration::from_secs(100), 12);

        assert_eq!(bucket.chain_len(10, &pool), 0);

        let seg1 = pool.reserve().unwrap();
        let seg2 = pool.reserve().unwrap();
        let seg3 = pool.reserve().unwrap();

        bucket.append_segment(seg1, &pool).unwrap();
        assert_eq!(bucket.chain_len(10, &pool), 1);

        bucket.append_segment(seg2, &pool).unwrap();
        assert_eq!(bucket.chain_len(10, &pool), 2);

        bucket.append_segment(seg3, &pool).unwrap();
        assert_eq!(bucket.chain_len(10, &pool), 3);

        // Test max_count limit
        assert_eq!(bucket.chain_len(2, &pool), 2);
    }

    #[test]
    fn test_select_bucket_for_eviction_empty() {
        let buckets = TtlBuckets::new();
        assert!(buckets.select_bucket_for_eviction().is_none());
    }

    #[test]
    fn test_select_merge_candidates_empty() {
        let pool = create_test_pool();
        let buckets = TtlBuckets::new();

        let candidates = buckets.select_merge_candidates(0, 5, &pool);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_select_merge_candidates() {
        let pool = create_test_pool();
        let buckets = TtlBuckets::new();
        let bucket = buckets.get_bucket(Duration::from_secs(100));

        let seg1 = pool.reserve().unwrap();
        let seg2 = pool.reserve().unwrap();
        let seg3 = pool.reserve().unwrap();

        bucket.append_segment(seg1, &pool).unwrap();
        bucket.append_segment(seg2, &pool).unwrap();
        bucket.append_segment(seg3, &pool).unwrap();

        let bucket_idx = buckets.get_bucket_index(Duration::from_secs(100));
        let candidates = buckets.select_merge_candidates(bucket_idx, 2, &pool);

        // Should get first 2 segments (not tail which is Live)
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0], seg1);
        assert_eq!(candidates[1], seg2);
    }

    #[test]
    fn test_remove_segment() {
        let pool = create_test_pool();
        let bucket = TtlBucket::new(Duration::from_secs(100), 12);

        let seg1 = pool.reserve().unwrap();
        let seg2 = pool.reserve().unwrap();
        let seg3 = pool.reserve().unwrap();

        bucket.append_segment(seg1, &pool).unwrap();
        bucket.append_segment(seg2, &pool).unwrap();
        bucket.append_segment(seg3, &pool).unwrap();

        // Remove middle segment (seg2)
        let removed = bucket
            .remove_segment(seg2, &pool)
            .expect("Remove should succeed");
        assert_eq!(removed, seg2);
        assert_eq!(bucket.segment_count(), 2);

        // Removed segment should be Draining
        let segment = pool.get(seg2).unwrap();
        assert_eq!(segment.state(), State::Draining);
    }

    #[test]
    fn test_remove_segment_cannot_remove_head() {
        let pool = create_test_pool();
        let bucket = TtlBucket::new(Duration::from_secs(100), 12);

        let seg1 = pool.reserve().unwrap();
        let seg2 = pool.reserve().unwrap();

        bucket.append_segment(seg1, &pool).unwrap();
        bucket.append_segment(seg2, &pool).unwrap();

        // Cannot remove head directly
        let result = bucket.remove_segment(seg1, &pool);
        assert_eq!(result, Err(TtlBucketError::CannotRemoveHead));
    }

    #[test]
    fn test_remove_segment_invalid_id() {
        let pool = create_test_pool();
        let bucket = TtlBucket::new(Duration::from_secs(100), 12);

        let result = bucket.remove_segment(999, &pool);
        assert_eq!(result, Err(TtlBucketError::InvalidSegmentId));
    }

    #[test]
    fn test_ttl_bucket_error_variants() {
        assert_eq!(
            TtlBucketError::InvalidSegmentId,
            TtlBucketError::InvalidSegmentId
        );
        assert_ne!(
            TtlBucketError::InvalidSegmentId,
            TtlBucketError::InvalidState
        );
        assert_ne!(
            TtlBucketError::StateTransitionFailed,
            TtlBucketError::EmptyBucket
        );
        assert_ne!(
            TtlBucketError::CannotEvictLiveSegment,
            TtlBucketError::CannotRemoveHead
        );
        assert_ne!(
            TtlBucketError::ActiveReaders,
            TtlBucketError::InvalidSegmentId
        );
    }

    #[test]
    fn test_ttl_bucket_error_debug() {
        let err = TtlBucketError::InvalidSegmentId;
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("InvalidSegmentId"));
    }

    #[test]
    fn test_bucket_index_all_ranges() {
        let buckets = TtlBuckets::new();

        // Range 0: 0-2047s (buckets 0-255)
        let idx = buckets.get_bucket_index(Duration::from_secs(16));
        assert!(idx < 256, "16s should be in range 0, got {}", idx);

        // Range 1: 2048-34815s (buckets 256-511)
        let idx = buckets.get_bucket_index(Duration::from_secs(5000));
        assert!(
            (256..512).contains(&idx),
            "5000s should be in range 1, got {}",
            idx
        );

        // Range 2: 34816-559103s (buckets 512-767)
        let idx = buckets.get_bucket_index(Duration::from_secs(100000));
        assert!(
            (512..768).contains(&idx),
            "100000s should be in range 2, got {}",
            idx
        );

        // Range 3: 559104+ (buckets 768-1023)
        let idx = buckets.get_bucket_index(Duration::from_secs(600000));
        assert!(
            (768..1024).contains(&idx),
            "600000s should be in range 3, got {}",
            idx
        );
    }
}
