//! Pool traits for segment allocation.
//!
//! This module provides the core abstraction for segment pools:
//!
//! - [`RamPool`]: Trait for RAM-based pools with direct memory access
//!
//! Pools are allocation-only - they manage segment lifecycle but do not
//! handle eviction. Eviction logic belongs at the layer level where the
//! hashtable and organization strategies are available.

use crate::segment::{Segment, SegmentKeyVerify};

/// Trait for RAM-based pools with direct memory access.
///
/// RAM pools provide zero-copy access to segment data through references.
/// This is a simple, generic pool interface that only handles allocation
/// and deallocation of segments.
///
/// # Example
///
/// ```ignore
/// let pool: impl RamPool = MemoryPoolBuilder::new(0)
///     .heap_size(64 * 1024 * 1024)
///     .build()?;
///
/// // Reserve a segment
/// let seg_id = pool.reserve()?;
/// let segment = pool.get(seg_id)?;
///
/// // Use the segment...
///
/// // Release when done
/// pool.release(seg_id);
/// ```
pub trait RamPool: Send + Sync {
    /// The segment type for this pool.
    type Segment: Segment + SegmentKeyVerify;

    /// Get the pool ID (0-3).
    fn pool_id(&self) -> u8;

    /// Get a reference to a segment by ID.
    ///
    /// Returns `None` if the segment ID is out of bounds.
    fn get(&self, id: u32) -> Option<&Self::Segment>;

    /// Get the total number of segments in this pool.
    fn segment_count(&self) -> usize;

    /// Get the size of each segment in bytes.
    fn segment_size(&self) -> usize;

    /// Reserve a segment from the pool.
    ///
    /// The segment transitions from `Free` to `Reserved` state.
    ///
    /// Returns `Some(segment_id)` if a segment was available, `None` if empty.
    fn reserve(&self) -> Option<u32>;

    /// Release a segment back to the pool.
    ///
    /// The segment transitions to `Free` state and becomes available for reuse.
    ///
    /// # Panics
    ///
    /// Panics if the segment ID is out of bounds.
    fn release(&self, id: u32);

    /// Get the approximate number of free segments.
    ///
    /// Note: Due to concurrent modifications, this is approximate.
    fn free_count(&self) -> usize;

    /// Get an iterator over all segment IDs.
    fn segment_ids(&self) -> Box<dyn Iterator<Item = u32>> {
        Box::new(0..self.segment_count() as u32)
    }
}
