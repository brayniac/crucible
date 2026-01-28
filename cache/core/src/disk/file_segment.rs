//! File-backed segment implementation.
//!
//! [`FileSegment`] wraps a [`SliceSegment`] that points to memory-mapped
//! file storage instead of heap-allocated memory.

use crate::error::CacheError;
use crate::item::BasicItemGuard;
use crate::segment::{
    PruneCollectingResult, Segment, SegmentGuard, SegmentKeyVerify, SegmentPrune,
};
use crate::slice_segment::SliceSegment;
use crate::state::State;
use crate::sync::*;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

/// A segment backed by memory-mapped file storage.
///
/// FileSegment is a thin wrapper around [`SliceSegment`] that adds:
/// - Dirty tracking for knowing when segments need to be synced
/// - The underlying memory points to an mmap'd region of a file
///
/// # Thread Safety
///
/// FileSegment inherits thread safety from SliceSegment.
/// All mutable state uses atomic operations.
pub struct FileSegment<'a> {
    /// The underlying slice segment pointing to mmap memory.
    inner: SliceSegment<'a>,

    /// Whether this segment has been modified since last sync.
    dirty: AtomicBool,
}

// SAFETY: FileSegment synchronizes access via atomics (inherited from SliceSegment)
unsafe impl<'a> Send for FileSegment<'a> {}
unsafe impl<'a> Sync for FileSegment<'a> {}

impl<'a> FileSegment<'a> {
    /// Create a new file segment from mmap'd memory.
    ///
    /// # Safety
    ///
    /// - `data` must point to at least `len` bytes of valid mmap'd memory
    /// - The memory must remain valid for the lifetime `'a`
    /// - The memory must be properly aligned (8-byte minimum)
    ///
    /// # Parameters
    /// - `pool_id`: Pool ID (0-3)
    /// - `is_per_item_ttl`: If true, use per-item TTL headers
    /// - `id`: Segment ID within the pool
    /// - `data`: Pointer to mmap'd segment memory
    /// - `len`: Size of segment memory in bytes
    pub unsafe fn new(
        pool_id: u8,
        is_per_item_ttl: bool,
        id: u32,
        data: *mut u8,
        len: usize,
    ) -> Self {
        // SAFETY: Caller ensures data pointer is valid and has proper alignment.
        // The SliceSegment::new call requires the same safety guarantees.
        Self {
            inner: unsafe { SliceSegment::new(pool_id, is_per_item_ttl, id, data, len) },
            dirty: AtomicBool::new(false),
        }
    }

    /// Check if this segment uses per-item TTL.
    #[inline]
    pub fn is_per_item_ttl(&self) -> bool {
        self.inner.is_per_item_ttl()
    }

    /// Get the raw data pointer.
    #[inline]
    pub fn data_ptr(&self) -> *mut u8 {
        self.inner.data_ptr()
    }

    /// Check if this segment has been modified since last sync.
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Mark this segment as dirty (modified).
    #[inline]
    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    /// Clear the dirty flag (after sync).
    #[inline]
    pub fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    /// Get access to the inner SliceSegment.
    #[inline]
    pub fn inner(&self) -> &SliceSegment<'a> {
        &self.inner
    }

    /// Force the segment back to Free state.
    ///
    /// This is used during recovery to reset segment state.
    pub fn force_free(&self) {
        self.inner.force_free();
    }
}

// Delegate SegmentKeyVerify to inner
impl SegmentKeyVerify for FileSegment<'_> {
    fn verify_key_at_offset(&self, offset: u32, key: &[u8], allow_deleted: bool) -> bool {
        self.inner.verify_key_at_offset(offset, key, allow_deleted)
    }

    fn verify_key_with_header(
        &self,
        offset: u32,
        key: &[u8],
        allow_deleted: bool,
    ) -> Option<(u8, u8, u32)> {
        self.inner
            .verify_key_with_header(offset, key, allow_deleted)
    }

    fn verify_key_unexpired(&self, offset: u32, key: &[u8], now: u32) -> Option<(u8, u8, u32)> {
        self.inner.verify_key_unexpired(offset, key, now)
    }
}

// Delegate Segment trait to inner SliceSegment
impl Segment for FileSegment<'_> {
    #[inline]
    fn id(&self) -> u32 {
        self.inner.id()
    }

    #[inline]
    fn pool_id(&self) -> u8 {
        self.inner.pool_id()
    }

    #[inline]
    fn generation(&self) -> u16 {
        self.inner.generation()
    }

    #[inline]
    fn increment_generation(&self) {
        self.inner.increment_generation();
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[inline]
    fn write_offset(&self) -> u32 {
        self.inner.write_offset()
    }

    #[inline]
    fn live_items(&self) -> u32 {
        self.inner.live_items()
    }

    #[inline]
    fn live_bytes(&self) -> u32 {
        self.inner.live_bytes()
    }

    #[inline]
    fn ref_count(&self) -> u32 {
        self.inner.ref_count()
    }

    #[inline]
    fn state(&self) -> State {
        self.inner.state()
    }

    #[inline]
    fn try_reserve(&self) -> bool {
        let result = self.inner.try_reserve();
        if result {
            self.mark_dirty();
        }
        result
    }

    #[inline]
    fn try_release(&self) -> bool {
        self.inner.try_release()
    }

    #[inline]
    fn cas_metadata(
        &self,
        expected_state: State,
        new_state: State,
        new_next: Option<u32>,
        new_prev: Option<u32>,
    ) -> bool {
        self.inner
            .cas_metadata(expected_state, new_state, new_next, new_prev)
    }

    #[inline]
    fn next(&self) -> Option<u32> {
        self.inner.next()
    }

    #[inline]
    fn prev(&self) -> Option<u32> {
        self.inner.prev()
    }

    #[inline]
    fn expire_at(&self) -> u32 {
        self.inner.expire_at()
    }

    #[inline]
    fn set_expire_at(&self, expire_at: u32) {
        self.inner.set_expire_at(expire_at);
        self.mark_dirty();
    }

    #[inline]
    fn item_ttl(&self, offset: u32, now: u32) -> Option<Duration> {
        self.inner.item_ttl(offset, now)
    }

    #[inline]
    fn bucket_id(&self) -> Option<u16> {
        self.inner.bucket_id()
    }

    #[inline]
    fn set_bucket_id(&self, bucket_id: u16) {
        self.inner.set_bucket_id(bucket_id);
    }

    #[inline]
    fn clear_bucket_id(&self) {
        self.inner.clear_bucket_id();
    }

    fn data_slice(&self, offset: u32, len: usize) -> Option<&[u8]> {
        self.inner.data_slice(offset, len)
    }

    fn append_item(&self, key: &[u8], value: &[u8], optional: &[u8]) -> Option<u32> {
        let result = self.inner.append_item(key, value, optional);
        if result.is_some() {
            self.mark_dirty();
        }
        result
    }

    fn append_item_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        expire_at: u32,
    ) -> Option<u32> {
        let result = self
            .inner
            .append_item_with_ttl(key, value, optional, expire_at);
        if result.is_some() {
            self.mark_dirty();
        }
        result
    }

    fn begin_append_with_ttl(
        &self,
        key: &[u8],
        value_len: usize,
        optional: &[u8],
        expire_at: u32,
    ) -> Option<(u32, u32, *mut u8)> {
        let result = self
            .inner
            .begin_append_with_ttl(key, value_len, optional, expire_at);
        if result.is_some() {
            self.mark_dirty();
        }
        result
    }

    fn begin_append(
        &self,
        key: &[u8],
        value_len: usize,
        optional: &[u8],
    ) -> Option<(u32, u32, *mut u8)> {
        let result = self.inner.begin_append(key, value_len, optional);
        if result.is_some() {
            self.mark_dirty();
        }
        result
    }

    fn finalize_append(&self, item_size: u32) {
        self.inner.finalize_append(item_size);
    }

    fn mark_deleted_at_offset(&self, offset: u32) {
        self.inner.mark_deleted_at_offset(offset);
        self.mark_dirty();
    }

    fn mark_deleted(&self, offset: u32, key: &[u8]) -> Result<bool, CacheError> {
        let result = self.inner.mark_deleted(offset, key)?;
        if result {
            self.mark_dirty();
        }
        Ok(result)
    }

    #[inline]
    fn merge_count(&self) -> u16 {
        self.inner.merge_count()
    }

    #[inline]
    fn increment_merge_count(&self) {
        self.inner.increment_merge_count();
    }

    fn reset(&self) {
        self.inner.reset();
        self.clear_dirty();
    }
}

// Delegate SegmentGuard to inner
impl<'a> SegmentGuard for FileSegment<'a> {
    type Guard<'b>
        = BasicItemGuard<'b>
    where
        Self: 'b;

    fn get_item(&self, offset: u32, key: &[u8]) -> Result<Self::Guard<'_>, CacheError> {
        self.inner.get_item(offset, key)
    }

    fn get_item_verified(
        &self,
        offset: u32,
        header_info: (u8, u8, u32),
    ) -> Result<Self::Guard<'_>, CacheError> {
        self.inner.get_item_verified(offset, header_info)
    }
}

// Delegate SegmentPrune to inner
impl SegmentPrune for FileSegment<'_> {
    fn prune<F>(&self, threshold: u8, get_frequency: F) -> (u32, u32, u32, u32)
    where
        F: Fn(&[u8]) -> Option<u8>,
    {
        let result = self.inner.prune(threshold, get_frequency);
        if result.1 > 0 {
            // Items were pruned
            self.mark_dirty();
        }
        result
    }

    fn prune_collecting<F>(&self, threshold: u8, get_frequency: F) -> PruneCollectingResult
    where
        F: Fn(&[u8]) -> Option<u8>,
    {
        let result = self.inner.prune_collecting(threshold, get_frequency);
        if result.1 > 0 {
            self.mark_dirty();
        }
        result
    }
}

impl FileSegment<'_> {
    /// Get raw value reference pointers for zero-copy scatter-gather I/O.
    ///
    /// Returns the raw components needed to construct a `ValueRef`:
    /// - `ref_count_ptr`: Pointer to segment's ref_count (already incremented)
    /// - `value_ptr`: Pointer to value bytes in segment memory
    /// - `value_len`: Length of the value
    ///
    /// Returns `Err` if the item is not found or not accessible.
    pub fn get_value_ref_raw(
        &self,
        offset: u32,
        key: &[u8],
    ) -> Result<(*const crate::sync::AtomicU32, *const u8, usize), CacheError> {
        self.inner.get_value_ref_raw(offset, key)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    fn create_test_segment() -> (Vec<u8>, FileSegment<'static>) {
        let mut data = vec![0u8; 64 * 1024]; // 64KB
        let ptr = data.as_mut_ptr();
        let len = data.len();

        let segment = unsafe { FileSegment::new(2, false, 0, ptr, len) };

        (data, segment)
    }

    #[test]
    fn test_file_segment_creation() {
        let (_data, segment) = create_test_segment();
        assert_eq!(segment.pool_id(), 2);
        assert_eq!(segment.id(), 0);
        assert_eq!(segment.capacity(), 64 * 1024);
        assert!(!segment.is_per_item_ttl());
    }

    #[test]
    fn test_dirty_tracking() {
        let (_data, segment) = create_test_segment();

        // Initially not dirty
        assert!(!segment.is_dirty());

        // Mark dirty
        segment.mark_dirty();
        assert!(segment.is_dirty());

        // Clear dirty
        segment.clear_dirty();
        assert!(!segment.is_dirty());
    }

    #[test]
    fn test_append_marks_dirty() {
        let (_data, segment) = create_test_segment();

        // Reserve the segment first
        assert!(segment.try_reserve());
        segment.clear_dirty(); // Clear the dirty flag from reservation

        // Transition to Live state (segments must be Live to accept writes)
        segment.cas_metadata(State::Reserved, State::Live, None, None);

        // Append should mark dirty
        let result = segment.append_item(b"key", b"value", b"");
        assert!(result.is_some());
        assert!(segment.is_dirty());
    }

    #[test]
    fn test_state_operations() {
        let (_data, segment) = create_test_segment();

        assert_eq!(segment.state(), State::Free);

        // Reserve
        assert!(segment.try_reserve());
        assert_eq!(segment.state(), State::Reserved);

        // Release
        assert!(segment.try_release());
        assert_eq!(segment.state(), State::Free);
    }
}
