//! Page-aligned buffer pool for io_uring disk I/O.
//!
//! Provides two buffer pools:
//! - [`AlignedBufferPool`]: Large segment-sized buffers for write staging
//! - [`ReadBufferPool`]: Small block-sized buffers for read staging
//!
//! Both pools allocate a single contiguous aligned region and carve it
//! into fixed-size slots, with O(1) alloc/free via a stack-based free list.

use std::alloc::{self, Layout};
use std::ptr;

/// A page-aligned buffer from an [`AlignedBufferPool`].
///
/// This buffer is backed by memory from the pool's contiguous allocation.
/// It must be returned to the pool via [`AlignedBufferPool::release`] when
/// no longer needed.
pub struct AlignedBuffer {
    ptr: *mut u8,
    capacity: usize,
    slot: u32,
}

// SAFETY: The buffer points to a stable allocation that doesn't move.
// Only one AlignedBuffer exists per slot at a time (enforced by the pool).
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Get a pointer to the buffer data.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// Get a mutable pointer to the buffer data.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    /// Get the buffer capacity in bytes.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the slot index (for returning to pool).
    #[inline]
    pub fn slot(&self) -> u32 {
        self.slot
    }

    /// Get the buffer address as u64 (for io_uring submissions).
    #[inline]
    pub fn addr(&self) -> u64 {
        self.ptr as u64
    }

    /// Get the buffer as a byte slice up to the given length.
    ///
    /// # Safety
    ///
    /// The caller must ensure `len <= capacity` and that the bytes
    /// have been initialized up to `len`.
    #[inline]
    pub unsafe fn as_slice(&self, len: usize) -> &[u8] {
        debug_assert!(len <= self.capacity);
        unsafe { std::slice::from_raw_parts(self.ptr, len) }
    }

    /// Get the buffer as a mutable byte slice.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: The entire capacity is valid writable memory from the pool.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.capacity) }
    }
}

/// Pool of page-aligned buffers for disk I/O.
///
/// Allocates a single contiguous memory region with the specified alignment
/// (typically 4096 for O_DIRECT) and divides it into equal-sized slots.
/// Allocation and deallocation are O(1) via a stack-based free list.
///
/// # Usage
///
/// - **Write buffers**: Use segment-sized slots to stage segment data before
///   flushing to disk via io_uring.
/// - **Read buffers**: Use block-sized slots (4KB-8KB) for staging disk reads.
pub struct AlignedBufferPool {
    /// Single contiguous aligned allocation.
    memory: *mut u8,
    /// Layout used for the allocation (for dealloc).
    layout: Layout,
    /// Bytes per slot (rounded up to alignment).
    slot_size: usize,
    /// Number of slots.
    slot_count: usize,
    /// Stack-based free list (indices of available slots).
    free_list: Vec<u32>,
    /// Alignment in bytes (typically 4096).
    alignment: usize,
}

// SAFETY: The pool owns its allocation. Slots are exclusively borrowed
// via AlignedBuffer and returned via release(). No concurrent access
// to the same slot is possible.
unsafe impl Send for AlignedBufferPool {}

impl AlignedBufferPool {
    /// Create a new aligned buffer pool.
    ///
    /// # Parameters
    /// - `slot_count`: Number of buffers in the pool
    /// - `slot_size`: Size of each buffer in bytes (rounded up to alignment)
    /// - `alignment`: Memory alignment in bytes (typically 4096 for O_DIRECT)
    ///
    /// # Panics
    ///
    /// Panics if `slot_count` is 0, `slot_size` is 0, or `alignment` is not
    /// a power of two.
    pub fn new(slot_count: usize, slot_size: usize, alignment: usize) -> Self {
        assert!(slot_count > 0, "slot_count must be > 0");
        assert!(slot_size > 0, "slot_size must be > 0");
        assert!(
            alignment.is_power_of_two(),
            "alignment must be a power of two"
        );

        // Round slot_size up to alignment boundary
        let slot_size = (slot_size + alignment - 1) & !(alignment - 1);

        let total_size = slot_count
            .checked_mul(slot_size)
            .expect("total pool size overflow");

        let layout = Layout::from_size_align(total_size, alignment)
            .expect("invalid layout for aligned buffer pool");

        // SAFETY: Layout is valid (non-zero size, power-of-two alignment).
        let memory = unsafe { alloc::alloc_zeroed(layout) };
        if memory.is_null() {
            alloc::handle_alloc_error(layout);
        }

        // Initialize free list with all slots
        let free_list: Vec<u32> = (0..slot_count as u32).collect();

        Self {
            memory,
            layout,
            slot_size,
            slot_count,
            free_list,
            alignment,
        }
    }

    /// Allocate a buffer from the pool.
    ///
    /// Returns `None` if the pool is exhausted.
    #[inline]
    pub fn allocate(&mut self) -> Option<AlignedBuffer> {
        let slot = self.free_list.pop()?;
        let offset = slot as usize * self.slot_size;

        // SAFETY: slot < slot_count, so offset + slot_size <= total_size
        let ptr = unsafe { self.memory.add(offset) };

        Some(AlignedBuffer {
            ptr,
            capacity: self.slot_size,
            slot,
        })
    }

    /// Return a buffer to the pool.
    ///
    /// # Panics
    ///
    /// Debug-panics if the buffer's slot index is out of range.
    #[inline]
    pub fn release(&mut self, buf: AlignedBuffer) {
        debug_assert!(
            (buf.slot as usize) < self.slot_count,
            "releasing buffer with invalid slot {}",
            buf.slot
        );
        self.free_list.push(buf.slot);
        // AlignedBuffer is consumed, no Drop needed
    }

    /// Get the number of available buffers.
    #[inline]
    pub fn available(&self) -> usize {
        self.free_list.len()
    }

    /// Get the total number of slots.
    #[inline]
    pub fn total(&self) -> usize {
        self.slot_count
    }

    /// Get the size of each slot in bytes.
    #[inline]
    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    /// Get the alignment in bytes.
    #[inline]
    pub fn alignment(&self) -> usize {
        self.alignment
    }
}

impl Drop for AlignedBufferPool {
    fn drop(&mut self) {
        // SAFETY: memory was allocated with this layout in new().
        unsafe {
            alloc::dealloc(self.memory, self.layout);
        }
        self.memory = ptr::null_mut();
    }
}

/// Alias for a pool of small read-staging buffers.
///
/// Identical to [`AlignedBufferPool`] but typically configured with
/// smaller slot sizes (e.g., 4KB or 8KB) for reading individual items
/// from disk.
pub type ReadBufferPool = AlignedBufferPool;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_alloc_release() {
        let mut pool = AlignedBufferPool::new(4, 4096, 4096);
        assert_eq!(pool.available(), 4);
        assert_eq!(pool.total(), 4);
        assert_eq!(pool.slot_size(), 4096);

        let buf = pool.allocate().unwrap();
        assert_eq!(pool.available(), 3);
        assert_eq!(buf.capacity(), 4096);

        pool.release(buf);
        assert_eq!(pool.available(), 4);
    }

    #[test]
    fn test_exhaustion() {
        let mut pool = AlignedBufferPool::new(2, 4096, 4096);

        let b1 = pool.allocate().unwrap();
        let b2 = pool.allocate().unwrap();
        assert!(pool.allocate().is_none());
        assert_eq!(pool.available(), 0);

        pool.release(b1);
        assert_eq!(pool.available(), 1);

        let _b3 = pool.allocate().unwrap();
        assert_eq!(pool.available(), 0);

        pool.release(b2);
        pool.release(_b3);
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_alignment() {
        let pool = AlignedBufferPool::new(4, 4096, 4096);
        assert_eq!(pool.alignment(), 4096);

        // Verify memory is aligned
        assert_eq!(pool.memory as usize % 4096, 0);
    }

    #[test]
    fn test_slot_size_rounding() {
        // Slot size should be rounded up to alignment
        let pool = AlignedBufferPool::new(2, 5000, 4096);
        assert_eq!(pool.slot_size(), 8192); // 5000 rounded up to 8192 (2 * 4096)
    }

    #[test]
    fn test_buffer_write_read() {
        let mut pool = AlignedBufferPool::new(1, 4096, 4096);
        let mut buf = pool.allocate().unwrap();

        // Write data
        let data = b"hello world";
        buf.as_mut_slice()[..data.len()].copy_from_slice(data);

        // Read data back
        let slice = unsafe { buf.as_slice(data.len()) };
        assert_eq!(slice, data);

        pool.release(buf);
    }

    #[test]
    fn test_buffer_addr() {
        let mut pool = AlignedBufferPool::new(2, 4096, 4096);
        let buf = pool.allocate().unwrap();

        // Address should be aligned
        assert_eq!(buf.addr() as usize % 4096, 0);
        assert_eq!(buf.addr(), buf.as_ptr() as u64);

        pool.release(buf);
    }

    #[test]
    fn test_distinct_slot_pointers() {
        let mut pool = AlignedBufferPool::new(3, 4096, 4096);

        let b0 = pool.allocate().unwrap();
        let b1 = pool.allocate().unwrap();
        let b2 = pool.allocate().unwrap();

        // Each buffer should have a distinct, non-overlapping pointer
        let ptrs = [
            b0.as_ptr() as usize,
            b1.as_ptr() as usize,
            b2.as_ptr() as usize,
        ];
        for i in 0..3 {
            for j in (i + 1)..3 {
                let diff = ptrs[i].abs_diff(ptrs[j]);
                assert!(diff >= 4096, "buffers overlap");
            }
        }

        pool.release(b0);
        pool.release(b1);
        pool.release(b2);
    }

    #[test]
    fn test_large_slot_size() {
        // 8MB segments (typical disk segment size)
        let mut pool = AlignedBufferPool::new(4, 8 * 1024 * 1024, 4096);
        assert_eq!(pool.slot_size(), 8 * 1024 * 1024);
        assert_eq!(pool.total(), 4);

        let buf = pool.allocate().unwrap();
        assert_eq!(buf.capacity(), 8 * 1024 * 1024);
        pool.release(buf);
    }

    #[test]
    #[should_panic(expected = "slot_count must be > 0")]
    fn test_zero_slot_count_panics() {
        AlignedBufferPool::new(0, 4096, 4096);
    }

    #[test]
    #[should_panic(expected = "alignment must be a power of two")]
    fn test_bad_alignment_panics() {
        AlignedBufferPool::new(1, 4096, 3000);
    }
}
