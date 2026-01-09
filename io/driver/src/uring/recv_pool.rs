//! Buffer pool for single-shot recv operations.
//!
//! This pool owns buffers that outlive individual connections, preventing
//! use-after-free when a connection closes while a recv is pending.
//!
//! # Safety
//!
//! The key safety property is that buffers are never freed while io_uring
//! holds a pointer to them. The pool lives as long as the driver, and
//! buffers are returned to the pool (not freed) when recv completes.

use std::collections::VecDeque;

/// A pool of fixed-size buffers for single-shot recv operations.
///
/// Each buffer slot has a unique ID that is encoded in the io_uring user_data.
/// When a recv completes, we use the buffer ID to find the buffer and copy
/// data to the connection's application buffer.
pub struct RecvBufferPool {
    /// Fixed buffer storage - never reallocated after creation.
    buffers: Box<[Buffer]>,
    /// Free buffer indices available for allocation.
    free_list: VecDeque<u16>,
    /// Size of each buffer.
    buffer_size: usize,
}

/// A single buffer slot in the pool.
struct Buffer {
    /// The actual buffer data.
    data: Box<[u8]>,
    /// Connection ID that borrowed this buffer (for validation).
    conn_id: Option<usize>,
    /// Generation of the connection (for stale detection).
    generation: u32,
}

impl RecvBufferPool {
    /// Create a new buffer pool.
    ///
    /// # Arguments
    /// * `count` - Number of buffers in the pool
    /// * `buffer_size` - Size of each buffer in bytes
    pub fn new(count: u16, buffer_size: usize) -> Self {
        let buffers: Vec<Buffer> = (0..count)
            .map(|_| Buffer {
                data: vec![0u8; buffer_size].into_boxed_slice(),
                conn_id: None,
                generation: 0,
            })
            .collect();

        let free_list: VecDeque<u16> = (0..count).collect();

        Self {
            buffers: buffers.into_boxed_slice(),
            free_list,
            buffer_size,
        }
    }

    /// Allocate a buffer for a connection's recv operation.
    ///
    /// Returns `(buffer_id, buffer_ptr, buffer_len)` if a buffer is available.
    /// The buffer_ptr is stable and safe to pass to io_uring.
    pub fn alloc(&mut self, conn_id: usize, generation: u32) -> Option<(u16, *mut u8, usize)> {
        let buf_id = self.free_list.pop_front()?;
        let buffer = &mut self.buffers[buf_id as usize];

        buffer.conn_id = Some(conn_id);
        buffer.generation = generation;

        let ptr = buffer.data.as_mut_ptr();
        Some((buf_id, ptr, self.buffer_size))
    }

    /// Get buffer data after recv completion.
    ///
    /// Returns the buffer slice if the buffer is valid and was allocated
    /// for the specified connection/generation. Returns None if stale.
    pub fn get(&self, buf_id: u16, conn_id: usize, generation: u32) -> Option<&[u8]> {
        let buffer = self.buffers.get(buf_id as usize)?;

        // Validate this is the expected connection
        if buffer.conn_id != Some(conn_id) || buffer.generation != generation {
            return None;
        }

        Some(&buffer.data)
    }

    /// Return a buffer to the pool.
    ///
    /// This should be called after processing a recv completion, regardless
    /// of whether it was successful, an error, or stale.
    pub fn free(&mut self, buf_id: u16) {
        if let Some(buffer) = self.buffers.get_mut(buf_id as usize) {
            buffer.conn_id = None;
            buffer.generation = 0;
            // Use push_front for LIFO behavior - recently freed buffers
            // are more likely to be cache-hot
            self.free_list.push_front(buf_id);
        }
    }

    /// Check if a buffer is currently allocated (for debugging).
    #[allow(dead_code)]
    pub fn is_allocated(&self, buf_id: u16) -> bool {
        self.buffers
            .get(buf_id as usize)
            .map(|b| b.conn_id.is_some())
            .unwrap_or(false)
    }

    /// Get number of available buffers.
    #[allow(dead_code)]
    pub fn available(&self) -> usize {
        self.free_list.len()
    }

    /// Get total number of buffers.
    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.buffers.len()
    }

    /// Check if pool is exhausted (no free buffers).
    #[inline]
    #[allow(dead_code)]
    pub fn is_exhausted(&self) -> bool {
        self.free_list.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alloc_and_free() {
        let mut pool = RecvBufferPool::new(4, 1024);

        assert_eq!(pool.available(), 4);

        // Allocate all buffers
        let (id0, ptr0, len0) = pool.alloc(0, 100).unwrap();
        let (id1, _, _) = pool.alloc(1, 101).unwrap();
        let (id2, _, _) = pool.alloc(2, 102).unwrap();
        let (id3, _, _) = pool.alloc(3, 103).unwrap();

        assert_eq!(pool.available(), 0);
        assert!(pool.alloc(4, 104).is_none()); // Pool exhausted

        // Verify buffer properties
        assert_eq!(len0, 1024);
        assert!(!ptr0.is_null());

        // Free one buffer
        pool.free(id1);
        assert_eq!(pool.available(), 1);

        // Can allocate again
        let (id_new, _, _) = pool.alloc(5, 105).unwrap();
        assert_eq!(id_new, id1); // Reuses freed slot

        // Free all
        pool.free(id0);
        pool.free(id_new);
        pool.free(id2);
        pool.free(id3);
        assert_eq!(pool.available(), 4);
    }

    #[test]
    fn test_get_validates_connection() {
        let mut pool = RecvBufferPool::new(2, 1024);

        let (id, _, _) = pool.alloc(42, 100).unwrap();

        // Correct conn_id and generation
        assert!(pool.get(id, 42, 100).is_some());

        // Wrong conn_id
        assert!(pool.get(id, 99, 100).is_none());

        // Wrong generation
        assert!(pool.get(id, 42, 999).is_none());

        // Wrong buffer id
        assert!(pool.get(99, 42, 100).is_none());

        pool.free(id);

        // After free, get should fail
        assert!(pool.get(id, 42, 100).is_none());
    }

    #[test]
    fn test_buffer_pointer_stability() {
        let mut pool = RecvBufferPool::new(2, 1024);

        // Allocate, free, reallocate - pointer should be the same
        let (id1, ptr1, _) = pool.alloc(1, 1).unwrap();
        pool.free(id1);

        let (id2, ptr2, _) = pool.alloc(2, 2).unwrap();
        assert_eq!(id1, id2);
        assert_eq!(ptr1, ptr2); // Same underlying buffer

        pool.free(id2);
    }

    #[test]
    fn test_buffer_data_accessible() {
        let mut pool = RecvBufferPool::new(1, 64);

        let (id, ptr, len) = pool.alloc(1, 1).unwrap();

        // Write to buffer through raw pointer (simulating kernel write)
        unsafe {
            std::ptr::copy_nonoverlapping(b"hello".as_ptr(), ptr, 5);
        }

        // Read through get()
        let data = pool.get(id, 1, 1).unwrap();
        assert_eq!(&data[..5], b"hello");

        pool.free(id);
    }
}
