//! Pooled buffer management for zero-copy I/O.
//!
//! This module provides a buffer pool where connections can "check out" buffers
//! and chain them together for handling fragmented messages. Buffers are returned
//! to the pool when no longer needed, enabling efficient memory reuse across
//! many connections.
//!
//! # Design
//!
//! - Fixed-size buffer chunks (e.g., 8KB each)
//! - Connections hold chains of buffer IDs, not the data itself
//! - Pool tracks which buffers are free vs checked out
//! - When parsing completes, buffers are returned to the pool
//!
//! # Integration with io_uring
//!
//! For io_uring's ring-provided buffers, this pool can be registered with the
//! kernel. The kernel writes directly to pool buffers, and we track ownership
//! via buffer IDs from completions.

use std::collections::VecDeque;

/// Default chunk size: 32KB
/// Large enough to hold most requests without chaining.
pub const DEFAULT_CHUNK_SIZE: usize = 32 * 1024;

/// Default pool size: 128MB worth of chunks
pub const DEFAULT_POOL_SIZE: usize = 128 * 1024 * 1024;

/// A pool of fixed-size buffers.
///
/// Thread-safety: This pool is NOT thread-safe. Each worker should have its own pool.
pub struct BufferPool {
    /// The actual buffer storage - one contiguous allocation sliced into chunks
    storage: Box<[u8]>,
    /// Size of each chunk
    chunk_size: usize,
    /// Number of chunks
    chunk_count: usize,
    /// Free list of available chunk indices
    free_list: VecDeque<u16>,
    /// Count of buffers currently checked out
    checked_out: usize,
}

impl BufferPool {
    /// Create a new buffer pool.
    ///
    /// # Arguments
    /// * `chunk_size` - Size of each buffer chunk in bytes
    /// * `chunk_count` - Number of chunks to allocate
    ///
    /// Total memory = chunk_size * chunk_count
    pub fn new(chunk_size: usize, chunk_count: usize) -> Self {
        assert!(
            chunk_count <= u16::MAX as usize,
            "chunk_count must fit in u16"
        );
        assert!(chunk_size > 0, "chunk_size must be positive");

        let total_size = chunk_size * chunk_count;
        let storage = vec![0u8; total_size].into_boxed_slice();

        let mut free_list = VecDeque::with_capacity(chunk_count);
        for i in 0..chunk_count {
            free_list.push_back(i as u16);
        }

        Self {
            storage,
            chunk_size,
            chunk_count,
            free_list,
            checked_out: 0,
        }
    }

    /// Create a pool with default settings (32KB chunks, 128MB total = 4096 chunks).
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_CHUNK_SIZE, DEFAULT_POOL_SIZE / DEFAULT_CHUNK_SIZE)
    }

    /// Get the chunk size.
    #[inline]
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Get the total number of chunks.
    #[inline]
    pub fn chunk_count(&self) -> usize {
        self.chunk_count
    }

    /// Get the number of free chunks available.
    #[inline]
    pub fn free_count(&self) -> usize {
        self.free_list.len()
    }

    /// Get the number of chunks currently checked out.
    #[inline]
    pub fn checked_out_count(&self) -> usize {
        self.checked_out
    }

    /// Check out a buffer from the pool.
    ///
    /// Returns `None` if the pool is exhausted.
    #[inline]
    pub fn checkout(&mut self) -> Option<u16> {
        let id = self.free_list.pop_front()?;
        self.checked_out += 1;
        Some(id)
    }

    /// Return a buffer to the pool.
    ///
    /// # Panics
    /// Panics if the buffer ID is invalid.
    #[inline]
    pub fn checkin(&mut self, id: u16) {
        debug_assert!((id as usize) < self.chunk_count, "invalid buffer id");
        self.free_list.push_back(id);
        self.checked_out -= 1;
    }

    /// Get a slice to write into for a checked-out buffer.
    ///
    /// # Safety
    /// The buffer must be checked out (owned by caller).
    #[inline]
    pub fn get_mut(&mut self, id: u16) -> &mut [u8] {
        let start = (id as usize) * self.chunk_size;
        let end = start + self.chunk_size;
        &mut self.storage[start..end]
    }

    /// Get a slice to read from for a checked-out buffer.
    #[inline]
    pub fn get(&self, id: u16) -> &[u8] {
        let start = (id as usize) * self.chunk_size;
        let end = start + self.chunk_size;
        &self.storage[start..end]
    }

    /// Get a pointer to a buffer (for io_uring registration).
    #[inline]
    pub fn get_ptr(&self, id: u16) -> *mut u8 {
        let start = (id as usize) * self.chunk_size;
        unsafe { self.storage.as_ptr().add(start) as *mut u8 }
    }

    /// Get the base pointer for the entire pool (for io_uring buf_ring registration).
    #[inline]
    pub fn base_ptr(&self) -> *mut u8 {
        self.storage.as_ptr() as *mut u8
    }
}

/// A chain of buffers representing received data.
///
/// This holds a sequence of buffer IDs from a pool, along with metadata
/// about how much data is valid in each buffer.
#[derive(Debug)]
pub struct BufferChain {
    /// Buffer IDs in order, with valid byte counts
    /// (buffer_id, valid_bytes)
    chunks: Vec<(u16, usize)>,
    /// Total readable bytes across all chunks
    total_bytes: usize,
    /// Read cursor: (chunk_index, offset_within_chunk)
    read_pos: (usize, usize),
    /// Cached count of consumed bytes (updated incrementally in advance())
    consumed_bytes: usize,
}

impl BufferChain {
    /// Create an empty buffer chain.
    pub fn new() -> Self {
        Self {
            chunks: Vec::with_capacity(8),
            total_bytes: 0,
            read_pos: (0, 0),
            consumed_bytes: 0,
        }
    }

    /// Append a buffer to the chain.
    ///
    /// # Arguments
    /// * `id` - Buffer ID from the pool
    /// * `valid_bytes` - Number of valid bytes in this buffer
    #[inline]
    pub fn push(&mut self, id: u16, valid_bytes: usize) {
        self.chunks.push((id, valid_bytes));
        self.total_bytes += valid_bytes;
    }

    /// Get the number of readable bytes remaining.
    #[inline]
    pub fn readable(&self) -> usize {
        self.total_bytes - self.consumed_bytes
    }

    /// Get the number of bytes that have been consumed.
    #[inline]
    pub fn consumed(&self) -> usize {
        self.consumed_bytes
    }

    /// Check if the chain is empty (no readable data).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.readable() == 0
    }

    /// Advance the read position by `n` bytes.
    ///
    /// # Panics
    /// Panics if `n` exceeds readable bytes.
    pub fn advance(&mut self, mut n: usize) {
        assert!(n <= self.readable(), "advance exceeds readable bytes");

        // Update cached consumed count
        self.consumed_bytes += n;

        while n > 0 {
            let (chunk_idx, offset) = self.read_pos;
            if chunk_idx >= self.chunks.len() {
                break;
            }

            let (_, valid) = self.chunks[chunk_idx];
            let remaining_in_chunk = valid - offset;

            if n >= remaining_in_chunk {
                // Move to next chunk
                n -= remaining_in_chunk;
                self.read_pos = (chunk_idx + 1, 0);
            } else {
                // Partial advance within chunk
                self.read_pos.1 += n;
                n = 0;
            }
        }
    }

    /// Get buffer IDs that have been fully consumed and can be returned to the pool.
    ///
    /// This drains consumed chunks from the front of the chain.
    pub fn drain_consumed(&mut self) -> impl Iterator<Item = u16> + '_ {
        let consumed_count = self.read_pos.0;
        // After draining, consumed_bytes is just the offset within the new first chunk
        self.consumed_bytes = self.read_pos.1;
        self.read_pos.0 = 0;
        self.chunks.drain(..consumed_count).map(|(id, valid)| {
            self.total_bytes -= valid;
            id
        })
    }

    /// Clear the chain, returning all buffer IDs.
    pub fn clear(&mut self) -> impl Iterator<Item = u16> + '_ {
        self.total_bytes = 0;
        self.read_pos = (0, 0);
        self.consumed_bytes = 0;
        self.chunks.drain(..).map(|(id, _)| id)
    }

    /// Copy readable data into a contiguous buffer.
    ///
    /// This is used when parsers need a contiguous `&[u8]`.
    /// Returns the number of bytes copied.
    pub fn copy_to(&self, pool: &BufferPool, dest: &mut [u8]) -> usize {
        let to_copy = std::cmp::min(dest.len(), self.readable());
        let mut copied = 0;
        let mut remaining = to_copy;

        let (mut chunk_idx, mut offset) = self.read_pos;

        while remaining > 0 && chunk_idx < self.chunks.len() {
            let (id, valid) = self.chunks[chunk_idx];
            let chunk_data = pool.get(id);
            let available = valid - offset;
            let n = std::cmp::min(remaining, available);

            dest[copied..copied + n].copy_from_slice(&chunk_data[offset..offset + n]);
            copied += n;
            remaining -= n;

            chunk_idx += 1;
            offset = 0;
        }

        copied
    }

    /// Get the first chunk's readable data (for peeking at protocol bytes).
    ///
    /// Returns None if the chain is empty.
    pub fn first_chunk<'a>(&self, pool: &'a BufferPool) -> Option<&'a [u8]> {
        if self.is_empty() {
            return None;
        }

        let (chunk_idx, offset) = self.read_pos;
        let (id, valid) = self.chunks.get(chunk_idx)?;
        let chunk_data = pool.get(*id);
        Some(&chunk_data[offset..*valid])
    }

    /// Check if the readable data is contiguous (single chunk, no offset).
    ///
    /// When true, you can use `first_chunk()` to get all data without copying.
    #[inline]
    pub fn is_contiguous(&self) -> bool {
        let readable = self.readable();
        if readable == 0 {
            return true;
        }

        let (chunk_idx, offset) = self.read_pos;
        if chunk_idx >= self.chunks.len() {
            return true;
        }

        let (_, valid) = self.chunks[chunk_idx];
        // All readable data is in the current chunk
        (valid - offset) >= readable
    }
}

impl Default for BufferChain {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_basic() {
        let mut pool = BufferPool::new(1024, 4);

        assert_eq!(pool.chunk_size(), 1024);
        assert_eq!(pool.chunk_count(), 4);
        assert_eq!(pool.free_count(), 4);

        let id1 = pool.checkout().unwrap();
        let id2 = pool.checkout().unwrap();
        assert_eq!(pool.free_count(), 2);
        assert_eq!(pool.checked_out_count(), 2);

        pool.checkin(id1);
        assert_eq!(pool.free_count(), 3);

        pool.checkin(id2);
        assert_eq!(pool.free_count(), 4);
    }

    #[test]
    fn test_pool_exhaustion() {
        let mut pool = BufferPool::new(1024, 2);

        let _id1 = pool.checkout().unwrap();
        let _id2 = pool.checkout().unwrap();
        assert!(pool.checkout().is_none());
    }

    #[test]
    fn test_pool_read_write() {
        let mut pool = BufferPool::new(1024, 4);

        let id = pool.checkout().unwrap();
        let buf = pool.get_mut(id);
        buf[..5].copy_from_slice(b"hello");

        let buf = pool.get(id);
        assert_eq!(&buf[..5], b"hello");
    }

    #[test]
    fn test_chain_single_buffer() {
        let mut pool = BufferPool::new(1024, 4);
        let mut chain = BufferChain::new();

        let id = pool.checkout().unwrap();
        pool.get_mut(id)[..5].copy_from_slice(b"hello");
        chain.push(id, 5);

        assert_eq!(chain.readable(), 5);
        assert!(chain.is_contiguous());

        let first = chain.first_chunk(&pool).unwrap();
        assert_eq!(first, b"hello");

        chain.advance(3);
        assert_eq!(chain.readable(), 2);

        let first = chain.first_chunk(&pool).unwrap();
        assert_eq!(first, b"lo");
    }

    #[test]
    fn test_chain_multiple_buffers() {
        let mut pool = BufferPool::new(8, 4);
        let mut chain = BufferChain::new();

        // Add "hello" to first buffer
        let id1 = pool.checkout().unwrap();
        pool.get_mut(id1)[..5].copy_from_slice(b"hello");
        chain.push(id1, 5);

        // Add " world" to second buffer
        let id2 = pool.checkout().unwrap();
        pool.get_mut(id2)[..6].copy_from_slice(b" world");
        chain.push(id2, 6);

        assert_eq!(chain.readable(), 11);
        assert!(!chain.is_contiguous());

        // Copy to contiguous buffer
        let mut dest = [0u8; 20];
        let n = chain.copy_to(&pool, &mut dest);
        assert_eq!(n, 11);
        assert_eq!(&dest[..11], b"hello world");
    }

    #[test]
    fn test_chain_drain_consumed() {
        let mut pool = BufferPool::new(8, 4);
        let mut chain = BufferChain::new();

        let id1 = pool.checkout().unwrap();
        pool.get_mut(id1)[..5].copy_from_slice(b"hello");
        chain.push(id1, 5);

        let id2 = pool.checkout().unwrap();
        pool.get_mut(id2)[..5].copy_from_slice(b"world");
        chain.push(id2, 5);

        // Consume first buffer completely
        chain.advance(5);

        // Drain should return the first buffer
        let drained: Vec<_> = chain.drain_consumed().collect();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0], id1);

        // Return to pool
        for id in drained {
            pool.checkin(id);
        }
        assert_eq!(pool.free_count(), 3);

        // Second buffer still readable
        assert_eq!(chain.readable(), 5);
    }

    #[test]
    fn test_chain_clear() {
        let mut pool = BufferPool::new(8, 4);
        let mut chain = BufferChain::new();

        let id1 = pool.checkout().unwrap();
        chain.push(id1, 5);
        let id2 = pool.checkout().unwrap();
        chain.push(id2, 5);

        assert_eq!(pool.free_count(), 2);

        // Clear and return all buffers
        for id in chain.clear() {
            pool.checkin(id);
        }

        assert_eq!(pool.free_count(), 4);
        assert!(chain.is_empty());
    }
}
