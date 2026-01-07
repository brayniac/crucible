//! Fixed-size buffer for zero-copy I/O operations.
//!
//! This module provides a buffer type specifically designed for use with
//! io_uring's zero-copy recv operations. The key property is that the
//! underlying allocation never moves, making it safe to pass pointers
//! to the kernel.
//!
//! # Safety
//!
//! When using `spare_mut()` with io_uring's `submit_recv()`:
//! - The buffer pointer remains stable until the completion arrives
//! - Do NOT call `compact()` while a recv is pending
//! - Call `commit(n)` only after the corresponding completion

/// A fixed-size buffer for network I/O operations.
///
/// Unlike `BytesMut`, this buffer uses a fixed `Box<[u8]>` allocation
/// that never reallocates. This makes it safe for io_uring operations
/// where the kernel holds a pointer to the buffer.
///
/// # Example
///
/// ```
/// use io_driver::RecvBuffer;
///
/// let mut buf = RecvBuffer::new(8192);
///
/// // Get spare capacity for receiving
/// let spare = buf.spare_mut();
/// // ... kernel writes to spare ...
/// buf.commit(100);  // 100 bytes were received
///
/// // Read the data
/// let data = buf.as_slice();
/// buf.consume(data.len());
/// ```
#[derive(Debug)]
pub struct RecvBuffer {
    data: Box<[u8]>,
    /// Read position: data before this has been consumed
    read_pos: usize,
    /// Write position: data has been written up to here
    write_pos: usize,
}

impl RecvBuffer {
    /// Create a new buffer with the specified capacity.
    ///
    /// The capacity is fixed and cannot be changed after creation.
    pub fn new(capacity: usize) -> Self {
        Self {
            data: vec![0u8; capacity].into_boxed_slice(),
            read_pos: 0,
            write_pos: 0,
        }
    }

    /// Returns the total capacity of the buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Returns the number of bytes available to read.
    #[inline]
    pub fn readable(&self) -> usize {
        self.write_pos - self.read_pos
    }

    /// Returns the number of bytes of spare capacity available.
    #[inline]
    pub fn spare_capacity(&self) -> usize {
        self.data.len() - self.write_pos
    }

    /// Returns true if there is no data to read.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.read_pos == self.write_pos
    }

    /// Returns a slice of the readable data.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data[self.read_pos..self.write_pos]
    }

    /// Returns a mutable slice of the spare capacity.
    ///
    /// This slice can be passed to io_uring's recv operation. The pointer
    /// is guaranteed to remain stable until the buffer is dropped.
    ///
    /// # Safety Note
    ///
    /// When using with io_uring:
    /// - Do NOT call `compact()` while a recv is pending
    /// - Call `commit(n)` after the completion to record how much was written
    #[inline]
    pub fn spare_mut(&mut self) -> &mut [u8] {
        &mut self.data[self.write_pos..]
    }

    /// Commit bytes that were written to the spare capacity.
    ///
    /// Call this after io_uring reports that `n` bytes were received.
    ///
    /// # Panics
    ///
    /// Panics if `n` exceeds the spare capacity. This indicates a bug
    /// in the caller or corrupted completion data.
    #[inline]
    pub fn commit(&mut self, n: usize) {
        assert!(
            n <= self.spare_capacity(),
            "commit({}) exceeds spare capacity ({})",
            n,
            self.spare_capacity()
        );
        self.write_pos += n;
    }

    /// Consume `n` bytes from the front of the readable data.
    ///
    /// # Panics
    ///
    /// Panics if `n` exceeds the readable bytes.
    #[inline]
    pub fn consume(&mut self, n: usize) {
        assert!(
            n <= self.readable(),
            "consume({}) exceeds readable bytes ({})",
            n,
            self.readable()
        );
        self.read_pos += n;

        // Auto-reset when all data consumed
        if self.read_pos == self.write_pos {
            self.read_pos = 0;
            self.write_pos = 0;
        }
    }

    /// Move unread data to the start of the buffer.
    ///
    /// This reclaims space from consumed data without allocating.
    ///
    /// # Safety Note
    ///
    /// Do NOT call this while an io_uring recv is pending, as it changes
    /// where `spare_mut()` points.
    pub fn compact(&mut self) {
        if self.read_pos == 0 {
            return;
        }

        let readable = self.readable();
        if readable > 0 {
            self.data.copy_within(self.read_pos..self.write_pos, 0);
        }
        self.read_pos = 0;
        self.write_pos = readable;
    }

    /// Clear the buffer, resetting both positions.
    #[inline]
    pub fn clear(&mut self) {
        self.read_pos = 0;
        self.write_pos = 0;
    }

    /// Append data to the buffer.
    ///
    /// # Panics
    ///
    /// Panics if the data exceeds spare capacity.
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        assert!(
            data.len() <= self.spare_capacity(),
            "extend_from_slice: {} bytes exceeds spare capacity ({})",
            data.len(),
            self.spare_capacity()
        );
        self.spare_mut()[..data.len()].copy_from_slice(data);
        self.write_pos += data.len();
    }

    /// Prepare buffer for receiving, compacting if needed.
    ///
    /// Returns a mutable slice for the recv operation. This method
    /// handles compaction automatically when spare capacity is low.
    ///
    /// # Safety Note
    ///
    /// Only call this when no io_uring recv is pending.
    #[inline]
    pub fn prepare_recv(&mut self) -> &mut [u8] {
        // Compact if spare capacity is less than 25% of total
        if self.spare_capacity() < self.capacity() / 4 {
            self.compact();
        }
        self.spare_mut()
    }
}

impl Default for RecvBuffer {
    fn default() -> Self {
        Self::new(65536) // 64KB default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut buf = RecvBuffer::new(1024);

        assert_eq!(buf.capacity(), 1024);
        assert_eq!(buf.readable(), 0);
        assert_eq!(buf.spare_capacity(), 1024);
        assert!(buf.is_empty());

        buf.extend_from_slice(b"hello");
        assert_eq!(buf.readable(), 5);
        assert_eq!(buf.as_slice(), b"hello");

        buf.consume(2);
        assert_eq!(buf.readable(), 3);
        assert_eq!(buf.as_slice(), b"llo");
    }

    #[test]
    fn test_commit() {
        let mut buf = RecvBuffer::new(1024);

        // Simulate io_uring writing to spare capacity
        let spare = buf.spare_mut();
        spare[..5].copy_from_slice(b"hello");
        buf.commit(5);

        assert_eq!(buf.as_slice(), b"hello");
    }

    #[test]
    #[should_panic(expected = "commit(2000) exceeds spare capacity")]
    fn test_commit_overflow_panics() {
        let mut buf = RecvBuffer::new(1024);
        buf.commit(2000);
    }

    #[test]
    fn test_compact() {
        let mut buf = RecvBuffer::new(16);

        buf.extend_from_slice(b"hello world!");
        assert_eq!(buf.spare_capacity(), 4);

        buf.consume(6);
        assert_eq!(buf.as_slice(), b"world!");

        buf.compact();
        assert_eq!(buf.as_slice(), b"world!");
        assert_eq!(buf.spare_capacity(), 10);
    }

    #[test]
    fn test_auto_reset_on_full_consume() {
        let mut buf = RecvBuffer::new(1024);

        buf.extend_from_slice(b"test");
        buf.consume(4);

        // Should auto-reset, giving back full capacity
        assert_eq!(buf.spare_capacity(), 1024);
    }

    #[test]
    fn test_prepare_recv_compacts() {
        let mut buf = RecvBuffer::new(100);

        // Fill most of the buffer
        buf.extend_from_slice(&[0u8; 80]);
        // Consume most of it
        buf.consume(75);

        // Now we have 5 readable bytes and 20 spare
        assert_eq!(buf.readable(), 5);
        assert_eq!(buf.spare_capacity(), 20);

        // prepare_recv should compact since spare < 25%
        let spare = buf.prepare_recv();
        assert!(spare.len() >= 95); // After compact: 100 - 5 = 95
    }

    #[test]
    fn test_pointer_stability() {
        let mut buf = RecvBuffer::new(1024);

        // Get pointer to spare capacity
        let ptr1 = buf.spare_mut().as_ptr();

        // Do some operations that don't reallocate
        buf.extend_from_slice(b"hello");
        buf.consume(5);

        // Pointer should still be valid (same allocation)
        let ptr2 = buf.spare_mut().as_ptr();

        // The pointers might differ due to position changes,
        // but they should be within the same allocation
        let base = buf.data.as_ptr();
        let end = unsafe { base.add(buf.capacity()) };

        assert!(ptr1 >= base && ptr1 < end);
        assert!(ptr2 >= base && ptr2 < end);
    }
}
