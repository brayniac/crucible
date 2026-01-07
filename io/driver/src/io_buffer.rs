//! A buffer type safe for use with io_uring single-shot recv.
//!
//! This buffer tracks whether it has been "loaned" to the kernel and prevents
//! any operations that could reallocate while loaned, avoiding use-after-free.

use bytes::BytesMut;

/// A buffer that can be safely loaned to the kernel for async I/O.
///
/// When loaned, the buffer's memory address must remain stable. This type
/// enforces that invariant by panicking if you attempt to reallocate while
/// the buffer is loaned.
///
/// # Example
///
/// ```ignore
/// let mut buf = IoBuffer::with_capacity(64 * 1024);
///
/// // Loan spare capacity to the kernel
/// let spare = buf.loan_spare();
/// driver.submit_recv(conn_id, spare)?;
///
/// // ... kernel writes data ...
///
/// // On completion, unloan and commit the bytes written
/// buf.unloan(bytes_received);
///
/// // Now safe to read or reallocate
/// let data = buf.as_slice();
/// ```
#[derive(Debug)]
pub struct IoBuffer {
    inner: BytesMut,
    /// True when the buffer's spare capacity has been handed to the kernel.
    /// While loaned, no operations that could reallocate are allowed.
    loaned: bool,
}

impl IoBuffer {
    /// Create a new buffer with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: BytesMut::with_capacity(capacity),
            loaned: false,
        }
    }

    /// Returns true if the buffer is currently loaned to the kernel.
    #[inline]
    pub fn is_loaned(&self) -> bool {
        self.loaned
    }

    /// Returns the number of bytes available to read.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if there are no readable bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the total capacity of the buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Returns the spare capacity available for writing.
    #[inline]
    pub fn spare_capacity(&self) -> usize {
        self.inner.capacity() - self.inner.len()
    }

    /// Get a slice of the readable data.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.inner
    }

    /// Loan the spare capacity to the kernel for async recv.
    ///
    /// Returns a mutable slice of the spare capacity. The buffer is marked
    /// as loaned and cannot be reallocated until `unloan()` is called.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is already loaned.
    #[inline]
    pub fn loan_spare(&mut self) -> &mut [u8] {
        assert!(!self.loaned, "buffer already loaned to kernel");
        self.loaned = true;

        let len = self.inner.len();
        let cap = self.inner.capacity();

        // Safety: we're returning the uninitialized spare capacity.
        // The kernel will write to it, and unloan() will set the length.
        unsafe { std::slice::from_raw_parts_mut(self.inner.as_mut_ptr().add(len), cap - len) }
    }

    /// Return the buffer from the kernel and commit bytes written.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is not loaned, or if `bytes` exceeds spare capacity.
    #[inline]
    pub fn unloan(&mut self, bytes: usize) {
        assert!(self.loaned, "buffer not loaned");
        assert!(
            bytes <= self.spare_capacity(),
            "bytes written exceeds spare capacity"
        );

        self.loaned = false;

        // Safety: kernel wrote `bytes` bytes to the spare capacity
        unsafe {
            self.inner.set_len(self.inner.len() + bytes);
        }
    }

    /// Cancel a loan without committing any bytes (e.g., on error).
    ///
    /// # Panics
    ///
    /// Panics if the buffer is not loaned.
    #[inline]
    pub fn unloan_cancel(&mut self) {
        assert!(self.loaned, "buffer not loaned");
        self.loaned = false;
    }

    /// Consume `n` bytes from the front of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `n` exceeds readable bytes.
    #[inline]
    pub fn consume(&mut self, n: usize) {
        use bytes::Buf;
        self.inner.advance(n);
    }

    /// Reserve additional capacity.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is loaned to the kernel.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        assert!(
            !self.loaned,
            "cannot reallocate while buffer is loaned to kernel"
        );
        self.inner.reserve(additional);
    }

    /// Extend the buffer with data.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is loaned, or if this would require reallocation
    /// and the buffer is loaned.
    #[inline]
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        assert!(
            !self.loaned,
            "cannot extend while buffer is loaned to kernel"
        );
        self.inner.extend_from_slice(data);
    }

    /// Clear all data from the buffer.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is loaned.
    #[inline]
    pub fn clear(&mut self) {
        assert!(
            !self.loaned,
            "cannot clear while buffer is loaned to kernel"
        );
        self.inner.clear();
    }

    /// Ensure at least `min_spare` bytes of spare capacity, reserving if needed.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is loaned.
    pub fn ensure_spare(&mut self, min_spare: usize) {
        if self.spare_capacity() < min_spare {
            self.reserve(min_spare);
        }
    }
}

impl Default for IoBuffer {
    fn default() -> Self {
        Self::with_capacity(64 * 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_loan_unloan() {
        let mut buf = IoBuffer::with_capacity(1024);
        assert!(!buf.is_loaned());
        assert_eq!(buf.spare_capacity(), 1024);

        {
            let spare = buf.loan_spare();
            spare[..5].copy_from_slice(b"hello");
        }
        assert!(buf.is_loaned());

        buf.unloan(5);
        assert!(!buf.is_loaned());
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.as_slice(), b"hello");
    }

    #[test]
    fn test_multiple_loans() {
        let mut buf = IoBuffer::with_capacity(1024);

        // First loan
        {
            let spare = buf.loan_spare();
            spare[..5].copy_from_slice(b"hello");
        }
        buf.unloan(5);

        // Second loan
        {
            let spare = buf.loan_spare();
            spare[..6].copy_from_slice(b" world");
        }
        buf.unloan(6);

        assert_eq!(buf.as_slice(), b"hello world");
    }

    #[test]
    #[should_panic(expected = "buffer already loaned")]
    fn test_double_loan_panics() {
        let mut buf = IoBuffer::with_capacity(1024);
        let _spare = buf.loan_spare();
        let _spare2 = buf.loan_spare(); // Should panic
    }

    #[test]
    #[should_panic(expected = "cannot reallocate while buffer is loaned")]
    fn test_reserve_while_loaned_panics() {
        let mut buf = IoBuffer::with_capacity(64);
        let _spare = buf.loan_spare();
        buf.reserve(1024); // Should panic
    }

    #[test]
    #[should_panic(expected = "cannot extend while buffer is loaned")]
    fn test_extend_while_loaned_panics() {
        let mut buf = IoBuffer::with_capacity(1024);
        let _spare = buf.loan_spare();
        buf.extend_from_slice(b"data"); // Should panic
    }

    #[test]
    #[should_panic(expected = "cannot clear while buffer is loaned")]
    fn test_clear_while_loaned_panics() {
        let mut buf = IoBuffer::with_capacity(1024);
        let _spare = buf.loan_spare();
        buf.clear(); // Should panic
    }

    #[test]
    fn test_unloan_cancel() {
        let mut buf = IoBuffer::with_capacity(1024);
        let _spare = buf.loan_spare();
        buf.unloan_cancel();
        assert!(!buf.is_loaned());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_consume() {
        let mut buf = IoBuffer::with_capacity(1024);
        buf.extend_from_slice(b"hello world");
        buf.consume(6);
        assert_eq!(buf.as_slice(), b"world");
    }

    #[test]
    fn test_ensure_spare() {
        let mut buf = IoBuffer::with_capacity(64);
        buf.extend_from_slice(&[0u8; 60]);
        assert_eq!(buf.spare_capacity(), 4);

        buf.ensure_spare(100);
        assert!(buf.spare_capacity() >= 100);
    }

    /// Test the full receive cycle: loan -> write -> unloan -> consume -> loan again
    /// This simulates the actual usage pattern in single-shot recv mode.
    #[test]
    fn test_recv_cycle_with_consume() {
        let mut buf = IoBuffer::with_capacity(1024);

        // First recv: loan spare, write 100 bytes, unloan
        {
            let spare = buf.loan_spare();
            spare[..100].copy_from_slice(&[b'A'; 100]);
        }
        buf.unloan(100);
        assert_eq!(buf.len(), 100);
        assert_eq!(&buf.as_slice()[..100], &[b'A'; 100]);

        // Consume 60 bytes (simulate processing a request)
        buf.consume(60);
        assert_eq!(buf.len(), 40);
        assert_eq!(&buf.as_slice()[..40], &[b'A'; 40]);

        // Second recv: loan spare, write 50 more bytes, unloan
        {
            let spare = buf.loan_spare();
            // Spare should start after the 40 remaining bytes
            spare[..50].copy_from_slice(&[b'B'; 50]);
        }
        buf.unloan(50);
        assert_eq!(buf.len(), 90);
        // First 40 bytes are still 'A', next 50 are 'B'
        assert_eq!(&buf.as_slice()[..40], &[b'A'; 40]);
        assert_eq!(&buf.as_slice()[40..90], &[b'B'; 50]);
    }

    /// Test that loan_spare returns correct pointer after consume
    #[test]
    fn test_loan_spare_pointer_after_consume() {
        let mut buf = IoBuffer::with_capacity(1024);

        // Write some data
        buf.extend_from_slice(&[b'X'; 100]);

        // Consume 50 bytes
        buf.consume(50);

        // Now loan spare and check pointer
        let spare = buf.loan_spare();
        let spare_ptr = spare.as_ptr();

        // Write to spare
        spare[..10].copy_from_slice(&[b'Y'; 10]);
        buf.unloan(10);

        // Verify the data is in the right place
        assert_eq!(buf.len(), 60); // 50 remaining + 10 new
        assert_eq!(&buf.as_slice()[..50], &[b'X'; 50]);
        assert_eq!(&buf.as_slice()[50..60], &[b'Y'; 10]);

        // The Y bytes should be at spare_ptr
        unsafe {
            assert_eq!(*spare_ptr, b'Y');
        }
    }
}
