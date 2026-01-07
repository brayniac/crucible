//! Unified receive buffer state for all I/O backends.
//!
//! This module provides a zero-copy-when-possible receive buffer that works
//! identically across mio and io_uring backends. Data is accessed through
//! a `ReadGuard` which provides a contiguous slice view.
//!
//! # Zero-Copy Behavior
//!
//! - When data fits in a single buffer and is fully consumed before the next
//!   recv, access is zero-copy (direct slice into BufRing/Pool buffer).
//! - When data spans recv boundaries (pipelining, large values, slow consumer),
//!   data is copied to an internal coalesce buffer.
//!
//! For typical cache workloads with small requests, the zero-copy path dominates.

use bytes::BytesMut;

/// Identifies the source of a buffer for return purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferSource {
    /// Buffer from BufRing (io_uring multishot recv).
    Ring,
    /// Buffer from Pool (io_uring single-shot recv).
    Pool,
}

/// A buffer that needs to be returned to its source.
#[derive(Debug, Clone, Copy)]
pub struct PendingReturn {
    pub source: BufferSource,
    pub buf_id: u16,
}

/// A reference to a buffer slice from BufRing or Pool.
#[derive(Debug)]
pub struct BufferSlice {
    /// Source of this buffer (for returning).
    pub source: BufferSource,
    /// Buffer ID for returning to ring/pool.
    pub buf_id: u16,
    /// Pointer to buffer data.
    data: *const u8,
    /// Length of data in buffer.
    len: usize,
}

// Safety: BufferSlice contains raw pointers but they point to memory owned
// by the driver (BufRing or Pool) which lives as long as the driver.
// The slice is only valid while the driver exists.
unsafe impl Send for BufferSlice {}

impl BufferSlice {
    /// Create a new buffer slice from a BufRing buffer.
    pub fn from_ring(buf_id: u16, data: *const u8, len: usize) -> Self {
        Self {
            source: BufferSource::Ring,
            buf_id,
            data,
            len,
        }
    }

    /// Create a new buffer slice from a Pool buffer.
    pub fn from_pool(buf_id: u16, data: *const u8, len: usize) -> Self {
        Self {
            source: BufferSource::Pool,
            buf_id,
            data,
            len,
        }
    }

    /// Get the slice data.
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }

    /// Get the length of data in buffer.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the slice is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Convert to a PendingReturn for buffer return tracking.
    pub fn to_pending_return(&self) -> PendingReturn {
        PendingReturn {
            source: self.source,
            buf_id: self.buf_id,
        }
    }
}

/// Per-connection receive buffer state.
///
/// Manages zero-copy access to received data with automatic fallback
/// to a coalesce buffer when data spans multiple recv operations.
pub struct ConnectionRecvState {
    /// Current buffer slice (zero-copy reference to BufRing/Pool).
    /// At most one slice is held at a time.
    current: Option<BufferSlice>,

    /// Offset into current slice (for partial consumes).
    offset: usize,

    /// Coalesce buffer for data that spans recv boundaries.
    /// When non-empty, this is the authoritative data source.
    coalesce_buf: BytesMut,

    /// Offset into coalesce buffer (for partial consumes).
    coalesce_offset: usize,

    /// Buffers that need to be returned to ring/pool.
    /// Processed by the driver after read operations.
    pending_returns: Vec<PendingReturn>,
}

impl ConnectionRecvState {
    /// Create a new receive state with the specified coalesce buffer capacity.
    pub fn new(coalesce_capacity: usize) -> Self {
        Self {
            current: None,
            offset: 0,
            coalesce_buf: BytesMut::with_capacity(coalesce_capacity),
            coalesce_offset: 0,
            pending_returns: Vec::new(),
        }
    }

    /// Check if there is any data available to read.
    pub fn has_data(&self) -> bool {
        !self.coalesce_buf.is_empty() || self.current.is_some()
    }

    /// Get the total number of bytes available to read.
    pub fn available(&self) -> usize {
        if !self.coalesce_buf.is_empty() {
            self.coalesce_buf.len() - self.coalesce_offset
        } else if let Some(ref slice) = self.current {
            slice.len() - self.offset
        } else {
            0
        }
    }

    /// Get a contiguous slice of all available data.
    ///
    /// If data is in a single buffer slice, returns it directly (zero-copy).
    /// If data has been coalesced, returns the coalesce buffer.
    pub fn as_slice(&self) -> &[u8] {
        if !self.coalesce_buf.is_empty() {
            &self.coalesce_buf[self.coalesce_offset..]
        } else if let Some(ref slice) = self.current {
            &slice.as_slice()[self.offset..]
        } else {
            &[]
        }
    }

    /// Mark `n` bytes as consumed.
    ///
    /// If a buffer slice is fully consumed, it's added to pending_returns
    /// for the driver to return to the ring/pool.
    pub fn consume(&mut self, n: usize) {
        if !self.coalesce_buf.is_empty() {
            // Consuming from coalesce buffer
            self.coalesce_offset += n;
            if self.coalesce_offset >= self.coalesce_buf.len() {
                // Fully consumed - reset coalesce buffer
                self.coalesce_buf.clear();
                self.coalesce_offset = 0;
            }
        } else if let Some(ref slice) = self.current {
            self.offset += n;
            if self.offset >= slice.len() {
                // Fully consumed - add to pending returns
                let slice = self.current.take().unwrap();
                self.pending_returns.push(slice.to_pending_return());
                self.offset = 0;
            }
        }
    }

    /// Handle new data arriving from a recv operation.
    ///
    /// If there's unconsumed data, coalesces old + new into the coalesce buffer
    /// and adds both buffer slices to pending_returns.
    /// Otherwise, takes zero-copy ownership of the new slice.
    pub fn on_recv(&mut self, new_slice: BufferSlice) {
        if !self.coalesce_buf.is_empty() {
            // Already coalescing - append new data and return buffer
            self.coalesce_buf.extend_from_slice(new_slice.as_slice());
            self.pending_returns.push(new_slice.to_pending_return());
        } else if let Some(old) = self.current.take() {
            // Have unconsumed data in current slice - must coalesce both
            let old_remaining = &old.as_slice()[self.offset..];
            self.coalesce_buf.extend_from_slice(old_remaining);
            self.coalesce_buf.extend_from_slice(new_slice.as_slice());
            self.offset = 0;
            self.coalesce_offset = 0;
            // Both buffers should be returned
            self.pending_returns.push(old.to_pending_return());
            self.pending_returns.push(new_slice.to_pending_return());
        } else {
            // Clean state - zero copy ownership
            self.current = Some(new_slice);
            self.offset = 0;
        }
    }

    /// Take pending buffer returns for processing by the driver.
    ///
    /// Call this after read operations to get buffers that should be
    /// returned to the ring/pool.
    pub fn take_pending_returns(&mut self) -> impl Iterator<Item = PendingReturn> + '_ {
        self.pending_returns.drain(..)
    }

    /// Check if there are pending buffer returns.
    pub fn has_pending_returns(&self) -> bool {
        !self.pending_returns.is_empty()
    }

    /// Append data directly to the coalesce buffer (for mio backend).
    ///
    /// This is used when the backend reads directly into a buffer rather
    /// than providing buffer slices.
    pub fn append_owned(&mut self, data: &[u8]) {
        if let Some(old) = self.current.take() {
            // Move current slice to coalesce buffer first
            let old_remaining = &old.as_slice()[self.offset..];
            self.coalesce_buf.extend_from_slice(old_remaining);
            self.offset = 0;
        }
        self.coalesce_buf.extend_from_slice(data);
    }

    /// Get mutable access to the coalesce buffer for direct reads (mio backend).
    ///
    /// Returns a slice of spare capacity that can be written to.
    /// After writing, call `commit_owned(n)` to commit the bytes.
    pub fn spare_capacity_mut(&mut self) -> &mut [u8] {
        // Ensure we have some capacity
        if self.coalesce_buf.capacity() - self.coalesce_buf.len() < 8192 {
            self.coalesce_buf.reserve(8192);
        }

        let len = self.coalesce_buf.len();
        let cap = self.coalesce_buf.capacity();
        unsafe {
            std::slice::from_raw_parts_mut(self.coalesce_buf.as_mut_ptr().add(len), cap - len)
        }
    }

    /// Commit bytes written directly to spare capacity (mio backend).
    pub fn commit_owned(&mut self, n: usize) {
        unsafe {
            self.coalesce_buf.set_len(self.coalesce_buf.len() + n);
        }
    }

    /// Clear all state, adding any held buffer slice to pending returns.
    pub fn clear(&mut self) {
        self.coalesce_buf.clear();
        self.coalesce_offset = 0;
        self.offset = 0;
        if let Some(slice) = self.current.take() {
            self.pending_returns.push(slice.to_pending_return());
        }
    }
}

impl Default for ConnectionRecvState {
    fn default() -> Self {
        Self::new(65536) // 64KB default coalesce capacity
    }
}

/// A guard providing read access to a connection's receive buffer.
///
/// This guard provides a unified interface for reading received data
/// across all I/O backends (mio, io_uring multishot, io_uring single-shot).
///
/// # Zero-Copy Semantics
///
/// When data fits in a single buffer and hasn't been coalesced, `as_slice()`
/// returns a direct reference to the underlying buffer with no copies.
/// When data spans multiple recv operations, it's automatically coalesced
/// and `as_slice()` returns a reference to the coalesced buffer.
///
/// # Example
///
/// ```ignore
/// let guard = driver.read(conn_id)?;
/// while let Ok((cmd, consumed)) = parse(guard.as_slice()) {
///     execute(cmd);
///     guard.consume(consumed);
/// }
/// // Guard dropped - pending buffer returns processed by driver
/// ```
pub struct ReadGuard<'a> {
    state: &'a mut ConnectionRecvState,
}

impl<'a> ReadGuard<'a> {
    /// Get a contiguous slice of all available data.
    ///
    /// If data is in a single buffer, this is zero-copy.
    /// If data has been coalesced, returns the coalesced buffer.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.state.as_slice()
    }

    /// Get the number of bytes available to read.
    #[inline]
    pub fn len(&self) -> usize {
        self.state.available()
    }

    /// Check if there's no data available.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.state.available() == 0
    }

    /// Mark `n` bytes as consumed.
    ///
    /// After consuming, `as_slice()` will return the remaining data.
    /// When a buffer is fully consumed, it's queued for return to the
    /// ring/pool (processed when the guard is dropped or on next poll).
    #[inline]
    pub fn consume(&mut self, n: usize) {
        self.state.consume(n);
    }

    /// Check if there are buffers pending return to ring/pool.
    #[inline]
    pub fn has_pending_returns(&self) -> bool {
        self.state.has_pending_returns()
    }
}

// Implement bytes::Buf for compatibility with parsers that use it
impl<'a> bytes::Buf for ReadGuard<'a> {
    #[inline]
    fn remaining(&self) -> usize {
        self.state.available()
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self.state.as_slice()
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        self.state.consume(cnt);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a test buffer slice (simulates ring/pool buffer)
    fn make_test_slice(data: &'static [u8], buf_id: u16) -> BufferSlice {
        BufferSlice::from_ring(buf_id, data.as_ptr(), data.len())
    }

    #[test]
    fn test_zero_copy_single_recv() {
        let mut state = ConnectionRecvState::new(1024);

        // Recv arrives
        let slice = make_test_slice(b"GET key\r\n", 0);
        state.on_recv(slice);
        assert!(!state.has_pending_returns());

        // Read data
        assert_eq!(state.as_slice(), b"GET key\r\n");
        assert_eq!(state.available(), 9);

        // Consume all
        state.consume(9);
        assert!(state.has_pending_returns());
        assert_eq!(state.available(), 0);

        // Check pending return
        let returns: Vec<_> = state.take_pending_returns().collect();
        assert_eq!(returns.len(), 1);
        assert_eq!(returns[0].buf_id, 0);
    }

    #[test]
    fn test_partial_consume() {
        let mut state = ConnectionRecvState::new(1024);

        let slice = make_test_slice(b"GET key\r\nGET foo\r\n", 0);
        state.on_recv(slice);

        // Consume first command
        assert_eq!(state.as_slice(), b"GET key\r\nGET foo\r\n");
        state.consume(9);
        assert!(!state.has_pending_returns()); // Still has data

        // Second command remains
        assert_eq!(state.as_slice(), b"GET foo\r\n");
        state.consume(9);
        assert!(state.has_pending_returns()); // Now empty
    }

    #[test]
    fn test_coalesce_on_second_recv() {
        let mut state = ConnectionRecvState::new(1024);

        // First recv
        let slice1 = make_test_slice(b"GET ke", 0);
        state.on_recv(slice1);
        assert_eq!(state.as_slice(), b"GET ke");
        assert!(!state.has_pending_returns());

        // Second recv before consuming - triggers coalesce
        let slice2 = make_test_slice(b"y\r\n", 1);
        state.on_recv(slice2);

        // Both buffers should be in pending returns
        assert!(state.has_pending_returns());
        let returns: Vec<_> = state.take_pending_returns().collect();
        assert_eq!(returns.len(), 2);
        assert_eq!(returns[0].buf_id, 0);
        assert_eq!(returns[1].buf_id, 1);

        // Data is coalesced
        assert_eq!(state.as_slice(), b"GET key\r\n");
    }

    #[test]
    fn test_coalesce_continues() {
        let mut state = ConnectionRecvState::new(1024);

        // First recv
        let slice1 = make_test_slice(b"GET ", 0);
        state.on_recv(slice1);

        // Second recv - coalesce starts
        let slice2 = make_test_slice(b"key", 1);
        state.on_recv(slice2);
        assert_eq!(state.as_slice(), b"GET key");

        // Drain pending returns
        let _: Vec<_> = state.take_pending_returns().collect();

        // Third recv - continues coalescing
        let slice3 = make_test_slice(b"\r\n", 2);
        state.on_recv(slice3);

        // Only the new slice should be in pending returns
        let returns: Vec<_> = state.take_pending_returns().collect();
        assert_eq!(returns.len(), 1);
        assert_eq!(returns[0].buf_id, 2);

        assert_eq!(state.as_slice(), b"GET key\r\n");
    }

    #[test]
    fn test_coalesce_reset_after_full_consume() {
        let mut state = ConnectionRecvState::new(1024);

        // Trigger coalesce: "GET " + "key\r\n" = "GET key\r\n" (9 bytes)
        let slice1 = make_test_slice(b"GET ", 0);
        state.on_recv(slice1);
        let slice2 = make_test_slice(b"key\r\n", 1);
        state.on_recv(slice2);

        // Drain pending returns
        let _: Vec<_> = state.take_pending_returns().collect();

        // Consume all (9 bytes)
        state.consume(9);
        assert_eq!(state.available(), 0);

        // Next recv should be zero-copy again
        let slice3 = make_test_slice(b"PING\r\n", 2);
        state.on_recv(slice3);

        // No pending returns - zero copy!
        assert!(!state.has_pending_returns());
        assert_eq!(state.as_slice(), b"PING\r\n");
    }

    #[test]
    fn test_append_owned_mio_style() {
        let mut state = ConnectionRecvState::new(1024);

        state.append_owned(b"GET ");
        state.append_owned(b"key\r\n");

        assert_eq!(state.as_slice(), b"GET key\r\n");
        assert!(!state.has_pending_returns()); // No ring/pool buffers involved
    }

    #[test]
    fn test_clear_returns_held_buffer() {
        let mut state = ConnectionRecvState::new(1024);

        let slice = make_test_slice(b"GET key\r\n", 42);
        state.on_recv(slice);

        state.clear();

        let returns: Vec<_> = state.take_pending_returns().collect();
        assert_eq!(returns.len(), 1);
        assert_eq!(returns[0].buf_id, 42);
    }
}
