//! Per-connection state for the cache server.

use bytes::{Buf, BytesMut};
use cache_core::Cache;
use io_driver::IoBuffer;
use protocol_memcache::binary::{BinaryCommand, REQUEST_MAGIC};
use protocol_memcache::{Command as MemcacheCommand, ParseError as MemcacheParseError};
use protocol_resp::{Command as RespCommand, ParseError as RespParseError};

use crate::execute::{RespVersion, execute_memcache, execute_memcache_binary, execute_resp};
use crate::metrics::PROTOCOL_ERRORS;

/// Protocol type detected for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetectedProtocol {
    Unknown,
    Resp,
    MemcacheAscii,
    MemcacheBinary,
}

/// Read buffer implementation - either BytesMut (multishot) or IoBuffer (single-shot).
enum ReadBuffer {
    /// Standard BytesMut for multishot recv mode.
    BytesMut(BytesMut),
    /// IoBuffer for single-shot recv mode - prevents reallocation while loaned to kernel.
    IoBuffer(IoBuffer),
}

/// Per-connection state for the cache server.
pub struct Connection {
    read_buf: ReadBuffer,
    write_buf: BytesMut,
    write_pos: usize,
    protocol: DetectedProtocol,
    should_close: bool,
    resp_version: RespVersion,
}

impl Connection {
    /// Create a new connection for multishot recv mode (default).
    pub fn new(read_buffer_size: usize) -> Self {
        Self {
            read_buf: ReadBuffer::BytesMut(BytesMut::with_capacity(read_buffer_size)),
            write_buf: BytesMut::with_capacity(65536),
            write_pos: 0,
            protocol: DetectedProtocol::Unknown,
            should_close: false,
            resp_version: RespVersion::default(),
        }
    }

    /// Create a new connection for single-shot recv mode.
    ///
    /// Uses IoBuffer which prevents reallocation while the buffer is loaned to the kernel.
    pub fn new_single_shot(read_buffer_size: usize) -> Self {
        Self {
            read_buf: ReadBuffer::IoBuffer(IoBuffer::with_capacity(read_buffer_size)),
            write_buf: BytesMut::with_capacity(65536),
            write_pos: 0,
            protocol: DetectedProtocol::Unknown,
            should_close: false,
            resp_version: RespVersion::default(),
        }
    }

    /// Returns true if this connection uses single-shot recv mode.
    #[inline]
    pub fn is_single_shot(&self) -> bool {
        matches!(self.read_buf, ReadBuffer::IoBuffer(_))
    }

    /// Append received data to the read buffer (multishot mode).
    #[inline]
    pub fn append_recv_data(&mut self, data: &[u8]) {
        match &mut self.read_buf {
            ReadBuffer::BytesMut(buf) => {
                // Compact the buffer periodically to prevent unbounded growth.
                let cap = buf.capacity();
                if cap > 0 && buf.len() * 2 < cap {
                    buf.reserve(data.len());
                }
                buf.extend_from_slice(data);
            }
            ReadBuffer::IoBuffer(buf) => {
                buf.extend_from_slice(data);
            }
        }
    }

    /// Get a mutable slice of spare capacity for direct recv (multishot mode).
    ///
    /// Returns a slice that the caller can pass directly to read()/recv()
    /// to avoid an intermediate copy. After reading, call `recv_commit(n)`
    /// to update the buffer length.
    #[inline]
    pub fn recv_spare(&mut self) -> &mut [u8] {
        const MIN_RECV_SPACE: usize = 8192;

        match &mut self.read_buf {
            ReadBuffer::BytesMut(buf) => {
                // Compact if utilization is low
                let cap = buf.capacity();
                let len = buf.len();
                if cap > 0 && len * 2 < cap {
                    buf.reserve(MIN_RECV_SPACE);
                } else if cap - len < MIN_RECV_SPACE {
                    buf.reserve(MIN_RECV_SPACE);
                }

                // Return spare capacity as mutable slice
                let spare_cap = buf.capacity() - buf.len();
                unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr().add(buf.len()), spare_cap)
                }
            }
            ReadBuffer::IoBuffer(buf) => {
                buf.ensure_spare(MIN_RECV_SPACE);
                let len = buf.len();
                let spare_cap = buf.spare_capacity();
                unsafe {
                    std::slice::from_raw_parts_mut(
                        (buf.as_slice().as_ptr() as *mut u8).add(len),
                        spare_cap,
                    )
                }
            }
        }
    }

    /// Commit bytes that were written directly to the spare capacity (multishot mode).
    ///
    /// # Safety
    /// Caller must ensure `n` bytes were actually written to the slice
    /// returned by `recv_spare()`.
    #[inline]
    pub fn recv_commit(&mut self, n: usize) {
        match &mut self.read_buf {
            ReadBuffer::BytesMut(buf) => unsafe {
                buf.set_len(buf.len() + n);
            },
            ReadBuffer::IoBuffer(_) => {
                panic!("recv_commit called on IoBuffer - use unloan_recv instead");
            }
        }
    }

    // --- Single-shot recv mode methods ---

    /// Loan the spare capacity to the kernel for single-shot recv.
    ///
    /// Returns a mutable slice that can be passed to `submit_recv()`.
    /// The buffer is marked as loaned and cannot be reallocated until
    /// `unloan_recv()` or `unloan_cancel()` is called.
    ///
    /// # Panics
    /// Panics if the buffer is already loaned or if not in single-shot mode.
    #[inline]
    pub fn loan_recv_spare(&mut self) -> &mut [u8] {
        const MIN_RECV_SPACE: usize = 8192;

        match &mut self.read_buf {
            ReadBuffer::IoBuffer(buf) => {
                buf.ensure_spare(MIN_RECV_SPACE);
                buf.loan_spare()
            }
            ReadBuffer::BytesMut(_) => {
                panic!("loan_recv_spare called on BytesMut - use recv_spare instead");
            }
        }
    }

    /// Return the buffer from the kernel and commit bytes received.
    ///
    /// # Panics
    /// Panics if the buffer is not loaned or if not in single-shot mode.
    #[inline]
    pub fn unloan_recv(&mut self, bytes: usize) {
        match &mut self.read_buf {
            ReadBuffer::IoBuffer(buf) => {
                buf.unloan(bytes);
            }
            ReadBuffer::BytesMut(_) => {
                panic!("unloan_recv called on BytesMut");
            }
        }
    }

    /// Cancel a loan without committing any bytes (e.g., on error).
    ///
    /// # Panics
    /// Panics if the buffer is not loaned or if not in single-shot mode.
    #[inline]
    pub fn unloan_cancel(&mut self) {
        match &mut self.read_buf {
            ReadBuffer::IoBuffer(buf) => {
                buf.unloan_cancel();
            }
            ReadBuffer::BytesMut(_) => {
                panic!("unloan_cancel called on BytesMut");
            }
        }
    }

    /// Returns true if the buffer is currently loaned to the kernel.
    #[inline]
    pub fn is_recv_loaned(&self) -> bool {
        match &self.read_buf {
            ReadBuffer::IoBuffer(buf) => buf.is_loaned(),
            ReadBuffer::BytesMut(_) => false,
        }
    }

    // --- Read buffer access helpers ---

    #[inline]
    fn read_buf_is_empty(&self) -> bool {
        match &self.read_buf {
            ReadBuffer::BytesMut(buf) => buf.is_empty(),
            ReadBuffer::IoBuffer(buf) => buf.is_empty(),
        }
    }

    #[inline]
    fn read_buf_first_byte(&self) -> Option<u8> {
        match &self.read_buf {
            ReadBuffer::BytesMut(buf) => buf.first().copied(),
            ReadBuffer::IoBuffer(buf) => buf.as_slice().first().copied(),
        }
    }

    #[inline]
    fn read_buf_advance(&mut self, n: usize) {
        match &mut self.read_buf {
            ReadBuffer::BytesMut(buf) => buf.advance(n),
            ReadBuffer::IoBuffer(buf) => buf.consume(n),
        }
    }

    #[inline]
    fn read_buf_clear(&mut self) {
        match &mut self.read_buf {
            ReadBuffer::BytesMut(buf) => buf.clear(),
            ReadBuffer::IoBuffer(buf) => buf.clear(),
        }
    }

    /// Detect protocol from the first byte of data.
    #[inline]
    fn detect_protocol(&mut self) {
        if self.protocol != DetectedProtocol::Unknown || self.read_buf_is_empty() {
            return;
        }

        match self.read_buf_first_byte() {
            Some(REQUEST_MAGIC) => {
                self.protocol = DetectedProtocol::MemcacheBinary;
            }
            Some(b'*') => {
                self.protocol = DetectedProtocol::Resp;
            }
            _ => {
                self.protocol = DetectedProtocol::MemcacheAscii;
            }
        }
    }

    /// Process all complete commands in the read buffer.
    #[inline]
    pub fn process<C: Cache>(&mut self, cache: &C) {
        if self.write_pos >= self.write_buf.len() {
            self.write_buf.clear();
            self.write_pos = 0;
        }

        self.detect_protocol();

        match self.protocol {
            DetectedProtocol::Unknown => {}
            DetectedProtocol::Resp => self.process_resp(cache),
            DetectedProtocol::MemcacheAscii => self.process_memcache_ascii(cache),
            DetectedProtocol::MemcacheBinary => self.process_memcache_binary(cache),
        }
    }

    /// Maximum pending write buffer size before applying backpressure.
    /// Stop processing new requests if we have this much unsent data.
    pub const MAX_PENDING_WRITE: usize = 256 * 1024; // 256KB

    /// Check if we should accept more data from the socket.
    /// Returns false when we have too much pending write data (backpressure).
    #[inline]
    pub fn should_read(&self) -> bool {
        self.pending_write_len() <= Self::MAX_PENDING_WRITE
    }

    /// Get the amount of pending write data.
    #[inline]
    pub fn pending_write_len(&self) -> usize {
        self.write_buf.len().saturating_sub(self.write_pos)
    }

    #[inline]
    fn process_resp<C: Cache>(&mut self, cache: &C) {
        loop {
            if self.read_buf_is_empty() {
                break;
            }

            // Backpressure: stop processing if write buffer is too large
            if self.write_buf.len() - self.write_pos > Self::MAX_PENDING_WRITE {
                break;
            }

            // Access buffer directly to allow separate borrows for cmd and write_buf
            let parse_result = match &self.read_buf {
                ReadBuffer::BytesMut(buf) => RespCommand::parse(buf),
                ReadBuffer::IoBuffer(buf) => RespCommand::parse(buf.as_slice()),
            };

            match parse_result {
                Ok((cmd, consumed)) => {
                    execute_resp(&cmd, cache, &mut self.write_buf, &mut self.resp_version);
                    self.read_buf_advance(consumed);
                }
                Err(RespParseError::Incomplete) => break,
                Err(e) => {
                    PROTOCOL_ERRORS.increment();
                    let msg = e.to_string();
                    if msg.contains("expected array") {
                        self.write_buf.extend_from_slice(
                            b"-ERR Protocol error: expected Redis RESP protocol\r\n",
                        );
                    } else {
                        self.write_buf.extend_from_slice(b"-ERR ");
                        self.write_buf.extend_from_slice(msg.as_bytes());
                        self.write_buf.extend_from_slice(b"\r\n");
                    }
                    self.read_buf_clear();
                    break;
                }
            }
        }
    }

    #[inline]
    fn process_memcache_ascii<C: Cache>(&mut self, cache: &C) {
        loop {
            if self.read_buf_is_empty() {
                break;
            }

            // Backpressure: stop processing if write buffer is too large
            if self.write_buf.len() - self.write_pos > Self::MAX_PENDING_WRITE {
                break;
            }

            // Access buffer directly to allow separate borrows for cmd and write_buf
            let parse_result = match &self.read_buf {
                ReadBuffer::BytesMut(buf) => MemcacheCommand::parse(buf),
                ReadBuffer::IoBuffer(buf) => MemcacheCommand::parse(buf.as_slice()),
            };

            match parse_result {
                Ok((cmd, consumed)) => {
                    if execute_memcache(&cmd, cache, &mut self.write_buf) {
                        self.should_close = true;
                    }
                    self.read_buf_advance(consumed);
                }
                Err(MemcacheParseError::Incomplete) => break,
                Err(e) => {
                    PROTOCOL_ERRORS.increment();
                    self.write_buf.extend_from_slice(b"ERROR ");
                    self.write_buf.extend_from_slice(e.to_string().as_bytes());
                    self.write_buf.extend_from_slice(b"\r\n");
                    self.read_buf_clear();
                    break;
                }
            }
        }
    }

    #[inline]
    fn process_memcache_binary<C: Cache>(&mut self, cache: &C) {
        loop {
            if self.read_buf_is_empty() {
                break;
            }

            // Backpressure: stop processing if write buffer is too large
            if self.write_buf.len() - self.write_pos > Self::MAX_PENDING_WRITE {
                break;
            }

            // Access buffer directly to allow separate borrows for cmd and write_buf
            let parse_result = match &self.read_buf {
                ReadBuffer::BytesMut(buf) => BinaryCommand::parse(buf),
                ReadBuffer::IoBuffer(buf) => BinaryCommand::parse(buf.as_slice()),
            };

            match parse_result {
                Ok((cmd, consumed)) => {
                    if execute_memcache_binary(&cmd, cache, &mut self.write_buf) {
                        self.should_close = true;
                    }
                    self.read_buf_advance(consumed);
                }
                Err(MemcacheParseError::Incomplete) => break,
                Err(_) => {
                    PROTOCOL_ERRORS.increment();
                    self.should_close = true;
                    self.read_buf_clear();
                    break;
                }
            }
        }
    }

    #[inline]
    pub fn has_pending_write(&self) -> bool {
        self.write_pos < self.write_buf.len()
    }

    #[inline]
    pub fn pending_write_data(&self) -> &[u8] {
        &self.write_buf[self.write_pos..]
    }

    #[inline]
    pub fn advance_write(&mut self, n: usize) {
        self.write_pos += n;
    }

    #[inline]
    pub fn should_close(&self) -> bool {
        self.should_close
    }

    /// Returns true if the read buffer has no data.
    #[inline]
    pub fn is_read_buf_empty(&self) -> bool {
        self.read_buf_is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A simple mock cache for testing
    struct MockCache;

    impl Cache for MockCache {
        fn get(&self, _key: &[u8]) -> Option<cache_core::OwnedGuard> {
            None
        }

        fn set(
            &self,
            _key: &[u8],
            _value: &[u8],
            _ttl: Option<std::time::Duration>,
        ) -> Result<(), cache_core::CacheError> {
            Ok(())
        }

        fn delete(&self, _key: &[u8]) -> bool {
            false
        }

        fn contains(&self, _key: &[u8]) -> bool {
            false
        }

        fn flush(&self) {}
    }

    #[test]
    fn test_partial_request() {
        let cache = MockCache;
        let mut conn = Connection::new(1024);

        // Send partial RESP command: "*2\r\n$3\r\nGET\r\n$3\r\nke"
        conn.append_recv_data(b"*2\r\n$3\r\nGET\r\n$3\r\nke");
        conn.process(&cache);

        // Should have no response yet (incomplete request)
        assert!(!conn.has_pending_write());

        // Complete the request: "y\r\n"
        conn.append_recv_data(b"y\r\n");
        conn.process(&cache);

        // Now should have a response
        assert!(conn.has_pending_write());
    }

    #[test]
    fn test_pipelined_requests() {
        let cache = MockCache;
        let mut conn = Connection::new(1024);

        // Send two complete GET requests in one batch
        conn.append_recv_data(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
        conn.process(&cache);

        // Should have responses for both
        assert!(conn.has_pending_write());
        let response = conn.pending_write_data();
        // Two $-1\r\n responses (null bulk string for cache miss)
        assert_eq!(response, b"$-1\r\n$-1\r\n");
    }

    #[test]
    fn test_complete_plus_partial() {
        let cache = MockCache;
        let mut conn = Connection::new(1024);

        // Send one complete request and start of another
        conn.append_recv_data(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*2\r\n$3\r\nGET\r\n$3\r\nba");
        conn.process(&cache);

        // Should have response for first request
        assert!(conn.has_pending_write());
        let response = conn.pending_write_data();
        assert_eq!(response, b"$-1\r\n");

        // Complete the second request
        conn.append_recv_data(b"r\r\n");
        conn.process(&cache);

        // Now should have both responses
        let response = conn.pending_write_data();
        assert_eq!(response, b"$-1\r\n$-1\r\n");
    }

    #[test]
    fn test_partial_write_advance() {
        let cache = MockCache;
        let mut conn = Connection::new(1024);

        // Generate a response
        conn.append_recv_data(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process(&cache);

        // Simulate partial write (only 2 bytes sent)
        assert!(conn.has_pending_write());
        let pending = conn.pending_write_data().len();
        conn.advance_write(2);

        // Should still have pending data
        assert!(conn.has_pending_write());
        assert_eq!(conn.pending_write_data().len(), pending - 2);

        // Simulate rest of write
        conn.advance_write(pending - 2);
        assert!(!conn.has_pending_write());
    }

    #[test]
    fn test_write_buffer_cleared_on_full_send() {
        let cache = MockCache;
        let mut conn = Connection::new(1024);

        // First request
        conn.append_recv_data(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process(&cache);

        // "Send" all data
        let pending = conn.pending_write_data().len();
        conn.advance_write(pending);
        assert!(!conn.has_pending_write());

        // Second request - should work fine
        conn.append_recv_data(b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
        conn.process(&cache);

        // New response should be generated
        assert!(conn.has_pending_write());
        assert_eq!(conn.pending_write_data(), b"$-1\r\n");
    }

    #[test]
    fn test_write_buffer_not_cleared_on_partial_send() {
        let cache = MockCache;
        let mut conn = Connection::new(1024);

        // First request
        conn.append_recv_data(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process(&cache);

        // Partial send (only 2 bytes)
        conn.advance_write(2);

        // Second request arrives - write_buf should NOT be cleared
        conn.append_recv_data(b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
        conn.process(&cache);

        // Should have remaining from first response + second response
        assert!(conn.has_pending_write());
        // First response: $-1\r\n (5 bytes), sent 2, remaining 3
        // Second response: $-1\r\n (5 bytes)
        // Total: 8 bytes
        assert_eq!(conn.pending_write_data().len(), 8);
    }

    #[test]
    fn test_backpressure_stops_processing() {
        let cache = MockCache;
        let mut conn = Connection::new(1024);

        // Generate enough response data to exceed MAX_PENDING_WRITE
        // Each GET response is "$-1\r\n" (5 bytes)
        // MAX_PENDING_WRITE is 256KB, so we need ~52,000 requests
        // But we can test with smaller amounts by checking that
        // requests stop being processed when write buffer is large

        // First, send many requests at once
        let single_request = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let mut large_request = Vec::new();
        for _ in 0..60000 {
            large_request.extend_from_slice(single_request);
        }
        conn.append_recv_data(&large_request);
        conn.process(&cache);

        // The write buffer should be capped at around MAX_PENDING_WRITE
        // (actually slightly more since we process one more after the check)
        let pending = conn.pending_write_data().len();
        assert!(
            pending <= Connection::MAX_PENDING_WRITE + 100,
            "pending write {} should be around MAX_PENDING_WRITE {}",
            pending,
            Connection::MAX_PENDING_WRITE
        );

        // There should still be unprocessed data in read buffer
        // (backpressure stopped processing)
        assert!(
            !conn.is_read_buf_empty(),
            "read buffer should still have unprocessed requests"
        );

        // Now "send" all the data and process again
        conn.advance_write(pending);
        conn.process(&cache);

        // More data should have been processed
        assert!(conn.has_pending_write());
    }
}
