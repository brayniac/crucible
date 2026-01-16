//! Per-connection state for the cache server.

use bytes::{Bytes, BytesMut};
use cache_core::{Cache, ValueRef};
use io_driver::RecvBuf;
use protocol_memcache::binary::{BinaryCommand, REQUEST_MAGIC};
use protocol_memcache::{Command as MemcacheCommand, ParseError as MemcacheParseError};
use protocol_resp::{Command as RespCommand, ParseError as RespParseError, ParseOptions};
use std::io::IoSlice;

use crate::config::ZeroCopyMode;
use crate::execute::{RespVersion, execute_memcache, execute_memcache_binary, execute_resp};
use crate::metrics::PROTOCOL_ERRORS;

/// A zero-copy GET response for vectored I/O.
///
/// This struct holds the header, value reference, and trailer for a GET response.
/// The value reference points directly into cache segment memory, avoiding copies.
pub struct ZeroCopyResponse {
    /// RESP header: `$<len>\r\n` (max ~25 bytes)
    header: [u8; 32],
    header_len: usize,
    /// Reference to value in cache segment memory
    value_ref: ValueRef,
    /// Static trailer: "\r\n"
    trailer: &'static [u8],
}

impl ZeroCopyResponse {
    /// Create a new zero-copy response for a RESP bulk string.
    #[inline]
    pub fn new_resp_bulk_string(value_ref: ValueRef) -> Self {
        let mut header = [0u8; 32];
        let mut pos = 0;

        // Write "$"
        header[pos] = b'$';
        pos += 1;

        // Write length using itoa
        let mut len_buf = itoa::Buffer::new();
        let len_str = len_buf.format(value_ref.len()).as_bytes();
        header[pos..pos + len_str.len()].copy_from_slice(len_str);
        pos += len_str.len();

        // Write "\r\n"
        header[pos] = b'\r';
        header[pos + 1] = b'\n';
        pos += 2;

        Self {
            header,
            header_len: pos,
            value_ref,
            trailer: b"\r\n",
        }
    }

    /// Get the header bytes.
    #[inline]
    pub fn header(&self) -> &[u8] {
        &self.header[..self.header_len]
    }

    /// Get the value reference.
    #[inline]
    pub fn value(&self) -> &[u8] {
        self.value_ref.as_slice()
    }

    /// Get the trailer bytes.
    #[inline]
    pub fn trailer(&self) -> &[u8] {
        self.trailer
    }

    /// Get total response length.
    #[inline]
    pub fn total_len(&self) -> usize {
        self.header_len + self.value_ref.len() + self.trailer.len()
    }

    /// Create IoSlices for vectored I/O.
    #[inline]
    pub fn as_io_slices(&self) -> [IoSlice<'_>; 3] {
        [
            IoSlice::new(self.header()),
            IoSlice::new(self.value()),
            IoSlice::new(self.trailer()),
        ]
    }

    /// Convert to owned Bytes for true zero-copy scatter-gather I/O.
    ///
    /// This method takes ownership of the response and returns a Vec of owned Bytes:
    /// - Header: small copy (~25 bytes)
    /// - Value: zero-copy from segment memory (ValueRef wrapped in Bytes)
    /// - Trailer: static reference
    ///
    /// The segment ref count is held in the value Bytes until it's dropped.
    #[inline]
    pub fn into_owned_bytes(self) -> Vec<Bytes> {
        vec![
            Bytes::copy_from_slice(&self.header[..self.header_len]),
            self.value_ref.into_bytes(),
            Bytes::from_static(self.trailer),
        ]
    }
}

/// Protocol type detected for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetectedProtocol {
    Unknown,
    Resp,
    MemcacheAscii,
    MemcacheBinary,
}

/// Per-connection state for the cache server.
///
/// The connection no longer owns a read buffer - instead, data is accessed
/// directly from the driver's receive buffer via `process_from()`.
/// This enables zero-copy parsing when data fits in a single kernel buffer.
pub struct Connection {
    write_buf: BytesMut,
    write_pos: usize,
    protocol: DetectedProtocol,
    should_close: bool,
    resp_version: RespVersion,
    /// Parse options for RESP protocol (controls max value size, etc.)
    resp_parse_options: ParseOptions,
}

impl Connection {
    /// Create a new connection with the specified max value size.
    pub fn new(max_value_size: usize) -> Self {
        Self {
            write_buf: BytesMut::with_capacity(65536),
            write_pos: 0,
            protocol: DetectedProtocol::Unknown,
            should_close: false,
            resp_version: RespVersion::default(),
            resp_parse_options: ParseOptions::new().max_bulk_string_len(max_value_size),
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

    /// Detect protocol from the first byte of data.
    #[inline]
    fn detect_protocol(&mut self, first_byte: u8) {
        if self.protocol != DetectedProtocol::Unknown {
            return;
        }

        match first_byte {
            REQUEST_MAGIC => {
                self.protocol = DetectedProtocol::MemcacheBinary;
            }
            b'*' => {
                self.protocol = DetectedProtocol::Resp;
            }
            _ => {
                self.protocol = DetectedProtocol::MemcacheAscii;
            }
        }
    }

    /// Process all complete commands from the receive buffer.
    ///
    /// This is the zero-copy path - data is read directly from the driver's
    /// buffer without copying into a connection-owned buffer.
    #[inline]
    pub fn process_from<C: Cache>(&mut self, buf: &mut dyn RecvBuf, cache: &C) {
        // Clear write buffer if all data has been sent
        if self.write_pos >= self.write_buf.len() {
            self.write_buf.clear();
            self.write_pos = 0;
        }

        // Detect protocol from first byte
        let data = buf.as_slice();
        if !data.is_empty() && self.protocol == DetectedProtocol::Unknown {
            self.detect_protocol(data[0]);
        }

        match self.protocol {
            DetectedProtocol::Unknown => {}
            DetectedProtocol::Resp => self.process_resp_from(buf, cache),
            DetectedProtocol::MemcacheAscii => self.process_memcache_ascii_from(buf, cache),
            DetectedProtocol::MemcacheBinary => self.process_memcache_binary_from(buf, cache),
        }
    }

    #[inline]
    fn process_resp_from<C: Cache>(&mut self, buf: &mut dyn RecvBuf, cache: &C) {
        loop {
            let data = buf.as_slice();
            if data.is_empty() {
                break;
            }

            // Backpressure: stop processing if write buffer is too large
            if self.pending_write_len() > Self::MAX_PENDING_WRITE {
                break;
            }

            match RespCommand::parse_with_options(data, &self.resp_parse_options) {
                Ok((cmd, consumed)) => {
                    execute_resp(&cmd, cache, &mut self.write_buf, &mut self.resp_version);
                    buf.consume(consumed);
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
                    // Consume all remaining data on error
                    let len = buf.len();
                    buf.consume(len);
                    break;
                }
            }
        }
    }

    #[inline]
    fn process_memcache_ascii_from<C: Cache>(&mut self, buf: &mut dyn RecvBuf, cache: &C) {
        loop {
            let data = buf.as_slice();
            if data.is_empty() {
                break;
            }

            // Backpressure: stop processing if write buffer is too large
            if self.pending_write_len() > Self::MAX_PENDING_WRITE {
                break;
            }

            match MemcacheCommand::parse(data) {
                Ok((cmd, consumed)) => {
                    if execute_memcache(&cmd, cache, &mut self.write_buf) {
                        self.should_close = true;
                    }
                    buf.consume(consumed);
                }
                Err(MemcacheParseError::Incomplete) => break,
                Err(e) => {
                    PROTOCOL_ERRORS.increment();
                    self.write_buf.extend_from_slice(b"ERROR ");
                    self.write_buf.extend_from_slice(e.to_string().as_bytes());
                    self.write_buf.extend_from_slice(b"\r\n");
                    // Consume all remaining data on error
                    let len = buf.len();
                    buf.consume(len);
                    break;
                }
            }
        }
    }

    #[inline]
    fn process_memcache_binary_from<C: Cache>(&mut self, buf: &mut dyn RecvBuf, cache: &C) {
        loop {
            let data = buf.as_slice();
            if data.is_empty() {
                break;
            }

            // Backpressure: stop processing if write buffer is too large
            if self.pending_write_len() > Self::MAX_PENDING_WRITE {
                break;
            }

            match BinaryCommand::parse(data) {
                Ok((cmd, consumed)) => {
                    if execute_memcache_binary(&cmd, cache, &mut self.write_buf) {
                        self.should_close = true;
                    }
                    buf.consume(consumed);
                }
                Err(MemcacheParseError::Incomplete) => break,
                Err(_) => {
                    PROTOCOL_ERRORS.increment();
                    self.should_close = true;
                    // Consume all remaining data on error
                    let len = buf.len();
                    buf.consume(len);
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

    /// Process a single RESP command with zero-copy support.
    ///
    /// Returns `Some(ZeroCopyResponse)` if the command is a GET that resulted in a hit
    /// and zero-copy is enabled for this value size. The caller should send the response
    /// using vectored I/O.
    ///
    /// Returns `None` if:
    /// - The command is not a GET
    /// - The GET resulted in a miss
    /// - Zero-copy is disabled or the value is below threshold
    /// - There's no complete command in the buffer
    ///
    /// In these cases, check `has_pending_write()` for regular response data.
    #[inline]
    pub fn process_one_zero_copy<C: Cache>(
        &mut self,
        buf: &mut dyn RecvBuf,
        cache: &C,
        zero_copy_mode: ZeroCopyMode,
    ) -> Option<ZeroCopyResponse> {
        // Clear write buffer if all data has been sent
        if self.write_pos >= self.write_buf.len() {
            self.write_buf.clear();
            self.write_pos = 0;
        }

        // Detect protocol from first byte
        let data = buf.as_slice();
        if data.is_empty() {
            return None;
        }

        if self.protocol == DetectedProtocol::Unknown {
            self.detect_protocol(data[0]);
        }

        // Only RESP protocol supports zero-copy GET for now
        if self.protocol != DetectedProtocol::Resp {
            // Process normally for other protocols
            self.process_from(buf, cache);
            return None;
        }

        // Try to parse a RESP command
        match RespCommand::parse_with_options(data, &self.resp_parse_options) {
            Ok((cmd, consumed)) => {
                // Check if it's a GET command
                if let RespCommand::Get { key } = cmd {
                    use crate::metrics::{GETS, HITS, MISSES};
                    GETS.increment();

                    // Try zero-copy path
                    if let Some(value_ref) = cache.get_value_ref(key) {
                        if zero_copy_mode.should_use_zero_copy(value_ref.len()) {
                            HITS.increment();
                            buf.consume(consumed);
                            return Some(ZeroCopyResponse::new_resp_bulk_string(value_ref));
                        }
                        // Fall through to copy path - value_ref is dropped here
                    }

                    // Copy path - either miss or below threshold
                    let hit = cache.with_value(key, |value| {
                        // Reserve: $ + max_len_digits(20) + \r\n + value + \r\n
                        let needed = 1 + 20 + 2 + value.len() + 2;
                        self.write_buf.reserve(needed);

                        let spare = self.write_buf.spare_capacity_mut();
                        let buf_slice = unsafe {
                            std::slice::from_raw_parts_mut(
                                spare.as_mut_ptr() as *mut u8,
                                spare.len(),
                            )
                        };
                        let written = unsafe { write_bulk_string(buf_slice, value) };
                        unsafe { self.write_buf.set_len(self.write_buf.len() + written) };
                    });

                    if hit.is_some() {
                        HITS.increment();
                    } else {
                        MISSES.increment();
                        if self.resp_version == RespVersion::Resp3 {
                            self.write_buf.extend_from_slice(b"_\r\n");
                        } else {
                            self.write_buf.extend_from_slice(b"$-1\r\n");
                        }
                    }
                    buf.consume(consumed);
                } else {
                    // Not a GET - execute normally
                    execute_resp(&cmd, cache, &mut self.write_buf, &mut self.resp_version);
                    buf.consume(consumed);
                }
                None
            }
            Err(RespParseError::Incomplete) => None,
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
                // Consume all remaining data on error
                let len = buf.len();
                buf.consume(len);
                None
            }
        }
    }
}

/// Write a RESP bulk string response directly to a buffer.
/// Returns the number of bytes written.
///
/// # Safety
/// Caller must ensure `buf` has enough capacity for the response:
/// at least 1 + 20 + 2 + value.len() + 2 bytes.
#[inline]
unsafe fn write_bulk_string(buf: &mut [u8], value: &[u8]) -> usize {
    let mut pos = 0;
    buf[pos] = b'$';
    pos += 1;

    // Write length using itoa
    let mut len_buf = itoa::Buffer::new();
    let len_str = len_buf.format(value.len()).as_bytes();
    // SAFETY: Caller guarantees buf has enough capacity
    unsafe {
        std::ptr::copy_nonoverlapping(len_str.as_ptr(), buf.as_mut_ptr().add(pos), len_str.len());
    }
    pos += len_str.len();

    buf[pos] = b'\r';
    buf[pos + 1] = b'\n';
    pos += 2;

    // SAFETY: Caller guarantees buf has enough capacity
    unsafe {
        std::ptr::copy_nonoverlapping(value.as_ptr(), buf.as_mut_ptr().add(pos), value.len());
    }
    pos += value.len();

    buf[pos] = b'\r';
    buf[pos + 1] = b'\n';
    pos += 2;

    pos
}

impl Default for Connection {
    fn default() -> Self {
        // Use 1MB as the default max value size (same as server default)
        Self::new(1024 * 1024)
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

        fn with_value<F, R>(&self, _key: &[u8], _f: F) -> Option<R>
        where
            F: FnOnce(&[u8]) -> R,
        {
            None
        }

        fn get_value_ref(&self, _key: &[u8]) -> Option<cache_core::ValueRef> {
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

    /// A simple test buffer that implements RecvBuf
    struct TestRecvBuf {
        data: Vec<u8>,
        offset: usize,
    }

    impl TestRecvBuf {
        fn new(data: &[u8]) -> Self {
            Self {
                data: data.to_vec(),
                offset: 0,
            }
        }

        fn append(&mut self, data: &[u8]) {
            self.data.extend_from_slice(data);
        }
    }

    impl RecvBuf for TestRecvBuf {
        fn as_slice(&self) -> &[u8] {
            &self.data[self.offset..]
        }

        fn len(&self) -> usize {
            self.data.len() - self.offset
        }

        fn consume(&mut self, n: usize) {
            self.offset += n;
            // Compact when all data consumed
            if self.offset >= self.data.len() {
                self.data.clear();
                self.offset = 0;
            }
        }
    }

    #[test]
    fn test_partial_request() {
        let cache = MockCache;
        let mut conn = Connection::default();

        // Send partial RESP command: "*2\r\n$3\r\nGET\r\n$3\r\nke"
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nke");
        conn.process_from(&mut buf, &cache);

        // Should have no response yet (incomplete request)
        assert!(!conn.has_pending_write());
        // Data should remain unconsumed
        assert_eq!(buf.as_slice(), b"*2\r\n$3\r\nGET\r\n$3\r\nke");

        // Complete the request: "y\r\n"
        buf.append(b"y\r\n");
        conn.process_from(&mut buf, &cache);

        // Now should have a response
        assert!(conn.has_pending_write());
        // Data should be consumed
        assert!(buf.as_slice().is_empty());
    }

    #[test]
    fn test_pipelined_requests() {
        let cache = MockCache;
        let mut conn = Connection::default();

        // Send two complete GET requests in one batch
        let mut buf =
            TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
        conn.process_from(&mut buf, &cache);

        // Should have responses for both
        assert!(conn.has_pending_write());
        let response = conn.pending_write_data();
        // Two $-1\r\n responses (null bulk string for cache miss)
        assert_eq!(response, b"$-1\r\n$-1\r\n");
        // All data consumed
        assert!(buf.as_slice().is_empty());
    }

    #[test]
    fn test_complete_plus_partial() {
        let cache = MockCache;
        let mut conn = Connection::default();

        // Send one complete request and start of another
        let mut buf =
            TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*2\r\n$3\r\nGET\r\n$3\r\nba");
        conn.process_from(&mut buf, &cache);

        // Should have response for first request
        assert!(conn.has_pending_write());
        let response = conn.pending_write_data();
        assert_eq!(response, b"$-1\r\n");
        // Partial request should remain
        assert_eq!(buf.as_slice(), b"*2\r\n$3\r\nGET\r\n$3\r\nba");

        // Complete the second request
        buf.append(b"r\r\n");
        conn.process_from(&mut buf, &cache);

        // Now should have both responses
        let response = conn.pending_write_data();
        assert_eq!(response, b"$-1\r\n$-1\r\n");
        assert!(buf.as_slice().is_empty());
    }

    #[test]
    fn test_partial_write_advance() {
        let cache = MockCache;
        let mut conn = Connection::default();

        // Generate a response
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process_from(&mut buf, &cache);

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
        let mut conn = Connection::default();

        // First request
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process_from(&mut buf, &cache);

        // "Send" all data
        let pending = conn.pending_write_data().len();
        conn.advance_write(pending);
        assert!(!conn.has_pending_write());

        // Second request - should work fine
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
        conn.process_from(&mut buf, &cache);

        // New response should be generated
        assert!(conn.has_pending_write());
        assert_eq!(conn.pending_write_data(), b"$-1\r\n");
    }

    #[test]
    fn test_write_buffer_not_cleared_on_partial_send() {
        let cache = MockCache;
        let mut conn = Connection::default();

        // First request
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process_from(&mut buf, &cache);

        // Partial send (only 2 bytes)
        conn.advance_write(2);

        // Second request arrives - write_buf should NOT be cleared
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
        conn.process_from(&mut buf, &cache);

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
        let mut conn = Connection::default();

        // Generate enough response data to exceed MAX_PENDING_WRITE
        // Each GET response is "$-1\r\n" (5 bytes)
        // MAX_PENDING_WRITE is 256KB, so we need ~52,000 requests

        // First, send many requests at once
        let single_request = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let mut large_request = Vec::new();
        for _ in 0..60000 {
            large_request.extend_from_slice(single_request);
        }
        let mut buf = TestRecvBuf::new(&large_request);
        conn.process_from(&mut buf, &cache);

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
            !buf.as_slice().is_empty(),
            "buffer should still have unprocessed requests"
        );

        // Now "send" all the data and process again
        conn.advance_write(pending);
        conn.process_from(&mut buf, &cache);

        // More data should have been processed
        assert!(conn.has_pending_write());
    }
}
