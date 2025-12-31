//! Per-connection state for the cache server.

use bytes::{Buf, BytesMut};
use cache_core::Cache;
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

/// Per-connection state for the cache server.
pub struct Connection {
    read_buf: BytesMut,
    write_buf: BytesMut,
    write_pos: usize,
    protocol: DetectedProtocol,
    should_close: bool,
    resp_version: RespVersion,
}

impl Connection {
    pub fn new(read_buffer_size: usize) -> Self {
        Self {
            read_buf: BytesMut::with_capacity(read_buffer_size),
            write_buf: BytesMut::with_capacity(65536),
            write_pos: 0,
            protocol: DetectedProtocol::Unknown,
            should_close: false,
            resp_version: RespVersion::default(),
        }
    }

    /// Append received data to the read buffer.
    #[inline]
    pub fn append_recv_data(&mut self, data: &[u8]) {
        // Compact the buffer periodically to prevent unbounded growth.
        // BytesMut::reserve() triggers compaction when the unused prefix
        // exceeds the requested amount plus remaining capacity.
        // Check: if len * 2 < capacity (i.e., less than 50% utilized), compact.
        let cap = self.read_buf.capacity();
        if cap > 0 && self.read_buf.len() * 2 < cap {
            self.read_buf.reserve(data.len());
        }
        self.read_buf.extend_from_slice(data);
    }

    /// Detect protocol from the first byte of data.
    #[inline]
    fn detect_protocol(&mut self) {
        if self.protocol != DetectedProtocol::Unknown || self.read_buf.is_empty() {
            return;
        }

        match self.read_buf[0] {
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

    #[inline]
    fn process_resp<C: Cache>(&mut self, cache: &C) {
        loop {
            if self.read_buf.is_empty() {
                break;
            }

            match RespCommand::parse(&self.read_buf) {
                Ok((cmd, consumed)) => {
                    execute_resp(&cmd, cache, &mut self.write_buf, &mut self.resp_version);
                    self.read_buf.advance(consumed);
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
                    self.read_buf.clear();
                    break;
                }
            }
        }
    }

    #[inline]
    fn process_memcache_ascii<C: Cache>(&mut self, cache: &C) {
        loop {
            if self.read_buf.is_empty() {
                break;
            }

            match MemcacheCommand::parse(&self.read_buf) {
                Ok((cmd, consumed)) => {
                    if execute_memcache(&cmd, cache, &mut self.write_buf) {
                        self.should_close = true;
                    }
                    self.read_buf.advance(consumed);
                }
                Err(MemcacheParseError::Incomplete) => break,
                Err(e) => {
                    PROTOCOL_ERRORS.increment();
                    self.write_buf.extend_from_slice(b"ERROR ");
                    self.write_buf.extend_from_slice(e.to_string().as_bytes());
                    self.write_buf.extend_from_slice(b"\r\n");
                    self.read_buf.clear();
                    break;
                }
            }
        }
    }

    #[inline]
    fn process_memcache_binary<C: Cache>(&mut self, cache: &C) {
        loop {
            if self.read_buf.is_empty() {
                break;
            }

            match BinaryCommand::parse(&self.read_buf) {
                Ok((cmd, consumed)) => {
                    if execute_memcache_binary(&cmd, cache, &mut self.write_buf) {
                        self.should_close = true;
                    }
                    self.read_buf.advance(consumed);
                }
                Err(MemcacheParseError::Incomplete) => break,
                Err(_) => {
                    PROTOCOL_ERRORS.increment();
                    self.should_close = true;
                    self.read_buf.clear();
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
}
