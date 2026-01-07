//! Per-connection state for the cache server.

use bytes::BytesMut;
use cache_core::Cache;
use io_driver::{BufferChain, BufferPool};
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
///
/// Uses a `BufferChain` for received data, with buffers borrowed from a shared pool.
/// This keeps per-connection memory overhead minimal (just the chain metadata).
pub struct Connection {
    /// Chain of buffer IDs holding received data
    recv_chain: BufferChain,
    /// Write buffer for responses (still owned, responses are generated)
    write_buf: BytesMut,
    write_pos: usize,
    protocol: DetectedProtocol,
    should_close: bool,
    resp_version: RespVersion,
}

impl Connection {
    /// Create a new connection.
    ///
    /// The connection starts with an empty recv chain. Buffers are added
    /// via `push_recv_buffer()` when data arrives.
    pub fn new() -> Self {
        Self {
            recv_chain: BufferChain::new(),
            write_buf: BytesMut::with_capacity(65536),
            write_pos: 0,
            protocol: DetectedProtocol::Unknown,
            should_close: false,
            resp_version: RespVersion::default(),
        }
    }

    /// Push a received buffer onto the chain.
    ///
    /// Called when data arrives. The buffer ID comes from the shared pool.
    #[inline]
    pub fn push_recv_buffer(&mut self, buf_id: u16, valid_bytes: usize) {
        self.recv_chain.push(buf_id, valid_bytes);
    }

    /// Get the number of readable bytes in the recv chain.
    #[inline]
    pub fn readable(&self) -> usize {
        self.recv_chain.readable()
    }

    /// Check if the recv chain is empty.
    #[inline]
    pub fn recv_is_empty(&self) -> bool {
        self.recv_chain.is_empty()
    }

    /// Drain consumed buffers from the recv chain.
    ///
    /// Returns buffer IDs that should be returned to the pool.
    #[inline]
    pub fn drain_consumed_buffers(&mut self) -> impl Iterator<Item = u16> + '_ {
        self.recv_chain.drain_consumed()
    }

    /// Clear all buffers from the recv chain.
    ///
    /// Returns all buffer IDs for return to the pool.
    /// Call this when closing the connection.
    #[inline]
    pub fn clear_recv_buffers(&mut self) -> impl Iterator<Item = u16> + '_ {
        self.recv_chain.clear()
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

    /// Process all complete commands in the recv chain.
    ///
    /// # Arguments
    /// * `cache` - The cache to execute commands against
    /// * `pool` - The buffer pool (for reading chain data)
    /// * `assembly` - Assembly buffer for fragmented messages
    ///
    /// After processing, call `drain_consumed_buffers()` to return
    /// used buffers to the pool.
    #[inline]
    pub fn process<C: Cache>(&mut self, cache: &C, pool: &BufferPool, assembly: &mut [u8]) {
        if self.write_pos >= self.write_buf.len() {
            self.write_buf.clear();
            self.write_pos = 0;
        }

        if self.recv_chain.is_empty() {
            return;
        }

        // Detect protocol from first byte
        if self.protocol == DetectedProtocol::Unknown
            && let Some(first_chunk) = self.recv_chain.first_chunk(pool)
            && !first_chunk.is_empty()
        {
            self.detect_protocol(first_chunk[0]);
        }

        match self.protocol {
            DetectedProtocol::Unknown => {}
            DetectedProtocol::Resp => self.process_resp(cache, pool, assembly),
            DetectedProtocol::MemcacheAscii => self.process_memcache_ascii(cache, pool, assembly),
            DetectedProtocol::MemcacheBinary => self.process_memcache_binary(cache, pool, assembly),
        }
    }

    /// Maximum pending write buffer size before applying backpressure.
    pub const MAX_PENDING_WRITE: usize = 256 * 1024; // 256KB

    /// Check if we should accept more data from the socket.
    #[inline]
    pub fn should_read(&self) -> bool {
        self.pending_write_len() <= Self::MAX_PENDING_WRITE
    }

    /// Get the amount of pending write data.
    #[inline]
    pub fn pending_write_len(&self) -> usize {
        self.write_buf.len().saturating_sub(self.write_pos)
    }

    /// Get data for parsing - either directly from chain (if contiguous) or assembled.
    ///
    /// Returns a slice of the readable data. If the chain is contiguous,
    /// this is a zero-copy reference into the pool. Otherwise, data is
    /// copied into the assembly buffer.
    #[inline]
    fn get_parse_data<'a>(&self, pool: &'a BufferPool, assembly: &'a mut [u8]) -> &'a [u8] {
        if self.recv_chain.is_contiguous() {
            // Zero-copy path: data is in a single chunk
            self.recv_chain.first_chunk(pool).unwrap_or(&[])
        } else {
            // Assembly path: copy fragmented data
            let n = self.recv_chain.copy_to(pool, assembly);
            &assembly[..n]
        }
    }

    #[inline]
    fn process_resp<C: Cache>(&mut self, cache: &C, pool: &BufferPool, assembly: &mut [u8]) {
        loop {
            if self.recv_chain.is_empty() {
                break;
            }

            // Backpressure: stop processing if write buffer is too large
            if self.write_buf.len() - self.write_pos > Self::MAX_PENDING_WRITE {
                break;
            }

            let data = self.get_parse_data(pool, assembly);
            if data.is_empty() {
                break;
            }

            match RespCommand::parse(data) {
                Ok((cmd, consumed)) => {
                    execute_resp(&cmd, cache, &mut self.write_buf, &mut self.resp_version);
                    self.recv_chain.advance(consumed);
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
                    // Clear chain on error
                    for _ in self.recv_chain.clear() {}
                    break;
                }
            }
        }
    }

    #[inline]
    fn process_memcache_ascii<C: Cache>(
        &mut self,
        cache: &C,
        pool: &BufferPool,
        assembly: &mut [u8],
    ) {
        loop {
            if self.recv_chain.is_empty() {
                break;
            }

            // Backpressure: stop processing if write buffer is too large
            if self.write_buf.len() - self.write_pos > Self::MAX_PENDING_WRITE {
                break;
            }

            let data = self.get_parse_data(pool, assembly);
            if data.is_empty() {
                break;
            }

            match MemcacheCommand::parse(data) {
                Ok((cmd, consumed)) => {
                    if execute_memcache(&cmd, cache, &mut self.write_buf) {
                        self.should_close = true;
                    }
                    self.recv_chain.advance(consumed);
                }
                Err(MemcacheParseError::Incomplete) => break,
                Err(e) => {
                    PROTOCOL_ERRORS.increment();
                    self.write_buf.extend_from_slice(b"ERROR ");
                    self.write_buf.extend_from_slice(e.to_string().as_bytes());
                    self.write_buf.extend_from_slice(b"\r\n");
                    for _ in self.recv_chain.clear() {}
                    break;
                }
            }
        }
    }

    #[inline]
    fn process_memcache_binary<C: Cache>(
        &mut self,
        cache: &C,
        pool: &BufferPool,
        assembly: &mut [u8],
    ) {
        loop {
            if self.recv_chain.is_empty() {
                break;
            }

            // Backpressure: stop processing if write buffer is too large
            if self.write_buf.len() - self.write_pos > Self::MAX_PENDING_WRITE {
                break;
            }

            let data = self.get_parse_data(pool, assembly);
            if data.is_empty() {
                break;
            }

            match BinaryCommand::parse(data) {
                Ok((cmd, consumed)) => {
                    if execute_memcache_binary(&cmd, cache, &mut self.write_buf) {
                        self.should_close = true;
                    }
                    self.recv_chain.advance(consumed);
                }
                Err(MemcacheParseError::Incomplete) => break,
                Err(_) => {
                    PROTOCOL_ERRORS.increment();
                    self.should_close = true;
                    for _ in self.recv_chain.clear() {}
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

impl Default for Connection {
    fn default() -> Self {
        Self::new()
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

    /// Helper to simulate receiving data into the pool and pushing to connection
    fn recv_data(conn: &mut Connection, pool: &mut BufferPool, data: &[u8]) {
        let buf_id = pool.checkout().expect("pool exhausted");
        let buf = pool.get_mut(buf_id);
        buf[..data.len()].copy_from_slice(data);
        conn.push_recv_buffer(buf_id, data.len());
    }

    /// Helper to return consumed buffers to pool
    fn return_consumed(conn: &mut Connection, pool: &mut BufferPool) {
        for id in conn.drain_consumed_buffers() {
            pool.checkin(id);
        }
    }

    #[test]
    fn test_single_request() {
        let cache = MockCache;
        let mut pool = BufferPool::new(1024, 16);
        let mut assembly = vec![0u8; 4096];
        let mut conn = Connection::new();

        recv_data(&mut conn, &mut pool, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process(&cache, &pool, &mut assembly);

        assert!(conn.has_pending_write());
        assert_eq!(conn.pending_write_data(), b"$-1\r\n");

        return_consumed(&mut conn, &mut pool);
        assert_eq!(pool.free_count(), 16);
    }

    #[test]
    fn test_partial_request() {
        let cache = MockCache;
        let mut pool = BufferPool::new(1024, 16);
        let mut assembly = vec![0u8; 4096];
        let mut conn = Connection::new();

        // Partial command
        recv_data(&mut conn, &mut pool, b"*2\r\n$3\r\nGET\r\n$3\r\nfo");
        conn.process(&cache, &pool, &mut assembly);
        assert!(!conn.has_pending_write());

        // Complete the command
        recv_data(&mut conn, &mut pool, b"o\r\n");
        conn.process(&cache, &pool, &mut assembly);
        assert!(conn.has_pending_write());
        assert_eq!(conn.pending_write_data(), b"$-1\r\n");

        return_consumed(&mut conn, &mut pool);
    }

    #[test]
    fn test_pipelined_requests() {
        let cache = MockCache;
        let mut pool = BufferPool::new(1024, 16);
        let mut assembly = vec![0u8; 4096];
        let mut conn = Connection::new();

        recv_data(
            &mut conn,
            &mut pool,
            b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n",
        );
        conn.process(&cache, &pool, &mut assembly);

        assert!(conn.has_pending_write());
        assert_eq!(conn.pending_write_data(), b"$-1\r\n$-1\r\n");

        return_consumed(&mut conn, &mut pool);
    }

    #[test]
    fn test_fragmented_request() {
        let cache = MockCache;
        let mut pool = BufferPool::new(16, 32); // Small chunks to force fragmentation
        let mut assembly = vec![0u8; 4096];
        let mut conn = Connection::new();

        // Split command across multiple chunks
        recv_data(&mut conn, &mut pool, b"*2\r\n$3\r\nGET");
        recv_data(&mut conn, &mut pool, b"\r\n$3\r\nfoo\r\n");

        // Should NOT be contiguous
        assert!(!conn.recv_chain.is_contiguous());

        conn.process(&cache, &pool, &mut assembly);

        assert!(conn.has_pending_write());
        assert_eq!(conn.pending_write_data(), b"$-1\r\n");

        return_consumed(&mut conn, &mut pool);
    }

    #[test]
    fn test_connection_cleanup() {
        let mut pool = BufferPool::new(1024, 16);
        let mut conn = Connection::new();

        recv_data(&mut conn, &mut pool, b"partial data");
        assert_eq!(pool.free_count(), 15);

        // Clear all buffers (simulating connection close)
        for id in conn.clear_recv_buffers() {
            pool.checkin(id);
        }

        assert_eq!(pool.free_count(), 16);
    }
}
