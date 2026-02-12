//! Memcache binary protocol codec.
//!
//! This module provides a wrapper around protocol-memcache's binary module
//! that integrates with brrr's Buffer type for zero-copy encoding and decoding.

use crate::buffer::Buffer;
use protocol_memcache::ParseError;
use protocol_memcache::binary::{BinaryRequest, HEADER_SIZE, ParsedBinaryResponse, Status};

/// Memcache binary protocol codec.
///
/// Wraps protocol-memcache's binary module to work with brrr's Buffer type.
/// Tracks pending responses using opaque values for request/response correlation.
#[derive(Debug)]
pub struct MemcacheBinaryCodec {
    /// Next opaque value to use
    next_opaque: u32,
    /// Number of pending responses expected
    pending_responses: usize,
}

impl MemcacheBinaryCodec {
    pub fn new() -> Self {
        Self {
            next_opaque: 0,
            pending_responses: 0,
        }
    }

    /// Encodes a GET command.
    ///
    /// Returns the opaque value used for this request.
    pub fn encode_get(&mut self, buf: &mut Buffer, key: &[u8]) -> u32 {
        let opaque = self.next_opaque;
        self.next_opaque = self.next_opaque.wrapping_add(1);

        // GET: header + key
        let needed = HEADER_SIZE + key.len();
        Self::ensure_space(buf, needed);
        let len = BinaryRequest::encode_get(buf.spare_mut(), key, opaque);
        buf.advance(len);
        self.pending_responses += 1;
        opaque
    }

    /// Encodes a SET command.
    ///
    /// Returns the opaque value used for this request.
    pub fn encode_set(
        &mut self,
        buf: &mut Buffer,
        key: &[u8],
        value: &[u8],
        flags: u32,
        expiration: u32,
    ) -> u32 {
        let opaque = self.next_opaque;
        self.next_opaque = self.next_opaque.wrapping_add(1);

        // SET: header + 8 bytes extras + key + value
        let needed = HEADER_SIZE + 8 + key.len() + value.len();
        Self::ensure_space(buf, needed);
        // CAS 0 means no CAS check
        let len =
            BinaryRequest::encode_set(buf.spare_mut(), key, value, flags, expiration, 0, opaque);
        buf.advance(len);
        self.pending_responses += 1;
        opaque
    }

    /// Encodes a DELETE command.
    ///
    /// Returns the opaque value used for this request.
    pub fn encode_delete(&mut self, buf: &mut Buffer, key: &[u8]) -> u32 {
        let opaque = self.next_opaque;
        self.next_opaque = self.next_opaque.wrapping_add(1);

        // DELETE: header + key
        let needed = HEADER_SIZE + key.len();
        Self::ensure_space(buf, needed);
        // CAS 0 means no CAS check
        let len = BinaryRequest::encode_delete(buf.spare_mut(), key, 0, opaque);
        buf.advance(len);
        self.pending_responses += 1;
        opaque
    }

    /// Encodes a quiet GET command (GETQ).
    ///
    /// Quiet commands don't receive a response on cache miss, only on hit.
    /// Returns the opaque value used for this request.
    pub fn encode_get_quiet(&mut self, buf: &mut Buffer, key: &[u8]) -> u32 {
        let opaque = self.next_opaque;
        self.next_opaque = self.next_opaque.wrapping_add(1);

        // GETQ: header + key
        let needed = HEADER_SIZE + key.len();
        Self::ensure_space(buf, needed);
        let len = BinaryRequest::encode_getq(buf.spare_mut(), key, opaque);
        buf.advance(len);
        // Note: for quiet ops we don't increment pending_responses
        // since we may not get a response
        opaque
    }

    /// Encodes a NOOP command.
    ///
    /// NOOP is typically used to flush pending quiet operations and ensure
    /// all responses have been received.
    pub fn encode_noop(&mut self, buf: &mut Buffer) -> u32 {
        let opaque = self.next_opaque;
        self.next_opaque = self.next_opaque.wrapping_add(1);

        // NOOP: header only
        Self::ensure_space(buf, HEADER_SIZE);
        let len = BinaryRequest::encode_noop(buf.spare_mut(), opaque);
        buf.advance(len);
        self.pending_responses += 1;
        opaque
    }

    /// Ensures the buffer has at least `needed` bytes of writable space.
    /// Compacts the buffer first, then grows if necessary.
    #[inline]
    fn ensure_space(buf: &mut Buffer, needed: usize) {
        if buf.writable() < needed {
            buf.compact();
        }
        if buf.writable() < needed {
            buf.grow(needed);
        }
    }

    /// Returns the number of pending responses.
    pub fn pending_responses(&self) -> usize {
        self.pending_responses
    }

    /// Allocate the next opaque value and increment pending responses
    /// (for guard-based send path where encoding is done externally).
    pub fn allocate_opaque(&mut self) -> u32 {
        let opaque = self.next_opaque;
        self.next_opaque = self.next_opaque.wrapping_add(1);
        self.pending_responses += 1;
        opaque
    }

    /// Attempts to decode a response from the buffer.
    ///
    /// Returns:
    /// - `Ok(Some(response))` if a complete response was parsed
    /// - `Ok(None)` if more data is needed
    /// - `Err(e)` if the data is malformed
    pub fn decode_response(
        &mut self,
        buf: &mut Buffer,
    ) -> Result<Option<MemcacheBinaryResponse>, MemcacheBinaryError> {
        let data = buf.as_slice();
        if data.len() < HEADER_SIZE {
            return Ok(None);
        }

        match ParsedBinaryResponse::parse(data) {
            Ok((response, consumed)) => {
                // Convert to owned before consuming buffer
                let owned = MemcacheBinaryResponse::from_parsed(&response);
                buf.consume(consumed);
                self.pending_responses = self.pending_responses.saturating_sub(1);
                Ok(Some(owned))
            }
            Err(ParseError::Incomplete) => Ok(None),
            Err(e) => Err(MemcacheBinaryError(e)),
        }
    }

    /// Decodes multiple responses from the buffer.
    pub fn decode_responses(
        &mut self,
        buf: &mut Buffer,
        out: &mut Vec<MemcacheBinaryResponse>,
    ) -> Result<usize, MemcacheBinaryError> {
        let mut count = 0;
        while let Some(response) = self.decode_response(buf)? {
            out.push(response);
            count += 1;
        }
        Ok(count)
    }

    /// Resets the codec state.
    pub fn reset(&mut self) {
        self.next_opaque = 0;
        self.pending_responses = 0;
    }

    /// Decrement pending responses counter (for zero-copy path).
    #[inline]
    pub fn decrement_pending(&mut self) {
        self.pending_responses = self.pending_responses.saturating_sub(1);
    }
}

impl Default for MemcacheBinaryCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// An owned memcache binary response.
#[derive(Debug, Clone)]
pub enum MemcacheBinaryResponse {
    /// Value response (GET hit)
    Value {
        opaque: u32,
        cas: u64,
        flags: u32,
        value: Vec<u8>,
    },
    /// Counter response (INCR/DECR)
    Counter { opaque: u32, cas: u64, value: u64 },
    /// Simple success response
    Success { opaque: u32, cas: u64 },
    /// Error response
    Error { opaque: u32, status: Status },
}

impl MemcacheBinaryResponse {
    /// Convert from a parsed response to an owned response.
    fn from_parsed(parsed: &ParsedBinaryResponse<'_>) -> Self {
        match parsed {
            ParsedBinaryResponse::Value {
                opaque,
                cas,
                flags,
                value,
                ..
            } => MemcacheBinaryResponse::Value {
                opaque: *opaque,
                cas: *cas,
                flags: *flags,
                value: value.to_vec(),
            },
            ParsedBinaryResponse::Counter {
                opaque, cas, value, ..
            } => MemcacheBinaryResponse::Counter {
                opaque: *opaque,
                cas: *cas,
                value: *value,
            },
            ParsedBinaryResponse::Success { opaque, cas, .. } => MemcacheBinaryResponse::Success {
                opaque: *opaque,
                cas: *cas,
            },
            ParsedBinaryResponse::Error { opaque, status, .. } => MemcacheBinaryResponse::Error {
                opaque: *opaque,
                status: *status,
            },
            ParsedBinaryResponse::Version { opaque, .. } => MemcacheBinaryResponse::Success {
                opaque: *opaque,
                cas: 0,
            },
            ParsedBinaryResponse::Stat { opaque, .. } => MemcacheBinaryResponse::Success {
                opaque: *opaque,
                cas: 0,
            },
        }
    }

    /// Returns the opaque value from the response.
    pub fn opaque(&self) -> u32 {
        match self {
            MemcacheBinaryResponse::Value { opaque, .. }
            | MemcacheBinaryResponse::Counter { opaque, .. }
            | MemcacheBinaryResponse::Success { opaque, .. }
            | MemcacheBinaryResponse::Error { opaque, .. } => *opaque,
        }
    }

    /// Returns true if this is an error response.
    pub fn is_error(&self) -> bool {
        matches!(self, MemcacheBinaryResponse::Error { .. })
    }

    /// Returns true if this is a cache miss (KeyNotFound).
    pub fn is_miss(&self) -> bool {
        matches!(
            self,
            MemcacheBinaryResponse::Error {
                status: Status::KeyNotFound,
                ..
            }
        )
    }

    /// Returns true if this is a successful storage response.
    pub fn is_stored(&self) -> bool {
        matches!(self, MemcacheBinaryResponse::Success { .. })
    }

    /// Returns the value if this is a Value response.
    pub fn value(&self) -> Option<&[u8]> {
        match self {
            MemcacheBinaryResponse::Value { value, .. } => Some(value),
            _ => None,
        }
    }

    /// Returns the flags if this is a Value response.
    pub fn flags(&self) -> Option<u32> {
        match self {
            MemcacheBinaryResponse::Value { flags, .. } => Some(*flags),
            _ => None,
        }
    }

    /// Returns the CAS value if available.
    pub fn cas(&self) -> Option<u64> {
        match self {
            MemcacheBinaryResponse::Value { cas, .. }
            | MemcacheBinaryResponse::Counter { cas, .. }
            | MemcacheBinaryResponse::Success { cas, .. } => Some(*cas),
            _ => None,
        }
    }

    /// Returns the status if this is an error response.
    pub fn status(&self) -> Option<Status> {
        match self {
            MemcacheBinaryResponse::Error { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// Returns the counter value if this is a Counter response.
    pub fn counter_value(&self) -> Option<u64> {
        match self {
            MemcacheBinaryResponse::Counter { value, .. } => Some(*value),
            _ => None,
        }
    }
}

/// Memcache binary protocol errors.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct MemcacheBinaryError(ParseError);

impl MemcacheBinaryError {
    /// Create from a parse error (for zero-copy path).
    pub fn from_parse_error(e: ParseError) -> Self {
        Self(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_get() {
        let mut codec = MemcacheBinaryCodec::new();
        let mut buf = Buffer::with_capacity(1024);

        let opaque = codec.encode_get(&mut buf, b"mykey");
        assert_eq!(opaque, 0);
        assert_eq!(codec.pending_responses(), 1);
        assert_eq!(buf.readable(), HEADER_SIZE + 5); // header + key
    }

    #[test]
    fn test_encode_set() {
        let mut codec = MemcacheBinaryCodec::new();
        let mut buf = Buffer::with_capacity(1024);

        let opaque = codec.encode_set(&mut buf, b"mykey", b"myvalue", 0, 3600);
        assert_eq!(opaque, 0);
        assert_eq!(codec.pending_responses(), 1);
        assert_eq!(buf.readable(), HEADER_SIZE + 8 + 5 + 7); // header + extras + key + value
    }

    #[test]
    fn test_opaque_incrementing() {
        let mut codec = MemcacheBinaryCodec::new();
        let mut buf = Buffer::with_capacity(4096);

        let op1 = codec.encode_get(&mut buf, b"key1");
        let op2 = codec.encode_get(&mut buf, b"key2");
        let op3 = codec.encode_set(&mut buf, b"key3", b"val", 0, 0);

        assert_eq!(op1, 0);
        assert_eq!(op2, 1);
        assert_eq!(op3, 2);
        assert_eq!(codec.pending_responses(), 3);
    }
}
