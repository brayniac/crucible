//! Memcache text protocol codec.
//!
//! This module provides a thin wrapper around protocol-memcache that integrates
//! with brrr's Buffer type for zero-copy encoding and decoding.

use crate::buffer::Buffer;
use protocol_memcache::{ParseError, Request, Response, Value};

/// Memcache text protocol codec.
///
/// Wraps protocol-memcache to work with brrr's Buffer type.
#[derive(Debug)]
pub struct MemcacheCodec {
    /// Number of pending responses expected
    pending_responses: usize,
}

impl MemcacheCodec {
    pub fn new() -> Self {
        Self {
            pending_responses: 0,
        }
    }

    /// Encodes a GET command.
    ///
    /// Format: get <key>\r\n
    /// Response: VALUE <key> <flags> <bytes>\r\n<data>\r\nEND\r\n
    ///       or: END\r\n (miss)
    pub fn encode_get(&mut self, buf: &mut Buffer, key: &[u8]) -> usize {
        let spare = buf.spare_mut();
        let len = Request::get(key).encode(spare);
        buf.advance(len);
        self.pending_responses += 1;
        len
    }

    /// Encodes a multi-GET command (more efficient than multiple single GETs).
    ///
    /// Format: get <key1> <key2> ... <keyN>\r\n
    /// Response: VALUE <key> <flags> <bytes>\r\n<data>\r\n... END\r\n
    pub fn encode_multi_get(&mut self, buf: &mut Buffer, keys: &[&[u8]]) -> usize {
        if keys.is_empty() {
            return 0;
        }

        let spare = buf.spare_mut();
        let len = Request::gets(keys).encode(spare);
        buf.advance(len);
        // Multi-get still produces one response (with multiple VALUE lines)
        self.pending_responses += 1;
        len
    }

    /// Encodes a SET command.
    ///
    /// Format: set <key> <flags> <exptime> <bytes>\r\n<data>\r\n
    /// Response: STORED\r\n or NOT_STORED\r\n or ERROR\r\n
    pub fn encode_set(
        &mut self,
        buf: &mut Buffer,
        key: &[u8],
        value: &[u8],
        flags: u32,
        exptime: u32,
    ) -> usize {
        let spare = buf.spare_mut();
        let len = Request::set(key, value)
            .flags(flags)
            .exptime(exptime)
            .encode(spare);
        buf.advance(len);
        self.pending_responses += 1;
        len
    }

    /// Encodes a DELETE command.
    ///
    /// Format: delete <key>\r\n
    /// Response: DELETED\r\n or NOT_FOUND\r\n
    pub fn encode_delete(&mut self, buf: &mut Buffer, key: &[u8]) -> usize {
        let spare = buf.spare_mut();
        let len = Request::delete(key).encode(spare);
        buf.advance(len);
        self.pending_responses += 1;
        len
    }

    /// Returns the number of pending responses.
    pub fn pending_responses(&self) -> usize {
        self.pending_responses
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
    ) -> Result<Option<MemcacheResponse>, MemcacheError> {
        let data = buf.as_slice();
        if data.is_empty() {
            return Ok(None);
        }

        match Response::parse(data) {
            Ok((response, consumed)) => {
                buf.consume(consumed);
                self.pending_responses = self.pending_responses.saturating_sub(1);
                Ok(Some(MemcacheResponse(response)))
            }
            Err(ParseError::Incomplete) => Ok(None),
            Err(e) => Err(MemcacheError(e)),
        }
    }

    /// Decodes multiple responses from the buffer.
    pub fn decode_responses(
        &mut self,
        buf: &mut Buffer,
        out: &mut Vec<MemcacheResponse>,
    ) -> Result<usize, MemcacheError> {
        let mut count = 0;
        while let Some(response) = self.decode_response(buf)? {
            out.push(response);
            count += 1;
        }
        Ok(count)
    }

    /// Resets the pending response counter.
    pub fn reset(&mut self) {
        self.pending_responses = 0;
    }
}

impl Default for MemcacheCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// A parsed memcache response (wrapper around protocol_memcache::Response).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemcacheResponse(Response);

impl MemcacheResponse {
    /// Returns true if this is an error response.
    pub fn is_error(&self) -> bool {
        self.0.is_error()
    }

    /// Returns true if this represents a cache miss.
    pub fn is_miss(&self) -> bool {
        self.0.is_miss()
    }

    /// Returns true if this is a successful storage response.
    pub fn is_stored(&self) -> bool {
        self.0.is_stored()
    }

    /// Returns the values if this is a Values response.
    pub fn values(&self) -> Option<&[Value]> {
        match &self.0 {
            Response::Values(values) => Some(values),
            _ => None,
        }
    }
}

/// A single value from a GET response.
pub use protocol_memcache::Value as MemcacheValue;

/// Memcache protocol errors (wrapper around protocol_memcache::ParseError).
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct MemcacheError(ParseError);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_get() {
        let mut codec = MemcacheCodec::new();
        let mut buf = Buffer::with_capacity(1024);

        codec.encode_get(&mut buf, b"mykey");
        assert_eq!(buf.as_slice(), b"get mykey\r\n");
        assert_eq!(codec.pending_responses(), 1);
    }

    #[test]
    fn test_encode_set() {
        let mut codec = MemcacheCodec::new();
        let mut buf = Buffer::with_capacity(1024);

        codec.encode_set(&mut buf, b"mykey", b"myvalue", 0, 3600);
        assert_eq!(buf.as_slice(), b"set mykey 0 3600 7\r\nmyvalue\r\n");
    }

    #[test]
    fn test_encode_multi_get() {
        let mut codec = MemcacheCodec::new();
        let mut buf = Buffer::with_capacity(1024);

        codec.encode_multi_get(&mut buf, &[b"key1", b"key2", b"key3"]);
        assert_eq!(buf.as_slice(), b"get key1 key2 key3\r\n");
        assert_eq!(codec.pending_responses(), 1);
    }

    #[test]
    fn test_decode_stored() {
        let mut codec = MemcacheCodec::new();
        codec.pending_responses = 1;
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"STORED\r\n");

        let result = codec.decode_response(&mut buf).unwrap();
        assert!(result.unwrap().is_stored());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_end() {
        let mut codec = MemcacheCodec::new();
        codec.pending_responses = 1;
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"END\r\n");

        let result = codec.decode_response(&mut buf).unwrap();
        assert!(result.unwrap().is_miss());
    }

    #[test]
    fn test_decode_value() {
        let mut codec = MemcacheCodec::new();
        codec.pending_responses = 1;
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"VALUE mykey 0 7\r\nmyvalue\r\nEND\r\n");

        let result = codec.decode_response(&mut buf).unwrap();
        let response = result.unwrap();
        let values = response.values().unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].key, b"mykey");
        assert_eq!(values[0].data, b"myvalue");
    }

    #[test]
    fn test_decode_multi_value() {
        let mut codec = MemcacheCodec::new();
        codec.pending_responses = 1;
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"VALUE key1 0 3\r\nfoo\r\nVALUE key2 0 3\r\nbar\r\nEND\r\n");

        let result = codec.decode_response(&mut buf).unwrap();
        let response = result.unwrap();
        let values = response.values().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].key, b"key1");
        assert_eq!(values[0].data, b"foo");
        assert_eq!(values[1].key, b"key2");
        assert_eq!(values[1].data, b"bar");
    }

    #[test]
    fn test_decode_incomplete() {
        let mut codec = MemcacheCodec::new();
        codec.pending_responses = 1;
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"VALUE mykey 0 7\r\nmyval");

        let result = codec.decode_response(&mut buf).unwrap();
        assert!(result.is_none());
    }
}
