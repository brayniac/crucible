//! RESP2 protocol codec for Redis.
//!
//! This module provides a thin wrapper around protocol-resp that integrates
//! with brrr's Buffer type for zero-copy encoding and decoding.

use crate::buffer::Buffer;
use protocol_resp::{ParseError, ParseOptions, Request, Value};

/// Maximum bulk string size: 512MB (matches official RESP protocol spec).
const MAX_BULK_STRING_LEN: usize = 512 * 1024 * 1024;

/// RESP2 protocol codec for Redis.
///
/// Wraps protocol-resp to work with brrr's Buffer type.
#[derive(Debug)]
pub struct RespCodec {
    /// Number of complete responses parsed since last reset
    responses_parsed: usize,
    /// Parse options with generous limits for benchmark use
    parse_options: ParseOptions,
}

impl RespCodec {
    pub fn new() -> Self {
        Self {
            responses_parsed: 0,
            parse_options: ParseOptions::new().max_bulk_string_len(MAX_BULK_STRING_LEN),
        }
    }

    /// Encodes a GET command.
    #[inline]
    pub fn encode_get(&self, buf: &mut Buffer, key: &[u8]) -> usize {
        let req = Request::get(key);
        let needed = req.encoded_len();
        Self::ensure_space(buf, needed);
        let len = req.encode(buf.spare_mut());
        buf.advance(len);
        len
    }

    /// Encodes a SET command.
    #[inline]
    pub fn encode_set(&self, buf: &mut Buffer, key: &[u8], value: &[u8]) -> usize {
        let req = Request::set(key, value);
        let needed = req.encoded_len();
        Self::ensure_space(buf, needed);
        let len = req.encode(buf.spare_mut());
        buf.advance(len);
        len
    }

    /// Encodes a DEL command.
    #[inline]
    pub fn encode_delete(&self, buf: &mut Buffer, key: &[u8]) -> usize {
        let req = Request::del(key);
        let needed = req.encoded_len();
        Self::ensure_space(buf, needed);
        let len = req.encode(buf.spare_mut());
        buf.advance(len);
        len
    }

    /// Encodes a SET command with expiration (EX seconds).
    #[inline]
    pub fn encode_set_ex(&self, buf: &mut Buffer, key: &[u8], value: &[u8], ex: u64) -> usize {
        let req = Request::set(key, value).ex(ex);
        let needed = req.encoded_len();
        Self::ensure_space(buf, needed);
        let len = req.encode(buf.spare_mut());
        buf.advance(len);
        len
    }

    /// Encodes a PING command.
    #[inline]
    pub fn encode_ping(&self, buf: &mut Buffer) -> usize {
        let req = Request::ping();
        let needed = req.encoded_len();
        Self::ensure_space(buf, needed);
        let len = req.encode(buf.spare_mut());
        buf.advance(len);
        len
    }

    /// Ensures the buffer has at least `needed` bytes of writable space.
    /// Compacts the buffer first, then grows if necessary.
    #[inline]
    fn ensure_space(buf: &mut Buffer, needed: usize) {
        if buf.writable() < needed {
            buf.compact();
        }
        if buf.writable() < needed {
            // Need to grow the buffer
            buf.grow(needed);
        }
    }

    /// Attempts to decode a response from the buffer.
    ///
    /// Returns:
    /// - `Ok(Some(value))` if a complete response was parsed
    /// - `Ok(None)` if more data is needed
    /// - `Err(e)` if the data is malformed
    ///
    /// On success, the consumed bytes are removed from the buffer.
    pub fn decode_response(&mut self, buf: &mut Buffer) -> Result<Option<RespValue>, RespError> {
        let data = buf.as_slice();
        if data.is_empty() {
            return Ok(None);
        }

        match Value::parse_with_options(data, &self.parse_options) {
            Ok((value, consumed)) => {
                buf.consume(consumed);
                self.responses_parsed += 1;
                Ok(Some(RespValue(value)))
            }
            Err(ParseError::Incomplete) => Ok(None),
            Err(e) => Err(RespError(e)),
        }
    }

    /// Decodes multiple responses from the buffer.
    ///
    /// Returns the number of responses decoded and adds them to the output vec.
    /// Stops when buffer is exhausted or an incomplete response is encountered.
    pub fn decode_responses(
        &mut self,
        buf: &mut Buffer,
        out: &mut Vec<RespValue>,
    ) -> Result<usize, RespError> {
        let mut count = 0;
        while let Some(value) = self.decode_response(buf)? {
            out.push(value);
            count += 1;
        }
        Ok(count)
    }

    /// Returns the number of responses parsed since creation or last reset.
    pub fn responses_parsed(&self) -> usize {
        self.responses_parsed
    }

    /// Resets the response counter.
    pub fn reset_counter(&mut self) {
        self.responses_parsed = 0;
    }

    /// Increment the parsed counter (for zero-copy path).
    #[inline]
    pub fn increment_parsed(&mut self) {
        self.responses_parsed += 1;
    }
}

impl Default for RespCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// A parsed RESP value (wrapper around protocol_resp::Value).
#[derive(Debug, Clone)]
pub struct RespValue(Value);

impl RespValue {
    /// Returns true if this is a null value.
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }

    /// Returns true if this is an error.
    pub fn is_error(&self) -> bool {
        self.0.is_error()
    }

    /// Returns the value as bytes if it's a string type.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.0.as_bytes()
    }

    /// Returns the value as an integer.
    pub fn as_integer(&self) -> Option<i64> {
        self.0.as_integer()
    }
}

/// RESP parsing/encoding errors (wrapper around protocol_resp::ParseError).
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct RespError(ParseError);

impl RespError {
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
        let codec = RespCodec::new();
        let mut buf = Buffer::with_capacity(1024);

        codec.encode_get(&mut buf, b"mykey");

        assert_eq!(buf.as_slice(), b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
    }

    #[test]
    fn test_encode_set() {
        let codec = RespCodec::new();
        let mut buf = Buffer::with_capacity(1024);

        codec.encode_set(&mut buf, b"mykey", b"myvalue");

        assert_eq!(
            buf.as_slice(),
            b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
        );
    }

    #[test]
    fn test_decode_simple_string() {
        let mut codec = RespCodec::new();
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"+OK\r\n");

        let result = codec.decode_response(&mut buf).unwrap();
        assert!(result.is_some());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_error() {
        let mut codec = RespCodec::new();
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"-ERR unknown command\r\n");

        let result = codec.decode_response(&mut buf).unwrap();
        assert!(result.as_ref().unwrap().is_error());
    }

    #[test]
    fn test_decode_integer() {
        let mut codec = RespCodec::new();
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b":1000\r\n");

        let result = codec.decode_response(&mut buf).unwrap();
        assert_eq!(result.unwrap().as_integer(), Some(1000));
    }

    #[test]
    fn test_decode_bulk_string() {
        let mut codec = RespCodec::new();
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"$6\r\nfoobar\r\n");

        let result = codec.decode_response(&mut buf).unwrap();
        assert_eq!(result.unwrap().as_bytes(), Some(b"foobar".as_slice()));
    }

    #[test]
    fn test_decode_null() {
        let mut codec = RespCodec::new();
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"$-1\r\n");

        let result = codec.decode_response(&mut buf).unwrap();
        assert!(result.unwrap().is_null());
    }

    #[test]
    fn test_decode_incomplete() {
        let mut codec = RespCodec::new();
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"$6\r\nfoo");

        let result = codec.decode_response(&mut buf).unwrap();
        assert!(result.is_none());
        // Buffer should not be consumed on incomplete
        assert_eq!(buf.readable(), 7);
    }

    #[test]
    fn test_decode_multiple() {
        let mut codec = RespCodec::new();
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"+OK\r\n+PONG\r\n:42\r\n");

        let mut out = Vec::new();
        let count = codec.decode_responses(&mut buf, &mut out).unwrap();

        assert_eq!(count, 3);
    }

    #[test]
    fn test_encode_large_value() {
        let codec = RespCodec::new();
        // Start with a small buffer
        let mut buf = Buffer::with_capacity(1024);

        // Create a 1MB value
        let key = b"testkey";
        let value = vec![0xABu8; 1_000_000];

        // This should trigger buffer growth
        codec.encode_set(&mut buf, key, &value);

        // Verify the encoded data is valid RESP
        let data = buf.as_slice();

        // Should start with array header for SET command
        assert!(
            data.starts_with(b"*3\r\n"),
            "Expected *3\\r\\n, got first 20 bytes: {:?}",
            &data[..20.min(data.len())]
        );

        // Verify total length matches expected
        // *3\r\n = 4
        // $3\r\nSET\r\n = 9
        // $7\r\ntestkey\r\n = 13
        // $1000000\r\n = 10
        // <1MB>\r\n = 1000002
        // Total = 4 + 9 + 13 + 10 + 1000002 = 1000038
        let expected_len = 1_000_038;
        assert_eq!(buf.readable(), expected_len);
    }

    #[test]
    fn test_encode_multiple_large_values_with_partial_consumption() {
        let codec = RespCodec::new();
        let mut buf = Buffer::with_capacity(64 * 1024);

        let key = b"testkey";
        let value = vec![0xCDu8; 100_000]; // 100KB value

        // Encode first SET
        let len1 = codec.encode_set(&mut buf, key, &value);
        assert!(len1 > 0);

        // Simulate partial send - only consume half
        let partial = buf.readable() / 2;
        buf.consume(partial);

        // Encode second SET - should still work after partial consumption
        let len2 = codec.encode_set(&mut buf, key, &value);
        assert!(len2 > 0);

        // Verify total data in buffer
        assert_eq!(buf.readable(), (len1 - partial) + len2);
    }

    #[test]
    fn test_buffer_grow_preserves_data() {
        let codec = RespCodec::new();
        // Start with tiny buffer
        let mut buf = Buffer::with_capacity(64);

        // Encode a small request first
        codec.encode_get(&mut buf, b"key1");
        let first_request = buf.as_slice().to_vec();

        // Now encode a large request that requires growth
        let value = vec![0xEFu8; 10_000];
        codec.encode_set(&mut buf, b"key2", &value);

        // Verify the first request data is still intact at the beginning
        let data = buf.as_slice();
        assert!(
            data.starts_with(&first_request),
            "First request not preserved after buffer grow"
        );

        // Verify the second request follows (check bytes directly, not as UTF-8)
        let second_request_start = &data[first_request.len()..first_request.len() + 4];
        assert_eq!(
            second_request_start, b"*3\r\n",
            "Second request should start with *3\\r\\n"
        );
    }
}
