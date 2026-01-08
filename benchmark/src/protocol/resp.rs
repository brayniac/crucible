//! RESP2 protocol codec for Redis.
//!
//! This module provides a thin wrapper around protocol-resp that integrates
//! with brrr's Buffer type for zero-copy encoding and decoding.

use crate::buffer::Buffer;
use protocol_resp::{ParseError, Request, Value};

/// RESP2 protocol codec for Redis.
///
/// Wraps protocol-resp to work with brrr's Buffer type.
#[derive(Debug)]
pub struct RespCodec {
    /// Number of complete responses parsed since last reset
    responses_parsed: usize,
}

impl RespCodec {
    pub fn new() -> Self {
        Self {
            responses_parsed: 0,
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
    /// Compacts the buffer if necessary.
    #[inline]
    fn ensure_space(buf: &mut Buffer, needed: usize) {
        if buf.writable() < needed {
            buf.compact();
        }
        assert!(
            buf.writable() >= needed,
            "buffer too small: need {} bytes but only {} available after compact",
            needed,
            buf.writable()
        );
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

        match Value::parse(data) {
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
}
