//! Simple ASCII PING/PONG protocol codec.
//!
//! This module provides a codec wrapper around protocol-ping that integrates
//! with the benchmark's Buffer type for zero-copy encoding and decoding.

use crate::buffer::Buffer;
use protocol_ping::{ParseError, Request, Response};

/// Simple ASCII PING/PONG protocol codec.
#[derive(Debug)]
pub struct PingCodec {
    pending_responses: usize,
}

impl PingCodec {
    pub fn new() -> Self {
        Self {
            pending_responses: 0,
        }
    }

    /// Encodes a PING command.
    pub fn encode_ping(&mut self, buf: &mut Buffer) -> usize {
        let spare = buf.spare_mut();
        let len = Request::Ping.encode(spare);
        buf.advance(len);
        self.pending_responses += 1;
        len
    }

    /// Returns the number of pending responses.
    pub fn pending_responses(&self) -> usize {
        self.pending_responses
    }

    /// Attempts to decode a response from the buffer.
    pub fn decode_response(&mut self, buf: &mut Buffer) -> Result<Option<PingResponse>, PingError> {
        let data = buf.as_slice();
        if data.is_empty() {
            return Ok(None);
        }

        match Response::parse(data) {
            Ok((response, consumed)) => {
                buf.consume(consumed);
                self.pending_responses = self.pending_responses.saturating_sub(1);
                Ok(Some(PingResponse(response)))
            }
            Err(ParseError::Incomplete) => Ok(None),
            Err(e) => Err(PingError(e)),
        }
    }

    /// Resets the pending response counter.
    pub fn reset(&mut self) {
        self.pending_responses = 0;
    }
}

impl Default for PingCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// A parsed PING response (wrapper around protocol_ping::Response).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PingResponse(Response);

impl PingResponse {
    /// Returns true if this is an error response.
    pub fn is_error(&self) -> bool {
        self.0.is_error()
    }
}

/// PING protocol errors (wrapper around protocol_ping::ParseError).
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct PingError(ParseError);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_ping() {
        let mut codec = PingCodec::new();
        let mut buf = Buffer::with_capacity(1024);

        let len = codec.encode_ping(&mut buf);
        assert_eq!(buf.as_slice(), b"PING\r\n");
        assert_eq!(len, 6);
        assert_eq!(codec.pending_responses(), 1);
    }

    #[test]
    fn test_decode_pong() {
        let mut codec = PingCodec::new();
        codec.pending_responses = 1;
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"PONG\r\n");

        let result = codec.decode_response(&mut buf).unwrap();
        assert!(result.is_some());
        assert!(!result.unwrap().is_error());
        assert!(buf.is_empty());
        assert_eq!(codec.pending_responses(), 0);
    }

    #[test]
    fn test_decode_resp_pong() {
        let mut codec = PingCodec::new();
        codec.pending_responses = 1;
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"+PONG\r\n");

        let result = codec.decode_response(&mut buf).unwrap();
        assert!(result.is_some());
        assert!(!result.unwrap().is_error());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_incomplete() {
        let mut codec = PingCodec::new();
        codec.pending_responses = 1;
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"PON");

        let result = codec.decode_response(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_error() {
        let mut codec = PingCodec::new();
        codec.pending_responses = 1;
        let mut buf = Buffer::with_capacity(1024);
        buf.extend_from_slice(b"-ERR unknown command\r\n");

        let result = codec.decode_response(&mut buf).unwrap();
        assert!(result.unwrap().is_error());
    }
}
