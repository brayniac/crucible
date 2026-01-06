//! Simple ASCII PING/PONG protocol implementation.
//!
//! This crate provides a minimal protocol for connectivity testing:
//! - Request: `PING\r\n`
//! - Response: `PONG\r\n` or `+PONG\r\n` (RESP-style)
//!
//! # Example
//!
//! ```
//! use protocol_ping::{Request, Response};
//!
//! // Encode a PING request
//! let mut buf = [0u8; 16];
//! let len = Request::Ping.encode(&mut buf);
//! assert_eq!(&buf[..len], b"PING\r\n");
//!
//! // Parse a PONG response
//! let data = b"PONG\r\n";
//! let (response, consumed) = Response::parse(data).unwrap();
//! assert_eq!(response, Response::Pong);
//! assert_eq!(consumed, 6);
//! ```

/// Parse error types.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseError {
    /// Need more data to complete parsing.
    #[error("incomplete")]
    Incomplete,
    /// Invalid response format.
    #[error("invalid response")]
    Invalid,
}

/// A PING protocol request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    /// PING command
    Ping,
}

impl Request {
    /// Encode the request into the buffer.
    ///
    /// Returns the number of bytes written.
    pub fn encode(self, buf: &mut [u8]) -> usize {
        const PING_CMD: &[u8] = b"PING\r\n";
        let len = PING_CMD.len();
        buf[..len].copy_from_slice(PING_CMD);
        len
    }

    /// Returns the encoded length of this request.
    pub const fn encoded_len(self) -> usize {
        6 // "PING\r\n"
    }
}

/// A PING protocol response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Response {
    /// Successful PONG response.
    Pong,
    /// Error response.
    Error,
}

impl Response {
    /// Parse a response from the buffer.
    ///
    /// Returns the parsed response and number of bytes consumed, or an error.
    ///
    /// Accepts both simple ASCII `PONG\r\n` and RESP-style `+PONG\r\n`.
    pub fn parse(data: &[u8]) -> Result<(Self, usize), ParseError> {
        const PONG_SIMPLE: &[u8] = b"PONG\r\n";
        const PONG_RESP: &[u8] = b"+PONG\r\n";

        if data.is_empty() {
            return Err(ParseError::Incomplete);
        }

        // Check for RESP-style "+PONG\r\n" or other RESP simple strings
        if data[0] == b'+' {
            if data.len() < PONG_RESP.len() {
                if PONG_RESP.starts_with(data) {
                    return Err(ParseError::Incomplete);
                }
            } else if data.starts_with(PONG_RESP) {
                return Ok((Response::Pong, PONG_RESP.len()));
            }

            // Look for line ending - any +... response is treated as success
            if let Some(pos) = data.iter().position(|&b| b == b'\n') {
                return Ok((Response::Pong, pos + 1));
            }
            return Err(ParseError::Incomplete);
        }

        // Check for simple ASCII "PONG\r\n"
        if data.starts_with(b"PONG") {
            if data.len() < PONG_SIMPLE.len() {
                if PONG_SIMPLE.starts_with(data) {
                    return Err(ParseError::Incomplete);
                }
            } else if data.starts_with(PONG_SIMPLE) {
                return Ok((Response::Pong, PONG_SIMPLE.len()));
            }
        }

        // Check for error responses "-ERR ...\r\n"
        if data[0] == b'-' {
            if let Some(pos) = data.iter().position(|&b| b == b'\n') {
                return Ok((Response::Error, pos + 1));
            }
            return Err(ParseError::Incomplete);
        }

        // Check for partial "PONG" match
        if PONG_SIMPLE.starts_with(data) {
            return Err(ParseError::Incomplete);
        }

        // Invalid response
        Err(ParseError::Invalid)
    }

    /// Encode the response into the buffer.
    ///
    /// Returns the number of bytes written.
    pub fn encode(self, buf: &mut [u8]) -> usize {
        let data = match self {
            Response::Pong => b"PONG\r\n",
            Response::Error => b"-ERR\r\n",
        };
        buf[..data.len()].copy_from_slice(data);
        data.len()
    }

    /// Returns true if this is an error response.
    pub fn is_error(self) -> bool {
        matches!(self, Response::Error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_ping() {
        let mut buf = [0u8; 16];
        let len = Request::Ping.encode(&mut buf);
        assert_eq!(&buf[..len], b"PING\r\n");
        assert_eq!(len, 6);
    }

    #[test]
    fn test_parse_pong_simple() {
        let (resp, consumed) = Response::parse(b"PONG\r\n").unwrap();
        assert_eq!(resp, Response::Pong);
        assert_eq!(consumed, 6);
    }

    #[test]
    fn test_parse_pong_resp() {
        let (resp, consumed) = Response::parse(b"+PONG\r\n").unwrap();
        assert_eq!(resp, Response::Pong);
        assert_eq!(consumed, 7);
    }

    #[test]
    fn test_parse_pong_resp_other() {
        // Redis might respond with +OK or other simple strings
        let (resp, consumed) = Response::parse(b"+OK\r\n").unwrap();
        assert_eq!(resp, Response::Pong);
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_parse_error() {
        let (resp, consumed) = Response::parse(b"-ERR unknown command\r\n").unwrap();
        assert_eq!(resp, Response::Error);
        assert_eq!(consumed, 22);
    }

    #[test]
    fn test_parse_incomplete() {
        assert!(matches!(Response::parse(b""), Err(ParseError::Incomplete)));
        assert!(matches!(
            Response::parse(b"PO"),
            Err(ParseError::Incomplete)
        ));
        assert!(matches!(
            Response::parse(b"PONG"),
            Err(ParseError::Incomplete)
        ));
        assert!(matches!(
            Response::parse(b"PONG\r"),
            Err(ParseError::Incomplete)
        ));
        assert!(matches!(
            Response::parse(b"+PON"),
            Err(ParseError::Incomplete)
        ));
    }

    #[test]
    fn test_parse_invalid() {
        assert!(matches!(
            Response::parse(b"INVALID\r\n"),
            Err(ParseError::Invalid)
        ));
        assert!(matches!(
            Response::parse(b"XONG\r\n"),
            Err(ParseError::Invalid)
        ));
    }

    #[test]
    fn test_encode_response() {
        let mut buf = [0u8; 16];

        let len = Response::Pong.encode(&mut buf);
        assert_eq!(&buf[..len], b"PONG\r\n");

        let len = Response::Error.encode(&mut buf);
        assert_eq!(&buf[..len], b"-ERR\r\n");
    }

    #[test]
    fn test_roundtrip() {
        let mut buf = [0u8; 16];
        let len = Response::Pong.encode(&mut buf);
        let (resp, consumed) = Response::parse(&buf[..len]).unwrap();
        assert_eq!(resp, Response::Pong);
        assert_eq!(consumed, len);
    }
}
