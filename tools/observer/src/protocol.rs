//! Protocol detection and parsing coordination.
//!
//! This module handles auto-detection of protocols and wraps the protocol-specific
//! parsers from the `protocol-resp` and `protocol-memcache` crates.

use crate::config::ProtocolHint;
use protocol_memcache::binary::{
    HEADER_SIZE, Opcode, REQUEST_MAGIC, RequestHeader, ResponseHeader, Status,
};

/// Detected protocol for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    #[default]
    Unknown,
    Resp,
    MemcacheAscii,
    MemcacheBinary,
}

impl Protocol {
    /// Convert from a protocol hint.
    pub fn from_hint(hint: ProtocolHint) -> Self {
        match hint {
            ProtocolHint::Auto => Protocol::Unknown,
            ProtocolHint::Resp => Protocol::Resp,
            ProtocolHint::Memcache => Protocol::MemcacheAscii,
            ProtocolHint::MemcacheBinary => Protocol::MemcacheBinary,
        }
    }
}

/// Command type extracted from a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandType {
    Get,
    Set,
    Delete,
    Other,
}

/// Response type extracted from a response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseType {
    Hit,
    Miss,
    Stored,
    Error,
    Other,
}

/// Result of parsing a request.
#[derive(Debug)]
pub struct ParsedRequest {
    pub command_type: CommandType,
    pub bytes_consumed: usize,
    /// For memcache binary: the opaque field for correlation.
    pub opaque: Option<u32>,
}

/// Result of parsing a response.
#[derive(Debug)]
pub struct ParsedResponse {
    pub response_type: ResponseType,
    pub bytes_consumed: usize,
    /// For memcache binary: the opaque field for correlation.
    pub opaque: Option<u32>,
}

/// Detect the protocol from the first bytes of client data.
pub fn detect_protocol(data: &[u8]) -> Protocol {
    if data.is_empty() {
        return Protocol::Unknown;
    }

    match data[0] {
        // RESP protocol starts with '*' for arrays (commands)
        b'*' => Protocol::Resp,
        // Memcache binary request magic
        REQUEST_MAGIC => Protocol::MemcacheBinary,
        // ASCII commands for memcache
        b'g' | b'G' | b's' | b'S' | b'd' | b'D' | b'f' | b'F' | b'v' | b'V' | b'q' | b'Q' => {
            Protocol::MemcacheAscii
        }
        _ => Protocol::Unknown,
    }
}

/// Parse a RESP request (command).
pub fn parse_resp_request(data: &[u8]) -> Option<ParsedRequest> {
    use protocol_resp::Command;

    match Command::parse(data) {
        Ok((cmd, consumed)) => {
            let command_type = match cmd {
                Command::Get { .. } | Command::MGet { .. } => CommandType::Get,
                Command::Set { .. } => CommandType::Set,
                Command::Del { .. } => CommandType::Delete,
                _ => CommandType::Other,
            };
            Some(ParsedRequest {
                command_type,
                bytes_consumed: consumed,
                opaque: None,
            })
        }
        Err(protocol_resp::ParseError::Incomplete) => None,
        Err(_) => {
            // Protocol error - try to skip to next command
            // For now, return None to avoid corrupting stream
            None
        }
    }
}

/// Parse a RESP response (value).
pub fn parse_resp_response(data: &[u8]) -> Option<ParsedResponse> {
    use protocol_resp::Value;

    match Value::parse(data) {
        Ok((value, consumed)) => {
            let response_type = match &value {
                Value::Null => ResponseType::Miss,
                Value::Error(_) => ResponseType::Error,
                Value::SimpleString(s) if s == b"OK" => ResponseType::Stored,
                Value::BulkString(_) => ResponseType::Hit,
                Value::Array(arr) => {
                    // For MGET: check if all elements are null (all misses)
                    if arr.iter().all(|v| matches!(v, Value::Null)) {
                        ResponseType::Miss
                    } else if arr.iter().any(|v| !matches!(v, Value::Null)) {
                        ResponseType::Hit
                    } else {
                        ResponseType::Other
                    }
                }
                _ => ResponseType::Other,
            };
            Some(ParsedResponse {
                response_type,
                bytes_consumed: consumed,
                opaque: None,
            })
        }
        Err(protocol_resp::ParseError::Incomplete) => None,
        Err(_) => None,
    }
}

/// Parse a Memcache ASCII request.
pub fn parse_memcache_ascii_request(data: &[u8]) -> Option<ParsedRequest> {
    use protocol_memcache::Command;

    match Command::parse(data) {
        Ok((cmd, consumed)) => {
            let command_type = match cmd {
                Command::Get { .. } | Command::Gets { .. } => CommandType::Get,
                Command::Set { .. } => CommandType::Set,
                Command::Delete { .. } => CommandType::Delete,
                _ => CommandType::Other,
            };
            Some(ParsedRequest {
                command_type,
                bytes_consumed: consumed,
                opaque: None,
            })
        }
        Err(protocol_memcache::ParseError::Incomplete) => None,
        Err(_) => None,
    }
}

/// Parse a Memcache ASCII response.
pub fn parse_memcache_ascii_response(data: &[u8]) -> Option<ParsedResponse> {
    use protocol_memcache::Response;

    match Response::parse(data) {
        Ok((resp, consumed)) => {
            let response_type = match resp {
                Response::Values(values) => {
                    if values.is_empty() {
                        ResponseType::Miss
                    } else {
                        ResponseType::Hit
                    }
                }
                Response::Stored => ResponseType::Stored,
                Response::NotStored | Response::NotFound => ResponseType::Miss,
                Response::Deleted => ResponseType::Other,
                Response::Error | Response::ClientError(_) | Response::ServerError(_) => {
                    ResponseType::Error
                }
                _ => ResponseType::Other,
            };
            Some(ParsedResponse {
                response_type,
                bytes_consumed: consumed,
                opaque: None,
            })
        }
        Err(protocol_memcache::ParseError::Incomplete) => None,
        Err(_) => None,
    }
}

/// Parse a Memcache binary request.
pub fn parse_memcache_binary_request(data: &[u8]) -> Option<ParsedRequest> {
    if data.len() < HEADER_SIZE {
        return None;
    }

    match RequestHeader::parse(data) {
        Ok(header) => {
            let total_len = HEADER_SIZE + header.total_body_length as usize;
            if data.len() < total_len {
                return None; // Incomplete
            }

            let command_type = match header.opcode {
                Opcode::Get | Opcode::GetQ | Opcode::GetK | Opcode::GetKQ => CommandType::Get,
                Opcode::Set | Opcode::SetQ => CommandType::Set,
                Opcode::Delete | Opcode::DeleteQ => CommandType::Delete,
                _ => CommandType::Other,
            };

            Some(ParsedRequest {
                command_type,
                bytes_consumed: total_len,
                opaque: Some(header.opaque),
            })
        }
        Err(_) => None,
    }
}

/// Parse a Memcache binary response.
pub fn parse_memcache_binary_response(data: &[u8]) -> Option<ParsedResponse> {
    if data.len() < HEADER_SIZE {
        return None;
    }

    match ResponseHeader::parse(data) {
        Ok(header) => {
            let total_len = HEADER_SIZE + header.total_body_length as usize;
            if data.len() < total_len {
                return None; // Incomplete
            }

            let response_type = match header.status {
                Status::NoError => {
                    // Check opcode to determine if this is a hit or stored
                    match header.opcode {
                        Opcode::Get | Opcode::GetQ | Opcode::GetK | Opcode::GetKQ => {
                            ResponseType::Hit
                        }
                        Opcode::Set | Opcode::SetQ => ResponseType::Stored,
                        _ => ResponseType::Other,
                    }
                }
                Status::KeyNotFound => ResponseType::Miss,
                _ => ResponseType::Error,
            };

            Some(ParsedResponse {
                response_type,
                bytes_consumed: total_len,
                opaque: Some(header.opaque),
            })
        }
        Err(_) => None,
    }
}
