//! Minimal protobuf encoding/decoding for Momento cache messages.
//!
//! This implements just enough protobuf wire format to encode/decode
//! the cache API messages without requiring prost or other heavy deps.

use bytes::Bytes;

/// Wire type for varint (int32, int64, uint32, uint64, bool, enum).
const WIRE_TYPE_VARINT: u8 = 0;
/// Wire type for length-delimited (string, bytes, embedded messages).
const WIRE_TYPE_LEN: u8 = 2;

/// Encode a varint.
pub fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode a varint from a buffer.
pub fn decode_varint(buf: &mut &[u8]) -> Option<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;

    loop {
        if buf.is_empty() {
            return None;
        }
        let byte = buf[0];
        *buf = &buf[1..];

        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
        if shift >= 64 {
            return None; // Overflow
        }
    }
}

/// Encode a field tag.
pub fn encode_tag(field_number: u32, wire_type: u8, buf: &mut Vec<u8>) {
    encode_varint(((field_number as u64) << 3) | (wire_type as u64), buf);
}

/// Decode a field tag, returning (field_number, wire_type).
pub fn decode_tag(buf: &mut &[u8]) -> Option<(u32, u8)> {
    let tag = decode_varint(buf)?;
    let field_number = (tag >> 3) as u32;
    let wire_type = (tag & 0x07) as u8;
    Some((field_number, wire_type))
}

/// Encode a bytes field.
pub fn encode_bytes(field_number: u32, data: &[u8], buf: &mut Vec<u8>) {
    encode_tag(field_number, WIRE_TYPE_LEN, buf);
    encode_varint(data.len() as u64, buf);
    buf.extend_from_slice(data);
}

/// Encode a string field (same as bytes in protobuf).
pub fn encode_string(field_number: u32, s: &str, buf: &mut Vec<u8>) {
    encode_bytes(field_number, s.as_bytes(), buf);
}

/// Encode a uint64 field.
pub fn encode_uint64(field_number: u32, value: u64, buf: &mut Vec<u8>) {
    encode_tag(field_number, WIRE_TYPE_VARINT, buf);
    encode_varint(value, buf);
}

/// Encode an embedded message field.
pub fn encode_message(field_number: u32, message: &[u8], buf: &mut Vec<u8>) {
    encode_tag(field_number, WIRE_TYPE_LEN, buf);
    encode_varint(message.len() as u64, buf);
    buf.extend_from_slice(message);
}

/// Decode a length-delimited field, returning the bytes.
pub fn decode_length_delimited<'a>(buf: &mut &'a [u8]) -> Option<&'a [u8]> {
    let len = decode_varint(buf)? as usize;
    if buf.len() < len {
        return None;
    }
    let data = &buf[..len];
    *buf = &buf[len..];
    Some(data)
}

/// Skip a field based on its wire type.
pub fn skip_field(wire_type: u8, buf: &mut &[u8]) -> Option<()> {
    match wire_type {
        WIRE_TYPE_VARINT => {
            decode_varint(buf)?;
        }
        WIRE_TYPE_LEN => {
            decode_length_delimited(buf)?;
        }
        1 => {
            // 64-bit fixed
            if buf.len() < 8 {
                return None;
            }
            *buf = &buf[8..];
        }
        5 => {
            // 32-bit fixed
            if buf.len() < 4 {
                return None;
            }
            *buf = &buf[4..];
        }
        _ => return None,
    }
    Some(())
}

// ============================================================================
// Momento Cache API Messages
// ============================================================================

/// ECacheResult enum values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ECacheResult {
    Invalid = 0,
    Ok = 1,
    Hit = 2,
    Miss = 3,
}

impl ECacheResult {
    pub fn from_u32(value: u32) -> Self {
        match value {
            0 => ECacheResult::Invalid,
            1 => ECacheResult::Ok,
            2 => ECacheResult::Hit,
            3 => ECacheResult::Miss,
            _ => ECacheResult::Invalid,
        }
    }
}

/// _GetRequest message.
/// Note: cache_name is passed via gRPC metadata header "cache", not in the message.
pub struct GetRequest<'a> {
    pub cache_key: &'a [u8],
}

impl<'a> GetRequest<'a> {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        encode_bytes(1, self.cache_key, &mut buf);
        buf
    }
}

/// Owned version of GetRequest for server-side use.
#[derive(Debug, Clone)]
pub struct GetRequestOwned {
    pub cache_key: Bytes,
}

impl GetRequestOwned {
    /// Decode a _GetRequest message from bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        let mut cache_key: Option<&[u8]> = None;

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;

            match field_number {
                // cache_key field
                1 => {
                    cache_key = Some(decode_length_delimited(&mut buf)?);
                }
                _ => {
                    skip_field(wire_type, &mut buf)?;
                }
            }
        }

        Some(Self {
            cache_key: Bytes::copy_from_slice(cache_key.unwrap_or(&[])),
        })
    }
}

/// Result of a Get operation.
#[derive(Debug, Clone)]
pub enum GetResponse {
    /// Cache miss - key not found.
    Miss,
    /// Cache hit - contains the value.
    Hit(Bytes),
    /// Error response.
    Error { message: String },
}

impl GetResponse {
    /// Decode a _GetResponse message.
    /// Format:
    /// - field 1: result (ECacheResult enum - varint)
    /// - field 2: cache_body (bytes)
    /// - field 3: message (string)
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        let mut result = ECacheResult::Invalid;
        let mut cache_body: Option<&[u8]> = None;
        let mut message: Option<String> = None;

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;

            match field_number {
                // result field (ECacheResult enum)
                1 => {
                    let value = decode_varint(&mut buf)? as u32;
                    result = ECacheResult::from_u32(value);
                }
                // cache_body field
                2 => {
                    cache_body = Some(decode_length_delimited(&mut buf)?);
                }
                // message field
                3 => {
                    let msg_bytes = decode_length_delimited(&mut buf)?;
                    message = Some(String::from_utf8_lossy(msg_bytes).into_owned());
                }
                _ => {
                    skip_field(wire_type, &mut buf)?;
                }
            }
        }

        match result {
            ECacheResult::Hit => {
                let body = cache_body.unwrap_or(&[]);
                Some(GetResponse::Hit(Bytes::copy_from_slice(body)))
            }
            ECacheResult::Miss => Some(GetResponse::Miss),
            ECacheResult::Ok => Some(GetResponse::Miss), // Shouldn't happen for Get
            ECacheResult::Invalid => Some(GetResponse::Error {
                message: message.unwrap_or_else(|| "invalid result".to_string()),
            }),
        }
    }

    /// Encode a _GetResponse message for server-side use.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        match self {
            GetResponse::Hit(value) => {
                encode_uint64(1, ECacheResult::Hit as u64, &mut buf);
                encode_bytes(2, value, &mut buf);
            }
            GetResponse::Miss => {
                encode_uint64(1, ECacheResult::Miss as u64, &mut buf);
            }
            GetResponse::Error { message } => {
                encode_uint64(1, ECacheResult::Invalid as u64, &mut buf);
                encode_string(3, message, &mut buf);
            }
        }
        buf
    }
}

/// _SetRequest message.
/// Note: cache_name is passed via gRPC metadata header "cache", not in the message.
pub struct SetRequest<'a> {
    pub cache_key: &'a [u8],
    pub cache_body: &'a [u8],
    pub ttl_milliseconds: u64,
}

impl<'a> SetRequest<'a> {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        encode_bytes(1, self.cache_key, &mut buf);
        encode_bytes(2, self.cache_body, &mut buf);
        encode_uint64(3, self.ttl_milliseconds, &mut buf);
        buf
    }
}

/// Owned version of SetRequest for server-side use.
#[derive(Debug, Clone)]
pub struct SetRequestOwned {
    pub cache_key: Bytes,
    pub cache_body: Bytes,
    pub ttl_milliseconds: u64,
}

impl SetRequestOwned {
    /// Decode a _SetRequest message from bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        let mut cache_key: Option<&[u8]> = None;
        let mut cache_body: Option<&[u8]> = None;
        let mut ttl_milliseconds: u64 = 0;

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;

            match field_number {
                // cache_key field
                1 => {
                    cache_key = Some(decode_length_delimited(&mut buf)?);
                }
                // cache_body field
                2 => {
                    cache_body = Some(decode_length_delimited(&mut buf)?);
                }
                // ttl_milliseconds field
                3 => {
                    ttl_milliseconds = decode_varint(&mut buf)?;
                }
                _ => {
                    skip_field(wire_type, &mut buf)?;
                }
            }
        }

        Some(Self {
            cache_key: Bytes::copy_from_slice(cache_key.unwrap_or(&[])),
            cache_body: Bytes::copy_from_slice(cache_body.unwrap_or(&[])),
            ttl_milliseconds,
        })
    }
}

/// Result of a Set operation.
#[derive(Debug, Clone)]
pub enum SetResponse {
    /// Set succeeded.
    Ok,
    /// Error response.
    Error { message: String },
}

impl SetResponse {
    /// Decode a _SetResponse message.
    /// Format:
    /// - field 1: result (ECacheResult enum - varint)
    /// - field 2: message (string)
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        let mut result = ECacheResult::Ok; // Default to Ok for empty response
        let mut message: Option<String> = None;

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;

            match field_number {
                // result field
                1 => {
                    let value = decode_varint(&mut buf)? as u32;
                    result = ECacheResult::from_u32(value);
                }
                // message field
                2 => {
                    let msg_bytes = decode_length_delimited(&mut buf)?;
                    message = Some(String::from_utf8_lossy(msg_bytes).into_owned());
                }
                _ => {
                    skip_field(wire_type, &mut buf)?;
                }
            }
        }

        match result {
            ECacheResult::Ok => Some(SetResponse::Ok),
            _ => Some(SetResponse::Error {
                message: message.unwrap_or_else(|| "set failed".to_string()),
            }),
        }
    }

    /// Encode a _SetResponse message for server-side use.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        match self {
            SetResponse::Ok => {
                encode_uint64(1, ECacheResult::Ok as u64, &mut buf);
            }
            SetResponse::Error { message } => {
                encode_uint64(1, ECacheResult::Invalid as u64, &mut buf);
                encode_string(2, message, &mut buf);
            }
        }
        buf
    }
}

/// _DeleteRequest message.
/// Note: cache_name is passed via gRPC metadata header "cache", not in the message.
pub struct DeleteRequest<'a> {
    pub cache_key: &'a [u8],
}

impl<'a> DeleteRequest<'a> {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        encode_bytes(1, self.cache_key, &mut buf);
        buf
    }
}

/// Owned version of DeleteRequest for server-side use.
#[derive(Debug, Clone)]
pub struct DeleteRequestOwned {
    pub cache_key: Bytes,
}

impl DeleteRequestOwned {
    /// Decode a _DeleteRequest message from bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        let mut cache_key: Option<&[u8]> = None;

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;

            match field_number {
                // cache_key field
                1 => {
                    cache_key = Some(decode_length_delimited(&mut buf)?);
                }
                _ => {
                    skip_field(wire_type, &mut buf)?;
                }
            }
        }

        Some(Self {
            cache_key: Bytes::copy_from_slice(cache_key.unwrap_or(&[])),
        })
    }
}

/// Result of a Delete operation.
/// Note: _DeleteResponse has no fields, always succeeds if no gRPC error.
#[derive(Debug, Clone)]
pub enum DeleteResponse {
    /// Delete succeeded.
    Ok,
}

impl DeleteResponse {
    pub fn decode(_data: &[u8]) -> Option<Self> {
        // _DeleteResponse has no fields
        Some(DeleteResponse::Ok)
    }

    /// Encode a _DeleteResponse message for server-side use.
    pub fn encode(&self) -> Vec<u8> {
        // _DeleteResponse has no fields
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Varint tests

    #[test]
    fn test_varint_roundtrip() {
        let values = [0, 1, 127, 128, 300, 16383, 16384, u64::MAX];

        for &value in &values {
            let mut buf = Vec::new();
            encode_varint(value, &mut buf);

            let mut slice = &buf[..];
            let decoded = decode_varint(&mut slice).unwrap();
            assert_eq!(decoded, value);
            assert!(slice.is_empty());
        }
    }

    #[test]
    fn test_decode_varint_empty() {
        let mut buf: &[u8] = &[];
        assert!(decode_varint(&mut buf).is_none());
    }

    #[test]
    fn test_decode_varint_overflow() {
        // Create a varint that would overflow (more than 10 continuation bytes)
        let mut buf: &[u8] = &[0x80; 11];
        assert!(decode_varint(&mut buf).is_none());
    }

    // Tag tests

    #[test]
    fn test_encode_decode_tag() {
        let test_cases = [(1, 0), (1, 2), (15, 0), (100, 2), (1000, 0)];

        for (field_number, wire_type) in test_cases {
            let mut buf = Vec::new();
            encode_tag(field_number, wire_type, &mut buf);

            let mut slice = &buf[..];
            let (decoded_field, decoded_wire) = decode_tag(&mut slice).unwrap();
            assert_eq!(decoded_field, field_number);
            assert_eq!(decoded_wire, wire_type);
        }
    }

    #[test]
    fn test_decode_tag_empty() {
        let mut buf: &[u8] = &[];
        assert!(decode_tag(&mut buf).is_none());
    }

    // Length-delimited tests

    #[test]
    fn test_decode_length_delimited() {
        let mut buf = Vec::new();
        encode_varint(5, &mut buf); // length = 5
        buf.extend_from_slice(b"hello");

        let mut slice = &buf[..];
        let data = decode_length_delimited(&mut slice).unwrap();
        assert_eq!(data, b"hello");
        assert!(slice.is_empty());
    }

    #[test]
    fn test_decode_length_delimited_empty() {
        let mut buf = Vec::new();
        encode_varint(0, &mut buf); // length = 0

        let mut slice = &buf[..];
        let data = decode_length_delimited(&mut slice).unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn test_decode_length_delimited_insufficient() {
        let mut buf = Vec::new();
        encode_varint(10, &mut buf); // length = 10
        buf.extend_from_slice(b"short"); // only 5 bytes

        let mut slice = &buf[..];
        assert!(decode_length_delimited(&mut slice).is_none());
    }

    // Skip field tests

    #[test]
    fn test_skip_field_varint() {
        let mut buf = Vec::new();
        encode_varint(12345, &mut buf);

        let mut slice = &buf[..];
        assert!(skip_field(WIRE_TYPE_VARINT, &mut slice).is_some());
        assert!(slice.is_empty());
    }

    #[test]
    fn test_skip_field_length_delimited() {
        let mut buf = Vec::new();
        encode_varint(3, &mut buf);
        buf.extend_from_slice(b"abc");

        let mut slice = &buf[..];
        assert!(skip_field(WIRE_TYPE_LEN, &mut slice).is_some());
        assert!(slice.is_empty());
    }

    #[test]
    fn test_skip_field_64bit() {
        let buf = [0u8; 8];
        let mut slice = &buf[..];
        assert!(skip_field(1, &mut slice).is_some()); // wire type 1 = 64-bit fixed
        assert!(slice.is_empty());
    }

    #[test]
    fn test_skip_field_64bit_insufficient() {
        let buf = [0u8; 4];
        let mut slice = &buf[..];
        assert!(skip_field(1, &mut slice).is_none());
    }

    #[test]
    fn test_skip_field_32bit() {
        let buf = [0u8; 4];
        let mut slice = &buf[..];
        assert!(skip_field(5, &mut slice).is_some()); // wire type 5 = 32-bit fixed
        assert!(slice.is_empty());
    }

    #[test]
    fn test_skip_field_32bit_insufficient() {
        let buf = [0u8; 2];
        let mut slice = &buf[..];
        assert!(skip_field(5, &mut slice).is_none());
    }

    #[test]
    fn test_skip_field_unknown_wire_type() {
        let buf = [0u8; 8];
        let mut slice = &buf[..];
        assert!(skip_field(6, &mut slice).is_none()); // Unknown wire type
    }

    // ECacheResult tests

    #[test]
    fn test_ecache_result_from_u32() {
        assert_eq!(ECacheResult::from_u32(0), ECacheResult::Invalid);
        assert_eq!(ECacheResult::from_u32(1), ECacheResult::Ok);
        assert_eq!(ECacheResult::from_u32(2), ECacheResult::Hit);
        assert_eq!(ECacheResult::from_u32(3), ECacheResult::Miss);
        assert_eq!(ECacheResult::from_u32(99), ECacheResult::Invalid);
    }

    #[test]
    fn test_ecache_result_debug() {
        assert!(format!("{:?}", ECacheResult::Hit).contains("Hit"));
        assert!(format!("{:?}", ECacheResult::Miss).contains("Miss"));
    }

    // GetRequest tests

    #[test]
    fn test_encode_get_request() {
        let req = GetRequest {
            cache_key: b"my-key",
        };

        let encoded = req.encode();
        assert!(!encoded.is_empty());

        // Field 1 (cache_key): tag = (1 << 3) | 2 = 0x0A
        assert_eq!(encoded[0], 0x0A);
    }

    #[test]
    fn test_encode_get_request_empty_key() {
        let req = GetRequest { cache_key: b"" };
        let encoded = req.encode();
        // Should still encode with empty key
        assert!(!encoded.is_empty());
    }

    // GetRequestOwned tests

    #[test]
    fn test_decode_get_request_owned() {
        let original = GetRequest {
            cache_key: b"test-key",
        };
        let encoded = original.encode();

        let decoded = GetRequestOwned::decode(&encoded).unwrap();
        assert_eq!(&decoded.cache_key[..], b"test-key");
    }

    #[test]
    fn test_decode_get_request_owned_empty() {
        let decoded = GetRequestOwned::decode(&[]).unwrap();
        assert!(decoded.cache_key.is_empty());
    }

    #[test]
    fn test_decode_get_request_owned_with_unknown_field() {
        // Encode with an extra unknown field
        let mut buf = Vec::new();
        encode_bytes(1, b"key", &mut buf);
        encode_bytes(99, b"unknown", &mut buf); // Unknown field

        let decoded = GetRequestOwned::decode(&buf).unwrap();
        assert_eq!(&decoded.cache_key[..], b"key");
    }

    // GetResponse tests

    #[test]
    fn test_decode_get_response_miss() {
        // Encode a miss response: result=Miss (3)
        let mut buf = Vec::new();
        encode_uint64(1, ECacheResult::Miss as u64, &mut buf);

        let response = GetResponse::decode(&buf).unwrap();
        assert!(matches!(response, GetResponse::Miss));
    }

    #[test]
    fn test_decode_get_response_hit() {
        // Encode a hit response: result=Hit (2), cache_body
        let mut buf = Vec::new();
        encode_uint64(1, ECacheResult::Hit as u64, &mut buf);
        encode_bytes(2, b"the-value", &mut buf);

        let response = GetResponse::decode(&buf).unwrap();
        match response {
            GetResponse::Hit(value) => {
                assert_eq!(&value[..], b"the-value");
            }
            _ => panic!("expected Hit"),
        }
    }

    #[test]
    fn test_decode_get_response_hit_empty_body() {
        let mut buf = Vec::new();
        encode_uint64(1, ECacheResult::Hit as u64, &mut buf);
        // No body field

        let response = GetResponse::decode(&buf).unwrap();
        match response {
            GetResponse::Hit(value) => {
                assert!(value.is_empty());
            }
            _ => panic!("expected Hit"),
        }
    }

    #[test]
    fn test_decode_get_response_ok_as_miss() {
        // ECacheResult::Ok shouldn't happen for Get, treated as Miss
        let mut buf = Vec::new();
        encode_uint64(1, ECacheResult::Ok as u64, &mut buf);

        let response = GetResponse::decode(&buf).unwrap();
        assert!(matches!(response, GetResponse::Miss));
    }

    #[test]
    fn test_decode_get_response_error() {
        let mut buf = Vec::new();
        encode_uint64(1, ECacheResult::Invalid as u64, &mut buf);
        encode_string(3, "something went wrong", &mut buf);

        let response = GetResponse::decode(&buf).unwrap();
        match response {
            GetResponse::Error { message } => {
                assert_eq!(message, "something went wrong");
            }
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn test_decode_get_response_error_no_message() {
        let mut buf = Vec::new();
        encode_uint64(1, ECacheResult::Invalid as u64, &mut buf);

        let response = GetResponse::decode(&buf).unwrap();
        match response {
            GetResponse::Error { message } => {
                assert_eq!(message, "invalid result");
            }
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn test_encode_get_response_hit() {
        let response = GetResponse::Hit(Bytes::from_static(b"value"));
        let encoded = response.encode();
        assert!(!encoded.is_empty());

        // Decode it back
        let decoded = GetResponse::decode(&encoded).unwrap();
        match decoded {
            GetResponse::Hit(v) => assert_eq!(&v[..], b"value"),
            _ => panic!("expected Hit"),
        }
    }

    #[test]
    fn test_encode_get_response_miss() {
        let response = GetResponse::Miss;
        let encoded = response.encode();
        assert!(!encoded.is_empty());

        let decoded = GetResponse::decode(&encoded).unwrap();
        assert!(matches!(decoded, GetResponse::Miss));
    }

    #[test]
    fn test_encode_get_response_error() {
        let response = GetResponse::Error {
            message: "test error".to_string(),
        };
        let encoded = response.encode();
        assert!(!encoded.is_empty());

        let decoded = GetResponse::decode(&encoded).unwrap();
        match decoded {
            GetResponse::Error { message } => assert_eq!(message, "test error"),
            _ => panic!("expected Error"),
        }
    }

    // SetRequest tests

    #[test]
    fn test_encode_set_request() {
        let req = SetRequest {
            cache_key: b"my-key",
            cache_body: b"my-value",
            ttl_milliseconds: 60000,
        };

        let encoded = req.encode();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_set_request_large_ttl() {
        let req = SetRequest {
            cache_key: b"key",
            cache_body: b"value",
            ttl_milliseconds: u64::MAX,
        };

        let encoded = req.encode();
        assert!(!encoded.is_empty());
    }

    // SetRequestOwned tests

    #[test]
    fn test_decode_set_request_owned() {
        let original = SetRequest {
            cache_key: b"key",
            cache_body: b"value",
            ttl_milliseconds: 30000,
        };
        let encoded = original.encode();

        let decoded = SetRequestOwned::decode(&encoded).unwrap();
        assert_eq!(&decoded.cache_key[..], b"key");
        assert_eq!(&decoded.cache_body[..], b"value");
        assert_eq!(decoded.ttl_milliseconds, 30000);
    }

    #[test]
    fn test_decode_set_request_owned_empty() {
        let decoded = SetRequestOwned::decode(&[]).unwrap();
        assert!(decoded.cache_key.is_empty());
        assert!(decoded.cache_body.is_empty());
        assert_eq!(decoded.ttl_milliseconds, 0);
    }

    #[test]
    fn test_decode_set_request_owned_with_unknown_field() {
        let mut buf = Vec::new();
        encode_bytes(1, b"key", &mut buf);
        encode_bytes(2, b"value", &mut buf);
        encode_uint64(3, 1000, &mut buf);
        encode_bytes(99, b"unknown", &mut buf);

        let decoded = SetRequestOwned::decode(&buf).unwrap();
        assert_eq!(&decoded.cache_key[..], b"key");
        assert_eq!(&decoded.cache_body[..], b"value");
        assert_eq!(decoded.ttl_milliseconds, 1000);
    }

    // SetResponse tests

    #[test]
    fn test_decode_set_response_ok() {
        let mut buf = Vec::new();
        encode_uint64(1, ECacheResult::Ok as u64, &mut buf);

        let response = SetResponse::decode(&buf).unwrap();
        assert!(matches!(response, SetResponse::Ok));
    }

    #[test]
    fn test_decode_set_response_empty() {
        // Empty response defaults to Ok
        let response = SetResponse::decode(&[]).unwrap();
        assert!(matches!(response, SetResponse::Ok));
    }

    #[test]
    fn test_decode_set_response_error() {
        let mut buf = Vec::new();
        encode_uint64(1, ECacheResult::Invalid as u64, &mut buf);
        encode_string(2, "set failed", &mut buf);

        let response = SetResponse::decode(&buf).unwrap();
        match response {
            SetResponse::Error { message } => {
                assert_eq!(message, "set failed");
            }
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn test_decode_set_response_error_no_message() {
        let mut buf = Vec::new();
        encode_uint64(1, ECacheResult::Miss as u64, &mut buf); // Non-Ok result

        let response = SetResponse::decode(&buf).unwrap();
        match response {
            SetResponse::Error { message } => {
                assert_eq!(message, "set failed");
            }
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn test_encode_set_response_ok() {
        let response = SetResponse::Ok;
        let encoded = response.encode();
        assert!(!encoded.is_empty());

        let decoded = SetResponse::decode(&encoded).unwrap();
        assert!(matches!(decoded, SetResponse::Ok));
    }

    #[test]
    fn test_encode_set_response_error() {
        let response = SetResponse::Error {
            message: "error msg".to_string(),
        };
        let encoded = response.encode();

        let decoded = SetResponse::decode(&encoded).unwrap();
        match decoded {
            SetResponse::Error { message } => assert_eq!(message, "error msg"),
            _ => panic!("expected Error"),
        }
    }

    // DeleteRequest tests

    #[test]
    fn test_encode_delete_request() {
        let req = DeleteRequest {
            cache_key: b"key-to-delete",
        };
        let encoded = req.encode();
        assert!(!encoded.is_empty());
    }

    // DeleteRequestOwned tests

    #[test]
    fn test_decode_delete_request_owned() {
        let original = DeleteRequest {
            cache_key: b"delete-key",
        };
        let encoded = original.encode();

        let decoded = DeleteRequestOwned::decode(&encoded).unwrap();
        assert_eq!(&decoded.cache_key[..], b"delete-key");
    }

    #[test]
    fn test_decode_delete_request_owned_empty() {
        let decoded = DeleteRequestOwned::decode(&[]).unwrap();
        assert!(decoded.cache_key.is_empty());
    }

    #[test]
    fn test_decode_delete_request_owned_with_unknown_field() {
        let mut buf = Vec::new();
        encode_bytes(1, b"key", &mut buf);
        encode_uint64(99, 12345, &mut buf); // Unknown field

        let decoded = DeleteRequestOwned::decode(&buf).unwrap();
        assert_eq!(&decoded.cache_key[..], b"key");
    }

    // DeleteResponse tests

    #[test]
    fn test_decode_delete_response() {
        let response = DeleteResponse::decode(&[]).unwrap();
        assert!(matches!(response, DeleteResponse::Ok));
    }

    #[test]
    fn test_decode_delete_response_with_data() {
        // Even with data, it's always Ok
        let response = DeleteResponse::decode(b"ignored").unwrap();
        assert!(matches!(response, DeleteResponse::Ok));
    }

    #[test]
    fn test_encode_delete_response() {
        let response = DeleteResponse::Ok;
        let encoded = response.encode();
        assert!(encoded.is_empty()); // DeleteResponse has no fields
    }

    // encode_string and encode_message tests

    #[test]
    fn test_encode_string() {
        let mut buf = Vec::new();
        encode_string(1, "hello", &mut buf);

        // Should be same as encode_bytes
        let mut expected = Vec::new();
        encode_bytes(1, b"hello", &mut expected);

        assert_eq!(buf, expected);
    }

    #[test]
    fn test_encode_message() {
        let inner_msg = b"inner message";
        let mut buf = Vec::new();
        encode_message(1, inner_msg, &mut buf);

        // Same format as bytes
        let mut expected = Vec::new();
        encode_bytes(1, inner_msg, &mut expected);

        assert_eq!(buf, expected);
    }

    // Debug trait tests

    #[test]
    fn test_get_response_debug() {
        let response = GetResponse::Hit(Bytes::from_static(b"val"));
        assert!(format!("{:?}", response).contains("Hit"));

        let response = GetResponse::Miss;
        assert!(format!("{:?}", response).contains("Miss"));

        let response = GetResponse::Error {
            message: "err".to_string(),
        };
        assert!(format!("{:?}", response).contains("Error"));
    }

    #[test]
    fn test_set_response_debug() {
        let response = SetResponse::Ok;
        assert!(format!("{:?}", response).contains("Ok"));

        let response = SetResponse::Error {
            message: "err".to_string(),
        };
        assert!(format!("{:?}", response).contains("Error"));
    }

    #[test]
    fn test_delete_response_debug() {
        let response = DeleteResponse::Ok;
        assert!(format!("{:?}", response).contains("Ok"));
    }

    #[test]
    fn test_get_request_owned_debug() {
        let req = GetRequestOwned {
            cache_key: Bytes::from_static(b"key"),
        };
        assert!(format!("{:?}", req).contains("GetRequestOwned"));
    }

    #[test]
    fn test_set_request_owned_debug() {
        let req = SetRequestOwned {
            cache_key: Bytes::from_static(b"key"),
            cache_body: Bytes::from_static(b"value"),
            ttl_milliseconds: 1000,
        };
        assert!(format!("{:?}", req).contains("SetRequestOwned"));
    }

    #[test]
    fn test_delete_request_owned_debug() {
        let req = DeleteRequestOwned {
            cache_key: Bytes::from_static(b"key"),
        };
        assert!(format!("{:?}", req).contains("DeleteRequestOwned"));
    }

    // Clone tests

    #[test]
    fn test_get_response_clone() {
        let response = GetResponse::Hit(Bytes::from_static(b"value"));
        let cloned = response.clone();
        match cloned {
            GetResponse::Hit(v) => assert_eq!(&v[..], b"value"),
            _ => panic!("expected Hit"),
        }
    }

    #[test]
    fn test_set_response_clone() {
        let response = SetResponse::Ok;
        let cloned = response.clone();
        assert!(matches!(cloned, SetResponse::Ok));
    }

    #[test]
    fn test_delete_response_clone() {
        let response = DeleteResponse::Ok;
        let cloned = response.clone();
        assert!(matches!(cloned, DeleteResponse::Ok));
    }

    #[test]
    fn test_get_request_owned_clone() {
        let req = GetRequestOwned {
            cache_key: Bytes::from_static(b"key"),
        };
        let cloned = req.clone();
        assert_eq!(cloned.cache_key, req.cache_key);
    }

    #[test]
    fn test_set_request_owned_clone() {
        let req = SetRequestOwned {
            cache_key: Bytes::from_static(b"key"),
            cache_body: Bytes::from_static(b"value"),
            ttl_milliseconds: 5000,
        };
        let cloned = req.clone();
        assert_eq!(cloned.cache_key, req.cache_key);
        assert_eq!(cloned.cache_body, req.cache_body);
        assert_eq!(cloned.ttl_milliseconds, req.ttl_milliseconds);
    }

    #[test]
    fn test_delete_request_owned_clone() {
        let req = DeleteRequestOwned {
            cache_key: Bytes::from_static(b"key"),
        };
        let cloned = req.clone();
        assert_eq!(cloned.cache_key, req.cache_key);
    }
}
