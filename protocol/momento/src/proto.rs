//! Minimal protobuf encoding/decoding for Momento cache messages.
//!
//! This implements just enough protobuf wire format to encode/decode
//! the cache API messages without requiring prost or other heavy deps.
//!
//! Supports both wire formats:
//! - gRPC: Direct Get/Set/Delete request/response messages
//! - Protosocket: CacheCommand/CacheResponse wrapper messages with message IDs

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

// ============================================================================
// Protosocket Wire Format Messages
// ============================================================================
//
// Protosocket uses a different wire format than gRPC:
// - Messages are length-delimited (varint prefix) over raw TCP
// - CacheCommand wraps requests with message_id for correlation
// - CacheResponse wraps responses with message_id
// - Authentication is done via AuthenticateCommand (first message on connection)

/// Control codes for protosocket messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u32)]
pub enum ControlCode {
    /// Normal message.
    #[default]
    Normal = 0,
    /// Cancel the RPC.
    Cancel = 1,
    /// End of stream (for streaming RPCs).
    End = 2,
}

impl ControlCode {
    pub fn from_u32(value: u32) -> Self {
        match value {
            0 => ControlCode::Normal,
            1 => ControlCode::Cancel,
            2 => ControlCode::End,
            _ => ControlCode::Normal,
        }
    }
}

/// Status codes for protosocket responses (matches gRPC codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u32)]
pub enum StatusCode {
    #[default]
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl StatusCode {
    pub fn from_u32(value: u32) -> Self {
        match value {
            0 => StatusCode::Ok,
            1 => StatusCode::Cancelled,
            2 => StatusCode::Unknown,
            3 => StatusCode::InvalidArgument,
            4 => StatusCode::DeadlineExceeded,
            5 => StatusCode::NotFound,
            6 => StatusCode::AlreadyExists,
            7 => StatusCode::PermissionDenied,
            8 => StatusCode::ResourceExhausted,
            9 => StatusCode::FailedPrecondition,
            10 => StatusCode::Aborted,
            11 => StatusCode::OutOfRange,
            12 => StatusCode::Unimplemented,
            13 => StatusCode::Internal,
            14 => StatusCode::Unavailable,
            15 => StatusCode::DataLoss,
            16 => StatusCode::Unauthenticated,
            _ => StatusCode::Unknown,
        }
    }
}

/// Error from a protosocket command.
#[derive(Debug, Clone)]
pub struct CommandError {
    pub code: StatusCode,
    pub message: String,
}

impl CommandError {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        encode_uint64(1, self.code as u64, &mut buf);
        if !self.message.is_empty() {
            encode_string(2, &self.message, &mut buf);
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        let mut code = StatusCode::Unknown;
        let mut message = String::new();

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;
            match field_number {
                1 => code = StatusCode::from_u32(decode_varint(&mut buf)? as u32),
                2 => {
                    let bytes = decode_length_delimited(&mut buf)?;
                    message = String::from_utf8_lossy(bytes).into_owned();
                }
                _ => skip_field(wire_type, &mut buf)?,
            }
        }

        Some(Self { code, message })
    }
}

/// Unary command types for protosocket.
#[derive(Debug, Clone)]
pub enum UnaryCommand {
    Authenticate {
        auth_token: String,
    },
    Get {
        namespace: String,
        key: Bytes,
    },
    Set {
        namespace: String,
        key: Bytes,
        value: Bytes,
        ttl_millis: u64,
    },
    Delete {
        namespace: String,
        key: Bytes,
    },
}

impl UnaryCommand {
    /// Encode the inner command (without the Unary wrapper).
    fn encode_inner(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        match self {
            UnaryCommand::Authenticate { auth_token } => {
                encode_string(1, auth_token, &mut buf);
            }
            UnaryCommand::Get { namespace, key } => {
                encode_string(1, namespace, &mut buf);
                encode_bytes(2, key, &mut buf);
            }
            UnaryCommand::Set {
                namespace,
                key,
                value,
                ttl_millis,
            } => {
                encode_string(1, namespace, &mut buf);
                encode_bytes(2, key, &mut buf);
                encode_bytes(3, value, &mut buf);
                encode_uint64(4, *ttl_millis, &mut buf);
            }
            UnaryCommand::Delete { namespace, key } => {
                encode_string(1, namespace, &mut buf);
                encode_bytes(2, key, &mut buf);
            }
        }
        buf
    }

    /// Encode as a Unary message with the command in the appropriate field.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        let inner = self.encode_inner();
        let field_number = match self {
            UnaryCommand::Authenticate { .. } => 1,
            UnaryCommand::Get { .. } => 2,
            UnaryCommand::Set { .. } => 3,
            UnaryCommand::Delete { .. } => 4,
        };
        encode_message(field_number, &inner, &mut buf);
        buf
    }

    /// Decode a Unary message.
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut buf = data;

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;
            if wire_type != WIRE_TYPE_LEN {
                skip_field(wire_type, &mut buf)?;
                continue;
            }

            let inner = decode_length_delimited(&mut buf)?;
            return match field_number {
                1 => Self::decode_authenticate(inner),
                2 => Self::decode_get(inner),
                3 => Self::decode_set(inner),
                4 => Self::decode_delete(inner),
                _ => None,
            };
        }

        None
    }

    fn decode_authenticate(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        let mut auth_token = String::new();

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;
            match field_number {
                1 => {
                    let bytes = decode_length_delimited(&mut buf)?;
                    auth_token = String::from_utf8_lossy(bytes).into_owned();
                }
                _ => skip_field(wire_type, &mut buf)?,
            }
        }

        Some(UnaryCommand::Authenticate { auth_token })
    }

    fn decode_get(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        let mut namespace = String::new();
        let mut key = Bytes::new();

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;
            match field_number {
                1 => {
                    let bytes = decode_length_delimited(&mut buf)?;
                    namespace = String::from_utf8_lossy(bytes).into_owned();
                }
                2 => {
                    let bytes = decode_length_delimited(&mut buf)?;
                    key = Bytes::copy_from_slice(bytes);
                }
                _ => skip_field(wire_type, &mut buf)?,
            }
        }

        Some(UnaryCommand::Get { namespace, key })
    }

    fn decode_set(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        let mut namespace = String::new();
        let mut key = Bytes::new();
        let mut value = Bytes::new();
        let mut ttl_millis = 0u64;

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;
            match field_number {
                1 => {
                    let bytes = decode_length_delimited(&mut buf)?;
                    namespace = String::from_utf8_lossy(bytes).into_owned();
                }
                2 => {
                    let bytes = decode_length_delimited(&mut buf)?;
                    key = Bytes::copy_from_slice(bytes);
                }
                3 => {
                    let bytes = decode_length_delimited(&mut buf)?;
                    value = Bytes::copy_from_slice(bytes);
                }
                4 => {
                    ttl_millis = decode_varint(&mut buf)?;
                }
                _ => skip_field(wire_type, &mut buf)?,
            }
        }

        Some(UnaryCommand::Set {
            namespace,
            key,
            value,
            ttl_millis,
        })
    }

    fn decode_delete(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        let mut namespace = String::new();
        let mut key = Bytes::new();

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;
            match field_number {
                1 => {
                    let bytes = decode_length_delimited(&mut buf)?;
                    namespace = String::from_utf8_lossy(bytes).into_owned();
                }
                2 => {
                    let bytes = decode_length_delimited(&mut buf)?;
                    key = Bytes::copy_from_slice(bytes);
                }
                _ => skip_field(wire_type, &mut buf)?,
            }
        }

        Some(UnaryCommand::Delete { namespace, key })
    }
}

/// A command sent from client to server over protosocket.
#[derive(Debug, Clone)]
pub struct CacheCommand {
    pub message_id: u64,
    pub control_code: ControlCode,
    pub command: Option<UnaryCommand>,
}

impl CacheCommand {
    /// Create a new command with the given message ID.
    pub fn new(message_id: u64, command: UnaryCommand) -> Self {
        Self {
            message_id,
            control_code: ControlCode::Normal,
            command: Some(command),
        }
    }

    /// Create a cancel command.
    pub fn cancel(message_id: u64) -> Self {
        Self {
            message_id,
            control_code: ControlCode::Cancel,
            command: None,
        }
    }

    /// Encode the command to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);

        // Field 2: message_id (per official proto)
        encode_uint64(2, self.message_id, &mut buf);

        // Field 3: control_code (per official proto)
        encode_uint64(3, self.control_code as u64, &mut buf);

        // Field 10: unary command (per official proto)
        if let Some(ref cmd) = self.command {
            let unary = cmd.encode();
            encode_message(10, &unary, &mut buf);
        }

        buf
    }

    /// Encode with length prefix for protosocket wire format.
    pub fn encode_length_delimited(&self) -> Vec<u8> {
        let inner = self.encode();
        let mut buf = Vec::with_capacity(inner.len() + 5);
        encode_varint(inner.len() as u64, &mut buf);
        buf.extend_from_slice(&inner);
        buf
    }

    /// Decode a command from bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        let mut message_id = 0u64;
        let mut control_code = ControlCode::Normal;
        let mut command = None;

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;
            match field_number {
                2 => message_id = decode_varint(&mut buf)?,
                3 => control_code = ControlCode::from_u32(decode_varint(&mut buf)? as u32),
                10 => {
                    let unary_data = decode_length_delimited(&mut buf)?;
                    command = UnaryCommand::decode(unary_data);
                }
                _ => skip_field(wire_type, &mut buf)?,
            }
        }

        Some(Self {
            message_id,
            control_code,
            command,
        })
    }
}

/// Response types for protosocket.
#[derive(Debug, Clone)]
pub enum UnaryResponse {
    Authenticate,
    Get { value: Option<Bytes> },
    Set,
    Delete,
}

impl UnaryResponse {
    /// Encode the inner response.
    fn encode_inner(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        match self {
            UnaryResponse::Authenticate => {
                // Empty message
            }
            UnaryResponse::Get { value } => {
                if let Some(v) = value {
                    encode_bytes(1, v, &mut buf);
                }
            }
            UnaryResponse::Set => {
                // Empty message
            }
            UnaryResponse::Delete => {
                // Empty message
            }
        }
        buf
    }

    /// Encode as a response with the appropriate field number.
    pub fn encode(&self, field_number: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        let inner = self.encode_inner();
        encode_message(field_number, &inner, &mut buf);
        buf
    }
}

/// A response sent from server to client over protosocket.
#[derive(Debug, Clone)]
pub struct CacheResponse {
    pub message_id: u64,
    pub control_code: ControlCode,
    pub result: CacheResponseResult,
}

/// The result portion of a CacheResponse.
#[derive(Debug, Clone)]
pub enum CacheResponseResult {
    Authenticate,
    Get { value: Option<Bytes> },
    Set,
    Delete,
    Error(CommandError),
}

impl CacheResponse {
    /// Create a successful authenticate response.
    pub fn authenticate(message_id: u64) -> Self {
        Self {
            message_id,
            control_code: ControlCode::Normal,
            result: CacheResponseResult::Authenticate,
        }
    }

    /// Create a successful get response (hit).
    pub fn get_hit(message_id: u64, value: Bytes) -> Self {
        Self {
            message_id,
            control_code: ControlCode::Normal,
            result: CacheResponseResult::Get { value: Some(value) },
        }
    }

    /// Create a get miss response.
    pub fn get_miss(message_id: u64) -> Self {
        Self {
            message_id,
            control_code: ControlCode::Normal,
            result: CacheResponseResult::Get { value: None },
        }
    }

    /// Create a successful set response.
    pub fn set_ok(message_id: u64) -> Self {
        Self {
            message_id,
            control_code: ControlCode::Normal,
            result: CacheResponseResult::Set,
        }
    }

    /// Create a successful delete response.
    pub fn delete_ok(message_id: u64) -> Self {
        Self {
            message_id,
            control_code: ControlCode::Normal,
            result: CacheResponseResult::Delete,
        }
    }

    /// Create an error response.
    pub fn error(message_id: u64, code: StatusCode, message: impl Into<String>) -> Self {
        Self {
            message_id,
            control_code: ControlCode::Normal,
            result: CacheResponseResult::Error(CommandError {
                code,
                message: message.into(),
            }),
        }
    }

    /// Encode the response to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);

        // Field 1: message_id
        encode_uint64(1, self.message_id, &mut buf);

        // Field 2: control_code
        encode_uint64(2, self.control_code as u64, &mut buf);

        // Response kind (per official proto field numbers)
        match &self.result {
            CacheResponseResult::Error(err) => {
                // Field 9: error
                let inner = err.encode();
                encode_message(9, &inner, &mut buf);
            }
            CacheResponseResult::Authenticate => {
                // Field 10: authenticate response (empty)
                encode_message(10, &[], &mut buf);
            }
            CacheResponseResult::Get { value } => {
                // Field 11: get response
                let mut inner = Vec::new();
                if let Some(v) = value {
                    encode_bytes(1, v, &mut inner);
                }
                encode_message(11, &inner, &mut buf);
            }
            CacheResponseResult::Set => {
                // Field 12: set response (empty)
                encode_message(12, &[], &mut buf);
            }
            CacheResponseResult::Delete => {
                // Field 13: delete response (empty)
                encode_message(13, &[], &mut buf);
            }
        }

        buf
    }

    /// Encode with length prefix for protosocket wire format.
    pub fn encode_length_delimited(&self) -> Vec<u8> {
        let inner = self.encode();
        let mut buf = Vec::with_capacity(inner.len() + 5);
        encode_varint(inner.len() as u64, &mut buf);
        buf.extend_from_slice(&inner);
        buf
    }

    /// Decode a response from bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut buf = data;
        let mut message_id = 0u64;
        let mut control_code = ControlCode::Normal;
        let mut result = None;

        while !buf.is_empty() {
            let (field_number, wire_type) = decode_tag(&mut buf)?;
            match field_number {
                1 if wire_type == WIRE_TYPE_VARINT => {
                    message_id = decode_varint(&mut buf)?;
                }
                2 if wire_type == WIRE_TYPE_VARINT => {
                    control_code = ControlCode::from_u32(decode_varint(&mut buf)? as u32);
                }
                // Response kinds (per official proto field numbers)
                9 if wire_type == WIRE_TYPE_LEN => {
                    let inner = decode_length_delimited(&mut buf)?;
                    let err = CommandError::decode(inner)?;
                    result = Some(CacheResponseResult::Error(err));
                }
                10 if wire_type == WIRE_TYPE_LEN => {
                    let _inner = decode_length_delimited(&mut buf)?;
                    result = Some(CacheResponseResult::Authenticate);
                }
                11 if wire_type == WIRE_TYPE_LEN => {
                    let inner = decode_length_delimited(&mut buf)?;
                    result = Some(Self::decode_get_response(inner));
                }
                12 if wire_type == WIRE_TYPE_LEN => {
                    let _inner = decode_length_delimited(&mut buf)?;
                    result = Some(CacheResponseResult::Set);
                }
                13 if wire_type == WIRE_TYPE_LEN => {
                    let _inner = decode_length_delimited(&mut buf)?;
                    result = Some(CacheResponseResult::Delete);
                }
                _ => skip_field(wire_type, &mut buf)?,
            }
        }

        Some(Self {
            message_id,
            control_code,
            result: result.unwrap_or(CacheResponseResult::Error(CommandError {
                code: StatusCode::Internal,
                message: "missing response".to_string(),
            })),
        })
    }

    fn decode_get_response(data: &[u8]) -> CacheResponseResult {
        let mut buf = data;
        let mut value = None;

        while !buf.is_empty() {
            if let Some((field_number, wire_type)) = decode_tag(&mut buf) {
                match field_number {
                    1 => {
                        if let Some(bytes) = decode_length_delimited(&mut buf) {
                            value = Some(Bytes::copy_from_slice(bytes));
                        }
                    }
                    _ => {
                        if skip_field(wire_type, &mut buf).is_none() {
                            break;
                        }
                    }
                }
            } else {
                break;
            }
        }

        CacheResponseResult::Get { value }
    }
}

/// Decode a length-delimited message from a buffer.
/// Returns (bytes_consumed, message) or None if incomplete.
pub fn decode_length_delimited_message(buf: &[u8]) -> Option<(usize, &[u8])> {
    let mut cursor = buf;
    let original_len = buf.len();

    // Decode varint length
    let len = decode_varint(&mut cursor)? as usize;
    let header_len = original_len - cursor.len();

    // Check if we have enough data
    if cursor.len() < len {
        return None;
    }

    let message = &cursor[..len];
    Some((header_len + len, message))
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

    // =========================================================================
    // Protosocket message tests
    // =========================================================================

    // ControlCode tests

    #[test]
    fn test_control_code_from_u32() {
        assert_eq!(ControlCode::from_u32(0), ControlCode::Normal);
        assert_eq!(ControlCode::from_u32(1), ControlCode::Cancel);
        assert_eq!(ControlCode::from_u32(2), ControlCode::End);
        assert_eq!(ControlCode::from_u32(99), ControlCode::Normal);
    }

    #[test]
    fn test_control_code_default() {
        assert_eq!(ControlCode::default(), ControlCode::Normal);
    }

    // StatusCode tests

    #[test]
    fn test_status_code_from_u32() {
        assert_eq!(StatusCode::from_u32(0), StatusCode::Ok);
        assert_eq!(StatusCode::from_u32(5), StatusCode::NotFound);
        assert_eq!(StatusCode::from_u32(14), StatusCode::Unavailable);
        assert_eq!(StatusCode::from_u32(16), StatusCode::Unauthenticated);
        assert_eq!(StatusCode::from_u32(99), StatusCode::Unknown);
    }

    #[test]
    fn test_status_code_default() {
        assert_eq!(StatusCode::default(), StatusCode::Ok);
    }

    // CommandError tests

    #[test]
    fn test_command_error_roundtrip() {
        let err = CommandError {
            code: StatusCode::NotFound,
            message: "key not found".to_string(),
        };
        let encoded = err.encode();
        let decoded = CommandError::decode(&encoded).unwrap();
        assert_eq!(decoded.code, StatusCode::NotFound);
        assert_eq!(decoded.message, "key not found");
    }

    #[test]
    fn test_command_error_empty_message() {
        let err = CommandError {
            code: StatusCode::Internal,
            message: String::new(),
        };
        let encoded = err.encode();
        let decoded = CommandError::decode(&encoded).unwrap();
        assert_eq!(decoded.code, StatusCode::Internal);
        assert!(decoded.message.is_empty());
    }

    // UnaryCommand tests

    #[test]
    fn test_unary_command_authenticate_roundtrip() {
        let cmd = UnaryCommand::Authenticate {
            auth_token: "test-token-123".to_string(),
        };
        let encoded = cmd.encode();
        let decoded = UnaryCommand::decode(&encoded).unwrap();
        match decoded {
            UnaryCommand::Authenticate { auth_token } => {
                assert_eq!(auth_token, "test-token-123");
            }
            _ => panic!("expected Authenticate"),
        }
    }

    #[test]
    fn test_unary_command_get_roundtrip() {
        let cmd = UnaryCommand::Get {
            namespace: "my-cache".to_string(),
            key: Bytes::from_static(b"my-key"),
        };
        let encoded = cmd.encode();
        let decoded = UnaryCommand::decode(&encoded).unwrap();
        match decoded {
            UnaryCommand::Get { namespace, key } => {
                assert_eq!(namespace, "my-cache");
                assert_eq!(key.as_ref(), b"my-key");
            }
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_unary_command_set_roundtrip() {
        let cmd = UnaryCommand::Set {
            namespace: "cache".to_string(),
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
            ttl_millis: 60000,
        };
        let encoded = cmd.encode();
        let decoded = UnaryCommand::decode(&encoded).unwrap();
        match decoded {
            UnaryCommand::Set {
                namespace,
                key,
                value,
                ttl_millis,
            } => {
                assert_eq!(namespace, "cache");
                assert_eq!(key.as_ref(), b"key");
                assert_eq!(value.as_ref(), b"value");
                assert_eq!(ttl_millis, 60000);
            }
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn test_unary_command_delete_roundtrip() {
        let cmd = UnaryCommand::Delete {
            namespace: "cache".to_string(),
            key: Bytes::from_static(b"delete-me"),
        };
        let encoded = cmd.encode();
        let decoded = UnaryCommand::decode(&encoded).unwrap();
        match decoded {
            UnaryCommand::Delete { namespace, key } => {
                assert_eq!(namespace, "cache");
                assert_eq!(key.as_ref(), b"delete-me");
            }
            _ => panic!("expected Delete"),
        }
    }

    // CacheCommand tests

    #[test]
    fn test_cache_command_roundtrip() {
        let cmd = CacheCommand::new(
            42,
            UnaryCommand::Get {
                namespace: "test".to_string(),
                key: Bytes::from_static(b"key"),
            },
        );
        let encoded = cmd.encode();
        let decoded = CacheCommand::decode(&encoded).unwrap();
        assert_eq!(decoded.message_id, 42);
        assert_eq!(decoded.control_code, ControlCode::Normal);
        assert!(decoded.command.is_some());
    }

    #[test]
    fn test_cache_command_cancel() {
        let cmd = CacheCommand::cancel(99);
        let encoded = cmd.encode();
        let decoded = CacheCommand::decode(&encoded).unwrap();
        assert_eq!(decoded.message_id, 99);
        assert_eq!(decoded.control_code, ControlCode::Cancel);
        assert!(decoded.command.is_none());
    }

    #[test]
    fn test_cache_command_length_delimited() {
        let cmd = CacheCommand::new(
            1,
            UnaryCommand::Authenticate {
                auth_token: "token".to_string(),
            },
        );
        let encoded = cmd.encode_length_delimited();
        // First bytes should be varint length
        let (consumed, message) = decode_length_delimited_message(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        let decoded = CacheCommand::decode(message).unwrap();
        assert_eq!(decoded.message_id, 1);
    }

    // CacheResponse tests

    #[test]
    fn test_cache_response_authenticate() {
        let resp = CacheResponse::authenticate(1);
        let encoded = resp.encode();
        let decoded = CacheResponse::decode(&encoded).unwrap();
        assert_eq!(decoded.message_id, 1);
        assert!(matches!(decoded.result, CacheResponseResult::Authenticate));
    }

    #[test]
    fn test_cache_response_get_hit() {
        let resp = CacheResponse::get_hit(2, Bytes::from_static(b"value"));
        let encoded = resp.encode();
        let decoded = CacheResponse::decode(&encoded).unwrap();
        assert_eq!(decoded.message_id, 2);
        match decoded.result {
            CacheResponseResult::Get { value } => {
                assert_eq!(value, Some(Bytes::from_static(b"value")));
            }
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_cache_response_get_miss() {
        let resp = CacheResponse::get_miss(3);
        let encoded = resp.encode();
        let decoded = CacheResponse::decode(&encoded).unwrap();
        assert_eq!(decoded.message_id, 3);
        match decoded.result {
            CacheResponseResult::Get { value } => {
                assert!(value.is_none());
            }
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_cache_response_set_ok() {
        let resp = CacheResponse::set_ok(4);
        let encoded = resp.encode();
        let decoded = CacheResponse::decode(&encoded).unwrap();
        assert_eq!(decoded.message_id, 4);
        assert!(matches!(decoded.result, CacheResponseResult::Set));
    }

    #[test]
    fn test_cache_response_delete_ok() {
        let resp = CacheResponse::delete_ok(5);
        let encoded = resp.encode();
        let decoded = CacheResponse::decode(&encoded).unwrap();
        assert_eq!(decoded.message_id, 5);
        assert!(matches!(decoded.result, CacheResponseResult::Delete));
    }

    #[test]
    fn test_cache_response_error() {
        let resp = CacheResponse::error(6, StatusCode::NotFound, "not found");
        let encoded = resp.encode();
        let decoded = CacheResponse::decode(&encoded).unwrap();
        assert_eq!(decoded.message_id, 6);
        match decoded.result {
            CacheResponseResult::Error(err) => {
                assert_eq!(err.code, StatusCode::NotFound);
                assert_eq!(err.message, "not found");
            }
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn test_cache_response_length_delimited() {
        let resp = CacheResponse::set_ok(7);
        let encoded = resp.encode_length_delimited();
        let (consumed, message) = decode_length_delimited_message(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        let decoded = CacheResponse::decode(message).unwrap();
        assert_eq!(decoded.message_id, 7);
    }

    // decode_length_delimited_message tests

    #[test]
    fn test_decode_length_delimited_message_complete() {
        let data = b"hello";
        let mut buf = Vec::new();
        encode_varint(data.len() as u64, &mut buf);
        buf.extend_from_slice(data);

        let (consumed, message) = decode_length_delimited_message(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(message, data);
    }

    #[test]
    fn test_decode_length_delimited_message_incomplete() {
        let mut buf = Vec::new();
        encode_varint(100, &mut buf); // Says 100 bytes
        buf.extend_from_slice(b"short"); // Only 5 bytes

        let result = decode_length_delimited_message(&buf);
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_length_delimited_message_empty() {
        let result = decode_length_delimited_message(&[]);
        assert!(result.is_none());
    }
}
