//! gRPC message framing.
//!
//! gRPC messages are length-prefixed with the following format:
//! - 1 byte: compressed flag (0 = uncompressed, 1 = compressed)
//! - 4 bytes: message length (big-endian u32)
//! - N bytes: message payload

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;

/// Size of the gRPC message header (1 byte flag + 4 bytes length).
pub const HEADER_SIZE: usize = 5;

/// Maximum message size (4MB default, matches gRPC default).
pub const MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// Encode a message into gRPC wire format.
///
/// Returns the encoded message with the length prefix.
pub fn encode_message(data: &[u8]) -> Bytes {
    encode_message_with_compression(data, false)
}

/// Encode a message with explicit compression flag.
pub fn encode_message_with_compression(data: &[u8], compressed: bool) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + data.len());

    // Compressed flag
    buf.put_u8(if compressed { 1 } else { 0 });

    // Message length (big-endian)
    buf.put_u32(data.len() as u32);

    // Message data
    buf.put_slice(data);

    buf.freeze()
}

/// Decode a single message from gRPC wire format.
///
/// Returns `Ok(Some((message, compressed)))` if a complete message was decoded,
/// `Ok(None)` if more data is needed, or `Err` on protocol error.
pub fn decode_message(buf: &mut BytesMut) -> io::Result<Option<(Bytes, bool)>> {
    if buf.len() < HEADER_SIZE {
        return Ok(None);
    }

    // Peek at header without consuming
    let compressed = buf[0] != 0;
    let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

    // Validate message size
    if length > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("message too large: {} bytes", length),
        ));
    }

    // Check if we have the complete message
    let total_size = HEADER_SIZE + length;
    if buf.len() < total_size {
        return Ok(None);
    }

    // Consume header
    buf.advance(HEADER_SIZE);

    // Extract message
    let message = buf.split_to(length).freeze();

    Ok(Some((message, compressed)))
}

/// Stateful decoder for gRPC messages.
///
/// Useful for incrementally decoding messages from a stream.
#[derive(Debug, Default)]
pub struct MessageDecoder {
    /// Buffer for incomplete messages.
    buffer: BytesMut,
}

impl MessageDecoder {
    /// Create a new message decoder.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Feed data into the decoder.
    pub fn feed(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Try to decode the next message.
    ///
    /// Returns `Ok(Some((message, compressed)))` if a complete message was decoded,
    /// `Ok(None)` if more data is needed.
    pub fn decode(&mut self) -> io::Result<Option<(Bytes, bool)>> {
        decode_message(&mut self.buffer)
    }

    /// Check if there's any buffered data.
    pub fn has_buffered_data(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// Get the amount of buffered data.
    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_empty_message() {
        let encoded = encode_message(&[]);
        assert_eq!(encoded.len(), HEADER_SIZE);
        assert_eq!(encoded[0], 0); // Not compressed
        assert_eq!(&encoded[1..5], &[0, 0, 0, 0]); // Length = 0
    }

    #[test]
    fn test_encode_message() {
        let data = b"hello world";
        let encoded = encode_message(data);

        assert_eq!(encoded.len(), HEADER_SIZE + data.len());
        assert_eq!(encoded[0], 0); // Not compressed
        assert_eq!(
            u32::from_be_bytes([encoded[1], encoded[2], encoded[3], encoded[4]]),
            data.len() as u32
        );
        assert_eq!(&encoded[HEADER_SIZE..], data);
    }

    #[test]
    fn test_encode_compressed() {
        let data = b"test";
        let encoded = encode_message_with_compression(data, true);
        assert_eq!(encoded[0], 1); // Compressed flag
    }

    #[test]
    fn test_decode_complete_message() {
        let data = b"hello world";
        let encoded = encode_message(data);
        let mut buf = BytesMut::from(&encoded[..]);

        let result = decode_message(&mut buf).unwrap();
        assert!(result.is_some());

        let (message, compressed) = result.unwrap();
        assert!(!compressed);
        assert_eq!(&message[..], data);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_incomplete_header() {
        let mut buf = BytesMut::from(&[0, 0, 0][..]);
        let result = decode_message(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_incomplete_body() {
        let data = b"hello world";
        let encoded = encode_message(data);
        // Only provide part of the message
        let mut buf = BytesMut::from(&encoded[..8]);

        let result = decode_message(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_multiple_messages() {
        let msg1 = b"first";
        let msg2 = b"second";

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&encode_message(msg1));
        buf.extend_from_slice(&encode_message(msg2));

        let (decoded1, _) = decode_message(&mut buf).unwrap().unwrap();
        assert_eq!(&decoded1[..], msg1);

        let (decoded2, _) = decode_message(&mut buf).unwrap().unwrap();
        assert_eq!(&decoded2[..], msg2);

        assert!(buf.is_empty());
    }

    #[test]
    fn test_message_decoder() {
        let mut decoder = MessageDecoder::new();

        let msg = b"test message";
        let encoded = encode_message(msg);

        // Feed partial data
        decoder.feed(&encoded[..3]);
        assert!(decoder.decode().unwrap().is_none());

        // Feed rest of data
        decoder.feed(&encoded[3..]);
        let (decoded, _) = decoder.decode().unwrap().unwrap();
        assert_eq!(&decoded[..], msg);
    }

    #[test]
    fn test_message_too_large() {
        let mut buf = BytesMut::new();
        buf.put_u8(0); // Not compressed
        buf.put_u32(MAX_MESSAGE_SIZE as u32 + 1); // Too large

        let result = decode_message(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_message_decoder_new() {
        let decoder = MessageDecoder::new();
        assert!(!decoder.has_buffered_data());
        assert_eq!(decoder.buffered_len(), 0);
    }

    #[test]
    fn test_message_decoder_default() {
        let decoder = MessageDecoder::default();
        assert!(!decoder.has_buffered_data());
    }

    #[test]
    fn test_message_decoder_feed() {
        let mut decoder = MessageDecoder::new();
        decoder.feed(&[1, 2, 3, 4, 5]);
        assert!(decoder.has_buffered_data());
        assert_eq!(decoder.buffered_len(), 5);
    }

    #[test]
    fn test_message_decoder_clear() {
        let mut decoder = MessageDecoder::new();
        decoder.feed(&[1, 2, 3, 4, 5]);
        assert!(decoder.has_buffered_data());

        decoder.clear();
        assert!(!decoder.has_buffered_data());
        assert_eq!(decoder.buffered_len(), 0);
    }

    #[test]
    fn test_message_decoder_multiple_feeds() {
        let mut decoder = MessageDecoder::new();
        let msg = b"hello";
        let encoded = encode_message(msg);

        // Feed in multiple small chunks
        for byte in &encoded[..] {
            decoder.feed(&[*byte]);
        }

        let (decoded, _) = decoder.decode().unwrap().unwrap();
        assert_eq!(&decoded[..], msg);
    }

    #[test]
    fn test_message_decoder_debug() {
        let decoder = MessageDecoder::new();
        let debug_str = format!("{:?}", decoder);
        assert!(debug_str.contains("MessageDecoder"));
    }

    #[test]
    fn test_header_size_constant() {
        assert_eq!(HEADER_SIZE, 5);
    }

    #[test]
    fn test_max_message_size_constant() {
        assert_eq!(MAX_MESSAGE_SIZE, 4 * 1024 * 1024);
    }

    #[test]
    fn test_decode_compressed_message() {
        let data = b"compressed data";
        let encoded = encode_message_with_compression(data, true);
        let mut buf = BytesMut::from(&encoded[..]);

        let (message, compressed) = decode_message(&mut buf).unwrap().unwrap();
        assert!(compressed);
        assert_eq!(&message[..], data);
    }
}
