//! HTTP/2 frame decoding.

use bytes::{Buf, Bytes, BytesMut};

use super::error::FrameError;
use super::types::*;
use super::{DEFAULT_MAX_FRAME_SIZE, FRAME_HEADER_SIZE, flags};

/// Frame decoder that parses HTTP/2 frames from a byte buffer.
pub struct FrameDecoder {
    max_frame_size: u32,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameDecoder {
    /// Create a new frame decoder with default settings.
    pub fn new() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    /// Set the maximum frame size.
    pub fn set_max_frame_size(&mut self, size: u32) {
        self.max_frame_size = size;
    }

    /// Try to decode a frame from the buffer.
    ///
    /// Returns `Ok(Some(frame))` if a complete frame was decoded,
    /// `Ok(None)` if more data is needed, or `Err` on protocol error.
    ///
    /// On success, the consumed bytes are removed from the buffer.
    pub fn decode(&self, buf: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
        // Need at least the header
        if buf.len() < FRAME_HEADER_SIZE {
            return Ok(None);
        }

        // Parse header without consuming
        let header = self.peek_header(buf)?;

        // Check frame size limit
        if header.length > self.max_frame_size {
            return Err(FrameError::FrameTooLarge {
                size: header.length,
                max: self.max_frame_size,
            });
        }

        // Check if we have the full frame
        let total_len = FRAME_HEADER_SIZE + header.length as usize;
        if buf.len() < total_len {
            return Ok(None);
        }

        // Consume the header
        buf.advance(FRAME_HEADER_SIZE);

        // Extract payload
        let payload = buf.split_to(header.length as usize).freeze();

        // Parse the frame based on type
        let frame = self.parse_frame(header, payload)?;

        Ok(Some(frame))
    }

    /// Peek at the frame header without consuming bytes.
    fn peek_header(&self, buf: &[u8]) -> Result<FrameHeader, FrameError> {
        debug_assert!(buf.len() >= FRAME_HEADER_SIZE);

        // Length is 24 bits (3 bytes), big-endian
        let length = ((buf[0] as u32) << 16) | ((buf[1] as u32) << 8) | (buf[2] as u32);

        let frame_type = buf[3];
        let flags = buf[4];

        // Stream ID is 31 bits (4 bytes), big-endian, high bit reserved
        let stream_id = StreamId::new(
            ((buf[5] as u32) << 24)
                | ((buf[6] as u32) << 16)
                | ((buf[7] as u32) << 8)
                | (buf[8] as u32),
        );

        Ok(FrameHeader {
            length,
            frame_type,
            flags,
            stream_id,
        })
    }

    /// Parse a frame given its header and payload.
    fn parse_frame(&self, header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        match FrameType::from_u8(header.frame_type) {
            Some(FrameType::Data) => self.parse_data(header, payload),
            Some(FrameType::Headers) => self.parse_headers(header, payload),
            Some(FrameType::Priority) => self.parse_priority(header, payload),
            Some(FrameType::RstStream) => self.parse_rst_stream(header, payload),
            Some(FrameType::Settings) => self.parse_settings(header, payload),
            Some(FrameType::PushPromise) => self.parse_push_promise(header, payload),
            Some(FrameType::Ping) => self.parse_ping(header, payload),
            Some(FrameType::GoAway) => self.parse_goaway(header, payload),
            Some(FrameType::WindowUpdate) => self.parse_window_update(header, payload),
            Some(FrameType::Continuation) => self.parse_continuation(header, payload),
            None => Ok(Frame::Unknown(UnknownFrame {
                frame_type: header.frame_type,
                flags: header.flags,
                stream_id: header.stream_id,
                payload,
            })),
        }
    }

    /// Parse DATA frame.
    fn parse_data(&self, header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        // DATA frames must not be sent on stream 0
        if header.stream_id.is_connection_level() {
            return Err(FrameError::StreamIdRequired {
                frame_type: header.frame_type,
            });
        }

        let end_stream = header.has_flag(flags::END_STREAM);
        let padded = header.has_flag(flags::PADDED);

        let data = if padded {
            self.remove_padding(payload)?
        } else {
            payload
        };

        Ok(Frame::Data(DataFrame {
            stream_id: header.stream_id,
            end_stream,
            data,
        }))
    }

    /// Parse HEADERS frame.
    fn parse_headers(&self, header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        // HEADERS frames must not be sent on stream 0
        if header.stream_id.is_connection_level() {
            return Err(FrameError::StreamIdRequired {
                frame_type: header.frame_type,
            });
        }

        let end_stream = header.has_flag(flags::END_STREAM);
        let end_headers = header.has_flag(flags::END_HEADERS);
        let padded = header.has_flag(flags::PADDED);
        let has_priority = header.has_flag(flags::PRIORITY);

        let mut payload = if padded {
            self.remove_padding(payload)?
        } else {
            payload
        };

        let priority = if has_priority {
            if payload.len() < 5 {
                return Err(FrameError::InvalidPayloadLength {
                    frame_type: header.frame_type,
                    expected: 5,
                    actual: payload.len(),
                });
            }

            let first = payload.get_u32();
            let exclusive = (first & 0x8000_0000) != 0;
            let dependency = StreamId::new(first & 0x7FFF_FFFF);
            let weight = payload.get_u8();

            Some(Priority {
                exclusive,
                dependency,
                weight,
            })
        } else {
            None
        };

        Ok(Frame::Headers(HeadersFrame {
            stream_id: header.stream_id,
            end_stream,
            end_headers,
            priority,
            header_block: payload,
        }))
    }

    /// Parse PRIORITY frame.
    fn parse_priority(&self, header: FrameHeader, mut payload: Bytes) -> Result<Frame, FrameError> {
        // PRIORITY frames must not be sent on stream 0
        if header.stream_id.is_connection_level() {
            return Err(FrameError::StreamIdRequired {
                frame_type: header.frame_type,
            });
        }

        // PRIORITY frame payload is exactly 5 bytes
        if payload.len() != 5 {
            return Err(FrameError::InvalidPayloadLength {
                frame_type: header.frame_type,
                expected: 5,
                actual: payload.len(),
            });
        }

        let first = payload.get_u32();
        let exclusive = (first & 0x8000_0000) != 0;
        let dependency = StreamId::new(first & 0x7FFF_FFFF);
        let weight = payload.get_u8();

        Ok(Frame::Priority(PriorityFrame {
            stream_id: header.stream_id,
            priority: Priority {
                exclusive,
                dependency,
                weight,
            },
        }))
    }

    /// Parse RST_STREAM frame.
    fn parse_rst_stream(
        &self,
        header: FrameHeader,
        mut payload: Bytes,
    ) -> Result<Frame, FrameError> {
        // RST_STREAM frames must not be sent on stream 0
        if header.stream_id.is_connection_level() {
            return Err(FrameError::StreamIdRequired {
                frame_type: header.frame_type,
            });
        }

        // RST_STREAM frame payload is exactly 4 bytes
        if payload.len() != 4 {
            return Err(FrameError::InvalidPayloadLength {
                frame_type: header.frame_type,
                expected: 4,
                actual: payload.len(),
            });
        }

        let error_code = payload.get_u32();

        Ok(Frame::RstStream(RstStreamFrame {
            stream_id: header.stream_id,
            error_code,
        }))
    }

    /// Parse SETTINGS frame.
    fn parse_settings(&self, header: FrameHeader, mut payload: Bytes) -> Result<Frame, FrameError> {
        // SETTINGS frames must be sent on stream 0
        if !header.stream_id.is_connection_level() {
            return Err(FrameError::InvalidStreamZero {
                frame_type: header.frame_type,
            });
        }

        let ack = header.has_flag(flags::ACK);

        // ACK SETTINGS must have empty payload
        if ack && !payload.is_empty() {
            return Err(FrameError::InvalidPayloadLength {
                frame_type: header.frame_type,
                expected: 0,
                actual: payload.len(),
            });
        }

        // SETTINGS payload must be a multiple of 6 bytes
        if !payload.len().is_multiple_of(6) {
            return Err(FrameError::InvalidPayloadLength {
                frame_type: header.frame_type,
                expected: (payload.len() / 6) * 6,
                actual: payload.len(),
            });
        }

        let mut settings = Vec::with_capacity(payload.len() / 6);

        while payload.has_remaining() {
            let id = SettingId::from_u16(payload.get_u16());
            let value = payload.get_u32();

            // Validate certain settings
            self.validate_setting(id, value)?;

            settings.push(Setting { id, value });
        }

        Ok(Frame::Settings(SettingsFrame { ack, settings }))
    }

    /// Validate a setting value.
    fn validate_setting(&self, id: SettingId, value: u32) -> Result<(), FrameError> {
        match id {
            SettingId::EnablePush => {
                if value > 1 {
                    return Err(FrameError::InvalidSettingValue {
                        id: id.to_u16(),
                        value,
                    });
                }
            }
            SettingId::InitialWindowSize => {
                // Must not exceed 2^31 - 1
                if value > 0x7FFF_FFFF {
                    return Err(FrameError::InvalidSettingValue {
                        id: id.to_u16(),
                        value,
                    });
                }
            }
            SettingId::MaxFrameSize => {
                // Must be between 16384 and 16777215
                if !(16_384..=16_777_215).contains(&value) {
                    return Err(FrameError::InvalidSettingValue {
                        id: id.to_u16(),
                        value,
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Parse PUSH_PROMISE frame.
    fn parse_push_promise(&self, header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        // PUSH_PROMISE frames must not be sent on stream 0
        if header.stream_id.is_connection_level() {
            return Err(FrameError::StreamIdRequired {
                frame_type: header.frame_type,
            });
        }

        let end_headers = header.has_flag(flags::END_HEADERS);
        let padded = header.has_flag(flags::PADDED);

        let mut payload = if padded {
            self.remove_padding(payload)?
        } else {
            payload
        };

        if payload.len() < 4 {
            return Err(FrameError::InvalidPayloadLength {
                frame_type: header.frame_type,
                expected: 4,
                actual: payload.len(),
            });
        }

        let promised_stream_id = StreamId::new(payload.get_u32() & 0x7FFF_FFFF);

        Ok(Frame::PushPromise(PushPromiseFrame {
            stream_id: header.stream_id,
            end_headers,
            promised_stream_id,
            header_block: payload,
        }))
    }

    /// Parse PING frame.
    fn parse_ping(&self, header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        // PING frames must be sent on stream 0
        if !header.stream_id.is_connection_level() {
            return Err(FrameError::InvalidStreamZero {
                frame_type: header.frame_type,
            });
        }

        // PING frame payload is exactly 8 bytes
        if payload.len() != 8 {
            return Err(FrameError::InvalidPayloadLength {
                frame_type: header.frame_type,
                expected: 8,
                actual: payload.len(),
            });
        }

        let ack = header.has_flag(flags::ACK);
        let mut data = [0u8; 8];
        data.copy_from_slice(&payload[..8]);

        Ok(Frame::Ping(PingFrame { ack, data }))
    }

    /// Parse GOAWAY frame.
    fn parse_goaway(&self, header: FrameHeader, mut payload: Bytes) -> Result<Frame, FrameError> {
        // GOAWAY frames must be sent on stream 0
        if !header.stream_id.is_connection_level() {
            return Err(FrameError::InvalidStreamZero {
                frame_type: header.frame_type,
            });
        }

        // GOAWAY frame payload is at least 8 bytes
        if payload.len() < 8 {
            return Err(FrameError::InvalidPayloadLength {
                frame_type: header.frame_type,
                expected: 8,
                actual: payload.len(),
            });
        }

        let last_stream_id = StreamId::new(payload.get_u32() & 0x7FFF_FFFF);
        let error_code = payload.get_u32();
        let debug_data = payload;

        Ok(Frame::GoAway(GoAwayFrame {
            last_stream_id,
            error_code,
            debug_data,
        }))
    }

    /// Parse WINDOW_UPDATE frame.
    fn parse_window_update(
        &self,
        header: FrameHeader,
        mut payload: Bytes,
    ) -> Result<Frame, FrameError> {
        // WINDOW_UPDATE frame payload is exactly 4 bytes
        if payload.len() != 4 {
            return Err(FrameError::InvalidPayloadLength {
                frame_type: header.frame_type,
                expected: 4,
                actual: payload.len(),
            });
        }

        let increment = payload.get_u32() & 0x7FFF_FFFF;

        // Window increment must be non-zero
        if increment == 0 {
            return Err(FrameError::InvalidWindowIncrement { increment });
        }

        Ok(Frame::WindowUpdate(WindowUpdateFrame {
            stream_id: header.stream_id,
            increment,
        }))
    }

    /// Parse CONTINUATION frame.
    fn parse_continuation(&self, header: FrameHeader, payload: Bytes) -> Result<Frame, FrameError> {
        // CONTINUATION frames must not be sent on stream 0
        if header.stream_id.is_connection_level() {
            return Err(FrameError::StreamIdRequired {
                frame_type: header.frame_type,
            });
        }

        let end_headers = header.has_flag(flags::END_HEADERS);

        Ok(Frame::Continuation(ContinuationFrame {
            stream_id: header.stream_id,
            end_headers,
            header_block: payload,
        }))
    }

    /// Remove padding from a padded frame payload.
    fn remove_padding(&self, mut payload: Bytes) -> Result<Bytes, FrameError> {
        if payload.is_empty() {
            return Err(FrameError::InvalidPadding {
                pad_length: 0,
                payload_length: 0,
            });
        }

        let pad_length = payload.get_u8() as usize;

        // Padding length must not exceed remaining payload
        if pad_length >= payload.len() {
            return Err(FrameError::InvalidPadding {
                pad_length: pad_length as u8,
                payload_length: payload.len() + 1,
            });
        }

        // Remove padding bytes from the end
        let data_len = payload.len() - pad_length;
        Ok(payload.slice(..data_len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // FrameDecoder basic tests

    #[test]
    fn test_decoder_default() {
        let decoder = FrameDecoder::default();
        // Should use default max frame size
        let mut buf = BytesMut::new();
        assert!(decoder.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_decoder_set_max_frame_size() {
        let mut decoder = FrameDecoder::new();
        decoder.set_max_frame_size(32768);

        // Create a frame that would exceed default but fit in 32768
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[
            0x00, 0x50, 0x00, // Length: 20480
            0x00, // Type: DATA
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
        ]);
        buf.extend_from_slice(&vec![0u8; 20480]);

        let result = decoder.decode(&mut buf);
        assert!(result.is_ok());
    }

    #[test]
    fn test_decode_frame_too_large() {
        let decoder = FrameDecoder::new(); // Default max is 16384

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[
            0x00, 0x50, 0x00, // Length: 20480 (exceeds 16384)
            0x00, // Type: DATA
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
        ]);
        buf.extend_from_slice(&vec![0u8; 20480]);

        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::FrameTooLarge {
                size: 20480,
                max: 16384
            }
        ));
    }

    #[test]
    fn test_decode_incomplete_header() {
        let decoder = FrameDecoder::new();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0x00, 0x00]); // Only 2 bytes, need 9

        let result = decoder.decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    // SETTINGS frame tests

    #[test]
    fn test_decode_settings_frame() {
        let mut buf = BytesMut::new();

        // SETTINGS frame with HEADER_TABLE_SIZE = 8192
        buf.extend_from_slice(&[
            0x00, 0x00, 0x06, // Length: 6
            0x04, // Type: SETTINGS
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x00, 0x01, // Setting ID: HEADER_TABLE_SIZE
            0x00, 0x00, 0x20, 0x00, // Value: 8192
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Settings(settings) => {
                assert!(!settings.ack);
                assert_eq!(settings.settings.len(), 1);
                assert_eq!(settings.settings[0].id, SettingId::HeaderTableSize);
                assert_eq!(settings.settings[0].value, 8192);
            }
            _ => panic!("Expected SETTINGS frame"),
        }

        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_settings_ack() {
        let mut buf = BytesMut::new();

        // SETTINGS ACK frame
        buf.extend_from_slice(&[
            0x00, 0x00, 0x00, // Length: 0
            0x04, // Type: SETTINGS
            0x01, // Flags: ACK
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Settings(settings) => {
                assert!(settings.ack);
                assert!(settings.settings.is_empty());
            }
            _ => panic!("Expected SETTINGS frame"),
        }
    }

    #[test]
    fn test_decode_settings_on_non_zero_stream() {
        let mut buf = BytesMut::new();

        // SETTINGS frame on stream 1 (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x00, // Length: 0
            0x04, // Type: SETTINGS
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1 (invalid)
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidStreamZero { frame_type: 0x04 }
        ));
    }

    #[test]
    fn test_decode_settings_ack_with_payload() {
        let mut buf = BytesMut::new();

        // SETTINGS ACK with payload (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x06, // Length: 6
            0x04, // Type: SETTINGS
            0x01, // Flags: ACK
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x00, 0x01, 0x00, 0x00, 0x20, 0x00,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidPayloadLength {
                frame_type: 0x04,
                expected: 0,
                actual: 6
            }
        ));
    }

    #[test]
    fn test_decode_settings_invalid_payload_length() {
        let mut buf = BytesMut::new();

        // SETTINGS with 5 bytes (not multiple of 6)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x05, // Length: 5
            0x04, // Type: SETTINGS
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x00, 0x01, 0x00, 0x00, 0x20,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidPayloadLength {
                frame_type: 0x04,
                ..
            }
        ));
    }

    #[test]
    fn test_decode_settings_invalid_enable_push() {
        let mut buf = BytesMut::new();

        // SETTINGS with ENABLE_PUSH = 2 (invalid, must be 0 or 1)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x06, // Length: 6
            0x04, // Type: SETTINGS
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x00, 0x02, // Setting ID: ENABLE_PUSH
            0x00, 0x00, 0x00, 0x02, // Value: 2 (invalid)
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidSettingValue { id: 0x02, value: 2 }
        ));
    }

    #[test]
    fn test_decode_settings_invalid_initial_window_size() {
        let mut buf = BytesMut::new();

        // SETTINGS with INITIAL_WINDOW_SIZE > 2^31-1 (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x06, // Length: 6
            0x04, // Type: SETTINGS
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x00, 0x04, // Setting ID: INITIAL_WINDOW_SIZE
            0x80, 0x00, 0x00, 0x00, // Value: 2^31 (invalid)
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidSettingValue { id: 0x04, .. }
        ));
    }

    #[test]
    fn test_decode_settings_invalid_max_frame_size_too_small() {
        let mut buf = BytesMut::new();

        // SETTINGS with MAX_FRAME_SIZE < 16384 (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x06, // Length: 6
            0x04, // Type: SETTINGS
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x00, 0x05, // Setting ID: MAX_FRAME_SIZE
            0x00, 0x00, 0x10, 0x00, // Value: 4096 (invalid, too small)
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidSettingValue { id: 0x05, .. }
        ));
    }

    #[test]
    fn test_decode_settings_invalid_max_frame_size_too_large() {
        let mut buf = BytesMut::new();

        // SETTINGS with MAX_FRAME_SIZE > 16777215 (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x06, // Length: 6
            0x04, // Type: SETTINGS
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x00, 0x05, // Setting ID: MAX_FRAME_SIZE
            0x01, 0x00, 0x00, 0x01, // Value: 16777217 (invalid, too large)
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidSettingValue { id: 0x05, .. }
        ));
    }

    #[test]
    fn test_decode_settings_multiple() {
        let mut buf = BytesMut::new();

        // SETTINGS with multiple settings
        buf.extend_from_slice(&[
            0x00, 0x00, 0x12, // Length: 18 (3 settings)
            0x04, // Type: SETTINGS
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x00, 0x01, 0x00, 0x00, 0x10, 0x00, // HEADER_TABLE_SIZE = 4096
            0x00, 0x03, 0x00, 0x00, 0x00, 0x64, // MAX_CONCURRENT_STREAMS = 100
            0x00, 0x04, 0x00, 0x00, 0xff, 0xff, // INITIAL_WINDOW_SIZE = 65535
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Settings(settings) => {
                assert_eq!(settings.settings.len(), 3);
                assert_eq!(settings.settings[0].id, SettingId::HeaderTableSize);
                assert_eq!(settings.settings[0].value, 4096);
                assert_eq!(settings.settings[1].id, SettingId::MaxConcurrentStreams);
                assert_eq!(settings.settings[1].value, 100);
                assert_eq!(settings.settings[2].id, SettingId::InitialWindowSize);
                assert_eq!(settings.settings[2].value, 65535);
            }
            _ => panic!("Expected SETTINGS frame"),
        }
    }

    // PING frame tests

    #[test]
    fn test_decode_ping_frame() {
        let mut buf = BytesMut::new();

        // PING frame
        buf.extend_from_slice(&[
            0x00, 0x00, 0x08, // Length: 8
            0x06, // Type: PING
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // Data
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Ping(ping) => {
                assert!(!ping.ack);
                assert_eq!(ping.data, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
            }
            _ => panic!("Expected PING frame"),
        }
    }

    #[test]
    fn test_decode_ping_ack() {
        let mut buf = BytesMut::new();

        // PING ACK frame
        buf.extend_from_slice(&[
            0x00, 0x00, 0x08, // Length: 8
            0x06, // Type: PING
            0x01, // Flags: ACK
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // Data
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Ping(ping) => {
                assert!(ping.ack);
            }
            _ => panic!("Expected PING frame"),
        }
    }

    #[test]
    fn test_decode_ping_on_non_zero_stream() {
        let mut buf = BytesMut::new();

        // PING frame on stream 1 (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x08, // Length: 8
            0x06, // Type: PING
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1 (invalid)
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidStreamZero { frame_type: 0x06 }
        ));
    }

    #[test]
    fn test_decode_ping_wrong_size() {
        let mut buf = BytesMut::new();

        // PING frame with wrong payload size
        buf.extend_from_slice(&[
            0x00, 0x00, 0x07, // Length: 7 (should be 8)
            0x06, // Type: PING
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidPayloadLength {
                frame_type: 0x06,
                expected: 8,
                actual: 7
            }
        ));
    }

    // WINDOW_UPDATE frame tests

    #[test]
    fn test_decode_window_update() {
        let mut buf = BytesMut::new();

        // WINDOW_UPDATE frame
        buf.extend_from_slice(&[
            0x00, 0x00, 0x04, // Length: 4
            0x08, // Type: WINDOW_UPDATE
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x00, 0x01, 0x00, 0x00, // Increment: 65536
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::WindowUpdate(wu) => {
                assert_eq!(wu.stream_id.value(), 1);
                assert_eq!(wu.increment, 65536);
            }
            _ => panic!("Expected WINDOW_UPDATE frame"),
        }
    }

    #[test]
    fn test_decode_window_update_connection_level() {
        let mut buf = BytesMut::new();

        // WINDOW_UPDATE on stream 0 (connection level)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x04, // Length: 4
            0x08, // Type: WINDOW_UPDATE
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x00, 0x01, 0x00, 0x00, // Increment: 65536
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::WindowUpdate(wu) => {
                assert!(wu.stream_id.is_connection_level());
            }
            _ => panic!("Expected WINDOW_UPDATE frame"),
        }
    }

    #[test]
    fn test_decode_window_update_wrong_size() {
        let mut buf = BytesMut::new();

        // WINDOW_UPDATE with wrong payload size
        buf.extend_from_slice(&[
            0x00, 0x00, 0x05, // Length: 5 (should be 4)
            0x08, // Type: WINDOW_UPDATE
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x00, 0x01, 0x00, 0x00, 0x00,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidPayloadLength {
                frame_type: 0x08,
                expected: 4,
                actual: 5
            }
        ));
    }

    #[test]
    fn test_decode_window_update_zero_increment() {
        let mut buf = BytesMut::new();

        // WINDOW_UPDATE with zero increment (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x04, // Length: 4
            0x08, // Type: WINDOW_UPDATE
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x00, 0x00, 0x00, 0x00, // Increment: 0 (invalid)
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidWindowIncrement { increment: 0 }
        ));
    }

    // DATA frame tests

    #[test]
    fn test_decode_incomplete() {
        let mut buf = BytesMut::new();

        // Incomplete frame (only header, no payload)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x08, // Length: 8
            0x06, // Type: PING
            0x00, // Flags: none
            0x00, 0x00, 0x00,
            0x00, // Stream ID: 0
                  // Missing 8 bytes of payload
        ]);

        let decoder = FrameDecoder::new();
        let result = decoder.decode(&mut buf).unwrap();
        assert!(result.is_none());

        // Buffer should be unchanged
        assert_eq!(buf.len(), 9);
    }

    #[test]
    fn test_decode_data_frame() {
        let mut buf = BytesMut::new();

        // DATA frame with END_STREAM
        buf.extend_from_slice(&[
            0x00, 0x00, 0x05, // Length: 5
            0x00, // Type: DATA
            0x01, // Flags: END_STREAM
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            b'h', b'e', b'l', b'l', b'o', // Data
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Data(data) => {
                assert_eq!(data.stream_id.value(), 1);
                assert!(data.end_stream);
                assert_eq!(&data.data[..], b"hello");
            }
            _ => panic!("Expected DATA frame"),
        }
    }

    #[test]
    fn test_decode_data_no_end_stream() {
        let mut buf = BytesMut::new();

        // DATA frame without END_STREAM
        buf.extend_from_slice(&[
            0x00, 0x00, 0x05, // Length: 5
            0x00, // Type: DATA
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            b'h', b'e', b'l', b'l', b'o',
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Data(data) => {
                assert!(!data.end_stream);
            }
            _ => panic!("Expected DATA frame"),
        }
    }

    #[test]
    fn test_decode_data_on_stream_zero() {
        let mut buf = BytesMut::new();

        // DATA frame on stream 0 (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x05, // Length: 5
            0x00, // Type: DATA
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0 (invalid)
            b'h', b'e', b'l', b'l', b'o',
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::StreamIdRequired { frame_type: 0x00 }
        ));
    }

    #[test]
    fn test_decode_data_padded() {
        let mut buf = BytesMut::new();

        // DATA frame with padding
        buf.extend_from_slice(&[
            0x00, 0x00, 0x09, // Length: 9
            0x00, // Type: DATA
            0x08, // Flags: PADDED
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x03, // Pad length: 3
            b'h', b'e', b'l', b'l', b'o', // Data: "hello"
            0x00, 0x00, 0x00, // Padding
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Data(data) => {
                assert_eq!(&data.data[..], b"hello");
            }
            _ => panic!("Expected DATA frame"),
        }
    }

    #[test]
    fn test_decode_data_invalid_padding() {
        let mut buf = BytesMut::new();

        // DATA frame with invalid padding (pad_length > payload)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x05, // Length: 5
            0x00, // Type: DATA
            0x08, // Flags: PADDED
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x10, // Pad length: 16 (exceeds payload)
            b'h', b'e', b'l', b'l',
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(err, FrameError::InvalidPadding { .. }));
    }

    #[test]
    fn test_decode_data_empty_padded_payload() {
        let mut buf = BytesMut::new();

        // DATA frame with PADDED flag but empty payload
        buf.extend_from_slice(&[
            0x00, 0x00, 0x00, // Length: 0
            0x00, // Type: DATA
            0x08, // Flags: PADDED
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidPadding {
                pad_length: 0,
                payload_length: 0
            }
        ));
    }

    // HEADERS frame tests

    #[test]
    fn test_decode_headers_frame() {
        let mut buf = BytesMut::new();

        // HEADERS frame with END_HEADERS
        buf.extend_from_slice(&[
            0x00, 0x00, 0x03, // Length: 3
            0x01, // Type: HEADERS
            0x04, // Flags: END_HEADERS
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x82, 0x86, 0x84, // HPACK encoded headers (example)
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Headers(headers) => {
                assert_eq!(headers.stream_id.value(), 1);
                assert!(!headers.end_stream);
                assert!(headers.end_headers);
                assert!(headers.priority.is_none());
                assert_eq!(headers.header_block.len(), 3);
            }
            _ => panic!("Expected HEADERS frame"),
        }
    }

    #[test]
    fn test_decode_headers_with_end_stream() {
        let mut buf = BytesMut::new();

        // HEADERS frame with END_STREAM and END_HEADERS
        buf.extend_from_slice(&[
            0x00, 0x00, 0x03, // Length: 3
            0x01, // Type: HEADERS
            0x05, // Flags: END_STREAM | END_HEADERS
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x82, 0x86, 0x84,
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Headers(headers) => {
                assert!(headers.end_stream);
                assert!(headers.end_headers);
            }
            _ => panic!("Expected HEADERS frame"),
        }
    }

    #[test]
    fn test_decode_headers_on_stream_zero() {
        let mut buf = BytesMut::new();

        // HEADERS frame on stream 0 (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x03, // Length: 3
            0x01, // Type: HEADERS
            0x04, // Flags: END_HEADERS
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0 (invalid)
            0x82, 0x86, 0x84,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::StreamIdRequired { frame_type: 0x01 }
        ));
    }

    #[test]
    fn test_decode_headers_with_priority() {
        let mut buf = BytesMut::new();

        // HEADERS frame with PRIORITY flag
        buf.extend_from_slice(&[
            0x00, 0x00, 0x08, // Length: 8 (5 priority + 3 headers)
            0x01, // Type: HEADERS
            0x24, // Flags: PRIORITY | END_HEADERS
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x80, 0x00, 0x00, 0x03, // Stream dependency: 3 (exclusive)
            0x0f, // Weight: 15
            0x82, 0x86, 0x84, // Header block
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Headers(headers) => {
                let priority = headers.priority.unwrap();
                assert!(priority.exclusive);
                assert_eq!(priority.dependency.value(), 3);
                assert_eq!(priority.weight, 15);
            }
            _ => panic!("Expected HEADERS frame"),
        }
    }

    #[test]
    fn test_decode_headers_with_priority_too_short() {
        let mut buf = BytesMut::new();

        // HEADERS frame with PRIORITY flag but payload too short
        buf.extend_from_slice(&[
            0x00, 0x00, 0x03, // Length: 3 (need 5 for priority)
            0x01, // Type: HEADERS
            0x20, // Flags: PRIORITY
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x82, 0x86, 0x84, // Not enough for priority
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidPayloadLength {
                frame_type: 0x01,
                expected: 5,
                ..
            }
        ));
    }

    #[test]
    fn test_decode_headers_padded() {
        let mut buf = BytesMut::new();

        // HEADERS frame with padding
        buf.extend_from_slice(&[
            0x00, 0x00, 0x07, // Length: 7
            0x01, // Type: HEADERS
            0x0c, // Flags: PADDED | END_HEADERS
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x03, // Pad length: 3
            0x82, 0x86, 0x84, // Header block
            0x00, 0x00, 0x00, // Padding
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Headers(headers) => {
                assert_eq!(headers.header_block.len(), 3);
            }
            _ => panic!("Expected HEADERS frame"),
        }
    }

    // PRIORITY frame tests

    #[test]
    fn test_decode_priority_frame() {
        let mut buf = BytesMut::new();

        // PRIORITY frame
        buf.extend_from_slice(&[
            0x00, 0x00, 0x05, // Length: 5
            0x02, // Type: PRIORITY
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x00, 0x00, 0x00, 0x03, // Stream dependency: 3
            0x10, // Weight: 16
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Priority(priority) => {
                assert_eq!(priority.stream_id.value(), 1);
                assert!(!priority.priority.exclusive);
                assert_eq!(priority.priority.dependency.value(), 3);
                assert_eq!(priority.priority.weight, 16);
            }
            _ => panic!("Expected PRIORITY frame"),
        }
    }

    #[test]
    fn test_decode_priority_exclusive() {
        let mut buf = BytesMut::new();

        // PRIORITY frame with exclusive flag
        buf.extend_from_slice(&[
            0x00, 0x00, 0x05, // Length: 5
            0x02, // Type: PRIORITY
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x80, 0x00, 0x00, 0x03, // Stream dependency: 3 (exclusive)
            0x10, // Weight: 16
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Priority(priority) => {
                assert!(priority.priority.exclusive);
                assert_eq!(priority.priority.dependency.value(), 3);
            }
            _ => panic!("Expected PRIORITY frame"),
        }
    }

    #[test]
    fn test_decode_priority_on_stream_zero() {
        let mut buf = BytesMut::new();

        // PRIORITY frame on stream 0 (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x05, // Length: 5
            0x02, // Type: PRIORITY
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0 (invalid)
            0x00, 0x00, 0x00, 0x03, 0x10,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::StreamIdRequired { frame_type: 0x02 }
        ));
    }

    #[test]
    fn test_decode_priority_wrong_size() {
        let mut buf = BytesMut::new();

        // PRIORITY frame with wrong payload size
        buf.extend_from_slice(&[
            0x00, 0x00, 0x04, // Length: 4 (should be 5)
            0x02, // Type: PRIORITY
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x00, 0x00, 0x00, 0x03,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidPayloadLength {
                frame_type: 0x02,
                expected: 5,
                actual: 4
            }
        ));
    }

    // RST_STREAM frame tests

    #[test]
    fn test_decode_rst_stream() {
        let mut buf = BytesMut::new();

        // RST_STREAM frame
        buf.extend_from_slice(&[
            0x00, 0x00, 0x04, // Length: 4
            0x03, // Type: RST_STREAM
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x00, 0x00, 0x00, 0x08, // Error code: CANCEL (8)
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::RstStream(rst) => {
                assert_eq!(rst.stream_id.value(), 1);
                assert_eq!(rst.error_code, 0x08);
            }
            _ => panic!("Expected RST_STREAM frame"),
        }
    }

    #[test]
    fn test_decode_rst_stream_on_stream_zero() {
        let mut buf = BytesMut::new();

        // RST_STREAM frame on stream 0 (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x04, // Length: 4
            0x03, // Type: RST_STREAM
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0 (invalid)
            0x00, 0x00, 0x00, 0x08,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::StreamIdRequired { frame_type: 0x03 }
        ));
    }

    #[test]
    fn test_decode_rst_stream_wrong_size() {
        let mut buf = BytesMut::new();

        // RST_STREAM frame with wrong payload size
        buf.extend_from_slice(&[
            0x00, 0x00, 0x05, // Length: 5 (should be 4)
            0x03, // Type: RST_STREAM
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x00, 0x00, 0x00, 0x08, 0x00,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidPayloadLength {
                frame_type: 0x03,
                expected: 4,
                actual: 5
            }
        ));
    }

    // GOAWAY frame tests

    #[test]
    fn test_decode_goaway() {
        let mut buf = BytesMut::new();

        // GOAWAY frame
        buf.extend_from_slice(&[
            0x00, 0x00, 0x08, // Length: 8
            0x07, // Type: GOAWAY
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x00, 0x00, 0x00, 0x01, // Last stream ID: 1
            0x00, 0x00, 0x00, 0x00, // Error code: NO_ERROR
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::GoAway(goaway) => {
                assert_eq!(goaway.last_stream_id.value(), 1);
                assert_eq!(goaway.error_code, 0);
                assert!(goaway.debug_data.is_empty());
            }
            _ => panic!("Expected GOAWAY frame"),
        }
    }

    #[test]
    fn test_decode_goaway_with_debug_data() {
        let mut buf = BytesMut::new();

        // GOAWAY frame with debug data
        buf.extend_from_slice(&[
            0x00, 0x00, 0x0d, // Length: 13
            0x07, // Type: GOAWAY
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x00, 0x00, 0x00, 0x01, // Last stream ID: 1
            0x00, 0x00, 0x00, 0x02, // Error code: INTERNAL_ERROR
            b'e', b'r', b'r', b'o', b'r', // Debug data
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::GoAway(goaway) => {
                assert_eq!(goaway.error_code, 2);
                assert_eq!(&goaway.debug_data[..], b"error");
            }
            _ => panic!("Expected GOAWAY frame"),
        }
    }

    #[test]
    fn test_decode_goaway_on_non_zero_stream() {
        let mut buf = BytesMut::new();

        // GOAWAY frame on stream 1 (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x08, // Length: 8
            0x07, // Type: GOAWAY
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1 (invalid)
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidStreamZero { frame_type: 0x07 }
        ));
    }

    #[test]
    fn test_decode_goaway_too_short() {
        let mut buf = BytesMut::new();

        // GOAWAY frame too short
        buf.extend_from_slice(&[
            0x00, 0x00, 0x06, // Length: 6 (need at least 8)
            0x07, // Type: GOAWAY
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidPayloadLength {
                frame_type: 0x07,
                expected: 8,
                actual: 6
            }
        ));
    }

    // PUSH_PROMISE frame tests

    #[test]
    fn test_decode_push_promise() {
        let mut buf = BytesMut::new();

        // PUSH_PROMISE frame
        buf.extend_from_slice(&[
            0x00, 0x00, 0x07, // Length: 7
            0x05, // Type: PUSH_PROMISE
            0x04, // Flags: END_HEADERS
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x00, 0x00, 0x00, 0x02, // Promised stream ID: 2
            0x82, 0x86, 0x84, // Header block
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::PushPromise(pp) => {
                assert_eq!(pp.stream_id.value(), 1);
                assert_eq!(pp.promised_stream_id.value(), 2);
                assert!(pp.end_headers);
                assert_eq!(pp.header_block.len(), 3);
            }
            _ => panic!("Expected PUSH_PROMISE frame"),
        }
    }

    #[test]
    fn test_decode_push_promise_on_stream_zero() {
        let mut buf = BytesMut::new();

        // PUSH_PROMISE frame on stream 0 (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x07, // Length: 7
            0x05, // Type: PUSH_PROMISE
            0x04, // Flags: END_HEADERS
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0 (invalid)
            0x00, 0x00, 0x00, 0x02, 0x82, 0x86, 0x84,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::StreamIdRequired { frame_type: 0x05 }
        ));
    }

    #[test]
    fn test_decode_push_promise_too_short() {
        let mut buf = BytesMut::new();

        // PUSH_PROMISE frame too short (need at least 4 bytes for promised stream ID)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x03, // Length: 3 (need at least 4)
            0x05, // Type: PUSH_PROMISE
            0x04, // Flags: END_HEADERS
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x00, 0x00, 0x02, // Not enough bytes
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::InvalidPayloadLength {
                frame_type: 0x05,
                expected: 4,
                ..
            }
        ));
    }

    #[test]
    fn test_decode_push_promise_padded() {
        let mut buf = BytesMut::new();

        // PUSH_PROMISE frame with padding
        buf.extend_from_slice(&[
            0x00, 0x00, 0x0b, // Length: 11
            0x05, // Type: PUSH_PROMISE
            0x0c, // Flags: PADDED | END_HEADERS
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x03, // Pad length: 3
            0x00, 0x00, 0x00, 0x02, // Promised stream ID: 2
            0x82, 0x86, 0x84, // Header block
            0x00, 0x00, 0x00, // Padding
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::PushPromise(pp) => {
                assert_eq!(pp.promised_stream_id.value(), 2);
            }
            _ => panic!("Expected PUSH_PROMISE frame"),
        }
    }

    // CONTINUATION frame tests

    #[test]
    fn test_decode_continuation() {
        let mut buf = BytesMut::new();

        // CONTINUATION frame
        buf.extend_from_slice(&[
            0x00, 0x00, 0x03, // Length: 3
            0x09, // Type: CONTINUATION
            0x04, // Flags: END_HEADERS
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x82, 0x86, 0x84, // Header block
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Continuation(cont) => {
                assert_eq!(cont.stream_id.value(), 1);
                assert!(cont.end_headers);
                assert_eq!(cont.header_block.len(), 3);
            }
            _ => panic!("Expected CONTINUATION frame"),
        }
    }

    #[test]
    fn test_decode_continuation_no_end_headers() {
        let mut buf = BytesMut::new();

        // CONTINUATION frame without END_HEADERS
        buf.extend_from_slice(&[
            0x00, 0x00, 0x03, // Length: 3
            0x09, // Type: CONTINUATION
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x82, 0x86, 0x84,
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Continuation(cont) => {
                assert!(!cont.end_headers);
            }
            _ => panic!("Expected CONTINUATION frame"),
        }
    }

    #[test]
    fn test_decode_continuation_on_stream_zero() {
        let mut buf = BytesMut::new();

        // CONTINUATION frame on stream 0 (invalid)
        buf.extend_from_slice(&[
            0x00, 0x00, 0x03, // Length: 3
            0x09, // Type: CONTINUATION
            0x04, // Flags: END_HEADERS
            0x00, 0x00, 0x00, 0x00, // Stream ID: 0 (invalid)
            0x82, 0x86, 0x84,
        ]);

        let decoder = FrameDecoder::new();
        let err = decoder.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::StreamIdRequired { frame_type: 0x09 }
        ));
    }

    // Unknown frame type tests

    #[test]
    fn test_decode_unknown_frame() {
        let mut buf = BytesMut::new();

        // Unknown frame type 0xFF
        buf.extend_from_slice(&[
            0x00, 0x00, 0x05, // Length: 5
            0xff, // Type: Unknown
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            b'h', b'e', b'l', b'l', b'o',
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::Unknown(unknown) => {
                assert_eq!(unknown.frame_type, 0xff);
                assert_eq!(unknown.flags, 0x00);
                assert_eq!(unknown.stream_id.value(), 1);
                assert_eq!(&unknown.payload[..], b"hello");
            }
            _ => panic!("Expected Unknown frame"),
        }
    }

    // Multiple frames test

    #[test]
    fn test_decode_multiple_frames() {
        let mut buf = BytesMut::new();

        // Two PING frames
        buf.extend_from_slice(&[
            // First PING
            0x00, 0x00, 0x08, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05,
            0x06, 0x07, 0x08, // Second PING
            0x00, 0x00, 0x08, 0x06, 0x01, // ACK
            0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        ]);

        let decoder = FrameDecoder::new();

        let frame1 = decoder.decode(&mut buf).unwrap().unwrap();
        assert!(matches!(frame1, Frame::Ping(p) if !p.ack));

        let frame2 = decoder.decode(&mut buf).unwrap().unwrap();
        assert!(matches!(frame2, Frame::Ping(p) if p.ack));

        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_window_update_masks_reserved_bit() {
        let mut buf = BytesMut::new();

        // WINDOW_UPDATE with reserved bit set
        buf.extend_from_slice(&[
            0x00, 0x00, 0x04, // Length: 4
            0x08, // Type: WINDOW_UPDATE
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x80, 0x01, 0x00, 0x00, // Increment with reserved bit set
        ]);

        let decoder = FrameDecoder::new();
        let frame = decoder.decode(&mut buf).unwrap().unwrap();

        match frame {
            Frame::WindowUpdate(wu) => {
                // Reserved bit should be masked off
                assert_eq!(wu.increment, 65536);
            }
            _ => panic!("Expected WINDOW_UPDATE frame"),
        }
    }
}
