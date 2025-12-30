//! HTTP/2 frame encoding.

use bytes::{BufMut, BytesMut};

use super::types::*;
use super::{FRAME_HEADER_SIZE, flags};

/// Frame encoder that writes HTTP/2 frames to a byte buffer.
pub struct FrameEncoder {
    max_frame_size: u32,
}

impl Default for FrameEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameEncoder {
    /// Create a new frame encoder with default settings.
    pub fn new() -> Self {
        Self {
            max_frame_size: super::DEFAULT_MAX_FRAME_SIZE,
        }
    }

    /// Set the maximum frame size.
    pub fn set_max_frame_size(&mut self, size: u32) {
        self.max_frame_size = size;
    }

    /// Get the maximum frame size.
    pub fn max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    /// Encode a frame to the buffer.
    pub fn encode(&self, frame: &Frame, buf: &mut BytesMut) {
        match frame {
            Frame::Data(f) => self.encode_data(f, buf),
            Frame::Headers(f) => self.encode_headers(f, buf),
            Frame::Priority(f) => self.encode_priority(f, buf),
            Frame::RstStream(f) => self.encode_rst_stream(f, buf),
            Frame::Settings(f) => self.encode_settings(f, buf),
            Frame::PushPromise(f) => self.encode_push_promise(f, buf),
            Frame::Ping(f) => self.encode_ping(f, buf),
            Frame::GoAway(f) => self.encode_goaway(f, buf),
            Frame::WindowUpdate(f) => self.encode_window_update(f, buf),
            Frame::Continuation(f) => self.encode_continuation(f, buf),
            Frame::Unknown(f) => self.encode_unknown(f, buf),
        }
    }

    /// Write a frame header to the buffer.
    #[inline]
    fn write_header(
        &self,
        buf: &mut BytesMut,
        length: u32,
        frame_type: FrameType,
        flags: u8,
        stream_id: StreamId,
    ) {
        // Length (24 bits, big-endian)
        buf.put_u8((length >> 16) as u8);
        buf.put_u8((length >> 8) as u8);
        buf.put_u8(length as u8);

        // Type
        buf.put_u8(frame_type as u8);

        // Flags
        buf.put_u8(flags);

        // Stream ID (31 bits, big-endian, high bit reserved)
        buf.put_u32(stream_id.value() & 0x7FFF_FFFF);
    }

    /// Encode a DATA frame.
    fn encode_data(&self, frame: &DataFrame, buf: &mut BytesMut) {
        let mut frame_flags = 0u8;
        if frame.end_stream {
            frame_flags |= flags::END_STREAM;
        }

        let length = frame.data.len() as u32;
        buf.reserve(FRAME_HEADER_SIZE + length as usize);

        self.write_header(buf, length, FrameType::Data, frame_flags, frame.stream_id);
        buf.extend_from_slice(&frame.data);
    }

    /// Encode a HEADERS frame.
    fn encode_headers(&self, frame: &HeadersFrame, buf: &mut BytesMut) {
        let mut frame_flags = 0u8;
        if frame.end_stream {
            frame_flags |= flags::END_STREAM;
        }
        if frame.end_headers {
            frame_flags |= flags::END_HEADERS;
        }
        if frame.priority.is_some() {
            frame_flags |= flags::PRIORITY;
        }

        let priority_len = if frame.priority.is_some() { 5 } else { 0 };
        let length = priority_len + frame.header_block.len() as u32;

        buf.reserve(FRAME_HEADER_SIZE + length as usize);

        self.write_header(
            buf,
            length,
            FrameType::Headers,
            frame_flags,
            frame.stream_id,
        );

        if let Some(priority) = &frame.priority {
            let mut dep = priority.dependency.value();
            if priority.exclusive {
                dep |= 0x8000_0000;
            }
            buf.put_u32(dep);
            buf.put_u8(priority.weight);
        }

        buf.extend_from_slice(&frame.header_block);
    }

    /// Encode a PRIORITY frame.
    fn encode_priority(&self, frame: &PriorityFrame, buf: &mut BytesMut) {
        buf.reserve(FRAME_HEADER_SIZE + 5);

        self.write_header(buf, 5, FrameType::Priority, 0, frame.stream_id);

        let mut dep = frame.priority.dependency.value();
        if frame.priority.exclusive {
            dep |= 0x8000_0000;
        }
        buf.put_u32(dep);
        buf.put_u8(frame.priority.weight);
    }

    /// Encode a RST_STREAM frame.
    fn encode_rst_stream(&self, frame: &RstStreamFrame, buf: &mut BytesMut) {
        buf.reserve(FRAME_HEADER_SIZE + 4);

        self.write_header(buf, 4, FrameType::RstStream, 0, frame.stream_id);
        buf.put_u32(frame.error_code);
    }

    /// Encode a SETTINGS frame.
    fn encode_settings(&self, frame: &SettingsFrame, buf: &mut BytesMut) {
        let frame_flags = if frame.ack { flags::ACK } else { 0 };
        let length = if frame.ack {
            0
        } else {
            (frame.settings.len() * 6) as u32
        };

        buf.reserve(FRAME_HEADER_SIZE + length as usize);

        self.write_header(
            buf,
            length,
            FrameType::Settings,
            frame_flags,
            StreamId::CONNECTION,
        );

        if !frame.ack {
            for setting in &frame.settings {
                buf.put_u16(setting.id.to_u16());
                buf.put_u32(setting.value);
            }
        }
    }

    /// Encode a PUSH_PROMISE frame.
    fn encode_push_promise(&self, frame: &PushPromiseFrame, buf: &mut BytesMut) {
        let mut frame_flags = 0u8;
        if frame.end_headers {
            frame_flags |= flags::END_HEADERS;
        }

        let length = 4 + frame.header_block.len() as u32;

        buf.reserve(FRAME_HEADER_SIZE + length as usize);

        self.write_header(
            buf,
            length,
            FrameType::PushPromise,
            frame_flags,
            frame.stream_id,
        );

        buf.put_u32(frame.promised_stream_id.value() & 0x7FFF_FFFF);
        buf.extend_from_slice(&frame.header_block);
    }

    /// Encode a PING frame.
    fn encode_ping(&self, frame: &PingFrame, buf: &mut BytesMut) {
        let frame_flags = if frame.ack { flags::ACK } else { 0 };

        buf.reserve(FRAME_HEADER_SIZE + 8);

        self.write_header(buf, 8, FrameType::Ping, frame_flags, StreamId::CONNECTION);
        buf.extend_from_slice(&frame.data);
    }

    /// Encode a GOAWAY frame.
    fn encode_goaway(&self, frame: &GoAwayFrame, buf: &mut BytesMut) {
        let length = 8 + frame.debug_data.len() as u32;

        buf.reserve(FRAME_HEADER_SIZE + length as usize);

        self.write_header(buf, length, FrameType::GoAway, 0, StreamId::CONNECTION);

        buf.put_u32(frame.last_stream_id.value() & 0x7FFF_FFFF);
        buf.put_u32(frame.error_code);
        buf.extend_from_slice(&frame.debug_data);
    }

    /// Encode a WINDOW_UPDATE frame.
    fn encode_window_update(&self, frame: &WindowUpdateFrame, buf: &mut BytesMut) {
        buf.reserve(FRAME_HEADER_SIZE + 4);

        self.write_header(buf, 4, FrameType::WindowUpdate, 0, frame.stream_id);
        buf.put_u32(frame.increment & 0x7FFF_FFFF);
    }

    /// Encode a CONTINUATION frame.
    fn encode_continuation(&self, frame: &ContinuationFrame, buf: &mut BytesMut) {
        let mut frame_flags = 0u8;
        if frame.end_headers {
            frame_flags |= flags::END_HEADERS;
        }

        let length = frame.header_block.len() as u32;

        buf.reserve(FRAME_HEADER_SIZE + length as usize);

        self.write_header(
            buf,
            length,
            FrameType::Continuation,
            frame_flags,
            frame.stream_id,
        );

        buf.extend_from_slice(&frame.header_block);
    }

    /// Encode an unknown frame.
    fn encode_unknown(&self, frame: &UnknownFrame, buf: &mut BytesMut) {
        let length = frame.payload.len() as u32;

        buf.reserve(FRAME_HEADER_SIZE + length as usize);

        // Write header manually for unknown type
        buf.put_u8((length >> 16) as u8);
        buf.put_u8((length >> 8) as u8);
        buf.put_u8(length as u8);
        buf.put_u8(frame.frame_type);
        buf.put_u8(frame.flags);
        buf.put_u32(frame.stream_id.value() & 0x7FFF_FFFF);

        buf.extend_from_slice(&frame.payload);
    }
}

/// Helper functions for encoding specific frames directly.
impl FrameEncoder {
    /// Encode the client connection preface to the buffer.
    pub fn encode_connection_preface(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(super::CONNECTION_PREFACE);
    }

    /// Encode a SETTINGS frame with common client settings.
    pub fn encode_client_settings(&self, buf: &mut BytesMut, settings: &[Setting]) {
        let frame = SettingsFrame {
            ack: false,
            settings: settings.to_vec(),
        };
        self.encode(&Frame::Settings(frame), buf);
    }

    /// Encode a SETTINGS ACK frame.
    pub fn encode_settings_ack(&self, buf: &mut BytesMut) {
        let frame = SettingsFrame {
            ack: true,
            settings: Vec::new(),
        };
        self.encode(&Frame::Settings(frame), buf);
    }

    /// Encode a PING response (ACK).
    pub fn encode_ping_ack(&self, data: [u8; 8], buf: &mut BytesMut) {
        let frame = PingFrame { ack: true, data };
        self.encode(&Frame::Ping(frame), buf);
    }

    /// Encode a WINDOW_UPDATE frame directly.
    pub fn write_window_update(&self, stream_id: StreamId, increment: u32, buf: &mut BytesMut) {
        let frame = WindowUpdateFrame {
            stream_id,
            increment,
        };
        self.encode(&Frame::WindowUpdate(frame), buf);
    }

    /// Encode a RST_STREAM frame directly.
    pub fn write_rst_stream(&self, stream_id: StreamId, error_code: u32, buf: &mut BytesMut) {
        let frame = RstStreamFrame {
            stream_id,
            error_code,
        };
        self.encode(&Frame::RstStream(frame), buf);
    }

    /// Encode a GOAWAY frame directly.
    pub fn write_goaway(
        &self,
        last_stream_id: StreamId,
        error_code: u32,
        debug_data: &[u8],
        buf: &mut BytesMut,
    ) {
        let frame = GoAwayFrame {
            last_stream_id,
            error_code,
            debug_data: bytes::Bytes::copy_from_slice(debug_data),
        };
        self.encode(&Frame::GoAway(frame), buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::decode::FrameDecoder;
    use bytes::Buf;

    // FrameEncoder basic tests

    #[test]
    fn test_encoder_default() {
        let encoder = FrameEncoder::default();
        assert_eq!(
            encoder.max_frame_size(),
            super::super::DEFAULT_MAX_FRAME_SIZE
        );
    }

    #[test]
    fn test_encoder_new() {
        let encoder = FrameEncoder::new();
        assert_eq!(
            encoder.max_frame_size(),
            super::super::DEFAULT_MAX_FRAME_SIZE
        );
    }

    #[test]
    fn test_encoder_set_max_frame_size() {
        let mut encoder = FrameEncoder::new();
        encoder.set_max_frame_size(32768);
        assert_eq!(encoder.max_frame_size(), 32768);
    }

    // Roundtrip tests

    #[test]
    fn test_roundtrip_settings() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = SettingsFrame {
            ack: false,
            settings: vec![
                Setting {
                    id: SettingId::HeaderTableSize,
                    value: 8192,
                },
                Setting {
                    id: SettingId::MaxConcurrentStreams,
                    value: 100,
                },
            ],
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Settings(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Settings(settings) => {
                assert_eq!(settings.ack, original.ack);
                assert_eq!(settings.settings.len(), original.settings.len());
                for (a, b) in settings.settings.iter().zip(original.settings.iter()) {
                    assert_eq!(a.id, b.id);
                    assert_eq!(a.value, b.value);
                }
            }
            _ => panic!("Expected SETTINGS frame"),
        }
    }

    #[test]
    fn test_roundtrip_settings_ack() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = SettingsFrame {
            ack: true,
            settings: vec![],
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Settings(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Settings(settings) => {
                assert!(settings.ack);
                assert!(settings.settings.is_empty());
            }
            _ => panic!("Expected SETTINGS frame"),
        }
    }

    #[test]
    fn test_roundtrip_ping() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = PingFrame {
            ack: false,
            data: [1, 2, 3, 4, 5, 6, 7, 8],
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Ping(original), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Ping(ping) => {
                assert_eq!(ping.ack, original.ack);
                assert_eq!(ping.data, original.data);
            }
            _ => panic!("Expected PING frame"),
        }
    }

    #[test]
    fn test_roundtrip_ping_ack() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = PingFrame {
            ack: true,
            data: [8, 7, 6, 5, 4, 3, 2, 1],
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Ping(original), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Ping(ping) => {
                assert!(ping.ack);
                assert_eq!(ping.data, [8, 7, 6, 5, 4, 3, 2, 1]);
            }
            _ => panic!("Expected PING frame"),
        }
    }

    #[test]
    fn test_roundtrip_data() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = DataFrame {
            stream_id: StreamId::new(1),
            end_stream: true,
            data: bytes::Bytes::from_static(b"hello world"),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Data(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Data(data) => {
                assert_eq!(data.stream_id, original.stream_id);
                assert_eq!(data.end_stream, original.end_stream);
                assert_eq!(data.data, original.data);
            }
            _ => panic!("Expected DATA frame"),
        }
    }

    #[test]
    fn test_roundtrip_data_no_end_stream() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = DataFrame {
            stream_id: StreamId::new(3),
            end_stream: false,
            data: bytes::Bytes::from_static(b"partial data"),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Data(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Data(data) => {
                assert!(!data.end_stream);
            }
            _ => panic!("Expected DATA frame"),
        }
    }

    #[test]
    fn test_roundtrip_headers() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = HeadersFrame {
            stream_id: StreamId::new(1),
            end_stream: false,
            end_headers: true,
            priority: Some(Priority {
                exclusive: true,
                dependency: StreamId::new(0),
                weight: 255,
            }),
            header_block: bytes::Bytes::from_static(&[0x82, 0x86, 0x84]),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Headers(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Headers(headers) => {
                assert_eq!(headers.stream_id, original.stream_id);
                assert_eq!(headers.end_stream, original.end_stream);
                assert_eq!(headers.end_headers, original.end_headers);
                assert!(headers.priority.is_some());
                let p = headers.priority.unwrap();
                let op = original.priority.unwrap();
                assert_eq!(p.exclusive, op.exclusive);
                assert_eq!(p.dependency, op.dependency);
                assert_eq!(p.weight, op.weight);
                assert_eq!(headers.header_block, original.header_block);
            }
            _ => panic!("Expected HEADERS frame"),
        }
    }

    #[test]
    fn test_roundtrip_headers_no_priority() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = HeadersFrame {
            stream_id: StreamId::new(1),
            end_stream: true,
            end_headers: true,
            priority: None,
            header_block: bytes::Bytes::from_static(&[0x82, 0x86, 0x84]),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Headers(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Headers(headers) => {
                assert!(headers.priority.is_none());
                assert!(headers.end_stream);
            }
            _ => panic!("Expected HEADERS frame"),
        }
    }

    #[test]
    fn test_roundtrip_headers_non_exclusive_priority() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = HeadersFrame {
            stream_id: StreamId::new(1),
            end_stream: false,
            end_headers: true,
            priority: Some(Priority {
                exclusive: false,
                dependency: StreamId::new(3),
                weight: 16,
            }),
            header_block: bytes::Bytes::from_static(&[0x82]),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Headers(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Headers(headers) => {
                let p = headers.priority.unwrap();
                assert!(!p.exclusive);
                assert_eq!(p.dependency.value(), 3);
                assert_eq!(p.weight, 16);
            }
            _ => panic!("Expected HEADERS frame"),
        }
    }

    #[test]
    fn test_roundtrip_priority() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = PriorityFrame {
            stream_id: StreamId::new(5),
            priority: Priority {
                exclusive: true,
                dependency: StreamId::new(3),
                weight: 128,
            },
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Priority(original), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Priority(priority) => {
                assert_eq!(priority.stream_id, original.stream_id);
                assert_eq!(priority.priority.exclusive, original.priority.exclusive);
                assert_eq!(priority.priority.dependency, original.priority.dependency);
                assert_eq!(priority.priority.weight, original.priority.weight);
            }
            _ => panic!("Expected PRIORITY frame"),
        }
    }

    #[test]
    fn test_roundtrip_priority_non_exclusive() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = PriorityFrame {
            stream_id: StreamId::new(7),
            priority: Priority {
                exclusive: false,
                dependency: StreamId::new(1),
                weight: 64,
            },
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Priority(original), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Priority(priority) => {
                assert!(!priority.priority.exclusive);
            }
            _ => panic!("Expected PRIORITY frame"),
        }
    }

    #[test]
    fn test_roundtrip_rst_stream() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = RstStreamFrame {
            stream_id: StreamId::new(3),
            error_code: 8, // CANCEL
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::RstStream(original), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::RstStream(rst) => {
                assert_eq!(rst.stream_id, original.stream_id);
                assert_eq!(rst.error_code, original.error_code);
            }
            _ => panic!("Expected RST_STREAM frame"),
        }
    }

    #[test]
    fn test_roundtrip_push_promise() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = PushPromiseFrame {
            stream_id: StreamId::new(1),
            end_headers: true,
            promised_stream_id: StreamId::new(2),
            header_block: bytes::Bytes::from_static(&[0x82, 0x86, 0x84]),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::PushPromise(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::PushPromise(pp) => {
                assert_eq!(pp.stream_id, original.stream_id);
                assert_eq!(pp.end_headers, original.end_headers);
                assert_eq!(pp.promised_stream_id, original.promised_stream_id);
                assert_eq!(pp.header_block, original.header_block);
            }
            _ => panic!("Expected PUSH_PROMISE frame"),
        }
    }

    #[test]
    fn test_roundtrip_push_promise_no_end_headers() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = PushPromiseFrame {
            stream_id: StreamId::new(1),
            end_headers: false,
            promised_stream_id: StreamId::new(4),
            header_block: bytes::Bytes::from_static(&[0x82]),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::PushPromise(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::PushPromise(pp) => {
                assert!(!pp.end_headers);
            }
            _ => panic!("Expected PUSH_PROMISE frame"),
        }
    }

    #[test]
    fn test_roundtrip_window_update() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = WindowUpdateFrame {
            stream_id: StreamId::new(5),
            increment: 65535,
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::WindowUpdate(original), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::WindowUpdate(wu) => {
                assert_eq!(wu.stream_id, original.stream_id);
                assert_eq!(wu.increment, original.increment);
            }
            _ => panic!("Expected WINDOW_UPDATE frame"),
        }
    }

    #[test]
    fn test_roundtrip_window_update_connection_level() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = WindowUpdateFrame {
            stream_id: StreamId::CONNECTION,
            increment: 1048576,
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::WindowUpdate(original), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::WindowUpdate(wu) => {
                assert!(wu.stream_id.is_connection_level());
            }
            _ => panic!("Expected WINDOW_UPDATE frame"),
        }
    }

    #[test]
    fn test_roundtrip_goaway() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = GoAwayFrame {
            last_stream_id: StreamId::new(7),
            error_code: 0,
            debug_data: bytes::Bytes::from_static(b"goodbye"),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::GoAway(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::GoAway(ga) => {
                assert_eq!(ga.last_stream_id, original.last_stream_id);
                assert_eq!(ga.error_code, original.error_code);
                assert_eq!(ga.debug_data, original.debug_data);
            }
            _ => panic!("Expected GOAWAY frame"),
        }
    }

    #[test]
    fn test_roundtrip_goaway_no_debug_data() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = GoAwayFrame {
            last_stream_id: StreamId::new(0),
            error_code: 2, // INTERNAL_ERROR
            debug_data: bytes::Bytes::new(),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::GoAway(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::GoAway(ga) => {
                assert!(ga.debug_data.is_empty());
                assert_eq!(ga.error_code, 2);
            }
            _ => panic!("Expected GOAWAY frame"),
        }
    }

    #[test]
    fn test_roundtrip_continuation() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = ContinuationFrame {
            stream_id: StreamId::new(1),
            end_headers: true,
            header_block: bytes::Bytes::from_static(&[0x82, 0x86, 0x84]),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Continuation(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Continuation(cont) => {
                assert_eq!(cont.stream_id, original.stream_id);
                assert_eq!(cont.end_headers, original.end_headers);
                assert_eq!(cont.header_block, original.header_block);
            }
            _ => panic!("Expected CONTINUATION frame"),
        }
    }

    #[test]
    fn test_roundtrip_continuation_no_end_headers() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = ContinuationFrame {
            stream_id: StreamId::new(3),
            end_headers: false,
            header_block: bytes::Bytes::from_static(&[0x82]),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Continuation(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Continuation(cont) => {
                assert!(!cont.end_headers);
            }
            _ => panic!("Expected CONTINUATION frame"),
        }
    }

    #[test]
    fn test_roundtrip_unknown() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = UnknownFrame {
            frame_type: 0xff,
            flags: 0x05,
            stream_id: StreamId::new(9),
            payload: bytes::Bytes::from_static(b"unknown data"),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Unknown(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Unknown(unknown) => {
                assert_eq!(unknown.frame_type, original.frame_type);
                assert_eq!(unknown.flags, original.flags);
                assert_eq!(unknown.stream_id, original.stream_id);
                assert_eq!(unknown.payload, original.payload);
            }
            _ => panic!("Expected Unknown frame"),
        }
    }

    // Helper function tests

    #[test]
    fn test_encode_connection_preface() {
        let encoder = FrameEncoder::new();
        let mut buf = BytesMut::new();
        encoder.encode_connection_preface(&mut buf);

        assert_eq!(&buf[..], super::super::CONNECTION_PREFACE);
    }

    #[test]
    fn test_encode_client_settings() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();
        let mut buf = BytesMut::new();

        let settings = vec![
            Setting {
                id: SettingId::MaxConcurrentStreams,
                value: 100,
            },
            Setting {
                id: SettingId::InitialWindowSize,
                value: 65535,
            },
        ];

        encoder.encode_client_settings(&mut buf, &settings);

        let frame = decoder.decode(&mut buf).unwrap().unwrap();
        match frame {
            Frame::Settings(s) => {
                assert!(!s.ack);
                assert_eq!(s.settings.len(), 2);
            }
            _ => panic!("Expected SETTINGS frame"),
        }
    }

    #[test]
    fn test_encode_settings_ack() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();
        let mut buf = BytesMut::new();

        encoder.encode_settings_ack(&mut buf);

        let frame = decoder.decode(&mut buf).unwrap().unwrap();
        match frame {
            Frame::Settings(s) => {
                assert!(s.ack);
                assert!(s.settings.is_empty());
            }
            _ => panic!("Expected SETTINGS frame"),
        }
    }

    #[test]
    fn test_encode_ping_ack() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();
        let mut buf = BytesMut::new();

        let data = [1, 2, 3, 4, 5, 6, 7, 8];
        encoder.encode_ping_ack(data, &mut buf);

        let frame = decoder.decode(&mut buf).unwrap().unwrap();
        match frame {
            Frame::Ping(p) => {
                assert!(p.ack);
                assert_eq!(p.data, data);
            }
            _ => panic!("Expected PING frame"),
        }
    }

    #[test]
    fn test_write_window_update() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();
        let mut buf = BytesMut::new();

        encoder.write_window_update(StreamId::new(5), 32768, &mut buf);

        let frame = decoder.decode(&mut buf).unwrap().unwrap();
        match frame {
            Frame::WindowUpdate(wu) => {
                assert_eq!(wu.stream_id.value(), 5);
                assert_eq!(wu.increment, 32768);
            }
            _ => panic!("Expected WINDOW_UPDATE frame"),
        }
    }

    #[test]
    fn test_write_rst_stream() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();
        let mut buf = BytesMut::new();

        encoder.write_rst_stream(StreamId::new(7), 8, &mut buf);

        let frame = decoder.decode(&mut buf).unwrap().unwrap();
        match frame {
            Frame::RstStream(rst) => {
                assert_eq!(rst.stream_id.value(), 7);
                assert_eq!(rst.error_code, 8);
            }
            _ => panic!("Expected RST_STREAM frame"),
        }
    }

    #[test]
    fn test_write_goaway() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();
        let mut buf = BytesMut::new();

        encoder.write_goaway(StreamId::new(11), 0, b"test debug", &mut buf);

        let frame = decoder.decode(&mut buf).unwrap().unwrap();
        match frame {
            Frame::GoAway(ga) => {
                assert_eq!(ga.last_stream_id.value(), 11);
                assert_eq!(ga.error_code, 0);
                assert_eq!(&ga.debug_data[..], b"test debug");
            }
            _ => panic!("Expected GOAWAY frame"),
        }
    }

    #[test]
    fn test_write_goaway_empty_debug() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();
        let mut buf = BytesMut::new();

        encoder.write_goaway(StreamId::new(0), 2, b"", &mut buf);

        let frame = decoder.decode(&mut buf).unwrap().unwrap();
        match frame {
            Frame::GoAway(ga) => {
                assert_eq!(ga.last_stream_id.value(), 0);
                assert_eq!(ga.error_code, 2);
                assert!(ga.debug_data.is_empty());
            }
            _ => panic!("Expected GOAWAY frame"),
        }
    }

    // Test encoding masks reserved bit in stream ID

    #[test]
    fn test_encode_window_update_masks_reserved_bit() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        // Even if we try to set the reserved bit, it should be masked off
        let original = WindowUpdateFrame {
            stream_id: StreamId::new(0x80000005), // Reserved bit set
            increment: 0x80001000,                // Reserved bit set
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::WindowUpdate(original), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::WindowUpdate(wu) => {
                // Reserved bits should be masked off
                assert_eq!(wu.stream_id.value(), 5);
                assert_eq!(wu.increment, 0x1000);
            }
            _ => panic!("Expected WINDOW_UPDATE frame"),
        }
    }

    // Test encoding empty data

    #[test]
    fn test_encode_empty_data() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();

        let original = DataFrame {
            stream_id: StreamId::new(1),
            end_stream: true,
            data: bytes::Bytes::new(),
        };

        let mut buf = BytesMut::new();
        encoder.encode(&Frame::Data(original.clone()), &mut buf);

        let decoded = decoder.decode(&mut buf).unwrap().unwrap();

        match decoded {
            Frame::Data(data) => {
                assert!(data.data.is_empty());
            }
            _ => panic!("Expected DATA frame"),
        }
    }

    #[test]
    fn test_encode_multiple_frames() {
        let encoder = FrameEncoder::new();
        let decoder = FrameDecoder::new();
        let mut buf = BytesMut::new();

        // Encode multiple frames
        encoder.encode_connection_preface(&mut buf);

        let settings = SettingsFrame {
            ack: false,
            settings: vec![Setting {
                id: SettingId::MaxConcurrentStreams,
                value: 100,
            }],
        };
        encoder.encode(&Frame::Settings(settings), &mut buf);

        encoder.encode_settings_ack(&mut buf);

        // Preface is raw bytes, not a frame
        assert_eq!(&buf[..24], super::super::CONNECTION_PREFACE);
        buf.advance(24);

        // First frame is SETTINGS
        let frame1 = decoder.decode(&mut buf).unwrap().unwrap();
        assert!(matches!(frame1, Frame::Settings(s) if !s.ack));

        // Second frame is SETTINGS ACK
        let frame2 = decoder.decode(&mut buf).unwrap().unwrap();
        assert!(matches!(frame2, Frame::Settings(s) if s.ack));

        assert!(buf.is_empty());
    }
}
