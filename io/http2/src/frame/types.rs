//! HTTP/2 frame type definitions.

use bytes::Bytes;

/// HTTP/2 frame types (RFC 7540 Section 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    Data = 0x0,
    Headers = 0x1,
    Priority = 0x2,
    RstStream = 0x3,
    Settings = 0x4,
    PushPromise = 0x5,
    Ping = 0x6,
    GoAway = 0x7,
    WindowUpdate = 0x8,
    Continuation = 0x9,
}

impl FrameType {
    /// Try to convert a byte to a frame type.
    pub fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            0x0 => Some(FrameType::Data),
            0x1 => Some(FrameType::Headers),
            0x2 => Some(FrameType::Priority),
            0x3 => Some(FrameType::RstStream),
            0x4 => Some(FrameType::Settings),
            0x5 => Some(FrameType::PushPromise),
            0x6 => Some(FrameType::Ping),
            0x7 => Some(FrameType::GoAway),
            0x8 => Some(FrameType::WindowUpdate),
            0x9 => Some(FrameType::Continuation),
            _ => None,
        }
    }
}

/// Frame flags.
pub mod flags {
    /// DATA frame: indicates this is the last frame.
    pub const END_STREAM: u8 = 0x1;
    /// DATA/HEADERS frame: padding is present.
    pub const PADDED: u8 = 0x8;
    /// HEADERS frame: indicates this is the last header block.
    pub const END_HEADERS: u8 = 0x4;
    /// HEADERS frame: priority information is present.
    pub const PRIORITY: u8 = 0x20;
    /// SETTINGS/PING frame: this is an acknowledgment.
    pub const ACK: u8 = 0x1;
}

/// Stream identifier (31 bits, high bit reserved).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct StreamId(pub u32);

impl StreamId {
    /// Connection-level stream (stream 0).
    pub const CONNECTION: StreamId = StreamId(0);

    /// Create a new stream ID, masking the reserved bit.
    #[inline]
    pub fn new(id: u32) -> Self {
        StreamId(id & 0x7FFF_FFFF)
    }

    /// Get the raw stream ID value.
    #[inline]
    pub fn value(self) -> u32 {
        self.0
    }

    /// Check if this is the connection-level stream.
    #[inline]
    pub fn is_connection_level(self) -> bool {
        self.0 == 0
    }

    /// Check if this is a client-initiated stream (odd numbers).
    #[inline]
    pub fn is_client_initiated(self) -> bool {
        self.0 % 2 == 1
    }

    /// Check if this is a server-initiated stream (even numbers, non-zero).
    #[inline]
    pub fn is_server_initiated(self) -> bool {
        self.0 != 0 && self.0.is_multiple_of(2)
    }
}

impl From<u32> for StreamId {
    fn from(id: u32) -> Self {
        StreamId::new(id)
    }
}

/// Raw frame header.
#[derive(Debug, Clone, Copy)]
pub struct FrameHeader {
    /// Payload length (24 bits).
    pub length: u32,
    /// Frame type.
    pub frame_type: u8,
    /// Frame flags.
    pub flags: u8,
    /// Stream identifier.
    pub stream_id: StreamId,
}

impl FrameHeader {
    /// Create a new frame header.
    pub fn new(frame_type: FrameType, flags: u8, stream_id: StreamId, length: u32) -> Self {
        Self {
            length,
            frame_type: frame_type as u8,
            flags,
            stream_id,
        }
    }

    /// Get the frame type as an enum, if known.
    pub fn get_type(&self) -> Option<FrameType> {
        FrameType::from_u8(self.frame_type)
    }

    /// Check if a flag is set.
    #[inline]
    pub fn has_flag(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }
}

/// Parsed HTTP/2 frame.
#[derive(Debug, Clone)]
pub enum Frame {
    Data(DataFrame),
    Headers(HeadersFrame),
    Priority(PriorityFrame),
    RstStream(RstStreamFrame),
    Settings(SettingsFrame),
    PushPromise(PushPromiseFrame),
    Ping(PingFrame),
    GoAway(GoAwayFrame),
    WindowUpdate(WindowUpdateFrame),
    Continuation(ContinuationFrame),
    /// Unknown frame type (must be ignored per spec).
    Unknown(UnknownFrame),
}

impl Frame {
    /// Get the stream ID for this frame.
    pub fn stream_id(&self) -> StreamId {
        match self {
            Frame::Data(f) => f.stream_id,
            Frame::Headers(f) => f.stream_id,
            Frame::Priority(f) => f.stream_id,
            Frame::RstStream(f) => f.stream_id,
            Frame::Settings(_) => StreamId::CONNECTION,
            Frame::PushPromise(f) => f.stream_id,
            Frame::Ping(_) => StreamId::CONNECTION,
            Frame::GoAway(_) => StreamId::CONNECTION,
            Frame::WindowUpdate(f) => f.stream_id,
            Frame::Continuation(f) => f.stream_id,
            Frame::Unknown(f) => f.stream_id,
        }
    }
}

/// DATA frame (type=0x0).
#[derive(Debug, Clone)]
pub struct DataFrame {
    pub stream_id: StreamId,
    pub end_stream: bool,
    pub data: Bytes,
}

/// HEADERS frame (type=0x1).
#[derive(Debug, Clone)]
pub struct HeadersFrame {
    pub stream_id: StreamId,
    pub end_stream: bool,
    pub end_headers: bool,
    pub priority: Option<Priority>,
    /// HPACK-encoded header block fragment.
    pub header_block: Bytes,
}

/// Stream priority information.
#[derive(Debug, Clone, Copy)]
pub struct Priority {
    /// Whether the dependency is exclusive.
    pub exclusive: bool,
    /// Stream dependency.
    pub dependency: StreamId,
    /// Weight (1-256, stored as 0-255).
    pub weight: u8,
}

/// PRIORITY frame (type=0x2).
#[derive(Debug, Clone, Copy)]
pub struct PriorityFrame {
    pub stream_id: StreamId,
    pub priority: Priority,
}

/// RST_STREAM frame (type=0x3).
#[derive(Debug, Clone, Copy)]
pub struct RstStreamFrame {
    pub stream_id: StreamId,
    pub error_code: u32,
}

/// SETTINGS frame (type=0x4).
#[derive(Debug, Clone)]
pub struct SettingsFrame {
    pub ack: bool,
    pub settings: Vec<Setting>,
}

/// Individual setting in a SETTINGS frame.
#[derive(Debug, Clone, Copy)]
pub struct Setting {
    pub id: SettingId,
    pub value: u32,
}

/// Known setting identifiers (RFC 7540 Section 6.5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum SettingId {
    HeaderTableSize = 0x1,
    EnablePush = 0x2,
    MaxConcurrentStreams = 0x3,
    InitialWindowSize = 0x4,
    MaxFrameSize = 0x5,
    MaxHeaderListSize = 0x6,
    /// Unknown setting ID.
    Unknown(u16),
}

impl SettingId {
    pub fn from_u16(id: u16) -> Self {
        match id {
            0x1 => SettingId::HeaderTableSize,
            0x2 => SettingId::EnablePush,
            0x3 => SettingId::MaxConcurrentStreams,
            0x4 => SettingId::InitialWindowSize,
            0x5 => SettingId::MaxFrameSize,
            0x6 => SettingId::MaxHeaderListSize,
            _ => SettingId::Unknown(id),
        }
    }

    pub fn to_u16(self) -> u16 {
        match self {
            SettingId::HeaderTableSize => 0x1,
            SettingId::EnablePush => 0x2,
            SettingId::MaxConcurrentStreams => 0x3,
            SettingId::InitialWindowSize => 0x4,
            SettingId::MaxFrameSize => 0x5,
            SettingId::MaxHeaderListSize => 0x6,
            SettingId::Unknown(id) => id,
        }
    }
}

/// PUSH_PROMISE frame (type=0x5).
#[derive(Debug, Clone)]
pub struct PushPromiseFrame {
    pub stream_id: StreamId,
    pub end_headers: bool,
    pub promised_stream_id: StreamId,
    /// HPACK-encoded header block fragment.
    pub header_block: Bytes,
}

/// PING frame (type=0x6).
#[derive(Debug, Clone, Copy)]
pub struct PingFrame {
    pub ack: bool,
    pub data: [u8; 8],
}

/// GOAWAY frame (type=0x7).
#[derive(Debug, Clone)]
pub struct GoAwayFrame {
    pub last_stream_id: StreamId,
    pub error_code: u32,
    pub debug_data: Bytes,
}

/// WINDOW_UPDATE frame (type=0x8).
#[derive(Debug, Clone, Copy)]
pub struct WindowUpdateFrame {
    pub stream_id: StreamId,
    pub increment: u32,
}

/// CONTINUATION frame (type=0x9).
#[derive(Debug, Clone)]
pub struct ContinuationFrame {
    pub stream_id: StreamId,
    pub end_headers: bool,
    /// HPACK-encoded header block fragment.
    pub header_block: Bytes,
}

/// Unknown frame type.
#[derive(Debug, Clone)]
pub struct UnknownFrame {
    pub frame_type: u8,
    pub flags: u8,
    pub stream_id: StreamId,
    pub payload: Bytes,
}

#[cfg(test)]
mod tests {
    use super::*;

    // FrameType tests

    #[test]
    fn test_frame_type_from_u8() {
        assert_eq!(FrameType::from_u8(0x0), Some(FrameType::Data));
        assert_eq!(FrameType::from_u8(0x1), Some(FrameType::Headers));
        assert_eq!(FrameType::from_u8(0x2), Some(FrameType::Priority));
        assert_eq!(FrameType::from_u8(0x3), Some(FrameType::RstStream));
        assert_eq!(FrameType::from_u8(0x4), Some(FrameType::Settings));
        assert_eq!(FrameType::from_u8(0x5), Some(FrameType::PushPromise));
        assert_eq!(FrameType::from_u8(0x6), Some(FrameType::Ping));
        assert_eq!(FrameType::from_u8(0x7), Some(FrameType::GoAway));
        assert_eq!(FrameType::from_u8(0x8), Some(FrameType::WindowUpdate));
        assert_eq!(FrameType::from_u8(0x9), Some(FrameType::Continuation));
    }

    #[test]
    fn test_frame_type_from_u8_unknown() {
        assert_eq!(FrameType::from_u8(0xa), None);
        assert_eq!(FrameType::from_u8(0xff), None);
    }

    #[test]
    fn test_frame_type_debug() {
        assert!(format!("{:?}", FrameType::Data).contains("Data"));
        assert!(format!("{:?}", FrameType::Headers).contains("Headers"));
    }

    #[test]
    fn test_frame_type_eq() {
        assert_eq!(FrameType::Data, FrameType::Data);
        assert_ne!(FrameType::Data, FrameType::Headers);
    }

    // StreamId tests

    #[test]
    fn test_stream_id_new() {
        let id = StreamId::new(1);
        assert_eq!(id.value(), 1);
    }

    #[test]
    fn test_stream_id_masks_reserved_bit() {
        // High bit should be masked off
        let id = StreamId::new(0x80000001);
        assert_eq!(id.value(), 1);
    }

    #[test]
    fn test_stream_id_connection_level() {
        assert!(StreamId::CONNECTION.is_connection_level());
        assert!(StreamId::new(0).is_connection_level());
        assert!(!StreamId::new(1).is_connection_level());
    }

    #[test]
    fn test_stream_id_client_initiated() {
        assert!(StreamId::new(1).is_client_initiated());
        assert!(StreamId::new(3).is_client_initiated());
        assert!(StreamId::new(5).is_client_initiated());
        assert!(!StreamId::new(0).is_client_initiated());
        assert!(!StreamId::new(2).is_client_initiated());
    }

    #[test]
    fn test_stream_id_server_initiated() {
        assert!(StreamId::new(2).is_server_initiated());
        assert!(StreamId::new(4).is_server_initiated());
        assert!(StreamId::new(6).is_server_initiated());
        assert!(!StreamId::new(0).is_server_initiated());
        assert!(!StreamId::new(1).is_server_initiated());
    }

    #[test]
    fn test_stream_id_from_u32() {
        let id: StreamId = 42.into();
        assert_eq!(id.value(), 42);
    }

    #[test]
    fn test_stream_id_default() {
        let id = StreamId::default();
        assert_eq!(id.value(), 0);
    }

    #[test]
    fn test_stream_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(StreamId::new(1));
        set.insert(StreamId::new(2));
        assert!(set.contains(&StreamId::new(1)));
        assert!(!set.contains(&StreamId::new(3)));
    }

    // FrameHeader tests

    #[test]
    fn test_frame_header_new() {
        let header = FrameHeader::new(FrameType::Data, flags::END_STREAM, StreamId::new(1), 100);

        assert_eq!(header.frame_type, 0x0);
        assert_eq!(header.flags, flags::END_STREAM);
        assert_eq!(header.stream_id.value(), 1);
        assert_eq!(header.length, 100);
    }

    #[test]
    fn test_frame_header_get_type() {
        let header = FrameHeader::new(FrameType::Headers, 0, StreamId::new(1), 0);
        assert_eq!(header.get_type(), Some(FrameType::Headers));

        // Unknown frame type
        let header = FrameHeader {
            length: 0,
            frame_type: 0xff,
            flags: 0,
            stream_id: StreamId::new(0),
        };
        assert_eq!(header.get_type(), None);
    }

    #[test]
    fn test_frame_header_has_flag() {
        let header = FrameHeader::new(
            FrameType::Headers,
            flags::END_STREAM | flags::END_HEADERS,
            StreamId::new(1),
            0,
        );

        assert!(header.has_flag(flags::END_STREAM));
        assert!(header.has_flag(flags::END_HEADERS));
        assert!(!header.has_flag(flags::PADDED));
    }

    // Frame tests

    #[test]
    fn test_frame_stream_id_data() {
        let frame = Frame::Data(DataFrame {
            stream_id: StreamId::new(5),
            end_stream: false,
            data: Bytes::new(),
        });
        assert_eq!(frame.stream_id().value(), 5);
    }

    #[test]
    fn test_frame_stream_id_headers() {
        let frame = Frame::Headers(HeadersFrame {
            stream_id: StreamId::new(7),
            end_stream: false,
            end_headers: true,
            priority: None,
            header_block: Bytes::new(),
        });
        assert_eq!(frame.stream_id().value(), 7);
    }

    #[test]
    fn test_frame_stream_id_priority() {
        let frame = Frame::Priority(PriorityFrame {
            stream_id: StreamId::new(9),
            priority: Priority {
                exclusive: false,
                dependency: StreamId::new(0),
                weight: 16,
            },
        });
        assert_eq!(frame.stream_id().value(), 9);
    }

    #[test]
    fn test_frame_stream_id_rst_stream() {
        let frame = Frame::RstStream(RstStreamFrame {
            stream_id: StreamId::new(11),
            error_code: 0,
        });
        assert_eq!(frame.stream_id().value(), 11);
    }

    #[test]
    fn test_frame_stream_id_settings() {
        let frame = Frame::Settings(SettingsFrame {
            ack: false,
            settings: vec![],
        });
        assert_eq!(frame.stream_id().value(), 0);
    }

    #[test]
    fn test_frame_stream_id_push_promise() {
        let frame = Frame::PushPromise(PushPromiseFrame {
            stream_id: StreamId::new(13),
            end_headers: true,
            promised_stream_id: StreamId::new(2),
            header_block: Bytes::new(),
        });
        assert_eq!(frame.stream_id().value(), 13);
    }

    #[test]
    fn test_frame_stream_id_ping() {
        let frame = Frame::Ping(PingFrame {
            ack: false,
            data: [0; 8],
        });
        assert_eq!(frame.stream_id().value(), 0);
    }

    #[test]
    fn test_frame_stream_id_goaway() {
        let frame = Frame::GoAway(GoAwayFrame {
            last_stream_id: StreamId::new(10),
            error_code: 0,
            debug_data: Bytes::new(),
        });
        assert_eq!(frame.stream_id().value(), 0);
    }

    #[test]
    fn test_frame_stream_id_window_update() {
        let frame = Frame::WindowUpdate(WindowUpdateFrame {
            stream_id: StreamId::new(15),
            increment: 1000,
        });
        assert_eq!(frame.stream_id().value(), 15);
    }

    #[test]
    fn test_frame_stream_id_continuation() {
        let frame = Frame::Continuation(ContinuationFrame {
            stream_id: StreamId::new(17),
            end_headers: true,
            header_block: Bytes::new(),
        });
        assert_eq!(frame.stream_id().value(), 17);
    }

    #[test]
    fn test_frame_stream_id_unknown() {
        let frame = Frame::Unknown(UnknownFrame {
            frame_type: 0xff,
            flags: 0,
            stream_id: StreamId::new(19),
            payload: Bytes::new(),
        });
        assert_eq!(frame.stream_id().value(), 19);
    }

    // SettingId tests

    #[test]
    fn test_setting_id_from_u16() {
        assert_eq!(SettingId::from_u16(0x1), SettingId::HeaderTableSize);
        assert_eq!(SettingId::from_u16(0x2), SettingId::EnablePush);
        assert_eq!(SettingId::from_u16(0x3), SettingId::MaxConcurrentStreams);
        assert_eq!(SettingId::from_u16(0x4), SettingId::InitialWindowSize);
        assert_eq!(SettingId::from_u16(0x5), SettingId::MaxFrameSize);
        assert_eq!(SettingId::from_u16(0x6), SettingId::MaxHeaderListSize);
        assert_eq!(SettingId::from_u16(0x99), SettingId::Unknown(0x99));
    }

    #[test]
    fn test_setting_id_to_u16() {
        assert_eq!(SettingId::HeaderTableSize.to_u16(), 0x1);
        assert_eq!(SettingId::EnablePush.to_u16(), 0x2);
        assert_eq!(SettingId::MaxConcurrentStreams.to_u16(), 0x3);
        assert_eq!(SettingId::InitialWindowSize.to_u16(), 0x4);
        assert_eq!(SettingId::MaxFrameSize.to_u16(), 0x5);
        assert_eq!(SettingId::MaxHeaderListSize.to_u16(), 0x6);
        assert_eq!(SettingId::Unknown(0x99).to_u16(), 0x99);
    }

    #[test]
    fn test_setting_id_roundtrip() {
        let ids = [
            SettingId::HeaderTableSize,
            SettingId::EnablePush,
            SettingId::MaxConcurrentStreams,
            SettingId::InitialWindowSize,
            SettingId::MaxFrameSize,
            SettingId::MaxHeaderListSize,
        ];

        for id in ids {
            assert_eq!(SettingId::from_u16(id.to_u16()), id);
        }
    }

    // Frame struct Debug tests

    #[test]
    fn test_data_frame_debug() {
        let frame = DataFrame {
            stream_id: StreamId::new(1),
            end_stream: true,
            data: Bytes::from_static(b"hello"),
        };
        assert!(format!("{:?}", frame).contains("DataFrame"));
    }

    #[test]
    fn test_headers_frame_debug() {
        let frame = HeadersFrame {
            stream_id: StreamId::new(1),
            end_stream: false,
            end_headers: true,
            priority: Some(Priority {
                exclusive: true,
                dependency: StreamId::new(0),
                weight: 16,
            }),
            header_block: Bytes::new(),
        };
        assert!(format!("{:?}", frame).contains("HeadersFrame"));
    }

    #[test]
    fn test_settings_frame_debug() {
        let frame = SettingsFrame {
            ack: true,
            settings: vec![Setting {
                id: SettingId::MaxFrameSize,
                value: 16384,
            }],
        };
        assert!(format!("{:?}", frame).contains("SettingsFrame"));
    }

    #[test]
    fn test_ping_frame_debug() {
        let frame = PingFrame {
            ack: false,
            data: [1, 2, 3, 4, 5, 6, 7, 8],
        };
        assert!(format!("{:?}", frame).contains("PingFrame"));
    }

    #[test]
    fn test_goaway_frame_debug() {
        let frame = GoAwayFrame {
            last_stream_id: StreamId::new(10),
            error_code: 0,
            debug_data: Bytes::from_static(b"goodbye"),
        };
        assert!(format!("{:?}", frame).contains("GoAwayFrame"));
    }

    // Clone tests

    #[test]
    fn test_frame_clone() {
        let frame = Frame::Data(DataFrame {
            stream_id: StreamId::new(1),
            end_stream: true,
            data: Bytes::from_static(b"test"),
        });
        let cloned = frame.clone();
        assert_eq!(cloned.stream_id().value(), 1);
    }
}
