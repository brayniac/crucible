//! HTTP/2 frame errors.

use std::fmt;

/// HTTP/2 error codes (RFC 7540 Section 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ErrorCode {
    /// Graceful shutdown.
    NoError = 0x0,
    /// Protocol error detected.
    ProtocolError = 0x1,
    /// Implementation fault.
    InternalError = 0x2,
    /// Flow control limits exceeded.
    FlowControlError = 0x3,
    /// Settings not acknowledged in time.
    SettingsTimeout = 0x4,
    /// Frame received for closed stream.
    StreamClosed = 0x5,
    /// Frame size incorrect.
    FrameSizeError = 0x6,
    /// Stream not processed.
    RefusedStream = 0x7,
    /// Stream cancelled.
    Cancel = 0x8,
    /// Compression state not updated.
    CompressionError = 0x9,
    /// TCP connection error.
    ConnectError = 0xa,
    /// Processing capacity exceeded.
    EnhanceYourCalm = 0xb,
    /// Negotiated TLS requirements not met.
    InadequateSecurity = 0xc,
    /// HTTP/1.1 required.
    Http11Required = 0xd,
}

impl ErrorCode {
    pub fn from_u32(code: u32) -> Self {
        match code {
            0x0 => ErrorCode::NoError,
            0x1 => ErrorCode::ProtocolError,
            0x2 => ErrorCode::InternalError,
            0x3 => ErrorCode::FlowControlError,
            0x4 => ErrorCode::SettingsTimeout,
            0x5 => ErrorCode::StreamClosed,
            0x6 => ErrorCode::FrameSizeError,
            0x7 => ErrorCode::RefusedStream,
            0x8 => ErrorCode::Cancel,
            0x9 => ErrorCode::CompressionError,
            0xa => ErrorCode::ConnectError,
            0xb => ErrorCode::EnhanceYourCalm,
            0xc => ErrorCode::InadequateSecurity,
            0xd => ErrorCode::Http11Required,
            // Unknown error codes are treated as INTERNAL_ERROR
            _ => ErrorCode::InternalError,
        }
    }

    pub fn to_u32(self) -> u32 {
        self as u32
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCode::NoError => write!(f, "NO_ERROR"),
            ErrorCode::ProtocolError => write!(f, "PROTOCOL_ERROR"),
            ErrorCode::InternalError => write!(f, "INTERNAL_ERROR"),
            ErrorCode::FlowControlError => write!(f, "FLOW_CONTROL_ERROR"),
            ErrorCode::SettingsTimeout => write!(f, "SETTINGS_TIMEOUT"),
            ErrorCode::StreamClosed => write!(f, "STREAM_CLOSED"),
            ErrorCode::FrameSizeError => write!(f, "FRAME_SIZE_ERROR"),
            ErrorCode::RefusedStream => write!(f, "REFUSED_STREAM"),
            ErrorCode::Cancel => write!(f, "CANCEL"),
            ErrorCode::CompressionError => write!(f, "COMPRESSION_ERROR"),
            ErrorCode::ConnectError => write!(f, "CONNECT_ERROR"),
            ErrorCode::EnhanceYourCalm => write!(f, "ENHANCE_YOUR_CALM"),
            ErrorCode::InadequateSecurity => write!(f, "INADEQUATE_SECURITY"),
            ErrorCode::Http11Required => write!(f, "HTTP_1_1_REQUIRED"),
        }
    }
}

/// Frame parsing/encoding errors.
#[derive(Debug)]
pub enum FrameError {
    /// Not enough data to parse frame (need more bytes).
    Incomplete,
    /// Frame exceeds maximum allowed size.
    FrameTooLarge { size: u32, max: u32 },
    /// Invalid frame for stream 0 (connection-level).
    InvalidStreamZero { frame_type: u8 },
    /// Frame requires non-zero stream ID.
    StreamIdRequired { frame_type: u8 },
    /// Invalid frame payload length.
    InvalidPayloadLength {
        frame_type: u8,
        expected: usize,
        actual: usize,
    },
    /// Invalid padding length.
    InvalidPadding {
        pad_length: u8,
        payload_length: usize,
    },
    /// Invalid setting value.
    InvalidSettingValue { id: u16, value: u32 },
    /// Invalid window update increment.
    InvalidWindowIncrement { increment: u32 },
    /// Protocol error.
    Protocol(ErrorCode),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Incomplete => write!(f, "incomplete frame data"),
            FrameError::FrameTooLarge { size, max } => {
                write!(f, "frame size {} exceeds maximum {}", size, max)
            }
            FrameError::InvalidStreamZero { frame_type } => {
                write!(f, "frame type 0x{:02x} invalid on stream 0", frame_type)
            }
            FrameError::StreamIdRequired { frame_type } => {
                write!(
                    f,
                    "frame type 0x{:02x} requires non-zero stream ID",
                    frame_type
                )
            }
            FrameError::InvalidPayloadLength {
                frame_type,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "frame type 0x{:02x} expected {} bytes, got {}",
                    frame_type, expected, actual
                )
            }
            FrameError::InvalidPadding {
                pad_length,
                payload_length,
            } => {
                write!(
                    f,
                    "padding length {} exceeds payload length {}",
                    pad_length, payload_length
                )
            }
            FrameError::InvalidSettingValue { id, value } => {
                write!(f, "invalid value {} for setting 0x{:04x}", value, id)
            }
            FrameError::InvalidWindowIncrement { increment } => {
                write!(f, "invalid window increment {}", increment)
            }
            FrameError::Protocol(code) => {
                write!(f, "protocol error: {}", code)
            }
        }
    }
}

impl std::error::Error for FrameError {}

#[cfg(test)]
mod tests {
    use super::*;

    // ErrorCode tests

    #[test]
    fn test_error_code_from_u32() {
        assert_eq!(ErrorCode::from_u32(0x0), ErrorCode::NoError);
        assert_eq!(ErrorCode::from_u32(0x1), ErrorCode::ProtocolError);
        assert_eq!(ErrorCode::from_u32(0x2), ErrorCode::InternalError);
        assert_eq!(ErrorCode::from_u32(0x3), ErrorCode::FlowControlError);
        assert_eq!(ErrorCode::from_u32(0x4), ErrorCode::SettingsTimeout);
        assert_eq!(ErrorCode::from_u32(0x5), ErrorCode::StreamClosed);
        assert_eq!(ErrorCode::from_u32(0x6), ErrorCode::FrameSizeError);
        assert_eq!(ErrorCode::from_u32(0x7), ErrorCode::RefusedStream);
        assert_eq!(ErrorCode::from_u32(0x8), ErrorCode::Cancel);
        assert_eq!(ErrorCode::from_u32(0x9), ErrorCode::CompressionError);
        assert_eq!(ErrorCode::from_u32(0xa), ErrorCode::ConnectError);
        assert_eq!(ErrorCode::from_u32(0xb), ErrorCode::EnhanceYourCalm);
        assert_eq!(ErrorCode::from_u32(0xc), ErrorCode::InadequateSecurity);
        assert_eq!(ErrorCode::from_u32(0xd), ErrorCode::Http11Required);
    }

    #[test]
    fn test_error_code_from_u32_unknown() {
        // Unknown codes map to InternalError
        assert_eq!(ErrorCode::from_u32(0xe), ErrorCode::InternalError);
        assert_eq!(ErrorCode::from_u32(0xff), ErrorCode::InternalError);
        assert_eq!(ErrorCode::from_u32(0xffffffff), ErrorCode::InternalError);
    }

    #[test]
    fn test_error_code_to_u32() {
        assert_eq!(ErrorCode::NoError.to_u32(), 0x0);
        assert_eq!(ErrorCode::ProtocolError.to_u32(), 0x1);
        assert_eq!(ErrorCode::InternalError.to_u32(), 0x2);
        assert_eq!(ErrorCode::FlowControlError.to_u32(), 0x3);
        assert_eq!(ErrorCode::SettingsTimeout.to_u32(), 0x4);
        assert_eq!(ErrorCode::StreamClosed.to_u32(), 0x5);
        assert_eq!(ErrorCode::FrameSizeError.to_u32(), 0x6);
        assert_eq!(ErrorCode::RefusedStream.to_u32(), 0x7);
        assert_eq!(ErrorCode::Cancel.to_u32(), 0x8);
        assert_eq!(ErrorCode::CompressionError.to_u32(), 0x9);
        assert_eq!(ErrorCode::ConnectError.to_u32(), 0xa);
        assert_eq!(ErrorCode::EnhanceYourCalm.to_u32(), 0xb);
        assert_eq!(ErrorCode::InadequateSecurity.to_u32(), 0xc);
        assert_eq!(ErrorCode::Http11Required.to_u32(), 0xd);
    }

    #[test]
    fn test_error_code_roundtrip() {
        let codes = [
            ErrorCode::NoError,
            ErrorCode::ProtocolError,
            ErrorCode::InternalError,
            ErrorCode::FlowControlError,
            ErrorCode::SettingsTimeout,
            ErrorCode::StreamClosed,
            ErrorCode::FrameSizeError,
            ErrorCode::RefusedStream,
            ErrorCode::Cancel,
            ErrorCode::CompressionError,
            ErrorCode::ConnectError,
            ErrorCode::EnhanceYourCalm,
            ErrorCode::InadequateSecurity,
            ErrorCode::Http11Required,
        ];

        for code in codes {
            assert_eq!(ErrorCode::from_u32(code.to_u32()), code);
        }
    }

    #[test]
    fn test_error_code_display() {
        assert_eq!(format!("{}", ErrorCode::NoError), "NO_ERROR");
        assert_eq!(format!("{}", ErrorCode::ProtocolError), "PROTOCOL_ERROR");
        assert_eq!(format!("{}", ErrorCode::InternalError), "INTERNAL_ERROR");
        assert_eq!(
            format!("{}", ErrorCode::FlowControlError),
            "FLOW_CONTROL_ERROR"
        );
        assert_eq!(
            format!("{}", ErrorCode::SettingsTimeout),
            "SETTINGS_TIMEOUT"
        );
        assert_eq!(format!("{}", ErrorCode::StreamClosed), "STREAM_CLOSED");
        assert_eq!(format!("{}", ErrorCode::FrameSizeError), "FRAME_SIZE_ERROR");
        assert_eq!(format!("{}", ErrorCode::RefusedStream), "REFUSED_STREAM");
        assert_eq!(format!("{}", ErrorCode::Cancel), "CANCEL");
        assert_eq!(
            format!("{}", ErrorCode::CompressionError),
            "COMPRESSION_ERROR"
        );
        assert_eq!(format!("{}", ErrorCode::ConnectError), "CONNECT_ERROR");
        assert_eq!(
            format!("{}", ErrorCode::EnhanceYourCalm),
            "ENHANCE_YOUR_CALM"
        );
        assert_eq!(
            format!("{}", ErrorCode::InadequateSecurity),
            "INADEQUATE_SECURITY"
        );
        assert_eq!(
            format!("{}", ErrorCode::Http11Required),
            "HTTP_1_1_REQUIRED"
        );
    }

    #[test]
    fn test_error_code_debug() {
        let code = ErrorCode::NoError;
        let debug = format!("{:?}", code);
        assert!(debug.contains("NoError"));
    }

    #[test]
    fn test_error_code_clone() {
        let code = ErrorCode::Cancel;
        let cloned = code;
        assert_eq!(code, cloned);
    }

    #[test]
    fn test_error_code_eq() {
        assert_eq!(ErrorCode::NoError, ErrorCode::NoError);
        assert_ne!(ErrorCode::NoError, ErrorCode::Cancel);
    }

    // FrameError tests

    #[test]
    fn test_frame_error_incomplete_display() {
        let err = FrameError::Incomplete;
        assert_eq!(format!("{}", err), "incomplete frame data");
    }

    #[test]
    fn test_frame_error_frame_too_large_display() {
        let err = FrameError::FrameTooLarge {
            size: 20000,
            max: 16384,
        };
        assert_eq!(format!("{}", err), "frame size 20000 exceeds maximum 16384");
    }

    #[test]
    fn test_frame_error_invalid_stream_zero_display() {
        let err = FrameError::InvalidStreamZero { frame_type: 0x01 };
        assert_eq!(format!("{}", err), "frame type 0x01 invalid on stream 0");
    }

    #[test]
    fn test_frame_error_stream_id_required_display() {
        let err = FrameError::StreamIdRequired { frame_type: 0x00 };
        assert_eq!(
            format!("{}", err),
            "frame type 0x00 requires non-zero stream ID"
        );
    }

    #[test]
    fn test_frame_error_invalid_payload_length_display() {
        let err = FrameError::InvalidPayloadLength {
            frame_type: 0x04,
            expected: 6,
            actual: 10,
        };
        assert_eq!(
            format!("{}", err),
            "frame type 0x04 expected 6 bytes, got 10"
        );
    }

    #[test]
    fn test_frame_error_invalid_padding_display() {
        let err = FrameError::InvalidPadding {
            pad_length: 100,
            payload_length: 50,
        };
        assert_eq!(
            format!("{}", err),
            "padding length 100 exceeds payload length 50"
        );
    }

    #[test]
    fn test_frame_error_invalid_setting_value_display() {
        let err = FrameError::InvalidSettingValue { id: 0x05, value: 0 };
        assert_eq!(format!("{}", err), "invalid value 0 for setting 0x0005");
    }

    #[test]
    fn test_frame_error_invalid_window_increment_display() {
        let err = FrameError::InvalidWindowIncrement { increment: 0 };
        assert_eq!(format!("{}", err), "invalid window increment 0");
    }

    #[test]
    fn test_frame_error_protocol_display() {
        let err = FrameError::Protocol(ErrorCode::FlowControlError);
        assert_eq!(format!("{}", err), "protocol error: FLOW_CONTROL_ERROR");
    }

    #[test]
    fn test_frame_error_debug() {
        let err = FrameError::Incomplete;
        let debug = format!("{:?}", err);
        assert!(debug.contains("Incomplete"));

        let err = FrameError::FrameTooLarge { size: 100, max: 50 };
        let debug = format!("{:?}", err);
        assert!(debug.contains("FrameTooLarge"));
    }

    #[test]
    fn test_frame_error_is_error() {
        // Verify FrameError implements std::error::Error
        fn assert_error<E: std::error::Error>() {}
        assert_error::<FrameError>();
    }
}
