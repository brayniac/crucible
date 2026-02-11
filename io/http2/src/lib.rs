//! http2 - HTTP/2 implementation with optional TLS transport.
//!
//! This crate provides a completion-based HTTP/2 implementation. It does not
//! use async/await or tokio.
//!
//! # Features
//!
//! - Full HTTP/2 frame encoding and decoding
//! - HPACK header compression
//! - Connection and stream state management
//! - Flow control
//! - Transport layer abstraction (plain TCP, TLS)

pub mod connection;
pub mod frame;
pub mod hpack;
pub mod transport;

// Re-export commonly used types
pub use frame::{
    CONNECTION_PREFACE, DEFAULT_HEADER_TABLE_SIZE, DEFAULT_INITIAL_WINDOW_SIZE,
    DEFAULT_MAX_CONCURRENT_STREAMS, DEFAULT_MAX_FRAME_SIZE, DataFrame, ErrorCode,
    FRAME_HEADER_SIZE, Frame, FrameDecoder, FrameEncoder, FrameError, FrameType, GoAwayFrame,
    HeadersFrame, PingFrame, Priority, RstStreamFrame, Setting, SettingId, SettingsFrame, StreamId,
    WindowUpdateFrame,
};

pub use hpack::{HeaderField, HpackDecoder, HpackEncoder};

// Re-export transport types for convenience
pub use transport::{PlainTransport, ProcessResult, Transport, TransportState};
#[cfg(feature = "tls")]
pub use transport::{TlsConfig, TlsTransport};

pub use connection::{
    Connection, ConnectionError, ConnectionEvent, ConnectionSettings, ConnectionState, FlowControl,
    ServerConnection, ServerEvent, Stream, StreamState,
};
