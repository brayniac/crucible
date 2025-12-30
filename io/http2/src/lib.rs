//! http2 - HTTP/2 implementation for io-driver-based applications.
//!
//! This crate provides a completion-based HTTP/2 implementation designed
//! to work with the io-driver I/O driver. It does not use async/await or tokio.
//!
//! # Features
//!
//! - Full HTTP/2 frame encoding and decoding
//! - HPACK header compression
//! - Connection and stream state management
//! - Flow control
//! - TLS integration via io-driver's transport layer
//!
//! # Architecture
//!
//! The crate is organized into several modules:
//!
//! - `frame`: HTTP/2 frame types, encoding, and decoding
//! - `hpack`: HPACK header compression
//! - `connection`: HTTP/2 connection state machine
//!
//! Transport layer (plain TCP, TLS) is provided by the `io-driver` crate.

pub mod connection;
pub mod frame;
pub mod hpack;

// Re-export commonly used types
pub use frame::{
    CONNECTION_PREFACE, DEFAULT_HEADER_TABLE_SIZE, DEFAULT_INITIAL_WINDOW_SIZE,
    DEFAULT_MAX_CONCURRENT_STREAMS, DEFAULT_MAX_FRAME_SIZE, DataFrame, ErrorCode,
    FRAME_HEADER_SIZE, Frame, FrameDecoder, FrameEncoder, FrameError, FrameType, GoAwayFrame,
    HeadersFrame, PingFrame, Priority, RstStreamFrame, Setting, SettingId, SettingsFrame, StreamId,
    WindowUpdateFrame,
};

pub use hpack::{HeaderField, HpackDecoder, HpackEncoder};

// Re-export transport types from io-driver for convenience
pub use io_driver::{PlainTransport, TlsConfig, TlsTransport, Transport, TransportState};

pub use connection::{
    Connection, ConnectionError, ConnectionEvent, ConnectionSettings, ConnectionState, FlowControl,
    ServerConnection, ServerEvent, Stream, StreamState,
};
