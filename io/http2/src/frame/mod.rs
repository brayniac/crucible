//! HTTP/2 frame types and parsing.
//!
//! HTTP/2 frames have a common 9-byte header:
//! ```text
//! +-----------------------------------------------+
//! |                 Length (24)                   |
//! +---------------+---------------+---------------+
//! |   Type (8)    |   Flags (8)   |
//! +-+-------------+---------------+-------------------------------+
//! |R|                 Stream Identifier (31)                      |
//! +=+=============================================================+
//! |                   Frame Payload (0...)                      ...
//! +---------------------------------------------------------------+
//! ```

mod decode;
mod encode;
mod error;
mod types;

pub use decode::FrameDecoder;
pub use encode::FrameEncoder;
pub use error::{ErrorCode, FrameError};
pub use types::*;

/// Maximum frame size allowed by HTTP/2 spec (2^24 - 1).
pub const MAX_FRAME_SIZE: u32 = 16_777_215;

/// Default maximum frame size (16 KB).
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 16_384;

/// Frame header size in bytes.
pub const FRAME_HEADER_SIZE: usize = 9;

/// Connection preface sent by clients.
pub const CONNECTION_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Default initial window size for flow control.
pub const DEFAULT_INITIAL_WINDOW_SIZE: u32 = 65_535;

/// Default header table size for HPACK.
pub const DEFAULT_HEADER_TABLE_SIZE: u32 = 4_096;

/// Maximum concurrent streams default.
pub const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 100;
