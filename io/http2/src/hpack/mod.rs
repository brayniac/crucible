//! HPACK header compression (RFC 7541).
//!
//! HPACK is the header compression algorithm used by HTTP/2. It uses:
//! - A static table of 61 common header fields
//! - A dynamic table of recently used headers
//! - Huffman coding for string literals
//! - Variable-length integer encoding

mod decode;
mod encode;
mod huffman;
mod table;

pub use decode::{HpackDecoder, HpackError};
pub use encode::HpackEncoder;
pub use table::{HeaderField, StaticTable};

/// Default dynamic table size (4096 bytes).
pub const DEFAULT_TABLE_SIZE: usize = 4096;
