//! protocol-momento - Lightweight Momento cache implementation for io-driver-based applications.
//!
//! This crate provides a minimal Momento cache client and server that supports both
//! the gRPC API and the higher-performance protosocket wire format, without pulling
//! in heavy dependencies like tokio or prost.
//!
//! # Wire Formats
//!
//! Two wire formats are supported:
//!
//! - **gRPC**: Standard Momento gRPC API over HTTP/2. Compatible with all Momento endpoints.
//! - **Protosocket**: Higher-performance format using length-delimited protobuf over raw TCP.
//!   Eliminates HTTP/2 overhead for ~2.5x better throughput.
//!
//! # Features
//!
//! - Get, Set, Delete cache operations
//! - Client and server implementations
//! - Minimal protobuf encoding (no code generation)
//! - Completion-based I/O via io-driver
//! - Support for both gRPC and protosocket wire formats
//!
//! # Client Example
//!
//! ```ignore
//! use protocol_momento::{CacheClient, Credential, WireFormat};
//! use io_driver::TlsConfig;
//!
//! // Create client with gRPC (default)
//! let credential = Credential::from_token("your-api-token")?;
//! let mut client = CacheClient::connect(credential)?;
//!
//! // Or use protosocket for higher performance
//! let credential = Credential::from_token("your-api-token")?
//!     .with_wire_format(WireFormat::Protosocket);
//! let mut client = CacheClient::connect(credential)?;
//!
//! // Cache operations happen through the event loop
//! // (see full example for event loop integration)
//! ```

mod client;
mod credential;
mod error;
pub mod proto;
mod server;
pub mod transport;

pub use client::{CacheClient, CacheValue, CompletedOp, PendingDelete, PendingGet, PendingSet};
pub use credential::Credential;
pub use error::{Error, Result};
pub use server::{CacheRequest, CacheResponse, CacheServer, CacheServerBuilder, CacheServerEvent};

// Re-export useful types
pub use grpc::{Code, Status};
pub use io_driver::{TlsConfig, TlsTransport};

/// Wire format for Momento communication.
///
/// Momento supports two wire formats:
/// - **Grpc**: Standard gRPC over HTTP/2, compatible with all Momento endpoints
/// - **Protosocket**: Higher-performance format using length-delimited protobuf
///   messages directly over TCP, eliminating HTTP/2 overhead
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WireFormat {
    /// Standard gRPC over HTTP/2.
    ///
    /// This is the default format and is compatible with all Momento endpoints.
    /// Uses HTTP/2 framing with gRPC message encoding.
    #[default]
    Grpc,

    /// High-performance protosocket format.
    ///
    /// Uses length-delimited protobuf messages directly over TCP, eliminating
    /// HTTP/2 overhead. Provides approximately 2.5x better throughput than gRPC
    /// by avoiding mutex contention in HTTP/2 connection handling.
    ///
    /// Messages include a `message_id` field for request/response correlation
    /// instead of HTTP/2 stream IDs.
    Protosocket,
}

impl WireFormat {
    /// Returns true if this is the gRPC wire format.
    pub fn is_grpc(&self) -> bool {
        matches!(self, WireFormat::Grpc)
    }

    /// Returns true if this is the protosocket wire format.
    pub fn is_protosocket(&self) -> bool {
        matches!(self, WireFormat::Protosocket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_format_default() {
        assert_eq!(WireFormat::default(), WireFormat::Grpc);
    }

    #[test]
    fn test_wire_format_is_grpc() {
        assert!(WireFormat::Grpc.is_grpc());
        assert!(!WireFormat::Protosocket.is_grpc());
    }

    #[test]
    fn test_wire_format_is_protosocket() {
        assert!(WireFormat::Protosocket.is_protosocket());
        assert!(!WireFormat::Grpc.is_protosocket());
    }

    #[test]
    fn test_wire_format_clone() {
        let format = WireFormat::Protosocket;
        let cloned = format;
        assert_eq!(cloned, WireFormat::Protosocket);
    }

    #[test]
    fn test_wire_format_debug() {
        assert!(format!("{:?}", WireFormat::Grpc).contains("Grpc"));
        assert!(format!("{:?}", WireFormat::Protosocket).contains("Protosocket"));
    }
}
