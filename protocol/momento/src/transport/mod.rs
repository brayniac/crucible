//! Transport abstraction for Momento wire formats.
//!
//! This module provides a unified interface for both gRPC and protosocket
//! wire formats. The transport layer handles:
//! - Connection management
//! - Request/response correlation
//! - Wire format encoding/decoding
//!
//! Both transports expose the same high-level operations (get, set, delete)
//! but use different underlying wire protocols.

mod grpc;
mod protosocket;

pub use grpc::GrpcTransport;
pub use protosocket::ProtosocketTransport;

use crate::CacheValue;
use crate::error::Result;
use bytes::Bytes;
use std::io;
use std::time::Duration;

/// A unique identifier for a pending request.
///
/// For gRPC, this wraps an HTTP/2 stream ID.
/// For protosocket, this is a message ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestId(u64);

impl RequestId {
    /// Create a new request ID.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the raw ID value.
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl From<u64> for RequestId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<::grpc::StreamId> for RequestId {
    fn from(id: ::grpc::StreamId) -> Self {
        Self(id.value() as u64)
    }
}

/// Result of a completed operation from the transport layer.
#[derive(Debug)]
pub enum TransportResult {
    /// Get operation completed.
    Get {
        request_id: RequestId,
        cache_name: String,
        key: Bytes,
        result: Result<CacheValue>,
    },
    /// Set operation completed.
    Set {
        request_id: RequestId,
        cache_name: String,
        key: Bytes,
        result: Result<()>,
    },
    /// Delete operation completed.
    Delete {
        request_id: RequestId,
        cache_name: String,
        key: Bytes,
        result: Result<()>,
    },
}

/// Trait for Momento transports.
///
/// This trait abstracts over the wire format (gRPC vs protosocket) so that
/// the cache client can use either transparently.
pub trait MomentoTransport {
    /// Check if the transport is ready for operations.
    fn is_ready(&self) -> bool;

    /// Process transport readiness (e.g., after TLS handshake).
    fn on_transport_ready(&mut self) -> io::Result<()>;

    /// Feed received data to the transport.
    fn on_recv(&mut self, data: &[u8]) -> io::Result<()>;

    /// Feed plaintext data directly, bypassing the Transport recv buffer.
    ///
    /// More efficient than `on_recv()` when TLS is handled externally
    /// (e.g., by kompio's native TLS). Default delegates to `on_recv()`.
    fn feed_data(&mut self, data: &[u8]) -> io::Result<()> {
        self.on_recv(data)
    }

    /// Get data pending to be sent.
    fn pending_send(&self) -> &[u8];

    /// Advance send buffer after data was sent.
    fn advance_send(&mut self, n: usize);

    /// Check if there's data to send.
    fn has_pending_send(&self) -> bool;

    /// Start a Get operation.
    fn get(&mut self, cache_name: &str, key: &[u8]) -> io::Result<RequestId>;

    /// Start a Set operation with TTL.
    fn set(
        &mut self,
        cache_name: &str,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> io::Result<RequestId>;

    /// Start a Delete operation.
    fn delete(&mut self, cache_name: &str, key: &[u8]) -> io::Result<RequestId>;

    /// Poll for completed operations.
    fn poll(&mut self) -> Vec<TransportResult>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_new() {
        let id = RequestId::new(42);
        assert_eq!(id.value(), 42);
    }

    #[test]
    fn test_request_id_from_u64() {
        let id: RequestId = 123u64.into();
        assert_eq!(id.value(), 123);
    }

    #[test]
    fn test_request_id_equality() {
        let id1 = RequestId::new(1);
        let id2 = RequestId::new(1);
        let id3 = RequestId::new(2);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_request_id_debug() {
        let id = RequestId::new(99);
        assert!(format!("{:?}", id).contains("99"));
    }

    #[test]
    fn test_request_id_clone() {
        let id = RequestId::new(5);
        let cloned = id;
        assert_eq!(cloned.value(), 5);
    }

    #[test]
    fn test_request_id_from_stream_id() {
        let stream_id = ::grpc::StreamId::new(7);
        let request_id: RequestId = stream_id.into();
        assert_eq!(request_id.value(), 7);
    }
}
