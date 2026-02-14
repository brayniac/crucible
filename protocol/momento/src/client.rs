//! Momento cache client.

use crate::WireFormat;
use crate::credential::Credential;
use crate::error::Result;
use crate::transport::{
    GrpcTransport, MomentoTransport, ProtosocketTransport, RequestId, TransportResult,
};

use bytes::Bytes;
use grpc::StreamId;
use http2::{TlsConfig, TlsTransport, Transport};
use std::io;
use std::time::Duration;

/// A value retrieved from the cache.
#[derive(Debug, Clone)]
pub enum CacheValue {
    /// Cache hit with the value.
    Hit(Bytes),
    /// Cache miss.
    Miss,
}

impl CacheValue {
    /// Check if this is a cache hit.
    pub fn is_hit(&self) -> bool {
        matches!(self, CacheValue::Hit(_))
    }

    /// Check if this is a cache miss.
    pub fn is_miss(&self) -> bool {
        matches!(self, CacheValue::Miss)
    }

    /// Get the value if this is a hit.
    pub fn into_value(self) -> Option<Bytes> {
        match self {
            CacheValue::Hit(v) => Some(v),
            CacheValue::Miss => None,
        }
    }

    /// Get a reference to the value if this is a hit.
    pub fn value(&self) -> Option<&Bytes> {
        match self {
            CacheValue::Hit(v) => Some(v),
            CacheValue::Miss => None,
        }
    }
}

/// A pending Get operation.
pub struct PendingGet {
    request_id: RequestId,
    cache_name: String,
    key: Bytes,
}

impl PendingGet {
    /// Get the stream ID (for backwards compatibility with gRPC).
    pub fn stream_id(&self) -> StreamId {
        StreamId::new(self.request_id.value() as u32)
    }

    /// Get the request ID.
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Get the cache name.
    pub fn cache_name(&self) -> &str {
        &self.cache_name
    }

    /// Get the key.
    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

/// A pending Set operation.
pub struct PendingSet {
    request_id: RequestId,
    cache_name: String,
    key: Bytes,
}

impl PendingSet {
    /// Get the stream ID (for backwards compatibility with gRPC).
    pub fn stream_id(&self) -> StreamId {
        StreamId::new(self.request_id.value() as u32)
    }

    /// Get the request ID.
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Get the cache name.
    pub fn cache_name(&self) -> &str {
        &self.cache_name
    }

    /// Get the key.
    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

/// A pending Delete operation.
pub struct PendingDelete {
    request_id: RequestId,
    cache_name: String,
    key: Bytes,
}

impl PendingDelete {
    /// Get the stream ID (for backwards compatibility with gRPC).
    pub fn stream_id(&self) -> StreamId {
        StreamId::new(self.request_id.value() as u32)
    }

    /// Get the request ID.
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Get the cache name.
    pub fn cache_name(&self) -> &str {
        &self.cache_name
    }

    /// Get the key.
    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

/// Result of a completed operation.
#[derive(Debug)]
pub enum CompletedOp {
    /// Get operation completed.
    Get {
        cache_name: String,
        key: Bytes,
        result: Result<CacheValue>,
    },
    /// Set operation completed.
    Set {
        cache_name: String,
        key: Bytes,
        result: Result<()>,
    },
    /// Delete operation completed.
    Delete {
        cache_name: String,
        key: Bytes,
        result: Result<()>,
    },
}

/// Internal transport wrapper for both gRPC and protosocket.
enum TransportInner<T: Transport> {
    Grpc(Box<GrpcTransport<T>>),
    Protosocket(ProtosocketTransport<T>),
}

/// Momento cache client.
///
/// This client uses a completion-based model. You initiate operations
/// (get, set, delete) which return immediately with a pending handle.
/// Then you drive the event loop and poll for completed operations.
///
/// # Wire Formats
///
/// The client supports two wire formats:
/// - **gRPC** (default): Standard Momento gRPC API over HTTP/2
/// - **Protosocket**: Higher-performance format using length-delimited protobuf
///
/// The wire format is specified via the `Credential`:
///
/// ```ignore
/// // Use gRPC (default)
/// let cred = Credential::from_token("token")?;
///
/// // Use protosocket
/// let cred = Credential::from_token("token")?
///     .with_wire_format(WireFormat::Protosocket);
/// ```
pub struct CacheClient<T: Transport = TlsTransport> {
    /// The internal transport.
    inner: TransportInner<T>,
    /// Credential for authentication.
    credential: Credential,
    /// Default TTL for set operations.
    default_ttl: Duration,
}

impl CacheClient<TlsTransport> {
    /// Create a new cache client with TLS transport.
    ///
    /// The wire format is determined by the credential's `wire_format()` setting.
    pub fn connect(credential: Credential) -> io::Result<Self> {
        let config = TlsConfig::http2()?;
        let transport = TlsTransport::new(&config, credential.host())?;

        let inner = match credential.wire_format() {
            WireFormat::Grpc => {
                let grpc = GrpcTransport::new(
                    transport,
                    credential.endpoint(),
                    credential.token().to_string(),
                );
                TransportInner::Grpc(Box::new(grpc))
            }
            WireFormat::Protosocket => {
                let ps = ProtosocketTransport::new(transport, credential.token().to_string());
                TransportInner::Protosocket(ps)
            }
        };

        Ok(Self {
            inner,
            credential,
            default_ttl: Duration::from_secs(3600), // 1 hour default
        })
    }
}

impl<T: Transport> CacheClient<T> {
    /// Create a new cache client with a custom transport using gRPC.
    ///
    /// For protosocket support with custom transports, use `with_protosocket_transport`.
    pub fn with_transport(transport: T, credential: Credential) -> Self {
        let grpc = GrpcTransport::new(
            transport,
            credential.endpoint(),
            credential.token().to_string(),
        );

        Self {
            inner: TransportInner::Grpc(Box::new(grpc)),
            credential,
            default_ttl: Duration::from_secs(3600),
        }
    }

    /// Create a new cache client with a custom transport using protosocket.
    pub fn with_protosocket_transport(transport: T, credential: Credential) -> Self {
        let ps = ProtosocketTransport::new(transport, credential.token().to_string());

        Self {
            inner: TransportInner::Protosocket(ps),
            credential,
            default_ttl: Duration::from_secs(3600),
        }
    }

    /// Set the default TTL for set operations.
    pub fn set_default_ttl(&mut self, ttl: Duration) {
        self.default_ttl = ttl;
    }

    /// Get the credential.
    pub fn credential(&self) -> &Credential {
        &self.credential
    }

    /// Get the wire format in use.
    pub fn wire_format(&self) -> WireFormat {
        match &self.inner {
            TransportInner::Grpc(_) => WireFormat::Grpc,
            TransportInner::Protosocket(_) => WireFormat::Protosocket,
        }
    }

    /// Check if the client is ready for operations.
    pub fn is_ready(&self) -> bool {
        match &self.inner {
            TransportInner::Grpc(t) => t.is_ready(),
            TransportInner::Protosocket(t) => t.is_ready(),
        }
    }

    /// Process transport readiness (call after TLS handshake).
    pub fn on_transport_ready(&mut self) -> io::Result<()> {
        match &mut self.inner {
            TransportInner::Grpc(t) => t.on_transport_ready(),
            TransportInner::Protosocket(t) => t.on_transport_ready(),
        }
    }

    /// Feed received data to the client.
    pub fn on_recv(&mut self, data: &[u8]) -> io::Result<()> {
        match &mut self.inner {
            TransportInner::Grpc(t) => t.on_recv(data),
            TransportInner::Protosocket(t) => t.on_recv(data),
        }
    }

    /// Feed plaintext data directly, bypassing the Transport recv buffer.
    ///
    /// More efficient than `on_recv()` when TLS is handled externally
    /// (e.g., by kompio's native TLS). Skips the Transport recv buffer entirely.
    pub fn feed_data(&mut self, data: &[u8]) -> io::Result<()> {
        match &mut self.inner {
            TransportInner::Grpc(t) => t.feed_data(data),
            TransportInner::Protosocket(t) => t.feed_data(data),
        }
    }

    /// Get data pending to be sent.
    pub fn pending_send(&self) -> &[u8] {
        match &self.inner {
            TransportInner::Grpc(t) => t.pending_send(),
            TransportInner::Protosocket(t) => t.pending_send(),
        }
    }

    /// Advance send buffer after data was sent.
    pub fn advance_send(&mut self, n: usize) {
        match &mut self.inner {
            TransportInner::Grpc(t) => t.advance_send(n),
            TransportInner::Protosocket(t) => t.advance_send(n),
        }
    }

    /// Check if there's data to send.
    pub fn has_pending_send(&self) -> bool {
        match &self.inner {
            TransportInner::Grpc(t) => t.has_pending_send(),
            TransportInner::Protosocket(t) => t.has_pending_send(),
        }
    }

    /// Start a Get operation.
    pub fn get(&mut self, cache_name: &str, key: &[u8]) -> io::Result<PendingGet> {
        let request_id = match &mut self.inner {
            TransportInner::Grpc(t) => t.get(cache_name, key)?,
            TransportInner::Protosocket(t) => t.get(cache_name, key)?,
        };

        Ok(PendingGet {
            request_id,
            cache_name: cache_name.to_string(),
            key: Bytes::copy_from_slice(key),
        })
    }

    /// Start a Set operation.
    pub fn set(&mut self, cache_name: &str, key: &[u8], value: &[u8]) -> io::Result<PendingSet> {
        self.set_with_ttl(cache_name, key, value, self.default_ttl)
    }

    /// Start a Set operation with explicit TTL.
    pub fn set_with_ttl(
        &mut self,
        cache_name: &str,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> io::Result<PendingSet> {
        let request_id = match &mut self.inner {
            TransportInner::Grpc(t) => t.set(cache_name, key, value, ttl)?,
            TransportInner::Protosocket(t) => t.set(cache_name, key, value, ttl)?,
        };

        Ok(PendingSet {
            request_id,
            cache_name: cache_name.to_string(),
            key: Bytes::copy_from_slice(key),
        })
    }

    /// Start a Delete operation.
    pub fn delete(&mut self, cache_name: &str, key: &[u8]) -> io::Result<PendingDelete> {
        let request_id = match &mut self.inner {
            TransportInner::Grpc(t) => t.delete(cache_name, key)?,
            TransportInner::Protosocket(t) => t.delete(cache_name, key)?,
        };

        Ok(PendingDelete {
            request_id,
            cache_name: cache_name.to_string(),
            key: Bytes::copy_from_slice(key),
        })
    }

    /// Poll for completed operations.
    ///
    /// Returns a list of operations that have completed since the last poll.
    pub fn poll(&mut self) -> Vec<CompletedOp> {
        let results = match &mut self.inner {
            TransportInner::Grpc(t) => t.poll(),
            TransportInner::Protosocket(t) => t.poll(),
        };

        results
            .into_iter()
            .map(|r| match r {
                TransportResult::Get {
                    cache_name,
                    key,
                    result,
                    ..
                } => CompletedOp::Get {
                    cache_name,
                    key,
                    result,
                },
                TransportResult::Set {
                    cache_name,
                    key,
                    result,
                    ..
                } => CompletedOp::Set {
                    cache_name,
                    key,
                    result,
                },
                TransportResult::Delete {
                    cache_name,
                    key,
                    result,
                    ..
                } => CompletedOp::Delete {
                    cache_name,
                    key,
                    result,
                },
            })
            .collect()
    }

    /// Get the underlying gRPC channel (if using gRPC transport).
    ///
    /// Returns `None` if using protosocket transport.
    pub fn channel(&self) -> Option<&grpc::Channel<T>> {
        match &self.inner {
            TransportInner::Grpc(t) => Some(t.channel()),
            TransportInner::Protosocket(_) => None,
        }
    }

    /// Get mutable access to the underlying gRPC channel (if using gRPC transport).
    ///
    /// Returns `None` if using protosocket transport.
    pub fn channel_mut(&mut self) -> Option<&mut grpc::Channel<T>> {
        match &mut self.inner {
            TransportInner::Grpc(t) => Some(t.channel_mut()),
            TransportInner::Protosocket(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::CacheResponse;
    use http2::PlainTransport;

    // CacheValue tests

    #[test]
    fn test_cache_value_hit() {
        let hit = CacheValue::Hit(Bytes::from_static(b"value"));
        assert!(hit.is_hit());
        assert!(!hit.is_miss());
        assert_eq!(hit.value(), Some(&Bytes::from_static(b"value")));
    }

    #[test]
    fn test_cache_value_miss() {
        let miss = CacheValue::Miss;
        assert!(!miss.is_hit());
        assert!(miss.is_miss());
        assert!(miss.value().is_none());
    }

    #[test]
    fn test_cache_value_into_value_hit() {
        let hit = CacheValue::Hit(Bytes::from_static(b"data"));
        let value = hit.into_value();
        assert_eq!(value, Some(Bytes::from_static(b"data")));
    }

    #[test]
    fn test_cache_value_into_value_miss() {
        let miss = CacheValue::Miss;
        let value = miss.into_value();
        assert!(value.is_none());
    }

    #[test]
    fn test_cache_value_debug() {
        let hit = CacheValue::Hit(Bytes::from_static(b"v"));
        assert!(format!("{:?}", hit).contains("Hit"));

        let miss = CacheValue::Miss;
        assert!(format!("{:?}", miss).contains("Miss"));
    }

    #[test]
    fn test_cache_value_clone() {
        let hit = CacheValue::Hit(Bytes::from_static(b"value"));
        let cloned = hit.clone();
        assert!(cloned.is_hit());
        assert_eq!(cloned.value(), Some(&Bytes::from_static(b"value")));
    }

    // PendingGet tests

    #[test]
    fn test_pending_get_accessors() {
        let pending = PendingGet {
            request_id: RequestId::new(1),
            cache_name: "my-cache".to_string(),
            key: Bytes::from_static(b"my-key"),
        };

        assert_eq!(pending.stream_id().value(), 1);
        assert_eq!(pending.request_id().value(), 1);
        assert_eq!(pending.cache_name(), "my-cache");
        assert_eq!(pending.key(), b"my-key");
    }

    // PendingSet tests

    #[test]
    fn test_pending_set_accessors() {
        let pending = PendingSet {
            request_id: RequestId::new(3),
            cache_name: "cache".to_string(),
            key: Bytes::from_static(b"key"),
        };

        assert_eq!(pending.stream_id().value(), 3);
        assert_eq!(pending.request_id().value(), 3);
        assert_eq!(pending.cache_name(), "cache");
        assert_eq!(pending.key(), b"key");
    }

    // PendingDelete tests

    #[test]
    fn test_pending_delete_accessors() {
        let pending = PendingDelete {
            request_id: RequestId::new(5),
            cache_name: "test-cache".to_string(),
            key: Bytes::from_static(b"delete-key"),
        };

        assert_eq!(pending.stream_id().value(), 5);
        assert_eq!(pending.request_id().value(), 5);
        assert_eq!(pending.cache_name(), "test-cache");
        assert_eq!(pending.key(), b"delete-key");
    }

    // CompletedOp tests

    #[test]
    fn test_completed_op_get_debug() {
        let op = CompletedOp::Get {
            cache_name: "cache".to_string(),
            key: Bytes::from_static(b"key"),
            result: Ok(CacheValue::Hit(Bytes::from_static(b"value"))),
        };
        let debug = format!("{:?}", op);
        assert!(debug.contains("Get"));
    }

    #[test]
    fn test_completed_op_set_debug() {
        let op = CompletedOp::Set {
            cache_name: "cache".to_string(),
            key: Bytes::from_static(b"key"),
            result: Ok(()),
        };
        let debug = format!("{:?}", op);
        assert!(debug.contains("Set"));
    }

    #[test]
    fn test_completed_op_delete_debug() {
        let op = CompletedOp::Delete {
            cache_name: "cache".to_string(),
            key: Bytes::from_static(b"key"),
            result: Ok(()),
        };
        let debug = format!("{:?}", op);
        assert!(debug.contains("Delete"));
    }

    // CacheClient with PlainTransport tests (gRPC)

    #[test]
    fn test_cache_client_with_transport() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        assert!(!client.is_ready());
        assert_eq!(client.credential().token(), "token");
        assert_eq!(client.credential().endpoint(), "endpoint.com");
        assert_eq!(client.wire_format(), WireFormat::Grpc);
    }

    #[test]
    fn test_cache_client_with_protosocket_transport() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_protosocket_transport(transport, credential);

        assert!(!client.is_ready());
        assert_eq!(client.wire_format(), WireFormat::Protosocket);
    }

    #[test]
    fn test_cache_client_set_default_ttl() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_transport(transport, credential);

        client.set_default_ttl(Duration::from_secs(300));
        // Can't directly verify, but the method should work without error
    }

    #[test]
    fn test_cache_client_channel_access() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_transport(transport, credential);

        // Test immutable access
        assert!(client.channel().is_some());

        // Test mutable access
        assert!(client.channel_mut().is_some());
    }

    #[test]
    fn test_cache_client_channel_access_protosocket() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_protosocket_transport(transport, credential);

        // Protosocket doesn't have a gRPC channel
        assert!(client.channel().is_none());
        assert!(client.channel_mut().is_none());
    }

    #[test]
    fn test_cache_client_pending_send() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let _pending = client.pending_send();
        let _has_pending = client.has_pending_send();
    }

    #[test]
    fn test_cache_client_poll_empty() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_transport(transport, credential);

        let completed = client.poll();
        assert!(completed.is_empty());
    }

    #[test]
    fn test_cache_client_advance_send() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_transport(transport, credential);

        // Should not panic with 0
        client.advance_send(0);
    }

    #[test]
    fn test_cache_client_on_transport_ready() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_transport(transport, credential);

        // Should work without panic
        let _ = client.on_transport_ready();
    }

    #[test]
    fn test_cache_client_on_recv() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_transport(transport, credential);

        // Empty data should be ok
        let result = client.on_recv(&[]);
        assert!(result.is_ok());
    }

    // Protosocket-specific tests

    #[test]
    fn test_protosocket_client_auth_flow() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_protosocket_transport(transport, credential);

        // Start auth
        let _ = client.on_transport_ready();
        assert!(!client.is_ready());
        assert!(client.has_pending_send());

        // Simulate auth response
        let auth_response = CacheResponse::authenticate(1);
        let _ = client.on_recv(&auth_response.encode_length_delimited());
        assert!(client.is_ready());
    }

    #[test]
    fn test_protosocket_client_get_after_auth() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_protosocket_transport(transport, credential);

        // Auth flow
        let _ = client.on_transport_ready();
        let auth_response = CacheResponse::authenticate(1);
        let _ = client.on_recv(&auth_response.encode_length_delimited());

        // Now get should work
        let result = client.get("my-cache", b"my-key");
        assert!(result.is_ok());
    }

    #[test]
    fn test_protosocket_client_get_response() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_protosocket_transport(transport, credential);

        // Auth flow
        let _ = client.on_transport_ready();
        let _ = client.on_recv(&CacheResponse::authenticate(1).encode_length_delimited());
        client.advance_send(client.pending_send().len());

        // Send get
        let pending = client.get("cache", b"key").unwrap();
        client.advance_send(client.pending_send().len());

        // Simulate hit response
        let hit_response =
            CacheResponse::get_hit(pending.request_id().value(), Bytes::from_static(b"value"));
        let _ = client.on_recv(&hit_response.encode_length_delimited());

        // Poll for result
        let results = client.poll();
        assert_eq!(results.len(), 1);
        match &results[0] {
            CompletedOp::Get { result, .. } => {
                assert!(result.is_ok());
                assert!(result.as_ref().unwrap().is_hit());
            }
            _ => panic!("expected Get"),
        }
    }
}
