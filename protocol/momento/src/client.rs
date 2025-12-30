//! Momento cache client.

use crate::credential::Credential;
use crate::error::{Error, Result};
use crate::proto::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, SetRequest, SetResponse,
};

use bytes::Bytes;
use grpc::{CallBuilder, CallEvent, Channel, Code, Status, StreamId};
use http2::Connection;
use io_driver::{TlsConfig, TlsTransport, Transport};
use std::collections::HashMap;
use std::io;
use std::time::Duration;

/// gRPC service path for the cache client.
const SERVICE_PATH: &str = "/cache_client.Scs";

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
    stream_id: StreamId,
    cache_name: String,
    key: Bytes,
}

impl PendingGet {
    /// Get the stream ID.
    pub fn stream_id(&self) -> StreamId {
        self.stream_id
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
    stream_id: StreamId,
    cache_name: String,
    key: Bytes,
}

impl PendingSet {
    /// Get the stream ID.
    pub fn stream_id(&self) -> StreamId {
        self.stream_id
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
    stream_id: StreamId,
    cache_name: String,
    key: Bytes,
}

impl PendingDelete {
    /// Get the stream ID.
    pub fn stream_id(&self) -> StreamId {
        self.stream_id
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

/// State of a pending operation.
enum PendingOp {
    Get { cache_name: String, key: Bytes },
    Set { cache_name: String, key: Bytes },
    Delete { cache_name: String, key: Bytes },
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

/// Momento cache client.
///
/// This client uses a completion-based model. You initiate operations
/// (get, set, delete) which return immediately with a pending handle.
/// Then you drive the event loop and poll for completed operations.
pub struct CacheClient<T: Transport = TlsTransport> {
    /// The gRPC channel.
    channel: Channel<T>,
    /// Credential for authentication.
    credential: Credential,
    /// Pending operations by stream ID.
    pending: HashMap<u32, PendingOp>,
    /// Response data by stream ID.
    responses: HashMap<u32, Bytes>,
    /// Default TTL for set operations.
    default_ttl: Duration,
}

impl CacheClient<TlsTransport> {
    /// Create a new cache client with TLS transport.
    pub fn connect(credential: Credential) -> io::Result<Self> {
        let config = TlsConfig::http2()?;
        let transport = TlsTransport::new(&config, credential.host())?;
        let conn = Connection::new(transport);
        let channel = Channel::new(conn, credential.endpoint());

        Ok(Self {
            channel,
            credential,
            pending: HashMap::new(),
            responses: HashMap::new(),
            default_ttl: Duration::from_secs(3600), // 1 hour default
        })
    }
}

impl<T: Transport> CacheClient<T> {
    /// Create a new cache client with a custom transport.
    pub fn with_transport(transport: T, credential: Credential) -> Self {
        let conn = Connection::new(transport);
        let channel = Channel::new(conn, credential.endpoint());

        Self {
            channel,
            credential,
            pending: HashMap::new(),
            responses: HashMap::new(),
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

    /// Check if the channel is ready.
    pub fn is_ready(&self) -> bool {
        self.channel.is_ready()
    }

    /// Process transport readiness (call after TLS handshake).
    pub fn on_transport_ready(&mut self) -> io::Result<()> {
        self.channel.on_transport_ready()
    }

    /// Feed received data to the client.
    pub fn on_recv(&mut self, data: &[u8]) -> io::Result<()> {
        self.channel.on_recv(data)
    }

    /// Get data pending to be sent.
    pub fn pending_send(&self) -> &[u8] {
        self.channel.pending_send()
    }

    /// Advance send buffer after data was sent.
    pub fn advance_send(&mut self, n: usize) {
        self.channel.advance_send(n);
    }

    /// Check if there's data to send.
    pub fn has_pending_send(&self) -> bool {
        self.channel.has_pending_send()
    }

    /// Start a Get operation.
    pub fn get(&mut self, cache_name: &str, key: &[u8]) -> io::Result<PendingGet> {
        let request = GetRequest { cache_key: key };
        let encoded = request.encode();

        let call = self.channel.unary(
            CallBuilder::new(format!("{}/Get", SERVICE_PATH))
                .metadata("authorization", self.credential.token())
                .metadata("cache", cache_name),
            &encoded,
        )?;

        let stream_id = call.stream_id();
        self.pending.insert(
            stream_id.value(),
            PendingOp::Get {
                cache_name: cache_name.to_string(),
                key: Bytes::copy_from_slice(key),
            },
        );

        Ok(PendingGet {
            stream_id,
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
        let request = SetRequest {
            cache_key: key,
            cache_body: value,
            ttl_milliseconds: ttl.as_millis() as u64,
        };
        let encoded = request.encode();

        let call = self.channel.unary(
            CallBuilder::new(format!("{}/Set", SERVICE_PATH))
                .metadata("authorization", self.credential.token())
                .metadata("cache", cache_name),
            &encoded,
        )?;

        let stream_id = call.stream_id();
        self.pending.insert(
            stream_id.value(),
            PendingOp::Set {
                cache_name: cache_name.to_string(),
                key: Bytes::copy_from_slice(key),
            },
        );

        Ok(PendingSet {
            stream_id,
            cache_name: cache_name.to_string(),
            key: Bytes::copy_from_slice(key),
        })
    }

    /// Start a Delete operation.
    pub fn delete(&mut self, cache_name: &str, key: &[u8]) -> io::Result<PendingDelete> {
        let request = DeleteRequest { cache_key: key };
        let encoded = request.encode();

        let call = self.channel.unary(
            CallBuilder::new(format!("{}/Delete", SERVICE_PATH))
                .metadata("authorization", self.credential.token())
                .metadata("cache", cache_name),
            &encoded,
        )?;

        let stream_id = call.stream_id();
        self.pending.insert(
            stream_id.value(),
            PendingOp::Delete {
                cache_name: cache_name.to_string(),
                key: Bytes::copy_from_slice(key),
            },
        );

        Ok(PendingDelete {
            stream_id,
            cache_name: cache_name.to_string(),
            key: Bytes::copy_from_slice(key),
        })
    }

    /// Poll for completed operations.
    ///
    /// Returns a list of operations that have completed since the last poll.
    pub fn poll(&mut self) -> Vec<CompletedOp> {
        let mut completed = Vec::new();

        // Process channel events
        for (stream_id, event) in self.channel.poll() {
            match event {
                CallEvent::Message(data) => {
                    // Store response data
                    self.responses.insert(stream_id.value(), data);
                }
                CallEvent::Complete(status) => {
                    // Get pending operation
                    if let Some(op) = self.pending.remove(&stream_id.value()) {
                        let response_data = self.responses.remove(&stream_id.value());
                        let result = self.complete_op(op, status, response_data);
                        completed.push(result);
                    }
                }
                CallEvent::Headers(_) => {
                    // Headers received, wait for data
                }
            }
        }

        completed
    }

    /// Complete an operation with its response.
    fn complete_op(
        &self,
        op: PendingOp,
        status: Status,
        response_data: Option<Bytes>,
    ) -> CompletedOp {
        match op {
            PendingOp::Get { cache_name, key } => {
                let result = self.complete_get(status, response_data);
                CompletedOp::Get {
                    cache_name,
                    key,
                    result,
                }
            }
            PendingOp::Set { cache_name, key } => {
                let result = self.complete_set(status, response_data);
                CompletedOp::Set {
                    cache_name,
                    key,
                    result,
                }
            }
            PendingOp::Delete { cache_name, key } => {
                let result = self.complete_delete(status, response_data);
                CompletedOp::Delete {
                    cache_name,
                    key,
                    result,
                }
            }
        }
    }

    fn complete_get(&self, status: Status, data: Option<Bytes>) -> Result<CacheValue> {
        if !status.is_ok() {
            return Err(Error::Grpc(status));
        }

        let data = data.ok_or_else(|| Error::Protocol("missing response data".into()))?;

        match GetResponse::decode(&data) {
            Some(GetResponse::Hit(value)) => Ok(CacheValue::Hit(value)),
            Some(GetResponse::Miss) => Ok(CacheValue::Miss),
            Some(GetResponse::Error { message }) => {
                Err(Error::Grpc(Status::new(Code::Internal, message)))
            }
            None => Err(Error::Protocol("failed to decode get response".into())),
        }
    }

    fn complete_set(&self, status: Status, data: Option<Bytes>) -> Result<()> {
        if !status.is_ok() {
            return Err(Error::Grpc(status));
        }

        // Set response may be empty on success
        if let Some(data) = data {
            if data.is_empty() {
                return Ok(());
            }
            match SetResponse::decode(&data) {
                Some(SetResponse::Ok) => Ok(()),
                Some(SetResponse::Error { message }) => {
                    Err(Error::Grpc(Status::new(Code::Internal, message)))
                }
                None => Err(Error::Protocol("failed to decode set response".into())),
            }
        } else {
            // Empty response is OK
            Ok(())
        }
    }

    fn complete_delete(&self, status: Status, data: Option<Bytes>) -> Result<()> {
        if !status.is_ok() {
            return Err(Error::Grpc(status));
        }

        // Delete response is always empty on success
        if let Some(data) = data {
            if data.is_empty() {
                return Ok(());
            }
            match DeleteResponse::decode(&data) {
                Some(DeleteResponse::Ok) => Ok(()),
                None => Err(Error::Protocol("failed to decode delete response".into())),
            }
        } else {
            Ok(())
        }
    }

    /// Get the underlying channel.
    pub fn channel(&self) -> &Channel<T> {
        &self.channel
    }

    /// Get mutable access to the underlying channel.
    pub fn channel_mut(&mut self) -> &mut Channel<T> {
        &mut self.channel
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grpc::Code;
    use http2::PlainTransport;

    // Test helper to access private completion methods
    impl<T: Transport> CacheClient<T> {
        fn test_complete_get(&self, status: Status, data: Option<Bytes>) -> Result<CacheValue> {
            self.complete_get(status, data)
        }

        fn test_complete_set(&self, status: Status, data: Option<Bytes>) -> Result<()> {
            self.complete_set(status, data)
        }

        fn test_complete_delete(&self, status: Status, data: Option<Bytes>) -> Result<()> {
            self.complete_delete(status, data)
        }

        fn test_complete_op(
            &self,
            op: PendingOp,
            status: Status,
            response_data: Option<Bytes>,
        ) -> CompletedOp {
            self.complete_op(op, status, response_data)
        }
    }

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
            stream_id: StreamId::new(1),
            cache_name: "my-cache".to_string(),
            key: Bytes::from_static(b"my-key"),
        };

        assert_eq!(pending.stream_id().value(), 1);
        assert_eq!(pending.cache_name(), "my-cache");
        assert_eq!(pending.key(), b"my-key");
    }

    // PendingSet tests

    #[test]
    fn test_pending_set_accessors() {
        let pending = PendingSet {
            stream_id: StreamId::new(3),
            cache_name: "cache".to_string(),
            key: Bytes::from_static(b"key"),
        };

        assert_eq!(pending.stream_id().value(), 3);
        assert_eq!(pending.cache_name(), "cache");
        assert_eq!(pending.key(), b"key");
    }

    // PendingDelete tests

    #[test]
    fn test_pending_delete_accessors() {
        let pending = PendingDelete {
            stream_id: StreamId::new(5),
            cache_name: "test-cache".to_string(),
            key: Bytes::from_static(b"delete-key"),
        };

        assert_eq!(pending.stream_id().value(), 5);
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

    // CacheClient with PlainTransport tests

    #[test]
    fn test_cache_client_with_transport() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        assert!(!client.is_ready());
        assert_eq!(client.credential().token(), "token");
        assert_eq!(client.credential().endpoint(), "endpoint.com");
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
        let _ = client.channel();

        // Test mutable access
        let _ = client.channel_mut();
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

    // complete_get tests

    #[test]
    fn test_complete_get_hit() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        // Create encoded GetResponse::Hit
        let response = GetResponse::Hit(Bytes::from_static(b"cached_value"));
        let encoded = Bytes::from(response.encode());

        let result = client.test_complete_get(Status::ok(), Some(encoded));
        assert!(result.is_ok());
        let value = result.unwrap();
        assert!(value.is_hit());
        assert_eq!(value.value(), Some(&Bytes::from_static(b"cached_value")));
    }

    #[test]
    fn test_complete_get_miss() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let response = GetResponse::Miss;
        let encoded = Bytes::from(response.encode());

        let result = client.test_complete_get(Status::ok(), Some(encoded));
        assert!(result.is_ok());
        let value = result.unwrap();
        assert!(value.is_miss());
    }

    #[test]
    fn test_complete_get_error_response() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let response = GetResponse::Error {
            message: "cache error".to_string(),
        };
        let encoded = Bytes::from(response.encode());

        let result = client.test_complete_get(Status::ok(), Some(encoded));
        assert!(result.is_err());
    }

    #[test]
    fn test_complete_get_grpc_error() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let result =
            client.test_complete_get(Status::new(Code::Unavailable, "service unavailable"), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_complete_get_missing_data() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let result = client.test_complete_get(Status::ok(), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_complete_get_invalid_decode() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        // Invalid protobuf data
        let result =
            client.test_complete_get(Status::ok(), Some(Bytes::from_static(&[0xFF, 0xFF])));
        assert!(result.is_err());
    }

    // complete_set tests

    #[test]
    fn test_complete_set_ok() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let response = SetResponse::Ok;
        let encoded = Bytes::from(response.encode());

        let result = client.test_complete_set(Status::ok(), Some(encoded));
        assert!(result.is_ok());
    }

    #[test]
    fn test_complete_set_empty_data() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        // Empty data is considered success
        let result = client.test_complete_set(Status::ok(), Some(Bytes::new()));
        assert!(result.is_ok());
    }

    #[test]
    fn test_complete_set_no_data() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        // No data is considered success
        let result = client.test_complete_set(Status::ok(), None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_complete_set_error_response() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let response = SetResponse::Error {
            message: "set error".to_string(),
        };
        let encoded = Bytes::from(response.encode());

        let result = client.test_complete_set(Status::ok(), Some(encoded));
        assert!(result.is_err());
    }

    #[test]
    fn test_complete_set_grpc_error() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let result =
            client.test_complete_set(Status::new(Code::PermissionDenied, "unauthorized"), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_complete_set_invalid_decode() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let result =
            client.test_complete_set(Status::ok(), Some(Bytes::from_static(&[0xFF, 0xFF])));
        assert!(result.is_err());
    }

    // complete_delete tests

    #[test]
    fn test_complete_delete_ok() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let response = DeleteResponse::Ok;
        let encoded = Bytes::from(response.encode());

        let result = client.test_complete_delete(Status::ok(), Some(encoded));
        assert!(result.is_ok());
    }

    #[test]
    fn test_complete_delete_empty_data() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let result = client.test_complete_delete(Status::ok(), Some(Bytes::new()));
        assert!(result.is_ok());
    }

    #[test]
    fn test_complete_delete_no_data() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let result = client.test_complete_delete(Status::ok(), None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_complete_delete_grpc_error() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let result =
            client.test_complete_delete(Status::new(Code::NotFound, "key not found"), None);
        assert!(result.is_err());
    }

    // Note: test_complete_delete_invalid_decode is not needed because
    // DeleteResponse has no fields so decode always succeeds

    // complete_op tests

    #[test]
    fn test_complete_op_get() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let op = PendingOp::Get {
            cache_name: "my-cache".to_string(),
            key: Bytes::from_static(b"my-key"),
        };

        let response = GetResponse::Hit(Bytes::from_static(b"value"));
        let encoded = Bytes::from(response.encode());

        let completed = client.test_complete_op(op, Status::ok(), Some(encoded));
        match completed {
            CompletedOp::Get {
                cache_name,
                key,
                result,
            } => {
                assert_eq!(cache_name, "my-cache");
                assert_eq!(key.as_ref(), b"my-key");
                assert!(result.is_ok());
            }
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_complete_op_set() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let op = PendingOp::Set {
            cache_name: "cache".to_string(),
            key: Bytes::from_static(b"key"),
        };

        let response = SetResponse::Ok;
        let encoded = Bytes::from(response.encode());

        let completed = client.test_complete_op(op, Status::ok(), Some(encoded));
        match completed {
            CompletedOp::Set {
                cache_name,
                key,
                result,
            } => {
                assert_eq!(cache_name, "cache");
                assert_eq!(key.as_ref(), b"key");
                assert!(result.is_ok());
            }
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn test_complete_op_delete() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let client = CacheClient::with_transport(transport, credential);

        let op = PendingOp::Delete {
            cache_name: "cache".to_string(),
            key: Bytes::from_static(b"key"),
        };

        let completed = client.test_complete_op(op, Status::ok(), None);
        match completed {
            CompletedOp::Delete {
                cache_name,
                key,
                result,
            } => {
                assert_eq!(cache_name, "cache");
                assert_eq!(key.as_ref(), b"key");
                assert!(result.is_ok());
            }
            _ => panic!("expected Delete"),
        }
    }

    // advance_send test

    #[test]
    fn test_cache_client_advance_send() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_transport(transport, credential);

        // Should not panic with 0
        client.advance_send(0);
    }

    // on_transport_ready test

    #[test]
    fn test_cache_client_on_transport_ready() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_transport(transport, credential);

        // Should work without panic
        let _ = client.on_transport_ready();
    }

    // on_recv test

    #[test]
    fn test_cache_client_on_recv() {
        let transport = PlainTransport::new();
        let credential = Credential::with_endpoint("token", "endpoint.com");
        let mut client = CacheClient::with_transport(transport, credential);

        // Empty data should be ok
        let result = client.on_recv(&[]);
        assert!(result.is_ok());
    }
}
