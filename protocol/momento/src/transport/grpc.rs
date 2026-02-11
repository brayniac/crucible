//! gRPC transport for Momento.
//!
//! This transport uses gRPC over HTTP/2 for communication with Momento.

use super::{MomentoTransport, RequestId, TransportResult};
use crate::CacheValue;
use crate::error::{Error, Result};
use crate::proto::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, SetRequest, SetResponse,
};

use bytes::Bytes;
use grpc::{CallBuilder, CallEvent, Channel, Code, Status};
use http2::Connection;
use http2::Transport;
use std::collections::HashMap;
use std::io;
use std::time::Duration;

/// gRPC service path for the cache client.
const SERVICE_PATH: &str = "/cache_client.Scs";

/// State of a pending operation.
enum PendingOp {
    Get { cache_name: String, key: Bytes },
    Set { cache_name: String, key: Bytes },
    Delete { cache_name: String, key: Bytes },
}

/// gRPC transport for Momento cache operations.
///
/// Uses gRPC over HTTP/2 for communication. This is the standard Momento
/// protocol, compatible with all Momento endpoints.
pub struct GrpcTransport<T: Transport> {
    /// The gRPC channel.
    channel: Channel<T>,
    /// Authentication token.
    auth_token: String,
    /// Pending operations by stream ID.
    pending: HashMap<u32, PendingOp>,
    /// Response data by stream ID.
    responses: HashMap<u32, Bytes>,
}

impl<T: Transport> GrpcTransport<T> {
    /// Create a new gRPC transport.
    pub fn new(transport: T, endpoint: &str, auth_token: String) -> Self {
        let conn = Connection::new(transport);
        let channel = Channel::new(conn, endpoint);

        Self {
            channel,
            auth_token,
            pending: HashMap::new(),
            responses: HashMap::new(),
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

    /// Complete a Get operation.
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

    /// Complete a Set operation.
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

    /// Complete a Delete operation.
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
}

impl<T: Transport> MomentoTransport for GrpcTransport<T> {
    fn is_ready(&self) -> bool {
        self.channel.is_ready()
    }

    fn on_transport_ready(&mut self) -> io::Result<()> {
        self.channel.on_transport_ready()
    }

    fn on_recv(&mut self, data: &[u8]) -> io::Result<()> {
        self.channel.on_recv(data)
    }

    fn pending_send(&self) -> &[u8] {
        self.channel.pending_send()
    }

    fn advance_send(&mut self, n: usize) {
        self.channel.advance_send(n);
    }

    fn has_pending_send(&self) -> bool {
        self.channel.has_pending_send()
    }

    fn get(&mut self, cache_name: &str, key: &[u8]) -> io::Result<RequestId> {
        let request = GetRequest { cache_key: key };
        let encoded = request.encode();

        let call = self.channel.unary(
            CallBuilder::new(format!("{}/Get", SERVICE_PATH))
                .metadata("authorization", &self.auth_token)
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

        Ok(stream_id.into())
    }

    fn set(
        &mut self,
        cache_name: &str,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> io::Result<RequestId> {
        let request = SetRequest {
            cache_key: key,
            cache_body: value,
            ttl_milliseconds: ttl.as_millis() as u64,
        };
        let encoded = request.encode();

        let call = self.channel.unary(
            CallBuilder::new(format!("{}/Set", SERVICE_PATH))
                .metadata("authorization", &self.auth_token)
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

        Ok(stream_id.into())
    }

    fn delete(&mut self, cache_name: &str, key: &[u8]) -> io::Result<RequestId> {
        let request = DeleteRequest { cache_key: key };
        let encoded = request.encode();

        let call = self.channel.unary(
            CallBuilder::new(format!("{}/Delete", SERVICE_PATH))
                .metadata("authorization", &self.auth_token)
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

        Ok(stream_id.into())
    }

    fn poll(&mut self) -> Vec<TransportResult> {
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
                        let request_id = stream_id.into();

                        let result = match op {
                            PendingOp::Get { cache_name, key } => {
                                let result = self.complete_get(status, response_data);
                                TransportResult::Get {
                                    request_id,
                                    cache_name,
                                    key,
                                    result,
                                }
                            }
                            PendingOp::Set { cache_name, key } => {
                                let result = self.complete_set(status, response_data);
                                TransportResult::Set {
                                    request_id,
                                    cache_name,
                                    key,
                                    result,
                                }
                            }
                            PendingOp::Delete { cache_name, key } => {
                                let result = self.complete_delete(status, response_data);
                                TransportResult::Delete {
                                    request_id,
                                    cache_name,
                                    key,
                                    result,
                                }
                            }
                        };
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use http2::PlainTransport;

    #[test]
    fn test_grpc_transport_new() {
        let transport = PlainTransport::new();
        let grpc = GrpcTransport::new(transport, "endpoint.com", "token".to_string());
        assert!(!grpc.is_ready());
    }

    #[test]
    fn test_grpc_transport_channel_access() {
        let transport = PlainTransport::new();
        let mut grpc = GrpcTransport::new(transport, "endpoint.com", "token".to_string());

        let _ = grpc.channel();
        let _ = grpc.channel_mut();
    }

    #[test]
    fn test_grpc_transport_pending_send() {
        let transport = PlainTransport::new();
        let grpc = GrpcTransport::new(transport, "endpoint.com", "token".to_string());

        let _pending = grpc.pending_send();
        let _has_pending = grpc.has_pending_send();
    }

    #[test]
    fn test_grpc_transport_advance_send() {
        let transport = PlainTransport::new();
        let mut grpc = GrpcTransport::new(transport, "endpoint.com", "token".to_string());
        grpc.advance_send(0);
    }

    #[test]
    fn test_grpc_transport_on_recv() {
        let transport = PlainTransport::new();
        let mut grpc = GrpcTransport::new(transport, "endpoint.com", "token".to_string());
        let result = grpc.on_recv(&[]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_grpc_transport_poll_empty() {
        let transport = PlainTransport::new();
        let mut grpc = GrpcTransport::new(transport, "endpoint.com", "token".to_string());
        let completed = grpc.poll();
        assert!(completed.is_empty());
    }

    #[test]
    fn test_grpc_transport_complete_get_hit() {
        let transport = PlainTransport::new();
        let grpc = GrpcTransport::new(transport, "endpoint.com", "token".to_string());

        let response = GetResponse::Hit(Bytes::from_static(b"value"));
        let encoded = Bytes::from(response.encode());

        let result = grpc.complete_get(Status::ok(), Some(encoded));
        assert!(result.is_ok());
        assert!(result.unwrap().is_hit());
    }

    #[test]
    fn test_grpc_transport_complete_get_miss() {
        let transport = PlainTransport::new();
        let grpc = GrpcTransport::new(transport, "endpoint.com", "token".to_string());

        let response = GetResponse::Miss;
        let encoded = Bytes::from(response.encode());

        let result = grpc.complete_get(Status::ok(), Some(encoded));
        assert!(result.is_ok());
        assert!(result.unwrap().is_miss());
    }

    #[test]
    fn test_grpc_transport_complete_set_ok() {
        let transport = PlainTransport::new();
        let grpc = GrpcTransport::new(transport, "endpoint.com", "token".to_string());

        let response = SetResponse::Ok;
        let encoded = Bytes::from(response.encode());

        let result = grpc.complete_set(Status::ok(), Some(encoded));
        assert!(result.is_ok());
    }

    #[test]
    fn test_grpc_transport_complete_delete_ok() {
        let transport = PlainTransport::new();
        let grpc = GrpcTransport::new(transport, "endpoint.com", "token".to_string());

        let result = grpc.complete_delete(Status::ok(), None);
        assert!(result.is_ok());
    }
}
