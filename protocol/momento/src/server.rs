//! Momento cache server implementation.
//!
//! Provides a server that implements the Momento cache API, supporting both
//! gRPC and protosocket wire formats.

use crate::WireFormat;
use crate::proto::{
    CacheCommand, CacheResponseResult, ControlCode, DeleteRequestOwned, DeleteResponse,
    GetRequestOwned, GetResponse, SetRequestOwned, SetResponse, StatusCode, UnaryCommand,
    decode_length_delimited_message,
};
use crate::transport::RequestId;
use bytes::Bytes;
use grpc::{GrpcServerEvent, Request, Server, Status, StreamId};
use std::io;

/// Method paths for the Momento cache API.
const METHOD_GET: &str = "/cache_client.Scs/Get";
const METHOD_SET: &str = "/cache_client.Scs/Set";
const METHOD_DELETE: &str = "/cache_client.Scs/Delete";

/// Connection state for protosocket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtosocketState {
    /// Waiting for authenticate command.
    WaitingAuth,
    /// Authenticated and ready.
    Ready,
}

/// Internal transport abstraction.
enum ServerTransport {
    Grpc(Box<Server>),
    Protosocket {
        state: ProtosocketState,
        recv_buf: Vec<u8>,
        send_buf: Vec<u8>,
        auth_token: Option<String>,
    },
}

/// A Momento cache server connection.
///
/// Supports both gRPC and protosocket wire formats. The wire format is
/// determined at construction time.
pub struct CacheServer {
    /// The underlying transport.
    transport: ServerTransport,
}

/// A parsed cache request.
#[derive(Debug)]
pub enum CacheRequest {
    /// Get request.
    Get {
        request_id: RequestId,
        cache_name: Option<String>,
        key: Bytes,
    },
    /// Set request.
    Set {
        request_id: RequestId,
        cache_name: Option<String>,
        key: Bytes,
        value: Bytes,
        ttl_ms: u64,
    },
    /// Delete request.
    Delete {
        request_id: RequestId,
        cache_name: Option<String>,
        key: Bytes,
    },
}

impl CacheRequest {
    /// Get the request ID.
    pub fn request_id(&self) -> RequestId {
        match self {
            CacheRequest::Get { request_id, .. } => *request_id,
            CacheRequest::Set { request_id, .. } => *request_id,
            CacheRequest::Delete { request_id, .. } => *request_id,
        }
    }

    /// Get the stream ID (for backwards compatibility with gRPC).
    pub fn stream_id(&self) -> StreamId {
        StreamId::new(self.request_id().value() as u32)
    }
}

/// Response to send back to the client.
#[derive(Debug)]
pub enum CacheResponse {
    /// Get response (hit or miss).
    Get(GetResponse),
    /// Set response.
    Set(SetResponse),
    /// Delete response.
    Delete(DeleteResponse),
}

/// Events from the cache server.
#[derive(Debug)]
pub enum CacheServerEvent {
    /// Server is ready to accept requests.
    Ready,
    /// Received a cache request.
    Request(CacheRequest),
    /// Client reset/cancelled a request.
    RequestCancelled { request_id: RequestId },
    /// Client sent GOAWAY (gRPC) or disconnected.
    GoAway,
    /// Connection error.
    Error(String),
}

/// Builder for CacheServer.
#[derive(Debug, Clone, Default)]
pub struct CacheServerBuilder {
    wire_format: WireFormat,
}

impl CacheServerBuilder {
    /// Create a new builder with default settings (gRPC).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the wire format.
    pub fn wire_format(mut self, format: WireFormat) -> Self {
        self.wire_format = format;
        self
    }

    /// Build the CacheServer.
    pub fn build(self) -> CacheServer {
        match self.wire_format {
            WireFormat::Grpc => CacheServer {
                transport: ServerTransport::Grpc(Box::new(Server::new())),
            },
            WireFormat::Protosocket => CacheServer {
                transport: ServerTransport::Protosocket {
                    state: ProtosocketState::WaitingAuth,
                    recv_buf: Vec::new(),
                    send_buf: Vec::new(),
                    auth_token: None,
                },
            },
        }
    }
}

impl CacheServer {
    /// Create a new builder for CacheServer.
    pub fn builder() -> CacheServerBuilder {
        CacheServerBuilder::new()
    }

    /// Create a new Momento cache server using gRPC (default).
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Get the wire format being used.
    pub fn wire_format(&self) -> WireFormat {
        match &self.transport {
            ServerTransport::Grpc(_) => WireFormat::Grpc,
            ServerTransport::Protosocket { .. } => WireFormat::Protosocket,
        }
    }

    /// Check if the server is ready.
    pub fn is_ready(&self) -> bool {
        match &self.transport {
            ServerTransport::Grpc(server) => server.is_ready(),
            ServerTransport::Protosocket { state, .. } => *state == ProtosocketState::Ready,
        }
    }

    /// Feed data received from the client.
    pub fn on_recv(&mut self, data: &[u8]) {
        match &mut self.transport {
            ServerTransport::Grpc(server) => server.on_recv(data),
            ServerTransport::Protosocket { recv_buf, .. } => {
                recv_buf.extend_from_slice(data);
            }
        }
    }

    /// Process received data and return events.
    pub fn process(&mut self) -> io::Result<Vec<CacheServerEvent>> {
        match &mut self.transport {
            ServerTransport::Grpc(server) => {
                let grpc_events = server.process()?;
                let mut result = Vec::new();

                for event in grpc_events {
                    match event {
                        GrpcServerEvent::Ready => {
                            result.push(CacheServerEvent::Ready);
                        }
                        GrpcServerEvent::Request(request) => {
                            if let Some(cache_request) = Self::parse_grpc_request(request) {
                                result.push(CacheServerEvent::Request(cache_request));
                            }
                        }
                        GrpcServerEvent::RequestData { .. } => {
                            // Streaming requests not supported for cache operations
                        }
                        GrpcServerEvent::StreamReset { stream_id } => {
                            result.push(CacheServerEvent::RequestCancelled {
                                request_id: stream_id.into(),
                            });
                        }
                        GrpcServerEvent::GoAway => {
                            result.push(CacheServerEvent::GoAway);
                        }
                        GrpcServerEvent::Error(e) => {
                            result.push(CacheServerEvent::Error(e));
                        }
                    }
                }

                Ok(result)
            }
            ServerTransport::Protosocket {
                state,
                recv_buf,
                send_buf,
                auth_token,
            } => {
                let mut result = Vec::new();

                // Process complete messages from recv_buf
                loop {
                    let buf_slice: &[u8] = recv_buf;
                    match decode_length_delimited_message(buf_slice) {
                        Some((consumed, msg_data)) => {
                            // Parse the CacheCommand
                            if let Some(cmd) = CacheCommand::decode(msg_data) {
                                // Handle based on state and command type
                                match *state {
                                    ProtosocketState::WaitingAuth => {
                                        // Expect authenticate command
                                        if let Some(UnaryCommand::Authenticate {
                                            auth_token: token,
                                        }) = cmd.command
                                        {
                                            *auth_token = Some(token);
                                            *state = ProtosocketState::Ready;

                                            // Send authenticate response
                                            let response =
                                                crate::proto::CacheResponse::authenticate(
                                                    cmd.message_id,
                                                );
                                            send_buf.extend_from_slice(
                                                &response.encode_length_delimited(),
                                            );

                                            result.push(CacheServerEvent::Ready);
                                        }
                                    }
                                    ProtosocketState::Ready => {
                                        // Handle cache commands
                                        if cmd.control_code == ControlCode::Cancel {
                                            result.push(CacheServerEvent::RequestCancelled {
                                                request_id: RequestId::new(cmd.message_id),
                                            });
                                        } else if let Some(cache_req) = cmd.command.and_then(|u| {
                                            Self::parse_protosocket_command(cmd.message_id, u)
                                        }) {
                                            result.push(CacheServerEvent::Request(cache_req));
                                        }
                                    }
                                }
                            }
                            // Remove consumed bytes
                            recv_buf.drain(..consumed);
                        }
                        None => break, // No complete message available
                    }
                }

                Ok(result)
            }
        }
    }

    /// Parse a gRPC request into a cache request.
    fn parse_grpc_request(request: Request) -> Option<CacheRequest> {
        // Extract cache name from metadata
        let cache_name = request.metadata.get("cache").map(|s| s.to_string());
        let request_id: RequestId = request.stream_id.into();

        match request.path.as_str() {
            METHOD_GET => {
                let req = GetRequestOwned::decode(&request.message)?;
                Some(CacheRequest::Get {
                    request_id,
                    cache_name,
                    key: req.cache_key,
                })
            }
            METHOD_SET => {
                let req = SetRequestOwned::decode(&request.message)?;
                Some(CacheRequest::Set {
                    request_id,
                    cache_name,
                    key: req.cache_key,
                    value: req.cache_body,
                    ttl_ms: req.ttl_milliseconds,
                })
            }
            METHOD_DELETE => {
                let req = DeleteRequestOwned::decode(&request.message)?;
                Some(CacheRequest::Delete {
                    request_id,
                    cache_name,
                    key: req.cache_key,
                })
            }
            _ => {
                // Unknown method
                None
            }
        }
    }

    /// Parse a protosocket command into a cache request.
    fn parse_protosocket_command(message_id: u64, command: UnaryCommand) -> Option<CacheRequest> {
        let request_id = RequestId::new(message_id);

        match command {
            UnaryCommand::Get { namespace, key } => Some(CacheRequest::Get {
                request_id,
                cache_name: Some(namespace),
                key,
            }),
            UnaryCommand::Set {
                namespace,
                key,
                value,
                ttl_millis,
            } => Some(CacheRequest::Set {
                request_id,
                cache_name: Some(namespace),
                key,
                value,
                ttl_ms: ttl_millis,
            }),
            UnaryCommand::Delete { namespace, key } => Some(CacheRequest::Delete {
                request_id,
                cache_name: Some(namespace),
                key,
            }),
            UnaryCommand::Authenticate { .. } => None, // Already handled
        }
    }

    /// Send a response for a cache request.
    pub fn send_response(
        &mut self,
        request_id: RequestId,
        response: CacheResponse,
    ) -> io::Result<()> {
        match &mut self.transport {
            ServerTransport::Grpc(server) => {
                let message = match response {
                    CacheResponse::Get(r) => r.encode(),
                    CacheResponse::Set(r) => r.encode(),
                    CacheResponse::Delete(r) => r.encode(),
                };
                let stream_id = StreamId::new(request_id.value() as u32);
                server.send_response(stream_id, Status::ok(), &message)
            }
            ServerTransport::Protosocket { send_buf, .. } => {
                let proto_response = match response {
                    CacheResponse::Get(GetResponse::Hit(value)) => crate::proto::CacheResponse {
                        message_id: request_id.value(),
                        control_code: ControlCode::Normal,
                        result: CacheResponseResult::Get { value: Some(value) },
                    },
                    CacheResponse::Get(GetResponse::Miss) => crate::proto::CacheResponse {
                        message_id: request_id.value(),
                        control_code: ControlCode::Normal,
                        result: CacheResponseResult::Get { value: None },
                    },
                    CacheResponse::Get(GetResponse::Error { message }) => {
                        crate::proto::CacheResponse::error(
                            request_id.value(),
                            StatusCode::Unknown,
                            message,
                        )
                    }
                    CacheResponse::Set(SetResponse::Ok) => crate::proto::CacheResponse {
                        message_id: request_id.value(),
                        control_code: ControlCode::Normal,
                        result: CacheResponseResult::Set,
                    },
                    CacheResponse::Set(SetResponse::Error { message }) => {
                        crate::proto::CacheResponse::error(
                            request_id.value(),
                            StatusCode::Unknown,
                            message,
                        )
                    }
                    CacheResponse::Delete(DeleteResponse::Ok) => crate::proto::CacheResponse {
                        message_id: request_id.value(),
                        control_code: ControlCode::Normal,
                        result: CacheResponseResult::Delete,
                    },
                };
                send_buf.extend_from_slice(&proto_response.encode_length_delimited());
                Ok(())
            }
        }
    }

    /// Send an error response.
    pub fn send_error(&mut self, request_id: RequestId, status: Status) -> io::Result<()> {
        match &mut self.transport {
            ServerTransport::Grpc(server) => {
                let stream_id = StreamId::new(request_id.value() as u32);
                server.send_error(stream_id, status)
            }
            ServerTransport::Protosocket { send_buf, .. } => {
                let code = match status.code() {
                    grpc::Code::NotFound => StatusCode::NotFound,
                    grpc::Code::InvalidArgument => StatusCode::InvalidArgument,
                    grpc::Code::PermissionDenied => StatusCode::PermissionDenied,
                    grpc::Code::Unauthenticated => StatusCode::Unauthenticated,
                    _ => StatusCode::Unknown,
                };
                let response = crate::proto::CacheResponse::error(
                    request_id.value(),
                    code,
                    status.message().unwrap_or(""),
                );
                send_buf.extend_from_slice(&response.encode_length_delimited());
                Ok(())
            }
        }
    }

    /// Get data to send to the client.
    pub fn pending_send(&self) -> &[u8] {
        match &self.transport {
            ServerTransport::Grpc(server) => server.pending_send(),
            ServerTransport::Protosocket { send_buf, .. } => send_buf,
        }
    }

    /// Mark data as sent.
    pub fn advance_send(&mut self, n: usize) {
        match &mut self.transport {
            ServerTransport::Grpc(server) => server.advance_send(n),
            ServerTransport::Protosocket { send_buf, .. } => {
                send_buf.drain(..n);
            }
        }
    }

    /// Check if there's data to send.
    pub fn has_pending_send(&self) -> bool {
        match &self.transport {
            ServerTransport::Grpc(server) => server.has_pending_send(),
            ServerTransport::Protosocket { send_buf, .. } => !send_buf.is_empty(),
        }
    }

    /// Get the underlying gRPC server (if using gRPC).
    pub fn grpc_server(&self) -> Option<&Server> {
        match &self.transport {
            ServerTransport::Grpc(server) => Some(server),
            ServerTransport::Protosocket { .. } => None,
        }
    }

    /// Get mutable access to the underlying gRPC server (if using gRPC).
    pub fn grpc_server_mut(&mut self) -> Option<&mut Server> {
        match &mut self.transport {
            ServerTransport::Grpc(server) => Some(server),
            ServerTransport::Protosocket { .. } => None,
        }
    }

    /// Get the authenticated token (protosocket only).
    pub fn auth_token(&self) -> Option<&str> {
        match &self.transport {
            ServerTransport::Grpc(_) => None,
            ServerTransport::Protosocket { auth_token, .. } => auth_token.as_deref(),
        }
    }
}

impl Default for CacheServer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{CacheCommand, DeleteRequest, GetRequest, SetRequest, UnaryCommand};
    use grpc::{Metadata, Request};

    #[test]
    fn test_server_new() {
        let server = CacheServer::new();
        assert!(!server.is_ready());
        assert_eq!(server.wire_format(), WireFormat::Grpc);
    }

    #[test]
    fn test_server_builder_protosocket() {
        let server = CacheServer::builder()
            .wire_format(WireFormat::Protosocket)
            .build();
        assert!(!server.is_ready());
        assert_eq!(server.wire_format(), WireFormat::Protosocket);
    }

    #[test]
    fn test_server_builder_grpc() {
        let server = CacheServer::builder().wire_format(WireFormat::Grpc).build();
        assert!(!server.is_ready());
        assert_eq!(server.wire_format(), WireFormat::Grpc);
    }

    #[test]
    fn test_server_builder_default() {
        let server = CacheServer::builder().build();
        assert_eq!(server.wire_format(), WireFormat::Grpc);
    }

    #[test]
    fn test_server_default() {
        let server = CacheServer::default();
        assert!(!server.is_ready());
    }

    #[test]
    fn test_server_grpc_server_access() {
        let mut server = CacheServer::new();

        // Test immutable access
        assert!(server.grpc_server().is_some());

        // Test mutable access
        assert!(server.grpc_server_mut().is_some());
    }

    #[test]
    fn test_server_grpc_server_access_protosocket() {
        let mut server = CacheServer::builder()
            .wire_format(WireFormat::Protosocket)
            .build();

        // Should return None for protosocket
        assert!(server.grpc_server().is_none());
        assert!(server.grpc_server_mut().is_none());
    }

    #[test]
    fn test_server_pending_send() {
        let server = CacheServer::new();
        let _pending = server.pending_send();
        let _has_pending = server.has_pending_send();
    }

    #[test]
    fn test_server_advance_send() {
        let mut server = CacheServer::new();
        // Should not panic with 0
        server.advance_send(0);
    }

    #[test]
    fn test_server_on_recv_empty() {
        let mut server = CacheServer::new();
        server.on_recv(&[]);
        // Should not panic
    }

    #[test]
    fn test_server_process_empty() {
        let mut server = CacheServer::new();
        let events = server.process().unwrap();
        assert!(events.is_empty());
    }

    // CacheRequest tests

    #[test]
    fn test_cache_request_get_debug() {
        let request = CacheRequest::Get {
            request_id: RequestId::new(1),
            cache_name: Some("test-cache".to_string()),
            key: Bytes::from_static(b"key"),
        };
        let debug = format!("{:?}", request);
        assert!(debug.contains("Get"));
        assert!(debug.contains("test-cache"));
    }

    #[test]
    fn test_cache_request_set_debug() {
        let request = CacheRequest::Set {
            request_id: RequestId::new(1),
            cache_name: Some("cache".to_string()),
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
            ttl_ms: 60000,
        };
        let debug = format!("{:?}", request);
        assert!(debug.contains("Set"));
    }

    #[test]
    fn test_cache_request_delete_debug() {
        let request = CacheRequest::Delete {
            request_id: RequestId::new(1),
            cache_name: None,
            key: Bytes::from_static(b"key"),
        };
        let debug = format!("{:?}", request);
        assert!(debug.contains("Delete"));
    }

    #[test]
    fn test_cache_request_accessors() {
        let request = CacheRequest::Get {
            request_id: RequestId::new(42),
            cache_name: None,
            key: Bytes::from_static(b"key"),
        };
        assert_eq!(request.request_id().value(), 42);
        assert_eq!(request.stream_id().value(), 42);
    }

    // CacheResponse tests

    #[test]
    fn test_cache_response_get_debug() {
        let response = CacheResponse::Get(GetResponse::Hit(Bytes::from_static(b"value")));
        let debug = format!("{:?}", response);
        assert!(debug.contains("Get"));
    }

    #[test]
    fn test_cache_response_set_debug() {
        let response = CacheResponse::Set(SetResponse::Ok);
        let debug = format!("{:?}", response);
        assert!(debug.contains("Set"));
    }

    #[test]
    fn test_cache_response_delete_debug() {
        let response = CacheResponse::Delete(DeleteResponse::Ok);
        let debug = format!("{:?}", response);
        assert!(debug.contains("Delete"));
    }

    // CacheServerEvent tests

    #[test]
    fn test_cache_server_event_ready_debug() {
        let event = CacheServerEvent::Ready;
        let debug = format!("{:?}", event);
        assert!(debug.contains("Ready"));
    }

    #[test]
    fn test_cache_server_event_request_debug() {
        let request = CacheRequest::Get {
            request_id: RequestId::new(1),
            cache_name: None,
            key: Bytes::from_static(b"key"),
        };
        let event = CacheServerEvent::Request(request);
        let debug = format!("{:?}", event);
        assert!(debug.contains("Request"));
    }

    #[test]
    fn test_cache_server_event_request_cancelled_debug() {
        let event = CacheServerEvent::RequestCancelled {
            request_id: RequestId::new(1),
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("RequestCancelled"));
    }

    #[test]
    fn test_cache_server_event_goaway_debug() {
        let event = CacheServerEvent::GoAway;
        let debug = format!("{:?}", event);
        assert!(debug.contains("GoAway"));
    }

    #[test]
    fn test_cache_server_event_error_debug() {
        let event = CacheServerEvent::Error("test error".to_string());
        let debug = format!("{:?}", event);
        assert!(debug.contains("Error"));
        assert!(debug.contains("test error"));
    }

    // Proto encoding tests for request types

    #[test]
    fn test_get_request_encode() {
        let request = GetRequest {
            cache_key: b"test-key",
        };
        let encoded = request.encode();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_set_request_encode() {
        let request = SetRequest {
            cache_key: b"key",
            cache_body: b"value",
            ttl_milliseconds: 5000,
        };
        let encoded = request.encode();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_delete_request_encode() {
        let request = DeleteRequest { cache_key: b"key" };
        let encoded = request.encode();
        assert!(!encoded.is_empty());
    }

    // parse_grpc_request tests

    #[test]
    fn test_parse_request_get() {
        let get_req = GetRequest {
            cache_key: b"my-key",
        };
        let encoded = get_req.encode();

        let mut metadata = Metadata::new();
        metadata.insert("cache".to_string(), "test-cache".to_string());

        let request = Request {
            stream_id: StreamId::new(1),
            path: "/cache_client.Scs/Get".to_string(),
            metadata,
            message: Bytes::from(encoded),
        };

        let result = CacheServer::parse_grpc_request(request);
        assert!(result.is_some());
        match result.unwrap() {
            CacheRequest::Get {
                request_id,
                cache_name,
                key,
            } => {
                assert_eq!(request_id.value(), 1);
                assert_eq!(cache_name, Some("test-cache".to_string()));
                assert_eq!(key.as_ref(), b"my-key");
            }
            _ => panic!("expected Get request"),
        }
    }

    #[test]
    fn test_parse_request_get_no_cache_name() {
        let get_req = GetRequest { cache_key: b"key" };
        let encoded = get_req.encode();

        let request = Request {
            stream_id: StreamId::new(3),
            path: "/cache_client.Scs/Get".to_string(),
            metadata: Metadata::new(),
            message: Bytes::from(encoded),
        };

        let result = CacheServer::parse_grpc_request(request);
        assert!(result.is_some());
        match result.unwrap() {
            CacheRequest::Get { cache_name, .. } => {
                assert!(cache_name.is_none());
            }
            _ => panic!("expected Get request"),
        }
    }

    #[test]
    fn test_parse_request_set() {
        let set_req = SetRequest {
            cache_key: b"key",
            cache_body: b"value",
            ttl_milliseconds: 60000,
        };
        let encoded = set_req.encode();

        let mut metadata = Metadata::new();
        metadata.insert("cache".to_string(), "my-cache".to_string());

        let request = Request {
            stream_id: StreamId::new(5),
            path: "/cache_client.Scs/Set".to_string(),
            metadata,
            message: Bytes::from(encoded),
        };

        let result = CacheServer::parse_grpc_request(request);
        assert!(result.is_some());
        match result.unwrap() {
            CacheRequest::Set {
                request_id,
                cache_name,
                key,
                value,
                ttl_ms,
            } => {
                assert_eq!(request_id.value(), 5);
                assert_eq!(cache_name, Some("my-cache".to_string()));
                assert_eq!(key.as_ref(), b"key");
                assert_eq!(value.as_ref(), b"value");
                assert_eq!(ttl_ms, 60000);
            }
            _ => panic!("expected Set request"),
        }
    }

    #[test]
    fn test_parse_request_delete() {
        let del_req = DeleteRequest {
            cache_key: b"delete-key",
        };
        let encoded = del_req.encode();

        let request = Request {
            stream_id: StreamId::new(7),
            path: "/cache_client.Scs/Delete".to_string(),
            metadata: Metadata::new(),
            message: Bytes::from(encoded),
        };

        let result = CacheServer::parse_grpc_request(request);
        assert!(result.is_some());
        match result.unwrap() {
            CacheRequest::Delete {
                request_id,
                cache_name,
                key,
            } => {
                assert_eq!(request_id.value(), 7);
                assert!(cache_name.is_none());
                assert_eq!(key.as_ref(), b"delete-key");
            }
            _ => panic!("expected Delete request"),
        }
    }

    #[test]
    fn test_parse_request_unknown_method() {
        let request = Request {
            stream_id: StreamId::new(1),
            path: "/cache_client.Scs/Unknown".to_string(),
            metadata: Metadata::new(),
            message: Bytes::new(),
        };

        let result = CacheServer::parse_grpc_request(request);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_request_invalid_get_message() {
        // Invalid protobuf data
        let request = Request {
            stream_id: StreamId::new(1),
            path: "/cache_client.Scs/Get".to_string(),
            metadata: Metadata::new(),
            message: Bytes::from_static(&[0xFF, 0xFF, 0xFF]),
        };

        let result = CacheServer::parse_grpc_request(request);
        // Should return None because decode fails
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_request_invalid_set_message() {
        let request = Request {
            stream_id: StreamId::new(1),
            path: "/cache_client.Scs/Set".to_string(),
            metadata: Metadata::new(),
            message: Bytes::from_static(&[0xFF, 0xFF, 0xFF]),
        };

        let result = CacheServer::parse_grpc_request(request);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_request_invalid_delete_message() {
        let request = Request {
            stream_id: StreamId::new(1),
            path: "/cache_client.Scs/Delete".to_string(),
            metadata: Metadata::new(),
            message: Bytes::from_static(&[0xFF, 0xFF, 0xFF]),
        };

        let result = CacheServer::parse_grpc_request(request);
        assert!(result.is_none());
    }

    // send_response tests

    #[test]
    fn test_send_response_get_hit() {
        let mut server = CacheServer::new();

        let response = CacheResponse::Get(GetResponse::Hit(Bytes::from_static(b"value")));
        // This will fail because server isn't ready, but we're testing the encoding path
        let result = server.send_response(RequestId::new(1), response);
        // Expected to fail because server state isn't ready
        assert!(result.is_err());
    }

    #[test]
    fn test_send_response_get_miss() {
        let mut server = CacheServer::new();

        let response = CacheResponse::Get(GetResponse::Miss);
        let result = server.send_response(RequestId::new(1), response);
        assert!(result.is_err());
    }

    #[test]
    fn test_send_response_set_ok() {
        let mut server = CacheServer::new();

        let response = CacheResponse::Set(SetResponse::Ok);
        let result = server.send_response(RequestId::new(1), response);
        assert!(result.is_err());
    }

    #[test]
    fn test_send_response_set_error() {
        let mut server = CacheServer::new();

        let response = CacheResponse::Set(SetResponse::Error {
            message: "error".to_string(),
        });
        let result = server.send_response(RequestId::new(1), response);
        assert!(result.is_err());
    }

    #[test]
    fn test_send_response_delete_ok() {
        let mut server = CacheServer::new();

        let response = CacheResponse::Delete(DeleteResponse::Ok);
        let result = server.send_response(RequestId::new(1), response);
        assert!(result.is_err());
    }

    #[test]
    fn test_send_error() {
        let mut server = CacheServer::new();

        let result = server.send_error(RequestId::new(1), Status::not_found("key not found"));
        // Expected to fail because server isn't ready
        assert!(result.is_err());
    }

    // CacheResponse encoding tests

    #[test]
    fn test_cache_response_get_hit_encoding() {
        let response = CacheResponse::Get(GetResponse::Hit(Bytes::from_static(b"data")));
        match response {
            CacheResponse::Get(r) => {
                let encoded = r.encode();
                assert!(!encoded.is_empty());
            }
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_cache_response_get_miss_encoding() {
        let response = CacheResponse::Get(GetResponse::Miss);
        match response {
            CacheResponse::Get(r) => {
                let encoded = r.encode();
                assert!(!encoded.is_empty());
            }
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_cache_response_get_error_encoding() {
        let response = CacheResponse::Get(GetResponse::Error {
            message: "error".to_string(),
        });
        match response {
            CacheResponse::Get(r) => {
                let encoded = r.encode();
                assert!(!encoded.is_empty());
            }
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_cache_response_set_error_encoding() {
        let response = CacheResponse::Set(SetResponse::Error {
            message: "set failed".to_string(),
        });
        match response {
            CacheResponse::Set(r) => {
                let encoded = r.encode();
                assert!(!encoded.is_empty());
            }
            _ => panic!("expected Set"),
        }
    }

    // Protosocket server tests

    #[test]
    fn test_protosocket_server_auth_flow() {
        let mut server = CacheServer::builder()
            .wire_format(WireFormat::Protosocket)
            .build();
        assert!(!server.is_ready());
        assert!(server.auth_token().is_none());

        // Send authenticate command
        let auth_cmd = CacheCommand::new(
            1,
            UnaryCommand::Authenticate {
                auth_token: "test-token".to_string(),
            },
        );
        server.on_recv(&auth_cmd.encode_length_delimited());

        let events = server.process().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], CacheServerEvent::Ready));
        assert!(server.is_ready());
        assert_eq!(server.auth_token(), Some("test-token"));

        // Should have auth response pending
        assert!(server.has_pending_send());
    }

    #[test]
    fn test_protosocket_server_get_request() {
        let mut server = CacheServer::builder()
            .wire_format(WireFormat::Protosocket)
            .build();

        // Authenticate first
        let auth_cmd = CacheCommand::new(
            1,
            UnaryCommand::Authenticate {
                auth_token: "token".to_string(),
            },
        );
        server.on_recv(&auth_cmd.encode_length_delimited());
        let _ = server.process().unwrap();

        // Clear auth response
        let pending_len = server.pending_send().len();
        server.advance_send(pending_len);

        // Send get command
        let get_cmd = CacheCommand::new(
            2,
            UnaryCommand::Get {
                namespace: "cache".to_string(),
                key: Bytes::from_static(b"key"),
            },
        );
        server.on_recv(&get_cmd.encode_length_delimited());

        let events = server.process().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            CacheServerEvent::Request(CacheRequest::Get {
                request_id,
                cache_name,
                key,
            }) => {
                assert_eq!(request_id.value(), 2);
                assert_eq!(cache_name.as_deref(), Some("cache"));
                assert_eq!(key.as_ref(), b"key");
            }
            _ => panic!("expected Get request"),
        }
    }

    #[test]
    fn test_protosocket_server_send_response() {
        let mut server = CacheServer::builder()
            .wire_format(WireFormat::Protosocket)
            .build();

        // Authenticate
        let auth_cmd = CacheCommand::new(
            1,
            UnaryCommand::Authenticate {
                auth_token: "token".to_string(),
            },
        );
        server.on_recv(&auth_cmd.encode_length_delimited());
        let _ = server.process().unwrap();

        // Clear auth response
        let pending_len = server.pending_send().len();
        server.advance_send(pending_len);

        // Send response
        let response = CacheResponse::Get(GetResponse::Hit(Bytes::from_static(b"value")));
        let result = server.send_response(RequestId::new(2), response);
        assert!(result.is_ok());
        assert!(server.has_pending_send());
    }

    #[test]
    fn test_protosocket_server_cancel_request() {
        let mut server = CacheServer::builder()
            .wire_format(WireFormat::Protosocket)
            .build();

        // Authenticate
        let auth_cmd = CacheCommand::new(
            1,
            UnaryCommand::Authenticate {
                auth_token: "token".to_string(),
            },
        );
        server.on_recv(&auth_cmd.encode_length_delimited());
        let _ = server.process().unwrap();

        // Send cancel command
        let cancel_cmd = CacheCommand::cancel(2);
        server.on_recv(&cancel_cmd.encode_length_delimited());

        let events = server.process().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            CacheServerEvent::RequestCancelled { request_id } => {
                assert_eq!(request_id.value(), 2);
            }
            _ => panic!("expected RequestCancelled"),
        }
    }

    #[test]
    fn test_protosocket_server_send_error() {
        let mut server = CacheServer::builder()
            .wire_format(WireFormat::Protosocket)
            .build();

        // Authenticate
        let auth_cmd = CacheCommand::new(
            1,
            UnaryCommand::Authenticate {
                auth_token: "token".to_string(),
            },
        );
        server.on_recv(&auth_cmd.encode_length_delimited());
        let _ = server.process().unwrap();

        // Clear auth response
        let pending_len = server.pending_send().len();
        server.advance_send(pending_len);

        // Send error
        let result = server.send_error(RequestId::new(2), Status::not_found("not found"));
        assert!(result.is_ok());
        assert!(server.has_pending_send());
    }

    #[test]
    fn test_parse_protosocket_command_set() {
        let command = UnaryCommand::Set {
            namespace: "cache".to_string(),
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
            ttl_millis: 60000,
        };

        let result = CacheServer::parse_protosocket_command(42, command);
        assert!(result.is_some());
        match result.unwrap() {
            CacheRequest::Set {
                request_id,
                cache_name,
                key,
                value,
                ttl_ms,
            } => {
                assert_eq!(request_id.value(), 42);
                assert_eq!(cache_name.as_deref(), Some("cache"));
                assert_eq!(key.as_ref(), b"key");
                assert_eq!(value.as_ref(), b"value");
                assert_eq!(ttl_ms, 60000);
            }
            _ => panic!("expected Set request"),
        }
    }

    #[test]
    fn test_parse_protosocket_command_delete() {
        let command = UnaryCommand::Delete {
            namespace: "cache".to_string(),
            key: Bytes::from_static(b"key"),
        };

        let result = CacheServer::parse_protosocket_command(99, command);
        assert!(result.is_some());
        match result.unwrap() {
            CacheRequest::Delete {
                request_id,
                cache_name,
                key,
            } => {
                assert_eq!(request_id.value(), 99);
                assert_eq!(cache_name.as_deref(), Some("cache"));
                assert_eq!(key.as_ref(), b"key");
            }
            _ => panic!("expected Delete request"),
        }
    }
}
