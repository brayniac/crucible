//! Momento cache server implementation.
//!
//! Provides a gRPC server that implements the Momento cache API.

use crate::proto::{
    DeleteRequestOwned, DeleteResponse, GetRequestOwned, GetResponse, SetRequestOwned, SetResponse,
};
use bytes::Bytes;
use grpc::{GrpcServerEvent, Request, Server, Status, StreamId};
use std::io;

/// Method paths for the Momento cache API.
const METHOD_GET: &str = "/cache_client.Scs/Get";
const METHOD_SET: &str = "/cache_client.Scs/Set";
const METHOD_DELETE: &str = "/cache_client.Scs/Delete";

/// A Momento cache server connection.
///
/// Wraps a gRPC server and provides Momento-specific request parsing.
pub struct CacheServer {
    /// The underlying gRPC server.
    server: Server,
}

/// A parsed cache request.
#[derive(Debug)]
pub enum CacheRequest {
    /// Get request.
    Get {
        stream_id: StreamId,
        cache_name: Option<String>,
        key: Bytes,
    },
    /// Set request.
    Set {
        stream_id: StreamId,
        cache_name: Option<String>,
        key: Bytes,
        value: Bytes,
        ttl_ms: u64,
    },
    /// Delete request.
    Delete {
        stream_id: StreamId,
        cache_name: Option<String>,
        key: Bytes,
    },
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
    /// Client reset the stream.
    StreamReset { stream_id: StreamId },
    /// Client sent GOAWAY.
    GoAway,
    /// Connection error.
    Error(String),
}

impl CacheServer {
    /// Create a new Momento cache server.
    pub fn new() -> Self {
        Self {
            server: Server::new(),
        }
    }

    /// Check if the server is ready.
    pub fn is_ready(&self) -> bool {
        self.server.is_ready()
    }

    /// Feed data received from the client.
    pub fn on_recv(&mut self, data: &[u8]) {
        self.server.on_recv(data);
    }

    /// Process received data and return events.
    pub fn process(&mut self) -> io::Result<Vec<CacheServerEvent>> {
        let grpc_events = self.server.process()?;
        let mut result = Vec::new();

        for event in grpc_events {
            match event {
                GrpcServerEvent::Ready => {
                    result.push(CacheServerEvent::Ready);
                }
                GrpcServerEvent::Request(request) => {
                    if let Some(cache_request) = self.parse_request(request) {
                        result.push(CacheServerEvent::Request(cache_request));
                    }
                }
                GrpcServerEvent::RequestData { .. } => {
                    // Streaming requests not supported for cache operations
                }
                GrpcServerEvent::StreamReset { stream_id } => {
                    result.push(CacheServerEvent::StreamReset { stream_id });
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

    /// Parse a gRPC request into a cache request.
    fn parse_request(&self, request: Request) -> Option<CacheRequest> {
        // Extract cache name from metadata
        let cache_name = request.metadata.get("cache").map(|s| s.to_string());

        match request.path.as_str() {
            METHOD_GET => {
                let req = GetRequestOwned::decode(&request.message)?;
                Some(CacheRequest::Get {
                    stream_id: request.stream_id,
                    cache_name,
                    key: req.cache_key,
                })
            }
            METHOD_SET => {
                let req = SetRequestOwned::decode(&request.message)?;
                Some(CacheRequest::Set {
                    stream_id: request.stream_id,
                    cache_name,
                    key: req.cache_key,
                    value: req.cache_body,
                    ttl_ms: req.ttl_milliseconds,
                })
            }
            METHOD_DELETE => {
                let req = DeleteRequestOwned::decode(&request.message)?;
                Some(CacheRequest::Delete {
                    stream_id: request.stream_id,
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

    /// Send a response for a cache request.
    pub fn send_response(
        &mut self,
        stream_id: StreamId,
        response: CacheResponse,
    ) -> io::Result<()> {
        let message = match response {
            CacheResponse::Get(r) => r.encode(),
            CacheResponse::Set(r) => r.encode(),
            CacheResponse::Delete(r) => r.encode(),
        };

        self.server.send_response(stream_id, Status::ok(), &message)
    }

    /// Send an error response.
    pub fn send_error(&mut self, stream_id: StreamId, status: Status) -> io::Result<()> {
        self.server.send_error(stream_id, status)
    }

    /// Get data to send to the client.
    pub fn pending_send(&self) -> &[u8] {
        self.server.pending_send()
    }

    /// Mark data as sent.
    pub fn advance_send(&mut self, n: usize) {
        self.server.advance_send(n);
    }

    /// Check if there's data to send.
    pub fn has_pending_send(&self) -> bool {
        self.server.has_pending_send()
    }

    /// Get the underlying gRPC server.
    pub fn grpc_server(&self) -> &Server {
        &self.server
    }

    /// Get mutable access to the underlying gRPC server.
    pub fn grpc_server_mut(&mut self) -> &mut Server {
        &mut self.server
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
    use crate::proto::{DeleteRequest, GetRequest, SetRequest};
    use grpc::{Metadata, Request};

    // Test helper to expose private parse_request method
    impl CacheServer {
        fn test_parse_request(&self, request: Request) -> Option<CacheRequest> {
            self.parse_request(request)
        }
    }

    #[test]
    fn test_server_new() {
        let server = CacheServer::new();
        assert!(!server.is_ready());
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
        let _ = server.grpc_server();

        // Test mutable access
        let _ = server.grpc_server_mut();
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
            stream_id: StreamId::new(1),
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
            stream_id: StreamId::new(1),
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
            stream_id: StreamId::new(1),
            cache_name: None,
            key: Bytes::from_static(b"key"),
        };
        let debug = format!("{:?}", request);
        assert!(debug.contains("Delete"));
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
            stream_id: StreamId::new(1),
            cache_name: None,
            key: Bytes::from_static(b"key"),
        };
        let event = CacheServerEvent::Request(request);
        let debug = format!("{:?}", event);
        assert!(debug.contains("Request"));
    }

    #[test]
    fn test_cache_server_event_stream_reset_debug() {
        let event = CacheServerEvent::StreamReset {
            stream_id: StreamId::new(1),
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("StreamReset"));
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

    // parse_request tests

    #[test]
    fn test_parse_request_get() {
        let server = CacheServer::new();

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

        let result = server.test_parse_request(request);
        assert!(result.is_some());
        match result.unwrap() {
            CacheRequest::Get {
                stream_id,
                cache_name,
                key,
            } => {
                assert_eq!(stream_id.value(), 1);
                assert_eq!(cache_name, Some("test-cache".to_string()));
                assert_eq!(key.as_ref(), b"my-key");
            }
            _ => panic!("expected Get request"),
        }
    }

    #[test]
    fn test_parse_request_get_no_cache_name() {
        let server = CacheServer::new();

        let get_req = GetRequest { cache_key: b"key" };
        let encoded = get_req.encode();

        let request = Request {
            stream_id: StreamId::new(3),
            path: "/cache_client.Scs/Get".to_string(),
            metadata: Metadata::new(),
            message: Bytes::from(encoded),
        };

        let result = server.test_parse_request(request);
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
        let server = CacheServer::new();

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

        let result = server.test_parse_request(request);
        assert!(result.is_some());
        match result.unwrap() {
            CacheRequest::Set {
                stream_id,
                cache_name,
                key,
                value,
                ttl_ms,
            } => {
                assert_eq!(stream_id.value(), 5);
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
        let server = CacheServer::new();

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

        let result = server.test_parse_request(request);
        assert!(result.is_some());
        match result.unwrap() {
            CacheRequest::Delete {
                stream_id,
                cache_name,
                key,
            } => {
                assert_eq!(stream_id.value(), 7);
                assert!(cache_name.is_none());
                assert_eq!(key.as_ref(), b"delete-key");
            }
            _ => panic!("expected Delete request"),
        }
    }

    #[test]
    fn test_parse_request_unknown_method() {
        let server = CacheServer::new();

        let request = Request {
            stream_id: StreamId::new(1),
            path: "/cache_client.Scs/Unknown".to_string(),
            metadata: Metadata::new(),
            message: Bytes::new(),
        };

        let result = server.test_parse_request(request);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_request_invalid_get_message() {
        let server = CacheServer::new();

        // Invalid protobuf data
        let request = Request {
            stream_id: StreamId::new(1),
            path: "/cache_client.Scs/Get".to_string(),
            metadata: Metadata::new(),
            message: Bytes::from_static(&[0xFF, 0xFF, 0xFF]),
        };

        let result = server.test_parse_request(request);
        // Should return None because decode fails
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_request_invalid_set_message() {
        let server = CacheServer::new();

        let request = Request {
            stream_id: StreamId::new(1),
            path: "/cache_client.Scs/Set".to_string(),
            metadata: Metadata::new(),
            message: Bytes::from_static(&[0xFF, 0xFF, 0xFF]),
        };

        let result = server.test_parse_request(request);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_request_invalid_delete_message() {
        let server = CacheServer::new();

        let request = Request {
            stream_id: StreamId::new(1),
            path: "/cache_client.Scs/Delete".to_string(),
            metadata: Metadata::new(),
            message: Bytes::from_static(&[0xFF, 0xFF, 0xFF]),
        };

        let result = server.test_parse_request(request);
        assert!(result.is_none());
    }

    // send_response tests

    #[test]
    fn test_send_response_get_hit() {
        let mut server = CacheServer::new();

        let response = CacheResponse::Get(GetResponse::Hit(Bytes::from_static(b"value")));
        // This will fail because server isn't ready, but we're testing the encoding path
        let result = server.send_response(StreamId::new(1), response);
        // Expected to fail because server state isn't ready
        assert!(result.is_err());
    }

    #[test]
    fn test_send_response_get_miss() {
        let mut server = CacheServer::new();

        let response = CacheResponse::Get(GetResponse::Miss);
        let result = server.send_response(StreamId::new(1), response);
        assert!(result.is_err());
    }

    #[test]
    fn test_send_response_set_ok() {
        let mut server = CacheServer::new();

        let response = CacheResponse::Set(SetResponse::Ok);
        let result = server.send_response(StreamId::new(1), response);
        assert!(result.is_err());
    }

    #[test]
    fn test_send_response_set_error() {
        let mut server = CacheServer::new();

        let response = CacheResponse::Set(SetResponse::Error {
            message: "error".to_string(),
        });
        let result = server.send_response(StreamId::new(1), response);
        assert!(result.is_err());
    }

    #[test]
    fn test_send_response_delete_ok() {
        let mut server = CacheServer::new();

        let response = CacheResponse::Delete(DeleteResponse::Ok);
        let result = server.send_response(StreamId::new(1), response);
        assert!(result.is_err());
    }

    #[test]
    fn test_send_error() {
        let mut server = CacheServer::new();

        let result = server.send_error(StreamId::new(1), Status::not_found("key not found"));
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
}
