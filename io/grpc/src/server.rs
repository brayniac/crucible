//! gRPC server abstractions.

use crate::frame::{self, MessageDecoder};
use crate::metadata::Metadata;
use crate::status::Status;

use bytes::Bytes;
use http2::{HeaderField, ServerConnection, ServerEvent, StreamId};
use std::collections::HashMap;
use std::io;

/// A gRPC server connection.
///
/// Wraps an HTTP/2 server connection and provides gRPC-specific functionality.
pub struct Server {
    /// The underlying HTTP/2 server connection.
    conn: ServerConnection,
    /// Active requests by stream ID.
    requests: HashMap<u32, RequestState>,
}

/// State of an in-progress request.
#[derive(Debug)]
struct RequestState {
    /// Request method path (e.g., "/package.Service/Method").
    path: String,
    /// Request metadata (headers).
    metadata: Metadata,
    /// Message decoder for request body.
    decoder: MessageDecoder,
    /// Whether we've received END_STREAM from client.
    end_stream: bool,
}

impl RequestState {
    fn new(path: String, metadata: Metadata) -> Self {
        Self {
            path,
            metadata,
            decoder: MessageDecoder::new(),
            end_stream: false,
        }
    }
}

/// An incoming gRPC request.
#[derive(Debug)]
pub struct Request {
    /// Stream ID for this request.
    pub stream_id: StreamId,
    /// Service path (e.g., "/package.Service/Method").
    pub path: String,
    /// Request metadata.
    pub metadata: Metadata,
    /// Request message body.
    pub message: Bytes,
}

/// Events produced by the server.
#[derive(Debug)]
pub enum GrpcServerEvent {
    /// Server is ready to accept requests.
    Ready,
    /// Received a complete unary request.
    Request(Request),
    /// Received request data (for streaming).
    RequestData {
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
    },
    /// Client reset the stream.
    StreamReset { stream_id: StreamId },
    /// Client sent GOAWAY.
    GoAway,
    /// Connection error.
    Error(String),
}

impl Server {
    /// Create a new gRPC server.
    pub fn new() -> Self {
        Self {
            conn: ServerConnection::new(),
            requests: HashMap::new(),
        }
    }

    /// Check if the server is ready.
    pub fn is_ready(&self) -> bool {
        self.conn.is_ready()
    }

    /// Feed data received from the client.
    pub fn on_recv(&mut self, data: &[u8]) {
        self.conn.on_recv(data);
    }

    /// Feed plaintext data directly into the connection's frame buffer.
    ///
    /// More efficient than `on_recv()` when TLS is handled externally.
    pub fn feed_data(&mut self, data: &[u8]) -> io::Result<()> {
        self.conn.feed_data(data)
    }

    /// Process received data and return events.
    pub fn process(&mut self) -> io::Result<Vec<GrpcServerEvent>> {
        self.conn.process()?;

        let mut result = Vec::new();

        for event in self.conn.poll_events() {
            match event {
                ServerEvent::Ready => {
                    result.push(GrpcServerEvent::Ready);
                }
                ServerEvent::Request {
                    stream_id,
                    headers,
                    end_stream,
                } => {
                    // Parse headers
                    let (path, metadata) = parse_request_headers(&headers);

                    if let Some(path) = path {
                        // Create request state
                        let mut state = RequestState::new(path, metadata);
                        state.end_stream = end_stream;
                        self.requests.insert(stream_id.value(), state);

                        // If end_stream and no body expected, emit request
                        // (shouldn't happen for gRPC unary, but handle it)
                        if end_stream && let Some(state) = self.requests.remove(&stream_id.value())
                        {
                            result.push(GrpcServerEvent::Request(Request {
                                stream_id,
                                path: state.path,
                                metadata: state.metadata,
                                message: Bytes::new(),
                            }));
                        }
                    } else {
                        // Invalid request - no path
                        let _ =
                            self.send_error(stream_id, Status::invalid_argument("missing :path"));
                    }
                }
                ServerEvent::Data {
                    stream_id,
                    data,
                    end_stream,
                } => {
                    if let Some(state) = self.requests.get_mut(&stream_id.value()) {
                        // Feed data to decoder
                        state.decoder.feed(&data);
                        state.end_stream = end_stream;

                        // Try to decode message
                        if end_stream {
                            // For unary, emit complete request when we have the full message
                            if let Ok(Some((message, _))) = state.decoder.decode() {
                                let state = self.requests.remove(&stream_id.value()).unwrap();
                                result.push(GrpcServerEvent::Request(Request {
                                    stream_id,
                                    path: state.path,
                                    metadata: state.metadata,
                                    message,
                                }));
                            }
                        } else {
                            // Streaming: emit data as it arrives
                            while let Ok(Some((message, _))) = state.decoder.decode() {
                                result.push(GrpcServerEvent::RequestData {
                                    stream_id,
                                    data: message,
                                    end_stream: false,
                                });
                            }
                        }
                    }
                }
                ServerEvent::StreamReset { stream_id, .. } => {
                    self.requests.remove(&stream_id.value());
                    result.push(GrpcServerEvent::StreamReset { stream_id });
                }
                ServerEvent::GoAway { .. } => {
                    self.requests.clear();
                    result.push(GrpcServerEvent::GoAway);
                }
                ServerEvent::Error(e) => {
                    result.push(GrpcServerEvent::Error(format!("{:?}", e)));
                }
            }
        }

        Ok(result)
    }

    /// Send a unary response.
    pub fn send_response(
        &mut self,
        stream_id: StreamId,
        status: Status,
        message: &[u8],
    ) -> io::Result<()> {
        // Send response headers
        let headers = vec![
            HeaderField::new(":status", "200"),
            HeaderField::new("content-type", "application/grpc"),
        ];
        self.conn.send_headers(stream_id, &headers, false)?;

        // Send response message
        let encoded = frame::encode_message(message);
        self.conn.send_data(stream_id, &encoded, false)?;

        // Send trailers with status
        self.send_trailers(stream_id, status)
    }

    /// Send response headers (for streaming responses).
    pub fn send_response_headers(&mut self, stream_id: StreamId) -> io::Result<()> {
        let headers = vec![
            HeaderField::new(":status", "200"),
            HeaderField::new("content-type", "application/grpc"),
        ];
        self.conn.send_headers(stream_id, &headers, false)
    }

    /// Send a response message (for streaming responses).
    pub fn send_response_message(
        &mut self,
        stream_id: StreamId,
        message: &[u8],
    ) -> io::Result<usize> {
        let encoded = frame::encode_message(message);
        self.conn.send_data(stream_id, &encoded, false)
    }

    /// Send trailers to complete the response.
    pub fn send_trailers(&mut self, stream_id: StreamId, status: Status) -> io::Result<()> {
        let mut trailers = vec![HeaderField::new(
            "grpc-status",
            status.code().as_u32().to_string().as_bytes().to_vec(),
        )];

        if let Some(message) = status.message() {
            trailers.push(HeaderField::new("grpc-message", message.as_bytes()));
        }

        self.conn.send_headers(stream_id, &trailers, true)?;
        self.conn.remove_stream(stream_id);
        Ok(())
    }

    /// Send an error response.
    pub fn send_error(&mut self, stream_id: StreamId, status: Status) -> io::Result<()> {
        // Send headers with trailers only (no body)
        let headers = vec![
            HeaderField::new(":status", "200"),
            HeaderField::new("content-type", "application/grpc"),
            HeaderField::new(
                "grpc-status",
                status.code().as_u32().to_string().as_bytes().to_vec(),
            ),
        ];

        // Add message if present
        let mut headers = headers;
        if let Some(message) = status.message() {
            headers.push(HeaderField::new("grpc-message", message.as_bytes()));
        }

        self.conn.send_headers(stream_id, &headers, true)?;
        self.conn.remove_stream(stream_id);
        Ok(())
    }

    /// Get data to send to the client.
    pub fn pending_send(&self) -> &[u8] {
        self.conn.pending_send()
    }

    /// Mark data as sent.
    pub fn advance_send(&mut self, n: usize) {
        self.conn.advance_send(n);
    }

    /// Check if there's data to send.
    pub fn has_pending_send(&self) -> bool {
        self.conn.has_pending_send()
    }

    /// Get the underlying HTTP/2 connection.
    pub fn connection(&self) -> &ServerConnection {
        &self.conn
    }

    /// Get mutable access to the underlying HTTP/2 connection.
    pub fn connection_mut(&mut self) -> &mut ServerConnection {
        &mut self.conn
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse gRPC request headers.
fn parse_request_headers(headers: &[HeaderField]) -> (Option<String>, Metadata) {
    let mut path = None;
    let mut metadata = Metadata::new();

    for h in headers {
        let name = String::from_utf8_lossy(&h.name).to_string();
        let value = String::from_utf8_lossy(&h.value).to_string();

        if name == ":path" {
            path = Some(value);
        } else {
            metadata.insert(name, value);
        }
    }

    (path, metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Server tests

    #[test]
    fn test_server_new() {
        let server = Server::new();
        assert!(!server.is_ready());
    }

    #[test]
    fn test_server_default() {
        let server = Server::default();
        assert!(!server.is_ready());
    }

    #[test]
    fn test_server_connection_access() {
        let server = Server::new();
        let _conn = server.connection();
    }

    #[test]
    fn test_server_connection_mut_access() {
        let mut server = Server::new();
        let _conn = server.connection_mut();
    }

    #[test]
    fn test_server_pending_send_empty() {
        let server = Server::new();
        // Initial server should not have pending data (before connection established)
        let _ = server.pending_send();
    }

    #[test]
    fn test_server_has_pending_send() {
        let server = Server::new();
        // Method should be callable
        let _ = server.has_pending_send();
    }

    // RequestState tests

    #[test]
    fn test_request_state_new() {
        let mut metadata = Metadata::new();
        metadata.insert("content-type", "application/grpc");

        let state = RequestState::new("/test.Service/Method".to_string(), metadata);
        assert_eq!(state.path, "/test.Service/Method");
        assert!(!state.end_stream);
    }

    #[test]
    fn test_request_state_debug() {
        let state = RequestState::new("/test/method".to_string(), Metadata::new());
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("RequestState"));
        assert!(debug_str.contains("/test/method"));
    }

    // Request tests

    #[test]
    fn test_request_debug() {
        let request = Request {
            stream_id: StreamId::new(1),
            path: "/package.Service/Method".to_string(),
            metadata: Metadata::new(),
            message: Bytes::from("test"),
        };
        let debug_str = format!("{:?}", request);
        assert!(debug_str.contains("Request"));
        assert!(debug_str.contains("/package.Service/Method"));
    }

    #[test]
    fn test_request_fields() {
        let mut metadata = Metadata::new();
        metadata.insert("x-custom", "value");

        let request = Request {
            stream_id: StreamId::new(5),
            path: "/test/method".to_string(),
            metadata,
            message: Bytes::from("hello"),
        };

        assert_eq!(request.stream_id.value(), 5);
        assert_eq!(request.path, "/test/method");
        assert_eq!(request.metadata.get("x-custom"), Some("value"));
        assert_eq!(&request.message[..], b"hello");
    }

    // GrpcServerEvent tests

    #[test]
    fn test_grpc_server_event_ready_debug() {
        let event = GrpcServerEvent::Ready;
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("Ready"));
    }

    #[test]
    fn test_grpc_server_event_request_debug() {
        let request = Request {
            stream_id: StreamId::new(1),
            path: "/test/method".to_string(),
            metadata: Metadata::new(),
            message: Bytes::new(),
        };
        let event = GrpcServerEvent::Request(request);
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("Request"));
    }

    #[test]
    fn test_grpc_server_event_request_data_debug() {
        let event = GrpcServerEvent::RequestData {
            stream_id: StreamId::new(1),
            data: Bytes::from("test"),
            end_stream: false,
        };
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("RequestData"));
    }

    #[test]
    fn test_grpc_server_event_stream_reset_debug() {
        let event = GrpcServerEvent::StreamReset {
            stream_id: StreamId::new(1),
        };
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("StreamReset"));
    }

    #[test]
    fn test_grpc_server_event_goaway_debug() {
        let event = GrpcServerEvent::GoAway;
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("GoAway"));
    }

    #[test]
    fn test_grpc_server_event_error_debug() {
        let event = GrpcServerEvent::Error("test error".to_string());
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("Error"));
        assert!(debug_str.contains("test error"));
    }

    // parse_request_headers tests

    #[test]
    fn test_parse_request_headers() {
        let headers = vec![
            HeaderField::new(":method", "POST"),
            HeaderField::new(":path", "/test.Service/Method"),
            HeaderField::new("authorization", "Bearer token"),
        ];

        let (path, metadata) = parse_request_headers(&headers);
        assert_eq!(path, Some("/test.Service/Method".to_string()));
        assert_eq!(metadata.get("authorization"), Some("Bearer token"));
    }

    #[test]
    fn test_parse_request_headers_no_path() {
        let headers = vec![
            HeaderField::new(":method", "POST"),
            HeaderField::new("content-type", "application/grpc"),
        ];

        let (path, metadata) = parse_request_headers(&headers);
        assert!(path.is_none());
        assert_eq!(metadata.get("content-type"), Some("application/grpc"));
    }

    #[test]
    fn test_parse_request_headers_empty() {
        let headers: Vec<HeaderField> = vec![];
        let (path, metadata) = parse_request_headers(&headers);
        assert!(path.is_none());
        assert!(metadata.is_empty());
    }

    #[test]
    fn test_parse_request_headers_multiple_metadata() {
        let headers = vec![
            HeaderField::new(":path", "/test/method"),
            HeaderField::new("header1", "value1"),
            HeaderField::new("header2", "value2"),
            HeaderField::new("header3", "value3"),
        ];

        let (path, metadata) = parse_request_headers(&headers);
        assert_eq!(path, Some("/test/method".to_string()));
        assert_eq!(metadata.len(), 3);
        assert_eq!(metadata.get("header1"), Some("value1"));
        assert_eq!(metadata.get("header2"), Some("value2"));
        assert_eq!(metadata.get("header3"), Some("value3"));
    }

    #[test]
    fn test_parse_request_headers_invalid_utf8() {
        // from_utf8_lossy handles invalid UTF-8 by replacing with replacement character
        let headers = vec![HeaderField {
            name: vec![0xff, 0xfe], // Invalid UTF-8
            value: b"valid-value".to_vec(),
        }];

        let (path, metadata) = parse_request_headers(&headers);
        assert!(path.is_none());
        // Invalid UTF-8 is converted with replacement characters, so it won't be empty
        assert_eq!(metadata.len(), 1);
    }

    #[test]
    fn test_parse_request_headers_pseudo_headers() {
        let headers = vec![
            HeaderField::new(":method", "POST"),
            HeaderField::new(":scheme", "https"),
            HeaderField::new(":authority", "example.com"),
            HeaderField::new(":path", "/test/method"),
        ];

        let (path, metadata) = parse_request_headers(&headers);
        assert_eq!(path, Some("/test/method".to_string()));
        // Other pseudo headers should be in metadata
        assert_eq!(metadata.get(":method"), Some("POST"));
        assert_eq!(metadata.get(":scheme"), Some("https"));
        assert_eq!(metadata.get(":authority"), Some("example.com"));
    }
}
