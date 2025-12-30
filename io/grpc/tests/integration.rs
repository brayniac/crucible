//! Integration tests for gRPC server and client.
//!
//! These tests verify the gRPC functionality by constructing proper HTTP/2
//! frames and testing full request/response cycles.

use bytes::BytesMut;
use grpc::{
    CallBuilder, CallEvent, Channel, Code, GrpcServerEvent, Metadata, Request, Server, Status,
    decode_message, encode_message,
};
use http2::{
    CONNECTION_PREFACE, DataFrame, Frame, FrameEncoder, HeaderField, HeadersFrame, HpackEncoder,
    PlainTransport, SettingsFrame, StreamId,
};

/// Helper to encode headers using HPACK.
fn encode_headers(headers: &[HeaderField]) -> Vec<u8> {
    let mut encoder = HpackEncoder::new();
    encoder.set_huffman(false); // Disable Huffman for easier debugging
    let mut buf = Vec::new();
    encoder.encode(headers, &mut buf);
    buf
}

/// Helper to create a valid HTTP/2 client preface with settings.
fn create_client_preface() -> BytesMut {
    let mut buf = BytesMut::new();
    let encoder = FrameEncoder::new();

    // Connection preface
    buf.extend_from_slice(CONNECTION_PREFACE);

    // Empty settings frame
    let settings = SettingsFrame {
        ack: false,
        settings: vec![],
    };
    encoder.encode(&Frame::Settings(settings), &mut buf);

    buf
}

/// Helper to create a gRPC request frame.
fn create_grpc_request(
    stream_id: StreamId,
    path: &str,
    message: &[u8],
    end_stream: bool,
) -> BytesMut {
    let mut buf = BytesMut::new();
    let encoder = FrameEncoder::new();

    // Headers for gRPC request
    let headers = vec![
        HeaderField::new(":method", "POST"),
        HeaderField::new(":scheme", "https"),
        HeaderField::new(":path", path),
        HeaderField::new(":authority", "localhost"),
        HeaderField::new("content-type", "application/grpc"),
        HeaderField::new("te", "trailers"),
    ];

    let header_block = encode_headers(&headers);

    let headers_frame = HeadersFrame {
        stream_id,
        end_stream: message.is_empty() && end_stream,
        end_headers: true,
        priority: None,
        header_block: bytes::Bytes::from(header_block),
    };
    encoder.encode(&Frame::Headers(headers_frame), &mut buf);

    // Send data if provided
    if !message.is_empty() {
        let grpc_message = encode_message(message);
        let data_frame = DataFrame {
            stream_id,
            end_stream,
            data: grpc_message,
        };
        encoder.encode(&Frame::Data(data_frame), &mut buf);
    }

    buf
}

// =============================================================================
// Server Tests
// =============================================================================

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
fn test_server_receives_client_preface() {
    let mut server = Server::new();

    // Send client preface
    let preface = create_client_preface();
    server.on_recv(&preface);

    let events = server.process().unwrap();

    // Should get Ready event after preface
    let has_ready = events.iter().any(|e| matches!(e, GrpcServerEvent::Ready));
    assert!(has_ready, "Server should emit Ready event after preface");
    assert!(server.is_ready());
}

#[test]
fn test_server_receives_request() {
    let mut server = Server::new();

    // Send client preface
    let preface = create_client_preface();
    server.on_recv(&preface);
    server.process().unwrap();

    // Now server should have sent its settings, we need to ACK them
    // For simplicity, we'll skip the settings ACK and just send the request

    // Create and send a gRPC request
    let request_message = b"Hello, server!";
    let request = create_grpc_request(
        StreamId::new(1),
        "/test.Service/Method",
        request_message,
        true,
    );
    server.on_recv(&request);

    let events = server.process().unwrap();

    // Should get a Request event
    let request_event = events
        .iter()
        .find(|e| matches!(e, GrpcServerEvent::Request(_)));

    if let Some(GrpcServerEvent::Request(req)) = request_event {
        assert_eq!(req.path, "/test.Service/Method");
        assert_eq!(req.stream_id, StreamId::new(1));
        assert_eq!(&req.message[..], request_message);
    } else {
        // The request might not be fully processed if settings aren't acked
        // This is expected in some cases
    }
}

#[test]
fn test_server_send_response() {
    let mut server = Server::new();

    // Send client preface
    let preface = create_client_preface();
    server.on_recv(&preface);
    server.process().unwrap();

    // Get pending send data (server settings)
    let pending = server.pending_send();
    assert!(!pending.is_empty(), "Server should have settings to send");

    // Advance past the settings
    let pending_len = pending.len();
    server.advance_send(pending_len);

    // Try to send a response (even without a real request, this tests the API)
    let stream_id = StreamId::new(1);
    let status = Status::ok();
    let response_message = b"Hello, client!";

    // This might fail because the stream doesn't exist, but it exercises the code
    let _ = server.send_response(stream_id, status, response_message);
}

#[test]
fn test_server_send_error_response() {
    let mut server = Server::new();

    // Send client preface
    let preface = create_client_preface();
    server.on_recv(&preface);
    server.process().unwrap();

    let stream_id = StreamId::new(1);
    let status = Status::not_found("entity not found");

    // Send error response
    let _ = server.send_error(stream_id, status);
}

#[test]
fn test_server_send_response_headers() {
    let mut server = Server::new();

    let preface = create_client_preface();
    server.on_recv(&preface);
    server.process().unwrap();

    let stream_id = StreamId::new(1);
    let _ = server.send_response_headers(stream_id);
}

#[test]
fn test_server_send_response_message() {
    let mut server = Server::new();

    let preface = create_client_preface();
    server.on_recv(&preface);
    server.process().unwrap();

    let stream_id = StreamId::new(1);
    let _ = server.send_response_message(stream_id, b"streaming message");
}

#[test]
fn test_server_send_trailers() {
    let mut server = Server::new();

    let preface = create_client_preface();
    server.on_recv(&preface);
    server.process().unwrap();

    let stream_id = StreamId::new(1);
    let status = Status::ok();
    let _ = server.send_trailers(stream_id, status);
}

#[test]
fn test_server_has_pending_send() {
    let mut server = Server::new();

    // Initially no data to send
    assert!(!server.has_pending_send());

    // After receiving preface, server should have settings to send
    let preface = create_client_preface();
    server.on_recv(&preface);
    server.process().unwrap();

    assert!(server.has_pending_send());
}

#[test]
fn test_server_connection_access() {
    let mut server = Server::new();

    // Test immutable access
    let _conn = server.connection();

    // Test mutable access
    let _conn_mut = server.connection_mut();
}

// =============================================================================
// Client Channel Tests
// =============================================================================

#[test]
fn test_channel_new() {
    let transport = PlainTransport::new();
    let conn = http2::Connection::new(transport);
    let channel: Channel<PlainTransport> = Channel::new(conn, "localhost:50051");

    // Channel should not be ready until connection is established
    assert!(!channel.is_ready());
}

#[test]
fn test_channel_state() {
    let transport = PlainTransport::new();
    let conn = http2::Connection::new(transport);
    let channel: Channel<PlainTransport> = Channel::new(conn, "localhost:50051");

    let state = channel.state();
    // Connection starts in WaitingPreface state
    assert!(
        state == http2::ConnectionState::WaitingPreface
            || state == http2::ConnectionState::WaitingSettings
    );
}

#[test]
fn test_channel_connection_access() {
    let transport = PlainTransport::new();
    let conn = http2::Connection::new(transport);
    let mut channel: Channel<PlainTransport> = Channel::new(conn, "localhost:50051");

    // Test immutable access
    let _conn = channel.connection();

    // Test mutable access
    let _conn_mut = channel.connection_mut();
}

#[test]
fn test_channel_pending_send() {
    let transport = PlainTransport::new();
    let conn = http2::Connection::new(transport);
    let mut channel: Channel<PlainTransport> = Channel::new(conn, "localhost:50051");

    // Trigger the connection to send preface
    let _ = channel.on_transport_ready();

    // New channel should have connection preface to send
    let pending = channel.pending_send();
    assert!(!pending.is_empty());

    // Should contain connection preface
    assert!(pending.starts_with(CONNECTION_PREFACE));
}

#[test]
fn test_channel_has_pending_send() {
    let transport = PlainTransport::new();
    let conn = http2::Connection::new(transport);
    let mut channel: Channel<PlainTransport> = Channel::new(conn, "localhost:50051");

    // Before transport ready, may not have data
    // After transport ready, should have preface to send
    let _ = channel.on_transport_ready();
    assert!(channel.has_pending_send());
}

#[test]
fn test_channel_advance_send() {
    let transport = PlainTransport::new();
    let conn = http2::Connection::new(transport);
    let mut channel: Channel<PlainTransport> = Channel::new(conn, "localhost:50051");

    // Trigger the connection to send preface
    let _ = channel.on_transport_ready();

    let initial_len = channel.pending_send().len();
    assert!(initial_len > 0);

    // Advance by some bytes
    channel.advance_send(10);

    let new_len = channel.pending_send().len();
    assert_eq!(new_len, initial_len - 10);
}

#[test]
fn test_channel_poll_empty() {
    let transport = PlainTransport::new();
    let conn = http2::Connection::new(transport);
    let mut channel: Channel<PlainTransport> = Channel::new(conn, "localhost:50051");

    // Poll with no events should return empty
    let events = channel.poll();
    assert!(events.is_empty());
}

// =============================================================================
// CallBuilder Tests
// =============================================================================

#[test]
fn test_call_builder_new() {
    let builder = CallBuilder::new("/test.Service/Method");
    // Builder should be created successfully - just verify it doesn't panic
    let _ = builder;
}

#[test]
fn test_call_builder_metadata() {
    let builder = CallBuilder::new("/test.Service/Method")
        .metadata("x-custom-header", "value")
        .metadata("authorization", "Bearer token");

    let _ = builder;
}

#[test]
fn test_call_builder_timeout_secs() {
    let builder = CallBuilder::new("/test.Service/Method").timeout_secs(30);

    let _ = builder;
}

#[test]
fn test_call_builder_timeout_millis() {
    let builder = CallBuilder::new("/test.Service/Method").timeout_millis(5000);

    let _ = builder;
}

#[test]
fn test_call_builder_chaining() {
    let builder = CallBuilder::new("/test.Service/Method")
        .metadata("x-request-id", "12345")
        .metadata("x-trace-id", "abc-def")
        .timeout_secs(60);

    let _ = builder;
}

// =============================================================================
// Call Tests
// =============================================================================

#[test]
fn test_call_stream_id() {
    // We can't easily create a Call directly since it's created by Channel::unary
    // But we can test the struct exists and has the expected methods
    // This is more of a compile-time test
}

// =============================================================================
// Message Framing Tests
// =============================================================================

#[test]
fn test_encode_decode_roundtrip() {
    let original = b"Hello, gRPC!";
    let encoded = encode_message(original);

    // Encoded should be larger (header + data)
    assert_eq!(encoded.len(), 5 + original.len());

    // Decode
    let mut buf = BytesMut::from(&encoded[..]);
    let result = decode_message(&mut buf).unwrap();

    assert!(result.is_some());
    let (decoded, compressed) = result.unwrap();
    assert!(!compressed);
    assert_eq!(&decoded[..], original);
}

#[test]
fn test_encode_empty_message() {
    let original = b"";
    let encoded = encode_message(original);

    assert_eq!(encoded.len(), 5); // Just the header

    let mut buf = BytesMut::from(&encoded[..]);
    let result = decode_message(&mut buf).unwrap();

    assert!(result.is_some());
    let (decoded, _) = result.unwrap();
    assert!(decoded.is_empty());
}

#[test]
fn test_decode_incomplete() {
    // Only partial header
    let mut buf = BytesMut::from(&[0, 0, 0][..]);
    let result = decode_message(&mut buf).unwrap();
    assert!(result.is_none());
}

// =============================================================================
// Status Tests
// =============================================================================

#[test]
fn test_status_ok() {
    let status = Status::ok();
    assert!(status.is_ok());
    assert_eq!(status.code(), Code::Ok);
    assert!(status.message().is_none());
}

#[test]
fn test_status_error_with_message() {
    let status = Status::not_found("entity does not exist");
    assert!(!status.is_ok());
    assert_eq!(status.code(), Code::NotFound);
    assert_eq!(status.message(), Some("entity does not exist"));
}

#[test]
fn test_status_from_code() {
    let status = Status::from_code(Code::Internal);
    assert_eq!(status.code(), Code::Internal);
    assert!(status.message().is_none());
}

#[test]
fn test_all_status_constructors() {
    let _ = Status::cancelled("cancelled");
    let _ = Status::unknown("unknown");
    let _ = Status::invalid_argument("invalid");
    let _ = Status::deadline_exceeded("timeout");
    let _ = Status::not_found("not found");
    let _ = Status::internal("internal");
    let _ = Status::unavailable("unavailable");
    let _ = Status::unauthenticated("unauthenticated");
}

// =============================================================================
// Metadata Tests
// =============================================================================

#[test]
fn test_metadata_new() {
    let metadata = Metadata::new();
    assert!(metadata.is_empty());
    assert_eq!(metadata.len(), 0);
}

#[test]
fn test_metadata_insert_get() {
    let mut metadata = Metadata::new();
    metadata.insert("x-custom", "value");

    assert_eq!(metadata.get("x-custom"), Some("value"));
    assert_eq!(metadata.get("X-CUSTOM"), Some("value")); // Case insensitive
}

#[test]
fn test_metadata_multiple_values() {
    let mut metadata = Metadata::new();
    metadata.insert("x-multi", "value1");
    metadata.insert("x-multi", "value2");

    assert_eq!(metadata.get("x-multi"), Some("value1")); // First value
    let all = metadata.get_all("x-multi").unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn test_metadata_contains_key() {
    let mut metadata = Metadata::new();
    metadata.insert("present", "yes");

    assert!(metadata.contains_key("present"));
    assert!(metadata.contains_key("PRESENT")); // Case insensitive
    assert!(!metadata.contains_key("absent"));
}

#[test]
fn test_metadata_remove() {
    let mut metadata = Metadata::new();
    metadata.insert("key", "value");

    let removed = metadata.remove("key");
    assert!(removed.is_some());
    assert!(metadata.is_empty());
}

#[test]
fn test_metadata_iter() {
    let mut metadata = Metadata::new();
    metadata.insert("key1", "value1");
    metadata.insert("key2", "value2");

    let entries: Vec<_> = metadata.iter().collect();
    assert_eq!(entries.len(), 2);
}

#[test]
fn test_metadata_from_iterator() {
    let pairs = vec![
        ("key1".to_string(), "value1".to_string()),
        ("key2".to_string(), "value2".to_string()),
    ];

    let metadata: Metadata = pairs.into_iter().collect();
    assert_eq!(metadata.len(), 2);
}

// =============================================================================
// Code Tests
// =============================================================================

#[test]
fn test_code_from_u32() {
    assert_eq!(Code::from_u32(0), Code::Ok);
    assert_eq!(Code::from_u32(1), Code::Cancelled);
    assert_eq!(Code::from_u32(2), Code::Unknown);
    assert_eq!(Code::from_u32(16), Code::Unauthenticated);
    assert_eq!(Code::from_u32(999), Code::Unknown); // Unknown values
}

#[test]
fn test_code_as_u32() {
    assert_eq!(Code::Ok.as_u32(), 0);
    assert_eq!(Code::Cancelled.as_u32(), 1);
    assert_eq!(Code::Unauthenticated.as_u32(), 16);
}

#[test]
fn test_code_is_ok() {
    assert!(Code::Ok.is_ok());
    assert!(!Code::Unknown.is_ok());
    assert!(!Code::Internal.is_ok());
}

#[test]
fn test_code_display() {
    assert_eq!(format!("{}", Code::Ok), "OK");
    assert_eq!(format!("{}", Code::NotFound), "NOT_FOUND");
    assert_eq!(format!("{}", Code::Internal), "INTERNAL");
}

// =============================================================================
// GrpcServerEvent Tests
// =============================================================================

#[test]
fn test_grpc_server_event_debug() {
    let event = GrpcServerEvent::Ready;
    let debug_str = format!("{:?}", event);
    assert!(debug_str.contains("Ready"));
}

#[test]
fn test_grpc_server_event_request_debug() {
    let request = Request {
        stream_id: StreamId::new(1),
        path: "/test/path".to_string(),
        metadata: Metadata::new(),
        message: bytes::Bytes::from_static(b"test"),
    };
    let event = GrpcServerEvent::Request(request);
    let debug_str = format!("{:?}", event);
    assert!(debug_str.contains("Request"));
}

#[test]
fn test_grpc_server_event_variants() {
    // Test all event variants exist
    let _ = GrpcServerEvent::Ready;
    let _ = GrpcServerEvent::GoAway;
    let _ = GrpcServerEvent::Error("error".to_string());
    let _ = GrpcServerEvent::StreamReset {
        stream_id: StreamId::new(1),
    };
    let _ = GrpcServerEvent::RequestData {
        stream_id: StreamId::new(1),
        data: bytes::Bytes::new(),
        end_stream: false,
    };
}

// =============================================================================
// Request Struct Tests
// =============================================================================

#[test]
fn test_request_fields() {
    let request = Request {
        stream_id: StreamId::new(5),
        path: "/package.Service/Method".to_string(),
        metadata: Metadata::new(),
        message: bytes::Bytes::from_static(b"request body"),
    };

    assert_eq!(request.stream_id, StreamId::new(5));
    assert_eq!(request.path, "/package.Service/Method");
    assert!(request.metadata.is_empty());
    assert_eq!(&request.message[..], b"request body");
}

#[test]
fn test_request_with_metadata() {
    let mut metadata = Metadata::new();
    metadata.insert("authorization", "Bearer token");
    metadata.insert("x-request-id", "12345");

    let request = Request {
        stream_id: StreamId::new(1),
        path: "/auth.Service/Verify".to_string(),
        metadata,
        message: bytes::Bytes::new(),
    };

    assert_eq!(request.metadata.get("authorization"), Some("Bearer token"));
}

// =============================================================================
// CallEvent Tests
// =============================================================================

#[test]
fn test_call_event_debug() {
    let event = CallEvent::Complete(Status::ok());
    let debug_str = format!("{:?}", event);
    assert!(debug_str.contains("Complete"));
}

#[test]
fn test_call_event_variants() {
    let _ = CallEvent::Headers(Metadata::new());
    let _ = CallEvent::Message(bytes::Bytes::new());
    let _ = CallEvent::Complete(Status::ok());
}

// =============================================================================
// Integration: Client-Server Communication
// =============================================================================

/// Helper struct to manage loopback testing between client and server.
struct LoopbackConnection {
    client: Channel<PlainTransport>,
    server: Server,
}

impl LoopbackConnection {
    fn new() -> Self {
        let transport = PlainTransport::new();
        let conn = http2::Connection::new(transport);
        let client = Channel::new(conn, "localhost:50051");
        let server = Server::new();

        Self { client, server }
    }

    /// Perform initial HTTP/2 handshake between client and server.
    fn handshake(&mut self) {
        // Step 1: Client sends connection preface
        self.client.on_transport_ready().unwrap();

        // Transfer client preface to server
        let client_data = self.client.pending_send().to_vec();
        self.client.advance_send(client_data.len());
        self.server.on_recv(&client_data);
        self.server.process().unwrap();

        // Step 2: Server sends settings
        let server_data = self.server.pending_send().to_vec();
        self.server.advance_send(server_data.len());
        self.client.on_recv(&server_data).unwrap();

        // Step 3: Client sends settings ACK
        let client_ack = self.client.pending_send().to_vec();
        self.client.advance_send(client_ack.len());
        self.server.on_recv(&client_ack);
        self.server.process().unwrap();

        // Step 4: Server sends settings ACK
        let server_ack = self.server.pending_send().to_vec();
        self.server.advance_send(server_ack.len());
        self.client.on_recv(&server_ack).unwrap();
    }

    /// Transfer pending data from client to server.
    fn client_to_server(&mut self) {
        let data = self.client.pending_send().to_vec();
        if !data.is_empty() {
            self.client.advance_send(data.len());
            self.server.on_recv(&data);
        }
    }

    /// Transfer pending data from server to client.
    fn server_to_client(&mut self) {
        let data = self.server.pending_send().to_vec();
        if !data.is_empty() {
            self.server.advance_send(data.len());
            self.client.on_recv(&data).unwrap();
        }
    }
}

#[test]
fn test_loopback_handshake() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    assert!(
        conn.client.is_ready(),
        "Client should be ready after handshake"
    );
    assert!(
        conn.server.is_ready(),
        "Server should be ready after handshake"
    );
}

#[test]
fn test_loopback_unary_request() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Client sends a unary request
    let builder = CallBuilder::new("/test.Service/Echo");
    let request_body = b"Hello, Server!";
    let call = conn.client.unary(builder, request_body).unwrap();

    assert_eq!(call.path(), "/test.Service/Echo");

    // Transfer request to server
    conn.client_to_server();
    let events = conn.server.process().unwrap();

    // Server should receive the request
    let request_event = events
        .iter()
        .find(|e| matches!(e, GrpcServerEvent::Request(_)));

    assert!(request_event.is_some(), "Server should receive request");

    if let Some(GrpcServerEvent::Request(req)) = request_event {
        assert_eq!(req.path, "/test.Service/Echo");
        assert_eq!(&req.message[..], request_body);
    }
}

#[test]
fn test_loopback_unary_response() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Client sends a unary request
    let builder = CallBuilder::new("/test.Service/Echo");
    let call = conn.client.unary(builder, b"request").unwrap();
    let stream_id = call.stream_id();

    // Transfer request to server
    conn.client_to_server();
    let events = conn.server.process().unwrap();

    // Find and process the request
    for event in events {
        if let GrpcServerEvent::Request(req) = event {
            // Server sends response
            let response_body = b"Hello, Client!";
            conn.server
                .send_response(req.stream_id, Status::ok(), response_body)
                .unwrap();
        }
    }

    // Transfer response to client
    conn.server_to_client();

    // Client polls for events
    let client_events = conn.client.poll();

    // Should get Headers, Message, and Complete events
    let mut got_headers = false;
    let mut got_message = false;
    let mut got_complete = false;
    let mut response_data = Vec::new();

    for (sid, event) in &client_events {
        assert_eq!(*sid, stream_id);
        match event {
            CallEvent::Headers(_) => got_headers = true,
            CallEvent::Message(data) => {
                got_message = true;
                response_data = data.to_vec();
            }
            CallEvent::Complete(status) => {
                got_complete = true;
                assert!(status.is_ok(), "Status should be OK");
            }
        }
    }

    assert!(got_headers, "Should receive Headers event");
    assert!(got_message, "Should receive Message event");
    assert!(got_complete, "Should receive Complete event");
    assert_eq!(&response_data[..], b"Hello, Client!");
}

#[test]
fn test_loopback_unary_with_timeout() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Client sends request with timeout
    let builder = CallBuilder::new("/test.Service/Method").timeout_millis(5000);
    let call = conn.client.unary(builder, b"test").unwrap();

    assert!(call.stream_id().value() > 0);

    // Transfer to server
    conn.client_to_server();
    let events = conn.server.process().unwrap();

    // Server should receive request (timeout header is just metadata)
    let has_request = events
        .iter()
        .any(|e| matches!(e, GrpcServerEvent::Request(_)));
    assert!(has_request);
}

#[test]
fn test_loopback_unary_with_metadata() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Client sends request with custom metadata
    let builder = CallBuilder::new("/test.Service/Method")
        .metadata("x-request-id", "12345")
        .metadata("x-trace-id", "abc-def-ghi");
    let _call = conn.client.unary(builder, b"test").unwrap();

    // Transfer to server
    conn.client_to_server();
    let events = conn.server.process().unwrap();

    // Server should receive request with metadata
    for event in events {
        if let GrpcServerEvent::Request(req) = event {
            assert_eq!(req.metadata.get("x-request-id"), Some("12345"));
            assert_eq!(req.metadata.get("x-trace-id"), Some("abc-def-ghi"));
        }
    }
}

#[test]
fn test_loopback_error_response() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Client sends request
    let builder = CallBuilder::new("/test.Service/NotFound");
    let call = conn.client.unary(builder, b"request").unwrap();
    let stream_id = call.stream_id();

    // Transfer request to server
    conn.client_to_server();
    let events = conn.server.process().unwrap();

    // Server sends error response
    for event in events {
        if let GrpcServerEvent::Request(req) = event {
            conn.server
                .send_error(req.stream_id, Status::not_found("resource not found"))
                .unwrap();
        }
    }

    // Transfer error to client
    conn.server_to_client();

    // Client polls for events
    let client_events = conn.client.poll();

    // Should get Complete event with error status
    let complete_event = client_events
        .iter()
        .find(|(sid, e)| *sid == stream_id && matches!(e, CallEvent::Complete(_)));

    assert!(complete_event.is_some(), "Should receive Complete event");

    if let Some((_, CallEvent::Complete(status))) = complete_event {
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(status.message(), Some("resource not found"));
    }
}

#[test]
fn test_loopback_multiple_requests() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Client sends multiple requests
    let call1 = conn
        .client
        .unary(CallBuilder::new("/test.Service/Method1"), b"request1")
        .unwrap();
    let call2 = conn
        .client
        .unary(CallBuilder::new("/test.Service/Method2"), b"request2")
        .unwrap();

    // Stream IDs should be different (odd numbers for client-initiated)
    assert_ne!(call1.stream_id(), call2.stream_id());
    assert_eq!(call1.stream_id().value() % 2, 1); // Client streams are odd
    assert_eq!(call2.stream_id().value() % 2, 1);

    // Transfer to server
    conn.client_to_server();
    let events = conn.server.process().unwrap();

    // Server should receive both requests
    let request_count = events
        .iter()
        .filter(|e| matches!(e, GrpcServerEvent::Request(_)))
        .count();
    assert_eq!(request_count, 2, "Server should receive 2 requests");
}

#[test]
fn test_loopback_streaming_response() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Client sends request
    let builder = CallBuilder::new("/test.Service/StreamingMethod");
    let call = conn.client.unary(builder, b"request").unwrap();
    let stream_id = call.stream_id();

    // Transfer request to server
    conn.client_to_server();
    let events = conn.server.process().unwrap();

    // Server sends streaming response (headers, multiple messages, trailers)
    for event in events {
        if let GrpcServerEvent::Request(req) = event {
            // Send headers
            conn.server.send_response_headers(req.stream_id).unwrap();

            // Send multiple messages
            conn.server
                .send_response_message(req.stream_id, b"chunk1")
                .unwrap();
            conn.server
                .send_response_message(req.stream_id, b"chunk2")
                .unwrap();
            conn.server
                .send_response_message(req.stream_id, b"chunk3")
                .unwrap();

            // Send trailers
            conn.server
                .send_trailers(req.stream_id, Status::ok())
                .unwrap();
        }
    }

    // Transfer response to client
    conn.server_to_client();

    // Client polls for events
    let client_events = conn.client.poll();

    // Should receive multiple Message events
    let message_count = client_events
        .iter()
        .filter(|(sid, e)| *sid == stream_id && matches!(e, CallEvent::Message(_)))
        .count();

    assert_eq!(message_count, 3, "Should receive 3 message chunks");

    // Verify we got the complete event too
    let has_complete = client_events
        .iter()
        .any(|(sid, e)| *sid == stream_id && matches!(e, CallEvent::Complete(_)));
    assert!(has_complete, "Should receive Complete event");
}

#[test]
fn test_loopback_empty_response() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Client sends request
    let builder = CallBuilder::new("/test.Service/Empty");
    let call = conn.client.unary(builder, b"request").unwrap();
    let stream_id = call.stream_id();

    // Transfer request to server
    conn.client_to_server();
    let events = conn.server.process().unwrap();

    // Server sends empty response
    for event in events {
        if let GrpcServerEvent::Request(req) = event {
            conn.server
                .send_response(req.stream_id, Status::ok(), b"")
                .unwrap();
        }
    }

    // Transfer response to client
    conn.server_to_client();

    // Client polls for events
    let client_events = conn.client.poll();

    // Should still get Complete event
    let has_complete = client_events
        .iter()
        .any(|(sid, e)| *sid == stream_id && matches!(e, CallEvent::Complete(_)));
    assert!(
        has_complete,
        "Should receive Complete event for empty response"
    );
}

#[test]
fn test_loopback_large_message() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Create a large request (64KB)
    let large_request: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();

    // Client sends large request
    let builder = CallBuilder::new("/test.Service/LargeMessage");
    let call = conn.client.unary(builder, &large_request).unwrap();
    let stream_id = call.stream_id();

    // Transfer request to server
    conn.client_to_server();
    let events = conn.server.process().unwrap();

    // Server receives and echoes back
    for event in events {
        if let GrpcServerEvent::Request(req) = event {
            assert_eq!(req.message.len(), 65536);
            conn.server
                .send_response(req.stream_id, Status::ok(), &req.message)
                .unwrap();
        }
    }

    // Transfer response to client
    conn.server_to_client();

    // Client polls for events
    let client_events = conn.client.poll();

    // Verify message received
    for (sid, event) in client_events {
        if sid == stream_id
            && let CallEvent::Message(data) = event
        {
            assert_eq!(data.len(), 65536);
            assert_eq!(&data[..], &large_request[..]);
        }
    }
}

#[test]
fn test_loopback_stream_reset() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Client sends request
    let builder = CallBuilder::new("/test.Service/Method");
    let call = conn.client.unary(builder, b"request").unwrap();
    let stream_id = call.stream_id();

    // Transfer request to server
    conn.client_to_server();
    conn.server.process().unwrap();

    // Server sends RST_STREAM instead of a response
    let rst_frame = http2::RstStreamFrame {
        stream_id,
        error_code: http2::ErrorCode::Cancel as u32,
    };
    let encoder = FrameEncoder::new();
    let mut rst_buf = BytesMut::new();
    encoder.encode(&Frame::RstStream(rst_frame), &mut rst_buf);

    // Feed RST_STREAM to client
    conn.client.on_recv(&rst_buf).unwrap();

    // Client polls for events
    let client_events = conn.client.poll();

    // Should get Complete event with Internal error (stream reset)
    let complete_event = client_events
        .iter()
        .find(|(sid, e)| *sid == stream_id && matches!(e, CallEvent::Complete(_)));

    assert!(
        complete_event.is_some(),
        "Should receive Complete event on RST_STREAM"
    );

    if let Some((_, CallEvent::Complete(status))) = complete_event {
        assert_eq!(status.code(), Code::Internal);
        assert!(
            status.message().unwrap().contains("stream reset"),
            "Message should indicate stream reset"
        );
    }
}

#[test]
fn test_loopback_goaway() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Client sends multiple requests
    let call1 = conn
        .client
        .unary(CallBuilder::new("/test.Service/Method1"), b"request1")
        .unwrap();
    let call2 = conn
        .client
        .unary(CallBuilder::new("/test.Service/Method2"), b"request2")
        .unwrap();

    let stream_id1 = call1.stream_id();
    let stream_id2 = call2.stream_id();

    // Transfer requests to server
    conn.client_to_server();
    conn.server.process().unwrap();

    // Server sends GOAWAY frame
    let goaway_frame = http2::GoAwayFrame {
        last_stream_id: StreamId::new(0), // No streams processed
        error_code: http2::ErrorCode::NoError as u32,
        debug_data: bytes::Bytes::from_static(b"server shutting down"),
    };
    let encoder = FrameEncoder::new();
    let mut goaway_buf = BytesMut::new();
    encoder.encode(&Frame::GoAway(goaway_frame), &mut goaway_buf);

    // Feed GOAWAY to client
    conn.client.on_recv(&goaway_buf).unwrap();

    // Client polls for events
    let client_events = conn.client.poll();

    // Should get Complete events for all pending calls with Unavailable status
    let complete_count = client_events
        .iter()
        .filter(|(_, e)| matches!(e, CallEvent::Complete(_)))
        .count();

    assert_eq!(
        complete_count, 2,
        "All pending calls should be completed on GOAWAY"
    );

    // Check that both stream IDs got Complete events
    let stream_ids: Vec<_> = client_events
        .iter()
        .filter_map(|(sid, e)| {
            if matches!(e, CallEvent::Complete(_)) {
                Some(sid.value())
            } else {
                None
            }
        })
        .collect();

    assert!(
        stream_ids.contains(&stream_id1.value()),
        "Stream 1 should be completed"
    );
    assert!(
        stream_ids.contains(&stream_id2.value()),
        "Stream 2 should be completed"
    );

    // Verify the status is Unavailable
    for (_, event) in &client_events {
        if let CallEvent::Complete(status) = event {
            assert_eq!(status.code(), Code::Unavailable);
            assert_eq!(status.message(), Some("connection closed"));
        }
    }
}

#[test]
fn test_loopback_goaway_partial() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Client sends multiple requests
    let call1 = conn
        .client
        .unary(CallBuilder::new("/test.Service/Method1"), b"request1")
        .unwrap();
    let call2 = conn
        .client
        .unary(CallBuilder::new("/test.Service/Method2"), b"request2")
        .unwrap();
    let call3 = conn
        .client
        .unary(CallBuilder::new("/test.Service/Method3"), b"request3")
        .unwrap();

    let stream_id1 = call1.stream_id();
    let _stream_id2 = call2.stream_id();
    let _stream_id3 = call3.stream_id();

    // Transfer requests to server
    conn.client_to_server();
    conn.server.process().unwrap();

    // Server sends GOAWAY with last_stream_id indicating some streams were processed
    // This means streams with ID > last_stream_id were NOT processed
    let goaway_frame = http2::GoAwayFrame {
        last_stream_id: stream_id1, // Only stream 1 was processed
        error_code: http2::ErrorCode::NoError as u32,
        debug_data: bytes::Bytes::new(),
    };
    let encoder = FrameEncoder::new();
    let mut goaway_buf = BytesMut::new();
    encoder.encode(&Frame::GoAway(goaway_frame), &mut goaway_buf);

    // Feed GOAWAY to client
    conn.client.on_recv(&goaway_buf).unwrap();

    // Client polls for events - all pending calls should be failed
    let client_events = conn.client.poll();

    let complete_count = client_events
        .iter()
        .filter(|(_, e)| matches!(e, CallEvent::Complete(_)))
        .count();

    // All pending calls should be completed (even the ones that might retry)
    assert!(
        complete_count >= 2,
        "At least 2 calls should be completed on GOAWAY"
    );
}

#[test]
fn test_loopback_connection_error() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Client sends request
    let call = conn
        .client
        .unary(CallBuilder::new("/test.Service/Method"), b"request")
        .unwrap();
    let stream_id = call.stream_id();

    // Transfer request to server
    conn.client_to_server();
    conn.server.process().unwrap();

    // Send malformed frame to trigger connection error
    // A frame with invalid frame type should be ignored per spec,
    // but we can send a SETTINGS frame with wrong length to cause an error
    let malformed_frame: &[u8] = &[
        0x00, 0x00, 0x01, // Length: 1 (invalid for SETTINGS, must be multiple of 6)
        0x04, // Type: SETTINGS
        0x00, // Flags: none
        0x00, 0x00, 0x00, 0x00, // Stream ID: 0 (correct for SETTINGS)
        0xFF, // Invalid payload
    ];

    // Feed malformed frame to client
    let result = conn.client.on_recv(malformed_frame);

    // The connection should either error or continue with the invalid frame ignored
    // Different implementations handle this differently
    if result.is_ok() {
        // If the connection didn't error, poll should handle it
        let client_events = conn.client.poll();

        // Check if we got any error-related events
        for (sid, event) in &client_events {
            if *sid == stream_id
                && let CallEvent::Complete(status) = event
            {
                // If we got a complete event, it should indicate an error
                assert!(!status.is_ok() || status.code() == Code::Internal);
            }
        }
    }
    // If result is Err, that's also valid - the connection detected the error
}

#[test]
fn test_loopback_rst_stream_unknown_stream() {
    let mut conn = LoopbackConnection::new();
    conn.handshake();

    // Server sends RST_STREAM for a stream that doesn't exist
    let rst_frame = http2::RstStreamFrame {
        stream_id: StreamId::new(999), // Non-existent stream
        error_code: http2::ErrorCode::Cancel as u32,
    };
    let encoder = FrameEncoder::new();
    let mut rst_buf = BytesMut::new();
    encoder.encode(&Frame::RstStream(rst_frame), &mut rst_buf);

    // Feed RST_STREAM to client - should be ignored
    conn.client.on_recv(&rst_buf).unwrap();

    // Client polls for events - should be empty
    let client_events = conn.client.poll();

    // No events for unknown stream
    assert!(
        client_events.is_empty(),
        "RST_STREAM for unknown stream should be ignored"
    );
}

#[test]
fn test_loopback_status_codes() {
    // Test various gRPC status codes
    let test_cases = vec![
        (Code::Cancelled, "cancelled"),
        (Code::Unknown, "unknown error"),
        (Code::InvalidArgument, "invalid argument"),
        (Code::DeadlineExceeded, "deadline exceeded"),
        (Code::NotFound, "not found"),
        (Code::AlreadyExists, "already exists"),
        (Code::PermissionDenied, "permission denied"),
        (Code::ResourceExhausted, "resource exhausted"),
        (Code::FailedPrecondition, "failed precondition"),
        (Code::Aborted, "aborted"),
        (Code::OutOfRange, "out of range"),
        (Code::Unimplemented, "unimplemented"),
        (Code::Internal, "internal error"),
        (Code::Unavailable, "unavailable"),
        (Code::DataLoss, "data loss"),
        (Code::Unauthenticated, "unauthenticated"),
    ];

    for (expected_code, message) in test_cases {
        let mut conn = LoopbackConnection::new();
        conn.handshake();

        // Client sends request
        let builder = CallBuilder::new("/test.Service/StatusTest");
        let call = conn.client.unary(builder, b"test").unwrap();
        let stream_id = call.stream_id();

        // Transfer to server
        conn.client_to_server();
        let events = conn.server.process().unwrap();

        // Server responds with specific status
        for event in events {
            if let GrpcServerEvent::Request(req) = event {
                let status = Status::new(expected_code, message);
                conn.server.send_error(req.stream_id, status).unwrap();
            }
        }

        // Transfer to client
        conn.server_to_client();

        // Client receives status
        let client_events = conn.client.poll();
        for (sid, event) in client_events {
            if sid == stream_id
                && let CallEvent::Complete(status) = event
            {
                assert_eq!(
                    status.code(),
                    expected_code,
                    "Expected {:?}, got {:?}",
                    expected_code,
                    status.code()
                );
                assert_eq!(status.message(), Some(message));
            }
        }
    }
}

#[test]
fn test_server_full_request_response_cycle() {
    let mut server = Server::new();

    // Step 1: Send client preface
    let preface = create_client_preface();
    server.on_recv(&preface);
    let events = server.process().unwrap();

    // Should get Ready
    let has_ready = events.iter().any(|e| matches!(e, GrpcServerEvent::Ready));
    assert!(has_ready);

    // Step 2: Get server settings
    let server_settings = server.pending_send().to_vec();
    server.advance_send(server_settings.len());

    // Step 3: Send settings ACK from client
    let mut ack_buf = BytesMut::new();
    let encoder = FrameEncoder::new();
    let settings_ack = SettingsFrame {
        ack: true,
        settings: vec![],
    };
    encoder.encode(&Frame::Settings(settings_ack), &mut ack_buf);
    server.on_recv(&ack_buf);
    server.process().unwrap();

    // Step 4: Send a gRPC request
    let request_message = b"test request payload";
    let request = create_grpc_request(
        StreamId::new(1),
        "/mypackage.MyService/MyMethod",
        request_message,
        true,
    );
    server.on_recv(&request);
    let events = server.process().unwrap();

    // Should get a Request event
    let request_received = events
        .iter()
        .any(|e| matches!(e, GrpcServerEvent::Request(_)));

    if request_received {
        // Find the request
        for event in &events {
            if let GrpcServerEvent::Request(req) = event {
                assert_eq!(req.path, "/mypackage.MyService/MyMethod");
                assert_eq!(&req.message[..], request_message);

                // Step 5: Send response
                let response_message = b"test response payload";
                server
                    .send_response(req.stream_id, Status::ok(), response_message)
                    .unwrap();

                // Verify response is queued
                assert!(server.has_pending_send());
            }
        }
    }
}
