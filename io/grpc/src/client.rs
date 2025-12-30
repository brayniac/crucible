//! gRPC client abstractions.

use crate::frame::{self, MessageDecoder};
use crate::metadata::{Metadata, Timeout};
use crate::status::{Code, Status};

use bytes::Bytes;
use http2::{Connection, ConnectionEvent, ConnectionState, HeaderField, StreamId, Transport};
use std::collections::HashMap;
use std::io;

/// A gRPC channel (connection to a server).
///
/// Wraps an HTTP/2 connection and provides gRPC-specific functionality.
pub struct Channel<T: Transport> {
    /// The underlying HTTP/2 connection.
    conn: Connection<T>,
    /// Authority (host:port) for requests.
    authority: String,
    /// Active calls by stream ID.
    calls: HashMap<u32, CallState>,
}

/// State of an in-progress call.
#[derive(Debug)]
struct CallState {
    /// Response headers received.
    headers: Option<Metadata>,
    /// Message decoder for response body.
    decoder: MessageDecoder,
    /// Whether we've received END_STREAM.
    end_stream: bool,
    /// Trailers (contain grpc-status).
    trailers: Option<Metadata>,
}

impl CallState {
    fn new() -> Self {
        Self {
            headers: None,
            decoder: MessageDecoder::new(),
            end_stream: false,
            trailers: None,
        }
    }
}

/// Result of polling a call.
#[derive(Debug)]
pub enum CallEvent {
    /// Received response headers.
    Headers(Metadata),
    /// Received a response message.
    Message(Bytes),
    /// Call completed with status.
    Complete(Status),
}

/// A gRPC call (request/response exchange).
pub struct Call {
    /// Stream ID for this call.
    stream_id: StreamId,
    /// Service path (e.g., "/package.Service/Method").
    path: String,
}

impl Call {
    /// Get the stream ID for this call.
    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    /// Get the path for this call.
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Builder for constructing gRPC calls.
pub struct CallBuilder {
    /// Service path.
    path: String,
    /// Request metadata.
    metadata: Metadata,
    /// Request timeout.
    timeout: Option<Timeout>,
}

impl CallBuilder {
    /// Create a new call builder for the given path.
    ///
    /// Path should be in format "/package.Service/Method".
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            metadata: Metadata::new(),
            timeout: None,
        }
    }

    /// Add metadata to the request.
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Set the request timeout.
    pub fn timeout(mut self, timeout: Timeout) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set timeout from seconds.
    pub fn timeout_secs(self, secs: u64) -> Self {
        self.timeout(Timeout::from_secs(secs))
    }

    /// Set timeout from milliseconds.
    pub fn timeout_millis(self, millis: u64) -> Self {
        self.timeout(Timeout::from_millis(millis))
    }
}

impl<T: Transport> Channel<T> {
    /// Create a new channel wrapping an HTTP/2 connection.
    pub fn new(conn: Connection<T>, authority: impl Into<String>) -> Self {
        Self {
            conn,
            authority: authority.into(),
            calls: HashMap::new(),
        }
    }

    /// Get the connection state.
    pub fn state(&self) -> ConnectionState {
        self.conn.state()
    }

    /// Check if the channel is ready for calls.
    pub fn is_ready(&self) -> bool {
        self.conn.is_ready()
    }

    /// Process transport readiness.
    pub fn on_transport_ready(&mut self) -> io::Result<()> {
        self.conn.on_transport_ready()
    }

    /// Feed received data to the channel.
    pub fn on_recv(&mut self, data: &[u8]) -> io::Result<()> {
        self.conn.on_recv(data)
    }

    /// Start a unary call (single request, single response).
    ///
    /// Sends the request immediately and returns a Call handle.
    pub fn unary(&mut self, builder: CallBuilder, request: &[u8]) -> io::Result<Call> {
        // Build headers
        let mut headers = vec![
            HeaderField::new(":method", "POST"),
            HeaderField::new(":scheme", "https"),
            HeaderField::new(":path", builder.path.as_bytes()),
            HeaderField::new(":authority", self.authority.as_bytes()),
            HeaderField::new("content-type", "application/grpc"),
            HeaderField::new("te", "trailers"),
        ];

        // Add timeout if specified
        if let Some(timeout) = builder.timeout {
            headers.push(HeaderField::new(
                "grpc-timeout",
                timeout.to_grpc_format().as_bytes().to_vec(),
            ));
        }

        // Add custom metadata
        for (key, value) in builder.metadata.iter() {
            // Skip pseudo-headers and reserved headers
            if !key.starts_with(':') && key != "content-type" && key != "te" {
                headers.push(HeaderField::new(key.as_bytes(), value.as_bytes()));
            }
        }

        // Encode the request message
        let message = frame::encode_message(request);

        // Start the stream with headers (not end_stream since we're sending data)
        let stream_id = self.conn.start_request(&headers, false)?;

        // Send the message with end_stream
        self.conn.send_data(stream_id, &message, true)?;

        // Track the call
        self.calls.insert(stream_id.value(), CallState::new());

        Ok(Call {
            stream_id,
            path: builder.path,
        })
    }

    /// Poll for events on all active calls.
    ///
    /// Returns events for each call that has activity.
    pub fn poll(&mut self) -> Vec<(StreamId, CallEvent)> {
        let mut result = Vec::new();

        // Process HTTP/2 events
        for event in self.conn.poll_events() {
            match event {
                ConnectionEvent::Headers {
                    stream_id,
                    headers,
                    end_stream,
                } => {
                    if let Some(call) = self.calls.get_mut(&stream_id.value()) {
                        let metadata = headers_to_metadata(&headers);

                        if end_stream {
                            // Headers-only response (trailers)
                            call.trailers = Some(metadata.clone());
                            call.end_stream = true;
                        } else if call.headers.is_none() {
                            // Initial headers
                            call.headers = Some(metadata.clone());
                            result.push((stream_id, CallEvent::Headers(metadata)));
                        } else {
                            // Trailers
                            call.trailers = Some(metadata);
                            call.end_stream = true;
                        }
                    }
                }
                ConnectionEvent::Data {
                    stream_id,
                    data,
                    end_stream,
                } => {
                    if let Some(call) = self.calls.get_mut(&stream_id.value()) {
                        // Feed data to decoder
                        call.decoder.feed(&data);

                        // Decode messages
                        while let Ok(Some((message, _compressed))) = call.decoder.decode() {
                            result.push((stream_id, CallEvent::Message(message)));
                        }

                        if end_stream {
                            call.end_stream = true;
                        }
                    }
                }
                ConnectionEvent::StreamReset {
                    stream_id,
                    error_code,
                } => {
                    if let Some(_call) = self.calls.remove(&stream_id.value()) {
                        let status =
                            Status::new(Code::Internal, format!("stream reset: {:?}", error_code));
                        result.push((stream_id, CallEvent::Complete(status)));
                    }
                }
                ConnectionEvent::GoAway { .. } => {
                    // Mark all pending calls as failed
                    for (id, _call) in self.calls.drain() {
                        let status = Status::unavailable("connection closed");
                        result.push((StreamId::new(id), CallEvent::Complete(status)));
                    }
                }
                ConnectionEvent::Error(e) => {
                    // Mark all pending calls as failed
                    for (id, _call) in self.calls.drain() {
                        let status = Status::internal(format!("connection error: {:?}", e));
                        result.push((StreamId::new(id), CallEvent::Complete(status)));
                    }
                }
                _ => {}
            }
        }

        // Check for completed calls
        let completed: Vec<u32> = self
            .calls
            .iter()
            .filter(|(_, call)| call.end_stream)
            .map(|(id, _)| *id)
            .collect();

        for id in completed {
            if let Some(call) = self.calls.remove(&id) {
                let status = extract_status(&call);
                result.push((StreamId::new(id), CallEvent::Complete(status)));
            }
        }

        result
    }

    /// Get data pending to be sent on the socket.
    pub fn pending_send(&self) -> &[u8] {
        self.conn.pending_send()
    }

    /// Advance the send buffer after data was written to the socket.
    pub fn advance_send(&mut self, n: usize) {
        self.conn.advance_send(n);
    }

    /// Check if there's data to send.
    pub fn has_pending_send(&self) -> bool {
        self.conn.has_pending_send()
    }

    /// Get the underlying connection.
    pub fn connection(&self) -> &Connection<T> {
        &self.conn
    }

    /// Get mutable access to the underlying connection.
    pub fn connection_mut(&mut self) -> &mut Connection<T> {
        &mut self.conn
    }
}

/// Convert HTTP/2 headers to gRPC metadata.
fn headers_to_metadata(headers: &[HeaderField]) -> Metadata {
    headers
        .iter()
        .filter_map(|h| {
            let name = String::from_utf8(h.name.clone()).ok()?;
            let value = String::from_utf8(h.value.clone()).ok()?;
            Some((name, value))
        })
        .collect()
}

/// Extract gRPC status from call state.
fn extract_status(call: &CallState) -> Status {
    if let Some(trailers) = &call.trailers {
        // Get grpc-status
        let code = trailers
            .get("grpc-status")
            .and_then(|s| s.parse::<u32>().ok())
            .map(Code::from_u32)
            .unwrap_or(Code::Unknown);

        // Get grpc-message
        let message = trailers.get("grpc-message").map(|s| s.to_string());

        if let Some(msg) = message {
            Status::new(code, msg)
        } else {
            Status::from_code(code)
        }
    } else {
        // No trailers - assume OK if we got data, otherwise unknown
        Status::from_code(Code::Unknown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // CallBuilder tests

    #[test]
    fn test_call_builder() {
        let builder = CallBuilder::new("/test.Service/Method")
            .metadata("x-custom", "value")
            .timeout_secs(10);

        assert_eq!(builder.path, "/test.Service/Method");
        assert!(builder.timeout.is_some());
    }

    #[test]
    fn test_call_builder_new() {
        let builder = CallBuilder::new("/package.Service/Method");
        assert_eq!(builder.path, "/package.Service/Method");
        assert!(builder.metadata.is_empty());
        assert!(builder.timeout.is_none());
    }

    #[test]
    fn test_call_builder_from_string() {
        let path = String::from("/my.Service/MyMethod");
        let builder = CallBuilder::new(path);
        assert_eq!(builder.path, "/my.Service/MyMethod");
    }

    #[test]
    fn test_call_builder_metadata() {
        let builder = CallBuilder::new("/test/method")
            .metadata("x-custom-header", "custom-value")
            .metadata("authorization", "Bearer token123");

        assert_eq!(builder.metadata.len(), 2);
        assert_eq!(
            builder.metadata.get("x-custom-header"),
            Some("custom-value")
        );
        assert_eq!(
            builder.metadata.get("authorization"),
            Some("Bearer token123")
        );
    }

    #[test]
    fn test_call_builder_timeout() {
        let timeout = Timeout::from_secs(30);
        let builder = CallBuilder::new("/test/method").timeout(timeout);

        assert!(builder.timeout.is_some());
        assert_eq!(builder.timeout.unwrap().as_duration().as_secs(), 30);
    }

    #[test]
    fn test_call_builder_timeout_secs() {
        let builder = CallBuilder::new("/test/method").timeout_secs(60);

        assert!(builder.timeout.is_some());
        assert_eq!(builder.timeout.unwrap().as_duration().as_secs(), 60);
    }

    #[test]
    fn test_call_builder_timeout_millis() {
        let builder = CallBuilder::new("/test/method").timeout_millis(500);

        assert!(builder.timeout.is_some());
        assert_eq!(builder.timeout.unwrap().as_duration().as_millis(), 500);
    }

    #[test]
    fn test_call_builder_chained() {
        let builder = CallBuilder::new("/test/method")
            .metadata("key1", "value1")
            .metadata("key2", "value2")
            .timeout_millis(1000);

        assert_eq!(builder.path, "/test/method");
        assert_eq!(builder.metadata.len(), 2);
        assert!(builder.timeout.is_some());
    }

    // Call tests

    #[test]
    fn test_call_stream_id() {
        let call = Call {
            stream_id: StreamId::new(5),
            path: String::from("/test/method"),
        };
        assert_eq!(call.stream_id().value(), 5);
    }

    #[test]
    fn test_call_path() {
        let call = Call {
            stream_id: StreamId::new(1),
            path: String::from("/package.Service/Method"),
        };
        assert_eq!(call.path(), "/package.Service/Method");
    }

    // CallState tests

    #[test]
    fn test_call_state_new() {
        let state = CallState::new();
        assert!(state.headers.is_none());
        assert!(!state.end_stream);
        assert!(state.trailers.is_none());
    }

    #[test]
    fn test_call_state_debug() {
        let state = CallState::new();
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("CallState"));
    }

    // CallEvent tests

    #[test]
    fn test_call_event_headers_debug() {
        let metadata = Metadata::new();
        let event = CallEvent::Headers(metadata);
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("Headers"));
    }

    #[test]
    fn test_call_event_message_debug() {
        let event = CallEvent::Message(Bytes::from("test message"));
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("Message"));
    }

    #[test]
    fn test_call_event_complete_debug() {
        let status = Status::ok();
        let event = CallEvent::Complete(status);
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("Complete"));
    }

    // headers_to_metadata tests

    #[test]
    fn test_headers_to_metadata() {
        let headers = vec![
            HeaderField::new(":status", "200"),
            HeaderField::new("content-type", "application/grpc"),
            HeaderField::new("grpc-status", "0"),
        ];

        let metadata = headers_to_metadata(&headers);
        assert_eq!(metadata.get(":status"), Some("200"));
        assert_eq!(metadata.get("grpc-status"), Some("0"));
    }

    #[test]
    fn test_headers_to_metadata_empty() {
        let headers: Vec<HeaderField> = vec![];
        let metadata = headers_to_metadata(&headers);
        assert!(metadata.is_empty());
    }

    #[test]
    fn test_headers_to_metadata_multiple() {
        let headers = vec![
            HeaderField::new("key1", "value1"),
            HeaderField::new("key2", "value2"),
            HeaderField::new("key3", "value3"),
        ];

        let metadata = headers_to_metadata(&headers);
        assert_eq!(metadata.len(), 3);
        assert_eq!(metadata.get("key1"), Some("value1"));
        assert_eq!(metadata.get("key2"), Some("value2"));
        assert_eq!(metadata.get("key3"), Some("value3"));
    }

    #[test]
    fn test_headers_to_metadata_invalid_utf8() {
        // Create header with invalid UTF-8 in value
        let headers = vec![HeaderField {
            name: b"valid-key".to_vec(),
            value: vec![0xff, 0xfe], // Invalid UTF-8
        }];

        let metadata = headers_to_metadata(&headers);
        // Invalid UTF-8 headers should be skipped
        assert!(metadata.is_empty());
    }

    #[test]
    fn test_headers_to_metadata_invalid_utf8_name() {
        // Create header with invalid UTF-8 in name
        let headers = vec![HeaderField {
            name: vec![0xff, 0xfe], // Invalid UTF-8
            value: b"valid-value".to_vec(),
        }];

        let metadata = headers_to_metadata(&headers);
        // Invalid UTF-8 headers should be skipped
        assert!(metadata.is_empty());
    }

    // extract_status tests

    #[test]
    fn test_extract_status_ok() {
        let mut state = CallState::new();
        let mut trailers = Metadata::new();
        trailers.insert("grpc-status", "0");
        state.trailers = Some(trailers);

        let status = extract_status(&state);
        assert_eq!(status.code(), Code::Ok);
        assert!(status.message().is_none());
    }

    #[test]
    fn test_extract_status_with_message() {
        let mut state = CallState::new();
        let mut trailers = Metadata::new();
        trailers.insert("grpc-status", "5");
        trailers.insert("grpc-message", "resource not found");
        state.trailers = Some(trailers);

        let status = extract_status(&state);
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(status.message(), Some("resource not found"));
    }

    #[test]
    fn test_extract_status_internal_error() {
        let mut state = CallState::new();
        let mut trailers = Metadata::new();
        trailers.insert("grpc-status", "13");
        trailers.insert("grpc-message", "internal server error");
        state.trailers = Some(trailers);

        let status = extract_status(&state);
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), Some("internal server error"));
    }

    #[test]
    fn test_extract_status_no_trailers() {
        let state = CallState::new();
        let status = extract_status(&state);
        assert_eq!(status.code(), Code::Unknown);
    }

    #[test]
    fn test_extract_status_invalid_code() {
        let mut state = CallState::new();
        let mut trailers = Metadata::new();
        trailers.insert("grpc-status", "not-a-number");
        state.trailers = Some(trailers);

        let status = extract_status(&state);
        assert_eq!(status.code(), Code::Unknown);
    }

    #[test]
    fn test_extract_status_missing_grpc_status() {
        let mut state = CallState::new();
        let trailers = Metadata::new();
        state.trailers = Some(trailers);

        let status = extract_status(&state);
        assert_eq!(status.code(), Code::Unknown);
    }

    #[test]
    fn test_extract_status_all_codes() {
        // Test all valid status codes
        for code_value in 0u32..=16 {
            let mut state = CallState::new();
            let mut trailers = Metadata::new();
            trailers.insert("grpc-status", code_value.to_string());
            state.trailers = Some(trailers);

            let status = extract_status(&state);
            assert_eq!(status.code(), Code::from_u32(code_value));
        }
    }

    #[test]
    fn test_extract_status_unknown_code() {
        let mut state = CallState::new();
        let mut trailers = Metadata::new();
        trailers.insert("grpc-status", "999"); // Unknown code
        state.trailers = Some(trailers);

        let status = extract_status(&state);
        assert_eq!(status.code(), Code::Unknown);
    }
}
