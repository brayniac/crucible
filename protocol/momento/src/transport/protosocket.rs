//! Protosocket transport for Momento.
//!
//! This transport uses length-delimited protobuf messages directly over TCP,
//! eliminating HTTP/2 overhead for higher performance.

use super::{MomentoTransport, RequestId, TransportResult};
use crate::CacheValue;
use crate::error::Error;
use crate::proto::{
    CacheCommand, CacheResponse, CacheResponseResult, UnaryCommand, decode_length_delimited_message,
};

use bytes::{Bytes, BytesMut};
use http2::Transport;
use std::collections::HashMap;
use std::io;
use std::time::Duration;

/// State of a pending operation.
#[derive(Debug)]
enum PendingOp {
    Authenticate,
    Get { cache_name: String, key: Bytes },
    Set { cache_name: String, key: Bytes },
    Delete { cache_name: String, key: Bytes },
}

/// Connection state for protosocket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    /// Waiting for transport to be ready.
    Connecting,
    /// Authenticating with Momento.
    Authenticating,
    /// Ready for operations.
    Ready,
}

/// Protosocket transport for Momento cache operations.
///
/// Uses length-delimited protobuf messages directly over TCP. This is a
/// higher-performance alternative to gRPC that eliminates HTTP/2 overhead.
///
/// # Wire Format
///
/// Messages are framed as:
/// ```text
/// [varint: message_length][protobuf: CacheCommand or CacheResponse]
/// ```
///
/// # Authentication
///
/// The first message sent must be an `AuthenticateCommand` with the auth token.
/// All subsequent messages can be cache operations.
pub struct ProtosocketTransport<T: Transport> {
    /// The underlying transport.
    transport: T,
    /// Authentication token.
    auth_token: String,
    /// Connection state.
    state: ConnectionState,
    /// Next message ID to use.
    next_message_id: u64,
    /// Pending operations by message ID.
    pending: HashMap<u64, PendingOp>,
    /// Buffer for incoming data.
    recv_buf: BytesMut,
    /// Buffer for outgoing data.
    send_buf: BytesMut,
    /// Results to return on next poll.
    results: Vec<TransportResult>,
}

impl<T: Transport> ProtosocketTransport<T> {
    /// Create a new protosocket transport.
    pub fn new(transport: T, auth_token: String) -> Self {
        Self {
            transport,
            auth_token,
            state: ConnectionState::Connecting,
            next_message_id: 1,
            pending: HashMap::new(),
            recv_buf: BytesMut::with_capacity(64 * 1024),
            send_buf: BytesMut::with_capacity(64 * 1024),
            results: Vec::new(),
        }
    }

    /// Get the next message ID.
    fn next_id(&mut self) -> u64 {
        let id = self.next_message_id;
        self.next_message_id += 1;
        id
    }

    /// Send the authentication command.
    fn send_authenticate(&mut self) -> io::Result<()> {
        let message_id = self.next_id();
        let cmd = CacheCommand::new(
            message_id,
            UnaryCommand::Authenticate {
                auth_token: self.auth_token.clone(),
            },
        );

        let encoded = cmd.encode_length_delimited();
        self.send_buf.extend_from_slice(&encoded);

        self.pending.insert(message_id, PendingOp::Authenticate);
        self.state = ConnectionState::Authenticating;

        // Flush through TLS layer
        self.flush_send_internal()?;

        Ok(())
    }

    /// Send a command.
    fn send_command(&mut self, command: UnaryCommand, op: PendingOp) -> io::Result<RequestId> {
        if self.state != ConnectionState::Ready {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "transport not ready",
            ));
        }

        let message_id = self.next_id();
        let cmd = CacheCommand::new(message_id, command);

        let encoded = cmd.encode_length_delimited();
        self.send_buf.extend_from_slice(&encoded);

        self.pending.insert(message_id, op);

        // Flush through TLS layer
        self.flush_send_internal()?;

        Ok(RequestId::new(message_id))
    }

    /// Flush send_buf through the TLS transport.
    fn flush_send_internal(&mut self) -> io::Result<()> {
        while !self.send_buf.is_empty() {
            match self.transport.send(&self.send_buf) {
                Ok(0) => break,
                Ok(n) => {
                    let _ = self.send_buf.split_to(n);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Process a received response.
    fn process_response(&mut self, response: CacheResponse) {
        let message_id = response.message_id;

        if let Some(op) = self.pending.remove(&message_id) {
            match op {
                PendingOp::Authenticate => {
                    // Authentication completed
                    match &response.result {
                        CacheResponseResult::Authenticate => {
                            self.state = ConnectionState::Ready;
                        }
                        CacheResponseResult::Error(err) => {
                            // Authentication failed - we could store this error
                            // For now, just stay in authenticating state
                            self.results.push(TransportResult::Get {
                                request_id: RequestId::new(message_id),
                                cache_name: String::new(),
                                key: Bytes::new(),
                                result: Err(Error::Protocol(format!(
                                    "authentication failed: {}",
                                    err.message
                                ))),
                            });
                        }
                        _ => {
                            // Unexpected response type
                        }
                    }
                }
                PendingOp::Get { cache_name, key } => {
                    let result = match response.result {
                        CacheResponseResult::Get { value } => match value {
                            Some(v) => Ok(CacheValue::Hit(v)),
                            None => Ok(CacheValue::Miss),
                        },
                        // Momento returns NotFound error for cache misses in protosocket
                        CacheResponseResult::Error(ref err)
                            if err.code == crate::proto::StatusCode::NotFound =>
                        {
                            Ok(CacheValue::Miss)
                        }
                        CacheResponseResult::Error(err) => Err(Error::Protocol(format!(
                            "{}: {}",
                            err.code as u32, err.message
                        ))),
                        _ => Err(Error::Protocol("unexpected response type for get".into())),
                    };
                    self.results.push(TransportResult::Get {
                        request_id: RequestId::new(message_id),
                        cache_name,
                        key,
                        result,
                    });
                }
                PendingOp::Set { cache_name, key } => {
                    let result = match response.result {
                        CacheResponseResult::Set => Ok(()),
                        CacheResponseResult::Error(err) => Err(Error::Protocol(format!(
                            "{}: {}",
                            err.code as u32, err.message
                        ))),
                        _ => Err(Error::Protocol("unexpected response type for set".into())),
                    };
                    self.results.push(TransportResult::Set {
                        request_id: RequestId::new(message_id),
                        cache_name,
                        key,
                        result,
                    });
                }
                PendingOp::Delete { cache_name, key } => {
                    let result = match response.result {
                        CacheResponseResult::Delete => Ok(()),
                        CacheResponseResult::Error(err) => Err(Error::Protocol(format!(
                            "{}: {}",
                            err.code as u32, err.message
                        ))),
                        _ => Err(Error::Protocol(
                            "unexpected response type for delete".into(),
                        )),
                    };
                    self.results.push(TransportResult::Delete {
                        request_id: RequestId::new(message_id),
                        cache_name,
                        key,
                        result,
                    });
                }
            }
        }
    }

    /// Parse and process all complete messages in the receive buffer.
    fn process_recv_buffer(&mut self) {
        while let Some((consumed, message)) = decode_length_delimited_message(&self.recv_buf) {
            if let Some(response) = CacheResponse::decode(message) {
                self.process_response(response);
            }
            // Remove consumed bytes from buffer
            let _ = self.recv_buf.split_to(consumed);
        }
    }

    /// Get the underlying transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Get mutable access to the underlying transport.
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    /// Check if authenticated and ready.
    pub fn is_authenticated(&self) -> bool {
        self.state == ConnectionState::Ready
    }
}

impl<T: Transport> MomentoTransport for ProtosocketTransport<T> {
    fn is_ready(&self) -> bool {
        self.state == ConnectionState::Ready
    }

    fn on_transport_ready(&mut self) -> io::Result<()> {
        // Transport is ready, send authentication
        self.send_authenticate()
    }

    fn on_recv(&mut self, data: &[u8]) -> io::Result<()> {
        // Pass encrypted data to TLS layer for decryption
        self.transport.on_recv(data)?;

        // Read decrypted data from TLS layer into our buffer
        let mut temp = [0u8; 16384];
        loop {
            match self.transport.recv(&mut temp) {
                Ok(0) => break,
                Ok(n) => {
                    self.recv_buf.extend_from_slice(&temp[..n]);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        self.process_recv_buffer();
        Ok(())
    }

    fn feed_data(&mut self, data: &[u8]) -> io::Result<()> {
        self.recv_buf.extend_from_slice(data);
        self.process_recv_buffer();
        Ok(())
    }

    fn pending_send(&self) -> &[u8] {
        // Return encrypted data from TLS layer
        self.transport.pending_send()
    }

    fn advance_send(&mut self, n: usize) {
        // Advance TLS layer's send buffer
        self.transport.advance_send(n);
    }

    fn has_pending_send(&self) -> bool {
        // Check TLS layer for pending encrypted data
        // (send_buf should be empty since we flush immediately after adding data)
        self.transport.has_pending_send() || !self.send_buf.is_empty()
    }

    fn get(&mut self, cache_name: &str, key: &[u8]) -> io::Result<RequestId> {
        let command = UnaryCommand::Get {
            namespace: cache_name.to_string(),
            key: Bytes::copy_from_slice(key),
        };
        let op = PendingOp::Get {
            cache_name: cache_name.to_string(),
            key: Bytes::copy_from_slice(key),
        };
        self.send_command(command, op)
    }

    fn set(
        &mut self,
        cache_name: &str,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> io::Result<RequestId> {
        let command = UnaryCommand::Set {
            namespace: cache_name.to_string(),
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
            ttl_millis: ttl.as_millis() as u64,
        };
        let op = PendingOp::Set {
            cache_name: cache_name.to_string(),
            key: Bytes::copy_from_slice(key),
        };
        self.send_command(command, op)
    }

    fn delete(&mut self, cache_name: &str, key: &[u8]) -> io::Result<RequestId> {
        let command = UnaryCommand::Delete {
            namespace: cache_name.to_string(),
            key: Bytes::copy_from_slice(key),
        };
        let op = PendingOp::Delete {
            cache_name: cache_name.to_string(),
            key: Bytes::copy_from_slice(key),
        };
        self.send_command(command, op)
    }

    fn poll(&mut self) -> Vec<TransportResult> {
        std::mem::take(&mut self.results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::StatusCode;
    use http2::PlainTransport;

    #[test]
    fn test_protosocket_transport_new() {
        let transport = PlainTransport::new();
        let ps = ProtosocketTransport::new(transport, "token".to_string());
        assert!(!ps.is_ready());
        assert!(!ps.is_authenticated());
    }

    #[test]
    fn test_protosocket_transport_transport_access() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        let _ = ps.transport();
        let _ = ps.transport_mut();
    }

    #[test]
    fn test_protosocket_transport_on_transport_ready() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "my-token".to_string());

        // Should send auth command
        let result = ps.on_transport_ready();
        assert!(result.is_ok());
        assert!(ps.has_pending_send());
        assert!(!ps.is_ready()); // Not ready until auth response
    }

    #[test]
    fn test_protosocket_transport_pending_send() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        // Initially empty
        assert!(!ps.has_pending_send());
        assert!(ps.pending_send().is_empty());

        // After auth
        let _ = ps.on_transport_ready();
        assert!(ps.has_pending_send());
        assert!(!ps.pending_send().is_empty());
    }

    #[test]
    fn test_protosocket_transport_advance_send() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        let _ = ps.on_transport_ready();
        let len = ps.pending_send().len();
        assert!(len > 0);

        ps.advance_send(len);
        assert!(!ps.has_pending_send());
    }

    #[test]
    fn test_protosocket_transport_get_not_ready() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        // Should fail because not authenticated
        let result = ps.get("cache", b"key");
        assert!(result.is_err());
    }

    #[test]
    fn test_protosocket_transport_poll_empty() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        let results = ps.poll();
        assert!(results.is_empty());
    }

    #[test]
    fn test_protosocket_transport_on_recv_empty() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        let result = ps.on_recv(&[]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_protosocket_transport_auth_flow() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        // Start auth
        let _ = ps.on_transport_ready();
        assert!(!ps.is_ready());

        // Simulate auth response
        let auth_response = CacheResponse::authenticate(1);
        let encoded = auth_response.encode_length_delimited();

        let _ = ps.on_recv(&encoded);
        assert!(ps.is_ready());
    }

    #[test]
    fn test_protosocket_transport_get_after_auth() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        // Auth flow
        let _ = ps.on_transport_ready();
        let auth_response = CacheResponse::authenticate(1);
        let _ = ps.on_recv(&auth_response.encode_length_delimited());

        // Now get should work
        let result = ps.get("my-cache", b"my-key");
        assert!(result.is_ok());
        assert!(ps.has_pending_send());
    }

    #[test]
    fn test_protosocket_transport_set_after_auth() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        // Auth flow
        let _ = ps.on_transport_ready();
        let auth_response = CacheResponse::authenticate(1);
        let _ = ps.on_recv(&auth_response.encode_length_delimited());

        // Consume auth send data
        ps.advance_send(ps.pending_send().len());

        // Now set should work
        let result = ps.set("cache", b"key", b"value", Duration::from_secs(60));
        assert!(result.is_ok());
        assert!(ps.has_pending_send());
    }

    #[test]
    fn test_protosocket_transport_delete_after_auth() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        // Auth flow
        let _ = ps.on_transport_ready();
        let auth_response = CacheResponse::authenticate(1);
        let _ = ps.on_recv(&auth_response.encode_length_delimited());

        // Consume auth send data
        ps.advance_send(ps.pending_send().len());

        // Now delete should work
        let result = ps.delete("cache", b"key");
        assert!(result.is_ok());
        assert!(ps.has_pending_send());
    }

    #[test]
    fn test_protosocket_transport_get_response_hit() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        // Auth flow
        let _ = ps.on_transport_ready();
        let _ = ps.on_recv(&CacheResponse::authenticate(1).encode_length_delimited());
        ps.advance_send(ps.pending_send().len());

        // Send get
        let request_id = ps.get("cache", b"key").unwrap();
        ps.advance_send(ps.pending_send().len());

        // Simulate hit response
        let hit_response = CacheResponse::get_hit(request_id.value(), Bytes::from_static(b"value"));
        let _ = ps.on_recv(&hit_response.encode_length_delimited());

        // Poll for result
        let results = ps.poll();
        assert_eq!(results.len(), 1);
        match &results[0] {
            TransportResult::Get { result, .. } => {
                assert!(result.is_ok());
                assert!(result.as_ref().unwrap().is_hit());
            }
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_protosocket_transport_get_response_miss() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        // Auth flow
        let _ = ps.on_transport_ready();
        let _ = ps.on_recv(&CacheResponse::authenticate(1).encode_length_delimited());
        ps.advance_send(ps.pending_send().len());

        // Send get
        let request_id = ps.get("cache", b"key").unwrap();
        ps.advance_send(ps.pending_send().len());

        // Simulate miss response
        let miss_response = CacheResponse::get_miss(request_id.value());
        let _ = ps.on_recv(&miss_response.encode_length_delimited());

        // Poll for result
        let results = ps.poll();
        assert_eq!(results.len(), 1);
        match &results[0] {
            TransportResult::Get { result, .. } => {
                assert!(result.is_ok());
                assert!(result.as_ref().unwrap().is_miss());
            }
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_protosocket_transport_set_response() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        // Auth flow
        let _ = ps.on_transport_ready();
        let _ = ps.on_recv(&CacheResponse::authenticate(1).encode_length_delimited());
        ps.advance_send(ps.pending_send().len());

        // Send set
        let request_id = ps
            .set("cache", b"key", b"value", Duration::from_secs(60))
            .unwrap();
        ps.advance_send(ps.pending_send().len());

        // Simulate success response
        let set_response = CacheResponse::set_ok(request_id.value());
        let _ = ps.on_recv(&set_response.encode_length_delimited());

        // Poll for result
        let results = ps.poll();
        assert_eq!(results.len(), 1);
        match &results[0] {
            TransportResult::Set { result, .. } => {
                assert!(result.is_ok());
            }
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn test_protosocket_transport_delete_response() {
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        // Auth flow
        let _ = ps.on_transport_ready();
        let _ = ps.on_recv(&CacheResponse::authenticate(1).encode_length_delimited());
        ps.advance_send(ps.pending_send().len());

        // Send delete
        let request_id = ps.delete("cache", b"key").unwrap();
        ps.advance_send(ps.pending_send().len());

        // Simulate success response
        let delete_response = CacheResponse::delete_ok(request_id.value());
        let _ = ps.on_recv(&delete_response.encode_length_delimited());

        // Poll for result
        let results = ps.poll();
        assert_eq!(results.len(), 1);
        match &results[0] {
            TransportResult::Delete { result, .. } => {
                assert!(result.is_ok());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_protosocket_transport_get_notfound_as_miss() {
        // Momento's protosocket returns NotFound error for cache misses
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        // Auth flow
        let _ = ps.on_transport_ready();
        let _ = ps.on_recv(&CacheResponse::authenticate(1).encode_length_delimited());
        ps.advance_send(ps.pending_send().len());

        // Send get
        let request_id = ps.get("cache", b"key").unwrap();
        ps.advance_send(ps.pending_send().len());

        // Simulate NotFound error (which should be treated as cache miss)
        let error_response =
            CacheResponse::error(request_id.value(), StatusCode::NotFound, "key not found");
        let _ = ps.on_recv(&error_response.encode_length_delimited());

        // Poll for result - should be a successful miss, not an error
        let results = ps.poll();
        assert_eq!(results.len(), 1);
        match &results[0] {
            TransportResult::Get { result, .. } => {
                assert!(result.is_ok());
                assert!(result.as_ref().unwrap().is_miss());
            }
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_protosocket_transport_get_other_error() {
        // Other errors (not NotFound) should still be errors
        let transport = PlainTransport::new();
        let mut ps = ProtosocketTransport::new(transport, "token".to_string());

        // Auth flow
        let _ = ps.on_transport_ready();
        let _ = ps.on_recv(&CacheResponse::authenticate(1).encode_length_delimited());
        ps.advance_send(ps.pending_send().len());

        // Send get
        let request_id = ps.get("cache", b"key").unwrap();
        ps.advance_send(ps.pending_send().len());

        // Simulate a real error (not NotFound)
        let error_response =
            CacheResponse::error(request_id.value(), StatusCode::Internal, "server error");
        let _ = ps.on_recv(&error_response.encode_length_delimited());

        // Poll for result - should be an error
        let results = ps.poll();
        assert_eq!(results.len(), 1);
        match &results[0] {
            TransportResult::Get { result, .. } => {
                assert!(result.is_err());
            }
            _ => panic!("expected Get"),
        }
    }
}
