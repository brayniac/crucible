//! TLS transport using rustls.

use super::{Transport, TransportState};
use bytes::BytesMut;
use rustls::pki_types::ServerName;
use std::io::{self, Read, Write};
use std::sync::Arc;

/// TLS configuration for client connections.
pub struct TlsConfig {
    /// rustls client configuration.
    config: Arc<rustls::ClientConfig>,
}

impl TlsConfig {
    /// Create a new TLS configuration with default root certificates.
    pub fn new() -> io::Result<Self> {
        let root_store =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Ok(Self {
            config: Arc::new(config),
        })
    }

    /// Create a TLS configuration with ALPN protocols.
    pub fn with_alpn(protocols: Vec<Vec<u8>>) -> io::Result<Self> {
        let root_store =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let mut config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        config.alpn_protocols = protocols;

        Ok(Self {
            config: Arc::new(config),
        })
    }

    /// Create a TLS configuration for HTTP/2.
    pub fn http2() -> io::Result<Self> {
        Self::with_alpn(vec![b"h2".to_vec()])
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self::new().expect("failed to create default TLS config")
    }
}

/// TLS transport using rustls.
///
/// This wraps a rustls `ClientConnection` and provides a completion-based
/// interface compatible with ioru.
pub struct TlsTransport {
    /// The TLS connection state.
    conn: rustls::ClientConnection,
    /// Current transport state.
    state: TransportState,
    /// Buffer for incoming encrypted data from the socket.
    incoming: BytesMut,
    /// Buffer for outgoing encrypted data to the socket.
    outgoing: BytesMut,
    /// How much of outgoing has been sent.
    outgoing_pos: usize,
    /// Buffer for decrypted application data.
    plaintext: BytesMut,
}

impl TlsTransport {
    /// Create a new TLS transport for the given server name.
    pub fn new(config: &TlsConfig, server_name: &str) -> io::Result<Self> {
        let name = ServerName::try_from(server_name.to_string())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let conn =
            rustls::ClientConnection::new(config.config.clone(), name).map_err(io::Error::other)?;

        let mut transport = Self {
            conn,
            state: TransportState::Handshaking,
            incoming: BytesMut::with_capacity(16384),
            outgoing: BytesMut::with_capacity(16384),
            outgoing_pos: 0,
            plaintext: BytesMut::with_capacity(16384),
        };

        // Generate initial handshake data
        transport.flush_tls_to_outgoing()?;

        Ok(transport)
    }

    /// Process TLS state and move data between buffers.
    fn process_tls(&mut self) -> io::Result<()> {
        // Feed incoming encrypted data to TLS
        if !self.incoming.is_empty() {
            let mut cursor = io::Cursor::new(&self.incoming[..]);
            match self.conn.read_tls(&mut cursor) {
                Ok(n) => {
                    let _ = self.incoming.split_to(n);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
        }

        // Process TLS messages
        match self.conn.process_new_packets() {
            Ok(state) => {
                // Read any decrypted data
                if state.plaintext_bytes_to_read() > 0 {
                    let mut buf = vec![0u8; state.plaintext_bytes_to_read()];
                    let n = self.conn.reader().read(&mut buf)?;
                    self.plaintext.extend_from_slice(&buf[..n]);
                }

                // Check if handshake completed
                if self.state == TransportState::Handshaking && !self.conn.is_handshaking() {
                    self.state = TransportState::Ready;
                }
            }
            Err(e) => {
                self.state = TransportState::Error;
                return Err(io::Error::other(e));
            }
        }

        // Flush any pending TLS output
        self.flush_tls_to_outgoing()?;

        Ok(())
    }

    /// Move pending TLS output to the outgoing buffer.
    fn flush_tls_to_outgoing(&mut self) -> io::Result<()> {
        if self.conn.wants_write() {
            let mut buf = Vec::with_capacity(4096);
            loop {
                match self.conn.write_tls(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e),
                }
            }
            self.outgoing.extend_from_slice(&buf);
        }
        Ok(())
    }

    /// Get the negotiated ALPN protocol, if any.
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.conn.alpn_protocol()
    }
}

impl Transport for TlsTransport {
    fn state(&self) -> TransportState {
        self.state
    }

    fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.state != TransportState::Ready {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "TLS handshake not complete",
            ));
        }

        // Write plaintext to TLS
        let n = self.conn.writer().write(data)?;

        // Encrypt and move to outgoing buffer
        self.flush_tls_to_outgoing()?;

        Ok(n)
    }

    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.plaintext.is_empty() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        let n = std::cmp::min(buf.len(), self.plaintext.len());
        buf[..n].copy_from_slice(&self.plaintext[..n]);

        // Remove read bytes
        let _ = self.plaintext.split_to(n);

        Ok(n)
    }

    fn on_recv(&mut self, data: &[u8]) -> io::Result<()> {
        self.incoming.extend_from_slice(data);
        self.process_tls()
    }

    fn pending_send(&self) -> &[u8] {
        &self.outgoing[self.outgoing_pos..]
    }

    fn advance_send(&mut self, n: usize) {
        self.outgoing_pos += n;

        // If all data sent, clear the buffer
        if self.outgoing_pos >= self.outgoing.len() {
            self.outgoing.clear();
            self.outgoing_pos = 0;
        }
    }

    fn shutdown(&mut self) -> io::Result<()> {
        self.conn.send_close_notify();
        self.flush_tls_to_outgoing()?;
        self.state = TransportState::Closed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_creation() {
        let config = TlsConfig::new().unwrap();
        assert!(config.config.alpn_protocols.is_empty());
    }

    #[test]
    fn test_tls_config_http2() {
        let config = TlsConfig::http2().unwrap();
        assert_eq!(config.config.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn test_tls_config_with_alpn() {
        let protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let config = TlsConfig::with_alpn(protocols.clone()).unwrap();
        assert_eq!(config.config.alpn_protocols, protocols);
    }

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(config.config.alpn_protocols.is_empty());
    }

    #[test]
    fn test_tls_transport_creation() {
        let config = TlsConfig::http2().unwrap();
        let transport = TlsTransport::new(&config, "example.com").unwrap();

        // Should be in handshaking state
        assert_eq!(transport.state(), TransportState::Handshaking);

        // Should have handshake data to send
        assert!(transport.has_pending_send());
    }

    #[test]
    fn test_tls_transport_is_ready() {
        let config = TlsConfig::new().unwrap();
        let transport = TlsTransport::new(&config, "example.com").unwrap();

        // Should not be ready during handshake
        assert!(!transport.is_ready());
    }

    #[test]
    fn test_tls_transport_invalid_server_name() {
        let config = TlsConfig::new().unwrap();
        // Empty server name should fail
        let result = TlsTransport::new(&config, "");
        assert!(result.is_err());
    }

    #[test]
    fn test_tls_transport_pending_send() {
        let config = TlsConfig::new().unwrap();
        let transport = TlsTransport::new(&config, "example.com").unwrap();

        // Should have initial handshake data
        let pending = transport.pending_send();
        assert!(!pending.is_empty());
    }

    #[test]
    fn test_tls_transport_advance_send() {
        let config = TlsConfig::new().unwrap();
        let mut transport = TlsTransport::new(&config, "example.com").unwrap();

        let initial_len = transport.pending_send().len();
        assert!(initial_len > 0);

        // Advance by a small amount
        transport.advance_send(10);
        assert_eq!(transport.pending_send().len(), initial_len - 10);

        // Advance remaining
        transport.advance_send(initial_len - 10);
        assert!(transport.pending_send().is_empty());
    }

    #[test]
    fn test_tls_transport_recv_empty() {
        let config = TlsConfig::new().unwrap();
        let mut transport = TlsTransport::new(&config, "example.com").unwrap();

        let mut buf = [0u8; 1024];
        let result = transport.recv(&mut buf);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn test_tls_transport_send_during_handshake() {
        let config = TlsConfig::new().unwrap();
        let mut transport = TlsTransport::new(&config, "example.com").unwrap();

        // Sending during handshake should fail
        let result = transport.send(b"hello");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotConnected);
    }

    #[test]
    fn test_tls_transport_shutdown() {
        let config = TlsConfig::new().unwrap();
        let mut transport = TlsTransport::new(&config, "example.com").unwrap();

        // Shutdown should succeed
        transport.shutdown().unwrap();
        assert_eq!(transport.state(), TransportState::Closed);
    }

    #[test]
    fn test_tls_transport_alpn_protocol() {
        let config = TlsConfig::http2().unwrap();
        let transport = TlsTransport::new(&config, "example.com").unwrap();

        // No ALPN negotiated yet (still handshaking)
        assert!(transport.alpn_protocol().is_none());
    }

    #[test]
    fn test_tls_transport_on_recv_empty() {
        let config = TlsConfig::new().unwrap();
        let mut transport = TlsTransport::new(&config, "example.com").unwrap();

        // Empty recv should be ok
        let result = transport.on_recv(&[]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_tls_transport_on_recv_garbage() {
        let config = TlsConfig::new().unwrap();
        let mut transport = TlsTransport::new(&config, "example.com").unwrap();

        // Send garbage data - should cause TLS error
        let result = transport.on_recv(b"this is not valid TLS data at all");
        // This should fail because it's not valid TLS
        assert!(result.is_err());
        assert_eq!(transport.state(), TransportState::Error);
    }

    #[test]
    fn test_tls_transport_has_pending_send() {
        let config = TlsConfig::new().unwrap();
        let transport = TlsTransport::new(&config, "example.com").unwrap();

        // Should have handshake data pending
        assert!(transport.has_pending_send());
    }

    #[test]
    fn test_tls_transport_is_ready_false() {
        let config = TlsConfig::new().unwrap();
        let transport = TlsTransport::new(&config, "example.com").unwrap();

        // Not ready during handshake
        assert!(!transport.is_ready());
        assert_eq!(transport.state(), TransportState::Handshaking);
    }

    #[test]
    fn test_tls_config_clone() {
        let _config1 = TlsConfig::new().unwrap();
        // TlsConfig uses Arc internally, so we can create another with same config
        let _config2 = TlsConfig::with_alpn(vec![b"h2".to_vec()]).unwrap();
    }

    #[test]
    fn test_tls_transport_multiple_advance() {
        let config = TlsConfig::new().unwrap();
        let mut transport = TlsTransport::new(&config, "example.com").unwrap();

        let initial = transport.pending_send().len();
        assert!(initial > 0);

        // Advance in chunks
        transport.advance_send(10);
        assert_eq!(transport.pending_send().len(), initial - 10);

        transport.advance_send(10);
        assert_eq!(transport.pending_send().len(), initial - 20);
    }

    #[test]
    fn test_tls_transport_state_transitions() {
        let config = TlsConfig::new().unwrap();
        let mut transport = TlsTransport::new(&config, "example.com").unwrap();

        // Initial state is Handshaking
        assert_eq!(transport.state(), TransportState::Handshaking);

        // After shutdown, state is Closed
        transport.shutdown().unwrap();
        assert_eq!(transport.state(), TransportState::Closed);
    }

    #[test]
    fn test_tls_transport_recv_into_small_buffer() {
        let config = TlsConfig::new().unwrap();
        let mut transport = TlsTransport::new(&config, "example.com").unwrap();

        // Try to receive into a small buffer (no data available)
        let mut buf = [0u8; 1];
        let result = transport.recv(&mut buf);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn test_tls_transport_pending_send_after_advance_all() {
        let config = TlsConfig::new().unwrap();
        let mut transport = TlsTransport::new(&config, "example.com").unwrap();

        let len = transport.pending_send().len();
        transport.advance_send(len);

        // After advancing all, pending should be empty
        assert!(transport.pending_send().is_empty());
        assert!(!transport.has_pending_send());
    }
}
