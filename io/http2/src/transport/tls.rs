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
/// interface.
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
