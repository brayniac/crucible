//! Plain (unencrypted) transport for testing and cleartext protocols.

use super::{Transport, TransportState};
use bytes::BytesMut;
use std::io;

/// Plain TCP transport without encryption.
///
/// This is useful for testing protocol framing without TLS overhead,
/// or for cleartext connections (h2c, plain RESP, etc.).
pub struct PlainTransport {
    /// State of the transport.
    state: TransportState,
    /// Buffer for incoming data.
    recv_buf: BytesMut,
    /// Buffer for outgoing data.
    send_buf: BytesMut,
    /// How much of send_buf has been sent.
    send_pos: usize,
}

impl PlainTransport {
    /// Create a new plain transport.
    pub fn new() -> Self {
        Self {
            state: TransportState::Ready, // Plain transport is immediately ready
            recv_buf: BytesMut::with_capacity(16384),
            send_buf: BytesMut::with_capacity(16384),
            send_pos: 0,
        }
    }
}

impl Default for PlainTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for PlainTransport {
    fn state(&self) -> TransportState {
        self.state
    }

    fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.state != TransportState::Ready {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "transport not ready",
            ));
        }

        self.send_buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.state != TransportState::Ready {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "transport not ready",
            ));
        }

        if self.recv_buf.is_empty() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        let n = std::cmp::min(buf.len(), self.recv_buf.len());
        buf[..n].copy_from_slice(&self.recv_buf[..n]);

        // Remove read bytes
        let _ = self.recv_buf.split_to(n);

        Ok(n)
    }

    fn on_recv(&mut self, data: &[u8]) -> io::Result<()> {
        self.recv_buf.extend_from_slice(data);
        Ok(())
    }

    fn pending_send(&self) -> &[u8] {
        &self.send_buf[self.send_pos..]
    }

    fn advance_send(&mut self, n: usize) {
        self.send_pos += n;

        // If all data sent, clear the buffer
        if self.send_pos >= self.send_buf.len() {
            self.send_buf.clear();
            self.send_pos = 0;
        }
    }

    fn shutdown(&mut self) -> io::Result<()> {
        self.state = TransportState::Closed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_recv() {
        let mut transport = PlainTransport::new();

        // Send some data
        let n = transport.send(b"hello").unwrap();
        assert_eq!(n, 5);

        // Check pending send
        assert_eq!(transport.pending_send(), b"hello");

        // Simulate receiving data
        transport.on_recv(b"world").unwrap();

        // Read it back
        let mut buf = [0u8; 10];
        let n = transport.recv(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"world");
    }

    #[test]
    fn test_advance_send() {
        let mut transport = PlainTransport::new();

        transport.send(b"hello world").unwrap();
        assert!(transport.has_pending_send());

        // Partial send
        transport.advance_send(5);
        assert_eq!(transport.pending_send(), b" world");

        // Complete send
        transport.advance_send(6);
        assert!(!transport.has_pending_send());
    }

    #[test]
    fn test_default() {
        let transport = PlainTransport::default();
        assert_eq!(transport.state(), TransportState::Ready);
        assert!(transport.is_ready());
    }

    #[test]
    fn test_state() {
        let transport = PlainTransport::new();
        assert_eq!(transport.state(), TransportState::Ready);
    }

    #[test]
    fn test_is_ready() {
        let transport = PlainTransport::new();
        assert!(transport.is_ready());
    }

    #[test]
    fn test_shutdown() {
        let mut transport = PlainTransport::new();
        assert_eq!(transport.state(), TransportState::Ready);

        transport.shutdown().unwrap();
        assert_eq!(transport.state(), TransportState::Closed);
    }

    #[test]
    fn test_send_after_shutdown() {
        let mut transport = PlainTransport::new();
        transport.shutdown().unwrap();

        let result = transport.send(b"hello");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotConnected);
    }

    #[test]
    fn test_recv_after_shutdown() {
        let mut transport = PlainTransport::new();
        transport.shutdown().unwrap();

        let mut buf = [0u8; 10];
        let result = transport.recv(&mut buf);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotConnected);
    }

    #[test]
    fn test_recv_empty_buffer() {
        let mut transport = PlainTransport::new();

        let mut buf = [0u8; 10];
        let result = transport.recv(&mut buf);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn test_recv_partial() {
        let mut transport = PlainTransport::new();

        // Add more data than we'll read
        transport.on_recv(b"hello world").unwrap();

        // Read only 5 bytes
        let mut buf = [0u8; 5];
        let n = transport.recv(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"hello");

        // Read remaining
        let n = transport.recv(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b" worl");

        // Read last byte
        let mut buf = [0u8; 10];
        let n = transport.recv(&mut buf).unwrap();
        assert_eq!(n, 1);
        assert_eq!(&buf[..n], b"d");
    }

    #[test]
    fn test_has_pending_send() {
        let mut transport = PlainTransport::new();
        assert!(!transport.has_pending_send());

        transport.send(b"data").unwrap();
        assert!(transport.has_pending_send());

        transport.advance_send(4);
        assert!(!transport.has_pending_send());
    }

    #[test]
    fn test_multiple_sends() {
        let mut transport = PlainTransport::new();

        transport.send(b"hello").unwrap();
        transport.send(b" world").unwrap();

        assert_eq!(transport.pending_send(), b"hello world");
    }

    #[test]
    fn test_on_recv_multiple() {
        let mut transport = PlainTransport::new();

        transport.on_recv(b"hello").unwrap();
        transport.on_recv(b" world").unwrap();

        let mut buf = [0u8; 20];
        let n = transport.recv(&mut buf).unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf[..n], b"hello world");
    }
}
