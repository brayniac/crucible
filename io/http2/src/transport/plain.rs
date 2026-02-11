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
