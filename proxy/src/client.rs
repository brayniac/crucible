//! Client connection handling.

use bytes::BytesMut;
use io_driver::ConnId;
use std::net::SocketAddr;

/// State of a client connection.
#[derive(Debug)]
pub enum ClientState {
    /// Reading RESP command from client.
    Reading,

    /// Waiting for backend response.
    WaitingForBackend {
        /// ID of the in-flight request.
        request_id: u64,
    },

    /// Writing response to client.
    Writing {
        /// Bytes already written.
        bytes_written: usize,
    },

    /// Connection is being closed.
    Closing,
}

/// A client connection.
pub struct ClientConnection {
    /// Connection ID from the I/O driver.
    pub conn_id: ConnId,

    /// Remote address.
    pub addr: SocketAddr,

    /// Current state.
    pub state: ClientState,

    /// Buffer for incoming data.
    pub recv_buf: BytesMut,

    /// Buffer for outgoing data.
    pub send_buf: BytesMut,

    /// Bytes already parsed from recv_buf.
    pub parsed_offset: usize,
}

impl ClientConnection {
    /// Create a new client connection.
    pub fn new(conn_id: ConnId, addr: SocketAddr) -> Self {
        Self {
            conn_id,
            addr,
            state: ClientState::Reading,
            recv_buf: BytesMut::with_capacity(4096),
            send_buf: BytesMut::with_capacity(4096),
            parsed_offset: 0,
        }
    }

    /// Append received data to the buffer.
    pub fn append_recv(&mut self, data: &[u8]) {
        self.recv_buf.extend_from_slice(data);
    }

    /// Consume parsed bytes from the buffer.
    pub fn consume_parsed(&mut self, len: usize) {
        // Remove parsed bytes from the front
        let _ = self.recv_buf.split_to(len);
        self.parsed_offset = 0;
    }

    /// Get unparsed data.
    pub fn unparsed(&self) -> &[u8] {
        &self.recv_buf[self.parsed_offset..]
    }

    /// Check if we have data to send.
    pub fn has_pending_send(&self) -> bool {
        !self.send_buf.is_empty()
    }

    /// Get data to send.
    pub fn send_data(&self) -> &[u8] {
        &self.send_buf
    }

    /// Mark bytes as sent.
    pub fn advance_sent(&mut self, bytes: usize) {
        let _ = self.send_buf.split_to(bytes);
    }

    /// Queue a response to send.
    pub fn queue_response(&mut self, data: &[u8]) {
        self.send_buf.extend_from_slice(data);
    }
}
