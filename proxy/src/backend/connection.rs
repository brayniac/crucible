//! Single backend connection.

use bytes::BytesMut;
use kompio::ConnToken;
use std::collections::VecDeque;
use std::net::SocketAddr;

/// State of a backend connection.
#[derive(Debug)]
pub enum BackendState {
    /// Connecting to the backend.
    Connecting,

    /// Idle, ready for requests.
    Idle,

    /// Sending request to backend.
    Sending {
        /// Bytes already sent.
        bytes_sent: usize,
    },

    /// Receiving response from backend.
    Receiving,

    /// Connection failed or closed.
    Failed,
}

/// An in-flight request to the backend.
#[derive(Debug)]
pub struct InFlightRequest {
    /// Originating client connection token.
    pub client: ConnToken,
    /// Request ID for correlation.
    pub request_id: u64,
    /// The key (for caching GET responses).
    pub key: Option<bytes::Bytes>,
    /// Whether this is a cacheable GET.
    pub cacheable: bool,
}

/// A connection to a backend node.
pub struct BackendConnection {
    /// Connection token from kompio.
    pub conn: ConnToken,

    /// Backend address.
    pub addr: SocketAddr,

    /// Current state.
    pub state: BackendState,

    /// Buffer for outgoing requests.
    pub send_buf: BytesMut,

    /// Buffer for incoming responses.
    pub recv_buf: BytesMut,

    /// Queue of in-flight requests (for pipelining).
    pub in_flight: VecDeque<InFlightRequest>,
}

impl BackendConnection {
    /// Create a new backend connection (in connecting state).
    pub fn new(conn: ConnToken, addr: SocketAddr) -> Self {
        Self {
            conn,
            addr,
            state: BackendState::Connecting,
            send_buf: BytesMut::with_capacity(4096),
            recv_buf: BytesMut::with_capacity(4096),
            in_flight: VecDeque::new(),
        }
    }

    /// Mark the connection as connected and idle.
    pub fn mark_connected(&mut self) {
        self.state = BackendState::Idle;
    }

    /// Mark the connection as failed.
    pub fn mark_failed(&mut self) {
        self.state = BackendState::Failed;
    }

    /// Check if the connection is idle and ready for requests.
    pub fn is_idle(&self) -> bool {
        matches!(self.state, BackendState::Idle)
    }

    /// Check if the connection is usable.
    pub fn is_usable(&self) -> bool {
        !matches!(self.state, BackendState::Failed | BackendState::Connecting)
    }

    /// Queue a request after data has already been encoded into send_buf.
    /// Use this with `send_buf_mut()` for zero-allocation encoding.
    #[inline]
    pub fn queue_request_encoded(&mut self, request: InFlightRequest) {
        self.in_flight.push_back(request);
        self.state = BackendState::Sending { bytes_sent: 0 };
    }

    /// Get mutable access to send buffer for direct encoding.
    #[inline]
    pub fn send_buf_mut(&mut self) -> &mut BytesMut {
        &mut self.send_buf
    }

    /// Append received data.
    pub fn append_recv(&mut self, data: &[u8]) {
        self.recv_buf.extend_from_slice(data);
    }

    /// Get the oldest in-flight request.
    pub fn oldest_request(&self) -> Option<&InFlightRequest> {
        self.in_flight.front()
    }

    /// Complete the oldest request.
    pub fn complete_request(&mut self) -> Option<InFlightRequest> {
        self.in_flight.pop_front()
    }

    /// Get data to send.
    pub fn send_data(&self) -> &[u8] {
        &self.send_buf
    }

    /// Mark bytes as sent.
    pub fn advance_sent(&mut self, bytes: usize) {
        let _ = self.send_buf.split_to(bytes);
        if self.send_buf.is_empty() {
            self.state = BackendState::Receiving;
        } else if let BackendState::Sending { bytes_sent } = &mut self.state {
            *bytes_sent += bytes;
        }
    }

    /// Consume response bytes.
    pub fn consume_response(&mut self, len: usize) {
        let _ = self.recv_buf.split_to(len);
        // If no more in-flight requests, go idle
        if self.in_flight.is_empty() && self.recv_buf.is_empty() {
            self.state = BackendState::Idle;
        }
    }
}
