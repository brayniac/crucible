mod momento;
mod session;

pub use momento::{MomentoConn, MomentoSetup, build_momento_tls_config};
pub use session::{RecvBuf, Session, SessionConfig, SessionError, Timestamp};

/// Result of a completed request.
#[derive(Debug, Clone)]
pub struct RequestResult {
    /// Unique request ID
    pub id: u64,
    /// Whether the request succeeded
    pub success: bool,
    /// Whether this was an error response (not a network error)
    pub is_error_response: bool,
    /// Latency in nanoseconds (sent_at → full response received)
    pub latency_ns: u64,
    /// Time to first byte in nanoseconds (sent_at → first recv notification), GET only
    pub ttfb_ns: Option<u64>,
    /// Request type
    pub request_type: RequestType,
    /// For GET requests: true if cache hit, false if miss, None for non-GET
    pub hit: Option<bool>,
    /// Key ID for backfill_on_miss tracking (GET only)
    pub key_id: Option<usize>,
    /// True if this SET was triggered by a miss (backfill)
    pub backfill: bool,
}

/// Type of request for metrics categorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestType {
    Get,
    Set,
    Ping,
    Delete,
    Other,
}

/// Connection state for reconnection handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    Disconnected,
    Connecting,
}
