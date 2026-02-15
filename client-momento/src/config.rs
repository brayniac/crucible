use protocol_momento::Credential;
use std::net::SocketAddr;
use std::time::Duration;

pub struct MomentoClientConfig {
    /// Momento credential (token + endpoint + wire format).
    pub credential: Credential,
    /// Cache name for all operations.
    pub cache_name: String,
    /// Number of kompio worker threads (default: 1).
    pub workers: usize,
    /// Connections per server per worker (default: 1).
    pub connections_per_server: usize,
    /// Connect timeout in milliseconds (default: 5000).
    pub connect_timeout_ms: u64,
    /// Enable TCP_NODELAY (default: true).
    pub tcp_nodelay: bool,
    /// Default TTL for set operations (default: 1 hour).
    pub default_ttl: Duration,
    /// Optional explicit server addresses (resolved from credential if empty).
    pub servers: Vec<SocketAddr>,
}
