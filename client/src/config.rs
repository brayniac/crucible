/// Configuration for the crucible Redis client.
pub struct ClientConfig {
    /// Redis server addresses ("host:port").
    pub servers: Vec<String>,
    /// Number of kompio worker threads (default: 1).
    pub workers: usize,
    /// Connections per server per worker (default: 1).
    pub connections_per_server: usize,
    /// Connect timeout in milliseconds (default: 5000).
    pub connect_timeout_ms: u64,
    /// Enable TCP_NODELAY (default: true).
    pub tcp_nodelay: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            servers: vec!["127.0.0.1:6379".to_string()],
            workers: 1,
            connections_per_server: 1,
            connect_timeout_ms: 5000,
            tcp_nodelay: true,
        }
    }
}
