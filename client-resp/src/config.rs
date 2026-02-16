use std::time::Duration;

/// Configuration for the crucible Redis client.
pub struct ClientConfig {
    /// Redis server addresses ("host:port").
    pub servers: Vec<String>,
    /// Connections per server (default: 1).
    pub pool_size: usize,
    /// Connect timeout (default: 5s).
    pub connect_timeout: Duration,
    /// Per-command timeout (default: none).
    pub command_timeout: Option<Duration>,
    /// Enable TCP_NODELAY (default: true).
    pub tcp_nodelay: bool,
    /// mpsc channel buffer per connection (default: 1024).
    pub channel_buffer: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            servers: vec!["127.0.0.1:6379".to_string()],
            pool_size: 1,
            connect_timeout: Duration::from_secs(5),
            command_timeout: None,
            tcp_nodelay: true,
            channel_buffer: 1024,
        }
    }
}
