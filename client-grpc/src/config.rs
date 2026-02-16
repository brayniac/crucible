pub struct GrpcClientConfig {
    /// gRPC server addresses ("host:port").
    pub servers: Vec<String>,
    /// Number of krio worker threads (default: 1).
    pub workers: usize,
    /// Connections per server per worker (default: 1).
    pub connections_per_server: usize,
    /// Connect timeout in milliseconds (default: 5000).
    pub connect_timeout_ms: u64,
    /// Enable TCP_NODELAY (default: true).
    pub tcp_nodelay: bool,
    /// Default :authority pseudo-header (defaults to server address).
    pub default_authority: Option<String>,
    /// Use TLS (requires "tls" feature).
    pub tls: bool,
    /// TLS server name for SNI (defaults to host portion of first server address).
    pub tls_server_name: Option<String>,
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            servers: vec!["127.0.0.1:50051".to_string()],
            workers: 1,
            connections_per_server: 1,
            connect_timeout_ms: 5000,
            tcp_nodelay: true,
            default_authority: None,
            tls: false,
            tls_server_name: None,
        }
    }
}
