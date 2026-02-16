#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connection closed")]
    ConnectionClosed,
    #[error("no available connections for shard")]
    NoAvailableConnections,
    #[error("worker pool shut down")]
    WorkerClosed,
    #[error("request cancelled")]
    RequestCancelled,
    #[error("memcache error: {0}")]
    Memcache(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("krio error: {0}")]
    Krio(#[from] krio::Error),
}
