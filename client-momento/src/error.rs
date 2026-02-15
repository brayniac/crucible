#[derive(Debug, thiserror::Error)]
pub enum MomentoError {
    #[error("connection closed")]
    ConnectionClosed,
    #[error("worker pool shut down")]
    WorkerClosed,
    #[error("request cancelled")]
    RequestCancelled,
    #[error("cache error: {0}")]
    Cache(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("kompio error: {0}")]
    Kompio(#[from] kompio::Error),
}
