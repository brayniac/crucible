#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("connection closed")]
    ConnectionClosed,
    #[error("worker pool shut down")]
    WorkerClosed,
    #[error("request cancelled")]
    RequestCancelled,
    #[error("http2 error: {0}")]
    Http2(String),
    #[error("stream reset: error code {0}")]
    StreamReset(u32),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("kompio error: {0}")]
    Kompio(#[from] kompio::Error),
}
