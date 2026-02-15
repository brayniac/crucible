#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    #[error("connection closed")]
    ConnectionClosed,
    #[error("worker pool shut down")]
    WorkerClosed,
    #[error("request cancelled")]
    RequestCancelled,
    #[error("grpc error: {0}")]
    Grpc(String),
    #[error("stream reset: error code {0}")]
    StreamReset(u32),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("kompio error: {0}")]
    Kompio(#[from] kompio::Error),
}
