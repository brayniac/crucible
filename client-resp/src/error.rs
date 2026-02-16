use protocol_resp::ParseError;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connection closed")]
    ConnectionClosed,
    #[error("pool closed")]
    PoolClosed,
    #[error("request cancelled")]
    RequestCancelled,
    #[error("command timed out")]
    Timeout,
    #[error("redis error: {0}")]
    Redis(String),
    #[error("unexpected response type")]
    UnexpectedResponse,
    #[error("protocol error: {0}")]
    Protocol(#[from] ParseError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
