//! Error types for the Momento client.

use grpc::{Code, Status};
use std::fmt;
use std::io;

/// Result type for Momento operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur when using the Momento client.
#[derive(Debug)]
pub enum Error {
    /// Configuration error.
    Config(String),
    /// Connection error.
    Connection(io::Error),
    /// gRPC error with status.
    Grpc(Status),
    /// Protocol error (e.g., malformed response).
    Protocol(String),
    /// Cache miss (not an error, but useful for Result-based APIs).
    CacheMiss,
}

impl Error {
    /// Check if this error represents a cache miss.
    pub fn is_cache_miss(&self) -> bool {
        matches!(self, Error::CacheMiss)
    }

    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Connection(_) => true,
            Error::Grpc(status) => matches!(
                status.code(),
                Code::Unavailable | Code::ResourceExhausted | Code::Aborted
            ),
            _ => false,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Config(msg) => write!(f, "configuration error: {}", msg),
            Error::Connection(e) => write!(f, "connection error: {}", e),
            Error::Grpc(status) => write!(f, "gRPC error: {}", status),
            Error::Protocol(msg) => write!(f, "protocol error: {}", msg),
            Error::CacheMiss => write!(f, "cache miss"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Connection(e) => Some(e),
            Error::Grpc(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Connection(e)
    }
}

impl From<Status> for Error {
    fn from(s: Status) -> Self {
        Error::Grpc(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as StdError;

    #[test]
    fn test_error_is_cache_miss() {
        assert!(Error::CacheMiss.is_cache_miss());
        assert!(!Error::Config("test".into()).is_cache_miss());
        assert!(!Error::Protocol("test".into()).is_cache_miss());
    }

    #[test]
    fn test_error_is_retryable_connection() {
        let err = Error::Connection(io::Error::new(io::ErrorKind::ConnectionReset, "reset"));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_error_is_retryable_grpc_unavailable() {
        let status = Status::unavailable("service unavailable");
        let err = Error::Grpc(status);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_error_is_retryable_grpc_resource_exhausted() {
        let status = Status::new(Code::ResourceExhausted, "rate limited");
        let err = Error::Grpc(status);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_error_is_retryable_grpc_aborted() {
        let status = Status::new(Code::Aborted, "aborted");
        let err = Error::Grpc(status);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_error_not_retryable_grpc_not_found() {
        let status = Status::not_found("not found");
        let err = Error::Grpc(status);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_error_not_retryable_config() {
        let err = Error::Config("bad config".into());
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_error_not_retryable_protocol() {
        let err = Error::Protocol("parse error".into());
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_error_not_retryable_cache_miss() {
        let err = Error::CacheMiss;
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_error_display_config() {
        let err = Error::Config("invalid setting".into());
        assert_eq!(format!("{}", err), "configuration error: invalid setting");
    }

    #[test]
    fn test_error_display_connection() {
        let err = Error::Connection(io::Error::new(io::ErrorKind::TimedOut, "timeout"));
        let display = format!("{}", err);
        assert!(display.contains("connection error"));
        assert!(display.contains("timeout"));
    }

    #[test]
    fn test_error_display_grpc() {
        let status = Status::not_found("entity not found");
        let err = Error::Grpc(status);
        let display = format!("{}", err);
        assert!(display.contains("gRPC error"));
    }

    #[test]
    fn test_error_display_protocol() {
        let err = Error::Protocol("malformed response".into());
        assert_eq!(format!("{}", err), "protocol error: malformed response");
    }

    #[test]
    fn test_error_display_cache_miss() {
        let err = Error::CacheMiss;
        assert_eq!(format!("{}", err), "cache miss");
    }

    #[test]
    fn test_error_source_connection() {
        let io_err = io::Error::other("test error");
        let err = Error::Connection(io_err);
        assert!(StdError::source(&err).is_some());
    }

    #[test]
    fn test_error_source_grpc() {
        let status = Status::internal("internal error");
        let err = Error::Grpc(status);
        assert!(StdError::source(&err).is_some());
    }

    #[test]
    fn test_error_source_none() {
        assert!(StdError::source(&Error::Config("test".into())).is_none());
        assert!(StdError::source(&Error::Protocol("test".into())).is_none());
        assert!(StdError::source(&Error::CacheMiss).is_none());
    }

    #[test]
    fn test_error_from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Connection(_)));
    }

    #[test]
    fn test_error_from_status() {
        let status = Status::ok();
        let err: Error = status.into();
        assert!(matches!(err, Error::Grpc(_)));
    }

    #[test]
    fn test_error_debug() {
        let err = Error::CacheMiss;
        let debug = format!("{:?}", err);
        assert!(debug.contains("CacheMiss"));

        let err = Error::Config("test".into());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Config"));
    }
}
