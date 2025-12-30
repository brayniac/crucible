//! gRPC status codes.

use std::fmt;

/// gRPC status codes (different from HTTP status codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Code {
    /// The operation completed successfully.
    Ok = 0,
    /// The operation was cancelled.
    Cancelled = 1,
    /// Unknown error.
    Unknown = 2,
    /// Invalid argument was provided.
    InvalidArgument = 3,
    /// Deadline expired before operation could complete.
    DeadlineExceeded = 4,
    /// Requested entity was not found.
    NotFound = 5,
    /// Entity already exists.
    AlreadyExists = 6,
    /// Permission denied.
    PermissionDenied = 7,
    /// Resource has been exhausted.
    ResourceExhausted = 8,
    /// Operation was rejected because the system is not in a state required for execution.
    FailedPrecondition = 9,
    /// Operation was aborted.
    Aborted = 10,
    /// Operation was attempted past the valid range.
    OutOfRange = 11,
    /// Operation is not implemented or supported.
    Unimplemented = 12,
    /// Internal error.
    Internal = 13,
    /// Service is currently unavailable.
    Unavailable = 14,
    /// Unrecoverable data loss or corruption.
    DataLoss = 15,
    /// Request does not have valid authentication credentials.
    Unauthenticated = 16,
}

impl Code {
    /// Create a Code from its numeric value.
    pub fn from_u32(value: u32) -> Self {
        match value {
            0 => Code::Ok,
            1 => Code::Cancelled,
            2 => Code::Unknown,
            3 => Code::InvalidArgument,
            4 => Code::DeadlineExceeded,
            5 => Code::NotFound,
            6 => Code::AlreadyExists,
            7 => Code::PermissionDenied,
            8 => Code::ResourceExhausted,
            9 => Code::FailedPrecondition,
            10 => Code::Aborted,
            11 => Code::OutOfRange,
            12 => Code::Unimplemented,
            13 => Code::Internal,
            14 => Code::Unavailable,
            15 => Code::DataLoss,
            16 => Code::Unauthenticated,
            _ => Code::Unknown,
        }
    }

    /// Get the numeric value of this code.
    pub fn as_u32(self) -> u32 {
        self as u32
    }

    /// Check if this is a successful status.
    pub fn is_ok(self) -> bool {
        self == Code::Ok
    }
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Code::Ok => "OK",
            Code::Cancelled => "CANCELLED",
            Code::Unknown => "UNKNOWN",
            Code::InvalidArgument => "INVALID_ARGUMENT",
            Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Code::NotFound => "NOT_FOUND",
            Code::AlreadyExists => "ALREADY_EXISTS",
            Code::PermissionDenied => "PERMISSION_DENIED",
            Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Code::FailedPrecondition => "FAILED_PRECONDITION",
            Code::Aborted => "ABORTED",
            Code::OutOfRange => "OUT_OF_RANGE",
            Code::Unimplemented => "UNIMPLEMENTED",
            Code::Internal => "INTERNAL",
            Code::Unavailable => "UNAVAILABLE",
            Code::DataLoss => "DATA_LOSS",
            Code::Unauthenticated => "UNAUTHENTICATED",
        };
        write!(f, "{}", name)
    }
}

/// gRPC status returned from an RPC.
#[derive(Debug, Clone)]
pub struct Status {
    /// The status code.
    code: Code,
    /// Optional human-readable message.
    message: Option<String>,
}

impl Status {
    /// Create a new status.
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: Some(message.into()),
        }
    }

    /// Create an OK status.
    pub fn ok() -> Self {
        Self {
            code: Code::Ok,
            message: None,
        }
    }

    /// Create a status from just a code.
    pub fn from_code(code: Code) -> Self {
        Self {
            code,
            message: None,
        }
    }

    /// Get the status code.
    pub fn code(&self) -> Code {
        self.code
    }

    /// Get the status message, if any.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Check if this is a successful status.
    pub fn is_ok(&self) -> bool {
        self.code.is_ok()
    }

    /// Create a cancelled status.
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(Code::Cancelled, message)
    }

    /// Create an unknown error status.
    pub fn unknown(message: impl Into<String>) -> Self {
        Self::new(Code::Unknown, message)
    }

    /// Create an invalid argument status.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(Code::InvalidArgument, message)
    }

    /// Create a deadline exceeded status.
    pub fn deadline_exceeded(message: impl Into<String>) -> Self {
        Self::new(Code::DeadlineExceeded, message)
    }

    /// Create a not found status.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(Code::NotFound, message)
    }

    /// Create an internal error status.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(Code::Internal, message)
    }

    /// Create an unavailable status.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(Code::Unavailable, message)
    }

    /// Create an unauthenticated status.
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(Code::Unauthenticated, message)
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code)?;
        if let Some(msg) = &self.message {
            write!(f, ": {}", msg)?;
        }
        Ok(())
    }
}

impl std::error::Error for Status {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_code_roundtrip() {
        for i in 0..=16 {
            let code = Code::from_u32(i);
            assert_eq!(code.as_u32(), i);
        }
    }

    #[test]
    fn test_code_from_u32_unknown() {
        // Values > 16 should return Unknown
        assert_eq!(Code::from_u32(17), Code::Unknown);
        assert_eq!(Code::from_u32(100), Code::Unknown);
        assert_eq!(Code::from_u32(u32::MAX), Code::Unknown);
    }

    #[test]
    fn test_code_is_ok() {
        assert!(Code::Ok.is_ok());
        assert!(!Code::Unknown.is_ok());
        assert!(!Code::Internal.is_ok());
    }

    #[test]
    fn test_code_display_all() {
        assert_eq!(format!("{}", Code::Ok), "OK");
        assert_eq!(format!("{}", Code::Cancelled), "CANCELLED");
        assert_eq!(format!("{}", Code::Unknown), "UNKNOWN");
        assert_eq!(format!("{}", Code::InvalidArgument), "INVALID_ARGUMENT");
        assert_eq!(format!("{}", Code::DeadlineExceeded), "DEADLINE_EXCEEDED");
        assert_eq!(format!("{}", Code::NotFound), "NOT_FOUND");
        assert_eq!(format!("{}", Code::AlreadyExists), "ALREADY_EXISTS");
        assert_eq!(format!("{}", Code::PermissionDenied), "PERMISSION_DENIED");
        assert_eq!(format!("{}", Code::ResourceExhausted), "RESOURCE_EXHAUSTED");
        assert_eq!(
            format!("{}", Code::FailedPrecondition),
            "FAILED_PRECONDITION"
        );
        assert_eq!(format!("{}", Code::Aborted), "ABORTED");
        assert_eq!(format!("{}", Code::OutOfRange), "OUT_OF_RANGE");
        assert_eq!(format!("{}", Code::Unimplemented), "UNIMPLEMENTED");
        assert_eq!(format!("{}", Code::Internal), "INTERNAL");
        assert_eq!(format!("{}", Code::Unavailable), "UNAVAILABLE");
        assert_eq!(format!("{}", Code::DataLoss), "DATA_LOSS");
        assert_eq!(format!("{}", Code::Unauthenticated), "UNAUTHENTICATED");
    }

    #[test]
    fn test_code_debug() {
        let code = Code::Internal;
        let debug_str = format!("{:?}", code);
        assert!(debug_str.contains("Internal"));
    }

    #[test]
    fn test_code_clone_copy() {
        let code1 = Code::NotFound;
        let code2 = code1;
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_code_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Code::Ok);
        set.insert(Code::Unknown);
        set.insert(Code::Ok); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_status_ok() {
        let status = Status::ok();
        assert!(status.is_ok());
        assert_eq!(status.code(), Code::Ok);
        assert!(status.message().is_none());
    }

    #[test]
    fn test_status_with_message() {
        let status = Status::not_found("key does not exist");
        assert!(!status.is_ok());
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(status.message(), Some("key does not exist"));
    }

    #[test]
    fn test_status_new() {
        let status = Status::new(Code::Internal, "something went wrong");
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), Some("something went wrong"));
    }

    #[test]
    fn test_status_from_code() {
        let status = Status::from_code(Code::Unavailable);
        assert_eq!(status.code(), Code::Unavailable);
        assert!(status.message().is_none());
    }

    #[test]
    fn test_status_cancelled() {
        let status = Status::cancelled("operation cancelled");
        assert_eq!(status.code(), Code::Cancelled);
        assert_eq!(status.message(), Some("operation cancelled"));
    }

    #[test]
    fn test_status_unknown() {
        let status = Status::unknown("unknown error");
        assert_eq!(status.code(), Code::Unknown);
        assert_eq!(status.message(), Some("unknown error"));
    }

    #[test]
    fn test_status_invalid_argument() {
        let status = Status::invalid_argument("bad input");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(status.message(), Some("bad input"));
    }

    #[test]
    fn test_status_deadline_exceeded() {
        let status = Status::deadline_exceeded("timeout");
        assert_eq!(status.code(), Code::DeadlineExceeded);
        assert_eq!(status.message(), Some("timeout"));
    }

    #[test]
    fn test_status_internal() {
        let status = Status::internal("internal error");
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), Some("internal error"));
    }

    #[test]
    fn test_status_unavailable() {
        let status = Status::unavailable("service down");
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(status.message(), Some("service down"));
    }

    #[test]
    fn test_status_unauthenticated() {
        let status = Status::unauthenticated("not logged in");
        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(status.message(), Some("not logged in"));
    }

    #[test]
    fn test_status_display_without_message() {
        let status = Status::from_code(Code::Ok);
        assert_eq!(format!("{}", status), "OK");
    }

    #[test]
    fn test_status_display_with_message() {
        let status = Status::not_found("entity not found");
        assert_eq!(format!("{}", status), "NOT_FOUND: entity not found");
    }

    #[test]
    fn test_status_clone() {
        let status1 = Status::internal("error");
        let status2 = status1.clone();
        assert_eq!(status1.code(), status2.code());
        assert_eq!(status1.message(), status2.message());
    }

    #[test]
    fn test_status_debug() {
        let status = Status::ok();
        let debug_str = format!("{:?}", status);
        assert!(debug_str.contains("Status"));
        assert!(debug_str.contains("Ok"));
    }

    #[test]
    fn test_status_is_error() {
        // Status implements std::error::Error
        fn assert_error<E: std::error::Error>() {}
        assert_error::<Status>();
    }
}
