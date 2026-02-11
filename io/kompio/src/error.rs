use std::fmt;
use std::io;

/// Errors returned by the kompio driver.
#[derive(Debug)]
pub enum Error {
    /// io_uring setup or operation failed.
    Io(io::Error),
    /// Ring setup failed (e.g., unsupported kernel features).
    RingSetup(String),
    /// Buffer registration failed.
    BufferRegistration(String),
    /// No free connection slots available.
    ConnectionLimitReached,
    /// Invalid connection token (stale or out of range).
    InvalidConnection,
    /// No send pool slots available.
    SendPoolExhausted,
    /// Invalid memory region ID.
    InvalidRegion,
    /// Pointer not within the specified registered region.
    PointerOutOfRegion,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::RingSetup(msg) => write!(f, "ring setup: {msg}"),
            Error::BufferRegistration(msg) => write!(f, "buffer registration: {msg}"),
            Error::ConnectionLimitReached => write!(f, "connection limit reached"),
            Error::InvalidConnection => write!(f, "invalid connection"),
            Error::SendPoolExhausted => write!(f, "send pool exhausted"),
            Error::InvalidRegion => write!(f, "invalid memory region ID"),
            Error::PointerOutOfRegion => write!(f, "pointer not within registered region"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
