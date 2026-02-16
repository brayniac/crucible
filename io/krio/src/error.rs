use std::fmt;
use std::io;

/// Errors returned by the krio driver.
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
    /// System resource limit too low (e.g., RLIMIT_NOFILE).
    ResourceLimit(String),
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
            Error::ResourceLimit(msg) => write!(f, "{msg}"),
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

/// Error returned by [`try_spawn`](crate::try_spawn) when the standalone task slab is full.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnError;

impl fmt::Display for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("standalone task slab exhausted")
    }
}

impl std::error::Error for SpawnError {}
