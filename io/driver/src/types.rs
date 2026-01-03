//! Core types for the ioru I/O framework.

use std::io;
use std::net::SocketAddr;

#[cfg(unix)]
use std::os::unix::io::RawFd;

/// Opaque connection identifier.
///
/// Returned when registering a connection or accepting a new one.
/// Used to identify the connection in subsequent operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnId(pub(crate) usize);

impl ConnId {
    /// Create a new connection ID from a raw value.
    ///
    /// This is primarily useful for testing purposes.
    #[inline]
    pub fn new(id: usize) -> Self {
        Self(id)
    }

    /// Get the raw value of the connection ID.
    #[inline]
    pub fn as_usize(&self) -> usize {
        self.0
    }
}

/// Opaque listener identifier.
///
/// Returned when creating a listener.
/// Used to identify the listener in subsequent operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ListenerId(pub(crate) usize);

impl ListenerId {
    /// Create a new listener ID from a raw value.
    ///
    /// This is primarily useful for testing purposes.
    #[inline]
    pub fn new(id: usize) -> Self {
        Self(id)
    }

    /// Get the raw value of the listener ID.
    #[inline]
    pub fn as_usize(&self) -> usize {
        self.0
    }
}

/// Result of a completed I/O operation.
#[derive(Debug)]
pub struct Completion {
    /// The type of completion.
    pub kind: CompletionKind,
}

impl Completion {
    /// Create a new completion.
    #[inline]
    pub fn new(kind: CompletionKind) -> Self {
        Self { kind }
    }
}

/// Type of I/O completion.
///
/// This enum represents all possible completion types from both connection
/// and listener operations. The API is unified across backends - users don't
/// need to know whether io_uring or mio is being used underneath.
#[derive(Debug)]
pub enum CompletionKind {
    // === Connection events ===
    /// Data is available to read.
    ///
    /// Call `recv()` on the driver to get the data.
    Recv {
        /// The connection that has data available.
        conn_id: ConnId,
    },

    /// The connection is ready to send more data.
    ///
    /// This is emitted after a send operation completes or when
    /// the connection becomes writable.
    SendReady {
        /// The connection that is ready to send.
        conn_id: ConnId,
    },

    /// The connection was closed by the peer.
    Closed {
        /// The connection that was closed.
        conn_id: ConnId,
    },

    /// An error occurred on this connection.
    Error {
        /// The connection that had an error.
        conn_id: ConnId,
        /// The error that occurred.
        error: io::Error,
    },

    // === Listener events ===
    /// A new connection was accepted.
    ///
    /// The new connection is already registered with the driver and
    /// ready for I/O operations.
    Accept {
        /// The listener that accepted the connection.
        listener_id: ListenerId,
        /// The new connection.
        conn_id: ConnId,
        /// The address of the remote peer.
        addr: SocketAddr,
    },

    /// An error occurred on a listener.
    ListenerError {
        /// The listener that had an error.
        listener_id: ListenerId,
        /// The error that occurred.
        error: io::Error,
    },

    /// A new connection was accepted in raw mode.
    ///
    /// Unlike `Accept`, the connection is NOT automatically registered.
    /// The raw fd must be explicitly registered with a worker using `register_fd()`.
    /// This is used for single-acceptor patterns with round-robin distribution.
    AcceptRaw {
        /// The listener that accepted the connection.
        listener_id: ListenerId,
        /// The raw file descriptor of the accepted connection.
        raw_fd: RawFd,
        /// The address of the remote peer.
        addr: SocketAddr,
    },
}

/// I/O engine selection.
///
/// Determines which I/O backend to use.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum IoEngine {
    /// Automatically select the best available engine.
    ///
    /// Uses io_uring on Linux 5.19+ with the feature enabled,
    /// falls back to mio elsewhere.
    #[default]
    Auto,

    /// Use mio (epoll on Linux, kqueue on macOS).
    ///
    /// This is the cross-platform fallback that works everywhere.
    Mio,

    /// Use io_uring (Linux only).
    ///
    /// Requires Linux kernel 5.19+ and the `io_uring` feature.
    /// Returns an error if not available.
    Uring,
}

impl std::fmt::Display for IoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoEngine::Auto => write!(f, "auto"),
            IoEngine::Mio => write!(f, "mio"),
            IoEngine::Uring => write!(f, "uring"),
        }
    }
}

impl std::str::FromStr for IoEngine {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(IoEngine::Auto),
            "mio" | "epoll" | "kqueue" => Ok(IoEngine::Mio),
            "uring" | "io_uring" | "io-uring" | "iou" => Ok(IoEngine::Uring),
            _ => Err(format!("unknown io engine: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_conn_id_new_and_as_usize() {
        let id = ConnId::new(42);
        assert_eq!(id.as_usize(), 42);
    }

    #[test]
    fn test_conn_id_clone_and_copy() {
        let id1 = ConnId::new(100);
        let id2 = id1;
        assert_eq!(id1, id2);
        assert_eq!(id1.as_usize(), 100);
    }

    #[test]
    fn test_conn_id_equality() {
        let id1 = ConnId::new(1);
        let id2 = ConnId::new(1);
        let id3 = ConnId::new(2);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_conn_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ConnId::new(1));
        set.insert(ConnId::new(2));
        set.insert(ConnId::new(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_conn_id_debug() {
        let id = ConnId::new(42);
        let debug_str = format!("{:?}", id);
        assert!(debug_str.contains("42"));
    }

    #[test]
    fn test_listener_id_new_and_as_usize() {
        let id = ListenerId::new(99);
        assert_eq!(id.as_usize(), 99);
    }

    #[test]
    fn test_listener_id_clone_and_copy() {
        let id1 = ListenerId::new(50);
        let id2 = id1;
        assert_eq!(id1, id2);
        assert_eq!(id1.as_usize(), 50);
    }

    #[test]
    fn test_listener_id_equality() {
        let id1 = ListenerId::new(1);
        let id2 = ListenerId::new(1);
        let id3 = ListenerId::new(2);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_listener_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ListenerId::new(1));
        set.insert(ListenerId::new(2));
        set.insert(ListenerId::new(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_listener_id_debug() {
        let id = ListenerId::new(123);
        let debug_str = format!("{:?}", id);
        assert!(debug_str.contains("123"));
    }

    #[test]
    fn test_completion_new() {
        let completion = Completion::new(CompletionKind::Recv {
            conn_id: ConnId::new(1),
        });
        match completion.kind {
            CompletionKind::Recv { conn_id } => assert_eq!(conn_id.as_usize(), 1),
            _ => panic!("unexpected completion kind"),
        }
    }

    #[test]
    fn test_completion_kind_recv() {
        let kind = CompletionKind::Recv {
            conn_id: ConnId::new(5),
        };
        if let CompletionKind::Recv { conn_id } = kind {
            assert_eq!(conn_id.as_usize(), 5);
        } else {
            panic!("expected Recv");
        }
    }

    #[test]
    fn test_completion_kind_send_ready() {
        let kind = CompletionKind::SendReady {
            conn_id: ConnId::new(10),
        };
        if let CompletionKind::SendReady { conn_id } = kind {
            assert_eq!(conn_id.as_usize(), 10);
        } else {
            panic!("expected SendReady");
        }
    }

    #[test]
    fn test_completion_kind_closed() {
        let kind = CompletionKind::Closed {
            conn_id: ConnId::new(15),
        };
        if let CompletionKind::Closed { conn_id } = kind {
            assert_eq!(conn_id.as_usize(), 15);
        } else {
            panic!("expected Closed");
        }
    }

    #[test]
    fn test_completion_kind_error() {
        let kind = CompletionKind::Error {
            conn_id: ConnId::new(20),
            error: io::Error::new(io::ErrorKind::ConnectionReset, "test error"),
        };
        if let CompletionKind::Error { conn_id, error } = kind {
            assert_eq!(conn_id.as_usize(), 20);
            assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        } else {
            panic!("expected Error");
        }
    }

    #[test]
    fn test_completion_kind_accept() {
        let kind = CompletionKind::Accept {
            listener_id: ListenerId::new(1),
            conn_id: ConnId::new(25),
            addr: "127.0.0.1:8080".parse().unwrap(),
        };
        if let CompletionKind::Accept {
            listener_id,
            conn_id,
            addr,
        } = kind
        {
            assert_eq!(listener_id.as_usize(), 1);
            assert_eq!(conn_id.as_usize(), 25);
            assert_eq!(addr.port(), 8080);
        } else {
            panic!("expected Accept");
        }
    }

    #[test]
    fn test_completion_kind_listener_error() {
        let kind = CompletionKind::ListenerError {
            listener_id: ListenerId::new(5),
            error: io::Error::new(io::ErrorKind::AddrInUse, "address in use"),
        };
        if let CompletionKind::ListenerError { listener_id, error } = kind {
            assert_eq!(listener_id.as_usize(), 5);
            assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
        } else {
            panic!("expected ListenerError");
        }
    }

    #[test]
    fn test_completion_kind_debug() {
        let kind = CompletionKind::Recv {
            conn_id: ConnId::new(1),
        };
        let debug_str = format!("{:?}", kind);
        assert!(debug_str.contains("Recv"));
    }

    #[test]
    fn test_io_engine_default() {
        let engine = IoEngine::default();
        assert_eq!(engine, IoEngine::Auto);
    }

    #[test]
    fn test_io_engine_display() {
        assert_eq!(format!("{}", IoEngine::Auto), "auto");
        assert_eq!(format!("{}", IoEngine::Mio), "mio");
        assert_eq!(format!("{}", IoEngine::Uring), "uring");
    }

    #[test]
    fn test_io_engine_from_str() {
        assert_eq!(IoEngine::from_str("auto").unwrap(), IoEngine::Auto);
        assert_eq!(IoEngine::from_str("AUTO").unwrap(), IoEngine::Auto);
        assert_eq!(IoEngine::from_str("mio").unwrap(), IoEngine::Mio);
        assert_eq!(IoEngine::from_str("MIO").unwrap(), IoEngine::Mio);
        assert_eq!(IoEngine::from_str("epoll").unwrap(), IoEngine::Mio);
        assert_eq!(IoEngine::from_str("kqueue").unwrap(), IoEngine::Mio);
        assert_eq!(IoEngine::from_str("uring").unwrap(), IoEngine::Uring);
        assert_eq!(IoEngine::from_str("io_uring").unwrap(), IoEngine::Uring);
        assert_eq!(IoEngine::from_str("io-uring").unwrap(), IoEngine::Uring);
        assert_eq!(IoEngine::from_str("iou").unwrap(), IoEngine::Uring);
    }

    #[test]
    fn test_io_engine_from_str_error() {
        let result = IoEngine::from_str("invalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown io engine"));
    }

    #[test]
    fn test_io_engine_clone() {
        let engine1 = IoEngine::Mio;
        let engine2 = engine1;
        assert_eq!(engine1, engine2);
    }

    #[test]
    fn test_io_engine_debug() {
        let engine = IoEngine::Uring;
        let debug_str = format!("{:?}", engine);
        assert!(debug_str.contains("Uring"));
    }
}
