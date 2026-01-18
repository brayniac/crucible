//! Core types for the ioru I/O framework.

use std::io;
use std::net::SocketAddr;

#[cfg(unix)]
use std::os::unix::io::RawFd;

// ============================================================================
// TCP Types
// ============================================================================

/// Opaque connection identifier.
///
/// Returned when registering a connection or accepting a new one.
/// Used to identify the connection in subsequent operations.
///
/// Internally encodes both a slot index and a generation counter to prevent
/// misattribution of data when connection IDs are reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnId(pub(crate) u64);

impl ConnId {
    /// Create a new connection ID from a raw slot value (generation 0).
    ///
    /// This is primarily useful for testing purposes.
    #[inline]
    pub fn new(slot: usize) -> Self {
        Self(slot as u64)
    }

    /// Create a connection ID with both slot and generation.
    #[inline]
    pub(crate) fn with_generation(slot: usize, generation: u32) -> Self {
        Self(((generation as u64) << 32) | (slot as u64 & 0xFFFF_FFFF))
    }

    /// Get the slot index from this connection ID.
    ///
    /// This extracts just the slot index, suitable for indexing into arrays.
    /// For HashMap keys, use `as_usize()` which includes the generation.
    #[inline]
    pub fn slot(&self) -> usize {
        (self.0 & 0xFFFF_FFFF) as usize
    }

    /// Get the generation counter from this connection ID.
    #[inline]
    pub(crate) fn generation(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// Get the raw value of the connection ID.
    ///
    /// This returns the full encoded value including generation,
    /// suitable for use as a HashMap key.
    #[inline]
    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }

    /// Get the raw u64 value of the connection ID.
    #[inline]
    pub fn as_u64(&self) -> u64 {
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

// ============================================================================
// UDP Types
// ============================================================================

/// Opaque UDP socket identifier.
///
/// Returned when binding a UDP socket.
/// Used to identify the socket in subsequent operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UdpSocketId(pub(crate) usize);

impl UdpSocketId {
    /// Create a new UDP socket ID from a raw value.
    #[inline]
    pub fn new(id: usize) -> Self {
        Self(id)
    }

    /// Get the raw value of the socket ID.
    #[inline]
    pub fn as_usize(&self) -> usize {
        self.0
    }
}

/// ECN (Explicit Congestion Notification) codepoint.
///
/// These are the two ECN bits from the IP header's Traffic Class (IPv6)
/// or TOS (IPv4) field. QUIC uses ECN for congestion control feedback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Ecn {
    /// Not ECN-Capable Transport (no ECN support)
    #[default]
    NotEct = 0b00,
    /// ECN Capable Transport (1)
    Ect1 = 0b01,
    /// ECN Capable Transport (0)
    Ect0 = 0b10,
    /// Congestion Experienced
    Ce = 0b11,
}

impl Ecn {
    /// Create an ECN value from the raw TOS/Traffic Class byte.
    #[inline]
    pub fn from_tos(tos: u8) -> Self {
        match tos & 0b11 {
            0b00 => Ecn::NotEct,
            0b01 => Ecn::Ect1,
            0b10 => Ecn::Ect0,
            0b11 => Ecn::Ce,
            _ => unreachable!(),
        }
    }

    /// Get the raw ECN bits for setting in TOS/Traffic Class.
    #[inline]
    pub fn to_tos(self) -> u8 {
        self as u8
    }
}

/// Metadata for a received UDP datagram.
#[derive(Debug, Clone)]
pub struct RecvMeta {
    /// Source address of the datagram.
    pub source: SocketAddr,
    /// Local destination address (if available via IP_PKTINFO/IPV6_PKTINFO).
    pub local: Option<SocketAddr>,
    /// ECN codepoint from the IP header.
    pub ecn: Ecn,
    /// Number of bytes received.
    pub len: usize,
}

/// Metadata for sending a UDP datagram.
#[derive(Debug, Clone)]
pub struct SendMeta {
    /// Destination address for the datagram.
    pub dest: SocketAddr,
    /// Source address to use (optional, requires IP_PKTINFO).
    pub source: Option<SocketAddr>,
    /// ECN codepoint to set in the IP header.
    pub ecn: Ecn,
}

impl SendMeta {
    /// Create send metadata with just a destination address.
    #[inline]
    pub fn new(dest: SocketAddr) -> Self {
        Self {
            dest,
            source: None,
            ecn: Ecn::NotEct,
        }
    }

    /// Set the ECN codepoint.
    #[inline]
    pub fn with_ecn(mut self, ecn: Ecn) -> Self {
        self.ecn = ecn;
        self
    }

    /// Set the source address.
    #[inline]
    pub fn with_source(mut self, source: SocketAddr) -> Self {
        self.source = Some(source);
        self
    }
}

// ============================================================================
// Completion Types
// ============================================================================

/// Result of a completed I/O operation.
#[derive(Debug)]
pub struct Completion {
    /// The type of completion.
    pub kind: CompletionKind,
    /// Whether more completions are expected from this operation.
    ///
    /// For multishot operations (recv, accept), this indicates whether the
    /// operation is still active and will produce more completions. When
    /// `more` is false, the operation has terminated and must be re-armed.
    ///
    /// For single-shot operations, this is always false.
    pub more: bool,
}

impl Completion {
    /// Create a new completion (more = false).
    #[inline]
    pub fn new(kind: CompletionKind) -> Self {
        Self { kind, more: false }
    }

    /// Create a new completion with explicit more flag.
    #[inline]
    pub fn with_more(kind: CompletionKind, more: bool) -> Self {
        Self { kind, more }
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
    /// Used by mio backend and io_uring multishot recv.
    Recv {
        /// The connection that has data available.
        conn_id: ConnId,
    },

    /// A submitted recv operation completed (io_uring single-shot mode).
    ///
    /// Data has been written directly to the buffer provided in `submit_recv()`.
    /// The `bytes` field indicates how many bytes were received.
    /// A value of 0 indicates EOF (peer closed connection).
    RecvComplete {
        /// The connection that received data.
        conn_id: ConnId,
        /// Number of bytes received (0 = EOF).
        bytes: usize,
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

    // === UDP events ===
    /// A UDP socket is readable (mio backend).
    ///
    /// Call `recvmsg()` on the driver to receive the datagram.
    UdpReadable {
        /// The socket that has data available.
        socket_id: UdpSocketId,
    },

    /// A submitted recvmsg operation completed (io_uring backend).
    ///
    /// The datagram data has been written to the buffer provided in `submit_recvmsg()`.
    RecvMsgComplete {
        /// The socket that received data.
        socket_id: UdpSocketId,
        /// Metadata about the received datagram.
        meta: RecvMeta,
    },

    /// A UDP socket is writable (mio backend).
    ///
    /// The socket is ready to send more datagrams.
    UdpWritable {
        /// The socket that is ready to send.
        socket_id: UdpSocketId,
    },

    /// A submitted sendmsg operation completed (io_uring backend).
    SendMsgComplete {
        /// The socket that sent data.
        socket_id: UdpSocketId,
        /// Number of bytes sent.
        bytes: usize,
    },

    /// An error occurred on a UDP socket.
    UdpError {
        /// The socket that had an error.
        socket_id: UdpSocketId,
        /// The error that occurred.
        error: io::Error,
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

/// Recv mode selection for io_uring.
///
/// Determines how the driver handles incoming data on connections.
///
/// Accepts: "multishot", "multi-shot", "singleshot", "single-shot"
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum RecvMode {
    /// Multishot recv mode (default).
    ///
    /// The driver automatically submits multishot recv operations using
    /// ring-provided buffers. Data is copied to per-connection buffers and
    /// `CompletionKind::Recv` events are produced.
    ///
    /// This mode is efficient for high-throughput scenarios as the kernel
    /// can complete multiple receives without re-submission.
    #[default]
    #[cfg_attr(feature = "serde", serde(alias = "multi-shot"))]
    Multishot,

    /// Single-shot recv mode.
    ///
    /// The driver does not automatically start recv operations. The caller
    /// must manually submit recv operations using `submit_recv()`, which
    /// produces `CompletionKind::RecvComplete` events with the number of
    /// bytes received.
    ///
    /// This mode gives the caller more control over buffer management and
    /// is useful when zero-copy semantics are desired.
    #[cfg_attr(feature = "serde", serde(alias = "singleshot", alias = "single-shot"))]
    SingleShot,
}

impl std::fmt::Display for RecvMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvMode::Multishot => write!(f, "multishot"),
            RecvMode::SingleShot => write!(f, "single-shot"),
        }
    }
}

bitflags::bitflags! {
    /// Capabilities supported by the driver backend.
    ///
    /// Use this to query which features are available at runtime.
    /// This enables writing code that can take advantage of advanced features
    /// when available while gracefully falling back on other platforms.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct DriverCapabilities: u32 {
        /// Zero-copy send (io_uring SEND_ZC).
        /// When available, send operations can avoid kernel copy.
        const ZEROCOPY_SEND = 1 << 0;
        /// Multishot receive - one submission, multiple completions.
        /// Kernel continues delivering data without re-submission.
        const MULTISHOT_RECV = 1 << 1;
        /// Multishot accept - one submission accepts multiple connections.
        /// Kernel continues accepting without re-submission.
        const MULTISHOT_ACCEPT = 1 << 2;
        /// Kernel-side buffer selection from provided rings.
        /// The kernel picks buffers without user-space involvement.
        const PROVIDED_BUFFERS = 1 << 3;
        /// Pre-registered fixed buffers for reduced syscall overhead.
        const FIXED_BUFFERS = 1 << 4;
        /// Async cancel support for in-flight operations.
        const ASYNC_CANCEL = 1 << 5;
        /// Vectored IO via sendmsg/recvmsg.
        const VECTORED_IO = 1 << 6;
        /// Direct descriptor support (io_uring registered fds).
        const DIRECT_DESCRIPTORS = 1 << 7;
    }
}

impl DriverCapabilities {
    /// Get all capabilities available on io_uring (Linux 6.0+).
    pub fn all_io_uring() -> Self {
        Self::ZEROCOPY_SEND
            | Self::MULTISHOT_RECV
            | Self::MULTISHOT_ACCEPT
            | Self::PROVIDED_BUFFERS
            | Self::FIXED_BUFFERS
            | Self::ASYNC_CANCEL
            | Self::VECTORED_IO
            | Self::DIRECT_DESCRIPTORS
    }

    /// Get baseline capabilities available on mio (all platforms).
    pub fn mio_baseline() -> Self {
        Self::VECTORED_IO
    }
}

/// Configuration for send behavior.
///
/// Controls when to use zero-copy sends vs buffered sends:
/// - Zero-copy: Driver holds the buffer until send completes (no copy)
/// - Buffered: Data copied to internal pool, original buffer released immediately
///
/// Zero-copy is optimal for large payloads but requires the driver to hold
/// ownership of the buffer. Buffered mode allows immediate buffer reuse
/// with predictable memory usage (no per-send allocation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum SendMode {
    /// Always use zero-copy sends.
    ///
    /// The driver takes ownership of the buffer (via `ZeroCopySend` trait)
    /// and holds it until the send completes. Best throughput for large
    /// payloads, but the buffer cannot be reused until completion.
    ZeroCopy,

    /// Always copy to internal buffer pool before sending.
    ///
    /// Data is copied to a pre-allocated pooled buffer, then sent from there.
    /// The original buffer can be reused immediately after send() returns.
    /// No per-send allocation - pool is pre-allocated.
    #[default]
    Buffered,

    /// Use zero-copy for sends >= threshold bytes, buffered otherwise.
    ///
    /// Small sends use buffered mode (low overhead for small copies).
    /// Large sends use zero-copy (avoids copying large payloads).
    /// The threshold should be tuned based on workload characteristics.
    Threshold(usize),
}

impl SendMode {
    /// Create a threshold-based mode.
    ///
    /// Sends >= `bytes` will use zero-copy, smaller sends use buffered.
    pub fn threshold(bytes: usize) -> Self {
        Self::Threshold(bytes)
    }

    /// Check if a send of `len` bytes should use zero-copy.
    pub fn should_zerocopy(&self, len: usize) -> bool {
        match self {
            SendMode::ZeroCopy => true,
            SendMode::Buffered => false,
            SendMode::Threshold(threshold) => len >= *threshold,
        }
    }
}

impl std::fmt::Display for SendMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendMode::ZeroCopy => write!(f, "zerocopy"),
            SendMode::Buffered => write!(f, "buffered"),
            SendMode::Threshold(n) => write!(f, "threshold({})", n),
        }
    }
}

impl std::str::FromStr for SendMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_lowercase();
        match lower.as_str() {
            "zerocopy" | "zero-copy" | "zc" => Ok(SendMode::ZeroCopy),
            "buffered" | "pool" | "pooled" => Ok(SendMode::Buffered),
            _ if lower.starts_with("threshold(") && lower.ends_with(')') => {
                let inner = &lower[10..lower.len() - 1];
                inner
                    .parse::<usize>()
                    .map(SendMode::Threshold)
                    .map_err(|_| format!("invalid threshold value: {}", inner))
            }
            _ => Err(format!("unknown send mode: {}", s)),
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
