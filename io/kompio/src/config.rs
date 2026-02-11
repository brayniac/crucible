use crate::buffer::fixed::MemoryRegion;

/// TLS configuration. Pass a pre-built rustls ServerConfig.
#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct TlsConfig {
    /// Pre-built rustls ServerConfig. User loads certs/keys and configures ALPN etc.
    pub server_config: std::sync::Arc<rustls::ServerConfig>,
}

/// TLS client configuration for outbound connections.
#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct TlsClientConfig {
    /// Pre-built rustls ClientConfig. User configures root certs, ALPN, etc.
    pub client_config: std::sync::Arc<rustls::ClientConfig>,
}

/// Configuration for the io_uring driver.
#[derive(Clone)]
pub struct Config {
    /// Number of SQ entries. CQ will be 4x this.
    pub sq_entries: u32,
    /// Enable SQPOLL mode (kernel-side submission polling).
    pub sqpoll: bool,
    /// SQPOLL idle timeout in milliseconds.
    pub sqpoll_idle_ms: u32,
    /// Pin SQPOLL kernel thread to this CPU core. Only meaningful when sqpoll=true.
    pub sqpoll_cpu: Option<u32>,
    /// Recv buffer configuration (provided buffer ring).
    pub recv_buffer: RecvBufferConfig,
    /// User-registered memory regions (e.g., mmap'd storage arenas).
    pub registered_regions: Vec<MemoryRegion>,
    /// Worker/thread configuration.
    pub worker: WorkerConfig,
    /// TCP listen backlog.
    pub backlog: i32,
    /// Maximum number of direct file descriptors (connections).
    pub max_connections: u32,
    /// Initial capacity for per-connection recv accumulators.
    pub recv_accumulator_capacity: usize,
    /// Number of copy-send pool slots. Each in-flight `send()` or copy part of a
    /// `send_parts()` call holds one slot until the kernel completes the send.
    /// Size this to cover your peak in-flight send count — exhaustion returns an
    /// error to the handler. Memory cost: `send_copy_count * send_copy_slot_size`.
    pub send_copy_count: u16,
    /// Size of each copy-send pool slot in bytes. A single `send()` or the
    /// combined copy parts of one `send_parts()` call must fit in one slot.
    pub send_copy_slot_size: u32,
    /// Number of InFlightSendSlab slots for in-flight scatter-gather sends
    /// (i.e., `send_parts()` calls that include at least one guard).
    /// Each slot is held until all ZC notifications arrive.
    pub send_slab_slots: u16,
    /// Deadline-based flush interval in microseconds during CQE processing.
    /// When non-SQPOLL, if this many microseconds elapse since the last submit
    /// while processing a CQE batch, pending SQEs are flushed mid-iteration.
    /// 0 = disabled. Ignored when SQPOLL is active (kernel handles it).
    pub flush_interval_us: u64,
    /// Optional TLS configuration. When set, all accepted connections use TLS.
    #[cfg(feature = "tls")]
    pub tls: Option<TlsConfig>,
    /// Optional TLS client configuration for outbound `connect_tls()` calls.
    #[cfg(feature = "tls")]
    pub tls_client: Option<TlsClientConfig>,
    /// Enable TCP_NODELAY on all connections (accepted and outbound).
    pub tcp_nodelay: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sq_entries: 256,
            sqpoll: false,
            sqpoll_idle_ms: 1000,
            sqpoll_cpu: None,
            recv_buffer: RecvBufferConfig::default(),
            registered_regions: Vec::new(),
            worker: WorkerConfig::default(),
            backlog: 1024,
            max_connections: 16384,
            recv_accumulator_capacity: 65536,
            send_copy_count: 1024,
            send_copy_slot_size: 16384,
            send_slab_slots: 512,
            flush_interval_us: 100,
            #[cfg(feature = "tls")]
            tls: None,
            #[cfg(feature = "tls")]
            tls_client: None,
            tcp_nodelay: true,
        }
    }
}

/// Configuration for the provided buffer ring (multishot recv).
#[derive(Clone)]
pub struct RecvBufferConfig {
    /// Number of buffers in the ring (must be power of 2).
    pub ring_size: u16,
    /// Size of each buffer in bytes.
    pub buffer_size: u32,
    /// Buffer group ID for the provided buffer ring.
    pub bgid: u16,
}

impl Default for RecvBufferConfig {
    fn default() -> Self {
        Self {
            ring_size: 256,
            buffer_size: 16384,
            bgid: 0,
        }
    }
}

/// Configuration for the thread-per-core worker model.
#[derive(Clone)]
pub struct WorkerConfig {
    /// Number of worker threads. 0 = number of CPUs.
    pub threads: usize,
    /// Whether to pin each worker to a CPU core.
    pub pin_to_core: bool,
    /// Starting CPU core index for pinning.
    pub core_offset: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            threads: 0,
            pin_to_core: true,
            core_offset: 0,
        }
    }
}
