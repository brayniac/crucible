//! Proxy configuration.

use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

/// Main proxy configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Proxy listener configuration.
    #[serde(default)]
    pub proxy: ProxyConfig,

    /// Worker thread configuration.
    #[serde(default)]
    pub workers: WorkersConfig,

    /// Backend cluster configuration.
    pub backend: BackendConfig,

    /// Local cache configuration.
    #[serde(default)]
    pub cache: CacheConfig,

    /// io_uring specific settings.
    #[serde(default)]
    pub uring: UringConfig,

    /// Metrics server configuration.
    #[serde(default)]
    pub metrics: MetricsConfig,

    /// Logging configuration.
    #[serde(default)]
    pub logging: LoggingConfig,

    /// Shutdown configuration.
    #[serde(default)]
    pub shutdown: ShutdownConfig,
}

impl Config {
    /// Load configuration from a TOML file.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(ConfigError::Io)?;
        toml::from_str(&content).map_err(ConfigError::Parse)
    }

    /// Get the number of worker threads.
    pub fn threads(&self) -> usize {
        self.workers.threads.unwrap_or_else(num_cpus::get)
    }

    /// Get CPU affinity list if configured.
    pub fn cpu_affinity(&self) -> Option<Vec<usize>> {
        self.workers
            .cpu_affinity
            .as_ref()
            .map(|s| parse_cpu_list(s))
    }

}

/// Proxy listener configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ProxyConfig {
    /// Address to listen on for client connections.
    #[serde(default = "ProxyConfig::default_listen")]
    pub listen: SocketAddr,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            listen: Self::default_listen(),
        }
    }
}

impl ProxyConfig {
    fn default_listen() -> SocketAddr {
        "0.0.0.0:6379".parse().unwrap()
    }
}

/// Worker thread configuration.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WorkersConfig {
    /// Number of worker threads. Defaults to number of CPUs.
    pub threads: Option<usize>,

    /// CPU affinity string (e.g., "0-3,6-8").
    pub cpu_affinity: Option<String>,
}

/// Backend cluster configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct BackendConfig {
    /// Backend node addresses (seed nodes for cluster discovery).
    pub nodes: Vec<SocketAddr>,

    /// Number of connections per backend node per worker.
    #[serde(default = "BackendConfig::default_pool_size")]
    pub pool_size: usize,

    /// Connection timeout in milliseconds.
    #[serde(default = "BackendConfig::default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// Read timeout in milliseconds.
    #[serde(default = "BackendConfig::default_read_timeout_ms")]
    pub read_timeout_ms: u64,
}

impl BackendConfig {
    fn default_pool_size() -> usize {
        2
    }

    fn default_connect_timeout_ms() -> u64 {
        1000
    }

    fn default_read_timeout_ms() -> u64 {
        500
    }
}

/// Local cache configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    /// Enable local caching of GET responses.
    #[serde(default = "CacheConfig::default_enabled")]
    pub enabled: bool,

    /// TTL for cached items in milliseconds.
    #[serde(default = "CacheConfig::default_ttl_ms")]
    pub ttl_ms: u64,

    /// Total heap size for the cache in bytes.
    #[serde(default = "CacheConfig::default_heap_size")]
    pub heap_size: usize,

    /// Segment size in bytes.
    #[serde(default = "CacheConfig::default_segment_size")]
    pub segment_size: usize,

    /// Eviction policy: "random", "fifo", "cte", "merge", or "s3fifo".
    #[serde(default)]
    pub eviction: EvictionPolicy,

    /// Hashtable power (2^power buckets).
    #[serde(default = "CacheConfig::default_hashtable_power")]
    pub hashtable_power: u8,
}

/// Cache eviction policy.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EvictionPolicy {
    /// Random segment selection within TTL buckets (default).
    #[default]
    Random,
    /// Strict FIFO segment eviction.
    Fifo,
    /// Closest to expiration eviction.
    Cte,
    /// Adaptive merge eviction - prunes low-frequency items before full eviction.
    Merge,
    /// S3-FIFO: Two-tier admission filtering architecture.
    S3fifo,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: Self::default_enabled(),
            ttl_ms: Self::default_ttl_ms(),
            heap_size: Self::default_heap_size(),
            segment_size: Self::default_segment_size(),
            eviction: EvictionPolicy::default(),
            hashtable_power: Self::default_hashtable_power(),
        }
    }
}

impl CacheConfig {
    fn default_enabled() -> bool {
        true
    }

    fn default_ttl_ms() -> u64 {
        300_000 // 5 minutes
    }

    fn default_heap_size() -> usize {
        64 * 1024 * 1024 // 64MB
    }

    fn default_segment_size() -> usize {
        1024 * 1024 // 1MB
    }

    fn default_hashtable_power() -> u8 {
        16 // 64K buckets
    }
}

/// io_uring configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct UringConfig {
    /// Buffer size for recv operations.
    #[serde(default = "UringConfig::default_buffer_size")]
    pub buffer_size: usize,

    /// Number of buffers in the buffer pool.
    #[serde(default = "UringConfig::default_buffer_count")]
    pub buffer_count: u16,

    /// Submission queue depth.
    #[serde(default = "UringConfig::default_sq_depth")]
    pub sq_depth: u32,

    /// Enable SQPOLL mode.
    #[serde(default)]
    pub sqpoll: bool,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            buffer_size: Self::default_buffer_size(),
            buffer_count: Self::default_buffer_count(),
            sq_depth: Self::default_sq_depth(),
            sqpoll: false,
        }
    }
}

impl UringConfig {
    fn default_buffer_size() -> usize {
        4096
    }

    fn default_buffer_count() -> u16 {
        1024
    }

    fn default_sq_depth() -> u32 {
        1024
    }
}

/// Metrics server configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    /// Address for the admin/metrics server.
    #[serde(default = "MetricsConfig::default_address")]
    pub address: SocketAddr,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            address: Self::default_address(),
        }
    }
}

impl MetricsConfig {
    fn default_address() -> SocketAddr {
        "0.0.0.0:9090".parse().unwrap()
    }
}

/// Logging configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    /// Log level.
    #[serde(default = "LoggingConfig::default_level")]
    pub level: String,

    /// Log format: "pretty", "json", or "compact".
    #[serde(default = "LoggingConfig::default_format")]
    pub format: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: Self::default_level(),
            format: Self::default_format(),
        }
    }
}

impl LoggingConfig {
    fn default_level() -> String {
        "info".to_string()
    }

    fn default_format() -> String {
        "pretty".to_string()
    }
}

/// Shutdown configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ShutdownConfig {
    /// Drain timeout in seconds.
    #[serde(default = "ShutdownConfig::default_drain_timeout_secs")]
    pub drain_timeout_secs: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            drain_timeout_secs: Self::default_drain_timeout_secs(),
        }
    }
}

impl ShutdownConfig {
    fn default_drain_timeout_secs() -> u64 {
        30
    }
}

/// Configuration error.
#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Parse(toml::de::Error),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "IO error: {}", e),
            ConfigError::Parse(e) => write!(f, "Parse error: {}", e),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Parse a CPU list string like "0-3,6-8" into a Vec of CPU IDs.
fn parse_cpu_list(s: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if let Some((start, end)) = part.split_once('-') {
            if let (Ok(start), Ok(end)) =
                (start.trim().parse::<usize>(), end.trim().parse::<usize>())
            {
                cpus.extend(start..=end);
            }
        } else if let Ok(cpu) = part.parse::<usize>() {
            cpus.push(cpu);
        }
    }
    cpus
}
