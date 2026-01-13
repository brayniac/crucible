//! Server configuration.
//!
//! Supports runtime selection (native vs tokio), cache backend selection,
//! and multiple protocol listeners.

use io_driver::IoEngine;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

/// Server configuration loaded from TOML file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Runtime selection: "native" (io_uring/mio) or "tokio"
    #[serde(default = "default_runtime")]
    pub runtime: Runtime,

    /// Worker thread configuration
    #[serde(default)]
    pub workers: WorkersConfig,

    /// Cache configuration
    #[serde(default)]
    pub cache: CacheConfig,

    /// Protocol listeners
    #[serde(default)]
    pub listener: Vec<ListenerConfig>,

    /// Metrics endpoint configuration
    #[serde(default)]
    pub metrics: MetricsConfig,

    /// I/O engine selection for native runtime: "auto", "mio", or "uring"
    #[serde(default)]
    pub io_engine: IoEngine,

    /// io_uring specific configuration (only used when io_engine = "uring" or "auto" on Linux 6.0+)
    #[serde(default)]
    pub uring: UringConfig,

    /// Zero-copy mode for GET responses.
    /// Controls whether to use scatter-gather I/O to send values directly from cache segments.
    #[serde(default)]
    pub zero_copy: ZeroCopyMode,
}

/// Zero-copy mode for GET responses.
///
/// Controls whether to use scatter-gather I/O to send cached values directly
/// from segment memory without copying to intermediate buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ZeroCopyMode {
    /// Always copy values to write buffer (current behavior).
    /// Most compatible, no segment hold concerns.
    #[default]
    Disabled,

    /// Always use zero-copy scatter-gather for all values.
    /// Maximum performance but holds segment refs during I/O.
    Enabled,

    /// Use zero-copy for values >= threshold bytes (default: 1024).
    /// Balances performance with segment hold concerns.
    /// Small values are copied (negligible overhead), large values are zero-copy.
    Threshold,
}

impl ZeroCopyMode {
    /// Get the threshold size for zero-copy mode.
    /// Returns None for Disabled/Enabled, Some(threshold) for Threshold mode.
    pub fn threshold(&self) -> Option<usize> {
        match self {
            ZeroCopyMode::Disabled => None,
            ZeroCopyMode::Enabled => Some(0), // All values
            ZeroCopyMode::Threshold => Some(DEFAULT_ZERO_COPY_THRESHOLD),
        }
    }

    /// Check if zero-copy should be used for a value of the given size.
    pub fn should_use_zero_copy(&self, value_len: usize) -> bool {
        match self {
            ZeroCopyMode::Disabled => false,
            ZeroCopyMode::Enabled => true,
            ZeroCopyMode::Threshold => value_len >= DEFAULT_ZERO_COPY_THRESHOLD,
        }
    }
}

/// Default threshold for zero-copy mode (1KB).
/// Values smaller than this are copied; larger values use scatter-gather.
pub const DEFAULT_ZERO_COPY_THRESHOLD: usize = 1024;

impl<'de> Deserialize<'de> for ZeroCopyMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum ZeroCopyValue {
            Bool(bool),
            String(String),
        }

        match ZeroCopyValue::deserialize(deserializer)? {
            ZeroCopyValue::Bool(true) => Ok(ZeroCopyMode::Enabled),
            ZeroCopyValue::Bool(false) => Ok(ZeroCopyMode::Disabled),
            ZeroCopyValue::String(s) => match s.to_lowercase().as_str() {
                "disabled" | "off" | "no" | "false" => Ok(ZeroCopyMode::Disabled),
                "enabled" | "on" | "yes" | "true" => Ok(ZeroCopyMode::Enabled),
                "threshold" | "auto" | "default" => Ok(ZeroCopyMode::Threshold),
                _ => Err(serde::de::Error::custom(format!(
                    "invalid zero_copy value: '{}' (expected 'disabled', 'enabled', or 'threshold')",
                    s
                ))),
            },
        }
    }
}

/// Runtime selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Runtime {
    /// Native completion-based I/O (io_uring on Linux 6.0+, mio fallback)
    #[default]
    Native,
    /// Tokio async runtime with work-stealing scheduler
    Tokio,
}

/// Worker thread configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct WorkersConfig {
    /// Number of worker threads (default: derived from cpu_affinity or number of CPUs)
    pub threads: Option<usize>,

    /// CPU cores to pin worker threads to, Linux-style (e.g., "0-3,6-8")
    pub cpu_affinity: Option<String>,
}

/// Cache backend configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    /// Cache backend: "segcache" or "s3fifo"
    #[serde(default = "default_cache_backend")]
    pub backend: CacheBackend,

    /// Total heap size for cache (e.g., "4GB", "512MB")
    #[serde(default = "default_heap_size", deserialize_with = "deserialize_size")]
    pub heap_size: usize,

    /// Segment size (e.g., "1MB", "512KB")
    #[serde(
        default = "default_segment_size",
        deserialize_with = "deserialize_size"
    )]
    pub segment_size: usize,

    /// Maximum value size (e.g., "1MB", "512KB").
    /// Values larger than this will be rejected by the server.
    /// Must be less than segment_size.
    #[serde(
        default = "default_max_value_size",
        deserialize_with = "deserialize_size"
    )]
    pub max_value_size: usize,

    /// Hashtable power (2^power buckets)
    #[serde(default = "default_hashtable_power")]
    pub hashtable_power: u8,

    /// Hugepage size preference: "none", "2mb", or "1gb"
    #[serde(default)]
    pub hugepage: HugepageConfig,

    /// NUMA node to bind cache memory to (Linux only).
    /// If not specified, memory is allocated according to the default policy.
    #[serde(default)]
    pub numa_node: Option<u32>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            backend: default_cache_backend(),
            heap_size: default_heap_size(),
            segment_size: default_segment_size(),
            max_value_size: default_max_value_size(),
            hashtable_power: default_hashtable_power(),
            hugepage: HugepageConfig::default(),
            numa_node: None,
        }
    }
}

/// Cache backend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CacheBackend {
    /// Segcache - segment-based cache
    #[default]
    Segcache,
    /// S3-FIFO - Simple, Scalable eviction with three FIFO queues
    S3fifo,
}

/// Hugepage size configuration.
///
/// Controls whether to use explicit hugepages for cache memory allocation.
/// Falls back to regular pages (with THP hint) if hugepages are unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HugepageConfig {
    /// No explicit hugepages, use regular 4KB pages.
    /// The OS may still use THP if configured system-wide.
    #[default]
    None,
    /// 2MB hugepages (Linux only).
    TwoMegabyte,
    /// 1GB hugepages (Linux only).
    OneGigabyte,
}

impl<'de> Deserialize<'de> for HugepageConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "none" => Ok(HugepageConfig::None),
            "2mb" => Ok(HugepageConfig::TwoMegabyte),
            "1gb" => Ok(HugepageConfig::OneGigabyte),
            _ => Err(serde::de::Error::custom(format!(
                "invalid hugepage size: '{}' (expected 'none', '2mb', or '1gb')",
                s
            ))),
        }
    }
}

/// Protocol listener configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenerConfig {
    /// Protocol to serve
    pub protocol: Protocol,

    /// Address to listen on
    pub address: SocketAddr,

    /// TLS configuration (optional)
    pub tls: Option<TlsConfig>,
}

/// Supported protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    /// Redis RESP protocol
    Resp,
    /// Memcache ASCII/binary protocol
    Memcache,
    /// Momento gRPC protocol
    Momento,
}

/// TLS configuration for a listener.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Path to certificate file (PEM format)
    pub cert: String,
    /// Path to private key file (PEM format)
    pub key: String,
}

/// Metrics endpoint configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Address for metrics endpoint
    #[serde(default = "default_metrics_address")]
    pub address: SocketAddr,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            address: default_metrics_address(),
        }
    }
}

/// Recv mode for io_uring connections.
///
/// Accepts: "multishot", "multi-shot", "singleshot", "single-shot"
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RecvMode {
    /// Multishot recv with kernel-managed buffer ring (default).
    /// 1 copy for long messages, 0 copies for short messages.
    /// More efficient for many small receives.
    #[default]
    #[serde(alias = "multi-shot")]
    Multishot,
    /// Single-shot recv directly into connection buffer.
    /// 0 copies (kernel writes directly to user buffer).
    /// May be more efficient for larger messages.
    #[serde(alias = "singleshot", alias = "single-shot")]
    SingleShot,
}

/// io_uring specific configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UringConfig {
    /// Enable SQPOLL mode (requires CAP_SYS_NICE or root)
    #[serde(default)]
    pub sqpoll: bool,

    /// SQPOLL idle timeout in milliseconds
    #[serde(default = "default_sqpoll_idle_ms")]
    pub sqpoll_idle_ms: u32,

    /// Number of buffers in the provided buffer pool per worker
    #[serde(default = "default_buffer_count")]
    pub buffer_count: u16,

    /// Size of each buffer in the pool
    #[serde(default = "default_buffer_size", deserialize_with = "deserialize_size")]
    pub buffer_size: usize,

    /// Submission queue depth
    #[serde(default = "default_sq_depth")]
    pub sq_depth: u32,

    /// Recv mode: "multishot"/"multi-shot" (default) or "singleshot"/"single-shot"
    #[serde(default)]
    pub recv_mode: RecvMode,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            sqpoll: false,
            sqpoll_idle_ms: default_sqpoll_idle_ms(),
            buffer_count: default_buffer_count(),
            buffer_size: default_buffer_size(),
            sq_depth: default_sq_depth(),
            recv_mode: RecvMode::default(),
        }
    }
}

// Default value functions

fn default_runtime() -> Runtime {
    Runtime::Native
}

fn default_cache_backend() -> CacheBackend {
    CacheBackend::Segcache
}

fn default_heap_size() -> usize {
    1024 * 1024 * 1024 // 1GB
}

fn default_segment_size() -> usize {
    8 * 1024 * 1024 // 8MB
}

fn default_max_value_size() -> usize {
    1024 * 1024 // 1MB
}

fn default_hashtable_power() -> u8 {
    26
}

fn default_metrics_address() -> SocketAddr {
    "0.0.0.0:9090".parse().unwrap()
}

fn default_sqpoll_idle_ms() -> u32 {
    1000
}

fn default_buffer_count() -> u16 {
    2048 // Enough for 1024 connections (2 buffers each)
}

fn default_buffer_size() -> usize {
    16 * 1024 // 16KB (TLS max record size)
}

fn default_sq_depth() -> u32 {
    1024
}

/// Deserialize a size string like "64MB" or "4GB" into bytes.
fn deserialize_size<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum SizeValue {
        Number(usize),
        String(String),
    }

    match SizeValue::deserialize(deserializer)? {
        SizeValue::Number(n) => Ok(n),
        SizeValue::String(s) => parse_size(&s).map_err(D::Error::custom),
    }
}

/// Parse a size string like "64MB", "4GB", "1TB" into bytes.
pub fn parse_size(s: &str) -> Result<usize, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty size string".to_string());
    }

    let (num_str, suffix) = match s.find(|c: char| c.is_alphabetic()) {
        Some(idx) => (&s[..idx], s[idx..].to_uppercase()),
        None => (s, String::new()),
    };

    let num: usize = num_str
        .trim()
        .parse()
        .map_err(|_| format!("invalid number: {}", num_str))?;

    let multiplier: usize = match suffix.as_str() {
        "" => 1,
        "B" => 1,
        "K" | "KB" | "KIB" => 1024,
        "M" | "MB" | "MIB" => 1024 * 1024,
        "G" | "GB" | "GIB" => 1024 * 1024 * 1024,
        "T" | "TB" | "TIB" => 1024 * 1024 * 1024 * 1024,
        _ => return Err(format!("unknown size suffix: {}", suffix)),
    };

    num.checked_mul(multiplier)
        .ok_or_else(|| "size overflow".to_string())
}

/// Parse a Linux-style CPU list string into a vector of CPU IDs.
pub fn parse_cpu_list(cpu_list: &str) -> Result<Vec<usize>, String> {
    let mut cpus = Vec::new();

    for part in cpu_list.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some((range_part, stride_str)) = part.split_once(':') {
            let stride: usize = stride_str
                .trim()
                .parse()
                .map_err(|_| format!("invalid stride: {}", stride_str))?;

            if stride == 0 {
                return Err("stride cannot be zero".to_string());
            }

            if let Some((start_str, end_str)) = range_part.split_once('-') {
                let start: usize = start_str
                    .trim()
                    .parse()
                    .map_err(|_| format!("invalid start of range: {}", start_str))?;
                let end: usize = end_str
                    .trim()
                    .parse()
                    .map_err(|_| format!("invalid end of range: {}", end_str))?;

                if start > end {
                    return Err(format!("invalid range: start ({}) > end ({})", start, end));
                }

                let mut cpu = start;
                while cpu <= end {
                    cpus.push(cpu);
                    cpu += stride;
                }
            } else {
                return Err(format!(
                    "stride requires a range (e.g., 0-15:2), got: {}",
                    part
                ));
            }
        } else if let Some((start_str, end_str)) = part.split_once('-') {
            let start: usize = start_str
                .trim()
                .parse()
                .map_err(|_| format!("invalid start of range: {}", start_str))?;
            let end: usize = end_str
                .trim()
                .parse()
                .map_err(|_| format!("invalid end of range: {}", end_str))?;

            if start > end {
                return Err(format!("invalid range: start ({}) > end ({})", start, end));
            }

            for cpu in start..=end {
                cpus.push(cpu);
            }
        } else {
            let cpu: usize = part
                .parse()
                .map_err(|_| format!("invalid CPU number: {}", part))?;
            cpus.push(cpu);
        }
    }

    if cpus.is_empty() {
        return Err("CPU list cannot be empty".to_string());
    }

    cpus.sort_unstable();
    cpus.dedup();

    Ok(cpus)
}

impl Config {
    /// Load configuration from a TOML file.
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.cache.heap_size < self.cache.segment_size {
            return Err(format!(
                "heap_size ({}) must be at least segment_size ({})",
                self.cache.heap_size, self.cache.segment_size
            )
            .into());
        }

        if self.cache.max_value_size >= self.cache.segment_size {
            return Err(format!(
                "max_value_size ({}) must be less than segment_size ({})",
                self.cache.max_value_size, self.cache.segment_size
            )
            .into());
        }

        if self.cache.hashtable_power > 30 {
            return Err("hashtable_power must be <= 30".into());
        }

        if let Some(ref affinity) = self.workers.cpu_affinity {
            parse_cpu_list(affinity).map_err(|e| format!("invalid cpu_affinity: {}", e))?;
        }

        if self.listener.is_empty() {
            return Err("at least one listener must be configured".into());
        }

        Ok(())
    }

    /// Get the number of worker threads.
    pub fn threads(&self) -> usize {
        if let Some(threads) = self.workers.threads {
            return threads;
        }
        if let Some(ref affinity) = self.workers.cpu_affinity
            && let Ok(cpus) = parse_cpu_list(affinity)
        {
            return cpus.len();
        }
        num_cpus::get()
    }

    /// Get the parsed CPU affinity list.
    pub fn cpu_affinity(&self) -> Option<Vec<usize>> {
        self.workers
            .cpu_affinity
            .as_ref()
            .and_then(|s| parse_cpu_list(s).ok())
    }

    /// Get the NUMA node for cache allocation.
    ///
    /// Returns the explicitly configured numa_node, or auto-detects from
    /// cpu_affinity if all CPUs are on the same NUMA node.
    pub fn numa_node(&self) -> Option<u32> {
        // Explicit configuration takes precedence
        if self.cache.numa_node.is_some() {
            return self.cache.numa_node;
        }

        // Try to auto-detect from CPU affinity
        #[cfg(target_os = "linux")]
        if let Some(cpus) = self.cpu_affinity() {
            return detect_numa_node_from_cpus(&cpus);
        }

        None
    }
}

/// Detect which NUMA node a set of CPUs belongs to.
///
/// Returns `Some(node)` if all CPUs are on the same NUMA node,
/// or `None` if CPUs span multiple nodes or detection fails.
#[cfg(target_os = "linux")]
fn detect_numa_node_from_cpus(cpus: &[usize]) -> Option<u32> {
    use std::collections::HashSet;
    use std::fs;

    if cpus.is_empty() {
        return None;
    }

    let cpu_set: HashSet<usize> = cpus.iter().copied().collect();

    // Read /sys/devices/system/node/ to find NUMA nodes
    let node_dir = std::path::Path::new("/sys/devices/system/node");
    if !node_dir.exists() {
        return None;
    }

    let entries = match fs::read_dir(node_dir) {
        Ok(e) => e,
        Err(_) => return None,
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Look for directories like "node0", "node1", etc.
        if !name_str.starts_with("node") {
            continue;
        }

        let node_id: u32 = match name_str[4..].parse() {
            Ok(id) => id,
            Err(_) => continue,
        };

        // Read the cpulist for this node
        let cpulist_path = entry.path().join("cpulist");
        let cpulist = match fs::read_to_string(&cpulist_path) {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Parse the CPU list for this node
        let node_cpus = match parse_cpu_list(cpulist.trim()) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let node_cpu_set: HashSet<usize> = node_cpus.into_iter().collect();

        // Check if all requested CPUs are on this node
        if cpu_set.is_subset(&node_cpu_set) {
            return Some(node_id);
        }
    }

    None
}

/// Format a size in bytes as a human-readable string.
pub fn format_size(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;
    const GB: usize = 1024 * MB;

    if bytes >= GB && bytes.is_multiple_of(GB) {
        format!("{} GB", bytes / GB)
    } else if bytes >= MB && bytes.is_multiple_of(MB) {
        format!("{} MB", bytes / MB)
    } else if bytes >= KB && bytes.is_multiple_of(KB) {
        format!("{} KB", bytes / KB)
    } else {
        format!("{} bytes", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("64MB").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_size("4GB").unwrap(), 4 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_cpu_list() {
        assert_eq!(parse_cpu_list("0-7").unwrap(), vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(parse_cpu_list("1,3,8").unwrap(), vec![1, 3, 8]);
        assert_eq!(parse_cpu_list("0-7:2").unwrap(), vec![0, 2, 4, 6]);
    }
}
