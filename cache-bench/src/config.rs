//! Configuration for the in-process cache benchmark.

use serde::Deserialize;
use std::time::Duration;

/// Top-level configuration.
#[derive(Deserialize)]
pub struct Config {
    pub general: GeneralConfig,
    pub cache: CacheConfig,
    pub workload: WorkloadConfig,
}

/// General benchmark settings.
#[derive(Deserialize)]
pub struct GeneralConfig {
    /// How long to run the measurement phase.
    #[serde(deserialize_with = "deserialize_duration")]
    pub duration: Duration,
    /// How long to warm up before recording metrics.
    #[serde(deserialize_with = "deserialize_duration")]
    pub warmup: Duration,
    /// Number of worker threads.
    pub threads: usize,
    /// Optional list of CPU cores to pin workers to (e.g., "0-3,6-8").
    pub cpu_list: Option<String>,
}

/// Cache backend configuration.
#[derive(Deserialize)]
pub struct CacheConfig {
    /// Storage backend.
    pub backend: CacheBackend,
    /// Eviction policy (depends on backend).
    pub policy: EvictionPolicy,
    /// Total heap size (e.g., "1GB", "512MB").
    #[serde(deserialize_with = "deserialize_size")]
    pub heap_size: usize,
    /// Segment/slab size (e.g., "1MB").
    #[serde(deserialize_with = "deserialize_size")]
    pub segment_size: usize,
    /// Hashtable power (2^power buckets).
    pub hashtable_power: u8,
    /// Optional disk tier configuration.
    #[serde(default)]
    pub disk: Option<DiskConfig>,
}

/// Workload configuration.
#[derive(Deserialize)]
pub struct WorkloadConfig {
    /// Rate limit in ops/sec. 0 = unlimited.
    #[serde(default)]
    pub rate_limit: u64,
    /// Whether to prefill the cache before measurement.
    #[serde(default)]
    pub prefill: bool,
    /// Keyspace parameters.
    pub keyspace: KeyspaceConfig,
    /// Command mix (must sum to 100).
    pub commands: CommandsConfig,
    /// Value parameters.
    pub values: ValuesConfig,
}

/// Keyspace configuration.
#[derive(Deserialize)]
pub struct KeyspaceConfig {
    /// Key length in bytes.
    pub length: usize,
    /// Number of distinct keys.
    pub count: usize,
}

/// Command mix (percentages, must sum to 100).
#[derive(Deserialize)]
pub struct CommandsConfig {
    pub get: u8,
    pub set: u8,
    pub delete: u8,
}

/// Value configuration.
#[derive(Deserialize)]
pub struct ValuesConfig {
    /// Value length in bytes.
    pub length: usize,
}

/// Cache backend type.
#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CacheBackend {
    Segment,
    Slab,
    Heap,
}

impl std::fmt::Display for CacheBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheBackend::Segment => write!(f, "segment"),
            CacheBackend::Slab => write!(f, "slab"),
            CacheBackend::Heap => write!(f, "heap"),
        }
    }
}

/// Eviction policy.
#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EvictionPolicy {
    S3Fifo,
    Fifo,
    Random,
    Cte,
    Merge,
    Lra,
    Lrc,
    Lfu,
    None,
}

#[derive(Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "lowercase")]
pub enum DiskSyncMode {
    Sync,
    #[default]
    Async,
    None,
}

#[derive(Deserialize)]
pub struct DiskConfig {
    #[serde(default)]
    pub enabled: bool,
    pub path: std::path::PathBuf,
    #[serde(deserialize_with = "deserialize_size")]
    pub size: usize,
    #[serde(default = "default_promotion_threshold")]
    pub promotion_threshold: u8,
    #[serde(default)]
    pub sync_mode: DiskSyncMode,
    #[serde(default)]
    pub recover_on_startup: bool,
}

fn default_promotion_threshold() -> u8 {
    2
}

impl std::fmt::Display for EvictionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionPolicy::S3Fifo => write!(f, "s3fifo"),
            EvictionPolicy::Fifo => write!(f, "fifo"),
            EvictionPolicy::Random => write!(f, "random"),
            EvictionPolicy::Cte => write!(f, "cte"),
            EvictionPolicy::Merge => write!(f, "merge"),
            EvictionPolicy::Lra => write!(f, "lra"),
            EvictionPolicy::Lrc => write!(f, "lrc"),
            EvictionPolicy::Lfu => write!(f, "lfu"),
            EvictionPolicy::None => write!(f, "none"),
        }
    }
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;

        // Validate command mix
        let total = config.workload.commands.get as u16
            + config.workload.commands.set as u16
            + config.workload.commands.delete as u16;
        if total != 100 {
            return Err(format!(
                "command mix must sum to 100 (got {}): get={} set={} delete={}",
                total,
                config.workload.commands.get,
                config.workload.commands.set,
                config.workload.commands.delete,
            )
            .into());
        }

        Ok(config)
    }
}

/// Deserialize a duration from a human-readable string (e.g., "60s", "5m").
fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    humantime::parse_duration(&s).map_err(serde::de::Error::custom)
}

/// Deserialize a size from a human-readable string (e.g., "1GB", "512MB", "1MB").
fn deserialize_size<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_size(&s).map_err(serde::de::Error::custom)
}

/// Parse a size string like "1GB", "512MB", "4KB" into bytes.
fn parse_size(s: &str) -> Result<usize, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty size string".to_string());
    }

    // Find where the numeric part ends
    let num_end = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());

    let (num_str, suffix) = s.split_at(num_end);
    let num: f64 = num_str
        .parse()
        .map_err(|e| format!("invalid number '{}': {}", num_str, e))?;

    let multiplier: usize = match suffix.trim().to_uppercase().as_str() {
        "" | "B" => 1,
        "KB" | "K" => 1024,
        "MB" | "M" => 1024 * 1024,
        "GB" | "G" => 1024 * 1024 * 1024,
        "TB" | "T" => 1024 * 1024 * 1024 * 1024,
        other => return Err(format!("unknown size suffix '{}'", other)),
    };

    Ok((num * multiplier as f64) as usize)
}

/// Parse a CPU list string like "0-3,6-8" into a Vec of CPU IDs.
pub fn parse_cpu_list(s: &str) -> Result<Vec<usize>, String> {
    let mut cpus = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if let Some((start, end)) = part.split_once('-') {
            let start: usize = start
                .trim()
                .parse()
                .map_err(|e| format!("invalid CPU id '{}': {}", start, e))?;
            let end: usize = end
                .trim()
                .parse()
                .map_err(|e| format!("invalid CPU id '{}': {}", end, e))?;
            if start > end {
                return Err(format!("invalid range {}-{}", start, end));
            }
            cpus.extend(start..=end);
        } else {
            let cpu: usize = part
                .parse()
                .map_err(|e| format!("invalid CPU id '{}': {}", part, e))?;
            cpus.push(cpu);
        }
    }
    Ok(cpus)
}
