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
    /// Pin the hashtable hash seed for a reproducible run.
    ///
    /// Unset, the table seeds from the OS and key placement differs run to
    /// run. Pin it to repeat a run; vary it across runs to size the noise
    /// floor that any policy difference has to clear.
    #[serde(default)]
    pub hashtable_seed: Option<[u64; 4]>,
    /// Pin the eviction PRNG seed for the segment backend.
    ///
    /// Which bucket `randomfifo` picks, and which segment `random` picks.
    /// Unset, the layers use a fixed built-in default, so a run is
    /// reproducible either way -- this is the knob for sweeping seeds, which
    /// is how a policy difference is told apart from the luck of one random
    /// sequence.
    #[serde(default)]
    pub eviction_seed: Option<u64>,
    /// How the S3-FIFO main cache (layer 1) reclaims segments.
    ///
    /// Only meaningful for `backend = "segment"` with `policy = "s3fifo"`.
    /// Unset leaves the built-in default, adaptive merge.
    #[serde(default)]
    pub main_policy: Option<MainPolicy>,
    /// Segments merge folds into one per eviction pass (layer 1).
    ///
    /// The lever the first results identified: the merge-vs-CLOCK gap was
    /// entirely chain length, not the prune threshold. Unset leaves
    /// `MergeConfig::default()`, which is 4.
    #[serde(default)]
    pub main_merge_segments: Option<usize>,
    /// Retention cap for merge, as a fraction of the chain's live bytes.
    ///
    /// Since #154 the spare's free space bounds a pass structurally, so this
    /// is a policy cap layered on top: `budget = min(spare, live * ratio)`.
    /// At 1.0 a pass keeps everything that fits, which is compaction; below
    /// that it discards live items there was room for. Unset leaves
    /// `MergeConfig::default()`, currently 0.5 -- see the reasoning on that
    /// default, which is a latency choice rather than a correctness one.
    #[serde(default)]
    pub main_target_ratio: Option<f64>,
    /// What an overwrite does with the superseded copy's bytes.
    ///
    /// `set`, `replace`, `cas` and a committed streaming set all supersede
    /// an existing item; all four leave its bytes for the next merge, while
    /// `delete` reclaims eagerly. On an 80%-SET trace that asymmetry showed
    /// as 1673 bytes per resident item against a comparable engine's 909,
    /// widening with heap size. Unset leaves the historical behaviour.
    #[serde(default)]
    pub overwrite_reclaim: Option<OverwriteReclaimConfig>,
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
    /// Keyspace parameters. Unused when replaying a trace.
    #[serde(default)]
    pub keyspace: KeyspaceConfig,
    /// Command mix (must sum to 100). Unused when replaying a trace.
    #[serde(default)]
    pub commands: CommandsConfig,
    /// Value parameters.
    #[serde(default)]
    pub values: ValuesConfig,
    /// Replay a trace instead of generating a synthetic workload.
    #[serde(default)]
    pub trace: Option<TraceConfig>,
}

/// Trace replay settings.
#[derive(Deserialize)]
pub struct TraceConfig {
    /// Path to the trace, optionally `.zst`.
    pub path: std::path::PathBuf,
    /// On-disk layout.
    pub format: TraceFormatConfig,
    /// Records to apply before the measured window opens.
    #[serde(default)]
    pub warmup_records: u64,
    /// Cap on measured records; unset replays to end of trace.
    #[serde(default)]
    pub max_records: Option<u64>,
    /// Records per reported interval, for the steady-state check.
    #[serde(default = "default_report_interval_records")]
    pub report_interval_records: u64,
    /// Largest value the replay will store.
    #[serde(
        default = "default_max_value_bytes",
        deserialize_with = "deserialize_size"
    )]
    pub max_value_bytes: usize,
    /// Report which value sizes survived, by probing every key written.
    ///
    /// Off by default: it holds a key-to-size map for the whole run and
    /// probes once per distinct key afterwards. Answers whether two engines
    /// holding different item counts are keeping different size
    /// distributions, which a total count cannot show.
    #[serde(default)]
    pub retained_sizes: bool,
}

/// On-disk trace layout.
#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TraceFormatConfig {
    /// Twitter cluster binary: op codes and TTLs.
    Twitter,
    /// libCacheSim oracleGeneral binary: GET-only.
    OracleGeneral,
    /// libCacheSim oracleGeneral CSV.
    OracleGeneralCsv,
}

impl TraceFormatConfig {
    /// Whether a GET miss should synthesize an insert.
    ///
    /// True only for the oracleGeneral layouts, which carry no writes at all.
    /// Synthesizing inserts for a Twitter trace would replace its real write
    /// mix with one the harness invented.
    pub fn insert_on_miss(&self) -> bool {
        !matches!(self, TraceFormatConfig::Twitter)
    }
}

impl From<TraceFormatConfig> for crate::trace::TraceFormat {
    fn from(f: TraceFormatConfig) -> Self {
        match f {
            TraceFormatConfig::Twitter => Self::Twitter,
            TraceFormatConfig::OracleGeneral => Self::OracleGeneral,
            TraceFormatConfig::OracleGeneralCsv => Self::OracleGeneralCsv,
        }
    }
}

fn default_report_interval_records() -> u64 {
    1_000_000
}

fn default_max_value_bytes() -> usize {
    1024 * 1024
}

/// Keyspace configuration.
#[derive(Deserialize, Default)]
pub struct KeyspaceConfig {
    /// Key length in bytes.
    pub length: usize,
    /// Number of distinct keys.
    pub count: usize,
}

/// Command mix (percentages, must sum to 100).
#[derive(Deserialize, Default)]
pub struct CommandsConfig {
    pub get: u8,
    pub set: u8,
    pub delete: u8,
}

/// Value configuration.
#[derive(Deserialize, Default)]
pub struct ValuesConfig {
    /// Value length in bytes.
    pub length: usize,
}

/// Cache backend type.
#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum CacheBackend {
    Segment,
    Slab,
    Heap,
    /// pelikan-io/cache-rs, for engine-vs-engine ranking.
    #[serde(rename = "cachers")]
    CacheRs,
}

impl std::fmt::Display for CacheBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheBackend::Segment => write!(f, "segment"),
            CacheBackend::Slab => write!(f, "slab"),
            CacheBackend::Heap => write!(f, "heap"),
            CacheBackend::CacheRs => write!(f, "cachers"),
        }
    }
}

/// What an overwrite does with the superseded copy's bytes.
///
/// `delete` has always reclaimed eagerly; the four overwrite paths never
/// did. See the field on `CacheConfig` for what that costs.
#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum OverwriteReclaimConfig {
    /// Leave the superseded bytes for the next merge pass.
    Deferred,
    /// Free the segment if the overwrite emptied it.
    FreeEmpty,
    /// Also attempt compaction with the predecessor, as `delete` does.
    Compact,
}

impl From<OverwriteReclaimConfig> for cache_core::OverwriteReclaim {
    fn from(value: OverwriteReclaimConfig) -> Self {
        match value {
            OverwriteReclaimConfig::Deferred => cache_core::OverwriteReclaim::Deferred,
            OverwriteReclaimConfig::FreeEmpty => cache_core::OverwriteReclaim::FreeEmpty,
            OverwriteReclaimConfig::Compact => cache_core::OverwriteReclaim::Compact,
        }
    }
}

/// How the S3-FIFO main cache reclaims segments.
///
/// Both run the same machinery; see the merge-vs-CLOCK design spec.
#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum MainPolicy {
    /// Adaptive merge: frequency histogram over a segment chain.
    Merge,
    /// CLOCK second chance: one segment, threshold pinned above the baseline.
    Clock,
    /// Merge's adaptive threshold over a one-segment chain.
    ///
    /// Not a policy anyone would ship. It exists to decompose the merge-vs-
    /// CLOCK result: those two differ in both the prune threshold and the
    /// chain length, so a difference between them cannot be attributed to
    /// either on its own. This arm holds the chain at one segment and varies
    /// only the threshold.
    MergeSingle,
}

impl From<MainPolicy> for cache_core::EvictionStrategy {
    fn from(p: MainPolicy) -> Self {
        match p {
            MainPolicy::Merge => Self::Merge(cache_core::MergeConfig::default()),
            MainPolicy::Clock => Self::Clock,
            MainPolicy::MergeSingle => Self::Merge(cache_core::MergeConfig {
                min_segments: 1,
                ..cache_core::MergeConfig::default()
            }),
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
    RandomFifo,
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

impl From<DiskSyncMode> for cache_core::SyncMode {
    fn from(mode: DiskSyncMode) -> Self {
        match mode {
            DiskSyncMode::Sync => Self::Sync,
            DiskSyncMode::Async => Self::Async,
            DiskSyncMode::None => Self::None,
        }
    }
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
            EvictionPolicy::RandomFifo => write!(f, "randomfifo"),
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
        Self::from_toml(&contents)
    }

    /// Parse and validate a config, separated from file IO so the validation
    /// rules can be tested without a fixture file on disk.
    pub fn from_toml(contents: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config: Config = toml::from_str(contents)?;
        // The command mix drives the synthetic generator only. A trace replay
        // takes its op mix from the trace, so demanding one here would reject
        // every replay config over a field it never reads.
        if config.workload.trace.is_none() {
            Self::validate_command_mix(&config)?;
        }
        Self::validate_main_cache(&config)?;
        Ok(config)
    }

    /// Reject a merge chain length that cannot mean what it says.
    fn validate_main_cache(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(n) = config.cache.main_merge_segments {
            if n == 0 {
                // `select_merge_candidates(_, 0, _)` returns nothing, merge
                // falls back to whole-segment eviction, and the run reports
                // itself as a merge arm while measuring something else.
                return Err("main_merge_segments must be at least 1".into());
            }
            if config.cache.main_policy == Some(MainPolicy::Clock) {
                return Err("main_merge_segments does not apply to \
                            main_policy = \"clock\", which pins its chain at \
                            one segment by definition"
                    .into());
            }
        }
        if let Some(r) = config.cache.main_target_ratio
            && !(r > 0.0 && r <= 1.0)
        {
            // Zero would retain nothing and make every merge pass a
            // whole-segment eviction wearing merge's name; above one is not
            // a fraction of anything. `MergeConfig` clamps silently, which
            // is the wrong behaviour for a measurement rig.
            return Err(format!("main_target_ratio must be in (0.0, 1.0], got {r}").into());
        }
        Ok(())
    }

    fn validate_command_mix(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
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

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    const TRACE_TOML: &str = r#"
[general]
duration = "60s"
warmup = "10s"
threads = 1

[cache]
backend = "segment"
policy = "s3fifo"
heap_size = "64MB"
segment_size = "1MB"
hashtable_power = 20
hashtable_seed = [1, 2, 3, 4]

[workload.trace]
path = "/tmp/t.bin"
format = "twitter"
warmup_records = 1000
"#;

    #[test]
    fn a_zero_length_merge_chain_is_rejected() {
        let toml = TRACE_TOML.replace(
            "hashtable_power = 20",
            "hashtable_power = 20\nmain_merge_segments = 0",
        );

        let err = match Config::from_toml(&toml) {
            Ok(_) => panic!("a zero-segment chain silently disables merge"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("at least 1"), "{err}");
    }

    #[test]
    fn an_overwrite_reclaim_policy_parses() {
        for (text, expected) in [
            ("deferred", OverwriteReclaimConfig::Deferred),
            ("free-empty", OverwriteReclaimConfig::FreeEmpty),
            ("compact", OverwriteReclaimConfig::Compact),
        ] {
            let toml = TRACE_TOML.replace(
                "hashtable_power = 20",
                &format!("hashtable_power = 20\noverwrite_reclaim = \"{text}\""),
            );
            let cfg =
                Config::from_toml(&toml).unwrap_or_else(|e| panic!("{text} should parse: {e}"));
            assert_eq!(cfg.cache.overwrite_reclaim, Some(expected), "{text}");
        }
    }

    #[test]
    fn an_unknown_overwrite_reclaim_policy_is_rejected() {
        // A typo must not fall back to the historical behaviour while the
        // run reports itself as having used the policy that was asked for.
        let toml = TRACE_TOML.replace(
            "hashtable_power = 20",
            "hashtable_power = 20\noverwrite_reclaim = \"eager\"",
        );
        assert!(
            Config::from_toml(&toml).is_err(),
            "an unknown reclaim policy should be rejected, not defaulted"
        );
    }

    #[test]
    fn overwrite_reclaim_defaults_to_the_historical_behaviour() {
        let cfg = Config::from_toml(TRACE_TOML).expect("parse");
        assert_eq!(cfg.cache.overwrite_reclaim, None);
    }

    #[test]
    fn a_merge_chain_length_is_rejected_for_the_clock_policy() {
        let toml = TRACE_TOML.replace(
            "hashtable_power = 20",
            "hashtable_power = 20\nmain_policy = \"clock\"\nmain_merge_segments = 4",
        );

        let err = match Config::from_toml(&toml) {
            Ok(_) => panic!("clock pins its chain at one segment; 4 is meaningless"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("clock"), "{err}");
    }

    #[test]
    fn a_merge_chain_length_is_accepted_for_the_default_main_policy() {
        let toml = TRACE_TOML.replace(
            "hashtable_power = 20",
            "hashtable_power = 20\nmain_merge_segments = 8",
        );

        let config = match Config::from_toml(&toml) {
            Ok(c) => c,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(config.cache.main_merge_segments, Some(8));
    }

    #[test]
    fn a_trace_config_does_not_require_a_synthetic_command_mix() {
        let config = match Config::from_toml(TRACE_TOML) {
            Ok(c) => c,
            Err(e) => panic!("a trace config needs no command mix: {e}"),
        };

        let trace = config.workload.trace.expect("trace section");
        assert_eq!(trace.warmup_records, 1000);
        assert_eq!(config.cache.hashtable_seed, Some([1, 2, 3, 4]));
    }

    /// A seed that parsed but never reached the cache would make a sweep
    /// over seeds report a spread of zero.
    #[test]
    fn an_eviction_seed_is_read_from_the_config() {
        let toml = TRACE_TOML.replace(
            "hashtable_power = 20",
            "hashtable_power = 20\neviction_seed = 12345",
        );
        let config = match Config::from_toml(&toml) {
            Ok(c) => c,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(config.cache.eviction_seed, Some(12345));
    }

    /// Unset means "use the built-in fixed default", not "seed from the OS".
    #[test]
    fn an_absent_eviction_seed_stays_absent() {
        let config = Config::from_toml(TRACE_TOML).expect("parse");
        assert_eq!(config.cache.eviction_seed, None);
    }

    /// `randomfifo` is the honest name for what `fifo`, `random` and `cte`
    /// all used to do (#156), so the config surface has to accept it.
    #[test]
    fn randomfifo_is_a_policy_the_config_accepts() {
        let toml = TRACE_TOML.replace("policy = \"s3fifo\"", "policy = \"randomfifo\"");
        let config = match Config::from_toml(&toml) {
            Ok(c) => c,
            Err(e) => panic!("{e}"),
        };
        assert!(config.cache.policy == EvictionPolicy::RandomFifo);
        assert_eq!(config.cache.policy.to_string(), "randomfifo");
    }

    #[test]
    fn a_synthetic_config_still_rejects_a_command_mix_that_does_not_sum_to_a_hundred() {
        let toml = TRACE_TOML.replace(
            "[workload.trace]\npath = \"/tmp/t.bin\"\nformat = \"twitter\"\nwarmup_records = 1000\n",
            "[workload.keyspace]\nlength = 16\ncount = 1000\n\n[workload.values]\nlength = 64\n\n[workload.commands]\nget = 50\nset = 10\ndelete = 0\n",
        );

        let err = match Config::from_toml(&toml) {
            Ok(_) => panic!("60 is not 100, but validation accepted it"),
            Err(e) => e,
        };

        assert!(err.to_string().contains("must sum to 100"), "{err}");
    }

    #[test]
    fn a_twitter_trace_does_not_synthesize_inserts_but_oracle_general_does() {
        assert!(!TraceFormatConfig::Twitter.insert_on_miss());
        assert!(TraceFormatConfig::OracleGeneral.insert_on_miss());
        assert!(TraceFormatConfig::OracleGeneralCsv.insert_on_miss());
    }

    #[test]
    fn a_cache_rs_backend_parses() {
        let toml = TRACE_TOML.replace("backend = \"segment\"", "backend = \"cachers\"");
        let config = match Config::from_toml(&toml) {
            Ok(c) => c,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(config.cache.backend, CacheBackend::CacheRs);
    }

    #[test]
    fn a_merge_retention_ratio_parses() {
        let toml = TRACE_TOML.replace(
            "hashtable_power = 20",
            "hashtable_power = 20\nmain_target_ratio = 1.0",
        );
        let config = match Config::from_toml(&toml) {
            Ok(c) => c,
            Err(e) => panic!("{e}"),
        };
        assert_eq!(config.cache.main_target_ratio, Some(1.0));
    }

    #[test]
    fn a_retention_ratio_outside_the_unit_interval_is_rejected() {
        // `MergeConfig::with_target_ratio` clamps silently. For a
        // measurement rig that is the wrong behaviour: an arm asking for 1.5
        // would quietly run at 1.0 and be reported under the label it asked
        // for, so the sweep would contain two identical points wearing
        // different names.
        for bad in ["0.0", "1.5", "-0.2"] {
            let toml = TRACE_TOML.replace(
                "hashtable_power = 20",
                &format!("hashtable_power = 20\nmain_target_ratio = {bad}"),
            );
            let err = match Config::from_toml(&toml) {
                Ok(_) => panic!("main_target_ratio = {bad} was accepted"),
                Err(e) => e,
            };
            assert!(
                err.to_string().contains("main_target_ratio"),
                "the rejection must name the knob: {err}"
            );
        }
    }
}
