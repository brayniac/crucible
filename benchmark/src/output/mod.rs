//! Output formatting for benchmark results.
//!
//! Provides multiple output formats:
//! - Clean: Human-readable table format with colors
//! - Json: NDJSON format for machine parsing
//! - Verbose: Tracing-style output with timestamps
//! - Quiet: Minimal single-line output

mod clean;
pub mod format;
mod json;
mod quiet;
mod verbose;

pub use clean::CleanFormatter;
pub use json::JsonFormatter;
pub use quiet::QuietFormatter;
pub use verbose::VerboseFormatter;

use crate::config::Config;
use chrono::{DateTime, Utc};
use std::time::Duration;

/// Output format selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OutputFormat {
    /// Clean table format with colors (default).
    #[default]
    Clean,
    /// NDJSON format for machine parsing.
    Json,
    /// Verbose tracing-style output.
    Verbose,
    /// Minimal single-line output.
    Quiet,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "clean" => Ok(OutputFormat::Clean),
            "json" => Ok(OutputFormat::Json),
            "verbose" => Ok(OutputFormat::Verbose),
            "quiet" => Ok(OutputFormat::Quiet),
            _ => Err(format!(
                "invalid format '{}', expected: clean, json, verbose, quiet",
                s
            )),
        }
    }
}

/// Color mode selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ColorMode {
    /// Auto-detect based on TTY and NO_COLOR env var (default).
    #[default]
    Auto,
    /// Always use colors.
    Always,
    /// Never use colors.
    Never,
}

impl std::str::FromStr for ColorMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(ColorMode::Auto),
            "always" => Ok(ColorMode::Always),
            "never" => Ok(ColorMode::Never),
            _ => Err(format!(
                "invalid color mode '{}', expected: auto, always, never",
                s
            )),
        }
    }
}

/// A periodic sample of benchmark metrics.
#[derive(Debug, Clone)]
pub struct Sample {
    pub timestamp: DateTime<Utc>,
    pub req_per_sec: f64,
    pub err_per_sec: f64,
    pub hit_pct: f64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub p9999_us: f64,
    pub max_us: f64,
}

/// Latency statistics for a specific operation type.
#[derive(Debug, Clone, Default)]
pub struct LatencyStats {
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub p9999_us: f64,
    pub max_us: f64,
}

/// Final benchmark results.
#[derive(Debug, Clone)]
pub struct Results {
    pub duration_secs: f64,
    pub requests: u64,
    pub responses: u64,
    pub errors: u64,
    pub hits: u64,
    pub misses: u64,
    pub bytes_tx: u64,
    pub bytes_rx: u64,
    pub get_count: u64,
    pub set_count: u64,
    pub get_latencies: LatencyStats,
    pub set_latencies: LatencyStats,
    pub conns_active: u64,
    pub conns_failed: u64,
    pub conns_total: u64,
}

/// A single step in saturation search.
#[derive(Debug, Clone)]
pub struct SaturationStep {
    /// Target request rate for this step.
    pub target_rate: u64,
    /// Actual achieved request rate.
    pub achieved_rate: f64,
    /// p50 latency in microseconds.
    pub p50_us: f64,
    /// p99 latency in microseconds.
    pub p99_us: f64,
    /// p99.9 latency in microseconds.
    pub p999_us: f64,
    /// Whether SLO was met for this step.
    pub slo_passed: bool,
}

/// Results from saturation search.
#[derive(Debug, Clone)]
pub struct SaturationResults {
    /// Maximum rate that met SLO, if any.
    pub max_compliant_rate: Option<u64>,
    /// All steps taken during the search.
    pub steps: Vec<SaturationStep>,
}

/// Trait for output formatters.
pub trait OutputFormatter: Send + Sync {
    /// Print the configuration summary at startup.
    fn print_config(&self, config: &Config);

    /// Print the prefill phase indicator.
    fn print_prefill(&self, key_count: usize) {
        println!("PREFILL: writing {} keys...", key_count);
    }

    /// Print the warmup phase indicator.
    fn print_warmup(&self, duration: Duration);

    /// Print the running phase indicator.
    fn print_running(&self, duration: Duration);

    /// Print the table header (for formats that use one).
    fn print_header(&self);

    /// Print a periodic sample.
    fn print_sample(&self, sample: &Sample);

    /// Print the final results.
    fn print_results(&self, results: &Results);

    /// Print the saturation search header (for formats that use one).
    fn print_saturation_header(&self) {}

    /// Print a saturation search step result.
    fn print_saturation_step(&self, _step: &SaturationStep) {}

    /// Print the final saturation search results.
    fn print_saturation_results(&self, _results: &SaturationResults) {}
}

/// Create a formatter based on the output format and color mode.
pub fn create_formatter(format: OutputFormat, color: ColorMode) -> Box<dyn OutputFormatter> {
    match format {
        OutputFormat::Clean => Box::new(CleanFormatter::new(color)),
        OutputFormat::Json => Box::new(JsonFormatter::new()),
        OutputFormat::Verbose => Box::new(VerboseFormatter::new()),
        OutputFormat::Quiet => Box::new(QuietFormatter::new()),
    }
}
