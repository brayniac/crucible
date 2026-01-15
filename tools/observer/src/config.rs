//! CLI configuration for the observer.

use clap::{Parser, ValueEnum};
use std::time::Duration;

/// Packet capture analytics for cache traffic latency and throughput.
#[derive(Parser, Debug)]
#[command(name = "crucible-observer")]
#[command(about = "Packet capture analytics for cache traffic")]
#[command(version)]
pub struct Cli {
    /// Network interface to capture on (e.g., "eth0", "lo")
    #[arg(short, long)]
    pub interface: String,

    /// Server port to monitor (e.g., 6379 for Redis, 11211 for Memcache)
    #[arg(short, long)]
    pub port: u16,

    /// Protocol hint for parsing (default: auto-detect)
    #[arg(long, default_value = "auto")]
    pub protocol: ProtocolHint,

    /// Duration to capture in seconds (default: run until interrupted)
    #[arg(short, long)]
    pub duration: Option<u64>,

    /// Reporting interval in seconds
    #[arg(long, default_value = "1")]
    pub interval: u64,

    /// Output format
    #[arg(long, default_value = "table")]
    pub format: OutputFormat,

    /// Additional BPF filter expression (advanced)
    #[arg(long)]
    pub filter: Option<String>,
}

impl Cli {
    /// Get the capture duration as a Duration, if specified.
    pub fn duration(&self) -> Option<Duration> {
        self.duration.map(Duration::from_secs)
    }

    /// Get the reporting interval as a Duration.
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval)
    }
}

/// Protocol hint for parsing packets.
#[derive(Debug, Clone, Copy, Default, ValueEnum, PartialEq, Eq)]
pub enum ProtocolHint {
    /// Auto-detect protocol from packet contents
    #[default]
    Auto,
    /// Redis RESP protocol
    Resp,
    /// Memcache ASCII protocol
    Memcache,
    /// Memcache binary protocol
    MemcacheBinary,
}

/// Output format for metrics reporting.
#[derive(Debug, Clone, Copy, Default, ValueEnum, PartialEq, Eq)]
pub enum OutputFormat {
    /// Human-readable table format
    #[default]
    Table,
    /// JSON format (one object per line)
    Json,
    /// Quiet mode (only final summary)
    Quiet,
}
