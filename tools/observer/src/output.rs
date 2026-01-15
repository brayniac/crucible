//! Output formatting for periodic reports and final summary.
//!
//! Matches the formatting style of crucible-benchmark.

use crate::config::OutputFormat;
use crate::metrics;
use metriken::histogram::Histogram;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Number of sample rows between header reprints.
const HEADER_REPEAT_INTERVAL: u64 = 25;

/// State for tracking metrics between reports.
pub struct OutputState {
    start_time: Instant,
    last_report: Instant,
    last_requests: u64,
    last_responses: u64,
    last_bytes_request: u64,
    last_bytes_response: u64,
    last_retransmits: u64,
    last_histogram: Option<Histogram>,
    sample_count: AtomicU64,
}

impl OutputState {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            start_time: now,
            last_report: now,
            last_requests: 0,
            last_responses: 0,
            last_bytes_request: 0,
            last_bytes_response: 0,
            last_retransmits: 0,
            last_histogram: None,
            sample_count: AtomicU64::new(0),
        }
    }

    /// Print the startup banner.
    pub fn print_banner(&self, interface: &str, port: u16, protocol: &str) {
        println!("crucible observer");
        println!("─────────────────");
        println!("interface  {}", interface);
        println!("port       {}", port);
        println!("protocol   {}", protocol);
        println!();
    }

    /// Print the header for table output.
    /// Widths: time=8, all others=5, except p99.99=6
    pub fn print_header(&self, format: OutputFormat) {
        if format != OutputFormat::Table {
            return;
        }
        println!(
            "time UTC │ req/s │   RX │   TX │ rexmt │  p50 │  p90 │  p99 │p99.9 │p99.99 │  max"
        );
        println!(
            "─────────┼───────┼──────┼──────┼───────┼──────┼──────┼──────┼──────┼───────┼─────"
        );
        let _ = io::stdout().flush();
    }

    /// Print a periodic report.
    pub fn print_periodic(&mut self, format: OutputFormat) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_report).as_secs_f64();
        if elapsed < 0.001 {
            return; // Avoid division by zero
        }

        // Get current values
        let requests = metrics::REQUESTS_TOTAL.value();
        let responses = metrics::RESPONSES_TOTAL.value();
        let bytes_request = metrics::BYTES_REQUEST.value();
        let bytes_response = metrics::BYTES_RESPONSE.value();
        let retransmits = metrics::TCP_RETRANSMITS.value();

        // Calculate request rate
        let delta_requests = requests.saturating_sub(self.last_requests);
        let req_rate = delta_requests as f64 / elapsed;

        // Calculate bandwidth (bits per second)
        let delta_bytes_rx = bytes_response.saturating_sub(self.last_bytes_response);
        let delta_bytes_tx = bytes_request.saturating_sub(self.last_bytes_request);
        let rx_bps = (delta_bytes_rx as f64 / elapsed) * 8.0;
        let tx_bps = (delta_bytes_tx as f64 / elapsed) * 8.0;

        // Calculate retransmits this interval
        let delta_retransmits = retransmits.saturating_sub(self.last_retransmits);

        // Get histogram snapshot and calculate percentiles
        let current_histogram = metrics::LATENCY.load();
        let (p50, p90, p99, p999, p9999, max) = match (&current_histogram, &self.last_histogram) {
            (Some(current), Some(previous)) => match current.wrapping_sub(previous) {
                Ok(delta) => extract_percentiles_full(&delta),
                Err(_) => (0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            },
            (Some(current), None) => extract_percentiles_full(current),
            _ => (0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        };

        // Output based on format
        match format {
            OutputFormat::Table => {
                // Reprint header periodically
                let count = self.sample_count.fetch_add(1, Ordering::Relaxed);
                if count > 0 && count.is_multiple_of(HEADER_REPEAT_INTERVAL) {
                    println!(
                        "─────────┼───────┼──────┼──────┼───────┼──────┼──────┼──────┼──────┼───────┼─────"
                    );
                    self.print_header(format);
                }

                // Get current UTC time
                let utc_now = chrono_lite_time();

                // Widths: time=8, all others=5, except p99.99=6
                println!(
                    "{} │ {:>5} │ {:>4} │ {:>4} │ {:>5} │ {:>4} │ {:>4} │ {:>4} │{:>5} │{:>6} │ {:>4}",
                    utc_now,
                    format_rate(req_rate),
                    format_bandwidth_compact(rx_bps),
                    format_bandwidth_compact(tx_bps),
                    delta_retransmits,
                    format_latency_compact(p50),
                    format_latency_compact(p90),
                    format_latency_compact(p99),
                    format_latency_compact(p999),
                    format_latency_compact(p9999),
                    format_latency_compact(max),
                );
                let _ = io::stdout().flush();
            }
            OutputFormat::Json => {
                println!(
                    r#"{{"elapsed_s":{},"req_per_sec":{:.1},"rx_bps":{:.0},"tx_bps":{:.0},"retransmits":{},"p50_us":{:.1},"p90_us":{:.1},"p99_us":{:.1},"p999_us":{:.1},"p9999_us":{:.1},"max_us":{:.1},"requests":{},"responses":{}}}"#,
                    now.duration_since(self.start_time).as_secs(),
                    req_rate,
                    rx_bps,
                    tx_bps,
                    delta_retransmits,
                    p50,
                    p90,
                    p99,
                    p999,
                    p9999,
                    max,
                    delta_requests,
                    responses.saturating_sub(self.last_responses),
                );
            }
            OutputFormat::Quiet => {}
        }

        // Update state
        self.last_report = now;
        self.last_requests = requests;
        self.last_responses = responses;
        self.last_bytes_request = bytes_request;
        self.last_bytes_response = bytes_response;
        self.last_retransmits = retransmits;
        self.last_histogram = current_histogram;
    }

    /// Print the final summary.
    pub fn print_summary(&self, connections: usize) {
        let elapsed = self.start_time.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        // Get final values
        let requests = metrics::REQUESTS_TOTAL.value();
        let responses = metrics::RESPONSES_TOTAL.value();
        let hits = metrics::RESPONSES_HIT.value();
        let misses = metrics::RESPONSES_MISS.value();
        let stored = metrics::RESPONSES_STORED.value();
        let errors = metrics::RESPONSES_ERROR.value();
        let bytes_request = metrics::BYTES_REQUEST.value();
        let bytes_response = metrics::BYTES_RESPONSE.value();
        let gets = metrics::REQUESTS_GET.value();
        let sets = metrics::REQUESTS_SET.value();
        let retransmits = metrics::TCP_RETRANSMITS.value();

        // Calculate rates
        let avg_resp_rate = responses as f64 / elapsed_secs;

        // Calculate hit rate
        let total_gets = hits + misses;
        let hit_pct = if total_gets > 0 {
            (hits as f64 / total_gets as f64) * 100.0
        } else {
            0.0
        };

        // Get overall latency percentiles
        let (p50, p90, p99, p999, max) = if let Some(hist) = metrics::LATENCY.load() {
            extract_percentiles(&hist)
        } else {
            (0.0, 0.0, 0.0, 0.0, 0.0)
        };
        let p9999 = if let Some(hist) = metrics::LATENCY.load() {
            percentile(&hist, 99.99)
        } else {
            0.0
        };

        // Get per-command latency
        let get_latency = if let Some(hist) = metrics::LATENCY_GET.load() {
            let (p50, p90, p99, p999, max) = extract_percentiles(&hist);
            let p9999 = percentile(&hist, 99.99);
            (p50, p90, p99, p999, p9999, max)
        } else {
            (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        };

        let set_latency = if let Some(hist) = metrics::LATENCY_SET.load() {
            let (p50, p90, p99, p999, max) = extract_percentiles(&hist);
            let p9999 = percentile(&hist, 99.99);
            (p50, p90, p99, p999, p9999, max)
        } else {
            (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        };

        println!();
        println!("────────────────────────────────────────────────────────────────────────────");
        println!("RESULTS ({:.0}s)", elapsed_secs);
        println!("────────────────────────────────────────────────────────────────────────────");

        // Throughput line
        let err_pct = if responses > 0 {
            (errors as f64 / responses as f64) * 100.0
        } else {
            0.0
        };
        println!(
            "throughput   {} req/s, {}% errors",
            format_count(avg_resp_rate as u64),
            format_pct(err_pct)
        );

        // Bandwidth line
        let rx_bps = (bytes_response as f64 / elapsed_secs) * 8.0;
        let tx_bps = (bytes_request as f64 / elapsed_secs) * 8.0;
        if bytes_request > 0 || bytes_response > 0 {
            println!(
                "bandwidth    {} RX, {} TX",
                format_bandwidth_bps(rx_bps),
                format_bandwidth_bps(tx_bps)
            );
        }

        println!();

        // Hit rate line
        println!(
            "hit rate     {}% ({} hit, {} miss)",
            format_pct(hit_pct),
            format_count(hits),
            format_count(misses)
        );

        // Stored/errors line
        if stored > 0 || errors > 0 {
            println!(
                "mutations    {} stored, {} errors",
                format_count(stored),
                format_count(errors)
            );
        }

        println!();

        // Latency table header
        println!(
            "latency      {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}",
            "p50", "p90", "p99", "p99.9", "p99.99", "max"
        );

        // Overall latency
        println!(
            "{:<12} {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}",
            "all",
            format_latency_padded(p50, 6),
            format_latency_padded(p90, 6),
            format_latency_padded(p99, 6),
            format_latency_padded(p999, 6),
            format_latency_padded(p9999, 6),
            format_latency_padded(max, 6),
        );

        // Per-command latency
        if gets > 0 {
            println!(
                "{:<12} {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}",
                "GET",
                format_latency_padded(get_latency.0, 6),
                format_latency_padded(get_latency.1, 6),
                format_latency_padded(get_latency.2, 6),
                format_latency_padded(get_latency.3, 6),
                format_latency_padded(get_latency.4, 6),
                format_latency_padded(get_latency.5, 6),
            );
        }
        if sets > 0 {
            println!(
                "{:<12} {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}",
                "SET",
                format_latency_padded(set_latency.0, 6),
                format_latency_padded(set_latency.1, 6),
                format_latency_padded(set_latency.2, 6),
                format_latency_padded(set_latency.3, 6),
                format_latency_padded(set_latency.4, 6),
                format_latency_padded(set_latency.5, 6),
            );
        }

        println!();

        // Connections line
        println!("connections  {} observed", connections);

        // TCP stats
        if retransmits > 0 {
            println!("tcp          {} retransmits", retransmits);
        }

        // Totals
        println!(
            "totals       {} requests, {} responses",
            format_count(requests),
            format_count(responses)
        );
    }
}

impl Default for OutputState {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Formatting helpers (matching benchmark/src/output/format.rs)
// ============================================================================

fn format_3sig(value: f64, suffix: &str) -> String {
    if value < 9.995 {
        format!("{:.2}{}", value, suffix)
    } else if value < 99.95 {
        format!("{:.1}{}", value, suffix)
    } else {
        format!("{:.0}{}", value, suffix)
    }
}

/// Format a rate with 3 significant figures and SI suffix.
fn format_rate(value: f64) -> String {
    if value < 1_000.0 {
        format_3sig(value, "")
    } else if value < 1_000_000.0 {
        format_3sig(value / 1_000.0, "K")
    } else {
        format_3sig(value / 1_000_000.0, "M")
    }
}

/// Format latency in microseconds with autoscaling.
fn format_latency_us(us: f64) -> String {
    if us < 1_000.0 {
        if us < 10.0 {
            format!("{:.2}us", us)
        } else if us < 100.0 {
            format!("{:.1}us", us)
        } else {
            format!("{:.0}us", us)
        }
    } else if us < 1_000_000.0 {
        let ms = us / 1_000.0;
        if ms < 10.0 {
            format!("{:.2}ms", ms)
        } else if ms < 100.0 {
            format!("{:.1}ms", ms)
        } else {
            format!("{:.0}ms", ms)
        }
    } else {
        let s = us / 1_000_000.0;
        if s < 10.0 {
            format!("{:.2}s", s)
        } else if s < 100.0 {
            format!("{:.1}s", s)
        } else {
            format!("{:.0}s", s)
        }
    }
}

fn format_latency_padded(us: f64, width: usize) -> String {
    let s = format_latency_us(us);
    format!("{:>width$}", s, width = width)
}

/// Format percentage with 3 significant figures.
fn format_pct(value: f64) -> String {
    if value >= 100.0 {
        "100".to_string()
    } else if value >= 10.0 {
        format!("{:.1}", value)
    } else {
        format!("{:.2}", value)
    }
}

/// Format count with SI suffix.
fn format_count(value: u64) -> String {
    let v = value as f64;
    if v < 1_000.0 {
        format!("{}", value)
    } else if v < 1_000_000.0 {
        format!("{:.1}K", v / 1_000.0)
    } else if v < 1_000_000_000.0 {
        format!("{:.1}M", v / 1_000_000.0)
    } else {
        format!("{:.1}B", v / 1_000_000_000.0)
    }
}

/// Format bandwidth in bits per second.
fn format_bandwidth_bps(bps: f64) -> String {
    if bps < 1_000.0 {
        format!("{:.0} bps", bps)
    } else if bps < 1_000_000.0 {
        format!("{:.1} Kbps", bps / 1_000.0)
    } else if bps < 1_000_000_000.0 {
        format!("{:.1} Mbps", bps / 1_000_000.0)
    } else {
        format!("{:.2} Gbps", bps / 1_000_000_000.0)
    }
}

/// Format bandwidth in compact form (no padding).
fn format_bandwidth_compact(bps: f64) -> String {
    if bps < 1_000.0 {
        format!("{:.0}b", bps)
    } else if bps < 1_000_000.0 {
        format_3sig(bps / 1_000.0, "K")
    } else if bps < 1_000_000_000.0 {
        format_3sig(bps / 1_000_000.0, "M")
    } else {
        format_3sig(bps / 1_000_000_000.0, "G")
    }
}

/// Format latency in compact form (no padding).
fn format_latency_compact(us: f64) -> String {
    if us < 1_000.0 {
        if us < 10.0 {
            format!("{:.1}u", us)
        } else {
            format!("{:.0}us", us)
        }
    } else if us < 1_000_000.0 {
        let ms = us / 1_000.0;
        if ms < 10.0 {
            format!("{:.1}m", ms)
        } else {
            format!("{:.0}ms", ms)
        }
    } else {
        let s = us / 1_000_000.0;
        format!("{:.1}s", s)
    }
}

/// Get current time as HH:MM:SS string (lightweight, no chrono dependency).
fn chrono_lite_time() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let hours = (secs / 3600) % 24;
    let mins = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", hours, mins, s)
}

// ============================================================================
// Histogram helpers
// ============================================================================

fn percentile(hist: &Histogram, p: f64) -> f64 {
    if let Ok(Some(results)) = hist.percentiles(&[p])
        && let Some((_, bucket)) = results.first()
    {
        // Convert from nanoseconds to microseconds
        return bucket.end() as f64 / 1_000.0;
    }
    0.0
}

fn extract_percentiles(hist: &Histogram) -> (f64, f64, f64, f64, f64) {
    (
        percentile(hist, 50.0),
        percentile(hist, 90.0),
        percentile(hist, 99.0),
        percentile(hist, 99.9),
        percentile(hist, 100.0),
    )
}

fn extract_percentiles_full(hist: &Histogram) -> (f64, f64, f64, f64, f64, f64) {
    (
        percentile(hist, 50.0),
        percentile(hist, 90.0),
        percentile(hist, 99.0),
        percentile(hist, 99.9),
        percentile(hist, 99.99),
        percentile(hist, 100.0),
    )
}
