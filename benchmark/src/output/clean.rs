//! Clean table formatter with optional color output.

use super::format::{
    format_bandwidth_bps, format_count, format_latency_padded, format_pct, format_rate_padded,
};
use super::{ColorMode, LatencyStats, OutputFormatter, Results, Sample};
use crate::config::Config;
use std::io::{self, IsTerminal, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Number of sample rows between header reprints.
const HEADER_REPEAT_INTERVAL: u64 = 25;

/// ANSI escape codes for colors.
mod ansi {
    pub const RED: &str = "\x1b[31m";
    pub const RESET: &str = "\x1b[0m";
}

/// Clean table formatter with optional color support.
pub struct CleanFormatter {
    use_color: bool,
    sample_count: AtomicU64,
}

impl CleanFormatter {
    pub fn new(color_mode: ColorMode) -> Self {
        let use_color = match color_mode {
            ColorMode::Always => true,
            ColorMode::Never => false,
            ColorMode::Auto => {
                // Check if stdout is a TTY and NO_COLOR is not set
                io::stdout().is_terminal() && std::env::var("NO_COLOR").is_err()
            }
        };
        Self {
            use_color,
            sample_count: AtomicU64::new(0),
        }
    }

    fn red(&self, s: &str) -> String {
        if self.use_color {
            format!("{}{}{}", ansi::RED, s, ansi::RESET)
        } else {
            s.to_string()
        }
    }

    fn maybe_red(&self, s: &str, condition: bool) -> String {
        if condition {
            self.red(s)
        } else {
            s.to_string()
        }
    }
}

impl OutputFormatter for CleanFormatter {
    fn print_config(&self, config: &Config) {
        println!("crucible benchmark");
        println!("──────────────────");

        // Target line
        let protocol = format!("{:?}", config.target.protocol);
        let tls_suffix = if config.target.tls { ", TLS" } else { "" };
        let endpoints: Vec<_> = config
            .target
            .endpoints
            .iter()
            .map(|e| e.to_string())
            .collect();
        println!(
            "target     {}{}:{} ({}{})",
            config
                .target
                .endpoints
                .first()
                .map(|e| e.ip())
                .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            if endpoints.len() > 1 {
                format!(" (+{})", endpoints.len() - 1)
            } else {
                String::new()
            },
            config
                .target
                .endpoints
                .first()
                .map(|e| e.port())
                .unwrap_or(6379),
            protocol,
            tls_suffix
        );

        // Workload line
        let keyspace_count = format_count(config.workload.keyspace.count as u64);
        println!(
            "workload   {} keys, {}B key, {}B value",
            keyspace_count, config.workload.keyspace.length, config.workload.values.length
        );

        // Threads line
        let threads_str = if let Some(ref cpu_list) = config.general.cpu_list {
            format!("{}, pinned {}", config.general.threads, cpu_list)
        } else {
            format!("{}", config.general.threads)
        };
        println!("threads    {}", threads_str);

        // Connections line
        println!(
            "conns      {}, pipeline {}",
            config.connection.total_connections(),
            config.connection.pipeline_depth
        );

        // Engine line
        let recv_mode = format!("{}", config.general.recv_mode);
        let engine_str = format!("{}", config.general.io_engine);
        if engine_str.to_lowercase().contains("uring") {
            println!("engine     {}, {}", engine_str, recv_mode);
        } else {
            println!("engine     {}", engine_str);
        }

        // Rate limit line (optional)
        if let Some(rate) = config.workload.rate_limit {
            println!("ratelimit  {} req/s", format_count(rate));
        }

        println!();
    }

    fn print_warmup(&self, duration: Duration) {
        println!("[warmup {}s]", duration.as_secs());
    }

    fn print_running(&self, duration: Duration) {
        println!("[running {}s]", duration.as_secs());
        println!();
    }

    fn print_header(&self) {
        println!(
            "time UTC │   req/s │ err% │ hit% │conn% │    p50 │    p90 │    p99 │  p99.9 │ p99.99 │    max"
        );
        println!(
            "─────────┼─────────┼──────┼──────┼──────┼────────┼────────┼────────┼────────┼────────┼───────"
        );
        let _ = io::stdout().flush();
    }

    fn print_sample(&self, sample: &Sample) {
        // Reprint header periodically for readability
        let count = self.sample_count.fetch_add(1, Ordering::Relaxed);
        if count > 0 && count % HEADER_REPEAT_INTERVAL == 0 {
            println!();
            self.print_header();
        }

        let time = sample.timestamp.format("%H:%M:%S");
        let rate = format_rate_padded(sample.req_per_sec, 7);
        let err = format_pct(sample.err_pct);
        let hit = format_pct(sample.hit_pct);
        let conn = format_pct(sample.conn_pct);

        // Color error% red if > 0
        let err_colored = self.maybe_red(&format!("{:>5}", err), sample.err_pct > 0.0);

        // Color conn% red if < 100
        let conn_colored = self.maybe_red(&format!("{:>5}", conn), sample.conn_pct < 100.0);

        let p50 = format_latency_padded(sample.p50_us, 7);
        let p90 = format_latency_padded(sample.p90_us, 7);
        let p99 = format_latency_padded(sample.p99_us, 7);
        let p999 = format_latency_padded(sample.p999_us, 7);
        let p9999 = format_latency_padded(sample.p9999_us, 7);
        let max = format_latency_padded(sample.max_us, 6);

        println!(
            "{} │ {} │{} │{:>5} │{} │ {} │ {} │ {} │ {} │ {} │ {}",
            time, rate, err_colored, hit, conn_colored, p50, p90, p99, p999, p9999, max
        );
        let _ = io::stdout().flush();
    }

    fn print_results(&self, results: &Results) {
        println!();
        println!(
            "──────────────────────────────────────────────────────────────────────────────────────────────"
        );
        println!("RESULTS ({:.0}s)", results.duration_secs);
        println!(
            "──────────────────────────────────────────────────────────────────────────────────────────────"
        );

        // Throughput line
        let throughput = results.responses as f64 / results.duration_secs;
        let err_pct = if results.responses > 0 {
            (results.errors as f64 / results.responses as f64) * 100.0
        } else {
            0.0
        };
        let err_str = format!("{}% errors", format_pct(err_pct));
        let err_colored = self.maybe_red(&err_str, err_pct > 0.0);
        println!(
            "throughput   {} req/s, {}",
            format_count(throughput as u64),
            err_colored
        );

        // Bandwidth line
        let rx_bps = (results.bytes_rx as f64 / results.duration_secs) * 8.0;
        let tx_bps = (results.bytes_tx as f64 / results.duration_secs) * 8.0;
        if results.bytes_rx > 0 || results.bytes_tx > 0 {
            println!(
                "bandwidth    {} RX, {} TX",
                format_bandwidth_bps(rx_bps),
                format_bandwidth_bps(tx_bps)
            );
        }

        println!();

        // Hit rate line
        let hit_pct = if results.hits + results.misses > 0 {
            (results.hits as f64 / (results.hits + results.misses) as f64) * 100.0
        } else {
            0.0
        };
        println!(
            "hit rate     {}% ({} hit, {} miss)",
            format_pct(hit_pct),
            format_count(results.hits),
            format_count(results.misses)
        );

        println!();

        // Latency table
        println!(
            "latency      {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}",
            "p50", "p90", "p99", "p99.9", "p99.99", "max"
        );

        fn format_latency_row(name: &str, stats: &LatencyStats) -> String {
            format!(
                "{:<12} {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}",
                name,
                format_latency_padded(stats.p50_us, 6),
                format_latency_padded(stats.p90_us, 6),
                format_latency_padded(stats.p99_us, 6),
                format_latency_padded(stats.p999_us, 6),
                format_latency_padded(stats.p9999_us, 6),
                format_latency_padded(stats.max_us, 6),
            )
        }

        if results.get_count > 0 {
            println!("{}", format_latency_row("GET", &results.get_latencies));
        }
        if results.set_count > 0 {
            println!("{}", format_latency_row("SET", &results.set_latencies));
        }

        println!();

        // Connections line
        let conn_str = if results.conns_failed > 0 {
            self.red(&format!(
                "{} active, {} failed",
                results.conns_active, results.conns_failed
            ))
        } else {
            format!("{} active, 0 failed", results.conns_active)
        };
        println!("connections  {}", conn_str);
    }
}
