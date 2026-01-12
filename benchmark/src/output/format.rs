//! Number formatting utilities for benchmark output.

/// Format a rate (requests/second) with SI suffixes.
/// - < 1K: raw number (e.g., "999")
/// - 1K - 999.9K: "XXX.XK" (e.g., "234.5K")
/// - >= 1M: "X.XXXM" (e.g., "10.23M")
pub fn format_rate(value: f64) -> String {
    if value < 1_000.0 {
        format!("{:.0}", value)
    } else if value < 1_000_000.0 {
        format!("{:.1}K", value / 1_000.0)
    } else {
        format!("{:.2}M", value / 1_000_000.0)
    }
}

/// Format a rate with padding for table alignment.
pub fn format_rate_padded(value: f64, width: usize) -> String {
    let s = format_rate(value);
    format!("{:>width$}", s, width = width)
}

/// Format a latency value in microseconds with autoscaling.
/// - < 1000us: "XXXus" (e.g., "42us")
/// - 1ms - 999ms: "X.Xms" (e.g., "1.2ms")
/// - >= 1s: "X.Xs" (e.g., "1.5s")
pub fn format_latency_us(us: f64) -> String {
    if us < 1_000.0 {
        format!("{:.0}us", us)
    } else if us < 1_000_000.0 {
        format!("{:.1}ms", us / 1_000.0)
    } else {
        format!("{:.1}s", us / 1_000_000.0)
    }
}

/// Format a latency with padding for table alignment.
pub fn format_latency_padded(us: f64, width: usize) -> String {
    let s = format_latency_us(us);
    format!("{:>width$}", s, width = width)
}

/// Format a percentage with adaptive precision.
/// - >= 10%: 1 decimal (e.g., "95.2")
/// - < 10%: 2 decimals (e.g., "9.99", "0.01")
pub fn format_pct(value: f64) -> String {
    if value >= 10.0 {
        format!("{:.1}", value)
    } else {
        format!("{:.2}", value)
    }
}

/// Format a percentage with padding for table alignment.
pub fn format_pct_padded(value: f64, width: usize) -> String {
    let s = format_pct(value);
    format!("{:>width$}", s, width = width)
}

/// Format bandwidth in bits per second with SI suffixes.
/// - < 1K: "XXX bps"
/// - 1K - 999.9K: "XXX.X Kbps"
/// - 1M - 999.9M: "XXX.X Mbps"
/// - >= 1G: "X.XX Gbps"
pub fn format_bandwidth_bps(bps: f64) -> String {
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

/// Format a count with SI suffixes for display.
/// - < 1K: raw number
/// - 1K - 999.9K: "XXX.XK"
/// - >= 1M: "X.XM"
pub fn format_count(value: u64) -> String {
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

/// Format bytes with SI suffixes.
pub fn format_bytes(bytes: u64) -> String {
    let v = bytes as f64;
    if v < 1_024.0 {
        format!("{}B", bytes)
    } else if v < 1_024.0 * 1_024.0 {
        format!("{:.1}KB", v / 1_024.0)
    } else if v < 1_024.0 * 1_024.0 * 1_024.0 {
        format!("{:.1}MB", v / (1_024.0 * 1_024.0))
    } else {
        format!("{:.2}GB", v / (1_024.0 * 1_024.0 * 1_024.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_rate() {
        assert_eq!(format_rate(500.0), "500");
        assert_eq!(format_rate(1_500.0), "1.5K");
        assert_eq!(format_rate(234_567.0), "234.6K");
        assert_eq!(format_rate(1_234_567.0), "1.23M");
        assert_eq!(format_rate(10_234_567.0), "10.23M");
    }

    #[test]
    fn test_format_latency() {
        assert_eq!(format_latency_us(42.0), "42us");
        assert_eq!(format_latency_us(999.0), "999us");
        assert_eq!(format_latency_us(1_200.0), "1.2ms");
        assert_eq!(format_latency_us(200_000.0), "200.0ms");
        assert_eq!(format_latency_us(1_500_000.0), "1.5s");
    }

    #[test]
    fn test_format_pct() {
        assert_eq!(format_pct(95.23), "95.2");
        assert_eq!(format_pct(100.0), "100.0");
        assert_eq!(format_pct(9.99), "9.99");
        assert_eq!(format_pct(0.01), "0.01");
        assert_eq!(format_pct(0.0), "0.00");
    }

    #[test]
    fn test_format_bandwidth() {
        assert_eq!(format_bandwidth_bps(500.0), "500 bps");
        assert_eq!(format_bandwidth_bps(1_500.0), "1.5 Kbps");
        assert_eq!(format_bandwidth_bps(892_000_000.0), "892.0 Mbps");
        assert_eq!(format_bandwidth_bps(1_240_000_000.0), "1.24 Gbps");
    }
}
