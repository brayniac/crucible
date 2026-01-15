//! Crucible Observer - Packet capture analytics for cache traffic.
//!
//! This tool passively captures network traffic to measure cache server
//! throughput and latency without adding overhead to the data path.

mod capture;
mod config;
mod connection;
mod metrics;
mod output;
mod protocol;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use clap::Parser;
use tracing::error;

use capture::PacketCapture;
use config::{Cli, OutputFormat, ProtocolHint};
use connection::ConnectionTracker;
use output::OutputState;

fn main() {
    // Initialize tracing (only for errors, not general output)
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::ERROR)
        .init();

    // Parse CLI arguments
    let cli = Cli::parse();

    // Set up signal handler for graceful shutdown
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    // Initialize packet capture
    let mut capture = match PacketCapture::new(&cli.interface, cli.port, cli.filter.as_deref()) {
        Ok(cap) => cap,
        Err(e) => {
            error!("Failed to initialize packet capture: {}", e);
            eprintln!("Error: {}", e);
            eprintln!();
            eprintln!("Make sure you have permission to capture packets.");
            eprintln!("On Linux, you may need to run with sudo or set capabilities:");
            eprintln!("  sudo setcap cap_net_raw,cap_net_admin=eip <binary>");
            std::process::exit(1);
        }
    };

    // Initialize connection tracker
    let mut tracker = ConnectionTracker::new(cli.protocol);

    // Initialize output state and print banner
    let mut output = OutputState::new();
    let protocol_str = match cli.protocol {
        ProtocolHint::Auto => "auto-detect",
        ProtocolHint::Resp => "RESP",
        ProtocolHint::Memcache => "Memcache ASCII",
        ProtocolHint::MemcacheBinary => "Memcache Binary",
    };
    if cli.format == OutputFormat::Table {
        output.print_banner(&cli.interface, cli.port, protocol_str);
    }
    output.print_header(cli.format);

    // Main capture loop
    let start = Instant::now();
    let mut last_report = Instant::now();
    let interval = cli.interval();
    let duration = cli.duration();

    while running.load(Ordering::SeqCst) {
        // Check duration limit
        if let Some(d) = duration
            && start.elapsed() >= d
        {
            break;
        }

        // Capture next packet
        match capture.next_packet() {
            Ok(Some(segment)) => {
                tracker.process_segment(segment);
            }
            Ok(None) => {
                // Timeout - no packet available
            }
            Err(e) => {
                error!("Capture error: {}", e);
            }
        }

        // Periodic report
        if last_report.elapsed() >= interval {
            output.print_periodic(cli.format);
            last_report = Instant::now();
        }
    }

    // Print final summary
    if cli.format != OutputFormat::Json {
        output.print_summary(tracker.connection_count());
    }
}
