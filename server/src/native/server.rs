//! Native runtime server loop using kompio.

use crate::config::Config;
use crate::metrics::{WorkerStats, WorkerStatsSnapshot};
use cache_core::Cache;
use kompio::KompioBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use super::handler::{
    HandlerConfig, ServerHandler, init_config_channel, launch_lock, wait_for_workers,
};

/// Run the native runtime server.
pub fn run<C: Cache + 'static>(
    config: &Config,
    cache: C,
    shutdown: Arc<AtomicBool>,
    drain_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let cache = Arc::new(cache);
    let num_workers = config.threads();
    let cpu_affinity = config.cpu_affinity();

    // Compute global allow_flush: enabled if ANY listener allows it
    let allow_flush = config.listener.iter().any(|l| l.allow_flush);

    // Create per-worker stats
    let worker_stats: Arc<Vec<WorkerStats>> =
        Arc::new((0..num_workers).map(|_| WorkerStats::new()).collect());

    // Check if diagnostics are enabled via environment variable
    let diagnostics_enabled = std::env::var("CRUCIBLE_DIAGNOSTICS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // Start diagnostics thread if enabled
    let diagnostics_handle = if diagnostics_enabled {
        let stats = worker_stats.clone();
        let shutdown_flag = shutdown.clone();
        Some(
            std::thread::Builder::new()
                .name("diagnostics".to_string())
                .spawn(move || {
                    run_diagnostics(stats, shutdown_flag);
                })
                .expect("failed to spawn diagnostics thread"),
        )
    } else {
        None
    };

    // Match kompio's default 16KB slot size. Large values are automatically
    // chunked. Use more slots than the default to support many concurrent
    // connections sending responses in parallel.
    // Memory cost: 8192 slots × 16KB = 128MB per worker.
    let send_copy_slot_size = 16384u32;
    let send_copy_count = 8192u16;

    // Build kompio config from server config
    let kompio_config = kompio::Config {
        sq_entries: config.uring.sq_depth,
        sqpoll: config.uring.sqpoll,
        sqpoll_idle_ms: config.uring.sqpoll_idle_ms,
        recv_buffer: kompio::RecvBufferConfig {
            ring_size: config.uring.buffer_count.next_power_of_two(),
            buffer_size: config.uring.buffer_size as u32,
            ..Default::default()
        },
        worker: kompio::WorkerConfig {
            threads: num_workers,
            pin_to_core: cpu_affinity.is_some(),
            core_offset: cpu_affinity.as_ref().map(|c| c[0]).unwrap_or(0),
        },
        tcp_nodelay: true,
        send_copy_slot_size,
        send_copy_count,
        ..Default::default()
    };

    // Use the first listener's address for binding
    let bind_addr = config.listener[0].address;

    // Serialize config channel init + launch + worker startup to prevent
    // races when multiple server instances run in the same process (e.g., tests).
    let _launch_guard = launch_lock();

    // Distribute HandlerConfigs to workers via a crossbeam channel.
    // Each worker's create_for_worker() receives one config from the channel.
    let (config_tx, config_rx) = crossbeam_channel::bounded::<HandlerConfig<C>>(num_workers);
    for _ in 0..num_workers {
        config_tx
            .send(HandlerConfig {
                cache: cache.clone(),
                stats: worker_stats.clone(),
                shutdown: shutdown.clone(),
                max_value_size: config.cache.max_value_size,
                allow_flush,
                send_copy_slot_size: send_copy_slot_size as usize,
            })
            .expect("failed to queue handler config");
    }
    init_config_channel(config_rx, num_workers);

    let (shutdown_handle, handles) = KompioBuilder::new(kompio_config)
        .bind(&bind_addr.to_string())
        .launch::<ServerHandler<C>>()?;

    // Wait for all workers to receive their configs before releasing
    // the launch lock, preventing other server instances from overwriting
    // the config channel before our workers have read from it.
    wait_for_workers();
    drop(_launch_guard);

    // Wait for shutdown signal
    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    info!("Shutdown signal received, draining connections...");
    shutdown_handle.shutdown();

    // Wait for workers to drain with timeout
    let drain_start = Instant::now();
    let mut workers_stopped = vec![false; handles.len()];

    while drain_start.elapsed() < drain_timeout {
        let mut all_stopped = true;
        for (i, handle) in handles.iter().enumerate() {
            if !workers_stopped[i] && handle.is_finished() {
                workers_stopped[i] = true;
                debug!(worker_id = i, "Worker stopped");
            }
            if !workers_stopped[i] {
                all_stopped = false;
            }
        }

        if all_stopped {
            break;
        }

        std::thread::sleep(Duration::from_millis(100));
    }

    let active_conns = crate::metrics::CONNECTIONS_ACTIVE.value();
    if active_conns > 0 {
        warn!(
            active_connections = active_conns,
            "Drain timeout reached, {} connections still active — forcing exit", active_conns
        );
        std::process::exit(0);
    }

    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!(worker_id = i, error = %e, "worker thread returned error"),
            Err(e) => warn!(worker_id = i, "worker thread panicked: {e:?}"),
        }
    }

    if let Some(handle) = diagnostics_handle {
        let _ = handle.join();
    }

    info!("Server shutdown complete");
    Ok(())
}

/// Diagnostics thread that periodically reports per-worker stats.
fn run_diagnostics(stats: Arc<Vec<WorkerStats>>, shutdown: Arc<AtomicBool>) {
    let mut prev_snapshots: Vec<WorkerStatsSnapshot> = stats.iter().map(|s| s.snapshot()).collect();
    let mut last_report = Instant::now();
    let report_interval = Duration::from_secs(10);

    debug!("Worker diagnostics enabled, reporting every 10s");

    while !shutdown.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_secs(1));

        if last_report.elapsed() >= report_interval {
            let current: Vec<WorkerStatsSnapshot> = stats.iter().map(|s| s.snapshot()).collect();

            eprintln!(
                "\n[diagnostics] === Worker Stats (last {}s) ===",
                report_interval.as_secs()
            );
            eprintln!(
                "{:>6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12} {:>10}",
                "worker",
                "polls",
                "accepts",
                "closes",
                "recv",
                "send_rdy",
                "bytes_in",
                "bytes_out",
                "conns"
            );

            for (i, (curr, prev)) in current.iter().zip(prev_snapshots.iter()).enumerate() {
                let delta = curr.delta(prev);
                eprintln!(
                    "{:>6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12} {:>10}",
                    i,
                    delta.poll_count,
                    delta.accepts,
                    delta.close_events,
                    delta.recv_events,
                    delta.send_ready_events,
                    format_bytes(delta.bytes_received),
                    format_bytes(delta.bytes_sent),
                    curr.active_connections,
                );

                if delta.backpressure_events > 0 {
                    eprintln!(
                        "  ^ INFO: Worker {} hit backpressure {} times",
                        i, delta.backpressure_events
                    );
                }
            }

            prev_snapshots = current;
            last_report = Instant::now();
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1}GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.1}MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1}KB", bytes as f64 / 1_000.0)
    } else {
        format!("{}B", bytes)
    }
}
