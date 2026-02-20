//! Native runtime server loop using krio.

use crate::config::Config;
use crate::metrics::{WorkerStats, WorkerStatsSnapshot};
use cache_core::Cache;
use krio::KrioBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use super::handler::{
    DiskIoWorkerConfig, HandlerConfig, ServerHandler, init_config_channel, launch_lock,
    wait_for_workers,
};

/// Run the native runtime server.
pub fn run<C: Cache + 'static>(
    config: &Config,
    cache: C,
    shutdown: Arc<AtomicBool>,
    drain_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    run_shared(config, Arc::new(cache), shutdown, drain_timeout)
}

/// Run the native runtime server with a pre-shared cache Arc.
///
/// Use this when the cache needs to be shared with other components
/// (e.g., the admin server's stats closure) before launching krio.
pub fn run_shared<C: Cache + 'static>(
    config: &Config,
    cache: Arc<C>,
    shutdown: Arc<AtomicBool>,
    drain_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
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

    // Match krio's default 16KB slot size. Large values are automatically
    // chunked. Use more slots than the default to support many concurrent
    // connections sending responses in parallel.
    // Memory cost: 8192 slots × 16KB = 128MB per worker.
    let send_copy_slot_size = 16384u32;
    let send_copy_count = 8192u16;

    // Determine disk I/O backend
    use crate::config::DiskIoBackendConfig;
    let disk_io_backend = config
        .cache
        .disk
        .as_ref()
        .filter(|d| d.enabled)
        .map(|d| d.io_backend);

    // Pre-create disk file before krio launch (workers need it for O_DIRECT open)
    if disk_io_backend == Some(DiskIoBackendConfig::DirectIo) {
        let disk_config = config.cache.disk.as_ref().unwrap();
        if let Err(e) = ensure_disk_file(&disk_config.path, disk_config.size) {
            return Err(format!("Failed to create disk file: {e}").into());
        }
        info!(
            path = %disk_config.path.display(),
            size = disk_config.size,
            "Pre-created disk file for Direct I/O"
        );
    }

    // Build krio config from server config
    let mut krio_config = krio::Config {
        sq_entries: config.uring.sq_depth,
        sqpoll: config.uring.sqpoll,
        sqpoll_idle_ms: config.uring.sqpoll_idle_ms,
        recv_buffer: krio::RecvBufferConfig {
            ring_size: config.uring.buffer_count.next_power_of_two(),
            buffer_size: config.uring.buffer_size as u32,
            ..Default::default()
        },
        worker: krio::WorkerConfig {
            threads: num_workers,
            pin_to_core: cpu_affinity.is_some(),
            core_offset: cpu_affinity.as_ref().map(|c| c[0]).unwrap_or(0),
        },
        tcp_nodelay: true,
        send_copy_slot_size,
        send_copy_count,
        ..Default::default()
    };

    // Enable krio's disk I/O subsystem based on backend
    match disk_io_backend {
        Some(DiskIoBackendConfig::DirectIo) => {
            krio_config.direct_io = Some(krio::direct_io::DirectIoConfig::default());
        }
        Some(DiskIoBackendConfig::Nvme) => {
            krio_config.nvme = Some(krio::nvme::NvmeConfig::default());
        }
        _ => {}
    }

    // Load TLS config if configured
    let krio_config = if let Some(ref tls_cfg) = config.listener[0].tls {
        let mut c = krio_config;
        c.tls = Some(crate::tls::load_server_config(tls_cfg)?);
        info!("TLS enabled");
        c
    } else {
        krio_config
    };

    // Use the first listener's address for binding
    let bind_addr = config.listener[0].address;

    // Serialize config channel init + launch + worker startup to prevent
    // races when multiple server instances run in the same process (e.g., tests).
    let _launch_guard = launch_lock();

    // Build disk I/O config for workers when io_uring-based backend is enabled
    let disk_io_worker_config: Option<DiskIoWorkerConfig> = match disk_io_backend {
        Some(DiskIoBackendConfig::DirectIo) => {
            let disk_config = config.cache.disk.as_ref().unwrap();
            Some(DiskIoWorkerConfig {
                backend: cache_core::DiskIoBackend::DirectIo,
                path: disk_config.path.to_string_lossy().into_owned(),
                read_buffer_count: 64,
                read_buffer_size: 4096,
                block_size: 4096,
            })
        }
        Some(DiskIoBackendConfig::Nvme) => {
            let disk_config = config.cache.disk.as_ref().unwrap();
            let device_path = disk_config
                .nvme_device
                .clone()
                .expect("nvme_device is required when io_backend = nvme");
            let nsid = disk_config
                .nvme_nsid
                .expect("nvme_nsid is required when io_backend = nvme");
            Some(DiskIoWorkerConfig {
                backend: cache_core::DiskIoBackend::Nvme { device_path, nsid },
                path: String::new(),
                read_buffer_count: 64,
                read_buffer_size: 4096,
                block_size: 4096,
            })
        }
        _ => None,
    };

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
                disk_io_config: disk_io_worker_config.as_ref().map(|c| DiskIoWorkerConfig {
                    backend: c.backend.clone(),
                    path: c.path.clone(),
                    read_buffer_count: c.read_buffer_count,
                    read_buffer_size: c.read_buffer_size,
                    block_size: c.block_size,
                }),
            })
            .expect("failed to queue handler config");
    }
    init_config_channel(config_rx, num_workers);

    let (shutdown_handle, handles) = KrioBuilder::new(krio_config)
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

/// Ensure the disk file exists and is pre-allocated to the required size.
///
/// Creates parent directories and the file if they don't exist, then
/// sets the file length. This must run before krio launch because
/// workers will open the file with O_DIRECT.
fn ensure_disk_file(path: &std::path::Path, size: usize) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    file.set_len(size as u64)?;
    Ok(())
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
