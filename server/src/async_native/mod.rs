//! Native runtime server implementation using ringline (io_uring).
//!
//! Uses `AsyncEventHandler` with one async task per connection, reusing
//! `Connection::process_from()` for protocol parsing and command execution.

mod handler;

pub use server::{run, run_shared};

mod server {
    use crate::config::{Config, DiskIoBackendConfig};
    use crate::disk_io::DiskIoWorkerConfig;
    use crate::metrics::{WorkerStats, WorkerStatsSnapshot};
    use cache_core::Cache;
    use ringline::RinglineBuilder;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};
    use tracing::{debug, info, warn};

    use super::handler::{
        AsyncServerHandler, HandlerConfig, init_config_channel, launch_lock, wait_for_workers,
    };

    /// Run the async native runtime server.
    pub fn run<C: Cache + 'static>(
        config: &Config,
        cache: C,
        shutdown: Arc<AtomicBool>,
        drain_timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        run_shared(config, Arc::new(cache), shutdown, drain_timeout)
    }

    /// Run the async native runtime server with a pre-shared cache Arc.
    pub fn run_shared<C: Cache + 'static>(
        config: &Config,
        cache: Arc<C>,
        shutdown: Arc<AtomicBool>,
        drain_timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let num_workers = config.threads();
        let cpu_affinity = config.cpu_affinity();

        let allow_flush = config.listener.iter().any(|l| l.allow_flush);

        let worker_stats: Arc<Vec<WorkerStats>> =
            Arc::new((0..num_workers).map(|_| WorkerStats::new()).collect());

        let diagnostics_enabled = std::env::var("CRUCIBLE_DIAGNOSTICS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

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

        let send_copy_slot_size = 16384u32;
        let send_copy_count = 8192u16;

        // Determine disk I/O backend
        let disk_io_backend = config
            .cache
            .disk
            .as_ref()
            .filter(|d| d.enabled)
            .map(|d| d.io_backend);

        // Pre-create disk file before ringline launch (workers need it for O_DIRECT open)
        if disk_io_backend == Some(DiskIoBackendConfig::DirectIo) {
            let disk_config = config
                .cache
                .disk
                .as_ref()
                .expect("disk config must be present when io_backend is DirectIo");
            if let Err(e) = ensure_disk_file(&disk_config.path, disk_config.size) {
                return Err(format!("Failed to create disk file: {e}").into());
            }
            info!(
                path = %disk_config.path.display(),
                size = disk_config.size,
                "Pre-created disk file for Direct I/O (async)"
            );
        }

        let krio_config = ringline::Config {
            sq_entries: config.uring.sq_depth,
            sqpoll: config.uring.sqpoll,
            sqpoll_idle_ms: config.uring.sqpoll_idle_ms,
            recv_buffer: ringline::RecvBufferConfig {
                ring_size: config.uring.buffer_count.next_power_of_two(),
                buffer_size: config.uring.buffer_size as u32,
                ..Default::default()
            },
            worker: ringline::WorkerConfig {
                threads: num_workers,
                pin_to_core: cpu_affinity.is_some(),
                core_offset: cpu_affinity.as_ref().map(|c| c[0]).unwrap_or(0),
            },
            tcp_nodelay: true,
            send_copy_slot_size,
            send_copy_count,
            send_slab_slots: 4096,
            ..Default::default()
        };

        // Enable ringline's disk I/O subsystem based on backend
        let mut krio_config = krio_config;
        match disk_io_backend {
            Some(DiskIoBackendConfig::DirectIo) => {
                krio_config.direct_io = Some(ringline::direct_io::DirectIoConfig::default());
            }
            Some(DiskIoBackendConfig::Nvme) => {
                krio_config.nvme = Some(ringline::nvme::NvmeConfig::default());
            }
            _ => {}
        }

        // Build disk I/O worker config
        let disk_io_worker_config: Option<DiskIoWorkerConfig> = match disk_io_backend {
            Some(DiskIoBackendConfig::DirectIo) => {
                let disk_config = config
                    .cache
                    .disk
                    .as_ref()
                    .expect("disk config must be present when io_backend is DirectIo");
                Some(DiskIoWorkerConfig {
                    backend: cache_core::DiskIoBackend::DirectIo,
                    path: disk_config.path.to_string_lossy().into_owned(),
                    read_buffer_count: 64,
                    read_buffer_size: 4096,
                    block_size: 4096,
                })
            }
            Some(DiskIoBackendConfig::Nvme) => {
                let disk_config = config
                    .cache
                    .disk
                    .as_ref()
                    .expect("disk config must be present when io_backend is Nvme");
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

        // Load TLS config if configured
        let krio_config = if let Some(ref tls_cfg) = config.listener[0].tls {
            let mut c = krio_config;
            c.tls = Some(crate::tls::load_server_config(tls_cfg)?);
            info!("TLS enabled");
            c
        } else {
            krio_config
        };

        let bind_target = config.listener[0]
            .bind_target()
            .ok_or("listener must have 'address' or 'unix_socket'")?;

        let _launch_guard = launch_lock();

        let (config_tx, config_rx) = crossbeam_channel::bounded::<HandlerConfig<C>>(num_workers);
        for _ in 0..num_workers {
            config_tx
                .send(HandlerConfig {
                    cache: cache.clone(),
                    shutdown: shutdown.clone(),
                    max_value_size: config.cache.max_value_size,
                    allow_flush,
                    send_copy_slot_size: send_copy_slot_size as usize,
                    set_retry_timeout_us: config.cache.set_retry_timeout_us,
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

        use crate::config::BindTarget;
        let mut builder = RinglineBuilder::new(krio_config);
        builder = match &bind_target {
            BindTarget::Tcp(addr) => builder.bind(*addr),
            BindTarget::Unix(path) => builder.bind_unix(path),
        };
        let (shutdown_handle, handles) = builder.launch::<AsyncServerHandler<C>>()?;

        wait_for_workers();
        drop(_launch_guard);

        // Wait for shutdown: either an OS signal (SIGINT/SIGTERM) or the
        // programmatic shutdown flag (used by tests).
        {
            let shutdown_for_signal = shutdown.clone();
            std::thread::Builder::new()
                .name("signal".to_string())
                .spawn(move || {
                    let signal = ringline::signal::wait();
                    info!(%signal, "Received signal, initiating shutdown...");
                    shutdown_for_signal.store(true, Ordering::SeqCst);
                })
                .expect("failed to spawn signal thread");
        }

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

        // Join workers that stopped within the drain timeout. For workers
        // that are still stuck, force exit — joining them would block forever.
        let mut stuck_count = 0;
        for (i, handle) in handles.into_iter().enumerate() {
            if workers_stopped[i] {
                match handle.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        warn!(worker_id = i, error = %e, "worker thread returned error")
                    }
                    Err(e) => warn!(worker_id = i, "worker thread panicked: {e:?}"),
                }
            } else {
                warn!(
                    worker_id = i,
                    "Worker did not stop within drain timeout — will force exit"
                );
                stuck_count += 1;
            }
        }

        if let Some(handle) = diagnostics_handle {
            let _ = handle.join();
        }

        if stuck_count > 0 {
            warn!(
                stuck_workers = stuck_count,
                "Shutdown complete with stuck worker threads — they will be \
                 cleaned up on process exit"
            );
            return Err(format!(
                "{stuck_count} worker thread(s) did not stop within drain timeout"
            )
            .into());
        }

        info!("Async server shutdown complete");
        Ok(())
    }

    fn run_diagnostics(stats: Arc<Vec<WorkerStats>>, shutdown: Arc<AtomicBool>) {
        let mut prev_snapshots: Vec<WorkerStatsSnapshot> =
            stats.iter().map(|s| s.snapshot()).collect();
        let mut last_report = Instant::now();
        let report_interval = Duration::from_secs(10);

        debug!("Worker diagnostics enabled, reporting every 10s");

        while !shutdown.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_secs(1));

            if last_report.elapsed() >= report_interval {
                let current: Vec<WorkerStatsSnapshot> =
                    stats.iter().map(|s| s.snapshot()).collect();

                use std::fmt::Write;
                let mut report = format!(
                    "\n=== Worker Stats (last {}s) ===\n{:>6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12} {:>10}",
                    report_interval.as_secs(),
                    "worker", "polls", "accepts", "closes", "recv",
                    "send_rdy", "bytes_in", "bytes_out", "conns"
                );

                for (i, (curr, prev)) in current.iter().zip(prev_snapshots.iter()).enumerate() {
                    let delta = curr.delta(prev);
                    write!(
                        &mut report,
                        "\n{:>6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>12} {:>10}",
                        i,
                        delta.poll_count,
                        delta.accepts,
                        delta.close_events,
                        delta.recv_events,
                        delta.send_ready_events,
                        format_bytes(delta.bytes_received),
                        format_bytes(delta.bytes_sent),
                        curr.active_connections,
                    ).unwrap();

                    if delta.backpressure_events > 0 {
                        write!(
                            &mut report,
                            "\n  ^ Worker {} hit backpressure {} times",
                            i, delta.backpressure_events
                        ).unwrap();
                    }
                }
                info!("{report}");

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

    /// Ensure the disk file exists and is pre-allocated to the required size.
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
}

/// Get the backend detail string for the banner.
pub fn backend_detail() -> &'static str {
    "io_uring (async)"
}
