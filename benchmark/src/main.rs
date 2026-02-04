use benchmark::config::Config;
use benchmark::metrics;
use benchmark::ratelimit::DynamicRateLimiter;
use benchmark::saturation::SaturationSearchState;
use benchmark::viewer;
use benchmark::worker::Phase;
use benchmark::{
    AdminServer, IoWorker, IoWorkerConfig, LatencyStats, OutputFormatter, Results, Sample,
    SharedState, create_formatter, parse_cpu_list,
};

use chrono::Utc;
use clap::{Parser, Subcommand};
use io_driver::IoEngine;
use metriken::{AtomicHistogram, histogram::Histogram};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

/// Timeout for waiting on worker threads to finish during shutdown.
const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Parser, Debug)]
#[command(name = "crucible-benchmark")]
#[command(about = "High-performance cache protocol benchmark tool")]
#[command(version)]
#[command(args_conflicts_with_subcommands = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Path to configuration file
    #[arg(value_name = "CONFIG")]
    config: Option<PathBuf>,

    /// Path to write Parquet output file
    #[arg(long)]
    parquet: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// View benchmark results in a web dashboard
    View(viewer::ViewArgs),
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::View(args)) => {
            // Run the viewer (viewer has its own logging via ringlog)
            viewer::run(args.into());
            Ok(())
        }
        None => {
            // Run benchmark if config was provided
            if let Some(ref config) = cli.config {
                init_tracing();
                run_benchmark_cli(config, &cli)
            } else {
                // Show help
                Cli::parse_from(["crucible-benchmark", "--help"]);
                Ok(())
            }
        }
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();
}

fn run_benchmark_cli(config_path: &PathBuf, cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let mut config = Config::load(config_path)?;

    // Apply CLI overrides
    if let Some(ref parquet) = cli.parquet {
        config.admin.parquet = Some(parquet.clone());
    }

    // Parse CPU list if configured
    let cpu_ids = if let Some(ref cpu_list) = config.general.cpu_list {
        match parse_cpu_list(cpu_list) {
            Ok(ids) => Some(ids),
            Err(e) => {
                tracing::error!("invalid cpu_list '{}': {}", cpu_list, e);
                return Err(e.into());
            }
        }
    } else {
        None
    };

    // Validate io_engine selection (still need this for internal logic)
    let _effective_engine = validate_io_engine(config.general.io_engine);

    // Create the output formatter
    let formatter = create_formatter(config.admin.format, config.admin.color);

    // Print config using the formatter
    formatter.print_config(&config);

    // Set up signal handler for graceful shutdown
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting signal handler");

    run_benchmark(config, cpu_ids, formatter, running)?;

    Ok(())
}

fn run_benchmark(
    config: Config,
    cpu_ids: Option<Vec<usize>>,
    formatter: Box<dyn OutputFormatter>,
    running: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_threads = config.general.threads;
    let warmup = config.general.warmup;
    let duration = config.general.duration;
    let total_connections = config.connection.total_connections();

    // Shared state
    let shared = Arc::new(SharedState::new());

    // Create shared rate limiter
    // Priority: saturation_search.start_rate > rate_limit > unlimited (0)
    let initial_rate = if let Some(ref sat) = config.workload.saturation_search {
        sat.start_rate
    } else {
        config.workload.rate_limit.unwrap_or(0)
    };

    let ratelimiter = if initial_rate > 0 || config.workload.saturation_search.is_some() {
        Some(Arc::new(DynamicRateLimiter::new(initial_rate)))
    } else {
        None
    };

    // Start admin server if configured
    let _admin_handle = if config.admin.listen.is_some() || config.admin.parquet.is_some() {
        let admin = AdminServer::new(
            config.admin.listen,
            config.admin.parquet.clone(),
            config.admin.parquet_interval,
            Arc::clone(&shared),
        );
        Some(admin.run())
    } else {
        None
    };

    // Calculate prefill ranges for each worker
    let prefill_enabled = config.workload.prefill;
    let key_count = config.workload.keyspace.count;
    let prefill_ranges: Vec<Option<std::ops::Range<usize>>> = if prefill_enabled {
        let keys_per_worker = key_count / num_threads;
        let remainder = key_count % num_threads;
        (0..num_threads)
            .map(|id| {
                let start = if id < remainder {
                    id * (keys_per_worker + 1)
                } else {
                    remainder * (keys_per_worker + 1) + (id - remainder) * keys_per_worker
                };
                let count = if id < remainder {
                    keys_per_worker + 1
                } else {
                    keys_per_worker
                };
                Some(start..start + count)
            })
            .collect()
    } else {
        vec![None; num_threads]
    };

    // Print prefill phase indicator if enabled
    if prefill_enabled {
        formatter.print_prefill(key_count);
    } else {
        // Print warmup phase indicator
        formatter.print_warmup(warmup);
    }

    // Channel for worker completion notifications
    let (completion_tx, completion_rx) = mpsc::channel::<usize>();

    // Spawn worker threads
    let mut handles = Vec::with_capacity(num_threads);

    for id in 0..num_threads {
        let config = config.clone();
        let shared = Arc::clone(&shared);
        let ratelimiter = ratelimiter.clone();
        let cpu_ids = cpu_ids.clone();
        let completion_tx = completion_tx.clone();
        let prefill_range = prefill_ranges[id].clone();

        let handle = thread::Builder::new()
            .name(format!("worker-{}", id))
            .spawn(move || {
                // Pin to CPU if configured
                if let Some(ref ids) = cpu_ids
                    && !ids.is_empty()
                {
                    let cpu_id = ids[id % ids.len()];
                    if let Err(e) = pin_to_cpu(cpu_id) {
                        tracing::warn!("failed to pin worker {} to CPU {}: {}", id, cpu_id, e);
                    } else {
                        tracing::debug!("pinned worker {} to CPU {}", id, cpu_id);
                    }
                }
                run_worker(id, config, shared, ratelimiter, prefill_range);
                // Signal completion (ignore send errors - receiver may be gone)
                let _ = completion_tx.send(id);
            })?;

        handles.push(handle);
    }

    // Drop our copy of the sender so the channel closes when all workers finish
    drop(completion_tx);

    // Start in prefill or warmup phase
    if prefill_enabled {
        shared.set_phase(Phase::Prefill);
    } else {
        shared.set_phase(Phase::Warmup);
    }

    // Main thread: reporting loop
    let start = Instant::now();
    let report_interval = Duration::from_secs(1);
    let mut last_report = Instant::now();
    let mut last_responses = 0u64;
    let mut last_errors = 0u64;
    let mut last_conn_failures = 0u64;
    let mut last_hits = 0u64;
    let mut last_misses = 0u64;
    let mut last_histogram: Option<Histogram> = None;
    let mut current_phase = if prefill_enabled {
        Phase::Prefill
    } else {
        Phase::Warmup
    };

    let mut actual_duration = duration;

    // Saturation search state (initialized after warmup if configured)
    let mut saturation_state: Option<SaturationSearchState> = None;

    // Track when warmup actually starts (after prefill completes)
    let mut warmup_start: Option<Instant> = if prefill_enabled { None } else { Some(start) };

    loop {
        thread::sleep(Duration::from_millis(100));

        // Check for signal
        if !running.load(Ordering::SeqCst) {
            shared.set_phase(Phase::Stop);
            // Calculate actual running duration (exclude warmup and prefill)
            if let Some(ws) = warmup_start {
                let warmup_elapsed = ws.elapsed();
                if warmup_elapsed > warmup {
                    actual_duration = warmup_elapsed - warmup;
                } else {
                    actual_duration = Duration::ZERO;
                }
            } else {
                actual_duration = Duration::ZERO;
            }
            break;
        }

        // Handle prefill -> warmup transition
        if current_phase == Phase::Prefill {
            let prefill_complete = shared.prefill_complete_count();
            if prefill_complete >= num_threads {
                shared.set_phase(Phase::Warmup);
                current_phase = Phase::Warmup;
                warmup_start = Some(Instant::now());
                formatter.print_warmup(warmup);
            }
            continue;
        }

        // Calculate elapsed time since warmup started
        let warmup_start_time = warmup_start.unwrap_or(start);
        let elapsed = warmup_start_time.elapsed();

        // Check if we're done
        if elapsed >= warmup + duration {
            shared.set_phase(Phase::Stop);
            break;
        }

        // Transition from warmup to running
        if current_phase == Phase::Warmup && elapsed >= warmup {
            shared.set_phase(Phase::Running);
            current_phase = Phase::Running;
            formatter.print_running(duration);
            formatter.print_header();
            last_report = Instant::now();
            // Initialize baseline for delta calculations
            last_responses = metrics::RESPONSES_RECEIVED.value();
            last_errors = metrics::REQUEST_ERRORS.value();
            last_conn_failures = metrics::CONNECTIONS_FAILED.value();
            last_hits = metrics::CACHE_HITS.value();
            last_misses = metrics::CACHE_MISSES.value();
            last_histogram = metrics::RESPONSE_LATENCY.load();

            // Initialize saturation search if configured
            if let Some(ref sat_config) = config.workload.saturation_search
                && let Some(ref rl) = ratelimiter
            {
                saturation_state = Some(SaturationSearchState::new(
                    sat_config.clone(),
                    Arc::clone(rl),
                ));
            }
        }

        // Skip reporting during warmup
        if current_phase != Phase::Running {
            continue;
        }

        // Periodic reporting
        if last_report.elapsed() >= report_interval {
            let responses = metrics::RESPONSES_RECEIVED.value();
            let errors = metrics::REQUEST_ERRORS.value();
            let conn_failures = metrics::CONNECTIONS_FAILED.value();
            let hits = metrics::CACHE_HITS.value();
            let misses = metrics::CACHE_MISSES.value();

            let elapsed_secs = last_report.elapsed().as_secs_f64();

            // Calculate rates
            let delta_responses = responses - last_responses;
            let rate = delta_responses as f64 / elapsed_secs;
            last_responses = responses;

            // Calculate error rate (request errors + connection failures)
            let delta_errors = errors - last_errors;
            let delta_conn_failures = conn_failures - last_conn_failures;
            let err_rate = (delta_errors + delta_conn_failures) as f64 / elapsed_secs;
            last_errors = errors;
            last_conn_failures = conn_failures;

            // Calculate hit rate for this interval
            let delta_hits = hits - last_hits;
            let delta_misses = misses - last_misses;
            let delta_gets = delta_hits + delta_misses;
            let hit_pct = if delta_gets > 0 {
                (delta_hits as f64 / delta_gets as f64) * 100.0
            } else {
                0.0
            };
            last_hits = hits;
            last_misses = misses;

            // Get percentiles from delta histogram (this interval only)
            let current_histogram = metrics::RESPONSE_LATENCY.load();
            let (p50, p90, p99, p999, p9999, max) = match (&current_histogram, &last_histogram) {
                (Some(current), Some(previous)) => {
                    if let Ok(delta) = current.wrapping_sub(previous) {
                        (
                            percentile_from_histogram(&delta, 50.0) / 1000.0,
                            percentile_from_histogram(&delta, 90.0) / 1000.0,
                            percentile_from_histogram(&delta, 99.0) / 1000.0,
                            percentile_from_histogram(&delta, 99.9) / 1000.0,
                            percentile_from_histogram(&delta, 99.99) / 1000.0,
                            max_from_histogram(&delta) / 1000.0,
                        )
                    } else {
                        (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
                    }
                }
                (Some(current), None) => (
                    percentile_from_histogram(current, 50.0) / 1000.0,
                    percentile_from_histogram(current, 90.0) / 1000.0,
                    percentile_from_histogram(current, 99.0) / 1000.0,
                    percentile_from_histogram(current, 99.9) / 1000.0,
                    percentile_from_histogram(current, 99.99) / 1000.0,
                    max_from_histogram(current) / 1000.0,
                ),
                _ => (0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            };
            last_histogram = current_histogram;

            let sample = Sample {
                timestamp: Utc::now(),
                req_per_sec: rate,
                err_per_sec: err_rate,
                hit_pct,
                p50_us: p50,
                p90_us: p90,
                p99_us: p99,
                p999_us: p999,
                p9999_us: p9999,
                max_us: max,
            };

            formatter.print_sample(&sample);

            // Check saturation search progress
            if let Some(ref mut state) = saturation_state {
                state.check_and_advance(&*formatter);
            }

            last_report = Instant::now();
        }
    }

    // Wait for workers to finish with timeout
    let shutdown_start = Instant::now();
    let mut completed = 0;

    while completed < num_threads {
        let remaining = WORKER_SHUTDOWN_TIMEOUT.saturating_sub(shutdown_start.elapsed());
        if remaining.is_zero() {
            tracing::warn!(
                "shutdown timeout: {} of {} workers did not finish within {:?}",
                num_threads - completed,
                num_threads,
                WORKER_SHUTDOWN_TIMEOUT
            );
            break;
        }

        match completion_rx.recv_timeout(remaining) {
            Ok(id) => {
                tracing::debug!("worker {} finished", id);
                completed += 1;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                tracing::warn!(
                    "shutdown timeout: {} of {} workers did not finish within {:?}",
                    num_threads - completed,
                    num_threads,
                    WORKER_SHUTDOWN_TIMEOUT
                );
                break;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // All senders dropped, all workers finished
                break;
            }
        }
    }

    // Join the handles (should be immediate for completed workers)
    for handle in handles {
        let _ = handle.join();
    }

    // Final report - collect all metrics
    let requests = metrics::REQUESTS_SENT.value();
    let responses = metrics::RESPONSES_RECEIVED.value();
    let errors = metrics::REQUEST_ERRORS.value();
    let hits = metrics::CACHE_HITS.value();
    let misses = metrics::CACHE_MISSES.value();
    let bytes_tx = metrics::BYTES_TX.value();
    let bytes_rx = metrics::BYTES_RX.value();
    let get_count = metrics::GET_COUNT.value();
    let set_count = metrics::SET_COUNT.value();
    let active = metrics::CONNECTIONS_ACTIVE.value();
    let failed = metrics::CONNECTIONS_FAILED.value();
    let elapsed_secs = actual_duration.as_secs_f64();

    // Get latencies for GET operations
    let get_latencies = LatencyStats {
        p50_us: percentile(&metrics::GET_LATENCY, 50.0) / 1000.0,
        p90_us: percentile(&metrics::GET_LATENCY, 90.0) / 1000.0,
        p99_us: percentile(&metrics::GET_LATENCY, 99.0) / 1000.0,
        p999_us: percentile(&metrics::GET_LATENCY, 99.9) / 1000.0,
        p9999_us: percentile(&metrics::GET_LATENCY, 99.99) / 1000.0,
        max_us: max_percentile(&metrics::GET_LATENCY) / 1000.0,
    };

    // Get latencies for SET operations
    let set_latencies = LatencyStats {
        p50_us: percentile(&metrics::SET_LATENCY, 50.0) / 1000.0,
        p90_us: percentile(&metrics::SET_LATENCY, 90.0) / 1000.0,
        p99_us: percentile(&metrics::SET_LATENCY, 99.0) / 1000.0,
        p999_us: percentile(&metrics::SET_LATENCY, 99.9) / 1000.0,
        p9999_us: percentile(&metrics::SET_LATENCY, 99.99) / 1000.0,
        max_us: max_percentile(&metrics::SET_LATENCY) / 1000.0,
    };

    let results = Results {
        duration_secs: elapsed_secs,
        requests,
        responses,
        errors,
        hits,
        misses,
        bytes_tx,
        bytes_rx,
        get_count,
        set_count,
        get_latencies,
        set_latencies,
        conns_active: active,
        conns_failed: failed,
        conns_total: total_connections as u64,
    };

    formatter.print_results(&results);

    // Print saturation search results if configured
    if let Some(state) = saturation_state {
        formatter.print_saturation_results(&state.results());
    }

    drop(_admin_handle);

    Ok(())
}

fn run_worker(
    id: usize,
    config: Config,
    shared: Arc<SharedState>,
    ratelimiter: Option<Arc<DynamicRateLimiter>>,
    prefill_range: Option<std::ops::Range<usize>>,
) {
    // Set this thread's shard ID for metrics to avoid false sharing
    metrics::set_thread_shard(id);

    let worker_config = IoWorkerConfig {
        id,
        config,
        shared: Arc::clone(&shared),
        ratelimiter,
        warmup: true,
        prefill_range,
    };

    let mut worker = match IoWorker::new(worker_config) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!("worker {} failed to initialize: {}", id, e);
            return;
        }
    };

    if let Err(e) = worker.connect() {
        tracing::error!("worker {} failed to connect: {}", id, e);
        return;
    }

    tracing::debug!(
        "worker {} started with {} connections",
        id,
        worker.active_connections()
    );

    let mut last_phase = Phase::Connect;

    loop {
        let phase = shared.phase();

        if phase.should_stop() {
            break;
        }

        // Update warmup state on phase transition
        if phase != last_phase {
            worker.set_warmup(!phase.is_recording());
            last_phase = phase;
        }

        // Handle prefill phase
        if phase == Phase::Prefill {
            if let Err(e) = worker.poll_once_prefill() {
                tracing::debug!("worker {} prefill error: {}", id, e);
            }
            continue;
        }

        if let Err(e) = worker.poll_once() {
            tracing::debug!("worker {} poll error: {}", id, e);
        }
    }

    tracing::debug!("worker {} exiting", id);
}

/// Get a percentile from the atomic histogram (cumulative).
fn percentile(hist: &AtomicHistogram, p: f64) -> f64 {
    if let Some(snapshot) = hist.load() {
        percentile_from_histogram(&snapshot, p)
    } else {
        0.0
    }
}

/// Get a percentile from a histogram snapshot.
fn percentile_from_histogram(hist: &Histogram, p: f64) -> f64 {
    if let Ok(Some(results)) = hist.percentiles(&[p])
        && let Some((_pct, bucket)) = results.first()
    {
        return bucket.end() as f64;
    }
    0.0
}

/// Get the max value from an atomic histogram.
fn max_percentile(hist: &AtomicHistogram) -> f64 {
    if let Some(snapshot) = hist.load() {
        max_from_histogram(&snapshot)
    } else {
        0.0
    }
}

/// Get the max value from a histogram snapshot.
fn max_from_histogram(hist: &Histogram) -> f64 {
    // Use 100th percentile as max
    if let Ok(Some(results)) = hist.percentiles(&[100.0])
        && let Some((_pct, bucket)) = results.first()
    {
        return bucket.end() as f64;
    }
    0.0
}

/// Pin the current thread to a specific CPU core.
#[cfg(target_os = "linux")]
fn pin_to_cpu(cpu_id: usize) -> std::io::Result<()> {
    use std::io;
    use std::mem;

    unsafe {
        let mut cpuset: libc::cpu_set_t = mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(cpu_id, &mut cpuset);

        let result = libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &cpuset);

        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

/// Pin the current thread to a specific CPU core (no-op on non-Linux).
#[cfg(not(target_os = "linux"))]
fn pin_to_cpu(_cpu_id: usize) -> std::io::Result<()> {
    // CPU pinning is only supported on Linux
    Ok(())
}

/// Validate and resolve the effective I/O engine.
/// Returns the engine that will actually be used at runtime.
fn validate_io_engine(engine: IoEngine) -> IoEngine {
    match engine {
        IoEngine::Auto => {
            if io_driver::uring_available() {
                IoEngine::Uring
            } else {
                IoEngine::Mio
            }
        }
        IoEngine::Uring => {
            if io_driver::uring_available() {
                IoEngine::Uring
            } else {
                #[cfg(target_os = "linux")]
                tracing::warn!(
                    "io_uring requested but not available on this kernel, falling back to mio"
                );
                #[cfg(not(target_os = "linux"))]
                tracing::warn!("io_uring is only available on Linux, falling back to mio");
                IoEngine::Mio
            }
        }
        IoEngine::Mio => IoEngine::Mio,
    }
}
