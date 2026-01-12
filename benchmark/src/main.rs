use benchmark::config::Config;
use benchmark::metrics;
use benchmark::worker::Phase;
use benchmark::{
    AdminServer, ColorMode, IoWorker, IoWorkerConfig, LatencyStats, OutputFormat, OutputFormatter,
    Results, Sample, SharedState, create_formatter, parse_cpu_list,
};

use chrono::Utc;
use io_driver::{IoEngine, RecvMode};

use clap::Parser;
use metriken::{AtomicHistogram, histogram::Histogram};
use ratelimit::Ratelimiter;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "crucible-benchmark")]
#[command(about = "High-performance cache protocol benchmark tool")]
#[command(version)]
struct Cli {
    /// Path to configuration file
    config: PathBuf,

    /// Override number of threads
    #[arg(short, long)]
    threads: Option<usize>,

    /// Override target rate limit (requests/second)
    #[arg(short, long)]
    rate: Option<u64>,

    /// Override total connections
    #[arg(long)]
    connections: Option<usize>,

    /// Listen address for Prometheus metrics (e.g., 127.0.0.1:9090)
    #[arg(long)]
    prometheus: Option<SocketAddr>,

    /// Path to write Parquet output file
    #[arg(long)]
    parquet: Option<PathBuf>,

    /// CPU list for worker thread pinning (e.g., "0-3,8-11,13")
    #[arg(long)]
    cpu_list: Option<String>,

    /// I/O engine selection (auto, mio, uring)
    #[arg(long, value_parser = parse_io_engine)]
    io_engine: Option<IoEngine>,

    /// Recv mode for io_uring (multishot/multi-shot, singleshot/single-shot)
    #[arg(long, value_parser = parse_recv_mode)]
    recv_mode: Option<RecvMode>,

    /// Output format (clean, json, verbose, quiet)
    #[arg(long, value_parser = parse_output_format)]
    format: Option<OutputFormat>,

    /// Color mode (auto, always, never)
    #[arg(long, value_parser = parse_color_mode)]
    color: Option<ColorMode>,
}

fn parse_io_engine(s: &str) -> Result<IoEngine, String> {
    s.parse()
}

fn parse_recv_mode(s: &str) -> Result<RecvMode, String> {
    match s.to_lowercase().as_str() {
        "multishot" | "multi-shot" | "multi" => Ok(RecvMode::Multishot),
        "singleshot" | "single-shot" | "single" => Ok(RecvMode::SingleShot),
        _ => Err(format!(
            "invalid recv_mode '{}', expected: multishot, multi-shot, singleshot, single-shot",
            s
        )),
    }
}

fn parse_output_format(s: &str) -> Result<OutputFormat, String> {
    s.parse()
}

fn parse_color_mode(s: &str) -> Result<ColorMode, String> {
    s.parse()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();

    // Load configuration
    let mut config = Config::load(&cli.config)?;

    // Apply CLI overrides
    if let Some(threads) = cli.threads {
        config.general.threads = threads;
    }
    if let Some(rate) = cli.rate {
        config.workload.rate_limit = Some(rate);
    }
    if let Some(connections) = cli.connections {
        config.connection.connections = connections;
    }
    if let Some(ref prometheus) = cli.prometheus {
        config.admin.listen = Some(*prometheus);
    }
    if let Some(ref parquet) = cli.parquet {
        config.admin.parquet = Some(parquet.clone());
    }
    if let Some(ref cpu_list) = cli.cpu_list {
        config.general.cpu_list = Some(cpu_list.clone());
    }
    if let Some(io_engine) = cli.io_engine {
        config.general.io_engine = io_engine;
    }
    if let Some(recv_mode) = cli.recv_mode {
        config.general.recv_mode = recv_mode;
    }
    if let Some(format) = cli.format {
        config.admin.format = format;
    }
    if let Some(color) = cli.color {
        config.admin.color = color;
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

    run_benchmark(config, cpu_ids, formatter)?;

    Ok(())
}

fn run_benchmark(
    config: Config,
    cpu_ids: Option<Vec<usize>>,
    formatter: Box<dyn OutputFormatter>,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_threads = config.general.threads;
    let warmup = config.general.warmup;
    let duration = config.general.duration;
    let total_connections = config.connection.total_connections();

    // Shared state
    let shared = Arc::new(SharedState::new());

    // Create shared rate limiter if configured
    let ratelimiter = config.workload.rate_limit.map(|rate| {
        Arc::new(
            Ratelimiter::builder(rate, Duration::from_secs(1))
                .max_tokens(rate)
                .build()
                .expect("failed to create rate limiter"),
        )
    });

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

    // Print warmup phase indicator
    formatter.print_warmup(warmup);

    // Spawn worker threads
    let mut handles = Vec::with_capacity(num_threads);

    for id in 0..num_threads {
        let config = config.clone();
        let shared = Arc::clone(&shared);
        let ratelimiter = ratelimiter.clone();
        let cpu_ids = cpu_ids.clone();

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
                run_worker(id, config, shared, ratelimiter);
            })?;

        handles.push(handle);
    }

    // Start in warmup phase
    shared.set_phase(Phase::Warmup);

    // Main thread: reporting loop
    let start = Instant::now();
    let report_interval = Duration::from_secs(1);
    let mut last_report = Instant::now();
    let mut last_responses = 0u64;
    let mut last_errors = 0u64;
    let mut last_hits = 0u64;
    let mut last_misses = 0u64;
    let mut last_histogram: Option<Histogram> = None;
    let mut current_phase = Phase::Warmup;

    loop {
        thread::sleep(Duration::from_millis(100));

        let elapsed = start.elapsed();

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
            last_hits = metrics::CACHE_HITS.value();
            last_misses = metrics::CACHE_MISSES.value();
            last_histogram = metrics::RESPONSE_LATENCY.load();
        }

        // Skip reporting during warmup
        if current_phase != Phase::Running {
            continue;
        }

        // Periodic reporting
        if last_report.elapsed() >= report_interval {
            let responses = metrics::RESPONSES_RECEIVED.value();
            let errors = metrics::REQUEST_ERRORS.value();
            let hits = metrics::CACHE_HITS.value();
            let misses = metrics::CACHE_MISSES.value();
            let active = metrics::CONNECTIONS_ACTIVE.value();

            let elapsed_secs = last_report.elapsed().as_secs_f64();

            // Calculate rates
            let delta_responses = responses - last_responses;
            let rate = delta_responses as f64 / elapsed_secs;
            last_responses = responses;

            let delta_errors = errors - last_errors;
            last_errors = errors;

            // Calculate error percentage for this interval
            let err_pct = if delta_responses > 0 {
                (delta_errors as f64 / delta_responses as f64) * 100.0
            } else {
                0.0
            };

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

            // Calculate connection health percentage
            let conn_pct = if total_connections > 0 {
                (active as f64 / total_connections as f64) * 100.0
            } else {
                100.0
            };

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
                err_pct,
                hit_pct,
                conn_pct,
                p50_us: p50,
                p90_us: p90,
                p99_us: p99,
                p999_us: p999,
                p9999_us: p9999,
                max_us: max,
            };

            formatter.print_sample(&sample);

            last_report = Instant::now();
        }
    }

    // Wait for workers to finish
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
    let elapsed_secs = duration.as_secs_f64();

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

    drop(_admin_handle);

    Ok(())
}

fn run_worker(
    id: usize,
    config: Config,
    shared: Arc<SharedState>,
    ratelimiter: Option<Arc<Ratelimiter>>,
) {
    // Set this thread's shard ID for metrics to avoid false sharing
    metrics::set_thread_shard(id);

    let worker_config = IoWorkerConfig {
        id,
        config,
        shared: Arc::clone(&shared),
        ratelimiter,
        warmup: true,
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
