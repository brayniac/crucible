use benchmark::config::Config;
use benchmark::worker::Phase;
use benchmark::{AdminServer, IoWorker, IoWorkerConfig, SharedState, parse_cpu_list};

use io_driver::{IoEngine, RecvMode};

use clap::Parser;
use metriken::{AtomicHistogram, histogram::Histogram, metric};
use ratelimit::Ratelimiter;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
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

    /// Recv mode for io_uring (multishot, singleshot)
    #[arg(long, value_parser = parse_recv_mode)]
    recv_mode: Option<RecvMode>,
}

fn parse_io_engine(s: &str) -> Result<IoEngine, String> {
    s.parse()
}

fn parse_recv_mode(s: &str) -> Result<RecvMode, String> {
    match s.to_lowercase().as_str() {
        "multishot" | "multi" => Ok(RecvMode::Multishot),
        "singleshot" | "single" | "single-shot" => Ok(RecvMode::SingleShot),
        _ => Err(format!(
            "invalid recv_mode '{}', expected: multishot, singleshot",
            s
        )),
    }
}

// Metrics - using static histograms that workers can reference
#[metric(
    name = "response_latency",
    description = "Response latency histogram (nanoseconds)"
)]
static RESPONSE_LATENCY: AtomicHistogram = AtomicHistogram::new(7, 64);

#[metric(
    name = "get_latency",
    description = "GET response latency histogram (nanoseconds)"
)]
static GET_LATENCY: AtomicHistogram = AtomicHistogram::new(7, 64);

#[metric(
    name = "set_latency",
    description = "SET response latency histogram (nanoseconds)"
)]
static SET_LATENCY: AtomicHistogram = AtomicHistogram::new(7, 64);

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

    let total_connections = config.connection.total_connections();

    // Validate io_engine selection
    let effective_engine = validate_io_engine(config.general.io_engine);

    tracing::info!("starting brrr");
    tracing::info!("endpoints: {:?}", config.target.endpoints);
    tracing::info!("protocol: {:?}", config.target.protocol);
    tracing::info!("threads: {}", config.general.threads);
    tracing::info!("connections: {} (total)", total_connections);
    tracing::info!("pipeline_depth: {}", config.connection.pipeline_depth);
    tracing::info!(
        "io_engine: {} (configured: {})",
        effective_engine,
        config.general.io_engine
    );
    tracing::info!("recv_mode: {}", config.general.recv_mode);
    tracing::info!("timestamp_mode: {:?}", config.timestamps.mode);
    if let Some(ref cpu_list) = config.general.cpu_list {
        tracing::info!("cpu_list: {}", cpu_list);
        #[cfg(not(target_os = "linux"))]
        tracing::warn!("cpu pinning is only supported on Linux");
    }
    if config.admin.listen.is_some() {
        tracing::info!("prometheus: {:?}", config.admin.listen);
    }
    if config.admin.parquet.is_some() {
        tracing::info!("parquet: {:?}", config.admin.parquet);
    }

    run_benchmark(config, cpu_ids)?;

    Ok(())
}

fn run_benchmark(
    config: Config,
    cpu_ids: Option<Vec<usize>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_threads = config.general.threads;
    let warmup = config.general.warmup;
    let duration = config.general.duration;

    // Shared state
    let shared = Arc::new(SharedState::new(num_threads));

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

    tracing::info!(
        "spawned {} worker threads, warming up for {}s...",
        num_threads,
        warmup.as_secs(),
    );

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
    let mut last_failed = 0u64;
    let mut last_eof = 0u64;
    let mut last_recv_err = 0u64;
    let mut last_send_err = 0u64;
    let mut last_closed = 0u64;
    let mut last_error_evt = 0u64;
    let mut last_connect_failed = 0u64;
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
            tracing::info!("warmup complete, running for {}s", duration.as_secs());
            last_report = Instant::now();
            // Initialize baseline for delta calculations
            last_responses = shared.responses_received.load(Ordering::Relaxed);
            last_errors = shared.errors.load(Ordering::Relaxed);
            last_hits = shared.hits.load(Ordering::Relaxed);
            last_misses = shared.misses.load(Ordering::Relaxed);
            last_histogram = RESPONSE_LATENCY.load();
        }

        // Skip reporting during warmup
        if current_phase != Phase::Running {
            continue;
        }

        // Periodic reporting
        if last_report.elapsed() >= report_interval {
            let responses = shared.responses_received.load(Ordering::Relaxed);
            let errors = shared.errors.load(Ordering::Relaxed);
            let hits = shared.hits.load(Ordering::Relaxed);
            let misses = shared.misses.load(Ordering::Relaxed);
            let active = shared.connections_active.load(Ordering::Relaxed);
            let failed = shared.connections_failed.load(Ordering::Relaxed);

            let elapsed_secs = last_report.elapsed().as_secs_f64();

            // Calculate rates
            let delta_responses = responses - last_responses;
            let rate = delta_responses as f64 / elapsed_secs;
            last_responses = responses;

            let delta_errors = errors - last_errors;
            let error_rate = delta_errors as f64 / elapsed_secs;
            last_errors = errors;

            // Calculate hit rate for this interval
            let delta_hits = hits - last_hits;
            let delta_misses = misses - last_misses;
            let delta_gets = delta_hits + delta_misses;
            let hit_rate = if delta_gets > 0 {
                (delta_hits as f64 / delta_gets as f64) * 100.0
            } else {
                0.0
            };
            last_hits = hits;
            last_misses = misses;

            // Get percentiles from delta histogram (this interval only)
            let current_histogram = RESPONSE_LATENCY.load();
            let (p50, p99, p999) = match (&current_histogram, &last_histogram) {
                (Some(current), Some(previous)) => {
                    if let Ok(delta) = current.wrapping_sub(previous) {
                        (
                            percentile_from_histogram(&delta, 50.0),
                            percentile_from_histogram(&delta, 99.0),
                            percentile_from_histogram(&delta, 99.9),
                        )
                    } else {
                        (0.0, 0.0, 0.0)
                    }
                }
                (Some(current), None) => (
                    percentile_from_histogram(current, 50.0),
                    percentile_from_histogram(current, 99.0),
                    percentile_from_histogram(current, 99.9),
                ),
                _ => (0.0, 0.0, 0.0),
            };
            last_histogram = current_histogram;

            tracing::info!(
                "rate={:.0}/s errors={:.0}/s hit_rate={:.1}% conns={}/{} p50={:.2}us p99={:.2}us p999={:.2}us",
                rate,
                error_rate,
                hit_rate,
                active,
                active + failed,
                p50 / 1000.0,
                p99 / 1000.0,
                p999 / 1000.0,
            );

            // Report disconnect reasons when connections have failed
            let delta_failed = failed - last_failed;
            if delta_failed > 0 {
                let eof = shared.disconnects_eof.load(Ordering::Relaxed);
                let recv_err = shared.disconnects_recv_error.load(Ordering::Relaxed);
                let send_err = shared.disconnects_send_error.load(Ordering::Relaxed);
                let closed = shared.disconnects_closed_event.load(Ordering::Relaxed);
                let error_evt = shared.disconnects_error_event.load(Ordering::Relaxed);
                let connect_failed = shared.disconnects_connect_failed.load(Ordering::Relaxed);

                let d_eof = eof - last_eof;
                let d_recv = recv_err - last_recv_err;
                let d_send = send_err - last_send_err;
                let d_closed = closed - last_closed;
                let d_error = error_evt - last_error_evt;
                let d_conn = connect_failed - last_connect_failed;

                tracing::info!(
                    "disconnects: {} (eof={} recv_err={} send_err={} closed={} error={} connect_failed={})",
                    delta_failed,
                    d_eof,
                    d_recv,
                    d_send,
                    d_closed,
                    d_error,
                    d_conn,
                );

                last_eof = eof;
                last_recv_err = recv_err;
                last_send_err = send_err;
                last_closed = closed;
                last_error_evt = error_evt;
                last_connect_failed = connect_failed;
            }
            last_failed = failed;

            // Print per-worker stats for diagnostics
            if std::env::var("CRUCIBLE_DIAGNOSTICS")
                .map(|v| v == "1")
                .unwrap_or(false)
            {
                let worker_rates: Vec<u64> = shared
                    .worker_stats
                    .iter()
                    .map(|s| s.requests_sent.load(Ordering::Relaxed))
                    .collect();
                tracing::info!("per-worker requests: {:?}", worker_rates);
            }

            last_report = Instant::now();
        }
    }

    // Wait for workers to finish
    for handle in handles {
        let _ = handle.join();
    }

    // Final report
    let requests = shared.requests_sent.load(Ordering::Relaxed);
    let responses = shared.responses_received.load(Ordering::Relaxed);
    let errors = shared.errors.load(Ordering::Relaxed);
    let hits = shared.hits.load(Ordering::Relaxed);
    let misses = shared.misses.load(Ordering::Relaxed);
    let elapsed_secs = duration.as_secs_f64();

    let total_gets = hits + misses;
    let hit_rate = if total_gets > 0 {
        (hits as f64 / total_gets as f64) * 100.0
    } else {
        0.0
    };

    let p50 = percentile(&RESPONSE_LATENCY, 50.0);
    let p99 = percentile(&RESPONSE_LATENCY, 99.0);
    let p999 = percentile(&RESPONSE_LATENCY, 99.9);
    let p9999 = percentile(&RESPONSE_LATENCY, 99.99);

    tracing::info!("=== Final Results ===");
    tracing::info!("total_requests: {}", requests);
    tracing::info!("total_responses: {}", responses);
    tracing::info!("total_errors: {}", errors);
    tracing::info!(
        "hit_rate: {:.2}% ({} hits, {} misses)",
        hit_rate,
        hits,
        misses
    );
    tracing::info!("throughput: {:.2} req/s", responses as f64 / elapsed_secs);
    tracing::info!("latency p50: {:.2} us", p50 / 1000.0);
    tracing::info!("latency p99: {:.2} us", p99 / 1000.0);
    tracing::info!("latency p99.9: {:.2} us", p999 / 1000.0);
    tracing::info!("latency p99.99: {:.2} us", p9999 / 1000.0);

    // Report final disconnect reason breakdown
    let total_failed = shared.connections_failed.load(Ordering::Relaxed);
    if total_failed > 0 {
        tracing::info!(
            "disconnects: {} (eof={} recv_err={} send_err={} closed={} error={} connect_failed={})",
            total_failed,
            shared.disconnects_eof.load(Ordering::Relaxed),
            shared.disconnects_recv_error.load(Ordering::Relaxed),
            shared.disconnects_send_error.load(Ordering::Relaxed),
            shared.disconnects_closed_event.load(Ordering::Relaxed),
            shared.disconnects_error_event.load(Ordering::Relaxed),
            shared.disconnects_connect_failed.load(Ordering::Relaxed),
        );
    }

    drop(_admin_handle);

    Ok(())
}

fn run_worker(
    id: usize,
    config: Config,
    shared: Arc<SharedState>,
    ratelimiter: Option<Arc<Ratelimiter>>,
) {
    let worker_config = IoWorkerConfig {
        id,
        config,
        shared: Arc::clone(&shared),
        ratelimiter,
        response_latency: &RESPONSE_LATENCY,
        get_latency: &GET_LATENCY,
        set_latency: &SET_LATENCY,
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
