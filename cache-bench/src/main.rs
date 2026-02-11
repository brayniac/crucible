//! In-process cache benchmark — exercises the Cache trait directly
//! without network, protocol, or I/O driver overhead.

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod config;
mod metrics;
mod ratelimit;
mod worker;

use crate::config::{CacheBackend, Config, DiskSyncMode, EvictionPolicy};
use crate::ratelimit::DynamicRateLimiter;
use crate::worker::{Phase, SharedState};

use cache_core::Cache;
use clap::Parser;
use metriken::{AtomicHistogram, histogram::Histogram};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(name = "crucible-cache-bench")]
#[command(about = "In-process cache benchmark")]
struct Args {
    /// Path to configuration file
    config: PathBuf,
}

fn main() {
    let args = Args::parse();

    let config = match Config::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load config: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = run(config) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    print_config(&config);

    match config.cache.backend {
        CacheBackend::Segment => {
            let cache = create_segment(&config)?;
            run_with_cache(config, Arc::new(cache))
        }
        CacheBackend::Slab => {
            let cache = create_slab(&config)?;
            run_with_cache(config, Arc::new(cache))
        }
        CacheBackend::Heap => {
            let cache = create_heap(&config)?;
            run_with_cache(config, Arc::new(cache))
        }
    }
}

fn run_with_cache<C: Cache>(
    config: Config,
    cache: Arc<C>,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_threads = config.general.threads;
    let warmup = config.general.warmup;
    let duration = config.general.duration;

    // Parse CPU list
    let cpu_ids = if let Some(ref cpu_list) = config.general.cpu_list {
        Some(config::parse_cpu_list(cpu_list).map_err(|e| format!("invalid cpu_list: {e}"))?)
    } else {
        None
    };

    // Shared state
    let shared = Arc::new(SharedState::new());

    // Rate limiter
    let ratelimiter = if config.workload.rate_limit > 0 {
        Some(Arc::new(DynamicRateLimiter::new(
            config.workload.rate_limit,
        )))
    } else {
        None
    };

    // Calculate prefill ranges
    let prefill_ranges: Vec<Option<std::ops::Range<usize>>> = if config.workload.prefill {
        let key_count = config.workload.keyspace.count;
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

    // Signal handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("failed to set signal handler");

    // Set initial phase
    if config.workload.prefill {
        eprintln!(
            "prefilling {} keys across {} threads...",
            config.workload.keyspace.count, num_threads
        );
        shared.set_phase(Phase::Prefill);
    } else {
        shared.set_phase(Phase::Warmup);
    }

    // Spawn workers
    // We need config to be shared across threads. Put it in an Arc.
    let config = Arc::new(config);
    let mut handles = Vec::with_capacity(num_threads);

    for id in 0..num_threads {
        let cache = Arc::clone(&cache);
        let shared = Arc::clone(&shared);
        let ratelimiter = ratelimiter.clone();
        let cpu_ids = cpu_ids.clone();
        let prefill_range = prefill_ranges[id].clone();
        let config = Arc::clone(&config);

        let handle = thread::Builder::new()
            .name(format!("worker-{id}"))
            .spawn(move || {
                // Pin to CPU if configured
                if let Some(ref ids) = cpu_ids
                    && !ids.is_empty()
                {
                    let cpu_id = ids[id % ids.len()];
                    let _ = pin_to_cpu(cpu_id);
                }
                worker::run_worker(
                    id,
                    &config,
                    &cache,
                    &shared,
                    ratelimiter.as_deref(),
                    prefill_range,
                );
            })?;

        handles.push(handle);
    }

    // Main thread: reporting loop
    let start = Instant::now();
    let report_interval = Duration::from_secs(1);
    let mut last_report = Instant::now();
    let mut last_completed = 0u64;
    let mut last_hits = 0u64;
    let mut last_misses = 0u64;
    let mut last_histogram: Option<Histogram> = None;
    let mut current_phase = if config.workload.prefill {
        Phase::Prefill
    } else {
        Phase::Warmup
    };
    let mut warmup_start: Option<Instant> = if config.workload.prefill {
        None
    } else {
        Some(start)
    };

    loop {
        thread::sleep(Duration::from_millis(100));

        // Check signal
        if !running.load(Ordering::SeqCst) {
            shared.set_phase(Phase::Stop);
            break;
        }

        // Handle prefill -> warmup transition
        if current_phase == Phase::Prefill {
            let done = shared.prefill_complete_count();
            if done >= num_threads {
                shared.set_phase(Phase::Warmup);
                current_phase = Phase::Warmup;
                warmup_start = Some(Instant::now());
                eprintln!("prefill complete, warming up for {:?}...", warmup);
            }
            continue;
        }

        let warmup_start_time = warmup_start.unwrap_or(start);
        let elapsed = warmup_start_time.elapsed();

        // Check if done
        if elapsed >= warmup + duration {
            shared.set_phase(Phase::Stop);
            break;
        }

        // Transition from warmup to running
        if current_phase == Phase::Warmup && elapsed >= warmup {
            shared.set_phase(Phase::Running);
            current_phase = Phase::Running;
            eprintln!("running for {:?}...", duration);
            print_header();
            last_report = Instant::now();
            last_completed = metrics::COMPLETED_COUNT.value();
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
            let completed = metrics::COMPLETED_COUNT.value();
            let hits = metrics::CACHE_HITS.value();
            let misses = metrics::CACHE_MISSES.value();

            let elapsed_secs = last_report.elapsed().as_secs_f64();

            let delta_completed = completed - last_completed;
            let rate = delta_completed as f64 / elapsed_secs;
            last_completed = completed;

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

            // Interval percentiles via wrapping_sub
            let current_histogram = metrics::RESPONSE_LATENCY.load();
            let (p50, p90, p99, p999, max) = match (&current_histogram, &last_histogram) {
                (Some(current), Some(previous)) => {
                    if let Ok(delta) = current.wrapping_sub(previous) {
                        (
                            percentile_from_histogram(&delta, 50.0) / 1000.0,
                            percentile_from_histogram(&delta, 90.0) / 1000.0,
                            percentile_from_histogram(&delta, 99.0) / 1000.0,
                            percentile_from_histogram(&delta, 99.9) / 1000.0,
                            max_from_histogram(&delta) / 1000.0,
                        )
                    } else {
                        (0.0, 0.0, 0.0, 0.0, 0.0)
                    }
                }
                (Some(current), None) => (
                    percentile_from_histogram(current, 50.0) / 1000.0,
                    percentile_from_histogram(current, 90.0) / 1000.0,
                    percentile_from_histogram(current, 99.0) / 1000.0,
                    percentile_from_histogram(current, 99.9) / 1000.0,
                    max_from_histogram(current) / 1000.0,
                ),
                _ => (0.0, 0.0, 0.0, 0.0, 0.0),
            };
            last_histogram = current_histogram;

            println!(
                "{:>12.0} {:>7.1}% {:>10.1} {:>10.1} {:>10.1} {:>10.1} {:>10.1}",
                rate, hit_pct, p50, p90, p99, p999, max,
            );

            last_report = Instant::now();
        }
    }

    // Wait for workers
    for handle in handles {
        let _ = handle.join();
    }

    // Final summary
    print_summary(&config);

    Ok(())
}

fn print_config(config: &Config) {
    eprintln!("cache-bench configuration:");
    eprintln!("  backend:    {}", config.cache.backend);
    eprintln!("  policy:     {}", config.cache.policy);
    eprintln!("  heap_size:  {} bytes", config.cache.heap_size);
    eprintln!("  seg_size:   {} bytes", config.cache.segment_size);
    eprintln!("  ht_power:   {}", config.cache.hashtable_power);
    eprintln!("  threads:    {}", config.general.threads);
    eprintln!("  duration:   {:?}", config.general.duration);
    eprintln!("  warmup:     {:?}", config.general.warmup);
    eprintln!(
        "  commands:   get={} set={} delete={}",
        config.workload.commands.get, config.workload.commands.set, config.workload.commands.delete,
    );
    eprintln!(
        "  keyspace:   {} keys x {} bytes",
        config.workload.keyspace.count, config.workload.keyspace.length,
    );
    eprintln!("  values:     {} bytes", config.workload.values.length);
    eprintln!();
}

fn print_header() {
    println!(
        "{:>12} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "ops/sec", "hit%", "p50(us)", "p90(us)", "p99(us)", "p999(us)", "max(us)",
    );
    println!("{}", "-".repeat(82));
}

fn print_summary(config: &Config) {
    let gets = metrics::GET_COUNT.value();
    let sets = metrics::SET_COUNT.value();
    let deletes = metrics::DELETE_COUNT.value();
    let completed = metrics::COMPLETED_COUNT.value();
    let set_errors = metrics::SET_ERRORS.value();
    let hits = metrics::CACHE_HITS.value();
    let misses = metrics::CACHE_MISSES.value();

    let total_gets = hits + misses;
    let hit_pct = if total_gets > 0 {
        (hits as f64 / total_gets as f64) * 100.0
    } else {
        0.0
    };

    let elapsed = config.general.duration.as_secs_f64();
    let avg_rate = if elapsed > 0.0 {
        completed as f64 / elapsed
    } else {
        0.0
    };

    eprintln!();
    eprintln!("=== Final Summary ===");
    eprintln!("  total ops:    {completed}");
    eprintln!("  avg ops/sec:  {avg_rate:.0}");
    eprintln!("  gets:         {gets}");
    eprintln!("  sets:         {sets} (errors: {set_errors})");
    eprintln!("  deletes:      {deletes}");
    eprintln!("  hit rate:     {hit_pct:.1}%");
    eprintln!();

    // Per-operation latency breakdown
    print_latency_summary("GET", &metrics::GET_LATENCY);
    print_latency_summary("SET", &metrics::SET_LATENCY);
    if deletes > 0 {
        print_latency_summary("DELETE", &metrics::DELETE_LATENCY);
    }
    print_latency_summary("ALL", &metrics::RESPONSE_LATENCY);
}

fn print_latency_summary(label: &str, hist: &AtomicHistogram) {
    let p50 = percentile(hist, 50.0) / 1000.0;
    let p90 = percentile(hist, 90.0) / 1000.0;
    let p99 = percentile(hist, 99.0) / 1000.0;
    let p999 = percentile(hist, 99.9) / 1000.0;
    let max = max_percentile(hist) / 1000.0;

    eprintln!(
        "  {label:<6} latency (us): p50={p50:.1}  p90={p90:.1}  p99={p99:.1}  p999={p999:.1}  max={max:.1}",
    );
}

// --- Cache constructors ---

fn create_segment(config: &Config) -> Result<impl Cache, Box<dyn std::error::Error>> {
    use segcache::{
        DiskTierConfig, EvictionPolicy as SegEvictionPolicy, MergeConfig, SegCache, SyncMode,
    };

    let mut builder = SegCache::builder()
        .heap_size(config.cache.heap_size)
        .segment_size(config.cache.segment_size)
        .hashtable_power(config.cache.hashtable_power);

    builder = match config.cache.policy {
        EvictionPolicy::S3Fifo => builder.s3fifo(),
        EvictionPolicy::Fifo => builder.eviction_policy(SegEvictionPolicy::Fifo),
        EvictionPolicy::Random => builder.eviction_policy(SegEvictionPolicy::Random),
        EvictionPolicy::Cte => builder.eviction_policy(SegEvictionPolicy::Cte),
        EvictionPolicy::Merge => {
            builder.eviction_policy(SegEvictionPolicy::Merge(MergeConfig::default()))
        }
        other => return Err(format!("invalid policy '{other}' for segment backend").into()),
    };

    if let Some(ref disk_config) = config.cache.disk
        && disk_config.enabled
    {
        let sync_mode = match disk_config.sync_mode {
            DiskSyncMode::Sync => SyncMode::Sync,
            DiskSyncMode::Async => SyncMode::Async,
            DiskSyncMode::None => SyncMode::None,
        };
        let disk_tier = DiskTierConfig::new(&disk_config.path, disk_config.size)
            .promotion_threshold(disk_config.promotion_threshold)
            .sync_mode(sync_mode)
            .recover_on_startup(disk_config.recover_on_startup);
        builder = builder.disk_tier(disk_tier);
    }

    let cache = builder.build()?;
    Ok(cache)
}

fn create_slab(config: &Config) -> Result<impl Cache, Box<dyn std::error::Error>> {
    use slab_cache::{DiskTierConfig, EvictionStrategy, SlabCache, SyncMode};

    let eviction_strategy = match config.cache.policy {
        EvictionPolicy::Lra => EvictionStrategy::SLAB_LRA,
        EvictionPolicy::Lrc => EvictionStrategy::SLAB_LRC,
        EvictionPolicy::Random => EvictionStrategy::RANDOM,
        EvictionPolicy::None => EvictionStrategy::NONE,
        other => return Err(format!("invalid policy '{other}' for slab backend").into()),
    };

    let mut builder = SlabCache::builder()
        .heap_size(config.cache.heap_size)
        .slab_size(config.cache.segment_size)
        .hashtable_power(config.cache.hashtable_power)
        .eviction_strategy(eviction_strategy);

    if let Some(ref disk_config) = config.cache.disk
        && disk_config.enabled
    {
        let sync_mode = match disk_config.sync_mode {
            DiskSyncMode::Sync => SyncMode::Sync,
            DiskSyncMode::Async => SyncMode::Async,
            DiskSyncMode::None => SyncMode::None,
        };
        let disk_tier = DiskTierConfig::new(&disk_config.path, disk_config.size)
            .promotion_threshold(disk_config.promotion_threshold)
            .sync_mode(sync_mode)
            .recover_on_startup(disk_config.recover_on_startup);
        builder = builder.disk_tier(disk_tier);
    }

    let cache = builder.build()?;

    Ok(cache)
}

fn create_heap(config: &Config) -> Result<impl Cache, Box<dyn std::error::Error>> {
    use heap_cache::{DiskTierConfig, EvictionPolicy as HeapEvictionPolicy, HeapCache, SyncMode};

    let heap_policy = match config.cache.policy {
        EvictionPolicy::S3Fifo => HeapEvictionPolicy::S3Fifo,
        EvictionPolicy::Lfu => HeapEvictionPolicy::Lfu,
        EvictionPolicy::Random => HeapEvictionPolicy::Random,
        other => return Err(format!("invalid policy '{other}' for heap backend").into()),
    };

    let mut builder = HeapCache::builder()
        .memory_limit(config.cache.heap_size)
        .hashtable_power(config.cache.hashtable_power)
        .eviction_policy(heap_policy);

    if let Some(ref disk_config) = config.cache.disk
        && disk_config.enabled
    {
        let sync_mode = match disk_config.sync_mode {
            DiskSyncMode::Sync => SyncMode::Sync,
            DiskSyncMode::Async => SyncMode::Async,
            DiskSyncMode::None => SyncMode::None,
        };
        let disk_tier = DiskTierConfig::new(&disk_config.path, disk_config.size)
            .promotion_threshold(disk_config.promotion_threshold)
            .sync_mode(sync_mode)
            .recover_on_startup(disk_config.recover_on_startup);
        builder = builder.disk_tier(disk_tier);
    }

    let cache = builder.build()?;

    Ok(cache)
}

// --- Histogram helpers ---

fn percentile(hist: &AtomicHistogram, p: f64) -> f64 {
    if let Some(snapshot) = hist.load() {
        percentile_from_histogram(&snapshot, p)
    } else {
        0.0
    }
}

fn percentile_from_histogram(hist: &Histogram, p: f64) -> f64 {
    if let Ok(Some(results)) = hist.percentiles(&[p])
        && let Some((_pct, bucket)) = results.first()
    {
        return bucket.end() as f64;
    }
    0.0
}

fn max_percentile(hist: &AtomicHistogram) -> f64 {
    if let Some(snapshot) = hist.load() {
        max_from_histogram(&snapshot)
    } else {
        0.0
    }
}

fn max_from_histogram(hist: &Histogram) -> f64 {
    if let Ok(Some(results)) = hist.percentiles(&[100.0])
        && let Some((_pct, bucket)) = results.first()
    {
        return bucket.end() as f64;
    }
    0.0
}

// --- CPU pinning ---

#[cfg(target_os = "linux")]
fn pin_to_cpu(cpu_id: usize) -> std::io::Result<()> {
    use std::mem;

    unsafe {
        let mut cpuset: libc::cpu_set_t = mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(cpu_id, &mut cpuset);

        let result = libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &cpuset);

        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_to_cpu(_cpu_id: usize) -> std::io::Result<()> {
    Ok(())
}
