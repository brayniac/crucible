//! In-process cache benchmark — exercises the Cache trait directly
//! without network, protocol, or I/O driver overhead.

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod cachers;
mod config;
mod metrics;
mod ratelimit;
mod replay;
mod trace;
mod worker;

use crate::config::{CacheBackend, Config, EvictionPolicy};
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
        CacheBackend::CacheRs => {
            let cache = create_cachers(&config)?;
            run_with_cache(config, Arc::new(cache))
        }
    }
}

fn run_with_cache<C: Cache>(
    config: Config,
    cache: Arc<C>,
) -> Result<(), Box<dyn std::error::Error>> {
    if config.workload.trace.is_some() {
        return run_trace_replay(config, cache);
    }

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

/// Replay a trace, then report the measured window and whether it is usable.
///
/// Single-threaded on purpose: concurrency reorders how evictions interleave
/// with reads, which changes which items are resident, which changes the miss
/// ratio being measured. Throughput belongs to a different rig.
fn run_trace_replay<C: Cache>(
    config: Config,
    cache: Arc<C>,
) -> Result<(), Box<dyn std::error::Error>> {
    let trace_cfg = config.workload.trace.as_ref().expect("checked by caller");

    let opts = replay::ReplayOptions {
        warmup_records: trace_cfg.warmup_records,
        max_records: trace_cfg.max_records,
        report_interval_records: trace_cfg.report_interval_records,
        max_value_bytes: trace_cfg.max_value_bytes,
        insert_on_miss: trace_cfg.format.insert_on_miss(),
        retained_sizes: trace_cfg.retained_sizes,
    };

    eprintln!("replaying {}", trace_cfg.path.display());
    eprintln!("  warmup:     {} records", opts.warmup_records);
    eprintln!(
        "  measured:   {}",
        match opts.max_records {
            Some(n) => format!("{n} records"),
            None => "to end of trace".to_string(),
        }
    );
    eprintln!("  insert on miss: {}", opts.insert_on_miss);
    // A result that does not say whether expiry was live is not comparable
    // with one that does: with the clock on wall time, a TTL shorter than
    // the run never fires, and every policy that consults expiry is
    // measured against a mechanism that was effectively switched off.
    eprintln!(
        "  clock:      {}",
        if cfg!(feature = "virtual-clock") {
            "trace time (TTLs fire as recorded)"
        } else {
            "wall clock (TTLs shorter than the run never fire)"
        }
    );
    eprintln!();

    let mut reader = trace::TraceReader::open(&trace_cfg.path, trace_cfg.format.into())?;

    let started = Instant::now();
    let outcome = replay::run_replay(cache.as_ref(), &mut reader, &opts)?;
    let elapsed = started.elapsed();

    let internal = cache.internal_stats();
    print_replay_report(&outcome, internal.clone(), elapsed);

    // The envelope check is a hard gate, not a warning: a point that measured
    // no eviction is not a weaker result, it is a different measurement.
    match replay::envelope_verdict(&outcome.measured, internal) {
        Ok(()) => Ok(()),
        Err(reason) => Err(format!("run rejected: {reason}").into()),
    }
}

fn print_replay_report(
    outcome: &replay::ReplayOutcome,
    internal: Option<cache_core::CacheInternalStats>,
    elapsed: Duration,
) {
    let m = &outcome.measured;
    let w = &outcome.warmup;

    eprintln!("=== Replay ===");
    eprintln!("  records read:   {}", outcome.records_read);
    eprintln!("  elapsed:        {elapsed:?}");
    eprintln!(
        "  warmup:         {} gets ({} miss), {} sets",
        w.hits + w.misses,
        w.misses,
        w.sets
    );
    eprintln!();
    eprintln!("=== Measured window ===");
    eprintln!("  gets:           {}", m.hits + m.misses);
    eprintln!("  sets:           {} (errors: {})", m.sets, m.set_errors);
    eprintln!("  deletes:        {}", m.deletes);
    eprintln!("  oversized:      {}", m.oversized);
    match m.miss_ratio() {
        Some(r) => eprintln!(
            "  MISS RATIO:     {:.4}  (hit {:.2}%)",
            r,
            (1.0 - r) * 100.0
        ),
        None => eprintln!("  MISS RATIO:     n/a (no GETs)"),
    }
    // Printed beside the request-weighted ratio rather than instead of it,
    // because retention policies trade one against the other. Ranking by
    // frequency alone keeps large hot items and favours this ratio; ranking
    // by frequency-over-size keeps small ones and favours the ratio above.
    // Reporting only one scores every policy on the axis that happens to
    // suit it.
    match m.byte_miss_ratio() {
        Some(r) => eprintln!(
            "  BYTE MISS:      {:.4}  (hit {:.2}%)  over {:.1} MiB of GETs",
            r,
            (1.0 - r) * 100.0,
            m.get_bytes as f64 / (1024.0 * 1024.0)
        ),
        None => eprintln!("  BYTE MISS:      n/a (no GET carried bytes)"),
    }

    // Merge eviction runs inline in `set`, so a reclamation pass is a stall on
    // whichever write triggered it. The tail is the whole signal here: at ~200
    // evictions per 10M records the stall is a 1-in-50,000 event, invisible in
    // p50 and p99 and potentially brutal above them.
    eprintln!();
    eprintln!("=== Latency (us) ===");
    print_replay_latency("READ ", outcome.read_latency.as_ref());
    print_replay_latency("WRITE", outcome.write_latency.as_ref());

    // Survival by value size, when asked for. Two engines holding
    // different item counts in the same heap may be keeping the same size
    // mix in different amounts, or different mixes entirely, and the totals
    // read identically either way. Measured from the replay side so one
    // code path covers both engines.
    if let Some(hist) = &outcome.retained_sizes {
        eprintln!("=== Retained by value size ===");
        for &(bucket, written, retained) in &hist.buckets {
            let pct = if written > 0 {
                100.0 * retained as f64 / written as f64
            } else {
                0.0
            };
            let label = if bucket >= 1024 {
                format!("{:>5} KiB", bucket / 1024)
            } else {
                format!("{bucket:>5} B  ")
            };
            eprintln!("  {label}  {retained:>9} of {written:>9} written  ({pct:5.1}%)");
        }
    }

    if let Some(stats) = internal {
        eprintln!();
        eprintln!("=== Cache internals ===");
        eprintln!("  evictions:      {}", stats.evictions);
        eprintln!("  demotions:      {}", stats.demotions);
        eprintln!("  demotion fails: {}", stats.demotion_failures);
        // Zero here with compaction configured means it never found an
        // eligible pair -- a different problem from compaction running and
        // not helping, and the two were indistinguishable before this was
        // counted.
        eprintln!("  compactions:    {}", stats.compactions);
        eprintln!("  resident items: {}", stats.resident_items);
        // Printed as a decomposition rather than a ratio, because the
        // question it answers -- why does this engine hold fewer items in
        // the same heap -- has three answers that one number cannot tell
        // apart: segments packed loosely, segments full of superseded items
        // nothing reclaimed, or a policy that retained fewer on purpose.
        if stats.capacity_bytes > 0 {
            let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
            let packed = 100.0 * stats.written_bytes as f64 / stats.capacity_bytes as f64;
            let live_share = if stats.written_bytes > 0 {
                100.0 * stats.live_bytes as f64 / stats.written_bytes as f64
            } else {
                0.0
            };
            eprintln!(
                "  segment bytes:  {:.1} MiB live / {:.1} MiB written / {:.1} MiB capacity",
                mib(stats.live_bytes),
                mib(stats.written_bytes),
                mib(stats.capacity_bytes)
            );
            eprintln!("    packed:       {packed:.1}% of capacity written  (low = loose packing)");
            eprintln!(
                "    live:         {live_share:.1}% of written still live  (low = unreclaimed)"
            );
            if stats.resident_items > 0 {
                eprintln!(
                    "    per item:     {:.0} B live, {:.0} B of capacity",
                    stats.live_bytes as f64 / stats.resident_items as f64,
                    stats.capacity_bytes as f64 / stats.resident_items as f64
                );
            }
        }
        // The mean above cannot say whether compaction is reachable.
        // `try_compact_segment` merges two adjacent sealed segments into one
        // spare, and only when their combined live bytes fit in 90% of a
        // single segment -- 45% average occupancy across the pair. So a
        // cache can sit at any mean at all and still have no eligible pairs,
        // and the decomposition above would look identical either way.
        //
        // The eligible count below is an upper bound, not a count of
        // available compactions: the two segments also have to be adjacent
        // in the same bucket's chain. If it is near zero, though, the bound
        // is enough -- no pairs means no compaction, whatever the chain
        // order.
        if let Some(deciles) = stats.occupancy_deciles {
            let counted: u64 = deciles.iter().sum();
            if counted > 0 {
                let bars: Vec<String> = (0..10)
                    .map(|i| format!("{:>2}0%:{}", i, deciles[i]))
                    .collect();
                eprintln!("  occupancy:      {}", bars.join("  "));
                // Deciles 0-3 are wholly under 45%; decile 4 straddles it,
                // so it is excluded rather than half-counted.
                let eligible: u64 = deciles[..4].iter().sum();
                eprintln!(
                    "    compactable:  {eligible} of {counted} segments under 40% live \
                     (a pair needs <=45% each)"
                );
            }
        }
        // Same figures `envelope_verdict`'s fill check reads, printed here so
        // a thin cell (the cache never filled) is visible to the analysis
        // step even on a run that passes the check.
        if stats.total_segments > 0 {
            eprintln!(
                "  segments:       {} free / {} total ({:.1}% free)",
                stats.free_segments,
                stats.total_segments,
                stats.free_segments as f64 / stats.total_segments as f64 * 100.0,
            );
        } else {
            eprintln!("  segments:       (not reported)");
        }

        // Independent of the write-latency histogram above: that one times
        // every record from the replay's side, this one times the eviction
        // path from inside the cache. They should agree on the tail, and
        // disagreeing would mean one of them is measuring the wrong thing.
        let ev = &stats.eviction_latency;
        match (ev.count(), ev.max_ns()) {
            (0, _) | (_, None) => eprintln!("  evict pass us: (no passes timed)"),
            (n, Some(max)) => eprintln!(
                "  evict pass us: n={n}  mean={:.1}  p50={:.1}  p99={:.1}  max={:.1}",
                ev.mean_ns().unwrap_or(0) as f64 / 1000.0,
                ev.percentile_ns(50.0).unwrap_or(0) as f64 / 1000.0,
                ev.percentile_ns(99.0).unwrap_or(0) as f64 / 1000.0,
                max as f64 / 1000.0,
            ),
        }
    }

    // The tail is where steady state is judged. A window still trending at its
    // end was too short, and its average is a value that occurs nowhere in it.
    if !outcome.intervals.is_empty() {
        eprintln!();
        eprintln!("=== Miss ratio per interval ===");
        for (i, r) in outcome.intervals.iter().enumerate() {
            eprintln!("  [{i:>3}] {r:.4}");
        }
        match replay::tail_spread(&outcome.intervals) {
            Some(spread) => eprintln!("  last third spread: {spread:.4}"),
            None => eprintln!(
                "  last third spread: n/a (need >= 9 intervals; \
                 lower report_interval_records or lengthen the window)"
            ),
        }
    }
    eprintln!();
}

/// Print one latency distribution, tail-weighted.
fn print_replay_latency(label: &str, hist: Option<&Histogram>) {
    let Some(hist) = hist else {
        eprintln!("  {label}  (no samples)");
        return;
    };
    eprintln!(
        "  {label}  p50={:.1}  p99={:.1}  p99.9={:.1}  p99.99={:.1}  max={:.1}",
        percentile_from_histogram(hist, 50.0) / 1000.0,
        percentile_from_histogram(hist, 99.0) / 1000.0,
        percentile_from_histogram(hist, 99.9) / 1000.0,
        percentile_from_histogram(hist, 99.99) / 1000.0,
        max_from_histogram(hist) / 1000.0,
    );
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

/// Apply the seeds a config pins to a segment-backend builder.
///
/// Split out from `create_segment` so the plumbing has a seam: a seed that
/// parses and is then dropped on the way to the builder would make a sweep
/// over seeds report a spread of zero, and every policy difference inside
/// that spread look significant.
fn apply_reproducibility_seeds(
    mut builder: segcache::SegCacheBuilder,
    cache: &crate::config::CacheConfig,
) -> segcache::SegCacheBuilder {
    if let Some(seed) = cache.hashtable_seed {
        builder = builder.hashtable_seed(seed);
    }
    if let Some(seed) = cache.eviction_seed {
        builder = builder.eviction_seed(seed);
    }
    builder
}

/// The merge knobs from config, applied over a base.
///
/// One function rather than two call sites, because there were two: the
/// s3fifo main layer read these and the single-layer merge arm did not, so
/// a sweep over chain length silently measured the compiled default on
/// every point. Duplicated plumbing is how that happens, and adding a third
/// knob to two places is how it happens again.
fn merge_config_from(
    cache: &config::CacheConfig,
    base: cache_core::MergeConfig,
) -> cache_core::MergeConfig {
    let mut cfg = base;
    if let Some(n) = cache.main_merge_segments {
        cfg.min_segments = n;
    }
    if let Some(r) = cache.main_target_ratio {
        cfg.target_ratio = r;
    }
    if let Some(e) = cache.main_cost_exponent {
        cfg.cost_exponent = e;
    }
    cfg
}

fn create_segment(config: &Config) -> Result<impl Cache, Box<dyn std::error::Error>> {
    use segcache::{DiskTierConfig, EvictionPolicy as SegEvictionPolicy, MergeConfig, SegCache};

    let mut builder = SegCache::builder()
        .heap_size(config.cache.heap_size)
        .segment_size(config.cache.segment_size)
        .hashtable_power(config.cache.hashtable_power);

    builder = apply_reproducibility_seeds(builder, &config.cache);

    // Applied before the policy selection below, so it holds whichever
    // layer topology the policy chooses.
    if let Some(policy) = config.cache.overwrite_reclaim {
        builder = builder.overwrite_reclaim(policy.into());
    }

    builder = match config.cache.policy {
        EvictionPolicy::S3Fifo => {
            let mut b = builder.s3fifo();
            let mut strategy = config.cache.main_policy.map(Into::into).unwrap_or(
                cache_core::EvictionStrategy::Merge(cache_core::MergeConfig::default()),
            );
            // Chain length is the lever the first results identified, so it
            // overrides the policy's default rather than the policy silently
            // winning. Rejected for clock in config validation.
            if let cache_core::EvictionStrategy::Merge(ref mut cfg) = strategy {
                *cfg = merge_config_from(&config.cache, *cfg);
            }
            b = b.main_eviction(strategy);
            b
        }
        EvictionPolicy::Fifo => builder.eviction_policy(SegEvictionPolicy::Fifo),
        EvictionPolicy::Random => builder.eviction_policy(SegEvictionPolicy::Random),
        EvictionPolicy::RandomFifo => builder.eviction_policy(SegEvictionPolicy::RandomFifo),
        EvictionPolicy::Cte => builder.eviction_policy(SegEvictionPolicy::Cte),
        EvictionPolicy::Merge => {
            // The single-layer merge arm took `MergeConfig::default()` and
            // ignored every knob, so a sweep over chain length or retention
            // silently measured the compiled default on every point. Both
            // knobs are read here for the same reason they are read for the
            // s3fifo main layer.
            let cfg = merge_config_from(&config.cache, MergeConfig::default());
            builder.eviction_policy(SegEvictionPolicy::Merge(cfg))
        }
        other => return Err(format!("invalid policy '{other}' for segment backend").into()),
    };

    // Accepting a main-cache policy the topology has no layer 1 for would run
    // the arm as whatever the single-layer default is and report it under the
    // name that was asked for.
    if config.cache.main_policy.is_some() && config.cache.policy != EvictionPolicy::S3Fifo {
        return Err("main_policy applies only to policy = \"s3fifo\"; the \
                    single-layer policies configure their own layer directly"
            .into());
    }

    if let Some(ref disk_config) = config.cache.disk
        && disk_config.enabled
    {
        let disk_tier = DiskTierConfig::new(&disk_config.path, disk_config.size)
            .promotion_threshold(disk_config.promotion_threshold)
            .sync_mode(disk_config.sync_mode.into())
            .recover_on_startup(disk_config.recover_on_startup);
        builder = builder.disk_tier(disk_tier);
    }

    let cache = builder.build()?;
    Ok(cache)
}

#[cfg(feature = "cache-rs")]
fn create_cachers(config: &Config) -> Result<impl Cache, Box<dyn std::error::Error>> {
    let policy = cachers::cachers_policy(config.cache.policy).ok_or_else(|| {
        format!(
            "policy '{}' has no cache-rs counterpart",
            config.cache.policy
        )
    })?;

    let inner = cache_rs::Segcache::builder()
        .heap_size(config.cache.heap_size)
        .segment_size(segment_size_i32(config.cache.segment_size)?)
        // Converted, not copied. See `cachers_hash_power`.
        .hash_power(cachers::cachers_hash_power(config.cache.hashtable_power))
        .eviction(policy)
        .build()?;

    Ok(cachers::CacheRs::new(
        inner,
        config.cache.segment_size as u64,
    ))
}

#[cfg(not(feature = "cache-rs"))]
fn create_cachers(_config: &Config) -> Result<impl Cache, Box<dyn std::error::Error>> {
    // Rejected rather than ignored: a config naming a backend the binary
    // cannot provide must fail, not silently run a different engine.
    Err::<crate::cachers::Unavailable, _>(
        "this binary was built without the `cache-rs` feature; \
         rebuild with --features cache-rs"
            .into(),
    )
}

/// crucible's `segment_size` is `usize`; cache-rs's builder takes `i32`. A
/// bare `as` cast would silently wrap a segment size above 2 GiB to
/// negative, so this rejects the config instead.
#[cfg(feature = "cache-rs")]
fn segment_size_i32(bytes: usize) -> Result<i32, Box<dyn std::error::Error>> {
    i32::try_from(bytes)
        .map_err(|_| format!("segment_size {bytes} exceeds cache-rs's i32 limit").into())
}

fn create_slab(config: &Config) -> Result<impl Cache, Box<dyn std::error::Error>> {
    use slab_cache::{DiskTierConfig, EvictionStrategy, SlabCache};

    // Accepting the seed and not applying it would make a noise-floor sweep
    // report a floor of zero, which makes every policy difference look
    // significant. Reject instead.
    if config.cache.hashtable_seed.is_some() {
        return Err("the slab backend does not support hashtable_seed yet".into());
    }

    if config.cache.eviction_seed.is_some() {
        return Err("the slab backend does not support eviction_seed yet".into());
    }

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
        let disk_tier = DiskTierConfig::new(&disk_config.path, disk_config.size)
            .promotion_threshold(disk_config.promotion_threshold)
            .sync_mode(disk_config.sync_mode.into())
            .recover_on_startup(disk_config.recover_on_startup);
        builder = builder.disk_tier(disk_tier);
    }

    let cache = builder.build()?;

    Ok(cache)
}

fn create_heap(config: &Config) -> Result<impl Cache, Box<dyn std::error::Error>> {
    use heap_cache::{EvictionPolicy as HeapEvictionPolicy, HeapCache};

    // See `create_slab` on why this is rejected rather than ignored.
    if config.cache.hashtable_seed.is_some() {
        return Err("the heap backend does not support hashtable_seed yet".into());
    }

    if config.cache.eviction_seed.is_some() {
        return Err("the heap backend does not support eviction_seed yet".into());
    }

    let heap_policy = match config.cache.policy {
        EvictionPolicy::S3Fifo => HeapEvictionPolicy::S3Fifo,
        EvictionPolicy::Lfu => HeapEvictionPolicy::Lfu,
        EvictionPolicy::Random => HeapEvictionPolicy::Random,
        other => return Err(format!("invalid policy '{other}' for heap backend").into()),
    };

    let builder = HeapCache::builder()
        .memory_limit(config.cache.heap_size)
        .hashtable_power(config.cache.hashtable_power)
        .eviction_policy(heap_policy);

    // The heap backend has no disk tier. Its location encoding spends bits
    // 43-41 on the value type, which is where every other backend keeps a
    // 2-bit pool id, so a complex-type location and a disk location would be
    // indistinguishable. Reject rather than silently ignore -- before this,
    // `enabled = true` built a disk layer that nothing ever wrote to.
    if let Some(ref disk_config) = config.cache.disk
        && disk_config.enabled
    {
        return Err("the heap backend does not support a disk tier; use \
                    backend = \"segment\" for disk tiering, or set \
                    [cache.disk] enabled = false"
            .into());
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
    if let Ok(Some(results)) = hist.quantile(p / 100.0)
        && let Some(bucket) = results.entries().values().next()
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
    if let Ok(Some(results)) = hist.quantile(1.0) {
        return results.max().end() as f64;
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

#[cfg(test)]
mod seed_plumbing_tests {
    use super::*;

    /// The seed has to survive the trip from the parsed config to the
    /// builder. Accepting it and dropping it here would leave a sweep over
    /// seeds reporting a spread of zero, which makes every policy difference
    /// inside the real spread look significant.
    #[test]
    fn a_configured_eviction_seed_reaches_the_segment_builder() {
        let toml = r#"
[general]
duration = "1s"
warmup = "0s"
threads = 1

[cache]
backend = "segment"
policy = "randomfifo"
heap_size = "16MB"
segment_size = "256KB"
hashtable_power = 16
eviction_seed = 4242

[workload.trace]
path = "/tmp/t.bin"
format = "twitter"
warmup_records = 0
"#;
        let config = crate::config::Config::from_toml(toml).expect("parse");
        let builder = apply_reproducibility_seeds(segcache::SegCache::builder(), &config.cache);
        assert_eq!(builder.configured_eviction_seed(), Some(4242));
    }

    /// Every merge knob must reach the config, and an unset one must not.
    ///
    /// Written against `merge_config_from` because the failure this guards
    /// is not a parse failure: `main_merge_segments` was once read on the
    /// s3fifo path and ignored on the single-layer merge path, so a sweep
    /// parsed the file, built a cache, produced numbers, and measured the
    /// compiled default at every point. Nothing in the output said so.
    #[test]
    fn every_merge_knob_reaches_the_config_and_an_unset_one_does_not() {
        let base = cache_core::MergeConfig::default();
        let toml = |extra: &str| {
            let text = format!(
                "[general]\nduration = \"1s\"\nwarmup = \"0s\"\nthreads = 1\n\n\
                 [cache]\nbackend = \"segment\"\npolicy = \"merge\"\n\
                 heap_size = \"16MB\"\nsegment_size = \"256KB\"\n\
                 hashtable_power = 16\n{extra}\n\n\
                 [workload.trace]\npath = \"/tmp/t.bin\"\nformat = \"twitter\"\n\
                 warmup_records = 0\n"
            );
            crate::config::Config::from_toml(&text).expect("parse")
        };

        // Unset: the compiled defaults survive untouched.
        let untouched = merge_config_from(&toml("").cache, base);
        assert_eq!(untouched, base, "an empty config must change nothing");

        // Set: each knob lands, and lands on its own field.
        let set = merge_config_from(
            &toml(
                "main_merge_segments = 7\n\
                 main_target_ratio = 0.9\n\
                 main_cost_exponent = 0.0",
            )
            .cache,
            base,
        );
        assert_eq!(set.min_segments, 7, "chain length must reach the config");
        assert_eq!(set.target_ratio, 0.9, "retention cap must reach it");
        assert_eq!(
            set.cost_exponent, 0.0,
            "cost exponent must reach it -- and 0.0 is exactly the value a \
             knob that was parsed and dropped would leave behind if the \
             default were 0.0, which is why the default is 1.0 and this \
             asserts the non-default"
        );
        assert_ne!(
            set.cost_exponent, base.cost_exponent,
            "the test is vacuous unless 0.0 differs from the default"
        );
    }

    /// And an unset seed must leave the builder alone, so the layer default
    /// applies rather than some stand-in value chosen here.
    #[test]
    fn an_unset_eviction_seed_leaves_the_builder_untouched() {
        let toml = r#"
[general]
duration = "1s"
warmup = "0s"
threads = 1

[cache]
backend = "segment"
policy = "randomfifo"
heap_size = "16MB"
segment_size = "256KB"
hashtable_power = 16

[workload.trace]
path = "/tmp/t.bin"
format = "twitter"
warmup_records = 0
"#;
        let config = crate::config::Config::from_toml(toml).expect("parse");
        let builder = apply_reproducibility_seeds(segcache::SegCache::builder(), &config.cache);
        assert_eq!(builder.configured_eviction_seed(), None);
    }
}
