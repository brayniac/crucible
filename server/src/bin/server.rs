//! Crucible cache server binary.

use clap::Parser;
use server::banner::{BannerConfig, print_banner};
use server::config::{CacheBackend, Config, EvictionPolicy, HugepageConfig, Runtime};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "crucible-server")]
#[command(about = "High-performance cache server")]
struct Args {
    /// Path to configuration file
    config: Option<PathBuf>,

    /// Print default configuration and exit
    #[arg(long)]
    print_config: bool,
}

fn main() {
    let args = Args::parse();

    if args.print_config {
        print_default_config();
        return;
    }

    let config = match &args.config {
        Some(path) => match Config::load(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to load config: {}", e);
                std::process::exit(1);
            }
        },
        None => {
            eprintln!("No config file specified. Use --config <path> or --print-config");
            std::process::exit(1);
        }
    };

    if let Err(e) = run(config) {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}

fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    // Print banner
    let listeners: Vec<_> = config
        .listener
        .iter()
        .map(|l| (l.protocol, l.address))
        .collect();
    let cpu_affinity = config.cpu_affinity();
    let cpu_affinity_slice = cpu_affinity.as_deref();

    let backend_detail: String = match config.runtime {
        Runtime::Native => {
            let base = server::native::backend_detail();
            // Show recv_mode when using io_uring
            if base == "io_uring" {
                let mode = match config.uring.recv_mode {
                    server::config::RecvMode::Multishot => "multishot",
                    server::config::RecvMode::SingleShot => "single-shot",
                };
                format!("{}, {}", base, mode)
            } else {
                base.to_string()
            }
        }
        #[cfg(feature = "tokio-runtime")]
        Runtime::Tokio => server::tokio::backend_detail().to_string(),
        #[cfg(not(feature = "tokio-runtime"))]
        Runtime::Tokio => "tokio (not compiled)".to_string(),
    };

    let numa_node = config.numa_node();
    let policy = config.cache.effective_policy();

    print_banner(&BannerConfig {
        version: env!("CARGO_PKG_VERSION"),
        runtime: config.runtime,
        backend_detail: &backend_detail,
        cache_backend: config.cache.backend,
        eviction_policy: policy,
        workers: config.threads(),
        listeners: &listeners,
        metrics_address: config.metrics.address,
        heap_size: config.cache.heap_size,
        segment_size: config.cache.segment_size,
        cpu_affinity: cpu_affinity_slice,
        numa_node,
    });

    // Create cache based on backend + policy selection

    match config.cache.backend {
        CacheBackend::Segment => {
            let cache = create_segment(&config, policy)?;
            run_with_cache(config, cache)
        }
        CacheBackend::Slab => {
            let cache = create_slab(&config, policy)?;
            run_with_cache(config, cache)
        }
        CacheBackend::Heap => {
            let cache = create_heap(&config, policy)?;
            run_with_cache(config, cache)
        }
    }
}

fn run_with_cache<C: cache_core::Cache + 'static>(
    config: Config,
    cache: C,
) -> Result<(), Box<dyn std::error::Error>> {
    match config.runtime {
        Runtime::Native => server::native::run(&config, cache),
        #[cfg(feature = "tokio-runtime")]
        Runtime::Tokio => server::tokio::run(&config, cache),
        #[cfg(not(feature = "tokio-runtime"))]
        Runtime::Tokio => Err("tokio runtime not compiled in".into()),
    }
}

fn create_segment(
    config: &Config,
    policy: EvictionPolicy,
) -> Result<impl cache_core::Cache, Box<dyn std::error::Error>> {
    use segcache::{
        DiskTierConfig, EvictionPolicy as SegEvictionPolicy, HugepageSize, MergeConfig, SegCache,
        SyncMode,
    };

    let hugepage_size = match config.cache.hugepage {
        HugepageConfig::None => HugepageSize::None,
        HugepageConfig::TwoMegabyte => HugepageSize::TwoMegabyte,
        HugepageConfig::OneGigabyte => HugepageSize::OneGigabyte,
    };

    let mut builder = SegCache::builder()
        .heap_size(config.cache.heap_size)
        .segment_size(config.cache.segment_size)
        .hashtable_power(config.cache.hashtable_power)
        .hugepage_size(hugepage_size);

    // Apply eviction policy
    builder = match policy {
        EvictionPolicy::S3Fifo => builder.s3fifo(),
        EvictionPolicy::Fifo => builder.eviction_policy(SegEvictionPolicy::Fifo),
        EvictionPolicy::Random => builder.eviction_policy(SegEvictionPolicy::Random),
        EvictionPolicy::Cte => builder.eviction_policy(SegEvictionPolicy::Cte),
        EvictionPolicy::Merge => {
            builder.eviction_policy(SegEvictionPolicy::Merge(MergeConfig::default()))
        }
        _ => unreachable!("invalid policy for segment backend"),
    };

    // Use auto-detected or explicit NUMA node
    if let Some(node) = config.numa_node() {
        builder = builder.numa_node(node);
    }

    // Configure disk tier if enabled
    if let Some(ref disk_config) = config.cache.disk {
        if disk_config.enabled {
            let sync_mode = match disk_config.sync_mode {
                server::config::DiskSyncMode::Sync => SyncMode::Sync,
                server::config::DiskSyncMode::Async => SyncMode::Async,
                server::config::DiskSyncMode::None => SyncMode::None,
            };

            let disk_tier = DiskTierConfig::new(&disk_config.path, disk_config.size)
                .promotion_threshold(disk_config.promotion_threshold)
                .sync_mode(sync_mode)
                .recover_on_startup(disk_config.recover_on_startup);

            builder = builder.disk_tier(disk_tier);
        }
    }

    let cache = builder.build()?;

    Ok(cache)
}

fn create_slab(
    config: &Config,
    policy: EvictionPolicy,
) -> Result<impl cache_core::Cache, Box<dyn std::error::Error>> {
    use slab_cache::{DiskTierConfig, EvictionStrategy, HugepageSize, SlabCache, SyncMode};

    let hugepage_size = match config.cache.hugepage {
        HugepageConfig::None => HugepageSize::None,
        HugepageConfig::TwoMegabyte => HugepageSize::TwoMegabyte,
        HugepageConfig::OneGigabyte => HugepageSize::OneGigabyte,
    };

    let eviction_strategy = match policy {
        EvictionPolicy::Lra => EvictionStrategy::SLAB_LRA,
        EvictionPolicy::Lrc => EvictionStrategy::SLAB_LRC,
        EvictionPolicy::Random => EvictionStrategy::RANDOM,
        EvictionPolicy::None => EvictionStrategy::NONE,
        _ => unreachable!("invalid policy for slab backend"),
    };

    let mut builder = SlabCache::builder()
        .heap_size(config.cache.heap_size)
        .slab_size(config.cache.segment_size)
        .hashtable_power(config.cache.hashtable_power)
        .hugepage_size(hugepage_size)
        .eviction_strategy(eviction_strategy);

    // Use auto-detected or explicit NUMA node
    if let Some(node) = config.numa_node() {
        builder = builder.numa_node(node);
    }

    // Configure disk tier if enabled
    if let Some(ref disk_config) = config.cache.disk {
        if disk_config.enabled {
            let sync_mode = match disk_config.sync_mode {
                server::config::DiskSyncMode::Sync => SyncMode::Sync,
                server::config::DiskSyncMode::Async => SyncMode::Async,
                server::config::DiskSyncMode::None => SyncMode::None,
            };

            let disk_tier = DiskTierConfig::new(&disk_config.path, disk_config.size)
                .promotion_threshold(disk_config.promotion_threshold)
                .sync_mode(sync_mode)
                .recover_on_startup(disk_config.recover_on_startup);

            builder = builder.disk_tier(disk_tier);
        }
    }

    let cache = builder.build()?;

    Ok(cache)
}

fn create_heap(
    config: &Config,
    policy: EvictionPolicy,
) -> Result<impl cache_core::Cache, Box<dyn std::error::Error>> {
    use heap_cache::{DiskTierConfig, EvictionPolicy as HeapEvictionPolicy, HeapCache, SyncMode};

    let heap_policy = match policy {
        EvictionPolicy::S3Fifo => HeapEvictionPolicy::S3Fifo,
        EvictionPolicy::Lfu => HeapEvictionPolicy::Lfu,
        _ => unreachable!("invalid policy for heap backend"),
    };

    let mut builder = HeapCache::builder()
        .memory_limit(config.cache.heap_size)
        .hashtable_power(config.cache.hashtable_power)
        .eviction_policy(heap_policy);

    // Configure disk tier if enabled
    if let Some(ref disk_config) = config.cache.disk {
        if disk_config.enabled {
            let sync_mode = match disk_config.sync_mode {
                server::config::DiskSyncMode::Sync => SyncMode::Sync,
                server::config::DiskSyncMode::Async => SyncMode::Async,
                server::config::DiskSyncMode::None => SyncMode::None,
            };

            let disk_tier = DiskTierConfig::new(&disk_config.path, disk_config.size)
                .promotion_threshold(disk_config.promotion_threshold)
                .sync_mode(sync_mode)
                .recover_on_startup(disk_config.recover_on_startup);

            builder = builder.disk_tier(disk_tier);
        }
    }

    let cache = builder.build()?;

    Ok(cache)
}

fn print_default_config() {
    let config = r#"# Crucible Server Configuration

# Runtime selection: "native" (io_uring/mio) or "tokio"
runtime = "native"

[workers]
# Number of worker threads (default: number of CPUs)
# threads = 8

# CPU cores to pin workers to (Linux-style, e.g., "0-3,6-8")
# cpu_affinity = "0-7"

[cache]
# Cache backend (storage type): "segment", "slab", or "heap"
backend = "segment"

# Eviction policy (depends on backend):
#   segment: "s3fifo" (default), "fifo", "random", "cte", "merge"
#   heap:    "s3fifo" (default), "lfu"
#   slab:    "lra" (default), "lrc", "random", "none"
policy = "s3fifo"

# Total heap size (e.g., "4GB", "512MB")
heap_size = "4GB"

# Segment size (e.g., "1MB", "512KB")
segment_size = "1MB"

# Hashtable power (2^power buckets)
hashtable_power = 26

# Hugepage size: "none", "2mb", or "1gb" (Linux only)
# Falls back to regular pages if hugepages are unavailable
hugepage = "none"

# NUMA node to bind cache memory to (Linux only)
# If not set, auto-detected from cpu_affinity when all CPUs are on the same node
# numa_node = 0

# Disk tier configuration (optional)
# When enabled, items evicted from RAM are demoted to disk instead of being discarded
# [cache.disk]
# enabled = true
# path = "/var/cache/crucible/disk.dat"
# size = "100GB"
# promotion_threshold = 2       # Promote items with freq > threshold to RAM
# sync_mode = "async"           # "sync", "async", or "none"
# recover_on_startup = true     # Recover existing disk cache on startup

# Protocol listeners - configure one or more
[[listener]]
protocol = "resp"
address = "0.0.0.0:6379"

[[listener]]
protocol = "memcache"
address = "0.0.0.0:11211"

# Optional: Momento gRPC listener
# [[listener]]
# protocol = "momento"
# address = "0.0.0.0:8443"
# [listener.tls]
# cert = "/path/to/cert.pem"
# key = "/path/to/key.pem"

[metrics]
# Prometheus metrics endpoint
address = "0.0.0.0:9090"

# io_uring specific settings (native runtime on Linux only)
[uring]
# Enable SQPOLL mode (requires CAP_SYS_NICE or root)
sqpoll = false

# SQPOLL idle timeout in milliseconds
sqpoll_idle_ms = 1000

# Buffer pool settings
buffer_count = 1024
buffer_size = 4096

# Submission queue depth
sq_depth = 1024
"#;
    print!("{}", config);
}
