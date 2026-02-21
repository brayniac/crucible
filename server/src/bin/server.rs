//! Crucible cache server binary.
//!
//! Uses ringline's AsyncEventHandler (one async task per connection)
//! with io_uring for high-performance cache I/O.

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::Parser;
use server::admin::{self, AdminConfig};
use server::banner::{BannerConfig, print_banner};
use server::config::{CacheBackend, Config, DiskIoBackendConfig, EvictionPolicy, HugepageConfig};
use server::{logging, signal};
use std::path::PathBuf;
use std::time::Duration;

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
        // Reuse the same config format — runtime choice is binary-level, not config-level.
        eprintln!("Use crucible-server --print-config (same config format)");
        std::process::exit(0);
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
            eprintln!("No config file specified. Use --config <path>");
            std::process::exit(1);
        }
    };

    logging::init(&config.logging);

    let shutdown = signal::install_signal_handler();

    if let Err(e) = run(config, shutdown) {
        tracing::error!(error = %e, "Server error");
        std::process::exit(1);
    }
}

fn run(
    config: Config,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listeners: Vec<_> = config
        .listener
        .iter()
        .map(|l| (l.protocol, l.address, l.tls.is_some()))
        .collect();
    let cpu_affinity = config.cpu_affinity();
    let cpu_affinity_slice = cpu_affinity.as_deref();

    let backend_detail = server::async_native::backend_detail();

    let numa_node = config.numa_node();
    let policy = config.cache.effective_policy();

    print_banner(&BannerConfig {
        version: env!("CARGO_PKG_VERSION"),
        backend_detail,
        cache_backend: config.cache.backend,
        eviction_policy: policy,
        small_queue_percent: config.cache.small_queue_percent,
        workers: config.threads(),
        listeners: &listeners,
        metrics_address: config.metrics.address,
        heap_size: config.cache.heap_size,
        segment_size: config.cache.segment_size,
        cpu_affinity: cpu_affinity_slice,
        numa_node,
    });

    let drain_timeout = Duration::from_secs(config.shutdown.drain_timeout_secs);

    match config.cache.backend {
        CacheBackend::Segment => {
            let cache = create_segment(&config, policy)?;
            run_with_admin(config, cache, shutdown, drain_timeout)
        }
        CacheBackend::Slab => {
            let cache = create_slab(&config, policy)?;
            run_with_admin(config, cache, shutdown, drain_timeout)
        }
        CacheBackend::Heap => {
            let cache = create_heap(&config, policy)?;
            run_with_admin(config, cache, shutdown, drain_timeout)
        }
    }
}

fn run_with_admin<C: cache_core::Cache + 'static>(
    config: Config,
    cache: C,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    drain_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;

    let cache = Arc::new(cache);
    let cache_for_stats = Arc::clone(&cache);

    let admin_handle = admin::start(AdminConfig {
        address: config.metrics.address,
        shutdown: shutdown.clone(),
        cache_stats_fn: Some(Arc::new(move || cache_for_stats.internal_stats())),
    })?;

    let result = server::async_native::run_shared(&config, cache, shutdown, drain_timeout);

    admin_handle.shutdown();

    result
}

fn create_segment(
    config: &Config,
    policy: EvictionPolicy,
) -> Result<impl cache_core::Cache, Box<dyn std::error::Error>> {
    use segcache::{
        DiskTierConfig, EvictionPolicy as SegEvictionPolicy, HugepageSize, IoUringDiskTierConfig,
        MergeConfig, SegCache, SyncMode,
    };
    use server::config::format_size;

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

    builder = match policy {
        EvictionPolicy::S3Fifo => builder.eviction_policy(SegEvictionPolicy::S3Fifo {
            small_queue_percent: config.cache.small_queue_percent,
            demotion_threshold: 1,
        }),
        EvictionPolicy::Fifo => builder.eviction_policy(SegEvictionPolicy::Fifo),
        EvictionPolicy::Random => builder.eviction_policy(SegEvictionPolicy::Random),
        EvictionPolicy::Cte => builder.eviction_policy(SegEvictionPolicy::Cte),
        EvictionPolicy::Merge => {
            builder.eviction_policy(SegEvictionPolicy::Merge(MergeConfig::default()))
        }
        _ => unreachable!("invalid policy for segment backend"),
    };

    if let Some(node) = config.numa_node() {
        builder = builder.numa_node(node);
    }

    if let Some(ref disk_config) = config.cache.disk
        && disk_config.enabled
    {
        match disk_config.io_backend {
            DiskIoBackendConfig::DirectIo | DiskIoBackendConfig::Nvme => {
                let segment_count = disk_config.size / config.cache.segment_size;
                let io_uring_tier = IoUringDiskTierConfig {
                    segment_count,
                    block_size: 4096,
                    promotion_threshold: disk_config.promotion_threshold,
                    ..Default::default()
                };
                let backend_name = match disk_config.io_backend {
                    DiskIoBackendConfig::DirectIo => "directio",
                    DiskIoBackendConfig::Nvme => "nvme",
                    _ => unreachable!(),
                };
                eprintln!(
                    "Disk tier: {} ({} segments x {}, write_buffers={})",
                    format_size(disk_config.size),
                    segment_count,
                    format_size(config.cache.segment_size),
                    io_uring_tier.write_buffer_count,
                );
                eprintln!(
                    "Disk I/O:  {} (promotion_threshold={})",
                    backend_name, disk_config.promotion_threshold,
                );
                builder = builder.io_uring_disk_tier(io_uring_tier);
            }
            DiskIoBackendConfig::Mmap => {
                let sync_mode = match disk_config.sync_mode {
                    server::config::DiskSyncMode::Sync => SyncMode::Sync,
                    server::config::DiskSyncMode::Async => SyncMode::Async,
                    server::config::DiskSyncMode::None => SyncMode::None,
                };

                let disk_tier = DiskTierConfig::new(&disk_config.path, disk_config.size)
                    .promotion_threshold(disk_config.promotion_threshold)
                    .sync_mode(sync_mode)
                    .recover_on_startup(disk_config.recover_on_startup);

                eprintln!(
                    "Disk tier: {} (mmap, sync={:?}, promotion_threshold={})",
                    format_size(disk_config.size),
                    disk_config.sync_mode,
                    disk_config.promotion_threshold,
                );
                eprintln!("Disk path: {}", disk_config.path.display());
                builder = builder.disk_tier(disk_tier);
            }
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

    if let Some(node) = config.numa_node() {
        builder = builder.numa_node(node);
    }

    if let Some(ref disk_config) = config.cache.disk
        && disk_config.enabled
    {
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
        EvictionPolicy::Random => HeapEvictionPolicy::Random,
        _ => unreachable!("invalid policy for heap backend"),
    };

    let mut builder = HeapCache::builder()
        .memory_limit(config.cache.heap_size)
        .hashtable_power(config.cache.hashtable_power)
        .eviction_policy(heap_policy)
        .small_queue_percent(config.cache.small_queue_percent);

    if let Some(ref disk_config) = config.cache.disk
        && disk_config.enabled
    {
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

    let cache = builder.build()?;

    Ok(cache)
}
