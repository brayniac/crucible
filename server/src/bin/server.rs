//! Crucible cache server binary.

use clap::Parser;
use server::banner::{BannerConfig, print_banner};
use server::config::{CacheBackend, Config, HugepageConfig, Runtime};
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

    let backend_detail = match config.runtime {
        Runtime::Native => server::native::backend_detail(),
        #[cfg(feature = "tokio-runtime")]
        Runtime::Tokio => server::tokio::backend_detail(),
        #[cfg(not(feature = "tokio-runtime"))]
        Runtime::Tokio => "tokio (not compiled)",
    };

    let numa_node = config.numa_node();

    print_banner(&BannerConfig {
        version: env!("CARGO_PKG_VERSION"),
        runtime: config.runtime,
        backend_detail,
        cache_backend: config.cache.backend,
        workers: config.threads(),
        listeners: &listeners,
        metrics_address: config.metrics.address,
        heap_size: config.cache.heap_size,
        segment_size: config.cache.segment_size,
        cpu_affinity: cpu_affinity_slice,
        numa_node,
    });

    // Create cache based on backend selection
    match config.cache.backend {
        CacheBackend::Segcache => {
            let cache = create_segcache(&config)?;
            run_with_cache(config, cache)
        }
        CacheBackend::S3fifo => {
            let cache = create_s3fifo(&config)?;
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

fn create_segcache(config: &Config) -> Result<impl cache_core::Cache, Box<dyn std::error::Error>> {
    use segcache::{HugepageSize, SegCache};

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

    // Use auto-detected or explicit NUMA node
    if let Some(node) = config.numa_node() {
        builder = builder.numa_node(node);
    }

    let cache = builder.build()?;

    Ok(cache)
}

fn create_s3fifo(config: &Config) -> Result<impl cache_core::Cache, Box<dyn std::error::Error>> {
    use s3fifo::{HugepageSize, S3FifoCache};

    let hugepage_size = match config.cache.hugepage {
        HugepageConfig::None => HugepageSize::None,
        HugepageConfig::TwoMegabyte => HugepageSize::TwoMegabyte,
        HugepageConfig::OneGigabyte => HugepageSize::OneGigabyte,
    };

    let mut builder = S3FifoCache::builder()
        .ram_size(config.cache.heap_size)
        .segment_size(config.cache.segment_size)
        .hashtable_power(config.cache.hashtable_power)
        .hugepage_size(hugepage_size);

    // Use auto-detected or explicit NUMA node
    if let Some(node) = config.numa_node() {
        builder = builder.numa_node(node);
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
# Cache backend: "segcache" or "s3fifo"
backend = "segcache"

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
