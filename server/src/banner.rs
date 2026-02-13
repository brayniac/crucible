//! Startup banner utilities.

use crate::config::{CacheBackend, EvictionPolicy, Protocol, format_size};
use std::fmt::Write;
use std::net::SocketAddr;

/// Configuration for the startup banner.
pub struct BannerConfig<'a> {
    /// Version string
    pub version: &'a str,
    /// I/O backend detail (e.g., "io_uring", "mio")
    pub backend_detail: &'a str,
    /// Cache backend (storage type)
    pub cache_backend: CacheBackend,
    /// Eviction policy
    pub eviction_policy: EvictionPolicy,
    /// S3-FIFO small queue percentage
    pub small_queue_percent: u8,
    /// Number of worker threads
    pub workers: usize,
    /// Protocol listeners
    pub listeners: &'a [(Protocol, SocketAddr)],
    /// Metrics address
    pub metrics_address: SocketAddr,
    /// Cache heap size in bytes
    pub heap_size: usize,
    /// Segment size in bytes
    pub segment_size: usize,
    /// Optional CPU affinity list
    pub cpu_affinity: Option<&'a [usize]>,
    /// Optional NUMA node for cache memory
    pub numa_node: Option<u32>,
}

/// Print a startup banner to stdout.
pub fn print_banner(config: &BannerConfig) {
    let mut output = String::with_capacity(512);

    let name = "crucible-server";
    writeln!(output, "{} v{}", name, config.version).unwrap();
    writeln!(
        output,
        "{}",
        "=".repeat(name.len() + config.version.len() + 2)
    )
    .unwrap();
    writeln!(output).unwrap();

    // I/O engine
    writeln!(output, "I/O:         {}", config.backend_detail).unwrap();

    // Cache backend and policy
    let backend_str = match config.cache_backend {
        CacheBackend::Segment => "segment",
        CacheBackend::Slab => "slab",
        CacheBackend::Heap => "heap",
    };
    let policy_str = match config.eviction_policy {
        EvictionPolicy::S3Fifo => "s3fifo",
        EvictionPolicy::Fifo => "fifo",
        EvictionPolicy::Random => "random",
        EvictionPolicy::Cte => "cte",
        EvictionPolicy::Merge => "merge",
        EvictionPolicy::Lfu => "lfu",
        EvictionPolicy::Lra => "lra",
        EvictionPolicy::Lrc => "lrc",
        EvictionPolicy::None => "none",
    };
    if config.eviction_policy == EvictionPolicy::S3Fifo {
        writeln!(
            output,
            "Cache:       {} ({}, small_queue={}%)",
            backend_str, policy_str, config.small_queue_percent
        )
        .unwrap();
    } else {
        writeln!(output, "Cache:       {} ({})", backend_str, policy_str).unwrap();
    }
    writeln!(output, "Workers:     {}", config.workers).unwrap();

    if let Some(cpus) = config.cpu_affinity {
        writeln!(output, "CPU Affinity: {:?}", cpus).unwrap();
    }

    writeln!(output).unwrap();

    // Listeners
    writeln!(output, "Listeners:").unwrap();
    for (protocol, addr) in config.listeners {
        let proto_str = match protocol {
            Protocol::Resp => "RESP",
            Protocol::Memcache => "Memcache",
            Protocol::Momento => "Momento",
        };
        writeln!(output, "  {}: {}", proto_str, addr).unwrap();
    }
    writeln!(output, "  Metrics: {}", config.metrics_address).unwrap();

    writeln!(output).unwrap();

    // Cache configuration
    writeln!(output, "Cache Config:").unwrap();
    writeln!(output, "  Heap:      {}", format_size(config.heap_size)).unwrap();
    writeln!(output, "  Segment:   {}", format_size(config.segment_size)).unwrap();
    writeln!(
        output,
        "  Segments:  {}",
        config.heap_size / config.segment_size
    )
    .unwrap();
    if let Some(node) = config.numa_node {
        writeln!(output, "  NUMA Node: {}", node).unwrap();
    }

    writeln!(output).unwrap();

    print!("{}", output);
}
