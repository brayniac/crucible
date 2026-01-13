# Crucible

A high-performance cache server and benchmarking toolkit written in Rust.

## Overview

Crucible provides:

- **Cache Server** (`crucible-server`) - A multi-protocol cache server with support for Redis (RESP) and Memcache protocols
- **Benchmark Tool** (`crucible-benchmark`) - A load generator for benchmarking cache servers with detailed latency metrics

## Features

### Cache Server
- Multiple cache backends: S3-FIFO (admission-filtered), Segcache (TTL-organized)
- Protocol support: RESP2/RESP3 (Redis-compatible), Memcache ASCII/Binary
- Dual runtime backends: Native (io_uring/mio) or Tokio
- io_uring optimizations: multishot recv/accept, zero-copy send, ring-provided buffers
- Thread-per-core architecture with CPU pinning and NUMA-aware allocation
- Hugepage support (2MB/1GB) for reduced TLB pressure
- Prometheus metrics exposition

### Benchmark Tool
- Multi-threaded load generation with io_uring or mio backends
- Configurable workloads: key distribution (uniform/zipf), command mix, value sizes
- Request pipelining for maximum throughput
- High-resolution latency histograms (p50, p90, p99, p99.9, p99.99)
- Parquet output for post-hoc analysis
- Multiple output formats (clean, verbose, JSON, quiet)

## Requirements

- **Rust**: Edition 2024 (nightly recommended for best performance)
- **Linux**: Kernel 6.0+ for io_uring features (falls back to mio on older kernels)
- **macOS**: Supported via mio backend

## Quick Start

```bash
# Build release binaries
cargo build --release

# Start the cache server
./target/release/crucible-server server/config/example.toml

# In another terminal, run the benchmark
./target/release/crucible-benchmark benchmark/config/redis.toml
```

## Building

```bash
# Build all binaries (includes Tokio runtime and io_uring by default)
cargo build --release

# Build without Tokio runtime
cargo build --release -p server --no-default-features --features io_uring

# Build with validation checks (for debugging)
cargo build --release -p server --features validation
```

## Usage

### Cache Server

```bash
# Run with native runtime (io_uring on Linux 6.0+, mio fallback)
./target/release/crucible-server server/config/example.toml

# Run with Tokio runtime
./target/release/crucible-server server/config/tokio.toml
```

Example server configuration (`server/config/example.toml`):
```toml
runtime = "native"

[workers]
threads = 4
# cpu_affinity = "0-3"  # Pin to specific CPUs

[cache]
backend = "segcache"    # or "s3fifo"
heap_size = "1GB"
segment_size = "8MB"
# hugepage = "2mb"      # Use hugepages
# numa_node = 0         # Bind to NUMA node

[[listener]]
protocol = "resp"
address = "127.0.0.1:6379"

[[listener]]
protocol = "memcache"
address = "127.0.0.1:11211"

[metrics]
address = "127.0.0.1:9090"
```

### Benchmark Tool

```bash
# Run benchmark against a Redis-compatible server
./target/release/crucible-benchmark benchmark/config/redis.toml

# Override settings via CLI
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --threads 4 \
    --connections 32 \
    --rate 100000

# Output to Parquet for analysis
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --parquet results.parquet

# JSON output format
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --format json
```

Available benchmark configurations:
- `benchmark/config/redis.toml` - Redis RESP protocol
- `benchmark/config/memcache.toml` - Memcache protocol
- `benchmark/config/momento.toml` - Momento gRPC protocol
- `benchmark/config/quick-test.toml` - Quick validation test

## Workspace Structure

```
crucible/
  cache/
    core/       # Cache traits, segments, hashtable, pools, layers
    segcache/   # Segment-based cache with TTL buckets
    s3fifo/     # S3-FIFO eviction policy (admission filter + main cache)
  io/
    driver/     # Unified I/O driver (io_uring + mio backends)
    http2/      # HTTP/2 framing
    grpc/       # gRPC client implementation
  protocol/
    resp/       # Redis RESP2/RESP3 protocol
    memcache/   # Memcache ASCII and binary protocols
    momento/    # Momento cache protocol
    ping/       # Simple ping protocol for testing
  server/       # Cache server binary
  benchmark/    # Benchmark tool binary
  xtask/        # Development tasks (fuzz testing, flamegraphs)
  metrics/      # Metrics infrastructure
```

## Development

```bash
# Run tests
cargo test

# Run tests with concurrency checking (loom)
cargo test -p cache-core --features loom

# Run all fuzz tests (requires nightly)
cargo xtask fuzz-all --duration 60

# Generate flamegraph under load
cargo xtask flamegraph --duration 10 --output profile.svg

# Format and lint
cargo fmt
cargo clippy
```

## Performance Tuning

For best performance on Linux:

1. **Enable hugepages**: Configure 2MB or 1GB hugepages and set `hugepage = "2mb"` in config
2. **CPU pinning**: Use `cpu_affinity` to pin workers to specific cores
3. **NUMA awareness**: Set `numa_node` to bind cache memory to the local NUMA node
4. **io_uring tuning**: Adjust `buffer_count`, `buffer_size`, and `sq_depth` in `[uring]` section
5. **Disable SMT**: For latency-sensitive workloads, consider pinning to physical cores only

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
