# Crucible

A high-performance cache server and benchmarking toolkit written in Rust.

## Overview

Crucible provides:

- **Cache Server** (`crucible-server`) - A multi-protocol cache server with support for Redis (RESP) and Memcache protocols
- **Benchmark Tool** (`crucible-benchmark`) - A load generator for benchmarking cache servers with detailed latency metrics

## Features

### Cache Server
- Multiple storage backends: Segment (default), Slab, Heap
- Pluggable eviction policies: S3-FIFO, FIFO, LRU variants, Random, CTE
- Protocol support: RESP2/RESP3 (Redis-compatible), Memcache ASCII
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

## Architecture

### Cache Design

Crucible uses a tiered cache architecture with a shared lock-free hashtable:

```
┌─────────────────────────────────────┐
│           Hashtable                 │  Lock-free multi-choice hashtable
│  key → (location, freq, ghost bit)  │  with ghost entries for "second chance"
└─────────────────┬───────────────────┘
                  │
         ┌────────┴────────┐
         ▼                 ▼
┌─────────────────┐ ┌─────────────────┐
│    Layer 0      │ │    Layer 1      │
│  (FIFO Queue)   │ │  (TTL Buckets)  │
│                 │ │                 │
│  New items      │ │  Hot items      │
│  enter here     │ │  promoted here  │
└────────┬────────┘ └─────────────────┘
         │
         └─→ Eviction: items with freq > threshold
             are demoted to Layer 1
```

**Key features:**
- Ghost entries track recently-evicted keys for admission filtering
- Frequency counter prevents scan-resistant pollution
- Items start in FIFO layer, hot items promoted to TTL layer

### Storage Backends

| Backend | Description | Best For |
|---------|-------------|----------|
| `segment` | Fixed-size segments with sequential item storage | General purpose, good hit rates with S3-FIFO |
| `slab` | Memcached-style slab allocator with size classes | Memcached migration, high churn workloads |
| `heap` | System allocator, per-item allocation | Variable-size items, no fragmentation concerns |

### Eviction Policies

| Backend | Available Policies |
|---------|-------------------|
| `segment` | `s3fifo` (default), `fifo`, `random`, `cte`, `merge` |
| `slab` | `lra` (default), `lrc`, `random`, `none` |
| `heap` | `s3fifo` (default), `lfu` |

- **s3fifo**: Admission-filtered FIFO with frequency counter
- **fifo**: Simple first-in-first-out
- **lra/lrc**: Least recently accessed/created (slab only)
- **lfu**: Least frequently used
- **cte**: Closest to expiration
- **random**: Random eviction
- **merge**: Merge eviction strategy

### I/O Driver

Unified async I/O abstraction with automatic backend selection:
- **Linux 6.0+**: io_uring with multishot recv/accept, SendZc, ring-provided buffers
- **Other platforms**: mio (epoll on Linux, kqueue on macOS)

See [io/driver/ARCHITECTURE.md](io/driver/ARCHITECTURE.md) for detailed copy/allocation analysis.

## Protocol Support

### Redis (RESP)

Supported commands:
- **Basic**: `GET`, `SET`, `MGET`, `DEL`
- **SET options**: `EX` (seconds), `PX` (milliseconds), `NX`, `XX`
- **Counters**: `INCR`, `DECR`, `INCRBY`, `DECRBY`
- **Admin**: `PING`, `HELLO`, `CONFIG GET/SET`
- **No-op**: `FLUSHDB`, `FLUSHALL` (accepted, does nothing)

Not supported: data structures (lists, sets, hashes), persistence, pub/sub, Lua scripting.

### Memcache

Supported commands:
- **Retrieval**: `get`, `gets` (multi-get)
- **Storage**: `set`, `add`, `replace`, `cas`
- **Deletion**: `delete`
- **Counters**: `incr`, `decr`
- **Admin**: `version`, `quit`, `flush_all` (no-op)

## Usage

### Cache Server

```bash
# Run with native runtime (io_uring on Linux 6.0+, mio fallback)
./target/release/crucible-server server/config/example.toml

# Run with Tokio runtime
./target/release/crucible-server server/config/tokio.toml
```

#### Configuration

The server separates **storage backend** from **eviction policy**:

```toml
runtime = "native"

[workers]
threads = 4
# cpu_affinity = "0-3"  # Pin to specific CPUs

[cache]
# Storage backend: "segment", "slab", or "heap"
backend = "segment"

# Eviction policy (available policies depend on backend)
policy = "s3fifo"

# Memory allocation
heap_size = "1GB"
segment_size = "8MB"      # segment/slab only
max_value_size = "1MB"    # segment only

# Hashtable sizing: 2^power buckets
# Size for ~50% load factor of expected item count
hashtable_power = 20

# Hugepage support (Linux only): "none", "2mb", "1gb"
# hugepage = "2mb"

# NUMA binding (Linux only)
# numa_node = 0

# Protocol listeners
[[listener]]
protocol = "resp"
address = "127.0.0.1:6379"

[[listener]]
protocol = "memcache"
address = "127.0.0.1:11211"

[metrics]
address = "127.0.0.1:9090"

# io_uring settings (Linux only)
[uring]
sqpoll = false
buffer_count = 2048
buffer_size = "16KB"
sq_depth = 1024
recv_mode = "multishot"  # or "singleshot"
```

#### Pre-built Configurations

| Config | Use Case |
|--------|----------|
| `example.toml` | General purpose with segment backend |
| `redis.toml` | Redis migration (RESP on port 6379) |
| `memcached.toml` | Memcached migration (slab backend, port 11211) |
| `heap.toml` | Heap backend for variable-size items |
| `slab.toml` | Slab allocator with LRA eviction |
| `tokio.toml` | Tokio runtime instead of native |

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
- `redis.toml` - Redis RESP protocol
- `memcache.toml` - Memcache protocol
- `momento.toml` - Momento gRPC protocol
- `ping.toml` - Simple ping test
- `quick-test.toml` - Quick validation test

## Workspace Structure

```
crucible/
├── cache/
│   ├── core/       # Cache traits, hashtable, layers, memory pools
│   ├── segcache/   # Segment-based cache with TTL buckets
│   ├── slab/       # Memcached-style slab allocator
│   └── heap/       # Heap-allocated cache using system allocator
├── io/
│   ├── driver/     # Unified I/O driver (io_uring + mio backends)
│   ├── http2/      # HTTP/2 framing
│   └── grpc/       # gRPC client implementation
├── protocol/
│   ├── resp/       # Redis RESP2/RESP3 protocol
│   ├── memcache/   # Memcache ASCII protocol
│   ├── momento/    # Momento cache protocol
│   └── ping/       # Simple ping protocol for testing
├── server/         # Cache server binary
├── benchmark/      # Benchmark tool binary
├── metrics/        # Metrics infrastructure
└── xtask/          # Development tasks (fuzz testing, flamegraphs)
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
   ```bash
   echo 2048 > /proc/sys/vm/nr_hugepages
   ```
2. **CPU pinning**: Use `cpu_affinity` to pin workers to specific cores
3. **NUMA awareness**: Set `numa_node` to bind cache memory to the local NUMA node
4. **io_uring tuning**: Adjust `buffer_count`, `buffer_size`, and `sq_depth` in `[uring]` section
5. **Disable SMT**: For latency-sensitive workloads, consider pinning to physical cores only

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
