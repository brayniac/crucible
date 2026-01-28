# Crucible

A high-performance cache server and benchmarking toolkit written in Rust.

## Overview

Crucible provides:

- **Cache Server** (`crucible-server`) - A multi-protocol cache server with support for Redis (RESP) and Memcache protocols
- **Benchmark Tool** (`crucible-benchmark`) - A load generator for benchmarking cache servers with detailed latency metrics

## Features

### Cache Server
- **Multiple storage backends**: Segment (default), Slab, Heap
- **RAM → Disk tiering**: Extend cache capacity beyond RAM (segment and slab backends)
- **Pluggable eviction policies**: S3-FIFO, FIFO, LRU variants, Random, CTE, Merge
- **TTL-aware storage**: Segment backend groups items by expiration for efficient proactive expiration
- **Protocol support**: RESP2/RESP3 (Redis-compatible), Memcache ASCII
- **Custom I/O driver**: Completion-based io_uring/mio abstraction (not Tokio) for predictable latency
- **Zero-copy I/O**: io_uring multishot recv, SendZc, ring-provided buffers
- **Zero allocations on hot path**: Pre-allocated buffer pools and segment storage (segment backend)
- **Thread-per-core architecture**: CPU pinning, NUMA-aware allocation, no work-stealing
- **Hugepage support**: 2MB/1GB pages for reduced TLB pressure
- **Prometheus metrics**: Separate control plane for metrics exposition

### Benchmark Tool
- Multi-threaded load generation with the same io-driver as the server
- Configurable workloads: key distribution (uniform/zipf), command mix, value sizes
- Request pipelining for maximum throughput
- High-resolution latency histograms (p50, p90, p99, p99.9, p99.99)
- Precise timing without async runtime overhead
- Parquet output for post-hoc analysis
- **Web dashboard viewer**: Interactive analysis of benchmark results with optional Rezolus telemetry correlation

## Design

Crucible is built for **latency-sensitive deployments where microseconds matter**. Key design decisions:

- **Completion-based I/O** instead of async/await—eliminates task scheduler overhead
- **Custom io-driver** instead of Tokio—direct access to io_uring features (multishot, SendZc, ring buffers)
- **Thread-per-core** with CPU pinning—no work-stealing, predictable cache locality
- **Pre-allocated everything**—buffer pools and segment storage avoid malloc on hot path

See [docs/design.md](docs/design.md) for detailed rationale.

## Development Approach

Crucible was developed using AI-assisted coding, guided by years of experience building high-performance cache systems. The architectural decisions come from domain expertise; AI accelerated the implementation.

We pair rapid development with rigorous verification for both correctness and performance:

- **Rust's compiler** catches memory safety and data race bugs at compile time
- **Fuzz testing** on all protocol parsers and data encoding
- **Loom testing** for exhaustive concurrency verification of lock-free structures
- **Benchmarking and profiling** to validate performance and identify bottlenecks
- **Validation mode** with runtime assertions for development builds

See [docs/design.md](docs/design.md#development-philosophy) for our full philosophy on AI-assisted development.

## Requirements

- **Linux**: Kernel 6.0+ for full io_uring features (falls back to mio on older kernels)
- **macOS**: Supported via mio backend
- **Rust**: 1.85+ (Edition 2024) - only required for building from source

## Installation

Pre-built packages are available for Debian/Ubuntu and RHEL/CentOS/Fedora:

```bash
# Debian/Ubuntu
curl -fsSL https://apt.thermitesolutions.com/gpg-key.asc | sudo gpg --dearmor -o /usr/share/keyrings/crucible-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/crucible-archive-keyring.gpg] https://apt.thermitesolutions.com stable main" | sudo tee /etc/apt/sources.list.d/crucible.list
sudo apt update && sudo apt install crucible-server crucible-benchmark

# RHEL/CentOS/Fedora/Amazon Linux 2023
sudo tee /etc/yum.repos.d/crucible.repo << 'EOF'
[crucible]
name=Crucible Repository
baseurl=https://yum.thermitesolutions.com
enabled=1
gpgcheck=1
gpgkey=https://yum.thermitesolutions.com/gpg-key.asc
EOF
sudo dnf install crucible-server crucible-benchmark
```

See [docs/INSTALL.md](docs/INSTALL.md) for detailed installation instructions.

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

All storage backends share a **lock-free hashtable** with optional tiering:

```
┌───────────────────────────────────────┐
│              Hashtable                │
│  key → (location, frequency, ghost)   │
└──────┬─────────────┬─────────────┬────┘
       │             │             │
       ▼             ▼             ▼
┌ ─ ─ ─ ─ ─ ─ ┐ ┌─────────────┐ ┌ ─ ─ ─ ─ ─ ─ ┐
  Layer 0:      │  Layer 1:   │    Layer 2:
│ Admission   │ │  Main Cache │ │   Disk      │
  (optional)    │  (RAM)      │   (optional)
│ S3-FIFO     │ │  seg/slab/  │ │ seg/slab    │
  only          │  heap       │   only
└ ─ ─ ─ ─ ─ ─ ┘ └─────────────┘ └ ─ ─ ─ ─ ─ ─ ┘
       │            ▲    │          ▲    │
       └── admit ───┘    │          │    │
                    ▲    └─ demote ─┘    │
                    └────── promote ─────┘
```

**Key features:**
- Lock-free multi-choice hashing with atomic CAS operations
- Ghost entries track recently-evicted keys for smarter re-admission
- Per-key frequency counter enables scan-resistant policies

### Storage Backends

| Backend | Description | Disk Tiering | Best For |
|---------|-------------|--------------|----------|
| `segment` | Fixed-size segments, TTL-aware organization | Yes | General purpose, TTL-heavy workloads |
| `slab` | Memcached-style slab allocator with size classes | Yes | Memcached migration, in-place updates |
| `heap` | System allocator, per-item allocation | No | Variable-size items, future data structures |

### Eviction Policies

| Backend | Available Policies |
|---------|-------------------|
| `segment` | `s3fifo` (default), `fifo`, `random`, `cte`, `merge` |
| `slab` | `lra` (default), `lrc`, `random`, `none` |
| `heap` | `s3fifo` (default), `lfu` |

- **s3fifo**: Admission-filtered FIFO with frequency tracking—scan resistant, high hit rates
- **fifo**: Simple first-in-first-out
- **lra/lrc**: Least recently accessed/created (slab only)
- **lfu**: Least frequently used
- **cte**: Closest to expiration
- **random**: Random eviction
- **merge**: Frequency-aware pruning—retains high-value items, evicts low-value ones (segment only)

### I/O Driver

We built a custom completion-based I/O driver instead of using Tokio. This gives us:
- **Direct io_uring access**: Multishot recv/accept, SendZc, ring-provided buffers, SQPOLL
- **Predictable latency**: No async task scheduler overhead or work-stealing
- **Zero-copy potential**: 0 copies with single-shot recv + SendZc on Linux 6.0+
- **Graceful fallback**: mio backend (epoll/kqueue) on older kernels and macOS

See [io/driver/ARCHITECTURE.md](io/driver/ARCHITECTURE.md) for buffer management details and [docs/design.md](docs/design.md) for design rationale.

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

# Output to Parquet for analysis
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --parquet results.parquet

# View results in web dashboard (opens browser automatically)
./target/release/crucible-benchmark view results.parquet

# Correlate with Rezolus system telemetry
./target/release/crucible-benchmark view results.parquet \
    --server server-rezolus.parquet \
    --client client-rezolus.parquet
```

The viewer provides interactive dashboards for throughput, latency percentiles, cache hit rates, and connection statistics. When combined with [Rezolus](https://github.com/brayniac/rezolus) parquet files, you can correlate benchmark performance with CPU, network, and scheduler metrics.

Available benchmark configurations:
- `redis.toml` - Redis RESP protocol
- `memcache.toml` - Memcache protocol
- `momento.toml` - Momento gRPC protocol
- `ping.toml` - Simple ping test
- `quick-test.toml` - Quick validation test

See [benchmark/README.md](benchmark/README.md) for complete configuration reference and tuning guide.

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

## Documentation

- [Design Philosophy](docs/design.md) - Why we built things the way we did
- [Architecture](docs/architecture.md) - Component details and data flow
- [Configuration Reference](docs/configuration.md) - Complete config options
- [Operations Guide](docs/operations.md) - Production deployment and tuning
- [Development Guide](docs/development.md) - Testing, profiling, and contributing
- [I/O Driver Architecture](io/driver/ARCHITECTURE.md) - Buffer management and copy semantics
- [Benchmark Guide](benchmark/README.md) - Load testing and performance measurement

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
