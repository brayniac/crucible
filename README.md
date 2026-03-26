# Crucible

A high-performance cache server written in Rust.

## Overview

Crucible provides:

- **Cache Server** (`crucible-server`) - A multi-protocol cache server with support for Redis (RESP) and Memcache protocols

The benchmark tool has been moved to a separate repository: [cachecannon](https://github.com/cachecannon/cachecannon).

## Features

### Cache Server
- **Multiple storage backends**: Segment (default), Slab, Heap
- **RAM → Disk tiering**: Extend cache capacity beyond RAM (segment and slab backends)
- **Pluggable eviction policies**: S3-FIFO, FIFO, LRU variants, Random, CTE, Merge
- **TTL-aware storage**: Segment backend groups items by expiration for efficient proactive expiration
- **Protocol support**: RESP2/RESP3 (Redis-compatible), Memcache ASCII and binary
- **ringline I/O framework**: io_uring event loop with async task-per-connection model (not Tokio) for predictable latency
- **Zero-copy I/O**: io_uring multishot recv, SendZc, ring-provided buffers
- **Zero allocations on hot path**: Pre-allocated buffer pools and segment storage (segment backend)
- **Thread-per-core architecture**: CPU pinning, NUMA-aware allocation, no work-stealing
- **Hugepage support**: 2MB/1GB pages for reduced TLB pressure
- **Prometheus metrics**: Separate control plane for metrics exposition

## Design

Crucible is built for **latency-sensitive deployments where microseconds matter**. Key design decisions:

- **io_uring with async task-per-connection** via [ringline](https://github.com/ringline-rs/ringline)—direct access to io_uring features (multishot, SendMsgZc, ring buffers) without Tokio
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

- **Linux**: Kernel 6.0+ (io_uring required)
- **Architectures**: x86_64 and ARM64
- **Rust**: 1.85+ (Edition 2024) - only required for building from source

## Installation

Pre-built packages are available for Debian/Ubuntu and RHEL/CentOS/Fedora:

```bash
# Debian/Ubuntu
curl -fsSL https://apt.thermitesolutions.com/gpg-key.asc | sudo gpg --dearmor -o /usr/share/keyrings/crucible-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/crucible-archive-keyring.gpg] https://apt.thermitesolutions.com stable main" | sudo tee /etc/apt/sources.list.d/crucible.list
sudo apt update && sudo apt install crucible-server

# RHEL/CentOS/Fedora/Amazon Linux 2023
sudo tee /etc/yum.repos.d/crucible.repo << 'EOF'
[crucible]
name=Crucible Repository
baseurl=https://yum.thermitesolutions.com
enabled=1
gpgcheck=1
gpgkey=https://yum.thermitesolutions.com/gpg-key.asc
EOF
sudo dnf install crucible-server
```

See [docs/INSTALL.md](docs/INSTALL.md) for detailed installation instructions.

## Quick Start

```bash
# Build release binaries
cargo build --release

# Start the cache server
./target/release/crucible-server server/config/example.toml

# In another terminal, run cachecannon (https://github.com/cachecannon/cachecannon)
cachecannon config/redis.toml
```

## Building

```bash
# Build all binaries
cargo build --release

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
| `heap` | System allocator, per-item allocation | No | Variable-size items, data structures (hashes, lists, sets) |

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

### I/O Layer

The server uses [ringline](https://github.com/ringline-rs/ringline), an io_uring event loop with an async task-per-connection model. This gives us:
- **Direct io_uring access**: Multishot recv, SendMsgZc, ring-provided buffers, fixed file descriptors
- **Predictable latency**: Thread-per-core with CPU pinning, no work-stealing
- **Zero-copy potential**: 0 copies with single-shot recv + SendMsgZc on Linux 6.0+

See [docs/design.md](docs/design.md) for design rationale.

## Protocol Support

### Redis (RESP)

Supported commands:
- **Basic**: `GET`, `SET`, `MGET`, `DEL`, `APPEND`, `TYPE`
- **SET options**: `EX` (seconds), `PX` (milliseconds), `NX`, `XX`
- **Counters**: `INCR`, `DECR`, `INCRBY`, `DECRBY`
- **Hashes** (heap backend): `HSET`, `HGET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HSETNX`, `HINCRBY`
- **Lists** (heap backend): `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LTRIM`, `LPUSHX`, `RPUSHX`
- **Sets** (heap backend): `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SPOP`, `SRANDMEMBER`
- **Admin**: `PING`, `HELLO`, `CONFIG GET/SET`
- **No-op**: `FLUSHDB`, `FLUSHALL` (accepted, does nothing)

Not supported: persistence, pub/sub, Lua scripting, streams, sorted sets.

### Memcache

ASCII protocol:
- **Retrieval**: `get`, `gets` (multi-get)
- **Storage**: `set`, `add`, `replace`, `cas`, `append`, `prepend`
- **Deletion**: `delete`
- **Counters**: `incr`, `decr`
- **Admin**: `version`, `quit`, `flush_all` (no-op)

Binary protocol:
- **Retrieval**: `Get`, `GetK`, `GetQ`, `GetKQ`
- **Storage**: `Set`, `SetQ`, `Add`, `Replace`
- **Deletion**: `Delete`, `DeleteQ`
- **Counters**: `Increment`, `Decrement`
- **Admin**: `Noop`, `Version`, `Quit`, `Stat`, `Flush`

## Usage

### Cache Server

```bash
# Run the cache server (requires Linux 6.0+ for io_uring)
./target/release/crucible-server server/config/example.toml
```

#### Configuration

The server separates **storage backend** from **eviction policy**:

```toml
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
```

#### Pre-built Configurations

| Config | Use Case |
|--------|----------|
| `example.toml` | General purpose with segment backend |
| `redis.toml` | Redis migration (RESP on port 6379) |
| `memcached.toml` | Memcached migration (slab backend, port 11211) |
| `heap.toml` | Heap backend for variable-size items |
| `slab.toml` | Slab allocator with LRA eviction |

### Benchmark Tool

For load testing, use [cachecannon](https://github.com/cachecannon/cachecannon):

```bash
curl -fsSL https://cachecannon.cc/install.sh | bash
cachecannon config/redis.toml
```

## Workspace Structure

```
crucible/
├── cache/
│   ├── core/       # Cache traits, hashtable, layers, memory pools
│   ├── segcache/   # Segment-based cache with TTL buckets
│   ├── slab/       # Memcached-style slab allocator
│   └── heap/       # Heap-allocated cache using system allocator
├── protocol/
│   ├── momento/    # Momento cache protocol
│   └── ping/       # Simple ping protocol for testing
├── server/         # Cache server binary
├── proxy/          # Redis proxy with optional local caching
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
- [Benchmark Tool (cachecannon)](https://github.com/cachecannon/cachecannon) - Load testing and performance measurement

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
