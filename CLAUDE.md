# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Crucible is a high-performance cache server and benchmarking toolkit written in Rust. It provides:

- **Cache Server** (`crucible-server`) - Multi-protocol cache server supporting Redis (RESP) and Memcache protocols
- **Benchmark Tool** (`crucible-benchmark`) - Load generator with detailed latency metrics
- **Cache Libraries** - Segment-based cache implementations (S3-FIFO, Segcache)
- **I/O Framework** (`io-driver`) - Unified I/O abstraction with io_uring and mio backends

## Build Commands

```bash
# Build the workspace
cargo build

# Build for release (with LTO)
cargo build --release

# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p cache-core
cargo test -p server
cargo test -p benchmark

# Run a specific test
cargo test test_name

# Run tests with loom for concurrency testing
cargo test -p cache-core --features loom

# Format code
cargo fmt

# Lint with clippy
cargo clippy

# Run all fuzz tests (requires nightly)
cargo xtask fuzz-all --duration 60

# Generate flamegraph
cargo xtask flamegraph --duration 10 --output profile.svg
```

## Running the Server and Benchmark

```bash
# Run cache server with native runtime (io_uring on Linux 6.0+)
./target/release/crucible-server server/config/example.toml

# Run benchmark against a server
./target/release/crucible-benchmark benchmark/config/example.toml

# Override benchmark settings via CLI
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --threads 4 --connections 32 --rate 100000
```

## Architecture

### Workspace Structure

```
crucible/
  cache/
    core/       # Cache traits, segments, hashtable, pools, layers
    segcache/   # Segment-based cache with TTL buckets
    s3fifo/     # S3-FIFO eviction policy (FIFO admission + main cache)
  io/
    driver/     # Unified I/O driver (io_uring + mio backends)
    http2/      # HTTP/2 framing
    grpc/       # gRPC client implementation
  protocol/
    resp/       # Redis RESP2/RESP3 protocol
    memcache/   # Memcache ASCII and binary protocols
    momento/    # Momento cache protocol
    ping/       # Simple ping protocol for testing
  server/       # Cache server binary with native/tokio runtimes
  benchmark/    # Benchmark tool binary
  xtask/        # Development tasks (fuzz-all, flamegraph)
  metrics/      # Metrics infrastructure
```

### Cache Architecture (cache-core)

The cache uses a tiered layer architecture:

```
+---------------------------+
|        Hashtable          |  <- Lock-free cuckoo hashtable
| (location + freq + ghost) |     Key -> (ItemLocation, Frequency)
+---------------------------+
             |
             v
      +-------------+
      |   Layer 0   |  <- Admission queue (FIFO organization)
      | (FifoLayer) |     New items enter here
      +------+------+
             | evict (demote items with freq > threshold)
             v
      +-------------+
      |   Layer 1   |  <- Main cache (TTL bucket organization)
      | (TtlLayer)  |     Hot items promoted here
      +-------------+
```

**Key Components:**
- `TieredCache` - Orchestrates layers with shared hashtable
- `FifoLayer` / `TtlLayer` - Layer implementations with different eviction strategies
- `Segment` - Fixed-size memory regions storing items sequentially
- `MemoryPool` - Segment allocation with optional hugepage support
- `CuckooHashtable` - Lock-free hashtable with ghost entries for "second chance"

**Eviction Strategies:** FIFO, Random, CTE (closest to expiration), Merge

### I/O Driver Architecture (io-driver)

Unified async I/O abstraction with automatic backend selection:
- **Linux 6.0+**: io_uring with multishot recv/accept, SendZc, ring-provided buffers
- **Other platforms**: mio (epoll on Linux, kqueue on macOS)

The driver provides a completion-based event loop: `poll()` -> `drain_completions()` -> process events.

### Server Runtime Options

- **Native** (default): Thread-per-core with io_uring/mio, CPU pinning support
- **Tokio**: Async runtime with work-stealing scheduler (feature flag: `tokio-runtime`)

## Concurrency Model

- All public types are `Send + Sync`
- Hashtable uses lock-free CAS operations
- Segments use atomic state machine transitions
- Reference counting prevents segment clearing during active reads
- The `loom` feature enables deterministic concurrency testing

## Configuration

Server config (`server/config/example.toml`):
- Runtime selection (native/tokio)
- Cache backend (segcache/s3fifo), heap size, segment size
- Protocol listeners (resp/memcache) with optional TLS
- io_uring settings (sqpoll, buffer pools, recv mode)

Benchmark config (`benchmark/config/example.toml`):
- Target endpoints and protocol
- Connection pool settings (pool_size, pipeline_depth)
- Workload (keyspace, command distribution, value sizes)

## Platform Support

- **Linux**: Full support including io_uring (kernel 6.0+), NUMA binding
- **macOS**: mio backend only
- **Architectures**: x86_64 and ARM64
