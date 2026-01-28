# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Crucible is a high-performance cache server and benchmarking toolkit written in Rust. It provides:

- **Cache Server** (`crucible-server`) - Multi-protocol cache server supporting Redis (RESP) and Memcache protocols
- **Benchmark Tool** (`crucible-benchmark`) - Load generator with detailed latency metrics
- **Cache Libraries** - Multiple cache implementations (Segcache, Slab, Heap)
- **I/O Framework** (`io-driver`) - Unified I/O abstraction with io_uring and mio backends

## Design Philosophy

Crucible is built for **performance-critical deployments where microseconds matter**. The architecture reflects deliberate trade-offs that prioritize latency predictability and throughput over developer convenience.

### Why We Built Our Own I/O Driver (Not Tokio)

Tokio is an excellent general-purpose async runtime, but it abstracts away platform-specific capabilities that are essential for cache server performance:

1. **Completion-based model vs async/await**: Our `io-driver` uses explicit `poll()` → `drain_completions()` loops instead of async/await. This eliminates task scheduler overhead, context switching, and the latency variance introduced by work-stealing. Each worker thread runs a tight loop with predictable behavior.

2. **Direct access to io_uring features**: Tokio's abstractions hide advanced io_uring capabilities that we need:
   - **Multishot recv/accept**: Single submission, multiple completions without re-submission overhead
   - **Ring-provided buffers**: Kernel manages buffer pool with zero-copy semantics
   - **SendZc**: Kernel DMAs directly from user buffers—true zero-copy send
   - **SQPOLL mode**: Kernel thread polls submission queue, eliminating syscalls entirely

3. **Explicit buffer lifecycle management**: Zero-copy I/O requires precise control over when buffers can be released. Async/await complicates lifetime management across await points. Our completion model lets us hold buffer ownership until the kernel signals completion.

4. **Thread-per-core with CPU pinning**: Workers are pinned to specific cores, keeping hot data in CPU cache. Tokio's work-stealing scheduler moves tasks between threads, invalidating caches and adding unpredictable latency.

### Why The Benchmark Tool Doesn't Use Tokio

The benchmark needs to measure latency accurately without introducing measurement artifacts:

- **Precise timing**: Direct `Instant::now()` calls at exact points in the event loop, not filtered through async task scheduling
- **CPU pinning**: Threads pinned to cores for reproducible results across runs
- **Same I/O advantages**: Uses `io-driver` to leverage io_uring on Linux, matching the server's I/O characteristics
- **Dual timestamp modes**: Supports both userspace timing and kernel SO_TIMESTAMPING for sub-microsecond accuracy

The admin/metrics server within the benchmark *does* use Tokio—it's isolated on a separate thread where convenience matters more than latency.

### Native vs Tokio Runtime

The server supports both runtimes via configuration:

| Aspect | Native (default) | Tokio |
|--------|------------------|-------|
| Threading | Explicit thread-per-core | Work-stealing pool |
| I/O | io_uring (Linux 6.0+) / mio | Tokio reactor (epoll/kqueue) |
| CPU pinning | Direct, per-worker | Optional callback |
| Latency | Predictable, low variance | Higher variance from scheduler |
| Use case | Production, performance-critical | Development, portability |

**Choose native** when performance matters. **Choose Tokio** for portability or when integrating with async ecosystems.

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

# Run all fuzz tests (requires nightly, parallel execution)
cargo xtask fuzz-all --duration 60 --jobs 4

# Generate flamegraph (Linux only, requires perf and inferno)
cargo xtask flamegraph --duration 10 --output profile.svg
```

## Running the Server and Benchmark

```bash
# Run cache server with native runtime (io_uring on Linux 6.0+)
./target/release/crucible-server server/config/example.toml

# Run benchmark against a server
./target/release/crucible-benchmark benchmark/config/redis.toml

# Save benchmark results to parquet
./target/release/crucible-benchmark benchmark/config/redis.toml --parquet results.parquet

# View results in web dashboard
./target/release/crucible-benchmark view results.parquet
```

## Architecture

### Workspace Structure

```
crucible/
  cache/
    core/       # Cache traits, segments, hashtable, pools, layers (TieredCache)
    segcache/   # Segment-based cache with TTL buckets
    slab/       # Memcached-style slab allocator with slab-level eviction
    heap/       # Heap-allocated cache using system allocator
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

### I/O Driver Design

The `io-driver` crate provides a unified completion-based I/O abstraction:

```rust
// The core event loop pattern used throughout Crucible
loop {
    driver.poll(Some(Duration::from_millis(1)))?;
    for completion in driver.drain_completions() {
        match completion.kind {
            CompletionKind::Recv { conn_id, .. } => { /* process recv */ }
            CompletionKind::SendReady { conn_id } => { /* send response */ }
            // ...
        }
    }
}
```

**Backend selection** (automatic):
- **Linux 6.0+**: io_uring with multishot recv/accept, SendZc, ring-provided buffers
- **Other platforms**: mio (epoll on Linux, kqueue on macOS)

**Copy semantics** (why this matters for performance):
- io_uring single-shot recv + SendZc: **0 copies** (kernel writes to user buffer, DMAs response directly)
- io_uring multishot recv + SendZc: **1 copy** (ring buffer → coalesce buffer)
- mio fallback: **2 copies** (standard read/write syscalls)

See `io/driver/ARCHITECTURE.md` for detailed buffer management documentation.

### Cache Architecture (cache-core)

The cache uses a tiered S3-FIFO architecture with a shared lock-free hashtable:

```
+---------------------------+
|        Hashtable          |  <- Lock-free multi-choice hashtable
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

**Key components:**
- `TieredCache` - Orchestrates layers with shared hashtable
- `Segment` - Fixed-size memory regions storing items sequentially
- `MemoryPool` - Segment allocation with optional hugepage support
- `MultiChoiceHashtable` - Lock-free hashtable with ghost entries for "second chance" admission

### Concurrency Model

- All public types are `Send + Sync`
- Hashtable uses lock-free CAS operations
- Segments use atomic state machine transitions
- Reference counting prevents segment clearing during active reads
- The `loom` feature enables deterministic concurrency testing

## Configuration

Server config files (`server/config/`):
- `example.toml` - General-purpose configuration
- `redis.toml` - Redis migration (RESP protocol on port 6379)
- `memcached.toml` - Memcached migration (port 11211)
- `heap.toml` - Heap backend (no segment fragmentation)
- `slab.toml` - Slab allocator with slab-level eviction

Cache configuration separates **backend** (storage) from **policy** (eviction):

```toml
[cache]
backend = "segment"  # Storage: segment, slab, heap
policy = "s3fifo"    # Eviction policy (depends on backend)
```

**Eviction policies by backend:**
- `segment`: s3fifo (default), fifo, random, cte, merge
- `slab`: lra (default), lrc, random, none
- `heap`: s3fifo (default), lfu

## Platform Support

- **Linux 6.0+**: Full support with io_uring, CPU pinning, NUMA binding, hugepages
- **Linux < 6.0**: mio backend (epoll), still supports CPU pinning
- **macOS**: mio backend only (kqueue)
- **Architectures**: x86_64 and ARM64

## Testing

- `/smoketest` - Quick end-to-end validation (starts server, runs benchmark, checks results)
- Fuzz tests require nightly: `rustup run nightly cargo fuzz`
- Loom tests (`--features loom`) verify lock-free concurrency but run slowly
- Protocol fuzz targets in `protocol/*/fuzz/`
