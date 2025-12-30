# Crucible

A high-performance cache server and benchmarking toolkit written in Rust.

## Overview

Crucible provides:

- **Cache Server** (`crucible-server`) - A multi-protocol cache server with support for Redis (RESP) and Memcache protocols
- **Benchmark Tool** (`crucible-benchmark`) - A load generator for benchmarking cache servers with detailed latency metrics

## Features

- Multiple cache implementations (S3-FIFO, Segcache)
- Protocol support: RESP2/RESP3 (Redis), Memcache ASCII/Binary, Momento
- Dual I/O backends: io_uring (Linux) and mio (cross-platform)
- Thread-per-core architecture for native runtime
- Optional Tokio runtime support
- CPU pinning for predictable latency
- Prometheus metrics exposition

## Building

```bash
# Build all binaries
cargo build --release

# Build with Tokio runtime support for server
cargo build --release -p server --features tokio-runtime
```

## Usage

### Cache Server

```bash
# Run with native thread-per-core runtime
./target/release/crucible-server config/native.toml

# Run with Tokio runtime
./target/release/crucible-server config/tokio.toml
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
```

## Workspace Structure

```
crucible/
  cache/
    core/       # Cache traits and building blocks
    segcache/   # Segment-based cache with TTL buckets
    s3fifo/     # S3-FIFO eviction policy implementation
  io/
    driver/     # Unified I/O driver (io_uring + mio)
    http2/      # HTTP/2 framing
    grpc/       # gRPC client implementation
  protocol/
    resp/       # Redis RESP2/RESP3 protocol
    memcache/   # Memcache ASCII and binary protocols
    momento/    # Momento cache protocol
  server/       # Cache server binary
  benchmark/    # Benchmark tool binary
```

## Configuration

See example configurations in:
- `server/config/` - Server configuration examples
- `benchmark/config/` - Benchmark configuration examples

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
