# Development Guide

This guide covers development workflows, testing, and contributing to Crucible.

## Building

```bash
# Debug build
cargo build

# Release build (with LTO)
cargo build --release

# Build specific crate
cargo build -p cache-core
cargo build -p server
```

## Running Tests

### Unit and Integration Tests

```bash
# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p cache-core
cargo test -p server
cargo test -p protocol-resp

# Run a specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture
```

### Loom Concurrency Tests

[Loom](https://github.com/tokio-rs/loom) exhaustively explores thread interleavings to find concurrency bugs in lock-free data structures. This is critical for the hashtable and cache layer code.

```bash
# Run loom tests for cache-core
cargo test -p cache-core --features loom

# Run loom tests for slab cache
cargo test -p cache-slab --features loom

# Run loom tests for heap cache
cargo test -p cache-heap --features loom
```

**Note:** Loom tests can be slow because they explore many interleavings. They're not run in CI on every commit but should be run before major changes to concurrent code.

### Fuzz Testing

Fuzz testing finds edge cases in parsing and encoding by generating random inputs. We fuzz all protocol parsers and cache header formats.

Requires nightly Rust:

```bash
# Install cargo-fuzz if needed
cargo install cargo-fuzz

# Run all fuzz targets (60 seconds each by default)
cargo xtask fuzz-all

# Run with custom duration (in seconds)
cargo xtask fuzz-all --duration 300

# Run multiple fuzz targets in parallel
cargo xtask fuzz-all --jobs 4

# Run a specific fuzz target manually
cd protocol/resp/fuzz
cargo +nightly fuzz run fuzz_command_parse
```

**Available fuzz targets:**

| Crate | Targets |
|-------|---------|
| `cache/core/fuzz` | `fuzz_basic_header`, `fuzz_ttl_header` |
| `protocol/resp/fuzz` | `fuzz_value_parse`, `fuzz_command_parse` |
| `protocol/memcache/fuzz` | `fuzz_ascii_command`, `fuzz_ascii_response`, `fuzz_binary_command`, `fuzz_binary_response` |
| `protocol/momento/fuzz` | `fuzz_proto_decode`, `fuzz_get_response`, `fuzz_set_request` |
| `protocol/ping/fuzz` | `fuzz_response` |
| `io/grpc/fuzz` | `fuzz_decode_message`, `fuzz_message_decoder` |
| `io/http2/fuzz` | `fuzz_frame_decode`, `fuzz_hpack_decode` |

### Validation Mode

The `validation` feature enables runtime checks that help catch bugs during development:

- Magic bytes in item headers to detect corruption
- Checksums to verify data integrity
- Additional assertions on invariants

```bash
# Build server with validation enabled
cargo build -p server --features validation

# Run with validation (useful for debugging)
./target/debug/crucible-server server/config/example.toml
```

**Note:** Validation adds overhead and is not recommended for production or benchmarking.

## Profiling

### Flamegraphs

Generate CPU flamegraphs of the server under load:

```bash
# Generate flamegraph (requires perf on Linux, DTrace on macOS)
cargo xtask flamegraph --duration 10 --output profile.svg

# With custom configs
cargo xtask flamegraph \
    --config server/config/example.toml \
    --bench-config benchmark/config/redis.toml \
    --duration 30 \
    --output profile.svg

# Profile without load (idle server)
cargo xtask flamegraph --no-load --output idle.svg

# Profile Tokio runtime
cargo xtask flamegraph --runtime tokio --output tokio-profile.svg
```

### Perf

For more detailed profiling on Linux:

```bash
# Record with perf
sudo perf record -g ./target/release/crucible-server server/config/example.toml

# In another terminal, run load
./target/release/crucible-benchmark benchmark/config/redis.toml

# Analyze
sudo perf report
```

## Code Quality

### Formatting

```bash
# Format all Rust code
cargo fmt

# Check formatting without modifying
cargo fmt --check
```

### Linting

```bash
# Run clippy
cargo clippy

# Run clippy on all targets
cargo clippy --all-targets

# Treat warnings as errors (CI mode)
cargo clippy -- -D warnings
```

## Smoke Testing

Quick validation that the server and benchmark work together:

```bash
# Build release binaries
cargo build --release

# Start server in background
./target/release/crucible-server server/config/example.toml &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Run quick benchmark
./target/release/crucible-benchmark benchmark/config/quick-test.toml

# Verify with redis-cli
redis-cli -p 6379 PING
redis-cli -p 6379 SET foo bar
redis-cli -p 6379 GET foo

# Cleanup
kill $SERVER_PID
```

## Project Structure

```
crucible/
├── cache/
│   ├── core/           # Traits, hashtable, segments, pools
│   │   └── fuzz/       # Fuzz tests for cache headers
│   ├── segcache/       # Segment-based cache
│   ├── slab/           # Slab allocator cache
│   └── heap/           # Heap-allocated cache
├── io/
│   ├── driver/         # io_uring + mio abstraction
│   ├── http2/          # HTTP/2 framing
│   │   └── fuzz/       # Fuzz tests for HTTP/2
│   └── grpc/           # gRPC client
│       └── fuzz/       # Fuzz tests for gRPC
├── protocol/
│   ├── resp/           # Redis protocol
│   │   └── fuzz/       # Fuzz tests for RESP
│   ├── memcache/       # Memcache protocol
│   │   └── fuzz/       # Fuzz tests for Memcache
│   ├── momento/        # Momento protocol
│   │   └── fuzz/       # Fuzz tests for Momento
│   └── ping/           # Ping protocol
│       └── fuzz/       # Fuzz tests for Ping
├── server/             # Cache server binary
├── benchmark/          # Benchmark tool binary
├── metrics/            # Metrics infrastructure
└── xtask/              # Development tasks
```

## Adding a New Fuzz Target

1. Create a `fuzz` directory in your crate
2. Add `Cargo.toml`:

```toml
[package]
name = "my-crate-fuzz"
version = "0.0.0"
edition = "2024"
publish = false

[package.metadata]
cargo-fuzz = true

[dependencies]
libfuzzer-sys = "0.4"
my-crate = { path = ".." }

[[bin]]
name = "fuzz_my_target"
path = "fuzz_targets/fuzz_my_target.rs"
test = false
doc = false
bench = false
```

3. Create `fuzz_targets/fuzz_my_target.rs`:

```rust
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Your fuzzing code here
    let _ = my_crate::parse(data);
});
```

4. Run with `cargo +nightly fuzz run fuzz_my_target`

## Adding Loom Tests

For concurrent code, add loom tests:

1. Add loom as an optional dependency:

```toml
[features]
loom = ["dep:loom"]

[dependencies]
loom = { version = "0.7", optional = true }
```

2. Use conditional compilation:

```rust
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};
```

3. Write loom tests:

```rust
#[cfg(loom)]
#[test]
fn test_concurrent_access() {
    loom::model(|| {
        // Your concurrent test here
    });
}
```

## CI Pipeline

The CI runs:
1. `cargo fmt --check` - Formatting
2. `cargo clippy` - Linting
3. `cargo test` - Unit and integration tests
4. `cargo build --release` - Release build verification

Fuzz tests and loom tests are run periodically, not on every commit.
