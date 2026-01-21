# Crucible Benchmark

A high-performance load generator for cache servers with support for multiple protocols, detailed latency measurements, and flexible workload configuration.

## Quick Start

```bash
# Build the benchmark tool
cargo build --release -p benchmark

# Run against a local Redis server
./target/release/crucible-benchmark benchmark/config/redis.toml

# Run against a local Memcached server
./target/release/crucible-benchmark benchmark/config/memcache.toml

# Quick validation test (5 seconds)
./target/release/crucible-benchmark benchmark/config/quick-test.toml
```

## Command Line Interface

```
crucible-benchmark <CONFIG> [OPTIONS]

Arguments:
  <CONFIG>  Path to TOML configuration file

Options:
  --threads <N>         Override worker thread count
  --connections <N>     Override total connection count
  --rate <N>            Override rate limit (requests/second)
  --format <FORMAT>     Output format: clean, json, verbose, quiet
  --color <MODE>        Color mode: auto, always, never
  --prometheus <ADDR>   Enable Prometheus metrics endpoint
  --parquet <PATH>      Write metrics to Parquet file
  --cpu_list <LIST>     CPU pinning (e.g., "0-3,8-11")
  --io_engine <ENGINE>  I/O engine: auto, mio, uring
  --recv_mode <MODE>    Recv mode: multishot, singleshot
  -h, --help            Print help
```

### Examples

```bash
# Override settings via CLI
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --threads 8 \
    --connections 64 \
    --rate 100000

# Export metrics to Parquet for analysis
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --parquet results.parquet

# Machine-readable JSON output
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --format json > results.jsonl

# Pin to specific CPUs for consistent results
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --cpu_list "0-3"

# Enable Prometheus metrics endpoint
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --prometheus 127.0.0.1:9090
```

## Configuration Reference

Configuration files use TOML format. Here's a complete reference:

### General Settings

```toml
[general]
# Test duration (required for bounded tests)
duration = "60s"

# Warmup period before recording metrics
# Allows cache to warm up before measurement
warmup = "10s"

# Number of worker threads (default: CPU count)
threads = 4

# CPU pinning (Linux only)
# Formats: "0-3", "0,2,4,6", "0-3,8-11"
cpu_list = "0-3"

# I/O engine selection
# "auto" - Use io_uring if available, fallback to mio
# "uring" - Force io_uring (Linux 6.0+ only)
# "mio" - Force mio (portable)
io_engine = "auto"
```

### Target Settings

```toml
[target]
# Target server addresses (supports multiple for load balancing)
endpoints = ["127.0.0.1:6379"]
# endpoints = ["server1:6379", "server2:6379", "server3:6379"]

# Protocol selection
# "resp" - Redis RESP2 protocol
# "resp3" - Redis RESP3 protocol
# "memcache" - Memcache ASCII protocol
# "memcache_binary" - Memcache binary protocol
# "momento" - Momento cache (cloud)
# "ping" - Simple PING/PONG
protocol = "resp"

# Enable TLS encryption
tls = false
```

### Connection Settings

```toml
[connection]
# Total connections across all threads
# Distributed evenly: connections / threads per thread
connections = 16

# Maximum in-flight requests per connection (pipelining)
# Higher values = higher throughput, slightly higher latency
# Typical values: 1 (no pipelining), 32 (high throughput)
pipeline_depth = 32

# Connection establishment timeout
connect_timeout = "5s"

# Individual request timeout
request_timeout = "1s"

# Request distribution strategy
# "roundrobin" - Distribute requests evenly across connections
# "greedy" - Fill one connection's pipeline before moving to next
request_distribution = "roundrobin"
```

### Workload Settings

```toml
[workload]
# Global rate limit (requests/second, optional)
# Useful for simulating production traffic patterns
rate_limit = 100000

[workload.keyspace]
# Key size in bytes
length = 16

# Number of unique keys in the keyspace
# Larger values = more cache misses, more realistic
count = 1000000

# Key selection distribution
# "uniform" - All keys equally likely
# "zipf" - Zipfian distribution (hot keys accessed more often)
distribution = "uniform"

[workload.commands]
# Command ratio as weights (will be normalized)
get = 80  # 80% GETs
set = 20  # 20% SETs

[workload.values]
# Value size for SET commands (bytes)
length = 64
```

### Timestamp Settings

```toml
[timestamps]
# Enable latency measurement
enabled = true

# Timestamp source
# "userspace" - Application-level timing (portable, ~1µs overhead)
# "software" - Kernel SO_TIMESTAMPING (Linux only, lower overhead)
mode = "userspace"
```

### Output Settings

```toml
[admin]
# Prometheus metrics endpoint (optional)
listen = "127.0.0.1:9090"

# Write metrics to Parquet file (optional)
parquet = "results.parquet"

# Parquet snapshot interval
parquet_interval = "1s"

# Output format: clean, json, verbose, quiet
format = "clean"

# Color mode: auto, always, never
color = "auto"
```

### Momento-Specific Settings

```toml
[momento]
# Cache name on Momento
cache_name = "my-cache"

# Momento endpoint (overrides MOMENTO_ENDPOINT env var)
endpoint = "cell-us-east-1-1.prod.a.]momentohq.com"

# TTL for SET operations (seconds)
ttl_seconds = 3600

# Wire format: "grpc" or "protosocket"
wire_format = "grpc"
```

## Output Formats

### Clean (Default)

Human-readable table with ANSI colors:

```
Configuration:
  Protocol: resp
  Endpoints: 127.0.0.1:6379
  Threads: 4
  Connections: 16
  Pipeline depth: 32
  Keyspace: 1000000 keys (uniform)
  Value size: 64 bytes
  Duration: 60s (warmup: 10s)

Running benchmark...

  Time   Req/s    Err/s   Hit%     P50     P90     P99   P99.9  P99.99     Max
─────────────────────────────────────────────────────────────────────────────────
  1.0s   523847       0  79.2%    48µs    89µs   156µs   312µs   891µs  1.24ms
  2.0s   531294       0  79.4%    47µs    87µs   152µs   298µs   756µs  1.12ms
  ...

Results:
  Duration: 60.0s
  Requests: 31,847,291
  Throughput: 530,788 req/s
  Cache hit rate: 79.3%
  Latency (GET): P50=47µs P99=154µs P99.9=301µs
  Latency (SET): P50=52µs P99=167µs P99.9=334µs
```

### JSON

Newline-delimited JSON (NDJSON) for programmatic consumption:

```json
{"type":"config","protocol":"resp","endpoints":["127.0.0.1:6379"],"threads":4,...}
{"type":"sample","time":1.0,"requests":523847,"errors":0,"hit_rate":0.792,"p50":48000,...}
{"type":"sample","time":2.0,"requests":531294,"errors":0,"hit_rate":0.794,"p50":47000,...}
{"type":"result","duration":60.0,"total_requests":31847291,"throughput":530788,...}
```

### Verbose

Tracing-style output with debug information:

```
2024-01-15T10:30:00.000Z INFO  benchmark: Starting benchmark
2024-01-15T10:30:00.001Z DEBUG benchmark: Worker 0 connecting to 127.0.0.1:6379
...
```

### Quiet

Single-line summary only:

```
530788 req/s, 0 errors, 79.3% hit rate, P99=154µs
```

## Metrics Reported

### Per-Second Samples

| Metric | Description |
|--------|-------------|
| Requests/sec | Successful requests completed |
| Errors/sec | Failed requests |
| Hit % | Cache hit rate (GET commands) |
| P50-P99.99 | Latency percentiles |
| Max | Maximum latency observed |

### Final Results

| Metric | Description |
|--------|-------------|
| Duration | Actual test duration |
| Total requests | Requests sent and received |
| Throughput | Average requests/second |
| Cache hits/misses | Hit/miss counts |
| Hit rate | Percentage of GETs that hit |
| Bytes TX/RX | Network bytes transferred |
| GET latency | Percentiles for GET operations |
| SET latency | Percentiles for SET operations |
| Connections | Active/failed connection counts |

## Workload Patterns

### Read-Heavy (Default)

```toml
[workload.commands]
get = 80
set = 20
```

Typical for caching workloads where most requests are reads.

### Write-Heavy

```toml
[workload.commands]
get = 20
set = 80
```

Tests cache write performance and eviction behavior.

### Read-Only

```toml
[workload.commands]
get = 100
set = 0
```

Requires pre-populated cache. Tests pure read throughput.

### Zipfian Distribution

```toml
[workload.keyspace]
distribution = "zipf"
count = 10000000
```

Realistic access pattern where some keys are "hot" (accessed frequently). Better simulates production traffic.

## Tuning for Maximum Throughput

### High-Throughput Configuration

```toml
[general]
threads = 8
io_engine = "uring"

[connection]
connections = 64           # 8 per thread
pipeline_depth = 64        # High pipelining

[workload.keyspace]
length = 16                # Small keys
count = 100000             # Smaller keyspace = more hits

[workload.values]
length = 64                # Small values
```

### Low-Latency Configuration

```toml
[general]
threads = 4
cpu_list = "0-3"           # Pin to dedicated cores
io_engine = "uring"

[connection]
connections = 4            # 1 per thread
pipeline_depth = 1         # No pipelining

[timestamps]
mode = "software"          # Lower measurement overhead
```

## Protocol Details

### RESP (Redis)

Supported commands:
- `GET key` - Retrieve value
- `SET key value` - Store value
- `PING` - Connection health check

The benchmark generates random keys based on keyspace settings and random values based on value length.

### Memcache

Supported commands:
- `get key` - Retrieve value (supports multi-get)
- `set key flags exptime bytes\r\nvalue` - Store value

ASCII protocol by default. Use `protocol = "memcache_binary"` for binary protocol.

### Momento

Requires environment variables:
- `MOMENTO_API_KEY` - API authentication key
- `MOMENTO_ENDPOINT` - Optional endpoint override

```bash
export MOMENTO_API_KEY="your-api-key"
./target/release/crucible-benchmark benchmark/config/momento.toml
```

## Example Configurations

### Redis Performance Test

```toml
# benchmark/config/redis.toml
[general]
duration = "60s"
warmup = "10s"
threads = 4

[target]
endpoints = ["127.0.0.1:6379"]
protocol = "resp"

[connection]
connections = 16
pipeline_depth = 32

[workload.keyspace]
length = 16
count = 1000000
distribution = "uniform"

[workload.commands]
get = 80
set = 20

[workload.values]
length = 64
```

### Memcache Migration Validation

```toml
# Test against both old and new servers
[general]
duration = "300s"
warmup = "30s"
threads = 8

[target]
endpoints = ["new-server:11211"]
protocol = "memcache"

[connection]
connections = 32
pipeline_depth = 16

[workload]
rate_limit = 50000  # Match production rate

[workload.keyspace]
distribution = "zipf"
count = 10000000

[admin]
parquet = "migration-test.parquet"
```

### Quick Smoke Test

```toml
# benchmark/config/quick-test.toml
[general]
duration = "5s"
warmup = "1s"
threads = 1

[target]
endpoints = ["127.0.0.1:6379"]
protocol = "resp"

[connection]
connections = 1
pipeline_depth = 1
```

## Troubleshooting

### Connection Refused

```
Error: connection refused
```

Ensure the target server is running and listening on the configured endpoint.

### Rate Limit Not Achieved

If actual throughput is lower than `rate_limit`:
1. Increase `connections` and `pipeline_depth`
2. Add more `threads`
3. Check target server capacity

### High Latency Variance

For consistent measurements:
1. Use `cpu_list` to pin threads to dedicated cores
2. Disable CPU frequency scaling
3. Use `io_engine = "uring"` on Linux 6.0+
4. Increase warmup period

### Memory Usage

Memory usage scales with:
- `connections × pipeline_depth × (key_length + value_length)`
- Buffer pools (configurable via io-driver)

For high connection counts, ensure sufficient memory.

## Comparison with Other Tools

| Feature | crucible-benchmark | redis-benchmark | memtier_benchmark |
|---------|-------------------|-----------------|-------------------|
| io_uring support | Yes | No | No |
| Zero-copy send | Yes | No | No |
| Multiple protocols | RESP, Memcache, Momento | RESP only | RESP, Memcache |
| Parquet export | Yes | No | No |
| Zipfian distribution | Yes | No | Yes |
| Pipelining | Yes | Yes | Yes |
| CPU pinning | Yes | No | Yes |

## Building

```bash
# Build release binary
cargo build --release -p benchmark

# Build with specific features
cargo build --release -p benchmark --features io_uring

# Run tests
cargo test -p benchmark
```
