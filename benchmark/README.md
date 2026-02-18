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

## End-to-End Walkthrough

This walkthrough takes you from zero to analyzed results. It assumes you have a Redis-compatible server running on `127.0.0.1:6379` — either Crucible itself, Redis, or any RESP-compatible server.

### 1. Start the server

```bash
# Option A: Start a Crucible cache server
./target/release/crucible-server server/config/redis.toml

# Option B: Use any Redis-compatible server already running on port 6379
```

### 2. Run a benchmark with Parquet output

```bash
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --parquet results.parquet
```

The benchmark goes through these phases automatically:

1. **Connect** — Establishes TCP connections to the server
2. **Prefill** — *(if enabled)* Writes every key once to populate the cache
3. **Warmup** — Runs the workload without recording metrics, letting caches stabilize
4. **Running** — Records metrics for the configured duration
5. **Stop** — Drains in-flight requests and writes the Parquet file

During the Running phase, you'll see per-second output like:

```
  Time   Req/s    Err/s   Hit%     P50     P90     P99   P99.9  P99.99     Max
─────────────────────────────────────────────────────────────────────────────────
  1.0s   523847       0  79.2%    48µs    89µs   156µs   312µs   891µs  1.24ms
  2.0s   531294       0  79.4%    47µs    87µs   152µs   298µs   756µs  1.12ms
```

### 3. View results in the dashboard

```bash
./target/release/crucible-benchmark view results.parquet
```

This launches a local web server and opens an interactive dashboard in your browser. See [Results Viewer](#results-viewer) for details on each dashboard section.

### 4. (Optional) Correlate with system telemetry

For deeper analysis, capture system-level metrics with [Rezolus](https://github.com/brayniac/rezolus) during the benchmark:

```bash
# Terminal 1: Record server-side telemetry
rezolus record http://localhost:4241 server-rezolus.parquet &

# Terminal 2: Record client-side telemetry
rezolus record http://localhost:4241 client-rezolus.parquet &

# Terminal 3: Run the benchmark
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --parquet results.parquet

# View everything together
./target/release/crucible-benchmark view results.parquet \
    --server server-rezolus.parquet \
    --client client-rezolus.parquet
```

### 5. Interpreting results

Key things to look for:

- **Throughput stability** — Is req/s consistent or oscillating? Oscillation may indicate GC pauses, eviction storms, or connection churn.
- **Tail latency** — P99.9 and P99.99 reveal worst-case behavior. A P99.9 that is 10x the P50 suggests queuing or contention.
- **Hit rate** — With a uniform keyspace and 80/20 read/write mix, expect ~80% hit rate at steady state. Lower values may mean the cache is undersized for the keyspace.
- **Error rate** — Should be 0%. Non-zero errors indicate server overload, timeouts, or connection failures.

## Command Line Interface

The benchmark is primarily configured via TOML files. CLI options are minimal:

```
crucible-benchmark <CONFIG> [OPTIONS]

Arguments:
  <CONFIG>  Path to TOML configuration file

Options:
  --parquet <PATH>      Override Parquet output path from config
  -h, --help            Print help

Subcommands:
  view                  View benchmark results in a web dashboard
```

### Examples

```bash
# Run benchmark with config file
./target/release/crucible-benchmark benchmark/config/redis.toml

# Override parquet output path
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --parquet results.parquet
```

## Results Viewer

The `view` subcommand launches an interactive web dashboard for analyzing benchmark results. It automatically opens in your browser.

```
crucible-benchmark view <INPUT> [OPTIONS]

Arguments:
  <INPUT>               Path to benchmark parquet file

Options:
  --server <PATH>       Path to server-side Rezolus parquet file
  --client <PATH>       Path to client-side Rezolus parquet file
  --listen <ADDR>       Listen address [default: 127.0.0.1:0]
  -v, --verbose         Increase verbosity
```

### Dashboard Sections

The dashboard is organized into sections, each with multiple charts. All time-series charts share a synchronized time axis and support zooming, panning, and tooltips.

#### Overview

A single-page summary of the benchmark run with six key metrics:

| Chart | What it shows | Unit |
|-------|---------------|------|
| **Throughput** | Responses per second (10s irate) | req/s |
| **Hit Rate** | `hits / (hits + misses)` as a percentage | % |
| **Latency** | Percentiles (p50, p90, p99, p99.9, p99.99) on a log scale | time |
| **Error Rate** | `errors / requests_sent` as a percentage | % |
| **TX Bandwidth** | Bytes sent, converted to bits/sec | bit/s |
| **RX Bandwidth** | Bytes received, converted to bits/sec | bit/s |

#### Throughput

Detailed throughput breakdown:

| Chart | What it shows | Unit |
|-------|---------------|------|
| **Responses/sec** | Total response rate | req/s |
| **Error Rate** | Error percentage over time | % |
| **TX Bytes/sec** | Network send throughput | bytes/s |
| **RX Bytes/sec** | Network receive throughput | bytes/s |
| **GET/sec** | GET operation rate | req/s |
| **SET/sec** | SET operation rate | req/s |
| **DELETE/sec** | DELETE operation rate | req/s |

#### Latency

Per-operation latency percentiles, all on log scale:

| Chart | What it shows |
|-------|---------------|
| **Response** | All operations combined (p50, p90, p99, p99.9, p99.99) |
| **GET** | GET latency percentiles |
| **SET** | SET latency percentiles |
| **DELETE** | DELETE latency percentiles |

#### Cache

Cache behavior over time:

| Chart | What it shows | Unit |
|-------|---------------|------|
| **Hit Rate** | Cache hits per second | req/s |
| **Miss Rate** | Cache misses per second | req/s |
| **GET/sec** | GET operation rate | req/s |
| **SET/sec** | SET operation rate | req/s |
| **DELETE/sec** | DELETE operation rate | req/s |

#### Connections

Connection pool health:

| Chart | What it shows | Unit |
|-------|---------------|------|
| **Active Connections** | Current live connection count (gauge) | count |
| **Connection Failures/sec** | Rate of failed connection attempts | req/s |
| **EOF** | Disconnects from server closing the connection | req/s |
| **Recv Error** | Disconnects from receive errors | req/s |
| **Send Error** | Disconnects from send errors | req/s |
| **Closed Event** | Disconnects from close events | req/s |
| **Error Event** | Disconnects from error events | req/s |
| **Connect Failed** | Disconnects from failed connect attempts | req/s |

#### Server Metrics / Client Metrics

When you provide `--server` or `--client` Rezolus parquet files, these sections display system-level telemetry (CPU utilization, network statistics, scheduler behavior, etc.) aligned to the benchmark timeline.

### Correlating with Rezolus

For deeper analysis, you can correlate benchmark results with system-level telemetry from [Rezolus](https://github.com/brayniac/rezolus). Run Rezolus in record mode on the server and/or client during the benchmark:

```bash
# On the server machine
rezolus record http://localhost:4241 server-rezolus.parquet &

# On the client machine
rezolus record http://localhost:4241 client-rezolus.parquet &

# Run benchmark
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --parquet benchmark.parquet

# View with all data sources
./target/release/crucible-benchmark view benchmark.parquet \
    --server server-rezolus.parquet \
    --client client-rezolus.parquet
```

This lets you correlate cache performance with CPU utilization, network statistics, scheduler behavior, and other system metrics.

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

# Enable Redis Cluster mode (see "Cluster Mode" section below)
# cluster = true
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

> **Note:** `pool_size` is accepted as a legacy alias for `connections`. Prefer `connections` in new configurations.

### Workload Settings

```toml
[workload]
# Global rate limit (requests/second, optional)
# Useful for simulating production traffic patterns
rate_limit = 100000

# Pre-populate the cache before measurement (see "Prefill" section below)
# prefill = true

# Automatically SET on GET miss (see "Backfill on Miss" section below)
# backfill_on_miss = true

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
endpoint = "cell-us-east-1-1.prod.a.momentohq.com"

# TTL for SET operations (seconds)
ttl_seconds = 3600

# Wire format: "grpc" or "protosocket"
wire_format = "grpc"
```

## Prefill

Prefill populates the cache with every key in the keyspace before the benchmark begins. This is useful for read-only workloads, hit-rate measurements, or any test where you need the cache to be warm from the start.

### Enabling Prefill

```toml
[workload]
prefill = true
```

### How It Works

1. After connections are established, each worker thread receives a deterministic slice of the keyspace to write. Keys are distributed proportionally based on each worker's connection count.
2. Workers issue SET commands for each assigned key, throttled to 16 in-flight requests per connection to avoid overwhelming the server.
3. The benchmark monitors global progress. Once all workers have confirmed their assigned keys, the prefill phase ends and warmup begins.

### Timeout and Stall Detection

Prefill has a default timeout of **300 seconds**. If no progress is made for 30 consecutive seconds (stall detection), the benchmark aborts with diagnostic information including active workers, connection failures, and bytes transferred. You can adjust the timeout:

```toml
[workload]
prefill = true
prefill_timeout = "600s"   # Increase for very large keyspaces
# prefill_timeout = "0s"   # Disable timeout entirely
```

### When to Use Prefill

- **Read-only benchmarks** (`get = 100, set = 0`) — without prefill, every GET will miss
- **Hit-rate testing** — start from a known cache state instead of random population
- **Latency comparisons** — eliminate the warmup variability of gradual cache filling
- **Cluster mode** — ensure all hash slots have data before measurement

## Backfill on Miss

Backfill implements the **cache-aside** pattern: when a GET misses, the benchmark automatically issues a SET for that key, populating the cache for future reads.

### Enabling Backfill

```toml
[workload]
backfill_on_miss = true
```

### How It Works

1. When a GET response indicates a cache miss, the key ID is added to a per-worker backfill queue.
2. On each event loop tick, the backfill queue is drained before normal workload requests, ensuring misses are filled promptly.
3. Backfill SETs use the same value generation as regular SETs.

### Metrics

Backfill operations are tracked separately from regular SETs:

| Metric | Description |
|--------|-------------|
| `backfill_set_count` | Number of backfill SET operations issued |
| `backfill_set_latency` | Latency histogram for backfill SETs (nanoseconds) |

This lets you distinguish organic write traffic from cache-filling writes when analyzing results.

### When to Use Backfill

- **Gradual cache warming** — the cache fills naturally as reads discover misses, similar to production behavior
- **Testing convergence** — observe how hit rate improves over time as the cache populates
- **Alternative to prefill** — when you want the cache to warm during the measured period rather than before it

## Saturation Search

Saturation search automatically finds the **maximum throughput that meets your latency SLO**. Instead of manually tuning rate limits, the benchmark starts at a low rate and geometrically increases until the SLO is consistently violated.

### Enabling Saturation Search

```toml
[workload.saturation_search]
# Latency SLO thresholds — at least one must be specified
# All specified thresholds must be met for the SLO to pass
slo = { p999 = "1ms" }

# Starting request rate (req/s)
start_rate = 1000

# Increase rate by this factor each step (1.05 = 5% increase)
step_multiplier = 1.05

# Duration to sample at each rate level
sample_window = "5s"

# Stop after this many consecutive SLO violations
stop_after_failures = 3

# Maximum rate to try (absolute ceiling)
max_rate = 100000000

# Minimum ratio of achieved/target throughput (0.0-1.0)
# When achieved throughput falls below this fraction of the target,
# the step fails regardless of latency (detects saturation)
min_throughput_ratio = 0.9
```

### Algorithm

1. Set the initial rate to `start_rate` (default: 1000 req/s)
2. Run the workload at the current rate for `sample_window` (default: 5s)
3. Collect the latency histogram and achieved throughput for the window
4. Check the dual SLO:
   - **Latency**: All specified percentile thresholds must be met (e.g., p99.9 < 1ms)
   - **Throughput**: Achieved throughput must be at least `min_throughput_ratio` of the target rate (default: 90%). This detects when the server can't keep up regardless of latency.
5. If the SLO passes: record this rate as the current maximum, multiply by `step_multiplier`, continue
6. If the SLO fails: increment the consecutive failure counter
7. If consecutive failures reach `stop_after_failures` (default: 3): stop and report
8. Otherwise: continue stepping up
9. The rate is capped at `max_rate` (default: 100M req/s)

### SLO Options

You can specify any combination of latency percentiles:

```toml
# Single threshold
slo = { p999 = "1ms" }

# Multiple thresholds (all must be met)
slo = { p99 = "500us", p999 = "2ms" }

# Available percentiles: p50, p99, p999
```

### Results

The benchmark reports the maximum rate that met the SLO, along with a table of each step:

```
Saturation Search Results:
  Maximum compliant rate: 45,000 req/s (P99.9 = 890µs)

  Step  Target    Achieved    P50     P99    P99.9   Pass
  ────────────────────────────────────────────────────────
     1   1,000      1,000    32µs    45µs     67µs   ✓
     2   1,050      1,050    32µs    46µs     71µs   ✓
    ...
    42  45,000     45,000    41µs   210µs    890µs   ✓
    43  47,250     47,250    48µs   340µs   1.2ms    ✗
    44  49,612     46,100    55µs   410µs   1.5ms    ✗
    45  52,093     44,800    62µs   520µs   1.8ms    ✗
```

### Example: Full Saturation Search Config

See `benchmark/config/saturation.toml` for a ready-to-use configuration.

## Cluster Mode

Cluster mode supports **Redis Cluster** by discovering topology via `CLUSTER SLOTS` and routing keys to the correct shard by hash slot.

### Enabling Cluster Mode

```toml
[target]
endpoints = ["127.0.0.1:7000"]  # One or more seed nodes
protocol = "resp"
cluster = true
```

### How It Works

**Topology Discovery:**
1. Before launching worker threads, the benchmark connects to seed nodes and sends `CLUSTER SLOTS`
2. The response maps hash slot ranges (0–16383) to primary node endpoints
3. A 16384-entry slot table is built: `slot → endpoint`

**Request Routing:**
1. Each key is hashed via CRC16 to determine its hash slot (0–16383)
2. The slot table maps to the responsible endpoint
3. The request is sent to a connection for that endpoint

**MOVED Redirect Handling:**
1. If the server responds with `MOVED <slot> <host>:<port>`, the benchmark extracts the new endpoint
2. If the endpoint is unknown, a new connection is established dynamically
3. The slot table is updated so future requests for that slot route directly to the new node
4. The request is retried on the correct node

**ASK Redirects:**
ASK redirects (indicating a slot migration in progress) are counted in the `cluster_redirects` metric but do not update the slot table, since the migration is transient.

### Cluster Mode with Prefill

Prefill works with cluster mode — keys are routed to the correct shard during the prefill phase:

```toml
[target]
endpoints = ["127.0.0.1:7000"]
protocol = "resp"
cluster = true

[workload]
prefill = true
```

### Example

See `benchmark/config/redis-cluster.toml` for a ready-to-use configuration.

## Parquet Output

The benchmark can record all metrics to a Parquet file for post-hoc analysis with the built-in viewer, or programmatically with tools like DuckDB, pandas, or Polars.

### Enabling Parquet Output

```toml
[admin]
parquet = "results.parquet"
parquet_interval = "1s"     # Snapshot frequency (default: 1s)
```

Or override from the command line:

```bash
./target/release/crucible-benchmark config.toml --parquet results.parquet
```

### Recording Behavior

- Snapshots are only recorded during the **Running** phase (not during Connect, Prefill, or Warmup)
- One snapshot is captured per `parquet_interval`
- A final snapshot is captured on shutdown

### Parquet Schema

Each snapshot contains the full set of metrics. The file uses the metriken exposition format with these metric types:

**Counters** (monotonically increasing, use `irate()` in the viewer for per-second rates):

| Metric | Description |
|--------|-------------|
| `requests_sent` | Total requests sent |
| `responses_received` | Total responses received |
| `request_errors` | Total request errors |
| `cache_hits` | Total cache hits (GET responses with data) |
| `cache_misses` | Total cache misses (GET responses without data) |
| `get_count` | Total GET operations |
| `set_count` | Total SET operations |
| `delete_count` | Total DELETE operations |
| `backfill_set_count` | Backfill SET operations (from `backfill_on_miss`) |
| `bytes_tx` | Total bytes transmitted |
| `bytes_rx` | Total bytes received |
| `connections_failed` | Total failed connection attempts |
| `disconnects_eof` | Disconnects from EOF (server closed connection) |
| `disconnects_recv_error` | Disconnects from receive errors |
| `disconnects_send_error` | Disconnects from send errors |
| `disconnects_closed_event` | Disconnects from close events |
| `disconnects_error_event` | Disconnects from error events |
| `disconnects_connect_failed` | Disconnects from failed connect attempts |
| `cluster_redirects` | Total MOVED/ASK redirects (cluster mode) |

**Gauges** (point-in-time values):

| Metric | Description |
|--------|-------------|
| `connections_active` | Currently active connections |
| `target_rate` | Current target request rate (saturation search) |

**Histograms** (latency distributions in nanoseconds):

| Metric | Description |
|--------|-------------|
| `response_latency` | All responses combined |
| `get_latency` | GET response latency |
| `set_latency` | SET response latency |
| `delete_latency` | DELETE response latency |
| `get_ttfb` | GET time-to-first-byte |
| `backfill_set_latency` | Backfill SET latency |

### Programmatic Analysis

You can query Parquet files directly with DuckDB, pandas, or Polars:

```sql
-- DuckDB: query throughput over time
SELECT timestamp, responses_received
FROM 'results.parquet'
ORDER BY timestamp;
```

```python
# pandas
import pandas as pd
df = pd.read_parquet("results.parquet")
```

```python
# polars
import polars as pl
df = pl.read_parquet("results.parquet")
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
[workload]
prefill = true

[workload.commands]
get = 100
set = 0
```

Requires prefill to populate the cache first. Tests pure read throughput.

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

## Pre-built Configurations

| Config | Description |
|--------|-------------|
| `redis.toml` | Standard Redis RESP protocol benchmark |
| `memcache.toml` | Memcache protocol benchmark |
| `momento.toml` | Momento cloud cache protocol (requires API key) |
| `ping.toml` | Simple PING/PONG protocol test |
| `quick-test.toml` | 5-second smoke test for quick validation |
| `example.toml` | General-purpose config with all options commented |
| `redis-cluster.toml` | Redis Cluster with topology discovery and prefill |
| `saturation.toml` | Automated saturation search to find max SLO-compliant throughput |
| `eviction-test.toml` | High write rate to trigger eviction cycles |
| `desync-test.toml` | High concurrency stress test (128 connections) |

## Troubleshooting

### Connection Refused

```
Error: connection refused
```

Ensure the target server is running and listening on the configured endpoint.

### Prefill Stalls or Times Out

If prefill appears stuck:
1. Check that the server is accepting writes — verify with `redis-cli SET test value`
2. Ensure the server has enough memory for the full keyspace (`keyspace.count × (key_length + value_length)`)
3. Increase the timeout for very large keyspaces: `prefill_timeout = "600s"`
4. Check the diagnostic output for connection failures or zero bytes received

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
| Web dashboard | Yes | No | No |
| Saturation search | Yes | No | No |
| Prefill / backfill | Yes | No | No |
| Cluster mode | Yes | Yes | Yes |
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
