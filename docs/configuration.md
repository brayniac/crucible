# Configuration Reference

Crucible server and benchmark are configured via TOML files.

## Server Configuration

Example configurations are in `server/config/`. Start with `example.toml` for most use cases.

### Runtime Selection

```toml
# Runtime: "native" (recommended) or "tokio"
runtime = "native"

# I/O engine: "auto", "uring", or "mio"
# "auto" uses io_uring on Linux 6.0+, falls back to mio
io_engine = "auto"
```

**When to use each runtime:**

| Runtime | Best For |
|---------|----------|
| `native` | Production, maximum performance, Linux |
| `tokio` | Development, portability, async ecosystem integration |

### Workers

```toml
[workers]
# Number of worker threads
# Default: number of CPU cores
threads = 4

# CPU affinity (Linux only)
# Pin workers to specific cores for consistent performance
# Formats: "0-3", "0,2,4,6", "0-3,8-11"
cpu_affinity = "0-3"
```

**CPU affinity tips:**
- Use physical cores, not hyperthreads, for best latency
- On NUMA systems, use cores from one NUMA node
- Leave some cores for OS and other processes

### Cache

```toml
[cache]
# Storage backend: "segment" (recommended), "slab", or "heap"
backend = "segment"

# Eviction policy (depends on backend)
# segment: "s3fifo" (recommended), "fifo", "random", "cte", "merge"
# slab: "lra", "lrc", "random", "none"
# heap: "s3fifo", "lfu"
policy = "s3fifo"

# Total cache memory
# Accepts: bytes, "1GB", "512MB", etc.
heap_size = "4GB"

# Segment size (segment and slab backends)
# Larger = less metadata overhead, slower eviction
# Smaller = faster eviction, more metadata
# Recommended: 1MB - 16MB
segment_size = "8MB"

# Maximum value size (segment backend only)
# Values larger than this are rejected
max_value_size = "1MB"

# Hashtable size: 2^power buckets
# Size for ~50% load factor of expected item count
# Example: 1M items → power=21 (2M buckets)
hashtable_power = 20

# Hugepage support (Linux only): "none", "2mb", "1gb"
# Reduces TLB misses for large caches
hugepage = "none"

# NUMA node binding (Linux only)
# Allocate cache memory on specific NUMA node
# Should match cpu_affinity for best performance
numa_node = 0
```

**Backend comparison:**

| Backend | Best For | Eviction Granularity |
|---------|----------|---------------------|
| `segment` | Most workloads | Segment (many items) |
| `slab` | Memcached migration | Slab (many items) |
| `heap` | Variable-size items | Individual item |

**Policy comparison:**

| Policy | Description | Best For |
|--------|-------------|----------|
| `s3fifo` | Admission-filtered FIFO | Most workloads |
| `fifo` | Simple FIFO | Predictable eviction |
| `random` | Random eviction | Testing |
| `cte` | Closest to expiration | TTL-heavy workloads |
| `lru`/`lra` | Least recently used/accessed | Classic LRU behavior |

### Listeners

```toml
# Multiple listeners supported
[[listener]]
protocol = "resp"           # "resp", "memcache"
address = "0.0.0.0:6379"
allow_flush = false         # Allow FLUSHDB/FLUSHALL (dangerous)

[[listener]]
protocol = "memcache"
address = "0.0.0.0:11211"

# Optional: TLS encryption
[[listener]]
protocol = "resp"
address = "0.0.0.0:6380"

[listener.tls]
cert = "/path/to/cert.pem"
key = "/path/to/key.pem"
```

### Metrics

```toml
[metrics]
# Prometheus metrics endpoint
address = "0.0.0.0:9090"
```

Access metrics at `http://localhost:9090/metrics`.

### io_uring Settings (Linux only)

```toml
[uring]
# SQPOLL mode: kernel thread polls submission queue
# Eliminates syscalls but uses CPU
# Requires root or CAP_SYS_NICE
sqpoll = false
sqpoll_idle_ms = 1000

# Buffer pool settings
buffer_count = 2048         # Number of buffers
buffer_size = "16KB"        # Size of each buffer

# Submission queue depth
sq_depth = 1024
```

The driver uses a hybrid recv mode that automatically switches between multishot recv (efficient for small requests) and direct recv (zero-copy for large values).

## Complete Server Example

```toml
runtime = "native"
io_engine = "auto"

[workers]
threads = 8
cpu_affinity = "0-7"

[cache]
backend = "segment"
policy = "s3fifo"
heap_size = "16GB"
segment_size = "8MB"
max_value_size = "1MB"
hashtable_power = 24
hugepage = "2mb"
numa_node = 0

[[listener]]
protocol = "resp"
address = "0.0.0.0:6379"

[[listener]]
protocol = "memcache"
address = "0.0.0.0:11211"

[metrics]
address = "0.0.0.0:9090"

[uring]
sqpoll = false
buffer_count = 4096
buffer_size = "16KB"
sq_depth = 2048
```

## Benchmark Configuration

Example configurations are in `benchmark/config/`.

### General Settings

```toml
[general]
# Test duration
duration = "60s"

# Warmup period (metrics not recorded)
warmup = "10s"

# Number of worker threads
threads = 4

# CPU pinning (Linux only)
cpu_list = "0-3"

# I/O engine: "auto", "uring", "mio"
io_engine = "auto"
```

### Target

```toml
[target]
# Server endpoints (supports multiple for load balancing)
endpoints = ["127.0.0.1:6379"]

# Protocol: "resp", "resp3", "memcache", "memcache_binary", "momento", "ping"
protocol = "resp"

# Enable TLS
tls = false
```

### Connection

```toml
[connection]
# Total connections across all threads
connections = 16

# Pipeline depth (in-flight requests per connection)
# Higher = more throughput, slightly higher latency
pipeline_depth = 32

# Timeouts
connect_timeout = "5s"
request_timeout = "1s"

# Distribution: "roundrobin" or "greedy"
request_distribution = "roundrobin"
```

### Workload

```toml
[workload]
# Global rate limit (requests/second)
# Omit for unlimited
rate_limit = 100000

[workload.keyspace]
# Key length in bytes
length = 16

# Number of unique keys
count = 1000000

# Distribution: "uniform" or "zipf"
distribution = "uniform"

[workload.commands]
# Command weights (normalized to percentages)
get = 80
set = 20

[workload.values]
# Value size in bytes
length = 64
```

### Output

```toml
[admin]
# Prometheus endpoint
listen = "127.0.0.1:9090"

# Parquet output
parquet = "results.parquet"
parquet_interval = "1s"

# Output format: "clean", "json", "verbose", "quiet"
format = "clean"

# Color: "auto", "always", "never"
color = "auto"
```

## Complete Benchmark Example

```toml
[general]
duration = "60s"
warmup = "10s"
threads = 4
cpu_list = "0-3"
io_engine = "auto"

[target]
endpoints = ["127.0.0.1:6379"]
protocol = "resp"
tls = false

[connection]
connections = 32
pipeline_depth = 32
connect_timeout = "5s"
request_timeout = "1s"
request_distribution = "roundrobin"

[workload]
rate_limit = 500000

[workload.keyspace]
length = 16
count = 1000000
distribution = "zipf"

[workload.commands]
get = 80
set = 20

[workload.values]
length = 64

[admin]
listen = "127.0.0.1:9091"
parquet = "results.parquet"
format = "clean"
color = "auto"
```

## CLI Options

The benchmark tool is primarily configured via TOML files. The only CLI override is for the parquet output path:

```bash
# Override parquet output path
./target/release/crucible-benchmark config.toml --parquet results.parquet

# View results in web dashboard
./target/release/crucible-benchmark view results.parquet

# See all options
./target/release/crucible-benchmark --help
./target/release/crucible-server --help
```
