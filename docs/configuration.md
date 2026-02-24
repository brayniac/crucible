# Configuration Reference

Crucible server is configured via TOML files. For benchmark configuration, see
the [cachecannon](https://github.com/cachecannon/cachecannon) repository.

## Server Configuration

Example configurations are in `server/config/`. Start with `example.toml` for most use cases.

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

The benchmark tool (`cachecannon`) has been moved to a separate repository:
[cachecannon](https://github.com/cachecannon/cachecannon).

See the cachecannon repository for benchmark configuration reference and examples.

## CLI Options

```bash
# See server options
./target/release/crucible-server --help
```
