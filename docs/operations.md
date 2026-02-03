# Operations Guide

This guide covers running Crucible in production.

## Quick Start

```bash
# Build release binaries
cargo build --release

# Run server
./target/release/crucible-server server/config/example.toml

# Verify it's working
redis-cli -p 6379 PING
```

## Production Checklist

Before deploying to production:

- [ ] Use release build (`cargo build --release`)
- [ ] Configure CPU affinity for your hardware
- [ ] Size hashtable for expected item count
- [ ] Configure memory limits appropriately
- [ ] Set up monitoring (Prometheus endpoint)
- [ ] Test with realistic workload using benchmark tool
- [ ] Configure hugepages if cache > 1GB

## Performance Tuning

### Linux Kernel

For best performance, use Linux 6.0+ for full io_uring support.

```bash
# Check kernel version
uname -r

# Check io_uring availability
cat /proc/kallsyms | grep io_uring
```

### CPU Affinity

Pin workers to specific CPU cores to reduce context switching and improve cache locality.

```toml
[workers]
threads = 8
cpu_affinity = "0-7"
```

**Best practices:**
- Use physical cores, not hyperthreads
- Keep workers on one NUMA node if possible
- Leave cores 0 for OS/interrupts on busy systems
- Match thread count to physical core count

```bash
# Find physical cores (Linux)
lscpu -p | grep -v "^#"

# Find NUMA topology
numactl --hardware
```

### Hugepages

Hugepages reduce TLB misses for large caches. Recommended for caches > 1GB.

**2MB hugepages (recommended for most deployments):**

```bash
# Calculate pages needed: cache_size / 2MB
# For 16GB cache: 16384 / 2 = 8192 pages

# Allocate pages (requires root)
echo 8192 > /proc/sys/vm/nr_hugepages

# Verify
cat /proc/meminfo | grep Huge
```

```toml
[cache]
heap_size = "16GB"
hugepage = "2mb"
```

**1GB hugepages (for very large caches 100GB+):**

```bash
# Add to kernel boot parameters
# hugepagesz=1G hugepages=128

# After reboot, verify
cat /proc/meminfo | grep Huge
```

```toml
[cache]
heap_size = "128GB"
hugepage = "1gb"
```

### NUMA Awareness

On multi-socket systems, bind cache memory to the same NUMA node as the worker CPUs.

```bash
# Check NUMA topology
numactl --hardware

# Example output:
# node 0 cpus: 0 1 2 3 4 5 6 7
# node 1 cpus: 8 9 10 11 12 13 14 15
```

```toml
[cache]
numa_node = 0

[workers]
cpu_affinity = "0-7"  # CPUs on NUMA node 0
```

### io_uring Tuning

For Linux 6.0+, tune io_uring settings based on workload:

```toml
[uring]
# Buffer sizing
# Rule of thumb: 2 buffers per expected connection
buffer_count = 4096
buffer_size = "16KB"

# Submission queue depth
# Higher values allow more batching
sq_depth = 2048

# SQPOLL: kernel polls submission queue
# Reduces latency but uses CPU
# Requires root or CAP_SYS_NICE
sqpoll = false
```

### Hashtable Sizing

Size the hashtable for ~50% load factor:

```
buckets = 2^hashtable_power
target_items = buckets * 0.5
```

Examples:
- 1M items → `hashtable_power = 21` (2M buckets)
- 10M items → `hashtable_power = 24` (16M buckets)
- 100M items → `hashtable_power = 28` (256M buckets)

Undersized hashtables cause high collision rates. Oversized wastes memory.

## Monitoring

### Prometheus Metrics

Metrics are exposed at the configured endpoint:

```bash
curl http://localhost:9090/metrics
```

Key metrics:

| Metric | Description |
|--------|-------------|
| `cache_hits_total` | Total cache hits |
| `cache_misses_total` | Total cache misses |
| `cache_sets_total` | Total SET operations |
| `cache_deletes_total` | Total DELETE operations |
| `connections_active` | Current active connections |
| `bytes_received_total` | Total bytes received |
| `bytes_sent_total` | Total bytes sent |

### Grafana Dashboard

Example Prometheus queries:

```promql
# Hit rate
rate(cache_hits_total[1m]) / (rate(cache_hits_total[1m]) + rate(cache_misses_total[1m]))

# Request rate
rate(cache_hits_total[1m]) + rate(cache_misses_total[1m]) + rate(cache_sets_total[1m])

# Throughput (bytes/sec)
rate(bytes_received_total[1m]) + rate(bytes_sent_total[1m])
```

## Troubleshooting

### io_uring Not Available

The server falls back to mio (epoll) if io_uring isn't available.

```bash
# Check kernel version (need 6.0+)
uname -r

# Verify io_uring syscalls available
cat /proc/kallsyms | grep -c io_uring
```

Common causes:
- Kernel too old (< 5.1 for basic, < 6.0 for full features)
- Running in container without io_uring access
- Seccomp blocking io_uring syscalls

### SQPOLL Permission Denied

SQPOLL mode requires elevated privileges:

```bash
# Option 1: Run as root
sudo ./target/release/crucible-server config.toml

# Option 2: Grant capability
sudo setcap cap_sys_nice+ep ./target/release/crucible-server
```

### High Latency

If experiencing high tail latency:

1. **Check CPU affinity**: Ensure workers are pinned
   ```bash
   taskset -p $(pgrep crucible-server)
   ```

2. **Disable hyperthreading**: Use only physical cores
   ```bash
   # Find physical core IDs
   cat /sys/devices/system/cpu/cpu*/topology/thread_siblings_list | sort -u
   ```

3. **Check for noisy neighbors**: Other processes on same cores
   ```bash
   top -1  # Per-CPU view
   ```

4. **Verify hugepages**: TLB misses hurt latency
   ```bash
   perf stat -e dTLB-load-misses ./target/release/crucible-server config.toml
   ```

5. **Check network**: Is the NIC saturated?
   ```bash
   sar -n DEV 1
   ```

### Memory Issues

**Out of memory:**
- Reduce `heap_size`
- Reduce `hashtable_power`
- Check for memory leaks with `top` or `htop`

**Hugepage allocation failure:**
- Ensure enough free memory before allocating hugepages
- Check `cat /proc/meminfo | grep Huge`
- May need to reboot if memory is fragmented

### Connection Issues

**Connection refused:**
- Check server is running: `pgrep crucible-server`
- Check listener address in config
- Check firewall rules

**Connection reset:**
- Check for protocol mismatch (RESP vs Memcache)
- Check for max connection limits
- Review server logs

## Graceful Shutdown

The server handles SIGTERM and SIGINT gracefully:

```bash
# Graceful shutdown
kill -TERM $(pgrep crucible-server)

# Or
kill -INT $(pgrep crucible-server)
```

The server will:
1. Stop accepting new connections
2. Drain existing connections (30s timeout)
3. Exit cleanly

## Running in Containers

### Docker

```dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/crucible-server /usr/local/bin/
COPY server/config/example.toml /etc/crucible/config.toml
EXPOSE 6379 9090
CMD ["crucible-server", "/etc/crucible/config.toml"]
```

**Important container settings:**
- Disable seccomp for io_uring: `--security-opt seccomp=unconfined`
- Or allow io_uring syscalls in seccomp profile
- Mount hugepages if using: `-v /dev/hugepages:/dev/hugepages`
- Set CPU affinity via cgroups or `--cpuset-cpus`

### Kubernetes

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: crucible
    image: crucible:latest
    resources:
      limits:
        memory: "16Gi"
        cpu: "8"
        hugepages-2Mi: "16Gi"
    securityContext:
      capabilities:
        add: ["SYS_NICE"]  # For SQPOLL
    volumeMounts:
    - name: hugepage
      mountPath: /dev/hugepages
  volumes:
  - name: hugepage
    emptyDir:
      medium: HugePages
```

## Benchmarking

Before production deployment, validate performance:

```bash
# Start server
./target/release/crucible-server server/config/example.toml &

# Run benchmark (configure threads, connections, rate in config file)
./target/release/crucible-benchmark benchmark/config/redis.toml

# Save results to parquet for analysis
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --parquet results.parquet

# View results in web dashboard
./target/release/crucible-benchmark view results.parquet

# Check results:
# - Throughput meets requirements
# - p99 latency acceptable
# - No errors
# - Hit rate as expected
```

See [benchmark/README.md](../benchmark/README.md) for detailed benchmarking guide.
