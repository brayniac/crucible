# Crucible Documentation

Welcome to the Crucible documentation. Crucible is a high-performance cache server written in Rust.

## Getting Started

1. **[Quick Start](../README.md)** - Build and run Crucible in 5 minutes
2. **[Configuration](configuration.md)** - Server and benchmark configuration reference
3. **[Operations](operations.md)** - Running in production, performance tuning

## Understanding Crucible

4. **[Design Philosophy](design.md)** - Why we built things the way we did
5. **[Architecture](architecture.md)** - How the components fit together

## Deep Dives

6. **[I/O Driver Architecture](../io/driver/ARCHITECTURE.md)** - Buffer management and copy semantics
7. **[Benchmark Guide](../benchmark/README.md)** - Load testing and performance measurement

## Quick Reference

### Server Commands

```bash
# Start server
./target/release/crucible-server server/config/example.toml

# Redis-compatible mode
./target/release/crucible-server server/config/redis.toml

# Memcached-compatible mode
./target/release/crucible-server server/config/memcached.toml
```

### Benchmark Commands

```bash
# Run benchmark
./target/release/crucible-benchmark benchmark/config/redis.toml

# Override settings
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --threads 4 --connections 32 --rate 100000

# Output to parquet
./target/release/crucible-benchmark benchmark/config/redis.toml \
    --parquet results.parquet
```

### Supported Protocols

| Protocol | Port | Config |
|----------|------|--------|
| Redis RESP | 6379 | `redis.toml` |
| Memcache ASCII | 11211 | `memcached.toml` |

### Redis Commands

```
GET key
SET key value [EX seconds] [PX ms] [NX|XX]
MGET key [key ...]
DEL key [key ...]
INCR key
DECR key
INCRBY key increment
DECRBY key decrement
PING
```

### Memcache Commands

```
get key [key ...]
gets key [key ...]
set key flags exptime bytes\r\nvalue
add key flags exptime bytes\r\nvalue
replace key flags exptime bytes\r\nvalue
delete key
incr key value
decr key value
```

## Platform Support

| Platform | I/O Backend | CPU Pinning | NUMA | Hugepages |
|----------|-------------|-------------|------|-----------|
| Linux 6.0+ | io_uring | Yes | Yes | Yes |
| Linux < 6.0 | mio (epoll) | Yes | Yes | Yes |
| macOS | mio (kqueue) | No | No | No |
