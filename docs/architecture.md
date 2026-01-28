# Architecture

This document describes Crucible's architecture and how the components fit together.

## Overview

Crucible is a high-performance cache server with a clear separation between data plane and control plane:

```
┌───────────────────────────────────────────────────────────────────────┐
│                           crucible-server                              │
├───────────────────────────────────────────────────┬───────────────────┤
│                    DATA PLANE                     │   CONTROL PLANE   │
├───────────┬───────────┬───────────┬───────────────┤───────────────────┤
│  Worker 0 │  Worker 1 │    ...    │   Worker N    │      Admin        │
│  (CPU 0)  │  (CPU 1)  │           │   (CPU N)     │                   │
│           │           │           │               │   Tokio runtime   │
│ io-driver │ io-driver │ io-driver │   io-driver   │   /health         │
│ instance  │ instance  │ instance  │   instance    │   /ready          │
│           │           │           │               │   /metrics        │
├───────────┴───────────┴───────────┴───────────────┤                   │
│                                                   │                   │
│                   Shared Cache                    │                   │
│               (lock-free, thread-safe)            │                   │
│                                                   │                   │
└───────────────────────────────────────────────────┴───────────────────┘
```

**Data plane**: Workers handle RESP/Memcache requests. Each has its own io-driver instance (io_uring on Linux 6.0+, mio elsewhere), optionally pinned to a CPU core. All workers share a single lock-free cache.

**Control plane**: Admin thread runs a single-threaded Tokio runtime for health checks and Prometheus metrics. Isolated from workers so it doesn't affect request latency.

## Component Hierarchy

### Workspace Crates

```
crucible/
├── cache/
│   ├── core/         # Cache traits, hashtable, segments, pools
│   ├── segcache/     # Segment-based cache implementation
│   ├── slab/         # Memcached-style slab allocator
│   └── heap/         # Simple heap-allocated cache
├── io/
│   ├── driver/       # io_uring + mio abstraction
│   ├── http2/        # HTTP/2 framing (for Momento)
│   └── grpc/         # gRPC client (for Momento)
├── protocol/
│   ├── resp/         # Redis RESP2/RESP3
│   ├── memcache/     # Memcache ASCII + binary
│   ├── momento/      # Momento cache protocol
│   └── ping/         # Simple ping (testing)
├── server/           # Cache server binary
├── benchmark/        # Benchmark tool binary
├── metrics/          # Metrics infrastructure
└── xtask/            # Dev tasks (fuzz, flamegraph)
```

### Dependency Flow

```
server, benchmark
       │
       ▼
┌──────────────┐
│   io-driver  │ ←── Handles all network I/O
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   protocol   │ ←── Parses RESP, Memcache, etc.
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  cache-core  │ ←── TieredCache, hashtable, segments
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   segcache   │ ←── Or slab-cache, heap-cache
└──────────────┘
```

## I/O Layer (io-driver)

The I/O driver provides a unified completion-based API:

```rust
pub trait IoDriver {
    /// Poll for I/O completions
    fn poll(&mut self, timeout: Option<Duration>) -> io::Result<usize>;

    /// Drain completed operations
    fn drain_completions(&mut self) -> impl Iterator<Item = Completion>;

    /// Send data to a connection
    fn send(&mut self, id: ConnId, data: &[u8]) -> io::Result<usize>;

    /// Zero-copy send (takes ownership)
    fn send_owned(&mut self, id: ConnId, buf: BoxedZeroCopy) -> io::Result<usize>;

    /// Access received data
    fn with_recv_buf<F>(&mut self, id: ConnId, f: F) -> io::Result<()>
    where F: FnMut(&mut dyn RecvBuf) -> io::Result<()>;
}
```

### Backends

**io_uring (Linux 6.0+)**
- Multishot recv/accept
- Ring-provided buffers
- Zero-copy send (SendZc)
- SQPOLL mode
- Registered file descriptors

**mio (all platforms)**
- epoll on Linux
- kqueue on macOS/BSD
- Standard read/write syscalls

### Buffer Pools

```
┌─────────────────────────────────────────────────────┐
│                   Buffer Hierarchy                   │
├─────────────────────────────────────────────────────┤
│                                                      │
│  BufRing (io_uring multishot only)                  │
│  ├── 8192 × 16KB buffers                            │
│  ├── Kernel picks from ring                         │
│  └── Returned immediately after copy                │
│                                                      │
│  RecvBufferPool (io_uring single-shot)              │
│  ├── 8192 × 16KB buffers                            │
│  ├── Stable pointers for kernel I/O                 │
│  └── Held until app consumes data                   │
│                                                      │
│  Per-Connection Coalesce Buffer                     │
│  ├── 16KB initial (TLS record size)                 │
│  ├── Grows as needed                                │
│  └── Holds partial messages across recvs            │
│                                                      │
└─────────────────────────────────────────────────────┘
```

## Protocol Layer

Each protocol implements parsing and serialization:

### RESP (Redis)

```
*3\r\n           ← Array of 3 elements
$3\r\nSET\r\n    ← Bulk string "SET"
$3\r\nfoo\r\n    ← Bulk string "foo"
$3\r\nbar\r\n    ← Bulk string "bar"
```

Supported commands:
- GET, SET, MGET, DEL
- INCR, DECR, INCRBY, DECRBY
- SET with EX/PX/NX/XX
- PING, HELLO, CONFIG

### Memcache ASCII

```
set foo 0 3600 3\r\n
bar\r\n
```

Supported commands:
- get, gets (multi-get)
- set, add, replace, cas
- delete
- incr, decr

## Cache Layer (cache-core)

### Cache Structure

The cache consists of a shared hashtable with optional tiered storage layers:

```
┌───────────────────────────────────────┐
│              Hashtable                │
│  key → (location, frequency, ghost)   │
└──────┬─────────────┬─────────────┬────┘
       │             │             │
       ▼             ▼             ▼
┌ ─ ─ ─ ─ ─ ─ ┐ ┌─────────────┐ ┌ ─ ─ ─ ─ ─ ─ ┐
  Layer 0:      │  Layer 1:   │    Layer 2:
│ Admission   │ │  Main Cache │ │   Disk      │
  (optional)    │  (RAM)      │   (optional)
│ S3-FIFO     │ │  seg/slab/  │ │ seg/slab    │
  only          │  heap       │   only
└ ─ ─ ─ ─ ─ ─ ┘ └─────────────┘ └ ─ ─ ─ ─ ─ ─ ┘
       │            ▲    │          ▲    │
       └── admit ───┘    │          │    │
                    ▲    └─ demote ─┘    │
                    └────── promote ─────┘
```

- **Layer 0** (optional): S3-FIFO admission queue—filters one-hit wonders
- **Layer 1**: Main RAM cache using segment, slab, or heap backend
- **Layer 2** (optional): Disk tier for extended capacity (segment/slab only)

### Hashtable

Lock-free multi-choice hashtable shared by all layers:

```
┌─────────────────────────────────────────────────────┐
│                    Hashtable                         │
├─────────────────────────────────────────────────────┤
│  Bucket 0: [Slot][Slot][Slot][Slot][Slot][Slot]    │
│  Bucket 1: [Slot][Slot][Slot][Slot][Slot][Slot]    │
│  ...                                                │
│  Bucket N: [Slot][Slot][Slot][Slot][Slot][Slot]    │
└─────────────────────────────────────────────────────┘
```

Features:
- **Multi-choice hashing**: Two candidate buckets per key reduces collision rate
- **Frequency counter**: Per-key access tracking for scan-resistant eviction
- **Ghost entries**: Track recently-evicted keys for smarter re-admission
- **Lock-free**: All operations via atomic CAS, no blocking

### Segments (segment backend)

Fixed-size memory regions with sequential item storage:

```
┌─────────────────────────────────────────────────────┐
│                 Segment (1MB default)                │
├─────────────────────────────────────────────────────┤
│ Item │ Item │ Item │ Item │ ... │    Free Space    │
└─────────────────────────────────────────────────────┘
```

Key properties:
- **Sequential writes**: Items appended sequentially, no per-item allocation
- **TTL-aware organization**: Segments grouped by expiration time for efficient proactive expiration
- **Bulk eviction**: Entire segments evicted at once
- **Hugepage-friendly**: Segments align to 2MB/1GB pages

State machine:
```
Free → Writing → Readable → Evicting → Free
```

### Memory Pool

Pre-allocates segments from a contiguous allocation:

- Optional hugepage backing (2MB or 1GB pages)
- NUMA-aware allocation when configured
- Zero allocation on hot path—segments checked out and returned

## Server Architecture

### Native Runtime

```
┌─────────────────────────────────────────────────────┐
│                   Main Thread                        │
│  - Parse config                                     │
│  - Initialize cache                                 │
│  - Spawn workers                                    │
│  - Wait for shutdown                                │
└─────────────────────────────────────────────────────┘
         │
         │ spawn
         ▼
┌─────────────────────────────────────────────────────┐
│                  Acceptor Thread                     │
│  - Bind to listener addresses                       │
│  - Accept connections                               │
│  - Distribute FDs to workers (round-robin)          │
└─────────────────────────────────────────────────────┘
         │
         │ crossbeam channel (fd, addr)
         ▼
┌─────────────────────────────────────────────────────┐
│              Worker Threads (pinned)                 │
│                                                      │
│  Worker 0 (CPU 0)     Worker 1 (CPU 1)     ...     │
│  ┌─────────────┐      ┌─────────────┐              │
│  │ loop {      │      │ loop {      │              │
│  │   recv_fds()│      │   recv_fds()│              │
│  │   poll()    │      │   poll()    │              │
│  │   drain()   │      │   drain()   │              │
│  │   process() │      │   process() │              │
│  │ }           │      │ }           │              │
│  └─────────────┘      └─────────────┘              │
└─────────────────────────────────────────────────────┘
```

### Tokio Runtime

```
┌─────────────────────────────────────────────────────┐
│              Tokio Multi-Thread Runtime              │
│                                                      │
│  ┌─────────────┐      ┌─────────────┐              │
│  │ Accept Task │      │ Accept Task │     ...      │
│  │ (listener 0)│      │ (listener 1)│              │
│  └──────┬──────┘      └──────┬──────┘              │
│         │                    │                      │
│         │ spawn              │ spawn                │
│         ▼                    ▼                      │
│  ┌─────────────┐      ┌─────────────┐              │
│  │ Conn Task 0 │      │ Conn Task N │     ...      │
│  │  (async)    │      │  (async)    │              │
│  └─────────────┘      └─────────────┘              │
│                                                      │
│  Work-stealing scheduler moves tasks between threads │
└─────────────────────────────────────────────────────┘
```

## Benchmark Architecture

```
┌─────────────────────────────────────────────────────┐
│                  crucible-benchmark                  │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌─────────────┐                                    │
│  │ Main Thread │                                    │
│  │ - Config    │                                    │
│  │ - Spawn     │                                    │
│  │ - Report    │                                    │
│  └──────┬──────┘                                    │
│         │                                           │
│    ┌────┴────┬────────┬────────┐                   │
│    ▼         ▼        ▼        ▼                   │
│  ┌────┐   ┌────┐   ┌────┐   ┌────────────────┐    │
│  │ W0 │   │ W1 │   │ WN │   │ Admin (Tokio)  │    │
│  │    │   │    │   │    │   │ - Prometheus   │    │
│  │ io │   │ io │   │ io │   │ - Parquet      │    │
│  └────┘   └────┘   └────┘   └────────────────┘    │
│                                                      │
│  Workers:                                           │
│  - Generate requests (keyspace, distribution)       │
│  - Track in-flight requests with timestamps         │
│  - Record latency histograms                        │
│  - Report metrics to shared counters                │
│                                                      │
└─────────────────────────────────────────────────────┘
```

## Data Flow: GET Request

```
1. Client sends: *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n

2. io-driver receives data
   └── Completion { kind: Recv, conn_id, bytes }

3. Protocol parser extracts command
   └── Command::Get { key: "foo" }

4. Cache lookup
   ├── Hashtable lookup (lock-free CAS)
   ├── Increment frequency counter
   ├── Read value from storage (RAM or disk)
   └── If disk hit with high frequency: promote to RAM

5. Protocol encoder builds response
   └── $3\r\nbar\r\n (or $-1\r\n for miss)

6. io-driver sends response
   └── send_owned() for zero-copy, or send() with copy

7. Client receives: $3\r\nbar\r\n
```

## Data Flow: SET Request

```
1. Client sends: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n

2. io-driver receives data
   └── Completion { kind: Recv, conn_id, bytes }

3. Protocol parser extracts command
   └── Command::Set { key: "foo", value: "bar", ttl: None }

4. Cache insertion
   ├── Write to storage (segment/slab/heap)
   └── Update hashtable (CAS operation)

5. Protocol encoder builds response
   └── +OK\r\n

6. io-driver sends response

7. Client receives: +OK\r\n
```

## Eviction Flow

Eviction behavior depends on the storage backend and policy. General flow:

```
1. Memory pressure detected (used > threshold)

2. Select victim(s) based on policy:
   ├── FIFO: oldest items
   ├── Random: random selection
   ├── S3-FIFO: filter by frequency, admit hot items to main cache
   ├── CTE: closest to expiration (segment backend)
   └── Merge: prune low-frequency items, retain high-frequency (segment backend)

3. For each victim:
   ├── If disk tier enabled: demote to disk
   ├── Else if ghosts enabled: convert to ghost entry
   └── Else: remove from hashtable

4. Reclaim storage space
```
