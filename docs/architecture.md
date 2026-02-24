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
│ ringline │ ringline │ ringline │   ringline   │   /health         │
│ instance  │ instance  │ instance  │   instance    │   /ready          │
│           │           │           │               │   /metrics        │
├───────────┴───────────┴───────────┴───────────────┤                   │
│                                                   │                   │
│                   Shared Cache                    │                   │
│               (lock-free, thread-safe)            │                   │
│                                                   │                   │
└───────────────────────────────────────────────────┴───────────────────┘
```

**Data plane**: Workers handle RESP/Memcache requests. Each runs as an async task on a ringline io_uring worker (Linux 6.0+), optionally pinned to a CPU core. All workers share a single lock-free cache.

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
├── protocol/
│   ├── momento/      # Momento cache protocol
│   └── ping/         # Simple ping (testing)
├── server/           # Cache server binary
├── proxy/            # Redis proxy with optional local caching
├── metrics/          # Metrics infrastructure
└── xtask/            # Dev tasks (fuzz, flamegraph)
```

### Dependency Flow

```
server
  │
  ├──► ringline        ←── io_uring event loop (async task per connection)
  ├──► resp-proto      ←── Redis RESP2/RESP3 protocol
  ├──► memcache-proto  ←── Memcache ASCII protocol
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

## I/O Layer (ringline)

The server uses [ringline](https://github.com/ringline-rs/ringline), an io_uring event loop with an async task-per-connection model. Each accepted connection becomes an independent async task.

**io_uring features used:**
- Multishot recv/accept
- Ring-provided buffers
- Zero-copy send (SendMsgZc)
- Fixed file descriptors
- Dedicated acceptor thread distributing connections round-robin to workers

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

## Benchmark Tool

The benchmark tool has been moved to a separate repository:
[cachecannon](https://github.com/cachecannon/cachecannon).

Cachecannon uses the same ringline principles (direct io_uring, CPU pinning,
precise timing) to ensure benchmark results reflect actual server performance
rather than measurement overhead.

## Data Flow: GET Request

```
1. Client sends: *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n

2. ringline receives data
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

6. ringline sends response
   └── send_owned() for zero-copy, or send() with copy

7. Client receives: $3\r\nbar\r\n
```

## Data Flow: SET Request

```
1. Client sends: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n

2. ringline receives data
   └── Completion { kind: Recv, conn_id, bytes }

3. Protocol parser extracts command
   └── Command::Set { key: "foo", value: "bar", ttl: None }

4. Cache insertion
   ├── Write to storage (segment/slab/heap)
   └── Update hashtable (CAS operation)

5. Protocol encoder builds response
   └── +OK\r\n

6. ringline sends response

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
