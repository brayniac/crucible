# Design Philosophy

This document explains the design decisions behind Crucible and why we built things the way we did.

## Goals

Crucible is built for **production cache deployments where performance matters**. Our target is sub-millisecond p99 latency at hundreds of thousands of requests per second. This goal drives every architectural decision.

Non-goals:
- Feature parity with Redis (we support a subset of commands)
- Clustering or replication (use an external proxy)
- Persistence (this is a cache, not a database)

## Why We Don't Use Tokio

Tokio is an excellent async runtime for general-purpose Rust applications. We evaluated it extensively and chose to build our own I/O layer instead. Here's why:

### 1. Completion-Based vs Async/Await

Tokio uses Rust's async/await with a work-stealing task scheduler. Tasks can be moved between threads, and the scheduler adds overhead to every `.await` point.

Crucible uses a **completion-based event loop**:

```rust
loop {
    driver.poll(timeout)?;
    for completion in driver.drain_completions() {
        handle(completion);
    }
}
```

This model:
- **No task scheduler**: Workers run a simple loop with no scheduling decisions
- **No context switching**: Each completion is handled immediately, in order
- **Predictable latency**: No variance from work-stealing or task migration
- **Cache-friendly**: Data stays on one core, in L1/L2 cache

For a cache server handling millions of simple GET/SET operations, we expect the overhead of task scheduling to be measurable, though we haven't published formal benchmarks comparing the runtimes yet. The server supports both runtimes (`runtime = "native"` or `runtime = "tokio"`) so you can evaluate this for your workload.

### 2. Direct Access to io_uring Features

Linux's io_uring provides features that dramatically reduce system call overhead and enable zero-copy I/O.

**Why not Tokio?** Tokio uses epoll/kqueue, not io_uring. It doesn't support any of these features.

**Why not tokio-uring or glommio?** These provide io_uring with async/await, but the fundamental issue is the async model itself:
- Buffer lifetimes across `.await` points are complex (futures are state machines)
- Zero-copy send requires holding buffers until kernel signals completion—awkward with async ownership
- We wanted explicit control over the event loop, not a runtime making scheduling decisions

The completion-based model (poll → drain → handle) gives us direct control that async runtimes abstract away, regardless of whether they use io_uring underneath.

Features we use:

| Feature | What It Does | Performance Impact |
|---------|--------------|-------------------|
| **Multishot recv** | One submission handles multiple receives | Eliminates per-recv syscall overhead |
| **Ring-provided buffers** | Kernel manages buffer pool | Zero-copy receive path |
| **SendZc** | DMA directly from user buffers | Zero-copy send path |
| **SQPOLL** | Kernel thread polls submission queue | Eliminates all syscalls in hot path |
| **Registered file descriptors** | Pre-registered FDs with kernel | Faster FD lookup |

With all features enabled on Linux 6.0+, Crucible can handle a request with **zero memory copies** and **zero system calls** in the steady state.

### 3. Buffer Lifecycle Management

Zero-copy I/O requires careful control over buffer lifetimes. The kernel needs to know when it's safe to reuse a buffer.

With async/await, buffers must live across `.await` points, which means:
- They're typically heap-allocated
- Lifetimes are complex (futures are state machines)
- Hard to know when the kernel is done with a buffer

Our completion model makes this explicit:
- `send_owned()` takes ownership of the buffer
- We hold it until the kernel signals completion (NOTIF event)
- Then we release it back to the pool

This is simpler to reason about and enables true zero-copy.

### 4. Thread-Per-Core Architecture

Crucible pins each worker to a specific CPU core:

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Server Process                              │
├─────────────────────────────────────────────────┬───────────────────┤
│                   DATA PLANE                    │   CONTROL PLANE   │
├───────────┬───────────┬───────────┬─────────────┼───────────────────┤
│  Worker 0 │  Worker 1 │  Worker 2 │  Worker 3   │      Admin        │
│  (CPU 0)  │  (CPU 1)  │  (CPU 2)  │  (CPU 3)    │                   │
│           │           │           │             │   Tokio runtime   │
│ io-driver │ io-driver │ io-driver │  io-driver  │   /health         │
│ instance  │ instance  │ instance  │  instance   │   /ready          │
│           │           │           │             │   /metrics        │
├───────────┴───────────┴───────────┴─────────────┤                   │
│                                                 │                   │
│               Shared Cache                      │                   │
│           (lock-free, thread-safe)              │                   │
│                                                 │                   │
└─────────────────────────────────────────────────┴───────────────────┘
```

**Data plane**: Workers handle RESP/Memcache requests. Each has its own io-driver instance (io_uring on Linux 6.0+, mio elsewhere), pinned to a CPU core. All workers share a single lock-free cache.

**Control plane**: Admin thread runs a single-threaded Tokio runtime for health checks and Prometheus metrics. Isolated from workers so it doesn't affect request latency.

Benefits:
- **No I/O contention**: Each worker has independent I/O
- **Shared cache**: Full memory available to all workers, no partitioning
- **Lock-free access**: Workers don't block on cache operations
- **CPU cache friendly**: Each worker's hot data stays in its L1/L2
- **NUMA-aware**: Can bind workers and cache memory to same NUMA node

Tokio's work-stealing scheduler moves tasks between threads, which can hurt CPU cache locality. For our workload (many small, similar-cost operations), predictable per-core execution is more important than load balancing.

### When We Do Use Tokio

The admin/metrics server uses Tokio because:
- It's not performance-critical (metrics scraped every few seconds)
- It benefits from async ecosystem (hyper, tower)
- It runs on a separate thread, isolated from workers

## The io-driver Abstraction

We built `io-driver` as a unified abstraction over io_uring and mio:

```
┌─────────────────────────────────────────┐
│              Application                 │
│  (server, benchmark, your code)         │
├─────────────────────────────────────────┤
│              io-driver API               │
│  poll(), drain_completions(), send()    │
├─────────────┬───────────────────────────┤
│  io_uring   │          mio              │
│ (Linux 6.0+)│  (Linux, macOS, BSD)      │
└─────────────┴───────────────────────────┘
```

The driver auto-selects the best backend at runtime. Code written against `io-driver` works everywhere but gets io_uring optimizations on modern Linux.

### Copy Semantics

Understanding where copies happen is critical for performance.

**Network I/O copies:**

| Mode | Recv | Send | Total |
|------|------|------|-------|
| io_uring single-shot + SendZc | 0 | 0 | **0 copies** |
| io_uring multishot + SendZc | 1 (ring → coalesce) | 0 | **1 copy** |
| mio (epoll/kqueue) | 1 (read syscall) | 1 (write syscall) | **2 copies** |

We default to multishot recv because it reduces syscall overhead, but single-shot is available for latency-sensitive deployments.

**Cache storage copies (segment backend):**

For reads, SendZc can DMA directly from segment memory to network—zero-copy from cache to client.

For writes, it depends on item size:
- **Small items** (fit in single recv): The value is already in the receive buffer before we know where it goes, so we copy to segment (1 copy)
- **Large items** (span multiple recvs): After parsing the header, we can reserve segment space and recv remaining data directly into it (0 copies for the remaining data)

### Allocation Strategy

With the segment backend (default), the hot path (recv → parse → cache operation → send) has **zero heap allocations**:

- **I/O buffers**: Pre-allocated in pools at startup. Workers check out buffers, use them, and return them. LIFO reuse keeps recently-used buffers in CPU cache.
- **Cache storage**: Segments are pre-allocated from a contiguous memory pool (optionally backed by hugepages). Items are written sequentially into segments, no per-item allocation.
- **Hashtable**: Fixed-size array allocated at startup. Entries are updated in-place via atomic operations.
- **Connection state**: Pre-allocated per connection slot. Connection IDs are reused with generation counters.

This matters because `malloc`/`free` on every request adds latency and variance. By pre-allocating everything, the hot path is just pointer arithmetic and memory access.

**Note:** The heap backend (`backend = "heap"`) intentionally uses per-item allocation for workloads where that trade-off makes sense (e.g., highly variable item sizes). The I/O path remains allocation-free.

## Storage Backends

Crucible separates **storage** (how items are physically stored) from **eviction policy** (how we decide what to remove). This section covers the three storage backends.

### Segment Backend (default)

Based on the [Segcache paper](https://www.usenix.org/conference/nsdi21/presentation/yang-juncheng), items are stored sequentially in fixed-size segments:

```
┌────────────────────────────────────────────────────┐
│                    Segment (1MB)                    │
├────────┬────────┬────────┬────────┬───────────────┤
│ Item 1 │ Item 2 │ Item 3 │ Item 4 │    (free)     │
│ 128B   │ 256B   │ 64B    │ 512B   │               │
└────────┴────────┴────────┴────────┴───────────────┘
```

**Strengths:**
- No per-item allocation (pre-allocated segment pool)
- Hugepage-friendly (segments align to 2MB/1GB pages)
- Efficient bulk eviction (evict entire segments)
- TTL-aware organization: segments are grouped by expiration time, so we know where expired items are and can reclaim them efficiently without scanning the entire cache

**Trade-offs:**
- Overwrites leave "holes" (old item invalidated, new item appended)
- Holes waste space until segment is evicted
- Merge eviction policy can reclaim holes (see below)

### Slab Backend

Memcached-style slab allocator with size classes:

```
┌─────────────────────────────────────────────────────┐
│  Slab Class 64B    │  Slab Class 256B   │  ...     │
├────┬────┬────┬────┼──────┬──────┬──────┼──────────┤
│ 64 │ 64 │ 64 │ 64 │ 256  │ 256  │ 256  │          │
└────┴────┴────┴────┴──────┴──────┴──────┴──────────┘
```

**Strengths:**
- In-place overwrites (same size class reuses slot)
- No holes from updates
- Familiar model for Memcached migrations

**Trade-offs:**
- Internal fragmentation (items rounded up to size class)
- No TTL-aware organization (expiration is lazy on read, or would require scanning)

### Heap Backend

Per-item allocation using the system allocator:

**Strengths:**
- No internal fragmentation
- Flexible for variable-size items
- Easier to support data structures (lists, sets, etc.) since items can grow/shrink

**Trade-offs:**
- `malloc`/`free` on hot path (latency variance)
- External fragmentation over time
- No hugepage benefits

Choose heap when item sizes vary dramatically or you need data structure flexibility.

**Note:** This backend is under active development. We're integrating learnings from Redis/Valkey to improve memory management and fragmentation handling.

### RAM → Disk Tiering

Both segment and slab backends support optional disk tiering for extended capacity beyond RAM:

```
┌─────────────────┐
│   RAM Layer     │  ← Hot data, fast access
│  (segment/slab) │
└────────┬────────┘
         │ demote (on eviction)
         ▼
┌─────────────────┐
│   Disk Layer    │  ← Warm data, slower but larger
│  (mmap'd file)  │
└─────────────────┘
         │ promote (on access if freq > threshold)
         ▲
```

When enabled:
- Items evicted from RAM are **demoted** to disk instead of discarded
- On disk hit, items with frequency above threshold are **promoted** back to RAM
- Effectively gives you a much larger cache with RAM-speed access for hot data

The heap backend does not currently support disk tiering.

## Eviction Policies

Eviction policies determine which items to remove under memory pressure. Available policies depend on the backend.

### Standard Policies

| Policy | Description | Backends |
|--------|-------------|----------|
| **FIFO** | First-in, first-out | segment, slab, heap |
| **Random** | Random eviction | segment, slab, heap |
| **LRU/LRA** | Least recently accessed | slab |
| **LRC** | Least recently created | slab |
| **CTE** | Closest to expiration | segment |
| **LFU** | Least frequently used | heap |

### S3-FIFO Policy

S3-FIFO (Simple, Scalable, Scan-resistant FIFO) is our default policy. Traditional LRU has problems:
- Scan resistance: One large scan evicts the entire working set
- Metadata overhead: Maintaining access order requires locks
- Poor real-world hit rates compared to more sophisticated policies

S3-FIFO uses an admission filter with frequency tracking:

```
┌─────────────────────────────────────────────────────┐
│                    Hashtable                         │
│  key → (location, frequency counter, ghost bit)     │
└───────────────────────┬─────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        ▼               │               ▼
┌───────────────┐       │       ┌───────────────┐
│   Small FIFO  │       │       │   Main FIFO   │
│  (admission)  │───────┘       │   (hot data)  │
└───────────────┘               └───────────────┘
        │                               │
        └───── evict if freq == 0 ──────┘
                     │
                     ▼
              ┌─────────────┐
              │   Ghost     │
              │  (tracking) │
              └─────────────┘
```

How it works:
1. New items enter the **small FIFO** (admission queue)
2. On eviction from small FIFO:
   - If frequency > 0: promote to **main FIFO**
   - If frequency == 0: evict (add to ghost set)
3. Ghost entries track recently-evicted keys
4. If a ghost key is re-requested: admit directly to main FIFO

**Benefits:**
- **Scan resistant**: One-hit wonders never pollute main cache
- **Simple**: Two FIFOs + frequency counter, no complex reordering
- **Scalable**: FIFO operations are naturally concurrent
- **Effective**: Matches or beats LRU on most workloads

### Merge Eviction (Segment Backend)

Merge eviction is a frequency-aware policy that retains high-value items and evicts low-value ones:

1. Select oldest candidate segments from TTL buckets
2. For each segment, prune items by frequency:
   - Items with frequency ≥ threshold are **retained**
   - Items with frequency < threshold are **evicted** (converted to ghosts or discarded)
3. Segments that become empty after pruning are reclaimed

This differs from simple FIFO/random eviction which evicts entire segments regardless of item value. Merge eviction keeps your hottest items even as segments age.

**Side benefit:** Since overwrites in the segment backend leave holes (old item invalidated, new item appended elsewhere), merge eviction also reclaims this wasted space as a natural consequence of pruning and segment reclamation.

## Lock-Free Hashtable

The hashtable uses **multi-choice hashing** with lock-free CAS operations:

```rust
// Simplified lookup
fn get(&self, key: &[u8]) -> Option<Value> {
    let hash = hash(key);
    for bucket in [primary_bucket(hash), secondary_bucket(hash)] {
        for slot in bucket.slots() {
            if slot.matches(key) {
                return Some(slot.value());
            }
        }
    }
    None
}
```

Features:
- **No locks**: All operations use atomic compare-and-swap
- **Multi-choice**: Two candidate buckets reduces collision rate
- **Ghost entries**: Track recently-evicted keys for admission decisions
- **Inline frequency**: 4-bit counter stored in slot metadata

## Why Two Runtimes?

The server supports both native (io-driver) and Tokio runtimes:

| Use Case | Recommended Runtime |
|----------|-------------------|
| Production on Linux 6.0+ | Native |
| Development/testing | Either |
| macOS | Native (uses mio) |
| Integration with async code | Tokio |
| Maximum performance | Native |
| Simpler deployment | Either |

Both runtimes are production-ready. We expect the native runtime to have better tail latency due to the completion-based model and lack of task scheduler overhead, but recommend benchmarking with your specific workload to validate. The server makes it easy to switch between runtimes via configuration.

## Benchmark Design

The benchmark tool uses the same principles as the server:
- **No Tokio**: Direct io-driver for accurate measurements
- **CPU pinning**: Reproducible results across runs
- **Precise timing**: Captures timestamps at exact points in the I/O path
- **Kernel timestamps**: Optional SO_TIMESTAMPING for sub-microsecond accuracy

This ensures benchmark results reflect actual server performance, not measurement overhead.

**Note:** The benchmark tool only supports the native io-driver, not Tokio. This was a deliberate choice to minimize measurement artifacts, but it does mean you can't directly compare "Tokio benchmark vs native benchmark" scenarios. For comparing server runtimes, use the same benchmark binary against both `runtime = "native"` and `runtime = "tokio"` server configurations.
