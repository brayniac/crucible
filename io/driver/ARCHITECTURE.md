# I/O Driver Architecture

This document provides a detailed analysis of the Crucible I/O driver, focusing on buffer management, copy semantics, and allocation strategies that enable high-performance networking.

## Overview

The I/O driver provides a unified async I/O abstraction with two backends:

| Backend      | Platform      | Features                                             |
|--------------|---------------|------------------------------------------------------|
| **io_uring** | Linux 6.0+    | Multishot recv/accept, SendZc, ring-provided buffers |
| **mio**      | All platforms | epoll (Linux), kqueue (macOS/BSD)                    |

The driver automatically selects the best available backend at runtime.

## Copy Analysis

Understanding where data is copied is critical for performance. Here's a breakdown of copy counts for each code path:

### Receive Path

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           RECEIVE DATA FLOW                                  │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐      ┌──────────────┐      ┌──────────────┐                 │
│  │   Network   │──1──▶│ Kernel Buffer│──?──▶│  User Buffer │                 │
│  │   (wire)    │      │  (socket)    │      │  (app-owned) │                 │
│  └─────────────┘      └──────────────┘      └──────────────┘                 │
│                                                                              │
│  Copy 1: Always happens (NIC DMA to kernel)                                  │
│  Copy 2: Depends on recv mode (see below)                                    │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

| Backend  | Recv Mode   | Copies (kernel→user) | Notes                                 |
|----------|-------------|----------------------|---------------------------------------|
| io_uring | Single-shot | **0**                | Kernel writes directly to user buffer |
| io_uring | Multishot   | **1**                | Data copied to coalesce buffer        |
| mio      | N/A         | **1**                | Standard read() syscall               |

**Why multishot requires a copy:** With multishot recv, the kernel auto-rearms the recv operation immediately after completion. The buffer must be returned to the ring before the next completion can use it, so we copy to a coalesce buffer to hold the data while parsing.

**When single-shot achieves zero-copy:** The pool buffer remains valid until explicitly returned. If the application consumes all data before the next recv, no copy is needed.

### Send Path

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            SEND DATA FLOW                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐      ┌──────────────┐      ┌─────────────┐                 │
│  │  User Buffer │──?──▶│ Kernel Buffer│──1──▶│   Network   │                 │
│  │  (response)  │      │  (socket)    │      │   (wire)    │                 │
│  └──────────────┘      └──────────────┘      └─────────────┘                 │
│                                                                              │
│  Copy 1: Depends on send mode (see below)                                    │
│  Copy 2: Always happens (kernel to NIC DMA)                                  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

| Backend  | Send Mode | Copies (user→kernel) | Notes                                 |
|----------|-----------|----------------------|---------------------------------------|
| io_uring | SendZc    | **0**                | Kernel DMAs directly from user buffer |
| io_uring | Regular   | **1**                | Standard send() with kernel copy      |
| mio      | N/A       | **1**                | Standard write() syscall              |

**SendZc constraints:** The user buffer must remain valid until the kernel signals completion via a NOTIF event. The driver manages this by taking ownership of the buffer.

### End-to-End Copy Summary

| Scenario                                 | Recv Copies | Send Copies | Total |
|------------------------------------------|-------------|-------------|-------|
| io_uring single-shot recv + SendZc       | 0           | 0           | **0** |
| io_uring multishot recv + SendZc         | 1           | 0           | **1** |
| io_uring single-shot recv + regular send | 0           | 1           | **1** |
| io_uring multishot recv + regular send   | 1           | 1           | **2** |
| mio (any mode)                           | 1           | 1           | **2** |

## Buffer Architecture

### Memory Pools

The driver uses several specialized buffer pools optimized for different access patterns:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          BUFFER POOL HIERARCHY                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    BufRing (io_uring multishot only)                │    │
│  │  ┌────┬────┬────┬────┬────┬────┬────┬────┐                          │    │
│  │  │ 0  │ 1  │ 2  │ 3  │ 4  │ 5  │... │8191│  8192 × 16KB = 128MB     │    │
│  │  └────┴────┴────┴────┴────┴────┴────┴────┘                          │    │
│  │  Kernel picks buffers directly from ring. Page-aligned.             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                 RecvBufferPool (io_uring single-shot)               │    │
│  │  ┌────┬────┬────┬────┬────┬────┬────┬────┐                          │    │
│  │  │ 0  │ 1  │ 2  │ 3  │ 4  │ 5  │... │8191│  8192 × 16KB = 128MB     │    │
│  │  └────┴────┴────┴────┴────┴────┴────┴────┘                          │    │
│  │  Pre-allocated Box<[u8]> slots. Pointer-stable for kernel I/O.      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    BufferPool (general purpose)                     │    │
│  │  ┌────┬────┬────┬────┬────┬────┬────┬────┐                          │    │
│  │  │ 0  │ 1  │ 2  │ 3  │ 4  │ 5  │... │4095│  4096 × 32KB = 128MB     │    │
│  │  └────┴────┴────┴────┴────┴────┴────┴────┘                          │    │
│  │  Application-facing pool. Supports BufferChain for scatter-gather.  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Pool Characteristics

| Pool           | Chunk Size | Total Size | Allocation        | Use Case                     |
|----------------|------------|------------|-------------------|------------------------------|
| BufRing        | 16KB       | 128MB      | Page-aligned mmap | Kernel ring-provided buffers |
| RecvBufferPool | 16KB       | 128MB      | Box<[u8]> array   | Single-shot recv targets     |
| BufferPool     | 32KB       | 128MB      | Contiguous Vec    | Application data handling    |

### Allocation Strategy

All pools use **O(1) allocation** via free lists:

```rust
// Checkout: O(1)
fn checkout(&mut self) -> Option<BufferId> {
    self.free_list.pop_front()  // VecDeque for LIFO
}

// Checkin: O(1)
fn checkin(&mut self, id: BufferId) {
    self.free_list.push_front(id)  // LIFO for cache locality
}
```

**LIFO reuse** ensures recently-used buffers (still in CPU cache) are reused first.

### Per-Connection Buffers

Each connection maintains its own receive state:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PER-CONNECTION BUFFER LAYOUT                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ConnectionRecvState {                                                      │
│      coalesce_buf: BytesMut,      // 16KB initial, grows as needed          │
│      pending_buffers: Vec<...>,   // Pool buffer references (single-shot)   │
│      read_position: (idx, off),   // Current parse position                 │
│  }                                                                          │
│                                                                             │
│  Coalesce buffer sizing:                                                    │
│  - Initial: 16KB (matches TLS max record size)                              │
│  - Growth: Automatic via BytesMut                                           │
│  - Shrink: When > 64KB and empty, shrinks to initial                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Details

### io_uring Single-Shot Recv (Zero-Copy Path)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                     SINGLE-SHOT RECV FLOW (ZERO-COPY)                        │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. submit_recv()                                                            │
│     ├── Allocate buffer from RecvBufferPool                                  │
│     ├── Get stable pointer (Box<[u8]> never moves)                           │
│     └── Submit SQE with buffer pointer                                       │
│                                                                              │
│  2. Kernel I/O                                                               │
│     ├── Kernel writes directly to pool buffer                                │
│     └── No user-space involvement                                            │
│                                                                              │
│  3. RecvComplete event                                                       │
│     ├── CQE contains buffer_id + bytes_received                              │
│     ├── Create BufferSlice (pointer to pool buffer)                          │
│     └── No copy yet                                                          │
│                                                                              │
│  4. Application callback                                                     │
│     ├── with_recv_buf(|slice| { ... })                                       │
│     ├── slice.as_slice() returns &[u8] to pool buffer                        │
│     ├── Parse protocol directly from pool buffer                             │
│     └── ZERO COPIES so far                                                   │
│                                                                              │
│  5. consume(n)                                                               │
│     ├── If all data consumed: return buffer to pool                          │
│     └── If partial: keep buffer, track position                              │
│                                                                              │
│  Total copies: 0 (when single recv satisfies request)                        │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### io_uring Multishot Recv (One-Copy Path)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                     MULTISHOT RECV FLOW (ONE-COPY)                           │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. Setup (once per connection)                                              │
│     ├── Submit multishot recv with BUFFER_SELECT flag                        │
│     └── Kernel will auto-rearm after each completion                         │
│                                                                              │
│  2. Kernel I/O                                                               │
│     ├── Kernel picks buffer from BufRing                                     │
│     ├── Writes data to ring buffer                                           │
│     └── Signals completion with buffer_id                                    │
│                                                                              │
│  3. RecvComplete event                                                       │
│     ├── CQE contains buffer_id from ring                                     │
│     ├── MUST return buffer quickly (kernel needs it)                         │
│     └── Copy data to coalesce_buf ← ONE COPY HERE                            │
│                                                                              │
│  4. Return ring buffer                                                       │
│     ├── return_buffer(buffer_id)                                             │
│     └── Buffer available for next multishot completion                       │
│                                                                              │
│  5. Application callback                                                     │
│     ├── with_recv_buf(|slice| { ... })                                       │
│     └── slice.as_slice() returns &[u8] to coalesce_buf                       │
│                                                                              │
│  Total copies: 1 (ring buffer → coalesce buffer)                             │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### SendZc Flow (Zero-Copy Send)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        SENDZC FLOW (ZERO-COPY)                               │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. Application prepares response                                            │
│     ├── Build response in BoxedZeroCopy buffer                               │
│     └── May use scatter-gather: [header, value, trailer]                     │
│                                                                              │
│  2. send_owned(conn_id, buffer)                                              │
│     ├── Driver takes ownership of BoxedZeroCopy                              │
│     ├── Extract io_slices() for scatter-gather                               │
│     └── Submit sendmsg_zc SQE                                                │
│                                                                              │
│  3. Kernel I/O                                                               │
│     ├── Kernel DMAs directly from user buffer                                │
│     ├── No copy to kernel socket buffer                                      │
│     └── ZERO COPIES                                                          │
│                                                                              │
│  4. NOTIF completion                                                         │
│     ├── Kernel signals buffer can be released                                │
│     └── Driver drops BoxedZeroCopy                                           │
│                                                                              │
│  Total copies: 0                                                             │
│                                                                              │
│  Constraint: Buffer must remain valid until NOTIF                        │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Buffer Lifecycle

### Ring-Provided Buffer Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    RING BUFFER LIFECYCLE                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│    ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐          │
│    │ In Ring  │────▶│  Kernel  │────▶│  Driver  │────▶│ In Ring  │          │
│    │ (avail)  │     │ (in-use) │     │ (copying)│     │ (avail)  │          │
│    └──────────┘     └──────────┘     └──────────┘     └──────────┘          │
│         │                │                │                │                │
│         │   kernel       │   completion   │   return_      │                │
│         │   picks        │   event        │   buffer()     │                │
│         │                │                │                │                │
│    ─────┴────────────────┴────────────────┴────────────────┴─────▶ time     │
│                                                                             │
│    Ring buffer lifetime in driver: ~microseconds (copy + return)            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Pool Buffer Lifecycle (Single-Shot)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    POOL BUFFER LIFECYCLE                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│    ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐          │
│    │ In Pool  │────▶│  Kernel  │────▶│   App    │────▶│ In Pool  │          │
│    │ (free)   │     │ (recv)   │     │ (parsing)│     │ (free)   │          │
│    └──────────┘     └──────────┘     └──────────┘     └──────────┘          │
│         │                │                │                │                │
│         │   checkout()   │   completion   │   consume() +  │                │
│         │   + submit     │   event        │   checkin()    │                │
│         │                │                │                │                │
│    ─────┴────────────────┴────────────────┴────────────────┴─────▶ time     │
│                                                                             │
│    Pool buffer lifetime in app: ~milliseconds (parse + process)             │
│    ZERO-COPY: App reads directly from pool buffer                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Connection ID and Generation Tracking

To prevent use-after-free bugs when connection slots are reused:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONNECTION ID ENCODING                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│    ConnId(u64) bit layout:                                                  │
│    ┌────────────────────────┬────────────────────────────────────┐          │
│    │     generation (32)    │           slot (32)                │          │
│    └────────────────────────┴────────────────────────────────────┘          │
│    63                      32 31                                 0          │
│                                                                             │
│    Scenario: Connection closes and slot is reused                           │
│                                                                             │
│    Time 0: ConnId(gen=0, slot=5) active                                     │
│    Time 1: Connection closes, slot 5 freed                                  │
│    Time 2: New connection, ConnId(gen=1, slot=5)                            │
│    Time 3: Stale completion arrives for gen=0                               │
│            → Generation mismatch detected, completion ignored               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## User Data Encoding (io_uring)

The 64-bit user_data field encodes operation context:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    USER_DATA BIT LAYOUT                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│    bits 63-40: generation (24 bits) - detect stale completions              │
│    bits 39-8:  id (32 bits) - connection/listener/socket id                 │
│    bits 7-4:   buf_idx (4 bits) - buffer index for sends                    │
│    bits 3-0:   op (4 bits) - operation type                                 │
│                                                                             │
│    ┌────────────────┬────────────────────────┬────────┬────────┐            │
│    │  generation    │          id            │buf_idx │   op   │            │
│    │   (24 bits)    │       (32 bits)        │(4 bits)│(4 bits)│            │
│    └────────────────┴────────────────────────┴────────┴────────┘            │
│    63              40 39                     8 7      4 3      0            │
│                                                                             │
│    Operation types: Accept, Recv, Send, SendZc, SendNotif, Close, etc.      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Configuration Options

### io_uring Settings

```toml
[uring]
# Submission queue polling (reduces syscalls, uses CPU)
sqpoll = false
sqpoll_idle_ms = 1000

# Ring-provided buffer pool
buffer_count = 8192    # Number of buffers in ring
buffer_size = "16KB"   # Size of each buffer

# Submission queue depth
sq_depth = 1024
```

### Hybrid Recv Mode

The driver uses a hybrid recv mode that automatically optimizes for both small requests
and large values:

- **Multishot recv** for command headers and small requests (efficient, 1 copy)
- **Direct recv** into user memory for large values (zero-copy for large SET operations)

This happens automatically - no configuration needed.

### Send Mode Options

```rust
pub enum SendMode {
    /// Always copy to kernel buffer (simple, safe)
    Buffered,

    /// Never copy, use SendZc (requires ownership transfer)
    ZeroCopy,

    /// Use ZeroCopy for sends larger than threshold
    Threshold(usize),
}
```

## Platform Capabilities

The driver queries capabilities at runtime:

```rust
pub struct Capabilities {
    pub zerocopy_send: bool,      // io_uring SendZc
    pub multishot_recv: bool,     // io_uring multishot
    pub multishot_accept: bool,   // io_uring multishot accept
    pub provided_buffers: bool,   // io_uring ring-provided buffers
    pub fixed_buffers: bool,      // io_uring registered buffers
    pub vectored_io: bool,        // Scatter-gather I/O
    pub direct_descriptors: bool, // io_uring fixed file descriptors
}
```

| Platform    | Capabilities                       |
|-------------|------------------------------------|
| Linux 6.0+  | All features                       |
| Linux < 6.0 | Basic io_uring (varies by version) |
| macOS/BSD   | vectored_io only (mio backend)     |

## Memory Usage Summary

| Component        | Per-Connection | Shared | Notes                   |
|------------------|----------------|--------|-------------------------|
| Coalesce buffer  | 16KB initial   | -      | Grows/shrinks as needed |
| Connection state | ~100 bytes     | -      | Metadata only           |
| BufRing          | -              | 128MB  | Multishot mode only     |
| BufferPool       | -              | 128MB  | Application-facing      |

**Total shared memory:** ~128-256MB

## Best Practices

### For Lowest Latency

1. Enable SendZc for responses
2. Process and consume data immediately
3. Avoid partial consumes that span recv boundaries

### For Highest Throughput

1. Enable `sqpoll` for reduced syscall overhead
2. Use vectored sends for multi-part responses
3. Batch buffer returns with `commit()`

### For Memory Efficiency

1. Set appropriate `buffer_count` for connection count
2. Monitor coalesce buffer growth
3. Use `SendMode::Threshold` for adaptive zero-copy
