# krio

**io_uring-native async I/O runtime for Linux.**

krio is a push-based, thread-per-core I/O framework built directly on io_uring.
It provides two APIs for high-performance network applications:

- **Callback API** (`EventHandler`) — zero-overhead callbacks driven by io_uring
  completions. Ideal for latency-critical servers.
- **Async API** (`AsyncEventHandler`) — async/await ergonomics on the same
  thread-per-core event loop with no work-stealing.

## What krio is

- An io_uring-native runtime that exploits advanced kernel features: multishot
  recv, ring-provided buffers, SendMsgZc (zero-copy send), fixed file table
- Thread-per-core with CPU pinning — no work-stealing, no task migration
- Linux 6.0+ only — no epoll/kqueue/IOCP fallback, no portability tax

## What krio is NOT

- A cross-platform runtime (Linux only, io_uring required)
- A Tokio replacement (different abstractions, not API-compatible)
- A general-purpose task scheduler (all tasks are `!Send`, pinned to cores)

## Quick Start: Callback API

```rust
use krio::{Config, ConnToken, DriverCtx, EventHandler, KrioBuilder};
use std::io;

struct Echo;

impl EventHandler for Echo {
    fn on_accept(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}
    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        ctx.send(conn, data).ok();
        data.len()
    }
    fn on_send_complete(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken, _r: io::Result<u32>) {}
    fn on_close(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}
    fn create_for_worker(_id: usize) -> Self { Echo }
}

fn main() -> Result<(), krio::Error> {
    let config = Config::default();
    let (_shutdown, handles) = KrioBuilder::new(config)
        .bind("127.0.0.1:7878")
        .launch::<Echo>()?;
    for h in handles { h.join().unwrap()?; }
    Ok(())
}
```

## Quick Start: Async API

```rust
use krio::{AsyncEventHandler, Config, ConnCtx, KrioBuilder};
use std::future::Future;
use std::pin::Pin;

struct Echo;

impl AsyncEventHandler for Echo {
    fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        Box::pin(async move {
            loop {
                let n = conn.with_data(|data| {
                    conn.send(data).ok();
                    data.len()
                }).await;
                if n == 0 { break; }
            }
        })
    }
    fn create_for_worker(_id: usize) -> Self { Echo }
}

fn main() -> Result<(), krio::Error> {
    let config = Config::default();
    let (_shutdown, handles) = KrioBuilder::new(config)
        .bind("127.0.0.1:7878")
        .launch_async::<Echo>()?;
    for h in handles { h.join().unwrap()?; }
    Ok(())
}
```

## Architecture

```
                      ┌─────────────────────────────┐
                      │        Acceptor Thread       │
                      │   blocking accept4() loop    │
                      └──────────┬──────────────────┘
                                 │ round-robin
          ┌──────────────────────┼──────────────────────┐
          ▼                      ▼                      ▼
   ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
   │  Worker 0   │       │  Worker 1   │       │  Worker N   │
   │ (CPU pinned)│       │ (CPU pinned)│       │ (CPU pinned)│
   │             │       │             │       │             │
   │  io_uring   │       │  io_uring   │       │  io_uring   │
   │  event loop │       │  event loop │       │  event loop │
   │             │       │             │       │             │
   │ EventHandler│       │ EventHandler│       │ EventHandler│
   │ callbacks   │       │ callbacks   │       │ callbacks   │
   └─────────────┘       └─────────────┘       └─────────────┘
```

Each worker thread owns:
- A dedicated **io_uring** instance (SQ + CQ)
- A **ring-provided buffer pool** for recv (kernel selects buffers at completion time)
- A **send copy pool** for small sends and a **send slab** for scatter-gather zero-copy sends
- A **fixed file table** for O(1) fd lookups (no per-syscall fd table traversal)
- A **connection table** with generation-based stale detection

## io_uring Features Used

| Feature | Purpose |
|---------|---------|
| Multishot recv | Single SQE submission, multiple completions — no resubmission overhead |
| Ring-provided buffers | Kernel-managed recv buffer pool — kernel picks buffer at completion time |
| SendMsgZc | Zero-copy scatter-gather send — kernel DMAs directly from app buffers |
| Fixed file table | Direct descriptors — no per-syscall fd table lookup |
| IO_LINK chains | Atomic multi-step operations (connect + timeout) |
| COOP_TASKRUN | Reduced context switches |
| SINGLE_ISSUER | Lock-free kernel-side optimizations |

## Key Types

### Callback API

| Type | Description |
|------|-------------|
| `EventHandler` | Trait with callbacks: `on_accept`, `on_data`, `on_send_complete`, `on_close`, `on_tick` |
| `ConnToken` | Opaque connection handle with `index()` for per-connection arrays |
| `DriverCtx` | I/O context: `send()`, `send_parts()`, `close()`, `connect()`, `shutdown_write()` |
| `SendBuilder` | Scatter-gather send builder with `.copy()` and `.guard()` parts |
| `SendGuard` | Trait for zero-copy send buffers (pins memory until kernel send completes) |

### Async API

| Type | Description |
|------|-------------|
| `AsyncEventHandler` | Trait: `on_accept(ConnCtx) -> Future` — one task per connection |
| `ConnCtx` | Async connection context: `send()`, `with_data()`, `send_await()`, `connect()` |
| `WithDataFuture` | Future that resolves when recv data is available |
| `SendFuture` | Future that resolves when a send completes |
| `ConnectFuture` | Future that resolves when an outbound connection completes |

### Shared

| Type | Description |
|------|-------------|
| `KrioBuilder` | Builder: `KrioBuilder::new(config).bind(addr).launch::<H>()` |
| `Config` | Runtime configuration (SQ size, buffer sizes, worker count, TLS, etc.) |
| `ShutdownHandle` | Triggers graceful shutdown of all workers |
| `GuardBox` | Type-erased container for `SendGuard` (64-byte inline storage, no heap) |

## Platform Requirements

- **Linux 6.0+** (io_uring with required features)
- **x86_64** or **ARM64**

## MSRV

Rust 1.85+ (edition 2024)

## Examples

```bash
# Echo server (callback API)
cargo run --example echo_server

# Echo client (connects to echo server)
cargo run --example echo_client

# Benchmark: callback vs async
cargo run --release --example echo_bench

# Outbound connect example
cargo run --example connect_echo

# TLS echo server
cargo run --example echo_tls_server --features tls
```

## License

MIT OR Apache-2.0
