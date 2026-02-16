//! krio — io_uring-native async I/O runtime for Linux.
//!
//! krio is a push-based, thread-per-core I/O framework built directly on
//! io_uring. It provides two APIs for building high-performance network
//! applications:
//!
//! - **Callback API** ([`EventHandler`]) — zero-overhead callbacks driven by
//!   io_uring completions. Ideal for latency-critical servers.
//! - **Async API** ([`AsyncEventHandler`]) — async/await ergonomics on the
//!   same thread-per-core event loop with no work-stealing.
//!
//! # Quick Start (Callback API)
//!
//! ```rust,no_run
//! use krio::{Config, ConnToken, DriverCtx, EventHandler, KrioBuilder};
//! use std::io;
//!
//! struct Echo;
//!
//! impl EventHandler for Echo {
//!     fn on_accept(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}
//!     fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
//!         ctx.send(conn, data).ok();
//!         data.len()
//!     }
//!     fn on_send_complete(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken, _r: io::Result<u32>) {}
//!     fn on_close(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}
//!     fn create_for_worker(_worker_id: usize) -> Self { Echo }
//! }
//!
//! fn main() -> Result<(), krio::Error> {
//!     let config = Config::default();
//!     let (_shutdown, handles) = KrioBuilder::new(config)
//!         .bind("127.0.0.1:7878")
//!         .launch::<Echo>()?;
//!     for h in handles { h.join().unwrap()?; }
//!     Ok(())
//! }
//! ```
//!
//! # Platform
//!
//! Linux 6.0+ only. Requires io_uring with multishot recv, ring-provided
//! buffers, SendMsgZc, and fixed file table support.

// ── Internal modules ────────────────────────────────────────────────────
pub(crate) mod acceptor;
pub(crate) mod accumulator;
pub(crate) mod async_event_loop;
pub(crate) mod buffer;
pub(crate) mod chain;
pub(crate) mod completion;
pub(crate) mod connection;
pub(crate) mod driver;
pub(crate) mod event_loop;
pub(crate) mod ring;
pub(crate) mod runtime;
#[cfg(feature = "tls")]
pub(crate) mod tls;
pub(crate) mod worker;

// ── Public modules ──────────────────────────────────────────────────────
pub mod config;
pub mod error;
pub mod guard;
pub mod handler;

// ── Re-exports: Callback API ────────────────────────────────────────────

/// Builder for chained send parts.
pub use handler::ChainPartsBuilder;
/// Opaque connection handle.
pub use handler::ConnToken;
/// I/O context passed to [`EventHandler`] callbacks.
pub use handler::DriverCtx;
/// Trait for callback-driven event handlers.
pub use handler::EventHandler;
/// Builder for constructing a scatter-gather send.
pub use handler::SendBuilder;
/// Builder for IO_LINK chained sends.
pub use handler::SendChainBuilder;

// ── Re-exports: Async API ───────────────────────────────────────────────

/// Trait for async event handlers (one task per connection).
pub use runtime::handler::AsyncEventHandler;
/// Async scatter-gather send builder.
pub use runtime::io::AsyncSendBuilder;
/// Async connection context with send/recv futures.
pub use runtime::io::ConnCtx;
/// Future that completes when a connect finishes.
pub use runtime::io::ConnectFuture;
/// Error returned when a [`timeout()`] expires.
pub use runtime::io::Elapsed;
/// Future that completes when a send finishes.
pub use runtime::io::SendFuture;
/// Future returned by [`sleep()`].
pub use runtime::io::SleepFuture;
/// Future returned by [`timeout()`].
pub use runtime::io::TimeoutFuture;
/// Future that provides received data.
pub use runtime::io::WithDataFuture;
/// Create a future that completes after a duration.
pub use runtime::io::sleep;
/// Spawn a standalone async task on the current worker.
pub use runtime::io::spawn;
/// Wrap a future with a deadline.
pub use runtime::io::timeout;
/// Fallible sleep that returns an error if the timer pool is exhausted.
pub use runtime::io::try_sleep;
/// Spawn a standalone task, returning an error if the slab is full.
pub use runtime::io::try_spawn;
/// Fallible timeout that returns an error if the timer pool is exhausted.
pub use runtime::io::try_timeout;
/// Opaque handle for a standalone spawned task.
pub use runtime::task::TaskId;
/// Error returned by [`try_spawn()`] when the standalone task slab is full.
pub use error::SpawnError;
/// Error returned by [`try_sleep()`] and [`try_timeout()`] when the timer pool is full.
pub use error::TimerExhausted;
/// Poll two futures concurrently, returning whichever completes first.
pub use runtime::select::select;
/// Poll three futures concurrently, returning whichever completes first.
pub use runtime::select::select3;
/// Result of [`select()`] — which branch completed.
pub use runtime::select::Either;
/// Result of [`select3()`] — which branch completed.
pub use runtime::select::Either3;
/// Future returned by [`select()`].
pub use runtime::select::Select;
/// Future returned by [`select3()`].
pub use runtime::select::Select3;

// ── Re-exports: Shared types ────────────────────────────────────────────

/// Memory region for io_uring fixed buffer registration.
pub use buffer::fixed::MemoryRegion;
/// Region identifier for [`SendGuard`] implementations.
pub use buffer::fixed::RegionId;
/// Maximum zero-copy guards per scatter-gather send.
pub use buffer::send_slab::MAX_GUARDS;
/// Maximum iovecs per scatter-gather send.
pub use buffer::send_slab::MAX_IOVECS;
/// Runtime configuration.
pub use config::Config;
/// Recv buffer ring configuration.
pub use config::RecvBufferConfig;
/// Worker thread configuration.
pub use config::WorkerConfig;
/// Runtime errors.
pub use error::Error;
/// Zero-copy send guard trait.
pub use guard::{GuardBox, SendGuard};
/// Builder for launching krio workers.
pub use worker::KrioBuilder;
/// Handle for triggering graceful shutdown.
pub use worker::ShutdownHandle;
/// Convenience function to launch workers with a listener.
pub use worker::launch;

// ── Re-exports: TLS (feature-gated) ────────────────────────────────────

/// Client-side TLS configuration.
#[cfg(feature = "tls")]
pub use config::TlsClientConfig;
/// Server-side TLS configuration.
#[cfg(feature = "tls")]
pub use config::TlsConfig;
/// TLS session info (protocol version, cipher suite, etc.).
#[cfg(feature = "tls")]
pub use tls::TlsInfo;
