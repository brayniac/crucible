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

/// Trait for callback-driven event handlers.
pub use handler::EventHandler;
/// Opaque connection handle.
pub use handler::ConnToken;
/// I/O context passed to [`EventHandler`] callbacks.
pub use handler::DriverCtx;
/// Builder for constructing a scatter-gather send.
pub use handler::SendBuilder;
/// Builder for IO_LINK chained sends.
pub use handler::SendChainBuilder;
/// Builder for chained send parts.
pub use handler::ChainPartsBuilder;

// ── Re-exports: Async API ───────────────────────────────────────────────

/// Trait for async event handlers (one task per connection).
pub use runtime::handler::AsyncEventHandler;
/// Async connection context with send/recv futures.
pub use runtime::io::ConnCtx;
/// Future that provides received data.
pub use runtime::io::WithDataFuture;
/// Future that completes when a send finishes.
pub use runtime::io::SendFuture;
/// Future that completes when a connect finishes.
pub use runtime::io::ConnectFuture;
/// Async scatter-gather send builder.
pub use runtime::io::AsyncSendBuilder;

// ── Re-exports: Shared types ────────────────────────────────────────────

/// Builder for launching krio workers.
pub use worker::KrioBuilder;
/// Handle for triggering graceful shutdown.
pub use worker::ShutdownHandle;
/// Convenience function to launch workers with a listener.
pub use worker::launch;
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
/// Memory region for io_uring fixed buffer registration.
pub use buffer::fixed::MemoryRegion;
/// Region identifier for [`SendGuard`] implementations.
pub use buffer::fixed::RegionId;
/// Maximum zero-copy guards per scatter-gather send.
pub use buffer::send_slab::MAX_GUARDS;
/// Maximum iovecs per scatter-gather send.
pub use buffer::send_slab::MAX_IOVECS;

// ── Re-exports: TLS (feature-gated) ────────────────────────────────────

/// Server-side TLS configuration.
#[cfg(feature = "tls")]
pub use config::TlsConfig;
/// Client-side TLS configuration.
#[cfg(feature = "tls")]
pub use config::TlsClientConfig;
/// TLS session info (protocol version, cipher suite, etc.).
#[cfg(feature = "tls")]
pub use tls::TlsInfo;
