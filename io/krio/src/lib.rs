//! krio — high-performance I/O runtime for Linux.
//!
//! krio is a push-based, thread-per-core I/O framework. It supports two
//! backends selected via Cargo features:
//!
//! - **`io_uring`** (default) — built directly on io_uring (Linux 6.0+).
//!   Provides zero-copy sends, ring-provided recv buffers, fixed file table,
//!   and NVMe passthrough. Both the callback API ([`EventHandler`]) and the
//!   async API are available.
//!
//! - **`mio_backend`** — epoll-based fallback via mio. Works on any Linux
//!   kernel that supports epoll. Only the callback API is available; sends
//!   always copy through the kernel (no zero-copy).
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
//! Linux only. The io_uring backend requires Linux 6.0+; the mio backend
//! works on any Linux version with epoll support.

// Compile-time check: io_uring and mio_backend are mutually exclusive.
#[cfg(all(feature = "io_uring", feature = "mio_backend"))]
compile_error!(
    "features `io_uring` and `mio_backend` are mutually exclusive — \
     enable one or the other, not both"
);

#[cfg(not(any(feature = "io_uring", feature = "mio_backend")))]
compile_error!(
    "either feature `io_uring` or `mio_backend` must be enabled"
);

// ── Internal modules (always compiled) ───────────────────────────────
pub(crate) mod acceptor;
pub(crate) mod accumulator;
pub(crate) mod connection;
pub(crate) mod metrics;
pub(crate) mod worker;

// ── Public modules (always compiled) ─────────────────────────────────
pub mod config;
pub mod direct_io;
pub mod error;
pub mod guard;
pub mod nvme;

// ── Buffer modules ───────────────────────────────────────────────────
pub(crate) mod buffer {
    pub mod fixed;
    pub mod send_copy;

    #[cfg(feature = "io_uring")]
    pub mod provided;
    #[cfg(feature = "io_uring")]
    pub mod send_slab;
}

// ── io_uring-only internal modules ───────────────────────────────────
#[cfg(feature = "io_uring")]
pub(crate) mod async_event_loop;
#[cfg(feature = "io_uring")]
pub(crate) mod chain;
#[cfg(feature = "io_uring")]
pub(crate) mod completion;
#[cfg(feature = "io_uring")]
pub(crate) mod driver;
#[cfg(feature = "io_uring")]
pub(crate) mod event_loop;
#[cfg(feature = "io_uring")]
pub mod handler;
#[cfg(feature = "io_uring")]
pub(crate) mod ring;
#[cfg(feature = "io_uring")]
pub(crate) mod runtime;
#[cfg(all(feature = "io_uring", feature = "tls"))]
pub(crate) mod tls;

// ── mio fallback module ──────────────────────────────────────────────
#[cfg(feature = "mio_backend")]
pub(crate) mod mio_backend;

// ══════════════════════════════════════════════════════════════════════
// Re-exports: Callback API
// ══════════════════════════════════════════════════════════════════════

// io_uring backend re-exports
#[cfg(feature = "io_uring")]
pub use handler::ChainPartsBuilder;
#[cfg(feature = "io_uring")]
pub use handler::ConnToken;
#[cfg(feature = "io_uring")]
pub use handler::DriverCtx;
#[cfg(feature = "io_uring")]
pub use handler::EventHandler;
#[cfg(feature = "io_uring")]
pub use handler::SendBuilder;
#[cfg(feature = "io_uring")]
pub use handler::SendChainBuilder;
#[cfg(feature = "io_uring")]
pub use handler::SendPart;
#[cfg(feature = "io_uring")]
pub use handler::UdpToken;

// mio backend re-exports (same names, different implementations)
#[cfg(feature = "mio_backend")]
pub use mio_backend::ctx::ConnToken;
#[cfg(feature = "mio_backend")]
pub use mio_backend::ctx::DriverCtx;
#[cfg(feature = "mio_backend")]
pub use mio_backend::ctx::EventHandler;
#[cfg(feature = "mio_backend")]
pub use mio_backend::ctx::SendBuilder;
#[cfg(feature = "mio_backend")]
pub use mio_backend::ctx::SendPart;
#[cfg(feature = "mio_backend")]
pub use mio_backend::ctx::UdpToken;

// ── Re-exports: Async API (io_uring only) ────────────────────────────
#[cfg(feature = "io_uring")]
pub use error::SpawnError;
#[cfg(feature = "io_uring")]
pub use error::TimerExhausted;
#[cfg(feature = "io_uring")]
pub use runtime::handler::AsyncEventHandler;
#[cfg(feature = "io_uring")]
pub use runtime::io::AsyncSendBuilder;
#[cfg(feature = "io_uring")]
pub use runtime::io::ConnCtx;
#[cfg(feature = "io_uring")]
pub use runtime::io::ConnectFuture;
#[cfg(feature = "io_uring")]
pub use runtime::io::Deadline;
#[cfg(feature = "io_uring")]
pub use runtime::io::Elapsed;
#[cfg(feature = "io_uring")]
pub use runtime::io::RecvReadyFuture;
#[cfg(feature = "io_uring")]
pub use runtime::io::SendFuture;
#[cfg(feature = "io_uring")]
pub use runtime::io::SleepFuture;
#[cfg(feature = "io_uring")]
pub use runtime::io::TimeoutFuture;
#[cfg(feature = "io_uring")]
pub use runtime::io::UdpCtx;
#[cfg(feature = "io_uring")]
pub use runtime::io::UdpRecvFuture;
#[cfg(feature = "io_uring")]
pub use runtime::io::WithBytesFuture;
#[cfg(feature = "io_uring")]
pub use runtime::io::WithDataFuture;
#[cfg(feature = "io_uring")]
pub use runtime::io::connect;
#[cfg(all(feature = "io_uring", feature = "tls"))]
pub use runtime::io::connect_tls;
#[cfg(all(feature = "io_uring", feature = "tls"))]
pub use runtime::io::connect_tls_with_timeout;
#[cfg(feature = "io_uring")]
pub use runtime::io::connect_with_timeout;
#[cfg(feature = "io_uring")]
pub use runtime::io::request_shutdown;
#[cfg(feature = "io_uring")]
pub use runtime::io::sleep;
#[cfg(feature = "io_uring")]
pub use runtime::io::sleep_until;
#[cfg(feature = "io_uring")]
pub use runtime::io::spawn;
#[cfg(feature = "io_uring")]
pub use runtime::io::timeout;
#[cfg(feature = "io_uring")]
pub use runtime::io::timeout_at;
#[cfg(feature = "io_uring")]
pub use runtime::io::try_sleep;
#[cfg(feature = "io_uring")]
pub use runtime::io::try_sleep_until;
#[cfg(feature = "io_uring")]
pub use runtime::io::try_spawn;
#[cfg(feature = "io_uring")]
pub use runtime::io::try_timeout;
#[cfg(feature = "io_uring")]
pub use runtime::io::try_timeout_at;
#[cfg(feature = "io_uring")]
pub use runtime::join::Join;
#[cfg(feature = "io_uring")]
pub use runtime::join::Join3;
#[cfg(feature = "io_uring")]
pub use runtime::join::join;
#[cfg(feature = "io_uring")]
pub use runtime::join::join3;
#[cfg(feature = "io_uring")]
pub use runtime::select::Either;
#[cfg(feature = "io_uring")]
pub use runtime::select::Either3;
#[cfg(feature = "io_uring")]
pub use runtime::select::Select;
#[cfg(feature = "io_uring")]
pub use runtime::select::Select3;
#[cfg(feature = "io_uring")]
pub use runtime::select::select;
#[cfg(feature = "io_uring")]
pub use runtime::select::select3;
#[cfg(feature = "io_uring")]
pub use runtime::task::TaskId;

// ── Re-exports: Shared types ─────────────────────────────────────────

pub use buffer::fixed::MemoryRegion;
pub use buffer::fixed::RegionId;
#[cfg(feature = "io_uring")]
pub use buffer::send_slab::MAX_GUARDS;
#[cfg(feature = "io_uring")]
pub use buffer::send_slab::MAX_IOVECS;
pub use config::Config;
pub use config::RecvBufferConfig;
pub use config::WorkerConfig;
pub use direct_io::DirectIoCompletion;
pub use direct_io::DirectIoConfig;
pub use direct_io::DirectIoFile;
pub use direct_io::DirectIoOp;
pub use error::Error;
pub use guard::{GuardBox, SendGuard};
pub use nvme::NvmeCompletion;
pub use nvme::NvmeConfig;
pub use nvme::NvmeDevice;
pub use worker::KrioBuilder;
pub use worker::ShutdownHandle;
pub use worker::launch;

#[cfg(all(feature = "io_uring", feature = "tls"))]
pub use config::TlsClientConfig;
#[cfg(all(feature = "io_uring", feature = "tls"))]
pub use config::TlsConfig;
#[cfg(all(feature = "io_uring", feature = "tls"))]
pub use tls::TlsInfo;
