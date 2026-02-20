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
pub(crate) mod counter;
pub(crate) mod connection;
pub mod direct_io;
pub(crate) mod driver;
pub(crate) mod event_loop;
pub(crate) mod metrics;
pub mod nvme;
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
/// Pre-classified part for [`AsyncSendBuilder::submit_batch`].
pub use handler::SendPart;
/// Opaque handle for a UDP socket.
pub use handler::UdpToken;

// ── Re-exports: Async API ───────────────────────────────────────────────

/// Error returned by [`try_spawn()`] when the standalone task slab is full.
pub use error::SpawnError;
/// Error returned by [`try_sleep()`] and [`try_timeout()`] when the timer pool is full.
pub use error::TimerExhausted;
/// Trait for async event handlers (one task per connection).
pub use runtime::handler::AsyncEventHandler;
/// Async scatter-gather send builder.
pub use runtime::io::AsyncSendBuilder;
/// Async connection context with send/recv futures.
pub use runtime::io::ConnCtx;
/// Future that completes when a connect finishes.
pub use runtime::io::ConnectFuture;
/// A monotonic clock deadline for absolute timers.
pub use runtime::io::Deadline;
/// Future that awaits a disk I/O completion (NVMe or Direct I/O).
pub use runtime::io::DiskIoFuture;
/// Error returned when a [`timeout()`] expires.
pub use runtime::io::Elapsed;
/// Future that resolves when recv data is available (sink, accumulator, or close).
pub use runtime::io::RecvReadyFuture;
/// Future that completes when a send finishes.
pub use runtime::io::SendFuture;
/// Future returned by [`sleep()`].
pub use runtime::io::SleepFuture;
/// Future returned by [`timeout()`].
pub use runtime::io::TimeoutFuture;
/// Async context for a UDP socket.
pub use runtime::io::UdpCtx;
/// Future returned by [`UdpCtx::recv_from()`].
pub use runtime::io::UdpRecvFuture;
/// Future that provides received data as zero-copy `Bytes`.
pub use runtime::io::WithBytesFuture;
/// Future that provides received data.
pub use runtime::io::WithDataFuture;
/// Initiate an outbound TCP connection from any async task.
pub use runtime::io::connect;
/// Initiate an outbound TLS connection from any async task.
#[cfg(feature = "tls")]
pub use runtime::io::connect_tls;
/// Initiate an outbound TLS connection with a timeout from any async task.
#[cfg(feature = "tls")]
pub use runtime::io::connect_tls_with_timeout;
/// Initiate an outbound TCP connection with a timeout from any async task.
pub use runtime::io::connect_with_timeout;
/// Submit a Direct I/O read and return a future for the result.
pub use runtime::io::direct_io_read;
/// Submit an NVMe read and return a future for the result.
pub use runtime::io::nvme_read;
/// Open a Direct I/O file from any async task.
pub use runtime::io::open_direct_io_file;
/// Open an NVMe device from any async task.
pub use runtime::io::open_nvme_device;
/// Request graceful shutdown from any async task.
pub use runtime::io::request_shutdown;
/// Create a future that completes after a duration.
pub use runtime::io::sleep;
/// Create a future that completes at an absolute deadline.
pub use runtime::io::sleep_until;
/// Spawn a standalone async task on the current worker.
pub use runtime::io::spawn;
/// Wrap a future with a deadline.
pub use runtime::io::timeout;
/// Wrap a future with an absolute deadline.
pub use runtime::io::timeout_at;
/// Fallible sleep that returns an error if the timer pool is exhausted.
pub use runtime::io::try_sleep;
/// Fallible sleep_until that returns an error if the timer pool is exhausted.
pub use runtime::io::try_sleep_until;
/// Spawn a standalone task, returning an error if the slab is full.
pub use runtime::io::try_spawn;
/// Fallible timeout that returns an error if the timer pool is exhausted.
pub use runtime::io::try_timeout;
/// Fallible timeout_at that returns an error if the timer pool is exhausted.
pub use runtime::io::try_timeout_at;
/// Future returned by [`join()`].
pub use runtime::join::Join;
/// Future returned by [`join3()`].
pub use runtime::join::Join3;
/// Poll two futures concurrently, returning both outputs when complete.
pub use runtime::join::join;
/// Poll three futures concurrently, returning all outputs when complete.
pub use runtime::join::join3;
/// Result of [`select()`] — which branch completed.
pub use runtime::select::Either;
/// Result of [`select3()`] — which branch completed.
pub use runtime::select::Either3;
/// Future returned by [`select()`].
pub use runtime::select::Select;
/// Future returned by [`select3()`].
pub use runtime::select::Select3;
/// Poll two futures concurrently, returning whichever completes first.
pub use runtime::select::select;
/// Poll three futures concurrently, returning whichever completes first.
pub use runtime::select::select3;
/// Opaque handle for a standalone spawned task.
pub use runtime::task::TaskId;

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
/// Direct I/O completion result.
pub use direct_io::DirectIoCompletion;
/// Direct I/O configuration.
pub use direct_io::DirectIoConfig;
/// Direct I/O file handle.
pub use direct_io::DirectIoFile;
/// Direct I/O operation type.
pub use direct_io::DirectIoOp;
/// Runtime errors.
pub use error::Error;
/// Zero-copy send guard trait.
pub use guard::{GuardBox, SendGuard};
/// NVMe passthrough completion result.
pub use nvme::NvmeCompletion;
/// NVMe passthrough configuration.
pub use nvme::NvmeConfig;
/// NVMe passthrough device handle.
pub use nvme::NvmeDevice;
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
