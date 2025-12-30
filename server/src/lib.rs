//! Crucible cache server.
//!
//! A high-performance cache server supporting multiple protocols (RESP, Memcache, Momento)
//! and multiple runtime backends (native io_uring/mio, tokio).

pub mod affinity;
pub mod banner;
pub mod config;
pub mod connection;
pub mod execute;
pub mod metrics;
pub mod native;
pub mod workers;

// Tokio runtime (behind feature flag or always available)
#[cfg(feature = "tokio-runtime")]
pub mod tokio;

pub use config::{CacheBackend, Config, Protocol, Runtime};
