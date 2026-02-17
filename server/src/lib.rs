//! Crucible cache server.
//!
//! A high-performance cache server supporting multiple protocols (RESP, Memcache, Momento)
//! with native io_uring runtime via krio.

pub mod admin;
pub mod affinity;
pub mod async_native;
pub mod banner;
pub mod config;
pub mod connection;
pub mod execute;
pub mod logging;
pub mod metrics;
pub mod native;
pub mod signal;
pub mod tls;
pub mod workers;

pub use config::{CacheBackend, Config, Protocol};
