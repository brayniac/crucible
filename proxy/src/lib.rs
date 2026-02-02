//! Crucible Proxy - High-performance Valkey/Redis proxy.
//!
//! Uses io_uring for both client and backend connections with thread-per-core
//! architecture for maximum performance.

pub mod backend;
pub mod cache;
pub mod client;
pub mod config;
pub mod metrics;
pub mod worker;

pub mod logging;
pub mod signal;

pub use config::Config;
pub use worker::run;
