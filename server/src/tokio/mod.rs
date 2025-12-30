//! Tokio runtime server implementation.
//!
//! This module provides an async/work-stealing server implementation
//! using the Tokio runtime.

mod server;

pub use server::run;

/// Get the backend detail string for the banner.
pub fn backend_detail() -> &'static str {
    "tokio"
}
