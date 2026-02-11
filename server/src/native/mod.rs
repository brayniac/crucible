//! Native runtime server implementation using kompio (io_uring).

mod handler;
mod server;

pub use server::run;

/// Get the backend detail string for the banner.
pub fn backend_detail() -> &'static str {
    "io_uring"
}
