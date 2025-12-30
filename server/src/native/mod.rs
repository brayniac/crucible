//! Native runtime server implementation using io-driver (io_uring/mio).

mod server;

pub use server::run;

/// Get the backend detail string for the banner.
pub fn backend_detail() -> &'static str {
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        if io_driver::uring_available() {
            return "io_uring";
        }
    }
    "mio"
}
