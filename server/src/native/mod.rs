//! Native runtime server implementation using io-driver (io_uring/mio).

mod server;

pub use server::run;

/// Get the backend detail string for the banner.
pub fn backend_detail() -> &'static str {
    // io-driver has io_uring enabled by default and handles runtime detection
    #[cfg(target_os = "linux")]
    {
        if io_driver::uring_available() {
            return "io_uring";
        }
    }
    "mio"
}
