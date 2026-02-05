//! Native runtime server implementation using io-driver (io_uring/mio).

mod server;

pub use server::run;

use io_driver::IoEngine;

/// Get the backend detail string for the banner based on the configured engine.
pub fn backend_detail(engine: IoEngine) -> &'static str {
    match engine {
        IoEngine::Mio => "mio",
        IoEngine::Uring => "io_uring",
        IoEngine::Auto => {
            #[cfg(target_os = "linux")]
            {
                if io_driver::uring_available() {
                    return "io_uring";
                }
            }
            "mio"
        }
    }
}
