//! io-driver - Unified I/O framework with io_uring and mio backends.
//!
//! This crate provides a high-performance, cross-platform I/O abstraction
//! that uses io_uring on Linux and falls back to mio (epoll/kqueue) elsewhere.
//!
//! # Features
//!
//! - **Cross-platform**: Works on Linux, macOS, and other Unix systems
//! - **Automatic optimization**: Uses io_uring's advanced features when available
//! - **Unified API**: Same code works regardless of backend
//! - **Zero-copy**: Leverages kernel features for minimal copying
//! - **Completion-based**: Simple poll/drain event loop model
//!
//! # Quick Start
//!
//! ```ignore
//! use io_driver::{Driver, CompletionKind};
//! use std::time::Duration;
//!
//! // Create a driver with default settings (auto-selects best backend)
//! let mut driver = Driver::new()?;
//!
//! // Server: listen for connections
//! let listener_id = driver.listen("0.0.0.0:8080".parse()?, 128)?;
//!
//! loop {
//!     // Wait for events
//!     driver.poll(Some(Duration::from_millis(100)))?;
//!
//!     // Process completions
//!     for completion in driver.drain_completions() {
//!         match completion.kind {
//!             CompletionKind::Accept { conn_id, addr, .. } => {
//!                 println!("New connection from {}", addr);
//!             }
//!             CompletionKind::Recv { conn_id } => {
//!                 let mut buf = [0u8; 4096];
//!                 if let Ok(n) = driver.recv(conn_id, &mut buf) {
//!                     // Echo the data back
//!                     driver.send(conn_id, &buf[..n])?;
//!                 }
//!             }
//!             CompletionKind::Closed { conn_id } => {
//!                 driver.close(conn_id)?;
//!             }
//!             _ => {}
//!         }
//!     }
//! }
//! ```
//!
//! # Backend Selection
//!
//! By default, io-driver automatically selects the best available backend:
//!
//! - **Linux 6.0+**: Uses io_uring with multishot recv/accept, SendZc, and ring-provided buffers
//! - **Other platforms**: Uses mio (epoll on Linux, kqueue on macOS)
//!
//! You can force a specific backend using the builder:
//!
//! ```ignore
//! use io_driver::{Driver, IoEngine};
//!
//! // Force mio backend
//! let driver = Driver::builder()
//!     .engine(IoEngine::Mio)
//!     .build()?;
//! ```

mod builder;
mod driver;
mod types;

pub mod mio;
pub mod transport;

#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub mod uring;

// Re-exports
pub use builder::DriverBuilder;
pub use driver::IoDriver;
pub use types::{Completion, CompletionKind, ConnId, IoEngine, ListenerId};

// Transport re-exports
pub use transport::{PlainTransport, ProcessResult, Transport, TransportState};

#[cfg(feature = "tls")]
pub use transport::{TlsConfig, TlsTransport};

use std::io;

/// Convenience wrapper for creating drivers.
pub struct Driver;

impl Driver {
    /// Create a new driver with default settings.
    ///
    /// Automatically selects the best available backend.
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> io::Result<Box<dyn IoDriver>> {
        DriverBuilder::new().build()
    }

    /// Create a builder for configuring the driver.
    pub fn builder() -> DriverBuilder {
        DriverBuilder::new()
    }
}

/// Check if io_uring is available on this system.
///
/// Returns `true` if the system supports io_uring with the required features.
pub fn uring_available() -> bool {
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        uring::is_supported()
    }
    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_driver_new() {
        let result = Driver::new();
        assert!(result.is_ok());
    }

    #[test]
    fn test_driver_builder() {
        let builder = Driver::builder();
        // Builder should have default values
        let result = builder.build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_uring_available() {
        // On non-Linux or without io_uring feature, should return false
        #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
        {
            assert!(!uring_available());
        }
        // On Linux with io_uring feature, result depends on kernel
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        {
            // Just call it to verify it doesn't panic
            let _ = uring_available();
        }
    }
}
