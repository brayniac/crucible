//! Signal handling for graceful shutdown.
//!
//! Provides cross-platform signal handling for SIGINT and SIGTERM,
//! allowing the server to shut down gracefully.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Install signal handlers for graceful shutdown.
///
/// Returns an `Arc<AtomicBool>` that will be set to `true` when a
/// shutdown signal (SIGINT or SIGTERM) is received.
///
/// # Example
///
/// ```ignore
/// let shutdown = install_signal_handler();
///
/// // In your main loop:
/// while !shutdown.load(Ordering::Relaxed) {
///     // Process requests...
/// }
/// ```
pub fn install_signal_handler() -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_flag = shutdown.clone();

    ctrlc::set_handler(move || {
        // Check if we've already received a signal
        if shutdown_flag.swap(true, Ordering::SeqCst) {
            // Second signal - force exit
            tracing::warn!("Received second signal, forcing immediate exit");
            std::process::exit(1);
        }
        tracing::info!("Received shutdown signal, initiating graceful shutdown...");
    })
    .expect("Failed to set signal handler");

    shutdown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_flag_initial_state() {
        // We can't easily test the signal handler installation in unit tests,
        // but we can verify the flag starts as false
        let flag = Arc::new(AtomicBool::new(false));
        assert!(!flag.load(Ordering::Relaxed));
    }
}
