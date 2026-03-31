//! Signal handling for graceful shutdown.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Install signal handlers and return a shutdown flag.
///
/// Spawns a dedicated thread that blocks on `ringline::signal::wait()`
/// and sets the shutdown flag when SIGINT or SIGTERM is received.
pub fn install_signal_handler() -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    std::thread::Builder::new()
        .name("signal".to_string())
        .spawn(move || {
            let signal = ringline::signal::wait();
            tracing::info!(%signal, "Received signal, initiating shutdown...");
            shutdown_clone.store(true, Ordering::SeqCst);
        })
        .expect("failed to spawn signal thread");

    shutdown
}
