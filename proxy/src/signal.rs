//! Signal handling for graceful shutdown.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Install signal handlers and return a shutdown flag.
pub fn install_signal_handler() -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    ctrlc::set_handler(move || {
        shutdown_clone.store(true, Ordering::SeqCst);
    })
    .expect("failed to set signal handler");

    shutdown
}
