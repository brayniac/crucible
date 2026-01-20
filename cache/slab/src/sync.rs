//! Synchronization primitives with optional loom support.
//!
//! This module provides atomic types that work with both std and loom,
//! enabling concurrency testing with loom while using efficient std
//! atomics in production.
//!
//! Note: The struct layout (SlabItemHeader) always uses std atomics to ensure
//! consistent size. Loom atomics are only used in loom tests for model checking.

#[cfg(feature = "loom")]
pub use loom::sync::atomic::{AtomicU32, Ordering};

/// Spin loop hint for busy waiting.
///
/// In production (non-loom), this uses `std::hint::spin_loop()` which
/// provides a hint to the CPU that we're in a spin-wait loop.
///
/// Under loom, this yields to allow other threads to make progress,
/// which is necessary for loom's model checking to work correctly.
#[inline]
pub fn spin_loop() {
    #[cfg(not(feature = "loom"))]
    std::hint::spin_loop();

    #[cfg(feature = "loom")]
    loom::thread::yield_now();
}
