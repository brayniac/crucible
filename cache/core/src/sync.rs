//! Synchronization primitives with optional loom support.
//!
//! This module provides atomic types that work with both std and loom,
//! enabling concurrency testing with loom while using efficient std
//! atomics in production.

#[cfg(not(feature = "loom"))]
pub use std::sync::atomic::{AtomicU8, AtomicU16, AtomicU32, AtomicU64, Ordering, fence};

#[cfg(feature = "loom")]
pub use loom::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering, fence};

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
