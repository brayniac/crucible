//! Synchronization primitives with optional loom support.
//!
//! This module provides atomic types that work with both std and loom,
//! enabling concurrency testing with loom while using efficient std
//! atomics in production.

#[cfg(not(feature = "loom"))]
pub use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};

#[cfg(feature = "loom")]
pub use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};

#[cfg(not(feature = "loom"))]
pub use std::sync::Arc;

#[cfg(feature = "loom")]
pub use loom::sync::Arc;

/// Spin loop hint for busy waiting.
#[inline]
pub fn spin_loop() {
    #[cfg(not(feature = "loom"))]
    std::hint::spin_loop();

    #[cfg(feature = "loom")]
    loom::thread::yield_now();
}

/// Yield the current thread.
#[inline]
pub fn yield_now() {
    #[cfg(not(feature = "loom"))]
    std::thread::yield_now();

    #[cfg(feature = "loom")]
    loom::thread::yield_now();
}
