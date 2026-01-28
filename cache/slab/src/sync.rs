//! Synchronization primitives with optional loom support.
//!
//! This module provides atomic types that work with both std and loom,
//! enabling concurrency testing with loom while using efficient std
//! atomics in production.

#[cfg(not(feature = "loom"))]
pub use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};

#[cfg(feature = "loom")]
pub use loom::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};

/// Spin loop hint - yields to other threads in loom.
#[allow(dead_code)]
#[cfg(not(feature = "loom"))]
#[inline]
pub fn spin_loop() {
    std::hint::spin_loop();
}

#[allow(dead_code)]
#[cfg(feature = "loom")]
#[inline]
pub fn spin_loop() {
    loom::thread::yield_now();
}
