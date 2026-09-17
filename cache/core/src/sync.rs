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

/// Number of randomized schedules each shuttle model explores.
///
/// Overridable via `SHUTTLE_ITERS` for deeper local soaks; the default is
/// sized so the whole shuttle suite stays in CI-friendly territory while
/// still being well past where the mutation red-proofs reproduced.
///
/// Note that `sync.rs` deliberately has **no shuttle arm alongside its loom
/// one**. The loom arm exists because loom cannot instrument `std` atomics, so
/// the production types have to be rebuilt against `loom::sync`. The models in
/// `slice_segment.rs` are standalone mirrors written directly against the model
/// checker's own atomics (the loom ones already are), not instantiations of
/// `SliceSegment`, so shuttle needs nothing here but this knob. Re-exporting
/// `shuttle::sync::atomic` for the whole crate would drag every `static`
/// atomic in `metrics.rs` into shuttle's thread-local state and buy nothing.
#[cfg(all(test, feature = "shuttle", not(feature = "loom")))]
pub(crate) fn shuttle_iters(default: usize) -> usize {
    match std::env::var("SHUTTLE_ITERS") {
        Ok(v) => v
            .parse()
            // A set-but-malformed override must fail loudly: falling back
            // silently would report a "deeper soak" that never ran.
            .unwrap_or_else(|_| panic!("SHUTTLE_ITERS must be a number, got {v:?}")),
        Err(_) => default,
    }
}
