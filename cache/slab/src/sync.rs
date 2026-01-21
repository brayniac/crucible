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
