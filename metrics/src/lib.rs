//! High-performance sharded metrics with metriken integration.
//!
//! This crate provides [`Counter`] and [`CounterGroup`] for high-throughput
//! metrics that avoid cache-line contention by using per-thread shards.
//!
//! # Overview
//!
//! A [`CounterGroup`] provides sharded storage for up to 16 counters.
//! Each thread writes to its own shard (selected by thread ID), so
//! counters in the same group don't cause false sharing.
//!
//! A [`Counter`] references a slot in a group and implements [`metriken::Metric`],
//! allowing it to be registered with the `#[metric]` attribute for Prometheus
//! exposition.
//!
//! # Example
//!
//! ```
//! use metrics::{Counter, CounterGroup};
//!
//! // Define named slot constants for clarity
//! mod slots {
//!     pub const GETS: usize = 0;
//!     pub const SETS: usize = 1;
//!     pub const HITS: usize = 2;
//!     pub const MISSES: usize = 3;
//! }
//!
//! // Create counter groups (just storage)
//! static REQUEST: CounterGroup = CounterGroup::new();
//! static CACHE: CounterGroup = CounterGroup::new();
//!
//! // Create counters referencing group slots
//! static REQUEST_GETS: Counter = Counter::new(&REQUEST, slots::GETS);
//! static REQUEST_SETS: Counter = Counter::new(&REQUEST, slots::SETS);
//! static CACHE_HITS: Counter = Counter::new(&CACHE, slots::HITS);
//! static CACHE_MISSES: Counter = Counter::new(&CACHE, slots::MISSES);
//!
//! // Use in hot path
//! REQUEST_GETS.increment();
//! CACHE_HITS.add(1);
//! ```
//!
//! # Memory Layout
//!
//! Each `CounterGroup` uses 8KB (64 shards × 128 bytes per shard). Each shard
//! holds 16 counter slots. This means:
//!
//! - One counter effectively uses ~512 bytes (8KB / 16)
//! - 5 groups with 16 counters each = 40KB total
//!
//! Compare to a naive sharded counter that uses 8KB per counter.

mod counter;

pub use counter::{Counter, CounterGroup};

// Re-export metriken for convenience
pub use metriken;
