//! Cache benchmark metrics using sharded counters.

pub use metrics::set_thread_shard;
use metrics::{Counter, CounterGroup};
use metriken::{AtomicHistogram, metric};

// Counter groups
static OPS: CounterGroup = CounterGroup::new();
static CACHE: CounterGroup = CounterGroup::new();

/// Counter slot indices for operation metrics.
pub mod ops {
    pub const GET: usize = 0;
    pub const SET: usize = 1;
    pub const DELETE: usize = 2;
    pub const COMPLETED: usize = 3;
    pub const SET_ERRORS: usize = 4;
}

/// Counter slot indices for cache metrics.
pub mod cache {
    pub const HITS: usize = 0;
    pub const MISSES: usize = 1;
}

// Operation counters
#[metric(name = "get_count", description = "Total GET operations")]
pub static GET_COUNT: Counter = Counter::new(&OPS, ops::GET);

#[metric(name = "set_count", description = "Total SET operations")]
pub static SET_COUNT: Counter = Counter::new(&OPS, ops::SET);

#[metric(name = "delete_count", description = "Total DELETE operations")]
pub static DELETE_COUNT: Counter = Counter::new(&OPS, ops::DELETE);

#[metric(name = "completed_count", description = "Total completed operations")]
pub static COMPLETED_COUNT: Counter = Counter::new(&OPS, ops::COMPLETED);

#[metric(name = "set_errors", description = "Total SET errors")]
pub static SET_ERRORS: Counter = Counter::new(&OPS, ops::SET_ERRORS);

// Cache counters
#[metric(name = "cache_hits", description = "Total cache hits")]
pub static CACHE_HITS: Counter = Counter::new(&CACHE, cache::HITS);

#[metric(name = "cache_misses", description = "Total cache misses")]
pub static CACHE_MISSES: Counter = Counter::new(&CACHE, cache::MISSES);

// Latency histograms
#[metric(
    name = "response_latency",
    description = "Response latency histogram (nanoseconds)"
)]
pub static RESPONSE_LATENCY: AtomicHistogram = AtomicHistogram::new(7, 64);

#[metric(
    name = "get_latency",
    description = "GET latency histogram (nanoseconds)"
)]
pub static GET_LATENCY: AtomicHistogram = AtomicHistogram::new(7, 64);

#[metric(
    name = "set_latency",
    description = "SET latency histogram (nanoseconds)"
)]
pub static SET_LATENCY: AtomicHistogram = AtomicHistogram::new(7, 64);

#[metric(
    name = "delete_latency",
    description = "DELETE latency histogram (nanoseconds)"
)]
pub static DELETE_LATENCY: AtomicHistogram = AtomicHistogram::new(7, 64);
