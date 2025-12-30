//! Cache server metrics.

use metriken::{Counter, Gauge, metric};

// Connection metrics
#[metric(
    name = "connections_accepted",
    description = "Total number of connections accepted"
)]
pub static CONNECTIONS_ACCEPTED: Counter = Counter::new();

#[metric(
    name = "connections_active",
    description = "Number of currently active connections"
)]
pub static CONNECTIONS_ACTIVE: Gauge = Gauge::new();

// Operation counters
#[metric(name = "cache_gets", description = "Total GET operations")]
pub static GETS: Counter = Counter::new();

#[metric(name = "cache_sets", description = "Total SET operations")]
pub static SETS: Counter = Counter::new();

#[metric(name = "cache_deletes", description = "Total DELETE operations")]
pub static DELETES: Counter = Counter::new();

#[metric(name = "cache_flushes", description = "Total FLUSH operations")]
pub static FLUSHES: Counter = Counter::new();

// Cache effectiveness
#[metric(name = "cache_hits", description = "Total cache hits")]
pub static HITS: Counter = Counter::new();

#[metric(name = "cache_misses", description = "Total cache misses")]
pub static MISSES: Counter = Counter::new();

// Errors
#[metric(
    name = "cache_set_errors",
    description = "Total SET errors (cache full)"
)]
pub static SET_ERRORS: Counter = Counter::new();

#[metric(name = "protocol_errors", description = "Total protocol parse errors")]
pub static PROTOCOL_ERRORS: Counter = Counter::new();
