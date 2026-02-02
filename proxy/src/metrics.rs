//! Proxy metrics using sharded counters to avoid contention.

use metrics::{Counter, CounterGroup};
use metriken::{Gauge, metric};

// Counter groups (sharded storage) - each group can hold up to 16 counters
static CLIENT: CounterGroup = CounterGroup::new();
static BACKEND: CounterGroup = CounterGroup::new();
static CACHE: CounterGroup = CounterGroup::new();

/// Counter slot indices for client metrics.
mod client {
    pub const COMMANDS: usize = 0;
    pub const PARSE_ERRORS: usize = 1;
}

/// Counter slot indices for backend metrics.
mod backend {
    pub const REQUESTS: usize = 0;
    pub const RESPONSES: usize = 1;
}

/// Counter slot indices for cache metrics.
mod cache {
    pub const HITS: usize = 0;
    pub const MISSES: usize = 1;
}

/// Total client connections (gauge, not high-frequency).
#[metric(name = "proxy_client_connections")]
pub static CLIENT_CONNECTIONS: Gauge = Gauge::new();

/// Total commands received from clients.
#[metric(name = "proxy_client_commands")]
pub static CLIENT_COMMANDS: Counter = Counter::new(&CLIENT, client::COMMANDS);

/// Parse errors.
#[metric(name = "proxy_parse_errors")]
pub static PARSE_ERRORS: Counter = Counter::new(&CLIENT, client::PARSE_ERRORS);

/// Total requests sent to backend.
#[metric(name = "proxy_backend_requests")]
pub static BACKEND_REQUESTS: Counter = Counter::new(&BACKEND, backend::REQUESTS);

/// Total responses received from backend.
#[metric(name = "proxy_backend_responses")]
pub static BACKEND_RESPONSES: Counter = Counter::new(&BACKEND, backend::RESPONSES);

/// Cache hits (when caching is enabled).
#[metric(name = "proxy_cache_hits")]
pub static CACHE_HITS: Counter = Counter::new(&CACHE, cache::HITS);

/// Cache misses (when caching is enabled).
#[metric(name = "proxy_cache_misses")]
pub static CACHE_MISSES: Counter = Counter::new(&CACHE, cache::MISSES);
