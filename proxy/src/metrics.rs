//! Proxy metrics.

use metriken::{Counter, Gauge, metric};

/// Total client connections.
#[metric(name = "proxy_client_connections")]
pub static CLIENT_CONNECTIONS: Gauge = Gauge::new();

/// Total commands received from clients.
#[metric(name = "proxy_client_commands")]
pub static CLIENT_COMMANDS: Counter = Counter::new();

/// Total requests sent to backend.
#[metric(name = "proxy_backend_requests")]
pub static BACKEND_REQUESTS: Counter = Counter::new();

/// Total responses received from backend.
#[metric(name = "proxy_backend_responses")]
pub static BACKEND_RESPONSES: Counter = Counter::new();

/// Parse errors.
#[metric(name = "proxy_parse_errors")]
pub static PARSE_ERRORS: Counter = Counter::new();

/// Cache hits (when caching is enabled).
#[metric(name = "proxy_cache_hits")]
pub static CACHE_HITS: Counter = Counter::new();

/// Cache misses (when caching is enabled).
#[metric(name = "proxy_cache_misses")]
pub static CACHE_MISSES: Counter = Counter::new();
