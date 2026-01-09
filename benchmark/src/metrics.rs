//! Benchmark metrics using sharded counters.

pub use metrics::set_thread_shard;
use metrics::{Counter, CounterGroup};
use metriken::{AtomicHistogram, metric};

// Counter groups
static REQUEST: CounterGroup = CounterGroup::new();
static CACHE: CounterGroup = CounterGroup::new();
static CONNECTION: CounterGroup = CounterGroup::new();

/// Counter slot indices for request metrics.
pub mod request {
    pub const SENT: usize = 0;
    pub const RECEIVED: usize = 1;
    pub const ERRORS: usize = 2;
}

/// Counter slot indices for cache metrics.
pub mod cache {
    pub const HITS: usize = 0;
    pub const MISSES: usize = 1;
}

/// Counter slot indices for connection metrics.
pub mod connection {
    pub const ACTIVE: usize = 0;
    pub const FAILED: usize = 1;
    pub const DISCONNECT_EOF: usize = 2;
    pub const DISCONNECT_RECV_ERROR: usize = 3;
    pub const DISCONNECT_SEND_ERROR: usize = 4;
    pub const DISCONNECT_CLOSED_EVENT: usize = 5;
    pub const DISCONNECT_ERROR_EVENT: usize = 6;
    pub const DISCONNECT_CONNECT_FAILED: usize = 7;
}

// Request counters
#[metric(name = "requests_sent", description = "Total requests sent")]
pub static REQUESTS_SENT: Counter = Counter::new(&REQUEST, request::SENT);

#[metric(name = "responses_received", description = "Total responses received")]
pub static RESPONSES_RECEIVED: Counter = Counter::new(&REQUEST, request::RECEIVED);

#[metric(name = "request_errors", description = "Total request errors")]
pub static REQUEST_ERRORS: Counter = Counter::new(&REQUEST, request::ERRORS);

// Cache counters
#[metric(name = "cache_hits", description = "Total cache hits")]
pub static CACHE_HITS: Counter = Counter::new(&CACHE, cache::HITS);

#[metric(name = "cache_misses", description = "Total cache misses")]
pub static CACHE_MISSES: Counter = Counter::new(&CACHE, cache::MISSES);

// Connection counters
#[metric(name = "connections_active", description = "Active connections")]
pub static CONNECTIONS_ACTIVE: Counter = Counter::new(&CONNECTION, connection::ACTIVE);

#[metric(name = "connections_failed", description = "Failed connections")]
pub static CONNECTIONS_FAILED: Counter = Counter::new(&CONNECTION, connection::FAILED);

#[metric(name = "disconnects_eof", description = "Disconnects due to EOF")]
pub static DISCONNECTS_EOF: Counter = Counter::new(&CONNECTION, connection::DISCONNECT_EOF);

#[metric(
    name = "disconnects_recv_error",
    description = "Disconnects due to recv error"
)]
pub static DISCONNECTS_RECV_ERROR: Counter =
    Counter::new(&CONNECTION, connection::DISCONNECT_RECV_ERROR);

#[metric(
    name = "disconnects_send_error",
    description = "Disconnects due to send error"
)]
pub static DISCONNECTS_SEND_ERROR: Counter =
    Counter::new(&CONNECTION, connection::DISCONNECT_SEND_ERROR);

#[metric(
    name = "disconnects_closed_event",
    description = "Disconnects due to closed event"
)]
pub static DISCONNECTS_CLOSED_EVENT: Counter =
    Counter::new(&CONNECTION, connection::DISCONNECT_CLOSED_EVENT);

#[metric(
    name = "disconnects_error_event",
    description = "Disconnects due to error event"
)]
pub static DISCONNECTS_ERROR_EVENT: Counter =
    Counter::new(&CONNECTION, connection::DISCONNECT_ERROR_EVENT);

#[metric(
    name = "disconnects_connect_failed",
    description = "Disconnects due to connect failure"
)]
pub static DISCONNECTS_CONNECT_FAILED: Counter =
    Counter::new(&CONNECTION, connection::DISCONNECT_CONNECT_FAILED);

// Latency histograms (kept as metriken AtomicHistogram)
#[metric(
    name = "response_latency",
    description = "Response latency histogram (nanoseconds)"
)]
pub static RESPONSE_LATENCY: AtomicHistogram = AtomicHistogram::new(7, 64);

#[metric(
    name = "get_latency",
    description = "GET response latency histogram (nanoseconds)"
)]
pub static GET_LATENCY: AtomicHistogram = AtomicHistogram::new(7, 64);

#[metric(
    name = "set_latency",
    description = "SET response latency histogram (nanoseconds)"
)]
pub static SET_LATENCY: AtomicHistogram = AtomicHistogram::new(7, 64);
