//! Observer metrics using sharded counters and histograms.

use metrics::{Counter, CounterGroup};
use metriken::{AtomicHistogram, metric};

// Counter groups
static REQUEST: CounterGroup = CounterGroup::new();
static RESPONSE: CounterGroup = CounterGroup::new();
static BYTES: CounterGroup = CounterGroup::new();
static CONNECTION: CounterGroup = CounterGroup::new();
static TCP: CounterGroup = CounterGroup::new();

/// Counter slot indices for request metrics.
pub mod request {
    pub const TOTAL: usize = 0;
    pub const GET: usize = 1;
    pub const SET: usize = 2;
    pub const DELETE: usize = 3;
    pub const OTHER: usize = 4;
}

/// Counter slot indices for response metrics.
pub mod response {
    pub const TOTAL: usize = 0;
    pub const HIT: usize = 1;
    pub const MISS: usize = 2;
    pub const STORED: usize = 3;
    pub const ERROR: usize = 4;
    pub const OTHER: usize = 5;
}

/// Counter slot indices for byte metrics.
pub mod bytes {
    pub const REQUEST: usize = 0;
    pub const RESPONSE: usize = 1;
}

/// Counter slot indices for connection metrics.
pub mod connection {
    pub const OBSERVED: usize = 0;
}

/// Counter slot indices for TCP metrics.
pub mod tcp {
    pub const GAPS: usize = 0;
    pub const OUT_OF_ORDER: usize = 1;
}

// Request counters
#[metric(
    name = "observer_requests_total",
    description = "Total requests observed"
)]
pub static REQUESTS_TOTAL: Counter = Counter::new(&REQUEST, request::TOTAL);

#[metric(name = "observer_requests_get", description = "GET requests observed")]
pub static REQUESTS_GET: Counter = Counter::new(&REQUEST, request::GET);

#[metric(name = "observer_requests_set", description = "SET requests observed")]
pub static REQUESTS_SET: Counter = Counter::new(&REQUEST, request::SET);

#[metric(
    name = "observer_requests_delete",
    description = "DELETE requests observed"
)]
pub static REQUESTS_DELETE: Counter = Counter::new(&REQUEST, request::DELETE);

#[metric(
    name = "observer_requests_other",
    description = "Other requests observed"
)]
pub static REQUESTS_OTHER: Counter = Counter::new(&REQUEST, request::OTHER);

// Response counters
#[metric(
    name = "observer_responses_total",
    description = "Total responses observed"
)]
pub static RESPONSES_TOTAL: Counter = Counter::new(&RESPONSE, response::TOTAL);

#[metric(name = "observer_responses_hit", description = "Cache hits observed")]
pub static RESPONSES_HIT: Counter = Counter::new(&RESPONSE, response::HIT);

#[metric(
    name = "observer_responses_miss",
    description = "Cache misses observed"
)]
pub static RESPONSES_MISS: Counter = Counter::new(&RESPONSE, response::MISS);

#[metric(
    name = "observer_responses_stored",
    description = "Stored responses observed"
)]
pub static RESPONSES_STORED: Counter = Counter::new(&RESPONSE, response::STORED);

#[metric(
    name = "observer_responses_error",
    description = "Error responses observed"
)]
pub static RESPONSES_ERROR: Counter = Counter::new(&RESPONSE, response::ERROR);

#[metric(
    name = "observer_responses_other",
    description = "Other responses observed"
)]
pub static RESPONSES_OTHER: Counter = Counter::new(&RESPONSE, response::OTHER);

// Byte counters
#[metric(
    name = "observer_bytes_request",
    description = "Request bytes observed"
)]
pub static BYTES_REQUEST: Counter = Counter::new(&BYTES, bytes::REQUEST);

#[metric(
    name = "observer_bytes_response",
    description = "Response bytes observed"
)]
pub static BYTES_RESPONSE: Counter = Counter::new(&BYTES, bytes::RESPONSE);

// Connection counters
#[metric(name = "observer_connections", description = "Connections observed")]
pub static CONNECTIONS_OBSERVED: Counter = Counter::new(&CONNECTION, connection::OBSERVED);

// TCP counters
#[metric(
    name = "observer_tcp_gaps",
    description = "TCP sequence gaps detected (missing data)"
)]
pub static TCP_GAPS: Counter = Counter::new(&TCP, tcp::GAPS);

#[metric(
    name = "observer_tcp_ooo",
    description = "TCP out-of-order packets detected"
)]
pub static TCP_OUT_OF_ORDER: Counter = Counter::new(&TCP, tcp::OUT_OF_ORDER);

// Latency histograms (nanoseconds)
#[metric(
    name = "observer_latency",
    description = "Request-response latency (nanoseconds)"
)]
pub static LATENCY: AtomicHistogram = AtomicHistogram::new(7, 64);

#[metric(
    name = "observer_latency_get",
    description = "GET request latency (nanoseconds)"
)]
pub static LATENCY_GET: AtomicHistogram = AtomicHistogram::new(7, 64);

#[metric(
    name = "observer_latency_set",
    description = "SET request latency (nanoseconds)"
)]
pub static LATENCY_SET: AtomicHistogram = AtomicHistogram::new(7, 64);
