//! Cache server metrics.

use metrics::{Counter, CounterGroup};
use metriken::{Gauge, metric};
use std::sync::atomic::{AtomicU64, Ordering};

// Counter groups (sharded storage)
static CONNECTION: CounterGroup = CounterGroup::new();
static REQUEST: CounterGroup = CounterGroup::new();
static CACHE: CounterGroup = CounterGroup::new();
static ERROR: CounterGroup = CounterGroup::new();

/// Counter slot indices for connection metrics.
pub mod connection {
    pub const ACCEPTED: usize = 0;
}

/// Counter slot indices for request metrics.
pub mod request {
    pub const GETS: usize = 0;
    pub const SETS: usize = 1;
    pub const DELETES: usize = 2;
    pub const FLUSHES: usize = 3;
}

/// Counter slot indices for cache metrics.
pub mod cache {
    pub const HITS: usize = 0;
    pub const MISSES: usize = 1;
}

/// Counter slot indices for error metrics.
pub mod error {
    pub const SET_ERRORS: usize = 0;
    pub const PROTOCOL_ERRORS: usize = 1;
}

// Connection metrics
#[metric(
    name = "connections_accepted",
    description = "Total number of connections accepted"
)]
pub static CONNECTIONS_ACCEPTED: Counter = Counter::new(&CONNECTION, connection::ACCEPTED);

#[metric(
    name = "connections_active",
    description = "Number of currently active connections"
)]
pub static CONNECTIONS_ACTIVE: Gauge = Gauge::new();

// Operation counters
#[metric(name = "cache_gets", description = "Total GET operations")]
pub static GETS: Counter = Counter::new(&REQUEST, request::GETS);

#[metric(name = "cache_sets", description = "Total SET operations")]
pub static SETS: Counter = Counter::new(&REQUEST, request::SETS);

#[metric(name = "cache_deletes", description = "Total DELETE operations")]
pub static DELETES: Counter = Counter::new(&REQUEST, request::DELETES);

#[metric(name = "cache_flushes", description = "Total FLUSH operations")]
pub static FLUSHES: Counter = Counter::new(&REQUEST, request::FLUSHES);

// Cache effectiveness
#[metric(name = "cache_hits", description = "Total cache hits")]
pub static HITS: Counter = Counter::new(&CACHE, cache::HITS);

#[metric(name = "cache_misses", description = "Total cache misses")]
pub static MISSES: Counter = Counter::new(&CACHE, cache::MISSES);

// Errors
#[metric(
    name = "cache_set_errors",
    description = "Total SET errors (cache full)"
)]
pub static SET_ERRORS: Counter = Counter::new(&ERROR, error::SET_ERRORS);

#[metric(name = "protocol_errors", description = "Total protocol parse errors")]
pub static PROTOCOL_ERRORS: Counter = Counter::new(&ERROR, error::PROTOCOL_ERRORS);

/// Reason for closing a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// Client closed connection (recv returned 0)
    ClientEof,
    /// Protocol requested close (e.g., Connection: close)
    ProtocolClose,
    /// Error during recv
    RecvError,
    /// Error during send
    SendError,
    /// Closed completion event from driver
    ClosedEvent,
    /// Error completion event from driver
    ErrorEvent,
}

/// Per-worker statistics for diagnosing performance issues.
#[derive(Default)]
pub struct WorkerStats {
    pub poll_count: AtomicU64,
    pub empty_polls: AtomicU64,
    pub completions: AtomicU64,
    pub accepts: AtomicU64,
    pub channel_receives: AtomicU64,
    pub recv_events: AtomicU64,
    pub send_ready_events: AtomicU64,
    pub close_events: AtomicU64,
    pub bytes_received: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub active_connections: AtomicU64,
    pub backpressure_events: AtomicU64,
    // Close reason breakdown
    pub closes_client_eof: AtomicU64,
    pub closes_protocol: AtomicU64,
    pub closes_recv_error: AtomicU64,
    pub closes_send_error: AtomicU64,
    pub closes_closed_event: AtomicU64,
    pub closes_error_event: AtomicU64,
}

impl WorkerStats {
    pub const fn new() -> Self {
        Self {
            poll_count: AtomicU64::new(0),
            empty_polls: AtomicU64::new(0),
            completions: AtomicU64::new(0),
            accepts: AtomicU64::new(0),
            channel_receives: AtomicU64::new(0),
            recv_events: AtomicU64::new(0),
            send_ready_events: AtomicU64::new(0),
            close_events: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            backpressure_events: AtomicU64::new(0),
            closes_client_eof: AtomicU64::new(0),
            closes_protocol: AtomicU64::new(0),
            closes_recv_error: AtomicU64::new(0),
            closes_send_error: AtomicU64::new(0),
            closes_closed_event: AtomicU64::new(0),
            closes_error_event: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn inc_poll(&self) {
        self.poll_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_empty_poll(&self) {
        self.empty_polls.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_completions(&self, n: u64) {
        self.completions.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_accepts(&self) {
        self.accepts.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_channel_receive(&self) {
        self.channel_receives.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_recv(&self) {
        self.recv_events.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_send_ready(&self) {
        self.send_ready_events.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_close(&self, reason: CloseReason) {
        self.close_events.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);

        match reason {
            CloseReason::ClientEof => {
                self.closes_client_eof.fetch_add(1, Ordering::Relaxed);
            }
            CloseReason::ProtocolClose => {
                self.closes_protocol.fetch_add(1, Ordering::Relaxed);
            }
            CloseReason::RecvError => {
                self.closes_recv_error.fetch_add(1, Ordering::Relaxed);
            }
            CloseReason::SendError => {
                self.closes_send_error.fetch_add(1, Ordering::Relaxed);
            }
            CloseReason::ClosedEvent => {
                self.closes_closed_event.fetch_add(1, Ordering::Relaxed);
            }
            CloseReason::ErrorEvent => {
                self.closes_error_event.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[inline]
    pub fn add_bytes_received(&self, n: u64) {
        self.bytes_received.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_bytes_sent(&self, n: u64) {
        self.bytes_sent.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_backpressure(&self) {
        self.backpressure_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a snapshot of current stats.
    pub fn snapshot(&self) -> WorkerStatsSnapshot {
        WorkerStatsSnapshot {
            poll_count: self.poll_count.load(Ordering::Relaxed),
            empty_polls: self.empty_polls.load(Ordering::Relaxed),
            completions: self.completions.load(Ordering::Relaxed),
            accepts: self.accepts.load(Ordering::Relaxed),
            channel_receives: self.channel_receives.load(Ordering::Relaxed),
            recv_events: self.recv_events.load(Ordering::Relaxed),
            send_ready_events: self.send_ready_events.load(Ordering::Relaxed),
            close_events: self.close_events.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            backpressure_events: self.backpressure_events.load(Ordering::Relaxed),
            closes_client_eof: self.closes_client_eof.load(Ordering::Relaxed),
            closes_protocol: self.closes_protocol.load(Ordering::Relaxed),
            closes_recv_error: self.closes_recv_error.load(Ordering::Relaxed),
            closes_send_error: self.closes_send_error.load(Ordering::Relaxed),
            closes_closed_event: self.closes_closed_event.load(Ordering::Relaxed),
            closes_error_event: self.closes_error_event.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of worker stats.
#[derive(Debug, Clone, Default)]
pub struct WorkerStatsSnapshot {
    pub poll_count: u64,
    pub empty_polls: u64,
    pub completions: u64,
    pub accepts: u64,
    pub channel_receives: u64,
    pub recv_events: u64,
    pub send_ready_events: u64,
    pub close_events: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub active_connections: u64,
    pub backpressure_events: u64,
    // Close reason breakdown
    pub closes_client_eof: u64,
    pub closes_protocol: u64,
    pub closes_recv_error: u64,
    pub closes_send_error: u64,
    pub closes_closed_event: u64,
    pub closes_error_event: u64,
}

impl WorkerStatsSnapshot {
    /// Calculate the delta between two snapshots.
    pub fn delta(&self, prev: &Self) -> Self {
        Self {
            poll_count: self.poll_count.saturating_sub(prev.poll_count),
            empty_polls: self.empty_polls.saturating_sub(prev.empty_polls),
            completions: self.completions.saturating_sub(prev.completions),
            accepts: self.accepts.saturating_sub(prev.accepts),
            channel_receives: self.channel_receives.saturating_sub(prev.channel_receives),
            recv_events: self.recv_events.saturating_sub(prev.recv_events),
            send_ready_events: self
                .send_ready_events
                .saturating_sub(prev.send_ready_events),
            close_events: self.close_events.saturating_sub(prev.close_events),
            bytes_received: self.bytes_received.saturating_sub(prev.bytes_received),
            bytes_sent: self.bytes_sent.saturating_sub(prev.bytes_sent),
            active_connections: self.active_connections,
            backpressure_events: self
                .backpressure_events
                .saturating_sub(prev.backpressure_events),
            closes_client_eof: self
                .closes_client_eof
                .saturating_sub(prev.closes_client_eof),
            closes_protocol: self.closes_protocol.saturating_sub(prev.closes_protocol),
            closes_recv_error: self
                .closes_recv_error
                .saturating_sub(prev.closes_recv_error),
            closes_send_error: self
                .closes_send_error
                .saturating_sub(prev.closes_send_error),
            closes_closed_event: self
                .closes_closed_event
                .saturating_sub(prev.closes_closed_event),
            closes_error_event: self
                .closes_error_event
                .saturating_sub(prev.closes_error_event),
        }
    }
}
