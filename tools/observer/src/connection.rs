//! Per-connection state tracking and request/response correlation.
//!
//! This module maintains state for each observed TCP connection and correlates
//! requests with their corresponding responses to calculate latency.

use std::collections::{HashMap, VecDeque};

use ahash::RandomState;

use crate::capture::{ConnectionId, Direction, TcpSegment};
use crate::config::ProtocolHint;
use crate::metrics;
use crate::protocol::{self, CommandType, ParsedRequest, ParsedResponse, Protocol, ResponseType};

/// A pending request awaiting its response.
#[derive(Debug)]
struct PendingRequest {
    timestamp_ns: u64,
    command_type: CommandType,
}

/// State for a single TCP connection.
struct ConnectionState {
    /// Detected or configured protocol.
    protocol: Protocol,
    /// Buffer for incomplete request data.
    request_buffer: Vec<u8>,
    /// Buffer for incomplete response data.
    response_buffer: Vec<u8>,
    /// FIFO queue of pending requests (for RESP and Memcache ASCII).
    pending_fifo: VecDeque<PendingRequest>,
    /// Map of opaque -> pending request (for Memcache binary).
    pending_opaque: HashMap<u32, PendingRequest>,
    /// Next expected sequence number for request direction.
    request_next_seq: Option<u32>,
    /// Next expected sequence number for response direction.
    response_next_seq: Option<u32>,
}

impl ConnectionState {
    fn new(protocol: Protocol) -> Self {
        Self {
            protocol,
            request_buffer: Vec::with_capacity(4096),
            response_buffer: Vec::with_capacity(4096),
            pending_fifo: VecDeque::new(),
            pending_opaque: HashMap::new(),
            request_next_seq: None,
            response_next_seq: None,
        }
    }

    /// Check if this segment is a retransmit and update sequence tracking.
    /// Returns true if this appears to be a retransmit.
    fn check_retransmit(&mut self, direction: Direction, seq: u32, payload_len: usize) -> bool {
        let next_seq = match direction {
            Direction::Request => &mut self.request_next_seq,
            Direction::Response => &mut self.response_next_seq,
        };

        match *next_seq {
            None => {
                // First packet in this direction - initialize tracking
                *next_seq = Some(seq.wrapping_add(payload_len as u32));
                false
            }
            Some(expected) => {
                // Check if seq is before expected (using signed comparison for wraparound)
                let diff = seq.wrapping_sub(expected) as i32;
                if diff < 0 && payload_len > 0 {
                    // Sequence number is before expected - likely a retransmit
                    true
                } else {
                    // Update next expected sequence
                    let end_seq = seq.wrapping_add(payload_len as u32);
                    // Only advance if this packet extends beyond current expectation
                    if (end_seq.wrapping_sub(expected) as i32) > 0 {
                        *next_seq = Some(end_seq);
                    }
                    false
                }
            }
        }
    }
}

/// Tracks all observed connections and their state.
pub struct ConnectionTracker {
    connections: HashMap<ConnectionId, ConnectionState, RandomState>,
    protocol_hint: Protocol,
}

impl ConnectionTracker {
    /// Create a new connection tracker.
    pub fn new(protocol_hint: ProtocolHint) -> Self {
        Self {
            connections: HashMap::default(),
            protocol_hint: Protocol::from_hint(protocol_hint),
        }
    }

    /// Process a captured TCP segment.
    pub fn process_segment(&mut self, segment: TcpSegment) {
        // Handle connection lifecycle
        if segment.syn {
            // New connection
            let protocol = if self.protocol_hint != Protocol::Unknown {
                self.protocol_hint
            } else {
                Protocol::Unknown
            };
            self.connections
                .insert(segment.conn_id, ConnectionState::new(protocol));
            metrics::CONNECTIONS_OBSERVED.increment();
            return;
        }

        if segment.fin || segment.rst {
            // Connection closed - clean up
            self.connections.remove(&segment.conn_id);
            return;
        }

        // Skip empty payloads
        if segment.payload.is_empty() {
            return;
        }

        // Get or create connection state
        if !self.connections.contains_key(&segment.conn_id) {
            metrics::CONNECTIONS_OBSERVED.increment();
            self.connections
                .insert(segment.conn_id, ConnectionState::new(self.protocol_hint));
        }

        let state = self.connections.get_mut(&segment.conn_id).unwrap();

        // Note: Retransmit detection is disabled because it incorrectly flags
        // out-of-order packets as retransmits. Proper detection would require
        // tracking seen sequence ranges, not just the next expected sequence.
        // For now, we accept that some data may be processed twice.
        // TODO: Implement proper out-of-order handling with sequence range tracking

        // Process based on direction
        match segment.direction {
            Direction::Request => {
                Self::process_request(state, &segment);
            }
            Direction::Response => {
                Self::process_response(state, &segment);
            }
        }
    }

    /// Process request data (client to server).
    fn process_request(state: &mut ConnectionState, segment: &TcpSegment) {
        // Append to request buffer
        state.request_buffer.extend_from_slice(&segment.payload);
        metrics::BYTES_REQUEST.add(segment.payload.len() as u64);

        // Try to detect protocol if unknown
        if state.protocol == Protocol::Unknown && !state.request_buffer.is_empty() {
            state.protocol = protocol::detect_protocol(&state.request_buffer);
        }

        // Parse requests from buffer
        loop {
            let parsed = match state.protocol {
                Protocol::Resp => protocol::parse_resp_request(&state.request_buffer),
                Protocol::MemcacheAscii => {
                    protocol::parse_memcache_ascii_request(&state.request_buffer)
                }
                Protocol::MemcacheBinary => {
                    protocol::parse_memcache_binary_request(&state.request_buffer)
                }
                Protocol::Unknown => break,
            };

            match parsed {
                Some(req) => {
                    Self::record_request(state, &req, segment.timestamp_ns);
                    // Remove consumed bytes
                    state.request_buffer.drain(..req.bytes_consumed);
                }
                None => break, // Need more data
            }
        }
    }

    /// Process response data (server to client).
    fn process_response(state: &mut ConnectionState, segment: &TcpSegment) {
        // Append to response buffer
        state.response_buffer.extend_from_slice(&segment.payload);
        metrics::BYTES_RESPONSE.add(segment.payload.len() as u64);

        // Parse responses from buffer
        loop {
            let parsed = match state.protocol {
                Protocol::Resp => protocol::parse_resp_response(&state.response_buffer),
                Protocol::MemcacheAscii => {
                    protocol::parse_memcache_ascii_response(&state.response_buffer)
                }
                Protocol::MemcacheBinary => {
                    protocol::parse_memcache_binary_response(&state.response_buffer)
                }
                Protocol::Unknown => break,
            };

            match parsed {
                Some(resp) => {
                    Self::record_response(state, &resp, segment.timestamp_ns);
                    // Remove consumed bytes
                    state.response_buffer.drain(..resp.bytes_consumed);
                }
                None => break, // Need more data
            }
        }
    }

    /// Record a parsed request and add it to pending queue.
    fn record_request(state: &mut ConnectionState, req: &ParsedRequest, timestamp_ns: u64) {
        // Update request counters
        metrics::REQUESTS_TOTAL.increment();
        match req.command_type {
            CommandType::Get => metrics::REQUESTS_GET.increment(),
            CommandType::Set => metrics::REQUESTS_SET.increment(),
            CommandType::Delete => metrics::REQUESTS_DELETE.increment(),
            CommandType::Other => metrics::REQUESTS_OTHER.increment(),
        }

        // Add to pending requests
        let pending = PendingRequest {
            timestamp_ns,
            command_type: req.command_type,
        };

        if let Some(opaque) = req.opaque {
            // Memcache binary: use opaque for correlation
            state.pending_opaque.insert(opaque, pending);
        } else {
            // RESP / Memcache ASCII: use FIFO ordering
            state.pending_fifo.push_back(pending);
        }
    }

    /// Record a parsed response and correlate with pending request.
    fn record_response(state: &mut ConnectionState, resp: &ParsedResponse, timestamp_ns: u64) {
        // Update response counters
        metrics::RESPONSES_TOTAL.increment();
        match resp.response_type {
            ResponseType::Hit => metrics::RESPONSES_HIT.increment(),
            ResponseType::Miss => metrics::RESPONSES_MISS.increment(),
            ResponseType::Stored => metrics::RESPONSES_STORED.increment(),
            ResponseType::Error => metrics::RESPONSES_ERROR.increment(),
            ResponseType::Other => metrics::RESPONSES_OTHER.increment(),
        }

        // Find matching request for latency calculation
        let pending = if let Some(opaque) = resp.opaque {
            // Memcache binary: look up by opaque
            state.pending_opaque.remove(&opaque)
        } else {
            // RESP / Memcache ASCII: FIFO ordering
            state.pending_fifo.pop_front()
        };

        // Calculate and record latency
        if let Some(req) = pending {
            let latency_ns = timestamp_ns.saturating_sub(req.timestamp_ns);
            let _ = metrics::LATENCY.increment(latency_ns);

            // Record per-command latency
            match req.command_type {
                CommandType::Get => {
                    let _ = metrics::LATENCY_GET.increment(latency_ns);
                }
                CommandType::Set => {
                    let _ = metrics::LATENCY_SET.increment(latency_ns);
                }
                _ => {}
            }
        }
    }

    /// Get the number of active connections being tracked.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }
}
