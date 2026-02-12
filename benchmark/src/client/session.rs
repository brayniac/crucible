//! Protocol session abstraction for working with IoDriver.
//!
//! A session handles protocol encoding/decoding and request tracking,
//! while the IoDriver handles the actual I/O operations.

use crate::buffer::BufferPair;

/// Format bytes as hex for debug output, showing first and last N bytes.
fn format_hex_preview(data: &[u8], max_each: usize) -> String {
    if data.is_empty() {
        return "<empty>".to_string();
    }

    let len = data.len();
    if len <= max_each * 2 {
        // Short enough to show everything
        let hex: Vec<String> = data.iter().map(|b| format!("{:02x}", b)).collect();
        format!("[{}] ({}B)", hex.join(" "), len)
    } else {
        // Show first N and last N bytes
        let first: Vec<String> = data[..max_each]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        let last: Vec<String> = data[len - max_each..]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        format!(
            "[{} ... {}] ({}B, showing first/last {})",
            first.join(" "),
            last.join(" "),
            len,
            max_each
        )
    }
}
/// Receive buffer trait for zero-copy response parsing.
pub trait RecvBuf {
    fn as_slice(&self) -> &[u8];
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn consume(&mut self, n: usize);
    fn capacity(&self) -> usize;
    fn shrink_if_oversized(&mut self);
}

use crate::config::{Config, Protocol};
use crate::protocol::{
    MemcacheBinaryCodec, MemcacheBinaryError, MemcacheCodec, MemcacheError, PingCodec, PingError,
    RespCodec, RespError,
};

// Import underlying protocol parsers for zero-copy path
use protocol_memcache::Response as MemcacheResponseParser;
use protocol_memcache::binary::ParsedBinaryResponse as MemcacheBinaryResponseParser;
use protocol_ping::Response as PingResponseParser;
use protocol_resp::{ParseOptions as RespParseOptions, Value as RespValueParser};

/// Maximum bulk string size for benchmark: 512MB (matches official RESP protocol spec).
const MAX_BULK_STRING_LEN: usize = 512 * 1024 * 1024;

/// Parse options for RESP protocol in zero-copy path.
fn resp_parse_options() -> RespParseOptions {
    RespParseOptions::new().max_bulk_string_len(MAX_BULK_STRING_LEN)
}

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use super::{ConnectionState, RequestResult, RequestType};

/// Timestamp for latency calculation.
#[derive(Debug, Clone, Copy)]
pub struct Timestamp {
    pub instant: Instant,
    pub kernel_ns: Option<u64>,
}

impl Timestamp {
    pub fn now() -> Self {
        Self {
            instant: Instant::now(),
            kernel_ns: None,
        }
    }

    pub fn with_kernel_ns(kernel_ns: u64) -> Self {
        Self {
            instant: Instant::now(),
            kernel_ns: Some(kernel_ns),
        }
    }
}

/// A request in the pipeline waiting for a response.
#[derive(Debug)]
struct InFlightRequest {
    id: u64,
    request_type: RequestType,
    queued_at: Instant,
    /// When the request bytes were actually sent to the kernel.
    sent_at: Option<Instant>,
    /// When the first bytes of the response were received (readability notification).
    first_byte_at: Option<Instant>,
    /// Cumulative byte offset where this request's data ends in the send stream.
    bytes_end: u64,
    tx_timestamp: Option<Timestamp>,
}

/// Session configuration.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Target address for reconnection
    pub addr: SocketAddr,
    /// Maximum pipeline depth
    pub pipeline_depth: usize,
    /// Reconnect delay
    pub reconnect_delay: Duration,
    /// Connect timeout
    pub connect_timeout: Duration,
    /// Use kernel timestamps
    pub use_kernel_timestamps: bool,
    /// Buffer size for send/recv buffers
    pub buffer_size: usize,
}

impl SessionConfig {
    pub fn from_config(addr: SocketAddr, config: &Config) -> Self {
        // Calculate buffer size based on value size, key size, and pipeline depth.
        // Each pipelined request/response needs space for:
        // - Key + value + protocol overhead (~100 bytes for RESP/Memcache framing)
        // We need space for the largest possible message in both send and recv buffers.
        // For SET: key + value + overhead
        // For GET response: key + value + overhead
        let value_size = config.workload.values.length;
        let key_size = config.workload.keyspace.length;
        let protocol_overhead = 256; // Conservative overhead for protocol framing
        let message_size = value_size + key_size + protocol_overhead;

        // Buffer needs to hold at least one full message, but preferably enough
        // for the pipeline depth to avoid excessive compaction.
        // Minimum 64KB for small values, scale up for larger values.
        let min_buffer_size = 64 * 1024;
        let pipeline_depth = config.connection.pipeline_depth;
        let buffer_size = (message_size * pipeline_depth)
            .max(message_size * 2)
            .max(min_buffer_size);

        Self {
            addr,
            pipeline_depth,
            reconnect_delay: Duration::from_millis(100),
            connect_timeout: config.connection.connect_timeout,
            use_kernel_timestamps: matches!(
                config.timestamps.mode,
                crate::config::TimestampMode::Software
            ),
            buffer_size,
        }
    }
}

/// Protocol-specific codec and decoder.
enum ProtocolCodec {
    Resp(RespCodec),
    Memcache(MemcacheCodec),
    MemcacheBinary(MemcacheBinaryCodec),
    Ping(PingCodec),
}

/// A protocol session that works with IoDriver.
///
/// The session handles:
/// - Protocol encoding (requests -> bytes)
/// - Protocol decoding (bytes -> responses)
/// - Request tracking (in-flight queue)
/// - Latency calculation
///
/// The IoDriver handles:
/// - Socket ownership and lifecycle
/// - Actual send/recv operations
/// - Connection state
pub struct Session {
    /// Connection index (set when connected)
    conn_id: Option<usize>,
    /// Target address
    addr: SocketAddr,
    /// Protocol codec
    codec: ProtocolCodec,
    /// Send/recv buffers
    buffers: BufferPair,
    /// In-flight request queue
    in_flight: VecDeque<InFlightRequest>,
    /// Maximum pipeline depth
    max_pipeline_depth: usize,
    /// Next request ID
    next_id: u64,
    /// Total requests sent on this session (for fairness tracking)
    requests_sent: u64,
    /// Connection state
    state: ConnectionState,
    /// Last reconnect attempt
    last_reconnect_attempt: Option<Instant>,
    /// Current reconnect delay
    reconnect_delay: Duration,
    /// Base reconnect delay (for reset after success)
    base_reconnect_delay: Duration,
    /// Connect timeout
    connect_timeout: Duration,
    /// Use kernel timestamps
    use_kernel_timestamps: bool,
    /// Most recent TX timestamp
    last_tx_timestamp: Option<Timestamp>,
    /// Most recent RX timestamp
    last_rx_timestamp: Option<Timestamp>,
    /// Cumulative bytes written to the send buffer (for sent_at tracking).
    bytes_written: u64,
    /// Cumulative bytes confirmed sent by the driver (for sent_at tracking).
    bytes_confirmed: u64,
}

impl Session {
    /// Create a new RESP/Redis session.
    pub fn new_resp(config: SessionConfig) -> Self {
        Self::new(config, ProtocolCodec::Resp(RespCodec::new()))
    }

    /// Create a new Memcache ASCII session.
    pub fn new_memcache(config: SessionConfig) -> Self {
        Self::new(config, ProtocolCodec::Memcache(MemcacheCodec::new()))
    }

    /// Create a new Memcache binary protocol session.
    pub fn new_memcache_binary(config: SessionConfig) -> Self {
        Self::new(
            config,
            ProtocolCodec::MemcacheBinary(MemcacheBinaryCodec::new()),
        )
    }

    /// Create a new PING protocol session.
    pub fn new_ping(config: SessionConfig) -> Self {
        Self::new(config, ProtocolCodec::Ping(PingCodec::new()))
    }

    /// Create a new session from config.
    ///
    /// Returns an error if the protocol is not supported by Session (e.g., Momento).
    pub fn from_config(addr: SocketAddr, config: &Config) -> Result<Self, SessionError> {
        let session_config = SessionConfig::from_config(addr, config);
        match config.target.protocol {
            Protocol::Resp | Protocol::Resp3 => Ok(Self::new_resp(session_config)),
            Protocol::Memcache => Ok(Self::new_memcache(session_config)),
            Protocol::MemcacheBinary => Ok(Self::new_memcache_binary(session_config)),
            Protocol::Ping => Ok(Self::new_ping(session_config)),
            Protocol::Momento => Err(SessionError::UnsupportedProtocol(
                "Momento uses MomentoSession, not Session",
            )),
        }
    }

    fn new(config: SessionConfig, codec: ProtocolCodec) -> Self {
        Self {
            conn_id: None,
            addr: config.addr,
            codec,
            buffers: BufferPair::with_capacity(config.buffer_size, config.buffer_size),
            in_flight: VecDeque::with_capacity(config.pipeline_depth),
            max_pipeline_depth: config.pipeline_depth,
            next_id: 0,
            requests_sent: 0,
            state: ConnectionState::Disconnected,
            last_reconnect_attempt: None,
            reconnect_delay: config.reconnect_delay,
            base_reconnect_delay: config.reconnect_delay,
            connect_timeout: config.connect_timeout,
            use_kernel_timestamps: config.use_kernel_timestamps,
            last_tx_timestamp: None,
            last_rx_timestamp: None,
            bytes_written: 0,
            bytes_confirmed: 0,
        }
    }

    /// Get the target address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Get the connection index if connected.
    pub fn conn_id(&self) -> Option<usize> {
        self.conn_id
    }

    /// Set the connection index (called after connection established).
    pub fn set_conn_id(&mut self, id: usize) {
        self.conn_id = Some(id);
        self.state = ConnectionState::Connected;
        self.reconnect_delay = self.base_reconnect_delay;
        self.last_reconnect_attempt = None;
    }

    /// Mark the session as disconnected and clear all connection-specific state.
    ///
    /// This clears in-flight requests, buffers, and timestamps since they belong
    /// to the now-closed connection and are no longer relevant.
    pub fn disconnect(&mut self) {
        self.conn_id = None;
        self.state = ConnectionState::Disconnected;

        // Clear connection-specific state since the connection is gone
        self.in_flight.clear();
        self.buffers.send.clear();
        self.buffers.recv.clear();
        self.last_tx_timestamp = None;
        self.last_rx_timestamp = None;
        self.bytes_written = 0;
        self.bytes_confirmed = 0;

        // Reset codec state
        match &mut self.codec {
            ProtocolCodec::Resp(codec) => codec.reset_counter(),
            ProtocolCodec::Memcache(_) => {}
            ProtocolCodec::MemcacheBinary(codec) => codec.reset(),
            ProtocolCodec::Ping(codec) => codec.reset(),
        }
    }

    /// Check if the session is connected.
    pub fn is_connected(&self) -> bool {
        self.state == ConnectionState::Connected && self.conn_id.is_some()
    }

    /// Check if reconnection should be attempted.
    pub fn should_reconnect(&self) -> bool {
        if self.state == ConnectionState::Connected {
            return false;
        }

        if let Some(last) = self.last_reconnect_attempt {
            last.elapsed() >= self.reconnect_delay
        } else {
            true
        }
    }

    /// Mark that a reconnection attempt was made.
    pub fn reconnect_attempted(&mut self, success: bool) {
        self.last_reconnect_attempt = Some(Instant::now());
        if !success {
            // Exponential backoff, max 5 seconds
            self.reconnect_delay = (self.reconnect_delay * 2).min(Duration::from_secs(5));
        }
    }

    /// Get the connect timeout.
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Check if pipeline has room for more requests.
    #[inline]
    pub fn can_send(&self) -> bool {
        self.state == ConnectionState::Connected && self.in_flight.len() < self.max_pipeline_depth
    }

    /// Get the number of in-flight requests.
    #[inline]
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Get the total number of requests sent on this session.
    #[inline]
    pub fn requests_sent(&self) -> u64 {
        self.requests_sent
    }

    /// Queue a GET request.
    ///
    /// The `now` parameter should be the current time, shared across a batch of
    /// requests to avoid excessive clock_gettime calls.
    #[inline]
    pub fn get(&mut self, key: &[u8], now: Instant) -> Option<u64> {
        if !self.can_send() {
            return None;
        }

        let id = self.next_id;
        self.next_id += 1;

        let before = self.buffers.send.readable();
        match &mut self.codec {
            ProtocolCodec::Resp(codec) => {
                codec.encode_get(&mut self.buffers.send, key);
            }
            ProtocolCodec::Memcache(codec) => {
                codec.encode_get(&mut self.buffers.send, key);
            }
            ProtocolCodec::MemcacheBinary(codec) => {
                codec.encode_get(&mut self.buffers.send, key);
            }
            ProtocolCodec::Ping(codec) => {
                codec.encode_ping(&mut self.buffers.send);
            }
        }
        self.bytes_written += (self.buffers.send.readable() - before) as u64;

        self.in_flight.push_back(InFlightRequest {
            id,
            request_type: RequestType::Get,
            queued_at: now,
            sent_at: None,
            first_byte_at: None,
            bytes_end: self.bytes_written,
            tx_timestamp: None,
        });
        self.requests_sent += 1;

        tracing::trace!(
            conn_id = ?self.conn_id,
            id = id,
            in_flight = self.in_flight.len(),
            "queued GET request"
        );

        Some(id)
    }

    /// Queue a SET request.
    ///
    /// The `now` parameter should be the current time, shared across a batch of
    /// requests to avoid excessive clock_gettime calls.
    #[inline]
    pub fn set(&mut self, key: &[u8], value: &[u8], now: Instant) -> Option<u64> {
        if !self.can_send() {
            return None;
        }

        let id = self.next_id;
        self.next_id += 1;

        let before = self.buffers.send.readable();
        match &mut self.codec {
            ProtocolCodec::Resp(codec) => {
                codec.encode_set(&mut self.buffers.send, key, value);
            }
            ProtocolCodec::Memcache(codec) => {
                codec.encode_set(&mut self.buffers.send, key, value, 0, 0);
            }
            ProtocolCodec::MemcacheBinary(codec) => {
                codec.encode_set(&mut self.buffers.send, key, value, 0, 0);
            }
            ProtocolCodec::Ping(codec) => {
                codec.encode_ping(&mut self.buffers.send);
            }
        }
        self.bytes_written += (self.buffers.send.readable() - before) as u64;

        self.in_flight.push_back(InFlightRequest {
            id,
            request_type: RequestType::Set,
            queued_at: now,
            sent_at: None,
            first_byte_at: None,
            bytes_end: self.bytes_written,
            tx_timestamp: None,
        });
        self.requests_sent += 1;

        tracing::trace!(
            conn_id = ?self.conn_id,
            id = id,
            in_flight = self.in_flight.len(),
            "queued SET request"
        );

        Some(id)
    }

    /// Queue a DELETE request.
    ///
    /// The `now` parameter should be the current time, shared across a batch of
    /// requests to avoid excessive clock_gettime calls.
    #[inline]
    pub fn delete(&mut self, key: &[u8], now: Instant) -> Option<u64> {
        if !self.can_send() {
            return None;
        }

        let id = self.next_id;
        self.next_id += 1;

        let before = self.buffers.send.readable();
        match &mut self.codec {
            ProtocolCodec::Resp(codec) => {
                codec.encode_delete(&mut self.buffers.send, key);
            }
            ProtocolCodec::Memcache(codec) => {
                codec.encode_delete(&mut self.buffers.send, key);
            }
            ProtocolCodec::MemcacheBinary(codec) => {
                codec.encode_delete(&mut self.buffers.send, key);
            }
            ProtocolCodec::Ping(codec) => {
                // Ping protocol doesn't support DELETE, send PING instead
                codec.encode_ping(&mut self.buffers.send);
            }
        }
        self.bytes_written += (self.buffers.send.readable() - before) as u64;

        self.in_flight.push_back(InFlightRequest {
            id,
            request_type: RequestType::Delete,
            queued_at: now,
            sent_at: None,
            first_byte_at: None,
            bytes_end: self.bytes_written,
            tx_timestamp: None,
        });
        self.requests_sent += 1;

        tracing::trace!(
            conn_id = ?self.conn_id,
            id = id,
            in_flight = self.in_flight.len(),
            "queued DELETE request"
        );

        Some(id)
    }

    /// Get the send buffer to be written to the socket.
    pub fn send_buffer(&self) -> &[u8] {
        self.buffers.send.as_slice()
    }

    /// Mark bytes as sent (removes from send buffer).
    pub fn bytes_sent(&mut self, n: usize) {
        self.buffers.send.consume(n);

        // Track bytes transmitted
        crate::metrics::BYTES_TX.add(n as u64);

        // Advance confirmed byte counter and stamp requests that are fully sent
        self.bytes_confirmed += n as u64;
        let now = Instant::now();
        for req in &mut self.in_flight {
            if req.sent_at.is_some() {
                continue;
            }
            if req.bytes_end <= self.bytes_confirmed {
                req.sent_at = Some(now);
            } else {
                break;
            }
        }

        // Capture TX timestamp for in-flight requests
        if self.use_kernel_timestamps
            && let Some(ts) = self.last_tx_timestamp.take()
        {
            for req in &mut self.in_flight {
                if req.tx_timestamp.is_none() {
                    req.tx_timestamp = Some(ts);
                    break;
                }
            }
        }
    }

    /// Set the TX timestamp (from IoDriver if available).
    pub fn set_tx_timestamp(&mut self, ts: Timestamp) {
        self.last_tx_timestamp = Some(ts);
    }

    /// Set the RX timestamp (from IoDriver if available).
    pub fn set_rx_timestamp(&mut self, ts: Timestamp) {
        self.last_rx_timestamp = Some(ts);
    }

    /// Add received data to the receive buffer.
    pub fn bytes_received(&mut self, data: &[u8]) {
        // Compact if needed to make space
        if self.buffers.recv.writable() < data.len() {
            self.buffers.recv.compact();
        }
        self.buffers.recv.extend_from_slice(data);
    }

    /// Process received data and extract completed responses.
    ///
    /// The `now` parameter should be the current time, shared across a batch of
    /// response processing to avoid excessive clock_gettime calls.
    #[inline]
    pub fn poll_responses(
        &mut self,
        results: &mut Vec<RequestResult>,
        now: Instant,
    ) -> Result<usize, SessionError> {
        let rx_timestamp = self.last_rx_timestamp;
        let mut count = 0;

        loop {
            // Capture buffer preview before parsing (for debug output if orphan response)
            let buf_preview: Vec<u8> = {
                let data = self.buffers.recv.as_slice();
                let preview_len = data.len().min(128);
                data[..preview_len].to_vec()
            };

            let response = match &mut self.codec {
                ProtocolCodec::Resp(codec) => match codec.decode_response(&mut self.buffers.recv) {
                    Ok(Some(v)) => Some(ResponseInfo {
                        is_error: v.is_error(),
                        is_null: v.is_null(),
                    }),
                    Ok(None) => None,
                    Err(e) => return Err(SessionError::Resp(e)),
                },
                ProtocolCodec::Memcache(codec) => {
                    match codec.decode_response(&mut self.buffers.recv) {
                        Ok(Some(v)) => Some(ResponseInfo {
                            is_error: v.is_error(),
                            is_null: v.is_miss(),
                        }),
                        Ok(None) => None,
                        Err(e) => return Err(SessionError::Memcache(e)),
                    }
                }
                ProtocolCodec::MemcacheBinary(codec) => {
                    match codec.decode_response(&mut self.buffers.recv) {
                        Ok(Some(v)) => Some(ResponseInfo {
                            is_error: v.is_error(),
                            is_null: v.is_miss(),
                        }),
                        Ok(None) => None,
                        Err(e) => return Err(SessionError::MemcacheBinary(e)),
                    }
                }
                ProtocolCodec::Ping(codec) => match codec.decode_response(&mut self.buffers.recv) {
                    Ok(Some(v)) => Some(ResponseInfo {
                        is_error: v.is_error(),
                        is_null: false,
                    }),
                    Ok(None) => None,
                    Err(e) => return Err(SessionError::Ping(e)),
                },
            };

            match response {
                Some(resp) => {
                    if let Some(req) = self.in_flight.pop_front() {
                        tracing::trace!(
                            id = req.id,
                            in_flight_remaining = self.in_flight.len(),
                            path = "poll_responses",
                            "matched response"
                        );

                        let latency_ns = self.calculate_latency(&req, now, rx_timestamp);
                        let ttfb_ns = self.calculate_ttfb(&req);

                        let hit = if req.request_type == RequestType::Get {
                            Some(!resp.is_null)
                        } else {
                            None
                        };

                        results.push(RequestResult {
                            id: req.id,
                            success: !resp.is_error,
                            is_error_response: resp.is_error,
                            latency_ns,
                            ttfb_ns,
                            request_type: req.request_type,
                            hit,
                        });
                        count += 1;
                    } else {
                        // Debug: print info about the orphan response
                        let hex_preview = format_hex_preview(&buf_preview, 64);
                        let ascii_preview: String = buf_preview
                            .iter()
                            .take(128)
                            .map(|&b| {
                                if b.is_ascii_graphic() || b == b' ' {
                                    b as char
                                } else {
                                    '.'
                                }
                            })
                            .collect();
                        tracing::warn!(
                            in_flight = self.in_flight.len(),
                            is_error = resp.is_error,
                            is_null = resp.is_null,
                            path = "poll_responses",
                            "received response without pending request\n  hex: {}\n  ascii: {:?}",
                            hex_preview,
                            ascii_preview
                        );
                    }
                }
                None => break,
            }
        }

        if count > 0 {
            self.last_rx_timestamp = None;
        }

        Ok(count)
    }

    /// Process responses directly from a RecvBuf (zero-copy path).
    ///
    /// This method parses responses directly from the driver's buffer without
    /// copying data to an intermediate buffer first. For io_uring single-shot
    /// mode, this achieves true zero-copy receive.
    ///
    /// The `now` parameter should be the current time, shared across a batch of
    /// response processing to avoid excessive clock_gettime calls.
    #[inline]
    pub fn poll_responses_from(
        &mut self,
        recv_buf: &mut dyn RecvBuf,
        results: &mut Vec<RequestResult>,
        now: Instant,
    ) -> Result<usize, SessionError> {
        let rx_timestamp = self.last_rx_timestamp;
        let mut count = 0;

        loop {
            let data = recv_buf.as_slice();
            if data.is_empty() {
                break;
            }

            // Capture buffer preview before parsing (for debug output if orphan response)
            let buf_preview: Vec<u8> = {
                let preview_len = data.len().min(128);
                data[..preview_len].to_vec()
            };

            // Parse response based on protocol (zero-copy from RecvBuf)
            let response = match &mut self.codec {
                ProtocolCodec::Resp(codec) => {
                    match RespValueParser::parse_with_options(data, &resp_parse_options()) {
                        Ok((value, consumed)) => {
                            recv_buf.consume(consumed);
                            codec.increment_parsed();
                            Some(ResponseInfo {
                                is_error: value.is_error(),
                                is_null: value.is_null(),
                            })
                        }
                        Err(protocol_resp::ParseError::Incomplete) => None,
                        Err(e) => return Err(SessionError::Resp(RespError::from_parse_error(e))),
                    }
                }
                ProtocolCodec::Memcache(_codec) => match MemcacheResponseParser::parse(data) {
                    Ok((value, consumed)) => {
                        recv_buf.consume(consumed);
                        Some(ResponseInfo {
                            is_error: value.is_error(),
                            is_null: value.is_miss(),
                        })
                    }
                    Err(protocol_memcache::ParseError::Incomplete) => None,
                    Err(e) => {
                        return Err(SessionError::Memcache(MemcacheError::from_parse_error(e)));
                    }
                },
                ProtocolCodec::MemcacheBinary(codec) => {
                    match MemcacheBinaryResponseParser::parse(data) {
                        Ok((value, consumed)) => {
                            // Determine error/miss from the parsed response variant
                            // Must do this before consume() since value borrows from data
                            let (is_error, is_miss) = match &value {
                                MemcacheBinaryResponseParser::Error { status, .. } => (
                                    true,
                                    *status == protocol_memcache::binary::Status::KeyNotFound,
                                ),
                                _ => (false, false),
                            };
                            recv_buf.consume(consumed);
                            codec.decrement_pending();
                            Some(ResponseInfo {
                                is_error,
                                is_null: is_miss,
                            })
                        }
                        Err(protocol_memcache::ParseError::Incomplete) => None,
                        Err(e) => {
                            return Err(SessionError::MemcacheBinary(
                                MemcacheBinaryError::from_parse_error(e),
                            ));
                        }
                    }
                }
                ProtocolCodec::Ping(codec) => {
                    match PingResponseParser::parse(data) {
                        Ok((_response, consumed)) => {
                            recv_buf.consume(consumed);
                            codec.decrement_pending();
                            Some(ResponseInfo {
                                is_error: false, // PONG is never an error
                                is_null: false,
                            })
                        }
                        Err(protocol_ping::ParseError::Incomplete) => None,
                        Err(e) => return Err(SessionError::Ping(PingError::from_parse_error(e))),
                    }
                }
            };

            match response {
                Some(resp) => {
                    if let Some(req) = self.in_flight.pop_front() {
                        let latency_ns = self.calculate_latency(&req, now, rx_timestamp);
                        let ttfb_ns = self.calculate_ttfb(&req);

                        let hit = if req.request_type == RequestType::Get {
                            Some(!resp.is_null)
                        } else {
                            None
                        };

                        tracing::trace!(
                            conn_id = ?self.conn_id,
                            id = req.id,
                            in_flight_remaining = self.in_flight.len(),
                            is_error = resp.is_error,
                            is_null = resp.is_null,
                            path = "poll_responses_from",
                            "matched response"
                        );

                        results.push(RequestResult {
                            id: req.id,
                            success: !resp.is_error,
                            is_error_response: resp.is_error,
                            latency_ns,
                            ttfb_ns,
                            request_type: req.request_type,
                            hit,
                        });
                        count += 1;
                    } else {
                        // Debug: print info about the orphan response
                        let hex_preview = format_hex_preview(&buf_preview, 64);
                        let ascii_preview: String = buf_preview
                            .iter()
                            .take(128)
                            .map(|&b| {
                                if b.is_ascii_graphic() || b == b' ' {
                                    b as char
                                } else {
                                    '.'
                                }
                            })
                            .collect();
                        tracing::warn!(
                            conn_id = ?self.conn_id,
                            in_flight = self.in_flight.len(),
                            is_error = resp.is_error,
                            is_null = resp.is_null,
                            path = "poll_responses_from",
                            "received response without pending request\n  hex: {}\n  ascii: {:?}",
                            hex_preview,
                            ascii_preview
                        );
                    }
                }
                None => break,
            }
        }

        if count > 0 {
            self.last_rx_timestamp = None;
        }

        // Note: BYTES_RX is tracked by the caller (on_data in worker.rs)
        // to avoid double-counting when using the zero-copy DataSlice path.

        Ok(count)
    }

    /// Stamp `first_byte_at` on the first in-flight request that hasn't been
    /// stamped yet. Called when a recv completion indicates data is available.
    pub fn stamp_first_byte(&mut self, now: Instant) {
        for req in &mut self.in_flight {
            if req.first_byte_at.is_some() {
                continue;
            }
            req.first_byte_at = Some(now);
            break;
        }
    }

    fn calculate_ttfb(&self, req: &InFlightRequest) -> Option<u64> {
        let first_byte = req.first_byte_at?;
        let baseline = req.sent_at.unwrap_or(req.queued_at);
        Some(first_byte.duration_since(baseline).as_nanos() as u64)
    }

    fn calculate_latency(
        &self,
        req: &InFlightRequest,
        now: Instant,
        rx_timestamp: Option<Timestamp>,
    ) -> u64 {
        if self.use_kernel_timestamps
            && let (Some(tx), Some(rx)) = (req.tx_timestamp, rx_timestamp)
            && let (Some(tx_ns), Some(rx_ns)) = (tx.kernel_ns, rx.kernel_ns)
        {
            return rx_ns.saturating_sub(tx_ns);
        }
        // Use sent_at (when bytes actually reached kernel) instead of queued_at
        // (when request was encoded into buffer) to avoid inflating latency
        // from pipelining and send buffer queuing delays.
        let baseline = req.sent_at.unwrap_or(req.queued_at);
        now.duration_since(baseline).as_nanos() as u64
    }

    /// Clear all buffers and in-flight state (for reconnection).
    pub fn reset(&mut self) {
        self.buffers.send.clear();
        self.buffers.recv.clear();
        self.in_flight.clear();
        self.last_tx_timestamp = None;
        self.last_rx_timestamp = None;
        self.bytes_written = 0;
        self.bytes_confirmed = 0;

        match &mut self.codec {
            ProtocolCodec::Resp(codec) => codec.reset_counter(),
            ProtocolCodec::Memcache(_) => {}
            ProtocolCodec::MemcacheBinary(codec) => codec.reset(),
            ProtocolCodec::Ping(codec) => codec.reset(),
        }
    }
}

/// Simplified response info for latency tracking.
struct ResponseInfo {
    is_error: bool,
    is_null: bool,
}

/// Session error type.
#[derive(Debug)]
pub enum SessionError {
    Resp(RespError),
    Memcache(MemcacheError),
    MemcacheBinary(MemcacheBinaryError),
    Ping(PingError),
    /// Protocol not supported by Session (e.g., Momento uses MomentoSession)
    UnsupportedProtocol(&'static str),
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionError::Resp(e) => write!(f, "RESP error: {}", e),
            SessionError::Memcache(e) => write!(f, "Memcache error: {}", e),
            SessionError::MemcacheBinary(e) => write!(f, "Memcache binary error: {}", e),
            SessionError::Ping(e) => write!(f, "Ping error: {}", e),
            SessionError::UnsupportedProtocol(proto) => {
                write!(f, "unsupported protocol: {}", proto)
            }
        }
    }
}

impl std::error::Error for SessionError {}
