//! Per-connection state for the cache server.

use bytes::{Bytes, BytesMut};
use cache_core::{Cache, CacheError, SegmentReservation, SetReservation, ValueRef};
use io_driver::RecvBuf;
use protocol_memcache::binary::{
    BINARY_STREAMING_THRESHOLD, BinaryParseProgress, REQUEST_MAGIC, parse_binary_streaming,
};
use protocol_memcache::{
    ParseOptions as MemcacheParseOptions, ParseProgress as MemcacheParseProgress,
    STREAMING_THRESHOLD as MEMCACHE_STREAMING_THRESHOLD,
    parse_streaming as parse_memcache_streaming,
};
use protocol_resp::{
    Command as RespCommand, ParseOptions, ParseProgress, STREAMING_THRESHOLD, parse_streaming,
};
use std::collections::VecDeque;
use std::time::Duration;

use crate::execute::{RespVersion, execute_memcache, execute_memcache_binary, execute_resp};
use crate::metrics::PROTOCOL_ERRORS;

/// Minimum value size for the zero-copy send queue path.
/// Values below this threshold are copied into `write_buf` to avoid queue overhead.
const ZERO_COPY_MIN_VALUE_SIZE: usize = 1024;

/// Protocol type detected for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetectedProtocol {
    Unknown,
    Resp,
    MemcacheAscii,
    MemcacheBinary,
}

/// State for streaming receive of large SET values.
///
/// When a SET command with a large value (>= STREAMING_THRESHOLD) is parsed,
/// we reserve space and receive directly into it, avoiding the need for a
/// growing coalesce buffer.
enum StreamingState {
    /// Normal mode - parse complete commands
    None,
    /// Draining an oversized value that exceeded max_value_size.
    /// We need to consume `remaining` bytes (value + trailing CRLF) before resuming.
    Draining {
        /// Bytes remaining to drain (includes trailing CRLF)
        remaining: usize,
    },
    /// Receiving value data directly into segment memory (true zero-copy) - RESP protocol
    ReceivingSegment {
        /// Reservation for segment memory
        reservation: SegmentReservation,
        /// Bytes received so far
        received: usize,
    },
    /// Receiving value data into Vec buffer (fallback, one extra copy) - RESP protocol
    ReceivingVec {
        /// Pre-allocated buffer for the value
        reservation: SetReservation,
        /// Bytes received so far
        received: usize,
    },
    /// Receiving value data into segment for memcache ASCII protocol
    MemcacheAsciiSegment {
        /// Reservation for segment memory
        reservation: SegmentReservation,
        /// Bytes received so far
        received: usize,
        /// Whether to suppress the response
        noreply: bool,
    },
    /// Receiving value data into Vec for memcache ASCII protocol (fallback)
    MemcacheAsciiVec {
        /// Pre-allocated buffer for the value
        reservation: SetReservation,
        /// Bytes received so far
        received: usize,
        /// Whether to suppress the response
        noreply: bool,
    },
    /// Receiving value data into segment for memcache binary protocol
    MemcacheBinarySegment {
        /// Reservation for segment memory
        reservation: SegmentReservation,
        /// Bytes received so far
        received: usize,
        /// Original opcode for response
        opcode: protocol_memcache::binary::Opcode,
        /// Opaque value for response
        opaque: u32,
    },
    /// Receiving value data into Vec for memcache binary protocol (fallback)
    MemcacheBinaryVec {
        /// Pre-allocated buffer for the value
        reservation: SetReservation,
        /// Bytes received so far
        received: usize,
        /// Original opcode for response
        opcode: protocol_memcache::binary::Opcode,
        /// Opaque value for response
        opaque: u32,
    },
}

/// Per-connection state for the cache server.
///
/// The connection no longer owns a read buffer - instead, data is accessed
/// directly from the driver's receive buffer via `process_from()`.
/// This enables zero-copy parsing when data fits in a single kernel buffer.
pub struct Connection {
    write_buf: BytesMut,
    write_pos: usize,
    /// Ordered queue of owned send buffers for zero-copy GET responses.
    /// Drained before `write_buf` in the send path.
    send_queue: VecDeque<Bytes>,
    /// Byte progress into the front entry of `send_queue`.
    send_offset: usize,
    /// Total unsent bytes across all `send_queue` entries (for backpressure).
    send_queue_bytes: usize,
    protocol: DetectedProtocol,
    should_close: bool,
    resp_version: RespVersion,
    /// Parse options for RESP protocol (controls max value size, etc.)
    resp_parse_options: ParseOptions,
    /// Parse options for memcache ASCII protocol
    memcache_parse_options: MemcacheParseOptions,
    /// State for streaming receive of large SET values
    streaming_state: StreamingState,
    /// Whether flush commands are allowed on this connection
    allow_flush: bool,
}

impl Connection {
    /// Create a new connection with the specified max value size.
    pub fn new(max_value_size: usize) -> Self {
        Self::with_options(max_value_size, false)
    }

    /// Create a new connection with the specified max value size and flush permission.
    pub fn with_options(max_value_size: usize, allow_flush: bool) -> Self {
        Self {
            write_buf: BytesMut::with_capacity(65536),
            write_pos: 0,
            send_queue: VecDeque::new(),
            send_offset: 0,
            send_queue_bytes: 0,
            protocol: DetectedProtocol::Unknown,
            should_close: false,
            resp_version: RespVersion::default(),
            resp_parse_options: ParseOptions::new().max_bulk_string_len(max_value_size),
            memcache_parse_options: MemcacheParseOptions::new().max_value_len(max_value_size),
            streaming_state: StreamingState::None,
            allow_flush,
        }
    }

    /// Maximum pending write buffer size before applying backpressure.
    /// Stop processing new requests if we have this much unsent data.
    pub const MAX_PENDING_WRITE: usize = 256 * 1024; // 256KB

    /// Check if we should accept more data from the socket.
    /// Returns false when we have too much pending write data (backpressure).
    #[inline]
    pub fn should_read(&self) -> bool {
        self.pending_write_len() <= Self::MAX_PENDING_WRITE
    }

    /// Get the amount of pending write data (send queue + write buffer).
    #[inline]
    pub fn pending_write_len(&self) -> usize {
        self.send_queue_bytes + self.write_buf.len().saturating_sub(self.write_pos)
    }

    /// Detect protocol from the first byte of data.
    #[inline]
    fn detect_protocol(&mut self, first_byte: u8) {
        if self.protocol != DetectedProtocol::Unknown {
            return;
        }

        match first_byte {
            REQUEST_MAGIC => {
                self.protocol = DetectedProtocol::MemcacheBinary;
            }
            b'*' => {
                self.protocol = DetectedProtocol::Resp;
            }
            _ => {
                self.protocol = DetectedProtocol::MemcacheAscii;
            }
        }
    }

    /// Process all complete commands from the receive buffer.
    ///
    /// This is the zero-copy path - data is read directly from the driver's
    /// buffer without copying into a connection-owned buffer.
    #[inline]
    pub fn process_from<C: Cache>(&mut self, buf: &mut dyn RecvBuf, cache: &C) {
        // Clear write buffer if all data has been sent and queue is empty
        if self.send_queue.is_empty() && self.write_pos >= self.write_buf.len() {
            self.write_buf.clear();
            self.write_pos = 0;
        }

        // Detect protocol from first byte
        let data = buf.as_slice();
        if !data.is_empty() && self.protocol == DetectedProtocol::Unknown {
            self.detect_protocol(data[0]);
        }

        match self.protocol {
            DetectedProtocol::Unknown => {}
            DetectedProtocol::Resp => self.process_resp_from(buf, cache),
            DetectedProtocol::MemcacheAscii => self.process_memcache_ascii_from(buf, cache),
            DetectedProtocol::MemcacheBinary => self.process_memcache_binary_from(buf, cache),
        }
    }

    #[inline]
    fn process_resp_from<C: Cache>(&mut self, buf: &mut dyn RecvBuf, cache: &C) {
        loop {
            // Check for empty buffer (use len() to avoid holding borrow)
            if buf.is_empty() {
                break;
            }

            // Backpressure: stop processing if write buffer is too large
            if self.pending_write_len() > Self::MAX_PENDING_WRITE {
                break;
            }

            // Check if we're in the middle of receiving a large value
            // This must be checked before we borrow data for parsing
            if self.continue_streaming_recv(buf, cache) {
                // Still receiving value data, or completed a streaming SET
                continue;
            }

            // Normal command parsing with streaming support
            // Now safe to borrow data since streaming check is complete
            let data = buf.as_slice();
            match parse_streaming(data, &self.resp_parse_options, STREAMING_THRESHOLD) {
                Ok(ParseProgress::Complete(cmd, consumed)) => {
                    // Intercept GET for zero-copy path
                    if let RespCommand::Get { key } = cmd {
                        use crate::metrics::{GETS, HITS, MISSES};
                        GETS.increment();

                        if let Some(value_ref) = cache.get_value_ref(key) {
                            HITS.increment();
                            buf.consume(consumed);
                            // Write RESP header: $<len>\r\n
                            self.write_buf.extend_from_slice(b"$");
                            let mut len_buf = itoa::Buffer::new();
                            self.write_buf
                                .extend_from_slice(len_buf.format(value_ref.len()).as_bytes());
                            self.write_buf.extend_from_slice(b"\r\n");
                            // Queue value with CRLF trailer
                            self.queue_zero_copy_value(value_ref, b"\r\n");
                            continue;
                        }

                        MISSES.increment();
                        if self.resp_version == RespVersion::Resp3 {
                            self.write_buf.extend_from_slice(b"_\r\n");
                        } else {
                            self.write_buf.extend_from_slice(b"$-1\r\n");
                        }
                        buf.consume(consumed);
                        continue;
                    }

                    execute_resp(
                        &cmd,
                        cache,
                        &mut self.write_buf,
                        &mut self.resp_version,
                        self.allow_flush,
                    );
                    buf.consume(consumed);
                }
                Ok(ParseProgress::NeedValue {
                    header,
                    value_len,
                    value_prefix,
                    header_consumed,
                }) => {
                    // Extract all values from header before we consume the buffer
                    // (header borrows from data which borrows from buf)
                    let ttl = header.ttl().unwrap_or(Duration::from_secs(3600));
                    let key = header.key.to_vec(); // Copy key before consuming
                    let prefix_len = value_prefix.len();

                    // Try segment-based reservation first (true zero-copy)
                    match cache.begin_segment_set(&key, value_len, Some(ttl)) {
                        Ok(mut reservation) => {
                            // Copy any value bytes already in the buffer
                            if prefix_len > 0 {
                                reservation.value_mut()[..prefix_len].copy_from_slice(value_prefix);
                            }

                            // Consume the header + prefix
                            buf.consume(header_consumed + prefix_len);

                            // Transition to segment-based streaming
                            self.streaming_state = StreamingState::ReceivingSegment {
                                reservation,
                                received: prefix_len,
                            };
                        }
                        Err(CacheError::Unsupported) => {
                            // Fall back to Vec-based reservation (one extra copy)
                            let mut reservation = SetReservation::new(&key, value_len, &[], ttl);

                            // Copy any value bytes already in the buffer
                            if prefix_len > 0 {
                                reservation.value_mut()[..prefix_len].copy_from_slice(value_prefix);
                            }

                            // Consume the header + prefix
                            buf.consume(header_consumed + prefix_len);

                            // Transition to Vec-based streaming
                            self.streaming_state = StreamingState::ReceivingVec {
                                reservation,
                                received: prefix_len,
                            };
                        }
                        Err(e) => {
                            // Cache error (out of memory, etc.)
                            // Send error response
                            self.write_buf.extend_from_slice(b"-ERR ");
                            self.write_buf.extend_from_slice(e.to_string().as_bytes());
                            self.write_buf.extend_from_slice(b"\r\n");

                            // Consume the header + prefix we've already seen
                            buf.consume(header_consumed + prefix_len);

                            // Calculate remaining bytes to drain: rest of value + CRLF
                            let remaining_value = value_len - prefix_len;
                            let remaining_to_drain = remaining_value + 2; // +2 for trailing CRLF

                            // Transition to draining state
                            self.streaming_state = StreamingState::Draining {
                                remaining: remaining_to_drain,
                            };
                        }
                    }
                }
                Ok(ParseProgress::ValueTooLarge {
                    value_len,
                    value_prefix_len,
                    header_consumed,
                    max_value_size,
                }) => {
                    // Send error response
                    self.write_buf.extend_from_slice(b"-ERR value too large: ");
                    let mut len_buf = itoa::Buffer::new();
                    self.write_buf
                        .extend_from_slice(len_buf.format(value_len).as_bytes());
                    self.write_buf.extend_from_slice(b" bytes exceeds ");
                    self.write_buf
                        .extend_from_slice(len_buf.format(max_value_size).as_bytes());
                    self.write_buf.extend_from_slice(b" byte limit\r\n");

                    // Consume the header + prefix we've already seen
                    buf.consume(header_consumed + value_prefix_len);

                    // Calculate remaining bytes to drain: rest of value + CRLF
                    let remaining_value = value_len - value_prefix_len;
                    let remaining_to_drain = remaining_value + 2; // +2 for trailing CRLF

                    // Transition to draining state
                    self.streaming_state = StreamingState::Draining {
                        remaining: remaining_to_drain,
                    };
                }
                Ok(ParseProgress::Incomplete) => break,
                Err(e) => {
                    PROTOCOL_ERRORS.increment();
                    let msg = e.to_string();
                    if msg.contains("expected array") {
                        self.write_buf.extend_from_slice(
                            b"-ERR Protocol error: expected Redis RESP protocol\r\n",
                        );
                    } else {
                        self.write_buf.extend_from_slice(b"-ERR ");
                        self.write_buf.extend_from_slice(msg.as_bytes());
                        self.write_buf.extend_from_slice(b"\r\n");
                    }
                    // Reset streaming state on error
                    self.streaming_state = StreamingState::None;
                    // Consume all remaining data on error
                    let len = buf.len();
                    buf.consume(len);
                    break;
                }
            }
        }
    }

    /// Continue receiving value data for a streaming SET.
    ///
    /// Returns true if we were in streaming mode (regardless of completion).
    #[inline]
    fn continue_streaming_recv<C: Cache>(&mut self, buf: &mut dyn RecvBuf, cache: &C) -> bool {
        match &mut self.streaming_state {
            StreamingState::None => false,
            StreamingState::Draining { remaining } => {
                let data = buf.as_slice();
                if data.len() < *remaining {
                    // Not enough data yet - consume all and keep draining
                    *remaining -= data.len();
                    let len = data.len();
                    buf.consume(len);
                    return true;
                }

                // We have all the bytes to drain
                let to_consume = *remaining;
                buf.consume(to_consume);
                self.streaming_state = StreamingState::None;
                true
            }
            StreamingState::ReceivingSegment {
                reservation,
                received,
            } => {
                let value_len = reservation.value_len();
                let remaining = value_len - *received;
                let data = buf.as_slice();

                if data.len() < remaining {
                    // Not enough data yet - copy what we have to segment memory
                    let to_copy = data.len();
                    reservation.value_mut()[*received..*received + to_copy]
                        .copy_from_slice(&data[..to_copy]);
                    *received += to_copy;
                    buf.consume(to_copy);
                    return true;
                }

                // We have all the value bytes - copy to segment memory
                reservation.value_mut()[*received..value_len].copy_from_slice(&data[..remaining]);
                buf.consume(remaining);

                // Check for trailing CRLF
                let trailing = buf.as_slice();
                if trailing.len() < 2 {
                    *received = value_len;
                    return true;
                }

                if &trailing[..2] != b"\r\n" {
                    PROTOCOL_ERRORS.increment();
                    self.write_buf
                        .extend_from_slice(b"-ERR Protocol error: expected CRLF after value\r\n");
                    // Cancel the segment reservation
                    let state = std::mem::replace(&mut self.streaming_state, StreamingState::None);
                    if let StreamingState::ReceivingSegment { reservation, .. } = state {
                        cache.cancel_segment_set(reservation);
                    }
                    return true;
                }
                buf.consume(2);

                // Extract reservation and commit
                let state = std::mem::replace(&mut self.streaming_state, StreamingState::None);
                if let StreamingState::ReceivingSegment { reservation, .. } = state {
                    if let Err(_e) = cache.commit_segment_set(reservation) {
                        self.write_buf
                            .extend_from_slice(b"-ERR Failed to store value\r\n");
                    } else {
                        self.write_buf.extend_from_slice(b"+OK\r\n");
                    }
                }

                true
            }
            StreamingState::ReceivingVec {
                reservation,
                received,
            } => {
                let value_len = reservation.value_len();
                let remaining = value_len - *received;
                let data = buf.as_slice();

                if data.len() < remaining {
                    // Not enough data yet - copy what we have
                    let to_copy = data.len();
                    reservation.value_mut()[*received..*received + to_copy]
                        .copy_from_slice(&data[..to_copy]);
                    *received += to_copy;
                    buf.consume(to_copy);
                    return true;
                }

                // We have all the value bytes
                reservation.value_mut()[*received..value_len].copy_from_slice(&data[..remaining]);
                buf.consume(remaining);

                // Check for trailing CRLF
                let trailing = buf.as_slice();
                if trailing.len() < 2 {
                    *received = value_len;
                    return true;
                }

                if &trailing[..2] != b"\r\n" {
                    PROTOCOL_ERRORS.increment();
                    self.write_buf
                        .extend_from_slice(b"-ERR Protocol error: expected CRLF after value\r\n");
                    self.streaming_state = StreamingState::None;
                    return true;
                }
                buf.consume(2);

                // Extract reservation and commit (Vec-based uses commit_set)
                let state = std::mem::replace(&mut self.streaming_state, StreamingState::None);
                if let StreamingState::ReceivingVec { reservation, .. } = state {
                    if let Err(_e) = cache.commit_set(reservation) {
                        self.write_buf
                            .extend_from_slice(b"-ERR Failed to store value\r\n");
                    } else {
                        self.write_buf.extend_from_slice(b"+OK\r\n");
                    }
                }

                true
            }
            // Memcache streaming states are handled by their own continue methods
            StreamingState::MemcacheAsciiSegment { .. }
            | StreamingState::MemcacheAsciiVec { .. }
            | StreamingState::MemcacheBinarySegment { .. }
            | StreamingState::MemcacheBinaryVec { .. } => false,
        }
    }

    #[inline]
    fn process_memcache_ascii_from<C: Cache>(&mut self, buf: &mut dyn RecvBuf, cache: &C) {
        loop {
            // Check for empty buffer
            if buf.is_empty() {
                break;
            }

            // Backpressure: stop processing if write buffer is too large
            if self.pending_write_len() > Self::MAX_PENDING_WRITE {
                break;
            }

            // Check if we're in the middle of receiving a large value
            if self.continue_memcache_ascii_streaming_recv(buf, cache) {
                continue;
            }

            // Normal command parsing with streaming support
            let data = buf.as_slice();
            match parse_memcache_streaming(
                data,
                &self.memcache_parse_options,
                MEMCACHE_STREAMING_THRESHOLD,
            ) {
                Ok(MemcacheParseProgress::Complete(cmd, consumed)) => {
                    // Intercept GET for zero-copy path
                    if let protocol_memcache::Command::Get { key } = cmd {
                        use crate::metrics::{GETS, HITS, MISSES};
                        GETS.increment();
                        if let Some(value_ref) = cache.get_value_ref(key) {
                            HITS.increment();
                            // Header: VALUE <key> 0 <len>\r\n
                            self.write_buf.extend_from_slice(b"VALUE ");
                            self.write_buf.extend_from_slice(key);
                            self.write_buf.extend_from_slice(b" 0 ");
                            let mut len_buf = itoa::Buffer::new();
                            self.write_buf
                                .extend_from_slice(len_buf.format(value_ref.len()).as_bytes());
                            self.write_buf.extend_from_slice(b"\r\n");
                            // Queue value with trailer \r\nEND\r\n
                            self.queue_zero_copy_value(value_ref, b"\r\nEND\r\n");
                        } else {
                            MISSES.increment();
                            self.write_buf.extend_from_slice(b"END\r\n");
                        }
                        buf.consume(consumed);
                        continue;
                    }

                    if execute_memcache(&cmd, cache, &mut self.write_buf, self.allow_flush) {
                        self.should_close = true;
                    }
                    buf.consume(consumed);
                }
                Ok(MemcacheParseProgress::NeedValue {
                    header,
                    value_len,
                    value_prefix,
                    header_consumed,
                }) => {
                    // Extract values before consuming buffer
                    let key = header.key.to_vec();
                    let prefix_len = value_prefix.len();
                    let noreply = header.noreply;
                    let ttl = if header.exptime == 0 {
                        None
                    } else if header.exptime <= 60 * 60 * 24 * 30 {
                        // Seconds from now (up to 30 days)
                        Some(Duration::from_secs(header.exptime as u64))
                    } else {
                        // Unix timestamp - calculate duration from now
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        if header.exptime as u64 > now {
                            Some(Duration::from_secs(header.exptime as u64 - now))
                        } else {
                            Some(Duration::from_secs(1)) // Already expired, minimal TTL
                        }
                    };

                    // Try segment-based reservation first (true zero-copy)
                    match cache.begin_segment_set(&key, value_len, ttl) {
                        Ok(mut reservation) => {
                            // Copy any value bytes already in the buffer
                            if prefix_len > 0 {
                                reservation.value_mut()[..prefix_len].copy_from_slice(value_prefix);
                            }

                            // Consume the header + prefix
                            buf.consume(header_consumed + prefix_len);

                            // Transition to segment-based streaming
                            self.streaming_state = StreamingState::MemcacheAsciiSegment {
                                reservation,
                                received: prefix_len,
                                noreply,
                            };
                        }
                        Err(CacheError::Unsupported) => {
                            // Fall back to Vec-based reservation
                            let mut reservation = SetReservation::new(
                                &key,
                                value_len,
                                &[],
                                ttl.unwrap_or(Duration::from_secs(3600)),
                            );

                            if prefix_len > 0 {
                                reservation.value_mut()[..prefix_len].copy_from_slice(value_prefix);
                            }

                            buf.consume(header_consumed + prefix_len);

                            self.streaming_state = StreamingState::MemcacheAsciiVec {
                                reservation,
                                received: prefix_len,
                                noreply,
                            };
                        }
                        Err(_e) => {
                            // Send error response
                            self.write_buf
                                .extend_from_slice(b"SERVER_ERROR out of memory\r\n");

                            // Consume the header + prefix we've already seen
                            buf.consume(header_consumed + prefix_len);

                            // Calculate remaining bytes to drain: rest of value + CRLF
                            let remaining_value = value_len - prefix_len;
                            let remaining_to_drain = remaining_value + 2; // +2 for trailing CRLF

                            // Transition to draining state
                            self.streaming_state = StreamingState::Draining {
                                remaining: remaining_to_drain,
                            };
                        }
                    }
                }
                Ok(MemcacheParseProgress::Incomplete) => break,
                Err(e) => {
                    PROTOCOL_ERRORS.increment();
                    self.write_buf.extend_from_slice(b"ERROR ");
                    self.write_buf.extend_from_slice(e.to_string().as_bytes());
                    self.write_buf.extend_from_slice(b"\r\n");
                    self.streaming_state = StreamingState::None;
                    let len = buf.len();
                    buf.consume(len);
                    break;
                }
            }
        }
    }

    /// Continue receiving value data for a memcache ASCII streaming SET.
    #[inline]
    fn continue_memcache_ascii_streaming_recv<C: Cache>(
        &mut self,
        buf: &mut dyn RecvBuf,
        cache: &C,
    ) -> bool {
        match &mut self.streaming_state {
            StreamingState::MemcacheAsciiSegment {
                reservation,
                received,
                noreply,
            } => {
                let value_len = reservation.value_len();
                let remaining = value_len - *received;
                let data = buf.as_slice();

                if data.len() < remaining {
                    // Not enough data yet - copy what we have
                    let to_copy = data.len();
                    reservation.value_mut()[*received..*received + to_copy]
                        .copy_from_slice(&data[..to_copy]);
                    *received += to_copy;
                    buf.consume(to_copy);
                    return true;
                }

                // We have all the value bytes
                reservation.value_mut()[*received..value_len].copy_from_slice(&data[..remaining]);
                buf.consume(remaining);

                // Check for trailing CRLF
                let trailing = buf.as_slice();
                if trailing.len() < 2 {
                    *received = value_len;
                    return true;
                }

                if &trailing[..2] != b"\r\n" {
                    PROTOCOL_ERRORS.increment();
                    self.write_buf
                        .extend_from_slice(b"CLIENT_ERROR bad data chunk\r\n");
                    let state = std::mem::replace(&mut self.streaming_state, StreamingState::None);
                    if let StreamingState::MemcacheAsciiSegment { reservation, .. } = state {
                        cache.cancel_segment_set(reservation);
                    }
                    return true;
                }
                buf.consume(2);

                // Extract and commit
                let noreply_val = *noreply;
                let state = std::mem::replace(&mut self.streaming_state, StreamingState::None);
                if let StreamingState::MemcacheAsciiSegment { reservation, .. } = state {
                    if cache.commit_segment_set(reservation).is_ok() {
                        if !noreply_val {
                            self.write_buf.extend_from_slice(b"STORED\r\n");
                        }
                    } else if !noreply_val {
                        self.write_buf.extend_from_slice(b"NOT_STORED\r\n");
                    }
                }

                true
            }
            StreamingState::MemcacheAsciiVec {
                reservation,
                received,
                noreply,
            } => {
                let value_len = reservation.value_len();
                let remaining = value_len - *received;
                let data = buf.as_slice();

                if data.len() < remaining {
                    let to_copy = data.len();
                    reservation.value_mut()[*received..*received + to_copy]
                        .copy_from_slice(&data[..to_copy]);
                    *received += to_copy;
                    buf.consume(to_copy);
                    return true;
                }

                reservation.value_mut()[*received..value_len].copy_from_slice(&data[..remaining]);
                buf.consume(remaining);

                let trailing = buf.as_slice();
                if trailing.len() < 2 {
                    *received = value_len;
                    return true;
                }

                if &trailing[..2] != b"\r\n" {
                    PROTOCOL_ERRORS.increment();
                    self.write_buf
                        .extend_from_slice(b"CLIENT_ERROR bad data chunk\r\n");
                    self.streaming_state = StreamingState::None;
                    return true;
                }
                buf.consume(2);

                let noreply_val = *noreply;
                let state = std::mem::replace(&mut self.streaming_state, StreamingState::None);
                if let StreamingState::MemcacheAsciiVec { reservation, .. } = state {
                    if cache.commit_set(reservation).is_ok() {
                        if !noreply_val {
                            self.write_buf.extend_from_slice(b"STORED\r\n");
                        }
                    } else if !noreply_val {
                        self.write_buf.extend_from_slice(b"NOT_STORED\r\n");
                    }
                }

                true
            }
            _ => false,
        }
    }

    #[inline]
    fn process_memcache_binary_from<C: Cache>(&mut self, buf: &mut dyn RecvBuf, cache: &C) {
        loop {
            // Check for empty buffer
            if buf.is_empty() {
                break;
            }

            // Backpressure: stop processing if write buffer is too large
            if self.pending_write_len() > Self::MAX_PENDING_WRITE {
                break;
            }

            // Check if we're in the middle of receiving a large value
            if self.continue_memcache_binary_streaming_recv(buf, cache) {
                continue;
            }

            // Normal command parsing with streaming support
            let data = buf.as_slice();
            match parse_binary_streaming(data, BINARY_STREAMING_THRESHOLD) {
                Ok(BinaryParseProgress::Complete(cmd, consumed)) => {
                    // Intercept GET variants for zero-copy path
                    use protocol_memcache::binary::{
                        BinaryCommand, BinaryResponse, HEADER_SIZE, Opcode, ResponseHeader, Status,
                    };
                    match &cmd {
                        BinaryCommand::Get { key, opaque }
                        | BinaryCommand::GetK { key, opaque }
                        | BinaryCommand::GetQ { key, opaque }
                        | BinaryCommand::GetKQ { key, opaque } => {
                            use crate::metrics::{GETS, HITS, MISSES};
                            GETS.increment();
                            let is_getk = matches!(
                                cmd,
                                BinaryCommand::GetK { .. } | BinaryCommand::GetKQ { .. }
                            );
                            let is_quiet = matches!(
                                cmd,
                                BinaryCommand::GetQ { .. } | BinaryCommand::GetKQ { .. }
                            );
                            let opaque = *opaque;

                            if let Some(value_ref) = cache.get_value_ref(key) {
                                HITS.increment();
                                let opcode = match &cmd {
                                    BinaryCommand::Get { .. } => Opcode::Get,
                                    BinaryCommand::GetK { .. } => Opcode::GetK,
                                    BinaryCommand::GetQ { .. } => Opcode::GetQ,
                                    BinaryCommand::GetKQ { .. } => Opcode::GetKQ,
                                    _ => unreachable!(),
                                };
                                let extras_len: usize = 4;
                                let key_len = if is_getk { key.len() } else { 0 };
                                let total_body = extras_len + key_len + value_ref.len();

                                // Write response header + extras + key into write_buf
                                let header_total = HEADER_SIZE + extras_len + key_len;
                                let start = self.write_buf.len();
                                self.write_buf.reserve(header_total);
                                // Safety: we just reserved enough capacity
                                unsafe {
                                    self.write_buf.set_len(start + header_total);
                                }
                                let mut header = ResponseHeader::new(opcode, Status::NoError);
                                header.extras_length = extras_len as u8;
                                if is_getk {
                                    header.key_length = key.len() as u16;
                                }
                                header.total_body_length = total_body as u32;
                                header.opaque = opaque;
                                header.encode(&mut self.write_buf[start..]);
                                // Flags (always 0)
                                self.write_buf[start + HEADER_SIZE..start + HEADER_SIZE + 4]
                                    .copy_from_slice(&0u32.to_be_bytes());
                                // Key (for GetK/GetKQ only)
                                if is_getk {
                                    let key_start = start + HEADER_SIZE + extras_len;
                                    self.write_buf[key_start..key_start + key.len()]
                                        .copy_from_slice(key);
                                }
                                // Queue value (no trailer for binary protocol)
                                self.queue_zero_copy_value(value_ref, b"");
                            } else {
                                MISSES.increment();
                                if !is_quiet {
                                    let start = self.write_buf.len();
                                    self.write_buf.reserve(HEADER_SIZE + 32);
                                    unsafe {
                                        self.write_buf.set_len(start + HEADER_SIZE + 32);
                                    }
                                    let len = BinaryResponse::encode_not_found(
                                        &mut self.write_buf[start..],
                                        Opcode::Get,
                                        opaque,
                                    );
                                    self.write_buf.truncate(start + len);
                                }
                            }
                            buf.consume(consumed);
                            continue;
                        }
                        _ => {}
                    }

                    if execute_memcache_binary(&cmd, cache, &mut self.write_buf, self.allow_flush) {
                        self.should_close = true;
                    }
                    buf.consume(consumed);
                }
                Ok(BinaryParseProgress::NeedValue {
                    header,
                    value_len,
                    value_prefix,
                    total_consumed,
                }) => {
                    // Extract values before consuming buffer
                    let key = header.key.to_vec();
                    let prefix_len = value_prefix.len();
                    let opcode = header.opcode;
                    let opaque = header.opaque;
                    let ttl = if header.expiration == 0 {
                        None
                    } else if header.expiration <= 60 * 60 * 24 * 30 {
                        Some(Duration::from_secs(header.expiration as u64))
                    } else {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        if header.expiration as u64 > now {
                            Some(Duration::from_secs(header.expiration as u64 - now))
                        } else {
                            Some(Duration::from_secs(1))
                        }
                    };

                    // Calculate how much of the header we've consumed
                    // total_consumed includes header + value, we only want header for now
                    let header_consumed = total_consumed - value_len;

                    match cache.begin_segment_set(&key, value_len, ttl) {
                        Ok(mut reservation) => {
                            if prefix_len > 0 {
                                reservation.value_mut()[..prefix_len].copy_from_slice(value_prefix);
                            }

                            buf.consume(header_consumed + prefix_len);

                            self.streaming_state = StreamingState::MemcacheBinarySegment {
                                reservation,
                                received: prefix_len,
                                opcode,
                                opaque,
                            };
                        }
                        Err(CacheError::Unsupported) => {
                            let mut reservation = SetReservation::new(
                                &key,
                                value_len,
                                &[],
                                ttl.unwrap_or(Duration::from_secs(3600)),
                            );

                            if prefix_len > 0 {
                                reservation.value_mut()[..prefix_len].copy_from_slice(value_prefix);
                            }

                            buf.consume(header_consumed + prefix_len);

                            self.streaming_state = StreamingState::MemcacheBinaryVec {
                                reservation,
                                received: prefix_len,
                                opcode,
                                opaque,
                            };
                        }
                        Err(_e) => {
                            // Binary protocol error response
                            use protocol_memcache::binary::{BinaryResponse, Status};
                            let start = self.write_buf.len();
                            self.write_buf.reserve(32);
                            unsafe {
                                self.write_buf.set_len(start + 32);
                            }
                            let len = BinaryResponse::encode_error(
                                &mut self.write_buf[start..],
                                opcode,
                                opaque,
                                Status::OutOfMemory,
                            );
                            self.write_buf.truncate(start + len);

                            // Consume the header + prefix we've already seen
                            buf.consume(header_consumed + prefix_len);

                            // Calculate remaining bytes to drain (no CRLF for binary protocol)
                            let remaining_to_drain = value_len - prefix_len;

                            if remaining_to_drain > 0 {
                                // Transition to draining state
                                self.streaming_state = StreamingState::Draining {
                                    remaining: remaining_to_drain,
                                };
                            }
                        }
                    }
                }
                Ok(BinaryParseProgress::Incomplete) => break,
                Err(_) => {
                    PROTOCOL_ERRORS.increment();
                    self.should_close = true;
                    let len = buf.len();
                    buf.consume(len);
                    break;
                }
            }
        }
    }

    /// Continue receiving value data for a memcache binary streaming SET.
    #[inline]
    fn continue_memcache_binary_streaming_recv<C: Cache>(
        &mut self,
        buf: &mut dyn RecvBuf,
        cache: &C,
    ) -> bool {
        match &mut self.streaming_state {
            StreamingState::MemcacheBinarySegment {
                reservation,
                received,
                opcode,
                opaque,
            } => {
                let value_len = reservation.value_len();
                let remaining = value_len - *received;
                let data = buf.as_slice();

                if data.len() < remaining {
                    let to_copy = data.len();
                    reservation.value_mut()[*received..*received + to_copy]
                        .copy_from_slice(&data[..to_copy]);
                    *received += to_copy;
                    buf.consume(to_copy);
                    return true;
                }

                // We have all the value bytes (no trailing CRLF in binary protocol)
                reservation.value_mut()[*received..value_len].copy_from_slice(&data[..remaining]);
                buf.consume(remaining);

                // Extract and commit
                let opcode_val = *opcode;
                let opaque_val = *opaque;
                let state = std::mem::replace(&mut self.streaming_state, StreamingState::None);
                if let StreamingState::MemcacheBinarySegment { reservation, .. } = state {
                    use protocol_memcache::binary::BinaryResponse;
                    if cache.commit_segment_set(reservation).is_ok() {
                        if !opcode_val.is_quiet() {
                            let response_len = BinaryResponse::encode_stored(
                                &mut [0u8; 32],
                                opcode_val,
                                opaque_val,
                                0,
                            );
                            let start = self.write_buf.len();
                            self.write_buf.reserve(response_len);
                            unsafe {
                                self.write_buf.set_len(start + response_len);
                            }
                            BinaryResponse::encode_stored(
                                &mut self.write_buf[start..],
                                opcode_val,
                                opaque_val,
                                0,
                            );
                        }
                    } else if !opcode_val.is_quiet() {
                        use protocol_memcache::binary::Status;
                        let response_len = BinaryResponse::encode_error(
                            &mut [0u8; 32],
                            opcode_val,
                            opaque_val,
                            Status::ItemNotStored,
                        );
                        let start = self.write_buf.len();
                        self.write_buf.reserve(response_len);
                        unsafe {
                            self.write_buf.set_len(start + response_len);
                        }
                        BinaryResponse::encode_error(
                            &mut self.write_buf[start..],
                            opcode_val,
                            opaque_val,
                            Status::ItemNotStored,
                        );
                    }
                }

                true
            }
            StreamingState::MemcacheBinaryVec {
                reservation,
                received,
                opcode,
                opaque,
            } => {
                let value_len = reservation.value_len();
                let remaining = value_len - *received;
                let data = buf.as_slice();

                if data.len() < remaining {
                    let to_copy = data.len();
                    reservation.value_mut()[*received..*received + to_copy]
                        .copy_from_slice(&data[..to_copy]);
                    *received += to_copy;
                    buf.consume(to_copy);
                    return true;
                }

                reservation.value_mut()[*received..value_len].copy_from_slice(&data[..remaining]);
                buf.consume(remaining);

                let opcode_val = *opcode;
                let opaque_val = *opaque;
                let state = std::mem::replace(&mut self.streaming_state, StreamingState::None);
                if let StreamingState::MemcacheBinaryVec { reservation, .. } = state {
                    use protocol_memcache::binary::BinaryResponse;
                    if cache.commit_set(reservation).is_ok() {
                        if !opcode_val.is_quiet() {
                            let response_len = BinaryResponse::encode_stored(
                                &mut [0u8; 32],
                                opcode_val,
                                opaque_val,
                                0,
                            );
                            let start = self.write_buf.len();
                            self.write_buf.reserve(response_len);
                            unsafe {
                                self.write_buf.set_len(start + response_len);
                            }
                            BinaryResponse::encode_stored(
                                &mut self.write_buf[start..],
                                opcode_val,
                                opaque_val,
                                0,
                            );
                        }
                    } else if !opcode_val.is_quiet() {
                        use protocol_memcache::binary::Status;
                        let response_len = BinaryResponse::encode_error(
                            &mut [0u8; 32],
                            opcode_val,
                            opaque_val,
                            Status::ItemNotStored,
                        );
                        let start = self.write_buf.len();
                        self.write_buf.reserve(response_len);
                        unsafe {
                            self.write_buf.set_len(start + response_len);
                        }
                        BinaryResponse::encode_error(
                            &mut self.write_buf[start..],
                            opcode_val,
                            opaque_val,
                            Status::ItemNotStored,
                        );
                    }
                }

                true
            }
            _ => false,
        }
    }

    #[inline]
    pub fn has_pending_write(&self) -> bool {
        !self.send_queue.is_empty() || self.write_pos < self.write_buf.len()
    }

    #[inline]
    pub fn pending_write_data(&self) -> &[u8] {
        if let Some(front) = self.send_queue.front() {
            &front[self.send_offset..]
        } else {
            &self.write_buf[self.write_pos..]
        }
    }

    /// Collect all pending write data as owned `Bytes` for vectored send.
    ///
    /// Returns cloned `Bytes` for send queue entries (cheap Arc increment)
    /// and copies the write_buf tail. The `Connection` retains its own
    /// state so `advance_write()` can track progress on partial sends.
    pub fn collect_pending_writes(&self) -> Vec<Bytes> {
        let mut bufs = Vec::with_capacity(self.send_queue.len() + 1);

        for (i, entry) in self.send_queue.iter().enumerate() {
            if i == 0 && self.send_offset > 0 {
                bufs.push(entry.slice(self.send_offset..));
            } else {
                bufs.push(entry.clone());
            }
        }

        if self.write_pos < self.write_buf.len() {
            bufs.push(Bytes::copy_from_slice(&self.write_buf[self.write_pos..]));
        }

        bufs
    }

    #[inline]
    pub fn advance_write(&mut self, n: usize) {
        if self.send_queue.is_empty() {
            // No queue entries — advance through write_buf directly
            self.write_pos += n;
            if self.write_pos >= self.write_buf.len() {
                self.write_buf.clear();
                self.write_pos = 0;
            }
            return;
        }

        // Advance through send queue entries
        let mut remaining = n;
        while remaining > 0 {
            if let Some(front) = self.send_queue.front() {
                let avail = front.len() - self.send_offset;
                if remaining < avail {
                    self.send_offset += remaining;
                    self.send_queue_bytes -= remaining;
                    return;
                }
                // Completed this queue entry
                remaining -= avail;
                self.send_queue_bytes -= avail;
                self.send_offset = 0;
                self.send_queue.pop_front();
            } else {
                // Queue exhausted, advance through write_buf
                self.write_pos += remaining;
                if self.write_pos >= self.write_buf.len() {
                    self.write_buf.clear();
                    self.write_pos = 0;
                }
                return;
            }
        }
    }

    #[inline]
    pub fn should_close(&self) -> bool {
        self.should_close
    }

    /// Flush unsent `write_buf` contents into the send queue as a frozen `Bytes` entry.
    ///
    /// This is called before pushing a zero-copy value into the queue to preserve
    /// pipeline ordering: all preceding small responses (already in `write_buf`)
    /// must be sent before the zero-copy value.
    #[inline]
    fn flush_write_buf_to_queue(&mut self) {
        // Discard the already-sent prefix
        if self.write_pos > 0 {
            let _ = self.write_buf.split_to(self.write_pos);
            self.write_pos = 0;
        }

        // Freeze remaining unsent data into a queue entry
        if !self.write_buf.is_empty() {
            let frozen = self.write_buf.split().freeze();
            self.send_queue_bytes += frozen.len();
            self.send_queue.push_back(frozen);
            // write_buf is now empty (split() leaves it with 0 len but keeps capacity)
        }
    }

    /// Queue a value for sending, using zero-copy for large values.
    ///
    /// The caller must have already written the protocol-specific header
    /// into `write_buf` before calling this method.
    ///
    /// For large values (>= ZERO_COPY_MIN_VALUE_SIZE): flushes write_buf
    /// (including header) to the send queue, pushes the value zero-copy,
    /// then writes the trailer to write_buf.
    ///
    /// For small values: copies value + trailer directly into write_buf.
    #[inline]
    fn queue_zero_copy_value(&mut self, value_ref: ValueRef, trailer: &[u8]) {
        if value_ref.len() < ZERO_COPY_MIN_VALUE_SIZE {
            self.write_buf.extend_from_slice(value_ref.as_slice());
            self.write_buf.extend_from_slice(trailer);
            return;
        }

        // Flush write_buf (including the header the caller just wrote) to the queue
        self.flush_write_buf_to_queue();

        // Push the value directly into the queue (zero-copy)
        let value_bytes = value_ref.into_bytes();
        self.send_queue_bytes += value_bytes.len();
        self.send_queue.push_back(value_bytes);

        // Write the trailer into the fresh write_buf
        self.write_buf.extend_from_slice(trailer);
    }
}

impl Default for Connection {
    fn default() -> Self {
        // Use 1MB as the default max value size (same as server default)
        Self::new(1024 * 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A simple mock cache for testing
    struct MockCache;

    impl Cache for MockCache {
        fn get(&self, _key: &[u8]) -> Option<cache_core::OwnedGuard> {
            None
        }

        fn with_value<F, R>(&self, _key: &[u8], _f: F) -> Option<R>
        where
            F: FnOnce(&[u8]) -> R,
        {
            None
        }

        fn get_value_ref(&self, _key: &[u8]) -> Option<cache_core::ValueRef> {
            None
        }

        fn set(
            &self,
            _key: &[u8],
            _value: &[u8],
            _ttl: Option<std::time::Duration>,
        ) -> Result<(), cache_core::CacheError> {
            Ok(())
        }

        fn delete(&self, _key: &[u8]) -> bool {
            false
        }

        fn contains(&self, _key: &[u8]) -> bool {
            false
        }

        fn flush(&self) {}
    }

    /// A simple test buffer that implements RecvBuf
    struct TestRecvBuf {
        data: Vec<u8>,
        offset: usize,
    }

    impl TestRecvBuf {
        fn new(data: &[u8]) -> Self {
            Self {
                data: data.to_vec(),
                offset: 0,
            }
        }

        fn append(&mut self, data: &[u8]) {
            self.data.extend_from_slice(data);
        }
    }

    impl RecvBuf for TestRecvBuf {
        fn as_slice(&self) -> &[u8] {
            &self.data[self.offset..]
        }

        fn len(&self) -> usize {
            self.data.len() - self.offset
        }

        fn consume(&mut self, n: usize) {
            self.offset += n;
            // Compact when all data consumed
            if self.offset >= self.data.len() {
                self.data.clear();
                self.offset = 0;
            }
        }

        fn capacity(&self) -> usize {
            self.data.capacity()
        }

        fn shrink_if_oversized(&mut self) {
            // No-op for tests
        }
    }

    #[test]
    fn test_partial_request() {
        let cache = MockCache;
        let mut conn = Connection::default();

        // Send partial RESP command: "*2\r\n$3\r\nGET\r\n$3\r\nke"
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nke");
        conn.process_from(&mut buf, &cache);

        // Should have no response yet (incomplete request)
        assert!(!conn.has_pending_write());
        // Data should remain unconsumed
        assert_eq!(buf.as_slice(), b"*2\r\n$3\r\nGET\r\n$3\r\nke");

        // Complete the request: "y\r\n"
        buf.append(b"y\r\n");
        conn.process_from(&mut buf, &cache);

        // Now should have a response
        assert!(conn.has_pending_write());
        // Data should be consumed
        assert!(buf.as_slice().is_empty());
    }

    #[test]
    fn test_pipelined_requests() {
        let cache = MockCache;
        let mut conn = Connection::default();

        // Send two complete GET requests in one batch
        let mut buf =
            TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
        conn.process_from(&mut buf, &cache);

        // Should have responses for both
        assert!(conn.has_pending_write());
        let response = conn.pending_write_data();
        // Two $-1\r\n responses (null bulk string for cache miss)
        assert_eq!(response, b"$-1\r\n$-1\r\n");
        // All data consumed
        assert!(buf.as_slice().is_empty());
    }

    #[test]
    fn test_complete_plus_partial() {
        let cache = MockCache;
        let mut conn = Connection::default();

        // Send one complete request and start of another
        let mut buf =
            TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*2\r\n$3\r\nGET\r\n$3\r\nba");
        conn.process_from(&mut buf, &cache);

        // Should have response for first request
        assert!(conn.has_pending_write());
        let response = conn.pending_write_data();
        assert_eq!(response, b"$-1\r\n");
        // Partial request should remain
        assert_eq!(buf.as_slice(), b"*2\r\n$3\r\nGET\r\n$3\r\nba");

        // Complete the second request
        buf.append(b"r\r\n");
        conn.process_from(&mut buf, &cache);

        // Now should have both responses
        let response = conn.pending_write_data();
        assert_eq!(response, b"$-1\r\n$-1\r\n");
        assert!(buf.as_slice().is_empty());
    }

    #[test]
    fn test_partial_write_advance() {
        let cache = MockCache;
        let mut conn = Connection::default();

        // Generate a response
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process_from(&mut buf, &cache);

        // Simulate partial write (only 2 bytes sent)
        assert!(conn.has_pending_write());
        let pending = conn.pending_write_data().len();
        conn.advance_write(2);

        // Should still have pending data
        assert!(conn.has_pending_write());
        assert_eq!(conn.pending_write_data().len(), pending - 2);

        // Simulate rest of write
        conn.advance_write(pending - 2);
        assert!(!conn.has_pending_write());
    }

    #[test]
    fn test_write_buffer_cleared_on_full_send() {
        let cache = MockCache;
        let mut conn = Connection::default();

        // First request
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process_from(&mut buf, &cache);

        // "Send" all data
        let pending = conn.pending_write_data().len();
        conn.advance_write(pending);
        assert!(!conn.has_pending_write());

        // Second request - should work fine
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
        conn.process_from(&mut buf, &cache);

        // New response should be generated
        assert!(conn.has_pending_write());
        assert_eq!(conn.pending_write_data(), b"$-1\r\n");
    }

    #[test]
    fn test_write_buffer_not_cleared_on_partial_send() {
        let cache = MockCache;
        let mut conn = Connection::default();

        // First request
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process_from(&mut buf, &cache);

        // Partial send (only 2 bytes)
        conn.advance_write(2);

        // Second request arrives - write_buf should NOT be cleared
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
        conn.process_from(&mut buf, &cache);

        // Should have remaining from first response + second response
        assert!(conn.has_pending_write());
        // First response: $-1\r\n (5 bytes), sent 2, remaining 3
        // Second response: $-1\r\n (5 bytes)
        // Total: 8 bytes
        assert_eq!(conn.pending_write_data().len(), 8);
    }

    #[test]
    fn test_backpressure_stops_processing() {
        let cache = MockCache;
        let mut conn = Connection::default();

        // Generate enough response data to exceed MAX_PENDING_WRITE
        // Each GET response is "$-1\r\n" (5 bytes)
        // MAX_PENDING_WRITE is 256KB, so we need ~52,000 requests

        // First, send many requests at once
        let single_request = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let mut large_request = Vec::new();
        for _ in 0..60000 {
            large_request.extend_from_slice(single_request);
        }
        let mut buf = TestRecvBuf::new(&large_request);
        conn.process_from(&mut buf, &cache);

        // The write buffer should be capped at around MAX_PENDING_WRITE
        // (actually slightly more since we process one more after the check)
        let pending = conn.pending_write_data().len();
        assert!(
            pending <= Connection::MAX_PENDING_WRITE + 100,
            "pending write {} should be around MAX_PENDING_WRITE {}",
            pending,
            Connection::MAX_PENDING_WRITE
        );

        // There should still be unprocessed data in read buffer
        // (backpressure stopped processing)
        assert!(
            !buf.as_slice().is_empty(),
            "buffer should still have unprocessed requests"
        );

        // Now "send" all the data and process again
        conn.advance_write(pending);
        conn.process_from(&mut buf, &cache);

        // More data should have been processed
        assert!(conn.has_pending_write());
    }

    #[test]
    fn test_oversized_value_draining() {
        let cache = MockCache;
        // Create connection with small max value size (1KB)
        let mut conn = Connection::new(1024);

        // Send SET command with 2KB value (exceeds limit)
        // Format: *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$2048\r\n<2048 bytes of data>\r\n
        let header = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$2048\r\n";
        let mut data = header.to_vec();
        data.extend_from_slice(&[b'x'; 1000]); // First chunk: 1000 bytes of value

        let mut buf = TestRecvBuf::new(&data);
        conn.process_from(&mut buf, &cache);

        // Should have error response about value being too large
        assert!(conn.has_pending_write());
        let response = String::from_utf8_lossy(conn.pending_write_data());
        assert!(
            response.contains("value too large"),
            "expected 'value too large' error, got: {}",
            response
        );

        // Buffer should be consumed (header + 1000 bytes of value prefix)
        assert!(buf.as_slice().is_empty());

        // Now send the rest of the value + trailing CRLF
        // Remaining: 1048 bytes of value + 2 bytes CRLF = 1050 bytes
        let mut remaining = vec![b'x'; 1048];
        remaining.extend_from_slice(b"\r\n");
        buf.append(&remaining);
        conn.process_from(&mut buf, &cache);

        // All oversized data should be drained
        assert!(buf.as_slice().is_empty());

        // Clear the write buffer to test next request
        let pending = conn.pending_write_data().len();
        conn.advance_write(pending);

        // Now send a valid GET command - should work fine
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process_from(&mut buf, &cache);

        // Should have normal response (cache miss)
        assert!(conn.has_pending_write());
        assert_eq!(conn.pending_write_data(), b"$-1\r\n");
        assert!(buf.as_slice().is_empty());
    }

    #[test]
    fn test_oversized_value_draining_with_pipelined_request() {
        let cache = MockCache;
        // Create connection with small max value size (1KB)
        let mut conn = Connection::new(1024);

        // Send SET command with 2KB value followed by a GET command
        // This tests that after draining, the pipelined command is processed correctly
        let header = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$2048\r\n";
        let mut data = header.to_vec();
        data.extend_from_slice(&[b'x'; 2048]); // Full 2KB value
        data.extend_from_slice(b"\r\n"); // Trailing CRLF
        data.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"); // Pipelined GET

        let mut buf = TestRecvBuf::new(&data);
        conn.process_from(&mut buf, &cache);

        // Should have error response + GET response
        assert!(conn.has_pending_write());
        let response = String::from_utf8_lossy(conn.pending_write_data());
        assert!(
            response.contains("value too large"),
            "expected 'value too large' error, got: {}",
            response
        );
        // Should also have the cache miss response for GET
        assert!(
            response.ends_with("$-1\r\n"),
            "expected cache miss response, got: {}",
            response
        );

        // All data should be consumed
        assert!(buf.as_slice().is_empty());
    }

    // --- Zero-copy send queue tests ---

    /// A mock cache that returns a configurable value for any GET.
    /// Uses a leaked Box for the ref_count so the ValueRef is always valid.
    struct MockCacheWithValue {
        value: Vec<u8>,
        ref_count: &'static cache_core::sync::AtomicU32,
    }

    impl MockCacheWithValue {
        fn new(value: Vec<u8>) -> Self {
            let ref_count = Box::leak(Box::new(cache_core::sync::AtomicU32::new(1_000_000)));
            Self { value, ref_count }
        }
    }

    impl Cache for MockCacheWithValue {
        fn get(&self, _key: &[u8]) -> Option<cache_core::OwnedGuard> {
            Some(cache_core::OwnedGuard::new(self.value.clone()))
        }

        fn with_value<F, R>(&self, _key: &[u8], f: F) -> Option<R>
        where
            F: FnOnce(&[u8]) -> R,
        {
            Some(f(&self.value))
        }

        fn get_value_ref(&self, _key: &[u8]) -> Option<cache_core::ValueRef> {
            // Increment ref_count for each ValueRef created
            self.ref_count
                .fetch_add(1, cache_core::sync::Ordering::Relaxed);
            Some(unsafe {
                cache_core::ValueRef::new(
                    self.ref_count as *const cache_core::sync::AtomicU32,
                    self.value.as_ptr(),
                    self.value.len(),
                    std::ptr::null(),
                    std::ptr::null(),
                    0,
                )
            })
        }

        fn set(
            &self,
            _key: &[u8],
            _value: &[u8],
            _ttl: Option<std::time::Duration>,
        ) -> Result<(), cache_core::CacheError> {
            Ok(())
        }

        fn delete(&self, _key: &[u8]) -> bool {
            false
        }

        fn contains(&self, _key: &[u8]) -> bool {
            true
        }

        fn flush(&self) {}
    }

    /// Helper to drain all pending data from a connection into a Vec.
    fn drain_all_pending(conn: &mut Connection) -> Vec<u8> {
        let mut result = Vec::new();
        while conn.has_pending_write() {
            let data = conn.pending_write_data();
            result.extend_from_slice(data);
            let len = data.len();
            conn.advance_write(len);
        }
        result
    }

    #[test]
    fn test_zero_copy_queue_large_value() {
        // Value >= ZERO_COPY_MIN_VALUE_SIZE should use the send queue
        let value = vec![b'x'; 2048];
        let cache = MockCacheWithValue::new(value.clone());
        let mut conn = Connection::default();

        // process_from now handles zero-copy internally
        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process_from(&mut buf, &cache);

        // Verify the send queue is used (not empty)
        assert!(
            !conn.send_queue.is_empty(),
            "large value should use send queue"
        );

        // Drain all data and verify correctness
        let output = drain_all_pending(&mut conn);

        // Expected: $2048\r\n<2048 bytes>\r\n
        let expected_header = b"$2048\r\n";
        assert!(
            output.starts_with(expected_header),
            "output should start with RESP header"
        );
        assert_eq!(
            &output[expected_header.len()..expected_header.len() + 2048],
            &value[..],
            "output should contain the value"
        );
        assert!(
            output.ends_with(b"\r\n"),
            "output should end with CRLF trailer"
        );
        assert_eq!(output.len(), expected_header.len() + 2048 + 2);
    }

    #[test]
    fn test_zero_copy_queue_small_value_bypasses_queue() {
        // Value < ZERO_COPY_MIN_VALUE_SIZE should go directly into write_buf
        let value = vec![b'y'; 512];
        let cache = MockCacheWithValue::new(value.clone());
        let mut conn = Connection::default();

        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process_from(&mut buf, &cache);

        // Send queue should be empty for small values
        assert!(
            conn.send_queue.is_empty(),
            "small value should NOT use send queue"
        );

        // But data should be in write_buf
        assert!(conn.has_pending_write());

        let output = drain_all_pending(&mut conn);
        let expected_header = b"$512\r\n";
        assert!(output.starts_with(expected_header));
        assert_eq!(
            &output[expected_header.len()..expected_header.len() + 512],
            &value[..]
        );
        assert!(output.ends_with(b"\r\n"));
    }

    #[test]
    fn test_zero_copy_interleaved_small_and_large() {
        // Pipeline: PING, large GET hit, PING — all in one buffer
        // Tests that pipeline ordering is preserved through the send queue
        let value = vec![b'z'; 4096];
        let cache = MockCacheWithValue::new(value.clone());
        let mut conn = Connection::default();

        // All three commands in a single pipeline batch
        let mut buf = TestRecvBuf::new(
            b"*1\r\n$4\r\nPING\r\n\
              *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n\
              *1\r\n$4\r\nPING\r\n",
        );
        conn.process_from(&mut buf, &cache);

        // Drain everything and check ordering:
        // 1. +PONG\r\n (flushed to queue before large value)
        // 2. $4096\r\n (header, also flushed to queue)
        // 3. <4096 bytes> (zero-copy value in queue)
        // 4. \r\n+PONG\r\n (trailer + second PONG in write_buf)
        let output = drain_all_pending(&mut conn);

        let expected_prefix = b"+PONG\r\n$4096\r\n";
        assert!(
            output.starts_with(expected_prefix),
            "output should start with first PONG + RESP header, got: {:?}",
            String::from_utf8_lossy(&output[..std::cmp::min(30, output.len())])
        );

        let value_start = expected_prefix.len();
        let value_end = value_start + 4096;
        assert_eq!(
            &output[value_start..value_end],
            &value[..],
            "value should follow header"
        );

        let trailer_and_pong = b"\r\n+PONG\r\n";
        assert!(
            output.ends_with(trailer_and_pong),
            "output should end with trailer + second PONG, got: {:?}",
            String::from_utf8_lossy(&output[output.len().saturating_sub(20)..])
        );

        assert_eq!(
            output.len(),
            expected_prefix.len() + 4096 + trailer_and_pong.len()
        );
    }

    #[test]
    fn test_zero_copy_partial_write_through_queue() {
        // Test partial writes that span queue entries
        let value = vec![b'A'; 2048];
        let cache = MockCacheWithValue::new(value.clone());
        let mut conn = Connection::default();

        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process_from(&mut buf, &cache);

        // Total expected: header + value + trailer
        let total_len = conn.pending_write_len();
        assert!(total_len > 2048);

        // Simulate partial writes: send 10 bytes at a time
        let mut sent = Vec::new();
        while conn.has_pending_write() {
            let data = conn.pending_write_data();
            let chunk_size = std::cmp::min(10, data.len());
            sent.extend_from_slice(&data[..chunk_size]);
            conn.advance_write(chunk_size);
        }

        // Verify the assembled output is correct
        assert_eq!(sent.len(), total_len);
        let expected_header = b"$2048\r\n";
        assert!(sent.starts_with(expected_header));
        assert_eq!(
            &sent[expected_header.len()..expected_header.len() + 2048],
            &value[..]
        );
        assert!(sent.ends_with(b"\r\n"));
    }

    #[test]
    fn test_zero_copy_backpressure_includes_queue_bytes() {
        // Verify that pending_write_len includes send_queue_bytes for backpressure
        let value = vec![b'B'; 4096];
        let cache = MockCacheWithValue::new(value);
        let mut conn = Connection::default();

        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process_from(&mut buf, &cache);

        // pending_write_len should include the queue bytes
        let pending = conn.pending_write_len();
        assert!(
            pending >= 4096,
            "pending_write_len {} should include value size 4096",
            pending
        );

        // should_read should respect the total pending
        // With 4096 bytes it should still be readable (under 256KB threshold)
        assert!(conn.should_read());
    }

    #[test]
    fn test_zero_copy_queue_empty_after_drain() {
        let value = vec![b'C'; 2048];
        let cache = MockCacheWithValue::new(value);
        let mut conn = Connection::default();

        let mut buf = TestRecvBuf::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        conn.process_from(&mut buf, &cache);

        assert!(!conn.send_queue.is_empty());

        // Drain everything
        drain_all_pending(&mut conn);

        // Queue and write_buf should be clean
        assert!(conn.send_queue.is_empty());
        assert_eq!(conn.send_queue_bytes, 0);
        assert!(!conn.has_pending_write());
    }

    // --- Memcache ASCII zero-copy tests ---

    #[test]
    fn test_memcache_ascii_get_hit_large_value() {
        let value = vec![b'M'; 2048];
        let cache = MockCacheWithValue::new(value.clone());
        let mut conn = Connection::default();

        let mut buf = TestRecvBuf::new(b"get foo\r\n");
        conn.process_from(&mut buf, &cache);

        // Large value should use the send queue
        assert!(
            !conn.send_queue.is_empty(),
            "memcache ASCII large value should use send queue"
        );

        let output = drain_all_pending(&mut conn);
        // Expected: VALUE foo 0 2048\r\n<2048 bytes>\r\nEND\r\n
        let expected_header = b"VALUE foo 0 2048\r\n";
        assert!(
            output.starts_with(expected_header),
            "output should start with VALUE header, got: {:?}",
            String::from_utf8_lossy(&output[..std::cmp::min(30, output.len())])
        );
        assert_eq!(
            &output[expected_header.len()..expected_header.len() + 2048],
            &value[..],
            "output should contain the value"
        );
        assert!(
            output.ends_with(b"\r\nEND\r\n"),
            "output should end with \\r\\nEND\\r\\n"
        );
        assert_eq!(
            output.len(),
            expected_header.len() + 2048 + b"\r\nEND\r\n".len()
        );
    }

    #[test]
    fn test_memcache_ascii_get_hit_small_value() {
        let value = vec![b'S'; 512];
        let cache = MockCacheWithValue::new(value.clone());
        let mut conn = Connection::default();

        let mut buf = TestRecvBuf::new(b"get foo\r\n");
        conn.process_from(&mut buf, &cache);

        // Small value should NOT use the send queue
        assert!(
            conn.send_queue.is_empty(),
            "memcache ASCII small value should NOT use send queue"
        );
        assert!(conn.has_pending_write());

        let output = drain_all_pending(&mut conn);
        let expected_header = b"VALUE foo 0 512\r\n";
        assert!(output.starts_with(expected_header));
        assert_eq!(
            &output[expected_header.len()..expected_header.len() + 512],
            &value[..]
        );
        assert!(output.ends_with(b"\r\nEND\r\n"));
    }

    #[test]
    fn test_memcache_ascii_get_miss() {
        let cache = MockCache;
        let mut conn = Connection::default();

        let mut buf = TestRecvBuf::new(b"get foo\r\n");
        conn.process_from(&mut buf, &cache);

        let output = drain_all_pending(&mut conn);
        assert_eq!(output, b"END\r\n");
    }

    // --- Memcache binary zero-copy tests ---

    #[test]
    fn test_memcache_binary_get_hit_large_value() {
        use protocol_memcache::binary::{BinaryRequest, HEADER_SIZE, ParsedBinaryResponse};

        let value = vec![b'B'; 2048];
        let cache = MockCacheWithValue::new(value.clone());
        let mut conn = Connection::default();

        // Encode a binary GET request
        let mut req_buf = [0u8; 256];
        let req_len = BinaryRequest::encode_get(&mut req_buf, b"foo", 42);

        let mut buf = TestRecvBuf::new(&req_buf[..req_len]);
        conn.process_from(&mut buf, &cache);

        // Large value should use the send queue
        assert!(
            !conn.send_queue.is_empty(),
            "memcache binary large value should use send queue"
        );

        let output = drain_all_pending(&mut conn);

        // Parse the response to verify correctness
        let (resp, consumed) = ParsedBinaryResponse::parse(&output).unwrap();
        assert_eq!(consumed, output.len());
        if let ParsedBinaryResponse::Value {
            opaque,
            flags,
            value: resp_value,
            ..
        } = resp
        {
            assert_eq!(opaque, 42);
            assert_eq!(flags, 0);
            assert_eq!(resp_value, &value[..]);
        } else {
            panic!("Expected Value response, got: {:?}", resp);
        }

        // Verify total size: header(24) + extras(4) + value(2048)
        assert_eq!(output.len(), HEADER_SIZE + 4 + 2048);
    }

    #[test]
    fn test_memcache_binary_get_hit_small_value() {
        use protocol_memcache::binary::{BinaryRequest, ParsedBinaryResponse};

        let value = vec![b'S'; 512];
        let cache = MockCacheWithValue::new(value.clone());
        let mut conn = Connection::default();

        let mut req_buf = [0u8; 256];
        let req_len = BinaryRequest::encode_get(&mut req_buf, b"foo", 42);

        let mut buf = TestRecvBuf::new(&req_buf[..req_len]);
        conn.process_from(&mut buf, &cache);

        // Small value should NOT use the send queue
        assert!(
            conn.send_queue.is_empty(),
            "memcache binary small value should NOT use send queue"
        );

        let output = drain_all_pending(&mut conn);
        let (resp, _) = ParsedBinaryResponse::parse(&output).unwrap();
        if let ParsedBinaryResponse::Value {
            opaque,
            value: resp_value,
            ..
        } = resp
        {
            assert_eq!(opaque, 42);
            assert_eq!(resp_value, &value[..]);
        } else {
            panic!("Expected Value response");
        }
    }

    #[test]
    fn test_memcache_binary_get_miss() {
        use protocol_memcache::binary::{BinaryRequest, ParsedBinaryResponse, Status};

        let cache = MockCache;
        let mut conn = Connection::default();

        let mut req_buf = [0u8; 256];
        let req_len = BinaryRequest::encode_get(&mut req_buf, b"foo", 42);

        let mut buf = TestRecvBuf::new(&req_buf[..req_len]);
        conn.process_from(&mut buf, &cache);

        let output = drain_all_pending(&mut conn);
        let (resp, _) = ParsedBinaryResponse::parse(&output).unwrap();
        assert!(
            matches!(
                resp,
                ParsedBinaryResponse::Error {
                    status: Status::KeyNotFound,
                    opaque: 42,
                    ..
                }
            ),
            "expected KeyNotFound error, got: {:?}",
            resp
        );
    }

    #[test]
    fn test_memcache_binary_getq_miss_silent() {
        use protocol_memcache::binary::BinaryRequest;

        let cache = MockCache;
        let mut conn = Connection::default();

        // Encode a quiet GET request
        let mut req_buf = [0u8; 256];
        let req_len = BinaryRequest::encode_getq(&mut req_buf, b"foo", 42);

        let mut buf = TestRecvBuf::new(&req_buf[..req_len]);
        conn.process_from(&mut buf, &cache);

        // GetQ miss should produce no response
        assert!(!conn.has_pending_write(), "GetQ miss should be silent");
    }

    #[test]
    fn test_memcache_binary_getk_hit_large_value() {
        use protocol_memcache::binary::{BinaryRequest, ParsedBinaryResponse};

        let value = vec![b'K'; 2048];
        let cache = MockCacheWithValue::new(value.clone());
        let mut conn = Connection::default();

        let mut req_buf = [0u8; 256];
        let req_len = BinaryRequest::encode_getk(&mut req_buf, b"foo", 42);

        let mut buf = TestRecvBuf::new(&req_buf[..req_len]);
        conn.process_from(&mut buf, &cache);

        assert!(
            !conn.send_queue.is_empty(),
            "binary GetK large value should use send queue"
        );

        let output = drain_all_pending(&mut conn);
        let (resp, _) = ParsedBinaryResponse::parse(&output).unwrap();
        if let ParsedBinaryResponse::Value {
            opaque,
            key,
            value: resp_value,
            ..
        } = resp
        {
            assert_eq!(opaque, 42);
            assert_eq!(key, Some(b"foo".as_slice()));
            assert_eq!(resp_value, &value[..]);
        } else {
            panic!("Expected Value response with key");
        }
    }
}
