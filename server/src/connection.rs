//! Per-connection state for the cache server.

use bytes::{Bytes, BytesMut};
use cache_core::{Cache, CacheError, SegmentReservation, SetReservation, ValueRef};
use io_driver::{RecvBuf, ZeroCopySend};
use protocol_memcache::binary::{
    BINARY_STREAMING_THRESHOLD, BinaryParseProgress, REQUEST_MAGIC, parse_binary_streaming,
};
use protocol_memcache::{
    ParseOptions as MemcacheParseOptions, ParseProgress as MemcacheParseProgress,
    STREAMING_THRESHOLD as MEMCACHE_STREAMING_THRESHOLD,
    parse_streaming as parse_memcache_streaming,
};
use protocol_resp::{
    Command as RespCommand, ParseError as RespParseError, ParseOptions, ParseProgress,
    STREAMING_THRESHOLD, parse_streaming,
};
use std::io::IoSlice;
use std::time::Duration;

use crate::config::ZeroCopyMode;
use crate::execute::{RespVersion, execute_memcache, execute_memcache_binary, execute_resp};
use crate::metrics::PROTOCOL_ERRORS;

/// RESP trailer constant for zero-copy responses.
const RESP_CRLF: &[u8] = b"\r\n";

/// A zero-copy GET response for vectored I/O.
///
/// This struct holds the header and value reference for a GET response.
/// The value reference points directly into cache segment memory, avoiding copies.
///
/// Size: 64 bytes (fits in SmallBox<S8> inline storage)
/// - header: 32 bytes
/// - header_len: 1 byte + 7 padding
/// - value_ref: 24 bytes
pub struct ZeroCopyResponse {
    /// RESP header: `$<len>\r\n` (max ~25 bytes)
    header: [u8; 32],
    /// Length of valid header data (max 25)
    header_len: u8,
    /// Reference to value in cache segment memory
    value_ref: ValueRef,
}

impl ZeroCopyResponse {
    /// Create a new zero-copy response for a RESP bulk string.
    #[inline]
    pub fn new_resp_bulk_string(value_ref: ValueRef) -> Self {
        let mut header = [0u8; 32];
        let mut pos = 0;

        // Write "$"
        header[pos] = b'$';
        pos += 1;

        // Write length using itoa
        let mut len_buf = itoa::Buffer::new();
        let len_str = len_buf.format(value_ref.len()).as_bytes();
        header[pos..pos + len_str.len()].copy_from_slice(len_str);
        pos += len_str.len();

        // Write "\r\n"
        header[pos] = b'\r';
        header[pos + 1] = b'\n';
        pos += 2;

        Self {
            header,
            header_len: pos as u8,
            value_ref,
        }
    }

    /// Get the header bytes.
    #[inline]
    pub fn header(&self) -> &[u8] {
        &self.header[..self.header_len as usize]
    }

    /// Get the value reference.
    #[inline]
    pub fn value(&self) -> &[u8] {
        self.value_ref.as_slice()
    }

    /// Get total response length.
    #[inline]
    pub fn total_len(&self) -> usize {
        self.header_len as usize + self.value_ref.len() + RESP_CRLF.len()
    }

    /// Create IoSlices for vectored I/O.
    #[inline]
    pub fn as_io_slices(&self) -> [IoSlice<'_>; 3] {
        [
            IoSlice::new(self.header()),
            IoSlice::new(self.value()),
            IoSlice::new(RESP_CRLF),
        ]
    }

    /// Convert to owned Bytes for true zero-copy scatter-gather I/O.
    ///
    /// This method takes ownership of the response and returns a Vec of owned Bytes:
    /// - Header: small copy (~25 bytes)
    /// - Value: zero-copy from segment memory (ValueRef wrapped in Bytes)
    /// - Trailer: static reference
    ///
    /// The segment ref count is held in the value Bytes until it's dropped.
    #[inline]
    pub fn into_owned_bytes(self) -> Vec<Bytes> {
        vec![
            Bytes::copy_from_slice(&self.header[..self.header_len as usize]),
            self.value_ref.into_bytes(),
            Bytes::from_static(RESP_CRLF),
        ]
    }
}

impl ZeroCopySend for ZeroCopyResponse {
    #[inline]
    fn io_slices(&self) -> Vec<IoSlice<'_>> {
        vec![
            IoSlice::new(self.header()),
            IoSlice::new(self.value()),
            IoSlice::new(RESP_CRLF),
        ]
    }

    #[inline]
    fn total_len(&self) -> usize {
        self.header_len as usize + self.value_ref.len() + RESP_CRLF.len()
    }

    #[inline]
    fn slice_count(&self) -> usize {
        3
    }
}

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

    /// Get the amount of pending write data.
    #[inline]
    pub fn pending_write_len(&self) -> usize {
        self.write_buf.len().saturating_sub(self.write_pos)
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
        // Clear write buffer if all data has been sent
        if self.write_pos >= self.write_buf.len() {
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
            if buf.len() == 0 {
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
                            buf.consume(header_consumed + prefix_len);
                            self.write_buf.extend_from_slice(b"-ERR ");
                            self.write_buf.extend_from_slice(e.to_string().as_bytes());
                            self.write_buf.extend_from_slice(b"\r\n");
                        }
                    }
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
            if buf.len() == 0 {
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
                            buf.consume(header_consumed + prefix_len);
                            self.write_buf
                                .extend_from_slice(b"SERVER_ERROR out of memory\r\n");
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
            if buf.len() == 0 {
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
                            buf.consume(header_consumed + prefix_len);
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
        self.write_pos < self.write_buf.len()
    }

    #[inline]
    pub fn pending_write_data(&self) -> &[u8] {
        &self.write_buf[self.write_pos..]
    }

    #[inline]
    pub fn advance_write(&mut self, n: usize) {
        self.write_pos += n;
    }

    #[inline]
    pub fn should_close(&self) -> bool {
        self.should_close
    }

    /// Process a single RESP command with zero-copy support.
    ///
    /// Returns `Some(ZeroCopyResponse)` if the command is a GET that resulted in a hit
    /// and zero-copy is enabled for this value size. The caller should send the response
    /// using vectored I/O.
    ///
    /// Returns `None` if:
    /// - The command is not a GET
    /// - The GET resulted in a miss
    /// - Zero-copy is disabled or the value is below threshold
    /// - There's no complete command in the buffer
    ///
    /// In these cases, check `has_pending_write()` for regular response data.
    #[inline]
    pub fn process_one_zero_copy<C: Cache>(
        &mut self,
        buf: &mut dyn RecvBuf,
        cache: &C,
        zero_copy_mode: ZeroCopyMode,
    ) -> Option<ZeroCopyResponse> {
        // Clear write buffer if all data has been sent
        if self.write_pos >= self.write_buf.len() {
            self.write_buf.clear();
            self.write_pos = 0;
        }

        // Detect protocol from first byte
        let data = buf.as_slice();
        if data.is_empty() {
            return None;
        }

        if self.protocol == DetectedProtocol::Unknown {
            self.detect_protocol(data[0]);
        }

        // Only RESP protocol supports zero-copy GET for now
        if self.protocol != DetectedProtocol::Resp {
            // Process normally for other protocols
            self.process_from(buf, cache);
            return None;
        }

        // Try to parse a RESP command
        match RespCommand::parse_with_options(data, &self.resp_parse_options) {
            Ok((cmd, consumed)) => {
                // Check if it's a GET command
                if let RespCommand::Get { key } = cmd {
                    use crate::metrics::{GETS, HITS, MISSES};
                    GETS.increment();

                    // Try to get value reference (single lookup)
                    if let Some(value_ref) = cache.get_value_ref(key) {
                        HITS.increment();

                        // Check if we should use zero-copy or copy path
                        if zero_copy_mode.should_use_zero_copy(value_ref.len()) {
                            buf.consume(consumed);
                            return Some(ZeroCopyResponse::new_resp_bulk_string(value_ref));
                        }

                        // Below threshold - copy from the value_ref we already have
                        let value = value_ref.as_ref();
                        let needed = 1 + 20 + 2 + value.len() + 2;
                        self.write_buf.reserve(needed);

                        let spare = self.write_buf.spare_capacity_mut();
                        let buf_slice = unsafe {
                            std::slice::from_raw_parts_mut(
                                spare.as_mut_ptr() as *mut u8,
                                spare.len(),
                            )
                        };
                        let written = unsafe { write_bulk_string(buf_slice, value) };
                        unsafe { self.write_buf.set_len(self.write_buf.len() + written) };
                    } else {
                        // Cache miss
                        MISSES.increment();
                        if self.resp_version == RespVersion::Resp3 {
                            self.write_buf.extend_from_slice(b"_\r\n");
                        } else {
                            self.write_buf.extend_from_slice(b"$-1\r\n");
                        }
                    }
                    buf.consume(consumed);
                } else {
                    // Not a GET - execute normally
                    execute_resp(
                        &cmd,
                        cache,
                        &mut self.write_buf,
                        &mut self.resp_version,
                        self.allow_flush,
                    );
                    buf.consume(consumed);
                }
                None
            }
            Err(RespParseError::Incomplete) => None,
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
                // Consume all remaining data on error
                let len = buf.len();
                buf.consume(len);
                None
            }
        }
    }
}

/// Write a RESP bulk string response directly to a buffer.
/// Returns the number of bytes written.
///
/// # Safety
/// Caller must ensure `buf` has enough capacity for the response:
/// at least 1 + 20 + 2 + value.len() + 2 bytes.
#[inline]
unsafe fn write_bulk_string(buf: &mut [u8], value: &[u8]) -> usize {
    let mut pos = 0;
    buf[pos] = b'$';
    pos += 1;

    // Write length using itoa
    let mut len_buf = itoa::Buffer::new();
    let len_str = len_buf.format(value.len()).as_bytes();
    // SAFETY: Caller guarantees buf has enough capacity
    unsafe {
        std::ptr::copy_nonoverlapping(len_str.as_ptr(), buf.as_mut_ptr().add(pos), len_str.len());
    }
    pos += len_str.len();

    buf[pos] = b'\r';
    buf[pos + 1] = b'\n';
    pos += 2;

    // SAFETY: Caller guarantees buf has enough capacity
    unsafe {
        std::ptr::copy_nonoverlapping(value.as_ptr(), buf.as_mut_ptr().add(pos), value.len());
    }
    pos += value.len();

    buf[pos] = b'\r';
    buf[pos + 1] = b'\n';
    pos += 2;

    pos
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
}
