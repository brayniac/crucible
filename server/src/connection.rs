//! Per-connection state for the cache server.

use bytes::{Buf, BytesMut};
use cache_core::Cache;
use protocol_memcache::binary::{BinaryCommand, REQUEST_MAGIC};
use protocol_memcache::{Command as MemcacheCommand, ParseError as MemcacheParseError};
use protocol_resp::{Command as RespCommand, ParseError as RespParseError};

use crate::execute::{RespVersion, execute_memcache, execute_memcache_binary, execute_resp};
use crate::metrics::PROTOCOL_ERRORS;

/// Protocol type detected for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetectedProtocol {
    Unknown,
    Resp,
    MemcacheAscii,
    MemcacheBinary,
}

/// Per-connection state for the cache server.
pub struct Connection {
    read_buf: BytesMut,
    write_buf: BytesMut,
    write_pos: usize,
    protocol: DetectedProtocol,
    should_close: bool,
    resp_version: RespVersion,
}

impl Connection {
    pub fn new(read_buffer_size: usize) -> Self {
        Self {
            read_buf: BytesMut::with_capacity(read_buffer_size),
            write_buf: BytesMut::with_capacity(65536),
            write_pos: 0,
            protocol: DetectedProtocol::Unknown,
            should_close: false,
            resp_version: RespVersion::default(),
        }
    }

    /// Append received data to the read buffer.
    #[inline]
    pub fn append_recv_data(&mut self, data: &[u8]) {
        self.read_buf.extend_from_slice(data);
    }

    /// Detect protocol from the first byte of data.
    #[inline]
    fn detect_protocol(&mut self) {
        if self.protocol != DetectedProtocol::Unknown || self.read_buf.is_empty() {
            return;
        }

        match self.read_buf[0] {
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

    /// Process all complete commands in the read buffer.
    #[inline]
    pub fn process<C: Cache>(&mut self, cache: &C) {
        if self.write_pos >= self.write_buf.len() {
            self.write_buf.clear();
            self.write_pos = 0;
        }

        self.detect_protocol();

        match self.protocol {
            DetectedProtocol::Unknown => {}
            DetectedProtocol::Resp => self.process_resp(cache),
            DetectedProtocol::MemcacheAscii => self.process_memcache_ascii(cache),
            DetectedProtocol::MemcacheBinary => self.process_memcache_binary(cache),
        }
    }

    #[inline]
    fn process_resp<C: Cache>(&mut self, cache: &C) {
        loop {
            if self.read_buf.is_empty() {
                break;
            }

            match RespCommand::parse(&self.read_buf) {
                Ok((cmd, consumed)) => {
                    execute_resp(&cmd, cache, &mut self.write_buf, &mut self.resp_version);
                    self.read_buf.advance(consumed);
                }
                Err(RespParseError::Incomplete) => break,
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
                    self.read_buf.clear();
                    break;
                }
            }
        }
    }

    #[inline]
    fn process_memcache_ascii<C: Cache>(&mut self, cache: &C) {
        loop {
            if self.read_buf.is_empty() {
                break;
            }

            match MemcacheCommand::parse(&self.read_buf) {
                Ok((cmd, consumed)) => {
                    if execute_memcache(&cmd, cache, &mut self.write_buf) {
                        self.should_close = true;
                    }
                    self.read_buf.advance(consumed);
                }
                Err(MemcacheParseError::Incomplete) => break,
                Err(e) => {
                    PROTOCOL_ERRORS.increment();
                    self.write_buf.extend_from_slice(b"ERROR ");
                    self.write_buf.extend_from_slice(e.to_string().as_bytes());
                    self.write_buf.extend_from_slice(b"\r\n");
                    self.read_buf.clear();
                    break;
                }
            }
        }
    }

    #[inline]
    fn process_memcache_binary<C: Cache>(&mut self, cache: &C) {
        loop {
            if self.read_buf.is_empty() {
                break;
            }

            match BinaryCommand::parse(&self.read_buf) {
                Ok((cmd, consumed)) => {
                    if execute_memcache_binary(&cmd, cache, &mut self.write_buf) {
                        self.should_close = true;
                    }
                    self.read_buf.advance(consumed);
                }
                Err(MemcacheParseError::Incomplete) => break,
                Err(_) => {
                    PROTOCOL_ERRORS.increment();
                    self.should_close = true;
                    self.read_buf.clear();
                    break;
                }
            }
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
}
