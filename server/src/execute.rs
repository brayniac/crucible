//! Command execution - bridges protocol commands to cache operations.

use bytes::BytesMut;
use cache_core::Cache;
use protocol_memcache::Command as MemcacheCommand;
use protocol_memcache::binary::{BinaryCommand, BinaryResponse, Opcode};
use protocol_resp::Command as RespCommand;
use std::time::Duration;

use crate::metrics::{DELETES, FLUSHES, GETS, HITS, MISSES, SET_ERRORS, SETS};

/// RESP protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RespVersion {
    #[default]
    Resp2,
    Resp3,
}

/// Write a RESP bulk string response directly to spare capacity.
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
    unsafe {
        std::ptr::copy_nonoverlapping(len_str.as_ptr(), buf.as_mut_ptr().add(pos), len_str.len());
    }
    pos += len_str.len();

    buf[pos] = b'\r';
    buf[pos + 1] = b'\n';
    pos += 2;

    unsafe {
        std::ptr::copy_nonoverlapping(value.as_ptr(), buf.as_mut_ptr().add(pos), value.len());
    }
    pos += value.len();

    buf[pos] = b'\r';
    buf[pos + 1] = b'\n';
    pos += 2;

    pos
}

/// Execute a Redis RESP command against the cache.
#[inline]
pub fn execute_resp<C: Cache>(
    cmd: &RespCommand<'_>,
    cache: &C,
    write_buf: &mut BytesMut,
    version: &mut RespVersion,
    allow_flush: bool,
) {
    match cmd {
        RespCommand::Ping => {
            write_buf.extend_from_slice(b"+PONG\r\n");
        }
        RespCommand::Get { key } => {
            GETS.increment();
            let hit = cache.with_value(key, |value| {
                // Reserve: $ + max_len_digits(20) + \r\n + value + \r\n
                let needed = 1 + 20 + 2 + value.len() + 2;
                write_buf.reserve(needed);

                let spare = write_buf.spare_capacity_mut();
                let buf = unsafe {
                    std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, spare.len())
                };
                let written = unsafe { write_bulk_string(buf, value) };
                unsafe { write_buf.set_len(write_buf.len() + written) };
            });

            if hit.is_some() {
                HITS.increment();
            } else {
                MISSES.increment();
                if *version == RespVersion::Resp3 {
                    write_buf.extend_from_slice(b"_\r\n");
                } else {
                    write_buf.extend_from_slice(b"$-1\r\n");
                }
            }
        }
        RespCommand::Set {
            key,
            value,
            ex,
            px,
            nx,
            xx,
        } => {
            SETS.increment();

            // Handle TTL: EX (seconds) or PX (milliseconds)
            let ttl = if let Some(secs) = ex {
                Some(Duration::from_secs(*secs))
            } else if let Some(ms) = px {
                Some(Duration::from_millis(*ms))
            } else {
                None
            };

            // Determine operation type based on NX/XX flags
            let result = match (*nx, *xx) {
                (true, false) => cache.add(key, value, ttl),
                (false, true) => cache.replace(key, value, ttl),
                _ => {
                    // Default SET behavior (with retries)
                    let mut last_err = None;
                    for attempt in 0..10 {
                        match cache.set(key, value, ttl) {
                            Ok(()) => {
                                write_buf.extend_from_slice(b"+OK\r\n");
                                return;
                            }
                            Err(e) => {
                                last_err = Some(e);
                                if attempt < 9 {
                                    let delay = Duration::from_micros(100 << attempt);
                                    std::thread::sleep(delay);
                                }
                            }
                        }
                    }
                    Err(last_err.unwrap_or(cache_core::CacheError::OutOfMemory))
                }
            };

            match result {
                Ok(()) => {
                    write_buf.extend_from_slice(b"+OK\r\n");
                }
                Err(cache_core::CacheError::KeyExists)
                | Err(cache_core::CacheError::KeyNotFound) => {
                    // NX/XX condition not met - return nil
                    if *version == RespVersion::Resp3 {
                        write_buf.extend_from_slice(b"_\r\n");
                    } else {
                        write_buf.extend_from_slice(b"$-1\r\n");
                    }
                }
                Err(_) => {
                    SET_ERRORS.increment();
                    write_buf.extend_from_slice(b"-ERR cache full\r\n");
                }
            }
        }
        RespCommand::Del { key } => {
            DELETES.increment();
            let deleted = cache.delete(key);
            write_buf.extend_from_slice(if deleted { b":1\r\n" } else { b":0\r\n" });
        }
        RespCommand::MGet { keys } => {
            // Reserve for array header: * + max_len_digits + \r\n
            write_buf.reserve(1 + 20 + 2);
            let spare = write_buf.spare_capacity_mut();
            let buf = unsafe {
                std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, spare.len())
            };

            let mut pos = 0;
            buf[pos] = b'*';
            pos += 1;
            let mut len_buf = itoa::Buffer::new();
            let len_str = len_buf.format(keys.len()).as_bytes();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    len_str.as_ptr(),
                    buf.as_mut_ptr().add(pos),
                    len_str.len(),
                );
            }
            pos += len_str.len();
            buf[pos] = b'\r';
            buf[pos + 1] = b'\n';
            pos += 2;
            unsafe { write_buf.set_len(write_buf.len() + pos) };

            for key in keys {
                GETS.increment();
                let hit = cache.with_value(key, |value| {
                    let needed = 1 + 20 + 2 + value.len() + 2;
                    write_buf.reserve(needed);

                    let spare = write_buf.spare_capacity_mut();
                    let buf = unsafe {
                        std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, spare.len())
                    };
                    let written = unsafe { write_bulk_string(buf, value) };
                    unsafe { write_buf.set_len(write_buf.len() + written) };
                });

                if hit.is_some() {
                    HITS.increment();
                } else {
                    MISSES.increment();
                    if *version == RespVersion::Resp3 {
                        write_buf.extend_from_slice(b"_\r\n");
                    } else {
                        write_buf.extend_from_slice(b"$-1\r\n");
                    }
                }
            }
        }
        RespCommand::Config { subcommand, .. } => {
            if subcommand.eq_ignore_ascii_case(b"get") {
                write_buf.extend_from_slice(b"*0\r\n");
            } else if subcommand.eq_ignore_ascii_case(b"set")
                || subcommand.eq_ignore_ascii_case(b"resetstat")
            {
                write_buf.extend_from_slice(b"+OK\r\n");
            } else {
                write_buf.extend_from_slice(b"-ERR Unknown CONFIG subcommand\r\n");
            }
        }
        RespCommand::FlushDb | RespCommand::FlushAll => {
            if allow_flush {
                FLUSHES.increment();
                cache.flush();
                write_buf.extend_from_slice(b"+OK\r\n");
            } else {
                write_buf.extend_from_slice(b"-ERR FLUSH command disabled on this port\r\n");
            }
        }
        RespCommand::Incr { key } => {
            // Redis INCR: if key doesn't exist, create with 0, then increment
            match cache.increment(key, 1, Some(0), None) {
                Ok(new_val) => {
                    write_buf.extend_from_slice(b":");
                    let mut buf = itoa::Buffer::new();
                    write_buf.extend_from_slice(buf.format(new_val).as_bytes());
                    write_buf.extend_from_slice(b"\r\n");
                }
                Err(cache_core::CacheError::NotNumeric) => {
                    write_buf
                        .extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
                }
                Err(cache_core::CacheError::Overflow) => {
                    write_buf.extend_from_slice(b"-ERR increment or decrement would overflow\r\n");
                }
                Err(_) => {
                    write_buf.extend_from_slice(b"-ERR cache error\r\n");
                }
            }
        }
        RespCommand::Decr { key } => {
            // Redis DECR: if key doesn't exist, create with 0, then decrement
            match cache.decrement(key, 1, Some(0), None) {
                Ok(new_val) => {
                    write_buf.extend_from_slice(b":");
                    let mut buf = itoa::Buffer::new();
                    write_buf.extend_from_slice(buf.format(new_val).as_bytes());
                    write_buf.extend_from_slice(b"\r\n");
                }
                Err(cache_core::CacheError::NotNumeric) => {
                    write_buf
                        .extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
                }
                Err(_) => {
                    write_buf.extend_from_slice(b"-ERR cache error\r\n");
                }
            }
        }
        RespCommand::IncrBy { key, delta } => {
            // Redis INCRBY: supports negative delta (acts like DECRBY)
            let result = if *delta >= 0 {
                cache.increment(key, *delta as u64, Some(0), None)
            } else {
                cache.decrement(key, (-*delta) as u64, Some(0), None)
            };
            match result {
                Ok(new_val) => {
                    write_buf.extend_from_slice(b":");
                    let mut buf = itoa::Buffer::new();
                    write_buf.extend_from_slice(buf.format(new_val).as_bytes());
                    write_buf.extend_from_slice(b"\r\n");
                }
                Err(cache_core::CacheError::NotNumeric) => {
                    write_buf
                        .extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
                }
                Err(cache_core::CacheError::Overflow) => {
                    write_buf.extend_from_slice(b"-ERR increment or decrement would overflow\r\n");
                }
                Err(_) => {
                    write_buf.extend_from_slice(b"-ERR cache error\r\n");
                }
            }
        }
        RespCommand::DecrBy { key, delta } => {
            // Redis DECRBY: supports negative delta (acts like INCRBY)
            let result = if *delta >= 0 {
                cache.decrement(key, *delta as u64, Some(0), None)
            } else {
                cache.increment(key, (-*delta) as u64, Some(0), None)
            };
            match result {
                Ok(new_val) => {
                    write_buf.extend_from_slice(b":");
                    let mut buf = itoa::Buffer::new();
                    write_buf.extend_from_slice(buf.format(new_val).as_bytes());
                    write_buf.extend_from_slice(b"\r\n");
                }
                Err(cache_core::CacheError::NotNumeric) => {
                    write_buf
                        .extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
                }
                Err(_) => {
                    write_buf.extend_from_slice(b"-ERR cache error\r\n");
                }
            }
        }
        RespCommand::Append { key, value } => {
            // Redis APPEND: if key doesn't exist, creates it; returns new length
            // First check if key exists
            if !cache.contains(key) {
                // Create the key with the value
                match cache.set(key, value, None) {
                    Ok(()) => {
                        write_buf.extend_from_slice(b":");
                        let mut buf = itoa::Buffer::new();
                        write_buf.extend_from_slice(buf.format(value.len()).as_bytes());
                        write_buf.extend_from_slice(b"\r\n");
                    }
                    Err(_) => {
                        write_buf.extend_from_slice(b"-ERR cache error\r\n");
                    }
                }
            } else {
                // Append to existing key
                match cache.append(key, value) {
                    Ok(new_len) => {
                        write_buf.extend_from_slice(b":");
                        let mut buf = itoa::Buffer::new();
                        write_buf.extend_from_slice(buf.format(new_len).as_bytes());
                        write_buf.extend_from_slice(b"\r\n");
                    }
                    Err(cache_core::CacheError::KeyNotFound) => {
                        // Race condition: key was deleted between contains and append
                        // Try to set instead
                        match cache.set(key, value, None) {
                            Ok(()) => {
                                write_buf.extend_from_slice(b":");
                                let mut buf = itoa::Buffer::new();
                                write_buf.extend_from_slice(buf.format(value.len()).as_bytes());
                                write_buf.extend_from_slice(b"\r\n");
                            }
                            Err(_) => {
                                write_buf.extend_from_slice(b"-ERR cache error\r\n");
                            }
                        }
                    }
                    Err(_) => {
                        write_buf.extend_from_slice(b"-ERR cache error\r\n");
                    }
                }
            }
        }
        RespCommand::Hello { proto_version, .. } => {
            let requested_version = proto_version.unwrap_or(2);
            let actual_version = if requested_version >= 3 { 3 } else { 2 };

            *version = if actual_version == 3 {
                RespVersion::Resp3
            } else {
                RespVersion::Resp2
            };

            if *version == RespVersion::Resp3 {
                write_buf.extend_from_slice(b"%7\r\n");
            } else {
                write_buf.extend_from_slice(b"*14\r\n");
            }

            write_buf.extend_from_slice(b"$6\r\nserver\r\n");
            write_buf.extend_from_slice(b"$16\r\ncrucible-server\r\n");
            write_buf.extend_from_slice(b"$7\r\nversion\r\n");
            write_buf.extend_from_slice(b"$5\r\n0.1.0\r\n");
            write_buf.extend_from_slice(b"$5\r\nproto\r\n");
            write_buf.extend_from_slice(b":");
            write_buf.extend_from_slice(if actual_version == 3 { b"3" } else { b"2" });
            write_buf.extend_from_slice(b"\r\n");
            write_buf.extend_from_slice(b"$2\r\nid\r\n");
            write_buf.extend_from_slice(b":0\r\n");
            write_buf.extend_from_slice(b"$4\r\nmode\r\n");
            write_buf.extend_from_slice(b"$10\r\nstandalone\r\n");
            write_buf.extend_from_slice(b"$4\r\nrole\r\n");
            write_buf.extend_from_slice(b"$6\r\nmaster\r\n");
            write_buf.extend_from_slice(b"$7\r\nmodules\r\n");
            write_buf.extend_from_slice(b"*0\r\n");
        }
    }
}

/// Execute a Memcache ASCII command against the cache.
/// Returns true if the connection should be closed.
#[inline]
pub fn execute_memcache<C: Cache>(
    cmd: &MemcacheCommand<'_>,
    cache: &C,
    write_buf: &mut BytesMut,
    allow_flush: bool,
) -> bool {
    match cmd {
        MemcacheCommand::Get { key } => {
            GETS.increment();
            let hit = cache.with_value(key, |value| {
                write_buf.extend_from_slice(b"VALUE ");
                write_buf.extend_from_slice(key);
                write_buf.extend_from_slice(b" 0 ");
                let mut len_buf = itoa::Buffer::new();
                write_buf.extend_from_slice(len_buf.format(value.len()).as_bytes());
                write_buf.extend_from_slice(b"\r\n");
                write_buf.extend_from_slice(value);
                write_buf.extend_from_slice(b"\r\nEND\r\n");
            });

            if hit.is_some() {
                HITS.increment();
            } else {
                MISSES.increment();
                write_buf.extend_from_slice(b"END\r\n");
            }
            false
        }
        MemcacheCommand::Gets { keys } => {
            for key in keys {
                GETS.increment();
                // Use with_value_cas to get value and CAS token together
                let hit = cache.with_value_cas(key, |value| {
                    // We need the value for the response, so return a copy
                    value.to_vec()
                });

                if let Some((value, cas)) = hit {
                    HITS.increment();
                    write_buf.extend_from_slice(b"VALUE ");
                    write_buf.extend_from_slice(key);
                    write_buf.extend_from_slice(b" 0 ");
                    let mut len_buf = itoa::Buffer::new();
                    write_buf.extend_from_slice(len_buf.format(value.len()).as_bytes());
                    write_buf.extend_from_slice(b" ");
                    write_buf.extend_from_slice(len_buf.format(cas).as_bytes());
                    write_buf.extend_from_slice(b"\r\n");
                    write_buf.extend_from_slice(&value);
                    write_buf.extend_from_slice(b"\r\n");
                } else {
                    MISSES.increment();
                }
            }
            write_buf.extend_from_slice(b"END\r\n");
            false
        }
        MemcacheCommand::Set {
            key, exptime, data, ..
        } => {
            SETS.increment();
            let ttl = if *exptime == 0 {
                None
            } else {
                Some(Duration::from_secs(*exptime as u64))
            };

            for attempt in 0..10 {
                match cache.set(key, data, ttl) {
                    Ok(()) => {
                        write_buf.extend_from_slice(b"STORED\r\n");
                        return false;
                    }
                    Err(_) => {
                        if attempt < 9 {
                            let delay = Duration::from_micros(100 << attempt);
                            std::thread::sleep(delay);
                        }
                    }
                }
            }
            SET_ERRORS.increment();
            write_buf.extend_from_slice(b"SERVER_ERROR out of memory\r\n");
            false
        }
        MemcacheCommand::Add {
            key, exptime, data, ..
        } => {
            SETS.increment();
            let ttl = if *exptime == 0 {
                None
            } else {
                Some(Duration::from_secs(*exptime as u64))
            };

            match cache.add(key, data, ttl) {
                Ok(()) => {
                    write_buf.extend_from_slice(b"STORED\r\n");
                }
                Err(cache_core::CacheError::KeyExists) => {
                    write_buf.extend_from_slice(b"NOT_STORED\r\n");
                }
                Err(_) => {
                    SET_ERRORS.increment();
                    write_buf.extend_from_slice(b"SERVER_ERROR out of memory\r\n");
                }
            }
            false
        }
        MemcacheCommand::Replace {
            key, exptime, data, ..
        } => {
            SETS.increment();
            let ttl = if *exptime == 0 {
                None
            } else {
                Some(Duration::from_secs(*exptime as u64))
            };

            match cache.replace(key, data, ttl) {
                Ok(()) => {
                    write_buf.extend_from_slice(b"STORED\r\n");
                }
                Err(cache_core::CacheError::KeyNotFound) => {
                    write_buf.extend_from_slice(b"NOT_STORED\r\n");
                }
                Err(_) => {
                    SET_ERRORS.increment();
                    write_buf.extend_from_slice(b"SERVER_ERROR out of memory\r\n");
                }
            }
            false
        }
        MemcacheCommand::Cas {
            key,
            exptime,
            data,
            cas_unique,
            ..
        } => {
            SETS.increment();
            let ttl = if *exptime == 0 {
                None
            } else {
                Some(Duration::from_secs(*exptime as u64))
            };

            match cache.cas(key, data, ttl, *cas_unique) {
                Ok(true) => {
                    // CAS succeeded - item was updated
                    write_buf.extend_from_slice(b"STORED\r\n");
                }
                Ok(false) => {
                    // CAS failed - item was modified since GETS
                    write_buf.extend_from_slice(b"EXISTS\r\n");
                }
                Err(cache_core::CacheError::KeyNotFound) => {
                    // Key doesn't exist
                    write_buf.extend_from_slice(b"NOT_FOUND\r\n");
                }
                Err(_) => {
                    // Other error (e.g., out of memory)
                    SET_ERRORS.increment();
                    write_buf.extend_from_slice(b"SERVER_ERROR out of memory\r\n");
                }
            }
            false
        }
        MemcacheCommand::Delete { key } => {
            DELETES.increment();
            if cache.delete(key) {
                write_buf.extend_from_slice(b"DELETED\r\n");
            } else {
                write_buf.extend_from_slice(b"NOT_FOUND\r\n");
            }
            false
        }
        MemcacheCommand::FlushAll => {
            if allow_flush {
                FLUSHES.increment();
                cache.flush();
                write_buf.extend_from_slice(b"OK\r\n");
            } else {
                write_buf.extend_from_slice(b"SERVER_ERROR flush_all disabled on this port\r\n");
            }
            false
        }
        MemcacheCommand::Version => {
            write_buf.extend_from_slice(b"VERSION crucible-server\r\n");
            false
        }
        MemcacheCommand::Quit => true,
        MemcacheCommand::Incr {
            key,
            delta,
            noreply,
        } => {
            // Memcache incr: returns NOT_FOUND if key doesn't exist (no initial value)
            match cache.increment(key, *delta, None, None) {
                Ok(new_val) => {
                    if !noreply {
                        let mut buf = itoa::Buffer::new();
                        write_buf.extend_from_slice(buf.format(new_val).as_bytes());
                        write_buf.extend_from_slice(b"\r\n");
                    }
                }
                Err(cache_core::CacheError::KeyNotFound) => {
                    if !noreply {
                        write_buf.extend_from_slice(b"NOT_FOUND\r\n");
                    }
                }
                Err(cache_core::CacheError::NotNumeric) => {
                    write_buf
                        .extend_from_slice(b"CLIENT_ERROR cannot increment non-numeric value\r\n");
                }
                Err(_) => {
                    write_buf.extend_from_slice(b"SERVER_ERROR\r\n");
                }
            }
            false
        }
        MemcacheCommand::Decr {
            key,
            delta,
            noreply,
        } => {
            // Memcache decr: returns NOT_FOUND if key doesn't exist, clamps to 0 on underflow
            match cache.decrement(key, *delta, None, None) {
                Ok(new_val) => {
                    if !noreply {
                        let mut buf = itoa::Buffer::new();
                        write_buf.extend_from_slice(buf.format(new_val).as_bytes());
                        write_buf.extend_from_slice(b"\r\n");
                    }
                }
                Err(cache_core::CacheError::KeyNotFound) => {
                    if !noreply {
                        write_buf.extend_from_slice(b"NOT_FOUND\r\n");
                    }
                }
                Err(cache_core::CacheError::NotNumeric) => {
                    write_buf
                        .extend_from_slice(b"CLIENT_ERROR cannot decrement non-numeric value\r\n");
                }
                Err(_) => {
                    write_buf.extend_from_slice(b"SERVER_ERROR\r\n");
                }
            }
            false
        }
        MemcacheCommand::Append { key, data, noreply } => {
            // Memcache append: returns NOT_STORED if key doesn't exist
            match cache.append(key, data) {
                Ok(_) => {
                    if !noreply {
                        write_buf.extend_from_slice(b"STORED\r\n");
                    }
                }
                Err(cache_core::CacheError::KeyNotFound) => {
                    if !noreply {
                        write_buf.extend_from_slice(b"NOT_STORED\r\n");
                    }
                }
                Err(_) => {
                    write_buf.extend_from_slice(b"SERVER_ERROR\r\n");
                }
            }
            false
        }
        MemcacheCommand::Prepend { key, data, noreply } => {
            // Memcache prepend: returns NOT_STORED if key doesn't exist
            match cache.prepend(key, data) {
                Ok(_) => {
                    if !noreply {
                        write_buf.extend_from_slice(b"STORED\r\n");
                    }
                }
                Err(cache_core::CacheError::KeyNotFound) => {
                    if !noreply {
                        write_buf.extend_from_slice(b"NOT_STORED\r\n");
                    }
                }
                Err(_) => {
                    write_buf.extend_from_slice(b"SERVER_ERROR\r\n");
                }
            }
            false
        }
    }
}

/// Execute a Memcache binary protocol command against the cache.
/// Returns true if the connection should be closed.
#[inline]
pub fn execute_memcache_binary<C: Cache>(
    cmd: &BinaryCommand<'_>,
    cache: &C,
    write_buf: &mut BytesMut,
    allow_flush: bool,
) -> bool {
    write_buf.reserve(256);
    let buf = write_buf.spare_capacity_mut();
    let buf = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len()) };

    let len = match cmd {
        BinaryCommand::Get { key, opaque } | BinaryCommand::GetK { key, opaque } => {
            GETS.increment();
            let is_getk = matches!(cmd, BinaryCommand::GetK { .. });
            let opaque = *opaque;

            let len = cache.with_value(key, |value| {
                HITS.increment();
                let opcode = if is_getk { Opcode::GetK } else { Opcode::Get };
                if is_getk {
                    BinaryResponse::encode_getk(buf, opcode, opaque, 0, 0, key, value)
                } else {
                    BinaryResponse::encode_get(buf, opcode, opaque, 0, 0, value)
                }
            });

            match len {
                Some(n) => n,
                None => {
                    MISSES.increment();
                    BinaryResponse::encode_not_found(buf, Opcode::Get, opaque)
                }
            }
        }
        BinaryCommand::GetQ { key, opaque } | BinaryCommand::GetKQ { key, opaque } => {
            GETS.increment();
            let is_getkq = matches!(cmd, BinaryCommand::GetKQ { .. });
            let opaque = *opaque;

            let len = cache.with_value(key, |value| {
                HITS.increment();
                let opcode = if is_getkq {
                    Opcode::GetKQ
                } else {
                    Opcode::GetQ
                };
                if is_getkq {
                    BinaryResponse::encode_getk(buf, opcode, opaque, 0, 0, key, value)
                } else {
                    BinaryResponse::encode_get(buf, opcode, opaque, 0, 0, value)
                }
            });

            match len {
                Some(n) => n,
                None => {
                    MISSES.increment();
                    0
                }
            }
        }
        BinaryCommand::Set {
            key,
            value,
            expiration,
            opaque,
            ..
        } => {
            SETS.increment();
            let ttl = if *expiration == 0 {
                None
            } else {
                Some(Duration::from_secs(*expiration as u64))
            };

            for attempt in 0..10 {
                match cache.set(key, value, ttl) {
                    Ok(()) => {
                        let len = BinaryResponse::encode_stored(buf, Opcode::Set, *opaque, 0);
                        unsafe { write_buf.set_len(write_buf.len() + len) };
                        return false;
                    }
                    Err(_) => {
                        if attempt < 9 {
                            let delay = Duration::from_micros(100 << attempt);
                            std::thread::sleep(delay);
                        }
                    }
                }
            }
            SET_ERRORS.increment();
            BinaryResponse::encode_out_of_memory(buf, Opcode::Set, *opaque)
        }
        BinaryCommand::SetQ {
            key,
            value,
            expiration,
            ..
        } => {
            SETS.increment();
            let ttl = if *expiration == 0 {
                None
            } else {
                Some(Duration::from_secs(*expiration as u64))
            };

            for attempt in 0..10 {
                match cache.set(key, value, ttl) {
                    Ok(()) => return false,
                    Err(_) => {
                        if attempt < 9 {
                            let delay = Duration::from_micros(100 << attempt);
                            std::thread::sleep(delay);
                        }
                    }
                }
            }
            SET_ERRORS.increment();
            0
        }
        BinaryCommand::Add {
            key,
            value,
            expiration,
            opaque,
            ..
        } => {
            SETS.increment();
            let ttl = if *expiration == 0 {
                None
            } else {
                Some(Duration::from_secs(*expiration as u64))
            };

            match cache.add(key, value, ttl) {
                Ok(()) => BinaryResponse::encode_stored(buf, Opcode::Add, *opaque, 0),
                Err(cache_core::CacheError::KeyExists) => {
                    BinaryResponse::encode_exists(buf, Opcode::Add, *opaque)
                }
                Err(_) => {
                    SET_ERRORS.increment();
                    BinaryResponse::encode_out_of_memory(buf, Opcode::Add, *opaque)
                }
            }
        }
        BinaryCommand::Replace {
            key,
            value,
            expiration,
            opaque,
            ..
        } => {
            SETS.increment();
            let ttl = if *expiration == 0 {
                None
            } else {
                Some(Duration::from_secs(*expiration as u64))
            };

            match cache.replace(key, value, ttl) {
                Ok(()) => BinaryResponse::encode_stored(buf, Opcode::Replace, *opaque, 0),
                Err(cache_core::CacheError::KeyNotFound) => {
                    BinaryResponse::encode_not_found(buf, Opcode::Replace, *opaque)
                }
                Err(_) => {
                    SET_ERRORS.increment();
                    BinaryResponse::encode_out_of_memory(buf, Opcode::Replace, *opaque)
                }
            }
        }
        BinaryCommand::Delete { key, opaque, .. } => {
            DELETES.increment();
            if cache.delete(key) {
                BinaryResponse::encode_deleted(buf, Opcode::Delete, *opaque)
            } else {
                BinaryResponse::encode_not_found(buf, Opcode::Delete, *opaque)
            }
        }
        BinaryCommand::DeleteQ { key, .. } => {
            DELETES.increment();
            cache.delete(key);
            0
        }
        BinaryCommand::Flush { opaque, .. } => {
            if allow_flush {
                FLUSHES.increment();
                cache.flush();
                BinaryResponse::encode_flushed(buf, *opaque)
            } else {
                // Return "unknown command" error for disabled flush
                BinaryResponse::encode_unknown_command(buf, Opcode::Flush, *opaque)
            }
        }
        BinaryCommand::Noop { opaque } => BinaryResponse::encode_noop(buf, *opaque),
        BinaryCommand::Version { opaque } => {
            BinaryResponse::encode_version(buf, *opaque, b"crucible-server")
        }
        BinaryCommand::Quit { .. } => {
            return true;
        }
        BinaryCommand::Stat { opaque, .. } => BinaryResponse::encode_stat_end(buf, *opaque),
        BinaryCommand::Increment {
            key,
            delta,
            initial,
            expiration,
            opaque,
            ..
        } => {
            let ttl = if *expiration == 0 {
                None
            } else {
                Some(Duration::from_secs(*expiration as u64))
            };

            match cache.increment(key, *delta, Some(*initial), ttl) {
                Ok(new_val) => {
                    // Return the new counter value
                    BinaryResponse::encode_counter(buf, Opcode::Increment, *opaque, new_val, 0)
                }
                Err(cache_core::CacheError::KeyNotFound) => {
                    BinaryResponse::encode_not_found(buf, Opcode::Increment, *opaque)
                }
                Err(cache_core::CacheError::NotNumeric) => {
                    BinaryResponse::encode_non_numeric(buf, Opcode::Increment, *opaque)
                }
                Err(_) => BinaryResponse::encode_out_of_memory(buf, Opcode::Increment, *opaque),
            }
        }
        BinaryCommand::Decrement {
            key,
            delta,
            initial,
            expiration,
            opaque,
            ..
        } => {
            let ttl = if *expiration == 0 {
                None
            } else {
                Some(Duration::from_secs(*expiration as u64))
            };

            match cache.decrement(key, *delta, Some(*initial), ttl) {
                Ok(new_val) => {
                    // Return the new counter value
                    BinaryResponse::encode_counter(buf, Opcode::Decrement, *opaque, new_val, 0)
                }
                Err(cache_core::CacheError::KeyNotFound) => {
                    BinaryResponse::encode_not_found(buf, Opcode::Decrement, *opaque)
                }
                Err(cache_core::CacheError::NotNumeric) => {
                    BinaryResponse::encode_non_numeric(buf, Opcode::Decrement, *opaque)
                }
                Err(_) => BinaryResponse::encode_out_of_memory(buf, Opcode::Decrement, *opaque),
            }
        }
        _ => {
            // Unsupported commands
            0
        }
    };

    if len > 0 {
        unsafe { write_buf.set_len(write_buf.len() + len) };
    }
    false
}
