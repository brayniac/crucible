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
) {
    match cmd {
        RespCommand::Ping => {
            write_buf.extend_from_slice(b"+PONG\r\n");
        }
        RespCommand::Get { key } => {
            GETS.increment();
            match cache.get(key) {
                Some(guard) => {
                    HITS.increment();
                    let value = guard.as_ref();
                    // Reserve: $ + max_len_digits(20) + \r\n + value + \r\n
                    let needed = 1 + 20 + 2 + value.len() + 2;
                    write_buf.reserve(needed);

                    let spare = write_buf.spare_capacity_mut();
                    let buf = unsafe {
                        std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, spare.len())
                    };
                    let written = unsafe { write_bulk_string(buf, value) };
                    unsafe { write_buf.set_len(write_buf.len() + written) };
                }
                None => {
                    MISSES.increment();
                    if *version == RespVersion::Resp3 {
                        write_buf.extend_from_slice(b"_\r\n");
                    } else {
                        write_buf.extend_from_slice(b"$-1\r\n");
                    }
                }
            }
        }
        RespCommand::Set { key, value, ex, .. } => {
            SETS.increment();
            let ttl = ex.map(Duration::from_secs);

            for attempt in 0..10 {
                match cache.set(key, value, ttl) {
                    Ok(()) => {
                        write_buf.extend_from_slice(b"+OK\r\n");
                        return;
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
            write_buf.extend_from_slice(b"-ERR cache full\r\n");
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
                match cache.get(key) {
                    Some(guard) => {
                        HITS.increment();
                        let value = guard.as_ref();
                        let needed = 1 + 20 + 2 + value.len() + 2;
                        write_buf.reserve(needed);

                        let spare = write_buf.spare_capacity_mut();
                        let buf = unsafe {
                            std::slice::from_raw_parts_mut(
                                spare.as_mut_ptr() as *mut u8,
                                spare.len(),
                            )
                        };
                        let written = unsafe { write_bulk_string(buf, value) };
                        unsafe { write_buf.set_len(write_buf.len() + written) };
                    }
                    None => {
                        MISSES.increment();
                        if *version == RespVersion::Resp3 {
                            write_buf.extend_from_slice(b"_\r\n");
                        } else {
                            write_buf.extend_from_slice(b"$-1\r\n");
                        }
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
            FLUSHES.increment();
            cache.flush();
            write_buf.extend_from_slice(b"+OK\r\n");
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
) -> bool {
    match cmd {
        MemcacheCommand::Get { key } => {
            GETS.increment();
            match cache.get(key) {
                Some(guard) => {
                    HITS.increment();
                    let value = guard.as_ref();
                    write_buf.extend_from_slice(b"VALUE ");
                    write_buf.extend_from_slice(key);
                    write_buf.extend_from_slice(b" 0 ");
                    let mut len_buf = itoa::Buffer::new();
                    write_buf.extend_from_slice(len_buf.format(value.len()).as_bytes());
                    write_buf.extend_from_slice(b"\r\n");
                    write_buf.extend_from_slice(value);
                    write_buf.extend_from_slice(b"\r\nEND\r\n");
                }
                None => {
                    MISSES.increment();
                    write_buf.extend_from_slice(b"END\r\n");
                }
            }
            false
        }
        MemcacheCommand::Gets { keys } => {
            for key in keys {
                GETS.increment();
                if let Some(guard) = cache.get(key) {
                    HITS.increment();
                    let value = guard.as_ref();
                    write_buf.extend_from_slice(b"VALUE ");
                    write_buf.extend_from_slice(key);
                    write_buf.extend_from_slice(b" 0 ");
                    let mut len_buf = itoa::Buffer::new();
                    write_buf.extend_from_slice(len_buf.format(value.len()).as_bytes());
                    write_buf.extend_from_slice(b"\r\n");
                    write_buf.extend_from_slice(value);
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
            FLUSHES.increment();
            cache.flush();
            write_buf.extend_from_slice(b"OK\r\n");
            false
        }
        MemcacheCommand::Version => {
            write_buf.extend_from_slice(b"VERSION crucible-server\r\n");
            false
        }
        MemcacheCommand::Quit => true,
    }
}

/// Execute a Memcache binary protocol command against the cache.
/// Returns true if the connection should be closed.
#[inline]
pub fn execute_memcache_binary<C: Cache>(
    cmd: &BinaryCommand<'_>,
    cache: &C,
    write_buf: &mut BytesMut,
) -> bool {
    write_buf.reserve(256);
    let buf = write_buf.spare_capacity_mut();
    let buf = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len()) };

    let len = match cmd {
        BinaryCommand::Get { key, opaque } | BinaryCommand::GetK { key, opaque } => {
            GETS.increment();
            match cache.get(key) {
                Some(guard) => {
                    HITS.increment();
                    let value = guard.as_ref();
                    let opcode = match cmd {
                        BinaryCommand::GetK { .. } => Opcode::GetK,
                        _ => Opcode::Get,
                    };
                    if matches!(cmd, BinaryCommand::GetK { .. }) {
                        BinaryResponse::encode_getk(buf, opcode, *opaque, 0, 0, key, value)
                    } else {
                        BinaryResponse::encode_get(buf, opcode, *opaque, 0, 0, value)
                    }
                }
                None => {
                    MISSES.increment();
                    BinaryResponse::encode_not_found(buf, Opcode::Get, *opaque)
                }
            }
        }
        BinaryCommand::GetQ { key, opaque } | BinaryCommand::GetKQ { key, opaque } => {
            GETS.increment();
            match cache.get(key) {
                Some(guard) => {
                    HITS.increment();
                    let value = guard.as_ref();
                    let opcode = match cmd {
                        BinaryCommand::GetKQ { .. } => Opcode::GetKQ,
                        _ => Opcode::GetQ,
                    };
                    if matches!(cmd, BinaryCommand::GetKQ { .. }) {
                        BinaryResponse::encode_getk(buf, opcode, *opaque, 0, 0, key, value)
                    } else {
                        BinaryResponse::encode_get(buf, opcode, *opaque, 0, 0, value)
                    }
                }
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
            FLUSHES.increment();
            cache.flush();
            BinaryResponse::encode_flushed(buf, *opaque)
        }
        BinaryCommand::Noop { opaque } => BinaryResponse::encode_noop(buf, *opaque),
        BinaryCommand::Version { opaque } => {
            BinaryResponse::encode_version(buf, *opaque, b"crucible-server")
        }
        BinaryCommand::Quit { .. } => {
            return true;
        }
        BinaryCommand::Stat { opaque, .. } => BinaryResponse::encode_stat_end(buf, *opaque),
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
