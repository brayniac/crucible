//! Test that validates the buffer pool approach works for RESP parsing.
//!
//! This simulates the recv/parse flow without the actual I/O layer.

use io_driver::{BufferChain, BufferPool};

/// Simulated assembly buffer (would be per-worker in real implementation)
struct AssemblyBuffer {
    data: Vec<u8>,
}

impl AssemblyBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            data: vec![0u8; capacity],
        }
    }

    /// Assemble data from chain into contiguous buffer.
    /// Returns slice of assembled data.
    fn assemble<'a>(&'a mut self, chain: &BufferChain, pool: &BufferPool) -> &'a [u8] {
        let readable = chain.readable();
        if readable > self.data.len() {
            // Grow if needed (in production, might want to limit this)
            self.data.resize(readable, 0);
        }
        let copied = chain.copy_to(pool, &mut self.data);
        &self.data[..copied]
    }
}

/// Simplified RESP parser for testing.
/// Returns (command_type, consumed_bytes) or None if incomplete.
fn parse_resp_command(data: &[u8]) -> Option<(&'static str, usize)> {
    // Very basic RESP parsing for GET/SET
    if data.is_empty() {
        return None;
    }

    // Must start with *
    if data[0] != b'*' {
        return None;
    }

    // Find array length
    let mut pos = 1;
    while pos < data.len() && data[pos] != b'\r' {
        pos += 1;
    }
    if pos + 1 >= data.len() || data[pos + 1] != b'\n' {
        return None;
    }

    let array_len: usize = std::str::from_utf8(&data[1..pos]).ok()?.parse().ok()?;
    pos += 2;

    // Parse each element
    let mut elements = Vec::new();
    for _ in 0..array_len {
        if pos >= data.len() || data[pos] != b'$' {
            return None;
        }
        pos += 1;

        // Find bulk string length
        let len_start = pos;
        while pos < data.len() && data[pos] != b'\r' {
            pos += 1;
        }
        if pos + 1 >= data.len() || data[pos + 1] != b'\n' {
            return None;
        }

        let str_len: usize = std::str::from_utf8(&data[len_start..pos])
            .ok()?
            .parse()
            .ok()?;
        pos += 2;

        // Check we have the full string
        if pos + str_len + 2 > data.len() {
            return None;
        }

        elements.push(&data[pos..pos + str_len]);
        pos += str_len + 2; // +2 for \r\n
    }

    // Determine command type
    let cmd = if !elements.is_empty() {
        let cmd_bytes = elements[0];
        if cmd_bytes.eq_ignore_ascii_case(b"GET") {
            "GET"
        } else if cmd_bytes.eq_ignore_ascii_case(b"SET") {
            "SET"
        } else if cmd_bytes.eq_ignore_ascii_case(b"PING") {
            "PING"
        } else {
            "UNKNOWN"
        }
    } else {
        "EMPTY"
    };

    Some((cmd, pos))
}

#[test]
fn test_single_chunk_get() {
    let mut pool = BufferPool::new(1024, 16);
    let mut chain = BufferChain::new();
    // No assembly needed - chain is contiguous

    // Simulate receiving a GET command in one chunk
    let cmd = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";

    let buf_id = pool.checkout().unwrap();
    pool.get_mut(buf_id)[..cmd.len()].copy_from_slice(cmd);
    chain.push(buf_id, cmd.len());

    // Chain should be contiguous - can parse directly without assembly
    assert!(chain.is_contiguous());

    let data = chain.first_chunk(&pool).unwrap();
    let (cmd_type, consumed) = parse_resp_command(data).unwrap();

    assert_eq!(cmd_type, "GET");
    assert_eq!(consumed, cmd.len());

    // Advance and drain
    chain.advance(consumed);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }

    assert!(chain.is_empty());
    assert_eq!(pool.free_count(), 16);
}

#[test]
fn test_fragmented_set() {
    let mut pool = BufferPool::new(32, 16); // Small chunks to force fragmentation
    let mut chain = BufferChain::new();
    let mut assembly = AssemblyBuffer::new(4096);

    // A SET command that will span multiple chunks
    let cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$50\r\n01234567890123456789012345678901234567890123456789\r\n";

    // Simulate receiving in chunks
    let mut offset = 0;
    while offset < cmd.len() {
        let buf_id = pool.checkout().unwrap();
        let chunk_size = std::cmp::min(32, cmd.len() - offset);
        pool.get_mut(buf_id)[..chunk_size].copy_from_slice(&cmd[offset..offset + chunk_size]);
        chain.push(buf_id, chunk_size);
        offset += chunk_size;
    }

    // Chain should NOT be contiguous (spans multiple buffers)
    assert!(!chain.is_contiguous());
    assert_eq!(chain.readable(), cmd.len());

    // Assemble and parse
    let data = assembly.assemble(&chain, &pool);
    let (cmd_type, consumed) = parse_resp_command(data).unwrap();

    assert_eq!(cmd_type, "SET");
    assert_eq!(consumed, cmd.len());

    // Advance and drain
    chain.advance(consumed);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }

    assert!(chain.is_empty());
}

#[test]
fn test_incomplete_command() {
    let mut pool = BufferPool::new(1024, 16);
    let mut chain = BufferChain::new();

    // Partial command (incomplete)
    let partial = b"*2\r\n$3\r\nGET\r\n$3\r\nfo";

    let buf_id = pool.checkout().unwrap();
    pool.get_mut(buf_id)[..partial.len()].copy_from_slice(partial);
    chain.push(buf_id, partial.len());

    // Try to parse - should fail (incomplete)
    let data = chain.first_chunk(&pool).unwrap();
    assert!(parse_resp_command(data).is_none());

    // More data arrives
    let rest = b"o\r\n";
    let buf_id2 = pool.checkout().unwrap();
    pool.get_mut(buf_id2)[..rest.len()].copy_from_slice(rest);
    chain.push(buf_id2, rest.len());

    // Now need to assemble since we have two chunks
    let mut assembly = AssemblyBuffer::new(4096);
    let data = assembly.assemble(&chain, &pool);
    let (cmd_type, consumed) = parse_resp_command(data).unwrap();

    assert_eq!(cmd_type, "GET");
    assert_eq!(consumed, partial.len() + rest.len());
}

#[test]
fn test_pipelined_commands() {
    let mut pool = BufferPool::new(1024, 16);
    let mut chain = BufferChain::new();
    // No assembly needed - both commands fit in one chunk

    // Two commands in one chunk
    let cmds = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";

    let buf_id = pool.checkout().unwrap();
    pool.get_mut(buf_id)[..cmds.len()].copy_from_slice(cmds);
    chain.push(buf_id, cmds.len());

    // Parse first command
    let data = chain.first_chunk(&pool).unwrap();
    let (cmd_type, consumed) = parse_resp_command(data).unwrap();
    assert_eq!(cmd_type, "PING");

    chain.advance(consumed);

    // Parse second command (still in same chunk, so still contiguous)
    let data = chain.first_chunk(&pool).unwrap();
    let (cmd_type, consumed) = parse_resp_command(data).unwrap();
    assert_eq!(cmd_type, "GET");

    chain.advance(consumed);

    // Drain consumed
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }

    assert!(chain.is_empty());
}

#[test]
fn test_large_value() {
    let mut pool = BufferPool::new(32 * 1024, 64); // 32KB chunks, 2MB total
    let mut chain = BufferChain::new();
    let mut assembly = AssemblyBuffer::new(4 * 1024 * 1024); // 4MB assembly

    // Create a SET with a 100KB value
    let value_size = 100 * 1024;
    let value: Vec<u8> = (0..value_size).map(|i| (i % 256) as u8).collect();

    let mut cmd = format!("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n${}\r\n", value_size).into_bytes();
    cmd.extend_from_slice(&value);
    cmd.extend_from_slice(b"\r\n");

    // Simulate receiving in 32KB chunks
    let mut offset = 0;
    while offset < cmd.len() {
        let buf_id = pool.checkout().unwrap();
        let chunk_size = std::cmp::min(32 * 1024, cmd.len() - offset);
        pool.get_mut(buf_id)[..chunk_size].copy_from_slice(&cmd[offset..offset + chunk_size]);
        chain.push(buf_id, chunk_size);
        offset += chunk_size;
    }

    println!(
        "Large value test: {} bytes in {} chunks",
        cmd.len(),
        cmd.len().div_ceil(32 * 1024)
    );

    // Assemble and parse
    let data = assembly.assemble(&chain, &pool);
    let (cmd_type, consumed) = parse_resp_command(data).unwrap();

    assert_eq!(cmd_type, "SET");
    assert_eq!(consumed, cmd.len());

    // Cleanup
    chain.advance(consumed);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }
}

#[test]
fn test_pool_exhaustion_recovery() {
    let mut pool = BufferPool::new(1024, 4); // Only 4 buffers
    let mut chains: Vec<BufferChain> = (0..4).map(|_| BufferChain::new()).collect();

    // Exhaust the pool
    for chain in &mut chains {
        let id = pool.checkout().unwrap();
        pool.get_mut(id)[..5].copy_from_slice(b"hello");
        chain.push(id, 5);
    }

    // Pool should be exhausted
    assert!(pool.checkout().is_none());

    // Return buffers from first two chains
    for id in chains[0].clear() {
        pool.checkin(id);
    }
    for id in chains[1].clear() {
        pool.checkin(id);
    }

    // Now we can allocate again
    assert_eq!(pool.free_count(), 2);
    assert!(pool.checkout().is_some());
}
