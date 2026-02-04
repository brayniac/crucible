//! Large buffer tests for io-driver buffer management.
//!
//! These tests verify that the buffer pool and coalesce buffer correctly handle
//! large values that span multiple buffers. They test the lower-level buffer
//! management without the full server stack.

use io_driver::{BufferChain, BufferPool};

/// Assembly buffer for coalescing fragmented data.
struct AssemblyBuffer {
    data: Vec<u8>,
}

impl AssemblyBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            data: vec![0u8; capacity],
        }
    }

    fn assemble<'a>(&'a mut self, chain: &BufferChain, pool: &BufferPool) -> &'a [u8] {
        let readable = chain.readable();
        if readable > self.data.len() {
            self.data.resize(readable, 0);
        }
        let copied = chain.copy_to(pool, &mut self.data);
        &self.data[..copied]
    }
}

/// Generate a verifiable pattern for testing.
fn generate_pattern(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// Verify a buffer matches the expected pattern.
fn verify_pattern(data: &[u8]) -> bool {
    data.iter().enumerate().all(|(i, &b)| b == (i % 256) as u8)
}

// =============================================================================
// BufferPool Large Value Tests
// =============================================================================

#[test]
fn test_large_value_256kb() {
    // 32KB chunks, 32 buffers = 1MB total capacity
    let mut pool = BufferPool::new(32 * 1024, 32);
    let mut chain = BufferChain::new();
    let mut assembly = AssemblyBuffer::new(512 * 1024);

    let value_size = 256 * 1024; // 256KB
    let value = generate_pattern(value_size);

    // Simulate receiving 256KB in 32KB chunks
    let chunk_size = 32 * 1024;
    let mut offset = 0;
    while offset < value.len() {
        let buf_id = pool.checkout().expect("Pool exhausted");
        let copy_size = std::cmp::min(chunk_size, value.len() - offset);
        pool.get_mut(buf_id)[..copy_size].copy_from_slice(&value[offset..offset + copy_size]);
        chain.push(buf_id, copy_size);
        offset += copy_size;
    }

    assert_eq!(chain.readable(), value_size);
    assert!(!chain.is_contiguous()); // Should span multiple buffers

    // Assemble and verify
    let data = assembly.assemble(&chain, &pool);
    assert_eq!(data.len(), value_size);
    assert!(verify_pattern(data), "Data corruption in 256KB value");

    // Cleanup
    chain.advance(value_size);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }
}

#[test]
fn test_large_value_1mb() {
    // 32KB chunks, 64 buffers = 2MB total capacity
    let mut pool = BufferPool::new(32 * 1024, 64);
    let mut chain = BufferChain::new();
    let mut assembly = AssemblyBuffer::new(2 * 1024 * 1024);

    let value_size = 1024 * 1024; // 1MB
    let value = generate_pattern(value_size);

    let chunk_size = 32 * 1024;
    let mut offset = 0;
    while offset < value.len() {
        let buf_id = pool.checkout().expect("Pool exhausted");
        let copy_size = std::cmp::min(chunk_size, value.len() - offset);
        pool.get_mut(buf_id)[..copy_size].copy_from_slice(&value[offset..offset + copy_size]);
        chain.push(buf_id, copy_size);
        offset += copy_size;
    }

    assert_eq!(chain.readable(), value_size);

    let data = assembly.assemble(&chain, &pool);
    assert_eq!(data.len(), value_size);
    assert!(verify_pattern(data), "Data corruption in 1MB value");

    chain.advance(value_size);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }
}

#[test]
fn test_large_value_4mb() {
    // 64KB chunks, 128 buffers = 8MB total capacity
    let mut pool = BufferPool::new(64 * 1024, 128);
    let mut chain = BufferChain::new();
    let mut assembly = AssemblyBuffer::new(8 * 1024 * 1024);

    let value_size = 4 * 1024 * 1024; // 4MB
    let value = generate_pattern(value_size);

    let chunk_size = 64 * 1024;
    let mut offset = 0;
    while offset < value.len() {
        let buf_id = pool.checkout().expect("Pool exhausted");
        let copy_size = std::cmp::min(chunk_size, value.len() - offset);
        pool.get_mut(buf_id)[..copy_size].copy_from_slice(&value[offset..offset + copy_size]);
        chain.push(buf_id, copy_size);
        offset += copy_size;
    }

    assert_eq!(chain.readable(), value_size);

    let data = assembly.assemble(&chain, &pool);
    assert_eq!(data.len(), value_size);
    assert!(verify_pattern(data), "Data corruption in 4MB value");

    chain.advance(value_size);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }
}

#[test]
#[ignore] // Expensive test
fn test_large_value_16mb() {
    // 128KB chunks, 256 buffers = 32MB total capacity
    let mut pool = BufferPool::new(128 * 1024, 256);
    let mut chain = BufferChain::new();
    let mut assembly = AssemblyBuffer::new(32 * 1024 * 1024);

    let value_size = 16 * 1024 * 1024; // 16MB
    let value = generate_pattern(value_size);

    let chunk_size = 128 * 1024;
    let mut offset = 0;
    while offset < value.len() {
        let buf_id = pool.checkout().expect("Pool exhausted");
        let copy_size = std::cmp::min(chunk_size, value.len() - offset);
        pool.get_mut(buf_id)[..copy_size].copy_from_slice(&value[offset..offset + copy_size]);
        chain.push(buf_id, copy_size);
        offset += copy_size;
    }

    assert_eq!(chain.readable(), value_size);

    let data = assembly.assemble(&chain, &pool);
    assert_eq!(data.len(), value_size);
    assert!(verify_pattern(data), "Data corruption in 16MB value");

    chain.advance(value_size);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }
}

// =============================================================================
// Buffer Pool Recycling Tests
// =============================================================================

#[test]
fn test_buffer_recycling_with_large_values() {
    // Small pool that requires recycling for multiple large values
    let mut pool = BufferPool::new(32 * 1024, 16); // 512KB total
    let mut assembly = AssemblyBuffer::new(512 * 1024);

    let value_size = 128 * 1024; // 128KB per value

    // Process multiple large values, recycling buffers
    for iteration in 0..10 {
        let mut chain = BufferChain::new();
        let value = generate_pattern(value_size);

        let chunk_size = 32 * 1024;
        let mut offset = 0;
        while offset < value.len() {
            let buf_id = pool.checkout().unwrap_or_else(|| {
                panic!(
                    "Pool exhausted on iteration {}, offset {} (free: {})",
                    iteration,
                    offset,
                    pool.free_count()
                )
            });
            let copy_size = std::cmp::min(chunk_size, value.len() - offset);
            pool.get_mut(buf_id)[..copy_size].copy_from_slice(&value[offset..offset + copy_size]);
            chain.push(buf_id, copy_size);
            offset += copy_size;
        }

        let data = assembly.assemble(&chain, &pool);
        assert!(
            verify_pattern(data),
            "Data corruption on iteration {}",
            iteration
        );

        // Return all buffers
        chain.advance(value_size);
        for id in chain.drain_consumed() {
            pool.checkin(id);
        }

        // Verify all buffers returned
        assert_eq!(
            pool.free_count(),
            16,
            "Buffer leak on iteration {}",
            iteration
        );
    }
}

#[test]
fn test_partial_consume_large_value() {
    let mut pool = BufferPool::new(32 * 1024, 32);
    let mut chain = BufferChain::new();
    let mut assembly = AssemblyBuffer::new(512 * 1024);

    let value_size = 256 * 1024;
    let value = generate_pattern(value_size);

    // Load value into chain
    let chunk_size = 32 * 1024;
    let mut offset = 0;
    while offset < value.len() {
        let buf_id = pool.checkout().expect("Pool exhausted");
        let copy_size = std::cmp::min(chunk_size, value.len() - offset);
        pool.get_mut(buf_id)[..copy_size].copy_from_slice(&value[offset..offset + copy_size]);
        chain.push(buf_id, copy_size);
        offset += copy_size;
    }

    // Consume in chunks, verifying data at each step
    let consume_size = 64 * 1024; // 64KB at a time
    let mut consumed = 0;

    while consumed < value_size {
        let remaining = value_size - consumed;
        let to_consume = std::cmp::min(consume_size, remaining);

        let data = assembly.assemble(&chain, &pool);
        assert_eq!(
            data.len(),
            remaining,
            "Wrong remaining size after consuming {}",
            consumed
        );

        // Verify the remaining data starts at the right position
        for (i, &byte) in data[..to_consume].iter().enumerate() {
            let expected = ((consumed + i) % 256) as u8;
            assert_eq!(
                byte, expected,
                "Data mismatch at position {} (consumed {})",
                i, consumed
            );
        }

        chain.advance(to_consume);
        consumed += to_consume;

        // Return fully consumed buffers
        for id in chain.drain_consumed() {
            pool.checkin(id);
        }
    }

    assert!(chain.is_empty());
    assert_eq!(pool.free_count(), 32);
}

// =============================================================================
// Buffer Boundary Tests
// =============================================================================

#[test]
fn test_value_at_buffer_boundary() {
    let chunk_size = 32 * 1024;
    let mut pool = BufferPool::new(chunk_size, 16);
    let mut chain = BufferChain::new();
    // Test value exactly at buffer boundary
    let value_size = chunk_size;
    let value = generate_pattern(value_size);

    let buf_id = pool.checkout().expect("Pool exhausted");
    pool.get_mut(buf_id)[..value_size].copy_from_slice(&value);
    chain.push(buf_id, value_size);

    // Should be contiguous (single buffer)
    assert!(chain.is_contiguous());

    let data = chain.first_chunk(&pool).unwrap();
    assert_eq!(data.len(), value_size);
    assert!(verify_pattern(data));

    chain.advance(value_size);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }
}

#[test]
fn test_value_one_byte_over_boundary() {
    let chunk_size = 32 * 1024;
    let mut pool = BufferPool::new(chunk_size, 16);
    let mut chain = BufferChain::new();
    let mut assembly = AssemblyBuffer::new(128 * 1024);

    // Test value one byte over buffer boundary
    let value_size = chunk_size + 1;
    let value = generate_pattern(value_size);

    // First chunk fills entire buffer
    let buf1 = pool.checkout().expect("Pool exhausted");
    pool.get_mut(buf1)[..chunk_size].copy_from_slice(&value[..chunk_size]);
    chain.push(buf1, chunk_size);

    // Second chunk has just one byte
    let buf2 = pool.checkout().expect("Pool exhausted");
    pool.get_mut(buf2)[0] = value[chunk_size];
    chain.push(buf2, 1);

    // Should NOT be contiguous (spans two buffers)
    assert!(!chain.is_contiguous());

    let data = assembly.assemble(&chain, &pool);
    assert_eq!(data.len(), value_size);
    assert!(verify_pattern(data));

    chain.advance(value_size);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }
}

#[test]
fn test_value_one_byte_under_boundary() {
    let chunk_size = 32 * 1024;
    let mut pool = BufferPool::new(chunk_size, 16);
    let mut chain = BufferChain::new();

    // Test value one byte under buffer boundary
    let value_size = chunk_size - 1;
    let value = generate_pattern(value_size);

    let buf_id = pool.checkout().expect("Pool exhausted");
    pool.get_mut(buf_id)[..value_size].copy_from_slice(&value);
    chain.push(buf_id, value_size);

    // Should be contiguous (single buffer)
    assert!(chain.is_contiguous());

    let data = chain.first_chunk(&pool).unwrap();
    assert_eq!(data.len(), value_size);
    assert!(verify_pattern(data));

    chain.advance(value_size);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }
}

// =============================================================================
// Multiple Large Values Tests
// =============================================================================

#[test]
fn test_multiple_large_values_in_sequence() {
    let mut pool = BufferPool::new(32 * 1024, 64); // 2MB total
    let mut assembly = AssemblyBuffer::new(1024 * 1024);

    let sizes = [64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024];

    for &size in &sizes {
        let mut chain = BufferChain::new();
        let value = generate_pattern(size);

        let chunk_size = 32 * 1024;
        let mut offset = 0;
        while offset < value.len() {
            let buf_id = pool.checkout().expect("Pool exhausted");
            let copy_size = std::cmp::min(chunk_size, value.len() - offset);
            pool.get_mut(buf_id)[..copy_size].copy_from_slice(&value[offset..offset + copy_size]);
            chain.push(buf_id, copy_size);
            offset += copy_size;
        }

        let data = assembly.assemble(&chain, &pool);
        assert_eq!(data.len(), size);
        assert!(verify_pattern(data), "Data corruption for size {}", size);

        chain.advance(size);
        for id in chain.drain_consumed() {
            pool.checkin(id);
        }

        // Verify full cleanup
        assert_eq!(pool.free_count(), 64);
    }
}

#[test]
fn test_interleaved_small_and_large_values() {
    let mut pool = BufferPool::new(32 * 1024, 32);
    let mut assembly = AssemblyBuffer::new(512 * 1024);

    for i in 0..20 {
        let size = if i % 2 == 0 { 1024 } else { 256 * 1024 };

        let mut chain = BufferChain::new();
        let value = generate_pattern(size);

        let chunk_size = 32 * 1024;
        let mut offset = 0;
        while offset < value.len() {
            let buf_id = pool.checkout().expect("Pool exhausted");
            let copy_size = std::cmp::min(chunk_size, value.len() - offset);
            pool.get_mut(buf_id)[..copy_size].copy_from_slice(&value[offset..offset + copy_size]);
            chain.push(buf_id, copy_size);
            offset += copy_size;
        }

        let data = assembly.assemble(&chain, &pool);
        assert!(
            verify_pattern(data),
            "Data corruption on iteration {} (size {})",
            i,
            size
        );

        chain.advance(size);
        for id in chain.drain_consumed() {
            pool.checkin(id);
        }
    }
}

// =============================================================================
// Stress Tests
// =============================================================================

#[test]
fn test_rapid_alloc_free_cycles() {
    let mut pool = BufferPool::new(16 * 1024, 128);

    // Rapid allocation/deallocation cycles
    for _ in 0..1000 {
        let mut ids = Vec::new();

        // Allocate all buffers
        while let Some(id) = pool.checkout() {
            ids.push(id);
        }

        assert!(pool.checkout().is_none());
        assert!(!ids.is_empty());

        // Free all buffers
        for id in ids {
            pool.checkin(id);
        }

        assert_eq!(pool.free_count(), 128);
    }
}

#[test]
fn test_large_value_with_small_chunks() {
    // Simulate small MTU / fragmented receives
    let mut pool = BufferPool::new(1500, 1024); // 1500 byte chunks (MTU size)
    let mut chain = BufferChain::new();
    let mut assembly = AssemblyBuffer::new(256 * 1024);

    let value_size = 128 * 1024; // 128KB
    let value = generate_pattern(value_size);

    let chunk_size = 1500;
    let mut offset = 0;
    while offset < value.len() {
        let buf_id = pool.checkout().expect("Pool exhausted");
        let copy_size = std::cmp::min(chunk_size, value.len() - offset);
        pool.get_mut(buf_id)[..copy_size].copy_from_slice(&value[offset..offset + copy_size]);
        chain.push(buf_id, copy_size);
        offset += copy_size;
    }

    // Will have many buffers in the chain
    assert!(!chain.is_contiguous());

    let data = assembly.assemble(&chain, &pool);
    assert_eq!(data.len(), value_size);
    assert!(verify_pattern(data));

    chain.advance(value_size);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }
}

#[test]
fn test_assembly_buffer_growth() {
    let mut pool = BufferPool::new(32 * 1024, 64);
    let mut chain = BufferChain::new();

    // Start with small assembly buffer
    let mut assembly = AssemblyBuffer::new(1024);

    // Load a larger value - assembly should grow
    let value_size = 256 * 1024;
    let value = generate_pattern(value_size);

    let chunk_size = 32 * 1024;
    let mut offset = 0;
    while offset < value.len() {
        let buf_id = pool.checkout().expect("Pool exhausted");
        let copy_size = std::cmp::min(chunk_size, value.len() - offset);
        pool.get_mut(buf_id)[..copy_size].copy_from_slice(&value[offset..offset + copy_size]);
        chain.push(buf_id, copy_size);
        offset += copy_size;
    }

    let data = assembly.assemble(&chain, &pool);
    assert_eq!(data.len(), value_size);
    assert!(verify_pattern(data));

    // Assembly buffer should have grown
    assert!(assembly.data.len() >= value_size);

    chain.advance(value_size);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn test_empty_value() {
    let _pool = BufferPool::new(1024, 16);
    let chain = BufferChain::new();

    assert!(chain.is_empty());
    assert_eq!(chain.readable(), 0);
    assert!(chain.is_contiguous()); // Empty chain is considered contiguous
}

#[test]
fn test_single_byte_value() {
    let mut pool = BufferPool::new(1024, 16);
    let mut chain = BufferChain::new();

    let buf_id = pool.checkout().expect("Pool exhausted");
    pool.get_mut(buf_id)[0] = 42;
    chain.push(buf_id, 1);

    assert!(chain.is_contiguous());
    assert_eq!(chain.readable(), 1);

    let data = chain.first_chunk(&pool).unwrap();
    assert_eq!(data, &[42]);

    chain.advance(1);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }
}

#[test]
fn test_exactly_pool_capacity() {
    // Use entire pool capacity for a single value
    let chunk_size = 32 * 1024;
    let num_buffers = 8;
    let mut pool = BufferPool::new(chunk_size, num_buffers);
    let mut chain = BufferChain::new();
    let mut assembly = AssemblyBuffer::new(chunk_size * num_buffers);

    let value_size = chunk_size * num_buffers; // Exactly fills pool
    let value = generate_pattern(value_size);

    let mut offset = 0;
    while offset < value.len() {
        let buf_id = pool.checkout().expect("Pool exhausted");
        let copy_size = std::cmp::min(chunk_size, value.len() - offset);
        pool.get_mut(buf_id)[..copy_size].copy_from_slice(&value[offset..offset + copy_size]);
        chain.push(buf_id, copy_size);
        offset += copy_size;
    }

    // Pool should be exhausted
    assert!(pool.checkout().is_none());

    let data = assembly.assemble(&chain, &pool);
    assert!(verify_pattern(data));

    chain.advance(value_size);
    for id in chain.drain_consumed() {
        pool.checkin(id);
    }

    // Pool should be fully restored
    assert_eq!(pool.free_count(), num_buffers);
}
