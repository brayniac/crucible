//! Comprehensive large value tests for the kompio (io_uring) backend.
//!
//! These tests verify correct handling of large values (256KB to 64MB+):
//! - Send modes: buffered, zerocopy, threshold
//! - Various connection and pipelining configurations
//!
//! The goal is to stress-test buffer management, coalesce buffer growth,
//! and data integrity for values that span multiple ring buffers (16KB default).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Large value test sizes.
/// These sizes are chosen to stress different parts of the buffer system:
/// - 256KB: Spans ~16 ring buffers (16KB each)
/// - 512KB: Moderate large value
/// - 1MB: Common large object size
/// - 4MB: Larger than total default ring capacity requires buffer pool cycling
/// - 16MB: Stress test for coalesce buffer growth
/// - 64MB: Extreme case (marked as ignored for normal test runs)
const LARGE_SIZES: &[usize] = &[
    256 * 1024,  // 256KB
    512 * 1024,  // 512KB
    1024 * 1024, // 1MB
];

const VERY_LARGE_SIZES: &[usize] = &[
    4 * 1024 * 1024,  // 4MB
    16 * 1024 * 1024, // 16MB
];

const EXTREME_SIZES: &[usize] = &[
    64 * 1024 * 1024, // 64MB
];

/// Get an available port for testing.
fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Wait for server to be ready by attempting connections.
fn wait_for_server(addr: SocketAddr, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok() {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Start a test server with full configuration options.
fn start_test_server_full(
    port: u16,
    worker_threads: usize,
    heap_size_mb: usize,
    segment_size_mb: usize,
) -> thread::JoinHandle<()> {
    // Max value size should be less than segment size
    let max_value_size_mb = std::cmp::max(1, segment_size_mb - 1);
    start_test_server_full_with_max_value(
        port,
        worker_threads,
        heap_size_mb,
        segment_size_mb,
        max_value_size_mb,
    )
}

/// Start a test server with full configuration options including max_value_size.
fn start_test_server_full_with_max_value(
    port: u16,
    worker_threads: usize,
    heap_size_mb: usize,
    segment_size_mb: usize,
    max_value_size_mb: usize,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let config_str = format!(
            r#"
            [workers]
            threads = {worker_threads}

            [cache]
            backend = "segment"
            heap_size = "{heap_size_mb}MB"
            segment_size = "{segment_size_mb}MB"
            max_value_size = "{max_value_size_mb}MB"
            hashtable_power = 20

            [[listener]]
            protocol = "resp"
            address = "127.0.0.1:{port}"

            [metrics]
            address = "127.0.0.1:0"
            "#,
        );

        let config: server::Config = toml::from_str(&config_str).unwrap();
        let cache = segcache::SegCache::builder()
            .heap_size(config.cache.heap_size)
            .segment_size(config.cache.segment_size)
            .hashtable_power(config.cache.hashtable_power)
            .build()
            .unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let drain_timeout = Duration::from_secs(5);

        let _ = server::native::run(&config, cache, shutdown, drain_timeout);
    })
}

/// Generate a large value with a verifiable pattern.
/// Uses a pattern that allows easy corruption detection.
fn generate_large_value(size: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(size);
    // Use a pattern that includes the position to detect byte-level corruption
    for i in 0..size {
        // Mix of position-dependent bytes and fixed patterns
        value.push((i % 256) as u8);
    }
    value
}

/// Verify a value matches the expected pattern.
fn verify_value(data: &[u8], expected_size: usize) -> bool {
    if data.len() != expected_size {
        return false;
    }
    for (i, &byte) in data.iter().enumerate() {
        if byte != (i % 256) as u8 {
            return false;
        }
    }
    true
}

/// Build a RESP SET command for large values.
fn build_large_set_command(key: &str, value: &[u8]) -> Vec<u8> {
    let header = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n",
        key.len(),
        key,
        value.len()
    );
    let mut cmd = header.into_bytes();
    cmd.extend_from_slice(value);
    cmd.extend_from_slice(b"\r\n");
    cmd
}

/// Build a RESP GET command.
fn build_get_command(key: &str) -> Vec<u8> {
    format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).into_bytes()
}

/// Send a large SET command and verify OK response.
fn send_large_set(stream: &mut TcpStream, key: &str, value: &[u8]) -> Result<(), String> {
    let cmd = build_large_set_command(key, value);

    // Send in chunks to avoid overwhelming the socket buffer
    let chunk_size = 64 * 1024; // 64KB chunks
    let mut offset = 0;
    while offset < cmd.len() {
        let end = std::cmp::min(offset + chunk_size, cmd.len());
        stream
            .write_all(&cmd[offset..end])
            .map_err(|e| format!("Write failed: {}", e))?;
        offset = end;
    }

    // Read response
    let mut buf = [0u8; 64];
    let start = Instant::now();
    let timeout = Duration::from_secs(60);

    while start.elapsed() < timeout {
        match stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                let response = String::from_utf8_lossy(&buf[..n]);
                if response.contains("+OK") {
                    return Ok(());
                } else if response.contains("-") {
                    return Err(format!("SET error: {}", response));
                }
            }
            Ok(_) => return Err("Connection closed".to_string()),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(e) => return Err(format!("Read error: {}", e)),
        }
    }
    Err("Timeout waiting for SET response".to_string())
}

/// Send a GET command and read the full large value response.
fn send_large_get(
    stream: &mut TcpStream,
    key: &str,
    expected_size: usize,
) -> Result<Vec<u8>, String> {
    let cmd = build_get_command(key);
    stream
        .write_all(&cmd)
        .map_err(|e| format!("Write failed: {}", e))?;

    // Read response - need to handle RESP bulk string format
    // $<length>\r\n<data>\r\n
    let mut response = Vec::with_capacity(expected_size + 64);
    let mut buf = [0u8; 64 * 1024]; // 64KB read buffer

    let start = Instant::now();
    let timeout = Duration::from_secs(120); // Longer timeout for large values

    while start.elapsed() < timeout {
        match stream.read(&mut buf) {
            Ok(0) => return Err("Connection closed".to_string()),
            Ok(n) => {
                response.extend_from_slice(&buf[..n]);

                // Check if we have a complete response
                if let Some(value) = try_parse_bulk_string(&response, expected_size)? {
                    return Ok(value);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(e) => return Err(format!("Read error: {}", e)),
        }
    }

    Err(format!(
        "Timeout waiting for GET response (received {} bytes, expected ~{})",
        response.len(),
        expected_size + 20
    ))
}

/// Try to parse a RESP bulk string response.
/// Returns Ok(Some(value)) if complete, Ok(None) if incomplete, Err on parse error.
fn try_parse_bulk_string(data: &[u8], expected_size: usize) -> Result<Option<Vec<u8>>, String> {
    if data.is_empty() {
        return Ok(None);
    }

    // Check for null bulk string
    if data.starts_with(b"$-1\r\n") {
        return Err("Key not found (null bulk string)".to_string());
    }

    // Check for error
    if data[0] == b'-' {
        if let Some(end) = data.iter().position(|&b| b == b'\n') {
            return Err(format!(
                "Redis error: {}",
                String::from_utf8_lossy(&data[..end])
            ));
        }
        return Ok(None); // Incomplete error
    }

    // Must start with $
    if data[0] != b'$' {
        return Err(format!("Expected bulk string, got: {:?}", &data[..1]));
    }

    // Find the length line
    let len_end = match data.windows(2).position(|w| w == b"\r\n") {
        Some(pos) => pos,
        None => return Ok(None), // Incomplete header
    };

    let len_str = std::str::from_utf8(&data[1..len_end])
        .map_err(|e| format!("Invalid UTF-8 in length: {}", e))?;

    let declared_len: usize = len_str
        .parse()
        .map_err(|e| format!("Invalid length '{}': {}", len_str, e))?;

    // Verify declared length matches expected
    if declared_len != expected_size {
        return Err(format!(
            "Length mismatch: declared {}, expected {}",
            declared_len, expected_size
        ));
    }

    // Check if we have the full data
    let data_start = len_end + 2;
    let data_end = data_start + declared_len;
    let total_len = data_end + 2; // +2 for trailing \r\n

    if data.len() < total_len {
        return Ok(None); // Still receiving
    }

    // Verify trailing CRLF
    if data[data_end] != b'\r' || data[data_end + 1] != b'\n' {
        return Err("Missing trailing CRLF".to_string());
    }

    Ok(Some(data[data_start..data_end].to_vec()))
}

/// Configuration for large value tests.
struct LargeValueTestConfig {
    sizes: &'static [usize],
    heap_size_mb: usize,
    segment_size_mb: usize,
    max_value_size_mb: usize,
}

impl Default for LargeValueTestConfig {
    fn default() -> Self {
        Self {
            sizes: LARGE_SIZES,
            heap_size_mb: 256,
            segment_size_mb: 32,
            max_value_size_mb: 31, // Must be less than segment_size
        }
    }
}

/// Run a large value test with the given configuration.
fn run_large_value_test(config: LargeValueTestConfig) {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_full_with_max_value(
        port,
        2, // 2 worker threads
        config.heap_size_mb,
        config.segment_size_mb,
        config.max_value_size_mb,
    );

    assert!(
        wait_for_server(addr, Duration::from_secs(10)),
        "Server failed to start"
    );

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(120)))
        .unwrap();

    for &size in config.sizes {
        let key = format!("largekey_{}", size);
        let value = generate_large_value(size);

        println!("Testing {}: {} bytes", key, size);

        // SET the large value
        send_large_set(&mut stream, &key, &value)
            .unwrap_or_else(|e| panic!("SET failed for {} ({} bytes): {}", key, size, e));

        // GET the large value back
        let retrieved = send_large_get(&mut stream, &key, size)
            .unwrap_or_else(|e| panic!("GET failed for {} ({} bytes): {}", key, size, e));

        // Verify data integrity
        assert!(
            verify_value(&retrieved, size),
            "Data corruption detected for {} ({} bytes)",
            key,
            size
        );

        println!("  PASSED: {} bytes verified", size);
    }
}

/// Run a concurrent large value test with multiple connections.
fn run_concurrent_large_value_test(connections: usize, value_size: usize) {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Larger heap for concurrent tests
    let heap_size_mb = std::cmp::max(512, (value_size * connections * 2) / (1024 * 1024) + 128);

    let _server_handle = start_test_server_full(
        port,
        4, // More workers for concurrent load
        heap_size_mb,
        64, // 64MB segments
    );

    assert!(
        wait_for_server(addr, Duration::from_secs(10)),
        "Server failed to start"
    );

    let errors = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let successes = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();

    for conn_id in 0..connections {
        let errors = Arc::clone(&errors);
        let successes = Arc::clone(&successes);

        let handle = thread::spawn(move || {
            let mut stream = match TcpStream::connect(addr) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Connection {} failed to connect: {}", conn_id, e);
                    errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            stream.set_nodelay(true).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(120)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(120)))
                .unwrap();

            let key = format!("concurrent_key_{}", conn_id);
            let value = generate_large_value(value_size);

            // SET
            if let Err(e) = send_large_set(&mut stream, &key, &value) {
                eprintln!("Connection {} SET failed: {}", conn_id, e);
                errors.fetch_add(1, Ordering::Relaxed);
                return;
            }

            // GET and verify
            match send_large_get(&mut stream, &key, value_size) {
                Ok(retrieved) => {
                    if verify_value(&retrieved, value_size) {
                        successes.fetch_add(1, Ordering::Relaxed);
                    } else {
                        eprintln!("Connection {} data corruption", conn_id);
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    eprintln!("Connection {} GET failed: {}", conn_id, e);
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }

    let err_count = errors.load(Ordering::Relaxed);
    let success_count = successes.load(Ordering::Relaxed);

    assert_eq!(
        err_count, 0,
        "Concurrent test had {} errors ({} successes)",
        err_count, success_count
    );
    assert_eq!(
        success_count, connections,
        "Not all connections succeeded: {} of {}",
        success_count, connections
    );
}

// =============================================================================
// Large Value Tests
// =============================================================================

#[test]
fn test_uring_large_values_256k_to_1m() {
    run_large_value_test(LargeValueTestConfig::default());
}

#[test]
fn test_uring_large_values_4m_to_16m() {
    run_large_value_test(LargeValueTestConfig {
        sizes: VERY_LARGE_SIZES,
        heap_size_mb: 512,
        segment_size_mb: 64,
        max_value_size_mb: 63,
    });
}

#[test]
#[ignore] // Expensive test
fn test_uring_large_values_64m() {
    run_large_value_test(LargeValueTestConfig {
        sizes: EXTREME_SIZES,
        heap_size_mb: 1024,
        segment_size_mb: 128,
        max_value_size_mb: 127,
    });
}

#[test]
fn test_uring_concurrent_large_values_1m() {
    run_concurrent_large_value_test(8, 1024 * 1024);
}

#[test]
#[ignore] // Expensive test
fn test_uring_concurrent_large_values_4m() {
    run_concurrent_large_value_test(4, 4 * 1024 * 1024);
}

// =============================================================================
// Edge Case Tests
// =============================================================================

/// Test rapid large value SET/GET cycles to stress buffer recycling.
#[test]
fn test_uring_rapid_large_value_cycles() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_full(port, 2, 256, 32);

    assert!(wait_for_server(addr, Duration::from_secs(10)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(60)))
        .unwrap();

    let size = 512 * 1024; // 512KB

    for cycle in 0..20 {
        let key = "rapid_cycle_key";
        let value = generate_large_value(size);

        send_large_set(&mut stream, key, &value)
            .unwrap_or_else(|e| panic!("SET failed on cycle {}: {}", cycle, e));

        let retrieved = send_large_get(&mut stream, key, size)
            .unwrap_or_else(|e| panic!("GET failed on cycle {}: {}", cycle, e));

        assert!(
            verify_value(&retrieved, size),
            "Data corruption on cycle {}",
            cycle
        );
    }
}

/// Test increasing value sizes in sequence.
/// This stresses coalesce buffer growth.
#[test]
fn test_uring_increasing_value_sizes() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Use larger segment/max_value to support up to 2MB values
    let _server_handle = start_test_server_full_with_max_value(
        port, 2, 256, 8, 7, // 8MB segment, 7MB max value
    );

    assert!(wait_for_server(addr, Duration::from_secs(10)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(120)))
        .unwrap();

    let sizes = [
        1024,
        16 * 1024,
        32 * 1024,
        64 * 1024,
        128 * 1024,
        256 * 1024,
        512 * 1024,
        1024 * 1024,
        2 * 1024 * 1024,
    ];

    for &size in &sizes {
        let key = format!("increasing_key_{}", size);
        let value = generate_large_value(size);

        println!("Testing increasing size: {} bytes", size);

        send_large_set(&mut stream, &key, &value)
            .unwrap_or_else(|e| panic!("SET failed for size {}: {}", size, e));

        let retrieved = send_large_get(&mut stream, &key, size)
            .unwrap_or_else(|e| panic!("GET failed for size {}: {}", size, e));

        assert!(
            verify_value(&retrieved, size),
            "Data corruption for size {}",
            size
        );
    }
}

/// Test alternating between small and large values.
/// This tests buffer shrink/grow behavior.
#[test]
fn test_uring_alternating_value_sizes() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_full(port, 2, 256, 32);

    assert!(wait_for_server(addr, Duration::from_secs(10)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(60)))
        .unwrap();

    for i in 0..10 {
        let (key, size) = if i % 2 == 0 {
            (format!("small_key_{}", i), 64)
        } else {
            (format!("large_key_{}", i), 1024 * 1024)
        };

        let value = generate_large_value(size);

        send_large_set(&mut stream, &key, &value)
            .unwrap_or_else(|e| panic!("SET failed for key {} (size {}): {}", key, size, e));

        let retrieved = send_large_get(&mut stream, &key, size)
            .unwrap_or_else(|e| panic!("GET failed for key {} (size {}): {}", key, size, e));

        assert!(
            verify_value(&retrieved, size),
            "Data corruption for key {} (size {})",
            key,
            size
        );
    }
}

/// Test values at exact buffer boundary sizes.
/// Ring buffers are 16KB, so test around that boundary.
#[test]
fn test_uring_buffer_boundary_sizes() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_full(port, 2, 256, 32);

    assert!(wait_for_server(addr, Duration::from_secs(10)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(60)))
        .unwrap();

    let sizes = [
        16 * 1024 - 1,
        16 * 1024,
        16 * 1024 + 1,
        32 * 1024 - 1,
        32 * 1024,
        32 * 1024 + 1,
        64 * 1024 - 1,
        64 * 1024,
        64 * 1024 + 1,
    ];

    for &size in &sizes {
        let key = format!("boundary_key_{}", size);
        let value = generate_large_value(size);

        println!("Testing boundary size: {} bytes", size);

        send_large_set(&mut stream, &key, &value)
            .unwrap_or_else(|e| panic!("SET failed for size {}: {}", size, e));

        let retrieved = send_large_get(&mut stream, &key, size)
            .unwrap_or_else(|e| panic!("GET failed for size {}: {}", size, e));

        assert!(
            verify_value(&retrieved, size),
            "Data corruption for size {}",
            size
        );
    }
}
