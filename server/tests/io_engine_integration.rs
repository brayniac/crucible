//! Integration tests for different I/O engine backends.
//!
//! These tests verify that the server works correctly with both mio and io_uring
//! backends, including the zero-copy recv path.
//!
//! Test dimensions:
//! - I/O engines: mio, io_uring (Linux only)
//! - Connection counts: 1, 8, 64, 256
//! - Pipeline depths: 1, 8, 64
//! - Object sizes: small (64B), medium (1KB), large (16KB)

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Test configuration for parameterized tests.
#[derive(Clone, Debug)]
struct TestConfig {
    runtime: &'static str,
    io_engine: &'static str,
    recv_mode: Option<&'static str>,
    zero_copy: Option<&'static str>,
    connections: usize,
    pipeline_depth: usize,
    value_size: usize,
}

impl TestConfig {
    fn new(
        io_engine: &'static str,
        connections: usize,
        pipeline_depth: usize,
        value_size: usize,
    ) -> Self {
        Self {
            runtime: "native",
            io_engine,
            recv_mode: None,
            zero_copy: None,
            connections,
            pipeline_depth,
            value_size,
        }
    }

    fn with_runtime(
        runtime: &'static str,
        io_engine: &'static str,
        connections: usize,
        pipeline_depth: usize,
        value_size: usize,
    ) -> Self {
        Self {
            runtime,
            io_engine,
            recv_mode: None,
            zero_copy: None,
            connections,
            pipeline_depth,
            value_size,
        }
    }

    #[allow(dead_code)]
    fn with_recv_mode(
        io_engine: &'static str,
        recv_mode: &'static str,
        connections: usize,
        pipeline_depth: usize,
        value_size: usize,
    ) -> Self {
        Self {
            runtime: "native",
            io_engine,
            recv_mode: Some(recv_mode),
            zero_copy: None,
            connections,
            pipeline_depth,
            value_size,
        }
    }

    fn with_zero_copy(
        io_engine: &'static str,
        zero_copy: &'static str,
        connections: usize,
        pipeline_depth: usize,
        value_size: usize,
    ) -> Self {
        Self {
            runtime: "native",
            io_engine,
            recv_mode: None,
            zero_copy: Some(zero_copy),
            connections,
            pipeline_depth,
            value_size,
        }
    }
}

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

/// Start a test server with the specified I/O engine (native runtime).
fn start_test_server(port: u16, io_engine: &str, worker_threads: usize) -> thread::JoinHandle<()> {
    start_test_server_full(port, io_engine, "native", None, None, worker_threads)
}

/// Start a test server with the specified runtime and I/O engine.
fn start_test_server_with_runtime(
    port: u16,
    io_engine: &str,
    runtime: &str,
    worker_threads: usize,
) -> thread::JoinHandle<()> {
    start_test_server_full(port, io_engine, runtime, None, None, worker_threads)
}

/// Start a test server with zero-copy mode.
fn start_test_server_with_zero_copy(
    port: u16,
    io_engine: &str,
    zero_copy: &str,
    worker_threads: usize,
) -> thread::JoinHandle<()> {
    start_test_server_full(
        port,
        io_engine,
        "native",
        None,
        Some(zero_copy),
        worker_threads,
    )
}

/// Start a test server with full configuration options.
fn start_test_server_full(
    port: u16,
    io_engine: &str,
    runtime: &str,
    recv_mode: Option<&str>,
    zero_copy: Option<&str>,
    worker_threads: usize,
) -> thread::JoinHandle<()> {
    let io_engine = io_engine.to_string();
    let runtime = runtime.to_string();
    let recv_mode = recv_mode.map(|s| s.to_string());
    let zero_copy = zero_copy.map(|s| s.to_string());
    thread::spawn(move || {
        let recv_mode_config = recv_mode
            .as_ref()
            .map(|m| format!("recv_mode = \"{m}\""))
            .unwrap_or_default();

        let zero_copy_config = zero_copy
            .as_ref()
            .map(|m| format!("zero_copy = \"{m}\""))
            .unwrap_or_default();

        let config_str = format!(
            r#"
            runtime = "{runtime}"
            io_engine = "{io_engine}"
            {zero_copy_config}

            [workers]
            threads = {worker_threads}

            [uring]
            {recv_mode_config}

            [cache]
            backend = "segment"
            heap_size = "64MB"
            segment_size = "1MB"
            hashtable_power = 18

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

        // Run server (blocks forever, thread killed on test exit)
        match runtime.as_str() {
            "tokio" => {
                let _ = server::tokio::run(&config, cache);
            }
            _ => {
                let _ = server::native::run(&config, cache);
            }
        }
    })
}

/// Generate a value of the specified size.
fn generate_value(size: usize) -> String {
    // Use a repeating pattern for easy verification
    let pattern = "abcdefghijklmnopqrstuvwxyz0123456789";
    pattern.chars().cycle().take(size).collect()
}

/// Build a RESP SET command.
fn build_set_command(key: &str, value: &str) -> Vec<u8> {
    format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value
    )
    .into_bytes()
}

/// Build a RESP GET command.
fn build_get_command(key: &str) -> Vec<u8> {
    format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).into_bytes()
}

/// Build a RESP PING command.
fn build_ping_command() -> Vec<u8> {
    b"*1\r\n$4\r\nPING\r\n".to_vec()
}

/// Build a pipelined batch of SET commands.
fn build_pipelined_sets(key_prefix: &str, value: &str, count: usize) -> Vec<u8> {
    let mut commands = Vec::new();
    for i in 0..count {
        let key = format!("{}:{}", key_prefix, i);
        commands.extend(build_set_command(&key, value));
    }
    commands
}

/// Build a pipelined batch of GET commands.
fn build_pipelined_gets(key_prefix: &str, count: usize) -> Vec<u8> {
    let mut commands = Vec::new();
    for i in 0..count {
        let key = format!("{}:{}", key_prefix, i);
        commands.extend(build_get_command(&key));
    }
    commands
}

/// Read all expected responses from a stream.
fn read_responses(stream: &mut TcpStream, expected_count: usize) -> Vec<u8> {
    let mut buffer = vec![0u8; 64 * 1024]; // 64KB buffer
    let mut total = Vec::new();
    let mut responses_found = 0;

    let start = Instant::now();
    let timeout = Duration::from_secs(10);

    while responses_found < expected_count && start.elapsed() < timeout {
        match stream.read(&mut buffer) {
            Ok(0) => break, // Connection closed
            Ok(n) => {
                total.extend_from_slice(&buffer[..n]);
                // Count RESP responses by looking for \r\n patterns
                responses_found = count_resp_responses(&total);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(e) => panic!("Read error: {}", e),
        }
    }

    total
}

/// Count complete RESP responses in a buffer.
fn count_resp_responses(data: &[u8]) -> usize {
    let s = String::from_utf8_lossy(data);
    // Simple counting: each +OK\r\n, $N\r\n...\r\n, +PONG\r\n is a response
    // This is approximate but works for our test patterns
    let ok_count = s.matches("+OK\r\n").count();
    let pong_count = s.matches("+PONG\r\n").count();
    let bulk_count = s.matches("$").count(); // Bulk string responses from GET
    ok_count + pong_count + bulk_count
}

/// Run a connection worker that performs SET/GET operations.
fn run_connection_worker(
    addr: SocketAddr,
    worker_id: usize,
    config: &TestConfig,
    operations: usize,
    success_count: Arc<AtomicUsize>,
    running: Arc<AtomicBool>,
) {
    let mut stream = match TcpStream::connect_timeout(&addr, Duration::from_secs(5)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Worker {} failed to connect: {}", worker_id, e);
            return;
        }
    };

    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    let value = generate_value(config.value_size);
    let key_prefix = format!("worker{}:key", worker_id);

    let mut completed = 0;
    let ops_per_batch = config.pipeline_depth;

    while completed < operations && running.load(Ordering::Relaxed) {
        let batch_size = std::cmp::min(ops_per_batch, operations - completed);

        // Send pipelined SET commands
        let set_commands = build_pipelined_sets(&key_prefix, &value, batch_size);
        if stream.write_all(&set_commands).is_err() {
            break;
        }

        // Read SET responses
        let responses = read_responses(&mut stream, batch_size);
        let ok_count = String::from_utf8_lossy(&responses).matches("+OK").count();

        if ok_count != batch_size {
            eprintln!(
                "Worker {}: expected {} OKs, got {}",
                worker_id, batch_size, ok_count
            );
        }

        // Send pipelined GET commands
        let get_commands = build_pipelined_gets(&key_prefix, batch_size);
        if stream.write_all(&get_commands).is_err() {
            break;
        }

        // Read GET responses
        let responses = read_responses(&mut stream, batch_size);
        let response_str = String::from_utf8_lossy(&responses);
        let value_count = response_str
            .matches(&value[..std::cmp::min(32, value.len())])
            .count();

        if value_count >= batch_size {
            success_count.fetch_add(batch_size * 2, Ordering::Relaxed); // SET + GET
        }

        completed += batch_size;
    }
}

/// Run a parameterized test with the given configuration.
fn run_parameterized_test(config: TestConfig) {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Use more worker threads for higher connection counts
    let worker_threads = std::cmp::min(4, std::cmp::max(1, config.connections / 64));

    // Start server
    let _server_handle = start_test_server_full(
        port,
        config.io_engine,
        config.runtime,
        config.recv_mode,
        config.zero_copy,
        worker_threads,
    );

    // Wait for server to be ready
    let recv_mode_str = config.recv_mode.unwrap_or("default");
    let zero_copy_str = config.zero_copy.unwrap_or("default");
    assert!(
        wait_for_server(addr, Duration::from_secs(10)),
        "Server failed to start with {} runtime/{} backend/recv_mode={}/zero_copy={}",
        config.runtime,
        config.io_engine,
        recv_mode_str,
        zero_copy_str
    );

    let success_count = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let operations_per_connection = 100;

    // Spawn connection workers
    let mut handles = Vec::new();
    for worker_id in 0..config.connections {
        let addr = addr;
        let config = config.clone();
        let success_count = Arc::clone(&success_count);
        let running = Arc::clone(&running);

        let handle = thread::spawn(move || {
            run_connection_worker(
                addr,
                worker_id,
                &config,
                operations_per_connection,
                success_count,
                running,
            );
        });
        handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in handles {
        let _ = handle.join();
    }

    running.store(false, Ordering::Relaxed);

    let total_success = success_count.load(Ordering::Relaxed);
    let expected_min = (config.connections * operations_per_connection * 2) / 2; // At least 50% success

    assert!(
        total_success >= expected_min,
        "{} backend with {} connections, pipeline {}, value size {}: \
         expected at least {} successful ops, got {}",
        config.io_engine,
        config.connections,
        config.pipeline_depth,
        config.value_size,
        expected_min,
        total_success
    );
}

// =============================================================================
// Basic functionality tests
// =============================================================================

/// Send a RESP PING command and verify PONG response.
fn send_ping(stream: &mut TcpStream) -> bool {
    let ping = "*1\r\n$4\r\nPING\r\n";
    if stream.write_all(ping.as_bytes()).is_err() {
        return false;
    }

    let mut buf = [0u8; 64];
    match stream.read(&mut buf) {
        Ok(n) if n > 0 => {
            let response = String::from_utf8_lossy(&buf[..n]);
            response.contains("PONG")
        }
        _ => false,
    }
}

/// Send a SET command and verify OK response.
fn send_set(stream: &mut TcpStream, key: &str, value: &str) -> bool {
    let cmd = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value
    );
    if stream.write_all(cmd.as_bytes()).is_err() {
        return false;
    }

    let mut buf = [0u8; 64];
    match stream.read(&mut buf) {
        Ok(n) if n > 0 => {
            let response = String::from_utf8_lossy(&buf[..n]);
            response.contains("+OK")
        }
        _ => false,
    }
}

/// Send a GET command and verify the value.
fn send_get(stream: &mut TcpStream, key: &str, expected_value: &str) -> bool {
    let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    if stream.write_all(cmd.as_bytes()).is_err() {
        return false;
    }

    // Buffer must be large enough for large values
    // RESP bulk string format: $<len>\r\n<data>\r\n
    let expected_len = expected_value.len() + 20; // header + trailing CRLF + margin
    let mut buf = vec![0u8; expected_len];
    let mut total_read = 0;

    // Read in a loop - large responses may arrive in multiple TCP segments
    while total_read < expected_len {
        match stream.read(&mut buf[total_read..]) {
            Ok(0) => break, // EOF
            Ok(n) => {
                total_read += n;
                // Check if we have a complete RESP response (ends with \r\n)
                if total_read >= 2 && buf[total_read - 2] == b'\r' && buf[total_read - 1] == b'\n' {
                    // Verify it's not just the header ending
                    let response = &buf[..total_read];
                    if response.starts_with(b"$") && response.windows(4).any(|w| w == b"\r\n\r\n")
                        || (total_read > expected_value.len() + 5)
                    {
                        break;
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(_) => return false,
        }
    }

    let response = String::from_utf8_lossy(&buf[..total_read]);
    response.contains(expected_value)
}

/// Run the basic integration test for a given I/O engine (native runtime).
fn run_basic_test(io_engine: &str) {
    run_basic_test_with_runtime("native", io_engine);
}

/// Run the basic integration test for a given runtime and I/O engine.
fn run_basic_test_with_runtime(runtime: &str, io_engine: &str) {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Start server
    let _server_handle = start_test_server_with_runtime(port, io_engine, runtime, 1);

    // Wait for server to be ready
    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start with {} runtime/{} backend",
        runtime,
        io_engine
    );

    // Connect and run tests
    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Test PING
    assert!(
        send_ping(&mut stream),
        "PING failed with {} backend",
        io_engine
    );

    // Test SET
    assert!(
        send_set(&mut stream, "test_key", "test_value"),
        "SET failed with {} backend",
        io_engine
    );

    // Test GET
    assert!(
        send_get(&mut stream, "test_key", "test_value"),
        "GET failed with {} backend",
        io_engine
    );

    // Test pipelined commands
    let pipeline = "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
    stream.write_all(pipeline.as_bytes()).unwrap();

    let mut buf = [0u8; 256];
    let n = stream.read(&mut buf).unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    let pong_count = response.matches("PONG").count();
    assert!(
        pong_count >= 3,
        "Pipeline failed with {} backend: expected 3 PONGs, got {}",
        io_engine,
        pong_count
    );

    drop(stream);
}

// =============================================================================
// Mio backend tests
// =============================================================================

#[test]
fn test_mio_basic() {
    run_basic_test("mio");
}

#[test]
fn test_mio_1conn_p1_small() {
    run_parameterized_test(TestConfig::new("mio", 1, 1, 64));
}

#[test]
fn test_mio_1conn_p1_medium() {
    run_parameterized_test(TestConfig::new("mio", 1, 1, 1024));
}

#[test]
fn test_mio_1conn_p1_large() {
    run_parameterized_test(TestConfig::new("mio", 1, 1, 16384));
}

#[test]
fn test_mio_8conn_p1_small() {
    run_parameterized_test(TestConfig::new("mio", 8, 1, 64));
}

#[test]
fn test_mio_8conn_p8_small() {
    run_parameterized_test(TestConfig::new("mio", 8, 8, 64));
}

#[test]
fn test_mio_8conn_p64_small() {
    run_parameterized_test(TestConfig::new("mio", 8, 64, 64));
}

#[test]
fn test_mio_8conn_p8_medium() {
    run_parameterized_test(TestConfig::new("mio", 8, 8, 1024));
}

#[test]
fn test_mio_8conn_p8_large() {
    run_parameterized_test(TestConfig::new("mio", 8, 8, 16384));
}

#[test]
fn test_mio_64conn_p1_small() {
    run_parameterized_test(TestConfig::new("mio", 64, 1, 64));
}

#[test]
fn test_mio_64conn_p8_small() {
    run_parameterized_test(TestConfig::new("mio", 64, 8, 64));
}

#[test]
fn test_mio_64conn_p64_small() {
    run_parameterized_test(TestConfig::new("mio", 64, 64, 64));
}

#[test]
fn test_mio_64conn_p8_medium() {
    run_parameterized_test(TestConfig::new("mio", 64, 8, 1024));
}

#[test]
fn test_mio_64conn_p8_large() {
    run_parameterized_test(TestConfig::new("mio", 64, 8, 16384));
}

#[test]
#[ignore] // Expensive test, run with --ignored
fn test_mio_256conn_p1_small() {
    run_parameterized_test(TestConfig::new("mio", 256, 1, 64));
}

#[test]
#[ignore] // Expensive test, run with --ignored
fn test_mio_256conn_p8_small() {
    run_parameterized_test(TestConfig::new("mio", 256, 8, 64));
}

#[test]
#[ignore] // Expensive test, run with --ignored
fn test_mio_256conn_p64_small() {
    run_parameterized_test(TestConfig::new("mio", 256, 64, 64));
}

// =============================================================================
// io_uring backend tests (Linux only)
// =============================================================================

#[test]
#[cfg(target_os = "linux")]
fn test_uring_basic() {
    run_basic_test("uring");
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_1conn_p1_small() {
    run_parameterized_test(TestConfig::new("uring", 1, 1, 64));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_1conn_p1_medium() {
    run_parameterized_test(TestConfig::new("uring", 1, 1, 1024));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_1conn_p1_large() {
    run_parameterized_test(TestConfig::new("uring", 1, 1, 16384));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_8conn_p1_small() {
    run_parameterized_test(TestConfig::new("uring", 8, 1, 64));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_8conn_p8_small() {
    run_parameterized_test(TestConfig::new("uring", 8, 8, 64));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_8conn_p64_small() {
    run_parameterized_test(TestConfig::new("uring", 8, 64, 64));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_8conn_p8_medium() {
    run_parameterized_test(TestConfig::new("uring", 8, 8, 1024));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_8conn_p8_large() {
    run_parameterized_test(TestConfig::new("uring", 8, 8, 16384));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_64conn_p1_small() {
    run_parameterized_test(TestConfig::new("uring", 64, 1, 64));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_64conn_p8_small() {
    run_parameterized_test(TestConfig::new("uring", 64, 8, 64));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_64conn_p64_small() {
    run_parameterized_test(TestConfig::new("uring", 64, 64, 64));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_64conn_p8_medium() {
    run_parameterized_test(TestConfig::new("uring", 64, 8, 1024));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_64conn_p8_large() {
    run_parameterized_test(TestConfig::new("uring", 64, 8, 16384));
}

#[test]
#[cfg(target_os = "linux")]
#[ignore] // Expensive test, run with --ignored
fn test_uring_256conn_p1_small() {
    run_parameterized_test(TestConfig::new("uring", 256, 1, 64));
}

#[test]
#[cfg(target_os = "linux")]
#[ignore] // Expensive test, run with --ignored
fn test_uring_256conn_p8_small() {
    run_parameterized_test(TestConfig::new("uring", 256, 8, 64));
}

#[test]
#[cfg(target_os = "linux")]
#[ignore] // Expensive test, run with --ignored
fn test_uring_256conn_p64_small() {
    run_parameterized_test(TestConfig::new("uring", 256, 64, 64));
}

// =============================================================================
// Large object tests (stress the recv path)
// =============================================================================

#[test]
fn test_mio_large_objects() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server(port, "mio", 1);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    // Test various object sizes
    for &size in &[64, 256, 1024, 4096, 16384, 65536] {
        let key = format!("largekey_{}", size);
        let value = generate_value(size);

        assert!(
            send_set(&mut stream, &key, &value),
            "SET failed for size {}",
            size
        );
        assert!(
            send_get(&mut stream, &key, &value),
            "GET failed for size {}",
            size
        );
    }
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_large_objects() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server(port, "uring", 1);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    // Test various object sizes
    for &size in &[64, 256, 1024, 4096, 16384, 65536] {
        let key = format!("largekey_{}", size);
        let value = generate_value(size);

        assert!(
            send_set(&mut stream, &key, &value),
            "SET failed for size {}",
            size
        );
        assert!(
            send_get(&mut stream, &key, &value),
            "GET failed for size {}",
            size
        );
    }
}

// =============================================================================
// Connection churn tests
// =============================================================================

#[test]
fn test_mio_connection_churn() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server(port, "mio", 2);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    // Rapidly open and close connections while doing work
    for i in 0..100 {
        let mut stream = TcpStream::connect(addr).expect("Failed to connect");
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let key = format!("churn_key_{}", i);
        let value = format!("churn_value_{}", i);

        assert!(
            send_set(&mut stream, &key, &value),
            "SET failed on iteration {}",
            i
        );
        assert!(send_ping(&mut stream), "PING failed on iteration {}", i);

        // Explicit close
        drop(stream);
    }
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_connection_churn() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server(port, "uring", 2);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    // Rapidly open and close connections while doing work
    for i in 0..100 {
        let mut stream = TcpStream::connect(addr).expect("Failed to connect");
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let key = format!("churn_key_{}", i);
        let value = format!("churn_value_{}", i);

        assert!(
            send_set(&mut stream, &key, &value),
            "SET failed on iteration {}",
            i
        );
        assert!(send_ping(&mut stream), "PING failed on iteration {}", i);

        // Explicit close
        drop(stream);
    }
}

// =============================================================================
// Deep pipeline tests
// =============================================================================

#[test]
fn test_mio_deep_pipeline() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server(port, "mio", 1);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .unwrap();

    // Send 256 pipelined PING commands
    let depth = 256;
    let ping = build_ping_command();
    let mut pipeline = Vec::with_capacity(ping.len() * depth);
    for _ in 0..depth {
        pipeline.extend(&ping);
    }

    stream.write_all(&pipeline).unwrap();

    let responses = read_responses(&mut stream, depth);
    let pong_count = String::from_utf8_lossy(&responses).matches("PONG").count();

    assert!(
        pong_count >= depth,
        "Expected {} PONGs, got {}",
        depth,
        pong_count
    );
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_deep_pipeline() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server(port, "uring", 1);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .unwrap();

    // Send 256 pipelined PING commands
    let depth = 256;
    let ping = build_ping_command();
    let mut pipeline = Vec::with_capacity(ping.len() * depth);
    for _ in 0..depth {
        pipeline.extend(&ping);
    }

    stream.write_all(&pipeline).unwrap();

    let responses = read_responses(&mut stream, depth);
    let pong_count = String::from_utf8_lossy(&responses).matches("PONG").count();

    assert!(
        pong_count >= depth,
        "Expected {} PONGs, got {}",
        depth,
        pong_count
    );
}

// =============================================================================
// Tokio runtime backend tests
// =============================================================================

#[test]
fn test_tokio_basic() {
    run_basic_test_with_runtime("tokio", "mio");
}

#[test]
fn test_tokio_1conn_p1_small() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 1, 1, 64));
}

#[test]
fn test_tokio_1conn_p1_medium() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 1, 1, 1024));
}

#[test]
fn test_tokio_1conn_p1_large() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 1, 1, 16384));
}

#[test]
fn test_tokio_8conn_p1_small() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 8, 1, 64));
}

#[test]
fn test_tokio_8conn_p8_small() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 8, 8, 64));
}

#[test]
fn test_tokio_8conn_p64_small() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 8, 64, 64));
}

#[test]
fn test_tokio_8conn_p8_medium() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 8, 8, 1024));
}

#[test]
fn test_tokio_8conn_p8_large() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 8, 8, 16384));
}

#[test]
fn test_tokio_64conn_p1_small() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 64, 1, 64));
}

#[test]
fn test_tokio_64conn_p8_small() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 64, 8, 64));
}

#[test]
fn test_tokio_64conn_p64_small() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 64, 64, 64));
}

#[test]
fn test_tokio_64conn_p8_medium() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 64, 8, 1024));
}

#[test]
fn test_tokio_64conn_p8_large() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 64, 8, 16384));
}

#[test]
#[ignore] // Expensive test, run with --ignored
fn test_tokio_256conn_p1_small() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 256, 1, 64));
}

#[test]
#[ignore] // Expensive test, run with --ignored
fn test_tokio_256conn_p8_small() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 256, 8, 64));
}

#[test]
#[ignore] // Expensive test, run with --ignored
fn test_tokio_256conn_p64_small() {
    run_parameterized_test(TestConfig::with_runtime("tokio", "mio", 256, 64, 64));
}

#[test]
fn test_tokio_large_objects() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_with_runtime(port, "mio", "tokio", 1);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    // Test various object sizes
    for &size in &[64, 256, 1024, 4096, 16384, 65536] {
        let key = format!("largekey_{}", size);
        let value = generate_value(size);

        assert!(
            send_set(&mut stream, &key, &value),
            "SET failed for size {}",
            size
        );
        assert!(
            send_get(&mut stream, &key, &value),
            "GET failed for size {}",
            size
        );
    }
}

#[test]
fn test_tokio_connection_churn() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_with_runtime(port, "mio", "tokio", 2);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    // Rapidly open and close connections while doing work
    for i in 0..100 {
        let mut stream = TcpStream::connect(addr).expect("Failed to connect");
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let key = format!("churn_key_{}", i);
        let value = format!("churn_value_{}", i);

        assert!(
            send_set(&mut stream, &key, &value),
            "SET failed on iteration {}",
            i
        );
        assert!(send_ping(&mut stream), "PING failed on iteration {}", i);

        // Explicit close
        drop(stream);
    }
}

#[test]
fn test_tokio_deep_pipeline() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_with_runtime(port, "mio", "tokio", 1);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .unwrap();

    // Send 256 pipelined PING commands
    let depth = 256;
    let ping = build_ping_command();
    let mut pipeline = Vec::with_capacity(ping.len() * depth);
    for _ in 0..depth {
        pipeline.extend(&ping);
    }

    stream.write_all(&pipeline).unwrap();

    let responses = read_responses(&mut stream, depth);
    let pong_count = String::from_utf8_lossy(&responses).matches("PONG").count();

    assert!(
        pong_count >= depth,
        "Expected {} PONGs, got {}",
        depth,
        pong_count
    );
}

// =============================================================================
// io_uring single-shot recv mode tests (Linux only)
//
// These tests explicitly use single-shot recv mode to ensure coverage of the
// zero-copy recv path (kernel writes directly to pool buffer, no intermediate copy).
// =============================================================================

#[test]
#[cfg(target_os = "linux")]
fn test_uring_singleshot_basic() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle =
        start_test_server_full(port, "uring", "native", Some("singleshot"), None, 1);

    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start with uring/singleshot"
    );

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Test PING
    assert!(send_ping(&mut stream), "PING failed with uring/singleshot");

    // Test SET
    assert!(
        send_set(&mut stream, "test_key", "test_value"),
        "SET failed with uring/singleshot"
    );

    // Test GET
    assert!(
        send_get(&mut stream, "test_key", "test_value"),
        "GET failed with uring/singleshot"
    );

    drop(stream);
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_singleshot_1conn_p1_small() {
    run_parameterized_test(TestConfig::with_recv_mode("uring", "singleshot", 1, 1, 64));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_singleshot_1conn_p1_medium() {
    run_parameterized_test(TestConfig::with_recv_mode(
        "uring",
        "singleshot",
        1,
        1,
        1024,
    ));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_singleshot_1conn_p1_large() {
    run_parameterized_test(TestConfig::with_recv_mode(
        "uring",
        "singleshot",
        1,
        1,
        16384,
    ));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_singleshot_8conn_p8_small() {
    run_parameterized_test(TestConfig::with_recv_mode("uring", "singleshot", 8, 8, 64));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_singleshot_8conn_p8_medium() {
    run_parameterized_test(TestConfig::with_recv_mode(
        "uring",
        "singleshot",
        8,
        8,
        1024,
    ));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_singleshot_8conn_p8_large() {
    run_parameterized_test(TestConfig::with_recv_mode(
        "uring",
        "singleshot",
        8,
        8,
        16384,
    ));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_singleshot_64conn_p8_small() {
    run_parameterized_test(TestConfig::with_recv_mode("uring", "singleshot", 64, 8, 64));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_singleshot_64conn_p8_medium() {
    run_parameterized_test(TestConfig::with_recv_mode(
        "uring",
        "singleshot",
        64,
        8,
        1024,
    ));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_singleshot_large_objects() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle =
        start_test_server_full(port, "uring", "native", Some("singleshot"), None, 1);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    // Test various object sizes - exercises coalesce path for large values
    for &size in &[64, 256, 1024, 4096, 16384, 65536] {
        let key = format!("largekey_{}", size);
        let value = generate_value(size);

        assert!(
            send_set(&mut stream, &key, &value),
            "SET failed for size {} with singleshot",
            size
        );
        assert!(
            send_get(&mut stream, &key, &value),
            "GET failed for size {} with singleshot",
            size
        );
    }
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_singleshot_deep_pipeline() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle =
        start_test_server_full(port, "uring", "native", Some("singleshot"), None, 1);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .unwrap();

    // Send 256 pipelined PING commands
    let depth = 256;
    let ping = build_ping_command();
    let mut pipeline = Vec::with_capacity(ping.len() * depth);
    for _ in 0..depth {
        pipeline.extend(&ping);
    }

    stream.write_all(&pipeline).unwrap();

    let responses = read_responses(&mut stream, depth);
    let pong_count = String::from_utf8_lossy(&responses).matches("PONG").count();

    assert!(
        pong_count >= depth,
        "Expected {} PONGs with singleshot, got {}",
        depth,
        pong_count
    );
}

// =============================================================================
// io_uring multishot recv mode tests (Linux only)
//
// These tests explicitly use multishot recv mode to ensure coverage of the
// ring-provided buffer path (data copied from ring buffer to coalesce buffer).
// =============================================================================

#[test]
#[cfg(target_os = "linux")]
fn test_uring_multishot_basic() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle =
        start_test_server_full(port, "uring", "native", Some("multishot"), None, 1);

    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start with uring/multishot"
    );

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Test PING
    assert!(send_ping(&mut stream), "PING failed with uring/multishot");

    // Test SET
    assert!(
        send_set(&mut stream, "test_key", "test_value"),
        "SET failed with uring/multishot"
    );

    // Test GET
    assert!(
        send_get(&mut stream, "test_key", "test_value"),
        "GET failed with uring/multishot"
    );

    drop(stream);
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_multishot_8conn_p8_small() {
    run_parameterized_test(TestConfig::with_recv_mode("uring", "multishot", 8, 8, 64));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_multishot_8conn_p8_large() {
    run_parameterized_test(TestConfig::with_recv_mode(
        "uring",
        "multishot",
        8,
        8,
        16384,
    ));
}

#[test]
#[cfg(target_os = "linux")]
fn test_uring_multishot_64conn_p8_medium() {
    run_parameterized_test(TestConfig::with_recv_mode(
        "uring",
        "multishot",
        64,
        8,
        1024,
    ));
}

// =============================================================================
// Zero-copy mode tests (mio)
// =============================================================================

/// Test mio with zero-copy enabled for all GET responses.
#[test]
fn test_mio_zero_copy_enabled_basic() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_with_zero_copy(port, "mio", "enabled", 1);
    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start with mio/zero_copy=enabled"
    );

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Test PING
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut buf = [0u8; 64];
    let n = stream.read(&mut buf).expect("Failed to read PONG");
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("PONG"),
        "Expected PONG, got: {}",
        response
    );

    // Test SET then GET (zero-copy should be used for GET)
    let key = "test_key";
    let value = "test_value_for_zero_copy";
    let set_cmd = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value
    );
    stream.write_all(set_cmd.as_bytes()).unwrap();
    let n = stream.read(&mut buf).expect("Failed to read SET response");
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(response.contains("OK"), "Expected OK, got: {}", response);

    // GET the value back
    let get_cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    stream.write_all(get_cmd.as_bytes()).unwrap();
    let mut response_buf = vec![0u8; 256];
    let n = stream
        .read(&mut response_buf)
        .expect("Failed to read GET response");
    let response = String::from_utf8_lossy(&response_buf[..n]);
    assert!(
        response.contains(value),
        "Expected value '{}' in response, got: {}",
        value,
        response
    );
}

/// Test mio with zero-copy threshold mode (only values >= 1KB use zero-copy).
#[test]
fn test_mio_zero_copy_threshold_basic() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_with_zero_copy(port, "mio", "threshold", 1);
    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start with mio/zero_copy=threshold"
    );

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Test with small value (should NOT use zero-copy)
    let key1 = "small_key";
    let value1 = "small"; // < 1KB
    let set_cmd = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key1.len(),
        key1,
        value1.len(),
        value1
    );
    stream.write_all(set_cmd.as_bytes()).unwrap();
    let mut buf = [0u8; 64];
    let _ = stream.read(&mut buf).unwrap();

    let get_cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key1.len(), key1);
    stream.write_all(get_cmd.as_bytes()).unwrap();
    let mut response_buf = vec![0u8; 256];
    let n = stream.read(&mut response_buf).unwrap();
    let response = String::from_utf8_lossy(&response_buf[..n]);
    assert!(response.contains(value1), "Small value GET failed");

    // Test with large value (should use zero-copy)
    let key2 = "large_key";
    let value2 = "x".repeat(2048); // > 1KB threshold
    let set_cmd = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key2.len(),
        key2,
        value2.len(),
        value2
    );
    stream.write_all(set_cmd.as_bytes()).unwrap();
    let _ = stream.read(&mut buf).unwrap();

    let get_cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key2.len(), key2);
    stream.write_all(get_cmd.as_bytes()).unwrap();
    let mut response_buf = vec![0u8; 4096];
    let n = stream.read(&mut response_buf).unwrap();
    let response = String::from_utf8_lossy(&response_buf[..n]);
    assert!(
        response.contains(&value2[..100]), // Check at least first part
        "Large value GET failed (zero-copy path)"
    );
}

/// Test mio with zero-copy enabled using multiple connections and pipelining.
#[test]
fn test_mio_zero_copy_8conn_p8_medium() {
    run_parameterized_test(TestConfig::with_zero_copy("mio", "enabled", 8, 8, 1024));
}

/// Test mio with zero-copy enabled using larger values.
#[test]
fn test_mio_zero_copy_8conn_p8_large() {
    run_parameterized_test(TestConfig::with_zero_copy("mio", "enabled", 8, 8, 16384));
}

/// Test mio with zero-copy threshold mode with medium values (mix of copy and zero-copy).
#[test]
fn test_mio_zero_copy_threshold_8conn_p8_medium() {
    run_parameterized_test(TestConfig::with_zero_copy("mio", "threshold", 8, 8, 1024));
}

// =============================================================================
// Zero-copy mode tests (io_uring)
// =============================================================================

/// Test io_uring with zero-copy enabled for all GET responses.
#[test]
#[cfg(target_os = "linux")]
fn test_uring_zero_copy_enabled_basic() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_with_zero_copy(port, "uring", "enabled", 1);
    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start with uring/zero_copy=enabled"
    );

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Test SET then GET (zero-copy should be used for GET)
    let key = "test_key";
    let value = "test_value_for_zero_copy";
    let set_cmd = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value
    );
    stream.write_all(set_cmd.as_bytes()).unwrap();
    let mut buf = [0u8; 64];
    let n = stream.read(&mut buf).expect("Failed to read SET response");
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(response.contains("OK"), "Expected OK, got: {}", response);

    // GET the value back
    let get_cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    stream.write_all(get_cmd.as_bytes()).unwrap();
    let mut response_buf = vec![0u8; 256];
    let n = stream
        .read(&mut response_buf)
        .expect("Failed to read GET response");
    let response = String::from_utf8_lossy(&response_buf[..n]);
    assert!(
        response.contains(value),
        "Expected value '{}' in response, got: {}",
        value,
        response
    );
}

/// Test io_uring with zero-copy enabled using multiple connections and pipelining.
#[test]
#[cfg(target_os = "linux")]
fn test_uring_zero_copy_8conn_p8_medium() {
    run_parameterized_test(TestConfig::with_zero_copy("uring", "enabled", 8, 8, 1024));
}

/// Test io_uring with zero-copy enabled using larger values.
#[test]
#[cfg(target_os = "linux")]
fn test_uring_zero_copy_8conn_p8_large() {
    run_parameterized_test(TestConfig::with_zero_copy("uring", "enabled", 8, 8, 16384));
}

/// Test io_uring with zero-copy threshold mode.
#[test]
#[cfg(target_os = "linux")]
fn test_uring_zero_copy_threshold_8conn_p8_medium() {
    run_parameterized_test(TestConfig::with_zero_copy("uring", "threshold", 8, 8, 1024));
}
