//! Integration tests for the async server (krio AsyncEventHandler).
//!
//! Mirrors a subset of `io_engine_integration.rs` but uses
//! `server::async_native::run()` instead of `server::native::run()`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

// ── Helpers (shared with io_engine_integration.rs) ──────────────────────

fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn wait_for_server(addr: SocketAddr, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
            if stream.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 32];
                if let Ok(n) = stream.read(&mut buf) {
                    if n >= 5 && &buf[..5] == b"+PONG" {
                        return true;
                    }
                }
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

fn start_async_test_server(port: u16, worker_threads: usize) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let config_str = format!(
            r#"
            [workers]
            threads = {worker_threads}

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

        let shutdown = Arc::new(AtomicBool::new(false));
        let drain_timeout = Duration::from_secs(5);

        let _ = server::async_native::run(&config, cache, shutdown, drain_timeout);
    })
}

fn generate_value(size: usize) -> String {
    let pattern = "abcdefghijklmnopqrstuvwxyz0123456789";
    pattern.chars().cycle().take(size).collect()
}

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

fn build_get_command(key: &str) -> Vec<u8> {
    format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).into_bytes()
}

fn build_ping_command() -> Vec<u8> {
    b"*1\r\n$4\r\nPING\r\n".to_vec()
}

fn build_pipelined_sets(key_prefix: &str, value: &str, count: usize) -> Vec<u8> {
    let mut commands = Vec::new();
    for i in 0..count {
        let key = format!("{}:{}", key_prefix, i);
        commands.extend(build_set_command(&key, value));
    }
    commands
}

fn build_pipelined_gets(key_prefix: &str, count: usize) -> Vec<u8> {
    let mut commands = Vec::new();
    for i in 0..count {
        let key = format!("{}:{}", key_prefix, i);
        commands.extend(build_get_command(&key));
    }
    commands
}

fn read_responses(stream: &mut TcpStream, expected_count: usize) -> Vec<u8> {
    let mut buffer = vec![0u8; 64 * 1024];
    let mut total = Vec::new();
    let mut responses_found = 0;

    let start = Instant::now();
    let timeout = Duration::from_secs(10);

    while responses_found < expected_count && start.elapsed() < timeout {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => {
                total.extend_from_slice(&buffer[..n]);
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

fn count_resp_responses(data: &[u8]) -> usize {
    let mut count = 0;
    let mut pos = 0;

    while pos < data.len() {
        match data[pos] {
            b'+' | b'-' | b':' => {
                if let Some(end) = find_crlf(data, pos) {
                    count += 1;
                    pos = end + 2;
                } else {
                    break;
                }
            }
            b'$' => {
                if let Some(header_end) = find_crlf(data, pos) {
                    let len_bytes = &data[pos + 1..header_end];
                    if let Ok(s) = std::str::from_utf8(len_bytes) {
                        if let Ok(len) = s.parse::<i64>() {
                            if len < 0 {
                                count += 1;
                                pos = header_end + 2;
                            } else {
                                let body_end = header_end + 2 + len as usize + 2;
                                if body_end <= data.len() {
                                    count += 1;
                                    pos = body_end;
                                } else {
                                    break;
                                }
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            _ => break,
        }
    }

    count
}

fn find_crlf(data: &[u8], from: usize) -> Option<usize> {
    if data.len() < from + 2 {
        return None;
    }
    data[from..data.len() - 1]
        .iter()
        .position(|&b| b == b'\r')
        .and_then(|i| {
            if data[from + i + 1] == b'\n' {
                Some(from + i)
            } else {
                None
            }
        })
}

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

fn send_get(stream: &mut TcpStream, key: &str, expected_value: &str) -> bool {
    let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    if stream.write_all(cmd.as_bytes()).is_err() {
        return false;
    }

    let expected_len = expected_value.len() + 20;
    let mut buf = vec![0u8; expected_len];
    let mut total_read = 0;

    while total_read < expected_len {
        match stream.read(&mut buf[total_read..]) {
            Ok(0) => break,
            Ok(n) => {
                total_read += n;
                if total_read >= 2 && buf[total_read - 2] == b'\r' && buf[total_read - 1] == b'\n' {
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

// ── Parameterized test runner ───────────────────────────────────────────

#[derive(Clone, Debug)]
struct TestConfig {
    connections: usize,
    pipeline_depth: usize,
    value_size: usize,
}

impl TestConfig {
    fn new(connections: usize, pipeline_depth: usize, value_size: usize) -> Self {
        Self {
            connections,
            pipeline_depth,
            value_size,
        }
    }
}

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
    let key_prefix = format!("async_w{}:key", worker_id);

    let mut completed = 0;
    let ops_per_batch = config.pipeline_depth;

    while completed < operations && running.load(Ordering::Relaxed) {
        let batch_size = std::cmp::min(ops_per_batch, operations - completed);

        let set_commands = build_pipelined_sets(&key_prefix, &value, batch_size);
        if stream.write_all(&set_commands).is_err() {
            break;
        }

        let responses = read_responses(&mut stream, batch_size);
        let ok_count = String::from_utf8_lossy(&responses).matches("+OK").count();

        if ok_count != batch_size {
            eprintln!(
                "Worker {}: expected {} OKs, got {}",
                worker_id, batch_size, ok_count
            );
        }

        let get_commands = build_pipelined_gets(&key_prefix, batch_size);
        if stream.write_all(&get_commands).is_err() {
            break;
        }

        let responses = read_responses(&mut stream, batch_size);
        let response_str = String::from_utf8_lossy(&responses);
        let value_count = response_str
            .matches(&value[..std::cmp::min(32, value.len())])
            .count();

        if value_count >= batch_size {
            success_count.fetch_add(batch_size * 2, Ordering::Relaxed);
        }

        completed += batch_size;
    }
}

fn run_async_parameterized_test(config: TestConfig) {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let worker_threads = (config.connections / 64).clamp(1, 4);

    let _server_handle = start_async_test_server(port, worker_threads);

    assert!(
        wait_for_server(addr, Duration::from_secs(10)),
        "Async server failed to start"
    );

    let success_count = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let operations_per_connection = 100;

    let mut handles = Vec::new();
    for worker_id in 0..config.connections {
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

    for handle in handles {
        let _ = handle.join();
    }

    running.store(false, Ordering::Relaxed);

    let total_success = success_count.load(Ordering::Relaxed);
    let expected_min = (config.connections * operations_per_connection * 2) / 2;

    assert!(
        total_success >= expected_min,
        "{} connections, pipeline {}, value size {}: \
         expected at least {} successful ops, got {}",
        config.connections,
        config.pipeline_depth,
        config.value_size,
        expected_min,
        total_success
    );
}

// ── Tests ───────────────────────────────────────────────────────────────

#[test]
fn test_async_basic() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_async_test_server(port, 1);

    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Async server failed to start"
    );

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // PING
    assert!(send_ping(&mut stream), "PING failed");

    // SET
    assert!(
        send_set(&mut stream, "async_key", "async_value"),
        "SET failed"
    );

    // GET
    assert!(
        send_get(&mut stream, "async_key", "async_value"),
        "GET failed"
    );

    // Pipelined PINGs
    let pipeline = "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
    stream.write_all(pipeline.as_bytes()).unwrap();

    let mut buf = [0u8; 256];
    let n = stream.read(&mut buf).unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    let pong_count = response.matches("PONG").count();
    assert!(
        pong_count >= 3,
        "Pipeline failed: expected 3 PONGs, got {}",
        pong_count
    );

    drop(stream);
}

#[test]
fn test_async_1conn_p1_small() {
    run_async_parameterized_test(TestConfig::new(1, 1, 64));
}

#[test]
fn test_async_8conn_p8_medium() {
    run_async_parameterized_test(TestConfig::new(8, 8, 1024));
}

#[test]
fn test_async_8conn_p8_large() {
    run_async_parameterized_test(TestConfig::new(8, 8, 16384));
}

#[test]
fn test_async_large_objects() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_async_test_server(port, 1);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    for &size in &[64, 256, 1024, 4096, 16384, 65536] {
        let key = format!("async_largekey_{}", size);
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
fn test_async_connection_churn() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_async_test_server(port, 2);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    for i in 0..100 {
        let mut stream = TcpStream::connect(addr).expect("Failed to connect");
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let key = format!("async_churn_key_{}", i);
        let value = format!("async_churn_value_{}", i);

        assert!(
            send_set(&mut stream, &key, &value),
            "SET failed on iteration {}",
            i
        );
        assert!(send_ping(&mut stream), "PING failed on iteration {}", i);

        drop(stream);
    }
}

#[test]
fn test_async_deep_pipeline() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_async_test_server(port, 1);
    assert!(wait_for_server(addr, Duration::from_secs(5)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .unwrap();

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
