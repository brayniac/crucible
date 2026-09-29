//! Integration tests for graceful shutdown.
//!
//! Tests that the server shuts down gracefully when signaled.

use serial_test::serial;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Get an available port for testing.
fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Start a test server and return the shutdown flag.
fn start_test_server(
    cache_port: u16,
    admin_port: u16,
) -> (thread::JoinHandle<()>, Arc<AtomicBool>) {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    let handle = thread::spawn(move || {
        let config_str = format!(
            r#"
            [workers]
            threads = 1

            [shutdown]
            drain_timeout_secs = 1

            [cache]
            backend = "segment"
            heap_size = "16MB"
            segment_size = "1MB"
            hashtable_power = 16

            [[listener]]
            protocol = "resp"
            address = "127.0.0.1:{}"

            [metrics]
            address = "127.0.0.1:{}"
            "#,
            cache_port, admin_port
        );

        let config: server::Config = toml::from_str(&config_str).unwrap();
        let cache = segcache::SegCache::builder()
            .heap_size(config.cache.heap_size)
            .segment_size(config.cache.segment_size)
            .hashtable_power(config.cache.hashtable_power)
            .build()
            .unwrap();

        let drain_timeout = Duration::from_secs(config.shutdown.drain_timeout_secs);

        // Run server
        let _ = server::async_native::run(&config, cache, shutdown_clone, drain_timeout);
    });

    (handle, shutdown)
}

/// Send a RESP PING command and verify the response.
fn send_ping(stream: &mut TcpStream) -> bool {
    let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
    if stream.write_all(ping_cmd).is_err() {
        return false;
    }
    if stream.flush().is_err() {
        return false;
    }

    let mut response = vec![0u8; 64];
    stream.set_read_timeout(Some(Duration::from_secs(1))).ok();

    match stream.read(&mut response) {
        Ok(n) if n > 0 => {
            response.truncate(n);
            response.starts_with(b"+PONG")
        }
        _ => false,
    }
}

/// Test that server responds before shutdown.
#[test]
#[serial]
fn test_server_responds_before_shutdown() {
    let cache_port = get_available_port();
    let admin_port = get_available_port();

    let (handle, shutdown) = start_test_server(cache_port, admin_port);

    // Give server time to start
    thread::sleep(Duration::from_millis(200));

    let addr: SocketAddr = format!("127.0.0.1:{}", cache_port).parse().unwrap();

    // Verify we can connect before shutdown
    let mut conn = TcpStream::connect(addr).expect("Should connect before shutdown");
    conn.set_nodelay(true).unwrap();
    assert!(send_ping(&mut conn), "PING should work before shutdown");

    // Close connection
    drop(conn);

    // Signal shutdown
    shutdown.store(true, Ordering::SeqCst);

    // Wait for server to shut down (with timeout)
    let start = Instant::now();
    while !handle.is_finished() && start.elapsed() < Duration::from_secs(3) {
        thread::sleep(Duration::from_millis(50));
    }

    // Server should have stopped
    assert!(
        handle.is_finished() || start.elapsed() < Duration::from_secs(3),
        "Server should stop within drain timeout"
    );

    drop(handle);
}

/// Test that shutdown happens within the configured timeout.
#[test]
#[serial]
fn test_shutdown_timeout() {
    let cache_port = get_available_port();
    let admin_port = get_available_port();

    let (handle, shutdown) = start_test_server(cache_port, admin_port);

    // Give server time to start
    thread::sleep(Duration::from_millis(200));

    // Signal shutdown immediately
    let shutdown_time = Instant::now();
    shutdown.store(true, Ordering::SeqCst);

    // Wait for server to shut down
    let start = Instant::now();
    while !handle.is_finished() && start.elapsed() < Duration::from_secs(5) {
        thread::sleep(Duration::from_millis(50));
    }

    let shutdown_duration = shutdown_time.elapsed();

    // Server should stop within the drain timeout (1s) + some buffer
    assert!(
        shutdown_duration < Duration::from_secs(3),
        "Shutdown took too long: {:?}",
        shutdown_duration
    );

    drop(handle);
}

/// Send one RESP command and read one reply.
fn resp(stream: &mut TcpStream, args: &[&[u8]]) -> Vec<u8> {
    let mut cmd = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        cmd.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        cmd.extend_from_slice(a);
        cmd.extend_from_slice(b"\r\n");
    }
    stream.write_all(&cmd).unwrap();
    let mut reply = vec![0u8; 16 * 1024];
    let n = stream.read(&mut reply).unwrap();
    reply.truncate(n);
    reply
}

/// With `[cache.maintenance]` enabled the server evicts in the background,
/// serves reads, and still shuts down promptly: the maintenance thread
/// watches the same flag and is joined on the way out.
#[test]
#[serial]
fn test_shutdown_with_background_maintenance() {
    let cache_port = get_available_port();
    let admin_port = get_available_port();
    let shutdown = Arc::new(AtomicBool::new(false));
    let handle = {
        let shutdown = shutdown.clone();
        thread::spawn(move || {
            let config_str = format!(
                r#"
                [workers]
                threads = 1

                [shutdown]
                drain_timeout_secs = 1

                [cache]
                backend = "segment"
                heap_size = "8MB"
                segment_size = "256KB"
                max_value_size = "64KB"
                hashtable_power = 16

                [cache.maintenance]
                enabled = true
                interval_us = 200
                free_segments = 4

                [[listener]]
                protocol = "resp"
                address = "127.0.0.1:{cache_port}"

                [metrics]
                address = "127.0.0.1:{admin_port}"
                "#
            );
            let config: server::Config = toml::from_str(&config_str).unwrap();
            config.validate().unwrap();
            let cache = segcache::SegCache::builder()
                .heap_size(config.cache.heap_size)
                .segment_size(config.cache.segment_size)
                .hashtable_power(config.cache.hashtable_power)
                .build()
                .unwrap();
            let drain = Duration::from_secs(config.shutdown.drain_timeout_secs);
            let _ = server::async_native::run(&config, cache, shutdown, drain);
        })
    };
    thread::sleep(Duration::from_millis(200));

    let addr: SocketAddr = format!("127.0.0.1:{cache_port}").parse().unwrap();
    let mut conn = TcpStream::connect(addr).expect("connect");
    conn.set_nodelay(true).unwrap();
    conn.set_read_timeout(Some(Duration::from_secs(5))).ok();

    // 12MB of values into an 8MB heap: eviction has to run.
    let value = vec![b'v'; 4096];
    let n = 3000;
    for i in 0..n {
        let reply = resp(&mut conn, &[b"SET", format!("k{i}").as_bytes(), &value]);
        assert!(
            reply.starts_with(b"+OK"),
            "SET k{i}: {:?}",
            String::from_utf8_lossy(&reply)
        );
    }
    for i in n - 10..n {
        let reply = resp(&mut conn, &[b"GET", format!("k{i}").as_bytes()]);
        assert!(
            reply.starts_with(b"$4096\r\n"),
            "GET k{i}: {} bytes",
            reply.len()
        );
    }
    drop(conn);

    shutdown.store(true, Ordering::SeqCst);
    let start = Instant::now();
    while !handle.is_finished() && start.elapsed() < Duration::from_secs(3) {
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        handle.is_finished(),
        "server with maintenance did not shut down within 3s"
    );
    handle.join().expect("server thread panicked");
}
