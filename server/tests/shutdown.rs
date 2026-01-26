//! Integration tests for graceful shutdown.
//!
//! Tests that the server shuts down gracefully when signaled.

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
            runtime = "native"

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
        let _ = server::native::run(&config, cache, shutdown_clone, drain_timeout);
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
