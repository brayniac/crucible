//! Integration tests for the admin server.
//!
//! Tests health check, readiness, and metrics endpoints.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

/// Get an available port for testing.
fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Start just the admin server (without the cache server).
fn start_admin_server(admin_port: u16) -> (thread::JoinHandle<()>, Arc<AtomicBool>) {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    let handle = thread::spawn(move || {
        let addr: SocketAddr = format!("127.0.0.1:{}", admin_port).parse().unwrap();

        let admin_handle = server::admin::start(server::admin::AdminConfig {
            address: addr,
            shutdown: shutdown_clone.clone(),
        })
        .unwrap();

        // Wait for shutdown signal
        while !shutdown_clone.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(50));
        }

        admin_handle.shutdown();
    });

    (handle, shutdown)
}

/// Stop the admin server.
fn stop_server(handle: thread::JoinHandle<()>, shutdown: Arc<AtomicBool>) {
    shutdown.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(200));
    drop(handle);
}

/// Send an HTTP GET request and return the response.
fn http_get(addr: SocketAddr, path: &str) -> Result<(u16, String), std::io::Error> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, addr
    );
    stream.write_all(request.as_bytes())?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;

    // Parse HTTP response
    let status_line = response.lines().next().unwrap_or("");
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Find body (after empty line)
    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();

    Ok((status_code, body))
}

/// Test the /health endpoint returns 200 OK.
#[test]
fn test_health_endpoint() {
    let admin_port = get_available_port();

    let (handle, shutdown) = start_admin_server(admin_port);

    // Give server time to start
    thread::sleep(Duration::from_millis(200));

    let addr: SocketAddr = format!("127.0.0.1:{}", admin_port).parse().unwrap();

    let (status, body) = http_get(addr, "/health").expect("Failed to connect to admin server");

    assert_eq!(status, 200, "Health check should return 200");
    assert_eq!(body.trim(), "OK", "Health check should return 'OK'");

    stop_server(handle, shutdown);
}

/// Test the /ready endpoint returns 200 OK when server is running.
#[test]
fn test_ready_endpoint() {
    let admin_port = get_available_port();

    let (handle, shutdown) = start_admin_server(admin_port);

    // Give server time to start
    thread::sleep(Duration::from_millis(200));

    let addr: SocketAddr = format!("127.0.0.1:{}", admin_port).parse().unwrap();

    let (status, body) = http_get(addr, "/ready").expect("Failed to connect to admin server");

    assert_eq!(status, 200, "Ready check should return 200 when running");
    assert_eq!(body.trim(), "OK", "Ready check should return 'OK'");

    stop_server(handle, shutdown);
}

/// Test the /metrics endpoint returns Prometheus-formatted metrics.
#[test]
fn test_metrics_endpoint() {
    let admin_port = get_available_port();

    let (handle, shutdown) = start_admin_server(admin_port);

    // Give server time to start
    thread::sleep(Duration::from_millis(200));

    let addr: SocketAddr = format!("127.0.0.1:{}", admin_port).parse().unwrap();

    let (status, body) = http_get(addr, "/metrics").expect("Failed to connect to admin server");

    assert_eq!(status, 200, "Metrics endpoint should return 200");

    // Verify response is valid (either has content or is empty)
    // Empty is fine if no metrics have been registered yet
    assert!(
        body.is_empty()
            || body.contains("# TYPE")
            || body.contains("_count")
            || body.contains("_sum"),
        "Metrics should be empty or contain Prometheus-formatted content"
    );

    stop_server(handle, shutdown);
}
