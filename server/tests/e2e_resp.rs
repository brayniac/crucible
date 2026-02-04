//! End-to-end tests for RESP protocol.
//!
//! Basic functional tests for Redis RESP protocol operations.

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

/// Wait for the server to be ready by polling the port.
fn wait_for_server(addr: SocketAddr, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_ok() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    false
}

/// Start a test server and return the shutdown flag.
fn start_test_server(cache_port: u16) -> (thread::JoinHandle<()>, Arc<AtomicBool>) {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    let handle = thread::spawn(move || {
        let config_str = format!(
            r#"
            [workers]
            threads = 2

            [cache]
            backend = "segment"
            heap_size = "64MB"
            segment_size = "1MB"
            hashtable_power = 18

            [[listener]]
            protocol = "resp"
            address = "127.0.0.1:{}"

            [metrics]
            address = "127.0.0.1:0"
            "#,
            cache_port
        );

        let config: server::Config = toml::from_str(&config_str).unwrap();
        let cache = segcache::SegCache::builder()
            .heap_size(config.cache.heap_size)
            .segment_size(config.cache.segment_size)
            .hashtable_power(config.cache.hashtable_power)
            .build()
            .unwrap();

        // Use a short drain timeout for tests
        let drain_timeout = Duration::from_millis(500);
        let _ = server::native::run(&config, cache, shutdown_clone, drain_timeout);
    });

    (handle, shutdown)
}

/// Stop the test server - closes connections first, then signals shutdown.
fn stop_test_server(handle: thread::JoinHandle<()>, shutdown: Arc<AtomicBool>) {
    shutdown.store(true, Ordering::SeqCst);
    // Wait a bit for server to shut down, but don't block forever
    thread::sleep(Duration::from_millis(100));
    // Drop the handle - the server thread will be cleaned up when the test exits
    drop(handle);
}

/// Send a RESP command and read the response.
fn send_command(stream: &mut TcpStream, cmd: &[u8]) -> Vec<u8> {
    stream.write_all(cmd).unwrap();
    stream.flush().unwrap();

    let mut response = vec![0u8; 4096];
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    match stream.read(&mut response) {
        Ok(n) => {
            response.truncate(n);
            response
        }
        Err(_) => Vec::new(),
    }
}

/// Build a RESP SET command.
fn set_cmd(key: &str, value: &str) -> Vec<u8> {
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
fn get_cmd(key: &str) -> Vec<u8> {
    format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).into_bytes()
}

/// Build a RESP DEL command.
fn del_cmd(key: &str) -> Vec<u8> {
    format!("*2\r\n$3\r\nDEL\r\n${}\r\n{}\r\n", key.len(), key).into_bytes()
}

/// Test basic PING command.
#[test]
fn test_ping() {
    let cache_port = get_available_port();
    let (handle, shutdown) = start_test_server(cache_port);

    let addr: SocketAddr = format!("127.0.0.1:{}", cache_port).parse().unwrap();
    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start within timeout"
    );

    let mut conn = TcpStream::connect(addr).expect("Failed to connect");
    conn.set_nodelay(true).unwrap();

    let resp = send_command(&mut conn, b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(&resp, b"+PONG\r\n", "PING should return PONG");

    // Close connection before stopping server
    drop(conn);
    stop_test_server(handle, shutdown);
}

/// Test SET and GET cycle.
#[test]
fn test_set_get() {
    let cache_port = get_available_port();
    let (handle, shutdown) = start_test_server(cache_port);

    let addr: SocketAddr = format!("127.0.0.1:{}", cache_port).parse().unwrap();
    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start within timeout"
    );

    let mut conn = TcpStream::connect(addr).expect("Failed to connect");
    conn.set_nodelay(true).unwrap();

    // SET
    let resp = send_command(&mut conn, &set_cmd("mykey", "myvalue"));
    assert_eq!(&resp, b"+OK\r\n", "SET should return OK");

    // GET
    let resp = send_command(&mut conn, &get_cmd("mykey"));
    assert_eq!(&resp, b"$7\r\nmyvalue\r\n", "GET should return the value");

    drop(conn);
    stop_test_server(handle, shutdown);
}

/// Test GET on non-existent key.
#[test]
fn test_get_nonexistent() {
    let cache_port = get_available_port();
    let (handle, shutdown) = start_test_server(cache_port);

    let addr: SocketAddr = format!("127.0.0.1:{}", cache_port).parse().unwrap();
    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start within timeout"
    );

    let mut conn = TcpStream::connect(addr).expect("Failed to connect");
    conn.set_nodelay(true).unwrap();

    let resp = send_command(&mut conn, &get_cmd("nonexistent"));
    assert_eq!(&resp, b"$-1\r\n", "GET on missing key should return nil");

    drop(conn);
    stop_test_server(handle, shutdown);
}

/// Test DEL command.
#[test]
fn test_del() {
    let cache_port = get_available_port();
    let (handle, shutdown) = start_test_server(cache_port);

    let addr: SocketAddr = format!("127.0.0.1:{}", cache_port).parse().unwrap();
    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start within timeout"
    );

    let mut conn = TcpStream::connect(addr).expect("Failed to connect");
    conn.set_nodelay(true).unwrap();

    // SET
    let resp = send_command(&mut conn, &set_cmd("delkey", "value"));
    assert_eq!(&resp, b"+OK\r\n");

    // DEL
    let resp = send_command(&mut conn, &del_cmd("delkey"));
    assert_eq!(&resp, b":1\r\n", "DEL should return count of deleted keys");

    // GET should return nil
    let resp = send_command(&mut conn, &get_cmd("delkey"));
    assert_eq!(&resp, b"$-1\r\n", "GET after DEL should return nil");

    drop(conn);
    stop_test_server(handle, shutdown);
}

/// Test multiple concurrent connections.
#[test]
fn test_concurrent_connections() {
    let cache_port = get_available_port();
    let (handle, shutdown) = start_test_server(cache_port);

    let addr: SocketAddr = format!("127.0.0.1:{}", cache_port).parse().unwrap();
    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start within timeout"
    );

    // Open multiple connections
    let mut connections: Vec<TcpStream> = (0..10)
        .map(|_| {
            let conn = TcpStream::connect(addr).expect("Failed to connect");
            conn.set_nodelay(true).unwrap();
            conn
        })
        .collect();

    // Each connection sets and gets its own key
    for (i, conn) in connections.iter_mut().enumerate() {
        let key = format!("key{}", i);
        let value = format!("value{}", i);

        let resp = send_command(conn, &set_cmd(&key, &value));
        assert_eq!(&resp, b"+OK\r\n", "SET from connection {} failed", i);

        let resp = send_command(conn, &get_cmd(&key));
        let expected = format!("${}\r\n{}\r\n", value.len(), value);
        assert_eq!(
            resp,
            expected.as_bytes(),
            "GET from connection {} returned wrong value",
            i
        );
    }

    // Verify cross-connection visibility
    for (i, conn) in connections.iter_mut().enumerate() {
        let other_key = format!("key{}", (i + 5) % 10);
        let other_value = format!("value{}", (i + 5) % 10);

        let resp = send_command(conn, &get_cmd(&other_key));
        let expected = format!("${}\r\n{}\r\n", other_value.len(), other_value);
        assert_eq!(
            resp,
            expected.as_bytes(),
            "Cross-connection GET failed for key {}",
            other_key
        );
    }

    // Close all connections before stopping
    drop(connections);
    stop_test_server(handle, shutdown);
}
