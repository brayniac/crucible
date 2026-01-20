//! Integration tests for connection handling.
//!
//! These tests verify that connections are correctly tracked even after
//! connections are opened and closed, ensuring no ConnId/state mismatch.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::Duration;

/// Get an available port for testing.
fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Start a test server on the given port and return a handle to stop it.
fn start_test_server(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let config_str = format!(
            r#"
            runtime = "native"

            [workers]
            threads = 1

            [cache]
            backend = "segment"
            heap_size = "16MB"
            segment_size = "1MB"
            hashtable_power = 16

            [[listener]]
            protocol = "resp"
            address = "127.0.0.1:{}"

            [metrics]
            address = "127.0.0.1:0"
            "#,
            port
        );

        let config: server::Config = toml::from_str(&config_str).unwrap();
        let cache = segcache::SegCache::builder()
            .heap_size(config.cache.heap_size)
            .segment_size(config.cache.segment_size)
            .hashtable_power(config.cache.hashtable_power)
            .build()
            .unwrap();

        // Run server (this blocks, but we'll kill the thread)
        let _ = server::native::run(&config, cache);
    })
}

/// Send a RESP command and read the response.
fn send_resp_command(stream: &mut TcpStream, cmd: &[u8]) -> Vec<u8> {
    stream.write_all(cmd).unwrap();
    stream.flush().unwrap();

    let mut response = vec![0u8; 1024];
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    match stream.read(&mut response) {
        Ok(n) => response.truncate(n),
        Err(_) => response.clear(),
    }

    response
}

/// Test that connections work correctly after other connections are closed.
///
/// This tests the bug where using a Slab for connection storage caused
/// ConnId/index mismatches after connections were closed and new ones opened.
#[test]
fn test_connection_id_tracking() {
    let port = get_available_port();
    let _server = start_test_server(port);

    // Give the server time to start
    thread::sleep(Duration::from_millis(200));

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Phase 1: Open initial connections
    let mut conn1 = TcpStream::connect(addr).unwrap();
    let mut conn2 = TcpStream::connect(addr).unwrap();
    let mut conn3 = TcpStream::connect(addr).unwrap();

    conn1.set_nodelay(true).unwrap();
    conn2.set_nodelay(true).unwrap();
    conn3.set_nodelay(true).unwrap();

    // Verify all connections work
    let ping_cmd = b"*1\r\n$4\r\nPING\r\n";

    let resp1 = send_resp_command(&mut conn1, ping_cmd);
    assert_eq!(&resp1, b"+PONG\r\n", "conn1 PING failed");

    let resp2 = send_resp_command(&mut conn2, ping_cmd);
    assert_eq!(&resp2, b"+PONG\r\n", "conn2 PING failed");

    let resp3 = send_resp_command(&mut conn3, ping_cmd);
    assert_eq!(&resp3, b"+PONG\r\n", "conn3 PING failed");

    // Phase 2: Close some connections
    drop(conn1);
    drop(conn2);

    // Give server time to process the closes
    thread::sleep(Duration::from_millis(100));

    // Phase 3: Open new connections (these will have new ConnIds)
    let mut conn4 = TcpStream::connect(addr).unwrap();
    let mut conn5 = TcpStream::connect(addr).unwrap();

    conn4.set_nodelay(true).unwrap();
    conn5.set_nodelay(true).unwrap();

    // The bug was: conn4/conn5 would have ConnIds like 3,4 but with a Slab,
    // they might be stored at indices 0,1 (reused from closed conn1,conn2).
    // When receiving data for ConnId 3, we'd access Slab[3] which is wrong.

    // Verify new connections work correctly
    let resp4 = send_resp_command(&mut conn4, ping_cmd);
    assert_eq!(&resp4, b"+PONG\r\n", "conn4 PING failed after reuse");

    let resp5 = send_resp_command(&mut conn5, ping_cmd);
    assert_eq!(&resp5, b"+PONG\r\n", "conn5 PING failed after reuse");

    // conn3 should still work
    let resp3_again = send_resp_command(&mut conn3, ping_cmd);
    assert_eq!(
        &resp3_again, b"+PONG\r\n",
        "conn3 PING failed after siblings closed"
    );

    // Phase 4: Test SET/GET to ensure data isn't corrupted
    let set_cmd = b"*3\r\n$3\r\nSET\r\n$8\r\ntestkey1\r\n$6\r\nvalue1\r\n";
    let get_cmd = b"*2\r\n$3\r\nGET\r\n$8\r\ntestkey1\r\n";

    let set_resp = send_resp_command(&mut conn4, set_cmd);
    assert_eq!(&set_resp, b"+OK\r\n", "SET failed");

    let get_resp = send_resp_command(&mut conn5, get_cmd);
    assert_eq!(&get_resp, b"$6\r\nvalue1\r\n", "GET returned wrong value");

    // Phase 5: More aggressive connection churn
    for i in 0..10 {
        let mut temp_conn = TcpStream::connect(addr).unwrap();
        temp_conn.set_nodelay(true).unwrap();

        let key = format!("key{}", i);
        let value = format!("value{}", i);

        let set_cmd = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            value.len(),
            value
        );

        let resp = send_resp_command(&mut temp_conn, set_cmd.as_bytes());
        assert_eq!(&resp, b"+OK\r\n", "SET failed during churn iteration {}", i);

        // Close connection
        drop(temp_conn);
    }

    // Verify original conn3 still works after all that churn
    let final_resp = send_resp_command(&mut conn3, ping_cmd);
    assert_eq!(
        &final_resp, b"+PONG\r\n",
        "conn3 failed after connection churn"
    );

    // Verify data written during churn is accessible
    let get_key5_cmd = b"*2\r\n$3\r\nGET\r\n$4\r\nkey5\r\n";
    let get_resp = send_resp_command(&mut conn3, get_key5_cmd);
    assert_eq!(
        &get_resp, b"$6\r\nvalue5\r\n",
        "Data from churn not accessible"
    );
}

/// Test rapid connection open/close cycles.
#[test]
fn test_rapid_connection_cycles() {
    let port = get_available_port();
    let _server = start_test_server(port);

    thread::sleep(Duration::from_millis(200));

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let ping_cmd = b"*1\r\n$4\r\nPING\r\n";

    // Rapidly open, use, and close connections
    for i in 0..50 {
        let mut conn = match TcpStream::connect(addr) {
            Ok(c) => c,
            Err(e) => {
                // Server might be slow, retry
                thread::sleep(Duration::from_millis(10));
                TcpStream::connect(addr)
                    .unwrap_or_else(|_| panic!("Failed to connect on iteration {}: {}", i, e))
            }
        };

        conn.set_nodelay(true).unwrap();

        let resp = send_resp_command(&mut conn, ping_cmd);
        assert!(
            resp.starts_with(b"+PONG") || resp.starts_with(b"+"),
            "Iteration {}: got unexpected response: {:?}",
            i,
            String::from_utf8_lossy(&resp)
        );

        // Explicitly close
        drop(conn);
    }
}
