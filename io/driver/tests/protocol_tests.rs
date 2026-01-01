//! Integration tests using a simple line-based protocol.
//!
//! These tests exercise the io-driver with realistic protocol patterns including:
//! - Request/response cycles
//! - Pipelined requests
//! - Backpressure handling
//! - Connection lifecycle
//! - Error recovery
//! - Multiple concurrent connections

use io_driver::mio::MioDriver;
use io_driver::{CompletionKind, IoDriver};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

/// Global port counter to avoid port conflicts between tests.
static PORT_COUNTER: AtomicU16 = AtomicU16::new(20000);

fn get_test_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
}

fn get_test_addr() -> SocketAddr {
    format!("127.0.0.1:{}", get_test_port()).parse().unwrap()
}

/// Simple line-based protocol for testing.
/// Format: "COMMAND arg1 arg2\n"
/// Response: "OK result\n" or "ERR message\n"
mod protocol {
    pub const CMD_PING: &[u8] = b"PING\n";
    pub const CMD_QUIT: &[u8] = b"QUIT\n";

    pub const RESP_PONG: &[u8] = b"PONG\n";
    pub const RESP_OK: &[u8] = b"OK ";
    pub const RESP_BYE: &[u8] = b"BYE\n";
    pub const RESP_ERR: &[u8] = b"ERR ";

    /// Parse a command from a buffer, returns (command, consumed_bytes) or None if incomplete.
    pub fn parse_command(buf: &[u8]) -> Option<(Command, usize)> {
        let newline_pos = buf.iter().position(|&b| b == b'\n')?;
        let line = &buf[..newline_pos];

        let cmd = if line == b"PING" {
            Command::Ping
        } else if line == b"QUIT" {
            Command::Quit
        } else if line.starts_with(b"ECHO ") {
            Command::Echo(line[5..].to_vec())
        } else {
            Command::Unknown(line.to_vec())
        };

        Some((cmd, newline_pos + 1))
    }

    #[derive(Debug, Clone)]
    pub enum Command {
        Ping,
        Echo(Vec<u8>),
        Quit,
        Unknown(Vec<u8>),
    }

    /// Generate response for a command.
    pub fn generate_response(cmd: &Command) -> Vec<u8> {
        match cmd {
            Command::Ping => RESP_PONG.to_vec(),
            Command::Echo(data) => {
                let mut resp = RESP_OK.to_vec();
                resp.extend_from_slice(data);
                resp.push(b'\n');
                resp
            }
            Command::Quit => RESP_BYE.to_vec(),
            Command::Unknown(data) => {
                let mut resp = RESP_ERR.to_vec();
                resp.extend_from_slice(b"unknown command: ");
                resp.extend_from_slice(data);
                resp.push(b'\n');
                resp
            }
        }
    }
}

/// Connection state for our test server.
struct TestConnection {
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    write_pos: usize,
    should_close: bool,
}

impl TestConnection {
    fn new() -> Self {
        Self {
            read_buf: Vec::with_capacity(4096),
            write_buf: Vec::with_capacity(4096),
            write_pos: 0,
            should_close: false,
        }
    }

    fn append_recv(&mut self, data: &[u8]) {
        self.read_buf.extend_from_slice(data);
    }

    fn process(&mut self) {
        while let Some((cmd, consumed)) = protocol::parse_command(&self.read_buf) {
            // Generate response
            let response = protocol::generate_response(&cmd);
            self.write_buf.extend_from_slice(&response);

            // Check for quit
            if matches!(cmd, protocol::Command::Quit) {
                self.should_close = true;
            }

            // Remove processed data
            self.read_buf.drain(..consumed);
        }
    }

    fn has_pending_write(&self) -> bool {
        self.write_pos < self.write_buf.len()
    }

    fn pending_write_data(&self) -> &[u8] {
        &self.write_buf[self.write_pos..]
    }

    fn advance_write(&mut self, n: usize) {
        self.write_pos += n;
        if self.write_pos >= self.write_buf.len() {
            self.write_buf.clear();
            self.write_pos = 0;
        }
    }
}

/// Run a simple test server using the driver.
fn run_test_server(
    driver: &mut MioDriver,
    connections: &mut HashMap<usize, TestConnection>,
    iterations: usize,
) {
    for _ in 0..iterations {
        driver.poll(Some(Duration::from_millis(10))).unwrap();

        for completion in driver.drain_completions() {
            match completion.kind {
                CompletionKind::Accept { conn_id, .. } => {
                    connections.insert(conn_id.as_usize(), TestConnection::new());
                }

                CompletionKind::Recv { conn_id } => {
                    let idx = conn_id.as_usize();
                    let mut recv_buf = [0u8; 4096];
                    let mut need_close = false;

                    loop {
                        let Some(conn) = connections.get_mut(&idx) else {
                            break;
                        };

                        match driver.recv(conn_id, &mut recv_buf) {
                            Ok(0) => {
                                need_close = true;
                                break;
                            }
                            Ok(n) => {
                                conn.append_recv(&recv_buf[..n]);
                                conn.process();

                                // Try to send responses
                                while conn.has_pending_write() {
                                    let data = conn.pending_write_data();
                                    match driver.send(conn_id, data) {
                                        Ok(n) => conn.advance_write(n),
                                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                            break;
                                        }
                                        Err(_) => {
                                            need_close = true;
                                            break;
                                        }
                                    }
                                }

                                if conn.should_close {
                                    need_close = true;
                                    break;
                                }
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                            Err(_) => {
                                need_close = true;
                                break;
                            }
                        }
                    }

                    if need_close {
                        connections.remove(&idx);
                        let _ = driver.close(conn_id);
                    }
                }

                CompletionKind::SendReady { conn_id } => {
                    let idx = conn_id.as_usize();
                    if let Some(conn) = connections.get_mut(&idx) {
                        while conn.has_pending_write() {
                            let data = conn.pending_write_data();
                            match driver.send(conn_id, data) {
                                Ok(n) => conn.advance_write(n),
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                Err(_) => {
                                    connections.remove(&idx);
                                    let _ = driver.close(conn_id);
                                    break;
                                }
                            }
                        }
                    }
                }

                CompletionKind::Closed { conn_id } | CompletionKind::Error { conn_id, .. } => {
                    connections.remove(&conn_id.as_usize());
                    let _ = driver.close(conn_id);
                }

                CompletionKind::ListenerError { .. } => {}
            }
        }
    }
}

// =============================================================================
// Basic Protocol Tests
// =============================================================================

#[test]
fn test_ping_pong() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let mut connections = HashMap::new();

    // Send PING
    client.write_all(protocol::CMD_PING).unwrap();

    // Run server to process
    run_test_server(&mut driver, &mut connections, 10);

    // Read response
    let mut buf = [0u8; 1024];
    let n = client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], protocol::RESP_PONG);
}

#[test]
fn test_echo() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let mut connections = HashMap::new();

    // Send ECHO command
    client.write_all(b"ECHO hello world\n").unwrap();

    // Run server
    run_test_server(&mut driver, &mut connections, 10);

    // Read response
    let mut buf = [0u8; 1024];
    let n = client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"OK hello world\n");
}

#[test]
fn test_quit() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let mut connections = HashMap::new();

    // Accept and establish connection
    run_test_server(&mut driver, &mut connections, 5);
    assert_eq!(connections.len(), 1);

    // Send QUIT
    client.write_all(protocol::CMD_QUIT).unwrap();

    // Run server - should close connection
    run_test_server(&mut driver, &mut connections, 10);

    // Connection should be closed
    assert_eq!(connections.len(), 0);

    // Client should see connection closed
    let mut buf = [0u8; 1024];
    let result = client.read(&mut buf);
    // Either got BYE response and then EOF, or just EOF
    match result {
        Ok(0) => {} // EOF
        Ok(n) => assert_eq!(&buf[..n], protocol::RESP_BYE),
        Err(_) => {} // Connection reset
    }
}

#[test]
fn test_unknown_command() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let mut connections = HashMap::new();

    // Send unknown command
    client.write_all(b"FOOBAR test\n").unwrap();

    // Run server
    run_test_server(&mut driver, &mut connections, 10);

    // Read response - should be error
    let mut buf = [0u8; 1024];
    let n = client.read(&mut buf).unwrap();
    assert!(buf[..n].starts_with(protocol::RESP_ERR));
}

// =============================================================================
// Pipelining Tests
// =============================================================================

#[test]
fn test_pipelined_requests() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let mut connections = HashMap::new();

    // Send multiple commands at once (pipelined)
    client.write_all(b"PING\nPING\nPING\n").unwrap();

    // Run server
    run_test_server(&mut driver, &mut connections, 10);

    // Read all responses
    let mut buf = [0u8; 1024];
    let n = client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"PONG\nPONG\nPONG\n");
}

#[test]
fn test_pipelined_mixed_commands() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let mut connections = HashMap::new();

    // Send mixed commands pipelined
    client
        .write_all(b"PING\nECHO foo\nECHO bar\nPING\n")
        .unwrap();

    // Run server
    run_test_server(&mut driver, &mut connections, 10);

    // Read all responses
    let mut buf = [0u8; 1024];
    let n = client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"PONG\nOK foo\nOK bar\nPONG\n");
}

#[test]
fn test_partial_command_then_complete() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let mut connections = HashMap::new();

    // Send partial command
    client.write_all(b"PIN").unwrap();
    run_test_server(&mut driver, &mut connections, 5);

    // Complete the command
    client.write_all(b"G\n").unwrap();
    run_test_server(&mut driver, &mut connections, 10);

    // Should get response
    let mut buf = [0u8; 1024];
    let n = client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], protocol::RESP_PONG);
}

#[test]
fn test_many_pipelined_requests() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let mut connections = HashMap::new();

    // Send many pipelined requests
    let num_requests = 1000;
    let mut requests = Vec::new();
    for _ in 0..num_requests {
        requests.extend_from_slice(b"PING\n");
    }
    client.write_all(&requests).unwrap();

    // Run server enough iterations to process all
    run_test_server(&mut driver, &mut connections, 100);

    // Read all responses
    let mut total_received = 0;
    let expected_response = b"PONG\n";
    let expected_total = num_requests * expected_response.len();
    let mut buf = vec![0u8; expected_total + 1024];

    while total_received < expected_total {
        match client.read(&mut buf[total_received..]) {
            Ok(0) => break,
            Ok(n) => total_received += n,
            Err(_) => break,
        }
    }

    assert_eq!(total_received, expected_total);

    // Verify all responses are PONG
    for chunk in buf[..total_received].chunks(5) {
        assert_eq!(chunk, expected_response);
    }
}

// =============================================================================
// Multiple Connections Tests
// =============================================================================

#[test]
fn test_multiple_concurrent_connections() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let num_clients = 10;
    let mut clients: Vec<TcpStream> = Vec::new();

    for _ in 0..num_clients {
        if let Ok(client) = TcpStream::connect(addr) {
            client.set_nonblocking(false).unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            clients.push(client);
        }
    }

    if clients.is_empty() {
        return;
    }

    let mut connections = HashMap::new();

    // Accept all connections
    run_test_server(&mut driver, &mut connections, 20);
    assert_eq!(connections.len(), clients.len());

    // Each client sends a unique message
    for (i, client) in clients.iter_mut().enumerate() {
        let msg = format!("ECHO client{}\n", i);
        client.write_all(msg.as_bytes()).unwrap();
    }

    // Run server to process all
    run_test_server(&mut driver, &mut connections, 30);

    // Each client should receive their response
    for (i, client) in clients.iter_mut().enumerate() {
        let mut buf = [0u8; 1024];
        let n = client.read(&mut buf).unwrap();
        let expected = format!("OK client{}\n", i);
        assert_eq!(&buf[..n], expected.as_bytes());
    }
}

#[test]
fn test_connection_interleaved_requests() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client1 = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client1.set_nonblocking(false).unwrap();
    client1
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let mut client2 = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client2.set_nonblocking(false).unwrap();
    client2
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let mut connections = HashMap::new();

    // Accept both connections
    run_test_server(&mut driver, &mut connections, 10);
    assert_eq!(connections.len(), 2);

    // Interleave requests
    client1.write_all(b"ECHO first\n").unwrap();
    run_test_server(&mut driver, &mut connections, 5);

    client2.write_all(b"ECHO second\n").unwrap();
    run_test_server(&mut driver, &mut connections, 5);

    client1.write_all(b"PING\n").unwrap();
    run_test_server(&mut driver, &mut connections, 5);

    client2.write_all(b"PING\n").unwrap();
    run_test_server(&mut driver, &mut connections, 5);

    // Read responses
    let mut buf1 = [0u8; 1024];
    let n1 = client1.read(&mut buf1).unwrap();
    assert_eq!(&buf1[..n1], b"OK first\nPONG\n");

    let mut buf2 = [0u8; 1024];
    let n2 = client2.read(&mut buf2).unwrap();
    assert_eq!(&buf2[..n2], b"OK second\nPONG\n");
}

// =============================================================================
// Large Data Tests
// =============================================================================

#[test]
fn test_large_echo() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let mut connections = HashMap::new();

    // Send large ECHO command (64KB of data)
    let large_data = "x".repeat(64 * 1024);
    let cmd = format!("ECHO {}\n", large_data);
    client.write_all(cmd.as_bytes()).unwrap();

    // Run server with more iterations for large data
    run_test_server(&mut driver, &mut connections, 50);

    // Read response
    let expected = format!("OK {}\n", large_data);
    let mut total_received = 0;
    let mut buf = vec![0u8; expected.len() + 1024];

    while total_received < expected.len() {
        match client.read(&mut buf[total_received..]) {
            Ok(0) => break,
            Ok(n) => total_received += n,
            Err(_) => break,
        }
    }

    assert_eq!(total_received, expected.len());
    assert_eq!(&buf[..total_received], expected.as_bytes());
}

#[test]
fn test_many_small_echos() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let mut connections = HashMap::new();

    // Send many small ECHO commands
    let num_commands = 500;
    let mut requests = String::new();
    let mut expected = String::new();
    for i in 0..num_commands {
        requests.push_str(&format!("ECHO msg{}\n", i));
        expected.push_str(&format!("OK msg{}\n", i));
    }
    client.write_all(requests.as_bytes()).unwrap();

    // Run server
    run_test_server(&mut driver, &mut connections, 100);

    // Read all responses
    let mut total_received = 0;
    let mut buf = vec![0u8; expected.len() + 1024];

    while total_received < expected.len() {
        match client.read(&mut buf[total_received..]) {
            Ok(0) => break,
            Ok(n) => total_received += n,
            Err(_) => break,
        }
    }

    assert_eq!(total_received, expected.len());
    assert_eq!(
        std::str::from_utf8(&buf[..total_received]).unwrap(),
        expected
    );
}

// =============================================================================
// Connection Lifecycle Tests
// =============================================================================

#[test]
fn test_client_disconnect_mid_request() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut connections = HashMap::new();

    // Connect and send partial command
    {
        let mut client = match TcpStream::connect(addr) {
            Ok(c) => c,
            Err(_) => return,
        };
        client.set_nonblocking(false).unwrap();

        // Accept connection
        run_test_server(&mut driver, &mut connections, 10);
        assert_eq!(connections.len(), 1);

        // Send partial command
        client.write_all(b"ECHO partial").unwrap();

        // Client disconnects without completing command (drop)
    }

    // Give time for disconnect to propagate
    std::thread::sleep(Duration::from_millis(100));

    // Run server - should handle disconnect gracefully
    run_test_server(&mut driver, &mut connections, 20);

    // Connection should be cleaned up
    assert_eq!(connections.len(), 0);
}

#[test]
fn test_rapid_connect_disconnect() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut connections = HashMap::new();

    // Rapidly connect and disconnect many times
    for _ in 0..20 {
        if let Ok(client) = TcpStream::connect(addr) {
            client.set_nonblocking(false).unwrap();
            // Immediately drop
            drop(client);
        }
    }

    // Give time for all connections to propagate
    std::thread::sleep(Duration::from_millis(200));

    // Run server to process all accept/close events
    run_test_server(&mut driver, &mut connections, 50);

    // All connections should be cleaned up
    assert_eq!(connections.len(), 0);
}

#[test]
fn test_reconnect_same_port() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut connections = HashMap::new();

    for round in 0..3 {
        // Connect
        let mut client = match TcpStream::connect(addr) {
            Ok(c) => c,
            Err(_) => continue,
        };
        client.set_nonblocking(false).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        // Accept
        run_test_server(&mut driver, &mut connections, 10);

        // Exchange messages
        let msg = format!("ECHO round{}\n", round);
        client.write_all(msg.as_bytes()).unwrap();
        run_test_server(&mut driver, &mut connections, 5);

        let mut buf = [0u8; 1024];
        let n = client.read(&mut buf).unwrap();
        let expected = format!("OK round{}\n", round);
        assert_eq!(&buf[..n], expected.as_bytes());

        // Disconnect gracefully
        client.write_all(protocol::CMD_QUIT).unwrap();
        run_test_server(&mut driver, &mut connections, 10);

        // Wait for cleanup
        std::thread::sleep(Duration::from_millis(50));
        run_test_server(&mut driver, &mut connections, 10);
    }
}

// =============================================================================
// Stress Tests
// =============================================================================

#[test]
fn test_sustained_throughput() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(true).unwrap();

    let mut connections = HashMap::new();

    // Accept connection
    run_test_server(&mut driver, &mut connections, 10);

    let total_requests = 5000;
    let mut requests_sent = 0;
    let mut responses_received = 0;
    let mut recv_buf = [0u8; 65536];
    let mut partial_response = Vec::new();

    while responses_received < total_requests {
        // Try to send more requests
        while requests_sent < total_requests && requests_sent - responses_received < 100 {
            match client.write(b"PING\n") {
                Ok(_) => requests_sent += 1,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }

        // Run server
        run_test_server(&mut driver, &mut connections, 1);

        // Try to receive responses
        match client.read(&mut recv_buf) {
            Ok(0) => break, // EOF
            Ok(n) => {
                partial_response.extend_from_slice(&recv_buf[..n]);

                // Count complete responses
                while partial_response.len() >= 5 {
                    if partial_response.starts_with(b"PONG\n") {
                        responses_received += 1;
                        partial_response.drain(..5);
                    } else {
                        break;
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => break,
        }
    }

    assert_eq!(responses_received, total_requests);
}

#[test]
fn test_concurrent_clients_stress() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let num_clients = 20;
    let requests_per_client = 100;
    let mut clients: Vec<TcpStream> = Vec::new();

    for _ in 0..num_clients {
        if let Ok(client) = TcpStream::connect(addr) {
            client.set_nonblocking(true).unwrap();
            clients.push(client);
        }
    }

    if clients.is_empty() {
        return;
    }

    let mut connections = HashMap::new();

    // Accept all connections
    run_test_server(&mut driver, &mut connections, 30);

    let mut requests_sent: Vec<usize> = vec![0; clients.len()];
    let mut responses_received: Vec<usize> = vec![0; clients.len()];
    let mut recv_bufs: Vec<Vec<u8>> = vec![Vec::new(); clients.len()];

    let total_expected = clients.len() * requests_per_client;
    let mut total_responses = 0;

    while total_responses < total_expected {
        // Each client tries to send
        for (i, client) in clients.iter_mut().enumerate() {
            if requests_sent[i] < requests_per_client && client.write_all(b"PING\n").is_ok() {
                requests_sent[i] += 1;
            }
        }

        // Run server
        run_test_server(&mut driver, &mut connections, 1);

        // Each client tries to receive
        for (i, client) in clients.iter_mut().enumerate() {
            let mut buf = [0u8; 4096];
            match client.read(&mut buf) {
                Ok(n) if n > 0 => {
                    recv_bufs[i].extend_from_slice(&buf[..n]);

                    while recv_bufs[i].len() >= 5 && recv_bufs[i].starts_with(b"PONG\n") {
                        responses_received[i] += 1;
                        total_responses += 1;
                        recv_bufs[i].drain(..5);
                    }
                }
                _ => {}
            }
        }
    }

    // Verify each client got expected responses
    for (i, &received) in responses_received.iter().enumerate() {
        assert_eq!(
            received, requests_per_client,
            "Client {} got {} responses, expected {}",
            i, received, requests_per_client
        );
    }
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn test_empty_write() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let mut connections = HashMap::new();

    // Accept
    run_test_server(&mut driver, &mut connections, 10);

    // Send just newline (empty command)
    client.write_all(b"\n").unwrap();
    run_test_server(&mut driver, &mut connections, 5);

    // Should get error response for empty/unknown command
    let mut buf = [0u8; 1024];
    let n = client.read(&mut buf).unwrap();
    assert!(buf[..n].starts_with(protocol::RESP_ERR));
}

#[test]
fn test_binary_data_in_echo() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();

    let mut connections = HashMap::new();

    // Send ECHO with binary-ish data (high bytes, but no newlines)
    let mut cmd = b"ECHO ".to_vec();
    for i in 0..255u8 {
        if i != b'\n' {
            cmd.push(i);
        }
    }
    cmd.push(b'\n');
    client.write_all(&cmd).unwrap();

    run_test_server(&mut driver, &mut connections, 10);

    // Read response
    let mut buf = [0u8; 1024];
    let n = client.read(&mut buf).unwrap();
    assert!(buf[..n].starts_with(protocol::RESP_OK));
}

#[test]
fn test_max_concurrent_connections() {
    let mut driver = MioDriver::new().unwrap();
    let addr = get_test_addr();

    let _listener_id = match driver.listen(addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let max_clients = 100;
    let mut clients: Vec<TcpStream> = Vec::new();

    for _ in 0..max_clients {
        if let Ok(client) = TcpStream::connect(addr) {
            clients.push(client);
        }
    }

    let mut connections = HashMap::new();

    // Accept all
    run_test_server(&mut driver, &mut connections, max_clients);

    assert_eq!(connections.len(), clients.len());

    // All should still be functional
    for client in clients.iter_mut() {
        client.set_nonblocking(false).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        client.write_all(b"PING\n").unwrap();
    }

    run_test_server(&mut driver, &mut connections, 50);

    for client in clients.iter_mut() {
        let mut buf = [0u8; 1024];
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], protocol::RESP_PONG);
    }
}
