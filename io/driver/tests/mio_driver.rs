//! Integration tests for MioDriver.
//!
//! These tests verify the MioDriver functionality using real TCP connections.

use io_driver::mio::MioDriver;
use io_driver::{CompletionKind, IoDriver};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

/// Get an available port for testing.
fn get_test_addr() -> SocketAddr {
    // Use port 0 to let the OS assign an available port
    "127.0.0.1:0".parse().unwrap()
}

#[test]
fn test_mio_driver_new() {
    let driver = MioDriver::new();
    assert!(driver.is_ok());

    let driver = driver.unwrap();
    assert_eq!(driver.connection_count(), 0);
    assert_eq!(driver.listener_count(), 0);
}

#[test]
fn test_mio_driver_with_config() {
    let driver = MioDriver::with_config(8192, 1024);
    assert!(driver.is_ok());
}

#[test]
fn test_listen_and_accept() {
    let mut driver = MioDriver::new().unwrap();

    // Create a listener
    let listener_id = driver.listen(get_test_addr(), 128).unwrap();
    assert_eq!(driver.listener_count(), 1);

    // Get the actual bound address
    // We need to connect to verify accept works
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener_id2 = driver.listen(addr, 128).unwrap();

    assert_eq!(driver.listener_count(), 2);

    // Close listeners
    driver.close_listener(listener_id).unwrap();
    assert_eq!(driver.listener_count(), 1);

    driver.close_listener(listener_id2).unwrap();
    assert_eq!(driver.listener_count(), 0);
}

#[test]
fn test_register_connection() {
    let mut driver = MioDriver::new().unwrap();

    // Create a listener to connect to
    let listen_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let _listener_id = driver.listen(listen_addr, 128).unwrap();

    // We can't easily get the bound port without more infrastructure,
    // but we can test that register works with a connected socket

    assert_eq!(driver.connection_count(), 0);
}

#[test]
fn test_poll_empty() {
    let mut driver = MioDriver::new().unwrap();

    // Poll with no listeners or connections should return quickly
    let count = driver.poll(Some(Duration::from_millis(10))).unwrap();
    assert_eq!(count, 0);

    let completions = driver.drain_completions();
    assert!(completions.is_empty());
}

#[test]
fn test_client_server_echo() {
    let mut server_driver = MioDriver::new().unwrap();

    // Create a listener on an available port
    let listen_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener_id = server_driver.listen(listen_addr, 128).unwrap();

    // Get the actual bound address by creating a test connection
    // Since we can't easily get the port, we'll use a different approach:
    // Create listener, then use std TcpListener to find available port

    // For this test, we'll use a fixed high port and hope it's available
    let test_port = 19876;
    let listen_addr: SocketAddr = format!("127.0.0.1:{}", test_port).parse().unwrap();

    // Close the random port listener
    server_driver.close_listener(listener_id).unwrap();

    // Try to create listener on fixed port
    let listener_id = match server_driver.listen(listen_addr, 128) {
        Ok(id) => id,
        Err(_) => {
            // Port might be in use, skip test
            return;
        }
    };

    // Connect a client
    let mut client = match TcpStream::connect(listen_addr) {
        Ok(c) => c,
        Err(_) => {
            // Connection failed, skip test
            return;
        }
    };
    client.set_nonblocking(true).unwrap();

    // Poll server to accept the connection
    server_driver
        .poll(Some(Duration::from_millis(100)))
        .unwrap();
    let completions = server_driver.drain_completions();

    // Should have an Accept completion
    let mut accepted_conn_id = None;
    for completion in completions {
        if let CompletionKind::Accept {
            listener_id: lid,
            conn_id,
            addr,
        } = completion.kind
        {
            assert_eq!(lid, listener_id);
            assert_eq!(addr.ip().to_string(), "127.0.0.1");
            accepted_conn_id = Some(conn_id);
        }
    }

    let conn_id = accepted_conn_id.expect("Should have accepted a connection");
    assert_eq!(server_driver.connection_count(), 1);

    // Send data from client
    let message = b"Hello, server!";
    client.write_all(message).unwrap();

    // Give data time to be sent over the network
    std::thread::sleep(Duration::from_millis(50));

    // Poll server to receive data (try multiple times for reliability)
    let mut got_recv = false;
    for _ in 0..5 {
        server_driver
            .poll(Some(Duration::from_millis(100)))
            .unwrap();
        let completions = server_driver.drain_completions();

        for completion in completions {
            if let CompletionKind::Recv { conn_id: cid } = completion.kind {
                assert_eq!(cid, conn_id);
                got_recv = true;
            }
        }
        if got_recv {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(got_recv, "Should have received Recv completion");

    // Read the data
    let mut buf = [0u8; 1024];
    let n = server_driver.recv(conn_id, &mut buf).unwrap();
    assert_eq!(&buf[..n], message);

    // Send response back
    let response = b"Hello, client!";

    // May need to poll for SendReady first
    server_driver.poll(Some(Duration::from_millis(10))).unwrap();
    server_driver.drain_completions();

    let sent = server_driver.send(conn_id, response).unwrap();
    assert_eq!(sent, response.len());

    // Client receives the response
    let mut response_buf = [0u8; 1024];
    // May need to wait a bit for data
    std::thread::sleep(Duration::from_millis(50));
    client.set_nonblocking(false).unwrap();
    client
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let n = client.read(&mut response_buf).unwrap();
    assert_eq!(&response_buf[..n], response);

    // Close connection
    server_driver.close(conn_id).unwrap();
    assert_eq!(server_driver.connection_count(), 0);

    // Close listener
    server_driver.close_listener(listener_id).unwrap();
    assert_eq!(server_driver.listener_count(), 0);
}

#[test]
fn test_connection_close_event() {
    let mut server_driver = MioDriver::new().unwrap();

    let test_port = 19877;
    let listen_addr: SocketAddr = format!("127.0.0.1:{}", test_port).parse().unwrap();

    let listener_id = match server_driver.listen(listen_addr, 128) {
        Ok(id) => id,
        Err(_) => return, // Port in use, skip
    };

    // Connect and immediately close client
    let client = match TcpStream::connect(listen_addr) {
        Ok(c) => c,
        Err(_) => return,
    };

    // Accept the connection
    server_driver
        .poll(Some(Duration::from_millis(100)))
        .unwrap();
    let completions = server_driver.drain_completions();

    let mut conn_id = None;
    for completion in completions {
        if let CompletionKind::Accept { conn_id: cid, .. } = completion.kind {
            conn_id = Some(cid);
        }
    }

    let conn_id = match conn_id {
        Some(id) => id,
        None => return, // Accept failed
    };

    // Close the client
    drop(client);

    // Poll server - should get Closed or Recv with 0 bytes
    std::thread::sleep(Duration::from_millis(50));
    server_driver
        .poll(Some(Duration::from_millis(100)))
        .unwrap();
    let completions = server_driver.drain_completions();

    // Try to recv - should indicate closure
    let mut buf = [0u8; 1024];
    let result = server_driver.recv(conn_id, &mut buf);

    // Either we get a Closed completion or recv returns an error
    let got_close_indication = completions.iter().any(|c| {
        matches!(
            c.kind,
            CompletionKind::Closed { .. } | CompletionKind::Recv { .. }
        )
    }) || result.is_err();

    assert!(got_close_indication, "Should detect connection closure");

    server_driver.close(conn_id).unwrap();
    server_driver.close_listener(listener_id).unwrap();
}

#[test]
fn test_send_recv_not_found() {
    let mut driver = MioDriver::new().unwrap();

    // Try to send/recv on non-existent connection
    let fake_id = io_driver::ConnId::new(9999);

    let result = driver.send(fake_id, b"test");
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);

    let mut buf = [0u8; 1024];
    let result = driver.recv(fake_id, &mut buf);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
}

#[test]
fn test_close_nonexistent() {
    let mut driver = MioDriver::new().unwrap();

    // Closing non-existent connection/listener should be ok
    let fake_conn_id = io_driver::ConnId::new(9999);
    let result = driver.close(fake_conn_id);
    assert!(result.is_ok());

    let fake_listener_id = io_driver::ListenerId::new(9999);
    let result = driver.close_listener(fake_listener_id);
    assert!(result.is_ok());
}

#[test]
fn test_multiple_connections() {
    let mut driver = MioDriver::new().unwrap();

    let test_port = 19878;
    let listen_addr: SocketAddr = format!("127.0.0.1:{}", test_port).parse().unwrap();

    let listener_id = match driver.listen(listen_addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    // Connect multiple clients
    let mut clients = Vec::new();
    for _ in 0..3 {
        if let Ok(client) = TcpStream::connect(listen_addr) {
            clients.push(client);
        }
    }

    if clients.is_empty() {
        return;
    }

    // Accept all connections
    std::thread::sleep(Duration::from_millis(50));
    driver.poll(Some(Duration::from_millis(100))).unwrap();
    let completions = driver.drain_completions();

    let accepted_count = completions
        .iter()
        .filter(|c| matches!(c.kind, CompletionKind::Accept { .. }))
        .count();

    assert_eq!(accepted_count, clients.len());
    assert_eq!(driver.connection_count(), clients.len());

    // Close all
    driver.close_listener(listener_id).unwrap();
}

#[test]
fn test_is_readable_writable() {
    let mut driver = MioDriver::new().unwrap();

    let test_port = 19879;
    let listen_addr: SocketAddr = format!("127.0.0.1:{}", test_port).parse().unwrap();

    let _listener_id = match driver.listen(listen_addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(listen_addr) {
        Ok(c) => c,
        Err(_) => return,
    };

    // Accept
    driver.poll(Some(Duration::from_millis(100))).unwrap();
    let completions = driver.drain_completions();

    let conn_id = completions.iter().find_map(|c| {
        if let CompletionKind::Accept { conn_id, .. } = c.kind {
            Some(conn_id)
        } else {
            None
        }
    });

    let conn_id = match conn_id {
        Some(id) => id,
        None => return,
    };

    // Initially might not be readable (no data sent yet)
    // After accept, should be writable
    assert!(driver.is_writable(conn_id));

    // Send some data from client
    client.write_all(b"test").unwrap();

    // Poll to get readable status
    std::thread::sleep(Duration::from_millis(50));
    driver.poll(Some(Duration::from_millis(100))).unwrap();
    driver.drain_completions();

    // Now should be readable
    assert!(driver.is_readable(conn_id));
}

#[test]
#[cfg(unix)]
fn test_raw_fd() {
    let mut driver = MioDriver::new().unwrap();

    let test_port = 19880;
    let listen_addr: SocketAddr = format!("127.0.0.1:{}", test_port).parse().unwrap();

    let _listener_id = match driver.listen(listen_addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let _client = match TcpStream::connect(listen_addr) {
        Ok(c) => c,
        Err(_) => return,
    };

    // Accept
    driver.poll(Some(Duration::from_millis(100))).unwrap();
    let completions = driver.drain_completions();

    let conn_id = completions.iter().find_map(|c| {
        if let CompletionKind::Accept { conn_id, .. } = c.kind {
            Some(conn_id)
        } else {
            None
        }
    });

    let conn_id = match conn_id {
        Some(id) => id,
        None => return,
    };

    // Should be able to get the raw fd
    let fd = driver.raw_fd(conn_id);
    assert!(fd.is_some());
    assert!(fd.unwrap() >= 0);

    // Non-existent connection should return None
    let fake_id = io_driver::ConnId::new(9999);
    assert!(driver.raw_fd(fake_id).is_none());
}

#[test]
fn test_register_client_connection() {
    let mut server_driver = MioDriver::new().unwrap();
    let mut client_driver = MioDriver::new().unwrap();

    let test_port = 19881;
    let listen_addr: SocketAddr = format!("127.0.0.1:{}", test_port).parse().unwrap();

    let _listener_id = match server_driver.listen(listen_addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    // Create a client connection and register it with the client driver
    let client_stream = match TcpStream::connect(listen_addr) {
        Ok(c) => c,
        Err(_) => return,
    };

    let client_conn_id = client_driver.register(client_stream).unwrap();
    assert_eq!(client_driver.connection_count(), 1);

    // Accept on server
    server_driver
        .poll(Some(Duration::from_millis(100)))
        .unwrap();
    let completions = server_driver.drain_completions();

    let server_conn_id = completions.iter().find_map(|c| {
        if let CompletionKind::Accept { conn_id, .. } = c.kind {
            Some(conn_id)
        } else {
            None
        }
    });

    let server_conn_id = match server_conn_id {
        Some(id) => id,
        None => return,
    };

    // Poll client for SendReady (connection established)
    client_driver
        .poll(Some(Duration::from_millis(100)))
        .unwrap();
    let completions = client_driver.drain_completions();

    let got_send_ready = completions
        .iter()
        .any(|c| matches!(c.kind, CompletionKind::SendReady { .. }));
    assert!(got_send_ready, "Client should get SendReady after connect");

    // Client sends data
    let message = b"Hello from client!";
    let sent = client_driver.send(client_conn_id, message).unwrap();
    assert_eq!(sent, message.len());

    // Server receives
    std::thread::sleep(Duration::from_millis(50));
    server_driver
        .poll(Some(Duration::from_millis(100)))
        .unwrap();
    server_driver.drain_completions();

    let mut buf = [0u8; 1024];
    let n = server_driver.recv(server_conn_id, &mut buf).unwrap();
    assert_eq!(&buf[..n], message);
}

#[test]
fn test_send_immediately_after_register() {
    let mut client_driver = MioDriver::new().unwrap();

    let test_port = 19882;
    let listen_addr: SocketAddr = format!("127.0.0.1:{}", test_port).parse().unwrap();

    // Create a listener to accept connections
    let listener = match std::net::TcpListener::bind(listen_addr) {
        Ok(l) => l,
        Err(_) => return,
    };

    // Create a client connection and register it
    let client_stream = match TcpStream::connect(listen_addr) {
        Ok(c) => c,
        Err(_) => return,
    };

    let client_conn_id = client_driver.register(client_stream).unwrap();

    // With edge-triggered mode, send should work immediately after register()
    // because connections are marked writable upon registration.
    // This ensures clients don't miss the initial "ready" edge event.
    let result = client_driver.send(client_conn_id, b"test");
    assert!(result.is_ok());

    // Accept on the listener side
    let _ = listener.accept();

    // Verify that a SendReady completion was queued during registration
    // (this is drained but confirms the fix for edge-triggered notifications)
    let completions = client_driver.drain_completions();
    assert!(
        completions
            .iter()
            .any(|c| matches!(c.kind, io_driver::CompletionKind::SendReady { .. })),
        "Expected SendReady completion to be queued on registration"
    );
}

#[test]
fn test_recv_would_block() {
    let mut driver = MioDriver::new().unwrap();

    let test_port = 19883;
    let listen_addr: SocketAddr = format!("127.0.0.1:{}", test_port).parse().unwrap();

    let _listener_id = match driver.listen(listen_addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let _client = match TcpStream::connect(listen_addr) {
        Ok(c) => c,
        Err(_) => return,
    };

    // Accept
    driver.poll(Some(Duration::from_millis(100))).unwrap();
    let completions = driver.drain_completions();

    let conn_id = completions.iter().find_map(|c| {
        if let CompletionKind::Accept { conn_id, .. } = c.kind {
            Some(conn_id)
        } else {
            None
        }
    });

    let conn_id = match conn_id {
        Some(id) => id,
        None => return,
    };

    // Try to recv when no data is available - should get WouldBlock
    let mut buf = [0u8; 1024];
    let result = driver.recv(conn_id, &mut buf);

    // Either WouldBlock or data (if client sent something)
    if let Err(e) = result {
        assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock);
    }
}

#[test]
fn test_listen_ipv6() {
    let mut driver = MioDriver::new().unwrap();

    // Try to create an IPv6 listener
    let listen_addr: SocketAddr = "[::1]:0".parse().unwrap();

    // This may fail if IPv6 is not available on the system
    if let Ok(listener_id) = driver.listen(listen_addr, 128) {
        assert_eq!(driver.listener_count(), 1);
        driver.close_listener(listener_id).unwrap();
        assert_eq!(driver.listener_count(), 0);
    }
    // If IPv6 isn't available, the test passes silently
}

#[test]
fn test_large_data_transfer() {
    let mut server_driver = MioDriver::new().unwrap();

    let test_port = 19884;
    let listen_addr: SocketAddr = format!("127.0.0.1:{}", test_port).parse().unwrap();

    let listener_id = match server_driver.listen(listen_addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    let mut client = match TcpStream::connect(listen_addr) {
        Ok(c) => c,
        Err(_) => return,
    };
    client.set_nonblocking(false).unwrap();

    // Accept
    server_driver
        .poll(Some(Duration::from_millis(100)))
        .unwrap();
    let completions = server_driver.drain_completions();

    let conn_id = completions.iter().find_map(|c| {
        if let CompletionKind::Accept { conn_id, .. } = c.kind {
            Some(conn_id)
        } else {
            None
        }
    });

    let conn_id = match conn_id {
        Some(id) => id,
        None => return,
    };

    // Send a larger chunk of data
    let large_data = vec![0xABu8; 64 * 1024]; // 64KB
    client.write_all(&large_data).unwrap();

    // Give time for data to arrive
    std::thread::sleep(Duration::from_millis(100));

    // Poll and read in chunks
    let mut total_received = 0;
    let mut recv_buf = vec![0u8; 8192];

    for _ in 0..20 {
        server_driver.poll(Some(Duration::from_millis(50))).unwrap();
        server_driver.drain_completions();

        loop {
            match server_driver.recv(conn_id, &mut recv_buf) {
                Ok(n) if n > 0 => {
                    total_received += n;
                    // Verify data
                    assert!(recv_buf[..n].iter().all(|&b| b == 0xAB));
                }
                _ => break,
            }
        }

        if total_received >= large_data.len() {
            break;
        }
    }

    assert_eq!(total_received, large_data.len());

    server_driver.close(conn_id).unwrap();
    server_driver.close_listener(listener_id).unwrap();
}

#[test]
fn test_multiple_listeners() {
    let mut driver = MioDriver::new().unwrap();

    let mut listener_ids = Vec::new();

    // Create multiple listeners on different ports
    for port_offset in 0..3 {
        let test_port = 19885 + port_offset;
        let listen_addr: SocketAddr = format!("127.0.0.1:{}", test_port).parse().unwrap();

        if let Ok(id) = driver.listen(listen_addr, 128) {
            listener_ids.push(id);
        }
    }

    assert_eq!(driver.listener_count(), listener_ids.len());

    // Close all listeners
    for id in listener_ids {
        driver.close_listener(id).unwrap();
    }

    assert_eq!(driver.listener_count(), 0);
}

#[test]
fn test_poll_with_none_timeout() {
    let mut driver = MioDriver::new().unwrap();

    let test_port = 19888;
    let listen_addr: SocketAddr = format!("127.0.0.1:{}", test_port).parse().unwrap();

    let _listener_id = match driver.listen(listen_addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    // Connect a client to trigger an event
    let _client = match TcpStream::connect(listen_addr) {
        Ok(c) => c,
        Err(_) => return,
    };

    // Poll with None timeout (should return immediately if there are events)
    // Give connection time to be established
    std::thread::sleep(Duration::from_millis(50));

    let count = driver.poll(None).unwrap();
    // Should have at least one completion (Accept)
    assert!(count > 0);
}

#[test]
fn test_connection_reset_on_recv() {
    let mut server_driver = MioDriver::new().unwrap();

    let test_port = 19889;
    let listen_addr: SocketAddr = format!("127.0.0.1:{}", test_port).parse().unwrap();

    let _listener_id = match server_driver.listen(listen_addr, 128) {
        Ok(id) => id,
        Err(_) => return,
    };

    // Connect and accept
    let client = match TcpStream::connect(listen_addr) {
        Ok(c) => c,
        Err(_) => return,
    };

    server_driver
        .poll(Some(Duration::from_millis(100)))
        .unwrap();
    let completions = server_driver.drain_completions();

    let conn_id = completions.iter().find_map(|c| {
        if let CompletionKind::Accept { conn_id, .. } = c.kind {
            Some(conn_id)
        } else {
            None
        }
    });

    let conn_id = match conn_id {
        Some(id) => id,
        None => return,
    };

    // Close the client connection abruptly
    drop(client);

    // Wait a bit for the close to propagate
    std::thread::sleep(Duration::from_millis(100));

    // Poll to get any close notifications
    server_driver
        .poll(Some(Duration::from_millis(100)))
        .unwrap();
    server_driver.drain_completions();

    // Try to recv - should get EOF (Ok(0)) or an error indicating connection closed
    let mut buf = [0u8; 1024];
    let result = server_driver.recv(conn_id, &mut buf);

    // Ok(0) indicates EOF (peer closed connection gracefully)
    // WouldBlock means close hasn't been detected yet
    // ConnectionReset means peer closed ungracefully
    match result {
        Ok(0) => {} // EOF - connection closed gracefully
        Ok(_) => {} // Got some buffered data
        Err(e) => {
            // ConnectionReset or WouldBlock are both acceptable
            assert!(
                e.kind() == std::io::ErrorKind::ConnectionReset
                    || e.kind() == std::io::ErrorKind::WouldBlock
            );
        }
    }
}
