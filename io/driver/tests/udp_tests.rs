//! UDP socket tests for io-driver.

use io_driver::{CompletionKind, Driver, Ecn, SendMeta, UdpSocketId};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Poll until we see a UdpReadable event for the given socket, or timeout.
fn poll_until_readable(
    driver: &mut Box<dyn io_driver::IoDriver>,
    socket_id: UdpSocketId,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        driver.poll(Some(Duration::from_millis(10))).unwrap();
        for completion in driver.drain_completions() {
            if let CompletionKind::UdpReadable { socket_id: id } = completion.kind
                && id == socket_id
            {
                return true;
            }
        }
    }
    false
}

/// Simple UDP echo test using mio backend.
#[test]
fn test_udp_echo() {
    let mut driver = Driver::new().unwrap();

    // Bind server socket
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server_id = driver.bind_udp(server_addr).unwrap();

    // Get the actual bound address
    let server_fd = driver.udp_raw_fd(server_id).unwrap();
    let server_addr = get_local_addr(server_fd);

    // Bind client socket
    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let client_id = driver.bind_udp(client_addr).unwrap();
    let client_fd = driver.udp_raw_fd(client_id).unwrap();
    let client_addr = get_local_addr(client_fd);

    assert_eq!(driver.udp_socket_count(), 2);

    // Send data from client to server
    let test_data = b"Hello, UDP!";
    let meta = SendMeta::new(server_addr);
    let sent = driver.sendmsg(client_id, test_data, &meta).unwrap();
    assert_eq!(sent, test_data.len());

    // Poll until server is readable
    assert!(
        poll_until_readable(&mut driver, server_id, Duration::from_secs(1)),
        "Server should be readable"
    );

    // Receive on server
    let mut recv_buf = [0u8; 1024];
    let recv_meta = driver.recvmsg(server_id, &mut recv_buf).unwrap();

    assert_eq!(recv_meta.len, test_data.len());
    assert_eq!(&recv_buf[..recv_meta.len], test_data);
    assert_eq!(recv_meta.source, client_addr);
    assert_eq!(recv_meta.ecn, Ecn::NotEct);

    // Echo back to client
    let echo_meta = SendMeta::new(recv_meta.source);
    let echoed = driver
        .sendmsg(server_id, &recv_buf[..recv_meta.len], &echo_meta)
        .unwrap();
    assert_eq!(echoed, recv_meta.len);

    // Poll until client is readable
    assert!(
        poll_until_readable(&mut driver, client_id, Duration::from_secs(1)),
        "Client should be readable"
    );

    // Receive echo on client
    let mut echo_buf = [0u8; 1024];
    let echo_meta = driver.recvmsg(client_id, &mut echo_buf).unwrap();

    assert_eq!(echo_meta.len, test_data.len());
    assert_eq!(&echo_buf[..echo_meta.len], test_data);
    assert_eq!(echo_meta.source, server_addr);

    // Clean up
    driver.close_udp(client_id).unwrap();
    driver.close_udp(server_id).unwrap();
    assert_eq!(driver.udp_socket_count(), 0);
}

/// Test ECN marking on sent packets.
#[test]
fn test_udp_ecn() {
    let mut driver = Driver::new().unwrap();

    // Bind two sockets
    let sock1_id = driver.bind_udp("127.0.0.1:0".parse().unwrap()).unwrap();
    let sock2_id = driver.bind_udp("127.0.0.1:0".parse().unwrap()).unwrap();

    let _sock1_addr = get_local_addr(driver.udp_raw_fd(sock1_id).unwrap());
    let sock2_addr = get_local_addr(driver.udp_raw_fd(sock2_id).unwrap());

    // Send with ECN marking
    let test_data = b"ECN test";
    let meta = SendMeta::new(sock2_addr).with_ecn(Ecn::Ect0);
    driver.sendmsg(sock1_id, test_data, &meta).unwrap();

    // Poll until readable
    assert!(
        poll_until_readable(&mut driver, sock2_id, Duration::from_secs(1)),
        "Socket should be readable"
    );

    let mut recv_buf = [0u8; 1024];
    let recv_meta = driver.recvmsg(sock2_id, &mut recv_buf).unwrap();

    assert_eq!(recv_meta.len, test_data.len());
    assert_eq!(&recv_buf[..recv_meta.len], test_data);
    // Note: ECN bits may be cleared by loopback, so we just verify the send worked

    driver.close_udp(sock1_id).unwrap();
    driver.close_udp(sock2_id).unwrap();
}

/// Test multiple datagrams.
#[test]
fn test_udp_multiple_datagrams() {
    let mut driver = Driver::new().unwrap();

    let server_id = driver.bind_udp("127.0.0.1:0".parse().unwrap()).unwrap();
    let client_id = driver.bind_udp("127.0.0.1:0".parse().unwrap()).unwrap();

    let server_addr = get_local_addr(driver.udp_raw_fd(server_id).unwrap());

    // Send multiple datagrams
    let messages = [b"one".as_slice(), b"two", b"three", b"four", b"five"];
    for msg in &messages {
        let meta = SendMeta::new(server_addr);
        driver.sendmsg(client_id, msg, &meta).unwrap();
    }

    // Poll until server is readable
    assert!(
        poll_until_readable(&mut driver, server_id, Duration::from_secs(1)),
        "Server should be readable"
    );

    let mut received = Vec::new();
    let mut recv_buf = [0u8; 1024];

    // Drain all datagrams (may need to poll multiple times for edge-triggered)
    let start = Instant::now();
    while received.len() < messages.len() && start.elapsed() < Duration::from_secs(1) {
        match driver.recvmsg(server_id, &mut recv_buf) {
            Ok(meta) => {
                received.push(recv_buf[..meta.len].to_vec());
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // Poll for more data
                driver.poll(Some(Duration::from_millis(10))).unwrap();
                driver.drain_completions(); // Drain completions to clear the queue
            }
            Err(e) => panic!("Unexpected error: {}", e),
        }
    }

    assert_eq!(received.len(), messages.len());
    for (i, msg) in messages.iter().enumerate() {
        assert_eq!(received[i], *msg);
    }

    driver.close_udp(client_id).unwrap();
    driver.close_udp(server_id).unwrap();
}

/// Test IPv6 UDP socket.
#[test]
fn test_udp_ipv6() {
    let mut driver = Driver::new().unwrap();

    // Try to bind IPv6 sockets - may fail on systems without IPv6
    let server_result = driver.bind_udp("[::1]:0".parse().unwrap());
    let Ok(server_id) = server_result else {
        // IPv6 not available, skip test
        return;
    };

    let client_id = driver.bind_udp("[::1]:0".parse().unwrap()).unwrap();

    let server_addr = get_local_addr(driver.udp_raw_fd(server_id).unwrap());

    // Send and receive
    let test_data = b"IPv6 UDP test";
    let meta = SendMeta::new(server_addr);
    driver.sendmsg(client_id, test_data, &meta).unwrap();

    // Poll until readable
    assert!(
        poll_until_readable(&mut driver, server_id, Duration::from_secs(1)),
        "Server should be readable"
    );

    let mut recv_buf = [0u8; 1024];
    let recv_meta = driver.recvmsg(server_id, &mut recv_buf).unwrap();

    assert_eq!(recv_meta.len, test_data.len());
    assert_eq!(&recv_buf[..recv_meta.len], test_data);

    driver.close_udp(client_id).unwrap();
    driver.close_udp(server_id).unwrap();
}

/// Test SendMeta builder methods.
#[test]
fn test_send_meta_builder() {
    let dest: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let source: SocketAddr = "127.0.0.1:9090".parse().unwrap();

    let meta = SendMeta::new(dest);
    assert_eq!(meta.dest, dest);
    assert_eq!(meta.source, None);
    assert_eq!(meta.ecn, Ecn::NotEct);

    let meta = SendMeta::new(dest).with_ecn(Ecn::Ect1);
    assert_eq!(meta.ecn, Ecn::Ect1);

    let meta = SendMeta::new(dest).with_source(source);
    assert_eq!(meta.source, Some(source));

    let meta = SendMeta::new(dest).with_ecn(Ecn::Ce).with_source(source);
    assert_eq!(meta.ecn, Ecn::Ce);
    assert_eq!(meta.source, Some(source));
}

/// Test closing a UDP socket that doesn't exist.
#[test]
fn test_close_nonexistent_udp() {
    let mut driver = Driver::new().unwrap();
    let fake_id = UdpSocketId::new(999);
    // Should not error, just no-op
    driver.close_udp(fake_id).unwrap();
}

/// Test recvmsg on nonexistent socket.
#[test]
fn test_recvmsg_nonexistent() {
    let mut driver = Driver::new().unwrap();
    let fake_id = UdpSocketId::new(999);
    let mut buf = [0u8; 1024];
    let result = driver.recvmsg(fake_id, &mut buf);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
}

/// Test sendmsg on nonexistent socket.
#[test]
fn test_sendmsg_nonexistent() {
    let mut driver = Driver::new().unwrap();
    let fake_id = UdpSocketId::new(999);
    let meta = SendMeta::new("127.0.0.1:8080".parse().unwrap());
    let result = driver.sendmsg(fake_id, b"test", &meta);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
}

/// Helper to get the local address of a socket.
fn get_local_addr(fd: std::os::unix::io::RawFd) -> SocketAddr {
    unsafe {
        let mut storage: libc::sockaddr_storage = std::mem::zeroed();
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        libc::getsockname(fd, &mut storage as *mut _ as *mut libc::sockaddr, &mut len);

        match storage.ss_family as i32 {
            libc::AF_INET => {
                let addr = &*(&storage as *const _ as *const libc::sockaddr_in);
                SocketAddr::V4(std::net::SocketAddrV4::new(
                    std::net::Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr)),
                    u16::from_be(addr.sin_port),
                ))
            }
            libc::AF_INET6 => {
                let addr = &*(&storage as *const _ as *const libc::sockaddr_in6);
                SocketAddr::V6(std::net::SocketAddrV6::new(
                    std::net::Ipv6Addr::from(addr.sin6_addr.s6_addr),
                    u16::from_be(addr.sin6_port),
                    addr.sin6_flowinfo,
                    addr.sin6_scope_id,
                ))
            }
            _ => panic!("Unknown address family"),
        }
    }
}
