//! Integration tests: echo server using real TCP connections.
//!
//! Each test launches a krio server, connects via std TCP, sends data,
//! and verifies the echoed response.

use std::future::Future;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::pin::Pin;
use std::time::Duration;

use krio::{
    AsyncEventHandler, Config, ConnCtx, ConnToken, DriverCtx, EventHandler, KrioBuilder,
};

// ── Callback echo handler ──────────────────────────────────────────

struct Echo;

impl EventHandler for Echo {
    fn on_accept(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}
    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        let _ = ctx.send(conn, data);
        data.len()
    }
    fn on_send_complete(
        &mut self,
        _ctx: &mut DriverCtx,
        _conn: ConnToken,
        _r: io::Result<u32>,
    ) {
    }
    fn on_close(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}
    fn create_for_worker(_id: usize) -> Self {
        Echo
    }
}

// ── Async echo handler ─────────────────────────────────────────────

struct AsyncEcho;

impl AsyncEventHandler for AsyncEcho {
    fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        Box::pin(async move {
            loop {
                let n = conn
                    .with_data(|data| {
                        let _ = conn.send(data);
                        data.len()
                    })
                    .await;
                if n == 0 {
                    break;
                }
            }
        })
    }
    fn create_for_worker(_id: usize) -> Self {
        AsyncEcho
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn test_config() -> Config {
    let mut config = Config::default();
    config.worker.threads = 1;
    config.worker.pin_to_core = false;
    config.sq_entries = 64;
    config.recv_buffer.ring_size = 64;
    config.recv_buffer.buffer_size = 4096;
    config.max_connections = 64;
    config.send_copy_count = 64;
    config
}

/// Find an available port by binding to :0.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn wait_for_server(addr: &str) {
    for _ in 0..200 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("server did not start on {addr}");
}

fn echo_round_trip(addr: &str, msg: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(msg).unwrap();
    stream.flush().unwrap();

    let mut buf = vec![0u8; msg.len()];
    let mut total = 0;
    while total < msg.len() {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }
    buf.truncate(total);
    buf
}

// ── Tests ───────────────────────────────────────────────────────────

#[test]
fn callback_echo_small_message() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch::<Echo>()
        .expect("launch failed");

    wait_for_server(&addr);

    let msg = b"Hello, krio!";
    let response = echo_round_trip(&addr, msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn callback_echo_large_message() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch::<Echo>()
        .expect("launch failed");

    wait_for_server(&addr);

    // 8KB message — larger than typical TCP segment
    let msg: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
    let response = echo_round_trip(&addr, &msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn callback_echo_multiple_connections() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch::<Echo>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut join_handles = Vec::new();
    for i in 0..4 {
        let addr = addr.clone();
        join_handles.push(std::thread::spawn(move || {
            let msg = format!("connection {i}");
            let response = echo_round_trip(&addr, msg.as_bytes());
            assert_eq!(response, msg.as_bytes());
        }));
    }
    for h in join_handles {
        h.join().unwrap();
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn callback_echo_sequential_sends() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch::<Echo>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    for i in 0..10 {
        let msg = format!("msg-{i}\n");
        stream.write_all(msg.as_bytes()).unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0u8; msg.len()];
        let mut total = 0;
        while total < msg.len() {
            match stream.read(&mut buf[total..]) {
                Ok(0) => break,
                Ok(n) => total += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => panic!("read error: {e}"),
            }
        }
        assert_eq!(&buf[..total], msg.as_bytes(), "mismatch on send {i}");
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn async_echo_small_message() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch_async::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let msg = b"Hello, async krio!";
    let response = echo_round_trip(&addr, msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn async_echo_large_message() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch_async::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let msg: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
    let response = echo_round_trip(&addr, &msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn async_echo_multiple_connections() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch_async::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut join_handles = Vec::new();
    for i in 0..4 {
        let addr = addr.clone();
        join_handles.push(std::thread::spawn(move || {
            let msg = format!("async conn {i}");
            let response = echo_round_trip(&addr, msg.as_bytes());
            assert_eq!(response, msg.as_bytes());
        }));
    }
    for h in join_handles {
        h.join().unwrap();
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn connection_close_on_client_disconnect() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch::<Echo>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Open and immediately close 10 connections.
    for _ in 0..10 {
        let stream = TcpStream::connect(&addr).unwrap();
        drop(stream);
    }

    // Give the server time to process the closes.
    std::thread::sleep(Duration::from_millis(200));

    // Verify the server is still alive by connecting again.
    let msg = b"still alive";
    let response = echo_round_trip(&addr, msg);
    assert_eq!(response, msg);

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn graceful_shutdown() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch::<Echo>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Open a connection, send data, verify echo.
    let msg = b"pre-shutdown";
    let response = echo_round_trip(&addr, msg);
    assert_eq!(response, msg);

    // Trigger shutdown.
    shutdown.shutdown();

    // Workers should exit cleanly.
    for h in handles {
        let result = h.join().expect("worker panicked");
        result.expect("worker returned error");
    }
}
