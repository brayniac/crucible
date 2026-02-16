//! Integration tests: echo server using real TCP connections.
//!
//! Each test launches a krio server, connects via std TCP, sends data,
//! and verifies the echoed response.

use std::future::Future;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::pin::Pin;
use std::time::Duration;

use krio::{AsyncEventHandler, Config, ConnCtx, ConnToken, DriverCtx, EventHandler, KrioBuilder};
use std::sync::atomic::{AtomicU32, Ordering};

// ── Callback echo handler ──────────────────────────────────────────

struct Echo;

impl EventHandler for Echo {
    fn on_accept(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}
    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        let _ = ctx.send(conn, data);
        data.len()
    }
    fn on_send_complete(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken, _r: io::Result<u32>) {}
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

// ── Shutdown-write test ─────────────────────────────────────────────

/// Handler that echoes back data then half-closes the write side.
struct ShutdownWriteEcho;

impl AsyncEventHandler for ShutdownWriteEcho {
    fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        Box::pin(async move {
            let n = conn
                .with_data(|data| {
                    let _ = conn.send(data);
                    data.len()
                })
                .await;
            if n > 0 {
                conn.shutdown_write();
            }
            // Keep the task alive to receive more (should get EOF).
            let _ = conn.with_data(|_data| 0).await;
        })
    }
    fn create_for_worker(_id: usize) -> Self {
        ShutdownWriteEcho
    }
}

#[test]
fn async_shutdown_write_triggers_eof() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch_async::<ShutdownWriteEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Send data.
    let msg = b"shutdown test";
    stream.write_all(msg).unwrap();
    stream.flush().unwrap();

    // Read the echo.
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
    assert_eq!(&buf[..total], msg);

    // After echo, server does shutdown_write — we should get EOF.
    let mut extra = [0u8; 1];
    match stream.read(&mut extra) {
        Ok(0) => {} // EOF — correct!
        Ok(_) => panic!("expected EOF after shutdown_write"),
        Err(e) => panic!("unexpected error: {e}"),
    }

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Request-shutdown test ───────────────────────────────────────────

/// Handler that shuts down the worker after receiving any data.
struct RequestShutdownHandler;

impl AsyncEventHandler for RequestShutdownHandler {
    fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        Box::pin(async move {
            conn.with_data(|data| {
                // Echo back, then request shutdown.
                let _ = conn.send(data);
                conn.request_shutdown();
                data.len()
            })
            .await;
        })
    }
    fn create_for_worker(_id: usize) -> Self {
        RequestShutdownHandler
    }
}

#[test]
fn async_request_shutdown_exits_cleanly() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch_async::<RequestShutdownHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Send a message — the handler will request shutdown after echoing.
    let _response = echo_round_trip(&addr, b"trigger-shutdown");

    // Workers should exit on their own (request_shutdown triggers it).
    for h in handles {
        let result = h.join().expect("worker panicked");
        result.expect("worker returned error");
    }

    // ShutdownHandle is now redundant, but drop it cleanly.
    drop(shutdown);
}

// ── Spawn standalone task test ──────────────────────────────────────

static SPAWN_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Handler that spawns a standalone task from on_accept.
struct SpawnTestHandler;

impl AsyncEventHandler for SpawnTestHandler {
    fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        Box::pin(async move {
            // Spawn a standalone task that increments the counter.
            krio::spawn(async {
                SPAWN_COUNTER.fetch_add(1, Ordering::SeqCst);
            });

            // Echo one message to signal readiness.
            conn.with_data(|data| {
                let _ = conn.send(data);
                data.len()
            })
            .await;
        })
    }
    fn create_for_worker(_id: usize) -> Self {
        SpawnTestHandler
    }
}

#[test]
fn async_spawn_standalone_task() {
    // Reset counter.
    SPAWN_COUNTER.store(0, Ordering::SeqCst);

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch_async::<SpawnTestHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Connect 3 times — each accept spawns a standalone task.
    for _ in 0..3 {
        echo_round_trip(&addr, b"spawn-test");
    }

    // Give standalone tasks time to run.
    std::thread::sleep(Duration::from_millis(100));

    // Verify the standalone tasks ran.
    let count = SPAWN_COUNTER.load(Ordering::SeqCst);
    assert!(count >= 3, "expected at least 3, got {count}");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Sleep test ──────────────────────────────────────────────────────

/// Handler that sleeps before echoing back.
struct SleepEchoHandler;

impl AsyncEventHandler for SleepEchoHandler {
    fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        Box::pin(async move {
            loop {
                let n = conn
                    .with_data(|data| {
                        let len = data.len();
                        // Sleep 50ms then echo.
                        let data_copy = data.to_vec();
                        let conn2 = conn;
                        krio::spawn(async move {
                            krio::sleep(Duration::from_millis(50)).await;
                            let _ = conn2.send(&data_copy);
                        });
                        len
                    })
                    .await;
                if n == 0 {
                    break;
                }
            }
        })
    }
    fn create_for_worker(_id: usize) -> Self {
        SleepEchoHandler
    }
}

#[test]
fn async_sleep_completes() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch_async::<SleepEchoHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    let start = std::time::Instant::now();
    let response = echo_round_trip(&addr, b"hello sleep");
    let elapsed = start.elapsed();

    assert_eq!(response, b"hello sleep");
    // Should take at least ~50ms due to sleep.
    assert!(
        elapsed >= Duration::from_millis(30),
        "elapsed only {elapsed:?}, expected at least 30ms"
    );

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

// ── Timeout test ────────────────────────────────────────────────────

/// Handler that tests timeout — a fast operation should succeed,
/// then the handler echoes a response indicating success.
struct TimeoutTestHandler;

impl AsyncEventHandler for TimeoutTestHandler {
    fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        Box::pin(async move {
            conn.with_data(|data| {
                let msg = std::str::from_utf8(data).unwrap_or("");
                if msg == "test-timeout-ok" {
                    // Timeout wrapping an immediate future should succeed.
                    let conn2 = conn;
                    krio::spawn(async move {
                        let result = krio::timeout(Duration::from_secs(10), async { 42u32 }).await;
                        match result {
                            Ok(42) => {
                                let _ = conn2.send(b"OK");
                            }
                            _ => {
                                let _ = conn2.send(b"FAIL");
                            }
                        }
                    });
                } else if msg == "test-timeout-expire" {
                    // Timeout wrapping a long sleep should expire.
                    let conn2 = conn;
                    krio::spawn(async move {
                        let result = krio::timeout(
                            Duration::from_millis(20),
                            krio::sleep(Duration::from_secs(10)),
                        )
                        .await;
                        match result {
                            Err(_elapsed) => {
                                let _ = conn2.send(b"ELAPSED");
                            }
                            Ok(()) => {
                                let _ = conn2.send(b"FAIL");
                            }
                        }
                    });
                }
                data.len()
            })
            .await;
            // Keep the task alive so the spawned tasks can send.
            krio::sleep(Duration::from_secs(5)).await;
        })
    }
    fn create_for_worker(_id: usize) -> Self {
        TimeoutTestHandler
    }
}

#[test]
fn async_timeout_ok() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch_async::<TimeoutTestHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Test: timeout wrapping an immediate future should return Ok.
    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"test-timeout-ok").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 16];
    let mut total = 0;
    while total < 2 {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("read error: {e}"),
        }
    }
    assert_eq!(&buf[..total], b"OK");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

#[test]
fn async_timeout_expires() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_config())
        .bind(&addr)
        .launch_async::<TimeoutTestHandler>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Test: timeout wrapping a long sleep should return Err(Elapsed).
    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"test-timeout-expire").unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 16];
    let mut total = 0;
    while total < 7 {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("read error: {e}"),
        }
    }
    assert_eq!(&buf[..total], b"ELAPSED");

    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}
