//! Integration tests for the async proxy handler.
//!
//! Each test starts:
//! 1. A krio backend that speaks RESP (simple echo of raw bytes)
//! 2. An async proxy connected to that backend
//! 3. A TCP client that sends RESP commands to the proxy

use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use krio::{AsyncEventHandler, Config, ConnCtx, KrioBuilder};
use proxy::async_worker::run_async;

// ── RESP backend handler ────────────────────────────────────────────

/// A simple backend that echoes raw RESP bytes back.
/// This is sufficient for proxy testing since the proxy forwards
/// raw bytes to/from the backend.
struct RespEchoBackend;

impl AsyncEventHandler for RespEchoBackend {
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
        RespEchoBackend
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn test_krio_config() -> Config {
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

fn send_resp_command(stream: &mut TcpStream, cmd: &[u8]) -> String {
    stream.write_all(cmd).unwrap();
    stream.flush().unwrap();

    let mut buf = [0u8; 4096];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                // Check if we have a complete RESP response.
                let s = std::str::from_utf8(&buf[..total]).unwrap_or("");
                if is_complete_resp(s) {
                    break;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }
    String::from_utf8_lossy(&buf[..total]).to_string()
}

/// Simple heuristic to check if a RESP response is complete.
fn is_complete_resp(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    // Simple status/error: ends with \r\n
    if (s.starts_with('+') || s.starts_with('-') || s.starts_with(':')) && s.ends_with("\r\n") {
        return true;
    }
    // Bulk string: $N\r\n...data...\r\n or $-1\r\n
    if s.starts_with('$') {
        if s.starts_with("$-1\r\n") {
            return true;
        }
        // Count \r\n occurrences — need at least 2 for $N\r\ndata\r\n
        let crlf_count = s.matches("\r\n").count();
        return crlf_count >= 2;
    }
    // Fallback: if it ends with \r\n, assume complete.
    s.ends_with("\r\n")
}

/// Start a backend krio echo server.
fn start_backend() -> (
    String,
    krio::ShutdownHandle,
    Vec<std::thread::JoinHandle<Result<(), krio::Error>>>,
) {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = KrioBuilder::new(test_krio_config())
        .bind(&addr)
        .launch_async::<RespEchoBackend>()
        .expect("backend launch failed");

    wait_for_server(&addr);
    (addr, shutdown, handles)
}

/// Proxy config channel — separate from the handler's internal one.
/// We need a way to pass config to run_async that doesn't conflict with
/// the module-level OnceLock.
fn make_proxy_config(listen_port: u16, backend_addr: &str, cache_enabled: bool) -> proxy::Config {
    // Build a minimal config.
    let toml_str = format!(
        r#"
[proxy]
listen = "127.0.0.1:{listen_port}"

[backend]
nodes = ["{backend_addr}"]
pool_size = 1

[workers]
threads = 1

[cache]
enabled = {cache_enabled}
heap_size = 1048576
segment_size = 65536
ttl_ms = 60000
hashtable_power = 10

[uring]
buffer_size = 4096
buffer_count = 64
sq_depth = 64

[logging]
level = "warn"

[shutdown]
drain_timeout_secs = 5
"#
    );
    toml::from_str(&toml_str).expect("failed to parse proxy config")
}

/// Start the async proxy in a background thread.
fn start_async_proxy(
    config: &proxy::Config,
    shutdown: Arc<AtomicBool>,
) -> std::thread::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    let config = config.clone();
    std::thread::spawn(move || {
        run_async(&config, shutdown).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::other(e.to_string()))
        })
    })
}

// ── Tests ───────────────────────────────────────────────────────────

#[test]
fn async_proxy_ping() {
    let (backend_addr, backend_shutdown, backend_handles) = start_backend();

    let proxy_port = free_port();
    let proxy_addr = format!("127.0.0.1:{proxy_port}");
    let config = make_proxy_config(proxy_port, &backend_addr, false);
    let proxy_shutdown = Arc::new(AtomicBool::new(false));
    let proxy_handle = start_async_proxy(&config, Arc::clone(&proxy_shutdown));

    wait_for_server(&proxy_addr);

    // PING should be handled locally by the proxy.
    let mut stream = TcpStream::connect(&proxy_addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let response = send_resp_command(&mut stream, b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(response, "+PONG\r\n");

    drop(stream);
    proxy_shutdown.store(true, Ordering::SeqCst);
    let _ = proxy_handle.join();

    backend_shutdown.shutdown();
    for h in backend_handles {
        let _ = h.join();
    }
}

#[test]
fn async_proxy_set_get() {
    let (backend_addr, backend_shutdown, backend_handles) = start_backend();

    let proxy_port = free_port();
    let proxy_addr = format!("127.0.0.1:{proxy_port}");
    let config = make_proxy_config(proxy_port, &backend_addr, false);
    let proxy_shutdown = Arc::new(AtomicBool::new(false));
    let proxy_handle = start_async_proxy(&config, Arc::clone(&proxy_shutdown));

    wait_for_server(&proxy_addr);

    let mut stream = TcpStream::connect(&proxy_addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // SET key value — the backend echoes the raw RESP back, which the proxy
    // forwards to the client.
    let set_cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let response = send_resp_command(&mut stream, set_cmd);
    // The echo backend will echo back the entire SET command as raw bytes.
    // The proxy reads one RESP value from the backend response.
    // Since the backend echoes everything, the first parseable RESP value
    // from the echoed "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    // is actually parsed as a RESP array. The proxy reads it and forwards it.
    assert!(!response.is_empty(), "expected non-empty response for SET");

    // GET key
    let get_cmd = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    let response = send_resp_command(&mut stream, get_cmd);
    assert!(!response.is_empty(), "expected non-empty response for GET");

    drop(stream);
    proxy_shutdown.store(true, Ordering::SeqCst);
    let _ = proxy_handle.join();

    backend_shutdown.shutdown();
    for h in backend_handles {
        let _ = h.join();
    }
}

#[test]
fn async_proxy_cache_hit() {
    let (backend_addr, backend_shutdown, backend_handles) = start_backend();

    let proxy_port = free_port();
    let proxy_addr = format!("127.0.0.1:{proxy_port}");
    let config = make_proxy_config(proxy_port, &backend_addr, true);
    let proxy_shutdown = Arc::new(AtomicBool::new(false));
    let proxy_handle = start_async_proxy(&config, Arc::clone(&proxy_shutdown));

    wait_for_server(&proxy_addr);

    let mut stream = TcpStream::connect(&proxy_addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // First GET — goes to backend (cache miss).
    let get_cmd = b"*2\r\n$3\r\nGET\r\n$8\r\ncachekey\r\n";
    let response1 = send_resp_command(&mut stream, get_cmd);
    assert!(
        !response1.is_empty(),
        "expected response from backend for first GET"
    );

    // Second GET — should be served from cache (same response).
    let response2 = send_resp_command(&mut stream, get_cmd);
    assert_eq!(
        response1, response2,
        "second GET should return same response (from cache)"
    );

    drop(stream);
    proxy_shutdown.store(true, Ordering::SeqCst);
    let _ = proxy_handle.join();

    backend_shutdown.shutdown();
    for h in backend_handles {
        let _ = h.join();
    }
}

#[test]
fn async_proxy_backend_disconnect() {
    let (backend_addr, backend_shutdown, backend_handles) = start_backend();

    let proxy_port = free_port();
    let proxy_addr = format!("127.0.0.1:{proxy_port}");
    let config = make_proxy_config(proxy_port, &backend_addr, false);
    let proxy_shutdown = Arc::new(AtomicBool::new(false));
    let proxy_handle = start_async_proxy(&config, Arc::clone(&proxy_shutdown));

    wait_for_server(&proxy_addr);

    // Shut down the backend while the proxy is running.
    backend_shutdown.shutdown();
    for h in backend_handles {
        let _ = h.join();
    }

    // Give the proxy time to detect the backend shutdown.
    std::thread::sleep(Duration::from_millis(200));

    // Connect to proxy and send a command — should get an error.
    let mut stream = TcpStream::connect(&proxy_addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let get_cmd = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    let response = send_resp_command(&mut stream, get_cmd);

    // Proxy should return an error (either connect failure or disconnect).
    assert!(
        response.starts_with("-ERR"),
        "expected error response, got: {response}"
    );

    drop(stream);
    proxy_shutdown.store(true, Ordering::SeqCst);
    let _ = proxy_handle.join();
}

#[test]
fn async_proxy_multiple_clients() {
    let (backend_addr, backend_shutdown, backend_handles) = start_backend();

    let proxy_port = free_port();
    let proxy_addr = format!("127.0.0.1:{proxy_port}");
    let config = make_proxy_config(proxy_port, &backend_addr, false);
    let proxy_shutdown = Arc::new(AtomicBool::new(false));
    let proxy_handle = start_async_proxy(&config, Arc::clone(&proxy_shutdown));

    wait_for_server(&proxy_addr);

    // Connect multiple clients sequentially.
    for i in 0..3 {
        let mut stream = TcpStream::connect(&proxy_addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let response = send_resp_command(&mut stream, b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(response, "+PONG\r\n", "client {i} did not get PONG");
        drop(stream);
    }

    proxy_shutdown.store(true, Ordering::SeqCst);
    let _ = proxy_handle.join();

    backend_shutdown.shutdown();
    for h in backend_handles {
        let _ = h.join();
    }
}

#[test]
fn async_proxy_idle_backend_disconnect() {
    // Start backend and proxy.
    let (backend_addr, backend_shutdown, backend_handles) = start_backend();

    let proxy_port = free_port();
    let proxy_addr = format!("127.0.0.1:{proxy_port}");
    let config = make_proxy_config(proxy_port, &backend_addr, false);
    let proxy_shutdown = Arc::new(AtomicBool::new(false));
    let proxy_handle = start_async_proxy(&config, Arc::clone(&proxy_shutdown));

    wait_for_server(&proxy_addr);

    // 1. Connect client, send PING, get PONG (proves proxy is working).
    let mut stream = TcpStream::connect(&proxy_addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let response = send_resp_command(&mut stream, b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(response, "+PONG\r\n");

    // 2. Shut down the backend while the client connection is idle.
    backend_shutdown.shutdown();
    for h in backend_handles {
        let _ = h.join();
    }

    // 3. Give the proxy's select loop time to detect backend disconnect.
    std::thread::sleep(Duration::from_millis(500));

    // 4. Send another command — should get -ERR because backend is gone.
    //    The select-based proxy detects backend disconnect while idle
    //    and either returns an error immediately or on the next send.
    let response = send_resp_command(&mut stream, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");

    // The proxy should return an error OR the connection should be closed.
    // Either way, the response should indicate backend failure.
    let got_error = response.starts_with("-ERR") || response.is_empty();
    assert!(
        got_error,
        "expected -ERR or empty (closed), got: {response}"
    );

    drop(stream);
    proxy_shutdown.store(true, Ordering::SeqCst);
    let _ = proxy_handle.join();
}
