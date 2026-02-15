//! End-to-end integration tests for crucible-http-client.
//!
//! Spawns a plain-TCP HTTP/2 test server driving `http2::ServerConnection`
//! directly. No external deps needed — just `std::net::TcpListener` +
//! `ServerConnection`.
//!
//! All tests share a single server+client via `LazyLock` (OnceLock config
//! channel means one `HttpClient::connect` per process). Unique paths prevent
//! interference.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::LazyLock;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use crucible_http_client::{HttpClient, HttpClientConfig};
use http2::hpack::HeaderField;
use http2::{ServerConnection, ServerEvent};

// ── Shared test environment ─────────────────────────────────────────────

struct TestEnv {
    client: HttpClient,
    _server_handle: thread::JoinHandle<()>,
}

static ENV: LazyLock<TestEnv> = LazyLock::new(|| {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_handle = thread::spawn(move || {
        run_test_server(listener);
    });

    // Wait for server to be ready (it's already listening since we bound before spawning)
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start within timeout"
    );

    let client = HttpClient::connect(HttpClientConfig {
        servers: vec![format!("127.0.0.1:{port}")],
        workers: 1,
        connections_per_server: 1,
        ..Default::default()
    })
    .expect("HttpClient::connect failed");

    TestEnv {
        client,
        _server_handle: server_handle,
    }
});

// ── Test HTTP/2 server ──────────────────────────────────────────────────

fn run_test_server(listener: TcpListener) {
    listener.set_nonblocking(false).unwrap();

    // Accept connections in a loop
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    handle_connection(stream);
                });
            }
            Err(_) => break,
        }
    }
}

fn handle_connection(mut stream: TcpStream) {
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();

    let mut conn = ServerConnection::new();
    let mut buf = [0u8; 16384];

    // Accumulated request data per stream: (headers, body, end_stream)
    let mut requests: std::collections::HashMap<u32, (Vec<HeaderField>, Vec<u8>, bool)> =
        std::collections::HashMap::new();

    loop {
        // Read data from TCP
        let n = match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(_) => break,
        };

        // Feed to HTTP/2 server connection
        conn.on_recv(&buf[..n]);
        if conn.process().is_err() {
            break;
        }

        // Flush any pending data (settings, settings ack, etc.)
        flush_server(&mut conn, &mut stream);

        // Process events
        let events = conn.poll_events();
        for event in events {
            match event {
                ServerEvent::Ready => {}
                ServerEvent::Request {
                    stream_id,
                    headers,
                    end_stream,
                } => {
                    requests.insert(stream_id.value(), (headers, Vec::new(), end_stream));
                    if end_stream {
                        handle_request(&mut conn, &mut stream, stream_id.value(), &mut requests);
                    }
                }
                ServerEvent::Data {
                    stream_id,
                    data,
                    end_stream,
                } => {
                    if let Some(req) = requests.get_mut(&stream_id.value()) {
                        req.1.extend_from_slice(&data);
                        req.2 = end_stream;
                        if end_stream {
                            handle_request(
                                &mut conn,
                                &mut stream,
                                stream_id.value(),
                                &mut requests,
                            );
                        }
                    }
                }
                ServerEvent::StreamReset { .. } => {}
                ServerEvent::GoAway { .. } => return,
                ServerEvent::Error(_) => return,
            }
        }

        // Flush response data
        flush_server(&mut conn, &mut stream);
    }
}

fn handle_request(
    conn: &mut ServerConnection,
    stream: &mut TcpStream,
    stream_id: u32,
    requests: &mut std::collections::HashMap<u32, (Vec<HeaderField>, Vec<u8>, bool)>,
) {
    let (headers, body, _) = match requests.remove(&stream_id) {
        Some(v) => v,
        None => return,
    };

    // Extract :method and :path
    let mut method = String::new();
    let mut path = String::new();
    for hf in &headers {
        let name = String::from_utf8_lossy(&hf.name);
        let value = String::from_utf8_lossy(&hf.value);
        match name.as_ref() {
            ":method" => method = value.into_owned(),
            ":path" => path = value.into_owned(),
            _ => {}
        }
    }

    let sid = http2::frame::StreamId::new(stream_id);

    // Route
    match (method.as_str(), path.as_str()) {
        ("GET", "/ping") => {
            send_response(conn, stream, sid, 200, b"pong");
        }
        ("POST", "/echo") => {
            send_response(conn, stream, sid, 200, &body);
        }
        ("DELETE", "/resource") => {
            send_response(conn, stream, sid, 200, b"deleted");
        }
        ("GET", p) if p.starts_with("/status/") => {
            let status_str = &p[8..];
            let status: u16 = status_str.parse().unwrap_or(500);
            send_response(conn, stream, sid, status, b"");
        }
        _ => {
            send_response(conn, stream, sid, 404, b"not found");
        }
    }

    conn.remove_stream(sid);
}

fn send_response(
    conn: &mut ServerConnection,
    stream: &mut TcpStream,
    stream_id: http2::frame::StreamId,
    status: u16,
    body: &[u8],
) {
    let status_str = status.to_string();
    let headers = vec![HeaderField::new(
        b":status".to_vec(),
        status_str.into_bytes(),
    )];

    let end_stream = body.is_empty();
    let _ = conn.send_headers(stream_id, &headers, end_stream);
    flush_server(conn, stream);

    if !body.is_empty() {
        let _ = conn.send_data(stream_id, body, true);
        flush_server(conn, stream);
    }
}

fn flush_server(conn: &mut ServerConnection, stream: &mut TcpStream) {
    loop {
        let pending = conn.pending_send();
        if pending.is_empty() {
            break;
        }
        let n = pending.len();
        // Must copy before advancing (pending_send borrows conn)
        let data = pending.to_vec();
        conn.advance_send(n);
        if stream.write_all(&data).is_err() {
            break;
        }
    }
    let _ = stream.flush();
}

// ── Helpers ─────────────────────────────────────────────────────────────

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

/// Wait for the client to establish its HTTP/2 connection.
async fn wait_for_client(client: &HttpClient) {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        match client.get("/ping").await {
            Ok(resp) if resp.status() == 200 => return,
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Client failed to connect within timeout");
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get() {
    let client = &ENV.client;
    wait_for_client(client).await;

    let resp = client.get("/ping").await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.body().as_ref(), b"pong");
}

#[tokio::test]
async fn test_post_echo() {
    let client = &ENV.client;
    wait_for_client(client).await;

    let resp = client.post("/echo", Bytes::from("hello world")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.body().as_ref(), b"hello world");
}

#[tokio::test]
async fn test_post_echo_large() {
    let client = &ENV.client;
    wait_for_client(client).await;

    // 2KB body — exercises flow control
    let body = vec![0xABu8; 2048];
    let resp = client.post("/echo", Bytes::from(body.clone())).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.body().as_ref(), &body[..]);
}

#[tokio::test]
async fn test_get_not_found() {
    let client = &ENV.client;
    wait_for_client(client).await;

    let resp = client.get("/nonexistent").await.unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn test_delete() {
    let client = &ENV.client;
    wait_for_client(client).await;

    let resp = client.delete("/resource").await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.body().as_ref(), b"deleted");
}

#[tokio::test]
async fn test_concurrent_requests() {
    let client = &ENV.client;
    wait_for_client(client).await;

    // Send multiple requests in parallel (HTTP/2 multiplexing)
    let mut handles = Vec::new();
    for i in 0..5 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let path = format!("/status/{}", 200 + i);
            let resp = c.get(&path).await.unwrap();
            assert_eq!(resp.status(), 200 + i);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_latency_histograms() {
    let client = &ENV.client;
    wait_for_client(client).await;

    // Perform operations to populate histograms
    let _ = client.get("/ping").await.unwrap();
    let _ = client.post("/echo", Bytes::from("test")).await.unwrap();

    let latency = client.latency();

    assert!(
        latency.request().load().is_some(),
        "request histogram should have observations"
    );
    assert!(
        latency.get().load().is_some(),
        "GET histogram should have observations"
    );
    assert!(
        latency.post().load().is_some(),
        "POST histogram should have observations"
    );
}
