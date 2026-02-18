//! End-to-end integration tests for crucible-grpc-client.
//!
//! Spawns a plain-TCP gRPC test server driving `grpc::Server` directly.
//! No external deps needed — just `std::net::TcpListener` + `grpc::Server`.
//!
//! All tests share a single server+client via `LazyLock` (OnceLock config
//! channel means one `GrpcClient::connect` per process). Unique paths prevent
//! interference.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::LazyLock;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use crucible_grpc_client::{Code, GrpcClient, GrpcClientConfig, Metadata};
use grpc::{Server, Status};

// ── Platform check ──────────────────────────────────────────────────────

/// Check if io_uring is supported on this kernel.
fn io_uring_supported() -> bool {
    let ret = unsafe { libc::syscall(libc::SYS_io_uring_setup, 1u32, std::ptr::null_mut::<u8>()) };
    // EFAULT (bad params pointer) means the syscall exists; ENOSYS means it doesn't.
    ret != -1 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ENOSYS)
}

// ── Shared test environment ─────────────────────────────────────────────

struct TestEnv {
    client: GrpcClient,
    _server_handle: thread::JoinHandle<()>,
}

static ENV: LazyLock<Option<TestEnv>> = LazyLock::new(|| {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return None;
    }

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

    let client = GrpcClient::connect(GrpcClientConfig {
        servers: vec![format!("127.0.0.1:{port}")],
        workers: 1,
        connections_per_server: 1,
        ..Default::default()
    })
    .expect("GrpcClient::connect failed");

    Some(TestEnv {
        client,
        _server_handle: server_handle,
    })
});

// ── Test gRPC server ────────────────────────────────────────────────────

fn run_test_server(listener: TcpListener) {
    listener.set_nonblocking(false).unwrap();

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

    let mut server = Server::new();
    let mut buf = [0u8; 16384];

    loop {
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

        server.on_recv(&buf[..n]);
        let events = match server.process() {
            Ok(events) => events,
            Err(_) => break,
        };

        flush_server(&mut server, &mut stream);

        for event in events {
            match event {
                grpc::GrpcServerEvent::Ready => {}
                grpc::GrpcServerEvent::Request(request) => {
                    handle_request(&mut server, &mut stream, request);
                }
                grpc::GrpcServerEvent::RequestData { .. } => {}
                grpc::GrpcServerEvent::StreamReset { .. } => {}
                grpc::GrpcServerEvent::GoAway => return,
                grpc::GrpcServerEvent::Error(_) => return,
            }
        }

        flush_server(&mut server, &mut stream);
    }
}

fn handle_request(server: &mut Server, stream: &mut TcpStream, request: grpc::Request) {
    let stream_id = request.stream_id;
    let path = &request.path;

    match path.as_str() {
        "/test.Echo/Echo" => {
            // Echo: return request body as response with OK status
            let _ = server.send_response(stream_id, Status::ok(), &request.message);
        }
        "/test.Echo/Error" => {
            // Return NOT_FOUND error
            let _ = server.send_error(stream_id, Status::not_found("entity not found"));
        }
        "/test.Echo/Metadata" => {
            // Echo request metadata in response headers, then send body
            // We send response headers first, then data, then trailers
            let _ = server.send_response_headers(stream_id);
            if !request.message.is_empty() {
                let _ = server.send_response_message(stream_id, &request.message);
            }
            let _ = server.send_trailers(stream_id, Status::ok());
        }
        _ => {
            let _ = server.send_error(
                stream_id,
                Status::new(Code::Unimplemented, format!("unknown path: {path}")),
            );
        }
    }

    flush_server(server, stream);
}

fn flush_server(server: &mut Server, stream: &mut TcpStream) {
    loop {
        let pending = server.pending_send();
        if pending.is_empty() {
            break;
        }
        let n = pending.len();
        let data = pending.to_vec();
        server.advance_send(n);
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

/// Return the test client, or skip (return) if io_uring is unavailable.
macro_rules! require_env {
    () => {
        match ENV.as_ref() {
            Some(env) => &env.client,
            None => {
                eprintln!("SKIP: io_uring not supported");
                return;
            }
        }
    };
}

/// Wait for the client to establish its gRPC connection.
async fn wait_for_client(client: &GrpcClient) {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        match client.call("/test.Echo/Echo", Bytes::from("ping")).await {
            Ok(resp) if resp.is_ok() => return,
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Client failed to connect within timeout");
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_unary_echo() {
    let client = require_env!();
    wait_for_client(client).await;

    let resp = client
        .call("/test.Echo/Echo", Bytes::from("hello world"))
        .await
        .unwrap();
    assert!(resp.is_ok(), "status: {:?}", resp.status());
    assert_eq!(resp.body().as_ref(), b"hello world");
}

#[tokio::test]
async fn test_unary_echo_large() {
    let client = require_env!();
    wait_for_client(client).await;

    // 2KB body — exercises flow control
    let body = vec![0xABu8; 2048];
    let resp = client
        .call("/test.Echo/Echo", Bytes::from(body.clone()))
        .await
        .unwrap();
    assert!(resp.is_ok(), "status: {:?}", resp.status());
    assert_eq!(resp.body().as_ref(), &body[..]);
}

#[tokio::test]
async fn test_error_status() {
    let client = require_env!();
    wait_for_client(client).await;

    let resp = client.call("/test.Echo/Error", Bytes::new()).await.unwrap();
    assert!(!resp.is_ok());
    assert_eq!(resp.status().code(), Code::NotFound);
}

#[tokio::test]
async fn test_custom_metadata() {
    let client = require_env!();
    wait_for_client(client).await;

    let mut metadata = Metadata::new();
    metadata.insert("x-custom-header", "test-value");

    let resp = client
        .unary("/test.Echo/Metadata", metadata, Bytes::from("meta-body"))
        .await
        .unwrap();
    assert!(resp.is_ok(), "status: {:?}", resp.status());
    assert_eq!(resp.body().as_ref(), b"meta-body");
}

#[tokio::test]
async fn test_call_convenience() {
    let client = require_env!();
    wait_for_client(client).await;

    let resp = client
        .call("/test.Echo/Echo", Bytes::from("simple"))
        .await
        .unwrap();
    assert!(resp.is_ok());
    assert_eq!(resp.body().as_ref(), b"simple");
}

#[tokio::test]
async fn test_concurrent_calls() {
    let client = require_env!();
    wait_for_client(client).await;

    // Send multiple requests in parallel (HTTP/2 multiplexing)
    let mut handles = Vec::new();
    for i in 0..5 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let body = format!("msg-{i}");
            let resp = c
                .call("/test.Echo/Echo", Bytes::from(body.clone()))
                .await
                .unwrap();
            assert!(resp.is_ok());
            assert_eq!(resp.body().as_ref(), body.as_bytes());
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_latency_histograms() {
    let client = require_env!();
    wait_for_client(client).await;

    // Perform a call to populate histograms
    let _ = client
        .call("/test.Echo/Echo", Bytes::from("latency"))
        .await
        .unwrap();

    let latency = client.latency();

    assert!(
        latency.unary().load().is_some(),
        "unary histogram should have observations"
    );
}
