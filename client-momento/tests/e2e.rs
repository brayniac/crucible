//! End-to-end integration tests for crucible-momento-client.
//!
//! Spawns a plain-TCP Momento test server driving `CacheServer` directly.
//! No external deps needed — just `std::net::TcpListener` + `CacheServer`.
//!
//! All tests share a single server+client via `LazyLock` (OnceLock config
//! channel means one `MomentoClient::connect` per process). Unique keys
//! prevent interference.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::LazyLock;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use crucible_momento_client::{Credential, MomentoClient, MomentoClientConfig};
use protocol_momento::proto::{DeleteResponse, GetResponse, SetResponse};
use protocol_momento::{
    CacheRequest, CacheResponse, CacheServer, CacheServerBuilder, CacheServerEvent, WireFormat,
};

// ── Shared test environment ─────────────────────────────────────────────

struct TestEnv {
    client: MomentoClient,
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

    let credential = Credential::with_endpoint("test-token", "127.0.0.1");

    let client = MomentoClient::connect(MomentoClientConfig {
        credential,
        cache_name: "test-cache".to_string(),
        workers: 1,
        connections_per_server: 1,
        connect_timeout_ms: 5000,
        tcp_nodelay: true,
        default_ttl: Duration::from_secs(3600),
        servers: vec![addr],
    })
    .expect("MomentoClient::connect failed");

    TestEnv {
        client,
        _server_handle: server_handle,
    }
});

// ── Test Momento server ────────────────────────────────────────────────────

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

    let mut server = CacheServerBuilder::new()
        .wire_format(WireFormat::Grpc)
        .build();
    let mut store: HashMap<(String, Vec<u8>), Vec<u8>> = HashMap::new();
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
                CacheServerEvent::Ready => {}
                CacheServerEvent::Request(request) => {
                    handle_request(&mut server, &mut stream, &mut store, request);
                }
                CacheServerEvent::RequestCancelled { .. } => {}
                CacheServerEvent::GoAway => return,
                CacheServerEvent::Error(_) => return,
            }
        }

        flush_server(&mut server, &mut stream);
    }
}

fn handle_request(
    server: &mut CacheServer,
    stream: &mut TcpStream,
    store: &mut HashMap<(String, Vec<u8>), Vec<u8>>,
    request: CacheRequest,
) {
    let request_id = request.request_id();

    match request {
        CacheRequest::Get {
            cache_name, key, ..
        } => {
            let cache = cache_name.unwrap_or_default();
            let response = if let Some(value) = store.get(&(cache, key.to_vec())) {
                CacheResponse::Get(GetResponse::Hit(Bytes::from(value.clone())))
            } else {
                CacheResponse::Get(GetResponse::Miss)
            };
            let _ = server.send_response(request_id, response);
        }
        CacheRequest::Set {
            cache_name,
            key,
            value,
            ..
        } => {
            let cache = cache_name.unwrap_or_default();
            store.insert((cache, key.to_vec()), value.to_vec());
            let _ = server.send_response(request_id, CacheResponse::Set(SetResponse::Ok));
        }
        CacheRequest::Delete {
            cache_name, key, ..
        } => {
            let cache = cache_name.unwrap_or_default();
            store.remove(&(cache, key.to_vec()));
            let _ = server.send_response(request_id, CacheResponse::Delete(DeleteResponse::Ok));
        }
    }

    flush_server(server, stream);
}

fn flush_server(server: &mut CacheServer, stream: &mut TcpStream) {
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

/// Wait for the client to establish its Momento connection.
async fn wait_for_client(client: &MomentoClient) {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        match client.get(b"__health_check__").await {
            Ok(_) => return, // Even a miss means connection is working
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Client failed to connect within timeout");
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_set_get_roundtrip() {
    let client = &ENV.client;
    wait_for_client(client).await;

    client
        .set(b"roundtrip-key", b"roundtrip-value")
        .await
        .unwrap();

    let result = client.get(b"roundtrip-key").await.unwrap();
    assert!(result.is_hit(), "expected Hit, got {:?}", result);
    assert_eq!(result.into_value().unwrap().as_ref(), b"roundtrip-value");
}

#[tokio::test]
async fn test_get_miss() {
    let client = &ENV.client;
    wait_for_client(client).await;

    let result = client.get(b"nonexistent-key-12345").await.unwrap();
    assert!(result.is_miss(), "expected Miss, got {:?}", result);
}

#[tokio::test]
async fn test_delete() {
    let client = &ENV.client;
    wait_for_client(client).await;

    client.set(b"delete-key", b"delete-value").await.unwrap();

    // Verify it's there
    let result = client.get(b"delete-key").await.unwrap();
    assert!(result.is_hit());

    // Delete it
    client.delete(b"delete-key").await.unwrap();

    // Verify it's gone
    let result = client.get(b"delete-key").await.unwrap();
    assert!(
        result.is_miss(),
        "expected Miss after delete, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_large_value() {
    let client = &ENV.client;
    wait_for_client(client).await;

    // 2KB+ value for flow control testing
    let value = vec![0xABu8; 2048];
    client.set(b"large-key", &value).await.unwrap();

    let result = client.get(b"large-key").await.unwrap();
    assert!(result.is_hit());
    assert_eq!(result.into_value().unwrap().as_ref(), &value[..]);
}

#[tokio::test]
async fn test_concurrent_operations() {
    let client = &ENV.client;
    wait_for_client(client).await;

    // Send multiple requests in parallel
    let mut handles = Vec::new();
    for i in 0..5 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("concurrent-key-{i}");
            let value = format!("concurrent-value-{i}");
            c.set(key.as_bytes(), value.as_bytes()).await.unwrap();

            let result = c.get(key.as_bytes()).await.unwrap();
            assert!(result.is_hit());
            assert_eq!(result.into_value().unwrap().as_ref(), value.as_bytes());
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
    client.set(b"latency-key", b"latency-value").await.unwrap();
    let _ = client.get(b"latency-key").await.unwrap();
    client.delete(b"latency-key").await.unwrap();

    let latency = client.latency();

    assert!(
        latency.get().load().is_some(),
        "get histogram should have observations"
    );
    assert!(
        latency.set().load().is_some(),
        "set histogram should have observations"
    );
    assert!(
        latency.delete().load().is_some(),
        "delete histogram should have observations"
    );
}

#[tokio::test]
async fn test_set_with_ttl() {
    let client = &ENV.client;
    wait_for_client(client).await;

    // TTL is not enforced by the test server, just verify the API works
    client
        .set_with_ttl(b"ttl-key", b"ttl-value", Duration::from_secs(300))
        .await
        .unwrap();

    let result = client.get(b"ttl-key").await.unwrap();
    assert!(result.is_hit());
    assert_eq!(result.into_value().unwrap().as_ref(), b"ttl-value");
}
