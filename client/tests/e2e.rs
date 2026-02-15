//! End-to-end integration tests for crucible-client.
//!
//! Spawns a real crucible-server in a background thread and exercises the full
//! path: Client::connect → kompio workers → io_uring → server → response →
//! oneshot channel → caller.
//!
//! All tests share a single server and client because the client's config
//! channel uses `OnceLock` (can only be initialized once per process). Tests
//! use unique key prefixes to avoid interference.

use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, LazyLock};
use std::thread;
use std::time::Duration;

use crucible_client::{Client, ClientConfig};

// ── Shared test environment ─────────────────────────────────────────────

struct TestEnv {
    client: Client,
    _shutdown: Arc<AtomicBool>,
}

static ENV: LazyLock<TestEnv> = LazyLock::new(|| {
    let port = get_available_port();
    let (_handle, shutdown) = start_test_server(port);

    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start within timeout"
    );

    let client = Client::connect(ClientConfig {
        servers: vec![format!("127.0.0.1:{port}")],
        workers: 1,
        connections_per_server: 1,
        ..Default::default()
    })
    .expect("Client::connect failed");

    TestEnv {
        client,
        _shutdown: shutdown,
    }
});

// ── Helpers ─────────────────────────────────────────────────────────────

fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

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

fn start_test_server(cache_port: u16) -> (thread::JoinHandle<()>, Arc<AtomicBool>) {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    let handle = thread::spawn(move || {
        let config_str = format!(
            r#"
            [workers]
            threads = 2

            [cache]
            backend = "segment"
            heap_size = "64MB"
            segment_size = "1MB"
            hashtable_power = 18

            [[listener]]
            protocol = "resp"
            address = "127.0.0.1:{cache_port}"

            [metrics]
            address = "127.0.0.1:0"
            "#,
        );

        let config: server::Config = toml::from_str(&config_str).unwrap();
        let cache = segcache::SegCache::builder()
            .heap_size(config.cache.heap_size)
            .segment_size(config.cache.segment_size)
            .hashtable_power(config.cache.hashtable_power)
            .build()
            .unwrap();

        let drain_timeout = Duration::from_millis(500);
        let _ = server::native::run(&config, cache, shutdown_clone, drain_timeout);
    });

    (handle, shutdown)
}

/// Wait for the client to establish its connection to the server.
async fn wait_for_client(client: &Client) {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if client.ping().await.is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("Client failed to connect within timeout");
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_ping() {
    let client = &ENV.client;
    wait_for_client(client).await;

    client.ping().await.unwrap();
}

#[tokio::test]
async fn test_set_get() {
    let client = &ENV.client;
    wait_for_client(client).await;

    client.set(b"e2e:sg:key", &b"hello"[..]).await.unwrap();

    let val = client.get(b"e2e:sg:key").await.unwrap();
    assert_eq!(val.as_deref(), Some(b"hello".as_ref()));
}

#[tokio::test]
async fn test_set_get_large_value() {
    let client = &ENV.client;
    wait_for_client(client).await;

    // 1024 bytes — above ZC_THRESHOLD (512), exercises zero-copy send path
    let large_value = vec![0xABu8; 1024];

    client
        .set(b"e2e:lg:key", large_value.clone())
        .await
        .unwrap();

    let val = client.get(b"e2e:lg:key").await.unwrap();
    assert_eq!(val.as_deref(), Some(large_value.as_ref()));
}

#[tokio::test]
async fn test_get_nonexistent() {
    let client = &ENV.client;
    wait_for_client(client).await;

    let val = client.get(b"e2e:nx:missing").await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn test_del() {
    let client = &ENV.client;
    wait_for_client(client).await;

    client.set(b"e2e:del:key", &b"value"[..]).await.unwrap();

    let count = client.del(b"e2e:del:key").await.unwrap();
    assert_eq!(count, 1);

    let val = client.get(b"e2e:del:key").await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn test_set_overwrite() {
    let client = &ENV.client;
    wait_for_client(client).await;

    client.set(b"e2e:ow:key", &b"first"[..]).await.unwrap();
    client.set(b"e2e:ow:key", &b"second"[..]).await.unwrap();

    let val = client.get(b"e2e:ow:key").await.unwrap();
    assert_eq!(val.as_deref(), Some(b"second".as_ref()));
}

#[tokio::test]
async fn test_latency_histograms() {
    let client = &ENV.client;
    wait_for_client(client).await;

    // Perform operations to populate histograms
    client.set(b"e2e:lat:key", &b"val"[..]).await.unwrap();
    let _ = client.get(b"e2e:lat:key").await.unwrap();
    let _ = client.del(b"e2e:lat:key").await.unwrap();

    let latency = client.latency();

    // Combined request histogram should have observations
    assert!(
        latency.request().load().is_some(),
        "request histogram should have observations"
    );

    // Per-operation histograms should have observations
    assert!(
        latency.set().load().is_some(),
        "SET histogram should have observations"
    );
    assert!(
        latency.get().load().is_some(),
        "GET histogram should have observations"
    );
    assert!(
        latency.del().load().is_some(),
        "DEL histogram should have observations"
    );
}
