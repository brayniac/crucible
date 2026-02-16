//! End-to-end integration tests for crucible-resp-client.
//!
//! Spawns a real crucible-server in a background thread and exercises the full
//! path: Client::connect → Tokio connection tasks → server → response →
//! oneshot channel → caller.
//!
//! All tests share a single server, tokio runtime, and client. Tests use unique
//! key prefixes to avoid interference.

use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, LazyLock};
use std::thread;
use std::time::Duration;

use crucible_resp_client::{Client, ClientConfig};

// ── Shared test environment ─────────────────────────────────────────────

struct TestEnv {
    client: Client,
    rt: tokio::runtime::Runtime,
    _shutdown: Arc<AtomicBool>,
}

/// Shared test environment: one server + one tokio runtime + one client.
/// The runtime persists for the entire test process so spawned connection
/// tasks don't get dropped between tests.
static ENV: LazyLock<TestEnv> = LazyLock::new(|| {
    let port = get_available_port();
    let (_handle, shutdown) = start_test_server(port);

    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    assert!(
        wait_for_server(addr, Duration::from_secs(5)),
        "Server failed to start within timeout"
    );

    // Build a multi-threaded runtime that persists for all tests.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    let client = rt.block_on(async {
        Client::connect(ClientConfig {
            servers: vec![format!("127.0.0.1:{port}")],
            pool_size: 1,
            ..Default::default()
        })
        .await
        .expect("Client::connect failed")
    });

    // Wait for connection to be ready
    rt.block_on(async {
        wait_for_client(&client).await;
    });

    TestEnv {
        client,
        rt,
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

async fn wait_for_client(client: &Client) {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if client.ping().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("Client failed to connect within timeout");
}

/// Run an async block on the shared runtime.
fn run<F: std::future::Future<Output = T>, T>(f: F) -> T {
    ENV.rt.block_on(f)
}

// ── Tests ───────────────────────────────────────────────────────────────

#[test]
fn test_ping() {
    run(async {
        ENV.client.ping().await.unwrap();
    });
}

#[test]
fn test_set_get() {
    run(async {
        ENV.client.set(b"e2e:sg:key", b"hello").await.unwrap();

        let val = ENV.client.get(b"e2e:sg:key").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"hello".as_ref()));
    });
}

#[test]
fn test_set_get_large_value() {
    run(async {
        let large_value = vec![0xABu8; 1024];

        ENV.client
            .set(b"e2e:lg:key", &large_value[..])
            .await
            .unwrap();

        let val = ENV.client.get(b"e2e:lg:key").await.unwrap();
        assert_eq!(val.as_deref(), Some(large_value.as_ref()));
    });
}

#[test]
fn test_get_nonexistent() {
    run(async {
        let val = ENV.client.get(b"e2e:nx:missing").await.unwrap();
        assert_eq!(val, None);
    });
}

#[test]
fn test_del() {
    run(async {
        ENV.client.set(b"e2e:del:key", b"value").await.unwrap();

        let count = ENV.client.del(b"e2e:del:key").await.unwrap();
        assert_eq!(count, 1);

        let val = ENV.client.get(b"e2e:del:key").await.unwrap();
        assert_eq!(val, None);
    });
}

#[test]
fn test_set_overwrite() {
    run(async {
        ENV.client.set(b"e2e:ow:key", b"first").await.unwrap();
        ENV.client.set(b"e2e:ow:key", b"second").await.unwrap();

        let val = ENV.client.get(b"e2e:ow:key").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"second".as_ref()));
    });
}

#[test]
fn test_incr_decr() {
    run(async {
        ENV.client.set(b"e2e:id:counter", b"10").await.unwrap();

        let val = ENV.client.incr(b"e2e:id:counter").await.unwrap();
        assert_eq!(val, 11);

        let val = ENV.client.decr(b"e2e:id:counter").await.unwrap();
        assert_eq!(val, 10);

        let val = ENV.client.incrby(b"e2e:id:counter", 5).await.unwrap();
        assert_eq!(val, 15);

        let val = ENV.client.decrby(b"e2e:id:counter", 3).await.unwrap();
        assert_eq!(val, 12);
    });
}

#[test]
fn test_append() {
    run(async {
        ENV.client.set(b"e2e:ap:key", b"hello").await.unwrap();

        let len = ENV.client.append(b"e2e:ap:key", b" world").await.unwrap();
        assert_eq!(len, 11);

        let val = ENV.client.get(b"e2e:ap:key").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"hello world".as_ref()));
    });
}

#[test]
fn test_mget() {
    run(async {
        ENV.client.set(b"e2e:mg:k1", b"v1").await.unwrap();
        ENV.client.set(b"e2e:mg:k2", b"v2").await.unwrap();

        let vals = ENV
            .client
            .mget(&[b"e2e:mg:k1", b"e2e:mg:k2", b"e2e:mg:missing"])
            .await
            .unwrap();

        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0].as_deref(), Some(b"v1".as_ref()));
        assert_eq!(vals[1].as_deref(), Some(b"v2".as_ref()));
        assert_eq!(vals[2], None);
    });
}

#[test]
fn test_set_nx() {
    run(async {
        // Should succeed — key doesn't exist
        let set = ENV.client.set_nx(b"e2e:snx:key", b"first").await.unwrap();
        assert!(set);

        // Should fail — key exists
        let set = ENV.client.set_nx(b"e2e:snx:key", b"second").await.unwrap();
        assert!(!set);

        // Value should still be "first"
        let val = ENV.client.get(b"e2e:snx:key").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"first".as_ref()));
    });
}

#[test]
fn test_key_type() {
    run(async {
        ENV.client.set(b"e2e:type:str", b"val").await.unwrap();

        let t = ENV.client.key_type(b"e2e:type:str").await.unwrap();
        assert_eq!(t, "string");

        // Non-existent key
        let t = ENV.client.key_type(b"e2e:type:missing").await.unwrap();
        assert_eq!(t, "none");
    });
}

// NOTE: Hash, list, and set command tests are omitted because the native
// server handler does not yet dispatch to execute_resp_data_structures.
// The client methods are implemented and encode correctly — they just
// can't be integration-tested against the current server binary.

#[test]
fn test_pipeline() {
    run(async {
        let results = ENV
            .client
            .pipeline()
            .set(b"e2e:pipe:k1", b"v1")
            .set(b"e2e:pipe:k2", b"v2")
            .get(b"e2e:pipe:k1")
            .get(b"e2e:pipe:k2")
            .execute()
            .await
            .unwrap();

        assert_eq!(results.len(), 4);

        // First two are SET responses (+OK)
        assert!(results[0].is_simple_string());
        assert!(results[1].is_simple_string());

        // Last two are GET responses
        assert_eq!(results[2].as_bytes(), Some(b"v1".as_ref()));
        assert_eq!(results[3].as_bytes(), Some(b"v2".as_ref()));
    });
}
