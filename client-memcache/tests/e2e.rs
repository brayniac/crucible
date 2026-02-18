//! End-to-end integration tests for crucible-memcache-client.
//!
//! Spawns a real crucible-server in a background thread and exercises the full
//! path: Client::connect -> krio workers -> io_uring -> server -> response ->
//! oneshot channel -> caller.
//!
//! All tests share a single server and client because the client's config
//! channel uses `OnceLock` (can only be initialized once per process). Tests
//! use unique key prefixes to avoid interference.

use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, LazyLock};
use std::thread;
use std::time::Duration;

use crucible_memcache_client::{Client, ClientConfig};

/// Check if io_uring is supported on this kernel.
fn io_uring_supported() -> bool {
    let ret = unsafe { libc::syscall(libc::SYS_io_uring_setup, 1u32, std::ptr::null_mut::<u8>()) };
    ret != -1 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ENOSYS)
}

// -- Shared test environment ------------------------------------------------

struct TestEnv {
    client: Client,
    _shutdown: Arc<AtomicBool>,
}

static ENV: LazyLock<Option<TestEnv>> = LazyLock::new(|| {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return None;
    }

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

    Some(TestEnv {
        client,
        _shutdown: shutdown,
    })
});

// -- Helpers ----------------------------------------------------------------

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
            protocol = "memcache"
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
        if client.version().await.is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("Client failed to connect within timeout");
}

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

// -- Tests ------------------------------------------------------------------

#[tokio::test]
async fn test_version() {
    let client = require_env!();
    wait_for_client(client).await;

    client.version().await.unwrap();
}

#[tokio::test]
async fn test_set_get() {
    let client = require_env!();
    wait_for_client(client).await;

    client.set(b"mc:sg:key", &b"hello"[..]).await.unwrap();

    let val = client.get(b"mc:sg:key").await.unwrap();
    assert_eq!(val.as_deref(), Some(b"hello".as_ref()));
}

#[tokio::test]
async fn test_set_get_large_value() {
    let client = require_env!();
    wait_for_client(client).await;

    // 1024 bytes -- above ZC_THRESHOLD (512), exercises zero-copy send path
    let large_value = vec![0xABu8; 1024];

    client.set(b"mc:lg:key", large_value.clone()).await.unwrap();

    let val = client.get(b"mc:lg:key").await.unwrap();
    assert_eq!(val.as_deref(), Some(large_value.as_ref()));
}

#[tokio::test]
async fn test_get_nonexistent() {
    let client = require_env!();
    wait_for_client(client).await;

    let val = client.get(b"mc:nx:missing").await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn test_del() {
    let client = require_env!();
    wait_for_client(client).await;

    client.set(b"mc:del:key", &b"value"[..]).await.unwrap();

    let deleted = client.del(b"mc:del:key").await.unwrap();
    assert!(deleted);

    let val = client.get(b"mc:del:key").await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn test_del_nonexistent() {
    let client = require_env!();
    wait_for_client(client).await;

    let deleted = client.del(b"mc:delnx:missing").await.unwrap();
    assert!(!deleted);
}

#[tokio::test]
async fn test_set_overwrite() {
    let client = require_env!();
    wait_for_client(client).await;

    client.set(b"mc:ow:key", &b"first"[..]).await.unwrap();
    client.set(b"mc:ow:key", &b"second"[..]).await.unwrap();

    let val = client.get(b"mc:ow:key").await.unwrap();
    assert_eq!(val.as_deref(), Some(b"second".as_ref()));
}

#[tokio::test]
async fn test_set_with_options() {
    let client = require_env!();
    wait_for_client(client).await;

    // set_with_options sends flags/exptime in the protocol; the segcache
    // backend does not preserve flags, so we verify the value round-trips.
    client
        .set_with_options(b"mc:fl:key", &b"flagged"[..], 42, 0)
        .await
        .unwrap();

    let val = client.get(b"mc:fl:key").await.unwrap();
    assert_eq!(val.as_deref(), Some(b"flagged".as_ref()));

    // get_with_flags returns (data, flags) -- flags may be 0 since segcache
    // does not store memcache flags, but the API should work without error.
    let result = client.get_with_flags(b"mc:fl:key").await.unwrap();
    assert!(result.is_some());
    let (data, _flags) = result.unwrap();
    assert_eq!(&data[..], b"flagged");
}

#[tokio::test]
async fn test_latency_histograms() {
    let client = require_env!();
    wait_for_client(client).await;

    // Perform operations to populate histograms
    client.set(b"mc:lat:key", &b"val"[..]).await.unwrap();
    let _ = client.get(b"mc:lat:key").await.unwrap();
    let _ = client.del(b"mc:lat:key").await.unwrap();

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
