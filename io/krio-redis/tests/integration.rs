//! Integration tests for krio-redis.
//!
//! Each test starts a real crucible-server, launches krio in client-only mode
//! via `on_start()`, runs commands, asserts results, and shuts down.

use std::future::Future;
use std::net::{SocketAddr, TcpStream};
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use krio::{AsyncEventHandler, Config, ConnCtx, KrioBuilder};

// ── Shared test server ──────────────────────────────────────────────────

struct ServerInfo {
    addr: SocketAddr,
    _shutdown: Arc<AtomicBool>,
}

static SERVER: OnceLock<ServerInfo> = OnceLock::new();

fn server_addr() -> SocketAddr {
    SERVER
        .get_or_init(|| {
            let port = free_port();
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let shutdown = start_test_server(port);
            wait_for_server(addr);
            ServerInfo {
                addr,
                _shutdown: shutdown,
            }
        })
        .addr
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn start_test_server(port: u16) -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    thread::spawn(move || {
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
            address = "127.0.0.1:{port}"

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

    shutdown
}

fn wait_for_server(addr: SocketAddr) {
    for _ in 0..200 {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not start on {addr}");
}

fn krio_config() -> Config {
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

// ── Test runner helper ──────────────────────────────────────────────────

// Serialize tests — NEXT_TEST can only hold one test's state at a time.
static TEST_SERIALIZE: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Run an async test function inside krio's on_start().
///
/// The test function receives a connected `krio_redis::Client` and should
/// return `Ok(())` on success. Errors are captured and reported.
fn run_client_test<F>(test_fn: F)
where
    F: FnOnce(
            krio_redis::Client,
        ) -> Pin<Box<dyn Future<Output = Result<(), krio_redis::Error>> + 'static>>
        + Send
        + 'static,
{
    let _guard = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());
    let addr = server_addr();

    // Channel to send result from on_start back to test thread.
    let (tx, rx) = std::sync::mpsc::channel::<Result<(), String>>();

    type TestFn = Box<
        dyn FnOnce(
                krio_redis::Client,
            )
                -> Pin<Box<dyn Future<Output = Result<(), krio_redis::Error>> + 'static>>
            + Send,
    >;
    type TestState = Option<(
        std::sync::mpsc::Sender<Result<(), String>>,
        SocketAddr,
        TestFn,
    )>;

    use std::sync::Mutex;
    static NEXT_TEST: Mutex<TestState> = Mutex::new(None);

    *NEXT_TEST.lock().unwrap() = Some((tx, addr, Box::new(test_fn)));

    struct Handler;

    impl AsyncEventHandler for Handler {
        fn on_accept(&self, _conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
            Box::pin(async {})
        }

        fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
            let (tx, addr, test_fn) = NEXT_TEST.lock().unwrap().take().expect("no test state");
            Some(Box::pin(async move {
                let result = match krio::connect(addr) {
                    Ok(fut) => match fut.await {
                        Ok(conn) => {
                            let client = krio_redis::Client::new(conn);
                            test_fn(client).await.map_err(|e| e.to_string())
                        }
                        Err(e) => Err(format!("connect failed: {e}")),
                    },
                    Err(e) => Err(format!("connect submit failed: {e}")),
                };
                let _ = tx.send(result);
                krio::request_shutdown();
            }))
        }

        fn create_for_worker(_id: usize) -> Self {
            Handler
        }
    }

    let (_shutdown, handles) = KrioBuilder::new(krio_config())
        .launch_async::<Handler>()
        .expect("krio launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }

    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("test failed: {e}"),
        Err(_) => panic!("test timed out"),
    }
}

/// Run an async test function inside krio's on_start(), passing the server
/// address so the test can create its own connections or pools.
fn run_pool_test<F>(test_fn: F)
where
    F: FnOnce(SocketAddr) -> Pin<Box<dyn Future<Output = Result<(), krio_redis::Error>> + 'static>>
        + Send
        + 'static,
{
    let _guard = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());
    let addr = server_addr();

    let (tx, rx) = std::sync::mpsc::channel::<Result<(), String>>();

    type TestFn = Box<
        dyn FnOnce(
                SocketAddr,
            )
                -> Pin<Box<dyn Future<Output = Result<(), krio_redis::Error>> + 'static>>
            + Send,
    >;
    type TestState = Option<(
        std::sync::mpsc::Sender<Result<(), String>>,
        SocketAddr,
        TestFn,
    )>;

    use std::sync::Mutex;
    static NEXT_POOL_TEST: Mutex<TestState> = Mutex::new(None);

    *NEXT_POOL_TEST.lock().unwrap() = Some((tx, addr, Box::new(test_fn)));

    struct PoolHandler;

    impl AsyncEventHandler for PoolHandler {
        fn on_accept(&self, _conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
            Box::pin(async {})
        }

        fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
            let (tx, addr, test_fn) = NEXT_POOL_TEST
                .lock()
                .unwrap()
                .take()
                .expect("no test state");
            Some(Box::pin(async move {
                let result = test_fn(addr).await.map_err(|e| e.to_string());
                let _ = tx.send(result);
                krio::request_shutdown();
            }))
        }

        fn create_for_worker(_id: usize) -> Self {
            PoolHandler
        }
    }

    let (_shutdown, handles) = KrioBuilder::new(krio_config())
        .launch_async::<PoolHandler>()
        .expect("krio launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }

    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("test failed: {e}"),
        Err(_) => panic!("test timed out"),
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[test]
fn test_ping() {
    run_client_test(|client| {
        Box::pin(async move {
            client.ping().await?;
            Ok(())
        })
    });
}

#[test]
fn test_get_set_del() {
    run_client_test(|client| {
        Box::pin(async move {
            client.set(b"kc:gsd:key", b"hello").await?;

            let val = client.get(b"kc:gsd:key").await?;
            assert_eq!(val.as_deref(), Some(b"hello".as_ref()));

            let count = client.del(b"kc:gsd:key").await?;
            assert_eq!(count, 1);

            let val = client.get(b"kc:gsd:key").await?;
            assert_eq!(val, None);

            Ok(())
        })
    });
}

#[test]
fn test_set_nx() {
    run_client_test(|client| {
        Box::pin(async move {
            // Should succeed — key doesn't exist
            let set = client.set_nx(b"kc:snx:key", b"first").await?;
            assert!(set);

            // Should fail — key exists
            let set = client.set_nx(b"kc:snx:key", b"second").await?;
            assert!(!set);

            // Value should still be "first"
            let val = client.get(b"kc:snx:key").await?;
            assert_eq!(val.as_deref(), Some(b"first".as_ref()));

            Ok(())
        })
    });
}

#[test]
fn test_set_ex() {
    run_client_test(|client| {
        Box::pin(async move {
            client.set_ex(b"kc:sex:key", b"value", 3600).await?;

            let val = client.get(b"kc:sex:key").await?;
            assert_eq!(val.as_deref(), Some(b"value".as_ref()));

            Ok(())
        })
    });
}

#[test]
fn test_incr_decr() {
    run_client_test(|client| {
        Box::pin(async move {
            client.set(b"kc:id:counter", b"10").await?;

            let val = client.incr(b"kc:id:counter").await?;
            assert_eq!(val, 11);

            let val = client.decr(b"kc:id:counter").await?;
            assert_eq!(val, 10);

            let val = client.incrby(b"kc:id:counter", 5).await?;
            assert_eq!(val, 15);

            let val = client.decrby(b"kc:id:counter", 3).await?;
            assert_eq!(val, 12);

            Ok(())
        })
    });
}

#[test]
fn test_key_type() {
    run_client_test(|client| {
        Box::pin(async move {
            client.set(b"kc:type:str", b"val").await?;

            let t = client.key_type(b"kc:type:str").await?;
            assert_eq!(t, "string");

            let t = client.key_type(b"kc:type:missing").await?;
            assert_eq!(t, "none");

            Ok(())
        })
    });
}

#[test]
fn test_pipeline() {
    run_client_test(|client| {
        Box::pin(async move {
            let results = client
                .pipeline()
                .set(b"kc:pipe:k1", b"v1")
                .set(b"kc:pipe:k2", b"v2")
                .get(b"kc:pipe:k1")
                .execute()
                .await?;

            assert_eq!(results.len(), 3);

            // First two are SET responses (+OK)
            assert!(results[0].is_simple_string());
            assert!(results[1].is_simple_string());

            // Third is GET response
            assert_eq!(results[2].as_bytes(), Some(b"v1".as_ref()));

            Ok(())
        })
    });
}

#[test]
fn test_mget() {
    run_client_test(|client| {
        Box::pin(async move {
            client.set(b"kc:mg:k1", b"v1").await?;
            client.set(b"kc:mg:k2", b"v2").await?;

            let vals = client
                .mget(&[b"kc:mg:k1", b"kc:mg:k2", b"kc:mg:missing"])
                .await?;

            assert_eq!(vals.len(), 3);
            assert_eq!(vals[0].as_deref(), Some(b"v1".as_ref()));
            assert_eq!(vals[1].as_deref(), Some(b"v2".as_ref()));
            assert_eq!(vals[2], None);

            Ok(())
        })
    });
}

#[test]
fn test_append() {
    run_client_test(|client| {
        Box::pin(async move {
            client.set(b"kc:ap:key", b"hello").await?;

            let len = client.append(b"kc:ap:key", b" world").await?;
            assert_eq!(len, 11);

            let val = client.get(b"kc:ap:key").await?;
            assert_eq!(val.as_deref(), Some(b"hello world".as_ref()));

            Ok(())
        })
    });
}

#[test]
fn test_connection_closed() {
    run_client_test(|client| {
        Box::pin(async move {
            // Verify connection works
            client.ping().await?;

            // Close the connection
            // Access the underlying ConnCtx through the client to close it.
            // We'll create a second client with a closed connection instead.
            // Actually, the simplest way is to connect, close, then try to use.

            // Connect a second time, close it, and try to read
            let addr = server_addr();
            let conn2 = krio::connect(addr).unwrap().await.unwrap();
            let client2 = krio_redis::Client::new(conn2);

            // Verify it works
            client2.ping().await?;

            // Close the connection
            conn2.close();

            // Next command should fail with ConnectionClosed
            let result = client2.ping().await;
            assert!(
                matches!(result, Err(krio_redis::Error::ConnectionClosed)),
                "expected ConnectionClosed, got: {result:?}"
            );

            Ok(())
        })
    });
}

// ── Pool tests ──────────────────────────────────────────────────────────

#[test]
fn test_pool_basic() {
    run_pool_test(|addr| {
        Box::pin(async move {
            let mut pool = krio_redis::Pool::new(krio_redis::PoolConfig {
                addr,
                pool_size: 2,
                connect_timeout_ms: 0,
                tls_server_name: None,
                password: None,
                username: None,
            });

            pool.connect_all().await?;
            assert_eq!(pool.connected_count(), 2);
            assert_eq!(pool.pool_size(), 2);

            // Set via one client(), get via another (round-robin).
            pool.client().await?.set("kc:pool:k1", "v1").await?;
            let val = pool.client().await?.get("kc:pool:k1").await?;
            assert_eq!(val.as_deref(), Some(b"v1".as_ref()));

            pool.close_all();
            assert_eq!(pool.connected_count(), 0);

            Ok(())
        })
    });
}

#[test]
fn test_pool_lazy_connect() {
    run_pool_test(|addr| {
        Box::pin(async move {
            let mut pool = krio_redis::Pool::new(krio_redis::PoolConfig {
                addr,
                pool_size: 2,
                connect_timeout_ms: 0,
                tls_server_name: None,
                password: None,
                username: None,
            });

            // No connect_all — starts fully disconnected.
            assert_eq!(pool.connected_count(), 0);

            // First client() triggers a lazy connect.
            pool.client().await?.ping().await?;
            assert_eq!(pool.connected_count(), 1);

            // Second call connects a different slot.
            pool.client().await?.ping().await?;
            assert_eq!(pool.connected_count(), 2);

            pool.close_all();
            Ok(())
        })
    });
}

#[test]
fn test_pool_reconnect() {
    run_pool_test(|addr| {
        Box::pin(async move {
            let mut pool = krio_redis::Pool::new(krio_redis::PoolConfig {
                addr,
                pool_size: 2,
                connect_timeout_ms: 0,
                tls_server_name: None,
                password: None,
                username: None,
            });
            pool.connect_all().await?;

            // Get a client and mark it disconnected.
            let client = pool.client().await?;
            client.ping().await?;
            pool.mark_disconnected(client);
            assert_eq!(pool.connected_count(), 1);

            // Next client() call should lazily reconnect the dead slot.
            // We call client() twice to hit both slots (round-robin).
            pool.client().await?.ping().await?;
            pool.client().await?.ping().await?;
            assert_eq!(pool.connected_count(), 2);

            pool.close_all();
            Ok(())
        })
    });
}

#[test]
fn test_pool_pipeline() {
    run_pool_test(|addr| {
        Box::pin(async move {
            let mut pool = krio_redis::Pool::new(krio_redis::PoolConfig {
                addr,
                pool_size: 1,
                connect_timeout_ms: 0,
                tls_server_name: None,
                password: None,
                username: None,
            });
            pool.connect_all().await?;

            let results = pool
                .pipeline()
                .await?
                .set(b"kc:pp:k1", b"v1")
                .set(b"kc:pp:k2", b"v2")
                .get(b"kc:pp:k1")
                .execute()
                .await?;

            assert_eq!(results.len(), 3);
            assert!(results[0].is_simple_string());
            assert!(results[1].is_simple_string());
            assert_eq!(results[2].as_bytes(), Some(b"v1".as_ref()));

            pool.close_all();
            Ok(())
        })
    });
}
