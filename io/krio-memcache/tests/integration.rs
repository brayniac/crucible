//! Integration tests for krio-memcache.
//!
//! Each test starts a real crucible-server with memcache protocol, launches
//! krio in client-only mode via `on_start()`, runs commands, asserts results,
//! and shuts down.

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
            protocol = "memcache"
            address = "127.0.0.1:{port}"
            allow_flush = true

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

// ── Test runner helpers ──────────────────────────────────────────────────

static TEST_SERIALIZE: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Run an async test function inside krio's on_start() with a connected Client.
fn run_client_test<F>(test_fn: F)
where
    F: FnOnce(
            krio_memcache::Client,
        ) -> Pin<Box<dyn Future<Output = Result<(), krio_memcache::Error>> + 'static>>
        + Send
        + 'static,
{
    let _guard = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());
    let addr = server_addr();

    let (tx, rx) = std::sync::mpsc::channel::<Result<(), String>>();

    type TestFn = Box<
        dyn FnOnce(
                krio_memcache::Client,
            )
                -> Pin<Box<dyn Future<Output = Result<(), krio_memcache::Error>> + 'static>>
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
                            let client = krio_memcache::Client::new(conn);
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
    F: FnOnce(
            SocketAddr,
        ) -> Pin<Box<dyn Future<Output = Result<(), krio_memcache::Error>> + 'static>>
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
                -> Pin<Box<dyn Future<Output = Result<(), krio_memcache::Error>> + 'static>>
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

// ── io_uring availability check ──────────────────────────────────────────

fn io_uring_supported() -> bool {
    let ret = unsafe { libc::syscall(libc::SYS_io_uring_setup, 1u32, std::ptr::null_mut::<u8>()) };
    ret != -1 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ENOSYS)
}

// ── Tests ───────────────────────────────────────────────────────────────

#[test]
fn test_version() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            let version = client.version().await?;
            assert!(version.contains("crucible"), "got version: {version}");
            Ok(())
        })
    });
}

#[test]
fn test_set_get() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            client.set("mc:sg:key", "hello").await?;

            let val = client.get("mc:sg:key").await?;
            let val = val.expect("expected hit");
            assert_eq!(val.data.as_ref(), b"hello");
            assert_eq!(val.flags, 0);

            Ok(())
        })
    });
}

#[test]
fn test_set_get_with_options() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            // set_with_options should store the value (flags/exptime are sent
            // but the server may not round-trip flags in GET responses).
            client.set_with_options("mc:sgf:key", "data", 42, 0).await?;

            let val = client.get("mc:sgf:key").await?;
            let val = val.expect("expected hit");
            assert_eq!(val.data.as_ref(), b"data");

            Ok(())
        })
    });
}

#[test]
fn test_get_miss() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            let val = client.get("mc:miss:nonexistent").await?;
            assert!(val.is_none());
            Ok(())
        })
    });
}

#[test]
fn test_delete() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            client.set("mc:del:key", "value").await?;

            let deleted = client.delete("mc:del:key").await?;
            assert!(deleted);

            let deleted = client.delete("mc:del:key").await?;
            assert!(!deleted);

            let val = client.get("mc:del:key").await?;
            assert!(val.is_none());

            Ok(())
        })
    });
}

#[test]
fn test_delete_miss() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            let deleted = client.delete("mc:delmiss:nonexistent").await?;
            assert!(!deleted);
            Ok(())
        })
    });
}

#[test]
fn test_add() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            // Should succeed — key doesn't exist
            let stored = client.add("mc:add:key", "first").await?;
            assert!(stored);

            // Should fail — key already exists
            let stored = client.add("mc:add:key", "second").await?;
            assert!(!stored);

            // Value should still be "first"
            let val = client.get("mc:add:key").await?;
            assert_eq!(val.unwrap().data.as_ref(), b"first");

            Ok(())
        })
    });
}

#[test]
fn test_replace() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            // Replace on non-existent key should fail
            let stored = client.replace("mc:repl:key", "value").await?;
            assert!(!stored);

            // Set then replace should succeed
            client.set("mc:repl:key", "first").await?;
            let stored = client.replace("mc:repl:key", "second").await?;
            assert!(stored);

            // Value should be "second"
            let val = client.get("mc:repl:key").await?;
            assert_eq!(val.unwrap().data.as_ref(), b"second");

            Ok(())
        })
    });
}

#[test]
fn test_incr() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            // Set a numeric value
            client.set("mc:incr:key", "10").await?;

            // Increment by 5
            let val = client.incr("mc:incr:key", 5).await?;
            assert_eq!(val, Some(15));

            // Increment by 1
            let val = client.incr("mc:incr:key", 1).await?;
            assert_eq!(val, Some(16));

            // Verify via GET
            let val = client.get("mc:incr:key").await?;
            assert_eq!(val.unwrap().data.as_ref(), b"16");

            Ok(())
        })
    });
}

#[test]
fn test_incr_miss() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            let val = client.incr("mc:incrmiss:nonexistent", 1).await?;
            assert_eq!(val, None);
            Ok(())
        })
    });
}

#[test]
fn test_decr() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            // Set a numeric value
            client.set("mc:decr:key", "20").await?;

            // Decrement by 3
            let val = client.decr("mc:decr:key", 3).await?;
            assert_eq!(val, Some(17));

            // Decrement by 1
            let val = client.decr("mc:decr:key", 1).await?;
            assert_eq!(val, Some(16));

            // Verify via GET
            let val = client.get("mc:decr:key").await?;
            assert_eq!(val.unwrap().data.as_ref(), b"16");

            Ok(())
        })
    });
}

#[test]
fn test_decr_underflow() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            // Set a value of 5
            client.set("mc:decru:key", "5").await?;

            // Decrement by 100 — should clamp to 0
            let val = client.decr("mc:decru:key", 100).await?;
            assert_eq!(val, Some(0));

            Ok(())
        })
    });
}

#[test]
fn test_decr_miss() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            let val = client.decr("mc:decrmiss:nonexistent", 1).await?;
            assert_eq!(val, None);
            Ok(())
        })
    });
}

#[test]
fn test_append() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            // Append on non-existent key should fail
            let stored = client.append("mc:app:key", "suffix").await?;
            assert!(!stored);

            // Set then append
            client.set("mc:app:key", "hello").await?;
            let stored = client.append("mc:app:key", " world").await?;
            assert!(stored);

            // Verify the value
            let val = client.get("mc:app:key").await?;
            assert_eq!(val.unwrap().data.as_ref(), b"hello world");

            Ok(())
        })
    });
}

#[test]
fn test_prepend() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            // Prepend on non-existent key should fail
            let stored = client.prepend("mc:pre:key", "prefix").await?;
            assert!(!stored);

            // Set then prepend
            client.set("mc:pre:key", "world").await?;
            let stored = client.prepend("mc:pre:key", "hello ").await?;
            assert!(stored);

            // Verify the value
            let val = client.get("mc:pre:key").await?;
            assert_eq!(val.unwrap().data.as_ref(), b"hello world");

            Ok(())
        })
    });
}

#[test]
fn test_cas() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            // Set a value, then get it with CAS token via gets
            client.set("mc:cas:key", "original").await?;

            let keys: &[&[u8]] = &[b"mc:cas:key"];
            let values = client.gets(keys).await?;
            assert_eq!(values.len(), 1);
            let cas_token = values[0].cas.expect("expected CAS token from gets");

            // CAS with correct token should succeed
            let stored = client.cas("mc:cas:key", "updated", cas_token).await?;
            assert!(stored);

            // Verify value changed
            let val = client.get("mc:cas:key").await?;
            assert_eq!(val.unwrap().data.as_ref(), b"updated");

            // CAS with stale token should fail (EXISTS)
            let stored = client.cas("mc:cas:key", "stale", cas_token).await?;
            assert!(!stored);

            // Value should still be "updated"
            let val = client.get("mc:cas:key").await?;
            assert_eq!(val.unwrap().data.as_ref(), b"updated");

            Ok(())
        })
    });
}

#[test]
fn test_cas_not_found() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            // CAS on non-existent key should return error
            let result = client.cas("mc:casmiss:key", "value", 12345).await;
            assert!(result.is_err());
            Ok(())
        })
    });
}

#[test]
fn test_gets_returns_cas() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            client.set("mc:grc:k1", "v1").await?;
            client.set("mc:grc:k2", "v2").await?;

            let keys: &[&[u8]] = &[b"mc:grc:k1", b"mc:grc:k2"];
            let values = client.gets(keys).await?;

            assert_eq!(values.len(), 2);
            // Both should have CAS tokens
            assert!(values[0].cas.is_some());
            assert!(values[1].cas.is_some());

            Ok(())
        })
    });
}

#[test]
fn test_multi_get() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            client.set("mc:mg:k1", "v1").await?;
            client.set("mc:mg:k2", "v2").await?;

            let keys: &[&[u8]] = &[b"mc:mg:k1", b"mc:mg:k2", b"mc:mg:missing"];
            let values = client.gets(keys).await?;

            // Only hits are returned
            assert_eq!(values.len(), 2);
            assert_eq!(values[0].key.as_ref(), b"mc:mg:k1");
            assert_eq!(values[0].data.as_ref(), b"v1");
            assert_eq!(values[1].key.as_ref(), b"mc:mg:k2");
            assert_eq!(values[1].data.as_ref(), b"v2");

            Ok(())
        })
    });
}

#[test]
fn test_flush_all() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            client.set("mc:flush:key", "value").await?;

            client.flush_all().await?;

            let val = client.get("mc:flush:key").await?;
            assert!(val.is_none());

            Ok(())
        })
    });
}

#[test]
fn test_connection_closed() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_client_test(|client| {
        Box::pin(async move {
            // Verify connection works
            client.version().await?;

            // Connect a second time, close it, and try to use
            let addr = server_addr();
            let conn2 = krio::connect(addr).unwrap().await.unwrap();
            let client2 = krio_memcache::Client::new(conn2);

            client2.version().await?;

            conn2.close();

            let result = client2.version().await;
            assert!(
                matches!(result, Err(krio_memcache::Error::ConnectionClosed)),
                "expected ConnectionClosed, got: {result:?}"
            );

            Ok(())
        })
    });
}

// ── Pool tests ──────────────────────────────────────────────────────────

#[test]
fn test_pool_basic() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_pool_test(|addr| {
        Box::pin(async move {
            let mut pool = krio_memcache::Pool::new(krio_memcache::PoolConfig {
                addr,
                pool_size: 2,
                connect_timeout_ms: 0,
                tls_server_name: None,
            });

            pool.connect_all().await?;
            assert_eq!(pool.connected_count(), 2);
            assert_eq!(pool.pool_size(), 2);

            pool.client().await?.set("mc:pool:k1", "v1").await?;
            let val = pool.client().await?.get("mc:pool:k1").await?;
            assert_eq!(val.unwrap().data.as_ref(), b"v1");

            pool.close_all();
            assert_eq!(pool.connected_count(), 0);

            Ok(())
        })
    });
}

#[test]
fn test_pool_lazy_connect() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_pool_test(|addr| {
        Box::pin(async move {
            let mut pool = krio_memcache::Pool::new(krio_memcache::PoolConfig {
                addr,
                pool_size: 2,
                connect_timeout_ms: 0,
                tls_server_name: None,
            });

            assert_eq!(pool.connected_count(), 0);

            pool.client().await?.version().await?;
            assert_eq!(pool.connected_count(), 1);

            pool.client().await?.version().await?;
            assert_eq!(pool.connected_count(), 2);

            pool.close_all();
            Ok(())
        })
    });
}

#[test]
fn test_pool_reconnect() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_pool_test(|addr| {
        Box::pin(async move {
            let mut pool = krio_memcache::Pool::new(krio_memcache::PoolConfig {
                addr,
                pool_size: 2,
                connect_timeout_ms: 0,
                tls_server_name: None,
            });
            pool.connect_all().await?;

            let client = pool.client().await?;
            client.version().await?;
            pool.mark_disconnected(client);
            assert_eq!(pool.connected_count(), 1);

            pool.client().await?.version().await?;
            pool.client().await?.version().await?;
            assert_eq!(pool.connected_count(), 2);

            pool.close_all();
            Ok(())
        })
    });
}

// ── Sharded tests ───────────────────────────────────────────────────────

#[test]
fn test_sharded_basic() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    run_pool_test(|addr| {
        Box::pin(async move {
            let mut sharded = krio_memcache::ShardedClient::new(krio_memcache::ShardedConfig {
                servers: vec![addr],
                pool_size: 2,
                connect_timeout_ms: 0,
                tls_server_name: None,
            });

            sharded.connect_all().await?;

            sharded.set("mc:sh:k1", "v1").await?;
            let val = sharded.get("mc:sh:k1").await?;
            assert_eq!(val.unwrap().data.as_ref(), b"v1");

            let deleted = sharded.delete("mc:sh:k1").await?;
            assert!(deleted);

            let val = sharded.get("mc:sh:k1").await?;
            assert!(val.is_none());

            let version = sharded.version().await?;
            assert!(version.contains("crucible"));

            sharded.close_all();
            Ok(())
        })
    });
}
