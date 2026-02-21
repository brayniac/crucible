//! Async proxy worker using ringline's AsyncEventHandler.
//!
//! Each client connection gets a dedicated async task that:
//! 1. Connects to a backend on accept
//! 2. Reads commands from the client
//! 3. Checks the local cache for GET hits
//! 4. Forwards cache misses to the backend
//! 5. Caches backend responses and sends them back to the client

use crate::cache::SharedCache;
use crate::config::Config;

use bytes::BytesMut;
use ringline::{AsyncEventHandler, ConnCtx, DriverCtx, RinglineBuilder};
use resp_proto::{Command, ParseError, Value};
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, info, warn};

// ── Config channel ──────────────────────────────────────────────────────

/// Per-worker configuration passed through the config channel.
pub struct AsyncProxyWorkerConfig {
    pub worker_id: usize,
    pub config: Config,
    pub cache: Arc<SharedCache>,
    pub shutdown: Arc<AtomicBool>,
    pub cpu_id: Option<usize>,
}

static ASYNC_CONFIG_CHANNEL: Mutex<Option<crossbeam_channel::Receiver<AsyncProxyWorkerConfig>>> =
    Mutex::new(None);

fn init_config_channel(rx: crossbeam_channel::Receiver<AsyncProxyWorkerConfig>) {
    let mut guard = ASYNC_CONFIG_CHANNEL.lock().unwrap();
    *guard = Some(rx);
}

fn recv_config() -> AsyncProxyWorkerConfig {
    let guard = ASYNC_CONFIG_CHANNEL.lock().unwrap();
    let rx = guard.as_ref().expect("config channel not initialized");
    rx.recv().expect("config channel closed")
}

// ── AsyncProxyHandler ───────────────────────────────────────────────────

/// Async proxy handler implementing ringline's AsyncEventHandler.
pub struct AsyncProxyHandler {
    #[allow(dead_code)]
    worker_id: usize,
    shutdown: Arc<AtomicBool>,
    cache: Arc<SharedCache>,
    backends: Vec<SocketAddr>,
    ring: ketama::Ring,
}

impl AsyncEventHandler for AsyncProxyHandler {
    fn on_accept(&self, client: ConnCtx) -> impl Future<Output = ()> + 'static {
        let cache = Arc::clone(&self.cache);
        let backends = self.backends.clone();
        let ring = self.ring.clone();
        Box::pin(handle_client(client, ring, backends, cache))
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx<'_>) {
        if self.shutdown.load(Ordering::Relaxed) {
            ctx.request_shutdown();
        }
    }

    fn create_for_worker(worker_id: usize) -> Self {
        let cfg = recv_config();

        // Pin to CPU if configured.
        if let Some(cpu) = cfg.cpu_id {
            set_cpu_affinity(cpu);
        }

        ::metrics::set_thread_shard(worker_id);

        let backends = cfg.config.backend.nodes.clone();
        assert!(!backends.is_empty(), "at least one backend node required");

        // Build ketama consistent hash ring from backend addresses.
        let server_ids: Vec<String> = backends.iter().map(|a| a.to_string()).collect();
        let ring = ketama::Ring::build(&server_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        info!(
            worker_id = cfg.worker_id,
            backends = ?backends,
            "Async worker starting"
        );

        AsyncProxyHandler {
            worker_id: cfg.worker_id,
            shutdown: cfg.shutdown,
            cache: cfg.cache,
            backends,
            ring,
        }
    }
}

// ── Client handler ──────────────────────────────────────────────────────

/// Handle a single client connection: route commands to backends via
/// ketama consistent hashing, establishing backend connections lazily.
async fn handle_client(
    client: ConnCtx,
    ring: ketama::Ring,
    backends: Vec<SocketAddr>,
    cache: Arc<SharedCache>,
) {
    let mut conns: HashMap<usize, ConnCtx> = HashMap::new();
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        // Wait for client data.
        let got = client
            .with_data(|data| {
                if data.is_empty() {
                    return ringline::ParseResult::Consumed(0);
                }
                buf.extend_from_slice(data);
                ringline::ParseResult::Consumed(data.len())
            })
            .await;

        if got == 0 {
            // Client disconnected.
            for (_, conn) in conns.drain() {
                conn.close();
            }
            break;
        }

        // Parse and process all complete commands in the buffer.
        loop {
            if buf.is_empty() {
                break;
            }

            match Command::parse(&buf) {
                Ok((cmd, consumed)) => {
                    // Route the command to the appropriate backend.
                    let endpoint_idx = route_command(&cmd, &ring, backends.len());
                    let backend =
                        match get_or_connect(&client, &mut conns, &backends, endpoint_idx).await {
                            Some(conn) => conn,
                            None => {
                                let _ = client.send(b"-ERR backend unavailable\r\n");
                                for (_, conn) in conns.drain() {
                                    conn.close();
                                }
                                return;
                            }
                        };

                    match process_command(&cmd, &client, backend, &cache).await {
                        CommandResult::Responded => {}
                        CommandResult::ForwardedAndResponded => {}
                        CommandResult::BackendDisconnected => {
                            // Remove the failed backend; next use will reconnect.
                            if let Some(conn) = conns.remove(&endpoint_idx) {
                                conn.close();
                            }
                            let _ = client.send(b"-ERR backend disconnected\r\n");
                        }
                    }
                    let _ = buf.split_to(consumed);
                }
                Err(ParseError::Incomplete) => break,
                Err(e) => {
                    warn!(error = ?e, "Client parse error");
                    let _ = client.send(b"-ERR protocol error\r\n");
                    for (_, conn) in conns.drain() {
                        conn.close();
                    }
                    return;
                }
            }
        }
    }
}

/// Determine the backend endpoint index for a command using the ketama ring.
fn route_command(cmd: &Command<'_>, ring: &ketama::Ring, num_backends: usize) -> usize {
    if num_backends == 1 {
        return 0;
    }
    match cmd {
        Command::Get { key } | Command::Set { key, .. } | Command::Del { key } => ring.route(key),
        // Non-keyed commands go to endpoint 0.
        _ => 0,
    }
}

/// Get an existing backend connection or establish a new one lazily.
async fn get_or_connect<'a>(
    client: &ConnCtx,
    conns: &'a mut HashMap<usize, ConnCtx>,
    backends: &[SocketAddr],
    idx: usize,
) -> Option<&'a ConnCtx> {
    if let std::collections::hash_map::Entry::Vacant(e) = conns.entry(idx) {
        let addr = backends[idx];
        let conn = match client.connect(addr) {
            Ok(fut) => match fut.await {
                Ok(ctx) => {
                    debug!(
                        client_index = client.index(),
                        backend_index = ctx.index(),
                        backend_addr = %addr,
                        "Backend connected"
                    );
                    ctx
                }
                Err(e) => {
                    warn!(error = %e, backend_addr = %addr, "Backend connect failed");
                    return None;
                }
            },
            Err(e) => {
                warn!(error = %e, backend_addr = %addr, "Backend connect submission failed");
                return None;
            }
        };
        e.insert(conn);
    }
    conns.get(&idx)
}

/// Result of processing a single command.
enum CommandResult {
    /// Response was sent directly (PING, cache hit).
    Responded,
    /// Command was forwarded to backend and response sent to client.
    ForwardedAndResponded,
    /// Backend connection was lost.
    BackendDisconnected,
}

/// Process a single parsed command.
async fn process_command(
    cmd: &Command<'_>,
    client: &ConnCtx,
    backend: &ConnCtx,
    cache: &SharedCache,
) -> CommandResult {
    // PING: respond directly.
    if matches!(cmd, Command::Ping) {
        let _ = client.send(b"+PONG\r\n");
        return CommandResult::Responded;
    }

    // GET: check cache first.
    if let Command::Get { key } = cmd
        && let Some(data) = cache.with_value(key, |v| v.to_vec())
    {
        let _ = client.send(&data);
        return CommandResult::Responded;
    }

    // Invalidate cache on writes.
    match cmd {
        Command::Set { key, .. } | Command::Del { key } => {
            cache.delete(key);
        }
        _ => {}
    }

    // Encode and forward to backend.
    let mut encoded = BytesMut::with_capacity(64);
    encode_command(cmd, &mut encoded);

    if backend.send(&encoded).is_err() {
        return CommandResult::BackendDisconnected;
    }

    // Read the response from backend.
    let mut resp_buf = BytesMut::with_capacity(4096);
    loop {
        let got = backend
            .with_data(|data| {
                if data.is_empty() {
                    return ringline::ParseResult::Consumed(0);
                }
                resp_buf.extend_from_slice(data);
                ringline::ParseResult::Consumed(data.len())
            })
            .await;

        if got == 0 {
            return CommandResult::BackendDisconnected;
        }

        // Try to parse a complete RESP value.
        match Value::parse(&resp_buf) {
            Ok((_value, consumed)) => {
                let response_bytes = &resp_buf[..consumed];

                // Cache successful GET responses.
                if let Command::Get { key } = cmd
                    && !response_bytes.starts_with(b"$-1\r\n")
                    && !response_bytes.starts_with(b"_\r\n")
                    && !response_bytes.starts_with(b"-")
                {
                    cache.set(key, response_bytes);
                }

                let _ = client.send(response_bytes);
                return CommandResult::ForwardedAndResponded;
            }
            Err(ParseError::Incomplete) => {
                // Need more data — continue reading.
                continue;
            }
            Err(e) => {
                error!(error = ?e, "Backend response parse error");
                let _ = client.send(b"-ERR backend protocol error\r\n");
                return CommandResult::BackendDisconnected;
            }
        }
    }
}

// ── Command encoding ────────────────────────────────────────────────────

/// Encode a RESP command into a BytesMut buffer.
fn encode_command(cmd: &Command<'_>, buf: &mut BytesMut) {
    match cmd {
        Command::Get { key } => {
            buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$");
            write_usize(buf, key.len());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
        }
        Command::Set { key, value, .. } => {
            buf.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$");
            write_usize(buf, key.len());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n$");
            write_usize(buf, value.len());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(value);
            buf.extend_from_slice(b"\r\n");
        }
        Command::Del { key } => {
            buf.extend_from_slice(b"*2\r\n$3\r\nDEL\r\n$");
            write_usize(buf, key.len());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(key);
            buf.extend_from_slice(b"\r\n");
        }
        _ => {
            // Fallback: encode as PING.
            buf.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        }
    }
}

#[inline]
fn write_usize(buf: &mut BytesMut, mut n: usize) {
    if n == 0 {
        buf.extend_from_slice(b"0");
        return;
    }
    let mut digits = [0u8; 20];
    let mut i = 20;
    while n > 0 {
        i -= 1;
        digits[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    buf.extend_from_slice(&digits[i..]);
}

// ── Public API ──────────────────────────────────────────────────────────

/// Run the proxy in async mode with the given configuration.
pub fn run_async(
    config: &Config,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let num_workers = config.threads();
    let cpu_affinity = config.cpu_affinity();
    let listen_addr = config.proxy.listen;

    info!(
        workers = num_workers,
        listen = %listen_addr,
        backend = ?config.backend.nodes,
        mode = "async",
        "Starting async proxy"
    );

    // Create shared cache.
    let cache = Arc::new(SharedCache::new(&config.cache));
    if cache.is_enabled() {
        info!(
            heap_size = config.cache.heap_size,
            segment_size = config.cache.segment_size,
            ttl_ms = config.cache.ttl_ms,
            eviction = ?config.cache.eviction,
            "Shared cache enabled"
        );
    }

    // Set up config channel.
    let (config_tx, config_rx) = crossbeam_channel::bounded::<AsyncProxyWorkerConfig>(num_workers);
    for worker_id in 0..num_workers {
        let cpu_id = cpu_affinity
            .as_ref()
            .map(|cpus| cpus[worker_id % cpus.len()]);

        config_tx
            .send(AsyncProxyWorkerConfig {
                worker_id,
                config: config.clone(),
                cache: Arc::clone(&cache),
                shutdown: Arc::clone(&shutdown),
                cpu_id,
            })
            .expect("failed to queue worker config");
    }
    init_config_channel(config_rx);

    // Build ringline config.
    let krio_config = ringline::Config {
        recv_buffer: ringline::RecvBufferConfig {
            ring_size: config.uring.buffer_count.next_power_of_two(),
            buffer_size: config.uring.buffer_size as u32,
            ..Default::default()
        },
        worker: ringline::WorkerConfig {
            threads: num_workers,
            pin_to_core: false, // We pin in create_for_worker.
            core_offset: 0,
        },
        sq_entries: config.uring.sq_depth,
        tcp_nodelay: true,
        ..Default::default()
    };

    // Launch ringline with async event handler.
    let (shutdown_handle, handles) = RinglineBuilder::new(krio_config)
        .bind(listen_addr)
        .launch::<AsyncProxyHandler>()?;

    // Wait for shutdown signal.
    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    info!("Shutdown signal received, stopping async workers...");

    shutdown_handle.shutdown();

    let drain_timeout = Duration::from_secs(config.shutdown.drain_timeout_secs);
    let drain_start = std::time::Instant::now();
    for handle in handles {
        let remaining = drain_timeout.saturating_sub(drain_start.elapsed());
        if remaining.is_zero() {
            warn!("Drain timeout reached");
            break;
        }
        let _ = handle.join();
    }

    info!("Async proxy shutdown complete");
    Ok(())
}

// ── CPU affinity ────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn set_cpu_affinity(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

#[cfg(not(target_os = "linux"))]
fn set_cpu_affinity(_cpu: usize) {}
