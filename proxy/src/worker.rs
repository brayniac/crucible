//! Worker implementation using kompio EventHandler.
//!
//! Each worker runs inside a kompio event loop, handling both client connections
//! (accepted) and backend connections (outbound via ctx.connect()).

use crate::backend::{BackendConnection, BackendPool, InFlightRequest};
use crate::cache::SharedCache;
use crate::client::{ClientConnection, ClientState};
use crate::config::Config;
use crate::metrics::{
    BACKEND_REQUESTS, BACKEND_RESPONSES, CACHE_HITS, CACHE_MISSES, CLIENT_COMMANDS,
    CLIENT_CONNECTIONS, PARSE_ERRORS,
};

use ahash::AHashMap;
use kompio::{ConnToken, DriverCtx, EventHandler, KompioBuilder};
use protocol_resp::{Command, Value};
use std::any::Any;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

/// Request ID counter.
static NEXT_REQUEST_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

// ── Config channel ──────────────────────────────────────────────────────

/// Per-worker configuration passed through the config channel.
pub struct ProxyWorkerConfig {
    pub worker_id: usize,
    pub config: Config,
    pub cache: Arc<SharedCache>,
    pub shutdown: Arc<AtomicBool>,
    pub cpu_id: Option<usize>,
}

static CONFIG_CHANNEL: OnceLock<Mutex<Box<dyn Any + Send>>> = OnceLock::new();

fn init_config_channel(rx: crossbeam_channel::Receiver<ProxyWorkerConfig>) {
    let boxed: Box<dyn Any + Send> = Box::new(rx);
    CONFIG_CHANNEL
        .set(Mutex::new(boxed))
        .expect("init_config_channel called twice");
}

fn recv_config() -> ProxyWorkerConfig {
    let guard = CONFIG_CHANNEL
        .get()
        .expect("config channel not initialized")
        .lock()
        .unwrap();
    let rx = guard
        .downcast_ref::<crossbeam_channel::Receiver<ProxyWorkerConfig>>()
        .expect("wrong config channel type");
    rx.recv().expect("config channel closed")
}

// ── ProxyHandler ────────────────────────────────────────────────────────

/// Proxy worker event handler for kompio.
pub struct ProxyHandler {
    #[allow(dead_code)]
    worker_id: usize,
    #[allow(dead_code)]
    config: Config,
    shutdown: Arc<AtomicBool>,
    cache: Arc<SharedCache>,

    /// Client connections indexed by ConnToken::index().
    clients: Vec<Option<ClientConnection>>,

    /// Backend connection pool.
    backend_pool: BackendPool,

    /// Request tracking (request_id -> client ConnToken).
    pending_requests: AHashMap<u64, ConnToken>,

    /// Backend address.
    backend_addr: SocketAddr,

    /// Whether backend connections have been initiated.
    backends_initiated: bool,
}

impl ProxyHandler {
    /// Initiate backend connections.
    fn initiate_backends(&mut self, ctx: &mut DriverCtx) {
        while self.backend_pool.needs_connections() {
            let addr = self.backend_addr;
            match ctx.connect(addr) {
                Ok(token) => {
                    let conn = BackendConnection::new(token, addr);
                    self.backend_pool.add_connection(conn);
                    trace!(address = %addr, conn_index = token.index(), "Backend connection initiated");
                }
                Err(e) => {
                    warn!(error = %e, address = %addr, "Failed to initiate backend connection");
                    break;
                }
            }
        }
    }

    /// Handle data received from a client.
    fn handle_client_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) {
        let idx = conn.index();

        // Copy data to client's recv buffer
        if let Some(Some(client)) = self.clients.get_mut(idx) {
            client.append_recv(data);
        } else {
            return;
        }

        let mut should_close = false;

        // Parse and handle commands
        while let Some(Some(client)) = self.clients.get(idx) {
            let u = client.unparsed();
            if u.is_empty() {
                break;
            }
            // Copy to temp to avoid borrow conflict
            let unparsed = u.to_vec();

            match Command::parse(&unparsed) {
                Ok((cmd, consumed)) => {
                    CLIENT_COMMANDS.increment();
                    trace!(conn_index = idx, command = ?cmd, "Parsed command");

                    // Forward to backend or handle locally
                    let result = forward_to_backend(
                        &mut self.backend_pool,
                        &mut self.pending_requests,
                        conn,
                        &cmd,
                        &self.cache,
                    );

                    match result {
                        ForwardResult::Pong => {
                            if let Some(Some(client)) = self.clients.get_mut(idx) {
                                client.queue_response(b"+PONG\r\n");
                            }
                        }
                        ForwardResult::CacheHit(ref response_data) => {
                            if let Some(Some(client)) = self.clients.get_mut(idx) {
                                client.queue_response(response_data);
                            }
                        }
                        ForwardResult::Forwarded => {}
                        ForwardResult::Error(err) => {
                            if let Some(Some(client)) = self.clients.get_mut(idx) {
                                client.queue_response(err);
                            }
                        }
                    }

                    if let Some(Some(client)) = self.clients.get_mut(idx) {
                        client.consume_parsed(consumed);
                    }
                }
                Err(protocol_resp::ParseError::Incomplete) => break,
                Err(e) => {
                    PARSE_ERRORS.increment();
                    warn!(conn_index = idx, error = ?e, "Parse error");
                    if let Some(Some(client)) = self.clients.get_mut(idx) {
                        client.queue_response(b"-ERR protocol error\r\n");
                    }
                    should_close = true;
                    break;
                }
            }
        }

        if should_close {
            self.close_client(ctx, conn, "parse error");
            return;
        }

        // Drain client sends
        if let Some(Some(client)) = self.clients.get_mut(idx) {
            Self::drain_client(ctx, client);
        }

        // Drain backend sends
        for backend in self.backend_pool.connections_mut() {
            if backend.send_buf.is_empty() {
                continue;
            }
            Self::drain_backend(ctx, backend);
        }
    }

    /// Handle data received from a backend.
    fn handle_backend_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) {
        // Copy data to backend's recv buffer
        if let Some(backend) = self.backend_pool.get_connection_mut(conn) {
            backend.append_recv(data);
        } else {
            return;
        }

        let mut should_close = false;
        let mut responses_to_send: Vec<(ConnToken, Vec<u8>)> = Vec::new();

        // Parse responses - we need to work with the backend through the pool
        while let Some(backend) = self.backend_pool.get_connection_mut(conn) {
            let recv_data = &backend.recv_buf[..];
            if recv_data.is_empty() {
                break;
            }

            match Value::parse(recv_data) {
                Ok((_value, consumed)) => {
                    BACKEND_RESPONSES.increment();

                    // Get request info before mutating
                    let request_info = backend
                        .oldest_request()
                        .map(|req| (req.request_id, req.client, req.cacheable, req.key.clone()));

                    if let Some((request_id, client_token, cacheable, key)) = request_info {
                        let response_bytes = recv_data[..consumed].to_vec();

                        // Cache successful GET responses
                        if cacheable
                            && let Some(ref k) = key
                            && !response_bytes.starts_with(b"$-1\r\n")
                            && !response_bytes.starts_with(b"_\r\n")
                            && !response_bytes.starts_with(b"-")
                        {
                            self.cache.set(k, &response_bytes);
                        }

                        responses_to_send.push((client_token, response_bytes));

                        backend.complete_request();
                        self.pending_requests.remove(&request_id);
                    }

                    backend.consume_response(consumed);
                }
                Err(protocol_resp::ParseError::Incomplete) => break,
                Err(e) => {
                    let preview_len = recv_data.len().min(64);
                    let preview = String::from_utf8_lossy(&recv_data[..preview_len]);
                    error!(
                        conn_index = conn.index(),
                        error = ?e,
                        data_preview = %preview,
                        "Backend response parse error"
                    );
                    should_close = true;
                    break;
                }
            }
        }

        if should_close {
            let (in_flight, recv_buf_preview) = self
                .backend_pool
                .get_connection(conn)
                .map(|c| {
                    let preview = if c.recv_buf.is_empty() {
                        String::from("<empty>")
                    } else {
                        let len = c.recv_buf.len().min(128);
                        format!(
                            "{:?} ({}B)",
                            String::from_utf8_lossy(&c.recv_buf[..len]),
                            c.recv_buf.len()
                        )
                    };
                    (c.in_flight.len(), preview)
                })
                .unwrap_or((0, String::from("<no connection>")));
            warn!(
                conn_index = conn.index(),
                reason = "parse error",
                in_flight_requests = in_flight,
                recv_buf = %recv_buf_preview,
                "Backend connection closed"
            );
            self.backend_pool.remove_connection(conn);
            ctx.close(conn);
            return;
        }

        // Send responses to clients
        for (client_token, response) in responses_to_send {
            let client_idx = client_token.index();
            if let Some(Some(client)) = self.clients.get_mut(client_idx)
                && client.conn == client_token
            {
                client.queue_response(&response);
                Self::drain_client(ctx, client);
            }
        }
    }

    /// Drain a client's pending sends.
    fn drain_client(ctx: &mut DriverCtx, client: &mut ClientConnection) {
        if !client.has_pending_send() {
            return;
        }
        let data = client.send_data();
        let n = data.len();
        match ctx.send(client.conn, data) {
            Ok(()) => {
                client.advance_sent(n);
                client.state = ClientState::Reading;
            }
            Err(_) => {
                // Pool exhausted or error - will retry on on_send_complete
                client.state = ClientState::Writing { bytes_written: 0 };
            }
        }
    }

    /// Drain a backend's pending sends.
    fn drain_backend(ctx: &mut DriverCtx, backend: &mut BackendConnection) {
        if backend.send_buf.is_empty() {
            return;
        }
        let data = backend.send_data();
        let n = data.len();
        match ctx.send(backend.conn, data) {
            Ok(()) => {
                backend.advance_sent(n);
            }
            Err(_) => {
                // Pool exhausted - will retry on on_send_complete
            }
        }
    }

    /// Close a client connection.
    fn close_client(&mut self, ctx: &mut DriverCtx, conn: ConnToken, reason: &str) {
        let idx = conn.index();
        if let Some(slot) = self.clients.get_mut(idx)
            && slot.as_ref().is_some_and(|c| c.conn == conn)
        {
            slot.take();
            debug!(conn_index = idx, reason, "Closing client");
            ctx.close(conn);
            CLIENT_CONNECTIONS.decrement();
        }
    }

    /// Check if a connection index belongs to a client (verified by token).
    fn is_client(&self, conn: ConnToken) -> bool {
        self.clients
            .get(conn.index())
            .is_some_and(|c| c.as_ref().is_some_and(|c| c.conn == conn))
    }
}

impl EventHandler for ProxyHandler {
    fn create_for_worker(worker_id: usize) -> Self {
        let cfg = recv_config();

        // Pin to CPU if configured
        if let Some(cpu) = cfg.cpu_id {
            set_cpu_affinity(cpu);
        }

        // Set metrics thread shard
        metrics::set_thread_shard(worker_id);

        let backend_addr = cfg
            .config
            .backend
            .nodes
            .first()
            .copied()
            .expect("at least one backend node required");
        let backend_pool = BackendPool::new(backend_addr, cfg.config.backend.pool_size);

        info!(
            worker_id = cfg.worker_id,
            backend = %backend_addr,
            pool_size = cfg.config.backend.pool_size,
            "Worker starting"
        );

        ProxyHandler {
            worker_id: cfg.worker_id,
            config: cfg.config,
            shutdown: cfg.shutdown,
            cache: cfg.cache,
            clients: Vec::with_capacity(4096),
            backend_pool,
            pending_requests: AHashMap::new(),
            backend_addr,
            backends_initiated: false,
        }
    }

    fn on_accept(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        let idx = conn.index();
        if idx >= self.clients.len() {
            self.clients.resize_with(idx + 1, || None);
        }
        // We don't have the peer address from kompio's on_accept.
        // Use a placeholder; the actual address can be fetched via ctx.peer_addr() if needed.
        let addr = "0.0.0.0:0".parse().unwrap();
        self.clients[idx] = Some(ClientConnection::new(conn, addr));
        CLIENT_CONNECTIONS.increment();
        trace!(conn_index = idx, "Accepted client");
    }

    fn on_connect(&mut self, _ctx: &mut DriverCtx, conn: ConnToken, result: io::Result<()>) {
        match result {
            Ok(()) => {
                if let Some(backend) = self.backend_pool.get_connection_mut(conn) {
                    backend.mark_connected();
                    debug!(
                        conn_index = conn.index(),
                        address = %backend.addr,
                        "Backend connected"
                    );
                }
            }
            Err(e) => {
                error!(
                    conn_index = conn.index(),
                    error = %e,
                    "Backend connection failed"
                );
                self.backend_pool.remove_connection(conn);
            }
        }
    }

    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }

        let n = data.len();

        if self.is_client(conn) {
            self.handle_client_data(ctx, conn, data);
        } else if self.backend_pool.get_connection(conn).is_some() {
            self.handle_backend_data(ctx, conn, data);
        }

        n // Always consume all data (we copy to internal buffers)
    }

    fn on_send_complete(&mut self, ctx: &mut DriverCtx, conn: ConnToken, result: io::Result<u32>) {
        if result.is_err() {
            return;
        }

        // Drain any remaining sends
        if self.is_client(conn) {
            let idx = conn.index();
            if let Some(Some(client)) = self.clients.get_mut(idx) {
                Self::drain_client(ctx, client);
            }
        } else if let Some(backend) = self.backend_pool.get_connection_mut(conn) {
            Self::drain_backend(ctx, backend);
        }
    }

    fn on_close(&mut self, ctx: &mut DriverCtx, conn: ConnToken) {
        if self.is_client(conn) {
            let idx = conn.index();
            if let Some(slot) = self.clients.get_mut(idx) {
                slot.take();
                CLIENT_CONNECTIONS.decrement();
            }
        } else if let Some(backend) = self.backend_pool.remove_connection(conn) {
            let in_flight_count = backend.in_flight.len();
            if in_flight_count > 0 {
                warn!(
                    conn_index = conn.index(),
                    in_flight = in_flight_count,
                    "Backend connection closed with in-flight requests"
                );
            }

            // Send error responses to clients with in-flight requests
            for request in backend.in_flight {
                self.pending_requests.remove(&request.request_id);
                let client_idx = request.client.index();
                if let Some(Some(client)) = self.clients.get_mut(client_idx)
                    && client.conn == request.client
                {
                    client.queue_response(b"-ERR backend disconnected\r\n");
                    Self::drain_client(ctx, client);
                }
            }
        }
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx) {
        // Check shutdown
        if self.shutdown.load(Ordering::Relaxed) {
            ctx.request_shutdown();
            return;
        }

        // Initiate backend connections on first tick
        if !self.backends_initiated {
            self.initiate_backends(ctx);
            self.backends_initiated = true;
        }

        // Replenish backend pool if connections were lost
        if self.backend_pool.needs_connections() {
            self.initiate_backends(ctx);
        }
    }
}

// ── Command forwarding ──────────────────────────────────────────────────

/// Result of forwarding a command.
enum ForwardResult {
    Pong,
    CacheHit(Vec<u8>),
    Forwarded,
    Error(&'static [u8]),
}

/// Forward a command to the backend.
fn forward_to_backend(
    pool: &mut BackendPool,
    pending_requests: &mut AHashMap<u64, ConnToken>,
    client: ConnToken,
    cmd: &Command<'_>,
    cache: &Arc<SharedCache>,
) -> ForwardResult {
    if matches!(cmd, Command::Ping) {
        return ForwardResult::Pong;
    }

    // Check cache for GET commands
    if let Command::Get { key } = cmd {
        if let Some(data) = cache.with_value(key, |v| v.to_vec()) {
            CACHE_HITS.increment();
            return ForwardResult::CacheHit(data);
        }
        CACHE_MISSES.increment();
    }

    // Invalidate cache on writes
    match cmd {
        Command::Set { key, .. } | Command::Del { key } => {
            cache.delete(key);
        }
        _ => {}
    }

    let Some(conn) = pool.get_usable_connection() else {
        return ForwardResult::Error(b"-ERR backend unavailable\r\n");
    };

    encode_command(cmd, conn.send_buf_mut());

    let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let request = InFlightRequest {
        client,
        request_id,
        key: extract_key(cmd),
        cacheable: matches!(cmd, Command::Get { .. }),
    };

    conn.queue_request_encoded(request);
    pending_requests.insert(request_id, client);
    BACKEND_REQUESTS.increment();

    ForwardResult::Forwarded
}

/// Encode a RESP command directly into a BytesMut buffer.
fn encode_command(cmd: &Command<'_>, buf: &mut bytes::BytesMut) {
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
        _ => {
            buf.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        }
    }
}

#[inline]
fn write_usize(buf: &mut bytes::BytesMut, mut n: usize) {
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

fn extract_key(cmd: &Command<'_>) -> Option<bytes::Bytes> {
    match cmd {
        Command::Get { key } => Some(bytes::Bytes::copy_from_slice(key)),
        _ => None,
    }
}

// ── Public API ──────────────────────────────────────────────────────────

/// Run the proxy with the given configuration.
pub fn run(config: &Config, shutdown: Arc<AtomicBool>) -> Result<(), Box<dyn std::error::Error>> {
    let num_workers = config.threads();
    let cpu_affinity = config.cpu_affinity();
    let listen_addr = config.proxy.listen;

    info!(
        workers = num_workers,
        listen = %listen_addr,
        backend = ?config.backend.nodes,
        "Starting proxy"
    );

    // Create shared cache
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

    // Set up config channel
    let (config_tx, config_rx) = crossbeam_channel::bounded::<ProxyWorkerConfig>(num_workers);
    for worker_id in 0..num_workers {
        let cpu_id = cpu_affinity
            .as_ref()
            .map(|cpus| cpus[worker_id % cpus.len()]);

        config_tx
            .send(ProxyWorkerConfig {
                worker_id,
                config: config.clone(),
                cache: Arc::clone(&cache),
                shutdown: Arc::clone(&shutdown),
                cpu_id,
            })
            .expect("failed to queue worker config");
    }
    init_config_channel(config_rx);

    // Build kompio config
    let kompio_config = kompio::Config {
        recv_buffer: kompio::RecvBufferConfig {
            ring_size: config.uring.buffer_count.next_power_of_two(),
            buffer_size: config.uring.buffer_size as u32,
            ..Default::default()
        },
        worker: kompio::WorkerConfig {
            threads: num_workers,
            pin_to_core: false, // We pin in create_for_worker
            core_offset: 0,
        },
        sq_entries: config.uring.sq_depth,
        tcp_nodelay: true,
        ..Default::default()
    };

    // Launch kompio with accept on the listen address
    let (shutdown_handle, handles) = KompioBuilder::new(kompio_config)
        .bind(&listen_addr.to_string())
        .launch::<ProxyHandler>()?;

    // Wait for shutdown signal
    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    info!("Shutdown signal received, stopping workers...");

    // Shutdown kompio workers
    shutdown_handle.shutdown();

    // Wait for all threads
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

    info!("Proxy shutdown complete");
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
