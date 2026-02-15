use std::any::Any;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use http2::PlainTransport;
use kompio::{ConnToken, DriverCtx, EventHandler};
use protocol_momento::{CacheClient, CacheValue, CompletedOp, Credential, WireFormat};

use crate::command::MomentoCommand;
use crate::error::MomentoError;
use crate::latency::MomentoLatency;

/// Configuration passed to each worker via the global config channel.
pub(crate) struct MomentoWorkerConfig {
    pub cmd_rx: crossbeam_channel::Receiver<MomentoCommand>,
    pub pending: Arc<AtomicU32>,
    pub servers: Vec<SocketAddr>,
    pub connections_per_server: usize,
    pub connect_timeout_ms: u64,
    pub latency: Arc<MomentoLatency>,
    pub credential: Credential,
    pub cache_name: String,
    /// When Some, use TLS connections with this SNI hostname.
    pub tls_server_name: Option<String>,
}

// ── Config channel (same pattern as client-grpc) ─────────────────────

static CONFIG_CHANNEL: OnceLock<Mutex<Box<dyn Any + Send>>> = OnceLock::new();

/// Initialize the global config channel. Must be called before launch.
pub(crate) fn init_config_channel(rx: crossbeam_channel::Receiver<MomentoWorkerConfig>) {
    let boxed: Box<dyn Any + Send> = Box::new(rx);
    CONFIG_CHANNEL
        .set(Mutex::new(boxed))
        .expect("init_config_channel called twice");
}

fn recv_config() -> MomentoWorkerConfig {
    let guard = CONFIG_CHANNEL
        .get()
        .expect("config channel not initialized")
        .lock()
        .unwrap();
    let rx = guard
        .downcast_ref::<crossbeam_channel::Receiver<MomentoWorkerConfig>>()
        .expect("wrong config channel type");
    rx.recv().expect("config channel closed")
}

/// Pending operation waiting for a cache response.
struct PendingOp {
    kind: PendingKind,
    key: Vec<u8>,
    sent_at: Instant,
}

enum PendingKind {
    Get {
        tx: tokio::sync::oneshot::Sender<Result<CacheValue, MomentoError>>,
    },
    Set {
        tx: tokio::sync::oneshot::Sender<Result<(), MomentoError>>,
    },
    Delete {
        tx: tokio::sync::oneshot::Sender<Result<(), MomentoError>>,
    },
}

/// Per-connection state.
struct ConnState {
    client: CacheClient<PlainTransport>,
    ready: bool,
    /// Pending ops indexed by request_id.
    pending: Vec<PendingOp>,
    server_idx: usize,
}

/// Momento client EventHandler.
pub(crate) struct MomentoHandler {
    cmd_rx: crossbeam_channel::Receiver<MomentoCommand>,
    pending_counter: Arc<AtomicU32>,
    latency: Arc<MomentoLatency>,
    servers: Vec<SocketAddr>,
    connections_per_server: usize,
    connect_timeout_ms: u64,
    credential: Credential,
    cache_name: String,
    /// When Some, use TLS connections with this SNI hostname.
    tls_server_name: Option<String>,

    /// ConnToken.index() -> connection state.
    conn_map: Vec<Option<ConnState>>,
    /// Flat list of ConnTokens for active connections, grouped by server.
    conn_tokens: Vec<Vec<ConnToken>>,

    initialized: bool,
    /// Pending reconnects: (server_idx, attempt).
    reconnects: Vec<(usize, u32)>,
}

impl MomentoHandler {
    fn connect_all(&mut self, ctx: &mut DriverCtx) {
        self.conn_tokens = vec![Vec::new(); self.servers.len()];

        let servers: Vec<(usize, SocketAddr)> = self
            .servers
            .iter()
            .enumerate()
            .map(|(i, a)| (i, *a))
            .collect();
        let cps = self.connections_per_server;

        for (server_idx, addr) in servers {
            for _ in 0..cps {
                self.connect_one(ctx, server_idx, addr);
            }
        }
    }

    fn connect_one(&mut self, ctx: &mut DriverCtx, server_idx: usize, addr: SocketAddr) {
        let result = if let Some(ref sni) = self.tls_server_name {
            #[cfg(feature = "tls")]
            {
                ctx.connect_tls_with_timeout(addr, sni, self.connect_timeout_ms)
            }
            #[cfg(not(feature = "tls"))]
            {
                let _ = sni;
                Err(kompio::Error::RingSetup("TLS feature not enabled".into()))
            }
        } else {
            ctx.connect_with_timeout(addr, self.connect_timeout_ms)
        };
        match result {
            Ok(token) => {
                let idx = token.index();
                if idx >= self.conn_map.len() {
                    self.conn_map.resize_with(idx + 1, || None);
                }

                let client = match self.credential.wire_format() {
                    WireFormat::Grpc => {
                        CacheClient::with_transport(PlainTransport::new(), self.credential.clone())
                    }
                    WireFormat::Protosocket => CacheClient::with_protosocket_transport(
                        PlainTransport::new(),
                        self.credential.clone(),
                    ),
                };

                self.conn_map[idx] = Some(ConnState {
                    client,
                    ready: false,
                    pending: Vec::new(),
                    server_idx,
                });
                if server_idx >= self.conn_tokens.len() {
                    self.conn_tokens.resize_with(server_idx + 1, Vec::new);
                }
                self.conn_tokens[server_idx].push(token);
            }
            Err(_) => {
                self.schedule_reconnect(server_idx, 0);
            }
        }
    }

    fn schedule_reconnect(&mut self, server_idx: usize, attempt: u32) {
        self.reconnects.push((server_idx, attempt));
    }

    fn process_reconnects(&mut self, ctx: &mut DriverCtx) {
        let reconnects = std::mem::take(&mut self.reconnects);
        for (server_idx, _attempt) in reconnects {
            if server_idx < self.servers.len() {
                let addr = self.servers[server_idx];
                self.connect_one(ctx, server_idx, addr);
            }
        }
    }

    /// Pick a ready connection (round-robin across all servers).
    fn pick_connection(&self) -> Option<(ConnToken, usize)> {
        for tokens in &self.conn_tokens {
            for &token in tokens {
                if let Some(state) = self.conn_map.get(token.index()).and_then(|o| o.as_ref())
                    && state.ready
                {
                    return Some((token, token.index()));
                }
            }
        }
        None
    }

    fn drain_commands(&mut self, ctx: &mut DriverCtx) {
        // Reset the pending counter
        let count = self.pending_counter.swap(0, Ordering::AcqRel);
        if count == 0 {
            return;
        }

        let cmds: Vec<_> = self.cmd_rx.try_iter().collect();
        for cmd in cmds {
            self.dispatch_command(ctx, cmd);
        }
    }

    fn dispatch_command(&mut self, ctx: &mut DriverCtx, cmd: MomentoCommand) {
        let (token, conn_idx) = match self.pick_connection() {
            Some(v) => v,
            None => {
                // No ready connection — fail immediately
                match cmd {
                    MomentoCommand::Get { tx, .. } => {
                        let _ = tx.send(Err(MomentoError::ConnectionClosed));
                    }
                    MomentoCommand::Set { tx, .. } => {
                        let _ = tx.send(Err(MomentoError::ConnectionClosed));
                    }
                    MomentoCommand::Delete { tx, .. } => {
                        let _ = tx.send(Err(MomentoError::ConnectionClosed));
                    }
                }
                return;
            }
        };

        let state = self.conn_map[conn_idx].as_mut().unwrap();
        let now = Instant::now();

        match cmd {
            MomentoCommand::Get { key, tx } => match state.client.get(&self.cache_name, &key) {
                Ok(_pending_get) => {
                    state.pending.push(PendingOp {
                        kind: PendingKind::Get { tx },
                        key: key.clone(),
                        sent_at: now,
                    });
                    self.flush(ctx, conn_idx, token);
                }
                Err(e) => {
                    let _ = tx.send(Err(MomentoError::Cache(e.to_string())));
                }
            },
            MomentoCommand::Set {
                key,
                value,
                ttl,
                tx,
            } => {
                match state
                    .client
                    .set_with_ttl(&self.cache_name, &key, &value, ttl)
                {
                    Ok(_pending_set) => {
                        state.pending.push(PendingOp {
                            kind: PendingKind::Set { tx },
                            key: key.clone(),
                            sent_at: now,
                        });
                        self.flush(ctx, conn_idx, token);
                    }
                    Err(e) => {
                        let _ = tx.send(Err(MomentoError::Cache(e.to_string())));
                    }
                }
            }
            MomentoCommand::Delete { key, tx } => {
                match state.client.delete(&self.cache_name, &key) {
                    Ok(_pending_del) => {
                        state.pending.push(PendingOp {
                            kind: PendingKind::Delete { tx },
                            key: key.clone(),
                            sent_at: now,
                        });
                        self.flush(ctx, conn_idx, token);
                    }
                    Err(e) => {
                        let _ = tx.send(Err(MomentoError::Cache(e.to_string())));
                    }
                }
            }
        }
    }

    fn flush(&mut self, ctx: &mut DriverCtx, conn_idx: usize, token: ConnToken) {
        let state = self.conn_map[conn_idx].as_mut().unwrap();
        while state.client.has_pending_send() {
            let pending = state.client.pending_send();
            if pending.is_empty() {
                break;
            }
            let n = pending.len();
            if ctx.send(token, pending).is_err() {
                return;
            }
            state.client.advance_send(n);
        }
    }

    fn process_events(&mut self, ctx: &mut DriverCtx, conn_idx: usize, token: ConnToken) {
        let state = self.conn_map[conn_idx].as_mut().unwrap();

        // Check if the client just became ready
        if !state.ready && state.client.is_ready() {
            state.ready = true;
        }

        let completed = state.client.poll();

        for op in completed {
            let (key, is_get, is_set, is_delete) = match &op {
                CompletedOp::Get { key, .. } => (key.as_ref(), true, false, false),
                CompletedOp::Set { key, .. } => (key.as_ref(), false, true, false),
                CompletedOp::Delete { key, .. } => (key.as_ref(), false, false, true),
            };

            // Find matching pending op by (kind, key)
            let pos = state.pending.iter().position(|p| {
                p.key == key
                    && ((is_get && matches!(p.kind, PendingKind::Get { .. }))
                        || (is_set && matches!(p.kind, PendingKind::Set { .. }))
                        || (is_delete && matches!(p.kind, PendingKind::Delete { .. })))
            });

            if let Some(idx) = pos {
                let pending = state.pending.swap_remove(idx);
                let ns = pending.sent_at.elapsed().as_nanos() as u64;

                match (op, pending.kind) {
                    (CompletedOp::Get { result, .. }, PendingKind::Get { tx }) => {
                        let _ = self.latency.get().increment(ns);
                        let _ = tx.send(result.map_err(|e| MomentoError::Cache(e.to_string())));
                    }
                    (CompletedOp::Set { result, .. }, PendingKind::Set { tx }) => {
                        let _ = self.latency.set().increment(ns);
                        let _ = tx.send(result.map_err(|e| MomentoError::Cache(e.to_string())));
                    }
                    (CompletedOp::Delete { result, .. }, PendingKind::Delete { tx }) => {
                        let _ = self.latency.delete().increment(ns);
                        let _ = tx.send(result.map_err(|e| MomentoError::Cache(e.to_string())));
                    }
                    _ => unreachable!(),
                }
            }
        }

        // Flush any frames generated by processing (e.g., SETTINGS ACK, WINDOW_UPDATE)
        self.flush(ctx, conn_idx, token);
    }
}

impl EventHandler for MomentoHandler {
    fn create_for_worker(_worker_id: usize) -> Self {
        let config = recv_config();
        Self {
            cmd_rx: config.cmd_rx,
            pending_counter: config.pending,
            latency: config.latency,
            servers: config.servers,
            connections_per_server: config.connections_per_server,
            connect_timeout_ms: config.connect_timeout_ms,
            credential: config.credential,
            cache_name: config.cache_name,
            tls_server_name: config.tls_server_name,
            conn_map: Vec::new(),
            conn_tokens: Vec::new(),
            initialized: false,
            reconnects: Vec::new(),
        }
    }

    fn on_accept(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {
        // Client-only — no accepts.
    }

    fn on_connect(&mut self, ctx: &mut DriverCtx, conn: ConnToken, result: io::Result<()>) {
        let idx = conn.index();

        if result.is_err() {
            // Connection failed — schedule reconnect
            if let Some(state) = self.conn_map.get(idx).and_then(|o| o.as_ref()) {
                let server_idx = state.server_idx;
                self.schedule_reconnect(server_idx, 0);
            }
            // Remove from conn_tokens
            for tokens in &mut self.conn_tokens {
                tokens.retain(|t| t.index() != idx);
            }
            if idx < self.conn_map.len() {
                self.conn_map[idx] = None;
            }
            return;
        }

        // Connected (TCP, or TCP+TLS) — initialize transport
        if let Some(state) = self.conn_map.get_mut(idx).and_then(|o| o.as_mut()) {
            if state.client.on_transport_ready().is_err() {
                return;
            }
            self.flush(ctx, idx, conn);
        }
    }

    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        let idx = conn.index();

        if let Some(state) = self.conn_map.get_mut(idx).and_then(|o| o.as_mut())
            && state.client.feed_data(data).is_err()
        {
            return data.len();
        }

        self.process_events(ctx, idx, conn);

        data.len()
    }

    fn on_send_complete(&mut self, ctx: &mut DriverCtx, conn: ConnToken, _result: io::Result<u32>) {
        let idx = conn.index();
        if let Some(state) = self.conn_map.get(idx).and_then(|o| o.as_ref())
            && state.client.has_pending_send()
        {
            self.flush(ctx, idx, conn);
        }
    }

    fn on_close(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        let idx = conn.index();
        if let Some(state) = self.conn_map.get_mut(idx).and_then(|o| o.as_mut()) {
            let server_idx = state.server_idx;

            // Fail all pending ops
            let pending_ops = std::mem::take(&mut state.pending);
            for pending in pending_ops {
                match pending.kind {
                    PendingKind::Get { tx } => {
                        let _ = tx.send(Err(MomentoError::ConnectionClosed));
                    }
                    PendingKind::Set { tx } => {
                        let _ = tx.send(Err(MomentoError::ConnectionClosed));
                    }
                    PendingKind::Delete { tx } => {
                        let _ = tx.send(Err(MomentoError::ConnectionClosed));
                    }
                }
            }

            // Remove from conn_tokens
            for tokens in &mut self.conn_tokens {
                tokens.retain(|t| t.index() != idx);
            }

            self.schedule_reconnect(server_idx, 0);
        }
        if idx < self.conn_map.len() {
            self.conn_map[idx] = None;
        }
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx) {
        if !self.initialized {
            self.initialized = true;
            self.connect_all(ctx);
        }
        self.process_reconnects(ctx);
    }

    fn on_notify(&mut self, ctx: &mut DriverCtx) {
        self.drain_commands(ctx);
    }
}
