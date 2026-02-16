use std::any::Any;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use grpc::{CallBuilder, CallEvent, Channel, Metadata};
use http2::transport::PlainTransport;
use krio::{ConnToken, DriverCtx, EventHandler};

use crate::command::GrpcCommand;
use crate::error::GrpcError;
use crate::latency::GrpcLatency;
use crate::response::GrpcResponse;

/// Configuration passed to each worker via the global config channel.
pub(crate) struct GrpcWorkerConfig {
    pub cmd_rx: crossbeam_channel::Receiver<GrpcCommand>,
    pub pending: Arc<AtomicU32>,
    pub servers: Vec<SocketAddr>,
    pub connections_per_server: usize,
    pub connect_timeout_ms: u64,
    pub latency: Arc<GrpcLatency>,
    pub authority: String,
    /// TLS server name for SNI. When Some, use connect_tls; when None, use plain connect.
    pub tls_server_name: Option<String>,
}

// ── Config channel (same pattern as client-http) ─────────────────────

static CONFIG_CHANNEL: OnceLock<Mutex<Box<dyn Any + Send>>> = OnceLock::new();

/// Initialize the global config channel. Must be called before launch.
pub(crate) fn init_config_channel(rx: crossbeam_channel::Receiver<GrpcWorkerConfig>) {
    let boxed: Box<dyn Any + Send> = Box::new(rx);
    CONFIG_CHANNEL
        .set(Mutex::new(boxed))
        .expect("init_config_channel called twice");
}

fn recv_config() -> GrpcWorkerConfig {
    let guard = CONFIG_CHANNEL
        .get()
        .expect("config channel not initialized")
        .lock()
        .unwrap();
    let rx = guard
        .downcast_ref::<crossbeam_channel::Receiver<GrpcWorkerConfig>>()
        .expect("wrong config channel type");
    rx.recv().expect("config channel closed")
}

/// Pending call waiting for gRPC response.
struct PendingCall {
    tx: tokio::sync::oneshot::Sender<Result<GrpcResponse, GrpcError>>,
    metadata: Metadata,
    body: BytesMut,
    sent_at: Instant,
}

/// Per-connection state.
struct ConnState {
    channel: Channel<PlainTransport>,
    ready: bool,
    pending: HashMap<u32, PendingCall>,
    server_idx: usize,
}

/// gRPC client EventHandler.
pub(crate) struct GrpcHandler {
    cmd_rx: crossbeam_channel::Receiver<GrpcCommand>,
    pending_counter: Arc<AtomicU32>,
    latency: Arc<GrpcLatency>,
    servers: Vec<SocketAddr>,
    connections_per_server: usize,
    connect_timeout_ms: u64,
    authority: String,
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

impl GrpcHandler {
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
                Err(krio::Error::RingSetup("TLS feature not enabled".into()))
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
                self.conn_map[idx] = Some(ConnState {
                    channel: Channel::new(
                        http2::Connection::new(PlainTransport::new()),
                        &self.authority,
                    ),
                    ready: false,
                    pending: HashMap::new(),
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

    fn dispatch_command(&mut self, ctx: &mut DriverCtx, cmd: GrpcCommand) {
        let (token, conn_idx) = match self.pick_connection() {
            Some(v) => v,
            None => {
                // No ready connection — fail immediately
                match cmd {
                    GrpcCommand::Unary { tx, .. } => {
                        let _ = tx.send(Err(GrpcError::ConnectionClosed));
                    }
                }
                return;
            }
        };

        match cmd {
            GrpcCommand::Unary {
                path,
                metadata,
                body,
                tx,
            } => {
                let state = self.conn_map[conn_idx].as_mut().unwrap();

                // Build CallBuilder with path and metadata
                let mut builder = CallBuilder::new(&path);
                for (key, value) in metadata.iter() {
                    builder = builder.metadata(key, value);
                }

                match state.channel.unary(builder, &body) {
                    Ok(call) => {
                        state.pending.insert(
                            call.stream_id().value(),
                            PendingCall {
                                tx,
                                metadata: Metadata::new(),
                                body: BytesMut::new(),
                                sent_at: Instant::now(),
                            },
                        );

                        self.flush(ctx, conn_idx, token);
                    }
                    Err(e) => {
                        let _ = tx.send(Err(GrpcError::Grpc(e.to_string())));
                    }
                }
            }
        }
    }

    fn flush(&mut self, ctx: &mut DriverCtx, conn_idx: usize, token: ConnToken) {
        let state = self.conn_map[conn_idx].as_mut().unwrap();
        while state.channel.has_pending_send() {
            let pending = state.channel.pending_send();
            if pending.is_empty() {
                break;
            }
            let n = pending.len();
            if ctx.send(token, pending).is_err() {
                return;
            }
            state.channel.advance_send(n);
        }
    }

    fn process_events(&mut self, ctx: &mut DriverCtx, conn_idx: usize, token: ConnToken) {
        let state = self.conn_map[conn_idx].as_mut().unwrap();

        // Check if the channel just became ready
        if !state.ready && state.channel.is_ready() {
            state.ready = true;
        }

        let events = state.channel.poll();

        // Collect stream IDs to complete after processing all events.
        let mut completed_streams: Vec<(u32, grpc::Status)> = Vec::new();

        for (stream_id, event) in events {
            match event {
                CallEvent::Headers(metadata) => {
                    if let Some(pending) = state.pending.get_mut(&stream_id.value()) {
                        pending.metadata = metadata;
                    }
                }
                CallEvent::Message(data) => {
                    if let Some(pending) = state.pending.get_mut(&stream_id.value()) {
                        pending.body.extend_from_slice(&data);
                    }
                }
                CallEvent::Complete(status) => {
                    completed_streams.push((stream_id.value(), status));
                }
            }
        }

        // Complete calls after releasing the state borrow.
        for (sid, status) in completed_streams {
            self.complete_call(conn_idx, sid, status);
        }

        // Flush any frames generated by processing (e.g., SETTINGS ACK, WINDOW_UPDATE)
        self.flush(ctx, conn_idx, token);
    }

    fn complete_call(&mut self, conn_idx: usize, stream_id: u32, status: grpc::Status) {
        let state = self.conn_map[conn_idx].as_mut().unwrap();
        if let Some(pending) = state.pending.remove(&stream_id) {
            let ns = pending.sent_at.elapsed().as_nanos() as u64;
            let _ = self.latency.unary().increment(ns);

            let response = GrpcResponse::new(
                status,
                pending.metadata,
                if pending.body.is_empty() {
                    Bytes::new()
                } else {
                    pending.body.freeze()
                },
            );
            let _ = pending.tx.send(Ok(response));
        }
    }
}

impl EventHandler for GrpcHandler {
    fn create_for_worker(_worker_id: usize) -> Self {
        let config = recv_config();
        Self {
            cmd_rx: config.cmd_rx,
            pending_counter: config.pending,
            latency: config.latency,
            servers: config.servers,
            connections_per_server: config.connections_per_server,
            connect_timeout_ms: config.connect_timeout_ms,
            authority: config.authority,
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

        // Connected (TCP, or TCP+TLS) — send HTTP/2 connection preface
        if let Some(state) = self.conn_map.get_mut(idx).and_then(|o| o.as_mut()) {
            if state.channel.on_transport_ready().is_err() {
                return;
            }
            self.flush(ctx, idx, conn);
        }
    }

    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        let idx = conn.index();

        if let Some(state) = self.conn_map.get_mut(idx).and_then(|o| o.as_mut())
            && state.channel.feed_data(data).is_err()
        {
            return data.len();
        }

        self.process_events(ctx, idx, conn);

        data.len()
    }

    fn on_send_complete(&mut self, ctx: &mut DriverCtx, conn: ConnToken, _result: io::Result<u32>) {
        let idx = conn.index();
        if let Some(state) = self.conn_map.get(idx).and_then(|o| o.as_ref())
            && state.channel.has_pending_send()
        {
            self.flush(ctx, idx, conn);
        }
    }

    fn on_close(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        let idx = conn.index();
        if let Some(state) = self.conn_map.get_mut(idx).and_then(|o| o.as_mut()) {
            let server_idx = state.server_idx;

            // Fail all pending calls
            let pending_map = std::mem::take(&mut state.pending);
            for (_, pending) in pending_map {
                let _ = pending.tx.send(Err(GrpcError::ConnectionClosed));
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
