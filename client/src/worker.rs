use std::any::Any;
use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use kompio::{ConnToken, DriverCtx, EventHandler};

use crate::command::{
    Command, PendingResponse, ResponseKind, ZC_THRESHOLD, bytes_guard, encode_set_header,
    encode_set_suffix,
};
use crate::error::ClientError;
use crate::latency::ClientLatency;
use crate::router;

/// Configuration passed to each worker via the global config channel.
pub(crate) struct ClientWorkerConfig {
    pub cmd_rx: crossbeam_channel::Receiver<Command>,
    pub pending: Arc<AtomicU32>,
    pub servers: Vec<SocketAddr>,
    pub connections_per_server: usize,
    pub connect_timeout_ms: u64,
    pub latency: Arc<ClientLatency>,
}

// ── Config channel (same pattern as benchmark) ──────────────────────────

static CONFIG_CHANNEL: OnceLock<Mutex<Box<dyn Any + Send>>> = OnceLock::new();

/// Initialize the global config channel. Must be called before launch.
pub(crate) fn init_config_channel(rx: crossbeam_channel::Receiver<ClientWorkerConfig>) {
    let boxed: Box<dyn Any + Send> = Box::new(rx);
    CONFIG_CHANNEL
        .set(Mutex::new(boxed))
        .expect("init_config_channel called twice");
}

fn recv_config() -> ClientWorkerConfig {
    let guard = CONFIG_CHANNEL
        .get()
        .expect("config channel not initialized")
        .lock()
        .unwrap();
    let rx = guard
        .downcast_ref::<crossbeam_channel::Receiver<ClientWorkerConfig>>()
        .expect("wrong config channel type");
    rx.recv().expect("config channel closed")
}

/// Per-connection state.
struct ConnInfo {
    shard_idx: usize,
    in_flight: VecDeque<PendingResponse>,
    ready: bool,
}

/// Per-shard connection pool with command backlog.
struct ShardPool {
    server_addr: SocketAddr,
    connections: Vec<ConnToken>,
    next_conn: usize,
    /// Commands waiting for a ready connection on this shard.
    backlog: VecDeque<Command>,
}

/// Reconnection entry.
struct ReconnectEntry {
    shard_idx: usize,
    when: Instant,
    attempts: u32,
}

/// ClientHandler implements EventHandler for the client crate.
pub(crate) struct ClientHandler {
    cmd_rx: crossbeam_channel::Receiver<Command>,
    pending: Arc<AtomicU32>,

    // Connection management
    pools: Vec<ShardPool>,
    conn_map: Vec<Option<ConnInfo>>,

    // Reconnection
    reconnect_queue: VecDeque<ReconnectEntry>,

    // Reusable encoding buffers (avoids per-command allocation)
    header_buf: Vec<u8>,
    suffix_buf: Vec<u8>,

    // Config
    connections_per_server: usize,
    connect_timeout_ms: u64,

    // Latency histograms
    latency: Arc<ClientLatency>,

    // Initialization
    initialized: bool,
}

impl ClientHandler {
    fn connect_all(&mut self, ctx: &mut DriverCtx) {
        for shard_idx in 0..self.pools.len() {
            let addr = self.pools[shard_idx].server_addr;
            for _ in 0..self.connections_per_server {
                self.connect_one(ctx, shard_idx, addr);
            }
        }
    }

    fn connect_one(&mut self, ctx: &mut DriverCtx, shard_idx: usize, addr: SocketAddr) {
        let result = if self.connect_timeout_ms > 0 {
            ctx.connect_with_timeout(addr, self.connect_timeout_ms)
        } else {
            ctx.connect(addr)
        };

        match result {
            Ok(token) => {
                let idx = token.index();
                if idx >= self.conn_map.len() {
                    self.conn_map.resize_with(idx + 1, || None);
                }
                self.conn_map[idx] = Some(ConnInfo {
                    shard_idx,
                    in_flight: VecDeque::new(),
                    ready: false,
                });
            }
            Err(_) => {
                self.schedule_reconnect(shard_idx, 0);
            }
        }
    }

    fn schedule_reconnect(&mut self, shard_idx: usize, attempts: u32) {
        let backoff_ms = std::cmp::min(1000 * 2u64.saturating_pow(attempts), 32000);
        let when = Instant::now() + std::time::Duration::from_millis(backoff_ms);
        self.reconnect_queue.push_back(ReconnectEntry {
            shard_idx,
            when,
            attempts,
        });
    }

    fn process_reconnects(&mut self, ctx: &mut DriverCtx) {
        let now = Instant::now();
        while let Some(front) = self.reconnect_queue.front() {
            if front.when > now {
                break;
            }
            let entry = self.reconnect_queue.pop_front().unwrap();
            let addr = self.pools[entry.shard_idx].server_addr;
            let result = if self.connect_timeout_ms > 0 {
                ctx.connect_with_timeout(addr, self.connect_timeout_ms)
            } else {
                ctx.connect(addr)
            };
            match result {
                Ok(token) => {
                    let idx = token.index();
                    if idx >= self.conn_map.len() {
                        self.conn_map.resize_with(idx + 1, || None);
                    }
                    self.conn_map[idx] = Some(ConnInfo {
                        shard_idx: entry.shard_idx,
                        in_flight: VecDeque::new(),
                        ready: false,
                    });
                }
                Err(_) => {
                    self.schedule_reconnect(entry.shard_idx, entry.attempts + 1);
                }
            }
        }
    }

    fn pick_connection(&mut self, shard_idx: usize) -> Option<ConnToken> {
        let pool = &mut self.pools[shard_idx];
        if pool.connections.is_empty() {
            return None;
        }
        let start = pool.next_conn % pool.connections.len();
        let count = pool.connections.len();
        for i in 0..count {
            let idx = (start + i) % count;
            let token = pool.connections[idx];
            if let Some(info) = &self.conn_map[token.index()]
                && info.ready
            {
                pool.next_conn = idx + 1;
                return Some(token);
            }
        }
        None
    }

    /// Send a single command on a specific connection.
    fn send_command(&mut self, ctx: &mut DriverCtx, cmd: Command, conn_token: ConnToken) {
        // Check if this is a SET with a value large enough for zero-copy
        let is_large_set = matches!(&cmd, Command::Set { value, .. } if value.len() >= ZC_THRESHOLD);

        if is_large_set {
            let Command::Set {
                key,
                value,
                ttl_secs,
                tx,
            } = cmd
            else {
                unreachable!()
            };
            let pending = PendingResponse {
                kind: ResponseKind::Set(tx),
                sent_at: Instant::now(),
            };

            // Encode RESP framing around the value (not the value itself)
            self.header_buf.clear();
            encode_set_header(&mut self.header_buf, &key, value.len(), ttl_secs.is_some());

            self.suffix_buf.clear();
            encode_set_suffix(&mut self.suffix_buf, ttl_secs);

            // Scatter-gather: copy(header) + guard(value) + copy(suffix)
            let guard = bytes_guard(value);
            let result = ctx
                .send_parts(conn_token)
                .copy(&self.header_buf)
                .guard(guard)
                .copy(&self.suffix_buf)
                .submit();

            if result.is_err() {
                pending.fail(ClientError::ConnectionClosed);
                return;
            }

            if let Some(info) = self.conn_map[conn_token.index()].as_mut() {
                info.in_flight.push_back(pending);
            }
        } else {
            // Copy path for small commands (GET, DEL, PING, small SET)
            let mut buf = [0u8; 65536];
            let len = cmd.encode(&mut buf);
            let pending = cmd.into_pending();

            if ctx.send(conn_token, &buf[..len]).is_err() {
                pending.fail(ClientError::ConnectionClosed);
                return;
            }

            if let Some(info) = self.conn_map[conn_token.index()].as_mut() {
                info.in_flight.push_back(pending);
            }
        }
    }

    /// Drain the backlog for a specific shard, sending as many commands as possible.
    fn drain_shard_backlog(&mut self, ctx: &mut DriverCtx, shard_idx: usize) {
        while !self.pools[shard_idx].backlog.is_empty() {
            let conn_token = match self.pick_connection(shard_idx) {
                Some(t) => t,
                None => break,
            };
            let cmd = self.pools[shard_idx].backlog.pop_front().unwrap();
            self.send_command(ctx, cmd, conn_token);
        }
    }

    fn drain_commands(&mut self, ctx: &mut DriverCtx) {
        // Reset atomic counter (coalescing)
        self.pending.swap(0, Ordering::AcqRel);

        let shard_count = self.pools.len();

        while let Ok(cmd) = self.cmd_rx.try_recv() {
            let shard_idx = router::route_key(cmd.key(), shard_count);
            let conn_token = match self.pick_connection(shard_idx) {
                Some(t) => t,
                None => {
                    self.pools[shard_idx].backlog.push_back(cmd);
                    continue;
                }
            };
            self.send_command(ctx, cmd, conn_token);
        }
    }
}

impl EventHandler for ClientHandler {
    fn create_for_worker(_worker_id: usize) -> Self {
        let config = recv_config();

        let mut pools = Vec::with_capacity(config.servers.len());
        for addr in &config.servers {
            pools.push(ShardPool {
                server_addr: *addr,
                connections: Vec::with_capacity(config.connections_per_server),
                next_conn: 0,
                backlog: VecDeque::new(),
            });
        }

        ClientHandler {
            cmd_rx: config.cmd_rx,
            pending: config.pending,
            pools,
            conn_map: Vec::new(),
            reconnect_queue: VecDeque::new(),
            header_buf: Vec::with_capacity(256),
            suffix_buf: Vec::with_capacity(64),
            connections_per_server: config.connections_per_server,
            connect_timeout_ms: config.connect_timeout_ms,
            latency: config.latency,
            initialized: false,
        }
    }

    fn on_accept(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {
        // Client-only — never called.
    }

    fn on_connect(&mut self, ctx: &mut DriverCtx, conn: ConnToken, result: io::Result<()>) {
        let idx = conn.index();
        match result {
            Ok(()) => {
                let shard_idx = match self.conn_map.get_mut(idx).and_then(|o| o.as_mut()) {
                    Some(info) => {
                        info.ready = true;
                        let si = info.shard_idx;
                        self.pools[si].connections.push(conn);
                        si
                    }
                    None => return,
                };
                self.drain_shard_backlog(ctx, shard_idx);
            }
            Err(_) => {
                if let Some(info) = self.conn_map.get_mut(idx).and_then(|o| o.as_mut()) {
                    let shard_idx = info.shard_idx;
                    self.schedule_reconnect(shard_idx, 0);
                }
                if idx < self.conn_map.len() {
                    self.conn_map[idx] = None;
                }
            }
        }
    }

    fn on_data(&mut self, _ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        let idx = conn.index();
        let info = match self.conn_map.get_mut(idx).and_then(|o| o.as_mut()) {
            Some(i) => i,
            None => return data.len(),
        };

        // Parse directly from the accumulator slice — no intermediate buffer.
        // Kompio keeps unconsumed bytes for the next on_data call.
        let mut consumed = 0;
        loop {
            match protocol_resp::Value::parse(&data[consumed..]) {
                Ok((value, n)) => {
                    consumed += n;
                    if let Some(pending) = info.in_flight.pop_front() {
                        let ns = pending.sent_at.elapsed().as_nanos() as u64;
                        let _ = self.latency.request().increment(ns);
                        match &pending.kind {
                            ResponseKind::Get(_) => {
                                let _ = self.latency.get().increment(ns);
                            }
                            ResponseKind::Set(_) => {
                                let _ = self.latency.set().increment(ns);
                            }
                            ResponseKind::Del(_) => {
                                let _ = self.latency.del().increment(ns);
                            }
                            ResponseKind::Ping(_) => {}
                        }
                        pending.complete(value);
                    }
                }
                Err(protocol_resp::ParseError::Incomplete) => break,
                Err(e) => {
                    for p in info.in_flight.drain(..) {
                        p.fail(ClientError::Protocol(e.to_string()));
                    }
                    break;
                }
            }
        }

        consumed
    }

    fn on_send_complete(
        &mut self,
        _ctx: &mut DriverCtx,
        _conn: ConnToken,
        _result: io::Result<u32>,
    ) {
        // No action needed — responses tracked via on_data.
    }

    fn on_close(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        let idx = conn.index();
        if let Some(info) = self.conn_map.get_mut(idx).and_then(|o| o.as_mut()) {
            let shard_idx = info.shard_idx;

            for p in info.in_flight.drain(..) {
                p.fail(ClientError::ConnectionClosed);
            }

            self.pools[shard_idx]
                .connections
                .retain(|t| t.index() != idx);

            self.schedule_reconnect(shard_idx, 0);
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
