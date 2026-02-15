//! Worker implementation using kompio EventHandler.
//!
//! Each worker runs inside a kompio event loop, using callbacks
//! (`on_tick`, `on_connect`, `on_data`, `on_send_complete`, `on_close`)
//! to drive the benchmark workload.

use crate::client::{MomentoConn, MomentoSetup, RecvBuf, RequestResult, RequestType, Session};
use crate::config::{Config, Protocol as CacheProtocol};
use crate::metrics;
use crate::ratelimit::DynamicRateLimiter;

use kompio::{ConnToken, DriverCtx, EventHandler, GuardBox, RegionId, SendGuard};

use rand::prelude::*;
use rand_xoshiro::Xoshiro256PlusPlus;
use std::any::Any;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

/// Test phase, controlled by main thread and read by workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    /// Initial connection phase
    Connect = 0,
    /// Prefill phase - write each key exactly once
    Prefill = 1,
    /// Warmup phase - run workload but don't record metrics
    Warmup = 2,
    /// Main measurement phase - record metrics
    Running = 3,
    /// Stop phase - workers should exit
    Stop = 4,
}

impl Phase {
    #[inline]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Phase::Connect,
            1 => Phase::Prefill,
            2 => Phase::Warmup,
            3 => Phase::Running,
            _ => Phase::Stop,
        }
    }

    #[inline]
    pub fn is_recording(self) -> bool {
        self == Phase::Running
    }

    #[inline]
    pub fn should_stop(self) -> bool {
        self == Phase::Stop
    }
}

/// Reason for a connection disconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectReason {
    /// Server closed connection (recv returned 0)
    Eof,
    /// Error during recv
    RecvError,
    /// Error during send
    SendError,
    /// Closed completion event from driver
    ClosedEvent,
    /// Error completion event from driver
    ErrorEvent,
    /// Failed to establish connection
    ConnectFailed,
}

/// Shared state between workers and main thread.
pub struct SharedState {
    /// Current test phase (controlled by main thread)
    phase: AtomicU8,
    /// Number of workers that have completed prefill
    prefill_complete: AtomicUsize,
    /// Total number of prefill keys confirmed across all workers
    prefill_keys_confirmed: AtomicUsize,
    /// Total number of prefill keys assigned across all workers
    prefill_keys_total: AtomicUsize,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            phase: AtomicU8::new(Phase::Connect as u8),
            prefill_complete: AtomicUsize::new(0),
            prefill_keys_confirmed: AtomicUsize::new(0),
            prefill_keys_total: AtomicUsize::new(0),
        }
    }

    /// Get the current phase.
    #[inline]
    pub fn phase(&self) -> Phase {
        Phase::from_u8(self.phase.load(Ordering::Acquire))
    }

    /// Set the phase (called by main thread).
    #[inline]
    pub fn set_phase(&self, phase: Phase) {
        self.phase.store(phase as u8, Ordering::Release);
    }

    /// Mark this worker's prefill as complete.
    #[inline]
    pub fn mark_prefill_complete(&self) {
        self.prefill_complete.fetch_add(1, Ordering::Release);
    }

    /// Get the number of workers that have completed prefill.
    #[inline]
    pub fn prefill_complete_count(&self) -> usize {
        self.prefill_complete.load(Ordering::Acquire)
    }

    /// Add to the total prefill keys confirmed count.
    #[inline]
    pub fn add_prefill_confirmed(&self, n: usize) {
        self.prefill_keys_confirmed.fetch_add(n, Ordering::Release);
    }

    /// Get the total prefill keys confirmed across all workers.
    #[inline]
    pub fn prefill_keys_confirmed(&self) -> usize {
        self.prefill_keys_confirmed.load(Ordering::Acquire)
    }

    /// Add to the total prefill keys assigned count.
    #[inline]
    pub fn add_prefill_total(&self, n: usize) {
        self.prefill_keys_total.fetch_add(n, Ordering::Release);
    }

    /// Get the total prefill keys assigned across all workers.
    #[inline]
    pub fn prefill_keys_total(&self) -> usize {
        self.prefill_keys_total.load(Ordering::Acquire)
    }
}

impl Default for SharedState {
    fn default() -> Self {
        Self::new()
    }
}

/// Worker configuration passed through the config channel.
pub struct BenchWorkerConfig {
    pub id: usize,
    pub config: Config,
    pub shared: Arc<SharedState>,
    pub ratelimiter: Option<Arc<DynamicRateLimiter>>,
    /// Whether to record metrics (only true during Running phase)
    pub recording: bool,
    /// Range of key IDs this worker should prefill (start..end).
    pub prefill_range: Option<std::ops::Range<usize>>,
    /// CPU IDs for pinning (if configured).
    pub cpu_ids: Option<Vec<usize>>,
    /// Shared 1GB random value pool (all workers share the same Arc).
    pub value_pool: Arc<Vec<u8>>,
}

// ── Config channel (same pattern as server) ──────────────────────────────

static CONFIG_CHANNEL: OnceLock<Mutex<Box<dyn Any + Send>>> = OnceLock::new();

/// Initialize the global config channel. Must be called before launch.
pub fn init_config_channel(rx: crossbeam_channel::Receiver<BenchWorkerConfig>) {
    let boxed: Box<dyn Any + Send> = Box::new(rx);
    CONFIG_CHANNEL
        .set(Mutex::new(boxed))
        .expect("init_config_channel called twice");
}

fn recv_config() -> BenchWorkerConfig {
    let guard = CONFIG_CHANNEL
        .get()
        .expect("config channel not initialized")
        .lock()
        .unwrap();
    let rx = guard
        .downcast_ref::<crossbeam_channel::Receiver<BenchWorkerConfig>>()
        .expect("wrong config channel type");
    rx.recv().expect("config channel closed")
}

// ── DataSlice adapter for zero-copy parsing ──────────────────────────────

/// Wraps a `&[u8]` slice as a RecvBuf for zero-copy response parsing.
struct DataSlice<'a> {
    data: &'a [u8],
    consumed: usize,
}

impl<'a> DataSlice<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, consumed: 0 }
    }
}

impl RecvBuf for DataSlice<'_> {
    fn as_slice(&self) -> &[u8] {
        &self.data[self.consumed..]
    }
    fn len(&self) -> usize {
        self.data.len() - self.consumed
    }
    fn consume(&mut self, n: usize) {
        self.consumed += n;
    }
    fn capacity(&self) -> usize {
        self.data.len()
    }
    fn shrink_if_oversized(&mut self) {
        // no-op for borrowed slice
    }
}

// ── ValuePoolGuard ────────────────────────────────────────────────────────

/// Zero-copy send guard referencing a slice of the shared value pool.
///
/// When used with `ctx.send_parts().guard()`, the kernel sends directly from
/// the pool memory via SendMsgZc. The `Arc<Vec<u8>>` keeps the pool alive
/// until the kernel signals completion.
///
/// Size: 8 (Arc ptr) + 4 + 4 = 16 bytes, well within GuardBox's 64-byte limit.
struct ValuePoolGuard {
    pool: Arc<Vec<u8>>,
    offset: u32,
    len: u32,
}

impl SendGuard for ValuePoolGuard {
    fn as_ptr_len(&self) -> (*const u8, u32) {
        // SAFETY: offset + len is always within the pool bounds (enforced at construction).
        let ptr = unsafe { self.pool.as_ptr().add(self.offset as usize) };
        (ptr, self.len)
    }

    fn region(&self) -> RegionId {
        RegionId::UNREGISTERED
    }
}

// ── BenchHandler ─────────────────────────────────────────────────────────

/// Benchmark worker event handler for kompio.
pub struct BenchHandler {
    id: usize,
    config: Config,
    shared: Arc<SharedState>,

    /// Sessions (stable storage)
    sessions: Vec<Session>,

    /// Maps ConnToken::index() -> session index
    conn_to_session: HashMap<usize, usize>,

    /// Maps ConnToken::index() -> ConnToken (preserves generation)
    conn_tokens: HashMap<usize, ConnToken>,

    /// Tracks sessions that are being connected (ConnToken::index() -> session index)
    pending_connects: HashMap<usize, usize>,

    /// Momento connections (protocol state only, kompio handles I/O)
    momento_conns: Vec<MomentoConn>,
    /// Maps ConnToken::index() -> momento connection index
    conn_to_momento: HashMap<usize, usize>,
    /// Momento setup (credential, addresses, TLS host) resolved once per worker
    momento_setup: Option<MomentoSetup>,

    /// Workload generation
    rng: Xoshiro256PlusPlus,
    key_buf: Vec<u8>,
    /// Value buffer for Momento SET operations
    value_buf: Vec<u8>,
    /// Shared 1GB random value pool for zero-copy SET sends via send_parts()
    value_pool: Arc<Vec<u8>>,
    /// Reusable buffer for SET protocol framing (prefix bytes)
    set_prefix_buf: Vec<u8>,

    /// Results buffer (reused)
    results: Vec<RequestResult>,

    /// Rate limiting
    ratelimiter: Option<Arc<DynamicRateLimiter>>,

    /// Whether to record metrics (only true during Running phase)
    recording: bool,

    /// Last observed phase (for transition detection)
    last_phase: Phase,

    /// Whether initial connections have been established
    connections_initiated: bool,

    /// Prefill state
    prefill_pending: std::collections::VecDeque<usize>,
    prefill_in_flight: Vec<std::collections::VecDeque<usize>>,
    prefill_session_last_progress: Vec<std::time::Instant>,
    prefill_confirmed: usize,
    prefill_total: usize,
    prefill_done: bool,

    /// Endpoint list for connections
    endpoints: Vec<SocketAddr>,
    /// Number of connections this worker should manage
    my_connections: usize,
    /// Starting global connection index for endpoint distribution
    my_start: usize,

    /// Backfill queue: key IDs from GET misses that need SET backfills (backfill_on_miss).
    backfill_queue: Vec<usize>,
    /// Whether backfill_on_miss is enabled.
    backfill_on_miss: bool,

    /// Tick counter for periodic diagnostics
    tick_count: u64,
    /// Last diagnostic log time
    last_diag: std::time::Instant,
    /// Requests driven since last diagnostic
    requests_driven: u64,
    /// Responses parsed since last diagnostic
    responses_parsed: u64,
    /// Bytes received since last diagnostic
    bytes_received: u64,
    /// Send failures since last diagnostic
    send_failures: u64,
}

impl BenchHandler {
    /// Initiate connections to all endpoints.
    fn initiate_connections(&mut self, ctx: &mut DriverCtx) {
        if self.config.target.protocol == CacheProtocol::Momento {
            self.connect_momento(ctx);
            self.connections_initiated = true;
            return;
        }

        let num_endpoints = self.endpoints.len();

        for i in 0..self.my_connections {
            let global_conn_idx = self.my_start + i;
            let endpoint_idx = global_conn_idx % num_endpoints;
            let endpoint = self.endpoints[endpoint_idx];

            // Create session
            match Session::from_config(endpoint, &self.config) {
                Ok(session) => {
                    let idx = self.sessions.len();
                    self.sessions.push(session);
                    self.prefill_in_flight
                        .push(std::collections::VecDeque::new());
                    self.prefill_session_last_progress
                        .push(std::time::Instant::now());

                    // Mark as having a pending connect so try_reconnect doesn't
                    // issue a duplicate connect before this one completes.
                    self.sessions[idx].reconnect_attempted(true);

                    // Initiate async connect
                    match ctx.connect(endpoint) {
                        Ok(token) => {
                            self.conn_tokens.insert(token.index(), token);
                            self.pending_connects.insert(token.index(), idx);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "worker {} failed to connect to {}: {}",
                                self.id,
                                endpoint,
                                e
                            );
                            metrics::CONNECTIONS_FAILED.increment();
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "worker {} failed to create session for {}: {}",
                        self.id,
                        endpoint,
                        e
                    );
                }
            }
        }

        self.connections_initiated = true;
    }

    /// Connect Momento sessions via kompio TLS.
    fn connect_momento(&mut self, ctx: &mut DriverCtx) {
        // Resolve setup once per worker
        if self.momento_setup.is_none() {
            match MomentoSetup::from_config(&self.config) {
                Ok(setup) => self.momento_setup = Some(setup),
                Err(e) => {
                    tracing::error!("failed to resolve Momento config: {}", e);
                    metrics::CONNECTIONS_FAILED.increment();
                    return;
                }
            }
        }

        let total_connections = self.config.connection.total_connections();
        let num_threads = self.config.general.threads;

        let base_per_thread = total_connections / num_threads;
        let remainder = total_connections % num_threads;
        let my_connections = if self.id < remainder {
            base_per_thread + 1
        } else {
            base_per_thread
        };

        let setup = self.momento_setup.as_ref().unwrap();
        let num_addrs = setup.addresses.len();

        for i in 0..my_connections {
            let addr = setup.addresses[i % num_addrs];
            let conn = MomentoConn::new(&setup.credential, &self.config);
            let momento_idx = self.momento_conns.len();
            self.momento_conns.push(conn);

            match ctx.connect_tls(addr, &setup.tls_host) {
                Ok(token) => {
                    self.conn_tokens.insert(token.index(), token);
                    self.pending_connects.insert(token.index(), momento_idx);
                }
                Err(e) => {
                    tracing::warn!(
                        "worker {} failed to connect Momento to {}: {}",
                        self.id,
                        addr,
                        e
                    );
                    metrics::CONNECTIONS_FAILED.increment();
                }
            }
        }
    }

    /// Drive the Momento workload requests and flush pending sends.
    fn drive_momento_requests(&mut self, ctx: &mut DriverCtx, now: std::time::Instant) {
        let key_count = self.config.workload.keyspace.count;
        let get_ratio = self.config.workload.commands.get;
        let delete_ratio = self.config.workload.commands.delete;

        for idx in 0..self.momento_conns.len() {
            if !self.momento_conns[idx].is_ready() {
                continue;
            }

            while self.momento_conns[idx].can_send() {
                if let Some(ref rl) = self.ratelimiter
                    && !rl.try_acquire()
                {
                    break;
                }

                let key_id = self.rng.random_range(0..key_count);
                write_key(&mut self.key_buf, key_id);

                let roll = self.rng.random_range(0..100);
                let sent = if roll < get_ratio {
                    self.momento_conns[idx].get(&self.key_buf, now).is_some()
                } else if roll < get_ratio + delete_ratio {
                    self.momento_conns[idx].delete(&self.key_buf, now).is_some()
                } else {
                    self.rng.fill_bytes(&mut self.value_buf);
                    self.momento_conns[idx]
                        .set(&self.key_buf, &self.value_buf, now)
                        .is_some()
                };

                if sent {
                    self.requests_driven += 1;
                    metrics::REQUESTS_SENT.increment();
                } else {
                    break;
                }
            }

            self.flush_momento(ctx, idx);
        }
    }

    /// Drive Momento prefill requests and flush pending sends.
    fn drive_momento_prefill(&mut self, ctx: &mut DriverCtx, now: std::time::Instant) {
        for idx in 0..self.momento_conns.len() {
            if !self.momento_conns[idx].is_ready() {
                continue;
            }

            while self.momento_conns[idx].can_send() {
                let key_id = match self.prefill_pending.pop_front() {
                    Some(id) => id,
                    None => break,
                };

                write_key(&mut self.key_buf, key_id);
                self.rng.fill_bytes(&mut self.value_buf);

                if self.momento_conns[idx]
                    .set(&self.key_buf, &self.value_buf, now)
                    .is_none()
                {
                    self.prefill_pending.push_front(key_id);
                    break;
                }
                metrics::REQUESTS_SENT.increment();
            }

            self.flush_momento(ctx, idx);
        }
    }

    /// Flush pending send data for a Momento connection via kompio.
    fn flush_momento(&mut self, ctx: &mut DriverCtx, idx: usize) {
        let pending = self.momento_conns[idx].pending_send();
        if pending.is_empty() {
            return;
        }

        // Find the ConnToken for this momento connection
        let token = match self.find_momento_token(idx) {
            Some(t) => t,
            None => return,
        };

        let n = pending.len();
        match ctx.send(token, pending) {
            Ok(()) => {
                self.momento_conns[idx].advance_send(n);
            }
            Err(e) => {
                self.send_failures += 1;
                tracing::debug!(worker = self.id, error = %e, "momento send error");
            }
        }
    }

    /// Find the ConnToken for a Momento connection by scanning conn_to_momento.
    fn find_momento_token(&self, momento_idx: usize) -> Option<ConnToken> {
        for (&conn_index, &midx) in &self.conn_to_momento {
            if midx == momento_idx {
                return self.conn_tokens.get(&conn_index).copied();
            }
        }
        None
    }

    /// Handle Momento data received from kompio.
    fn on_momento_data(
        &mut self,
        ctx: &mut DriverCtx,
        _conn: ConnToken,
        midx: usize,
        data: &[u8],
    ) -> usize {
        let now = std::time::Instant::now();

        // Feed plaintext data directly into the HTTP/2 framing layer
        if let Err(e) = self.momento_conns[midx].feed_data(data) {
            tracing::debug!(worker = self.id, error = %e, "Momento feed_data error");
            return data.len();
        }

        // Poll for completed operations
        self.results.clear();
        self.momento_conns[midx].poll_responses(&mut self.results, now);

        // Handle prefill tracking
        let phase = self.shared.phase();
        if phase == Phase::Prefill && !self.prefill_done {
            self.handle_momento_prefill_results();
        }

        // Record metrics
        for result in &self.results {
            record_counters(result);
        }
        if self.recording {
            for result in &self.results {
                record_latencies(result);
            }
        }

        self.bytes_received += data.len() as u64;
        self.responses_parsed += self.results.len() as u64;

        if !data.is_empty() {
            metrics::BYTES_RX.add(data.len() as u64);
        }

        // Flush any pending sends generated by processing responses
        self.flush_momento(ctx, midx);

        // Drive more requests immediately when responses free pipeline slots
        if !self.results.is_empty() {
            if phase == Phase::Warmup || phase == Phase::Running {
                self.drive_momento_requests(ctx, now);
            } else if phase == Phase::Prefill && !self.prefill_done {
                self.drive_momento_prefill(ctx, now);
            }
        }

        data.len()
    }

    /// Close a Momento connection and clean up state.
    fn close_momento(&mut self, ctx: &mut DriverCtx, conn: ConnToken, _midx: usize) {
        self.conn_to_momento.remove(&conn.index());
        if let Some(token) = self.conn_tokens.remove(&conn.index()) {
            ctx.close(token);
        }
        metrics::CONNECTIONS_ACTIVE.decrement();
        metrics::CONNECTIONS_FAILED.increment();
    }

    /// Handle Momento prefill response tracking.
    fn handle_momento_prefill_results(&mut self) {
        if self.prefill_done {
            return;
        }
        let mut confirmed_batch = 0usize;
        for result in &self.results {
            if result.request_type == RequestType::Set && result.success {
                self.prefill_confirmed += 1;
                confirmed_batch += 1;
            }
        }
        if confirmed_batch > 0 {
            self.shared.add_prefill_confirmed(confirmed_batch);
        }
        if self.prefill_confirmed >= self.prefill_total {
            self.prefill_done = true;
            tracing::debug!(
                worker_id = self.id,
                confirmed = self.prefill_confirmed,
                total = self.prefill_total,
                "prefill complete (momento)"
            );
            self.shared.mark_prefill_complete();
        }
    }

    /// Drive normal workload requests.
    fn drive_requests(&mut self, ctx: &mut DriverCtx, now: std::time::Instant) {
        let key_count = self.config.workload.keyspace.count;
        let get_ratio = self.config.workload.commands.get;
        let delete_ratio = self.config.workload.commands.delete;
        let value_len = self.config.workload.values.length;
        let pool_len = self.value_pool.len();

        // Drain backfill queue before normal requests (backfill_on_miss)
        if !self.backfill_queue.is_empty() {
            self.drain_backfill_queue(ctx, now, value_len, pool_len);
        }

        for idx in 0..self.sessions.len() {
            if !self.sessions[idx].is_connected() {
                continue;
            }

            loop {
                if !self.sessions[idx].can_send() {
                    break;
                }

                if let Some(ref rl) = self.ratelimiter
                    && !rl.try_acquire()
                {
                    return;
                }

                let key_id = self.rng.random_range(0..key_count);
                write_key(&mut self.key_buf, key_id);

                let roll = self.rng.random_range(0..100);
                if roll < get_ratio {
                    // GET: encode into session buffer, batch-flush later
                    let kid = if self.backfill_on_miss {
                        Some(key_id)
                    } else {
                        None
                    };
                    if self.sessions[idx].get(&self.key_buf, now, kid).is_none() {
                        break;
                    }
                } else if roll < get_ratio + delete_ratio {
                    // DELETE: encode into session buffer, batch-flush later
                    if self.sessions[idx].delete(&self.key_buf, now).is_none() {
                        break;
                    }
                } else {
                    // SET: flush any pending GET/DELETE data first, then send via guard
                    self.flush_session(ctx, idx);

                    if let Some(suffix) = self.sessions[idx].encode_set_prefix(
                        &mut self.set_prefix_buf,
                        &self.key_buf,
                        value_len,
                    ) {
                        let prefix_len = self.set_prefix_buf.len();
                        let suffix_len = suffix.len();
                        if let Err(e) = self.send_set_parts(ctx, idx, value_len, pool_len, suffix) {
                            self.send_failures += 1;
                            if e.kind() != io::ErrorKind::Other {
                                tracing::debug!(worker = self.id, error = %e, "send_parts error (closing)");
                                self.close_session(ctx, idx, DisconnectReason::SendError);
                            }
                            break;
                        }
                        self.sessions[idx].confirm_set_sent(prefix_len, value_len, suffix_len, now);
                    } else {
                        break;
                    }
                }

                self.requests_driven += 1;
                metrics::REQUESTS_SENT.increment();
            }

            // Flush any remaining GET/DELETE data in the send buffer
            self.flush_session(ctx, idx);
        }
    }

    /// Drain the backfill queue by sending SET requests for keys that missed on GET.
    ///
    /// Backfill SETs use the same zero-copy path as regular SETs (encode_set_prefix +
    /// send_set_parts). Each backfill SET counts against rate limit and pipeline depth.
    fn drain_backfill_queue(
        &mut self,
        ctx: &mut DriverCtx,
        now: std::time::Instant,
        value_len: usize,
        pool_len: usize,
    ) {
        let mut session_idx = 0;
        let num_sessions = self.sessions.len();

        while !self.backfill_queue.is_empty() && session_idx < num_sessions {
            if !self.sessions[session_idx].is_connected() || !self.sessions[session_idx].can_send()
            {
                session_idx += 1;
                continue;
            }

            if let Some(ref rl) = self.ratelimiter
                && !rl.try_acquire()
            {
                return;
            }

            let key_id = self.backfill_queue.pop().unwrap();
            write_key(&mut self.key_buf, key_id);

            // Flush any pending GET/DELETE data first
            self.flush_session(ctx, session_idx);

            if let Some(suffix) = self.sessions[session_idx].encode_set_prefix(
                &mut self.set_prefix_buf,
                &self.key_buf,
                value_len,
            ) {
                let prefix_len = self.set_prefix_buf.len();
                let suffix_len = suffix.len();
                if let Err(e) = self.send_set_parts(ctx, session_idx, value_len, pool_len, suffix) {
                    self.send_failures += 1;
                    self.backfill_queue.push(key_id);
                    if e.kind() != io::ErrorKind::Other {
                        tracing::debug!(worker = self.id, error = %e, "backfill send_parts error (closing)");
                        self.close_session(ctx, session_idx, DisconnectReason::SendError);
                    }
                    session_idx += 1;
                    continue;
                }
                self.sessions[session_idx].confirm_set_sent(prefix_len, value_len, suffix_len, now);
                self.sessions[session_idx].mark_last_request_backfill();
                self.requests_driven += 1;
                metrics::REQUESTS_SENT.increment();
            } else {
                self.backfill_queue.push(key_id);
                session_idx += 1;
            }
        }
    }

    /// Maximum in-flight prefill requests per session, independent of the
    /// user-configured pipeline depth. Limits blast radius when a connection stalls.
    const PREFILL_MAX_PIPELINE: usize = 16;

    /// Drive prefill requests (sequential SET commands).
    fn drive_prefill_requests(&mut self, ctx: &mut DriverCtx, now: std::time::Instant) {
        let value_len = self.config.workload.values.length;
        let pool_len = self.value_pool.len();

        for idx in 0..self.sessions.len() {
            if !self.sessions[idx].is_connected() {
                continue;
            }

            loop {
                if !self.sessions[idx].can_send()
                    || self.prefill_in_flight[idx].len() >= Self::PREFILL_MAX_PIPELINE
                {
                    break;
                }

                let key_id = match self.prefill_pending.pop_front() {
                    Some(id) => id,
                    None => break,
                };

                write_key(&mut self.key_buf, key_id);

                if let Some(suffix) = self.sessions[idx].encode_set_prefix(
                    &mut self.set_prefix_buf,
                    &self.key_buf,
                    value_len,
                ) {
                    let prefix_len = self.set_prefix_buf.len();
                    let suffix_len = suffix.len();
                    if let Err(e) = self.send_set_parts(ctx, idx, value_len, pool_len, suffix) {
                        self.send_failures += 1;
                        self.prefill_pending.push_front(key_id);
                        if e.kind() != io::ErrorKind::Other {
                            tracing::debug!(worker = self.id, error = %e, "prefill send_parts error (closing)");
                            self.close_session(ctx, idx, DisconnectReason::SendError);
                        }
                        break;
                    }
                    self.sessions[idx].confirm_set_sent(prefix_len, value_len, suffix_len, now);
                    self.prefill_in_flight[idx].push_back(key_id);
                    metrics::REQUESTS_SENT.increment();
                } else {
                    self.prefill_pending.push_front(key_id);
                    break;
                }
            }
        }
    }

    /// Flush a session's send buffer via kompio.
    fn flush_session(&mut self, ctx: &mut DriverCtx, idx: usize) {
        let conn_id = match self.sessions[idx].conn_id() {
            Some(id) => id,
            None => return,
        };

        let send_buf = self.sessions[idx].send_buffer();
        if send_buf.is_empty() {
            return;
        }

        let token = match self.get_token(conn_id) {
            Some(t) => t,
            None => return,
        };

        let n = send_buf.len();
        match ctx.send(token, &send_buf[..n]) {
            Ok(()) => {
                self.sessions[idx].bytes_sent(n);
            }
            Err(e) => {
                self.send_failures += 1;
                if e.kind() != io::ErrorKind::Other {
                    tracing::debug!(worker = self.id, error = %e, "send submit error (closing)");
                    self.close_session(ctx, idx, DisconnectReason::SendError);
                } else {
                    tracing::debug!(worker = self.id, error = %e, "send submit pool exhausted");
                }
                // Pool exhausted - wait for on_send_complete
            }
        }
    }

    /// Submit a guard-based SET send via send_parts().
    fn send_set_parts(
        &mut self,
        ctx: &mut DriverCtx,
        idx: usize,
        value_len: usize,
        pool_len: usize,
        suffix: &'static [u8],
    ) -> io::Result<()> {
        let conn_id = match self.sessions[idx].conn_id() {
            Some(id) => id,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "session not connected",
                ));
            }
        };

        let token = match self.get_token(conn_id) {
            Some(t) => t,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "no token for connection",
                ));
            }
        };

        // Pick a random offset into the value pool
        let max_offset = pool_len - value_len;
        let offset = self.rng.random_range(0..=max_offset);

        let guard = ValuePoolGuard {
            pool: Arc::clone(&self.value_pool),
            offset: offset as u32,
            len: value_len as u32,
        };

        let mut builder = ctx.send_parts(token).copy(&self.set_prefix_buf);
        builder = builder.guard(GuardBox::new(guard));
        if !suffix.is_empty() {
            builder = builder.copy(suffix);
        }
        builder.submit()
    }

    /// Try to reconnect disconnected sessions.
    fn try_reconnect(&mut self, ctx: &mut DriverCtx) {
        // Build set of sessions that already have a pending connect to avoid duplicates.
        let pending_sessions: std::collections::HashSet<usize> =
            self.pending_connects.values().copied().collect();

        let to_reconnect: Vec<(usize, SocketAddr, Option<usize>)> = self
            .sessions
            .iter()
            .enumerate()
            .filter(|(idx, s)| s.should_reconnect() && !pending_sessions.contains(idx))
            .map(|(idx, s)| (idx, s.addr(), s.conn_id()))
            .collect();

        for (idx, addr, old_conn_id) in to_reconnect {
            // Remove old mapping
            if let Some(old_id) = old_conn_id {
                self.conn_to_session.remove(&old_id);
                self.conn_tokens.remove(&old_id);
            }

            match ctx.connect(addr) {
                Ok(token) => {
                    let session = &mut self.sessions[idx];
                    session.reset();
                    self.conn_tokens.insert(token.index(), token);
                    self.pending_connects.insert(token.index(), idx);
                }
                Err(_) => {
                    self.sessions[idx].reconnect_attempted(false);
                }
            }
        }
    }

    /// Close a session and mark it disconnected.
    fn close_session(&mut self, ctx: &mut DriverCtx, idx: usize, reason: DisconnectReason) {
        let conn_id = self.sessions[idx].conn_id();
        if let Some(conn_id) = conn_id {
            if let Some(token) = self.conn_tokens.remove(&conn_id) {
                ctx.close(token);
            }
            self.conn_to_session.remove(&conn_id);
            metrics::CONNECTIONS_ACTIVE.decrement();
        }
        self.sessions[idx].disconnect();

        // Move in-flight prefill keys back to pending
        if !self.prefill_done && idx < self.prefill_in_flight.len() {
            while let Some(key_id) = self.prefill_in_flight[idx].pop_front() {
                self.prefill_pending.push_back(key_id);
            }
        }

        metrics::CONNECTIONS_FAILED.increment();

        match reason {
            DisconnectReason::Eof => metrics::DISCONNECTS_EOF.increment(),
            DisconnectReason::RecvError => metrics::DISCONNECTS_RECV_ERROR.increment(),
            DisconnectReason::SendError => metrics::DISCONNECTS_SEND_ERROR.increment(),
            DisconnectReason::ClosedEvent => metrics::DISCONNECTS_CLOSED_EVENT.increment(),
            DisconnectReason::ErrorEvent => metrics::DISCONNECTS_ERROR_EVENT.increment(),
            DisconnectReason::ConnectFailed => metrics::DISCONNECTS_CONNECT_FAILED.increment(),
        }
    }

    /// Look up the stored ConnToken for a connection index.
    fn get_token(&self, conn_index: usize) -> Option<ConnToken> {
        self.conn_tokens.get(&conn_index).copied()
    }

    /// Handle prefill response tracking.
    fn handle_prefill_results(&mut self, idx: usize) {
        if self.prefill_done {
            return;
        }

        let mut confirmed_batch = 0usize;
        for result in &self.results {
            if result.request_type == RequestType::Set
                && let Some(key_id) = self.prefill_in_flight[idx].pop_front()
            {
                if result.success {
                    self.prefill_confirmed += 1;
                    confirmed_batch += 1;
                } else {
                    self.prefill_pending.push_back(key_id);
                }
            }
        }

        if confirmed_batch > 0 {
            self.shared.add_prefill_confirmed(confirmed_batch);
            if idx < self.prefill_session_last_progress.len() {
                self.prefill_session_last_progress[idx] = std::time::Instant::now();
            }
        }

        if self.prefill_confirmed >= self.prefill_total {
            self.prefill_done = true;
            tracing::debug!(
                worker_id = self.id,
                confirmed = self.prefill_confirmed,
                total = self.prefill_total,
                "prefill complete"
            );
            self.shared.mark_prefill_complete();
        }
    }

    /// Process responses from session internal buffers (for data not parsed in on_data).
    fn poll_session_responses(&mut self, now: std::time::Instant) {
        for idx in 0..self.sessions.len() {
            self.results.clear();
            let _ = self.sessions[idx].poll_responses(&mut self.results, now);

            self.handle_prefill_results(idx);

            for result in &self.results {
                record_counters(result);
            }
            if self.recording {
                for result in &self.results {
                    record_latencies(result);
                }
            }

            // Queue backfill SETs for GET misses (backfill_on_miss)
            if self.backfill_on_miss {
                for result in &self.results {
                    if result.request_type == RequestType::Get
                        && result.hit == Some(false)
                        && result.success
                        && let Some(key_id) = result.key_id
                    {
                        self.backfill_queue.push(key_id);
                    }
                }
            }
        }
    }
}

impl EventHandler for BenchHandler {
    fn create_for_worker(worker_id: usize) -> Self {
        tracing::debug!(worker_id, "worker thread starting create_for_worker");
        let cfg = recv_config();
        if let Some(ref ids) = cfg.cpu_ids
            && !ids.is_empty()
        {
            let cpu_id = ids[cfg.id % ids.len()];
            #[cfg(target_os = "linux")]
            {
                if let Err(e) = pin_to_cpu(cpu_id) {
                    tracing::warn!("failed to pin worker {} to CPU {}: {}", cfg.id, cpu_id, e);
                } else {
                    tracing::debug!("pinned worker {} to CPU {}", cfg.id, cpu_id);
                }
            }
            let _ = cpu_id;
        }

        // Set metrics thread shard
        metrics::set_thread_shard(worker_id);

        let rng = Xoshiro256PlusPlus::seed_from_u64(42 + cfg.id as u64);
        let key_buf = vec![0u8; cfg.config.workload.keyspace.length];
        let mut value_buf = vec![0u8; cfg.config.workload.values.length];
        let pipeline_depth = cfg.config.connection.pipeline_depth;
        let value_pool = cfg.value_pool;

        // Fill value buffer with random data (used only for Momento sessions)
        let mut init_rng = Xoshiro256PlusPlus::seed_from_u64(42);
        init_rng.fill_bytes(&mut value_buf);

        // Initialize prefill state
        let (prefill_pending, prefill_total, prefill_done) = match cfg.prefill_range {
            Some(range) => {
                let pending: std::collections::VecDeque<usize> = (range.start..range.end).collect();
                let total = range.end - range.start;
                cfg.shared.add_prefill_total(total);
                tracing::debug!(
                    worker_id = cfg.id,
                    total,
                    start = range.start,
                    end = range.end,
                    "prefill initialized"
                );
                (pending, total, total == 0)
            }
            None => {
                cfg.shared.mark_prefill_complete();
                (std::collections::VecDeque::new(), 0, true)
            }
        };

        // Compute connection distribution
        let endpoints = cfg.config.target.endpoints.clone();
        let total_connections = cfg.config.connection.total_connections();
        let num_threads = cfg.config.general.threads;

        let base_per_thread = total_connections / num_threads;
        let remainder = total_connections % num_threads;
        let my_connections = if cfg.id < remainder {
            base_per_thread + 1
        } else {
            base_per_thread
        };
        let my_start = if cfg.id < remainder {
            cfg.id * (base_per_thread + 1)
        } else {
            remainder * (base_per_thread + 1) + (cfg.id - remainder) * base_per_thread
        };

        let backfill_on_miss = cfg.config.workload.backfill_on_miss;

        let result = BenchHandler {
            id: cfg.id,
            config: cfg.config,
            shared: cfg.shared,
            sessions: Vec::new(),
            conn_to_session: HashMap::new(),
            conn_tokens: HashMap::new(),
            pending_connects: HashMap::new(),
            momento_conns: Vec::new(),
            conn_to_momento: HashMap::new(),
            momento_setup: None,
            rng,
            key_buf,
            value_buf,
            value_pool,
            set_prefix_buf: Vec::with_capacity(256),
            results: Vec::with_capacity(pipeline_depth),
            ratelimiter: cfg.ratelimiter,
            recording: cfg.recording,
            last_phase: Phase::Connect,
            connections_initiated: false,
            prefill_pending,
            prefill_in_flight: Vec::new(),
            prefill_session_last_progress: Vec::new(),
            prefill_confirmed: 0,
            prefill_total,
            prefill_done,
            endpoints,
            my_connections,
            my_start,
            backfill_queue: Vec::new(),
            backfill_on_miss,
            tick_count: 0,
            last_diag: std::time::Instant::now(),
            requests_driven: 0,
            responses_parsed: 0,
            bytes_received: 0,
            send_failures: 0,
        };
        tracing::debug!(
            worker_id = result.id,
            sessions = result.sessions.len(),
            my_connections = result.my_connections,
            "worker create_for_worker complete, entering event loop"
        );
        result
    }

    fn on_accept(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {
        // Benchmark is client-only, no accepts expected
    }

    fn on_connect(&mut self, ctx: &mut DriverCtx, conn: ConnToken, result: io::Result<()>) {
        let idx = match self.pending_connects.remove(&conn.index()) {
            Some(idx) => idx,
            None => return,
        };

        if self.config.target.protocol == CacheProtocol::Momento {
            // Momento connection: idx is a momento_conns index
            match result {
                Ok(()) => {
                    self.conn_to_momento.insert(conn.index(), idx);
                    self.conn_tokens.insert(conn.index(), conn);

                    // Initialize HTTP/2 preface (or protosocket auth)
                    if let Err(e) = self.momento_conns[idx].on_transport_ready() {
                        tracing::warn!("worker {} Momento transport ready failed: {}", self.id, e);
                        ctx.close(conn);
                        self.conn_to_momento.remove(&conn.index());
                        self.conn_tokens.remove(&conn.index());
                        metrics::CONNECTIONS_FAILED.increment();
                        return;
                    }

                    // Flush the HTTP/2 connection preface
                    self.flush_momento(ctx, idx);
                    metrics::CONNECTIONS_ACTIVE.increment();

                    tracing::debug!(
                        "worker {} connected Momento {} (conn_index={})",
                        self.id,
                        idx,
                        conn.index()
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "worker {} Momento connection failed for {}: {}",
                        self.id,
                        idx,
                        e
                    );
                    self.conn_tokens.remove(&conn.index());
                    metrics::CONNECTIONS_FAILED.increment();
                }
            }
        } else {
            // Regular session connection
            match result {
                Ok(()) => {
                    let session = &mut self.sessions[idx];
                    session.set_conn_id(conn.index());
                    session.reconnect_attempted(true);
                    self.conn_to_session.insert(conn.index(), idx);
                    self.conn_tokens.insert(conn.index(), conn);
                    metrics::CONNECTIONS_ACTIVE.increment();

                    tracing::debug!(
                        "worker {} connected session {} (conn_index={})",
                        self.id,
                        idx,
                        conn.index()
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "worker {} connection failed for session {}: {}",
                        self.id,
                        idx,
                        e
                    );
                    self.sessions[idx].reconnect_attempted(false);
                    metrics::CONNECTIONS_FAILED.increment();
                }
            }
        }
    }

    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        // Check if this is a Momento connection
        if let Some(&midx) = self.conn_to_momento.get(&conn.index()) {
            return self.on_momento_data(ctx, conn, midx, data);
        }

        let idx = match self.conn_to_session.get(&conn.index()) {
            Some(&idx) => idx,
            None => return data.len(),
        };

        let now = std::time::Instant::now();
        let session = &mut self.sessions[idx];

        // Stamp TTFB on the first unstamped in-flight request
        session.stamp_first_byte(now);

        // Zero-copy path: parse responses directly from kompio's buffer
        self.results.clear();
        let mut buf = DataSlice::new(data);
        if let Err(e) = session.poll_responses_from(&mut buf, &mut self.results, now) {
            tracing::debug!(
                worker = self.id,
                consumed = buf.consumed,
                remaining_len = data.len() - buf.consumed,
                in_flight = session.in_flight_count(),
                "protocol error: {}",
                e,
            );
        }

        // Handle prefill tracking
        self.handle_prefill_results(idx);

        // Record metrics (counters always, latencies only when recording)
        for result in &self.results {
            record_counters(result);
        }
        if self.recording {
            for result in &self.results {
                record_latencies(result);
            }
        }

        // Queue backfill SETs for GET misses (backfill_on_miss)
        if self.backfill_on_miss {
            for result in &self.results {
                if result.request_type == RequestType::Get
                    && result.hit == Some(false)
                    && result.success
                    && let Some(key_id) = result.key_id
                {
                    self.backfill_queue.push(key_id);
                }
            }
        }

        // Track bytes received
        if buf.consumed > 0 {
            metrics::BYTES_RX.add(buf.consumed as u64);
        }

        // Diagnostic tracking
        self.bytes_received += data.len() as u64;
        self.responses_parsed += self.results.len() as u64;

        // Drive more requests immediately when responses free pipeline slots.
        // Without this, we'd wait for the next on_tick (~1ms) before refilling
        // the pipeline, which severely limits throughput with large values where
        // each response takes several ms to arrive.
        if !self.results.is_empty() {
            let phase = self.shared.phase();
            if phase == Phase::Warmup || phase == Phase::Running {
                self.drive_requests(ctx, now);
            } else if phase == Phase::Prefill && !self.prefill_done {
                self.drive_prefill_requests(ctx, now);
            }
        }

        buf.consumed
    }

    fn on_send_complete(&mut self, ctx: &mut DriverCtx, conn: ConnToken, result: io::Result<u32>) {
        // Check if this is a Momento connection
        if let Some(&midx) = self.conn_to_momento.get(&conn.index()) {
            if result.is_err() {
                self.close_momento(ctx, conn, midx);
                return;
            }
            // Flush remaining pending data and drive more requests
            self.flush_momento(ctx, midx);
            let phase = self.shared.phase();
            let now = std::time::Instant::now();
            if phase == Phase::Prefill && !self.prefill_done {
                self.drive_momento_prefill(ctx, now);
            } else if phase == Phase::Warmup || phase == Phase::Running {
                self.drive_momento_requests(ctx, now);
            }
            return;
        }

        if result.is_err() {
            if let Some(&idx) = self.conn_to_session.get(&conn.index()) {
                self.close_session(ctx, idx, DisconnectReason::SendError);
            }
            return;
        }

        // Try to flush more pending data for this session
        if let Some(&idx) = self.conn_to_session.get(&conn.index()) {
            self.flush_session(ctx, idx);
        }

        // Drive more requests immediately so the pipeline refills without
        // waiting for the next on_tick. This is critical for throughput when
        // send_pending serializes sends to one-at-a-time per session.
        let phase = self.shared.phase();
        if phase == Phase::Warmup || phase == Phase::Running {
            let now = std::time::Instant::now();
            self.drive_requests(ctx, now);
        } else if phase == Phase::Prefill && !self.prefill_done {
            let now = std::time::Instant::now();
            self.drive_prefill_requests(ctx, now);
        }
    }

    fn on_close(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        // Check pending connects first
        if let Some(_idx) = self.pending_connects.remove(&conn.index()) {
            self.conn_tokens.remove(&conn.index());
            if self.config.target.protocol != CacheProtocol::Momento {
                self.sessions[_idx].reconnect_attempted(false);
            }
            metrics::CONNECTIONS_FAILED.increment();
            return;
        }

        // Momento connection close
        if let Some(&midx) = self.conn_to_momento.get(&conn.index()) {
            self.conn_to_momento.remove(&conn.index());
            self.conn_tokens.remove(&conn.index());
            metrics::CONNECTIONS_ACTIVE.decrement();
            metrics::CONNECTIONS_FAILED.increment();
            metrics::DISCONNECTS_CLOSED_EVENT.increment();
            tracing::debug!(
                worker = self.id,
                momento_idx = midx,
                "Momento connection closed"
            );
            return;
        }

        if let Some(&idx) = self.conn_to_session.get(&conn.index()) {
            self.sessions[idx].disconnect();
            self.conn_to_session.remove(&conn.index());
            self.conn_tokens.remove(&conn.index());

            // Move in-flight prefill keys back to pending
            if !self.prefill_done && idx < self.prefill_in_flight.len() {
                while let Some(key_id) = self.prefill_in_flight[idx].pop_front() {
                    self.prefill_pending.push_back(key_id);
                }
            }

            metrics::CONNECTIONS_ACTIVE.decrement();
            metrics::CONNECTIONS_FAILED.increment();
            metrics::DISCONNECTS_CLOSED_EVENT.increment();
        }
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx) {
        let phase = self.shared.phase();

        // Initiate connections on first tick
        if !self.connections_initiated {
            self.initiate_connections(ctx);
        }

        // Update recording state on phase transition
        if phase != self.last_phase {
            if phase == Phase::Running {
                let connected = self.sessions.iter().filter(|s| s.is_connected()).count();
                let total_in_flight: usize =
                    self.sessions.iter().map(|s| s.in_flight_count()).sum();
                tracing::debug!(
                    worker = self.id,
                    connected,
                    total_sessions = self.sessions.len(),
                    total_in_flight,
                    "entering Running phase"
                );
            }
            self.recording = phase.is_recording();
            self.last_phase = phase;
        }

        // Check for shutdown
        if phase.should_stop() {
            ctx.request_shutdown();
            return;
        }

        let now = std::time::Instant::now();

        // Momento connections: drive requests from on_tick
        if self.config.target.protocol == CacheProtocol::Momento {
            if phase == Phase::Prefill && !self.prefill_done {
                self.drive_momento_prefill(ctx, now);
            } else if phase == Phase::Warmup || phase == Phase::Running {
                self.drive_momento_requests(ctx, now);
            }

            // Periodic diagnostic heartbeat
            self.tick_count += 1;
            if self.last_diag.elapsed() >= std::time::Duration::from_secs(2) {
                let ready = self.momento_conns.iter().filter(|c| c.is_ready()).count();
                let total_in_flight: usize =
                    self.momento_conns.iter().map(|c| c.in_flight_count()).sum();
                tracing::trace!(
                    worker = self.id,
                    phase = ?phase,
                    recording = self.recording,
                    ready,
                    total_momento = self.momento_conns.len(),
                    total_in_flight,
                    ticks = self.tick_count,
                    reqs_driven = self.requests_driven,
                    resps_parsed = self.responses_parsed,
                    bytes_rx = self.bytes_received,
                    send_fails = self.send_failures,
                    "momento diagnostic heartbeat"
                );
                self.tick_count = 0;
                self.requests_driven = 0;
                self.responses_parsed = 0;
                self.bytes_received = 0;
                self.send_failures = 0;
                self.last_diag = std::time::Instant::now();
            }
            return;
        }

        // Try to reconnect disconnected sessions
        self.try_reconnect(ctx);

        // Phase-specific work
        if phase == Phase::Prefill && !self.prefill_done {
            self.drive_prefill_requests(ctx, now);

            // Per-session prefill timeout: close sessions that have in-flight
            // keys but haven't received a response within 10 seconds.
            const PREFILL_SESSION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
            for idx in 0..self.sessions.len() {
                if !self.prefill_in_flight[idx].is_empty()
                    && idx < self.prefill_session_last_progress.len()
                    && self.prefill_session_last_progress[idx].elapsed() > PREFILL_SESSION_TIMEOUT
                {
                    tracing::warn!(
                        worker = self.id,
                        session = idx,
                        in_flight = self.prefill_in_flight[idx].len(),
                        "prefill session stalled for >10s, closing"
                    );
                    self.close_session(ctx, idx, DisconnectReason::SendError);
                    // Reset the progress timer so we don't immediately re-trigger
                    // after reconnect. close_session moves keys back to prefill_pending.
                    self.prefill_session_last_progress[idx] = now;
                }
            }
        } else if phase == Phase::Warmup || phase == Phase::Running {
            self.drive_requests(ctx, now);
        }

        // Process any responses from session internal buffers
        self.poll_session_responses(std::time::Instant::now());

        // Periodic diagnostic heartbeat (every 2 seconds)
        self.tick_count += 1;
        if self.last_diag.elapsed() >= std::time::Duration::from_secs(2) {
            let connected = self.sessions.iter().filter(|s| s.is_connected()).count();
            let total_in_flight: usize = self.sessions.iter().map(|s| s.in_flight_count()).sum();
            let can_send_count = self.sessions.iter().filter(|s| s.can_send()).count();
            tracing::trace!(
                worker = self.id,
                phase = ?phase,
                recording = self.recording,
                connected,
                total_in_flight,
                can_send = can_send_count,
                ticks = self.tick_count,
                reqs_driven = self.requests_driven,
                resps_parsed = self.responses_parsed,
                bytes_rx = self.bytes_received,
                send_fails = self.send_failures,
                "diagnostic heartbeat"
            );
            self.tick_count = 0;
            self.requests_driven = 0;
            self.responses_parsed = 0;
            self.bytes_received = 0;
            self.send_failures = 0;
            self.last_diag = std::time::Instant::now();
        }
    }
}

/// Record counter metrics for a completed request result (always called).
fn record_counters(result: &RequestResult) {
    metrics::RESPONSES_RECEIVED.increment();
    if result.is_error_response {
        metrics::REQUEST_ERRORS.increment();
    }
    if let Some(hit) = result.hit {
        if hit {
            metrics::CACHE_HITS.increment();
        } else {
            metrics::CACHE_MISSES.increment();
        }
    }
    match result.request_type {
        RequestType::Get => metrics::GET_COUNT.increment(),
        RequestType::Set => metrics::SET_COUNT.increment(),
        RequestType::Delete => metrics::DELETE_COUNT.increment(),
        RequestType::Ping | RequestType::Other => {}
    }
}

/// Record latency histograms for a completed request result (only during Running phase).
fn record_latencies(result: &RequestResult) {
    let _ = metrics::RESPONSE_LATENCY.increment(result.latency_ns);
    match result.request_type {
        RequestType::Get => {
            let _ = metrics::GET_LATENCY.increment(result.latency_ns);
            if let Some(ttfb) = result.ttfb_ns {
                let _ = metrics::GET_TTFB.increment(ttfb);
            }
        }
        RequestType::Set => {
            let _ = metrics::SET_LATENCY.increment(result.latency_ns);
            if result.backfill {
                metrics::BACKFILL_SET_COUNT.increment();
                let _ = metrics::BACKFILL_SET_LATENCY.increment(result.latency_ns);
            }
        }
        RequestType::Delete => {
            let _ = metrics::DELETE_LATENCY.increment(result.latency_ns);
        }
        RequestType::Ping | RequestType::Other => {}
    }
}

/// Write a numeric key ID into the buffer as hex.
fn write_key(buf: &mut [u8], id: usize) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut n = id;
    for byte in buf.iter_mut().rev() {
        *byte = HEX[n & 0xf];
        n >>= 4;
    }
}

/// Pin the current thread to a specific CPU core.
#[cfg(target_os = "linux")]
fn pin_to_cpu(cpu_id: usize) -> std::io::Result<()> {
    use std::mem;

    unsafe {
        let mut cpuset: libc::cpu_set_t = mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(cpu_id, &mut cpuset);

        let result = libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &cpuset);

        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}
