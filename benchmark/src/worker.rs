//! Worker implementation using kompio EventHandler.
//!
//! Each worker runs inside a kompio event loop, using callbacks
//! (`on_tick`, `on_connect`, `on_data`, `on_send_complete`, `on_close`)
//! to drive the benchmark workload.

use crate::client::{MomentoSession, RecvBuf, RequestResult, RequestType, Session};
use crate::config::{Config, Protocol as CacheProtocol};
use crate::metrics;
use crate::ratelimit::DynamicRateLimiter;

use kompio::{ConnToken, DriverCtx, EventHandler};

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

    /// Momento sessions (handle their own I/O)
    momento_sessions: Vec<MomentoSession>,

    /// Workload generation
    rng: Xoshiro256PlusPlus,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,

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
    prefill_confirmed: usize,
    prefill_total: usize,
    prefill_done: bool,

    /// Endpoint list for connections
    endpoints: Vec<SocketAddr>,
    /// Number of connections this worker should manage
    my_connections: usize,
    /// Starting global connection index for endpoint distribution
    my_start: usize,

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
            self.connect_momento();
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

    /// Connect Momento sessions.
    fn connect_momento(&mut self) {
        let total_connections = self.config.connection.total_connections();
        let num_threads = self.config.general.threads;

        let base_per_thread = total_connections / num_threads;
        let remainder = total_connections % num_threads;
        let my_connections = if self.id < remainder {
            base_per_thread + 1
        } else {
            base_per_thread
        };

        for _ in 0..my_connections {
            match MomentoSession::new(&self.config) {
                Ok(mut session) => {
                    if let Err(e) = session.connect() {
                        tracing::warn!("failed to connect Momento session: {}", e);
                        metrics::CONNECTIONS_FAILED.increment();
                    }
                    self.momento_sessions.push(session);
                }
                Err(e) => {
                    tracing::error!("failed to create Momento session: {}", e);
                    metrics::CONNECTIONS_FAILED.increment();
                }
            }
        }
    }

    /// Drive the Momento workload (both prefill and normal).
    fn poll_momento(&mut self, now: std::time::Instant) {
        let phase = self.shared.phase();

        if phase == Phase::Prefill && !self.prefill_done {
            self.poll_momento_prefill(now);
            return;
        }

        let key_count = self.config.workload.keyspace.count;
        let get_ratio = self.config.workload.commands.get;
        let delete_ratio = self.config.workload.commands.delete;
        let recording = self.recording;

        for session in &mut self.momento_sessions {
            if !session.is_connected() {
                match session.drive() {
                    Ok(true) => {
                        metrics::CONNECTIONS_ACTIVE.increment();
                    }
                    Ok(false) => continue,
                    Err(e) => {
                        tracing::debug!("Momento connection error: {}", e);
                        continue;
                    }
                }
            }

            while session.can_send() {
                if let Some(ref rl) = self.ratelimiter
                    && !rl.try_acquire()
                {
                    break;
                }

                let key_id = self.rng.random_range(0..key_count);
                write_key(&mut self.key_buf, key_id);

                let roll = self.rng.random_range(0..100);
                let sent = if roll < get_ratio {
                    session.get(&self.key_buf, now).is_some()
                } else if roll < get_ratio + delete_ratio {
                    session.delete(&self.key_buf, now).is_some()
                } else {
                    self.rng.fill_bytes(&mut self.value_buf);
                    session.set(&self.key_buf, &self.value_buf, now).is_some()
                };

                if sent && recording {
                    metrics::REQUESTS_SENT.increment();
                }

                if !sent {
                    break;
                }
            }

            self.results.clear();
            if let Err(e) = session.poll_responses(&mut self.results, std::time::Instant::now()) {
                tracing::debug!("Momento poll error: {}", e);
            }

            if recording {
                for result in &self.results {
                    record_metrics(result);
                }
            }
        }
    }

    /// Momento prefill.
    fn poll_momento_prefill(&mut self, now: std::time::Instant) {
        for session in &mut self.momento_sessions {
            if !session.is_connected() {
                match session.drive() {
                    Ok(true) => {
                        metrics::CONNECTIONS_ACTIVE.increment();
                    }
                    Ok(false) => continue,
                    Err(e) => {
                        tracing::debug!("Momento connection error: {}", e);
                        continue;
                    }
                }
            }

            while session.can_send() {
                let key_id = match self.prefill_pending.pop_front() {
                    Some(id) => id,
                    None => break,
                };

                write_key(&mut self.key_buf, key_id);
                self.rng.fill_bytes(&mut self.value_buf);

                if session.set(&self.key_buf, &self.value_buf, now).is_none() {
                    self.prefill_pending.push_front(key_id);
                    break;
                }
            }

            self.results.clear();
            if let Err(e) = session.poll_responses(&mut self.results, std::time::Instant::now()) {
                tracing::debug!("Momento poll error: {}", e);
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
        let recording = self.recording;

        for idx in 0..self.sessions.len() {
            let session = &mut self.sessions[idx];
            if !session.is_connected() {
                continue;
            }

            while session.can_send() {
                if let Some(ref rl) = self.ratelimiter
                    && !rl.try_acquire()
                {
                    return;
                }

                let key_id = self.rng.random_range(0..key_count);
                write_key(&mut self.key_buf, key_id);

                let roll = self.rng.random_range(0..100);
                let sent = if roll < get_ratio {
                    session.get(&self.key_buf, now).is_some()
                } else if roll < get_ratio + delete_ratio {
                    session.delete(&self.key_buf, now).is_some()
                } else {
                    self.rng.fill_bytes(&mut self.value_buf);
                    session.set(&self.key_buf, &self.value_buf, now).is_some()
                };

                if sent {
                    self.requests_driven += 1;
                    if recording {
                        metrics::REQUESTS_SENT.increment();
                    }
                }

                if !sent {
                    break;
                }
            }

            // Flush send buffer
            self.flush_session(ctx, idx);
        }
    }

    /// Drive prefill requests (sequential SET commands).
    fn drive_prefill_requests(&mut self, ctx: &mut DriverCtx, now: std::time::Instant) {
        for idx in 0..self.sessions.len() {
            let session = &mut self.sessions[idx];
            if !session.is_connected() {
                continue;
            }

            while session.can_send() {
                let key_id = match self.prefill_pending.pop_front() {
                    Some(id) => id,
                    None => break,
                };

                write_key(&mut self.key_buf, key_id);
                self.rng.fill_bytes(&mut self.value_buf);

                if session.set(&self.key_buf, &self.value_buf, now).is_some() {
                    self.prefill_in_flight[idx].push_back(key_id);
                } else {
                    self.prefill_pending.push_front(key_id);
                    break;
                }
            }

            // Flush send buffer
            self.flush_session(ctx, idx);
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

        match ctx.send(token, send_buf) {
            Ok(()) => {
                self.sessions[idx].bytes_sent(n);
            }
            Err(e) => {
                self.send_failures += 1;
                if e.kind() != io::ErrorKind::Other {
                    tracing::debug!(worker = self.id, error = %e, "send submit error (closing)");
                    // Real error - close session
                    self.close_session(ctx, idx, DisconnectReason::SendError);
                } else {
                    tracing::debug!(worker = self.id, error = %e, "send submit pool exhausted");
                }
                // Pool exhausted - wait for on_send_complete
            }
        }
    }

    /// Try to reconnect disconnected sessions.
    fn try_reconnect(&mut self, ctx: &mut DriverCtx) {
        let to_reconnect: Vec<(usize, SocketAddr, Option<usize>)> = self
            .sessions
            .iter()
            .enumerate()
            .filter(|(_, s)| s.should_reconnect())
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

            if self.recording {
                for result in &self.results {
                    record_metrics(result);
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

        // Fill value buffer with random data
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
            None => (std::collections::VecDeque::new(), 0, true),
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

        let result = BenchHandler {
            id: cfg.id,
            config: cfg.config,
            shared: cfg.shared,
            sessions: Vec::new(),
            conn_to_session: HashMap::new(),
            conn_tokens: HashMap::new(),
            pending_connects: HashMap::new(),
            momento_sessions: Vec::new(),
            rng,
            key_buf,
            value_buf,
            results: Vec::with_capacity(pipeline_depth),
            ratelimiter: cfg.ratelimiter,
            recording: cfg.recording,
            last_phase: Phase::Connect,
            connections_initiated: false,
            prefill_pending,
            prefill_in_flight: Vec::new(),
            prefill_confirmed: 0,
            prefill_total,
            prefill_done,
            endpoints,
            my_connections,
            my_start,
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

    fn on_connect(&mut self, _ctx: &mut DriverCtx, conn: ConnToken, result: io::Result<()>) {
        let idx = match self.pending_connects.remove(&conn.index()) {
            Some(idx) => idx,
            None => return,
        };

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

    fn on_data(&mut self, _ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
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
            tracing::debug!("protocol error: {}", e);
        }

        // Handle prefill tracking
        self.handle_prefill_results(idx);

        // Record metrics
        if self.recording {
            for result in &self.results {
                record_metrics(result);
            }
        }

        // Track bytes received
        if buf.consumed > 0 {
            metrics::BYTES_RX.add(buf.consumed as u64);
        }

        // Diagnostic tracking
        self.bytes_received += data.len() as u64;
        self.responses_parsed += self.results.len() as u64;

        buf.consumed
    }

    fn on_send_complete(&mut self, ctx: &mut DriverCtx, conn: ConnToken, result: io::Result<u32>) {
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
    }

    fn on_close(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        // Check pending connects first
        if let Some(idx) = self.pending_connects.remove(&conn.index()) {
            self.conn_tokens.remove(&conn.index());
            self.sessions[idx].reconnect_attempted(false);
            metrics::CONNECTIONS_FAILED.increment();
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

        // Momento sessions: drive entirely from on_tick
        if self.config.target.protocol == CacheProtocol::Momento {
            self.poll_momento(now);
            return;
        }

        // Try to reconnect disconnected sessions
        self.try_reconnect(ctx);

        // Phase-specific work
        if phase == Phase::Prefill && !self.prefill_done {
            self.drive_prefill_requests(ctx, now);
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

/// Record metrics for a completed request result.
fn record_metrics(result: &RequestResult) {
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
    let _ = metrics::RESPONSE_LATENCY.increment(result.latency_ns);
    match result.request_type {
        RequestType::Get => {
            metrics::GET_COUNT.increment();
            let _ = metrics::GET_LATENCY.increment(result.latency_ns);
            if let Some(ttfb) = result.ttfb_ns {
                let _ = metrics::GET_TTFB.increment(ttfb);
            }
        }
        RequestType::Set => {
            metrics::SET_COUNT.increment();
            let _ = metrics::SET_LATENCY.increment(result.latency_ns);
        }
        RequestType::Delete => {
            metrics::DELETE_COUNT.increment();
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
