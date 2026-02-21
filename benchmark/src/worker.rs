//! Worker implementation using ringline AsyncEventHandler.
//!
//! Each worker runs inside a ringline event loop. On startup, `on_start()` spawns
//! one async task per connection. Each task independently connects, drives
//! requests, parses responses, and reconnects on failure. `on_tick()` handles
//! phase transitions, diagnostics, and shutdown.

use crate::client::{MomentoConn, MomentoSetup, RecvBuf, RequestResult, RequestType, Session};
use crate::config::{Config, Protocol as CacheProtocol};
use crate::metrics;
use crate::ratelimit::DynamicRateLimiter;

use ringline::{AsyncEventHandler, ConnCtx, DriverCtx, GuardBox, ParseResult, RegionId, SendGuard};

use rand::prelude::*;
use rand_xoshiro::Xoshiro256PlusPlus;
use std::any::Any;
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

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
    /// Cluster mode: slot → endpoint index (16384 entries). None = ketama routing.
    pub slot_table: Option<Vec<u16>>,
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
/// When used with `conn.send_parts().build()`, the kernel sends directly from
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

// ── Shared task state (Arc-wrapped, accessed by all connection tasks) ────

/// State shared across all connection tasks spawned by a single worker.
struct TaskSharedState {
    config: Config,
    shared: Arc<SharedState>,
    ratelimiter: Option<Arc<DynamicRateLimiter>>,
    value_pool: Arc<Vec<u8>>,
    endpoints: Vec<SocketAddr>,
    ring: ketama::Ring,
    slot_table: Mutex<Option<Vec<u16>>>,
    /// Shared prefill queue: tasks pop key IDs from here.
    prefill_queue: Mutex<VecDeque<usize>>,
    /// Worker ID for logging.
    worker_id: usize,
    /// Whether backfill_on_miss is enabled.
    backfill_on_miss: bool,
    /// Momento setup (resolved once, shared across tasks).
    momento_setup: Mutex<Option<MomentoSetup>>,
}

/// State shared between the BenchHandler (on_tick) and connection tasks.
/// Wrapped in Arc so on_tick and spawned tasks can both access it.
struct SharedWorkerState {
    task_state: Arc<TaskSharedState>,
    /// Prefill tracking: total keys assigned to this worker.
    prefill_total: usize,
    /// Whether prefill is already complete for this worker.
    prefill_done: AtomicU8, // 0 = not done, 1 = done
}

impl SharedWorkerState {
    fn is_prefill_done(&self) -> bool {
        self.prefill_done.load(Ordering::Acquire) != 0
    }
}

// ── BenchHandler ─────────────────────────────────────────────────────────

/// Benchmark worker async event handler for ringline.
pub struct BenchHandler {
    id: usize,
    shared: Arc<SharedState>,
    worker_state: Arc<SharedWorkerState>,

    /// Last observed phase (for transition detection)
    last_phase: Phase,
    /// Whether to record metrics (only true during Running phase)
    recording: bool,

    /// Tick counter for periodic diagnostics
    tick_count: u64,
    /// Last diagnostic log time
    last_diag: Instant,

    /// Number of connections this worker manages
    my_connections: usize,
}

impl AsyncEventHandler for BenchHandler {
    fn on_accept(&self, _conn: ConnCtx) -> impl Future<Output = ()> + 'static {
        // Benchmark is client-only, no accepts expected
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let worker_state = Arc::clone(&self.worker_state);
        let my_connections = self.my_connections;
        let worker_id = self.id;
        let is_momento =
            worker_state.task_state.config.target.protocol == CacheProtocol::Momento;

        Some(Box::pin(async move {
            if is_momento {
                spawn_momento_tasks(&worker_state, my_connections, worker_id);
            } else {
                spawn_session_tasks(&worker_state, my_connections, worker_id);
            }
        }))
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx<'_>) {
        let phase = self.shared.phase();

        // Update recording state on phase transition
        if phase != self.last_phase {
            if phase == Phase::Running {
                tracing::debug!(
                    worker = self.id,
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

        // Periodic diagnostic heartbeat (every 2 seconds)
        self.tick_count += 1;
        if self.last_diag.elapsed() >= Duration::from_secs(2) {
            tracing::trace!(
                worker = self.id,
                phase = ?phase,
                recording = self.recording,
                ticks = self.tick_count,
                connections = self.my_connections,
                "diagnostic heartbeat"
            );
            self.tick_count = 0;
            self.last_diag = Instant::now();
        }
    }

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

        // Initialize prefill state
        let (prefill_queue, prefill_total, prefill_done) = match cfg.prefill_range {
            Some(range) => {
                let pending: VecDeque<usize> = (range.start..range.end).collect();
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
                (VecDeque::new(), 0, true)
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

        let backfill_on_miss = cfg.config.workload.backfill_on_miss;
        let slot_table = cfg.slot_table;

        // Build ketama consistent hash ring from endpoint addresses
        let server_ids: Vec<String> = endpoints.iter().map(|a| a.to_string()).collect();
        let ring = ketama::Ring::build(&server_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        let task_state = Arc::new(TaskSharedState {
            config: cfg.config.clone(),
            shared: Arc::clone(&cfg.shared),
            ratelimiter: cfg.ratelimiter.clone(),
            value_pool: cfg.value_pool,
            endpoints,
            ring,
            slot_table: Mutex::new(slot_table),
            prefill_queue: Mutex::new(prefill_queue),
            worker_id: cfg.id,
            backfill_on_miss,
            momento_setup: Mutex::new(None),
        });

        let worker_state = Arc::new(SharedWorkerState {
            task_state,
            prefill_total,
            prefill_done: AtomicU8::new(if prefill_done { 1 } else { 0 }),
        });

        let result = BenchHandler {
            id: cfg.id,
            shared: cfg.shared,
            worker_state,
            last_phase: Phase::Connect,
            recording: cfg.recording,
            tick_count: 0,
            last_diag: Instant::now(),
            my_connections,
        };
        tracing::debug!(
            worker_id = result.id,
            my_connections = result.my_connections,
            "worker create_for_worker complete, entering event loop"
        );
        result
    }
}

// ── Spawn helpers ────────────────────────────────────────────────────────

/// Spawn one async task per regular (RESP/Memcache) connection.
fn spawn_session_tasks(
    worker_state: &Arc<SharedWorkerState>,
    my_connections: usize,
    worker_id: usize,
) {
    let num_endpoints = worker_state.task_state.endpoints.len();
    let total_connections = worker_state.task_state.config.connection.total_connections();
    let num_threads = worker_state.task_state.config.general.threads;

    let base_per_thread = total_connections / num_threads;
    let remainder = total_connections % num_threads;
    let my_start = if worker_id < remainder {
        worker_id * (base_per_thread + 1)
    } else {
        remainder * (base_per_thread + 1) + (worker_id - remainder) * base_per_thread
    };

    for i in 0..my_connections {
        let global_conn_idx = my_start + i;
        let endpoint_idx = global_conn_idx % num_endpoints;
        let state = Arc::clone(worker_state);
        let session_seed = 42 + worker_id as u64 * 10000 + i as u64;

        let _ = ringline::spawn(async move {
            session_connection_task(state, endpoint_idx, session_seed).await;
        });
    }
}

/// Spawn one async task per Momento TLS connection.
fn spawn_momento_tasks(
    worker_state: &Arc<SharedWorkerState>,
    my_connections: usize,
    worker_id: usize,
) {
    // Resolve Momento setup once
    {
        let mut setup_guard = worker_state.task_state.momento_setup.lock().unwrap();
        if setup_guard.is_none() {
            match MomentoSetup::from_config(&worker_state.task_state.config) {
                Ok(setup) => *setup_guard = Some(setup),
                Err(e) => {
                    tracing::error!("failed to resolve Momento config: {}", e);
                    metrics::CONNECTIONS_FAILED.increment();
                    return;
                }
            }
        }
    }

    for i in 0..my_connections {
        let state = Arc::clone(worker_state);
        let session_seed = 42 + worker_id as u64 * 10000 + i as u64;
        let conn_idx = i;

        let _ = ringline::spawn(async move {
            momento_connection_task(state, conn_idx, session_seed).await;
        });
    }
}

// ── Session connection task (RESP/Memcache) ──────────────────────────────

/// A single connection task that connects, drives workload, and reconnects.
async fn session_connection_task(
    state: Arc<SharedWorkerState>,
    endpoint_idx: usize,
    seed: u64,
) {
    let endpoint = state.task_state.endpoints[endpoint_idx];
    let config = &state.task_state.config;

    let mut session = match Session::from_config(endpoint, config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                worker = state.task_state.worker_id,
                endpoint = %endpoint,
                "failed to create session: {}",
                e
            );
            return;
        }
    };

    let mut rng = Xoshiro256PlusPlus::seed_from_u64(seed);
    let mut key_buf = vec![0u8; config.workload.keyspace.length];
    let mut set_prefix_buf = Vec::with_capacity(256);
    let mut results = Vec::with_capacity(config.connection.pipeline_depth);
    let mut backfill_queue: Vec<usize> = Vec::new();
    let mut prefill_in_flight: VecDeque<usize> = VecDeque::new();
    let mut prefill_confirmed: usize = 0;

    loop {
        let phase = state.task_state.shared.phase();
        if phase.should_stop() {
            return;
        }

        // Connect
        let conn = match ringline::connect(endpoint) {
            Ok(future) => match future.await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::debug!(
                        worker = state.task_state.worker_id,
                        endpoint = %endpoint,
                        "connect failed: {}",
                        e
                    );
                    metrics::CONNECTIONS_FAILED.increment();
                    metrics::DISCONNECTS_CONNECT_FAILED.increment();
                    ringline::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            Err(e) => {
                tracing::warn!(
                    worker = state.task_state.worker_id,
                    endpoint = %endpoint,
                    "connect initiation failed: {}",
                    e
                );
                metrics::CONNECTIONS_FAILED.increment();
                ringline::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        session.set_conn_id(conn.index());
        session.reconnect_attempted(true);
        metrics::CONNECTIONS_ACTIVE.increment();

        tracing::debug!(
            worker = state.task_state.worker_id,
            endpoint = %endpoint,
            conn_index = conn.index(),
            "connected"
        );

        // Drive workload on this connection
        let result = drive_session(
            &conn,
            &mut session,
            &state,
            endpoint_idx,
            &mut rng,
            &mut key_buf,
            &mut set_prefix_buf,
            &mut results,
            &mut backfill_queue,
            &mut prefill_in_flight,
            &mut prefill_confirmed,
        )
        .await;

        // Disconnected
        session.disconnect();
        metrics::CONNECTIONS_ACTIVE.decrement();

        // Move in-flight prefill keys back to the shared queue
        if !state.is_prefill_done() && !prefill_in_flight.is_empty() {
            let mut queue = state.task_state.prefill_queue.lock().unwrap();
            while let Some(key_id) = prefill_in_flight.pop_front() {
                queue.push_back(key_id);
            }
        }

        match result {
            Ok(()) => return, // Clean shutdown (phase == Stop)
            Err(reason) => {
                metrics::CONNECTIONS_FAILED.increment();
                match reason {
                    DisconnectReason::Eof => metrics::DISCONNECTS_EOF.increment(),
                    DisconnectReason::RecvError => metrics::DISCONNECTS_RECV_ERROR.increment(),
                    DisconnectReason::SendError => metrics::DISCONNECTS_SEND_ERROR.increment(),
                    DisconnectReason::ClosedEvent => metrics::DISCONNECTS_CLOSED_EVENT.increment(),
                    DisconnectReason::ErrorEvent => metrics::DISCONNECTS_ERROR_EVENT.increment(),
                    DisconnectReason::ConnectFailed => {
                        metrics::DISCONNECTS_CONNECT_FAILED.increment()
                    }
                }
            }
        }

        session.reset();
        ringline::sleep(Duration::from_millis(100)).await;
    }
}

/// Drive the workload on a connected session. Returns Ok(()) for clean shutdown,
/// Err(reason) for disconnect.
async fn drive_session(
    conn: &ConnCtx,
    session: &mut Session,
    state: &Arc<SharedWorkerState>,
    endpoint_idx: usize,
    rng: &mut Xoshiro256PlusPlus,
    key_buf: &mut Vec<u8>,
    set_prefix_buf: &mut Vec<u8>,
    results: &mut Vec<RequestResult>,
    backfill_queue: &mut Vec<usize>,
    prefill_in_flight: &mut VecDeque<usize>,
    prefill_confirmed: &mut usize,
) -> Result<(), DisconnectReason> {
    let config = &state.task_state.config;
    let key_count = config.workload.keyspace.count;
    let get_ratio = config.workload.commands.get as usize;
    let delete_ratio = config.workload.commands.delete as usize;
    let value_len = config.workload.values.length;
    let pool_len = state.task_state.value_pool.len();
    let backfill_on_miss = state.task_state.backfill_on_miss;

    loop {
        let phase = state.task_state.shared.phase();
        if phase.should_stop() {
            return Ok(());
        }

        let recording = phase.is_recording();

        // Phase-specific request driving
        if phase == Phase::Prefill && !state.is_prefill_done() {
            drive_prefill_requests(
                conn,
                session,
                state,
                rng,
                key_buf,
                set_prefix_buf,
                prefill_in_flight,
                value_len,
                pool_len,
            )?;
        } else if phase == Phase::Warmup || phase == Phase::Running {
            drive_normal_requests(
                conn,
                session,
                state,
                endpoint_idx,
                rng,
                key_buf,
                set_prefix_buf,
                backfill_queue,
                key_count,
                get_ratio,
                delete_ratio,
                value_len,
                pool_len,
            )?;
        }

        // Flush any pending send data
        flush_session(conn, session)?;

        // Wait for response data
        let consumed = conn.with_data(|data| {
            if data.is_empty() {
                return ParseResult::Consumed(0);
            }

            let now = Instant::now();

            // Stamp TTFB on the first unstamped in-flight request
            session.stamp_first_byte(now);

            // Zero-copy path: parse responses directly from ringline's buffer
            results.clear();
            let mut buf = DataSlice::new(data);
            if let Err(e) = session.poll_responses_from(&mut buf, results, now) {
                tracing::debug!(
                    consumed = buf.consumed,
                    remaining_len = data.len() - buf.consumed,
                    in_flight = session.in_flight_count(),
                    "protocol error: {}",
                    e,
                );
            }

            ParseResult::Consumed(buf.consumed)
        }).await;

        if consumed == 0 {
            // Connection closed (EOF)
            return Err(DisconnectReason::Eof);
        }

        metrics::BYTES_RX.add(consumed as u64);

        // Handle prefill tracking
        if phase == Phase::Prefill && !state.is_prefill_done() {
            handle_prefill_results(results, state, prefill_in_flight, prefill_confirmed);
        }

        // Record metrics (counters always, latencies only when recording)
        for result in results.iter() {
            record_counters(result);
        }
        if recording {
            for result in results.iter() {
                record_latencies(result);
            }
        }

        // Queue backfill SETs for GET misses (backfill_on_miss)
        if backfill_on_miss {
            for result in results.iter() {
                if result.request_type == RequestType::Get
                    && result.hit == Some(false)
                    && result.success
                    && let Some(key_id) = result.key_id
                {
                    backfill_queue.push(key_id);
                }
            }
        }

        // Handle cluster redirects (MOVED/ASK)
        handle_cluster_redirects(results, state);
    }
}

/// Drive normal workload requests (GET/SET/DELETE).
///
/// In multi-endpoint mode, generates random keys and only sends those that
/// route to this task's endpoint (via ketama or slot table). For single
/// endpoints, all keys are sent.
fn drive_normal_requests(
    conn: &ConnCtx,
    session: &mut Session,
    state: &Arc<SharedWorkerState>,
    endpoint_idx: usize,
    rng: &mut Xoshiro256PlusPlus,
    key_buf: &mut Vec<u8>,
    set_prefix_buf: &mut Vec<u8>,
    backfill_queue: &mut Vec<usize>,
    key_count: usize,
    get_ratio: usize,
    delete_ratio: usize,
    value_len: usize,
    pool_len: usize,
) -> Result<(), DisconnectReason> {
    let multi_endpoint = state.task_state.endpoints.len() > 1;

    // Drain backfill queue before normal requests
    if !backfill_queue.is_empty() {
        drain_backfill_queue(
            conn,
            session,
            state,
            rng,
            key_buf,
            set_prefix_buf,
            backfill_queue,
            value_len,
            pool_len,
        )?;
    }

    let now = Instant::now();
    let mut consecutive_skips = 0u32;

    loop {
        if !session.can_send() {
            break;
        }

        let key_id = rng.random_range(0..key_count);
        write_key(key_buf, key_id);

        // In multi-endpoint mode, skip keys that don't route to our endpoint
        if multi_endpoint {
            let routed = route_key(&state.task_state, key_buf);
            if routed != endpoint_idx {
                consecutive_skips += 1;
                // Avoid infinite loop if all keys route elsewhere
                if consecutive_skips > 100 {
                    break;
                }
                continue;
            }
            consecutive_skips = 0;
        }

        if let Some(ref rl) = state.task_state.ratelimiter
            && !rl.try_acquire()
        {
            break;
        }

        let roll = rng.random_range(0..100);
        if roll < get_ratio {
            let kid = if state.task_state.backfill_on_miss {
                Some(key_id)
            } else {
                None
            };
            if session.get(key_buf, now, kid).is_none() {
                break;
            }
        } else if roll < get_ratio + delete_ratio {
            if session.delete(key_buf, now).is_none() {
                break;
            }
        } else {
            // SET: flush pending GET/DELETE data first, then send via guard
            flush_session(conn, session)?;

            if let Some(suffix) = session.encode_set_prefix(set_prefix_buf, key_buf, value_len) {
                let prefix_len = set_prefix_buf.len();
                let suffix_len = suffix.len();
                if let Err(e) =
                    send_set_parts(conn, set_prefix_buf, rng, &state.task_state.value_pool, value_len, pool_len, suffix)
                {
                    if e.kind() != io::ErrorKind::Other {
                        return Err(DisconnectReason::SendError);
                    }
                    break;
                }
                session.confirm_set_sent(prefix_len, value_len, suffix_len, now);
            } else {
                break;
            }
        }

        metrics::REQUESTS_SENT.increment();
    }

    Ok(())
}

/// Drain the backfill queue by sending SET requests for keys that missed on GET.
fn drain_backfill_queue(
    conn: &ConnCtx,
    session: &mut Session,
    state: &Arc<SharedWorkerState>,
    rng: &mut Xoshiro256PlusPlus,
    key_buf: &mut Vec<u8>,
    set_prefix_buf: &mut Vec<u8>,
    backfill_queue: &mut Vec<usize>,
    value_len: usize,
    pool_len: usize,
) -> Result<(), DisconnectReason> {
    let now = Instant::now();
    let mut i = 0;
    while i < backfill_queue.len() {
        if !session.can_send() {
            break;
        }

        let key_id = backfill_queue[i];
        write_key(key_buf, key_id);

        if let Some(ref rl) = state.task_state.ratelimiter
            && !rl.try_acquire()
        {
            return Ok(());
        }

        backfill_queue.swap_remove(i);

        // Flush any pending GET/DELETE data first
        flush_session(conn, session)?;

        if let Some(suffix) = session.encode_set_prefix(set_prefix_buf, key_buf, value_len) {
            let prefix_len = set_prefix_buf.len();
            let suffix_len = suffix.len();
            if let Err(e) =
                send_set_parts(conn, set_prefix_buf, rng, &state.task_state.value_pool, value_len, pool_len, suffix)
            {
                backfill_queue.push(key_id);
                if e.kind() != io::ErrorKind::Other {
                    return Err(DisconnectReason::SendError);
                }
                continue;
            }
            session.confirm_set_sent(prefix_len, value_len, suffix_len, now);
            session.mark_last_request_backfill();
            metrics::REQUESTS_SENT.increment();
        } else {
            backfill_queue.push(key_id);
            i += 1;
        }
    }
    Ok(())
}

/// Maximum in-flight prefill requests per task.
const PREFILL_MAX_PIPELINE: usize = 16;

/// Drive prefill requests (sequential SET commands).
fn drive_prefill_requests(
    conn: &ConnCtx,
    session: &mut Session,
    state: &Arc<SharedWorkerState>,
    rng: &mut Xoshiro256PlusPlus,
    key_buf: &mut Vec<u8>,
    set_prefix_buf: &mut Vec<u8>,
    prefill_in_flight: &mut VecDeque<usize>,
    value_len: usize,
    pool_len: usize,
) -> Result<(), DisconnectReason> {
    let now = Instant::now();

    while prefill_in_flight.len() < PREFILL_MAX_PIPELINE && session.can_send() {
        // Pop a key from the shared prefill queue
        let key_id = {
            let mut queue = state.task_state.prefill_queue.lock().unwrap();
            match queue.pop_front() {
                Some(id) => id,
                None => break,
            }
        };

        write_key(key_buf, key_id);

        // Flush pending data first
        flush_session(conn, session)?;

        if let Some(suffix) = session.encode_set_prefix(set_prefix_buf, key_buf, value_len) {
            let prefix_len = set_prefix_buf.len();
            let suffix_len = suffix.len();
            if let Err(e) =
                send_set_parts(conn, set_prefix_buf, rng, &state.task_state.value_pool, value_len, pool_len, suffix)
            {
                // Put key back on failure
                let mut queue = state.task_state.prefill_queue.lock().unwrap();
                queue.push_front(key_id);
                if e.kind() != io::ErrorKind::Other {
                    return Err(DisconnectReason::SendError);
                }
                break;
            }
            session.confirm_set_sent(prefix_len, value_len, suffix_len, now);
            prefill_in_flight.push_back(key_id);
            metrics::REQUESTS_SENT.increment();
        } else {
            // Put key back if we can't encode
            let mut queue = state.task_state.prefill_queue.lock().unwrap();
            queue.push_front(key_id);
            break;
        }
    }

    Ok(())
}

/// Handle prefill response tracking.
fn handle_prefill_results(
    results: &[RequestResult],
    state: &Arc<SharedWorkerState>,
    prefill_in_flight: &mut VecDeque<usize>,
    prefill_confirmed: &mut usize,
) {
    if state.is_prefill_done() {
        return;
    }

    let mut confirmed_batch = 0usize;
    for result in results {
        if result.request_type == RequestType::Set
            && let Some(key_id) = prefill_in_flight.pop_front()
        {
            if result.success {
                *prefill_confirmed += 1;
                confirmed_batch += 1;
            } else {
                // Put failed key back
                let mut queue = state.task_state.prefill_queue.lock().unwrap();
                queue.push_back(key_id);
            }
        }
    }

    if confirmed_batch > 0 {
        let new_total = state
            .task_state
            .shared
            .prefill_keys_confirmed
            .fetch_add(confirmed_batch, Ordering::AcqRel)
            + confirmed_batch;

        if new_total >= state.prefill_total && state.prefill_total > 0 {
            // Use CAS to ensure only one task marks completion
            if state
                .prefill_done
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                tracing::debug!(
                    worker_id = state.task_state.worker_id,
                    confirmed = new_total,
                    total = state.prefill_total,
                    "prefill complete"
                );
                state.task_state.shared.mark_prefill_complete();
            }
        }
    }
}

/// Handle cluster redirects (MOVED/ASK) from response results.
fn handle_cluster_redirects(results: &[RequestResult], state: &Arc<SharedWorkerState>) {
    let mut slot_table = state.task_state.slot_table.lock().unwrap();
    if slot_table.is_none() {
        return;
    }

    for result in results {
        if let Some(ref redirect) = result.redirect {
            metrics::CLUSTER_REDIRECTS.increment();
            if redirect.kind == resp_proto::RedirectKind::Moved {
                if let Ok(addr) = redirect.address.parse::<SocketAddr>() {
                    // Find existing endpoint or note that we need to add it
                    let endpoint_idx = state.task_state.endpoints.iter().position(|a| *a == addr);
                    if let Some(idx) = endpoint_idx {
                        slot_table.as_mut().unwrap()[redirect.slot as usize] = idx as u16;
                    }
                    // Note: we cannot dynamically add endpoints to the shared
                    // Vec from a task. In the async model, new endpoints require
                    // a slot table update from the orchestrator. For now, log
                    // if we see a redirect to an unknown endpoint.
                    if endpoint_idx.is_none() {
                        tracing::warn!(
                            worker = state.task_state.worker_id,
                            addr = %addr,
                            slot = redirect.slot,
                            "MOVED redirect to unknown endpoint (cannot add dynamically)"
                        );
                    }
                }
            }
            // ASK: count but don't update slot table (migration in progress)
        }
    }
}

/// Route a key to an endpoint index.
fn route_key(state: &TaskSharedState, key: &[u8]) -> usize {
    let slot_table = state.slot_table.lock().unwrap();
    if let Some(ref table) = *slot_table {
        let slot = resp_proto::hash_slot(key);
        table[slot as usize] as usize
    } else if state.endpoints.len() == 1 {
        0
    } else {
        state.ring.route(key)
    }
}

/// Flush a session's send buffer via the connection.
fn flush_session(conn: &ConnCtx, session: &mut Session) -> Result<(), DisconnectReason> {
    let send_buf = session.send_buffer();
    if send_buf.is_empty() {
        return Ok(());
    }

    let n = send_buf.len();
    match conn.send_nowait(&send_buf[..n]) {
        Ok(()) => {
            session.bytes_sent(n);
            Ok(())
        }
        Err(e) => {
            if e.kind() != io::ErrorKind::Other {
                tracing::debug!(error = %e, "send error (closing)");
                Err(DisconnectReason::SendError)
            } else {
                tracing::debug!(error = %e, "send pool exhausted");
                Ok(())
            }
        }
    }
}

/// Submit a guard-based SET send via send_parts().
fn send_set_parts(
    conn: &ConnCtx,
    set_prefix_buf: &[u8],
    rng: &mut Xoshiro256PlusPlus,
    value_pool: &Arc<Vec<u8>>,
    value_len: usize,
    pool_len: usize,
    suffix: &'static [u8],
) -> io::Result<()> {
    // Pick a random offset into the value pool
    let max_offset = pool_len - value_len;
    let offset = rng.random_range(0..=max_offset);

    let guard = ValuePoolGuard {
        pool: Arc::clone(value_pool),
        offset: offset as u32,
        len: value_len as u32,
    };

    let prefix_copy = set_prefix_buf.to_vec();
    let suffix_copy = if !suffix.is_empty() {
        Some(suffix.to_vec())
    } else {
        None
    };

    conn.send_parts().build(move |b| {
        let mut builder = b.copy(&prefix_copy);
        builder = builder.guard(GuardBox::new(guard));
        if let Some(ref s) = suffix_copy {
            builder = builder.copy(s);
        }
        builder.submit()
    })
}

// ── Momento connection task ──────────────────────────────────────────────

/// A single Momento TLS connection task.
async fn momento_connection_task(
    state: Arc<SharedWorkerState>,
    conn_idx: usize,
    seed: u64,
) {
    let config = &state.task_state.config;
    let (addr, tls_host) = {
        let setup_guard = state.task_state.momento_setup.lock().unwrap();
        let setup = setup_guard.as_ref().unwrap();
        let num_addrs = setup.addresses.len();
        let addr = setup.addresses[conn_idx % num_addrs];
        let host = setup.tls_host.clone();
        (addr, host)
    };

    let credential = {
        let setup_guard = state.task_state.momento_setup.lock().unwrap();
        let setup = setup_guard.as_ref().unwrap();
        setup.credential.clone()
    };

    let mut momento_conn = MomentoConn::new(&credential, config);
    let mut rng = Xoshiro256PlusPlus::seed_from_u64(seed);
    let mut key_buf = vec![0u8; config.workload.keyspace.length];
    let mut value_buf = vec![0u8; config.workload.values.length];
    let mut results = Vec::with_capacity(config.connection.pipeline_depth);

    // Fill value buffer with random data
    let mut init_rng = Xoshiro256PlusPlus::seed_from_u64(42);
    init_rng.fill_bytes(&mut value_buf);

    loop {
        let phase = state.task_state.shared.phase();
        if phase.should_stop() {
            return;
        }

        // Connect via TLS
        let conn = match ringline::connect_tls(addr, &tls_host) {
            Ok(future) => match future.await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::debug!(
                        worker = state.task_state.worker_id,
                        addr = %addr,
                        "Momento connect failed: {}",
                        e
                    );
                    metrics::CONNECTIONS_FAILED.increment();
                    ringline::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            Err(e) => {
                tracing::warn!(
                    worker = state.task_state.worker_id,
                    addr = %addr,
                    "Momento connect initiation failed: {}",
                    e
                );
                metrics::CONNECTIONS_FAILED.increment();
                ringline::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        metrics::CONNECTIONS_ACTIVE.increment();

        // Initialize HTTP/2 preface
        if let Err(e) = momento_conn.on_transport_ready() {
            tracing::warn!(
                worker = state.task_state.worker_id,
                "Momento transport ready failed: {}",
                e
            );
            conn.close();
            metrics::CONNECTIONS_ACTIVE.decrement();
            metrics::CONNECTIONS_FAILED.increment();
            ringline::sleep(Duration::from_millis(100)).await;
            continue;
        }

        // Flush the HTTP/2 connection preface
        flush_momento(&conn, &mut momento_conn);

        tracing::debug!(
            worker = state.task_state.worker_id,
            addr = %addr,
            conn_index = conn.index(),
            "Momento connected"
        );

        // Drive workload
        let result = drive_momento_session(
            &conn,
            &mut momento_conn,
            &state,
            &mut rng,
            &mut key_buf,
            &mut value_buf,
            &mut results,
        )
        .await;

        metrics::CONNECTIONS_ACTIVE.decrement();

        match result {
            Ok(()) => return, // Clean shutdown
            Err(_) => {
                metrics::CONNECTIONS_FAILED.increment();
                metrics::DISCONNECTS_CLOSED_EVENT.increment();
            }
        }

        ringline::sleep(Duration::from_millis(100)).await;
    }
}

/// Drive the Momento workload on a connected session.
async fn drive_momento_session(
    conn: &ConnCtx,
    momento_conn: &mut MomentoConn,
    state: &Arc<SharedWorkerState>,
    rng: &mut Xoshiro256PlusPlus,
    key_buf: &mut Vec<u8>,
    value_buf: &mut Vec<u8>,
    results: &mut Vec<RequestResult>,
) -> Result<(), DisconnectReason> {
    let config = &state.task_state.config;
    let key_count = config.workload.keyspace.count;
    let get_ratio = config.workload.commands.get as usize;
    let delete_ratio = config.workload.commands.delete as usize;

    loop {
        let phase = state.task_state.shared.phase();
        if phase.should_stop() {
            return Ok(());
        }

        let recording = phase.is_recording();

        // Drive requests
        if phase == Phase::Prefill && !state.is_prefill_done() {
            drive_momento_prefill(conn, momento_conn, state, rng, key_buf, value_buf);
        } else if phase == Phase::Warmup || phase == Phase::Running {
            drive_momento_requests(
                conn, momento_conn, state, rng, key_buf, value_buf, key_count, get_ratio,
                delete_ratio,
            );
        }

        // Wait for response data
        let consumed = conn.with_data(|data| {
            if data.is_empty() {
                return ParseResult::Consumed(0);
            }

            // Feed data into HTTP/2 framing layer
            if let Err(e) = momento_conn.feed_data(data) {
                tracing::debug!(error = %e, "Momento feed_data error");
                return ParseResult::Consumed(data.len());
            }

            ParseResult::Consumed(data.len())
        }).await;

        if consumed == 0 {
            return Err(DisconnectReason::Eof);
        }

        metrics::BYTES_RX.add(consumed as u64);

        // Poll for completed operations
        let now = Instant::now();
        results.clear();
        momento_conn.poll_responses(results, now);

        // Handle prefill tracking
        if phase == Phase::Prefill && !state.is_prefill_done() {
            handle_momento_prefill_results(results, state);
        }

        // Record metrics
        for result in results.iter() {
            record_counters(result);
        }
        if recording {
            for result in results.iter() {
                record_latencies(result);
            }
        }

        // Flush any pending sends generated by processing responses
        flush_momento(conn, momento_conn);
    }
}

/// Drive Momento workload requests.
fn drive_momento_requests(
    conn: &ConnCtx,
    momento_conn: &mut MomentoConn,
    state: &Arc<SharedWorkerState>,
    rng: &mut Xoshiro256PlusPlus,
    key_buf: &mut Vec<u8>,
    value_buf: &mut Vec<u8>,
    key_count: usize,
    get_ratio: usize,
    delete_ratio: usize,
) {
    let now = Instant::now();

    while momento_conn.can_send() {
        if let Some(ref rl) = state.task_state.ratelimiter
            && !rl.try_acquire()
        {
            break;
        }

        let key_id = rng.random_range(0..key_count);
        write_key(key_buf, key_id);

        let roll = rng.random_range(0..100);
        let sent = if roll < get_ratio {
            momento_conn.get(key_buf, now).is_some()
        } else if roll < get_ratio + delete_ratio {
            momento_conn.delete(key_buf, now).is_some()
        } else {
            rng.fill_bytes(value_buf);
            momento_conn.set(key_buf, value_buf, now).is_some()
        };

        if sent {
            metrics::REQUESTS_SENT.increment();
        } else {
            break;
        }
    }

    flush_momento(conn, momento_conn);
}

/// Drive Momento prefill requests.
fn drive_momento_prefill(
    conn: &ConnCtx,
    momento_conn: &mut MomentoConn,
    state: &Arc<SharedWorkerState>,
    rng: &mut Xoshiro256PlusPlus,
    key_buf: &mut Vec<u8>,
    value_buf: &mut Vec<u8>,
) {
    while momento_conn.can_send() {
        let key_id = {
            let mut queue = state.task_state.prefill_queue.lock().unwrap();
            match queue.pop_front() {
                Some(id) => id,
                None => break,
            }
        };

        write_key(key_buf, key_id);
        rng.fill_bytes(value_buf);

        if momento_conn.set(key_buf, value_buf, Instant::now()).is_none() {
            let mut queue = state.task_state.prefill_queue.lock().unwrap();
            queue.push_front(key_id);
            break;
        }
        metrics::REQUESTS_SENT.increment();
    }

    flush_momento(conn, momento_conn);
}

/// Flush pending send data for a Momento connection.
fn flush_momento(conn: &ConnCtx, momento_conn: &mut MomentoConn) {
    let pending = momento_conn.pending_send();
    if pending.is_empty() {
        return;
    }

    let n = pending.len();
    match conn.send_nowait(pending) {
        Ok(()) => {
            momento_conn.advance_send(n);
        }
        Err(e) => {
            tracing::debug!(error = %e, "momento send error");
        }
    }
}

/// Handle Momento prefill response tracking.
fn handle_momento_prefill_results(results: &[RequestResult], state: &Arc<SharedWorkerState>) {
    if state.is_prefill_done() {
        return;
    }

    let mut confirmed_batch = 0usize;
    for result in results {
        if result.request_type == RequestType::Set && result.success {
            confirmed_batch += 1;
        }
    }

    if confirmed_batch > 0 {
        let new_total = state
            .task_state
            .shared
            .prefill_keys_confirmed
            .fetch_add(confirmed_batch, Ordering::AcqRel)
            + confirmed_batch;

        if new_total >= state.prefill_total && state.prefill_total > 0 {
            if state
                .prefill_done
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                tracing::debug!(
                    worker_id = state.task_state.worker_id,
                    confirmed = new_total,
                    total = state.prefill_total,
                    "prefill complete (momento)"
                );
                state.task_state.shared.mark_prefill_complete();
            }
        }
    }
}

// ── Utility functions ────────────────────────────────────────────────────

/// Record counter metrics for a completed request result (always called).
fn record_counters(result: &RequestResult) {
    metrics::RESPONSES_RECEIVED.increment();
    if result.redirect.is_some() {
        // Redirects are counted via CLUSTER_REDIRECTS in handle_cluster_redirects, not as errors
    } else if result.is_error_response {
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
