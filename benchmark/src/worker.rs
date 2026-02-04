//! Worker implementation using IoDriver abstraction.
//!
//! This worker uses the IoDriver trait to abstract over mio and io_uring,
//! enabling the use of advanced io_uring features like multishot recv and
//! registered file descriptors for lower latency.

use crate::client::{MomentoSession, RequestResult, RequestType, Session};
use crate::config::{Config, Protocol as CacheProtocol};
use crate::metrics;
use crate::ratelimit::DynamicRateLimiter;

use io_driver::{CompletionKind, ConnId, Driver, IoDriver, RecvBuf};

use rand::prelude::*;
use rand_xoshiro::Xoshiro256PlusPlus;
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

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
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            phase: AtomicU8::new(Phase::Connect as u8),
            prefill_complete: AtomicUsize::new(0),
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
}

impl Default for SharedState {
    fn default() -> Self {
        Self::new()
    }
}

/// Worker configuration.
pub struct IoWorkerConfig {
    pub id: usize,
    pub config: Config,
    pub shared: Arc<SharedState>,
    pub ratelimiter: Option<Arc<DynamicRateLimiter>>,
    pub warmup: bool,
    /// Range of key IDs this worker should prefill (start..end).
    /// Only used when prefill is enabled.
    pub prefill_range: Option<std::ops::Range<usize>>,
}

/// A worker using IoDriver for I/O operations.
pub struct IoWorker {
    id: usize,
    driver: Box<dyn IoDriver>,
    config: Config,
    shared: Arc<SharedState>,

    /// Sessions (stable storage)
    sessions: Vec<Session>,

    /// Maps conn_id -> session index (for completion lookup)
    conn_id_to_idx: HashMap<usize, usize>,

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

    /// Whether we're in warmup mode (don't record metrics)
    warmup: bool,

    /// Prefill state: keys pending to be written (not yet sent or need retry)
    prefill_pending: std::collections::VecDeque<usize>,
    /// Prefill state: in-flight key IDs per session index (waiting for response)
    prefill_in_flight: Vec<std::collections::VecDeque<usize>>,
    /// Number of keys successfully confirmed during prefill
    prefill_confirmed: usize,
    /// Total keys to prefill (for completion check)
    prefill_total: usize,
    /// Whether this worker has finished prefilling
    prefill_done: bool,
}

impl IoWorker {
    /// Create a new worker.
    pub fn new(cfg: IoWorkerConfig) -> io::Result<Self> {
        // Use io-driver defaults for buffer sizing (16KB * 2048 buffers, sq_depth 1024)
        let driver = Driver::builder()
            .engine(cfg.config.general.io_engine)
            .build()?;

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
                (pending, total, total == 0)
            }
            None => (std::collections::VecDeque::new(), 0, true),
        };

        Ok(Self {
            id: cfg.id,
            driver,
            config: cfg.config,
            shared: cfg.shared,
            sessions: Vec::new(),
            conn_id_to_idx: HashMap::new(),
            momento_sessions: Vec::new(),
            rng,
            key_buf,
            value_buf,
            results: Vec::with_capacity(pipeline_depth),
            ratelimiter: cfg.ratelimiter,
            warmup: cfg.warmup,
            prefill_pending,
            prefill_in_flight: Vec::new(), // Will be sized when sessions are created
            prefill_confirmed: 0,
            prefill_total,
            prefill_done,
        })
    }

    /// Connect to all endpoints.
    pub fn connect(&mut self) -> io::Result<()> {
        // Handle Momento differently - it manages its own connections
        if self.config.target.protocol == CacheProtocol::Momento {
            return self.connect_momento();
        }

        let endpoints = self.config.target.endpoints.clone();
        let total_connections = self.config.connection.total_connections();
        let num_threads = self.config.general.threads;
        let num_endpoints = endpoints.len();

        // Distribute connections across threads
        let base_per_thread = total_connections / num_threads;
        let remainder = total_connections % num_threads;
        let my_connections = if self.id < remainder {
            base_per_thread + 1
        } else {
            base_per_thread
        };

        let my_start = if self.id < remainder {
            self.id * (base_per_thread + 1)
        } else {
            remainder * (base_per_thread + 1) + (self.id - remainder) * base_per_thread
        };

        for i in 0..my_connections {
            let global_conn_idx = my_start + i;
            let endpoint_idx = global_conn_idx % num_endpoints;
            let endpoint = endpoints[endpoint_idx];

            match self.connect_one(endpoint) {
                Ok(()) => {
                    metrics::CONNECTIONS_ACTIVE.increment();
                }
                Err(e) => {
                    tracing::warn!(
                        "worker {} failed to connect to {}: {}",
                        self.id,
                        endpoint,
                        e
                    );
                    metrics::CONNECTIONS_FAILED.increment();
                    // Create disconnected session for reconnection attempts
                    match Session::from_config(endpoint, &self.config) {
                        Ok(session) => {
                            self.sessions.push(session);
                            self.prefill_in_flight
                                .push(std::collections::VecDeque::new());
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
                    // Not added to send_queue - will be added on successful reconnect
                }
            }
        }

        Ok(())
    }

    /// Connect Momento sessions.
    fn connect_momento(&mut self) -> io::Result<()> {
        let total_connections = self.config.connection.total_connections();
        let num_threads = self.config.general.threads;

        // Distribute connections across threads
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
                    // Add session even if connect failed - we'll retry in poll_once
                    self.momento_sessions.push(session);
                }
                Err(e) => {
                    tracing::error!("failed to create Momento session: {}", e);
                    metrics::CONNECTIONS_FAILED.increment();
                }
            }
        }

        Ok(())
    }

    fn connect_one(&mut self, addr: SocketAddr) -> io::Result<()> {
        let stream = Self::create_connection(addr, self.config.connection.connect_timeout)?;
        let conn_id = self.driver.register(stream)?;

        let mut session = Session::from_config(addr, &self.config)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        session.set_conn_id(conn_id);

        let idx = self.sessions.len();
        self.sessions.push(session);
        self.prefill_in_flight
            .push(std::collections::VecDeque::new());
        self.conn_id_to_idx.insert(conn_id.as_usize(), idx);

        Ok(())
    }

    fn create_connection(addr: SocketAddr, _timeout: Duration) -> io::Result<TcpStream> {
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };

        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_nonblocking(true)?;
        socket.set_nodelay(true)?;

        // Connect (non-blocking)
        match socket.connect(&addr.into()) {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
            Err(e) => return Err(e),
        }

        Ok(socket.into())
    }

    /// Set warmup mode.
    pub fn set_warmup(&mut self, warmup: bool) {
        self.warmup = warmup;
    }

    /// Run a single iteration of the event loop.
    #[inline]
    pub fn poll_once(&mut self) -> io::Result<()> {
        // Get timestamp once for the entire poll cycle
        let now = std::time::Instant::now();

        // Handle Momento sessions differently
        if self.config.target.protocol == CacheProtocol::Momento {
            return self.poll_once_momento(now);
        }

        // Try to reconnect disconnected sessions
        self.try_reconnect();

        // Generate and queue requests
        self.drive_requests(now)?;

        // Flush send buffers
        self.flush_sends()?;

        // Poll for I/O completions
        let count = self.driver.poll(Some(Duration::from_micros(100)))?;

        if count > 0 {
            // Process completions
            self.process_completions()?;
        }

        // Process any received data into responses
        // Use a fresh timestamp to accurately measure latency (not the one used for sending)
        self.poll_responses(std::time::Instant::now());

        Ok(())
    }

    /// Poll loop for Momento sessions.
    #[inline]
    fn poll_once_momento(&mut self, now: std::time::Instant) -> io::Result<()> {
        let key_count = self.config.workload.keyspace.count;
        let get_ratio = self.config.workload.commands.get;
        let delete_ratio = self.config.workload.commands.delete;
        let warmup = self.warmup;

        for session in &mut self.momento_sessions {
            // Drive connection (TLS handshake, HTTP/2 setup)
            if !session.is_connected() {
                match session.drive() {
                    Ok(true) => {
                        metrics::CONNECTIONS_ACTIVE.increment();
                    }
                    Ok(false) => continue, // Still connecting
                    Err(e) => {
                        tracing::debug!("Momento connection error: {}", e);
                        continue;
                    }
                }
            }

            // Generate and send requests
            while session.can_send() {
                // Check rate limiter
                if let Some(ref rl) = self.ratelimiter
                    && !rl.try_acquire()
                {
                    break;
                }

                let key_id = self.rng.random_range(0..key_count);
                write_key(&mut self.key_buf, key_id);

                // Command selection: GET if < get_ratio, DELETE if < get_ratio + delete_ratio, else SET
                let roll = self.rng.random_range(0..100);
                let sent = if roll < get_ratio {
                    session.get(&self.key_buf, now).is_some()
                } else if roll < get_ratio + delete_ratio {
                    session.delete(&self.key_buf, now).is_some()
                } else {
                    self.rng.fill_bytes(&mut self.value_buf);
                    session.set(&self.key_buf, &self.value_buf, now).is_some()
                };

                if sent && !warmup {
                    metrics::REQUESTS_SENT.increment();
                }

                if !sent {
                    break;
                }
            }

            // Poll for responses (this also drives I/O)
            // Use a fresh timestamp to accurately measure latency (not the one used for sending)
            self.results.clear();
            if let Err(e) = session.poll_responses(&mut self.results, std::time::Instant::now()) {
                tracing::debug!("Momento poll error: {}", e);
            }

            if !warmup {
                for result in &self.results {
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
            }
        }

        Ok(())
    }

    /// Run a single iteration of prefill (write sequential keys).
    /// Returns true when prefill is complete.
    #[inline]
    pub fn poll_once_prefill(&mut self) -> io::Result<bool> {
        if self.prefill_done {
            return Ok(true);
        }

        let now = std::time::Instant::now();

        // Handle Momento sessions differently
        if self.config.target.protocol == CacheProtocol::Momento {
            return self.poll_once_prefill_momento(now);
        }

        // Try to reconnect disconnected sessions
        self.try_reconnect();

        // Drive prefill requests (sequential SET commands)
        self.drive_prefill_requests(now)?;

        // Flush send buffers
        self.flush_sends()?;

        // Poll for I/O completions
        let count = self.driver.poll(Some(Duration::from_micros(100)))?;

        if count > 0 {
            self.process_completions()?;
        }

        // Process responses (to clear pipeline for more requests)
        self.poll_responses(std::time::Instant::now());

        // Check if prefill is done: all keys have been confirmed
        if self.prefill_confirmed >= self.prefill_total {
            self.prefill_done = true;
            self.shared.mark_prefill_complete();
            return Ok(true);
        }

        Ok(false)
    }

    /// Prefill for Momento sessions.
    ///
    /// Note: Momento uses a simpler prefill approach since it handles its own
    /// connection management. We pop keys from the pending queue but don't
    /// track per-session in-flight state since Momento sessions handle retries
    /// internally.
    #[inline]
    fn poll_once_prefill_momento(&mut self, now: std::time::Instant) -> io::Result<bool> {
        for session in &mut self.momento_sessions {
            // Drive connection
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

            // Send SET commands from pending queue
            while session.can_send() {
                let key_id = match self.prefill_pending.pop_front() {
                    Some(id) => id,
                    None => break,
                };

                write_key(&mut self.key_buf, key_id);
                self.rng.fill_bytes(&mut self.value_buf);

                if session.set(&self.key_buf, &self.value_buf, now).is_none() {
                    // Couldn't send - put the key back
                    self.prefill_pending.push_front(key_id);
                    break;
                }
                // Note: Momento handles retries internally, so we don't track in-flight
            }

            // Poll for responses and count successful SETs
            self.results.clear();
            if let Err(e) = session.poll_responses(&mut self.results, std::time::Instant::now()) {
                tracing::debug!("Momento poll error: {}", e);
            }
            // Count successful SET responses
            for result in &self.results {
                if result.request_type == RequestType::Set && result.success {
                    self.prefill_confirmed += 1;
                }
            }
        }

        // Check if prefill is done: all keys have been confirmed
        if self.prefill_confirmed >= self.prefill_total {
            self.prefill_done = true;
            self.shared.mark_prefill_complete();
            return Ok(true);
        }

        Ok(false)
    }

    /// Drive sequential SET requests for prefill.
    #[inline]
    fn drive_prefill_requests(&mut self, now: std::time::Instant) -> io::Result<()> {
        // Fill each session's pipeline with SET commands from the pending queue
        for idx in 0..self.sessions.len() {
            let session = &mut self.sessions[idx];
            if !session.is_connected() {
                continue;
            }

            while session.can_send() {
                // Pop a key from the pending queue
                let key_id = match self.prefill_pending.pop_front() {
                    Some(id) => id,
                    None => break, // No more keys to send
                };

                write_key(&mut self.key_buf, key_id);
                self.rng.fill_bytes(&mut self.value_buf);

                if session.set(&self.key_buf, &self.value_buf, now).is_some() {
                    // Track this key as in-flight for this session
                    self.prefill_in_flight[idx].push_back(key_id);
                } else {
                    // Couldn't send - put the key back at the front
                    self.prefill_pending.push_front(key_id);
                    break;
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn drive_requests(&mut self, now: std::time::Instant) -> io::Result<()> {
        let key_count = self.config.workload.keyspace.count;
        let get_ratio = self.config.workload.commands.get;
        let delete_ratio = self.config.workload.commands.delete;
        let warmup = self.warmup;

        // Pack each connection's pipeline to maximize throughput.
        // Fill each session's pipeline completely before moving to the next.
        for session in &mut self.sessions {
            if !session.is_connected() {
                continue;
            }

            // Fill this session's pipeline
            while session.can_send() {
                // Check rate limiter
                if let Some(ref rl) = self.ratelimiter
                    && !rl.try_acquire()
                {
                    return Ok(());
                }

                // Generate and send request
                let key_id = self.rng.random_range(0..key_count);
                write_key(&mut self.key_buf, key_id);

                // Command selection: GET if < get_ratio, DELETE if < get_ratio + delete_ratio, else SET
                let roll = self.rng.random_range(0..100);
                let sent = if roll < get_ratio {
                    session.get(&self.key_buf, now).is_some()
                } else if roll < get_ratio + delete_ratio {
                    session.delete(&self.key_buf, now).is_some()
                } else {
                    self.rng.fill_bytes(&mut self.value_buf);
                    session.set(&self.key_buf, &self.value_buf, now).is_some()
                };

                if sent && !warmup {
                    metrics::REQUESTS_SENT.increment();
                }

                if !sent {
                    break;
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn flush_sends(&mut self) -> io::Result<()> {
        // Collect indices of sessions that fail during send
        let mut to_close = Vec::new();

        for (idx, session) in self.sessions.iter_mut().enumerate() {
            if !session.is_connected() {
                continue;
            }

            let conn_id = match session.conn_id() {
                Some(id) => id,
                None => continue,
            };

            let send_buf = session.send_buffer();
            if send_buf.is_empty() {
                continue;
            }

            match self.driver.send(conn_id, send_buf) {
                Ok(n) => {
                    tracing::trace!("worker {} sent {} bytes", self.id, n);
                    session.bytes_sent(n);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    tracing::trace!(
                        "worker {} send would block ({} bytes pending)",
                        self.id,
                        send_buf.len()
                    );
                }
                Err(e) => {
                    tracing::debug!("worker {} send error: {}", self.id, e);
                    to_close.push(idx);
                }
            }
        }

        // Close failed sessions after iteration
        for idx in to_close {
            self.close_session(idx, DisconnectReason::SendError);
        }

        Ok(())
    }

    #[inline]
    fn process_completions(&mut self) -> io::Result<()> {
        let completions = self.driver.drain_completions();

        // Collect indices and reasons for sessions to close (to avoid borrow conflicts)
        let mut to_close: Vec<(usize, DisconnectReason)> = Vec::new();

        for completion in completions {
            match completion.kind {
                CompletionKind::Recv { conn_id } => {
                    // Data is available to read via with_recv_buf
                    let id = conn_id.as_usize();
                    if let Some(&idx) = self.conn_id_to_idx.get(&id) {
                        // Zero-copy path: parse responses directly from driver's buffer
                        // This avoids copying data to an intermediate session buffer
                        let now = std::time::Instant::now();
                        self.results.clear();
                        let results = &mut self.results;
                        let session = &mut self.sessions[idx];

                        let result =
                            self.driver
                                .with_recv_buf(conn_id, &mut |buf: &mut dyn RecvBuf| {
                                    if let Err(e) = session.poll_responses_from(buf, results, now) {
                                        tracing::debug!("protocol error: {}", e);
                                    }
                                    // Shrink buffer if it grew large from big responses
                                    buf.shrink_if_oversized();
                                });

                        // Check for EOF or errors
                        if let Err(e) = result {
                            if e.kind() == io::ErrorKind::UnexpectedEof {
                                to_close.push((idx, DisconnectReason::Eof));
                            } else if e.kind() != io::ErrorKind::NotFound {
                                to_close.push((idx, DisconnectReason::RecvError));
                            }
                        }

                        // Handle prefill tracking: pop from in-flight and confirm
                        if !self.prefill_done {
                            for result in &self.results {
                                if result.request_type == RequestType::Set {
                                    // Pop the oldest in-flight key for this session
                                    if let Some(_key_id) = self.prefill_in_flight[idx].pop_front() {
                                        if result.success {
                                            // Key confirmed successfully
                                            self.prefill_confirmed += 1;
                                        } else {
                                            // SET failed - put key back in pending for retry
                                            self.prefill_pending.push_back(_key_id);
                                        }
                                    }
                                }
                            }
                        }

                        // Record metrics from parsed responses (outside closure)
                        if !self.warmup {
                            for result in &self.results {
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
                                    }
                                    RequestType::Set => {
                                        metrics::SET_COUNT.increment();
                                        let _ = metrics::SET_LATENCY.increment(result.latency_ns);
                                    }
                                    RequestType::Delete => {
                                        metrics::DELETE_COUNT.increment();
                                        let _ =
                                            metrics::DELETE_LATENCY.increment(result.latency_ns);
                                    }
                                    RequestType::Ping | RequestType::Other => {}
                                }
                            }
                        }
                    }
                }
                CompletionKind::SendReady { conn_id } => {
                    // Socket is writable, try to flush
                    let id = conn_id.as_usize();
                    if let Some(&idx) = self.conn_id_to_idx.get(&id) {
                        let session = &mut self.sessions[idx];
                        let send_buf = session.send_buffer();
                        if !send_buf.is_empty() {
                            match self.driver.send(conn_id, send_buf) {
                                Ok(n) => {
                                    session.bytes_sent(n);
                                }
                                Err(_) => {
                                    // WouldBlock is expected, other errors handled elsewhere
                                }
                            }
                        }
                    }
                }
                CompletionKind::Closed { conn_id } => {
                    let id = conn_id.as_usize();
                    if let Some(&idx) = self.conn_id_to_idx.get(&id) {
                        to_close.push((idx, DisconnectReason::ClosedEvent));
                    }
                }
                CompletionKind::Error { conn_id, error } => {
                    let id = conn_id.as_usize();
                    tracing::trace!("connection {} error: {}", id, error);
                    if let Some(&idx) = self.conn_id_to_idx.get(&id) {
                        to_close.push((idx, DisconnectReason::ErrorEvent));
                    }
                }
                // Accept, AcceptRaw, and ListenerError are for server-side, not used here
                CompletionKind::Accept { .. }
                | CompletionKind::AcceptRaw { .. }
                | CompletionKind::ListenerError { .. } => {}

                // RecvComplete is for io_uring single-shot recv - not used anymore
                CompletionKind::RecvComplete { .. } => {}

                // UDP events not used in TCP client
                CompletionKind::UdpReadable { .. }
                | CompletionKind::RecvMsgComplete { .. }
                | CompletionKind::UdpWritable { .. }
                | CompletionKind::SendMsgComplete { .. }
                | CompletionKind::UdpError { .. } => {}
            }
        }

        // Close failed sessions after processing all completions
        for (idx, reason) in to_close {
            self.close_session(idx, reason);
        }

        Ok(())
    }

    #[inline]
    fn poll_responses(&mut self, now: std::time::Instant) {
        if self.warmup {
            // Still need to parse responses but don't record metrics
            for idx in 0..self.sessions.len() {
                self.results.clear();
                let _ = self.sessions[idx].poll_responses(&mut self.results, now);
                // Handle prefill tracking: pop from in-flight and confirm
                if !self.prefill_done {
                    for result in &self.results {
                        if result.request_type == RequestType::Set {
                            if let Some(_key_id) = self.prefill_in_flight[idx].pop_front() {
                                if result.success {
                                    self.prefill_confirmed += 1;
                                } else {
                                    self.prefill_pending.push_back(_key_id);
                                }
                            }
                        }
                    }
                }
            }
            return;
        }

        for idx in 0..self.sessions.len() {
            self.results.clear();
            if let Err(e) = self.sessions[idx].poll_responses(&mut self.results, now) {
                tracing::debug!("protocol error: {}", e);
            }

            for result in &self.results {
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
        }
    }

    /// Get the number of active connections.
    pub fn active_connections(&self) -> usize {
        let tcp_sessions = self.sessions.iter().filter(|s| s.is_connected()).count();
        let momento_sessions = self
            .momento_sessions
            .iter()
            .filter(|s| s.is_connected())
            .count();
        tcp_sessions + momento_sessions
    }

    /// Get the total in-flight count.
    #[allow(dead_code)]
    pub fn in_flight_count(&self) -> usize {
        let tcp: usize = self.sessions.iter().map(|s| s.in_flight_count()).sum();
        let momento: usize = self
            .momento_sessions
            .iter()
            .map(|s| s.in_flight_count())
            .sum();
        tcp + momento
    }

    /// Try to reconnect disconnected sessions.
    fn try_reconnect(&mut self) {
        // Collect indices of sessions that need reconnection
        let to_reconnect: Vec<_> = self
            .sessions
            .iter()
            .enumerate()
            .filter(|(_, s)| s.should_reconnect())
            .map(|(idx, s)| (idx, s.addr(), s.conn_id().map(|c| c.as_usize())))
            .collect();

        for (idx, addr, old_conn_id) in to_reconnect {
            // Close the old connection if it exists (it may have been marked
            // disconnected but the socket never closed)
            if let Some(old_id) = old_conn_id {
                let _ = self.driver.close(ConnId::new(old_id));
                self.conn_id_to_idx.remove(&old_id);
            }

            // Try to connect
            match Self::create_connection(addr, self.config.connection.connect_timeout) {
                Ok(stream) => {
                    match self.driver.register(stream) {
                        Ok(conn_id) => {
                            let session = &mut self.sessions[idx];
                            // Reset state BEFORE setting new conn_id to ensure
                            // there's no window where new conn_id has old state
                            session.reset();
                            session.set_conn_id(conn_id);
                            session.reconnect_attempted(true);

                            self.conn_id_to_idx.insert(conn_id.as_usize(), idx);

                            metrics::CONNECTIONS_ACTIVE.increment();
                        }
                        Err(_) => {
                            self.sessions[idx].reconnect_attempted(false);
                        }
                    }
                }
                Err(_) => {
                    self.sessions[idx].reconnect_attempted(false);
                }
            }
        }
    }

    /// Close a session's connection in the driver and mark it as disconnected.
    /// This ensures the socket is properly closed so the server receives FIN/RST.
    fn close_session(&mut self, idx: usize, reason: DisconnectReason) {
        let session = &mut self.sessions[idx];
        if let Some(conn_id) = session.conn_id() {
            let _ = self.driver.close(conn_id);
            self.conn_id_to_idx.remove(&conn_id.as_usize());
        }
        session.disconnect();

        // Move any in-flight prefill keys back to pending queue for retry
        if !self.prefill_done && idx < self.prefill_in_flight.len() {
            while let Some(key_id) = self.prefill_in_flight[idx].pop_front() {
                self.prefill_pending.push_back(key_id);
            }
        }

        // Track connection state
        metrics::CONNECTIONS_FAILED.increment();

        // Track disconnect reason
        match reason {
            DisconnectReason::Eof => {
                metrics::DISCONNECTS_EOF.increment();
            }
            DisconnectReason::RecvError => {
                metrics::DISCONNECTS_RECV_ERROR.increment();
            }
            DisconnectReason::SendError => {
                metrics::DISCONNECTS_SEND_ERROR.increment();
            }
            DisconnectReason::ClosedEvent => {
                metrics::DISCONNECTS_CLOSED_EVENT.increment();
            }
            DisconnectReason::ErrorEvent => {
                metrics::DISCONNECTS_ERROR_EVENT.increment();
            }
            DisconnectReason::ConnectFailed => {
                metrics::DISCONNECTS_CONNECT_FAILED.increment();
            }
        }
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
