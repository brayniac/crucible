//! Minimal PING/PONG server for benchmarking I/O performance.
//!
//! This server responds to `PING\r\n` with `PONG\r\n`.
//! No cache, no complex protocol handling - just pure I/O.

use clap::Parser;
use io_driver::{CompletionKind, ConnId, Driver, IoDriver, IoEngine};
use protocol_ping::Response;
use serde::Deserialize;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "ping-server")]
#[command(about = "Minimal PING/PONG server for I/O benchmarking")]
struct Args {
    /// Config file path (optional, CLI args override config)
    config: Option<String>,

    /// Listen address
    #[arg(short, long)]
    listen: Option<SocketAddr>,

    /// I/O engine (auto, mio, uring)
    #[arg(short, long)]
    engine: Option<String>,

    /// Number of worker threads (0 = single-threaded)
    #[arg(short, long)]
    threads: Option<usize>,
}

/// Server configuration loaded from TOML file.
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    workers: WorkersConfig,
    #[serde(default)]
    uring: UringConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerConfig {
    #[serde(default = "default_listen")]
    listen: SocketAddr,
    #[serde(default)]
    io_engine: IoEngine,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            io_engine: IoEngine::Auto,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct WorkersConfig {
    #[serde(default)]
    threads: usize,
    #[serde(default)]
    cpu_affinity: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UringConfig {
    #[serde(default = "default_buffer_size")]
    buffer_size: usize,
    #[serde(default = "default_buffer_count")]
    buffer_count: u16,
    #[serde(default = "default_sq_depth")]
    sq_depth: u32,
    #[serde(default)]
    sqpoll: bool,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            buffer_size: default_buffer_size(),
            buffer_count: default_buffer_count(),
            sq_depth: default_sq_depth(),
            sqpoll: false,
        }
    }
}

fn default_listen() -> SocketAddr {
    "0.0.0.0:6379".parse().unwrap()
}

fn default_buffer_size() -> usize {
    4096
}

fn default_buffer_count() -> u16 {
    512
}

fn default_sq_depth() -> u32 {
    512
}

impl Config {
    fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
}

/// Per-connection state.
struct Conn {
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    write_pos: usize,
}

impl Conn {
    fn new() -> Self {
        Self {
            read_buf: Vec::with_capacity(4096),
            write_buf: Vec::with_capacity(4096),
            write_pos: 0,
        }
    }

    fn process(&mut self) {
        // Parse as many PING requests as possible
        loop {
            if self.read_buf.len() >= 6 && &self.read_buf[..6] == b"PING\r\n" {
                // Consume the request
                self.read_buf.drain(..6);
                // Queue the response
                let start = self.write_buf.len();
                self.write_buf.resize(start + 6, 0);
                Response::Pong.encode(&mut self.write_buf[start..]);
            } else if self.read_buf.len() < 6 {
                // Need more data
                break;
            } else {
                // Invalid data - drain one byte and retry
                if !self.read_buf.is_empty() {
                    self.read_buf.remove(0);
                } else {
                    break;
                }
            }
        }
    }

    fn pending_write(&self) -> &[u8] {
        &self.write_buf[self.write_pos..]
    }

    fn advance_write(&mut self, n: usize) {
        self.write_pos += n;
        if self.write_pos >= self.write_buf.len() {
            self.write_buf.clear();
            self.write_pos = 0;
        }
    }

    fn has_pending_write(&self) -> bool {
        self.write_pos < self.write_buf.len()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load config file if provided
    let config = if let Some(ref path) = args.config {
        Config::load(Path::new(path))?
    } else {
        Config::default()
    };

    // CLI args override config file
    let listen = args.listen.unwrap_or(config.server.listen);
    let threads = args.threads.unwrap_or(config.workers.threads);
    let engine = if let Some(ref e) = args.engine {
        match e.as_str() {
            "mio" => IoEngine::Mio,
            "uring" | "io_uring" => IoEngine::Uring,
            _ => IoEngine::Auto,
        }
    } else {
        config.server.io_engine
    };

    eprintln!("Starting ping server on {}", listen);

    if threads == 0 {
        run_single_threaded(listen, engine, &config.uring)?;
    } else {
        run_multi_threaded(listen, engine, threads, &config.uring)?;
    }

    Ok(())
}

/// Single-threaded server - simplest possible implementation.
fn run_single_threaded(addr: SocketAddr, engine: IoEngine, uring: &UringConfig) -> io::Result<()> {
    let mut driver = Driver::builder()
        .engine(engine)
        .buffer_size(uring.buffer_size)
        .buffer_count(uring.buffer_count)
        .sq_depth(uring.sq_depth)
        .sqpoll(uring.sqpoll)
        .build()?;

    driver.listen(addr, 4096)?;
    eprintln!("Listening on {} (single-threaded)", addr);

    let mut connections: Vec<Option<Conn>> = Vec::with_capacity(1024);
    let mut recv_buf = vec![0u8; 64 * 1024];

    loop {
        let _ = driver.poll(Some(Duration::from_millis(1)))?;

        for completion in driver.drain_completions() {
            match completion.kind {
                CompletionKind::Accept { conn_id, .. } => {
                    let idx = conn_id.as_usize();
                    if idx >= connections.len() {
                        connections.resize_with(idx + 1, || None);
                    }
                    connections[idx] = Some(Conn::new());
                }

                CompletionKind::Recv { conn_id } => {
                    let idx = conn_id.as_usize();
                    let mut should_close = false;

                    'recv: loop {
                        let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut()) else {
                            break;
                        };

                        match driver.recv(conn_id, &mut recv_buf) {
                            Ok(0) => {
                                should_close = true;
                                break;
                            }
                            Ok(n) => {
                                conn.read_buf.extend_from_slice(&recv_buf[..n]);
                                conn.process();

                                // Send responses
                                while conn.has_pending_write() {
                                    match driver.send(conn_id, conn.pending_write()) {
                                        Ok(n) => conn.advance_write(n),
                                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                                        Err(_) => {
                                            should_close = true;
                                            break 'recv;
                                        }
                                    }
                                }
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(_) => {
                                should_close = true;
                                break;
                            }
                        }
                    }

                    if should_close {
                        close_conn(&mut driver, &mut connections, conn_id);
                    }
                }

                CompletionKind::SendReady { conn_id } => {
                    let idx = conn_id.as_usize();
                    let mut should_close = false;

                    loop {
                        let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut()) else {
                            break;
                        };

                        if !conn.has_pending_write() {
                            break;
                        }

                        match driver.send(conn_id, conn.pending_write()) {
                            Ok(n) => conn.advance_write(n),
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(_) => {
                                should_close = true;
                                break;
                            }
                        }
                    }

                    if should_close {
                        close_conn(&mut driver, &mut connections, conn_id);
                    }
                }

                CompletionKind::Closed { conn_id } | CompletionKind::Error { conn_id, .. } => {
                    close_conn(&mut driver, &mut connections, conn_id);
                }

                _ => {}
            }
        }
    }
}

fn close_conn(driver: &mut Box<dyn IoDriver>, connections: &mut [Option<Conn>], conn_id: ConnId) {
    let idx = conn_id.as_usize();
    if let Some(slot) = connections.get_mut(idx)
        && slot.take().is_some()
    {
        let _ = driver.close(conn_id);
    }
}

/// Multi-threaded server with acceptor + workers.
fn run_multi_threaded(
    addr: SocketAddr,
    engine: IoEngine,
    num_workers: usize,
    uring: &UringConfig,
) -> io::Result<()> {
    use std::os::unix::io::RawFd;
    use std::sync::atomic::AtomicBool;

    let shutdown = Arc::new(AtomicBool::new(false));

    // Create channels for FD distribution
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..num_workers)
        .map(|_| crossbeam_channel::bounded::<RawFd>(1024))
        .unzip();

    // Clone uring config for workers
    let uring_config = Arc::new(uring.clone());

    // Spawn workers
    let mut handles = Vec::with_capacity(num_workers + 1);
    for (worker_id, receiver) in receivers.into_iter().enumerate() {
        let uring = uring_config.clone();
        let handle = std::thread::Builder::new()
            .name(format!("ping-worker-{}", worker_id))
            .spawn(move || {
                if let Err(e) = run_worker(engine, receiver, &uring) {
                    eprintln!("Worker {} error: {}", worker_id, e);
                }
            })
            .expect("failed to spawn worker");
        handles.push(handle);
    }

    // Run acceptor on main thread
    eprintln!("Listening on {} ({} workers)", addr, num_workers);
    run_acceptor(addr, engine, senders, shutdown.clone())?;

    // Wait for workers
    for handle in handles {
        let _ = handle.join();
    }

    Ok(())
}

fn run_acceptor(
    addr: SocketAddr,
    engine: IoEngine,
    senders: Vec<crossbeam_channel::Sender<std::os::unix::io::RawFd>>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> io::Result<()> {
    use std::sync::atomic::Ordering;

    let mut driver = Driver::builder()
        .engine(engine)
        .buffer_size(4096)
        .buffer_count(16)
        .sq_depth(64)
        .build()?;

    driver.listen_raw(addr, 4096)?;

    let num_workers = senders.len();
    let mut next_worker = 0usize;

    while !shutdown.load(Ordering::Relaxed) {
        let _ = driver.poll(Some(Duration::from_millis(10)))?;

        for completion in driver.drain_completions() {
            if let CompletionKind::AcceptRaw { raw_fd, .. } = completion.kind {
                let sender = &senders[next_worker];
                next_worker = (next_worker + 1) % num_workers;

                if sender.try_send(raw_fd).is_err() {
                    unsafe { libc::close(raw_fd) };
                }
            }
        }
    }

    Ok(())
}

fn run_worker(
    engine: IoEngine,
    fd_receiver: crossbeam_channel::Receiver<std::os::unix::io::RawFd>,
    uring: &UringConfig,
) -> io::Result<()> {
    let mut driver = Driver::builder()
        .engine(engine)
        .buffer_size(uring.buffer_size)
        .buffer_count(uring.buffer_count)
        .sq_depth(uring.sq_depth)
        .sqpoll(uring.sqpoll)
        .build()?;

    let mut connections: Vec<Option<Conn>> = Vec::with_capacity(4096);
    let mut recv_buf = vec![0u8; 64 * 1024];

    loop {
        // Accept new connections from acceptor
        while let Ok(raw_fd) = fd_receiver.try_recv() {
            if let Ok(conn_id) = driver.register_fd(raw_fd) {
                let idx = conn_id.as_usize();
                if idx >= connections.len() {
                    connections.resize_with(idx + 1, || None);
                }
                connections[idx] = Some(Conn::new());
            }
        }

        let _ = driver.poll(Some(Duration::from_millis(1)))?;

        for completion in driver.drain_completions() {
            match completion.kind {
                CompletionKind::Accept { conn_id, .. } => {
                    let idx = conn_id.as_usize();
                    if idx >= connections.len() {
                        connections.resize_with(idx + 1, || None);
                    }
                    connections[idx] = Some(Conn::new());
                }

                CompletionKind::Recv { conn_id } => {
                    let idx = conn_id.as_usize();
                    let mut should_close = false;

                    'recv: loop {
                        let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut()) else {
                            break;
                        };

                        match driver.recv(conn_id, &mut recv_buf) {
                            Ok(0) => {
                                should_close = true;
                                break;
                            }
                            Ok(n) => {
                                conn.read_buf.extend_from_slice(&recv_buf[..n]);
                                conn.process();

                                while conn.has_pending_write() {
                                    match driver.send(conn_id, conn.pending_write()) {
                                        Ok(n) => conn.advance_write(n),
                                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                                        Err(_) => {
                                            should_close = true;
                                            break 'recv;
                                        }
                                    }
                                }
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(_) => {
                                should_close = true;
                                break;
                            }
                        }
                    }

                    if should_close {
                        let idx = conn_id.as_usize();
                        if let Some(slot) = connections.get_mut(idx)
                            && slot.take().is_some()
                        {
                            let _ = driver.close(conn_id);
                        }
                    }
                }

                CompletionKind::SendReady { conn_id } => {
                    let idx = conn_id.as_usize();
                    let mut should_close = false;

                    loop {
                        let Some(conn) = connections.get_mut(idx).and_then(|c| c.as_mut()) else {
                            break;
                        };

                        if !conn.has_pending_write() {
                            break;
                        }

                        match driver.send(conn_id, conn.pending_write()) {
                            Ok(n) => conn.advance_write(n),
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(_) => {
                                should_close = true;
                                break;
                            }
                        }
                    }

                    if should_close {
                        let idx = conn_id.as_usize();
                        if let Some(slot) = connections.get_mut(idx)
                            && slot.take().is_some()
                        {
                            let _ = driver.close(conn_id);
                        }
                    }
                }

                CompletionKind::Closed { conn_id } | CompletionKind::Error { conn_id, .. } => {
                    let idx = conn_id.as_usize();
                    if let Some(slot) = connections.get_mut(idx)
                        && slot.take().is_some()
                    {
                        let _ = driver.close(conn_id);
                    }
                }

                _ => {}
            }
        }
    }
}
