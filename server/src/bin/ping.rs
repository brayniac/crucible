//! Minimal PING/PONG server for benchmarking I/O performance.
//!
//! This server responds to `PING\r\n` with `PONG\r\n`.
//! No cache, no complex protocol handling - just pure I/O.

use clap::Parser;
use kompio::{ConnToken, DriverCtx, EventHandler, KompioBuilder};
use protocol_ping::Response;
use serde::Deserialize;
use std::io;
use std::net::SocketAddr;
use std::path::Path;

#[derive(Parser)]
#[command(name = "ping-server")]
#[command(about = "Minimal PING/PONG server for I/O benchmarking")]
struct Args {
    /// Config file path (optional, CLI args override config)
    config: Option<String>,

    /// Listen address
    #[arg(short, long)]
    listen: Option<SocketAddr>,

    /// Number of worker threads (0 = auto-detect)
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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct WorkersConfig {
    #[serde(default)]
    threads: usize,
    #[serde(default)]
    #[allow(dead_code)]
    cpu_affinity: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UringConfig {
    #[serde(default = "default_buffer_size")]
    buffer_size: u32,
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

fn default_buffer_size() -> u32 {
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
    write_buf: Vec<u8>,
    write_pos: usize,
}

impl Conn {
    fn new() -> Self {
        Self {
            write_buf: Vec::with_capacity(4096),
            write_pos: 0,
        }
    }

    /// Process received data, returning number of bytes consumed.
    fn process(&mut self, data: &[u8]) -> usize {
        let mut consumed = 0;
        let mut remaining = data;

        loop {
            if remaining.len() >= 6 && &remaining[..6] == b"PING\r\n" {
                remaining = &remaining[6..];
                consumed += 6;
                let start = self.write_buf.len();
                self.write_buf.resize(start + 6, 0);
                Response::Pong.encode(&mut self.write_buf[start..]);
            } else {
                break;
            }
        }

        consumed
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

/// Ping server event handler.
struct PingHandler {
    connections: Vec<Option<Conn>>,
}

impl EventHandler for PingHandler {
    fn create_for_worker(_worker_id: usize) -> Self {
        PingHandler {
            connections: Vec::with_capacity(1024),
        }
    }

    fn on_accept(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        let idx = conn.index();
        if idx >= self.connections.len() {
            self.connections.resize_with(idx + 1, || None);
        }
        self.connections[idx] = Some(Conn::new());
    }

    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        let idx = conn.index();

        if let Some(c) = self.connections.get_mut(idx).and_then(|c| c.as_mut()) {
            let n = c.process(data);

            // Send pending responses
            while c.has_pending_write() {
                match ctx.send(conn, c.pending_write()) {
                    Ok(()) => {
                        let len = c.pending_write().len();
                        c.advance_write(len);
                    }
                    Err(_) => break,
                }
            }

            n
        } else {
            0
        }
    }

    fn on_send_complete(&mut self, ctx: &mut DriverCtx, conn: ConnToken, result: io::Result<u32>) {
        if result.is_err() {
            ctx.close(conn);
            let idx = conn.index();
            if let Some(slot) = self.connections.get_mut(idx) {
                *slot = None;
            }
            return;
        }

        // Try to send more pending data
        let idx = conn.index();
        if let Some(c) = self.connections.get_mut(idx).and_then(|c| c.as_mut()) {
            while c.has_pending_write() {
                match ctx.send(conn, c.pending_write()) {
                    Ok(()) => {
                        let len = c.pending_write().len();
                        c.advance_write(len);
                    }
                    Err(_) => break,
                }
            }
        }
    }

    fn on_close(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        let idx = conn.index();
        if let Some(slot) = self.connections.get_mut(idx) {
            *slot = None;
        }
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

    eprintln!("Starting ping server on {}", listen);

    let kompio_config = kompio::Config {
        sq_entries: config.uring.sq_depth,
        sqpoll: config.uring.sqpoll,
        recv_buffer: kompio::RecvBufferConfig {
            ring_size: config.uring.buffer_count.next_power_of_two(),
            buffer_size: config.uring.buffer_size,
            ..Default::default()
        },
        worker: kompio::WorkerConfig {
            threads: if threads == 0 { 0 } else { threads },
            pin_to_core: false,
            core_offset: 0,
        },
        tcp_nodelay: true,
        ..Default::default()
    };

    let workers = if threads == 0 {
        "auto"
    } else {
        &threads.to_string()
    };
    eprintln!("Listening on {} ({} workers)", listen, workers);

    let (shutdown_handle, handles) = KompioBuilder::new(kompio_config)
        .bind(&listen.to_string())
        .launch::<PingHandler>()?;

    // Wait for Ctrl+C
    ctrlc::set_handler(move || {
        shutdown_handle.shutdown();
    })
    .expect("error setting Ctrl-C handler");

    for handle in handles {
        let _ = handle.join();
    }

    Ok(())
}
