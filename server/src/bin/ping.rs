//! Minimal PING/PONG server for benchmarking I/O performance.
//!
//! This server responds to `PING\r\n` with `PONG\r\n`.
//! No cache, no complex protocol handling - just pure I/O.

use clap::Parser;
use protocol_ping::Response;
use ringline::{AsyncEventHandler, ConnCtx, DriverCtx, ParseResult, RinglineBuilder};
use serde::Deserialize;
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

/// Ping server async event handler.
struct PingHandler;

impl AsyncEventHandler for PingHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
        async move {
            let mut write_buf = Vec::with_capacity(4096);

            loop {
                let consumed = conn
                    .with_data(|data| {
                        if data.is_empty() {
                            return ParseResult::Consumed(0);
                        }

                        let mut consumed = 0;
                        let mut remaining = data;

                        loop {
                            if remaining.len() >= 6 && &remaining[..6] == b"PING\r\n" {
                                remaining = &remaining[6..];
                                consumed += 6;
                                let start = write_buf.len();
                                write_buf.resize(start + 6, 0);
                                Response::Pong.encode(&mut write_buf[start..]);
                            } else {
                                break;
                            }
                        }

                        ParseResult::Consumed(consumed)
                    })
                    .await;

                if consumed == 0 {
                    break;
                }

                // Send pending responses
                if !write_buf.is_empty() {
                    if conn.send_nowait(&write_buf).is_err() {
                        break;
                    }
                    write_buf.clear();
                }
            }
        }
    }

    fn on_tick(&mut self, _ctx: &mut DriverCtx<'_>) {}

    fn create_for_worker(_worker_id: usize) -> Self {
        PingHandler
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

    let krio_config = ringline::Config {
        sq_entries: config.uring.sq_depth,
        sqpoll: config.uring.sqpoll,
        recv_buffer: ringline::RecvBufferConfig {
            ring_size: config.uring.buffer_count.next_power_of_two(),
            buffer_size: config.uring.buffer_size,
            ..Default::default()
        },
        worker: ringline::WorkerConfig {
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

    let (shutdown_handle, handles) = RinglineBuilder::new(krio_config)
        .bind(listen)
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
