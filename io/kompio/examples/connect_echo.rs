use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use kompio::{Config, ConnToken, DriverCtx, EventHandler, KompioBuilder};

/// Demonstrates outbound `connect()`. On the first tick, connects to a remote
/// echo server, sends "Hello from kompio!\n", prints the echoed response,
/// then shuts down.
struct ConnectHandler {
    worker_id: usize,
    target: std::net::SocketAddr,
    connected: bool,
    sent: bool,
    done: AtomicBool,
}

impl EventHandler for ConnectHandler {
    fn on_accept(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {
        // Not used in this example.
    }

    fn on_connect(&mut self, ctx: &mut DriverCtx, conn: ConnToken, result: io::Result<()>) {
        match result {
            Ok(()) => {
                eprintln!("[worker {}] connected to {}", self.worker_id, self.target);
                self.connected = true;
                let msg = b"Hello from kompio!\n";
                if let Err(e) = ctx.send(conn, msg) {
                    eprintln!("[worker {}] send error: {e}", self.worker_id);
                    ctx.close(conn);
                    return;
                }
                self.sent = true;
            }
            Err(e) => {
                eprintln!("[worker {}] connect failed: {e}", self.worker_id);
                ctx.request_shutdown();
            }
        }
    }

    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        let text = String::from_utf8_lossy(data);
        eprintln!("[worker {}] received: {}", self.worker_id, text.trim());
        // Got the echo — close and shut down.
        ctx.close(conn);
        ctx.request_shutdown();
        data.len()
    }

    fn on_send_complete(
        &mut self,
        _ctx: &mut DriverCtx,
        _conn: ConnToken,
        result: io::Result<u32>,
    ) {
        match result {
            Ok(n) => eprintln!("[worker {}] sent {n} bytes", self.worker_id),
            Err(e) => eprintln!("[worker {}] send error: {e}", self.worker_id),
        }
    }

    fn on_close(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        eprintln!("[worker {}] connection {:?} closed", self.worker_id, conn);
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx) {
        if self.connected || self.done.load(Ordering::Relaxed) {
            return;
        }
        // Initiate connection on first tick.
        self.done.store(true, Ordering::Relaxed);
        match ctx.connect(self.target) {
            Ok(token) => {
                eprintln!(
                    "[worker {}] connecting to {} (token: {:?})",
                    self.worker_id, self.target, token
                );
            }
            Err(e) => {
                eprintln!("[worker {}] connect() failed: {e}", self.worker_id);
                ctx.request_shutdown();
            }
        }
    }

    fn create_for_worker(worker_id: usize) -> Self {
        let target: std::net::SocketAddr = std::env::var("TARGET")
            .unwrap_or_else(|_| "127.0.0.1:7878".to_string())
            .parse()
            .expect("invalid TARGET address");

        eprintln!("[worker {worker_id}] will connect to {target}");
        ConnectHandler {
            worker_id,
            target,
            connected: false,
            sent: false,
            done: AtomicBool::new(false),
        }
    }
}

fn main() {
    // This example needs a running echo server (e.g., the echo_server example).
    // Start it first:  cargo run --example echo_server
    // Then run:         cargo run --example connect_echo
    // Or specify:       TARGET=10.0.0.1:8080 cargo run --example connect_echo

    let mut config = Config::default();
    config.worker.threads = 1;
    config.worker.pin_to_core = false;
    config.sq_entries = 64;
    config.recv_buffer.ring_size = 64;
    config.recv_buffer.buffer_size = 4096;
    config.max_connections = 64;

    eprintln!("starting connect_echo example (client-only mode)");

    // Client-only mode: no bind address, no acceptor thread.
    let (_shutdown, handles) = KompioBuilder::new(config)
        .launch::<ConnectHandler>()
        .expect("failed to launch workers");

    for handle in handles {
        if let Err(e) = handle.join().expect("worker thread panicked") {
            eprintln!("worker exited with error: {e}");
        }
    }
}
