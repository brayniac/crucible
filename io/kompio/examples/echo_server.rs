use std::io;

use kompio::{Config, ConnToken, DriverCtx, EventHandler};

struct EchoHandler {
    worker_id: usize,
    connections: u32,
}

impl EventHandler for EchoHandler {
    fn on_accept(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        self.connections += 1;
        eprintln!(
            "[worker {}] accepted connection {:?} (total: {})",
            self.worker_id, conn, self.connections
        );
    }

    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }

        if let Err(e) = ctx.send(conn, data) {
            eprintln!("[worker {}] send error: {e}", self.worker_id);
        }
        data.len()
    }

    fn on_send_complete(
        &mut self,
        _ctx: &mut DriverCtx,
        _conn: ConnToken,
        result: io::Result<u32>,
    ) {
        if let Err(e) = result {
            eprintln!("[worker {}] send completed with error: {e}", self.worker_id);
        }
    }

    fn on_close(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        self.connections = self.connections.saturating_sub(1);
        eprintln!(
            "[worker {}] connection {:?} closed (remaining: {})",
            self.worker_id, conn, self.connections
        );
    }

    fn create_for_worker(worker_id: usize) -> Self {
        eprintln!("[worker {worker_id}] starting");
        EchoHandler {
            worker_id,
            connections: 0,
        }
    }
}

fn main() {
    let bind_addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7878".to_string());

    let mut config = Config::default();
    config.worker.threads = 1;
    config.worker.pin_to_core = false; // Don't require root for the example
    config.sq_entries = 128;
    config.recv_buffer.ring_size = 128;
    config.recv_buffer.buffer_size = 4096;
    config.max_connections = 1024;

    eprintln!("starting echo server on {bind_addr}");

    let (_shutdown, handles) = kompio::launch::<EchoHandler>(config, &bind_addr)
        .expect("failed to launch workers");

    for handle in handles {
        if let Err(e) = handle.join().expect("worker thread panicked") {
            eprintln!("worker exited with error: {e}");
        }
    }
}
