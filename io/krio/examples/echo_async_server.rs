//! Async echo server using AsyncEventHandler.
//!
//! Usage:
//!   cargo run --example echo_async_server [BIND_ADDR]
//!   # default: 127.0.0.1:7878

use std::future::Future;
use std::pin::Pin;

use krio::{AsyncEventHandler, Config, ConnCtx, KrioBuilder};

struct AsyncEcho {
    worker_id: usize,
}

impl AsyncEventHandler for AsyncEcho {
    fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        let worker_id = self.worker_id;
        Box::pin(async move {
            eprintln!("[worker {worker_id}] accepted connection {}", conn.index());
            loop {
                let consumed = conn
                    .with_data(|data| {
                        if let Err(e) = conn.send(data) {
                            eprintln!("[worker {worker_id}] send error: {e}");
                        }
                        data.len()
                    })
                    .await;
                if consumed == 0 {
                    break;
                }
            }
            eprintln!("[worker {worker_id}] connection {} closed", conn.index());
        })
    }

    fn create_for_worker(worker_id: usize) -> Self {
        eprintln!("[worker {worker_id}] starting");
        AsyncEcho { worker_id }
    }
}

fn main() {
    let bind_addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7878".to_string());

    let mut config = Config::default();
    config.worker.threads = 1;
    config.worker.pin_to_core = false;
    config.sq_entries = 128;
    config.recv_buffer.ring_size = 128;
    config.recv_buffer.buffer_size = 4096;
    config.max_connections = 1024;

    eprintln!("starting async echo server on {bind_addr}");

    let (_shutdown, handles) = KrioBuilder::new(config)
        .bind(&bind_addr)
        .launch_async::<AsyncEcho>()
        .expect("failed to launch workers");

    for handle in handles {
        if let Err(e) = handle.join().expect("worker thread panicked") {
            eprintln!("worker exited with error: {e}");
        }
    }
}
