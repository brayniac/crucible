//! Benchmark comparing callback-based EventHandler vs async AsyncEventHandler
//! for a minimal echo server. Measures throughput (ops/sec) and per-operation
//! latency to detect any overhead from the async runtime.
//!
//! Usage:
//!   cargo run --release --example echo_bench -- [OPTIONS]
//!
//! Options:
//!   --duration <secs>    Test duration per mode (default: 5)
//!   --clients <n>        Number of concurrent client threads (default: 4)
//!   --msg-size <bytes>   Message size in bytes (default: 64)
//!   --workers <n>        Number of server worker threads (default: 1)
//!   --port <n>           Base port (default: 17171)

use std::future::Future;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kompio::{
    AsyncEventHandler, Config, ConnCtx, ConnToken, DriverCtx, EventHandler, KompioBuilder,
};

// ── Callback echo handler ────────────────────────────────────────────

struct CallbackEcho;

impl EventHandler for CallbackEcho {
    fn on_accept(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}

    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        let _ = ctx.send(conn, data);
        data.len()
    }

    fn on_send_complete(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken, _result: io::Result<u32>) {}

    fn on_close(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}

    fn create_for_worker(_worker_id: usize) -> Self {
        CallbackEcho
    }
}

// ── Async echo handler ──────────────────────────────────────────────

struct AsyncEcho;

impl AsyncEventHandler for AsyncEcho {
    fn on_accept(&self, conn: ConnCtx) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        Box::pin(async move {
            loop {
                let n = conn
                    .with_data(|data| {
                        let _ = conn.send(data);
                        data.len()
                    })
                    .await;
                if n == 0 {
                    break;
                }
            }
        })
    }

    fn create_for_worker(_worker_id: usize) -> Self {
        AsyncEcho
    }
}

// ── Client ──────────────────────────────────────────────────────────

fn run_client(addr: &str, msg_size: usize, stop: Arc<AtomicBool>, ops_counter: Arc<AtomicU64>) {
    let msg = vec![0xABu8; msg_size];
    let mut recv_buf = vec![0u8; msg_size];

    let mut stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("client connect failed: {e}");
            return;
        }
    };
    stream.set_nodelay(true).ok();

    let mut local_ops: u64 = 0;

    while !stop.load(Ordering::Relaxed) {
        // Send
        if stream.write_all(&msg).is_err() {
            break;
        }

        // Recv — read exactly msg_size bytes
        let mut total_read = 0;
        while total_read < msg_size {
            match stream.read(&mut recv_buf[total_read..]) {
                Ok(0) => return,
                Ok(n) => total_read += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => return,
            }
        }

        local_ops += 1;

        // Batch-update the shared counter every 256 ops to reduce contention.
        if local_ops & 0xFF == 0 {
            ops_counter.fetch_add(256, Ordering::Relaxed);
        }
    }

    // Flush remaining count.
    ops_counter.fetch_add(local_ops & 0xFF, Ordering::Relaxed);
}

// ── Benchmark runner ─────────────────────────────────────────────────

struct BenchResult {
    ops_per_sec: f64,
    ns_per_op: f64,
}

fn make_config(workers: usize) -> Config {
    let mut config = Config::default();
    config.worker.threads = workers;
    config.worker.pin_to_core = false;
    config.sq_entries = 256;
    config.recv_buffer.ring_size = 256;
    config.recv_buffer.buffer_size = 8192;
    config.max_connections = 4096;
    config.send_copy_count = 256;
    config.send_copy_slot_size = 8192;
    config
}

fn run_bench(
    mode: &str,
    addr: &str,
    num_clients: usize,
    msg_size: usize,
    warmup: Duration,
    duration: Duration,
) -> BenchResult {
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));

    // Wait for the server to be ready.
    for _ in 0..100 {
        if TcpStream::connect(addr).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Spawn client threads.
    let mut client_handles = Vec::with_capacity(num_clients);
    for _ in 0..num_clients {
        let addr = addr.to_string();
        let stop = stop.clone();
        let ops = ops.clone();
        client_handles.push(std::thread::spawn(move || {
            run_client(&addr, msg_size, stop, ops);
        }));
    }

    // Warmup phase.
    std::thread::sleep(warmup);
    ops.store(0, Ordering::Relaxed);

    // Measurement phase.
    let start = Instant::now();
    std::thread::sleep(duration);
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);

    // Collect.
    for h in client_handles {
        h.join().ok();
    }

    let total_ops = ops.load(Ordering::Relaxed);
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    let ns_per_op = if total_ops > 0 {
        elapsed.as_nanos() as f64 / total_ops as f64
    } else {
        0.0
    };

    eprintln!(
        "  {mode}: {total_ops:>10} ops in {:.2}s = {ops_per_sec:>10.0} ops/sec  ({ns_per_op:.0} ns/op)",
        elapsed.as_secs_f64()
    );

    BenchResult {
        ops_per_sec,
        ns_per_op,
    }
}

// ── Main ─────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut duration_secs = 5u64;
    let mut num_clients = 4usize;
    let mut msg_size = 64usize;
    let mut workers = 1usize;
    let mut base_port = 17171u16;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--duration" => {
                i += 1;
                duration_secs = args[i].parse().unwrap();
            }
            "--clients" => {
                i += 1;
                num_clients = args[i].parse().unwrap();
            }
            "--msg-size" => {
                i += 1;
                msg_size = args[i].parse().unwrap();
            }
            "--workers" => {
                i += 1;
                workers = args[i].parse().unwrap();
            }
            "--port" => {
                i += 1;
                base_port = args[i].parse().unwrap();
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let duration = Duration::from_secs(duration_secs);
    let warmup = Duration::from_secs(1);

    eprintln!(
        "Echo benchmark: {}B messages, {} clients, {} worker(s), {}s per test",
        msg_size, num_clients, workers, duration_secs
    );
    eprintln!();

    // ── Test 1: Callback mode ────────────────────────────────────────
    let callback_addr = format!("127.0.0.1:{}", base_port);
    eprintln!("Starting callback echo server on {callback_addr}...");

    let config = make_config(workers);
    let (shutdown, handles) = KompioBuilder::new(config)
        .bind(&callback_addr)
        .launch::<CallbackEcho>()
        .expect("failed to launch callback server");

    let callback_result = run_bench("callback", &callback_addr, num_clients, msg_size, warmup, duration);

    shutdown.shutdown();
    for h in handles {
        h.join().ok();
    }

    // Brief pause between tests to ensure port is released.
    std::thread::sleep(Duration::from_millis(100));
    eprintln!();

    // ── Test 2: Async mode ───────────────────────────────────────────
    let async_addr = format!("127.0.0.1:{}", base_port + 1);
    eprintln!("Starting async echo server on {async_addr}...");

    let config = make_config(workers);
    let (shutdown, handles) = KompioBuilder::new(config)
        .bind(&async_addr)
        .launch_async::<AsyncEcho>()
        .expect("failed to launch async server");

    let async_result = run_bench("async   ", &async_addr, num_clients, msg_size, warmup, duration);

    shutdown.shutdown();
    for h in handles {
        h.join().ok();
    }

    // ── Comparison ───────────────────────────────────────────────────
    eprintln!();
    eprintln!("═══════════════════════════════════════════════════════════");
    eprintln!(
        "  Callback: {:>10.0} ops/sec  ({:>6.0} ns/op)",
        callback_result.ops_per_sec, callback_result.ns_per_op
    );
    eprintln!(
        "  Async:    {:>10.0} ops/sec  ({:>6.0} ns/op)",
        async_result.ops_per_sec, async_result.ns_per_op
    );

    if callback_result.ops_per_sec > 0.0 {
        let ratio = async_result.ops_per_sec / callback_result.ops_per_sec;
        let pct = (1.0 - ratio) * 100.0;
        if pct.abs() < 0.5 {
            eprintln!("  Diff:     within noise (<0.5%)");
        } else if pct > 0.0 {
            eprintln!("  Diff:     async is {pct:.2}% slower");
        } else {
            eprintln!("  Diff:     async is {:.2}% faster", -pct);
        }
    }
    eprintln!("═══════════════════════════════════════════════════════════");
}
