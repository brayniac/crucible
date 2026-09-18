//! Comprehensive large value tests for the ringline (io_uring) backend.
//!
//! These tests verify correct handling of large values (256KB to 64MB+):
//! - Send modes: buffered, zerocopy, threshold
//! - Various connection and pipelining configurations
//!
//! The goal is to stress-test buffer management, coalesce buffer growth,
//! and data integrity for values that span multiple ring buffers (16KB default).

use serial_test::serial;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::{Duration, Instant};

/// Large value test sizes.
/// These sizes are chosen to stress different parts of the buffer system:
/// - 256KB: Spans ~16 ring buffers (16KB each)
/// - 512KB: Moderate large value
/// - 1MB: Common large object size
/// - 4MB: Larger than total default ring capacity requires buffer pool cycling
/// - 16MB: Stress test for coalesce buffer growth
/// - 64MB: Extreme case (marked as ignored for normal test runs)
const LARGE_SIZES: &[usize] = &[
    256 * 1024,  // 256KB
    512 * 1024,  // 512KB
    1024 * 1024, // 1MB
];

const VERY_LARGE_SIZES: &[usize] = &[
    4 * 1024 * 1024,  // 4MB
    16 * 1024 * 1024, // 16MB
];

const EXTREME_SIZES: &[usize] = &[
    64 * 1024 * 1024, // 64MB
];

/// Get an available port for testing.
fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Wait for server to be ready by attempting connections.
fn wait_for_server(addr: SocketAddr, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            // Send PING and wait for PONG to confirm a worker is processing
            stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
            if stream.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 32];
                if let Ok(n) = stream.read(&mut buf)
                    && n >= 5
                    && &buf[..5] == b"+PONG"
                {
                    return true;
                }
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Start a test server with full configuration options.
fn start_test_server_full(
    port: u16,
    worker_threads: usize,
    heap_size_mb: usize,
    segment_size_mb: usize,
) -> thread::JoinHandle<()> {
    // Max value size should be less than segment size
    let max_value_size_mb = std::cmp::max(1, segment_size_mb - 1);
    start_test_server_full_with_max_value(
        port,
        worker_threads,
        heap_size_mb,
        segment_size_mb,
        max_value_size_mb,
    )
}

/// Start a test server with full configuration options including max_value_size.
fn start_test_server_full_with_max_value(
    port: u16,
    worker_threads: usize,
    heap_size_mb: usize,
    segment_size_mb: usize,
    max_value_size_mb: usize,
) -> thread::JoinHandle<()> {
    // Metrics on an ephemeral port: unreachable, but no test below needs it.
    start_test_server_with_metrics(
        port,
        0,
        worker_threads,
        heap_size_mb,
        segment_size_mb,
        max_value_size_mb,
    )
}

/// As above, but with the admin/metrics listener on a *known* port so a test
/// can scrape it.
///
/// `metrics_port` of 0 keeps the ephemeral behaviour. This exists for #132:
/// when a connection fails, the client-side report says what that connection
/// saw, which cannot distinguish "the server never received the request" from
/// "the server answered and the response was never sent". The server's own
/// counters can.
fn start_test_server_with_metrics(
    port: u16,
    metrics_port: u16,
    worker_threads: usize,
    heap_size_mb: usize,
    segment_size_mb: usize,
    max_value_size_mb: usize,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let config_str = format!(
            r#"
            [workers]
            threads = {worker_threads}

            [cache]
            backend = "segment"
            heap_size = "{heap_size_mb}MB"
            segment_size = "{segment_size_mb}MB"
            max_value_size = "{max_value_size_mb}MB"
            hashtable_power = 20

            [[listener]]
            protocol = "resp"
            address = "127.0.0.1:{port}"

            [metrics]
            address = "127.0.0.1:{metrics_port}"
            "#,
        );

        let config: server::Config = toml::from_str(&config_str).unwrap();
        let cache = segcache::SegCache::builder()
            .heap_size(config.cache.heap_size)
            .segment_size(config.cache.segment_size)
            .hashtable_power(config.cache.hashtable_power)
            .build()
            .unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let drain_timeout = Duration::from_secs(5);

        // The admin/metrics listener is started by the server *binary*, not by
        // `async_native::run`, so a test that calls `run` directly has no
        // metrics endpoint unless it starts one itself. Only worth doing when a
        // caller asked for a known port.
        let _admin = if metrics_port != 0 {
            server::admin::start(server::admin::AdminConfig {
                address: config.metrics.address,
                shutdown: shutdown.clone(),
                cache_stats_fn: None,
            })
            .ok()
        } else {
            None
        };

        let _ = server::async_native::run(&config, cache, shutdown, drain_timeout);
    })
}

/// Generate a large value with a verifiable pattern.
/// Uses a pattern that allows easy corruption detection.
fn generate_large_value(size: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(size);
    // Use a pattern that includes the position to detect byte-level corruption
    for i in 0..size {
        // Mix of position-dependent bytes and fixed patterns
        value.push((i % 256) as u8);
    }
    value
}

/// Verify a value matches the expected pattern.
fn verify_value(data: &[u8], expected_size: usize) -> bool {
    describe_mismatch(data, expected_size).is_none()
}

/// Say *how* a retrieved value differs from the pattern, or `None` if it does
/// not.
///
/// A bool is enough to fail a test; it is not enough to read a CI log a week
/// later (#132). A short length says truncation, a single wrong byte says
/// corruption, and the offset of the first bad byte is the thing worth having
/// -- one landing on a 16 KiB boundary points at ring buffer handoff, one in
/// the middle of a buffer does not.
fn describe_mismatch(data: &[u8], expected_size: usize) -> Option<String> {
    if data.len() != expected_size {
        return Some(format!(
            "length {} bytes, expected {}",
            data.len(),
            expected_size
        ));
    }
    let (offset, &got) = data
        .iter()
        .enumerate()
        .find(|&(i, &b)| b != (i % 256) as u8)?;
    let wrong = data
        .iter()
        .enumerate()
        .filter(|&(i, &b)| b != (i % 256) as u8)
        .count();
    Some(format!(
        "{wrong} of {expected_size} bytes wrong; first at offset {offset} \
         (expected {:#04x}, got {got:#04x}; offset % 16 KiB = {})",
        (offset % 256) as u8,
        offset % (16 * 1024),
    ))
}

/// Build a RESP SET command for large values.
fn build_large_set_command(key: &str, value: &[u8]) -> Vec<u8> {
    let header = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n",
        key.len(),
        key,
        value.len()
    );
    let mut cmd = header.into_bytes();
    cmd.extend_from_slice(value);
    cmd.extend_from_slice(b"\r\n");
    cmd
}

/// Build a RESP GET command.
fn build_get_command(key: &str) -> Vec<u8> {
    format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).into_bytes()
}

/// Send a large SET command and verify OK response.
fn send_large_set(stream: &mut TcpStream, key: &str, value: &[u8]) -> Result<(), String> {
    let cmd = build_large_set_command(key, value);

    // Send in chunks to avoid overwhelming the socket buffer
    let chunk_size = 64 * 1024; // 64KB chunks
    let mut offset = 0;
    while offset < cmd.len() {
        let end = std::cmp::min(offset + chunk_size, cmd.len());
        stream
            .write_all(&cmd[offset..end])
            .map_err(|e| format!("Write failed: {}", e))?;
        offset = end;
    }

    // Read response
    let mut buf = [0u8; 64];
    let start = Instant::now();
    let timeout = Duration::from_secs(60);

    while start.elapsed() < timeout {
        match stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                let response = String::from_utf8_lossy(&buf[..n]);
                if response.contains("+OK") {
                    return Ok(());
                } else if response.contains("-") {
                    return Err(format!("SET error: {}", response));
                }
            }
            Ok(_) => return Err("Connection closed".to_string()),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(e) => return Err(format!("Read error: {}", e)),
        }
    }
    Err("Timeout waiting for SET response".to_string())
}

/// Send a GET command and read the full large value response.
fn send_large_get(
    stream: &mut TcpStream,
    key: &str,
    expected_size: usize,
) -> Result<Vec<u8>, String> {
    let cmd = build_get_command(key);
    stream
        .write_all(&cmd)
        .map_err(|e| format!("Write failed: {}", e))?;

    // Read response - need to handle RESP bulk string format
    // $<length>\r\n<data>\r\n
    let mut response = Vec::with_capacity(expected_size + 64);
    let mut buf = [0u8; 64 * 1024]; // 64KB read buffer

    let start = Instant::now();
    let timeout = Duration::from_secs(120); // Longer timeout for large values

    while start.elapsed() < timeout {
        match stream.read(&mut buf) {
            Ok(0) => return Err("Connection closed".to_string()),
            Ok(n) => {
                response.extend_from_slice(&buf[..n]);

                // Check if we have a complete response
                if let Some(value) = try_parse_bulk_string(&response, expected_size)? {
                    return Ok(value);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(e) => return Err(format!("Read error: {}", e)),
        }
    }

    Err(format!(
        "Timeout waiting for GET response (received {} bytes, expected ~{})",
        response.len(),
        expected_size + 20
    ))
}

/// Try to parse a RESP bulk string response.
/// Returns Ok(Some(value)) if complete, Ok(None) if incomplete, Err on parse error.
fn try_parse_bulk_string(data: &[u8], expected_size: usize) -> Result<Option<Vec<u8>>, String> {
    if data.is_empty() {
        return Ok(None);
    }

    // Check for null bulk string
    if data.starts_with(b"$-1\r\n") {
        return Err("Key not found (null bulk string)".to_string());
    }

    // Check for error
    if data[0] == b'-' {
        if let Some(end) = data.iter().position(|&b| b == b'\n') {
            return Err(format!(
                "Redis error: {}",
                String::from_utf8_lossy(&data[..end])
            ));
        }
        return Ok(None); // Incomplete error
    }

    // Must start with $
    if data[0] != b'$' {
        return Err(format!("Expected bulk string, got: {:?}", &data[..1]));
    }

    // Find the length line
    let len_end = match data.windows(2).position(|w| w == b"\r\n") {
        Some(pos) => pos,
        None => return Ok(None), // Incomplete header
    };

    let len_str = std::str::from_utf8(&data[1..len_end])
        .map_err(|e| format!("Invalid UTF-8 in length: {}", e))?;

    let declared_len: usize = len_str
        .parse()
        .map_err(|e| format!("Invalid length '{}': {}", len_str, e))?;

    // Verify declared length matches expected
    if declared_len != expected_size {
        return Err(format!(
            "Length mismatch: declared {}, expected {}",
            declared_len, expected_size
        ));
    }

    // Check if we have the full data
    let data_start = len_end + 2;
    let data_end = data_start + declared_len;
    let total_len = data_end + 2; // +2 for trailing \r\n

    if data.len() < total_len {
        return Ok(None); // Still receiving
    }

    // Verify trailing CRLF
    if data[data_end] != b'\r' || data[data_end + 1] != b'\n' {
        return Err("Missing trailing CRLF".to_string());
    }

    Ok(Some(data[data_start..data_end].to_vec()))
}

/// Configuration for large value tests.
struct LargeValueTestConfig {
    sizes: &'static [usize],
    heap_size_mb: usize,
    segment_size_mb: usize,
    max_value_size_mb: usize,
}

impl Default for LargeValueTestConfig {
    fn default() -> Self {
        Self {
            sizes: LARGE_SIZES,
            heap_size_mb: 256,
            segment_size_mb: 32,
            max_value_size_mb: 31, // Must be less than segment_size
        }
    }
}

/// Run a large value test with the given configuration.
fn run_large_value_test(config: LargeValueTestConfig) {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_full_with_max_value(
        port,
        2, // 2 worker threads
        config.heap_size_mb,
        config.segment_size_mb,
        config.max_value_size_mb,
    );

    assert!(
        wait_for_server(addr, Duration::from_secs(10)),
        "Server failed to start"
    );

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(120)))
        .unwrap();

    for &size in config.sizes {
        let key = format!("largekey_{}", size);
        let value = generate_large_value(size);

        println!("Testing {}: {} bytes", key, size);

        // SET the large value
        send_large_set(&mut stream, &key, &value)
            .unwrap_or_else(|e| panic!("SET failed for {} ({} bytes): {}", key, size, e));

        // GET the large value back
        let retrieved = send_large_get(&mut stream, &key, size)
            .unwrap_or_else(|e| panic!("GET failed for {} ({} bytes): {}", key, size, e));

        // Verify data integrity
        assert!(
            verify_value(&retrieved, size),
            "Data corruption detected for {} ({} bytes)",
            key,
            size
        );

        println!("  PASSED: {} bytes verified", size);
    }
}

/// One connection's turn through the concurrent test.
///
/// The assertion used to print a count and nothing else, which is how #132
/// came to be reported with "1 errors (7 successes)" and no way to tell what
/// the one was. Every field here exists to be printed on failure.
struct ConnOutcome {
    conn_id: usize,
    /// How far it got: `connect`, `SET`, `GET`, `verify`, or `ok`.
    stage: &'static str,
    /// Wall time in the SET, and in the GET. A failure at the 120s read
    /// timeout reads very differently from one at 0.02s, and the successes'
    /// timings are the baseline that says which it was.
    set_elapsed: Option<Duration>,
    get_elapsed: Option<Duration>,
    /// Exactly what the failing operation returned; `None` on success.
    error: Option<String>,
}

impl ConnOutcome {
    fn new(conn_id: usize) -> Self {
        Self {
            conn_id,
            stage: "connect",
            set_elapsed: None,
            get_elapsed: None,
            error: None,
        }
    }

    fn failed(mut self, stage: &'static str, error: String) -> Self {
        self.stage = stage;
        self.error = Some(error);
        self
    }

    fn succeeded(mut self) -> Self {
        self.stage = "ok";
        self
    }

    fn line(&self) -> String {
        fn elapsed(d: Option<Duration>) -> String {
            d.map_or_else(|| "-".to_string(), |d| format!("{:.3}s", d.as_secs_f64()))
        }
        format!(
            "  conn {:>2}  {:<7}  set={:>8}  get={:>8}  {}",
            self.conn_id,
            self.stage,
            elapsed(self.set_elapsed),
            elapsed(self.get_elapsed),
            match &self.error {
                Some(e) => format!("ERROR: {e}"),
                None => String::new(),
            },
        )
        .trim_end()
        .to_string()
    }
}

/// SET then GET then verify one large value on its own connection, reporting
/// where it got to rather than just whether it got there.
fn run_one_connection(conn_id: usize, addr: SocketAddr, value_size: usize) -> ConnOutcome {
    let outcome = ConnOutcome::new(conn_id);

    let mut stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => return outcome.failed("connect", format!("{e} (kind {:?})", e.kind())),
    };

    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(120)))
        .unwrap();

    let key = format!("concurrent_key_{conn_id}");
    let value = generate_large_value(value_size);

    let mut outcome = outcome;
    let started = Instant::now();
    let set = send_large_set(&mut stream, &key, &value);
    outcome.set_elapsed = Some(started.elapsed());
    if let Err(e) = set {
        return outcome.failed("SET", e);
    }

    let started = Instant::now();
    let get = send_large_get(&mut stream, &key, value_size);
    outcome.get_elapsed = Some(started.elapsed());
    match get {
        Ok(retrieved) => match describe_mismatch(&retrieved, value_size) {
            Some(how) => outcome.failed("verify", how),
            None => outcome.succeeded(),
        },
        Err(e) => outcome.failed("GET", e),
    }
}

/// The whole picture, printed by whichever assertion fires.
///
/// Every connection gets a line, not just the failing ones: the successes'
/// timings are what say whether a failure stalled or flapped.
/// Scrape the server's own counters, for a failure report that can say more
/// than what one client saw.
///
/// Raw HTTP/1.0 over TCP rather than a client crate: this is a test, the
/// endpoint is on loopback, and a dev-dependency for six lines is not worth
/// it. Returns `None` rather than failing -- this runs only on a path that has
/// already failed, and losing the scrape must not replace the real diagnosis
/// with a scrape error.
fn scrape_metrics(metrics_port: u16) -> Option<String> {
    let addr: SocketAddr = format!("127.0.0.1:{metrics_port}").parse().ok()?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    stream
        .write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

/// The counters that discriminate between the ways a GET can produce nothing.
///
/// `cache_gets` is the load-bearing one. If it counted every GET the test
/// issued, the server read and parsed the request, and the response either was
/// never produced or never sent. If it is short, the request never reached
/// command dispatch at all -- a recv or backpressure problem, not a send one.
/// `cache_hits`/`cache_misses` separate a third possibility: the item was
/// evicted between the SET and the GET, and the miss path failed to answer.
///
/// **`cache_sets` reads 0 here and that is not a symptom.** The streaming
/// large-value SET path in `connection.rs` never increments it -- only
/// `execute.rs` does, and these values do not go through it. See #145. It is
/// printed anyway because a *non-zero* value would mean a small-value SET took
/// a path this test does not expect.
fn metrics_summary(body: &str) -> String {
    const KEYS: [&str; 8] = [
        "cache_gets",
        "cache_sets",
        "cache_hits",
        "cache_misses",
        "protocol_errors",
        "set_errors",
        "connections_accepted",
        "connections_active",
    ];
    let mut lines: Vec<String> = Vec::new();
    for key in KEYS {
        for line in body.lines() {
            let line = line.trim();
            if line.starts_with('#') {
                continue;
            }
            if line.split_whitespace().next().is_some_and(|n| n == key) {
                lines.push(format!("  {line}"));
            }
        }
    }
    if lines.is_empty() {
        "  (no counters matched; endpoint reachable but body unrecognised)".to_string()
    } else {
        lines.join("\n")
    }
}

fn failure_report(connections: usize, value_size: usize, outcomes: &[ConnOutcome]) -> String {
    let err_count = outcomes.iter().filter(|o| o.error.is_some()).count();
    let success_count = outcomes.len() - err_count;
    // A thread that panicked outright leaves no outcome at all, which is why
    // the reported total is printed alongside the expected one.
    std::iter::once(format!(
        "{value_size}-byte value on each of {connections} connections; \
         {} reported, {err_count} failed, {success_count} ok",
        outcomes.len()
    ))
    .chain(outcomes.iter().map(ConnOutcome::line))
    .collect::<Vec<_>>()
    .join("\n")
}

/// Run a concurrent large value test with multiple connections.
fn run_concurrent_large_value_test(connections: usize, value_size: usize) {
    let port = get_available_port();
    let metrics_port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Larger heap for concurrent tests
    let heap_size_mb = std::cmp::max(512, (value_size * connections * 2) / (1024 * 1024) + 128);

    let _server_handle = start_test_server_with_metrics(
        port,
        metrics_port,
        4, // More workers for concurrent load
        heap_size_mb,
        64,     // 64MB segments
        64 - 1, // max_value_size, as start_test_server_full derives it
    );

    assert!(
        wait_for_server(addr, Duration::from_secs(10)),
        "Server failed to start"
    );

    let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));

    let mut handles = Vec::new();

    for conn_id in 0..connections {
        let outcomes = Arc::clone(&outcomes);

        let handle = thread::spawn(move || {
            let outcome = run_one_connection(conn_id, addr, value_size);
            if let Some(error) = &outcome.error {
                eprintln!("Connection {conn_id} failed at {}: {error}", outcome.stage);
            }
            outcomes.lock().unwrap().push(outcome);
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }

    let mut outcomes = Arc::try_unwrap(outcomes)
        .unwrap_or_else(|_| unreachable!("every worker thread has been joined"))
        .into_inner()
        .unwrap();
    outcomes.sort_by_key(|o| o.conn_id);

    let err_count = outcomes.iter().filter(|o| o.error.is_some()).count();
    let success_count = outcomes.iter().filter(|o| o.error.is_none()).count();

    let mut report = failure_report(connections, value_size, &outcomes);

    // Only on failure: the scrape costs a connection and a read, and on the
    // happy path there is nothing to explain. See #132 -- the client-side
    // report alone cannot tell "the server never saw the request" from "the
    // server answered and the response never left".
    if err_count != 0 {
        let scraped = match scrape_metrics(metrics_port) {
            Some(body) => metrics_summary(&body),
            None => format!(
                "  (metrics endpoint 127.0.0.1:{metrics_port} unreachable; \
                 the server may be wedged, which is itself a datum)"
            ),
        };
        report = format!("{report}\nserver counters:\n{scraped}");
    }

    // Deliberately still exact. The point of #132 is that an intermittent red
    // must be readable, not that it must be tolerated -- a retry loop or a
    // loosened threshold here would bury whatever produces the error under
    // load, which is the thing worth knowing.
    assert_eq!(err_count, 0, "Concurrent test had errors\n{report}");
    assert_eq!(
        success_count, connections,
        "Not all connections succeeded\n{report}"
    );
}

// =============================================================================
// Large Value Tests
// =============================================================================

#[test]
#[serial]
fn test_uring_large_values_256k_to_1m() {
    run_large_value_test(LargeValueTestConfig::default());
}

// Ignored: passes locally but times out in CI due to resource pressure on
// GitHub Actions runners. The 256KB-1MB test covers the same code paths.
#[test]
#[serial]
#[ignore]
fn test_uring_large_values_4m_to_16m() {
    run_large_value_test(LargeValueTestConfig {
        sizes: VERY_LARGE_SIZES,
        heap_size_mb: 512,
        segment_size_mb: 64,
        max_value_size_mb: 63,
    });
}

#[test]
#[serial]
#[ignore] // Expensive test
fn test_uring_large_values_64m() {
    run_large_value_test(LargeValueTestConfig {
        sizes: EXTREME_SIZES,
        heap_size_mb: 1024,
        segment_size_mb: 128,
        max_value_size_mb: 127,
    });
}

#[test]
#[serial]
fn test_uring_concurrent_large_values_1m() {
    run_concurrent_large_value_test(8, 1024 * 1024);
}

#[test]
#[serial]
#[ignore] // Expensive test
fn test_uring_concurrent_large_values_4m() {
    run_concurrent_large_value_test(4, 4 * 1024 * 1024);
}

// =============================================================================
// Edge Case Tests
// =============================================================================

/// Test rapid large value SET/GET cycles to stress buffer recycling.
#[test]
#[serial]
fn test_uring_rapid_large_value_cycles() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_full(port, 2, 256, 32);

    assert!(wait_for_server(addr, Duration::from_secs(10)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(60)))
        .unwrap();

    let size = 512 * 1024; // 512KB

    for cycle in 0..20 {
        let key = "rapid_cycle_key";
        let value = generate_large_value(size);

        send_large_set(&mut stream, key, &value)
            .unwrap_or_else(|e| panic!("SET failed on cycle {}: {}", cycle, e));

        let retrieved = send_large_get(&mut stream, key, size)
            .unwrap_or_else(|e| panic!("GET failed on cycle {}: {}", cycle, e));

        assert!(
            verify_value(&retrieved, size),
            "Data corruption on cycle {}",
            cycle
        );
    }
}

/// Test increasing value sizes in sequence.
/// This stresses coalesce buffer growth.
#[test]
#[serial]
fn test_uring_increasing_value_sizes() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Use larger segment/max_value to support up to 2MB values
    let _server_handle = start_test_server_full_with_max_value(
        port, 2, 256, 8, 7, // 8MB segment, 7MB max value
    );

    assert!(wait_for_server(addr, Duration::from_secs(10)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(120)))
        .unwrap();

    let sizes = [
        1024,
        16 * 1024,
        32 * 1024,
        64 * 1024,
        128 * 1024,
        256 * 1024,
        512 * 1024,
        1024 * 1024,
        2 * 1024 * 1024,
    ];

    for &size in &sizes {
        let key = format!("increasing_key_{}", size);
        let value = generate_large_value(size);

        println!("Testing increasing size: {} bytes", size);

        send_large_set(&mut stream, &key, &value)
            .unwrap_or_else(|e| panic!("SET failed for size {}: {}", size, e));

        let retrieved = send_large_get(&mut stream, &key, size)
            .unwrap_or_else(|e| panic!("GET failed for size {}: {}", size, e));

        assert!(
            verify_value(&retrieved, size),
            "Data corruption for size {}",
            size
        );
    }
}

/// Test alternating between small and large values.
/// This tests buffer shrink/grow behavior.
#[test]
#[serial]
fn test_uring_alternating_value_sizes() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_full(port, 2, 256, 32);

    assert!(wait_for_server(addr, Duration::from_secs(10)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(60)))
        .unwrap();

    for i in 0..10 {
        let (key, size) = if i % 2 == 0 {
            (format!("small_key_{}", i), 64)
        } else {
            (format!("large_key_{}", i), 1024 * 1024)
        };

        let value = generate_large_value(size);

        send_large_set(&mut stream, &key, &value)
            .unwrap_or_else(|e| panic!("SET failed for key {} (size {}): {}", key, size, e));

        let retrieved = send_large_get(&mut stream, &key, size)
            .unwrap_or_else(|e| panic!("GET failed for key {} (size {}): {}", key, size, e));

        assert!(
            verify_value(&retrieved, size),
            "Data corruption for key {} (size {})",
            key,
            size
        );
    }
}

/// Test values at exact buffer boundary sizes.
/// Ring buffers are 16KB, so test around that boundary.
#[test]
#[serial]
fn test_uring_buffer_boundary_sizes() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _server_handle = start_test_server_full(port, 2, 256, 32);

    assert!(wait_for_server(addr, Duration::from_secs(10)));

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(60)))
        .unwrap();

    let sizes = [
        16 * 1024 - 1,
        16 * 1024,
        16 * 1024 + 1,
        32 * 1024 - 1,
        32 * 1024,
        32 * 1024 + 1,
        64 * 1024 - 1,
        64 * 1024,
        64 * 1024 + 1,
    ];

    for &size in &sizes {
        let key = format!("boundary_key_{}", size);
        let value = generate_large_value(size);

        println!("Testing boundary size: {} bytes", size);

        send_large_set(&mut stream, &key, &value)
            .unwrap_or_else(|e| panic!("SET failed for size {}: {}", size, e));

        let retrieved = send_large_get(&mut stream, &key, size)
            .unwrap_or_else(|e| panic!("GET failed for size {}: {}", size, e));

        assert!(
            verify_value(&retrieved, size),
            "Data corruption for size {}",
            size
        );
    }
}

// =============================================================================
// Diagnosability of the concurrent failure path (#132)
// =============================================================================
//
// These two run anywhere -- they are about what the assertion says, not about
// io_uring, and the point of #132 is that the next occurrence has to be
// readable from the CI log alone.

/// The failure report must name the connection, the stage it died at, and
/// what the operation actually returned.
#[test]
fn the_failure_report_names_what_actually_failed() {
    let outcomes = vec![
        ConnOutcome {
            conn_id: 0,
            stage: "ok",
            set_elapsed: Some(Duration::from_millis(12)),
            get_elapsed: Some(Duration::from_millis(31)),
            error: None,
        },
        ConnOutcome {
            conn_id: 3,
            stage: "GET",
            set_elapsed: Some(Duration::from_millis(20)),
            get_elapsed: Some(Duration::from_secs(120)),
            error: Some("Timeout waiting for GET response (received 65536 bytes)".to_string()),
        },
    ];

    let report = failure_report(8, 1024 * 1024, &outcomes);

    assert!(report.contains("2 reported, 1 failed, 1 ok"), "{report}");
    assert!(
        report.contains("conn  3"),
        "the failing connection: {report}"
    );
    assert!(report.contains("GET"), "the stage it died at: {report}");
    assert!(
        report.contains("Timeout waiting for GET response (received 65536 bytes)"),
        "what the operation returned: {report}"
    );
    assert!(
        report.contains("120.000s") && report.contains("0.031s"),
        "the stall and the baseline it stalled against: {report}"
    );
    assert!(
        report.contains("conn  0"),
        "the successes are the baseline: {report}"
    );
}

/// A corrupt value must say how it was corrupt, not just that it was.
#[test]
fn a_mismatch_describes_itself() {
    let size = 32 * 1024;
    let good = generate_large_value(size);
    assert_eq!(describe_mismatch(&good, size), None);

    let mut truncated = good.clone();
    truncated.truncate(size - 1);
    let how = describe_mismatch(&truncated, size).expect("a short value is a mismatch");
    assert!(how.contains(&format!("length {} bytes", size - 1)), "{how}");

    let mut corrupt = good.clone();
    corrupt[16 * 1024] ^= 0xff;
    let how = describe_mismatch(&corrupt, size).expect("a flipped byte is a mismatch");
    assert!(how.contains("first at offset 16384"), "{how}");
    assert!(how.contains("offset % 16 KiB = 0"), "{how}");
    assert!(how.starts_with("1 of "), "{how}");
}
