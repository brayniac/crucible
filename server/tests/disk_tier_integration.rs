//! Integration tests for the Direct I/O disk tier.
//!
//! Exercises the full disk demotion/promotion path:
//! 1. Start a server with a small RAM heap and a Direct I/O disk tier.
//! 2. Write enough data to trigger eviction/demotion to disk.
//! 3. Read back keys that were demoted to verify disk reads work.

use serial_test::serial;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::{Duration, Instant};

fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn wait_for_server(addr: SocketAddr, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
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

fn send_set(stream: &mut TcpStream, key: &str, value: &str) -> bool {
    let cmd = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value
    );
    if stream.write_all(cmd.as_bytes()).is_err() {
        return false;
    }

    let mut buf = [0u8; 64];
    match stream.read(&mut buf) {
        Ok(n) if n > 0 => {
            let response = String::from_utf8_lossy(&buf[..n]);
            response.contains("+OK")
        }
        _ => false,
    }
}

fn send_get(stream: &mut TcpStream, key: &str) -> Option<String> {
    let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    if stream.write_all(cmd.as_bytes()).is_err() {
        return None;
    }

    let mut buf = vec![0u8; 256 * 1024];
    let mut total_read = 0;
    let start = Instant::now();

    while start.elapsed() < Duration::from_secs(5) {
        match stream.read(&mut buf[total_read..]) {
            Ok(0) => break,
            Ok(n) => {
                total_read += n;
                // Check if we have a complete RESP response
                if total_read >= 5 && &buf[..3] == b"$-1" {
                    return None; // null bulk string (miss)
                }
                if total_read >= 2 && buf[total_read - 2] == b'\r' && buf[total_read - 1] == b'\n' {
                    let resp = String::from_utf8_lossy(&buf[..total_read]);
                    // Parse bulk string: $<len>\r\n<data>\r\n
                    if resp.starts_with('$')
                        && let Some(header_end) = resp.find("\r\n")
                        && let Ok(len) = resp[1..header_end].parse::<i64>()
                    {
                        if len < 0 {
                            return None;
                        }
                        let body_start = header_end + 2;
                        let needed = body_start + len as usize + 2;
                        if total_read >= needed {
                            return Some(resp[body_start..body_start + len as usize].to_string());
                        }
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(_) => return None,
        }
    }

    None
}

/// Start a callback server with a small RAM heap + Direct I/O disk tier.
fn start_disk_test_server_callback(
    port: u16,
    disk_path: &std::path::Path,
) -> thread::JoinHandle<()> {
    let disk_path = disk_path.to_path_buf();
    thread::spawn(move || {
        let config_str = format!(
            r#"
            [workers]
            threads = 1

            [cache]
            backend = "segment"
            heap_size = "4MB"
            segment_size = "1MB"
            max_value_size = "64KB"
            hashtable_power = 14

            [cache.disk]
            enabled = true
            io_backend = "directio"
            path = "{}"
            size = "8MB"
            promotion_threshold = 2

            [[listener]]
            protocol = "resp"
            address = "127.0.0.1:{port}"

            [metrics]
            address = "127.0.0.1:0"
            "#,
            disk_path.display(),
        );

        let config: server::Config = toml::from_str(&config_str).unwrap();

        let segment_count = config.cache.disk.as_ref().unwrap().size / config.cache.segment_size;
        let cache = segcache::SegCache::builder()
            .heap_size(config.cache.heap_size)
            .segment_size(config.cache.segment_size)
            .hashtable_power(config.cache.hashtable_power)
            .io_uring_disk_tier(segcache::IoUringDiskTierConfig {
                segment_count,
                block_size: 4096,
                promotion_threshold: 2,
                ..Default::default()
            })
            .build()
            .unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let drain_timeout = Duration::from_secs(5);

        let _ = server::async_native::run(&config, cache, shutdown, drain_timeout);
    })
}

/// Start an async server with a small RAM heap + Direct I/O disk tier.
fn start_disk_test_server_async(port: u16, disk_path: &std::path::Path) -> thread::JoinHandle<()> {
    start_disk_test_server_with(port, disk_path, "resp", "64KB")
}

/// As `start_disk_test_server_async`, speaking `protocol` and accepting
/// values up to `max_value_size`.
fn start_disk_test_server_with(
    port: u16,
    disk_path: &std::path::Path,
    protocol: &'static str,
    max_value_size: &'static str,
) -> thread::JoinHandle<()> {
    let disk_path = disk_path.to_path_buf();
    thread::spawn(move || {
        let config_str = format!(
            r#"
            [workers]
            threads = 1

            [cache]
            backend = "segment"
            heap_size = "4MB"
            segment_size = "1MB"
            max_value_size = "{max_value_size}"
            hashtable_power = 14

            [cache.disk]
            enabled = true
            io_backend = "directio"
            path = "{}"
            size = "8MB"
            promotion_threshold = 2

            [[listener]]
            protocol = "{protocol}"
            address = "127.0.0.1:{port}"

            [metrics]
            address = "127.0.0.1:0"
            "#,
            disk_path.display(),
        );

        let config: server::Config = toml::from_str(&config_str).unwrap();

        let segment_count = config.cache.disk.as_ref().unwrap().size / config.cache.segment_size;
        let cache = segcache::SegCache::builder()
            .heap_size(config.cache.heap_size)
            .segment_size(config.cache.segment_size)
            .hashtable_power(config.cache.hashtable_power)
            .io_uring_disk_tier(segcache::IoUringDiskTierConfig {
                segment_count,
                block_size: 4096,
                promotion_threshold: 2,
                ..Default::default()
            })
            .build()
            .unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let drain_timeout = Duration::from_secs(5);

        let _ = server::async_native::run(&config, cache, shutdown, drain_timeout);
    })
}

/// Generate a value of the given size with a key-specific pattern for verification.
fn make_value(key_id: usize, size: usize) -> String {
    let seed = format!("val_{}_", key_id);
    seed.chars().cycle().take(size).collect()
}

/// Run the disk tier test: fill RAM, trigger demotion, read back demoted keys.
fn run_disk_tier_test(port: u16, addr: SocketAddr) {
    // 4MB RAM with 1MB segments = 4 segments.
    // With S3-FIFO (10% small queue), effective capacity is ~3.6 segments.
    // Each item is ~256B, so ~14000 items per segment, ~50000 total in RAM.
    // We write far more than that to guarantee demotion to disk.

    let value_size = 256;
    let num_keys = 2000;

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    // Phase 1: Write keys to fill RAM and trigger disk demotion.
    for i in 0..num_keys {
        let key = format!("dkey:{}", i);
        let value = make_value(i, value_size);
        assert!(
            send_set(&mut stream, &key, &value),
            "SET failed for key {}",
            i
        );
    }

    // Phase 2: Read back ALL keys. Some should come from disk.
    let mut hits = 0;
    let mut correct = 0;

    for i in 0..num_keys {
        let key = format!("dkey:{}", i);
        let expected = make_value(i, value_size);
        if let Some(got) = send_get(&mut stream, &key) {
            hits += 1;
            if got == expected {
                correct += 1;
            }
        }
    }

    eprintln!(
        "Disk tier test: {} keys written, {} hits, {} correct (port {})",
        num_keys, hits, correct, port
    );

    // We expect at least some hits (RAM + disk) and all hits should have correct values.
    assert!(
        hits > 0,
        "Expected at least some hits from RAM + disk, got 0"
    );
    assert_eq!(
        hits, correct,
        "Some returned values were corrupted: {} hits but only {} correct",
        hits, correct
    );

    drop(stream);
}

struct TempDiskFile {
    path: std::path::PathBuf,
}

impl TempDiskFile {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("crucible-test-{}-{}.dat", name, std::process::id()));
        // Pre-create the file
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("failed to create temp disk file");
        file.set_len(8 * 1024 * 1024).unwrap(); // 8MB
        drop(file);
        Self { path }
    }
}

impl Drop for TempDiskFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[test]
#[serial]
fn test_disk_tier_callback_basic() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let disk_file = TempDiskFile::new("callback");

    let _server_handle = start_disk_test_server_callback(port, &disk_file.path);

    assert!(
        wait_for_server(addr, Duration::from_secs(10)),
        "Callback server with disk tier failed to start"
    );

    run_disk_tier_test(port, addr);
}

#[test]
#[serial]
fn test_disk_tier_async_basic() {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let disk_file = TempDiskFile::new("async");

    let _server_handle = start_disk_test_server_async(port, &disk_file.path);

    assert!(
        wait_for_server(addr, Duration::from_secs(10)),
        "Async server with disk tier failed to start"
    );

    run_disk_tier_test(port, addr);
}

/// SET `num_keys` values of `value_size` bytes, 6MB in all, through a 4MB
/// RAM tier so the earliest are demoted to the 8MB disk tier, then read every
/// one back. Every value comes back intact and some are served from disk.
fn serves_values_from_disk(name: &str, value_size: usize, num_keys: usize) {
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let disk_file = TempDiskFile::new(name);
    let _server_handle = start_disk_test_server_async(port, &disk_file.path);
    assert!(
        wait_for_server(addr, Duration::from_secs(10)),
        "Async server with disk tier failed to start"
    );

    let mut stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    for i in 0..num_keys {
        let key = format!("{name}:{i}");
        assert!(
            send_set(&mut stream, &key, &make_value(i, value_size)),
            "SET failed for key {i}"
        );
    }
    // Let the disk tier's flushes complete, so demoted items are read from
    // disk rather than from their staging buffers.
    thread::sleep(Duration::from_millis(500));

    let disk_hits_before = server::metrics::DISK_READ_HITS.value();
    let mut hits = 0;
    for i in 0..num_keys {
        if let Some(got) = send_get(&mut stream, &format!("{name}:{i}")) {
            hits += 1;
            assert_eq!(got, make_value(i, value_size), "key {i} came back wrong");
        }
    }
    let disk_hits = server::metrics::DISK_READ_HITS.value() - disk_hits_before;
    eprintln!("{name}: {hits} hits, {disk_hits} served from disk");
    assert_eq!(hits, num_keys, "a stored value was not served");
    assert!(disk_hits > 0, "no value was served from disk");
}

/// A value that extends past the first disk read is read again in full and
/// served from disk.
#[test]
#[serial]
fn test_disk_tier_serves_values_larger_than_a_block() {
    serves_values_from_disk("large", 20 * 1024, 300);
}

/// A value that fits the first disk read, but is large enough to be sent
/// without a copy into the write buffer, is served from disk.
#[test]
#[serial]
fn test_disk_tier_serves_values_within_a_block() {
    serves_values_from_disk("block", 2 * 1024, 3000);
}

fn send_del(stream: &mut TcpStream, key: &str) -> Option<i64> {
    let cmd = format!("*2\r\n$3\r\nDEL\r\n${}\r\n{}\r\n", key.len(), key);
    stream.write_all(cmd.as_bytes()).ok()?;
    let mut buf = [0u8; 64];
    let n = stream.read(&mut buf).ok()?;
    let response = std::str::from_utf8(&buf[..n]).ok()?;
    response.strip_prefix(':')?.trim_end().parse().ok()
}

/// A connected client for one of the disk-tier servers.
fn connect(addr: SocketAddr) -> TcpStream {
    assert!(
        wait_for_server(addr, Duration::from_secs(10)),
        "Async server with disk tier failed to start"
    );
    let stream = TcpStream::connect(addr).expect("Failed to connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
}

/// Read until the bytes read end with `suffix`, or fail after 10 seconds.
fn read_until(stream: &mut TcpStream, suffix: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    let start = Instant::now();
    while !buf.ends_with(suffix) {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "no complete reply"
        );
        match stream.read(&mut chunk) {
            Ok(0) => panic!("connection closed"),
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => panic!("read failed: {e}"),
        }
    }
    buf
}

/// Write `num_keys` values of `value_size` bytes over RESP, then wait for the
/// disk tier's flushes, so the items demoted to disk are only on disk.
fn fill_to_disk(stream: &mut TcpStream, name: &str, value_size: usize, num_keys: usize) {
    for i in 0..num_keys {
        assert!(send_set(
            stream,
            &format!("{name}:{i}"),
            &make_value(i, value_size)
        ));
    }
    thread::sleep(Duration::from_millis(500));
}

/// Overwrites and deletes of keys whose items are only on disk read each
/// key from disk first. An overwrite that is served afterwards is never the
/// older value, most overwrites are served, a discarded attempt is not
/// counted as a SET error, and a deleted key stays deleted.
#[test]
#[serial]
fn test_disk_tier_overwrites_and_deletes_keys_only_on_disk() {
    let name = "rewrite";
    let (value_size, num_keys) = (2 * 1024, 3000);
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let disk_file = TempDiskFile::new(name);
    let _server_handle = start_disk_test_server_async(port, &disk_file.path);
    let mut stream = connect(addr);

    let key = |i: usize| format!("{name}:{i}");
    fill_to_disk(&mut stream, name, value_size, num_keys);

    let key_reads_before = server::metrics::DISK_KEY_READS.value();
    let set_errors_before = server::metrics::SET_ERRORS.value();
    let newer = |i: usize| make_value(i + num_keys, value_size);
    for i in 0..num_keys {
        assert!(send_set(&mut stream, &key(i), &newer(i)), "SET {i} failed");
    }
    let key_reads = server::metrics::DISK_KEY_READS.value() - key_reads_before;
    eprintln!("{name}: {key_reads} keys read from disk for overwrites");
    assert!(key_reads > 0, "no overwrite read a key from disk");
    assert_eq!(
        server::metrics::SET_ERRORS.value(),
        set_errors_before,
        "an overwrite that waited on a key read counted as a SET error"
    );
    thread::sleep(Duration::from_millis(500));

    let mut served = Vec::new();
    for i in 0..num_keys {
        if let Some(got) = send_get(&mut stream, &key(i)) {
            assert_eq!(
                got,
                newer(i),
                "key {i} served a value older than its overwrite"
            );
            served.push(i);
        }
    }
    assert!(
        served.len() > num_keys / 2,
        "only {} of {num_keys} overwrites were served",
        served.len()
    );
    for &i in served.iter().step_by(2) {
        assert_eq!(send_del(&mut stream, &key(i)), Some(1), "DEL {i}");
        assert_eq!(
            send_get(&mut stream, &key(i)),
            None,
            "deleted key {i} came back"
        );
    }
}

/// The same over memcache ASCII: overwrites of keys only on disk read the
/// key first, and a served value is never the older one.
#[test]
#[serial]
fn test_disk_tier_memcache_overwrites_keys_only_on_disk() {
    let name = "mcrewrite";
    let (value_size, num_keys) = (2 * 1024, 3000);
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let disk_file = TempDiskFile::new(name);
    let _server_handle = start_disk_test_server_with(port, &disk_file.path, "memcache", "64KB");
    let mut stream = connect(addr);

    let key = |i: usize| format!("{name}:{i}");
    let set = |stream: &mut TcpStream, key: &str, value: &str| {
        let cmd = format!("set {key} 0 0 {}\r\n{value}\r\n", value.len());
        stream.write_all(cmd.as_bytes()).unwrap();
        assert_eq!(read_until(stream, b"\r\n"), b"STORED\r\n", "set {key}");
    };
    for i in 0..num_keys {
        set(&mut stream, &key(i), &make_value(i, value_size));
    }
    thread::sleep(Duration::from_millis(500));

    let key_reads_before = server::metrics::DISK_KEY_READS.value();
    let newer = |i: usize| make_value(i + num_keys, value_size);
    for i in 0..num_keys {
        set(&mut stream, &key(i), &newer(i));
    }
    assert!(
        server::metrics::DISK_KEY_READS.value() > key_reads_before,
        "no overwrite read a key from disk"
    );
    thread::sleep(Duration::from_millis(500));

    let mut served = 0;
    for i in 0..num_keys {
        stream
            .write_all(format!("get {}\r\n", key(i)).as_bytes())
            .unwrap();
        let reply = read_until(&mut stream, b"END\r\n");
        if reply != b"END\r\n" {
            let expected = format!(
                "VALUE {} 0 {}\r\n{}\r\nEND\r\n",
                key(i),
                value_size,
                newer(i)
            );
            assert_eq!(
                String::from_utf8_lossy(&reply),
                expected,
                "key {i} served a value older than its overwrite"
            );
            served += 1;
        }
    }
    assert!(
        served > num_keys / 2,
        "only {served} of {num_keys} overwrites were served"
    );
}

/// A streamed SET (a value large enough to be received into its segment
/// directly) over a key whose item is only on disk waits on the key read at
/// its commit, then stores: the served value is the streamed one.
#[test]
#[serial]
fn test_disk_tier_streamed_overwrites_of_keys_only_on_disk() {
    let name = "streamed";
    let (value_size, num_keys) = (2 * 1024, 3000);
    let large = 70 * 1024;
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let disk_file = TempDiskFile::new(name);
    let _server_handle = start_disk_test_server_with(port, &disk_file.path, "resp", "256KB");
    let mut stream = connect(addr);

    let key = |i: usize| format!("{name}:{i}");
    fill_to_disk(&mut stream, name, value_size, num_keys);

    let key_reads_before = server::metrics::DISK_KEY_READS.value();
    let overwritten: Vec<usize> = (0..num_keys).step_by(50).collect();
    for &i in &overwritten {
        assert!(
            send_set(&mut stream, &key(i), &make_value(i, large)),
            "SET {i}"
        );
    }
    assert!(
        server::metrics::DISK_KEY_READS.value() > key_reads_before,
        "no streamed overwrite read a key from disk"
    );

    let mut served = 0;
    for &i in &overwritten {
        if let Some(got) = send_get(&mut stream, &key(i)) {
            assert_eq!(
                got.len(),
                large,
                "key {i} served a value older than its overwrite"
            );
            assert_eq!(got, make_value(i, large));
            served += 1;
        }
    }
    assert!(
        served > overwritten.len() / 2,
        "only {served} of {} streamed overwrites were served",
        overwritten.len()
    );
}

/// Pipelined SET and GET pairs over keys only on disk come back in order:
/// each GET answers with the value its own SET just stored.
#[test]
#[serial]
fn test_disk_tier_pipelined_overwrites_stay_in_order() {
    let name = "pipeline";
    let (value_size, num_keys) = (2 * 1024, 3000);
    let port = get_available_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let disk_file = TempDiskFile::new(name);
    let _server_handle = start_disk_test_server_async(port, &disk_file.path);
    let mut stream = connect(addr);
    fill_to_disk(&mut stream, name, value_size, num_keys);

    let key_reads_before = server::metrics::DISK_KEY_READS.value();
    let batch = 8;
    for start in (0..num_keys).step_by(batch) {
        let mut request = Vec::new();
        let mut expected = Vec::new();
        for i in start..(start + batch).min(num_keys) {
            let (key, value) = (format!("{name}:{i}"), format!("new{i}"));
            request.extend_from_slice(
                format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${}\r\n{value}\r\n",
                    key.len(),
                    value.len()
                )
                .as_bytes(),
            );
            request.extend_from_slice(
                format!("*2\r\n$3\r\nGET\r\n${}\r\n{key}\r\n", key.len()).as_bytes(),
            );
            expected.extend_from_slice(b"+OK\r\n");
            expected.extend_from_slice(format!("${}\r\n{value}\r\n", value.len()).as_bytes());
        }
        stream.write_all(&request).unwrap();
        let reply = read_until(&mut stream, &expected[expected.len() - 8..]);
        assert_eq!(
            String::from_utf8_lossy(&reply),
            String::from_utf8_lossy(&expected),
            "batch at {start}"
        );
    }
    assert!(
        server::metrics::DISK_KEY_READS.value() > key_reads_before,
        "no pipelined overwrite read a key from disk"
    );
}
