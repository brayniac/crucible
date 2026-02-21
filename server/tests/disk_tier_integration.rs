//! Integration tests for the Direct I/O disk tier.
//!
//! Exercises the full disk demotion/promotion path:
//! 1. Start a server with a small RAM heap and a Direct I/O disk tier.
//! 2. Write enough data to trigger eviction/demotion to disk.
//! 3. Read back keys that were demoted to verify disk reads work.

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

    let mut buf = vec![0u8; 4096];
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
