//! Integration tests: direct I/O via krio's `O_DIRECT` support.
//!
//! These tests exercise the full io_uring submission path for
//! `IORING_OP_READ`, `IORING_OP_WRITE`, and `IORING_OP_FSYNC` using
//! `O_DIRECT` files.
//!
//! Requirements:
//! - Linux 5.6+ (io_uring read/write)
//! - A real filesystem (ext4, xfs, etc.) — O_DIRECT does not work on tmpfs
//! - The test binary must be on a filesystem that supports O_DIRECT

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::OnceLock;

use krio::{Config, ConnToken, DirectIoCompletion, DirectIoOp, DriverCtx, EventHandler, KrioBuilder};

// ── Helpers ─────────────────────────────────────────────────────────

fn direct_io_test_config() -> Config {
    let mut config = Config::default();
    config.worker.threads = 1;
    config.worker.pin_to_core = false;
    config.sq_entries = 64;
    config.recv_buffer.ring_size = 64;
    config.recv_buffer.buffer_size = 4096;
    config.max_connections = 8;
    config.send_copy_count = 8;
    config.tick_timeout_us = 1000; // 1ms tick for responsive tests
    config.direct_io = Some(krio::DirectIoConfig {
        max_files: 4,
        max_commands_in_flight: 32,
    });
    config
}

/// Allocate a 4096-byte aligned buffer.
fn aligned_buf(size: usize) -> Vec<u8> {
    let layout = std::alloc::Layout::from_size_align(size, 4096).unwrap();
    let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
    if ptr.is_null() {
        std::alloc::handle_alloc_error(layout);
    }
    unsafe { Vec::from_raw_parts(ptr, size, size) }
}

/// Generate a temp file path in the current working directory.
/// Using cwd (the repo root) ensures we're on a real filesystem, not tmpfs.
fn temp_file_path(name: &str) -> PathBuf {
    std::env::current_dir().unwrap().join(name)
}

/// Check if io_uring is supported on this kernel.
fn io_uring_supported() -> bool {
    // Try to create a minimal io_uring. ENOSYS means the syscall doesn't exist.
    let ret = unsafe { libc::syscall(libc::SYS_io_uring_setup, 1u32, std::ptr::null_mut::<u8>()) };
    // EFAULT (bad params pointer) means the syscall exists; ENOSYS means it doesn't.
    ret != -1 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ENOSYS)
}

/// Check if O_DIRECT is supported by trying to open a file.
fn o_direct_supported() -> bool {
    let path = temp_file_path(".krio_direct_io_probe");
    let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    let fd = unsafe {
        libc::open(
            c_path.as_ptr(),
            libc::O_CREAT | libc::O_RDWR | libc::O_DIRECT,
            0o644,
        )
    };
    if fd >= 0 {
        unsafe {
            libc::close(fd);
            libc::unlink(c_path.as_ptr());
        }
        true
    } else {
        let _ = std::fs::remove_file(&path);
        false
    }
}

// ── Write then Read roundtrip test ──────────────────────────────────

/// Test state shared across on_tick calls via static.
static ROUNDTRIP_DONE: AtomicBool = AtomicBool::new(false);
static ROUNDTRIP_OK: AtomicBool = AtomicBool::new(false);
static ROUNDTRIP_ERR: OnceLock<String> = OnceLock::new();

/// Per-worker state for the roundtrip test handler.
struct RoundtripState {
    phase: RoundtripPhase,
    file: Option<krio::DirectIoFile>,
    write_buf: Vec<u8>,
    read_buf: Vec<u8>,
    path: PathBuf,
}

#[derive(Debug, PartialEq)]
enum RoundtripPhase {
    Init,
    WaitingWrite,
    WaitingFsync,
    WaitingRead,
    Done,
}

struct RoundtripHandler {
    state: RoundtripState,
}

impl EventHandler for RoundtripHandler {
    fn create_for_worker(_id: usize) -> Self {
        let path = temp_file_path(".krio_direct_io_roundtrip_test");

        // Pre-create the file with 4096 bytes so read doesn't hit EOF.
        std::fs::write(&path, [0u8; 4096]).unwrap();

        let mut write_buf = aligned_buf(4096);
        // Fill with a known pattern.
        for (i, byte) in write_buf.iter_mut().enumerate() {
            *byte = (i % 251) as u8; // prime modulus for non-repeating pattern
        }
        let read_buf = aligned_buf(4096);

        RoundtripHandler {
            state: RoundtripState {
                phase: RoundtripPhase::Init,
                file: None,
                write_buf,
                read_buf,
                path,
            },
        }
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx) {
        if self.state.phase != RoundtripPhase::Init {
            return;
        }

        // Open the file.
        let path_str = self.state.path.to_str().unwrap();
        match ctx.open_direct_io_file(path_str) {
            Ok(file) => self.state.file = Some(file),
            Err(e) => {
                let _ = ROUNDTRIP_ERR.set(format!("open failed: {e}"));
                ROUNDTRIP_DONE.store(true, Ordering::Release);
                ctx.request_shutdown();
                return;
            }
        }

        let file = self.state.file.unwrap();

        // Submit write.
        match unsafe {
            ctx.direct_io_write(
                file,
                0, // offset
                self.state.write_buf.as_ptr(),
                self.state.write_buf.len() as u32,
            )
        } {
            Ok(_seq) => {
                self.state.phase = RoundtripPhase::WaitingWrite;
            }
            Err(e) => {
                let _ = ROUNDTRIP_ERR.set(format!("write submit failed: {e}"));
                ROUNDTRIP_DONE.store(true, Ordering::Release);
                ctx.request_shutdown();
            }
        }
    }

    fn on_direct_io_complete(&mut self, ctx: &mut DriverCtx, completion: DirectIoCompletion) {
        match self.state.phase {
            RoundtripPhase::WaitingWrite => {
                if !completion.is_success() {
                    let _ = ROUNDTRIP_ERR
                        .set(format!("write failed: result={}", completion.result));
                    ROUNDTRIP_DONE.store(true, Ordering::Release);
                    ctx.request_shutdown();
                    return;
                }
                assert_eq!(completion.op, DirectIoOp::Write);
                assert_eq!(completion.bytes_transferred(), Some(4096));

                // Submit fsync.
                let file = self.state.file.unwrap();
                match ctx.direct_io_fsync(file) {
                    Ok(_) => self.state.phase = RoundtripPhase::WaitingFsync,
                    Err(e) => {
                        let _ = ROUNDTRIP_ERR.set(format!("fsync submit failed: {e}"));
                        ROUNDTRIP_DONE.store(true, Ordering::Release);
                        ctx.request_shutdown();
                    }
                }
            }
            RoundtripPhase::WaitingFsync => {
                if !completion.is_success() {
                    let _ = ROUNDTRIP_ERR
                        .set(format!("fsync failed: result={}", completion.result));
                    ROUNDTRIP_DONE.store(true, Ordering::Release);
                    ctx.request_shutdown();
                    return;
                }
                assert_eq!(completion.op, DirectIoOp::Fsync);

                // Submit read.
                let file = self.state.file.unwrap();
                match unsafe {
                    ctx.direct_io_read(
                        file,
                        0, // offset
                        self.state.read_buf.as_mut_ptr(),
                        self.state.read_buf.len() as u32,
                    )
                } {
                    Ok(_) => self.state.phase = RoundtripPhase::WaitingRead,
                    Err(e) => {
                        let _ = ROUNDTRIP_ERR.set(format!("read submit failed: {e}"));
                        ROUNDTRIP_DONE.store(true, Ordering::Release);
                        ctx.request_shutdown();
                    }
                }
            }
            RoundtripPhase::WaitingRead => {
                if !completion.is_success() {
                    let _ = ROUNDTRIP_ERR
                        .set(format!("read failed: result={}", completion.result));
                    ROUNDTRIP_DONE.store(true, Ordering::Release);
                    ctx.request_shutdown();
                    return;
                }
                assert_eq!(completion.op, DirectIoOp::Read);
                assert_eq!(completion.bytes_transferred(), Some(4096));

                // Verify data matches.
                if self.state.read_buf == self.state.write_buf {
                    ROUNDTRIP_OK.store(true, Ordering::Release);
                } else {
                    let _ = ROUNDTRIP_ERR.set("data mismatch".to_string());
                }

                // Close the file and shut down.
                let file = self.state.file.unwrap();
                let _ = ctx.close_direct_io_file(file);
                self.state.phase = RoundtripPhase::Done;
                ROUNDTRIP_DONE.store(true, Ordering::Release);
                ctx.request_shutdown();
            }
            _ => {}
        }
    }

    fn on_accept(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}
    fn on_data(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken, _data: &[u8]) -> usize { 0 }
    fn on_send_complete(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken, _r: io::Result<u32>) {}
    fn on_close(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}
}

#[test]
fn direct_io_write_fsync_read_roundtrip() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    if !o_direct_supported() {
        eprintln!("SKIP: O_DIRECT not supported on this filesystem");
        return;
    }

    // Reset statics (in case test runner reuses the process).
    ROUNDTRIP_DONE.store(false, Ordering::Release);
    ROUNDTRIP_OK.store(false, Ordering::Release);

    let (_shutdown, handles) = KrioBuilder::new(direct_io_test_config())
        .launch::<RoundtripHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }

    // Clean up temp file.
    let path = temp_file_path(".krio_direct_io_roundtrip_test");
    let _ = std::fs::remove_file(&path);

    if let Some(err) = ROUNDTRIP_ERR.get() {
        panic!("direct I/O roundtrip failed: {err}");
    }
    assert!(
        ROUNDTRIP_DONE.load(Ordering::Acquire),
        "test did not complete"
    );
    assert!(
        ROUNDTRIP_OK.load(Ordering::Acquire),
        "data verification failed"
    );
}

// ── Multiple files test ─────────────────────────────────────────────

static MULTI_FILE_DONE: AtomicBool = AtomicBool::new(false);
static MULTI_FILE_OK: AtomicBool = AtomicBool::new(false);
static MULTI_FILE_ERR: OnceLock<String> = OnceLock::new();
static MULTI_FILE_COMPLETIONS: AtomicU32 = AtomicU32::new(0);

struct MultiFileState {
    started: bool,
    files: Vec<krio::DirectIoFile>,
    bufs: Vec<Vec<u8>>,
    paths: Vec<PathBuf>,
    expected_completions: u32,
}

struct MultiFileHandler {
    state: MultiFileState,
}

impl EventHandler for MultiFileHandler {
    fn create_for_worker(_id: usize) -> Self {
        let mut paths = Vec::new();
        let mut bufs = Vec::new();
        for i in 0..3 {
            let path = temp_file_path(&format!(".krio_direct_io_multi_{i}"));
            std::fs::write(&path, [0u8; 4096]).unwrap();
            paths.push(path);

            let mut buf = aligned_buf(4096);
            for (j, byte) in buf.iter_mut().enumerate() {
                *byte = ((i * 47 + j) % 256) as u8;
            }
            bufs.push(buf);
        }

        MultiFileHandler {
            state: MultiFileState {
                started: false,
                files: Vec::new(),
                bufs,
                paths,
                expected_completions: 3, // one write per file
            },
        }
    }

    fn on_tick(&mut self, ctx: &mut DriverCtx) {
        if self.state.started {
            return;
        }
        self.state.started = true;

        // Open all files.
        for path in &self.state.paths {
            let path_str = path.to_str().unwrap();
            match ctx.open_direct_io_file(path_str) {
                Ok(file) => self.state.files.push(file),
                Err(e) => {
                    let _ = MULTI_FILE_ERR.set(format!("open failed: {e}"));
                    MULTI_FILE_DONE.store(true, Ordering::Release);
                    ctx.request_shutdown();
                    return;
                }
            }
        }

        // Submit writes to all files.
        for (i, file) in self.state.files.iter().copied().enumerate() {
            if let Err(e) = unsafe {
                ctx.direct_io_write(file, 0, self.state.bufs[i].as_ptr(), 4096)
            } {
                let _ = MULTI_FILE_ERR.set(format!("write {i} failed: {e}"));
                MULTI_FILE_DONE.store(true, Ordering::Release);
                ctx.request_shutdown();
                return;
            }
        }
    }

    fn on_direct_io_complete(&mut self, ctx: &mut DriverCtx, completion: DirectIoCompletion) {
        if !completion.is_success() {
            let _ = MULTI_FILE_ERR.set(format!(
                "completion failed: file={} result={}",
                completion.file.index(),
                completion.result
            ));
            MULTI_FILE_DONE.store(true, Ordering::Release);
            ctx.request_shutdown();
            return;
        }

        let count = MULTI_FILE_COMPLETIONS.fetch_add(1, Ordering::AcqRel) + 1;
        if count >= self.state.expected_completions {
            // Close all files.
            for file in &self.state.files {
                let _ = ctx.close_direct_io_file(*file);
            }
            MULTI_FILE_OK.store(true, Ordering::Release);
            MULTI_FILE_DONE.store(true, Ordering::Release);
            ctx.request_shutdown();
        }
    }

    fn on_accept(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}
    fn on_data(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken, _data: &[u8]) -> usize { 0 }
    fn on_send_complete(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken, _r: io::Result<u32>) {}
    fn on_close(&mut self, _ctx: &mut DriverCtx, _conn: ConnToken) {}
}

#[test]
fn direct_io_multiple_files() {
    if !io_uring_supported() {
        eprintln!("SKIP: io_uring not supported on this kernel");
        return;
    }
    if !o_direct_supported() {
        eprintln!("SKIP: O_DIRECT not supported on this filesystem");
        return;
    }

    MULTI_FILE_DONE.store(false, Ordering::Release);
    MULTI_FILE_OK.store(false, Ordering::Release);
    MULTI_FILE_COMPLETIONS.store(0, Ordering::Release);

    let (_shutdown, handles) = KrioBuilder::new(direct_io_test_config())
        .launch::<MultiFileHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }

    // Clean up.
    for i in 0..3 {
        let path = temp_file_path(&format!(".krio_direct_io_multi_{i}"));
        let _ = std::fs::remove_file(&path);
    }

    if let Some(err) = MULTI_FILE_ERR.get() {
        panic!("multi-file test failed: {err}");
    }
    assert!(MULTI_FILE_DONE.load(Ordering::Acquire));
    assert!(MULTI_FILE_OK.load(Ordering::Acquire));
}
