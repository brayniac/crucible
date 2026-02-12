use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crate::acceptor::{AcceptorConfig, run_acceptor};
use crate::config::Config;
use crate::event_loop::EventLoop;
use crate::handler::EventHandler;

/// Result type for `launch` / `KompioBuilder::launch` to avoid type-complexity warnings.
type LaunchResult = Result<
    (
        ShutdownHandle,
        Vec<thread::JoinHandle<Result<(), crate::error::Error>>>,
    ),
    crate::error::Error,
>;

/// Handle returned by `launch()` to trigger graceful shutdown of all workers.
pub struct ShutdownHandle {
    shutdown_flag: Arc<AtomicBool>,
    worker_eventfds: Vec<RawFd>,
    listen_fd: Option<RawFd>,
    listen_fd_closed: Option<Arc<AtomicBool>>,
}

impl ShutdownHandle {
    /// Signal all workers to shut down gracefully.
    ///
    /// Workers will stop accepting new connections, close all active connections,
    /// drain remaining CQEs, and exit their event loops returning `Ok(())`.
    /// Also closes the listen fd to unblock the acceptor's `accept4`.
    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Release);
        // Close listen_fd to unblock the acceptor thread's accept4() call.
        if let (Some(fd), Some(closed)) = (self.listen_fd, &self.listen_fd_closed)
            && !closed.swap(true, Ordering::AcqRel)
        {
            unsafe {
                libc::close(fd);
            }
        }
        // Wake all workers so they see the flag even if blocked in submit_and_wait.
        for &efd in &self.worker_eventfds {
            let val: u64 = 1;
            unsafe {
                libc::write(efd, &val as *const u64 as *const libc::c_void, 8);
            }
        }
    }
}

/// Builder for launching kompio workers with optional listener/acceptor.
pub struct KompioBuilder {
    config: Config,
    bind_addr: Option<String>,
}

impl KompioBuilder {
    /// Create a new builder with the given config.
    pub fn new(config: Config) -> Self {
        KompioBuilder {
            config,
            bind_addr: None,
        }
    }

    /// Set the bind address for the TCP listener. If not set, no listener
    /// or acceptor thread is created (client-only mode).
    pub fn bind(mut self, addr: &str) -> Self {
        self.bind_addr = Some(addr.to_string());
        self
    }

    /// Launch worker threads.
    ///
    /// If `bind()` was called, creates a listener + acceptor thread.
    /// Otherwise runs in client-only mode (no acceptor).
    #[allow(clippy::needless_range_loop)]
    pub fn launch<H: EventHandler>(self) -> LaunchResult {
        let num_threads = if self.config.worker.threads == 0 {
            num_cpus()
        } else {
            self.config.worker.threads
        };

        ensure_nofile_limit(self.config.max_connections, num_threads)?;

        // Create per-worker channels and eventfds.
        let mut worker_txs = Vec::with_capacity(num_threads);
        let mut worker_rxs = Vec::with_capacity(num_threads);
        let mut worker_eventfds = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let (tx, rx) = crossbeam_channel::unbounded::<(RawFd, SocketAddr)>();
            let efd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
            if efd < 0 {
                for &fd in &worker_eventfds {
                    unsafe {
                        libc::close(fd);
                    }
                }
                return Err(crate::error::Error::Io(io::Error::last_os_error()));
            }
            worker_txs.push(tx);
            worker_rxs.push(rx);
            worker_eventfds.push(efd);
        }

        let shutdown_flag = Arc::new(AtomicBool::new(false));

        // Optionally create listener + acceptor.
        let (listen_fd, listen_fd_closed) = if let Some(ref addr) = self.bind_addr {
            let fd = create_listener(addr, self.config.backlog)?;
            let closed = Arc::new(AtomicBool::new(false));

            let acceptor_config = AcceptorConfig {
                listen_fd: fd,
                worker_channels: worker_txs,
                worker_eventfds: worker_eventfds.clone(),
                shutdown_flag: shutdown_flag.clone(),
                tcp_nodelay: self.config.tcp_nodelay,
            };

            let acceptor_closed = closed.clone();
            thread::Builder::new()
                .name("kompio-acceptor".to_string())
                .spawn(move || {
                    run_acceptor(acceptor_config);
                    if !acceptor_closed.swap(true, Ordering::AcqRel) {
                        unsafe {
                            libc::close(fd);
                        }
                    }
                })
                .map_err(crate::error::Error::Io)?;

            (Some(fd), Some(closed))
        } else {
            // Client-only mode — drop txs so workers don't expect accept data.
            drop(worker_txs);
            (None, None)
        };

        // Spawn worker threads.
        let mut handles = Vec::with_capacity(num_threads);

        for worker_id in 0..num_threads {
            let config = self.config.clone();
            let rx = worker_rxs.remove(0);
            let eventfd = worker_eventfds[worker_id];
            let shutdown_flag = shutdown_flag.clone();
            let has_acceptor = self.bind_addr.is_some();

            let handle = thread::Builder::new()
                .name(format!("kompio-worker-{worker_id}"))
                .spawn(move || {
                    if config.worker.pin_to_core {
                        let core = config.worker.core_offset + worker_id;
                        pin_to_core(core)?;
                    }

                    let handler = H::create_for_worker(worker_id);

                    let accept_rx = if has_acceptor { Some(rx) } else { None };
                    let mut event_loop =
                        EventLoop::new(&config, handler, accept_rx, eventfd, shutdown_flag)?;
                    event_loop.run()?;

                    Ok(())
                })
                .map_err(crate::error::Error::Io)?;

            handles.push(handle);
        }

        let shutdown_handle = ShutdownHandle {
            shutdown_flag,
            worker_eventfds,
            listen_fd,
            listen_fd_closed,
        };

        Ok((shutdown_handle, handles))
    }
}

/// Launch worker threads with a centralized acceptor thread.
///
/// This is a convenience wrapper around `KompioBuilder`.
pub fn launch<H: EventHandler>(config: Config, bind_addr: &str) -> LaunchResult {
    KompioBuilder::new(config).bind(bind_addr).launch::<H>()
}

/// Ensure RLIMIT_NOFILE is high enough for the io_uring fixed file table.
///
/// Each worker calls `register_files_sparse(max_connections)`, and the kernel
/// checks `nr_args > rlimit(RLIMIT_NOFILE)` per call (not cumulative across
/// workers). Connections use the fixed file table — the original FD is closed
/// immediately after `register_files_update` — so they don't consume process
/// FD table entries. We only need headroom for ring fds, eventfds, the listen
/// socket, stdin/stdout/stderr, etc.
fn ensure_nofile_limit(max_connections: u32, num_workers: usize) -> Result<(), crate::error::Error> {
    let mut rlim: libc::rlimit = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
    if ret != 0 {
        return Err(crate::error::Error::Io(io::Error::last_os_error()));
    }

    // register_files_sparse(max_connections) needs RLIMIT_NOFILE >= max_connections.
    // Add per-worker overhead (ring fd, eventfd, transient socket fds) and global
    // overhead (listen socket, stdio, misc).
    let per_worker_overhead: u64 = 8;
    let global_overhead: u64 = 64;
    let required =
        max_connections as u64 + per_worker_overhead * num_workers as u64 + global_overhead;

    let soft = rlim.rlim_cur;
    let hard = rlim.rlim_max;

    if soft >= required {
        return Ok(());
    }

    if hard >= required || hard == libc::RLIM_INFINITY {
        // Raise soft limit to required (or hard if hard is finite and smaller)
        let new_soft = if hard == libc::RLIM_INFINITY {
            required
        } else {
            std::cmp::min(required, hard)
        };
        rlim.rlim_cur = new_soft;
        let ret = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) };
        if ret != 0 {
            return Err(crate::error::Error::Io(io::Error::last_os_error()));
        }
        Ok(())
    } else {
        Err(crate::error::Error::ResourceLimit(format!(
            "RLIMIT_NOFILE too low: need {} but hard limit is {} (soft: {}). \
             Raise it with: ulimit -n {}",
            required, hard, soft, required
        )))
    }
}

/// Pin the current thread to a specific CPU core.
fn pin_to_core(core: usize) -> Result<(), crate::error::Error> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        if ret != 0 {
            return Err(crate::error::Error::Io(io::Error::last_os_error()));
        }
    }
    Ok(())
}

/// Create a TCP listener without SO_REUSEPORT (just SO_REUSEADDR).
fn create_listener(addr: &str, backlog: i32) -> Result<RawFd, crate::error::Error> {
    let parsed: std::net::SocketAddr = addr.parse().map_err(|e: std::net::AddrParseError| {
        crate::error::Error::RingSetup(format!("invalid address: {e}"))
    })?;

    let domain = if parsed.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };

    let fd = unsafe { libc::socket(domain, libc::SOCK_STREAM | libc::SOCK_NONBLOCK, 0) };
    if fd < 0 {
        return Err(crate::error::Error::Io(io::Error::last_os_error()));
    }

    // Set SO_REUSEADDR only (no SO_REUSEPORT).
    let optval: libc::c_int = 1;
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }

    // Bind — use stack-allocated sockaddr_storage, cast pointer for bind().
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let addr_len = match parsed {
        std::net::SocketAddr::V4(v4) => {
            let sa = &mut storage as *mut _ as *mut libc::sockaddr_in;
            unsafe {
                (*sa).sin_family = libc::AF_INET as libc::sa_family_t;
                (*sa).sin_port = v4.port().to_be();
                (*sa).sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            }
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        std::net::SocketAddr::V6(v6) => {
            let sa = &mut storage as *mut _ as *mut libc::sockaddr_in6;
            unsafe {
                (*sa).sin6_family = libc::AF_INET6 as libc::sa_family_t;
                (*sa).sin6_port = v6.port().to_be();
                (*sa).sin6_flowinfo = v6.flowinfo();
                (*sa).sin6_addr.s6_addr = v6.ip().octets();
                (*sa).sin6_scope_id = v6.scope_id();
            }
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    };

    let ret = unsafe { libc::bind(fd, &storage as *const _ as *const libc::sockaddr, addr_len) };
    if ret < 0 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(crate::error::Error::Io(err));
    }

    // Switch to blocking mode for the acceptor thread's accept4 call.
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        libc::fcntl(fd, libc::F_SETFL, flags & !libc::O_NONBLOCK);
    }

    let ret = unsafe { libc::listen(fd, backlog) };
    if ret < 0 {
        let err = io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        return Err(crate::error::Error::Io(err));
    }

    Ok(fd)
}

/// Get the number of available CPU cores.
fn num_cpus() -> usize {
    let ret = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
    if ret < 1 { 1 } else { ret as usize }
}
