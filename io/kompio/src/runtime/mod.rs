//! Async runtime for kompio: task executor, waker, and I/O primitives.
//!
//! # Portability boundary
//!
//! This module is designed so that the core async machinery is portable
//! across I/O backends (io_uring, mio/epoll, kqueue):
//!
//! - **Portable** (no io_uring dependency):
//!   - `task` — `TaskSlab`, `TaskSlot` (slab of per-connection futures)
//!   - `waker` — `conn_waker()`, thread-local `READY_QUEUE`
//!   - `mod.rs` — `Executor`, `IoResult` (waiter flags, ready queue, result storage)
//!   - `handler` — `AsyncEventHandler` trait (references `DriverCtx` by borrowed ref only)
//!
//! - **Backend-specific** (tied to the concrete `Driver` type):
//!   - `io` — `ConnCtx`, futures (`WithDataFuture`, `SendFuture`, `ConnectFuture`)
//!     accesses `Driver` via thread-local pointer
//!
//! A mio backend would provide an alternative `driver.rs`, `async_event_loop.rs`,
//! and `runtime/io.rs` while reusing everything else unchanged.

pub(crate) mod handler;
pub(crate) mod io;
pub(crate) mod task;
pub(crate) mod waker;

use std::collections::VecDeque;
use std::io as stdio;

use self::task::TaskSlab;
use self::waker::drain_ready_queue;

/// I/O result stored per-connection for async task wakeup.
#[allow(dead_code)]
pub(crate) enum IoResult {
    /// Send completed with total bytes or error.
    Send(stdio::Result<u32>),
    /// Connect completed with success or error.
    Connect(stdio::Result<()>),
}

/// Per-worker async executor. Owns the task slab and coordinates
/// CQE-driven wakeups with future polling.
pub(crate) struct Executor {
    pub(crate) task_slab: TaskSlab,
    /// Connection indices ready to poll (drained from thread-local waker queue).
    pub(crate) ready_queue: VecDeque<u32>,
    /// Per-connection: task is awaiting recv data.
    pub(crate) recv_waiters: Vec<bool>,
    /// Per-connection: task is awaiting send completion.
    pub(crate) send_waiters: Vec<bool>,
    /// Per-connection: task is awaiting connect result.
    pub(crate) connect_waiters: Vec<bool>,
    /// Per-connection: CQE result storage for send/connect.
    pub(crate) io_results: Vec<Option<IoResult>>,
}

impl Executor {
    /// Create a new executor with capacity for `max_connections` tasks.
    pub(crate) fn new(max_connections: u32) -> Self {
        let cap = max_connections as usize;
        Executor {
            task_slab: TaskSlab::new(max_connections),
            ready_queue: VecDeque::with_capacity(64),
            recv_waiters: vec![false; cap],
            send_waiters: vec![false; cap],
            connect_waiters: vec![false; cap],
            io_results: {
                let mut v = Vec::with_capacity(cap);
                for _ in 0..cap {
                    v.push(None);
                }
                v
            },
        }
    }

    /// Drain the thread-local waker queue into our ready_queue,
    /// then wake corresponding tasks in the slab.
    pub(crate) fn collect_wakeups(&mut self) {
        drain_ready_queue(&mut self.ready_queue);
    }

    /// Reset all per-connection state for a connection that was closed.
    pub(crate) fn remove_connection(&mut self, conn_index: u32) {
        let idx = conn_index as usize;
        self.task_slab.remove(conn_index);
        if idx < self.recv_waiters.len() {
            self.recv_waiters[idx] = false;
            self.send_waiters[idx] = false;
            self.connect_waiters[idx] = false;
            self.io_results[idx] = None;
        }
    }

    /// Wake a task that was waiting for recv data.
    pub(crate) fn wake_recv(&mut self, conn_index: u32) {
        let idx = conn_index as usize;
        if idx < self.recv_waiters.len() && self.recv_waiters[idx] {
            self.recv_waiters[idx] = false;
            if self.task_slab.wake(conn_index) {
                self.ready_queue.push_back(conn_index);
            }
        }
    }

    /// Wake a task that was waiting for send completion.
    pub(crate) fn wake_send(&mut self, conn_index: u32, result: stdio::Result<u32>) {
        let idx = conn_index as usize;
        if idx < self.send_waiters.len() && self.send_waiters[idx] {
            self.send_waiters[idx] = false;
            self.io_results[idx] = Some(IoResult::Send(result));
            if self.task_slab.wake(conn_index) {
                self.ready_queue.push_back(conn_index);
            }
        }
    }

    /// Wake a task that was waiting for connect completion.
    pub(crate) fn wake_connect(&mut self, conn_index: u32, result: stdio::Result<()>) {
        let idx = conn_index as usize;
        if idx < self.connect_waiters.len() && self.connect_waiters[idx] {
            self.connect_waiters[idx] = false;
            self.io_results[idx] = Some(IoResult::Connect(result));
            if self.task_slab.wake(conn_index) {
                self.ready_queue.push_back(conn_index);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_new() {
        let exec = Executor::new(16);
        assert!(exec.ready_queue.is_empty());
        assert_eq!(exec.recv_waiters.len(), 16);
        assert_eq!(exec.send_waiters.len(), 16);
        assert_eq!(exec.connect_waiters.len(), 16);
        assert_eq!(exec.io_results.len(), 16);
    }

    #[test]
    fn remove_connection_clears_state() {
        let mut exec = Executor::new(4);
        exec.recv_waiters[1] = true;
        exec.send_waiters[1] = true;
        exec.connect_waiters[1] = true;
        exec.io_results[1] = Some(IoResult::Send(Ok(42)));

        exec.remove_connection(1);
        assert!(!exec.recv_waiters[1]);
        assert!(!exec.send_waiters[1]);
        assert!(!exec.connect_waiters[1]);
        assert!(exec.io_results[1].is_none());
    }
}
