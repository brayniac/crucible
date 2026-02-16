//! Async runtime for krio: task executor, waker, and I/O primitives.
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

use std::cell::Cell;
use std::collections::VecDeque;
use std::io as stdio;

use self::task::{StandaloneTaskSlab, TaskSlab};
use self::waker::drain_ready_queue;

/// I/O result stored per-connection for async task wakeup.
#[allow(dead_code)]
pub(crate) enum IoResult {
    /// Send completed with total bytes or error.
    Send(stdio::Result<u32>),
    /// Connect completed with success or error.
    Connect(stdio::Result<()>),
}

thread_local! {
    /// The current task ID being polled. Set by the executor before each poll.
    /// Connection tasks: conn_index (bits 0..23).
    /// Standalone tasks: task_idx | STANDALONE_BIT.
    /// Used by SleepFuture to know which task to wake on timer completion.
    pub(crate) static CURRENT_TASK_ID: Cell<u32> = const { Cell::new(0) };
}

/// Pool of timer slots for io_uring timeout SQEs.
///
/// Each `sleep()` call allocates a slot that holds the `Timespec` (stable
/// memory for io_uring) and metadata. Generation counters prevent stale
/// CQEs from waking the wrong task after slot reuse.
pub(crate) struct TimerSlotPool {
    /// Timespec values — must remain at stable addresses for io_uring.
    pub(crate) timespecs: Vec<io_uring::types::Timespec>,
    /// Which task (with STANDALONE_BIT encoding) to wake when this timer fires.
    pub(crate) waker_ids: Vec<u32>,
    /// Whether the CQE has arrived for this timer.
    pub(crate) fired: Vec<bool>,
    /// Generation counter per slot to prevent stale CQE races.
    pub(crate) generations: Vec<u16>,
    /// Free slot indices for O(1) allocation.
    free_list: Vec<u32>,
}

impl TimerSlotPool {
    /// Create a new pool with the given capacity.
    pub(crate) fn new(capacity: u32) -> Self {
        let cap = capacity as usize;
        let mut free_list = Vec::with_capacity(cap);
        for i in 0..capacity {
            free_list.push(i);
        }
        TimerSlotPool {
            timespecs: vec![io_uring::types::Timespec::new(); cap],
            waker_ids: vec![0; cap],
            fired: vec![false; cap],
            generations: vec![0; cap],
            free_list,
        }
    }

    /// Allocate a timer slot. Returns `(slot_index, generation)` or None if full.
    pub(crate) fn allocate(&mut self, waker_id: u32) -> Option<(u32, u16)> {
        let slot = self.free_list.pop()?;
        let idx = slot as usize;
        self.waker_ids[idx] = waker_id;
        self.fired[idx] = false;
        let generation = self.generations[idx];
        Some((slot, generation))
    }

    /// Release a timer slot back to the free list.
    pub(crate) fn release(&mut self, slot: u32) {
        let idx = slot as usize;
        if idx < self.generations.len() {
            self.generations[idx] = self.generations[idx].wrapping_add(1);
            self.free_list.push(slot);
        }
    }

    /// Mark a timer as fired. Returns the waker_id if generation matches.
    pub(crate) fn fire(&mut self, slot: u32, generation: u16) -> Option<u32> {
        let idx = slot as usize;
        if idx >= self.generations.len() || self.generations[idx] != generation {
            return None; // stale CQE
        }
        self.fired[idx] = true;
        Some(self.waker_ids[idx])
    }

    /// Check if a timer slot has fired.
    pub(crate) fn is_fired(&self, slot: u32) -> bool {
        self.fired.get(slot as usize).copied().unwrap_or(false)
    }

    /// Encode `(slot_index, generation)` into a 32-bit payload for UserData.
    pub(crate) fn encode_payload(slot: u32, generation: u16) -> u32 {
        (slot & 0xFFFF) | ((generation as u32) << 16)
    }

    /// Decode payload back to `(slot_index, generation)`.
    pub(crate) fn decode_payload(payload: u32) -> (u32, u16) {
        let slot = payload & 0xFFFF;
        let generation = (payload >> 16) as u16;
        (slot, generation)
    }
}

/// Per-worker async executor. Owns the task slab and coordinates
/// CQE-driven wakeups with future polling.
pub(crate) struct Executor {
    pub(crate) task_slab: TaskSlab,
    /// Standalone tasks not bound to any connection.
    pub(crate) standalone_slab: StandaloneTaskSlab,
    /// Timer slot pool for sleep/timeout.
    pub(crate) timer_pool: TimerSlotPool,
    /// Connection indices (and standalone task indices with STANDALONE_BIT) ready to poll.
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
    /// Create a new executor with the given capacities.
    pub(crate) fn new(max_connections: u32, standalone_capacity: u32, timer_slots: u32) -> Self {
        let cap = max_connections as usize;
        Executor {
            task_slab: TaskSlab::new(max_connections),
            standalone_slab: StandaloneTaskSlab::new(standalone_capacity),
            timer_pool: TimerSlotPool::new(timer_slots),
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
        let exec = Executor::new(16, 8, 8);
        assert!(exec.ready_queue.is_empty());
        assert_eq!(exec.recv_waiters.len(), 16);
        assert_eq!(exec.send_waiters.len(), 16);
        assert_eq!(exec.connect_waiters.len(), 16);
        assert_eq!(exec.io_results.len(), 16);
    }

    #[test]
    fn remove_connection_clears_state() {
        let mut exec = Executor::new(4, 4, 4);
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
