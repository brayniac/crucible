use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::command::GrpcCommand;
use crate::error::GrpcError;

/// Per-worker handle used by the GrpcClient to send commands and wake workers.
///
/// Uses an atomic counter for eventfd write coalescing: only the first sender
/// (transitioning 0 -> 1) writes to the eventfd, avoiding redundant syscalls
/// when multiple commands arrive before the worker drains.
pub(crate) struct WorkerHandle {
    tx: crossbeam_channel::Sender<GrpcCommand>,
    eventfd: RawFd,
    pub(crate) pending: Arc<AtomicU32>,
}

impl WorkerHandle {
    pub fn new(
        tx: crossbeam_channel::Sender<GrpcCommand>,
        eventfd: RawFd,
        pending: Arc<AtomicU32>,
    ) -> Self {
        Self {
            tx,
            eventfd,
            pending,
        }
    }

    /// Send a command and wake the worker if needed.
    pub fn send(&self, cmd: GrpcCommand) -> Result<(), GrpcError> {
        self.tx.send(cmd).map_err(|_| GrpcError::WorkerClosed)?;

        // Only the first sender (prev == 0) writes to the eventfd.
        if self.pending.fetch_add(1, Ordering::AcqRel) == 0 {
            let val: u64 = 1;
            unsafe {
                libc::write(self.eventfd, &val as *const u64 as *const libc::c_void, 8);
            }
        }
        Ok(())
    }
}
