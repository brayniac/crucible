//! Worker thread management utilities.

use crate::affinity::set_cpu_affinity;
use std::thread::{self, JoinHandle};

/// Handle to a spawned worker thread.
pub struct WorkerHandle<R> {
    /// The thread handle
    pub handle: JoinHandle<R>,
    /// The worker ID (0-indexed)
    pub worker_id: usize,
    /// The CPU ID the worker is pinned to, if any
    pub cpu_id: Option<usize>,
}

/// Spawn worker threads with optional CPU affinity.
pub fn spawn_workers<F, R>(
    num_workers: usize,
    cpu_affinity: Option<&[usize]>,
    name_prefix: &str,
    worker_fn: F,
) -> Vec<WorkerHandle<R>>
where
    F: Fn(usize) -> R + Send + Clone + 'static,
    R: Send + 'static,
{
    let mut handles = Vec::with_capacity(num_workers);

    for worker_id in 0..num_workers {
        let cpu_id = cpu_affinity.map(|cpus| cpus[worker_id % cpus.len()]);
        let worker_fn = worker_fn.clone();
        let thread_name = format!("{}-{}", name_prefix, worker_id);

        let handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                if let Some(cpu) = cpu_id {
                    let _ = set_cpu_affinity(cpu);
                }
                worker_fn(worker_id)
            })
            .expect("failed to spawn worker thread");

        handles.push(WorkerHandle {
            handle,
            worker_id,
            cpu_id,
        });
    }

    handles
}

/// Wait for all worker threads to complete.
pub fn join_workers<R>(handles: Vec<WorkerHandle<R>>) -> Vec<R> {
    handles
        .into_iter()
        .map(|h| h.handle.join().expect("worker thread panicked"))
        .collect()
}
