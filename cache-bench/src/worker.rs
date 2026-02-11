//! Worker thread for in-process cache benchmarking.

use crate::config::Config;
use crate::metrics;
use crate::ratelimit::DynamicRateLimiter;

use cache_core::Cache;
use rand::prelude::*;
use rand_xoshiro::Xoshiro256PlusPlus;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Instant;

/// Test phase, controlled by main thread and read by workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    /// Prefill phase — write each key exactly once.
    Prefill = 0,
    /// Warmup phase — run workload but don't record metrics.
    Warmup = 1,
    /// Main measurement phase — record metrics.
    Running = 2,
    /// Stop phase — workers should exit.
    Stop = 3,
}

impl Phase {
    #[inline]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Phase::Prefill,
            1 => Phase::Warmup,
            2 => Phase::Running,
            _ => Phase::Stop,
        }
    }
}

/// Shared state between main thread and workers.
pub struct SharedState {
    phase: AtomicU8,
    prefill_complete: AtomicUsize,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            phase: AtomicU8::new(Phase::Prefill as u8),
            prefill_complete: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn phase(&self) -> Phase {
        Phase::from_u8(self.phase.load(Ordering::Acquire))
    }

    pub fn set_phase(&self, phase: Phase) {
        self.phase.store(phase as u8, Ordering::Release);
    }

    pub fn mark_prefill_complete(&self) {
        self.prefill_complete.fetch_add(1, Ordering::Release);
    }

    pub fn prefill_complete_count(&self) -> usize {
        self.prefill_complete.load(Ordering::Acquire)
    }
}

/// Run a single worker thread.
pub fn run_worker<C: Cache>(
    id: usize,
    config: &Config,
    cache: &Arc<C>,
    shared: &SharedState,
    ratelimiter: Option<&DynamicRateLimiter>,
    prefill_range: Option<std::ops::Range<usize>>,
) {
    // Set this thread's shard ID for metrics
    metrics::set_thread_shard(id);

    let key_len = config.workload.keyspace.length;
    let key_count = config.workload.keyspace.count;
    let value_len = config.workload.values.length;
    let get_threshold = config.workload.commands.get;
    let set_threshold = get_threshold + config.workload.commands.set;

    // Pre-allocate buffers
    let mut key_buf = vec![0u8; key_len];
    let mut value_buf = vec![0u8; value_len];

    // Initialize RNG
    let mut rng = Xoshiro256PlusPlus::seed_from_u64(42 + id as u64);

    // Fill value buffer with random data
    rng.fill_bytes(&mut value_buf);

    // Prefill phase
    if let Some(range) = prefill_range {
        for key_id in range {
            write_key(&mut key_buf, key_id);
            let _ = cache.set(&key_buf, &value_buf, None);
        }
    }
    shared.mark_prefill_complete();

    // Main loop
    loop {
        let phase = shared.phase();
        match phase {
            Phase::Prefill => {
                // Wait for all workers to finish prefill
                std::hint::spin_loop();
                continue;
            }
            Phase::Stop => break,
            Phase::Warmup | Phase::Running => {}
        }

        // Rate limiting
        if let Some(rl) = ratelimiter
            && !rl.try_acquire()
        {
            std::hint::spin_loop();
            continue;
        }

        // Generate random key
        let key_id = rng.random_range(0..key_count);
        write_key(&mut key_buf, key_id);

        // Roll command
        let roll: u8 = rng.random_range(0..100);
        let recording = phase == Phase::Running;

        if roll < get_threshold {
            // GET
            let start = Instant::now();
            let hit = cache.with_value(&key_buf, |_| ()).is_some();
            let elapsed_ns = start.elapsed().as_nanos() as u64;

            if recording {
                metrics::GET_COUNT.increment();
                metrics::COMPLETED_COUNT.increment();
                if hit {
                    metrics::CACHE_HITS.increment();
                } else {
                    metrics::CACHE_MISSES.increment();
                }
                let _ = metrics::RESPONSE_LATENCY.increment(elapsed_ns);
                let _ = metrics::GET_LATENCY.increment(elapsed_ns);
            }
        } else if roll < set_threshold {
            // SET — regenerate value for variety
            rng.fill_bytes(&mut value_buf);
            let start = Instant::now();
            let result = cache.set(&key_buf, &value_buf, None);
            let elapsed_ns = start.elapsed().as_nanos() as u64;

            if recording {
                metrics::SET_COUNT.increment();
                metrics::COMPLETED_COUNT.increment();
                if result.is_err() {
                    metrics::SET_ERRORS.increment();
                }
                let _ = metrics::RESPONSE_LATENCY.increment(elapsed_ns);
                let _ = metrics::SET_LATENCY.increment(elapsed_ns);
            }
        } else {
            // DELETE
            let start = Instant::now();
            cache.delete(&key_buf);
            let elapsed_ns = start.elapsed().as_nanos() as u64;

            if recording {
                metrics::DELETE_COUNT.increment();
                metrics::COMPLETED_COUNT.increment();
                let _ = metrics::RESPONSE_LATENCY.increment(elapsed_ns);
                let _ = metrics::DELETE_LATENCY.increment(elapsed_ns);
            }
        }
    }
}

/// Write a numeric key ID into the buffer as hex.
fn write_key(buf: &mut [u8], id: usize) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut n = id;
    for byte in buf.iter_mut().rev() {
        *byte = HEX[n & 0xf];
        n >>= 4;
    }
}
