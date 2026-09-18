//! Duration histogram for eviction passes.
//!
//! Merge eviction runs inline in the write path, so a reclamation pass is a
//! stall on whichever `set` triggered it (crucible#152). `CacheStats` counts
//! passes; nothing times them, so the stall is invisible outside a replay rig.
//!
//! Deliberately log2-bucketed, giving 2x resolution rather than the ~1% a
//! real latency histogram offers. The question this answers is "microseconds
//! or milliseconds", which 2x settles, and the whole structure is 256 bytes
//! with no new dependency. Anything needing better precision should export
//! the raw samples instead of widening this.

use crate::sync::{AtomicU64, Ordering};

/// Number of log2 buckets. Bucket `i > 0` holds durations in `[2^(i-1), 2^i)`
/// nanoseconds; bucket 0 holds exactly zero. Bucket 31 saturates, covering
/// everything from ~1.07 s upward -- far past any pass worth distinguishing.
pub const LATENCY_BUCKETS: usize = 32;

/// Which bucket a duration belongs in.
fn bucket_index(nanos: u64) -> usize {
    if nanos == 0 {
        return 0;
    }
    // floor(log2(nanos)) + 1, saturating at the last bucket so an absurd
    // duration lands there rather than indexing out of range.
    let idx = (u64::BITS - nanos.leading_zeros()) as usize;
    idx.min(LATENCY_BUCKETS - 1)
}

/// Exclusive upper bound of a bucket, in nanoseconds.
fn bucket_upper_ns(index: usize) -> u64 {
    if index == 0 { 0 } else { 1u64 << index }
}

/// Lock-free duration histogram.
#[derive(Debug, Default)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKETS],
}

impl LatencyHistogram {
    /// Create an empty histogram.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one duration.
    ///
    /// `Relaxed` throughout: these counters are diagnostic and are never used
    /// to order anything. An eviction pass runs roughly once in several
    /// thousand writes, so the two atomics it costs are not on any hot path.
    pub fn record(&self, nanos: u64) {
        self.buckets[bucket_index(nanos)].fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent-enough snapshot for reporting.
    pub fn snapshot(&self) -> LatencySnapshot {
        // Read bucket by bucket, so a concurrent `record` can land between two
        // reads. That skews a total by at most the number of passes running
        // right now, which for a diagnostic counted in thousands is noise --
        // and the alternative is a lock on the eviction path to make a
        // reporting call tidy.
        let mut buckets = [0u64; LATENCY_BUCKETS];
        for (out, b) in buckets.iter_mut().zip(self.buckets.iter()) {
            *out = b.load(Ordering::Relaxed);
        }
        LatencySnapshot { buckets }
    }
}

/// A point-in-time read of a [`LatencyHistogram`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LatencySnapshot {
    /// Per-bucket counts.
    pub buckets: [u64; LATENCY_BUCKETS],
}

impl LatencySnapshot {
    /// Total samples recorded.
    pub fn count(&self) -> u64 {
        self.buckets.iter().sum()
    }

    /// Upper bound of the bucket holding the `p`th percentile, in nanoseconds.
    ///
    /// `None` when nothing has been recorded -- an empty histogram has no
    /// percentile, and reporting 0 would read as "instant" rather than "no
    /// data".
    pub fn percentile_ns(&self, p: f64) -> Option<u64> {
        let total = self.count();
        if total == 0 {
            return None;
        }
        let target = ((total as f64) * (p / 100.0)).ceil().max(1.0) as u64;
        let mut seen = 0u64;
        for (i, &c) in self.buckets.iter().enumerate() {
            seen += c;
            if seen >= target {
                return Some(bucket_upper_ns(i));
            }
        }
        Some(bucket_upper_ns(LATENCY_BUCKETS - 1))
    }

    /// Upper bound of the highest non-empty bucket.
    pub fn max_ns(&self) -> Option<u64> {
        self.buckets
            .iter()
            .rposition(|&c| c > 0)
            .map(bucket_upper_ns)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn a_duration_lands_in_the_bucket_for_its_magnitude() {
        let h = LatencyHistogram::new();
        h.record(0);
        h.record(1);
        h.record(1_000);
        h.record(5_600_000);

        let s = h.snapshot();
        assert_eq!(s.count(), 4);
        assert_eq!(s.buckets[0], 1, "zero belongs in its own bucket");
        assert_eq!(s.buckets[1], 1, "1ns is in [2^0, 2^1)");
        assert_eq!(s.buckets[10], 1, "1000ns is in [512, 1024)");
        assert_eq!(s.buckets[23], 1, "5.6ms is in [4.19ms, 8.39ms)");
    }

    #[test]
    fn an_empty_histogram_has_no_percentile_rather_than_zero() {
        let s = LatencyHistogram::new().snapshot();
        assert_eq!(s.count(), 0);
        assert_eq!(s.percentile_ns(50.0), None);
        assert_eq!(s.max_ns(), None);
    }

    #[test]
    fn a_rare_long_stall_shows_at_the_tail_and_not_at_the_median() {
        // The shape crucible#152 is about: thousands of fast writes and one
        // multi-millisecond eviction pass. A median that reported the stall
        // would be as wrong as a max that hid it.
        let h = LatencyHistogram::new();
        for _ in 0..9_999 {
            h.record(300);
        }
        h.record(5_600_000);

        let s = h.snapshot();
        assert_eq!(s.percentile_ns(50.0), Some(512), "median is the fast path");
        assert!(
            s.max_ns().unwrap() >= 5_600_000,
            "the stall must survive in the tail"
        );
    }

    #[test]
    fn a_duration_past_the_last_bucket_saturates_rather_than_wrapping() {
        let h = LatencyHistogram::new();
        h.record(u64::MAX);

        let s = h.snapshot();
        assert_eq!(s.count(), 1);
        assert_eq!(
            s.buckets[LATENCY_BUCKETS - 1],
            1,
            "an absurd duration must land in the last bucket, not index out of range"
        );
    }
}
