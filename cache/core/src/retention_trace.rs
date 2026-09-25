//! What a merge pass kept, by item size and frequency.
//!
//! Five separate hypotheses about crucible's retention differing from
//! cache-rs's have been falsified by measuring outcomes -- ranking
//! precision, selection algorithm, counter semantics, eviction volume and
//! the size-cost exponent. Each was a guess about the decision, tested
//! through its effect on a miss ratio, and the miss ratio cannot tell
//! "decided differently" from "was shown different items".
//!
//! This records the decision itself. For every item a merge pass judges, it
//! tallies the item's size band and frequency band against whether the pass
//! kept it. Two engines with the same retention *rate* in every cell decide
//! identically and differ only in what reaches them, which is a question
//! about chain selection. Two engines with different rates in the same cell
//! decide differently, and the cell says where.
//!
//! The bucketing is duplicated verbatim in the cache-rs fork. It has to be:
//! a comparison across two implementations of the measurement measures the
//! implementations as much as the caches.
//!
//! # Why the accumulator is per thread
//!
//! Same reason as [`crate::clock`]: the cache spawns no threads, so every
//! merge pass runs on the caller's thread, and a process-global would leak
//! between tests that cargo runs in parallel. That cost this codebase four
//! flaky tests once already.
//!
//! Behind `retention-trace` because the record sits inside the merge loop.
//! With the feature off there is no tally and no branch.

/// Size bands, by item stride: `<64B`, then powers of two to `>=512KiB`.
pub const SIZE_BANDS: usize = 14;

/// Frequency bands: 1, 2-3, 4-7, 8-15, 16-63, 64+.
pub const FREQ_BANDS: usize = 6;

/// The band an item's stride falls in.
#[inline]
pub fn size_band(stride: u32) -> usize {
    if stride < 64 {
        return 0;
    }
    // 64 -> 1, 128 -> 2, ... saturating at the top band.
    let log = (u32::BITS - 1 - stride.leading_zeros()) as usize;
    (log - 5).min(SIZE_BANDS - 1)
}

/// The band an item's frequency falls in.
#[inline]
pub fn freq_band(freq: u8) -> usize {
    match freq {
        0 | 1 => 0,
        2..=3 => 1,
        4..=7 => 2,
        8..=15 => 3,
        16..=63 => 4,
        _ => 5,
    }
}

/// Counts of items judged and items kept, per size and frequency band.
#[derive(Clone, Copy)]
pub struct RetentionTrace {
    /// Items a merge pass judged.
    pub considered: [[u64; FREQ_BANDS]; SIZE_BANDS],
    /// Items it kept.
    pub kept: [[u64; FREQ_BANDS]; SIZE_BANDS],
}

impl Default for RetentionTrace {
    fn default() -> Self {
        Self {
            considered: [[0; FREQ_BANDS]; SIZE_BANDS],
            kept: [[0; FREQ_BANDS]; SIZE_BANDS],
        }
    }
}

#[cfg(feature = "retention-trace")]
thread_local! {
    static TRACE: core::cell::RefCell<RetentionTrace> =
        const { core::cell::RefCell::new(RetentionTrace {
            considered: [[0; FREQ_BANDS]; SIZE_BANDS],
            kept: [[0; FREQ_BANDS]; SIZE_BANDS],
        }) };
}

/// Record one retention decision.
#[inline]
pub fn record(stride: u32, freq: u8, kept: bool) {
    #[cfg(feature = "retention-trace")]
    {
        let (s, f) = (size_band(stride), freq_band(freq));
        TRACE.with(|t| {
            let mut t = t.borrow_mut();
            t.considered[s][f] += 1;
            if kept {
                t.kept[s][f] += 1;
            }
        });
    }
    #[cfg(not(feature = "retention-trace"))]
    {
        let _ = (stride, freq, kept);
    }
}

/// The decisions recorded on this thread so far.
#[cfg(feature = "retention-trace")]
pub fn snapshot() -> RetentionTrace {
    TRACE.with(|t| *t.borrow())
}

/// Forget every decision recorded on this thread.
///
/// Called at the warmup boundary, so the table covers the measured window
/// rather than the window plus whatever the warmup happened to do.
#[cfg(feature = "retention-trace")]
pub fn reset() {
    TRACE.with(|t| *t.borrow_mut() = RetentionTrace::default());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bands must be contiguous and monotone, because the cache-rs fork
    /// reimplements them and a comparison across two bucketings is not a
    /// comparison of two caches.
    #[test]
    fn size_bands_are_monotone_and_cover_every_stride() {
        let mut last = 0;
        for stride in [
            0u32,
            1,
            63,
            64,
            65,
            127,
            128,
            1023,
            1024,
            65535,
            1 << 20,
            u32::MAX,
        ] {
            let b = size_band(stride);
            assert!(b < SIZE_BANDS, "stride {stride} fell outside the table");
            assert!(b >= last, "band went backwards at stride {stride}");
            last = b;
        }
        assert_eq!(size_band(0), 0);
        assert_eq!(size_band(63), 0, "below 64 is one band");
        assert_eq!(size_band(64), 1, "64 opens the first power-of-two band");
        assert_eq!(size_band(127), 1, "a band runs to the next power of two");
        assert_eq!(size_band(128), 2);
        assert_eq!(
            size_band(u32::MAX),
            SIZE_BANDS - 1,
            "the top band saturates"
        );
    }

    /// Frequency 0 and 1 share a band deliberately: 0 means the hashtable no
    /// longer knows the item, 1 means it was inserted and never read again,
    /// and neither is evidence of use.
    #[test]
    fn freq_bands_are_monotone_and_start_at_the_unused() {
        assert_eq!(freq_band(0), 0);
        assert_eq!(freq_band(1), 0);
        assert_eq!(freq_band(2), 1);
        assert_eq!(freq_band(3), 1);
        assert_eq!(freq_band(4), 2);
        assert_eq!(freq_band(255), FREQ_BANDS - 1);
        let mut last = 0;
        for f in 0..=255u8 {
            let b = freq_band(f);
            assert!(b < FREQ_BANDS);
            assert!(b >= last, "band went backwards at frequency {f}");
            last = b;
        }
    }
}
