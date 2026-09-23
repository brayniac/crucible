//! The cache's notion of the current time.
//!
//! Every expiry check reads this. In production it is the coarse system
//! clock and this module compiles away to exactly that call.
//!
//! A trace replay needs it to follow the trace instead. A replay consumes
//! hours of recorded time in seconds of wall clock, so a TTL shorter than
//! the run never fires and expiry is measured as though it did not exist --
//! which is precisely the mechanism TTL-bucketed segments exist to exploit.
//! Measured on one trace: 2200s of recorded time replayed in 16s, so a 120s
//! TTL carried by 27% of records fired zero times against 18 in production.
//!
//! It also makes expiry testable. Without it a test can only assert that an
//! item with an hour-long TTL has *not* expired; the positive case would
//! need the test to wait an hour.
//!
//! # Why the override is per thread
//!
//! The cache spawns no threads of its own: every read, write and expiry
//! check runs on the caller's thread. So the thread that drives a replay is
//! the thread whose expiry decisions matter, and scoping the override to it
//! is both sufficient and the tighter blast radius.
//!
//! A process-global override is the obvious alternative and is worse. It
//! reaches every thread in the process, including unrelated ones -- in this
//! codebase's own test suite a global made four tests across two modules
//! flaky, because cargo runs tests in parallel threads and one test setting
//! the clock changed what the others saw. Per-thread state cannot do that.
//!
//! The cost is that a caller which sets the clock on one thread and reads
//! the cache from another gets the system clock on the reader. A replay
//! driving several threads must therefore set it on each, which is also the
//! honest shape: each worker's expiry should follow the records it is
//! actually replaying.
//!
//! Gated behind `virtual-clock` because the check sits on the read path.
//! With the feature off there is no lookup, no branch, and no difference
//! from calling the system clock directly.

// Available in the crate's own tests regardless of the feature. Frequency
// smoothing rate-limits the counter to one increment per epoch, so any test
// that warms an item by reading it repeatedly needs time to advance -- and
// within a single test every read lands in the same coarse second. Gating
// the override on the feature alone would mean those tests could only run
// under `--features virtual-clock`, which is how a default `cargo test`
// silently stops covering the counter.
#[cfg(any(feature = "virtual-clock", test))]
use core::cell::Cell;

#[cfg(any(feature = "virtual-clock", test))]
thread_local! {
    /// Seconds to report on this thread instead of the system clock.
    ///
    /// `Option` rather than a zero sentinel. Trace timestamps are seconds
    /// from the start of the trace, so a trace legitimately begins at 0,
    /// and a sentinel would silently hand those records the system clock --
    /// then jump the cache backwards by decades on the first non-zero one,
    /// stranding everything written during the gap with an expiry no
    /// subsequent read could reach.
    static VIRTUAL_NOW: Cell<Option<u32>> = const { Cell::new(None) };
}

/// Current time in unix seconds, as every expiry check sees it.
#[inline]
pub fn now_unix_secs() -> u32 {
    #[cfg(any(feature = "virtual-clock", test))]
    {
        if let Some(virtual_secs) = VIRTUAL_NOW.with(Cell::get) {
            return virtual_secs;
        }
    }
    system_now_unix_secs()
}

/// The system clock, whatever the virtual clock is set to.
#[inline]
pub fn system_now_unix_secs() -> u32 {
    clocksource::coarse::UnixInstant::now()
        .duration_since(clocksource::coarse::UnixInstant::EPOCH)
        .as_secs()
}

/// Drive this thread's expiry from a caller-supplied clock.
///
/// Intended for trace replay, where the caller advances it from record
/// timestamps. The value need not be unix seconds: the cache only ever
/// compares it against expiries derived from it, so any monotonic seconds
/// count works, which is what trace-relative timestamps are.
#[cfg(any(feature = "virtual-clock", test))]
pub fn set_virtual_now(secs: u32) {
    VIRTUAL_NOW.with(|now| now.set(Some(secs)));
}

/// Restore the system clock on this thread.
#[cfg(any(feature = "virtual-clock", test))]
pub fn clear_virtual_now() {
    VIRTUAL_NOW.with(|now| now.set(None));
}

/// The virtual time in force on this thread, or `None` for the system clock.
#[cfg(any(feature = "virtual-clock", test))]
pub fn virtual_now() -> Option<u32> {
    VIRTUAL_NOW.with(Cell::get)
}

/// A clock this thread controls, restored on drop.
///
/// Frequency smoothing counts at most one increment per epoch, so a test
/// that warms an item by reading it repeatedly gets one increment however
/// many reads it issues -- every read inside a single test lands in the
/// same coarse second. `tick` advances the clock so each read falls in its
/// own epoch, which is what the cache would see from accesses spread over
/// real time.
///
/// Restoring on drop is the point. The override is thread-local and a test
/// thread runs many tests, so a leaked one would freeze the clock for every
/// later test on that thread -- and the tests it would break are the expiry
/// tests, which would fail somewhere else entirely.
#[cfg(any(feature = "virtual-clock", test))]
pub struct TestClock {
    secs: core::cell::Cell<u32>,
}

#[cfg(any(feature = "virtual-clock", test))]
impl TestClock {
    /// Take control of this thread's clock, starting at a fixed second.
    ///
    /// A fixed base rather than the system clock, so a test's epochs do not
    /// depend on the second it happened to run in.
    pub fn start() -> Self {
        let clock = Self {
            secs: core::cell::Cell::new(1_000_000),
        };
        set_virtual_now(clock.secs.get());
        clock
    }

    /// Advance one second, so the next read counts against a fresh epoch.
    pub fn tick(&self) {
        self.secs.set(self.secs.get() + 1);
        set_virtual_now(self.secs.get());
    }

    /// The second currently in force.
    pub fn now(&self) -> u32 {
        self.secs.get()
    }
}

#[cfg(any(feature = "virtual-clock", test))]
impl Drop for TestClock {
    fn drop(&mut self) {
        clear_virtual_now();
    }
}
