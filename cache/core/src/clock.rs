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

#[cfg(feature = "virtual-clock")]
use core::cell::Cell;

#[cfg(feature = "virtual-clock")]
thread_local! {
    /// Unix seconds to report on this thread instead of the system clock,
    /// or 0 for "use the system clock". Zero is safe as the sentinel: it is
    /// the epoch itself, and no live cache entry can carry it.
    static VIRTUAL_NOW: Cell<u32> = const { Cell::new(0) };
}

/// Current time in unix seconds, as every expiry check sees it.
#[inline]
pub fn now_unix_secs() -> u32 {
    #[cfg(feature = "virtual-clock")]
    {
        let virtual_secs = VIRTUAL_NOW.with(Cell::get);
        if virtual_secs != 0 {
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

/// Drive this thread's expiry from a caller-supplied clock, in unix seconds.
///
/// Intended for trace replay, where the caller advances it from record
/// timestamps. Passing 0 restores the system clock.
#[cfg(feature = "virtual-clock")]
pub fn set_virtual_now(unix_secs: u32) {
    VIRTUAL_NOW.with(|now| now.set(unix_secs));
}

/// Restore the system clock on this thread.
#[cfg(feature = "virtual-clock")]
pub fn clear_virtual_now() {
    VIRTUAL_NOW.with(|now| now.set(0));
}

/// The virtual time in force on this thread, or `None` for the system clock.
#[cfg(feature = "virtual-clock")]
pub fn virtual_now() -> Option<u32> {
    match VIRTUAL_NOW.with(Cell::get) {
        0 => None,
        secs => Some(secs),
    }
}
