//! Keys read from the io_uring disk tier for the command in progress.
//!
//! An entry on a flushed io_uring segment has its key only on disk. A key
//! check on such an entry answers [`Verdict::Unknown`] unless the key has
//! been read for the command being executed. The server reads it
//! asynchronously and records it here with [`KeyMemo::record`], then runs
//! the command again.
//!
//! The server owns one `KeyMemo` per connection and installs it for the
//! duration of each synchronous attempt with [`with_key_memo`]. Cache code
//! reaches the installed memo through the functions in this module. With no
//! memo installed, which is the case for every caller other than the
//! server, a check that would answer `Unknown` still does, and no command is
//! suspended.
//!
//! Once a command meets an unknown key, [`KeyMemo::unresolved`] holds its
//! location and every later cache operation in the same command refuses
//! before changing anything. The caller then discards the command's
//! response, reads the key, records it, and runs the command again.
//!
//! A write that was stored but whose duplicate resolution met an unknown
//! entry leaves [`KeyMemo::pending_resolve`] set instead. The command has
//! finished; the caller reads that entry's key, records it, and calls
//! [`crate::Cache::resolve_key`] until nothing is pending.

use std::cell::{Cell, RefCell};

use crate::hashtable::Verdict;
use crate::location::Location;

/// Keys read for one command, and what that command is waiting on.
#[derive(Debug, Default)]
pub struct KeyMemo {
    /// Location to the key read there, or `None` for a location whose
    /// segment could not be pinned for a read. `None` answers every key
    /// `Mismatch`.
    keys: RefCell<Vec<(Location, Option<Vec<u8>>)>>,
    /// The location whose key the command needs next.
    unresolved: Cell<Option<Location>>,
    /// A key whose write was stored but whose duplicate resolution met an
    /// unknown entry, and that entry's location; see
    /// [`crate::Hashtable::resolve`].
    pending_resolve: RefCell<Option<(Vec<u8>, Location)>>,
}

impl KeyMemo {
    /// An empty memo.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the key read at `location`. Clears [`Self::unresolved`] if it
    /// named `location`.
    ///
    /// Record `None` only when [`crate::Cache::key_read`] returned `None`:
    /// the segment could not be pinned, because it is freed, condemned,
    /// expired or reused, so its entries are being removed. Checks then
    /// answer `Mismatch` for `location`. A read that failed or returned too
    /// few bytes is not that case: answering `Mismatch` would let a write
    /// insert a second entry beside a live one. Read again, or fail the
    /// command without recording.
    pub fn record(&self, location: Location, key: Option<Vec<u8>>) {
        let mut keys = self.keys.borrow_mut();
        match keys.iter_mut().find(|(l, _)| *l == location) {
            Some(entry) => entry.1 = key,
            None => keys.push((location, key)),
        }
        if self.unresolved.get() == Some(location) {
            self.unresolved.set(None);
        }
    }

    /// The location whose key the command needs before it can run again.
    pub fn unresolved(&self) -> Option<Location> {
        self.unresolved.get()
    }

    /// The key a stored write still needs resolved with
    /// [`crate::Cache::resolve_key`], and the location whose key must be
    /// read and recorded first.
    pub fn pending_resolve(&self) -> Option<(Vec<u8>, Location)> {
        self.pending_resolve.borrow().clone()
    }

    /// Forget everything: called when a command completes.
    pub fn clear(&self) {
        self.keys.borrow_mut().clear();
        self.unresolved.set(None);
        *self.pending_resolve.borrow_mut() = None;
    }

    /// Whether the memo holds nothing.
    pub fn is_empty(&self) -> bool {
        self.keys.borrow().is_empty()
            && self.unresolved.get().is_none()
            && self.pending_resolve.borrow().is_none()
    }

    fn answer(&self, location: Location, key: &[u8]) -> Option<Verdict> {
        let keys = self.keys.borrow();
        let (_, read) = keys.iter().find(|(l, _)| *l == location)?;
        Some(match read {
            Some(read) if read.as_slice() == key => Verdict::Match,
            _ => Verdict::Mismatch,
        })
    }
}

thread_local! {
    static ACTIVE: Cell<*const KeyMemo> = const { Cell::new(std::ptr::null()) };
}

/// Run `f` with `memo` installed for this thread, restoring whatever was
/// installed before when `f` returns or unwinds.
pub fn with_key_memo<R>(memo: &KeyMemo, f: impl FnOnce() -> R) -> R {
    struct Restore(*const KeyMemo);
    impl Drop for Restore {
        fn drop(&mut self) {
            ACTIVE.with(|active| active.set(self.0));
        }
    }
    let _restore = Restore(ACTIVE.with(|active| active.replace(memo)));
    f()
}

fn active<R>(f: impl FnOnce(&KeyMemo) -> R) -> Option<R> {
    ACTIVE.with(|active| {
        let ptr = active.get();
        // SAFETY: `with_key_memo` installs a pointer to a memo borrowed for
        // the whole call and restores the previous value before returning,
        // so a non-null pointer refers to a live `KeyMemo` on this thread.
        (!ptr.is_null()).then(|| f(unsafe { &*ptr }))
    })
}

/// The installed memo's answer for `key` at `location`, or `None` if no memo
/// is installed or it holds nothing for `location`.
pub(crate) fn answer(location: Location, key: &[u8]) -> Option<Verdict> {
    active(|memo| memo.answer(location, key)).flatten()
}

/// Record in the installed memo that the command needs the key at
/// `location`, as a [`crate::KeyVerifier`] does when a check answers
/// [`Verdict::Unknown`]. Without an installed memo this does nothing.
pub fn need(location: Location) {
    active(|memo| {
        if memo.unresolved.get().is_none() {
            memo.unresolved.set(Some(location));
        }
    });
}

/// Record that `key`'s write was stored but its duplicate resolution must be
/// finished once the key at `location` is read. Does not refuse later
/// operations: the write has happened. Without an installed memo this does
/// nothing, and the duplicate stays until a later write of the key resolves
/// it.
pub(crate) fn need_resolve(key: &[u8], location: Location) {
    active(|memo| *memo.pending_resolve.borrow_mut() = Some((key.to_vec(), location)));
}

/// The installed memo's unresolved location, if any. A cache operation that
/// finds one must return before changing anything, because an earlier
/// operation in the same command answered without knowing a key.
pub(crate) fn unresolved_location() -> Option<Location> {
    active(|memo| memo.unresolved.get()).flatten()
}

/// `KeyUnresolved` for the installed memo's unresolved location, if any; see
/// [`unresolved_location`].
pub(crate) fn refusal() -> Option<crate::CacheError> {
    unresolved_location().map(crate::CacheError::KeyUnresolved)
}

/// Clear the installed memo's unresolved location after a hashtable write
/// returned `Ok`. A check can answer `Unknown` only after such a write has
/// published its entry, in its duplicate scan; the write happened, so later
/// operations in the command must not refuse. The caller records what is
/// left to do with [`need_resolve`].
pub(crate) fn clear_unresolved_after_publish() {
    active(|memo| memo.unresolved.set(None));
}

/// Clear the installed memo's pending resolve: [`crate::Hashtable::resolve`]
/// met no unknown entry.
pub(crate) fn resolve_done() {
    active(|memo| *memo.pending_resolve.borrow_mut() = None);
}

/// Record that the key at `location` cannot be read (its segment cannot be
/// pinned), so checks answer `Mismatch` for it in this command. Returns
/// `false` if no memo is installed.
pub(crate) fn unreadable(location: Location) -> bool {
    active(|memo| memo.record(location, None)).is_some()
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn answers_only_inside_with_key_memo() {
        let memo = KeyMemo::new();
        let (here, other) = (Location::new(7), Location::new(8));
        memo.record(here, Some(b"k".to_vec()));
        memo.record(other, None);

        assert_eq!(answer(here, b"k"), None, "no memo installed");
        with_key_memo(&memo, || {
            assert_eq!(answer(here, b"k"), Some(Verdict::Match));
            assert_eq!(answer(here, b"j"), Some(Verdict::Mismatch));
            assert_eq!(answer(other, b"k"), Some(Verdict::Mismatch));
            assert_eq!(answer(Location::new(9), b"k"), None);
        });
        assert_eq!(answer(here, b"k"), None, "restored after the call");
    }

    #[test]
    fn need_sets_unresolved_and_record_clears_it() {
        let memo = KeyMemo::new();
        let location = Location::new(3);
        with_key_memo(&memo, || {
            assert_eq!(unresolved_location(), None);
            need(location);
            assert_eq!(unresolved_location(), Some(location));
        });
        assert_eq!(memo.unresolved(), Some(location));
        memo.record(location, Some(b"k".to_vec()));
        assert_eq!(memo.unresolved(), None);
        memo.clear();
        assert!(memo.is_empty());
    }
}
