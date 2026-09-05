//! Model-checking fixture: a stateful location -> key oracle.
//!
//! Used by the loom models in `hashtable_impl.rs`. Everything here routes
//! through [`crate::sync`], so loom instruments the oracle's atomics.
//!
//! # Why this exists
//!
//! [`KeyVerifier`] is the seam between the hashtable's slot protocol and raw
//! storage: the hashtable NEVER touches item bytes itself, it asks the
//! verifier "does `location` hold `key`?". That makes the verifier the only
//! thing a loom model needs in order to reproduce the hazard the slot
//! protocol actually defends against — **a location whose bytes stopped
//! being this entry's while a thread was looking at them**.
//!
//! The loom models historically stubbed the verifier with `AlwaysVerifier`,
//! which answers `true` for everything. That is fine for CAS-uniqueness and
//! election-shaped invariants, but it makes every model using it structurally
//! blind to key identity: a location can be relocated, recycled, refilled
//! with somebody else's key, or freed outright and the stub keeps saying
//! "yes, your key is there". The entire verify-FAILURE half of the protocol —
//! [`MultiChoiceHashtable::verify_slot`], its relocation walk, its tombstone
//! poll, every `allow_deleted` decision — is dead code under `AlwaysVerifier`.
//!
//! [`KeyOracle`] replaces that stub with model atomics representing "which key
//! currently lives at this location". Raw segment bytes
//! stay entirely outside the model; there is no production hook and nothing to
//! compile out of release builds.
//!
//! # What it can model
//!
//! - **relocation** — the key moves to a new location and the slot is relinked
//!   in place ([`KeyOracle::drain_relocate`]);
//! - **recycle + refill** — the source segment is recycled and rewritten by
//!   another writer, so the old location now holds an unrelated key ([`OTHER`]);
//!
//! # What it deliberately cannot model
//!
//! **Recycle + refill with the SAME key at the SAME address.** In cache-rs
//! that hazard is caught by an incarnation tag packed inside the location
//! word, so a stale location fails validation even when the bytes really do
//! spell the key again. `cache-core`'s [`Location`] is opaque — the backend
//! owns all 44 bits and no generation rides in them — so there is nothing for
//! such a model to assert against. Modelling it here would only prove the
//! oracle can lie.
//!
//! # Faithfulness rules
//!
//! Models must sequence oracle mutations in the order the real system
//! performs them, or they manufacture states production cannot reach and the
//! resulting "bug" is a fiction. [`KeyOracle::drain_relocate`] encodes the one
//! ordering that matters (copy -> publish -> recycle) so callers cannot get it
//! wrong.

use crate::hashtable::{Hashtable, KeyVerifier};
use crate::hashtable_impl::MultiChoiceHashtable;
use crate::location::Location;
use crate::sync::{AtomicU64, Ordering};

/// The key every oracle-backed model tracks.
pub(crate) const KEY: &[u8] = b"key";

/// A key that only ever appears as the OCCUPANT of a recycled location: it is
/// never inserted into the hashtable. Placing it at a cell is how a model says
/// "this segment was finalized, recycled, and rewritten by an unrelated
/// writer".
pub(crate) const OTHER: &[u8] = b"other";

/// Non-zero so a vacant cell (`0`) is distinguishable from an occupied one.
const KEY_ID: u64 = 1;
const OTHER_ID: u64 = 2;

/// Set in a cell word when the occupant is delete-marked in place.
const DELETED: u64 = 1 << 32;

/// Where the subject key starts out.
pub(crate) const SRC: usize = 0;
/// An intermediate location, for models that need two successive drains.
pub(crate) const MID: usize = 1;
/// Where a relocation moves the key to.
pub(crate) const DST: usize = 2;
/// Where a racing writer publishes a replacement copy of the key.
pub(crate) const NEW: usize = 3;
/// Number of distinct storage locations the oracle models. Kept small on
/// purpose: every cell is a loom-tracked atomic.
pub(crate) const NUM_CELLS: usize = 4;

fn key_id(key: &[u8]) -> u64 {
    if key == KEY {
        KEY_ID
    } else if key == OTHER {
        OTHER_ID
    } else {
        // Unknown keys never match an occupied cell (ids start at 1).
        0
    }
}

/// A location -> key map backed by loom-tracked atomics.
///
/// One cell per modeled storage location: a cell holds the id of the key whose
/// bytes currently live there, or `0` for "nothing this model knows about".
pub(crate) struct KeyOracle {
    cells: [AtomicU64; NUM_CELLS],
}

impl KeyOracle {
    /// All locations start vacant. Seed with [`KeyOracle::place`].
    pub(crate) fn new() -> Self {
        Self {
            cells: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// The [`Location`] naming `cell`.
    ///
    /// Offset by one so no cell maps to the all-zero location word, which the
    /// slot encoding reserves for "empty".
    pub(crate) fn location(cell: usize) -> Location {
        debug_assert!(cell < NUM_CELLS);
        Location::new(cell as u64 + 1)
    }

    /// Write `key`'s bytes at `cell` — a segment write (reserve + define, or a
    /// merge's copy destination).
    ///
    /// `Release`, because in production the bytes must be visible to any
    /// thread that later observes a slot published with this location.
    pub(crate) fn place(&self, cell: usize, key: &[u8]) {
        let id = key_id(key);
        debug_assert!(id != 0, "place() called with a key the oracle cannot name");
        self.cells[cell].store(id, Ordering::Release);
    }

    /// Delete-mark the occupant of `cell` in place.
    ///
    /// The bytes still spell the key — this is a flag flip in the item header,
    /// not a rewrite — so the location keeps matching under
    /// `allow_deleted = true` and stops matching under `allow_deleted = false`.
    /// That asymmetry is the whole input to `verify_slot`'s tombstone poll.
    ///
    /// # This MUST be one atomic read-modify-write
    ///
    /// It is a `fetch_or`, matching production (`mark_deleted` is a
    /// `fetch_or` on the item flags byte). Writing it the obvious way —
    /// `load` then `store` — is not merely unfaithful, it silently destroys
    /// the model: with a non-atomic RMW in the writer, loom stopped exploring
    /// any interleaving in which the reader observes the writer mid-flight.
    /// The model then passed against deliberately broken code, because its
    /// hazard was never reached even once. Instrumenting the verifier showed
    /// the difference starkly — 0 observed misses with `load`/`store`, 183
    /// with `fetch_or`.
    ///
    /// The general lesson for this fixture: a non-atomic read-modify-write on
    /// a model atomic can collapse the explored state space to nothing, and a
    /// model that cannot reach its hazard passes for the wrong reason. Prefer
    /// a single atomic operation for every oracle mutation, and confirm each
    /// model reddens against broken code.
    pub(crate) fn tombstone(&self, cell: usize) {
        self.cells[cell].fetch_or(DELETED, Ordering::Release);
    }

    /// One merge-drain relocation of [`KEY`], in the order production performs
    /// it:
    ///
    /// 1. copy the item into `dst` — its bytes are valid there BEFORE anything
    ///    points at them;
    /// 2. relink the slot with the `Release` CAS, publishing `dst`;
    /// 3. the source segment is finalized and recycled, then rewritten by
    ///    another writer, so `src` now holds an unrelated key.
    ///
    /// Step 3 runs whether or not the relink landed: a lost relink means the
    /// item at `src` was superseded by a racing writer, and the source segment
    /// is recycled all the same.
    ///
    /// Returns whether the relink CAS landed. Models that race the drain
    /// against a mutator must tolerate `false`; models where nothing else
    /// touches the entry should assert `true`.
    pub(crate) fn drain_relocate(&self, ht: &MultiChoiceHashtable, src: usize, dst: usize) -> bool {
        self.place(dst, KEY);
        let relinked = ht.cas_location(KEY, Self::location(src), Self::location(dst), true);
        self.place(src, OTHER);
        relinked
    }

    /// Count the live hashtable entries for [`KEY`] across every modeled
    /// location — the duplicate detector for insert-path models.
    ///
    /// DESTRUCTIVE, and deliberately so: it counts by unlinking. `remove`
    /// matches on tag AND location, so each call removes at most one slot, and
    /// the inner loop catches the pathological case of two slots published
    /// with the same location. Call it once, after every thread has joined.
    ///
    /// Counting this way keeps the fixture out of `hashtable_impl`'s private
    /// internals — the alternative is a hand-rolled bucket scan, which is what
    /// the `AlwaysVerifier` models copy-paste.
    pub(crate) fn drain_live_entries(ht: &MultiChoiceHashtable) -> usize {
        let mut found = 0;
        for cell in 0..NUM_CELLS {
            while ht.remove(KEY, Self::location(cell)) {
                found += 1;
            }
        }
        found
    }
}

impl KeyVerifier for KeyOracle {
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let raw = location.as_raw();
        // Locations the oracle never issued (GHOST, or a backend-shaped word a
        // model handed in by mistake) match nothing.
        if raw == 0 || raw > NUM_CELLS as u64 {
            return false;
        }
        let word = self.cells[(raw - 1) as usize].load(Ordering::Acquire);
        if word & !DELETED != key_id(key) {
            return false;
        }
        allow_deleted || (word & DELETED) == 0
    }
}
