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

/// How many times a model's reader was handed a location that no longer held
/// its key, counted across every loom execution of the current model.
///
/// These are `std` atomics, not model atomics, on purpose: they must survive
/// across loom's repeated executions of the closure, which reset all loom
/// state. They are the model's HAZARD WITNESS — see
/// [`KeyOracle::assert_hazard_reached`].
static OCCUPANT_MISS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static TOMBSTONE_MISS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Serializes witness-bearing models against each other; see
/// [`KeyOracle::arm_hazard_witness`].
static WITNESS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Proof that a model reached the hazard it claims to model.
///
/// Holds the witness lock for the duration of one model. Consumed by one of
/// its `assert_*` methods, which is what makes forgetting the check visible:
/// an unused `HazardWitness` is an unused variable.
pub(crate) struct HazardWitness(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);

impl HazardWitness {
    /// Assert that the model actually REACHED the hazard it claims to model.
    ///
    /// A model whose reader is never handed a stale location is not testing
    /// the slot protocol at all — it is asserting that a quiet table stays
    /// quiet, and it passes against arbitrarily broken code. That is not
    /// hypothetical: `loom_lookup_survives_tombstoned_relocation` shipped in
    /// exactly that state until #95, because a non-atomic read-modify-write in
    /// [`KeyOracle::tombstone`] silently collapsed loom's exploration and the
    /// reader observed zero misses across every execution.
    ///
    /// Reddening under a neutered `verify_slot` is the proof that a model CAN
    /// fail; this is the standing check that it still can, so the failure mode
    /// cannot come back unnoticed when something unrelated changes.
    ///
    /// Only for models whose hazard is expressed through the verifier. Models
    /// about CAS contention (`remove` and `convert_to_ghost` take no verifier)
    /// have a different witness and must not use this.
    pub(crate) fn assert_reached(self) {
        let occupant = OCCUPANT_MISS.load(std::sync::atomic::Ordering::Relaxed);
        let tombstone = TOMBSTONE_MISS.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            occupant + tombstone > 0,
            "model never reached its hazard: the verifier was never handed a \
             location that had stopped being the key's, so this model would \
             pass against broken code"
        );
    }

    /// As [`HazardWitness::assert_reached`], but for a model whose hazard is
    /// specifically a DELETE-MARKED occupant rather than a replaced one.
    pub(crate) fn assert_tombstone_reached(self) {
        assert!(
            TOMBSTONE_MISS.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "model never observed a delete-marked occupant, so it is not \
             exercising verify_slot's tombstone poll"
        );
    }
}

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

    /// Arm the hazard witness. Call immediately before `loom::model`, and
    /// call [`HazardWitness::assert_reached`] on the result afterwards.
    ///
    /// The returned guard holds a process-wide lock for as long as it lives.
    /// The counters are global — the verifier has no idea which model is
    /// running — and `cargo test` runs tests in PARALLEL, so without the lock
    /// two witness-bearing models would reset and read each other's counts and
    /// the witness would report whatever the scheduler happened to produce.
    /// Only witness-bearing models contend for it; the rest still run
    /// concurrently.
    pub(crate) fn arm_hazard_witness() -> HazardWitness {
        // A panicking model poisons the lock; the next model still wants a
        // working witness, so take the guard either way.
        let guard = WITNESS_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        OCCUPANT_MISS.store(0, std::sync::atomic::Ordering::Relaxed);
        TOMBSTONE_MISS.store(0, std::sync::atomic::Ordering::Relaxed);
        HazardWitness(guard)
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
        use std::sync::atomic::Ordering as O;
        if word & !DELETED != key_id(key) {
            let n = OCCUPANT_MISS.fetch_add(1, O::Relaxed) + 1;
            eprintln!(
                "MISS occupant={} tombstone={}",
                n,
                TOMBSTONE_MISS.load(O::Relaxed)
            );
            return false;
        }
        if !allow_deleted && (word & DELETED) != 0 {
            let n = TOMBSTONE_MISS.fetch_add(1, O::Relaxed) + 1;
            eprintln!(
                "MISS occupant={} tombstone={}",
                OCCUPANT_MISS.load(O::Relaxed),
                n
            );
            return false;
        }
        true
    }
}
