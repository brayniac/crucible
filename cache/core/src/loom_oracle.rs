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
//! - **recycle + refill with the SAME key at the SAME address**
//!   ([`KeyOracle::bump_incarnation`]).
//!
//! That last one used to be out of reach. `cache-core`'s [`Location`] was
//! opaque — the backend owned all 44 bits and no generation rode in them — so
//! a model had nothing to assert against, and modelling it would only have
//! proved the oracle could lie. Segment locations now carry a 6-bit
//! incarnation tag, bumped when a used incarnation ends and compared by every
//! verifier before item bytes are touched, so the oracle models it too: a
//! cell's occupant and its incarnation live in SEPARATE atomics, exactly as
//! the item bytes and the segment's metadata word do in production. That
//! separation is what makes the race real — a reader can load one and be
//! preempted before the other.
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

/// Width of the modeled incarnation tag, matching
/// [`crate::location_layout::TAG_BITS`].
const TAG_BITS: u32 = 6;
/// Mask for a modeled incarnation tag.
const TAG_MASK: u64 = (1 << TAG_BITS) - 1;

/// How many times a model's reader was handed a location that no longer held
/// its key, counted across every loom execution of the current model.
///
/// These are `std` atomics, not model atomics, on purpose: they must survive
/// across loom's repeated executions of the closure, which reset all loom
/// state. They are the model's HAZARD WITNESS — see
/// [`KeyOracle::assert_hazard_reached`].
static OCCUPANT_MISS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static TOMBSTONE_MISS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// How many times the verifier was handed a location whose incarnation had
/// already advanced — the hazard witness for the recycle models. Same
/// rationale as the two above for being `std` rather than model atomics.
static STALE_REJECT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// How many times the verifier ACCEPTED such a location. Must stay zero: it
/// can only move if the tag comparison is missing, which is precisely what
/// `loom_stale_location_never_resolves_to_the_new_occupant` asserts.
///
/// Counted separately from the guard rather than inside it, so that neutering
/// the guard (the model's proof that it can fail) still records the accept.
static STALE_ACCEPT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

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
    /// specifically a location from a SUPERSEDED INCARNATION of its segment.
    ///
    /// The occupant and tombstone witnesses cannot stand in for this one: the
    /// recycle model refills the cell with the SAME key, precisely so the
    /// occupant check has nothing to say and only the tag can reject. If this
    /// counter stays at zero the reader never raced the bump, and the model is
    /// asserting that a quiet table stays quiet.
    pub(crate) fn assert_stale_incarnation_reached(self) {
        assert!(
            STALE_REJECT.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "model never handed the verifier a location from a superseded \
             incarnation, so it would pass against a missing tag check"
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
    /// The occupant of each cell: the id of the key whose bytes live there,
    /// plus [`DELETED`]. The model's stand-in for ITEM BYTES.
    cells: [AtomicU64; NUM_CELLS],

    /// The incarnation each cell currently carries. The model's stand-in for
    /// the SEGMENT METADATA WORD, and a separate atomic for the same reason it
    /// is a separate word in production: a reader loads the tag and the bytes
    /// at two different instants, and everything interesting lives in the gap.
    tags: [AtomicU64; NUM_CELLS],
}

impl KeyOracle {
    /// All locations start vacant. Seed with [`KeyOracle::place`].
    pub(crate) fn new() -> Self {
        Self {
            cells: std::array::from_fn(|_| AtomicU64::new(0)),
            tags: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// The [`Location`] naming `cell` under its FIRST incarnation.
    ///
    /// Equivalent to `location_tagged(cell, 0)`. Every model that predates the
    /// incarnation tag uses this and stays on tag 0 throughout, so their
    /// behaviour is unchanged.
    pub(crate) fn location(cell: usize) -> Location {
        Self::location_tagged(cell, 0)
    }

    /// The [`Location`] naming `cell` under a specific incarnation.
    ///
    /// The cell index is offset by one so no cell maps to the all-zero
    /// location word, which the slot encoding reserves for "empty", and shifted
    /// above the tag so `location(cell)` keeps a stable value per cell.
    pub(crate) fn location_tagged(cell: usize, incarnation: u8) -> Location {
        debug_assert!(cell < NUM_CELLS);
        Location::new(((cell as u64 + 1) << TAG_BITS) | (incarnation as u64 & TAG_MASK))
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

    /// End the incarnation `cell` is currently on.
    ///
    /// The model's stand-in for a segment leaving `Locked` — the point at
    /// which every [`Location`] issued against the old incarnation must stop
    /// resolving. The refill that follows is an ordinary [`KeyOracle::place`],
    /// because in production the tag advances first and items are appended
    /// under the new one afterwards.
    ///
    /// # This MUST be one atomic read-modify-write
    ///
    /// Splitting it into a load and a store collapses loom's interleaving
    /// space: the hazard this models stops being reachable, and the model goes
    /// green while proving nothing. The same trap cost this fixture 183 hazard
    /// observations once already — see [`KeyOracle::tombstone`].
    pub(crate) fn bump_incarnation(&self, cell: usize) {
        debug_assert!(cell < NUM_CELLS);
        self.tags[cell].fetch_add(1, Ordering::AcqRel);
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
        STALE_REJECT.store(0, std::sync::atomic::Ordering::Relaxed);
        STALE_ACCEPT.store(0, std::sync::atomic::Ordering::Relaxed);
        HazardWitness(guard)
    }
}

impl KeyVerifier for KeyOracle {
    fn verify(&self, key: &[u8], location: Location, allow_deleted: bool) -> bool {
        let raw = location.as_raw();
        let index = raw >> TAG_BITS;
        // Locations the oracle never issued (GHOST, or a backend-shaped word a
        // model handed in by mistake) match nothing.
        if index == 0 || index > NUM_CELLS as u64 {
            return false;
        }
        let cell = (index - 1) as usize;
        use std::sync::atomic::Ordering as O;

        // The tag first, before any "item bytes" are read: that is the order
        // every production verifier uses, and the point of the check is to
        // avoid resolving into a later incarnation's item at all.
        let stale = (self.tags[cell].load(Ordering::Acquire) & TAG_MASK) != (raw & TAG_MASK);
        if stale {
            STALE_REJECT.fetch_add(1, O::Relaxed);
            // NEUTER THIS `return` to prove the recycle model can fail.
            return false;
        }

        let word = self.cells[cell].load(Ordering::Acquire);
        if word & !DELETED != key_id(key) {
            OCCUPANT_MISS.fetch_add(1, O::Relaxed);
            return false;
        }
        if !allow_deleted && (word & DELETED) != 0 {
            TOMBSTONE_MISS.fetch_add(1, O::Relaxed);
            return false;
        }
        if stale {
            // Only reachable with the guard above neutered. Recorded here
            // rather than inside the guard so the model still has something to
            // assert on when the guard is removed.
            STALE_ACCEPT.fetch_add(1, O::Relaxed);
        }
        true
    }
}

impl KeyOracle {
    /// How many times the verifier accepted a location from a superseded
    /// incarnation. Zero unless the tag comparison is missing.
    pub(crate) fn stale_accepts() -> usize {
        STALE_ACCEPT.load(std::sync::atomic::Ordering::Relaxed)
    }
}
