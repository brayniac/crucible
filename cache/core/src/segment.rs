//! Segment trait for cache storage.
//!
//! A segment is a fixed-size memory region that stores cache items sequentially.
//! Items are appended to the segment until it's full, then the segment is sealed
//! and a new one is allocated.
//!
//! This module provides:
//! - [`Segment`] - Core trait for segment operations
//! - [`SegmentGuard`] - Extension trait for zero-copy access
//! - [`SegmentCopy`] - Extension trait for tier migration
//! - [`SegmentPrune`] - Extension trait for merge eviction

use crate::error::CacheError;
use crate::item::ItemGuard;
use crate::state::{INVALID_SEGMENT_ID, Metadata, State};
use crate::sync::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

/// The single commit point for `AwaitingRelease -> Free`: free a condemned
/// segment and return it to the pool's free queue, iff it is genuinely
/// unreferenced.
///
/// Every condemned-free path in the crate routes through here --
/// `SliceSegment::release_condemned` (and through it `release_ref` and the
/// layers' race fixes), `DiskSegmentMeta::release_condemned`,
/// `BasicItemGuard::drop`, and `ValueRef::drop`. They used to carry four
/// copies of this CAS, which is how the re-validation below came to be needed
/// in four places at once; one body means one place to get it right and one
/// place to test.
///
/// `on_freed` runs after a winning CAS and before the push, for per-segment
/// bookkeeping the caller owns (`SliceSegment` zeroes `merge_count` there).
/// It must not run when the CAS loses, and it must not be visible to whoever
/// picks the segment off the free queue -- hence before the push, not after.
///
/// Returns `true` only for the caller that actually performed the transition.
///
/// # Safe to race
///
/// The CAS names the exact metadata word it loaded, so of any number of
/// concurrent callers at most one can succeed, and only that one pushes to
/// the free queue. The losers observe the changed word and return `false`.
///
/// # Why `prev == 1` is not enough on its own
///
/// Callers reach here off a `fetch_sub` that returned 1. That says the count
/// *was* 1. By the time the state load below runs, another reader may have
/// pinned the segment again -- legitimately, because the pin happens while
/// the segment is still `Sealed`, before the evictor condemns it. So the
/// sequence
///
/// ```text
/// reader A: fetch_sub -> prev == 1 (count now 0)
/// reader B: fetch_add on a still-Sealed segment, re-check passes -> pinned
/// evictor : sees ref_count == 1, condemns -> AwaitingRelease
/// reader A: loads the state, sees AwaitingRelease, frees
/// ```
///
/// frees the segment under B's live reference. Re-reading `ref_count` here is
/// what closes it: A declines, and B's own drop -- which will see
/// `prev == 1` and `AwaitingRelease` -- completes the handoff. Under the
/// SeqCst orderings this path uses, B's pin cannot be missed by this load:
/// B's `fetch_add` precedes its re-check, which saw a pre-condemn state and so
/// precedes the condemn CAS, which precedes the state load below.
///
/// Found by `shuttle_reader_never_coexists_with_committed_drain`; pinned
/// deterministically by `segment::tests::condemned_segment_is_not_freed_*`.
///
/// # Safety
///
/// `free_queue` must point to the pool's free queue and remain valid for the
/// pool's lifetime; `segment_id` must be this segment's id within that pool.
/// `#[inline]`: this sits on the hot path. `BasicItemGuard::drop` and
/// `ValueRef::drop` reach it on every GET whose guard was the last reference,
/// which is the common case, and the early return for a non-condemned segment
/// is a single load and branch. It inlines at all six call sites today; the
/// attribute is here so that stays deliberate.
#[inline]
pub(crate) unsafe fn try_free_condemned<F: FnOnce()>(
    ref_count: &AtomicU32,
    metadata: &AtomicU64,
    free_queue: *const crossbeam_deque::Injector<u32>,
    segment_id: u32,
    on_freed: F,
) -> bool {
    // SeqCst on the load and the CAS: this is the commit point of the
    // condemned handoff, reached from both halves of the Dekker pair (the last
    // reader's decrement and the evictor's `ref_count == 0` race fix). This
    // load has to be ordered after whichever of those the caller just
    // performed, or a caller can observe a pre-condemn word and decline a
    // release it owes. See [`Segment::ref_count_seqcst`].
    let packed = metadata.load(Ordering::SeqCst);
    let meta = Metadata::unpack(packed);

    if meta.state != State::AwaitingRelease {
        return false;
    }

    // Re-validate the caller's `prev == 1` -- see the note above. Deleting
    // this is caught by `condemned_segment_is_not_freed_while_referenced`.
    if ref_count.load(Ordering::SeqCst) != 0 {
        return false;
    }

    // `AwaitingRelease -> Free` is unconditionally the end of a used
    // incarnation: the segment was condemned while live and its last reader
    // has just dropped. Always bump.
    //
    // `with_chain_ids` must stay: returning a freed segment to the free queue
    // still linked to its neighbours is its own defect.
    let new_meta = meta
        .with_state(State::Free)
        .with_chain_ids(INVALID_SEGMENT_ID, INVALID_SEGMENT_ID)
        .bump_incarnation();

    if metadata
        .compare_exchange(packed, new_meta.pack(), Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        on_freed();
        // SAFETY: the caller guarantees `free_queue` is the pool's queue and
        // valid for its lifetime, and that `segment_id` names this segment.
        unsafe {
            (*free_queue).push(segment_id);
        }

        #[cfg(all(test, not(feature = "loom"), not(feature = "shuttle")))]
        interpose::fire(interpose::FREE_AFTER_PUBLISH);

        true
    } else {
        false
    }
}

/// The two-phase reader pin, in one body: check the state, `fetch_add`, then
/// re-check.
///
/// Returns `true` iff the caller leaves holding a reference.
///
/// # Why the second check exists
///
/// The pin and the evictor's claim are mirror images of each other:
///
/// ```text
/// reader : W(ref_count)  then R(state)
/// evictor: W(state)      then R(ref_count)
/// ```
///
/// The pre-increment check is a pure fast path -- its answer is re-derived
/// below, and it buys only the cost of an RMW on an obviously-inaccessible
/// segment. The *post*-increment check is the load-bearing one: it is the
/// only read that can see a claim published after this thread's increment,
/// and it is what the evictor's `ref_count` load pairs against. Delete it and
/// a reader pins a segment the evictor has already claimed -- the hazard
/// #127/#128 closed and #133 finished.
///
/// # One body, five call sites
///
/// `SliceSegment::{get_item, get_item_verified, get_value_ref_raw,
/// try_acquire_read}` and `DiskSegmentMeta::try_acquire_read` each used to
/// carry their own copy of this sequence, which is why #134 could delete any
/// one of them and watch the whole suite stay green: there was no single
/// place a test could aim at. `admits` is the only thing that differed -- the
/// guard sites pass [`State::admits_guard_reader`], the key-verify sites the
/// wider [`State::admits_verify_reader`], which still admits `Draining` so
/// the demoter can verify keys on a segment it is draining.
///
/// `back_out` is the caller's `release_ref`: the decrement *plus* the
/// `AwaitingRelease -> Free` handoff, because a back-out that removed the
/// last reference from a condemned segment owes that free and nothing else
/// will ever do it (#127 part 2).
///
/// # Testability
///
/// The window this protocol closes -- between the `fetch_add` and the
/// re-check -- cannot be entered by any single-threaded caller and cannot be
/// entered *deterministically* by threads. [`interpose`] is the interposition
/// point: a test installs a closure that runs the racing transition by hand at
/// exactly that instant, with no scheduler, and drives the real production
/// body rather than a copy of it.
///
/// # Cost
///
/// `#[inline]`, and generic over `admits` so the predicate is a direct call
/// that folds into a pair of compares rather than an indirect one. This is
/// the GET path; a release build must emit no standalone symbol for it.
#[inline]
pub(crate) fn try_acquire_pin<A, B>(
    ref_count: &AtomicU32,
    metadata: &AtomicU64,
    admits: A,
    back_out: B,
) -> bool
where
    A: Fn(State) -> bool,
    B: FnOnce(),
{
    // `Acquire`: a pure fast path whose answer is re-derived below, ordered
    // against nothing this thread has stored yet.
    if !admits(Metadata::unpack(metadata.load(Ordering::Acquire)).state) {
        return false;
    }

    // SeqCst, both halves. Acquire/release permits both loads of the Dekker
    // pair above to come back stale, so the evictor clears or frees the
    // segment while this reader believes its pin is good. Only the SeqCst
    // total order forbids it -- and pairing a SeqCst `fetch_add` with an
    // `Acquire` re-check buys nothing, because both accesses have to sit in
    // the one total order. See [`Segment::ref_count_seqcst`] (#129).
    ref_count.fetch_add(1, Ordering::SeqCst);

    #[cfg(all(test, not(feature = "loom"), not(feature = "shuttle")))]
    interpose::fire(interpose::ACQUIRE_AFTER_INCREMENT);

    if !admits(Metadata::unpack(metadata.load(Ordering::SeqCst)).state) {
        #[cfg(all(test, not(feature = "loom"), not(feature = "shuttle")))]
        interpose::fire(interpose::ACQUIRE_BEFORE_BACKOUT);

        back_out();
        return false;
    }

    true
}

/// Interposition points inside the reader/evictor handshake, for tests only.
///
/// The windows this protocol is built around are only entered when the other
/// side moves inside them, which no single-threaded test can arrange and no
/// multi-threaded one can arrange *deterministically*. This hook lets a test
/// park one side at a named instant and run the other by hand, with no
/// scheduler -- the same "put the race where it happens" idiom cache-rs uses.
///
/// Ambient (a thread-local) rather than a parameter, so every hooked function
/// keeps its exact production signature and body: the test drives the real
/// `SliceSegment::get_item` / `FifoLayer::evict_nonblocking`, not a copy.
///
/// Compiled out of every non-test build, and deliberately also out of the
/// model-checking ones: a `std` thread-local inside a loom or shuttle
/// execution is state the checker cannot see.
#[cfg(all(test, not(feature = "loom"), not(feature = "shuttle")))]
pub(crate) mod interpose {
    use std::cell::RefCell;

    /// [`try_acquire_pin`]: after the `ref_count` increment, before the state
    /// re-check.
    pub(crate) const ACQUIRE_AFTER_INCREMENT: u8 = 0;
    /// [`try_acquire_pin`]: after the re-check failed, before the back-out.
    pub(crate) const ACQUIRE_BEFORE_BACKOUT: u8 = 1;
    /// `layer::try_claim_for_clear`: after the caller decided to claim,
    /// before the `Draining -> Locked` CAS is published. A reader pinned here
    /// is one that arrives while the segment is still admitting -- the arrival
    /// a `ref_count` read taken *before* the claim would miss (#133).
    pub(crate) const CLAIM_BEFORE_CAS: u8 = 2;
    /// `layer::wait_for_readers`: before the first `ref_count` poll. Fired
    /// from inside the wait, so it moves with it: a test that reads the
    /// segment's state here sees whether the blocking paths claim before they
    /// wait or after (#133).
    pub(crate) const WAIT_BEFORE_POLL: u8 = 3;
    /// [`try_free_condemned`]: immediately after the freed segment is pushed
    /// to the free queue, and only on the path that actually freed it.
    ///
    /// This is the instant the segment becomes another thread's to reserve. A
    /// test parks here and reserves it by hand, which is the only way to make
    /// *late* per-segment cleanup observable from one thread: anything a
    /// caller does after `try_free_condemned` returns is already operating on
    /// a segment someone else may own. `IoUringDiskLayer`'s staging-buffer
    /// return is the cleanup that has to be inside `on_freed` for that reason.
    pub(crate) const FREE_AFTER_PUBLISH: u8 = 4;

    /// What a test installs: called with the phase.
    pub(crate) type Hook = Box<dyn FnMut(u8)>;

    thread_local! {
        static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    /// Uninstalls the hook when dropped, so a failing test cannot leak it
    /// onto the next test sharing this thread.
    pub(crate) struct Installed;

    impl Drop for Installed {
        fn drop(&mut self) {
            HOOK.with(|h| *h.borrow_mut() = None);
        }
    }

    /// Install `hook` on this thread until the returned guard drops.
    pub(crate) fn install(hook: Hook) -> Installed {
        HOOK.with(|h| *h.borrow_mut() = Some(hook));
        Installed
    }

    /// Run the installed hook, if any. It is taken out of the slot for the
    /// duration of the call, so an acquire reached from *inside* the hook
    /// runs unhooked (and cannot re-borrow the cell).
    pub(crate) fn fire(phase: u8) {
        let taken = HOOK.with(|h| h.borrow_mut().take());
        if let Some(mut hook) = taken {
            hook(phase);
            HOOK.with(|h| {
                let mut slot = h.borrow_mut();
                if slot.is_none() {
                    *slot = Some(hook);
                }
            });
        }
    }
}

/// Minimal trait for key verification, used by hashtables.
///
/// This is the only segment functionality the hashtable needs. By using this
/// minimal trait, types that don't provide full segment access (like SSD-backed
/// segments that require async I/O) can still work with the hashtable.
pub trait SegmentKeyVerify {
    /// Take a read reference on this segment, or fail if it is not readable.
    ///
    /// On success the caller MUST call [`SegmentKeyVerify::release_read`].
    /// Prefer [`SegmentKeyVerify::verify_key_guarded`], which pairs them.
    ///
    /// Holding a reference is what stops a drained segment being recycled and
    /// rewritten underneath a reader: eviction checks `ref_count() == 0` before
    /// condemning. Acquiring must therefore increment first and re-check the
    /// state after, or it races the evictor that already saw zero.
    ///
    /// No default body on purpose: a segment type that silently answered `true`
    /// would read bytes with nothing holding the segment still.
    fn try_acquire_read(&self) -> bool;

    /// Release a reference taken by [`SegmentKeyVerify::try_acquire_read`].
    fn release_read(&self);

    /// Verify a key under a read guard, checking the incarnation first.
    ///
    /// This is the ONLY correct order, and it is centralised here so no call
    /// site can get it wrong:
    ///
    /// 1. **guard** -- so the incarnation cannot advance underneath us;
    /// 2. **check the tag** -- so this is the incarnation the location names;
    /// 3. **read the bytes**;
    /// 4. **release**.
    ///
    /// The guard alone is not enough: it stops the segment being reclaimed but
    /// says nothing about which incarnation it holds. The tag alone is not
    /// enough either -- that was crucible#109: a reader could pass the check
    /// and then be preempted while the segment was drained and refilled, so its
    /// byte reads raced `append_with_header`'s `copy_nonoverlapping`.
    fn verify_key_guarded(
        &self,
        offset: u32,
        key: &[u8],
        allow_deleted: bool,
        incarnation: u8,
    ) -> bool {
        if !self.try_acquire_read() {
            return false;
        }
        let matched = self.incarnation() == incarnation
            && self.verify_key_at_offset(offset, key, allow_deleted);
        self.release_read();
        matched
    }

    /// The incarnation tag this segment currently carries.
    ///
    /// A location whose tag differs names a previous incarnation of this
    /// segment and must not be resolved: the segment has been drained and
    /// refilled, and the offset now holds a different item.
    ///
    /// This lives here rather than on [`Segment`] because it is part of
    /// deciding whether a location verifies, and the hashtable's verifiers are
    /// generic over this trait alone. Widening them to [`Segment`] would defeat
    /// the point of this trait being minimal.
    ///
    /// Distinct from [`Segment::generation`]: that is a 16-bit counter bumped
    /// on `Free -> Reserved` and consumed by `CasToken`. This one is 6 bits and
    /// advances only when a *used* incarnation ends.
    fn incarnation(&self) -> u8;

    /// Verify that the key at the given offset matches.
    ///
    /// Used by hashtables to verify tag matches against actual keys.
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `key`: The key to compare against
    /// - `allow_deleted`: If `true`, matches deleted items
    ///
    /// # Returns
    /// `true` if the key matches (and item is not deleted unless `allow_deleted`).
    fn verify_key_at_offset(&self, offset: u32, key: &[u8], allow_deleted: bool) -> bool;

    /// Verify key and return parsed header info if matched.
    ///
    /// This avoids double-parsing when the caller needs header information
    /// after verification (e.g., for computing item size in delete operations).
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `key`: The key to compare against
    /// - `allow_deleted`: If `true`, matches deleted items
    ///
    /// # Returns
    /// `Some((key_len, optional_len, value_len))` if key matches, `None` otherwise.
    fn verify_key_with_header(
        &self,
        offset: u32,
        key: &[u8],
        allow_deleted: bool,
    ) -> Option<(u8, u8, u32)>;

    /// Verify key and check expiration in one parse.
    ///
    /// For segments with per-item TTL (TtlHeader), this checks the item's
    /// expiration time. For segment-level TTL (BasicHeader), this just
    /// verifies the key since TTL is checked at the segment level.
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `key`: The key to compare against
    /// - `now`: Current time as coarse seconds since epoch
    ///
    /// # Returns
    /// `Some((key_len, optional_len, value_len))` if key matches and item
    /// is not expired, `None` otherwise.
    fn verify_key_unexpired(&self, offset: u32, key: &[u8], now: u32) -> Option<(u8, u8, u32)>;
}

/// Trait for a cache segment.
///
/// Segments are the fundamental storage unit in segment-based caches.
/// They provide:
/// - Sequential item storage with atomic append
/// - State machine for concurrent access control
/// - Chain pointers for linked list organization (TTL buckets, FIFO, etc.)
/// - Reference counting for safe concurrent reads
///
/// # Concurrency
///
/// Segment operations use atomic primitives and CAS loops for thread safety.
/// The state machine ensures that:
/// - Only `Live` segments accept writes
/// - Only `Live`, `Sealed`, and `Relinking` segments allow reads
/// - Reference counting prevents clearing segments with active readers
///
/// # TTL Handling
///
/// Segments support two TTL models:
/// - **Segment-level TTL**: All items share `expire_at()`, stored in segment metadata
/// - **Per-item TTL**: Each item has individual TTL in `TtlHeader`
///
/// Use `item_ttl()` to get the TTL of a specific item, which will return the
/// appropriate TTL based on how the segment stores TTL information.
pub trait Segment: SegmentKeyVerify + Send + Sync {
    // ========== Identity ==========

    /// Get the segment ID within its pool.
    fn id(&self) -> u32;

    /// Get the pool ID this segment belongs to.
    ///
    /// Useful when multiple pools exist (e.g., RAM and SSD tiers).
    fn pool_id(&self) -> u8;

    /// Get the generation counter.
    ///
    /// Incremented each time the segment is reused. Combined with
    /// (pool_id, segment_id, offset), forms a unique identifier that
    /// prevents ABA problems.
    fn generation(&self) -> u16;

    /// Increment the generation counter (wraps at u16::MAX).
    fn increment_generation(&self);

    // ========== Capacity ==========

    /// Get the offset alignment factor of the pool that owns this segment.
    ///
    /// Always a power of two and at least 8. Item offsets are multiples of it,
    /// because a `Location` stores `offset / align_bytes` and cannot represent
    /// anything finer -- see `LocationLayout`.
    fn align_bytes(&self) -> u32;

    /// Round an item size up to this segment's offset alignment.
    ///
    /// This is the segment's *stride*: appends advance the write offset by it,
    /// and **every scan must advance by it too**. A scan that advances by
    /// `header.padded_size()` -- which rounds to 8 regardless of the pool --
    /// desyncs after the first item on a coarser-aligned pool and then reads
    /// garbage headers. Both sides are `u32`, so the compiler cannot catch the
    /// mismatch; this method exists so there is only one definition to get
    /// right.
    ///
    /// Safe to apply to an already-8-padded size: `align_bytes` is a power of
    /// two and at least 8, so rounding twice is the same as rounding once.
    ///
    /// Note this is *not* the size of the item's bytes. A bounds check against
    /// `capacity` should use `header.padded_size()`, since the padding beyond
    /// it is dead space that the last item in a segment need not own.
    #[inline]
    fn item_stride(&self, item_size: usize) -> u32 {
        let align = self.align_bytes() as usize;
        debug_assert!(align.is_power_of_two() && align >= 8);
        ((item_size + align - 1) & !(align - 1)) as u32
    }

    /// Get the total data capacity in bytes.
    fn capacity(&self) -> usize;

    /// Get the current write offset (next write position).
    fn write_offset(&self) -> u32;

    /// Get free space remaining in the segment.
    fn free_space(&self) -> usize {
        self.capacity().saturating_sub(self.write_offset() as usize)
    }

    // ========== Statistics ==========

    /// Get the count of live (non-deleted) items.
    fn live_items(&self) -> u32;

    /// Get the total bytes used by live items.
    fn live_bytes(&self) -> u32;

    /// Get the current reference count (active readers).
    ///
    /// `Acquire`. Use this for statistics, diagnostics, and heuristic scans --
    /// anywhere a stale answer is merely suboptimal. When the answer *gates* an
    /// action that is exclusive against readers, use [`Self::ref_count_seqcst`].
    fn ref_count(&self) -> u32;

    /// Get the current reference count, ordered after a preceding `SeqCst`
    /// state transition -- the condemner half of the Dekker pair.
    ///
    /// The drain/condemn protocol is a store-buffering (Dekker) pattern:
    ///
    /// ```text
    /// reader (try_acquire_read / get_item / get_value_ref_raw):
    ///     ref_count.fetch_add(1)   // store
    ///     load state               // load  -> back out if inaccessible
    ///
    /// condemner (the layers' evict paths):
    ///     cas state -> Draining / AwaitingRelease   // store
    ///     load ref_count                            // load -> act if zero
    /// ```
    ///
    /// Acquire/release does **not** forbid the outcome where both loads come
    /// back stale: the reader's re-check still sees an admitting state while
    /// the condemner sees `ref_count == 0`, so both proceed and the condemner
    /// clears or frees the segment under a live reader. Only the `SeqCst`
    /// total order rules it out, and only if *both* halves are `SeqCst` --
    /// which is why the reader's `fetch_add` and re-check are `SeqCst` too.
    /// This is the same reason crossbeam-epoch's `pin()` is `SeqCst`.
    ///
    /// Note that no in-tree tool can *prove* the distinction. loom
    /// over-approximates and reports the store-buffering outcome even for a
    /// pure-`SeqCst` litmus, so it is green either way; shuttle executes
    /// sequentially consistently and so treats an AcqRel program as `SeqCst`,
    /// so it is green either way too. The justification is the memory model
    /// plus precedent (pelikan-io/cache-rs `SegmentHeader::ref_count_seqcst`),
    /// not a red test. See crucible#129.
    ///
    /// A load that is *not* preceded by a transition it must be ordered
    /// against gains nothing from `SeqCst` -- see the call sites for which are
    /// which.
    fn ref_count_seqcst(&self) -> u32;

    // ========== State Machine ==========

    /// Get the current state.
    fn state(&self) -> State;

    /// Attempt to transition from `Free` to `Reserved`.
    ///
    /// Resets all segment statistics for reuse.
    ///
    /// # Returns
    /// `true` if successful, `false` if not in `Free` state.
    fn try_reserve(&self) -> bool;

    /// Attempt to transition back to `Free` state.
    ///
    /// Valid from `Reserved`, `Linking`, or `Locked` states.
    ///
    /// # Returns
    /// `true` if successful, `false` if already `Free` (idempotent).
    ///
    /// # Panics
    /// May panic if in an invalid state for release (`Live`, `Sealed`, etc.).
    fn try_release(&self) -> bool;

    /// Atomically update state and chain pointers.
    ///
    /// # Parameters
    /// - `expected_state`: The state we expect the segment to be in
    /// - `new_state`: The state to transition to
    /// - `new_next`: New next pointer, or `None` to preserve current
    /// - `new_prev`: New prev pointer, or `None` to preserve current
    ///
    /// # Returns
    /// `true` if the CAS succeeded, `false` if expected state didn't match.
    fn cas_metadata(
        &self,
        expected_state: State,
        new_state: State,
        new_next: Option<u32>,
        new_prev: Option<u32>,
    ) -> bool;

    /// Try to free a segment stuck in `AwaitingRelease` state.
    ///
    /// This handles the race where the last reader drops between the eviction
    /// thread's `ref_count()` check and the CAS to `AwaitingRelease`. In that
    /// window the reader sees `Draining` (not `AwaitingRelease`) and does nothing,
    /// leaving the segment orphaned. Calling this after the CAS reclaims it.
    ///
    /// Returns `true` if the segment was freed, `false` otherwise.
    fn release_condemned(&self) -> bool;

    // ========== Chain Pointers ==========

    /// Get the next segment ID in the chain, or `None` if this is the tail.
    fn next(&self) -> Option<u32>;

    /// Get the previous segment ID in the chain, or `None` if this is the head.
    fn prev(&self) -> Option<u32>;

    // ========== TTL ==========

    /// Get the segment-level expiration time (coarse seconds since epoch).
    ///
    /// For segments using segment-level TTL (BasicHeader), all items inherit
    /// this expiration time.
    fn expire_at(&self) -> u32;

    /// Set the segment-level expiration time.
    fn set_expire_at(&self, expire_at: u32);

    /// Get the remaining TTL for the entire segment.
    ///
    /// # Parameters
    /// - `now`: Current time as coarse seconds since epoch
    ///
    /// # Returns
    /// `None` if the segment is expired, otherwise the remaining duration.
    fn segment_ttl(&self, now: u32) -> Option<Duration> {
        let expire_at = self.expire_at();
        if now >= expire_at {
            None
        } else {
            Some(Duration::from_secs((expire_at - now) as u64))
        }
    }

    /// Get the TTL for a specific item.
    ///
    /// This method abstracts over the TTL model:
    /// - For segment-level TTL: Returns `segment_ttl(now)`
    /// - For per-item TTL: Reads the item's header to get its specific TTL
    ///
    /// Used when moving items between layers to reconstruct their TTL.
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `now`: Current time as coarse seconds since epoch
    ///
    /// # Returns
    /// `None` if the item is expired or offset is invalid, otherwise remaining TTL.
    fn item_ttl(&self, offset: u32, now: u32) -> Option<Duration>;

    /// Get the TTL bucket ID this segment belongs to, or `None` if not in a bucket.
    fn bucket_id(&self) -> Option<u16>;

    /// Set the TTL bucket ID.
    fn set_bucket_id(&self, bucket_id: u16);

    /// Clear the TTL bucket ID (mark as not in a bucket).
    fn clear_bucket_id(&self);

    // ========== Data Access ==========

    /// Get a raw slice of segment data at the given offset and length.
    ///
    /// # Safety
    /// The caller must ensure:
    /// - `offset + len <= capacity()`
    /// - The segment is in a readable state
    ///
    /// # Returns
    /// `Some(&[u8])` if the range is valid, `None` otherwise.
    fn data_slice(&self, offset: u32, len: usize) -> Option<&[u8]>;

    /// A raw pointer to `len` readable bytes at `offset`, or `None` if that
    /// range leaves the segment.
    ///
    /// This exists for decoding ITEM HEADERS, which [`data_slice`] cannot do
    /// soundly. A header spans the flags byte, and `mark_deleted` writes that
    /// byte through an `AtomicU8` after the item is published. A `&[u8]` over
    /// it would claim `noalias readonly` at the ABI boundary and be
    /// `SharedReadOnly` under Stacked Borrows, neither of which holds for
    /// memory a concurrent writer mutates; reading the flags byte from such a
    /// slice retags a `SharedReadWrite` `&AtomicU8` from a read-only parent,
    /// which Miri rejects outright.
    ///
    /// Key and value bytes stay on [`data_slice`] deliberately: they are
    /// written before the item is published and never mutated afterwards, so
    /// a shared reference over them tells the truth.
    ///
    /// [`data_slice`]: Segment::data_slice
    fn header_ptr(&self, offset: u32, len: usize) -> Option<*const u8>;

    // ========== Item Operations ==========

    /// Append an item to the segment (segment-level TTL).
    ///
    /// Atomically reserves space and writes the item using BasicHeader.
    /// The segment should be in `Live` state.
    ///
    /// # Parameters
    /// - `key`: The item's key
    /// - `value`: The item's value
    /// - `optional`: Optional metadata (e.g., flags, CAS tag)
    ///
    /// # Returns
    /// `Some(offset)` where the item was written, `None` if segment is full.
    fn append_item(&self, key: &[u8], value: &[u8], optional: &[u8]) -> Option<u32>;

    /// Append an item to the segment with per-item TTL.
    ///
    /// Atomically reserves space and writes the item using TtlHeader.
    /// The segment should be in `Live` state.
    ///
    /// # Parameters
    /// - `key`: The item's key
    /// - `value`: The item's value
    /// - `optional`: Optional metadata (e.g., flags, CAS tag)
    /// - `expire_at`: Expiration time as coarse seconds since epoch
    ///
    /// # Returns
    /// `Some(offset)` where the item was written, `None` if segment is full.
    fn append_item_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        expire_at: u32,
    ) -> Option<u32>;

    /// Begin a two-phase append operation (for zero-copy receive).
    ///
    /// This reserves space and writes the header, optional, and key,
    /// returning a mutable pointer to the value area. The caller must:
    /// 1. Write exactly `value_len` bytes to the returned pointer
    /// 2. Call `finalize_append` to complete the operation
    ///
    /// If the caller doesn't complete the operation (e.g., connection closes),
    /// the item will have garbage in the value but the segment remains valid.
    /// Use `mark_deleted_at_offset` to clean up incomplete items.
    ///
    /// # Parameters
    /// - `key`: The item's key
    /// - `value_len`: Size of value to reserve (caller will write this many bytes)
    /// - `optional`: Optional metadata
    /// - `expire_at`: Expiration time as coarse seconds since epoch
    ///
    /// # Returns
    /// `Some((offset, item_size, value_ptr))` where:
    /// - `offset` is the item offset within the segment
    /// - `item_size` is the padded item size (for use with `finalize_append`)
    /// - `value_ptr` points to the reserved value area
    ///
    /// Returns `None` if the segment is full.
    ///
    /// # Safety
    ///
    /// The returned pointer is valid until the segment is cleared. The caller must:
    /// - Write exactly `value_len` bytes to the pointer
    /// - Not hold the pointer beyond the segment's lifetime
    fn begin_append_with_ttl(
        &self,
        key: &[u8],
        value_len: usize,
        optional: &[u8],
        expire_at: u32,
    ) -> Option<(u32, u32, *mut u8)>;

    /// Begin a two-phase append operation for segments without per-item TTL.
    ///
    /// This is similar to `begin_append_with_ttl` but uses `BasicHeader` instead
    /// of `TtlHeader`. Used by TtlLayer where TTL is tracked at segment level.
    ///
    /// # Parameters
    /// - `key`: The item's key
    /// - `value_len`: Size of value to reserve
    /// - `optional`: Optional metadata
    ///
    /// # Returns
    /// `Some((offset, item_size, value_ptr))` - see `begin_append_with_ttl` for details.
    fn begin_append(
        &self,
        key: &[u8],
        value_len: usize,
        optional: &[u8],
    ) -> Option<(u32, u32, *mut u8)>;

    /// Finalize a two-phase append operation.
    ///
    /// Called after writing the value data to complete the append.
    /// Updates live_items and live_bytes statistics.
    ///
    /// # Parameters
    /// - `item_size`: Total padded item size (from begin_append_with_ttl)
    fn finalize_append(&self, item_size: u32);

    /// Mark an item as deleted at a given offset (without key verification).
    ///
    /// Used for cleanup of incomplete two-phase appends.
    fn mark_deleted_at_offset(&self, offset: u32);

    /// Mark an item as deleted.
    ///
    /// Decrements `live_items` and `live_bytes` if successful.
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `key`: Expected key (for verification)
    ///
    /// # Returns
    /// - `Ok(true)`: Item was successfully marked as deleted
    /// - `Ok(false)`: Item was already deleted
    /// - `Err(CacheError)`: Key mismatch or segment in invalid state
    fn mark_deleted(&self, offset: u32, key: &[u8]) -> Result<bool, CacheError>;

    // ========== Maintenance ==========

    /// Get the merge count (times this segment was a merge destination).
    fn merge_count(&self) -> u16;

    /// Increment the merge count (saturates at u16::MAX).
    fn increment_merge_count(&self);

    /// Reset the segment for reuse.
    ///
    /// Clears all data and statistics. Should only be called when
    /// the segment is in `Locked` state with ref_count == 0.
    ///
    /// The implementations are asymmetric, and the difference now carries
    /// meaning: `DiskSegmentMeta::reset` publishes `Free` and advances the
    /// incarnation, ending the segment's lifecycle, while `SliceSegment::reset`
    /// touches only statistics and leaves both state and tag alone. A caller
    /// that needs the RAM equivalent of the disk behaviour wants
    /// `SliceSegment::force_free`.
    fn reset(&self);
}

/// Extension trait for segments that support zero-copy access via guards.
pub trait SegmentGuard: Segment {
    /// The guard type returned by `get_item`.
    type Guard<'a>: ItemGuard<'a>
    where
        Self: 'a;

    /// Get a zero-copy guard for an item.
    ///
    /// The guard holds a reference to the segment and increments the
    /// reference count, preventing the segment from being cleared while
    /// the guard exists.
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `key`: Expected key (for verification)
    ///
    /// # Returns
    /// A guard providing access to key, value, and optional data.
    fn get_item(&self, offset: u32, key: &[u8]) -> Result<Self::Guard<'_>, CacheError>;

    /// Get a zero-copy guard for an item that has already been verified.
    ///
    /// This is an optimized path that skips header parsing and key verification,
    /// using pre-parsed header info from a prior `verify_key_unexpired` call.
    /// The caller must ensure the item was recently verified and is valid.
    ///
    /// # Parameters
    /// - `offset`: Item offset within the segment
    /// - `header_info`: Pre-parsed `(key_len, optional_len, value_len)` from verification
    ///
    /// # Returns
    /// A guard providing access to key, value, and optional data.
    ///
    /// # Safety
    /// The caller must ensure:
    /// - The header_info matches the actual item at offset
    /// - The item was verified (key match, not deleted, not expired) recently
    fn get_item_verified(
        &self,
        offset: u32,
        header_info: (u8, u8, u32),
    ) -> Result<Self::Guard<'_>, CacheError>;
}

/// Extension trait for segments that support copying items.
///
/// Used for tier migration (demotion/promotion) and merge eviction.
pub trait SegmentCopy: Segment {
    /// Copy items matching a predicate to a destination segment.
    ///
    /// Items are copied from this segment to the destination. The predicate
    /// receives the item's frequency and returns `true` if it should be copied.
    ///
    /// # Type Parameters
    /// - `S`: Destination segment type
    /// - `F`: Predicate function `(frequency) -> should_copy`
    /// - `L`: Location update callback `(old_offset, new_offset) -> ()`
    ///
    /// # Parameters
    /// - `dest`: Destination segment
    /// - `predicate`: Returns `true` for items that should be copied
    /// - `on_copy`: Called for each copied item with old and new offsets
    ///
    /// # Returns
    /// Number of items copied, or `None` if destination is full.
    fn copy_into<S, F, L>(&self, dest: &S, predicate: F, on_copy: L) -> Option<u32>
    where
        S: Segment,
        F: Fn(u8) -> bool,
        L: FnMut(u32, u32);
}

/// Result type for `SegmentPrune::prune_collecting`.
///
/// Contains `(items_retained, items_pruned, bytes_retained, bytes_pruned, items_to_demote)`
/// where `items_to_demote` is a Vec of `(key, value, optional, ttl_secs)` tuples.
pub type PruneCollectingResult = (u32, u32, u32, u32, Vec<(Vec<u8>, Vec<u8>, Vec<u8>, u32)>);

/// Extension trait for segments that support pruning low-frequency items.
///
/// Used in merge eviction to remove cold items.
pub trait SegmentPrune: Segment {
    /// Prune items with frequency below threshold.
    ///
    /// Items with frequency at or below the threshold are marked as deleted.
    /// Their data remains until the segment is cleared, but they are removed
    /// from the hashtable (or converted to ghosts).
    ///
    /// # Type Parameters
    /// - `F`: Frequency lookup function `(key) -> Option<frequency>`
    ///
    /// # Parameters
    /// - `threshold`: Maximum frequency to prune (items with freq <= threshold are pruned)
    /// - `get_frequency`: Returns the frequency for a key
    ///
    /// # Returns
    /// `(items_retained, items_pruned, bytes_retained, bytes_pruned)`
    fn prune<F>(&self, threshold: u8, get_frequency: F) -> (u32, u32, u32, u32)
    where
        F: Fn(&[u8]) -> Option<u8>;

    /// Prune with collection of demoted items.
    ///
    /// Similar to `prune`, but instead of just marking items deleted,
    /// collects them for demotion to another tier.
    ///
    /// # Type Parameters
    /// - `F`: Frequency lookup function `(key) -> Option<frequency>`
    ///
    /// # Parameters
    /// - `threshold`: Maximum frequency to prune
    /// - `get_frequency`: Returns the frequency for a key
    ///
    /// # Returns
    /// A `PruneCollectingResult` containing the pruned items and statistics.
    fn prune_collecting<F>(&self, threshold: u8, get_frequency: F) -> PruneCollectingResult
    where
        F: Fn(&[u8]) -> Option<u8>;
}

/// Extension trait for iterating over items in a segment.
///
/// Used during eviction, migration, and debugging.
pub trait SegmentIter: Segment {
    /// Iterate over all items in the segment.
    ///
    /// The callback receives `(offset, key, value, optional, is_deleted)` for each item.
    /// Returns early if the callback returns `false`.
    ///
    /// # Type Parameters
    /// - `F`: Callback function returning `true` to continue, `false` to stop
    ///
    /// # Parameters
    /// - `callback`: Called for each item in the segment
    fn for_each_item<F>(&self, callback: F)
    where
        F: FnMut(u32, &[u8], &[u8], &[u8], bool) -> bool;

    /// Count items by their properties.
    ///
    /// # Returns
    /// `(total_items, deleted_items, total_bytes, deleted_bytes)`
    fn count_items(&self) -> (u32, u32, u32, u32) {
        let mut total_items = 0u32;
        let mut deleted_items = 0u32;
        let mut total_bytes = 0u32;
        let mut deleted_bytes = 0u32;

        self.for_each_item(|_offset, key, value, optional, is_deleted| {
            let item_size = key.len() + value.len() + optional.len();
            total_items += 1;
            total_bytes += item_size as u32;
            if is_deleted {
                deleted_items += 1;
                deleted_bytes += item_size as u32;
            }
            true
        });

        (total_items, deleted_items, total_bytes, deleted_bytes)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::state::INVALID_SEGMENT_ID;

    /// A condemned segment with a live reference, and the queue it would be
    /// pushed to. `try_free_condemned` is the single commit point every
    /// condemned-free path routes through, so these tests cover
    /// `SliceSegment::release_condemned`, `DiskSegmentMeta::release_condemned`,
    /// `BasicItemGuard::drop` and `ValueRef::drop` at once.
    fn condemned(ref_count: u32) -> (AtomicU32, AtomicU64, crossbeam_deque::Injector<u32>) {
        let meta = Metadata {
            next: INVALID_SEGMENT_ID,
            prev: INVALID_SEGMENT_ID,
            state: State::AwaitingRelease,
            incarnation: 7,
        };
        (
            AtomicU32::new(ref_count),
            AtomicU64::new(meta.pack()),
            crossbeam_deque::Injector::new(),
        )
    }

    fn state_of(m: &AtomicU64) -> State {
        Metadata::unpack(m.load(Ordering::SeqCst)).state
    }

    /// The production re-validation, pinned deterministically (#129).
    ///
    /// A caller arrives having seen `prev == 1` from its own `fetch_sub` --
    /// which is why every call site reaches here -- but a reader has pinned
    /// the segment again since, on a still-`Sealed` word, before the evictor
    /// condemned it. Freeing now would return the segment to the pool under a
    /// live reference, and the pool can hand it straight to a writer.
    ///
    /// The shuttle models cover this hazard against a *mirror* of this logic;
    /// this is the one that fails if the check leaves the shipped code.
    #[test]
    fn condemned_segment_is_not_freed_while_referenced() {
        let (rc, m, q) = condemned(1);

        let freed = unsafe { try_free_condemned(&rc, &m, &q, 42, || unreachable!()) };

        assert!(
            !freed,
            "freed a condemned segment that still has a live reference"
        );
        assert_eq!(
            state_of(&m),
            State::AwaitingRelease,
            "the segment must stay condemned so the real last reader can free it"
        );
        assert_eq!(rc.load(Ordering::SeqCst), 1, "the reference is untouched");
        assert!(q.is_empty(), "a referenced segment must not reach the pool");
        assert_eq!(
            Metadata::unpack(m.load(Ordering::SeqCst)).incarnation,
            7,
            "a declined release must not advance the incarnation"
        );
    }

    /// `Injector::steal` returns `Retry` transiently -- "another thread was
    /// mid-operation, ask again", not "the queue is empty" -- and `.success()`
    /// maps that to `None`. A single steal therefore reports an empty queue
    /// spuriously, which miri's scheduling makes reproducible. Same bounded
    /// retry as `MemoryPool::reserve`, for the same reason.
    fn steal_one(q: &crossbeam_deque::Injector<u32>) -> Option<u32> {
        for _ in 0..64 {
            match q.steal() {
                crossbeam_deque::Steal::Success(v) => return Some(v),
                crossbeam_deque::Steal::Retry => continue,
                crossbeam_deque::Steal::Empty => return None,
            }
        }
        None
    }

    /// The other side of the same gate: the genuinely-last reference does
    /// free it, exactly once, ending the incarnation.
    #[test]
    fn condemned_segment_is_freed_when_unreferenced() {
        let (rc, m, q) = condemned(0);
        let mut hook_ran = 0;

        let freed = unsafe {
            try_free_condemned(&rc, &m, &q, 42, || {
                hook_ran += 1;
                // The hook must run BEFORE the push, not after: `IoUringDiskLayer`
                // returns the segment's staging buffer here, and a segment that is
                // already on the free queue can be reserved by another thread and
                // given a fresh buffer, which the hook would then return instead.
                assert!(
                    q.is_empty(),
                    "on_freed ran after the segment was published to the free queue"
                );
            })
        };

        assert!(freed);
        assert_eq!(
            hook_ran, 1,
            "the on_freed hook runs once on the winning CAS"
        );
        assert_eq!(state_of(&m), State::Free);
        assert_eq!(
            Metadata::unpack(m.load(Ordering::SeqCst)).incarnation,
            8,
            "AwaitingRelease -> Free ends a used incarnation"
        );
        assert_eq!(steal_one(&q), Some(42), "pushed to the free queue");

        // Racing callers: the CAS admits exactly one winner, so a second
        // attempt declines and does not double-push.
        let again = unsafe { try_free_condemned(&rc, &m, &q, 42, || unreachable!()) };
        assert!(
            !again,
            "only the caller that performed the CAS returns true"
        );
        assert!(q.is_empty(), "no double-push to the free queue");
    }

    /// A segment that was never condemned is not freed, whatever its count.
    #[test]
    fn a_live_segment_is_never_freed() {
        for state in [State::Live, State::Sealed, State::Draining, State::Locked] {
            let m = AtomicU64::new(Metadata::new(state).pack());
            let rc = AtomicU32::new(0);
            let q = crossbeam_deque::Injector::new();

            let freed = unsafe { try_free_condemned(&rc, &m, &q, 42, || unreachable!()) };

            assert!(!freed, "{state:?} is not condemned but was freed");
            assert_eq!(state_of(&m), state);
            assert!(q.is_empty());
        }
    }

    // Tests will be added when we have a concrete Segment implementation
    // For now, just verify the trait is object-safe where applicable

    #[test]
    fn test_segment_key_verify_is_object_safe() {
        fn _takes_dyn(_: &dyn SegmentKeyVerify) {}
    }

    // Note: Segment trait is not fully object-safe due to associated types
    // in extension traits, but the base Segment trait operations work with
    // concrete types via generics.
}
