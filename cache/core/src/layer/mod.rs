//! Layer abstraction combining pool + organization + config.
//!
//! A layer is a single tier of storage in the cache hierarchy:
//!
//! - [`FifoLayer`]: FIFO-organized layer for admission queues (S3FIFO small queue)
//! - [`TtlLayer`]: TTL bucket-organized layer for main cache storage
//!
//! # Architecture
//!
//! ```text
//! +--------------------------------------------------+
//! |                     Layer                        |
//! |  +------------+  +-------------+  +-----------+  |
//! |  |    Pool    |  | Organization|  |  Config   |  |
//! |  | (segments) |  | (FIFO/TTL)  |  | (policy)  |  |
//! |  +------------+  +-------------+  +-----------+  |
//! +--------------------------------------------------+
//! ```
//!
//! Layers handle:
//! - Segment allocation and release
//! - Item storage and retrieval
//! - Eviction when space is needed
//! - Ghost/demotion decisions based on config

mod fifo_layer;
mod traits;
mod ttl_layer;

pub use fifo_layer::{EvictResult, FifoLayer, FifoLayerBuilder};
pub use traits::Layer;
pub use ttl_layer::{TtlLayer, TtlLayerBuilder};

use crate::segment::Segment;
use crate::state::State;

/// Wait for all readers to release a segment.
///
/// Spins briefly using `spin_loop` hints for low-latency cases, then
/// falls back to `thread::yield_now()` to avoid starving other work
/// on the core.
///
/// **Call this only on a segment already claimed `Locked`** -- via
/// [`claim_and_wait_for_readers`], which is the only caller. `Draining` is not
/// an exclusive claim (it still admits key-verify readers, see
/// [`State::admits_verify_reader`]), so a spin that converges to zero under
/// `Draining` says nothing about the instant after it: a fresh pin can land
/// between the observation and the `Draining -> Locked` CAS, and the caller
/// then rewrites the segment's bytes under it. That was #133 on the blocking
/// path. Under `Locked` no fresh pin is admitted, so the count is monotonically
/// non-increasing and the zero this returns on is final.
///
/// `ref_count_seqcst`, not `ref_count`: the claim CAS above is the store and
/// this is the load of the Dekker pair with each reader's (store `ref_count`,
/// load state). Repeating the load does not rescue an `Acquire` one -- the
/// guarantee needed is that the *observation of zero* cannot coexist with a
/// reader whose re-check saw an admitting state, and only the SC total order
/// gives that. See [`crate::segment::Segment::ref_count_seqcst`] (#129).
pub(crate) fn wait_for_readers<S: Segment>(segment: &S) {
    let mut spins = 0u32;
    while segment.ref_count_seqcst() > 0 {
        if spins < 64 {
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
        spins = spins.saturating_add(1);
    }
}

/// Claim a drained segment for exclusive access: `Draining -> Locked`.
///
/// Returns `true` iff this caller won the claim. `Locked` is the only state
/// that refuses *every* class of fresh reader -- `Draining` deliberately still
/// admits key-verify readers so the demoter can verify keys on a segment it is
/// draining -- so it is the transition past which the segment's bytes may be
/// rewritten.
///
/// # Claim, then count. Never count, then claim (#133)
///
/// Before this existed, both non-blocking eviction paths read `ref_count`
/// first and CASed to `Locked` only if it was zero:
///
/// ```text
/// evictor: R(ref_count)   then W(state = Locked)   then clears bytes
/// reader : W(ref_count)   then R(state)            then reads bytes
/// ```
///
/// Every access there is already `SeqCst`, and the bad outcome is still
/// reachable under a full SC total order -- the reader's `fetch_add` and
/// re-check simply both land in the gap between the evictor's load and its
/// CAS. It is a TOCTOU, not a Dekker pair, so no memory ordering fixes it;
/// #131's `SeqCst` work does not touch it. The failure is not a
/// use-after-free (pool memory stays mapped) but a *wrong verify result*: a
/// reader mid-`verify_key_at_offset` while the evictor clears the segment.
///
/// Inverting the order removes it outright. Once the claim is published no
/// fresh pin is admitted, so a `ref_count` read taken after it can only be
/// invalidated downwards, and "zero" stays true. The `SeqCst` on the CAS (via
/// `state::transition_excludes_readers`) is what orders that read against a
/// reader still mid-pin.
///
/// Callers that lose the claim, or that win it and then find readers, must
/// **not** clear anything: they defer through [`condemn_and_reclaim`].
pub(crate) fn try_claim_for_clear<S: Segment>(segment: &S) -> bool {
    #[cfg(all(test, not(feature = "loom"), not(feature = "shuttle")))]
    crate::segment::interpose::fire(crate::segment::interpose::CLAIM_BEFORE_CAS);

    segment.cas_metadata(State::Draining, State::Locked, None, None)
}

/// Claim a drained segment and then wait out its readers, for the blocking
/// eviction paths.
///
/// Returns `true` iff this caller holds the `Locked` claim on return, with
/// `ref_count` observed at zero. The order is the point: see
/// [`try_claim_for_clear`] for why the claim has to precede the count, and
/// [`wait_for_readers`] for why waiting under `Draining` would not have been
/// exclusive.
pub(crate) fn claim_and_wait_for_readers<S: Segment>(segment: &S) -> bool {
    if !try_claim_for_clear(segment) {
        return false;
    }
    wait_for_readers(segment);
    true
}

/// Condemn a segment whose readers have not all left, and reclaim it if they
/// have in the meantime.
///
/// `from` is the state this caller holds: `Locked` if it won
/// [`try_claim_for_clear`] and then found the count non-zero, `Draining` if it
/// never claimed. Either way the segment lands in `AwaitingRelease`, where no
/// fresh reference is admitted and the last one out owes the
/// `AwaitingRelease -> Free` handoff.
///
/// Returns `true` iff *this* call completed that handoff -- i.e. the segment
/// is already back on the free queue and the caller may report the eviction as
/// fully processed.
///
/// # The race fix, and why it is not optional
///
/// The `ref_count` re-read after the CAS is the condemner half of the *other*
/// Dekker pair: this thread stores `AwaitingRelease` then loads `ref_count`,
/// while the last reader's drop stores `ref_count` then loads the state. If
/// the reader's decrement landed before the CAS, its own drop saw a state that
/// was not yet `AwaitingRelease` and declined; without this re-read the
/// segment strands in `AwaitingRelease` with `ref_count == 0` and nothing
/// sweeps it. `ref_count_seqcst`, because both halves have to sit in the one
/// total order or each side can conclude the other owes the free (#129).
///
/// This was written out four times -- both layers, both non-blocking paths --
/// which is why deleting any one copy left the suite green (#134). One body
/// means one place a test can call directly:
/// `condemn_and_reclaim_discharges_the_handoff_when_the_last_reader_already_left`.
pub(crate) fn condemn_and_reclaim<S: Segment>(segment: &S, from: State) -> bool {
    segment.cas_metadata(from, State::AwaitingRelease, None, None);
    segment.ref_count_seqcst() == 0 && segment.release_condemned()
}
