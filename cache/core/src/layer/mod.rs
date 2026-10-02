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

/// The demoter type for an eviction that demotes nothing.
pub(crate) type NoDemoter = fn(&[u8], &[u8], &[u8], std::time::Duration, crate::location::Location);

/// Claim a drained segment for exclusive access: `Draining -> Locked`.
///
/// Returns `true` iff this caller won the claim. `Locked` is the only state
/// that refuses *every* class of fresh reader -- `Draining` deliberately still
/// admits key-verify readers, so a lookup or insert resolving an entry that
/// still points into a segment being swept can verify its key -- so it is the
/// transition past which the segment's bytes may be rewritten.
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
/// `condemn_and_reclaim_discharges_the_handoff_when_the_last_reader_already_left`,
/// which reddens when the reclaim attempt is dropped.
///
/// The `ref_count_seqcst() == 0` *gate* is a fast path and nothing more, and
/// is stated as such because a mutation matrix will otherwise report it as
/// uncovered. Since #131 the load-bearing copy of that check lives inside
/// `segment::try_free_condemned`, which re-reads `ref_count` (SeqCst) after
/// loading the state and declines if a reader has pinned since. Deleting the
/// gate here therefore changes no outcome and reddens nothing -- it only saves
/// a call on the common "readers still in" path. The check that does the work
/// is pinned by
/// `segment::tests::condemned_segment_is_not_freed_while_referenced`.
pub(crate) fn condemn_and_reclaim<S: Segment>(segment: &S, from: State) -> bool {
    condemn_and_reclaim_with(segment, from, S::release_condemned)
}

/// [`condemn_and_reclaim`] with the reclaim supplied by the caller, for a
/// segment that owns something the segment type itself does not know about.
///
/// The one caller is `IoUringDiskLayer`, whose segments hold a staging buffer
/// borrowed from a layer-owned pool. `DiskSegmentMeta::release_condemned`
/// cannot return it -- the segment has no handle on the pool -- so the buffer
/// return has to ride along with the free.
///
/// # Why a hook and not a line before the condemn
///
/// The obvious spelling is to detach the buffer just before condemning. That
/// is wrong in both directions. If the condemn or the reclaim then declines,
/// the buffer has been pulled out from under a reader still resolving
/// `write_buffer_ptr()` into it. And if the release is instead moved to after
/// the reclaim, it lands *after* the segment is on the free queue, where
/// another thread may already have reserved it and attached a fresh buffer --
/// which the late detach then returns to the pool while its new owner is
/// still writing into it.
///
/// The only safe instant is inside `segment::try_free_condemned`'s `on_freed`
/// hook: after the winning CAS, so it runs exactly once and only for a
/// segment that really was condemned with no references left, and before the
/// push, so no one can have taken the segment yet. `reclaim` is how a caller
/// reaches that hook; `IoUringDiskLayer::free_condemned_returning_buffer` is
/// the body that uses it.
pub(crate) fn condemn_and_reclaim_with<S: Segment, R: FnOnce(&S) -> bool>(
    segment: &S,
    from: State,
    reclaim: R,
) -> bool {
    segment.cas_metadata(from, State::AwaitingRelease, None, None);
    segment.ref_count_seqcst() == 0 && reclaim(segment)
}
