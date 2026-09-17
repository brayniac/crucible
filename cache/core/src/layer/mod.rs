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

/// Wait for all readers to release a segment.
///
/// Spins briefly using `spin_loop` hints for low-latency cases, then
/// falls back to `thread::yield_now()` to avoid starving other work
/// on the core.
///
/// `ref_count_seqcst`, not `ref_count`. Every caller reaches here after a
/// `Sealed -> Draining` CAS and leaves it to take `Draining -> Locked` and
/// rewrite the segment's bytes, so this spin is the load half of the Dekker
/// pair with each reader's (store `ref_count`, load state). Repeating the load
/// does not rescue an `Acquire` one: the guarantee needed is that the
/// *observation of zero* cannot coexist with a reader whose re-check saw an
/// admitting state, and only the SC total order gives that. See
/// [`crate::segment::Segment::ref_count_seqcst`] (#129).
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
