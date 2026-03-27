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
pub(crate) fn wait_for_readers<S: Segment>(segment: &S) {
    let mut spins = 0u32;
    while segment.ref_count() > 0 {
        if spins < 64 {
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
        spins = spins.saturating_add(1);
    }
}
