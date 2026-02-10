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
