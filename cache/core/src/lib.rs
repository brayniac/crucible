//! cache-core-v2: Redesigned cache-core with layer-based architecture.
//!
//! This crate provides the core building blocks for segment-based caches:
//!
//! - **Location**: Opaque `Location` for hashtable storage, `ItemLocation` for segment interpretation
//! - **Configuration**: `LayerConfig`, `FrequencyDecay`, `MergeConfig`
//! - **Segments**: Fixed-size storage units with item append/read
//! - **Pools**: Segment allocation and lifecycle management
//! - **Organization**: FIFO queues and TTL buckets
//! - **Eviction**: Simple (FIFO/Random/CTE) and adaptive merge
//! - **Layers**: Combining pool + organization + config
//! - **Cache**: TieredCache orchestrating multiple layers
//!
//! # Architecture
//!
//! ```text
//!                     +---------------------------+
//!                     |        Hashtable          |
//!                     | (location + freq + ghost) |
//!                     +---------------------------+
//!                                  |
//!                                  v
//!                          +-------------+
//!                          |   Layer 0   |
//!                          | (FIFO/RAM)  |
//!                          +------+------+
//!                                 | evict
//!                                 v
//!                          +-------------+
//!                          |   Layer 1   |
//!                          | (TTL/RAM)   |
//!                          +------+------+
//!                                 | evict
//!                                 v
//!                          +-------------+
//!                          |   Layer 2   |
//!                          | (TTL/Disk)  |
//!                          +-------------+
//! ```
//!
//! # Example: S3FIFO Configuration
//!
//! ```ignore
//! use cache_core_v2::*;
//!
//! let cache = TieredCacheBuilder::new()
//!     .with_layer(FifoLayer::builder()
//!         .pool(memory_pool_0)
//!         .config(LayerConfig::new()
//!             .with_ghosts(true)
//!             .with_next_layer(1)
//!             .with_demotion_threshold(1))
//!         .build())
//!     .with_layer(TtlLayer::builder()
//!         .pool(memory_pool_1)
//!         .config(LayerConfig::new().with_ghosts(true))
//!         .merge_config(MergeConfig::default())
//!         .build())
//!     .build();
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

// Core types
mod config;
mod error;
mod location;
mod sync;

// Segment-specific location interpretation
mod item_location;

// Phase 2 - Item and Segment types
mod item;
mod segment;
mod state;

// Re-exports
pub use config::{EvictionStrategy, FrequencyDecay, LayerConfig, LayerId, MergeConfig};
pub use error::{CacheError, CacheResult};

// Location re-exports
pub use item_location::{FnVerifier, ItemLocation, MultiPoolVerifier, SinglePoolVerifier};
pub use location::Location;

// Phase 2 re-exports
pub use item::{BasicHeader, BasicItemGuard, ItemGuard, TtlHeader};
pub use segment::{
    Segment, SegmentCopy, SegmentGuard, SegmentIter, SegmentKeyVerify, SegmentPrune,
};
pub use state::{INVALID_SEGMENT_ID, Metadata, State};

// Phase 3 - Hashtable
mod hashtable;
mod hashtable_impl;

// Phase 3 re-exports
pub use hashtable::{Hashtable, KeyVerifier};
pub use hashtable_impl::{Hashbucket, MultiChoiceHashtable};

// Phase 4 - Pools
mod hugepage;
mod memory_pool;
mod pool;
mod slice_segment;

// Phase 4 re-exports
pub use hugepage::{
    AllocatedPageSize, HugepageAllocation, HugepageSize, allocate, allocate_on_node,
};
pub use memory_pool::{MemoryPool, MemoryPoolBuilder};
pub use pool::RamPool;
pub use slice_segment::SliceSegment;

// Phase 5 - Organization
mod organization;

// Phase 5 re-exports
pub use organization::{
    ChainError, FifoChain, MAX_TTL_BUCKETS, TtlBucket, TtlBucketError, TtlBuckets,
};

// Phase 6 - Eviction
mod eviction;

// Phase 6 re-exports
pub use eviction::{
    ItemFate, apply_frequency_decay, calculate_merge_threshold, determine_item_fate,
};

// Phase 7 - Layer
mod layer;

// Phase 7 re-exports
pub use layer::{FifoLayer, FifoLayerBuilder, Layer, TtlLayer, TtlLayerBuilder};

// Phase 8 - Cache
mod cache;

// Phase 8 re-exports
pub use cache::{CacheLayer, TieredCache, TieredCacheBuilder};

// Phase 9 - Metrics
mod metrics;

// Phase 9 re-exports
pub use metrics::{
    AtomicCounters, CacheMetrics, CounterSnapshot, LayerMetrics, MetricsExport, PoolMetrics,
};

// Phase 10 - Disk storage tier
pub mod disk;

// Phase 10 re-exports
pub use disk::{DiskConfig, DiskLayer, DiskLayerBuilder, FilePool, FilePoolBuilder, SyncMode};

// Cache trait for server compatibility
mod cache_trait;
pub use cache_trait::{Cache, DEFAULT_TTL, OwnedGuard, ValueRef};

// Redis-like data structure traits
mod hash_cache;
mod list_cache;
mod set_cache;
pub use hash_cache::HashCache;
pub use list_cache::ListCache;
pub use set_cache::SetCache;
mod arc_cache;
pub use arc_cache::{ArcCache, Entry};

// Reservation for zero-copy receive
mod reservation;
pub use reservation::{SegmentReservation, SetReservation};

// CAS (Compare-And-Swap) token
mod cas;
pub use cas::CasToken;

// Numeric utilities for efficient storage of numeric values
pub mod numeric;
