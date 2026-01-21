//! Disk-backed storage tier for cache hierarchy.
//!
//! This module provides file-backed storage for extending cache capacity
//! beyond available RAM:
//!
//! - [`FilePool`]: File-backed segment pool using mmap
//! - [`FileSegment`]: Disk segment wrapper with dirty tracking
//! - [`DiskLayer`]: Layer implementation for disk tier
//! - [`DiskConfig`]: Configuration for disk storage
//!
//! # Architecture
//!
//! ```text
//! +--------------------------------------------------+
//! |                  DiskLayer                       |
//! |  +------------+  +-------------+  +-----------+  |
//! |  |  FilePool  |  | TtlBuckets  |  |  Config   |  |
//! |  | (mmap file)|  | (organize)  |  | (policy)  |  |
//! |  +------------+  +-------------+  +-----------+  |
//! +--------------------------------------------------+
//!                         |
//!                         v
//!              +--------------------+
//!              |   Disk File        |
//!              | +----------------+ |
//!              | | FilePoolHeader | |
//!              | +----------------+ |
//!              | | Segment 0      | |
//!              | | Segment 1      | |
//!              | | ...            | |
//!              | +----------------+ |
//!              +--------------------+
//! ```
//!
//! # Recovery
//!
//! On startup, the disk tier can recover items from a previous session:
//! 1. Validate file header (magic, version, configuration)
//! 2. Scan segments to find valid items
//! 3. Rebuild hashtable entries for recovered items
//!
//! # Example
//!
//! ```ignore
//! use cache_core::disk::{DiskConfig, DiskLayer, FilePoolBuilder};
//!
//! let disk_layer = DiskLayer::builder()
//!     .layer_id(2)
//!     .pool_id(2)
//!     .path("/var/cache/crucible")
//!     .size(100 * 1024 * 1024 * 1024) // 100GB
//!     .config(LayerConfig::new()
//!         .with_promotion_threshold(2))
//!     .build()?;
//! ```

mod config;
mod disk_layer;
mod file_pool;
mod file_segment;
mod recovery;

pub use config::{DiskConfig, SyncMode};
pub use disk_layer::{DiskLayer, DiskLayerBuilder};
pub use file_pool::{FilePool, FilePoolBuilder, FilePoolHeader};
pub use file_segment::FileSegment;
pub use recovery::{RecoveryStats, WarmStats};
