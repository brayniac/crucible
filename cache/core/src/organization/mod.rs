//! Segment organization strategies.
//!
//! This module provides different ways to organize segments within a layer:
//!
//! - [`FifoChain`]: Simple FIFO ordering for small queue (S3FIFO Layer 0)
//! - [`TtlBuckets`]: TTL-based organization with 1024 buckets (Layer 1+)
//!
//! # Organization vs Pool
//!
//! - **Pool**: Manages segment memory and allocation (reserve/release)
//! - **Organization**: Manages segment ordering within a layer (chain/bucket)
//!
//! The layer combines pool + organization + config to provide the full
//! segment lifecycle management.

mod fifo;
mod ttl_buckets;

pub use fifo::{ChainError, FifoChain};
pub use ttl_buckets::{MAX_TTL_BUCKETS, TtlBucket, TtlBucketError, TtlBuckets};
