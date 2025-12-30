//! Eviction policies and strategies.
//!
//! This module defines eviction helpers for cache segment selection:
//!
//! - [`ItemFate`]: Decision for an item during eviction (ghost/demote/discard)
//! - [`apply_frequency_decay`]: Apply frequency decay during demotion
//! - [`determine_item_fate`]: Decide item fate based on frequency and config
//! - [`calculate_merge_threshold`]: Calculate adaptive threshold for merge eviction
//!
//! # Eviction Strategies
//!
//! The eviction strategy is configured via [`crate::config::EvictionStrategy`]:
//!
//! - **FIFO**: Evict oldest segment (head of chain)
//! - **Random**: Evict a random segment
//! - **CTE**: Evict segment closest to expiration
//! - **Merge**: Combine segments, pruning low-frequency items

mod policy;
mod simple;

pub use policy::ItemFate;
pub use simple::{apply_frequency_decay, calculate_merge_threshold, determine_item_fate};
