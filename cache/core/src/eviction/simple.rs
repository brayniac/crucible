//! Simple eviction helpers.
//!
//! This module provides helper functions for eviction operations:
//!
//! - [`apply_frequency_decay`]: Apply frequency decay during demotion
//! - [`determine_item_fate`]: Decide whether to ghost/demote/discard an item

use crate::config::{FrequencyDecay, LayerConfig};
use crate::eviction::ItemFate;

/// Apply frequency decay to a frequency value.
///
/// Called when demoting an item to a lower layer. The decay reduces
/// the item's apparent hotness, making it more likely to be evicted
/// in the destination layer if it doesn't receive new accesses.
///
/// This is a convenience wrapper around [`FrequencyDecay::apply`].
///
/// # Arguments
/// * `frequency` - Current frequency value (0-255)
/// * `decay` - Decay strategy to apply
///
/// # Returns
/// The decayed frequency value (0-255)
#[inline]
pub fn apply_frequency_decay(frequency: u8, decay: FrequencyDecay) -> u8 {
    decay.apply(frequency)
}

/// Determine the fate of an item during eviction.
///
/// Based on the item's frequency and the layer configuration, decides
/// whether the item should become a ghost, be demoted to the next layer,
/// or be discarded entirely.
///
/// # Arguments
/// * `frequency` - Item's current frequency (0-255)
/// * `config` - Layer configuration with thresholds
///
/// # Returns
/// The [`ItemFate`] for this item
///
/// # Decision Logic
///
/// 1. If frequency >= demotion_threshold AND next_layer exists: **Demote**
/// 2. If create_ghosts is true: **Ghost**
/// 3. Otherwise: **Discard**
pub fn determine_item_fate(frequency: u8, config: &LayerConfig) -> ItemFate {
    // Check if item qualifies for demotion
    if frequency >= config.demotion_threshold && config.next_layer.is_some() {
        return ItemFate::Demote;
    }

    // Not demoting - decide between ghost and discard
    if config.create_ghosts {
        ItemFate::Ghost
    } else {
        ItemFate::Discard
    }
}

/// Calculate the adaptive frequency threshold for merge eviction.
///
/// Merge eviction uses a two-pass algorithm:
/// 1. Build a histogram of item frequencies
/// 2. Calculate threshold that keeps `target_ratio`% of items
///
/// This function implements step 2: given frequency counts, find
/// the threshold that retains approximately `target_ratio` percent
/// of items.
///
/// # Arguments
/// * `freq_histogram` - Array of 256 counts, one per frequency value
/// * `total_items` - Total number of items
/// * `target_ratio` - Target percentage of items to keep (0-100)
///
/// # Returns
/// The frequency threshold (items >= threshold are kept)
pub fn calculate_merge_threshold(
    freq_histogram: &[u32; 256],
    total_items: u32,
    target_ratio: u8,
) -> u8 {
    if total_items == 0 {
        return 0;
    }

    let target_ratio = target_ratio.min(100) as u32;
    let target_keep = (total_items * target_ratio) / 100;

    // Start from highest frequency, accumulate until we hit target
    let mut accumulated = 0u32;
    for freq in (0..=255u8).rev() {
        accumulated += freq_histogram[freq as usize];
        if accumulated >= target_keep {
            return freq;
        }
    }

    0 // Keep everything if target not reached
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::config::LayerConfig;

    #[test]
    fn test_frequency_decay_linear() {
        let decay = FrequencyDecay::Linear { amount: 1 };
        assert_eq!(apply_frequency_decay(10, decay), 9);
        assert_eq!(apply_frequency_decay(1, decay), 0);
        assert_eq!(apply_frequency_decay(0, decay), 0);
        assert_eq!(apply_frequency_decay(255, decay), 254);
    }

    #[test]
    fn test_frequency_decay_exponential() {
        let decay = FrequencyDecay::Exponential { shift: 1 };
        assert_eq!(apply_frequency_decay(10, decay), 5);
        assert_eq!(apply_frequency_decay(1, decay), 0);
        assert_eq!(apply_frequency_decay(0, decay), 0);
        assert_eq!(apply_frequency_decay(255, decay), 127);
    }

    #[test]
    fn test_frequency_decay_proportional() {
        // Proportional: subtract max(min_decay, freq/divisor)
        let decay = FrequencyDecay::Proportional {
            divisor: 4,
            min_decay: 1,
        };

        // freq=100: max(1, 100/4) = max(1, 25) = 25, so 100-25 = 75
        assert_eq!(apply_frequency_decay(100, decay), 75);

        // freq=2: max(1, 2/4) = max(1, 0) = 1, so 2-1 = 1
        assert_eq!(apply_frequency_decay(2, decay), 1);

        // freq=0: max(1, 0/4) = max(1, 0) = 1, but 0-1 saturates to 0
        assert_eq!(apply_frequency_decay(0, decay), 0);
    }

    #[test]
    fn test_determine_item_fate_s3fifo_small_queue() {
        // S3FIFO small queue: ghosts=true, threshold=1, next_layer=1
        let config = LayerConfig::new()
            .with_ghosts(true)
            .with_demotion_threshold(1)
            .with_next_layer(1);

        // Hot items (freq >= threshold) get demoted
        assert_eq!(determine_item_fate(2, &config), ItemFate::Demote);
        assert_eq!(determine_item_fate(10, &config), ItemFate::Demote);
        assert_eq!(determine_item_fate(1, &config), ItemFate::Demote);

        // Cold items (freq < threshold) become ghosts
        assert_eq!(determine_item_fate(0, &config), ItemFate::Ghost);
    }

    #[test]
    fn test_determine_item_fate_main_cache() {
        // Main cache: ghosts=true, no next layer
        let config = LayerConfig::new().with_ghosts(true);

        // No next layer, so everything becomes ghosts
        assert_eq!(determine_item_fate(0, &config), ItemFate::Ghost);
        assert_eq!(determine_item_fate(10, &config), ItemFate::Ghost);
        assert_eq!(determine_item_fate(255, &config), ItemFate::Ghost);
    }

    #[test]
    fn test_determine_item_fate_no_ghosts() {
        // No ghosts, no next layer
        let config = LayerConfig::new().with_ghosts(false);

        // Everything gets discarded
        assert_eq!(determine_item_fate(0, &config), ItemFate::Discard);
        assert_eq!(determine_item_fate(255, &config), ItemFate::Discard);
    }

    #[test]
    fn test_calculate_merge_threshold_empty() {
        let histogram = [0u32; 256];
        assert_eq!(calculate_merge_threshold(&histogram, 0, 50), 0);
    }

    #[test]
    fn test_calculate_merge_threshold_uniform() {
        // 100 items at each frequency level 0-9
        let mut histogram = [0u32; 256];
        for item in histogram.iter_mut().take(10) {
            *item = 100;
        }
        let total = 1000;

        // Keep 50% = 500 items, should include freq 5-9
        let threshold = calculate_merge_threshold(&histogram, total, 50);
        assert!(threshold >= 5);
    }

    #[test]
    fn test_calculate_merge_threshold_skewed() {
        // Most items at high frequency
        let mut histogram = [0u32; 256];
        histogram[255] = 900;
        histogram[0] = 100;
        let total = 1000;

        // Keep 50% = 500 items, threshold should be high
        let threshold = calculate_merge_threshold(&histogram, total, 50);
        assert_eq!(threshold, 255); // Only high-freq items needed
    }
}
