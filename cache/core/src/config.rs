//! Configuration types for layers and eviction.

/// Layer identifier for referencing other layers.
pub type LayerId = u8;

/// Decay strategy for frequency during eviction.
///
/// When items are retained or demoted during eviction, their frequency is decayed
/// so they must prove continued hotness. Ghosts do not have their frequency decayed
/// (they preserve it for second-chance admission).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrequencyDecay {
    /// Subtract a fixed amount (floor at 0).
    ///
    /// Example: `Linear { amount: 1 }` decrements frequency by 1.
    Linear {
        /// Amount to subtract from frequency.
        amount: u8,
    },

    /// Right shift by n bits (divide by 2^n).
    ///
    /// Example: `Exponential { shift: 1 }` halves the frequency.
    Exponential {
        /// Number of bits to shift right.
        shift: u8,
    },

    /// Subtract proportional amount: `max(min_decay, freq / divisor)`.
    ///
    /// Example: `Proportional { divisor: 4, min_decay: 1 }` subtracts at least 1,
    /// or 25% of the frequency, whichever is larger.
    Proportional {
        /// Divisor for proportional decay.
        divisor: u8,
        /// Minimum decay amount.
        min_decay: u8,
    },
}

impl FrequencyDecay {
    /// Apply the decay strategy to a frequency value.
    #[inline]
    pub fn apply(&self, freq: u8) -> u8 {
        match self {
            Self::Linear { amount } => freq.saturating_sub(*amount),
            Self::Exponential { shift } => freq >> shift,
            Self::Proportional { divisor, min_decay } => {
                let proportional = freq / divisor;
                let decay = proportional.max(*min_decay);
                freq.saturating_sub(decay)
            }
        }
    }
}

impl Default for FrequencyDecay {
    fn default() -> Self {
        Self::Linear { amount: 1 }
    }
}

/// Layer configuration - determines behavior during eviction.
#[derive(Debug, Clone, Default)]
pub struct LayerConfig {
    /// If true, evicted (cold) items become ghosts instead of being discarded.
    /// Ghosts preserve frequency for second-chance admission.
    pub create_ghosts: bool,

    /// Reference to next layer for demotion (if any).
    /// When an item is demoted, it is copied to this layer.
    pub next_layer: Option<LayerId>,

    /// How to decay frequency for retained/demoted items.
    /// Default: `Linear { amount: 1 }`
    pub frequency_decay: FrequencyDecay,

    /// Frequency threshold for demotion in simple eviction (FIFO/Random/CTE).
    /// Items with `freq > threshold` are demoted; items `<= threshold` are ghosted/discarded.
    /// Not used for adaptive merge eviction (which computes threshold dynamically).
    pub demotion_threshold: u8,

    /// Frequency threshold for promotion (disk layers only).
    /// On read, if `freq > threshold`, item is promoted to memory layer.
    /// `None` for memory layers (no promotion needed).
    pub promotion_threshold: Option<u8>,

    /// Eviction strategy for this layer.
    /// Determines how segments are selected for eviction.
    /// Default: `Random`
    pub eviction_strategy: EvictionStrategy,
}

impl LayerConfig {
    /// Create a new layer config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable ghost creation for evicted cold items.
    pub fn with_ghosts(mut self, enabled: bool) -> Self {
        self.create_ghosts = enabled;
        self
    }

    /// Set the next layer for demotion.
    pub fn with_next_layer(mut self, layer_id: LayerId) -> Self {
        self.next_layer = Some(layer_id);
        self
    }

    /// Set the frequency decay strategy.
    pub fn with_frequency_decay(mut self, decay: FrequencyDecay) -> Self {
        self.frequency_decay = decay;
        self
    }

    /// Set the demotion threshold for simple eviction.
    pub fn with_demotion_threshold(mut self, threshold: u8) -> Self {
        self.demotion_threshold = threshold;
        self
    }

    /// Set the promotion threshold (for disk layers).
    pub fn with_promotion_threshold(mut self, threshold: u8) -> Self {
        self.promotion_threshold = Some(threshold);
        self
    }

    /// Set the eviction strategy for this layer.
    pub fn with_eviction_strategy(mut self, strategy: EvictionStrategy) -> Self {
        self.eviction_strategy = strategy;
        self
    }
}

/// Configuration for adaptive merge eviction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MergeConfig {
    /// Minimum segments needed to trigger merge.
    pub min_segments: usize,

    /// Target retention ratio (0.0 - 1.0, typically 0.5).
    /// Lower = more aggressive pruning, higher = more retention.
    pub target_ratio: f64,
}

impl Default for MergeConfig {
    fn default() -> Self {
        Self {
            min_segments: 4,
            target_ratio: 0.5,
        }
    }
}

impl MergeConfig {
    /// Create a new merge config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the minimum segments required for merge.
    pub fn with_min_segments(mut self, count: usize) -> Self {
        self.min_segments = count;
        self
    }

    /// Set the target retention ratio.
    pub fn with_target_ratio(mut self, ratio: f64) -> Self {
        self.target_ratio = ratio.clamp(0.0, 1.0);
        self
    }
}

/// Eviction strategy for a layer.
#[derive(Debug, Clone, Default)]
pub enum EvictionStrategy {
    /// Evict expired segments first (no item-level decisions needed).
    ExpireFirst,

    /// Merge N oldest segments using adaptive threshold algorithm.
    Merge(MergeConfig),

    /// Simple FIFO - evict oldest segment, apply threshold to items.
    Fifo,

    /// Random segment selection, apply threshold to items.
    #[default]
    Random,

    /// CTE - Closest To Expiration segment, apply threshold to items.
    Cte,
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_frequency_decay_linear() {
        let decay = FrequencyDecay::Linear { amount: 1 };
        assert_eq!(decay.apply(10), 9);
        assert_eq!(decay.apply(1), 0);
        assert_eq!(decay.apply(0), 0); // No underflow

        let decay = FrequencyDecay::Linear { amount: 5 };
        assert_eq!(decay.apply(10), 5);
        assert_eq!(decay.apply(3), 0); // Saturates at 0
    }

    #[test]
    fn test_frequency_decay_exponential() {
        let decay = FrequencyDecay::Exponential { shift: 1 };
        assert_eq!(decay.apply(10), 5); // 10 >> 1 = 5
        assert_eq!(decay.apply(1), 0); // 1 >> 1 = 0
        assert_eq!(decay.apply(255), 127); // 255 >> 1 = 127

        let decay = FrequencyDecay::Exponential { shift: 2 };
        assert_eq!(decay.apply(16), 4); // 16 >> 2 = 4
    }

    #[test]
    fn test_frequency_decay_proportional() {
        let decay = FrequencyDecay::Proportional {
            divisor: 4,
            min_decay: 1,
        };
        // For freq=20: max(1, 20/4) = max(1, 5) = 5, so 20-5 = 15
        assert_eq!(decay.apply(20), 15);

        // For freq=2: max(1, 2/4) = max(1, 0) = 1, so 2-1 = 1
        assert_eq!(decay.apply(2), 1);

        // For freq=0: max(1, 0/4) = max(1, 0) = 1, so 0-1 = 0 (saturates)
        assert_eq!(decay.apply(0), 0);
    }

    #[test]
    fn test_layer_config_builder() {
        let config = LayerConfig::new()
            .with_ghosts(true)
            .with_next_layer(1)
            .with_demotion_threshold(2)
            .with_frequency_decay(FrequencyDecay::Exponential { shift: 1 });

        assert!(config.create_ghosts);
        assert_eq!(config.next_layer, Some(1));
        assert_eq!(config.demotion_threshold, 2);
        assert_eq!(
            config.frequency_decay,
            FrequencyDecay::Exponential { shift: 1 }
        );
    }

    #[test]
    fn test_layer_config_promotion() {
        let config = LayerConfig::new().with_promotion_threshold(3);
        assert_eq!(config.promotion_threshold, Some(3));
    }

    #[test]
    fn test_merge_config_defaults() {
        let config = MergeConfig::default();
        assert_eq!(config.min_segments, 4);
        assert!((config.target_ratio - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_merge_config_builder() {
        let config = MergeConfig::new()
            .with_min_segments(8)
            .with_target_ratio(0.6);

        assert_eq!(config.min_segments, 8);
        assert!((config.target_ratio - 0.6).abs() < f64::EPSILON);
    }

    #[test]
    fn test_merge_config_clamps_ratio() {
        let config = MergeConfig::new().with_target_ratio(1.5);
        assert!((config.target_ratio - 1.0).abs() < f64::EPSILON);

        let config = MergeConfig::new().with_target_ratio(-0.5);
        assert!(config.target_ratio.abs() < f64::EPSILON);
    }
}
