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

    /// Optional policy cap on retention, as a fraction of the chain's live
    /// bytes (0.0 - 1.0).
    ///
    /// This is *not* what keeps a pass inside its spare. A pass reserves one
    /// spare and the retained set is bounded by that spare's free space
    /// (#154), so capacity is enforced structurally whatever this is set to.
    /// The two budgets compose as a minimum, so this can only ever prune
    /// further than capacity requires -- it is a "throw away live data the
    /// cache had room for" knob, and the reason to reach for it is a
    /// deliberate one, such as keeping a chain's occupancy low.
    ///
    /// At `1.0` the cap asks for nothing: the pass prunes exactly what the
    /// spare cannot hold, so a fragmented chain whose live set fits is
    /// compacted with every live item intact and only the dead bytes
    /// reclaimed.
    pub target_ratio: f64,

    /// Frequency an item must exceed to survive, before any adaptive rise.
    ///
    /// Not zero-by-default-and-forget: a fresh insert is packed with
    /// frequency 1 and nothing ever lowers it, so a threshold of 0 retains
    /// every still-indexed item and prunes only dead bytes. Merge gets away
    /// with starting at 0 because its threshold climbs past the baseline on
    /// its own; a policy that pins the threshold must start above the
    /// baseline or it prunes nothing live.
    pub initial_threshold: u8,
}

impl Default for MergeConfig {
    fn default() -> Self {
        Self {
            min_segments: 4,
            // Prune half the live set, even where capacity would allow
            // keeping it. This is a latency choice, not a correctness one,
            // and it is held here deliberately.
            //
            // Before #154 the ratio did double duty: it was the only thing
            // stopping a pass from over-subscribing its spare, and 0.5 was
            // the margin that bought. #154 made the spare's free space the
            // budget, bounding the pass structurally, and left the ratio as
            // a cap on top -- `budget = min(spare_bytes, live * ratio)`.
            //
            // So a ratio below 1.0 no longer protects anything; it only
            // discards live items there was room to keep. Raising it to 1.0
            // turns the pass into pure compaction wherever the live set
            // fits, which is what #155 asks for -- and that was measured on
            // a quiet ARM board as **-7.3% miss ratio for +80.7%
            // eviction cost**, worst-case stall 4.9ms -> 8.6ms. Retaining
            // more means copying more; there is no free lunch in the ratio
            // alone.
            //
            // While #152 (merge stalls on the write path) is open that is
            // the wrong default, so it stays at 0.5 until #155's second half
            // lands: choosing the chain length from measured occupancy, so
            // a pass frees more segments per byte copied instead of simply
            // copying more. The knob still reaches 1.0 explicitly for
            // deployments that want the hit ratio.
            target_ratio: 0.5,
            initial_threshold: 0,
        }
    }
}

impl MergeConfig {
    /// Parameters that make merge behave as CLOCK second chance.
    ///
    /// `min_segments: 1` reclaims one segment per pass rather than a chain.
    /// `target_ratio: 1.0` pins the prune threshold at its floor, by two
    /// separate routes that both have to hold:
    ///
    /// - The retention budget is `live_bytes * target_ratio`, which at 1.0 is
    ///   the whole live set, so the policy cap prunes nothing.
    /// - A single candidate's live bytes cannot exceed its own capacity, and
    ///   the spare has the same capacity, so the capacity bound prunes
    ///   nothing either.
    ///
    /// What is left is `initial_threshold`, and CLOCK's second chance is
    /// exactly that fixed rule. Both properties are load-bearing and subtle,
    /// so they are pinned by
    /// `clock_params_stop_the_threshold_rising_above_its_baseline` here and by
    /// `clock_parameters_prune_exactly_the_items_untouched_since_admission`
    /// in `layer::ttl_layer`, rather than left to a reader to re-derive.
    pub const CLOCK: Self = Self {
        min_segments: 1,
        target_ratio: 1.0,
        initial_threshold: 1,
    };

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
#[derive(Debug, Clone, Copy, PartialEq, Default)]
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

    /// CLOCK second chance: reclaim one segment at a time, copying every item
    /// whose frequency is non-zero into a fresh segment and dropping the rest.
    ///
    /// Deliberately not a separate implementation. It is [`Merge`] with the
    /// chain length pinned to one segment and the prune threshold pinned to
    /// zero, so the two share every line of claim protocol, item scan, copy
    /// and relink. A measured merge-vs-CLOCK difference is then the algorithm
    /// rather than an artifact of two independently written code paths --
    /// which is the whole point of the comparison in
    /// `docs/superpowers/specs/2026-09-18-s3fifo-main-pool-experiment-design.md`.
    ///
    /// [`Merge`]: EvictionStrategy::Merge
    Clock,
}

impl EvictionStrategy {
    /// The merge parameters this strategy runs with, or `None` if it is not
    /// merge-shaped.
    pub fn merge_params(&self) -> Option<MergeConfig> {
        match self {
            EvictionStrategy::Merge(config) => Some(*config),
            EvictionStrategy::Clock => Some(MergeConfig::CLOCK),
            EvictionStrategy::ExpireFirst
            | EvictionStrategy::Fifo
            | EvictionStrategy::Random
            | EvictionStrategy::Cte => None,
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn clock_prunes_at_the_insert_baseline_rather_than_at_zero() {
        // A fresh insert carries frequency 1 and nothing lowers it, so
        // `freq > 0` retains every live item and reclaims only dead bytes.
        // CLOCK's intent -- a second chance for anything touched since
        // admission -- is `freq > 1` in this convention.
        assert_eq!(MergeConfig::CLOCK.initial_threshold, 1);
    }

    #[test]
    fn merge_still_starts_its_adaptive_threshold_at_zero() {
        assert_eq!(MergeConfig::default().initial_threshold, 0);
    }

    #[test]
    fn clock_params_stop_the_threshold_rising_above_its_baseline() {
        // `try_merge_eviction` derives its retention budget as
        // `live_bytes * target_ratio` and prunes only what will not fit in
        // it. At 1.0 the budget must come out as the whole live set --
        // exactly, with no floating-point shortfall, or the cap would clip
        // the coldest class and CLOCK would silently start pruning
        // adaptively like merge instead of holding a fixed second-chance
        // rule.
        let clock = MergeConfig::CLOCK;
        for live_bytes in [0u64, 1, 7, 1023, 65536, 1 << 20, (1 << 32) + 1] {
            let budget = (live_bytes as f64 * clock.target_ratio) as u64;
            assert_eq!(
                budget, live_bytes,
                "a retention budget of {budget} for {live_bytes} live bytes \
                 would prune under CLOCK parameters"
            );
        }
    }

    #[test]
    fn clock_reclaims_one_segment_at_a_time_rather_than_a_chain() {
        assert_eq!(MergeConfig::CLOCK.min_segments, 1);
    }

    #[test]
    fn clock_runs_the_merge_machinery_with_clock_parameters() {
        assert_eq!(
            EvictionStrategy::Clock.merge_params(),
            Some(MergeConfig::CLOCK)
        );
    }

    #[test]
    fn merge_reports_its_own_configured_parameters() {
        let cfg = MergeConfig::new().with_min_segments(7);
        assert_eq!(EvictionStrategy::Merge(cfg).merge_params(), Some(cfg));
    }

    #[test]
    fn a_strategy_that_is_not_merge_shaped_has_no_merge_parameters() {
        assert_eq!(EvictionStrategy::Fifo.merge_params(), None);
        assert_eq!(EvictionStrategy::Random.merge_params(), None);
        assert_eq!(EvictionStrategy::Cte.merge_params(), None);
    }

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
        // 1.0, not 0.5: capacity is enforced by the spare's byte budget
        // (#154), so the ratio is a pure "prune more than capacity forces"
        // Held at 0.5 while the occupancy-adaptive chain length (#155's
        // second half) is built. Raising it to 1.0 was measured as
        // -7.3% miss ratio for +80.7% eviction cost and a worst-case stall
        // of 8.6ms against 4.9ms, which is the wrong trade while #152 is
        // open. The knob still reaches that behaviour explicitly.
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
