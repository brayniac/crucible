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

/// The eviction PRNG seed a layer uses unless the caller picks one.
///
/// Fixed, so that two runs of the same workload evict the same segments --
/// the reproducibility every measurement in this project rests on. It is
/// *only* a default: a single seed makes a randomised policy one arbitrary
/// sample of itself, and the way to tell a real policy difference from one
/// sequence's luck is to re-run across seeds. See
/// [`LayerConfig::with_eviction_seed`].
///
/// The value is the golden-ratio word, the constant splitmix64 is usually
/// stepped with; nothing depends on which constant it is.
pub const DEFAULT_EVICTION_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// Layer configuration - determines behavior during eviction.
#[derive(Debug, Clone)]
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
    /// Default: [`EvictionStrategy::RandomFifo`]
    pub eviction_strategy: EvictionStrategy,

    /// Seed for the layer's eviction randomness.
    ///
    /// Default: [`DEFAULT_EVICTION_SEED`]. See
    /// [`with_eviction_seed`](LayerConfig::with_eviction_seed).
    pub eviction_seed: u64,
}

impl Default for LayerConfig {
    fn default() -> Self {
        Self {
            create_ghosts: false,
            next_layer: None,
            frequency_decay: FrequencyDecay::default(),
            demotion_threshold: 0,
            promotion_threshold: None,
            eviction_strategy: EvictionStrategy::default(),
            eviction_seed: DEFAULT_EVICTION_SEED,
        }
    }
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

    /// Seed the randomness this layer's eviction draws on.
    ///
    /// Which bucket [`RandomFifo`] picks, and which segment [`Random`] picks,
    /// come from a PRNG seeded with this and stepped by the layer's own
    /// evictions -- never from a clock, so the same workload replays to the
    /// same victims on any machine.
    ///
    /// It is exposed for the same reason
    /// [`SegCacheBuilder::hashtable_seed`] is: pinned, one run repeats;
    /// varied across runs, the spread over seeds is the noise floor a claimed
    /// policy difference has to clear. Ten seeds agreeing is a result, one
    /// seed is a sample.
    ///
    /// [`RandomFifo`]: EvictionStrategy::RandomFifo
    /// [`Random`]: EvictionStrategy::Random
    /// [`SegCacheBuilder::hashtable_seed`]: https://docs.rs/segcache
    pub fn with_eviction_seed(mut self, seed: u64) -> Self {
        self.eviction_seed = seed;
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

    /// How strongly an item's size counts against it when ranking for
    /// retention: items are ranked by `frequency * (mean_size / size)^e`.
    ///
    /// This is the cost exponent of greedy dual size frequency, which ranks
    /// by `frequency * cost / size`. The two ends are not a tuning range but
    /// two different assumptions about what a miss costs:
    ///
    /// - **`1.0`** -- cost is per request. Equivalent to GDSF with unit
    ///   cost, which is what Segcache specifies (NSDI '21 3.6.3: "Segcache
    ///   uses the frequency-over-size ratio to rank objects"). Keeps small
    ///   items, and minimises the *request* miss ratio.
    /// - **`0.0`** -- cost is proportional to size, so it cancels and items
    ///   rank by raw frequency. Keeps large hot items, and minimises
    ///   something closer to the *byte* miss ratio. Right when refilling a
    ///   miss costs in bandwidth rather than per round trip.
    ///
    /// Read both miss ratios together when changing this: raising it
    /// improves one at the other's expense, so a run reporting only the
    /// request ratio cannot show the trade.
    ///
    /// Defaults to `1.0`. Crucible ranked by raw frequency until this
    /// existed, which was a deviation from the paper rather than a choice;
    /// `0.0` restores that behaviour exactly.
    pub cost_exponent: f64,
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
            cost_exponent: 1.0,
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
        // CLOCK's second chance is "was this touched since admission", a
        // question about the frequency alone. Weighting by size would make
        // a large untouched item rank below a small untouched one, and
        // there is no ordering between them to express.
        cost_exponent: 0.0,
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

    /// Set the size-cost exponent used when ranking items for retention.
    ///
    /// Clamped to `0.0 ..= 1.0`: outside that range the ranking either
    /// inverts (a negative exponent prefers large cold items over small hot
    /// ones) or amplifies size beyond what any cost model motivates.
    pub fn with_cost_exponent(mut self, exponent: f64) -> Self {
        self.cost_exponent = if exponent.is_nan() {
            0.0
        } else {
            exponent.clamp(0.0, 1.0)
        };
        self
    }
}

/// Eviction strategy for a layer.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum EvictionStrategy {
    /// Merge N oldest segments using adaptive threshold algorithm.
    Merge(MergeConfig),

    /// Evict the oldest segment in the layer, across every TTL bucket.
    ///
    /// Age is the order in which segments entered service -- see
    /// [`SliceSegment::create_seq`]. A merge destination enters service when
    /// it is reserved, so a merged segment counts as new, which is cache-rs's
    /// "the later of its creation and last-merge timestamps".
    ///
    /// Because segments are append-only this behaves like LRU at segment
    /// granularity. It is *not* [`RandomFifo`]: that one picks a bucket first
    /// and takes its head, so it evicts the oldest segment of a randomly
    /// chosen TTL range rather than the oldest segment there is.
    ///
    /// [`RandomFifo`]: EvictionStrategy::RandomFifo
    /// [`SliceSegment::create_seq`]: crate::slice_segment::SliceSegment::create_seq
    Fifo,

    /// Evict an evictable segment chosen uniformly at random.
    ///
    /// Simple and fast, but blind to item value -- and, unlike every other
    /// strategy here, it can take a segment from the middle of a bucket
    /// chain, not just a chain head.
    Random,

    /// Pick a bucket at random, weighted by how many segments it holds, and
    /// evict that bucket's head (oldest) segment.
    ///
    /// This weights eviction toward the TTL ranges that consume the most
    /// memory while preserving the overall TTL distribution. It is the
    /// default because it is what this layer has always done: before #156,
    /// `Fifo`, `Random` and `Cte` all ran this code and nothing else.
    #[default]
    RandomFifo,

    /// CTE - evict the segment whose items expire soonest.
    ///
    /// The victim is the smallest `expire_at` among evictable segments, ties
    /// broken by age so the choice is deterministic. A segment with no
    /// segment-level expiry (`expire_at == 0`) sorts last: it never expires,
    /// so it is never the closest to expiring.
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
            EvictionStrategy::Fifo
            | EvictionStrategy::Random
            | EvictionStrategy::RandomFifo
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
        assert_eq!(EvictionStrategy::RandomFifo.merge_params(), None);
        assert_eq!(EvictionStrategy::Cte.merge_params(), None);
    }

    /// The default must stay the strategy this layer has always run, or
    /// every caller that never set one silently changes behaviour. Before
    /// #156 the default was spelled `Random` and did random-FIFO; naming it
    /// honestly must not move it.
    /// splitmix64 maps 0 to 0, and the layer's stream starts by returning
    /// its seed unmixed, so a seed of zero makes the first draw of every run
    /// zero -- the first eviction would always fall to the lowest-indexed
    /// bucket. Whatever the default is, it must not be that.
    #[test]
    fn the_default_eviction_seed_is_not_the_one_value_that_degenerates() {
        assert_ne!(DEFAULT_EVICTION_SEED, 0);
    }

    /// A layer that was never given a seed must use the documented default,
    /// or the value the docs point callers at is not the value they get.
    #[test]
    fn an_unconfigured_layer_uses_the_default_eviction_seed() {
        assert_eq!(LayerConfig::new().eviction_seed, DEFAULT_EVICTION_SEED);
        assert_eq!(
            LayerConfig::new().with_eviction_seed(99).eviction_seed,
            99,
            "the setter must reach the field"
        );
    }

    #[test]
    fn the_default_strategy_is_random_fifo() {
        assert_eq!(EvictionStrategy::default(), EvictionStrategy::RandomFifo);
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

/// What an overwrite does with the space the superseded copy held.
///
/// `set`, `replace`, `cas` and a committed streaming set all supersede an
/// existing item, and all four historically marked the old copy deleted and
/// left its bytes in place until a merge pass happened to sweep the segment.
/// `delete` alone reclaimed eagerly.
///
/// That asymmetry is measurable. On an overwrite-heavy trace (80% SET) the
/// cache held 1673 bytes per resident item against a comparable engine's
/// 909, and the figure grew with heap size -- a larger heap gives dead bytes
/// more room to accumulate before pressure forces a merge -- while the
/// comparable engine's stayed flat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverwriteReclaim {
    /// Mark the old copy deleted and leave its bytes for the next merge.
    #[default]
    Deferred,
    /// Also free the segment if the overwrite emptied it. This is what the
    /// comparable engine does on the same operation.
    FreeEmpty,
    /// Also attempt compaction with the predecessor segment, which is what
    /// `delete` has always done.
    Compact,
}
