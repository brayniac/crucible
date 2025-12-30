//! Eviction policy definitions.

/// Decision for an item during eviction.
///
/// When evicting a segment, each item must be processed. This enum
/// represents the possible outcomes for an item based on its frequency
/// and the layer configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemFate {
    /// Convert to ghost entry in hashtable.
    ///
    /// Preserves frequency information for later re-admission decisions.
    /// Used when an item is being evicted but we want to remember it was here.
    Ghost,

    /// Demote to next layer.
    ///
    /// Item has sufficient frequency to be worth keeping but must move
    /// to make room for new items. Frequency may be decayed during demotion.
    Demote,

    /// Discard completely.
    ///
    /// Item has low frequency and no ghost tracking is desired.
    /// The hashtable entry is simply removed.
    Discard,
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_item_fate_variants() {
        assert_eq!(ItemFate::Ghost, ItemFate::Ghost);
        assert_ne!(ItemFate::Ghost, ItemFate::Demote);
        assert_ne!(ItemFate::Demote, ItemFate::Discard);
    }
}
