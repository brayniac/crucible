//! Crash recovery logic for disk-backed cache.
//!
//! Provides functionality to recover items from a disk cache file
//! after a restart and rebuild the hashtable index.

use crate::disk::file_pool::FilePool;
use crate::disk::file_segment::FileSegment;
use crate::hashtable::Hashtable;
use crate::item::BasicHeader;
use crate::item_location::ItemLocation;
use crate::pool::RamPool;
use crate::segment::Segment;
use crate::state::State;

/// Statistics from segment recovery.
#[derive(Debug, Clone, Default)]
pub struct RecoveryStats {
    /// Number of segments scanned.
    pub segments_scanned: usize,
    /// Number of valid items found.
    pub items_recovered: usize,
    /// Number of expired items skipped.
    pub items_expired: usize,
    /// Number of deleted items skipped.
    pub items_deleted: usize,
    /// Number of corrupted items found.
    pub items_corrupted: usize,
    /// Total bytes recovered.
    pub bytes_recovered: usize,
    /// Total bytes truncated due to corruption.
    pub bytes_truncated: usize,
}

impl RecoveryStats {
    /// Create new empty stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge another stats into this one.
    pub fn merge(&mut self, other: &RecoveryStats) {
        self.segments_scanned += other.segments_scanned;
        self.items_recovered += other.items_recovered;
        self.items_expired += other.items_expired;
        self.items_deleted += other.items_deleted;
        self.items_corrupted += other.items_corrupted;
        self.bytes_recovered += other.bytes_recovered;
        self.bytes_truncated += other.bytes_truncated;
    }
}

/// Statistics from index warm-up.
#[derive(Debug, Clone, Default)]
pub struct WarmStats {
    /// Number of items indexed.
    pub items_indexed: usize,
    /// Number of items that failed to insert (hashtable full).
    pub items_skipped: usize,
    /// Number of segments processed.
    pub segments_processed: usize,
}

impl WarmStats {
    /// Create new empty stats.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Recover valid items from a file pool segment.
///
/// Scans the segment to find valid items, skipping expired and deleted ones.
/// Returns stats about what was found.
///
/// # Arguments
/// * `segment` - The segment to scan
/// * `now` - Current timestamp (seconds since epoch)
/// * `segment_expire_at` - Segment-level expiration time
pub fn recover_segment(
    segment: &FileSegment<'_>,
    now: u32,
    segment_expire_at: u32,
) -> RecoveryStats {
    let mut stats = RecoveryStats::new();
    stats.segments_scanned = 1;

    // Check segment-level expiration first
    if segment_expire_at > 0 && now >= segment_expire_at {
        // Entire segment is expired
        stats.bytes_truncated = segment.write_offset() as usize;
        return stats;
    }

    // Scan items in the segment
    let mut offset = 0u32;
    let write_offset = segment.write_offset();

    while offset < write_offset {
        // Try to parse header at this offset
        if let Some(data) = segment.data_slice(offset, BasicHeader::SIZE) {
            if let Some(header) = BasicHeader::try_from_bytes(data) {
                let item_size = header.padded_size() as u32;

                // Validate item bounds
                if offset + item_size > write_offset {
                    stats.items_corrupted += 1;
                    stats.bytes_truncated += (write_offset - offset) as usize;
                    break;
                }

                if header.is_deleted() {
                    stats.items_deleted += 1;
                } else {
                    // Valid item
                    stats.items_recovered += 1;
                    stats.bytes_recovered += item_size as usize;
                }

                offset += item_size;
            } else {
                // Corrupted header - truncate rest of segment
                stats.items_corrupted += 1;
                stats.bytes_truncated += (write_offset - offset) as usize;
                break;
            }
        } else {
            // Can't read header data
            stats.bytes_truncated += (write_offset - offset) as usize;
            break;
        }
    }

    stats
}

/// Rebuild hashtable index from a recovered file pool.
///
/// Scans all segments in the pool and inserts valid items into the hashtable.
///
/// # Arguments
/// * `pool` - The file pool to scan
/// * `hashtable` - The hashtable to populate
/// * `now` - Current timestamp (seconds since epoch)
///
/// # Returns
/// Warm-up statistics
pub fn warm_from_pool<H: Hashtable>(pool: &FilePool, hashtable: &H, now: u32) -> WarmStats {
    let mut stats = WarmStats::new();

    for segment_id in pool.segment_ids() {
        if let Some(segment) = pool.get(segment_id) {
            stats.segments_processed += 1;

            let segment_expire_at = segment.expire_at();

            // Skip expired segments
            if segment_expire_at > 0 && now >= segment_expire_at {
                continue;
            }

            // Scan items in the segment
            let mut offset = 0u32;
            let write_offset = segment.write_offset();

            while offset < write_offset {
                if let Some(data) = segment.data_slice(offset, BasicHeader::SIZE) {
                    if let Some(header) = BasicHeader::try_from_bytes(data) {
                        let item_size = header.padded_size() as u32;

                        // Skip deleted items
                        if !header.is_deleted() {
                            // Extract key
                            let key_start = offset as usize
                                + BasicHeader::SIZE
                                + header.optional_len() as usize;
                            let key_len = header.key_len() as usize;

                            if let Some(key) = segment.data_slice(key_start as u32, key_len) {
                                // Create location for this item
                                let location =
                                    ItemLocation::new(pool.pool_id(), segment_id, offset);

                                // Insert into hashtable
                                // Note: We use a dummy verifier since we're rebuilding
                                let verifier = DummyVerifier;
                                match hashtable.insert_if_absent(
                                    key,
                                    location.to_location(),
                                    &verifier,
                                ) {
                                    Ok(()) => stats.items_indexed += 1,
                                    Err(_) => stats.items_skipped += 1,
                                }
                            }
                        }

                        offset += item_size;
                    } else {
                        // Corrupted - stop scanning this segment
                        break;
                    }
                } else {
                    break;
                }
            }

            // Mark segment as Live so it can be read
            if segment.state() == State::Free {
                segment.try_reserve();
                segment.cas_metadata(State::Reserved, State::Live, None, None);
            }
        }
    }

    stats
}

/// Dummy key verifier for index rebuild.
///
/// During warm-up, we don't need to verify keys since we're rebuilding
/// the index from the authoritative disk data.
struct DummyVerifier;

impl crate::hashtable::KeyVerifier for DummyVerifier {
    fn verify(
        &self,
        _key: &[u8],
        _location: crate::location::Location,
        _allow_deleted: bool,
    ) -> bool {
        // During rebuild, we trust the disk data
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Tests for recovery require a real FilePool which is tested
    // in the file_pool and disk_layer modules. Here we test the stats.

    #[test]
    fn test_recovery_stats_default() {
        let stats = RecoveryStats::default();
        assert_eq!(stats.segments_scanned, 0);
        assert_eq!(stats.items_recovered, 0);
    }

    #[test]
    fn test_recovery_stats_merge() {
        let mut stats1 = RecoveryStats {
            segments_scanned: 1,
            items_recovered: 10,
            items_expired: 2,
            items_deleted: 1,
            items_corrupted: 0,
            bytes_recovered: 1000,
            bytes_truncated: 0,
        };

        let stats2 = RecoveryStats {
            segments_scanned: 2,
            items_recovered: 20,
            items_expired: 3,
            items_deleted: 2,
            items_corrupted: 1,
            bytes_recovered: 2000,
            bytes_truncated: 100,
        };

        stats1.merge(&stats2);

        assert_eq!(stats1.segments_scanned, 3);
        assert_eq!(stats1.items_recovered, 30);
        assert_eq!(stats1.items_expired, 5);
        assert_eq!(stats1.items_deleted, 3);
        assert_eq!(stats1.items_corrupted, 1);
        assert_eq!(stats1.bytes_recovered, 3000);
        assert_eq!(stats1.bytes_truncated, 100);
    }

    #[test]
    fn test_warm_stats_default() {
        let stats = WarmStats::default();
        assert_eq!(stats.items_indexed, 0);
        assert_eq!(stats.items_skipped, 0);
        assert_eq!(stats.segments_processed, 0);
    }
}
