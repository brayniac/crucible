//! Configuration types for disk storage.

use std::path::PathBuf;

/// Configuration for disk-backed storage tier.
#[derive(Debug, Clone)]
pub struct DiskConfig {
    /// Whether the disk tier is enabled.
    pub enabled: bool,

    /// Path to the disk cache file or directory.
    pub path: PathBuf,

    /// Total size of disk storage in bytes.
    pub size: usize,

    /// Frequency threshold for promoting items from disk to RAM.
    /// Items with frequency > threshold are promoted on read.
    /// Default: 2
    pub promotion_threshold: u8,

    /// Synchronization mode for disk writes.
    pub sync_mode: SyncMode,

    /// Whether to attempt recovery from existing disk cache on startup.
    /// Default: true
    pub recover_on_startup: bool,

    /// Segment size in bytes (inherited from RAM tier if not specified).
    /// Default: None (use RAM tier segment size)
    pub segment_size: Option<usize>,
}

impl Default for DiskConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: PathBuf::from("/var/cache/crucible"),
            size: 10 * 1024 * 1024 * 1024, // 10GB
            promotion_threshold: 2,
            sync_mode: SyncMode::default(),
            recover_on_startup: true,
            segment_size: None,
        }
    }
}

impl DiskConfig {
    /// Create a new disk config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable disk tier.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the path for disk cache storage.
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = path.into();
        self
    }

    /// Set the total disk storage size in bytes.
    pub fn size(mut self, size: usize) -> Self {
        self.size = size;
        self
    }

    /// Set the promotion threshold.
    ///
    /// Items with frequency > threshold are promoted to RAM on disk hit.
    pub fn promotion_threshold(mut self, threshold: u8) -> Self {
        self.promotion_threshold = threshold;
        self
    }

    /// Set the synchronization mode.
    pub fn sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    /// Set whether to recover from existing disk cache on startup.
    pub fn recover_on_startup(mut self, recover: bool) -> Self {
        self.recover_on_startup = recover;
        self
    }

    /// Set the segment size in bytes.
    pub fn segment_size(mut self, size: usize) -> Self {
        self.segment_size = Some(size);
        self
    }

    /// Calculate the number of segments based on size and segment_size.
    pub fn segment_count(&self, default_segment_size: usize) -> usize {
        let segment_size = self.segment_size.unwrap_or(default_segment_size);
        if segment_size == 0 {
            0
        } else {
            self.size / segment_size
        }
    }
}

/// Synchronization mode for disk writes.
///
/// Controls how aggressively data is flushed to disk, trading off
/// durability against write performance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Synchronous writes - fsync after each write operation.
    ///
    /// Provides strongest durability guarantees but lowest performance.
    /// Use for critical data where no loss is acceptable.
    Sync,

    /// Asynchronous writes - periodic fsync via background thread.
    ///
    /// Balances durability and performance. Data may be lost on crash
    /// if written since last sync. Default mode.
    #[default]
    Async,

    /// No explicit sync - let the OS handle flushing.
    ///
    /// Highest performance but data may be lost on crash.
    /// Suitable for pure cache use cases where loss is acceptable.
    None,
}

impl SyncMode {
    /// Check if this mode requires immediate fsync after writes.
    pub fn is_sync(&self) -> bool {
        matches!(self, SyncMode::Sync)
    }

    /// Check if this mode uses background sync.
    pub fn is_async(&self) -> bool {
        matches!(self, SyncMode::Async)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_config_defaults() {
        let config = DiskConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.promotion_threshold, 2);
        assert_eq!(config.sync_mode, SyncMode::Async);
        assert!(config.recover_on_startup);
    }

    #[test]
    fn test_disk_config_builder() {
        let config = DiskConfig::new()
            .enabled(true)
            .path("/tmp/cache")
            .size(1024 * 1024 * 1024)
            .promotion_threshold(3)
            .sync_mode(SyncMode::Sync)
            .recover_on_startup(false)
            .segment_size(8 * 1024 * 1024);

        assert!(config.enabled);
        assert_eq!(config.path, PathBuf::from("/tmp/cache"));
        assert_eq!(config.size, 1024 * 1024 * 1024);
        assert_eq!(config.promotion_threshold, 3);
        assert_eq!(config.sync_mode, SyncMode::Sync);
        assert!(!config.recover_on_startup);
        assert_eq!(config.segment_size, Some(8 * 1024 * 1024));
    }

    #[test]
    fn test_segment_count() {
        let config = DiskConfig::new().size(100 * 1024 * 1024); // 100MB

        // Without explicit segment size, use default
        assert_eq!(config.segment_count(1024 * 1024), 100); // 100 segments

        // With explicit segment size
        let config = config.segment_size(8 * 1024 * 1024); // 8MB segments
        assert_eq!(config.segment_count(1024 * 1024), 12); // Uses explicit size
    }

    #[test]
    fn test_sync_mode_checks() {
        assert!(SyncMode::Sync.is_sync());
        assert!(!SyncMode::Sync.is_async());

        assert!(!SyncMode::Async.is_sync());
        assert!(SyncMode::Async.is_async());

        assert!(!SyncMode::None.is_sync());
        assert!(!SyncMode::None.is_async());
    }
}
