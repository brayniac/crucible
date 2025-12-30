//! HTTP/2 connection settings.

use crate::frame;

/// HTTP/2 connection settings.
///
/// These track both local settings (what we advertise to the peer) and
/// remote settings (what the peer advertises to us).
#[derive(Debug, Clone, Copy)]
pub struct ConnectionSettings {
    /// Maximum number of concurrent streams.
    pub max_concurrent_streams: u32,
    /// Initial stream window size.
    pub initial_window_size: u32,
    /// Maximum frame size.
    pub max_frame_size: u32,
    /// Maximum header list size.
    pub max_header_list_size: u32,
    /// HPACK header table size.
    pub header_table_size: u32,
    /// Whether server push is enabled.
    pub enable_push: bool,
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 100,
            initial_window_size: frame::DEFAULT_INITIAL_WINDOW_SIZE,
            max_frame_size: frame::DEFAULT_MAX_FRAME_SIZE,
            max_header_list_size: 16384,
            header_table_size: 4096,
            enable_push: false, // Client typically disables server push
        }
    }
}

impl ConnectionSettings {
    /// Create new settings with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum concurrent streams.
    pub fn max_concurrent_streams(mut self, value: u32) -> Self {
        self.max_concurrent_streams = value;
        self
    }

    /// Set initial window size.
    pub fn initial_window_size(mut self, value: u32) -> Self {
        self.initial_window_size = value;
        self
    }

    /// Set maximum frame size.
    pub fn max_frame_size(mut self, value: u32) -> Self {
        self.max_frame_size = value;
        self
    }

    /// Set maximum header list size.
    pub fn max_header_list_size(mut self, value: u32) -> Self {
        self.max_header_list_size = value;
        self
    }

    /// Set header table size.
    pub fn header_table_size(mut self, value: u32) -> Self {
        self.header_table_size = value;
        self
    }

    /// Enable or disable server push.
    pub fn enable_push(mut self, value: bool) -> Self {
        self.enable_push = value;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_settings() {
        let settings = ConnectionSettings::default();
        assert_eq!(settings.max_concurrent_streams, 100);
        assert_eq!(settings.initial_window_size, 65535);
        assert_eq!(settings.max_frame_size, 16384);
        assert!(!settings.enable_push);
    }

    #[test]
    fn test_new_settings() {
        let settings = ConnectionSettings::new();
        assert_eq!(settings.max_concurrent_streams, 100);
    }

    #[test]
    fn test_builder_pattern() {
        let settings = ConnectionSettings::new()
            .max_concurrent_streams(200)
            .initial_window_size(32768);

        assert_eq!(settings.max_concurrent_streams, 200);
        assert_eq!(settings.initial_window_size, 32768);
    }

    #[test]
    fn test_max_frame_size() {
        let settings = ConnectionSettings::new().max_frame_size(32768);
        assert_eq!(settings.max_frame_size, 32768);
    }

    #[test]
    fn test_max_header_list_size() {
        let settings = ConnectionSettings::new().max_header_list_size(8192);
        assert_eq!(settings.max_header_list_size, 8192);
    }

    #[test]
    fn test_header_table_size() {
        let settings = ConnectionSettings::new().header_table_size(8192);
        assert_eq!(settings.header_table_size, 8192);
    }

    #[test]
    fn test_enable_push() {
        let settings = ConnectionSettings::new().enable_push(true);
        assert!(settings.enable_push);

        let settings = ConnectionSettings::new().enable_push(false);
        assert!(!settings.enable_push);
    }

    #[test]
    fn test_builder_chained() {
        let settings = ConnectionSettings::new()
            .max_concurrent_streams(50)
            .initial_window_size(16384)
            .max_frame_size(65535)
            .max_header_list_size(32768)
            .header_table_size(2048)
            .enable_push(true);

        assert_eq!(settings.max_concurrent_streams, 50);
        assert_eq!(settings.initial_window_size, 16384);
        assert_eq!(settings.max_frame_size, 65535);
        assert_eq!(settings.max_header_list_size, 32768);
        assert_eq!(settings.header_table_size, 2048);
        assert!(settings.enable_push);
    }

    #[test]
    fn test_settings_debug() {
        let settings = ConnectionSettings::new();
        let debug = format!("{:?}", settings);
        assert!(debug.contains("ConnectionSettings"));
    }

    #[test]
    fn test_settings_clone() {
        let settings = ConnectionSettings::new().max_concurrent_streams(42);
        let cloned = settings;
        assert_eq!(cloned.max_concurrent_streams, 42);
    }
}
