//! gRPC metadata (headers and trailers).

use std::collections::HashMap;

/// gRPC metadata key-value pairs.
///
/// Metadata is used for headers (sent before the message) and
/// trailers (sent after the message with status).
#[derive(Debug, Clone, Default)]
pub struct Metadata {
    /// Key-value pairs. Keys are lowercase ASCII.
    entries: HashMap<String, Vec<String>>,
}

impl Metadata {
    /// Create empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a key-value pair.
    ///
    /// Keys are normalized to lowercase. Multiple values for the same
    /// key are allowed.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into().to_ascii_lowercase();
        let value = value.into();

        self.entries.entry(key).or_default().push(value);
    }

    /// Get the first value for a key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .get(&key.to_ascii_lowercase())
            .and_then(|v| v.first())
            .map(|s| s.as_str())
    }

    /// Get all values for a key.
    pub fn get_all(&self, key: &str) -> Option<&[String]> {
        self.entries
            .get(&key.to_ascii_lowercase())
            .map(|v| v.as_slice())
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.contains_key(&key.to_ascii_lowercase())
    }

    /// Remove all values for a key.
    pub fn remove(&mut self, key: &str) -> Option<Vec<String>> {
        self.entries.remove(&key.to_ascii_lowercase())
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries
            .iter()
            .flat_map(|(k, values)| values.iter().map(move |v| (k.as_str(), v.as_str())))
    }

    /// Check if metadata is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the number of entries (including duplicates).
    pub fn len(&self) -> usize {
        self.entries.values().map(|v| v.len()).sum()
    }
}

impl FromIterator<(String, String)> for Metadata {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        let mut metadata = Metadata::new();
        for (key, value) in iter {
            metadata.insert(key, value);
        }
        metadata
    }
}

/// Standard gRPC timeout format.
///
/// Format: `<value><unit>` where unit is:
/// - `n`: nanoseconds
/// - `u`: microseconds
/// - `m`: milliseconds
/// - `S`: seconds
/// - `M`: minutes
/// - `H`: hours
#[derive(Debug, Clone, Copy)]
pub struct Timeout {
    /// Timeout in nanoseconds.
    nanos: u64,
}

impl Timeout {
    /// Create a timeout from seconds.
    pub fn from_secs(secs: u64) -> Self {
        Self {
            nanos: secs * 1_000_000_000,
        }
    }

    /// Create a timeout from milliseconds.
    pub fn from_millis(millis: u64) -> Self {
        Self {
            nanos: millis * 1_000_000,
        }
    }

    /// Create a timeout from a duration.
    pub fn from_duration(duration: std::time::Duration) -> Self {
        Self {
            nanos: duration.as_nanos() as u64,
        }
    }

    /// Get the timeout as a duration.
    pub fn as_duration(&self) -> std::time::Duration {
        std::time::Duration::from_nanos(self.nanos)
    }

    /// Format as gRPC timeout header value.
    pub fn to_grpc_format(self) -> String {
        // Use the largest unit that gives an integer value
        if self.nanos >= 3_600_000_000_000 && self.nanos.is_multiple_of(3_600_000_000_000) {
            format!("{}H", self.nanos / 3_600_000_000_000)
        } else if self.nanos >= 60_000_000_000 && self.nanos.is_multiple_of(60_000_000_000) {
            format!("{}M", self.nanos / 60_000_000_000)
        } else if self.nanos >= 1_000_000_000 && self.nanos.is_multiple_of(1_000_000_000) {
            format!("{}S", self.nanos / 1_000_000_000)
        } else if self.nanos >= 1_000_000 && self.nanos.is_multiple_of(1_000_000) {
            format!("{}m", self.nanos / 1_000_000)
        } else if self.nanos >= 1_000 && self.nanos.is_multiple_of(1_000) {
            format!("{}u", self.nanos / 1_000)
        } else {
            format!("{}n", self.nanos)
        }
    }

    /// Parse from gRPC timeout header value.
    pub fn parse(s: &str) -> Option<Self> {
        if s.is_empty() {
            return None;
        }

        let (value_str, unit) = s.split_at(s.len() - 1);
        let value: u64 = value_str.parse().ok()?;

        let nanos = match unit {
            "n" => value,
            "u" => value * 1_000,
            "m" => value * 1_000_000,
            "S" => value * 1_000_000_000,
            "M" => value * 60_000_000_000,
            "H" => value * 3_600_000_000_000,
            _ => return None,
        };

        Some(Self { nanos })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_new() {
        let md = Metadata::new();
        assert!(md.is_empty());
        assert_eq!(md.len(), 0);
    }

    #[test]
    fn test_metadata_default() {
        let md = Metadata::default();
        assert!(md.is_empty());
    }

    #[test]
    fn test_metadata_insert_get() {
        let mut md = Metadata::new();
        md.insert("Content-Type", "application/grpc");

        assert_eq!(md.get("content-type"), Some("application/grpc"));
        assert_eq!(md.get("Content-Type"), Some("application/grpc"));
    }

    #[test]
    fn test_metadata_multiple_values() {
        let mut md = Metadata::new();
        md.insert("key", "value1");
        md.insert("key", "value2");

        assert_eq!(md.get("key"), Some("value1"));
        assert_eq!(
            md.get_all("key"),
            Some(&["value1".to_string(), "value2".to_string()][..])
        );
    }

    #[test]
    fn test_metadata_get_nonexistent() {
        let md = Metadata::new();
        assert!(md.get("nonexistent").is_none());
        assert!(md.get_all("nonexistent").is_none());
    }

    #[test]
    fn test_metadata_contains_key() {
        let mut md = Metadata::new();
        md.insert("key", "value");

        assert!(md.contains_key("key"));
        assert!(md.contains_key("KEY")); // Case insensitive
        assert!(!md.contains_key("other"));
    }

    #[test]
    fn test_metadata_remove() {
        let mut md = Metadata::new();
        md.insert("key", "value1");
        md.insert("key", "value2");

        let removed = md.remove("key");
        assert_eq!(
            removed,
            Some(vec!["value1".to_string(), "value2".to_string()])
        );
        assert!(md.is_empty());
    }

    #[test]
    fn test_metadata_remove_nonexistent() {
        let mut md = Metadata::new();
        assert!(md.remove("nonexistent").is_none());
    }

    #[test]
    fn test_metadata_iter() {
        let mut md = Metadata::new();
        md.insert("key1", "value1");
        md.insert("key2", "value2");

        let entries: Vec<_> = md.iter().collect();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_metadata_iter_with_duplicates() {
        let mut md = Metadata::new();
        md.insert("key", "value1");
        md.insert("key", "value2");

        let entries: Vec<_> = md.iter().collect();
        assert_eq!(entries.len(), 2);
        assert!(entries.contains(&("key", "value1")));
        assert!(entries.contains(&("key", "value2")));
    }

    #[test]
    fn test_metadata_len() {
        let mut md = Metadata::new();
        assert_eq!(md.len(), 0);

        md.insert("key1", "value1");
        assert_eq!(md.len(), 1);

        md.insert("key1", "value2"); // Same key
        assert_eq!(md.len(), 2);

        md.insert("key2", "value3");
        assert_eq!(md.len(), 3);
    }

    #[test]
    fn test_metadata_from_iterator() {
        let pairs = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ];

        let md: Metadata = pairs.into_iter().collect();
        assert_eq!(md.len(), 2);
        assert_eq!(md.get("key1"), Some("value1"));
        assert_eq!(md.get("key2"), Some("value2"));
    }

    #[test]
    fn test_metadata_clone() {
        let mut md1 = Metadata::new();
        md1.insert("key", "value");

        let md2 = md1.clone();
        assert_eq!(md1.get("key"), md2.get("key"));
    }

    #[test]
    fn test_metadata_debug() {
        let md = Metadata::new();
        let debug_str = format!("{:?}", md);
        assert!(debug_str.contains("Metadata"));
    }

    #[test]
    fn test_timeout_from_secs() {
        let timeout = Timeout::from_secs(10);
        assert_eq!(timeout.as_duration().as_secs(), 10);
    }

    #[test]
    fn test_timeout_from_millis() {
        let timeout = Timeout::from_millis(500);
        assert_eq!(timeout.as_duration().as_millis(), 500);
    }

    #[test]
    fn test_timeout_from_duration() {
        let duration = std::time::Duration::from_secs(5);
        let timeout = Timeout::from_duration(duration);
        assert_eq!(timeout.as_duration(), duration);
    }

    #[test]
    fn test_timeout_format() {
        assert_eq!(Timeout::from_secs(10).to_grpc_format(), "10S");
        assert_eq!(Timeout::from_millis(500).to_grpc_format(), "500m");
        assert_eq!(Timeout::from_secs(3600).to_grpc_format(), "1H");
    }

    #[test]
    fn test_timeout_format_minutes() {
        // 60 seconds = 1 minute
        let timeout = Timeout::from_secs(60);
        assert_eq!(timeout.to_grpc_format(), "1M");

        // 120 seconds = 2 minutes
        let timeout = Timeout::from_secs(120);
        assert_eq!(timeout.to_grpc_format(), "2M");
    }

    #[test]
    fn test_timeout_format_microseconds() {
        // 1000 nanoseconds = 1 microsecond
        let timeout = Timeout { nanos: 1000 };
        assert_eq!(timeout.to_grpc_format(), "1u");
    }

    #[test]
    fn test_timeout_format_nanoseconds() {
        let timeout = Timeout { nanos: 500 };
        assert_eq!(timeout.to_grpc_format(), "500n");
    }

    #[test]
    fn test_timeout_parse() {
        assert_eq!(Timeout::parse("10S").unwrap().as_duration().as_secs(), 10);
        assert_eq!(
            Timeout::parse("500m").unwrap().as_duration().as_millis(),
            500
        );
        assert_eq!(Timeout::parse("2H").unwrap().as_duration().as_secs(), 7200);
    }

    #[test]
    fn test_timeout_parse_all_units() {
        // Nanoseconds
        let t = Timeout::parse("100n").unwrap();
        assert_eq!(t.as_duration().as_nanos(), 100);

        // Microseconds
        let t = Timeout::parse("50u").unwrap();
        assert_eq!(t.as_duration().as_micros(), 50);

        // Milliseconds
        let t = Timeout::parse("200m").unwrap();
        assert_eq!(t.as_duration().as_millis(), 200);

        // Seconds
        let t = Timeout::parse("30S").unwrap();
        assert_eq!(t.as_duration().as_secs(), 30);

        // Minutes
        let t = Timeout::parse("5M").unwrap();
        assert_eq!(t.as_duration().as_secs(), 300);

        // Hours
        let t = Timeout::parse("1H").unwrap();
        assert_eq!(t.as_duration().as_secs(), 3600);
    }

    #[test]
    fn test_timeout_parse_empty() {
        assert!(Timeout::parse("").is_none());
    }

    #[test]
    fn test_timeout_parse_invalid_unit() {
        assert!(Timeout::parse("10x").is_none());
        assert!(Timeout::parse("10s").is_none()); // lowercase s
    }

    #[test]
    fn test_timeout_parse_invalid_value() {
        assert!(Timeout::parse("abcS").is_none());
    }

    #[test]
    fn test_timeout_clone_copy() {
        let t1 = Timeout::from_secs(10);
        let t2 = t1;
        assert_eq!(t1.as_duration(), t2.as_duration());
    }

    #[test]
    fn test_timeout_debug() {
        let timeout = Timeout::from_secs(10);
        let debug_str = format!("{:?}", timeout);
        assert!(debug_str.contains("Timeout"));
    }
}
