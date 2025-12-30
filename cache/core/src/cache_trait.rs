//! Cache trait for compatibility with server implementations.
//!
//! This module provides a simple `Cache` trait that can be implemented by
//! different cache backends (S3FifoCache, SegCache, etc.) for use with
//! protocol servers like iou-cache and tokio-cache.

use crate::error::CacheError;
use std::time::Duration;

/// A simple guard that owns the cached value.
///
/// This is returned by cache get operations and provides access to the value.
#[derive(Debug, Clone)]
pub struct OwnedGuard {
    value: Vec<u8>,
}

impl OwnedGuard {
    /// Create a new owned guard from a value.
    pub fn new(value: Vec<u8>) -> Self {
        Self { value }
    }

    /// Get the value as a byte slice.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Consume the guard and return the owned value.
    pub fn into_value(self) -> Vec<u8> {
        self.value
    }
}

impl AsRef<[u8]> for OwnedGuard {
    fn as_ref(&self) -> &[u8] {
        &self.value
    }
}

/// Trait for cache implementations.
///
/// This trait defines the core operations that cache backends must support
/// for use with protocol servers.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow concurrent access from
/// multiple threads.
pub trait Cache: Send + Sync + 'static {
    /// Get a value from the cache.
    ///
    /// Returns `Some(guard)` if the key exists, `None` otherwise.
    /// The guard provides access to the value.
    fn get(&self, key: &[u8]) -> Option<OwnedGuard>;

    /// Set a key-value pair in the cache.
    ///
    /// `ttl` specifies the time-to-live for the entry. If `None`, a default
    /// TTL is used (implementation-dependent).
    fn set(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError>;

    /// Delete a key from the cache.
    ///
    /// Returns `true` if the key was present and deleted, `false` otherwise.
    fn delete(&self, key: &[u8]) -> bool;

    /// Check if a key exists in the cache.
    fn contains(&self, key: &[u8]) -> bool;

    /// Flush all entries from the cache.
    ///
    /// Note: This may be a no-op for some implementations.
    fn flush(&self);
}

/// Default TTL used when None is provided (1 hour).
pub const DEFAULT_TTL: Duration = Duration::from_secs(3600);

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_owned_guard_new() {
        let data = vec![1, 2, 3, 4, 5];
        let guard = OwnedGuard::new(data.clone());
        assert_eq!(guard.value(), &data[..]);
    }

    #[test]
    fn test_owned_guard_value() {
        let guard = OwnedGuard::new(vec![10, 20, 30]);
        assert_eq!(guard.value(), &[10, 20, 30]);
    }

    #[test]
    fn test_owned_guard_into_value() {
        let data = vec![1, 2, 3];
        let guard = OwnedGuard::new(data.clone());
        let extracted = guard.into_value();
        assert_eq!(extracted, data);
    }

    #[test]
    fn test_owned_guard_as_ref() {
        let guard = OwnedGuard::new(vec![5, 6, 7]);
        let slice: &[u8] = guard.as_ref();
        assert_eq!(slice, &[5, 6, 7]);
    }

    #[test]
    fn test_owned_guard_clone() {
        let guard1 = OwnedGuard::new(vec![1, 2, 3]);
        let guard2 = guard1.clone();
        assert_eq!(guard1.value(), guard2.value());
    }

    #[test]
    fn test_owned_guard_debug() {
        let guard = OwnedGuard::new(vec![1, 2, 3]);
        let debug_str = format!("{:?}", guard);
        assert!(debug_str.contains("OwnedGuard"));
    }

    #[test]
    fn test_owned_guard_empty() {
        let guard = OwnedGuard::new(vec![]);
        assert!(guard.value().is_empty());
        assert_eq!(guard.into_value().len(), 0);
    }

    #[test]
    fn test_default_ttl() {
        assert_eq!(DEFAULT_TTL, Duration::from_secs(3600));
        assert_eq!(DEFAULT_TTL.as_secs(), 3600);
    }
}
