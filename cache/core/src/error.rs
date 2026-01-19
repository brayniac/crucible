//! Error types for cache operations.

use std::fmt;

/// Errors that can occur during cache operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheError {
    /// No memory available to store the item.
    /// Eviction was attempted but failed to free sufficient space.
    OutOfMemory,

    /// Hashtable is full (all buckets exhausted).
    /// This can happen with very high load factors.
    HashTableFull,

    /// Key already exists (for ADD operations).
    KeyExists,

    /// Key not found (for REPLACE/DELETE/CAS operations).
    KeyNotFound,

    /// CAS token mismatch - item was modified since last read.
    CasMismatch,

    /// Item has expired and is no longer valid.
    ItemExpired,

    /// Item was deleted before the operation could complete.
    ItemDeleted,

    /// The key is too long (max 255 bytes).
    KeyTooLong,

    /// The value is too long (max 16MB).
    ValueTooLong,

    /// The optional data is too long (max 64 bytes).
    OptionalTooLong,

    /// Invalid TTL value.
    InvalidTtl,

    /// Segment or item has expired.
    Expired,

    /// Invalid offset within a segment.
    InvalidOffset,

    /// Data corruption detected (bad magic, checksum, etc.).
    Corrupted,

    /// Key does not match at expected location.
    KeyMismatch,

    /// Segment is not in an accessible state for the operation.
    SegmentNotAccessible,

    /// Operation not supported by this cache implementation.
    Unsupported,
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfMemory => write!(f, "out of memory"),
            Self::HashTableFull => write!(f, "hashtable full"),
            Self::KeyExists => write!(f, "key already exists"),
            Self::KeyNotFound => write!(f, "key not found"),
            Self::CasMismatch => write!(f, "CAS token mismatch"),
            Self::ItemExpired => write!(f, "item expired"),
            Self::ItemDeleted => write!(f, "item deleted"),
            Self::KeyTooLong => write!(f, "key too long (max 255 bytes)"),
            Self::ValueTooLong => write!(f, "value too long (max 16MB)"),
            Self::OptionalTooLong => write!(f, "optional data too long (max 64 bytes)"),
            Self::InvalidTtl => write!(f, "invalid TTL value"),
            Self::Expired => write!(f, "expired"),
            Self::InvalidOffset => write!(f, "invalid offset"),
            Self::Corrupted => write!(f, "data corrupted"),
            Self::KeyMismatch => write!(f, "key mismatch"),
            Self::SegmentNotAccessible => write!(f, "segment not accessible"),
            Self::Unsupported => write!(f, "operation not supported"),
        }
    }
}

impl std::error::Error for CacheError {}

/// Result type for cache operations.
pub type CacheResult<T> = Result<T, CacheError>;

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_all_variants() {
        // Test all error variants have expected display messages
        assert_eq!(format!("{}", CacheError::OutOfMemory), "out of memory");
        assert_eq!(format!("{}", CacheError::HashTableFull), "hashtable full");
        assert_eq!(format!("{}", CacheError::KeyExists), "key already exists");
        assert_eq!(format!("{}", CacheError::KeyNotFound), "key not found");
        assert_eq!(format!("{}", CacheError::CasMismatch), "CAS token mismatch");
        assert_eq!(format!("{}", CacheError::ItemExpired), "item expired");
        assert_eq!(format!("{}", CacheError::ItemDeleted), "item deleted");
        assert_eq!(
            format!("{}", CacheError::KeyTooLong),
            "key too long (max 255 bytes)"
        );
        assert_eq!(
            format!("{}", CacheError::ValueTooLong),
            "value too long (max 16MB)"
        );
        assert_eq!(
            format!("{}", CacheError::OptionalTooLong),
            "optional data too long (max 64 bytes)"
        );
        assert_eq!(format!("{}", CacheError::InvalidTtl), "invalid TTL value");
        assert_eq!(format!("{}", CacheError::Expired), "expired");
        assert_eq!(format!("{}", CacheError::InvalidOffset), "invalid offset");
        assert_eq!(format!("{}", CacheError::Corrupted), "data corrupted");
        assert_eq!(format!("{}", CacheError::KeyMismatch), "key mismatch");
        assert_eq!(
            format!("{}", CacheError::SegmentNotAccessible),
            "segment not accessible"
        );
    }

    #[test]
    fn test_error_is_error_trait() {
        fn assert_error<E: std::error::Error>() {}
        assert_error::<CacheError>();
    }

    #[test]
    fn test_error_debug() {
        // Verify Debug trait is implemented and works
        let err = CacheError::OutOfMemory;
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("OutOfMemory"));
    }

    #[test]
    fn test_error_clone() {
        let err1 = CacheError::KeyNotFound;
        let err2 = err1;
        assert_eq!(err1, err2);
    }

    #[test]
    fn test_error_equality() {
        assert_eq!(CacheError::OutOfMemory, CacheError::OutOfMemory);
        assert_ne!(CacheError::OutOfMemory, CacheError::KeyNotFound);
    }

    #[test]
    fn test_cache_result_ok() {
        let result: CacheResult<i32> = Ok(42);
        assert!(result.is_ok());
        assert!(matches!(result, Ok(42)));
    }

    #[test]
    fn test_cache_result_err() {
        let result: CacheResult<i32> = Err(CacheError::KeyNotFound);
        assert!(result.is_err());
        assert!(matches!(result, Err(CacheError::KeyNotFound)));
    }
}
