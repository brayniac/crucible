//! HashCache trait for Redis-like hash operations.
//!
//! Provides a trait interface for hash data structure operations.
//! Hash values are field-value maps stored under a single key.

use crate::error::CacheResult;
use std::time::Duration;

/// Trait for caches supporting hash data structures.
///
/// Hashes are maps of field -> value pairs stored under a single key.
/// All operations are atomic at the individual operation level.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow concurrent access.
pub trait HashCache: Send + Sync {
    /// Set a field in a hash.
    ///
    /// Creates the hash if it doesn't exist.
    /// Returns the number of new fields created (0 if field was updated, 1 if created).
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a hash.
    fn hset(
        &self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> CacheResult<usize>;

    /// Set multiple fields in a hash atomically.
    ///
    /// Creates the hash if it doesn't exist.
    /// Returns the number of new fields created.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a hash.
    fn hmset(
        &self,
        key: &[u8],
        fields: &[(&[u8], &[u8])],
        ttl: Option<Duration>,
    ) -> CacheResult<usize>;

    /// Get a field from a hash.
    ///
    /// Returns `None` if the key or field doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a hash.
    fn hget(&self, key: &[u8], field: &[u8]) -> CacheResult<Option<Vec<u8>>>;

    /// Get multiple fields from a hash.
    ///
    /// Returns a vector of optional values, one for each requested field.
    /// Missing fields return `None` in their position.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a hash.
    fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> CacheResult<Vec<Option<Vec<u8>>>>;

    /// Get all fields and values from a hash.
    ///
    /// Returns a vector of (field, value) pairs.
    /// Returns empty vector if key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a hash.
    fn hgetall(&self, key: &[u8]) -> CacheResult<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Delete one or more fields from a hash.
    ///
    /// Returns the number of fields that were removed.
    /// Non-existent fields are ignored.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a hash.
    fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> CacheResult<usize>;

    /// Check if a field exists in a hash.
    ///
    /// Returns `false` if the key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a hash.
    fn hexists(&self, key: &[u8], field: &[u8]) -> CacheResult<bool>;

    /// Get the number of fields in a hash.
    ///
    /// Returns 0 if the key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a hash.
    fn hlen(&self, key: &[u8]) -> CacheResult<usize>;

    /// Get all field names in a hash.
    ///
    /// Returns empty vector if key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a hash.
    fn hkeys(&self, key: &[u8]) -> CacheResult<Vec<Vec<u8>>>;

    /// Get all values in a hash.
    ///
    /// Returns empty vector if key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a hash.
    fn hvals(&self, key: &[u8]) -> CacheResult<Vec<Vec<u8>>>;

    /// Set a field only if it doesn't exist.
    ///
    /// Returns true if the field was set, false if it already existed.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a hash.
    fn hsetnx(
        &self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> CacheResult<bool>;

    /// Increment a hash field's numeric value.
    ///
    /// Creates the field with value 0 if it doesn't exist.
    /// Returns the new value after incrementing.
    ///
    /// # Errors
    ///
    /// - `CacheError::WrongType` if the key exists but is not a hash.
    /// - `CacheError::NotNumeric` if the field value is not numeric.
    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> CacheResult<i64>;
}
