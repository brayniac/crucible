//! ListCache trait for Redis-like list operations.
//!
//! Provides a trait interface for list data structure operations.
//! Lists are ordered collections of elements that support push/pop from both ends.

use crate::error::CacheResult;
use std::time::Duration;

/// Trait for caches supporting list data structures.
///
/// Lists are ordered collections that support push/pop from both ends.
/// They can be used as stacks or queues.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow concurrent access.
pub trait ListCache: Send + Sync {
    /// Push elements to the left (head) of a list.
    ///
    /// Creates the list if it doesn't exist.
    /// Returns the length of the list after the push.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a list.
    fn lpush(&self, key: &[u8], values: &[&[u8]], ttl: Option<Duration>) -> CacheResult<usize>;

    /// Push elements to the right (tail) of a list.
    ///
    /// Creates the list if it doesn't exist.
    /// Returns the length of the list after the push.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a list.
    fn rpush(&self, key: &[u8], values: &[&[u8]], ttl: Option<Duration>) -> CacheResult<usize>;

    /// Pop and return the first element from the left (head) of a list.
    ///
    /// Returns `None` if the list is empty or doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a list.
    fn lpop(&self, key: &[u8]) -> CacheResult<Option<Vec<u8>>>;

    /// Pop and return the first element from the right (tail) of a list.
    ///
    /// Returns `None` if the list is empty or doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a list.
    fn rpop(&self, key: &[u8]) -> CacheResult<Option<Vec<u8>>>;

    /// Pop multiple elements from the left (head) of a list.
    ///
    /// Returns up to `count` elements.
    /// Returns empty vector if the list is empty or doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a list.
    fn lpop_count(&self, key: &[u8], count: usize) -> CacheResult<Vec<Vec<u8>>>;

    /// Pop multiple elements from the right (tail) of a list.
    ///
    /// Returns up to `count` elements.
    /// Returns empty vector if the list is empty or doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a list.
    fn rpop_count(&self, key: &[u8], count: usize) -> CacheResult<Vec<Vec<u8>>>;

    /// Get a range of elements from a list.
    ///
    /// Indices are 0-based. Negative indices count from the end (-1 = last element).
    /// Returns empty vector if the key doesn't exist or range is out of bounds.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a list.
    fn lrange(&self, key: &[u8], start: i64, stop: i64) -> CacheResult<Vec<Vec<u8>>>;

    /// Get the length of a list.
    ///
    /// Returns 0 if the key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a list.
    fn llen(&self, key: &[u8]) -> CacheResult<usize>;

    /// Get an element at a specific index.
    ///
    /// Indices are 0-based. Negative indices count from the end (-1 = last element).
    /// Returns `None` if the index is out of range or key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a list.
    fn lindex(&self, key: &[u8], index: i64) -> CacheResult<Option<Vec<u8>>>;

    /// Set the value at a specific index.
    ///
    /// Indices are 0-based. Negative indices count from the end.
    ///
    /// # Errors
    ///
    /// - `CacheError::WrongType` if the key exists but is not a list.
    /// - `CacheError::KeyNotFound` if the key doesn't exist.
    /// - `CacheError::InvalidOffset` if the index is out of range.
    fn lset(&self, key: &[u8], index: i64, value: &[u8]) -> CacheResult<()>;

    /// Trim a list to the specified range.
    ///
    /// Removes elements outside the range. Indices are 0-based.
    /// Negative indices count from the end.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a list.
    fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> CacheResult<()>;

    /// Push to the left only if the key already exists.
    ///
    /// Returns the length after push, or 0 if the key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a list.
    fn lpushx(&self, key: &[u8], values: &[&[u8]]) -> CacheResult<usize>;

    /// Push to the right only if the key already exists.
    ///
    /// Returns the length after push, or 0 if the key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a list.
    fn rpushx(&self, key: &[u8], values: &[&[u8]]) -> CacheResult<usize>;
}
