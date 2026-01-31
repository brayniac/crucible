//! SetCache trait for Redis-like set operations.
//!
//! Provides a trait interface for set data structure operations.
//! Sets are unordered collections of unique members.

use crate::error::CacheResult;
use std::time::Duration;

/// Trait for caches supporting set data structures.
///
/// Sets are unordered collections of unique members.
/// Adding a member that already exists has no effect.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow concurrent access.
pub trait SetCache: Send + Sync {
    /// Add one or more members to a set.
    ///
    /// Creates the set if it doesn't exist.
    /// Returns the number of new members added (ignores duplicates).
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a set.
    fn sadd(&self, key: &[u8], members: &[&[u8]], ttl: Option<Duration>) -> CacheResult<usize>;

    /// Remove one or more members from a set.
    ///
    /// Returns the number of members that were removed.
    /// Non-existent members are ignored.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a set.
    fn srem(&self, key: &[u8], members: &[&[u8]]) -> CacheResult<usize>;

    /// Get all members of a set.
    ///
    /// Returns empty vector if the key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a set.
    fn smembers(&self, key: &[u8]) -> CacheResult<Vec<Vec<u8>>>;

    /// Check if a member exists in a set.
    ///
    /// Returns `false` if the key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a set.
    fn sismember(&self, key: &[u8], member: &[u8]) -> CacheResult<bool>;

    /// Check if multiple members exist in a set.
    ///
    /// Returns a vector of booleans, one for each member.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a set.
    fn smismember(&self, key: &[u8], members: &[&[u8]]) -> CacheResult<Vec<bool>>;

    /// Get the number of members in a set.
    ///
    /// Returns 0 if the key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a set.
    fn scard(&self, key: &[u8]) -> CacheResult<usize>;

    /// Remove and return a random member from a set.
    ///
    /// Returns `None` if the set is empty or doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a set.
    fn spop(&self, key: &[u8]) -> CacheResult<Option<Vec<u8>>>;

    /// Remove and return multiple random members from a set.
    ///
    /// Returns up to `count` members.
    /// Returns empty vector if the set is empty or doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a set.
    fn spop_count(&self, key: &[u8], count: usize) -> CacheResult<Vec<Vec<u8>>>;

    /// Get a random member from a set without removing it.
    ///
    /// Returns `None` if the set is empty or doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a set.
    fn srandmember(&self, key: &[u8]) -> CacheResult<Option<Vec<u8>>>;

    /// Get multiple random members from a set without removing them.
    ///
    /// If count is positive, returns up to count distinct members.
    /// If count is negative, returns abs(count) members (may have duplicates).
    ///
    /// # Errors
    ///
    /// Returns `CacheError::WrongType` if the key exists but is not a set.
    fn srandmember_count(&self, key: &[u8], count: i64) -> CacheResult<Vec<Vec<u8>>>;
}
