//! Owned buffer types that track buffer provenance.
//!
//! This module provides buffer types that remember where they came from,
//! enabling proper buffer return and zero-copy optimizations.

use std::ops::{Deref, DerefMut};

/// A buffer with tracked ownership, enabling zero-copy optimizations.
///
/// This enum tracks where a buffer came from, which is critical for:
/// - Returning buffers to the correct pool/ring
/// - Ensuring proper lifetime management for zero-copy operations
/// - Avoiding copies when the buffer can be reused
///
/// # Variants
///
/// - `User`: Standard user-allocated buffer (owned Vec)
/// - `Pooled`: Buffer from a [`BufferPool`] - must be returned via pool ID
/// - `Provided`: Buffer from io_uring provided ring - kernel-selected
/// - `Registered`: Reference to pre-registered fixed buffer (zero-copy send)
///
/// # Example
///
/// ```ignore
/// // Create from a user Vec
/// let buf = OwnedBuffer::user(vec![1, 2, 3]);
/// assert_eq!(buf.len(), 3);
///
/// // Create from a buffer pool
/// let buf = OwnedBuffer::pooled(0, 42, vec![0; 1024]);
/// assert_eq!(buf.pool_id(), Some(0));
/// assert_eq!(buf.buffer_idx(), Some(42));
/// ```
#[derive(Debug)]
pub enum OwnedBuffer {
    /// User-allocated buffer (owned Vec).
    /// Used for mio fallback or when explicit allocation is needed.
    User(Vec<u8>),

    /// Buffer from a BufferPool (singleshot recv on both backends).
    /// Contains pool ID and buffer index for return.
    Pooled {
        pool_id: u16,
        buffer_idx: usize,
        data: Vec<u8>,
    },

    /// Buffer from io_uring provided buffer ring (multishot recv).
    /// The kernel selected this buffer; must be returned to the ring.
    Provided {
        ring_id: u16,
        buffer_id: u16,
        data: Vec<u8>,
    },

    /// Registered fixed buffer for zero-copy send.
    /// Points into pre-registered memory; does not own the data.
    Registered {
        index: u16,
        offset: usize,
        len: usize,
    },
}

impl OwnedBuffer {
    /// Create a new user-owned buffer.
    #[inline]
    pub fn user(data: Vec<u8>) -> Self {
        Self::User(data)
    }

    /// Create a new pooled buffer.
    #[inline]
    pub fn pooled(pool_id: u16, buffer_idx: usize, data: Vec<u8>) -> Self {
        Self::Pooled {
            pool_id,
            buffer_idx,
            data,
        }
    }

    /// Create a new provided buffer (from io_uring buffer ring).
    #[inline]
    pub fn provided(ring_id: u16, buffer_id: u16, data: Vec<u8>) -> Self {
        Self::Provided {
            ring_id,
            buffer_id,
            data,
        }
    }

    /// Create a registered buffer reference.
    #[inline]
    pub fn registered(index: u16, offset: usize, len: usize) -> Self {
        Self::Registered { index, offset, len }
    }

    /// Returns the length of valid data in the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::User(v) => v.len(),
            Self::Pooled { data, .. } => data.len(),
            Self::Provided { data, .. } => data.len(),
            Self::Registered { len, .. } => *len,
        }
    }

    /// Returns true if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the pool ID if this is a pooled buffer.
    #[inline]
    pub fn pool_id(&self) -> Option<u16> {
        match self {
            Self::Pooled { pool_id, .. } => Some(*pool_id),
            _ => None,
        }
    }

    /// Returns the buffer index if this is a pooled buffer.
    #[inline]
    pub fn buffer_idx(&self) -> Option<usize> {
        match self {
            Self::Pooled { buffer_idx, .. } => Some(*buffer_idx),
            _ => None,
        }
    }

    /// Returns the ring ID if this is a provided buffer.
    #[inline]
    pub fn ring_id(&self) -> Option<u16> {
        match self {
            Self::Provided { ring_id, .. } => Some(*ring_id),
            _ => None,
        }
    }

    /// Returns the buffer ID if this is a provided buffer.
    #[inline]
    pub fn buffer_id(&self) -> Option<u16> {
        match self {
            Self::Provided { buffer_id, .. } => Some(*buffer_id),
            _ => None,
        }
    }

    /// Returns the registered buffer index if this is a registered buffer.
    #[inline]
    pub fn registered_index(&self) -> Option<u16> {
        match self {
            Self::Registered { index, .. } => Some(*index),
            _ => None,
        }
    }

    /// Consume and return the inner data as a Vec.
    /// Returns None for Registered buffers (they don't own data).
    #[inline]
    pub fn into_vec(self) -> Option<Vec<u8>> {
        match self {
            Self::User(v) => Some(v),
            Self::Pooled { data, .. } => Some(data),
            Self::Provided { data, .. } => Some(data),
            Self::Registered { .. } => None,
        }
    }

    /// Get a slice of the buffer data.
    /// Returns None for Registered buffers.
    #[inline]
    pub fn as_slice(&self) -> Option<&[u8]> {
        match self {
            Self::User(v) => Some(v.as_slice()),
            Self::Pooled { data, .. } => Some(data.as_slice()),
            Self::Provided { data, .. } => Some(data.as_slice()),
            Self::Registered { .. } => None,
        }
    }

    /// Get a mutable slice of the buffer data.
    /// Returns None for Registered buffers.
    #[inline]
    pub fn as_mut_slice(&mut self) -> Option<&mut [u8]> {
        match self {
            Self::User(v) => Some(v.as_mut_slice()),
            Self::Pooled { data, .. } => Some(data.as_mut_slice()),
            Self::Provided { data, .. } => Some(data.as_mut_slice()),
            Self::Registered { .. } => None,
        }
    }

    /// Check if this buffer is user-owned.
    #[inline]
    pub fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }

    /// Check if this buffer is from a pool.
    #[inline]
    pub fn is_pooled(&self) -> bool {
        matches!(self, Self::Pooled { .. })
    }

    /// Check if this buffer is from a provided ring.
    #[inline]
    pub fn is_provided(&self) -> bool {
        matches!(self, Self::Provided { .. })
    }

    /// Check if this buffer is a registered buffer reference.
    #[inline]
    pub fn is_registered(&self) -> bool {
        matches!(self, Self::Registered { .. })
    }
}

impl Deref for OwnedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice().unwrap_or(&[])
    }
}

impl DerefMut for OwnedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // For Registered buffers, return empty slice
        match self {
            Self::User(v) => v.as_mut_slice(),
            Self::Pooled { data, .. } => data.as_mut_slice(),
            Self::Provided { data, .. } => data.as_mut_slice(),
            Self::Registered { .. } => &mut [],
        }
    }
}

impl From<Vec<u8>> for OwnedBuffer {
    fn from(v: Vec<u8>) -> Self {
        Self::User(v)
    }
}

impl AsRef<[u8]> for OwnedBuffer {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl AsMut<[u8]> for OwnedBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.deref_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_buffer() {
        let buf = OwnedBuffer::user(vec![1, 2, 3, 4, 5]);
        assert_eq!(buf.len(), 5);
        assert!(!buf.is_empty());
        assert!(buf.is_user());
        assert!(!buf.is_pooled());
        assert_eq!(buf.pool_id(), None);
        assert_eq!(&*buf, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_pooled_buffer() {
        let buf = OwnedBuffer::pooled(3, 42, vec![0; 1024]);
        assert_eq!(buf.len(), 1024);
        assert!(buf.is_pooled());
        assert!(!buf.is_user());
        assert_eq!(buf.pool_id(), Some(3));
        assert_eq!(buf.buffer_idx(), Some(42));
        assert_eq!(buf.ring_id(), None);
    }

    #[test]
    fn test_provided_buffer() {
        let buf = OwnedBuffer::provided(1, 99, vec![1, 2, 3]);
        assert_eq!(buf.len(), 3);
        assert!(buf.is_provided());
        assert_eq!(buf.ring_id(), Some(1));
        assert_eq!(buf.buffer_id(), Some(99));
        assert_eq!(buf.pool_id(), None);
    }

    #[test]
    fn test_registered_buffer() {
        let buf = OwnedBuffer::registered(5, 100, 256);
        assert_eq!(buf.len(), 256);
        assert!(buf.is_registered());
        assert_eq!(buf.registered_index(), Some(5));
        assert_eq!(buf.as_slice(), None);
        assert_eq!(buf.into_vec(), None);
    }

    #[test]
    fn test_from_vec() {
        let v = vec![10, 20, 30];
        let buf: OwnedBuffer = v.into();
        assert!(buf.is_user());
        assert_eq!(buf.len(), 3);
    }

    #[test]
    fn test_into_vec() {
        let buf = OwnedBuffer::user(vec![1, 2, 3]);
        let v = buf.into_vec().unwrap();
        assert_eq!(v, vec![1, 2, 3]);

        let buf = OwnedBuffer::pooled(0, 0, vec![4, 5, 6]);
        let v = buf.into_vec().unwrap();
        assert_eq!(v, vec![4, 5, 6]);
    }

    #[test]
    fn test_deref() {
        let buf = OwnedBuffer::user(vec![1, 2, 3]);
        assert_eq!(&*buf, &[1, 2, 3]);
        assert_eq!(buf.as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn test_deref_mut() {
        let mut buf = OwnedBuffer::user(vec![1, 2, 3]);
        buf[0] = 10;
        assert_eq!(&*buf, &[10, 2, 3]);
    }

    #[test]
    fn test_empty_buffer() {
        let buf = OwnedBuffer::user(vec![]);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
    }
}
