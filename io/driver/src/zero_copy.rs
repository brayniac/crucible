//! Zero-copy send traits and types.
//!
//! This module provides the trait for zero-copy sends where the driver
//! holds ownership of the buffer until the send completes.

use smallbox::{SmallBox, space::S8};
use std::io::IoSlice;

/// Trait for buffers that can be sent with zero-copy semantics.
///
/// Implementors provide access to data slices that the driver will send
/// using vectored IO. The driver takes ownership of the implementor and
/// holds it until the send completes, then drops it.
///
/// This enables zero-copy sends where the data (e.g., cache values) is
/// sent directly from its source without intermediate copies.
///
/// # Example
///
/// ```ignore
/// use io_driver::ZeroCopySend;
/// use std::io::IoSlice;
///
/// struct MyResponse {
///     header: Vec<u8>,
///     body: Vec<u8>,
/// }
///
/// impl ZeroCopySend for MyResponse {
///     fn io_slices(&self) -> Vec<IoSlice<'_>> {
///         vec![
///             IoSlice::new(&self.header),
///             IoSlice::new(&self.body),
///         ]
///     }
///
///     fn total_len(&self) -> usize {
///         self.header.len() + self.body.len()
///     }
/// }
/// ```
///
/// # Protocol Response Example
///
/// ```ignore
/// // A Redis RESP GET response that holds an ItemRef
/// struct RespGetResponse {
///     header: ArrayVec<u8, 32>,  // "$<len>\r\n"
///     item: Option<ItemRef>,     // The cached value
/// }
///
/// impl ZeroCopySend for RespGetResponse {
///     fn io_slices(&self) -> Vec<IoSlice<'_>> {
///         match &self.item {
///             Some(item) => vec![
///                 IoSlice::new(&self.header),
///                 IoSlice::new(item.value()),
///                 IoSlice::new(b"\r\n"),
///             ],
///             None => vec![
///                 IoSlice::new(b"$-1\r\n"),
///             ],
///         }
///     }
///
///     fn total_len(&self) -> usize {
///         // ...
///     }
/// }
/// ```
pub trait ZeroCopySend: Send + Sync + 'static {
    /// Get the IO slices for vectored send.
    ///
    /// Returns a vector of slices that will be sent in order.
    /// The slices must remain valid for the lifetime of `self`.
    fn io_slices(&self) -> Vec<IoSlice<'_>>;

    /// Total bytes across all slices.
    ///
    /// Used for buffer pool decisions and progress tracking.
    fn total_len(&self) -> usize;

    /// Number of slices.
    ///
    /// Default implementation calls `io_slices().len()` but implementors
    /// can override for efficiency if the count is known without building slices.
    fn slice_count(&self) -> usize {
        self.io_slices().len()
    }
}

/// A simple owned buffer that implements ZeroCopySend.
///
/// Useful for cases where you just have bytes to send without
/// complex multi-slice serialization.
#[derive(Debug)]
pub struct OwnedBytes {
    data: Vec<u8>,
}

impl OwnedBytes {
    /// Create from a Vec.
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Create from a slice (copies the data).
    pub fn from_slice(data: &[u8]) -> Self {
        Self {
            data: data.to_vec(),
        }
    }
}

impl ZeroCopySend for OwnedBytes {
    fn io_slices(&self) -> Vec<IoSlice<'_>> {
        vec![IoSlice::new(&self.data)]
    }

    fn total_len(&self) -> usize {
        self.data.len()
    }

    fn slice_count(&self) -> usize {
        1
    }
}

impl From<Vec<u8>> for OwnedBytes {
    fn from(data: Vec<u8>) -> Self {
        Self::new(data)
    }
}

impl From<&[u8]> for OwnedBytes {
    fn from(data: &[u8]) -> Self {
        Self::from_slice(data)
    }
}

/// Small-box optimized zero-copy send buffer.
///
/// Uses inline storage (64 bytes) for small response types to avoid
/// heap allocation on the hot path. Falls back to heap for larger types.
///
/// This is the type stored by the driver for pending zero-copy sends.
pub type BoxedZeroCopy = SmallBox<dyn ZeroCopySend, S8>;

/// Create a `BoxedZeroCopy` from a value implementing `ZeroCopySend`.
///
/// This macro uses small-box optimization to store small types inline,
/// avoiding heap allocation for typical response types.
///
/// Re-export of `smallbox::smallbox!` for convenience.
///
/// # Example
///
/// ```ignore
/// use io_driver::{boxed_zc, OwnedBytes};
///
/// let zc = boxed_zc!(OwnedBytes::new(vec![1, 2, 3]));
/// ```
pub use smallbox::smallbox as boxed_zc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_owned_bytes() {
        let buf = OwnedBytes::new(vec![1, 2, 3, 4, 5]);
        assert_eq!(buf.total_len(), 5);
        assert_eq!(buf.slice_count(), 1);

        let slices = buf.io_slices();
        assert_eq!(slices.len(), 1);
        assert_eq!(&*slices[0], &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_owned_bytes_from_slice() {
        let buf = OwnedBytes::from_slice(b"hello");
        assert_eq!(buf.total_len(), 5);
    }

    #[test]
    fn test_owned_bytes_from_vec() {
        let buf: OwnedBytes = vec![1, 2, 3].into();
        assert_eq!(buf.total_len(), 3);
    }

    struct MultiSliceResponse {
        header: Vec<u8>,
        body: Vec<u8>,
        trailer: &'static [u8],
    }

    impl ZeroCopySend for MultiSliceResponse {
        fn io_slices(&self) -> Vec<IoSlice<'_>> {
            vec![
                IoSlice::new(&self.header),
                IoSlice::new(&self.body),
                IoSlice::new(self.trailer),
            ]
        }

        fn total_len(&self) -> usize {
            self.header.len() + self.body.len() + self.trailer.len()
        }

        fn slice_count(&self) -> usize {
            3
        }
    }

    #[test]
    fn test_multi_slice() {
        let resp = MultiSliceResponse {
            header: b"$5\r\n".to_vec(),
            body: b"hello".to_vec(),
            trailer: b"\r\n",
        };

        assert_eq!(resp.total_len(), 4 + 5 + 2);
        assert_eq!(resp.slice_count(), 3);

        let slices = resp.io_slices();
        assert_eq!(&*slices[0], b"$5\r\n");
        assert_eq!(&*slices[1], b"hello");
        assert_eq!(&*slices[2], b"\r\n");
    }

    #[test]
    fn test_boxed_zero_copy() {
        let buf: BoxedZeroCopy = smallbox::smallbox!(OwnedBytes::new(vec![1, 2, 3]));
        assert_eq!(buf.total_len(), 3);
    }
}
