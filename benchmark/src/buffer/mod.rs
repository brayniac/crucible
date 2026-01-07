use std::io::{self, Read, Write};

/// Default buffer size: 64KB
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Maximum buffer size: 4MB (prevents unbounded growth)
const MAX_BUFFER_SIZE: usize = 4 * 1024 * 1024;

/// A growable, reusable buffer for network I/O.
///
/// This buffer is designed for efficient network I/O:
/// - Starts with initial capacity, grows as needed up to MAX_BUFFER_SIZE
/// - Tracks read/write positions separately
/// - Supports compact operation to reclaim consumed space
/// - Shrinks back to initial capacity when empty and oversized
#[derive(Debug)]
pub struct Buffer {
    data: Vec<u8>,
    /// Initial capacity (used for shrinking)
    initial_capacity: usize,
    /// Read position: data before this has been consumed
    read_pos: usize,
    /// Write position: data has been written up to here
    write_pos: usize,
}

impl Buffer {
    /// Create a new buffer with the default size (64KB).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_BUFFER_SIZE)
    }

    /// Create a new buffer with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: vec![0u8; capacity],
            initial_capacity: capacity,
            read_pos: 0,
            write_pos: 0,
        }
    }

    /// Returns the total capacity of the buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Ensures the buffer has at least `additional` bytes of writable space.
    /// Grows the buffer if necessary, up to MAX_BUFFER_SIZE.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        let needed = self.write_pos + additional;
        if needed <= self.data.len() {
            return;
        }

        // First try compacting
        if self.read_pos > 0 {
            self.compact();
            if self.writable() >= additional {
                return;
            }
        }

        // Need to grow
        let new_size = (needed.next_power_of_two()).min(MAX_BUFFER_SIZE);
        if new_size > self.data.len() {
            self.data.resize(new_size, 0);
        }
    }

    /// Returns the number of bytes available to read.
    #[inline]
    pub fn readable(&self) -> usize {
        self.write_pos - self.read_pos
    }

    /// Returns the number of bytes available to write.
    #[inline]
    pub fn writable(&self) -> usize {
        self.data.len() - self.write_pos
    }

    /// Returns true if there is no data to read.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.read_pos == self.write_pos
    }

    /// Returns a slice of the readable data.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data[self.read_pos..self.write_pos]
    }

    /// Returns a mutable slice of the writable space.
    #[inline]
    pub fn spare_mut(&mut self) -> &mut [u8] {
        &mut self.data[self.write_pos..]
    }

    /// Advances the read position, consuming data.
    ///
    /// # Panics
    /// Panics if `n` exceeds the readable bytes.
    #[inline]
    pub fn consume(&mut self, n: usize) {
        assert!(n <= self.readable(), "consume exceeds readable bytes");
        self.read_pos += n;

        // If we've consumed everything, reset to start and maybe shrink
        if self.read_pos == self.write_pos {
            self.read_pos = 0;
            self.write_pos = 0;

            // Shrink if we've grown beyond initial capacity
            if self.data.len() > self.initial_capacity * 2 {
                self.data.truncate(self.initial_capacity);
                self.data.shrink_to_fit();
                self.data.resize(self.initial_capacity, 0);
            }
        }
    }

    /// Advances the write position after writing data.
    ///
    /// # Panics
    /// Panics if `n` exceeds the writable space.
    #[inline]
    pub fn advance(&mut self, n: usize) {
        assert!(n <= self.writable(), "advance exceeds writable space");
        self.write_pos += n;
    }

    /// Compacts the buffer by moving unread data to the start.
    ///
    /// This reclaims space from consumed data without allocating.
    pub fn compact(&mut self) {
        if self.read_pos == 0 {
            return;
        }

        let readable = self.readable();
        if readable > 0 {
            self.data.copy_within(self.read_pos..self.write_pos, 0);
        }
        self.read_pos = 0;
        self.write_pos = readable;
    }

    /// Clears the buffer, resetting both positions.
    /// Shrinks the buffer if it has grown beyond initial capacity.
    #[inline]
    pub fn clear(&mut self) {
        self.read_pos = 0;
        self.write_pos = 0;

        // Shrink if we've grown beyond initial capacity
        if self.data.len() > self.initial_capacity * 2 {
            self.data.truncate(self.initial_capacity);
            self.data.shrink_to_fit();
            self.data.resize(self.initial_capacity, 0);
        }
    }

    /// Reads from the given reader into the buffer.
    ///
    /// Returns the number of bytes read, or an error.
    /// If the buffer is full, compacts first to make space.
    pub fn read_from<R: Read>(&mut self, reader: &mut R) -> io::Result<usize> {
        // Compact if we're running low on writable space
        if self.writable() < self.capacity() / 4 {
            self.compact();
        }

        if self.writable() == 0 {
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "buffer is full"));
        }

        let n = reader.read(self.spare_mut())?;
        self.advance(n);
        Ok(n)
    }

    /// Writes from the buffer to the given writer.
    ///
    /// Returns the number of bytes written, or an error.
    pub fn write_to<W: Write>(&mut self, writer: &mut W) -> io::Result<usize> {
        if self.is_empty() {
            return Ok(0);
        }

        let n = writer.write(self.as_slice())?;
        self.consume(n);
        Ok(n)
    }

    /// Extends the buffer with the given data.
    ///
    /// # Panics
    /// Panics if the data exceeds the writable space.
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        assert!(
            data.len() <= self.writable(),
            "extend_from_slice exceeds writable space"
        );
        self.spare_mut()[..data.len()].copy_from_slice(data);
        self.advance(data.len());
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new()
    }
}

/// A pair of buffers for bidirectional I/O.
#[derive(Debug)]
pub struct BufferPair {
    pub send: Buffer,
    pub recv: Buffer,
}

impl BufferPair {
    pub fn new() -> Self {
        Self {
            send: Buffer::new(),
            recv: Buffer::new(),
        }
    }

    pub fn with_capacity(send_cap: usize, recv_cap: usize) -> Self {
        Self {
            send: Buffer::with_capacity(send_cap),
            recv: Buffer::with_capacity(recv_cap),
        }
    }
}

impl Default for BufferPair {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_basic() {
        let mut buf = Buffer::with_capacity(1024);

        assert_eq!(buf.capacity(), 1024);
        assert_eq!(buf.readable(), 0);
        assert_eq!(buf.writable(), 1024);
        assert!(buf.is_empty());

        buf.extend_from_slice(b"hello");
        assert_eq!(buf.readable(), 5);
        assert_eq!(buf.as_slice(), b"hello");

        buf.consume(2);
        assert_eq!(buf.readable(), 3);
        assert_eq!(buf.as_slice(), b"llo");
    }

    #[test]
    fn test_buffer_compact() {
        let mut buf = Buffer::with_capacity(16);

        buf.extend_from_slice(b"hello world!");
        assert_eq!(buf.writable(), 4);

        buf.consume(6);
        assert_eq!(buf.as_slice(), b"world!");

        buf.compact();
        assert_eq!(buf.as_slice(), b"world!");
        assert_eq!(buf.writable(), 10);
    }

    #[test]
    fn test_buffer_consume_all_resets() {
        let mut buf = Buffer::with_capacity(1024);

        buf.extend_from_slice(b"test");
        buf.consume(4);

        // Should auto-reset when all data consumed
        assert_eq!(buf.writable(), 1024);
    }

    #[test]
    fn test_buffer_grow() {
        let mut buf = Buffer::with_capacity(64);

        // Fill the buffer
        let data = vec![b'x'; 64];
        buf.extend_from_slice(&data);
        assert_eq!(buf.writable(), 0);

        // Reserve more space - should grow
        buf.reserve(128);
        assert!(buf.capacity() >= 64 + 128);
        assert!(buf.writable() >= 128);
    }

    #[test]
    fn test_buffer_shrink_on_consume() {
        let mut buf = Buffer::with_capacity(64);

        // Grow the buffer significantly
        buf.reserve(1024);
        let large_data = vec![b'x'; 1024];
        buf.extend_from_slice(&large_data);

        // Buffer should be large now
        assert!(buf.capacity() >= 1024);

        // Consume all data - should shrink back
        buf.consume(1024);
        assert_eq!(buf.capacity(), 64);
    }

    #[test]
    fn test_buffer_reserve_compacts_first() {
        let mut buf = Buffer::with_capacity(128);

        // Write some data
        buf.extend_from_slice(&[b'a'; 100]);
        // Consume most of it
        buf.consume(90);

        // Now we have 10 bytes of data and 28 bytes writable
        assert_eq!(buf.readable(), 10);
        assert_eq!(buf.writable(), 28);

        // Reserve 50 bytes - should compact (moving 10 bytes to front)
        // giving us 118 bytes writable without growing
        buf.reserve(50);
        assert_eq!(buf.capacity(), 128); // Didn't grow
        assert_eq!(buf.readable(), 10);
        assert!(buf.writable() >= 50);
    }
}
