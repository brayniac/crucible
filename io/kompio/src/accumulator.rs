/// Per-connection byte accumulator for contiguous recv data.
///
/// Handlers always see a contiguous `&[u8]` and return the number of bytes consumed.
/// Unconsumed bytes are shifted to the front for the next callback.
pub struct RecvAccumulator {
    buf: Vec<u8>,
    len: usize,
}

impl RecvAccumulator {
    /// Create a new accumulator with the given initial capacity.
    pub fn new(capacity: usize) -> Self {
        RecvAccumulator {
            buf: vec![0u8; capacity],
            len: 0,
        }
    }

    /// Append received bytes. Grows the buffer if necessary.
    pub fn append(&mut self, data: &[u8]) {
        let needed = self.len + data.len();
        if needed > self.buf.len() {
            self.buf.resize(needed.next_power_of_two(), 0);
        }
        self.buf[self.len..self.len + data.len()].copy_from_slice(data);
        self.len += data.len();
    }

    /// Get a reference to the accumulated data.
    pub fn data(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    /// Consume `n` bytes from the front, shifting remaining bytes.
    pub fn consume(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        let n = n.min(self.len);
        if n < self.len {
            self.buf.copy_within(n..self.len, 0);
        }
        self.len -= n;
    }

    /// Reset the accumulator (discard all data).
    pub fn reset(&mut self) {
        self.len = 0;
    }
}

/// Parallel `Vec<RecvAccumulator>` indexed by connection index.
/// Stored as a separate field in EventLoop for borrow splitting.
pub struct AccumulatorTable {
    accumulators: Vec<RecvAccumulator>,
}

impl AccumulatorTable {
    /// Create a table with `count` accumulators, each with the given capacity.
    pub fn new(count: u32, capacity: usize) -> Self {
        let mut accumulators = Vec::with_capacity(count as usize);
        for _ in 0..count {
            accumulators.push(RecvAccumulator::new(capacity));
        }
        AccumulatorTable { accumulators }
    }

    /// Append data to the accumulator at the given index.
    pub fn append(&mut self, index: u32, data: &[u8]) {
        self.accumulators[index as usize].append(data);
    }

    /// Get accumulated data at the given index.
    pub fn data(&self, index: u32) -> &[u8] {
        self.accumulators[index as usize].data()
    }

    /// Consume `n` bytes from the accumulator at the given index.
    pub fn consume(&mut self, index: u32, n: usize) {
        self.accumulators[index as usize].consume(n);
    }

    /// Reset the accumulator at the given index.
    pub fn reset(&mut self, index: u32) {
        self.accumulators[index as usize].reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_consume() {
        let mut acc = RecvAccumulator::new(64);
        acc.append(b"hello ");
        acc.append(b"world");
        assert_eq!(acc.data(), b"hello world");
        acc.consume(6);
        assert_eq!(acc.data(), b"world");
        acc.consume(5);
        assert_eq!(acc.data(), b"");
    }

    #[test]
    fn grow_on_overflow() {
        let mut acc = RecvAccumulator::new(4);
        acc.append(b"abcdef"); // exceeds initial capacity
        assert_eq!(acc.data(), b"abcdef");
    }

    #[test]
    fn reset_clears() {
        let mut acc = RecvAccumulator::new(16);
        acc.append(b"data");
        acc.reset();
        assert_eq!(acc.data(), b"");
    }

    #[test]
    fn table_operations() {
        let mut table = AccumulatorTable::new(4, 64);
        table.append(2, b"hello");
        assert_eq!(table.data(2), b"hello");
        table.consume(2, 3);
        assert_eq!(table.data(2), b"lo");
        table.reset(2);
        assert_eq!(table.data(2), b"");
    }
}
