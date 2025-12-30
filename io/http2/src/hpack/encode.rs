//! HPACK header encoding.

use super::huffman;
use super::table::{DynamicTable, HeaderField, StaticTable};

/// HPACK encoder.
pub struct HpackEncoder {
    /// Dynamic table for encoding.
    dynamic_table: DynamicTable,
    /// Whether to use Huffman encoding for strings.
    use_huffman: bool,
}

impl Default for HpackEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl HpackEncoder {
    /// Create a new HPACK encoder with default settings.
    pub fn new() -> Self {
        Self {
            dynamic_table: DynamicTable::new(super::DEFAULT_TABLE_SIZE),
            use_huffman: true,
        }
    }

    /// Create a new HPACK encoder with a specific table size.
    pub fn with_table_size(size: usize) -> Self {
        Self {
            dynamic_table: DynamicTable::new(size),
            use_huffman: true,
        }
    }

    /// Set whether to use Huffman encoding.
    pub fn set_huffman(&mut self, use_huffman: bool) {
        self.use_huffman = use_huffman;
    }

    /// Set the dynamic table size.
    pub fn set_table_size(&mut self, size: usize) {
        self.dynamic_table.set_max_size(size);
    }

    /// Encode a list of headers into an HPACK header block.
    pub fn encode(&mut self, headers: &[HeaderField], buf: &mut Vec<u8>) {
        for header in headers {
            self.encode_header(header, buf);
        }
    }

    /// Encode a single header field.
    fn encode_header(&mut self, header: &HeaderField, buf: &mut Vec<u8>) {
        // Try to find in static table first, then dynamic table
        let static_match = StaticTable::find(&header.name, &header.value);
        let dynamic_match = self.dynamic_table.find(&header.name, &header.value);

        // Determine best encoding strategy
        match (static_match, dynamic_match) {
            // Exact match in static table - use indexed representation
            (Some((idx, true)), _) => {
                self.encode_indexed(idx, buf);
            }
            // Exact match in dynamic table
            (_, Some((dyn_idx, true))) => {
                // Dynamic table index = static table size + 1 + dyn_idx
                let idx = StaticTable::len() + 1 + dyn_idx;
                self.encode_indexed(idx, buf);
            }
            // Name match in static table - use literal with indexing
            (Some((idx, false)), _) => {
                self.encode_literal_indexed(idx, &header.value, buf);
                self.dynamic_table.insert(header.clone());
            }
            // Name match in dynamic table
            (_, Some((dyn_idx, false))) => {
                let idx = StaticTable::len() + 1 + dyn_idx;
                self.encode_literal_indexed(idx, &header.value, buf);
                self.dynamic_table.insert(header.clone());
            }
            // No match - literal with new name
            (None, None) => {
                self.encode_literal_new(&header.name, &header.value, buf);
                self.dynamic_table.insert(header.clone());
            }
        }
    }

    /// Encode an indexed header field (Section 6.1).
    /// Format: 1xxxxxxx
    fn encode_indexed(&self, index: usize, buf: &mut Vec<u8>) {
        encode_integer(index, 7, 0x80, buf);
    }

    /// Encode a literal header field with incremental indexing (Section 6.2.1).
    /// Format: 01xxxxxx
    fn encode_literal_indexed(&self, name_index: usize, value: &[u8], buf: &mut Vec<u8>) {
        encode_integer(name_index, 6, 0x40, buf);
        self.encode_string(value, buf);
    }

    /// Encode a literal header field with new name (Section 6.2.1).
    fn encode_literal_new(&self, name: &[u8], value: &[u8], buf: &mut Vec<u8>) {
        buf.push(0x40); // Literal with incremental indexing, new name
        self.encode_string(name, buf);
        self.encode_string(value, buf);
    }

    /// Encode a string (with optional Huffman encoding).
    fn encode_string(&self, data: &[u8], buf: &mut Vec<u8>) {
        if self.use_huffman {
            let huffman_len = huffman::encoded_len(data);
            if huffman_len < data.len() {
                // Use Huffman encoding
                encode_integer(huffman_len, 7, 0x80, buf);
                huffman::encode(data, buf);
                return;
            }
        }

        // Plain encoding
        encode_integer(data.len(), 7, 0x00, buf);
        buf.extend_from_slice(data);
    }

    /// Encode a dynamic table size update.
    pub fn encode_table_size_update(&self, size: usize, buf: &mut Vec<u8>) {
        encode_integer(size, 5, 0x20, buf);
    }
}

/// Encode an integer with a prefix (RFC 7541 Section 5.1).
fn encode_integer(mut value: usize, prefix_bits: u8, prefix: u8, buf: &mut Vec<u8>) {
    let max_prefix: usize = (1 << prefix_bits) - 1;

    if value < max_prefix {
        buf.push(prefix | (value as u8));
    } else {
        buf.push(prefix | (max_prefix as u8));
        value -= max_prefix;
        while value >= 128 {
            buf.push((value % 128) as u8 | 0x80);
            value /= 128;
        }
        buf.push(value as u8);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_integer_small() {
        let mut buf = Vec::new();
        encode_integer(10, 5, 0x00, &mut buf);
        assert_eq!(buf, vec![10]);
    }

    #[test]
    fn test_encode_integer_max_prefix() {
        let mut buf = Vec::new();
        encode_integer(31, 5, 0x00, &mut buf);
        assert_eq!(buf, vec![31, 0]);
    }

    #[test]
    fn test_encode_integer_large() {
        let mut buf = Vec::new();
        // 1337 with 5-bit prefix (RFC 7541 example)
        encode_integer(1337, 5, 0x00, &mut buf);
        assert_eq!(buf, vec![31, 154, 10]);
    }

    #[test]
    fn test_encode_indexed() {
        let encoder = HpackEncoder::new();
        let mut buf = Vec::new();

        // Index 2 = :method: GET
        encoder.encode_indexed(2, &mut buf);
        assert_eq!(buf, vec![0x82]);
    }

    #[test]
    fn test_encode_headers() {
        let mut encoder = HpackEncoder::new();
        encoder.set_huffman(false); // Disable Huffman for predictable output

        let headers = vec![HeaderField::new(b":method".to_vec(), b"GET".to_vec())];

        let mut buf = Vec::new();
        encoder.encode(&headers, &mut buf);

        // Should be indexed (index 2)
        assert_eq!(buf, vec![0x82]);
    }
}
