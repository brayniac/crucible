//! HPACK header decoding.

use super::huffman;
use super::table::{DynamicTable, HeaderField, StaticTable};

/// HPACK decoding error.
#[derive(Debug)]
pub enum HpackError {
    /// Not enough data to decode.
    Incomplete,
    /// Invalid integer encoding.
    InvalidInteger,
    /// Invalid string encoding.
    InvalidString,
    /// Invalid Huffman encoding.
    InvalidHuffman(huffman::HuffmanError),
    /// Invalid table index.
    InvalidIndex(usize),
    /// Invalid table size update.
    InvalidTableSize,
}

impl std::fmt::Display for HpackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HpackError::Incomplete => write!(f, "incomplete HPACK data"),
            HpackError::InvalidInteger => write!(f, "invalid HPACK integer encoding"),
            HpackError::InvalidString => write!(f, "invalid HPACK string encoding"),
            HpackError::InvalidHuffman(e) => write!(f, "invalid Huffman encoding: {}", e),
            HpackError::InvalidIndex(idx) => write!(f, "invalid table index: {}", idx),
            HpackError::InvalidTableSize => write!(f, "invalid table size update"),
        }
    }
}

impl std::error::Error for HpackError {}

impl From<huffman::HuffmanError> for HpackError {
    fn from(e: huffman::HuffmanError) -> Self {
        HpackError::InvalidHuffman(e)
    }
}

/// HPACK decoder.
pub struct HpackDecoder {
    /// Dynamic table for decoding.
    dynamic_table: DynamicTable,
    /// Maximum allowed table size.
    max_table_size: usize,
}

impl Default for HpackDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl HpackDecoder {
    /// Create a new HPACK decoder with default settings.
    pub fn new() -> Self {
        Self {
            dynamic_table: DynamicTable::new(super::DEFAULT_TABLE_SIZE),
            max_table_size: super::DEFAULT_TABLE_SIZE,
        }
    }

    /// Create a new HPACK decoder with a specific table size.
    pub fn with_table_size(size: usize) -> Self {
        Self {
            dynamic_table: DynamicTable::new(size),
            max_table_size: size,
        }
    }

    /// Set the maximum allowed table size.
    pub fn set_max_table_size(&mut self, size: usize) {
        self.max_table_size = size;
    }

    /// Decode an HPACK header block into a list of headers.
    pub fn decode(&mut self, data: &[u8]) -> Result<Vec<HeaderField>, HpackError> {
        let mut headers = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            let (header, consumed) = self.decode_header(&data[pos..])?;
            if let Some(h) = header {
                headers.push(h);
            }
            pos += consumed;
        }

        Ok(headers)
    }

    /// Decode a single header representation.
    /// Returns (Option<HeaderField>, bytes_consumed).
    fn decode_header(&mut self, data: &[u8]) -> Result<(Option<HeaderField>, usize), HpackError> {
        if data.is_empty() {
            return Err(HpackError::Incomplete);
        }

        let first_byte = data[0];

        if first_byte & 0x80 != 0 {
            // Indexed Header Field (Section 6.1)
            // Format: 1xxxxxxx
            self.decode_indexed(data)
        } else if first_byte & 0x40 != 0 {
            // Literal Header Field with Incremental Indexing (Section 6.2.1)
            // Format: 01xxxxxx
            self.decode_literal_indexed(data)
        } else if first_byte & 0x20 != 0 {
            // Dynamic Table Size Update (Section 6.3)
            // Format: 001xxxxx
            self.decode_table_size_update(data)
        } else {
            // Literal Header Field without Indexing (Section 6.2.2)
            // or Never Indexed (Section 6.2.3)
            // Format: 0000xxxx or 0001xxxx
            self.decode_literal_not_indexed(data)
        }
    }

    /// Decode an indexed header field.
    fn decode_indexed(&mut self, data: &[u8]) -> Result<(Option<HeaderField>, usize), HpackError> {
        let (index, consumed) = decode_integer(data, 7)?;

        if index == 0 {
            return Err(HpackError::InvalidIndex(0));
        }

        let header = self.get_header(index)?;
        Ok((Some(header), consumed))
    }

    /// Decode a literal header field with incremental indexing.
    fn decode_literal_indexed(
        &mut self,
        data: &[u8],
    ) -> Result<(Option<HeaderField>, usize), HpackError> {
        let (name_index, mut consumed) = decode_integer(data, 6)?;

        let name = if name_index > 0 {
            self.get_header(name_index)?.name
        } else {
            let (n, c) = decode_string(&data[consumed..])?;
            consumed += c;
            n
        };

        let (value, c) = decode_string(&data[consumed..])?;
        consumed += c;

        let header = HeaderField::new(name, value);
        self.dynamic_table.insert(header.clone());

        Ok((Some(header), consumed))
    }

    /// Decode a literal header field without indexing.
    fn decode_literal_not_indexed(
        &mut self,
        data: &[u8],
    ) -> Result<(Option<HeaderField>, usize), HpackError> {
        let (name_index, mut consumed) = decode_integer(data, 4)?;

        let name = if name_index > 0 {
            self.get_header(name_index)?.name
        } else {
            let (n, c) = decode_string(&data[consumed..])?;
            consumed += c;
            n
        };

        let (value, c) = decode_string(&data[consumed..])?;
        consumed += c;

        let header = HeaderField::new(name, value);
        // Not added to dynamic table

        Ok((Some(header), consumed))
    }

    /// Decode a dynamic table size update.
    fn decode_table_size_update(
        &mut self,
        data: &[u8],
    ) -> Result<(Option<HeaderField>, usize), HpackError> {
        let (new_size, consumed) = decode_integer(data, 5)?;

        if new_size > self.max_table_size {
            return Err(HpackError::InvalidTableSize);
        }

        self.dynamic_table.set_max_size(new_size);

        Ok((None, consumed))
    }

    /// Get a header from the static or dynamic table by index.
    fn get_header(&self, index: usize) -> Result<HeaderField, HpackError> {
        if index == 0 {
            return Err(HpackError::InvalidIndex(0));
        }

        let static_len = StaticTable::len();

        if index <= static_len {
            // Static table
            let entry = StaticTable::get(index).ok_or(HpackError::InvalidIndex(index))?;
            Ok(HeaderField::new(entry.name.to_vec(), entry.value.to_vec()))
        } else {
            // Dynamic table
            let dyn_index = index - static_len - 1;
            self.dynamic_table
                .get(dyn_index)
                .cloned()
                .ok_or(HpackError::InvalidIndex(index))
        }
    }
}

/// Decode an HPACK integer (RFC 7541 Section 5.1).
fn decode_integer(data: &[u8], prefix_bits: u8) -> Result<(usize, usize), HpackError> {
    if data.is_empty() {
        return Err(HpackError::Incomplete);
    }

    let max_prefix = (1usize << prefix_bits) - 1;
    let mut value = (data[0] as usize) & max_prefix;
    let mut consumed = 1;

    if value < max_prefix {
        return Ok((value, consumed));
    }

    let mut shift = 0;
    loop {
        if consumed >= data.len() {
            return Err(HpackError::Incomplete);
        }

        let byte = data[consumed] as usize;
        consumed += 1;

        value += (byte & 0x7f) << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            break;
        }

        if shift > 28 {
            return Err(HpackError::InvalidInteger);
        }
    }

    Ok((value, consumed))
}

/// Decode an HPACK string (RFC 7541 Section 5.2).
fn decode_string(data: &[u8]) -> Result<(Vec<u8>, usize), HpackError> {
    if data.is_empty() {
        return Err(HpackError::Incomplete);
    }

    let huffman = (data[0] & 0x80) != 0;
    let (length, mut consumed) = decode_integer(data, 7)?;

    if consumed + length > data.len() {
        return Err(HpackError::Incomplete);
    }

    let string_data = &data[consumed..consumed + length];
    consumed += length;

    let result = if huffman {
        let mut decoded = Vec::new();
        huffman::decode(string_data, &mut decoded)?;
        decoded
    } else {
        string_data.to_vec()
    };

    Ok((result, consumed))
}

#[cfg(test)]
mod tests {
    use super::*;

    // HpackError tests

    #[test]
    fn test_hpack_error_display_incomplete() {
        let err = HpackError::Incomplete;
        assert_eq!(format!("{}", err), "incomplete HPACK data");
    }

    #[test]
    fn test_hpack_error_display_invalid_integer() {
        let err = HpackError::InvalidInteger;
        assert_eq!(format!("{}", err), "invalid HPACK integer encoding");
    }

    #[test]
    fn test_hpack_error_display_invalid_string() {
        let err = HpackError::InvalidString;
        assert_eq!(format!("{}", err), "invalid HPACK string encoding");
    }

    #[test]
    fn test_hpack_error_display_invalid_huffman() {
        let err = HpackError::InvalidHuffman(huffman::HuffmanError::InvalidCode);
        let display = format!("{}", err);
        assert!(display.contains("invalid Huffman encoding"));
    }

    #[test]
    fn test_hpack_error_display_invalid_index() {
        let err = HpackError::InvalidIndex(999);
        assert_eq!(format!("{}", err), "invalid table index: 999");
    }

    #[test]
    fn test_hpack_error_display_invalid_table_size() {
        let err = HpackError::InvalidTableSize;
        assert_eq!(format!("{}", err), "invalid table size update");
    }

    #[test]
    fn test_hpack_error_debug() {
        let err = HpackError::Incomplete;
        let debug = format!("{:?}", err);
        assert!(debug.contains("Incomplete"));
    }

    #[test]
    fn test_hpack_error_from_huffman_error() {
        let huffman_err = huffman::HuffmanError::InvalidCode;
        let hpack_err: HpackError = huffman_err.into();
        assert!(matches!(hpack_err, HpackError::InvalidHuffman(_)));
    }

    #[test]
    fn test_hpack_error_is_error_trait() {
        fn assert_error<E: std::error::Error>() {}
        assert_error::<HpackError>();
    }

    // HpackDecoder basic tests

    #[test]
    fn test_decoder_new() {
        let decoder = HpackDecoder::new();
        assert_eq!(decoder.max_table_size, super::super::DEFAULT_TABLE_SIZE);
    }

    #[test]
    fn test_decoder_default() {
        let decoder = HpackDecoder::default();
        assert_eq!(decoder.max_table_size, super::super::DEFAULT_TABLE_SIZE);
    }

    #[test]
    fn test_decoder_with_table_size() {
        let decoder = HpackDecoder::with_table_size(8192);
        assert_eq!(decoder.max_table_size, 8192);
    }

    #[test]
    fn test_decoder_set_max_table_size() {
        let mut decoder = HpackDecoder::new();
        decoder.set_max_table_size(16384);
        assert_eq!(decoder.max_table_size, 16384);
    }

    // decode_integer tests

    #[test]
    fn test_decode_integer_small() {
        let data = [10u8];
        let (value, consumed) = decode_integer(&data, 5).unwrap();
        assert_eq!(value, 10);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_decode_integer_large() {
        // 1337 with 5-bit prefix (RFC 7541 example)
        let data = [31u8, 154, 10];
        let (value, consumed) = decode_integer(&data, 5).unwrap();
        assert_eq!(value, 1337);
        assert_eq!(consumed, 3);
    }

    #[test]
    fn test_decode_integer_empty() {
        let data: [u8; 0] = [];
        let result = decode_integer(&data, 5);
        assert!(matches!(result, Err(HpackError::Incomplete)));
    }

    #[test]
    fn test_decode_integer_incomplete_continuation() {
        // Start of multi-byte integer but no continuation
        let data = [31u8]; // Max prefix value, needs continuation
        let result = decode_integer(&data, 5);
        assert!(matches!(result, Err(HpackError::Incomplete)));
    }

    #[test]
    fn test_decode_integer_overflow() {
        // Create a very long integer that would overflow
        let data = [
            0x1f, // Max 5-bit prefix
            0xff, 0xff, 0xff, 0xff, 0xff, // Too many continuation bytes
        ];
        let result = decode_integer(&data, 5);
        assert!(matches!(result, Err(HpackError::InvalidInteger)));
    }

    #[test]
    fn test_decode_integer_max_prefix() {
        // Exactly at max prefix value (31 for 5-bit prefix)
        let data = [30u8];
        let (value, consumed) = decode_integer(&data, 5).unwrap();
        assert_eq!(value, 30);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_decode_integer_different_prefix() {
        // Test with 7-bit prefix
        let data = [0x7f, 0x00]; // 127 + 0
        let (value, consumed) = decode_integer(&data, 7).unwrap();
        assert_eq!(value, 127);
        assert_eq!(consumed, 2);
    }

    // decode_string tests

    #[test]
    fn test_decode_string_empty_input() {
        let data: [u8; 0] = [];
        let result = decode_string(&data);
        assert!(matches!(result, Err(HpackError::Incomplete)));
    }

    #[test]
    fn test_decode_string_incomplete() {
        // Length says 10 but only 5 bytes of data
        let data = [0x0a, b'h', b'e', b'l', b'l', b'o'];
        let result = decode_string(&data);
        assert!(matches!(result, Err(HpackError::Incomplete)));
    }

    #[test]
    fn test_decode_string_plain() {
        // Plain string "hello" (no Huffman)
        let data = [0x05, b'h', b'e', b'l', b'l', b'o'];
        let (result, consumed) = decode_string(&data).unwrap();
        assert_eq!(result, b"hello");
        assert_eq!(consumed, 6);
    }

    #[test]
    fn test_decode_string_huffman() {
        // Huffman encoded string
        // First encode "www" using Huffman
        let mut encoded = Vec::new();
        huffman::encode(b"www", &mut encoded);

        let mut data = vec![(0x80 | encoded.len() as u8)]; // Huffman flag + length
        data.extend_from_slice(&encoded);

        let (result, _consumed) = decode_string(&data).unwrap();
        assert_eq!(result, b"www");
    }

    // decode_indexed tests

    #[test]
    fn test_decode_indexed() {
        let mut decoder = HpackDecoder::new();

        // Index 2 = :method: GET
        let data = [0x82];
        let headers = decoder.decode(&data).unwrap();

        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name, b":method");
        assert_eq!(headers[0].value, b"GET");
    }

    #[test]
    fn test_decode_indexed_zero_index() {
        let mut decoder = HpackDecoder::new();

        // Index 0 is invalid
        let data = [0x80]; // Indexed with index 0
        let result = decoder.decode(&data);
        assert!(matches!(result, Err(HpackError::InvalidIndex(0))));
    }

    #[test]
    fn test_decode_indexed_invalid_index() {
        let mut decoder = HpackDecoder::new();

        // Index 100 doesn't exist in static table and dynamic is empty
        let data = [0xff, 0x45]; // Large index value
        let result = decoder.decode(&data);
        assert!(matches!(result, Err(HpackError::InvalidIndex(_))));
    }

    // decode_literal_indexed tests

    #[test]
    fn test_decode_literal_indexed() {
        let mut decoder = HpackDecoder::new();

        // Literal with indexing, name index 1 (:authority), value "example.com"
        let data = [
            0x41, // Literal with indexing, name index 1
            0x0b, // Value length 11 (no Huffman)
            b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c', b'o', b'm',
        ];

        let headers = decoder.decode(&data).unwrap();

        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name, b":authority");
        assert_eq!(headers[0].value, b"example.com");

        // Should now be in dynamic table
        assert_eq!(decoder.dynamic_table.len(), 1);
    }

    #[test]
    fn test_decode_literal_indexed_new_name() {
        let mut decoder = HpackDecoder::new();

        // Literal with indexing, new name "custom-header", value "custom-value"
        let data = [
            0x40, // Literal with indexing, name index 0 (new name)
            0x0d, // Name length 13 (no Huffman)
            b'c', b'u', b's', b't', b'o', b'm', b'-', b'h', b'e', b'a', b'd', b'e', b'r',
            0x0c, // Value length 12 (no Huffman)
            b'c', b'u', b's', b't', b'o', b'm', b'-', b'v', b'a', b'l', b'u', b'e',
        ];

        let headers = decoder.decode(&data).unwrap();

        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name, b"custom-header");
        assert_eq!(headers[0].value, b"custom-value");

        // Should be added to dynamic table
        assert_eq!(decoder.dynamic_table.len(), 1);
    }

    // decode_literal_not_indexed tests

    #[test]
    fn test_decode_literal_not_indexed() {
        let mut decoder = HpackDecoder::new();

        // Literal without indexing, name index 1 (:authority), value "test.com"
        let data = [
            0x01, // Literal without indexing, name index 1
            0x08, // Value length 8 (no Huffman)
            b't', b'e', b's', b't', b'.', b'c', b'o', b'm',
        ];

        let headers = decoder.decode(&data).unwrap();

        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name, b":authority");
        assert_eq!(headers[0].value, b"test.com");

        // Should NOT be in dynamic table
        assert_eq!(decoder.dynamic_table.len(), 0);
    }

    #[test]
    fn test_decode_literal_not_indexed_new_name() {
        let mut decoder = HpackDecoder::new();

        // Literal without indexing, new name
        let data = [
            0x00, // Literal without indexing, name index 0 (new name)
            0x04, // Name length 4
            b't', b'e', b's', b't', 0x05, // Value length 5
            b'v', b'a', b'l', b'u', b'e',
        ];

        let headers = decoder.decode(&data).unwrap();

        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name, b"test");
        assert_eq!(headers[0].value, b"value");

        // Should NOT be in dynamic table
        assert_eq!(decoder.dynamic_table.len(), 0);
    }

    #[test]
    fn test_decode_literal_never_indexed() {
        let mut decoder = HpackDecoder::new();

        // Never indexed (0001xxxx pattern)
        let data = [
            0x11, // Never indexed, name index 1
            0x08, // Value length 8
            b't', b'e', b's', b't', b'.', b'c', b'o', b'm',
        ];

        let headers = decoder.decode(&data).unwrap();

        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].name, b":authority");

        // Should NOT be in dynamic table
        assert_eq!(decoder.dynamic_table.len(), 0);
    }

    // decode_table_size_update tests

    #[test]
    fn test_decode_table_size_update() {
        let mut decoder = HpackDecoder::new();
        decoder.set_max_table_size(8192);

        // Dynamic table size update to 4096
        let data = [0x3f, 0xe1, 0x1f]; // 32 + (4096 - 31) encoded

        let headers = decoder.decode(&data).unwrap();

        // Table size update produces no headers
        assert_eq!(headers.len(), 0);
    }

    #[test]
    fn test_decode_table_size_update_exceeds_max() {
        let mut decoder = HpackDecoder::new();
        decoder.set_max_table_size(1024); // Set small max

        // Try to update to 4096 (exceeds max)
        let data = [0x3f, 0xe1, 0x1f]; // 4096 encoded

        let result = decoder.decode(&data);
        assert!(matches!(result, Err(HpackError::InvalidTableSize)));
    }

    #[test]
    fn test_decode_table_size_update_zero() {
        let mut decoder = HpackDecoder::new();

        // Dynamic table size update to 0
        let data = [0x20]; // Size = 0

        let headers = decoder.decode(&data).unwrap();
        assert_eq!(headers.len(), 0);
    }

    // get_header tests

    #[test]
    fn test_get_header_static_table() {
        let decoder = HpackDecoder::new();

        // Index 2 is :method: GET in static table
        let header = decoder.get_header(2).unwrap();
        assert_eq!(header.name, b":method");
        assert_eq!(header.value, b"GET");
    }

    #[test]
    fn test_get_header_dynamic_table() {
        let mut decoder = HpackDecoder::new();

        // First add something to dynamic table via literal indexed
        let data = [
            0x40, // Literal with indexing, new name
            0x04, // Name length 4
            b't', b'e', b's', b't', 0x05, // Value length 5
            b'v', b'a', b'l', b'u', b'e',
        ];
        decoder.decode(&data).unwrap();

        // Now get from dynamic table (static table has 61 entries)
        let header = decoder.get_header(62).unwrap();
        assert_eq!(header.name, b"test");
        assert_eq!(header.value, b"value");
    }

    #[test]
    fn test_get_header_zero_index() {
        let decoder = HpackDecoder::new();
        let result = decoder.get_header(0);
        assert!(matches!(result, Err(HpackError::InvalidIndex(0))));
    }

    #[test]
    fn test_get_header_invalid_dynamic_index() {
        let decoder = HpackDecoder::new();
        // Dynamic table is empty, so any index > 61 is invalid
        let result = decoder.get_header(100);
        assert!(matches!(result, Err(HpackError::InvalidIndex(100))));
    }

    // Multiple headers tests

    #[test]
    fn test_decode_multiple_headers() {
        let mut decoder = HpackDecoder::new();

        // Multiple indexed headers
        let data = [
            0x82, // :method: GET
            0x86, // :scheme: http
            0x84, // :path: /
        ];

        let headers = decoder.decode(&data).unwrap();

        assert_eq!(headers.len(), 3);
        assert_eq!(headers[0].name, b":method");
        assert_eq!(headers[0].value, b"GET");
        assert_eq!(headers[1].name, b":scheme");
        assert_eq!(headers[1].value, b"http");
        assert_eq!(headers[2].name, b":path");
        assert_eq!(headers[2].value, b"/");
    }

    #[test]
    fn test_decode_empty() {
        let mut decoder = HpackDecoder::new();
        let data: [u8; 0] = [];
        let headers = decoder.decode(&data).unwrap();
        assert_eq!(headers.len(), 0);
    }

    // Roundtrip test

    #[test]
    fn test_roundtrip() {
        use super::super::encode::HpackEncoder;

        let mut encoder = HpackEncoder::new();
        let mut decoder = HpackDecoder::new();

        let headers = vec![
            HeaderField::new(b":method".to_vec(), b"GET".to_vec()),
            HeaderField::new(b":path".to_vec(), b"/".to_vec()),
            HeaderField::new(b":scheme".to_vec(), b"https".to_vec()),
            HeaderField::new(b":authority".to_vec(), b"example.com".to_vec()),
        ];

        let mut encoded = Vec::new();
        encoder.encode(&headers, &mut encoded);

        let decoded = decoder.decode(&encoded).unwrap();

        assert_eq!(headers.len(), decoded.len());
        for (orig, dec) in headers.iter().zip(decoded.iter()) {
            assert_eq!(orig.name, dec.name);
            assert_eq!(orig.value, dec.value);
        }
    }

    #[test]
    fn test_roundtrip_with_custom_headers() {
        use super::super::encode::HpackEncoder;

        let mut encoder = HpackEncoder::new();
        let mut decoder = HpackDecoder::new();

        let headers = vec![
            HeaderField::new(b"x-custom-header".to_vec(), b"custom-value".to_vec()),
            HeaderField::new(b"another-header".to_vec(), b"another-value".to_vec()),
        ];

        let mut encoded = Vec::new();
        encoder.encode(&headers, &mut encoded);

        let decoded = decoder.decode(&encoded).unwrap();

        assert_eq!(headers.len(), decoded.len());
        for (orig, dec) in headers.iter().zip(decoded.iter()) {
            assert_eq!(orig.name, dec.name);
            assert_eq!(orig.value, dec.value);
        }
    }

    // Dynamic table interaction tests

    #[test]
    fn test_dynamic_table_reuse() {
        let mut decoder = HpackDecoder::new();

        // First request: add custom header to dynamic table
        let data1 = [
            0x40, // Literal with indexing, new name
            0x04, // Name length 4
            b't', b'e', b's', b't', 0x05, // Value length 5
            b'v', b'a', b'l', b'u', b'e',
        ];
        let headers1 = decoder.decode(&data1).unwrap();
        assert_eq!(headers1.len(), 1);

        // Second request: reference it from dynamic table (index 62)
        let data2 = [0xbe]; // Indexed, index 62
        let headers2 = decoder.decode(&data2).unwrap();

        assert_eq!(headers2.len(), 1);
        assert_eq!(headers2[0].name, b"test");
        assert_eq!(headers2[0].value, b"value");
    }
}
