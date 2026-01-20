//! Numeric utilities for storing and retrieving numeric values efficiently.
//!
//! This module provides functions for:
//! - Detecting "simple numeric" values that can be stored as u64
//! - Encoding/decoding u64 values to/from bytes
//! - Formatting u64 values back to ASCII strings

/// Parse a byte slice as a "simple numeric" value.
///
/// A value is "simple numeric" if:
/// - It contains only ASCII digits [0-9]
/// - It has no leading zeros (except "0" itself)
/// - It has no whitespace
/// - It fits in a u64
///
/// Returns `Some(value)` if the bytes represent a simple numeric value,
/// `None` otherwise.
///
/// # Examples
/// ```
/// use cache_core::numeric::parse_simple_numeric;
///
/// assert_eq!(parse_simple_numeric(b"0"), Some(0));
/// assert_eq!(parse_simple_numeric(b"123"), Some(123));
/// assert_eq!(parse_simple_numeric(b"01"), None);      // Leading zero
/// assert_eq!(parse_simple_numeric(b" 5"), None);      // Whitespace
/// assert_eq!(parse_simple_numeric(b"hello"), None);   // Non-digit
/// ```
pub fn parse_simple_numeric(bytes: &[u8]) -> Option<u64> {
    // Empty input is not numeric
    if bytes.is_empty() {
        return None;
    }

    // Check for leading zeros (except "0" itself)
    if bytes.len() > 1 && bytes[0] == b'0' {
        return None;
    }

    // Parse digits
    let mut value: u64 = 0;
    for &byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        let digit = (byte - b'0') as u64;
        value = value.checked_mul(10)?.checked_add(digit)?;
    }

    // Round-trip check: ensure formatting back produces the same bytes
    // This handles edge cases like overflow during formatting
    let mut buf = [0u8; 20];
    let len = format_numeric(value, &mut buf);
    if len != bytes.len() || &buf[..len] != bytes {
        return None;
    }

    Some(value)
}

/// Format a u64 value to ASCII digits.
///
/// Writes the ASCII representation of `value` to `buf` and returns the
/// number of bytes written. The buffer must be at least 20 bytes to hold
/// the maximum u64 value (18446744073709551615).
///
/// # Examples
/// ```
/// use cache_core::numeric::format_numeric;
///
/// let mut buf = [0u8; 20];
/// let len = format_numeric(0, &mut buf);
/// assert_eq!(&buf[..len], b"0");
///
/// let len = format_numeric(12345, &mut buf);
/// assert_eq!(&buf[..len], b"12345");
/// ```
pub fn format_numeric(value: u64, buf: &mut [u8; 20]) -> usize {
    if value == 0 {
        buf[0] = b'0';
        return 1;
    }

    // Write digits in reverse order
    let mut v = value;
    let mut pos = 20;
    while v > 0 {
        pos -= 1;
        buf[pos] = b'0' + (v % 10) as u8;
        v /= 10;
    }

    // Shift to front of buffer
    let len = 20 - pos;
    buf.copy_within(pos..20, 0);
    len
}

/// Encode a u64 value to 8 bytes in little-endian format.
///
/// This is the storage format for numeric values in the cache.
#[inline]
pub fn encode_numeric(value: u64) -> [u8; 8] {
    value.to_le_bytes()
}

/// Decode a u64 value from 8 bytes in little-endian format.
///
/// This reverses `encode_numeric`.
#[inline]
pub fn decode_numeric(bytes: &[u8; 8]) -> u64 {
    u64::from_le_bytes(*bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_numeric_valid() {
        assert_eq!(parse_simple_numeric(b"0"), Some(0));
        assert_eq!(parse_simple_numeric(b"1"), Some(1));
        assert_eq!(parse_simple_numeric(b"123"), Some(123));
        assert_eq!(parse_simple_numeric(b"999999999"), Some(999999999));
        assert_eq!(
            parse_simple_numeric(b"18446744073709551615"),
            Some(u64::MAX)
        );
    }

    #[test]
    fn test_parse_simple_numeric_invalid() {
        // Empty
        assert_eq!(parse_simple_numeric(b""), None);

        // Leading zeros
        assert_eq!(parse_simple_numeric(b"01"), None);
        assert_eq!(parse_simple_numeric(b"007"), None);
        assert_eq!(parse_simple_numeric(b"00"), None);

        // Whitespace
        assert_eq!(parse_simple_numeric(b" 5"), None);
        assert_eq!(parse_simple_numeric(b"5 "), None);
        assert_eq!(parse_simple_numeric(b" "), None);

        // Non-digits
        assert_eq!(parse_simple_numeric(b"hello"), None);
        assert_eq!(parse_simple_numeric(b"12a"), None);
        assert_eq!(parse_simple_numeric(b"-5"), None);
        assert_eq!(parse_simple_numeric(b"+5"), None);
        assert_eq!(parse_simple_numeric(b"1.5"), None);

        // Overflow (larger than u64::MAX)
        assert_eq!(parse_simple_numeric(b"18446744073709551616"), None);
        assert_eq!(parse_simple_numeric(b"99999999999999999999"), None);
    }

    #[test]
    fn test_format_numeric() {
        let mut buf = [0u8; 20];

        let len = format_numeric(0, &mut buf);
        assert_eq!(&buf[..len], b"0");

        let len = format_numeric(1, &mut buf);
        assert_eq!(&buf[..len], b"1");

        let len = format_numeric(123, &mut buf);
        assert_eq!(&buf[..len], b"123");

        let len = format_numeric(u64::MAX, &mut buf);
        assert_eq!(&buf[..len], b"18446744073709551615");
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let values = [0, 1, 255, 256, 65535, 65536, u64::MAX / 2, u64::MAX];

        for value in values {
            let encoded = encode_numeric(value);
            let decoded = decode_numeric(&encoded);
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn test_parse_format_roundtrip() {
        let values = [0, 1, 123, 999999999, u64::MAX];

        for value in values {
            let mut buf = [0u8; 20];
            let len = format_numeric(value, &mut buf);
            let parsed = parse_simple_numeric(&buf[..len]);
            assert_eq!(parsed, Some(value));
        }
    }
}
