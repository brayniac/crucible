#![no_main]

use cache_core::BasicHeader;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Test BasicHeader parsing
    if let Some(header) = BasicHeader::try_from_bytes(data) {
        // Verify all accessors work
        let key_len = header.key_len();
        let optional_len = header.optional_len();
        let value_len = header.value_len();
        let is_deleted = header.is_deleted();
        let is_numeric = header.is_numeric();

        // Verify padded_size doesn't panic
        let padded_size = header.padded_size();
        assert!(padded_size >= BasicHeader::SIZE);
        assert_eq!(padded_size % 8, 0);

        // Verify ranges don't panic for offset 0
        let _ = header.optional_range(0);
        let _ = header.key_range(0);
        let _ = header.value_range(0);

        // Roundtrip test: write and re-parse
        let mut buf = vec![0u8; BasicHeader::SIZE];
        header.to_bytes(&mut buf);

        let reparsed = BasicHeader::try_from_bytes(&buf).expect("roundtrip parse failed");
        assert_eq!(key_len, reparsed.key_len(), "key_len mismatch");
        assert_eq!(optional_len, reparsed.optional_len(), "optional_len mismatch");
        assert_eq!(value_len, reparsed.value_len(), "value_len mismatch");
        assert_eq!(is_deleted, reparsed.is_deleted(), "is_deleted mismatch");
        assert_eq!(is_numeric, reparsed.is_numeric(), "is_numeric mismatch");
    }

    // Test with_flags constructor and roundtrip
    if data.len() >= 6 {
        let key_len = data[0];
        let optional_len = data[1] & 0x3F; // Max 6 bits
        let value_len = u32::from_le_bytes([data[2], data[3], data[4], 0]) & 0xFFFFFF; // 24 bits
        let is_deleted = (data[5] & 0x01) != 0;
        let is_numeric = (data[5] & 0x02) != 0;

        let header = BasicHeader::with_flags(key_len, optional_len, value_len, is_deleted, is_numeric);

        let mut buf = vec![0u8; BasicHeader::SIZE];
        header.to_bytes(&mut buf);

        if let Some(reparsed) = BasicHeader::try_from_bytes(&buf) {
            assert_eq!(key_len, reparsed.key_len());
            assert_eq!(optional_len, reparsed.optional_len());
            assert_eq!(value_len, reparsed.value_len());
            assert_eq!(is_deleted, reparsed.is_deleted());
            assert_eq!(is_numeric, reparsed.is_numeric());
        }
    }
});
