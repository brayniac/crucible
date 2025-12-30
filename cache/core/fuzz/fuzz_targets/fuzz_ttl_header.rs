#![no_main]

use cache_core::TtlHeader;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Test TtlHeader parsing
    if let Some(header) = TtlHeader::try_from_bytes(data) {
        // Verify all accessors work
        let key_len = header.key_len();
        let optional_len = header.optional_len();
        let value_len = header.value_len();
        let is_deleted = header.is_deleted();
        let is_numeric = header.is_numeric();
        let expire_at = header.expire_at();

        // Verify padded_size doesn't panic
        let padded_size = header.padded_size();
        assert!(padded_size >= TtlHeader::SIZE);
        assert_eq!(padded_size % 8, 0);

        // Verify ranges don't panic for offset 0
        let _ = header.optional_range(0);
        let _ = header.key_range(0);
        let _ = header.value_range(0);

        // Test expiration methods
        let _ = header.is_expired(0);
        let _ = header.is_expired(expire_at);
        let _ = header.is_expired(u32::MAX);
        let _ = header.remaining_ttl(0);
        let _ = header.remaining_ttl(expire_at);

        // Roundtrip test: write and re-parse
        let mut buf = vec![0u8; TtlHeader::SIZE];
        header.to_bytes(&mut buf);

        let reparsed = TtlHeader::try_from_bytes(&buf).expect("roundtrip parse failed");
        assert_eq!(key_len, reparsed.key_len(), "key_len mismatch");
        assert_eq!(optional_len, reparsed.optional_len(), "optional_len mismatch");
        assert_eq!(value_len, reparsed.value_len(), "value_len mismatch");
        assert_eq!(is_deleted, reparsed.is_deleted(), "is_deleted mismatch");
        assert_eq!(is_numeric, reparsed.is_numeric(), "is_numeric mismatch");
        assert_eq!(expire_at, reparsed.expire_at(), "expire_at mismatch");
    }

    // Test with_flags constructor and roundtrip
    if data.len() >= 10 {
        let key_len = data[0];
        let optional_len = data[1] & 0x3F; // Max 6 bits
        let value_len = u32::from_le_bytes([data[2], data[3], data[4], 0]) & 0xFFFFFF; // 24 bits
        let is_deleted = (data[5] & 0x01) != 0;
        let is_numeric = (data[5] & 0x02) != 0;
        let expire_at = u32::from_le_bytes([data[6], data[7], data[8], data[9]]);

        let header =
            TtlHeader::with_flags(key_len, optional_len, value_len, is_deleted, is_numeric, expire_at);

        let mut buf = vec![0u8; TtlHeader::SIZE];
        header.to_bytes(&mut buf);

        if let Some(reparsed) = TtlHeader::try_from_bytes(&buf) {
            assert_eq!(key_len, reparsed.key_len());
            assert_eq!(optional_len, reparsed.optional_len());
            assert_eq!(value_len, reparsed.value_len());
            assert_eq!(is_deleted, reparsed.is_deleted());
            assert_eq!(is_numeric, reparsed.is_numeric());
            assert_eq!(expire_at, reparsed.expire_at());
        }
    }
});
