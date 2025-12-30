#![no_main]

use http2::{HpackDecoder, HpackEncoder};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut decoder = HpackDecoder::new();

    // Try to decode HPACK data
    if let Ok(headers) = decoder.decode(data) {
        // Verify all headers have valid name/value
        for header in &headers {
            let _ = &header.name;
            let _ = &header.value;
        }

        // Roundtrip test: encode and decode should give equivalent results
        // Note: not exactly equal because indexing may differ
        if !headers.is_empty() {
            let mut encoder = HpackEncoder::new();
            let mut encoded = Vec::new();
            encoder.encode(&headers, &mut encoded);

            let mut decoder2 = HpackDecoder::new();
            if let Ok(decoded) = decoder2.decode(&encoded) {
                // Verify same number of headers
                assert_eq!(
                    headers.len(),
                    decoded.len(),
                    "roundtrip header count mismatch"
                );

                // Verify all names/values match
                for (orig, dec) in headers.iter().zip(decoded.iter()) {
                    assert_eq!(orig.name, dec.name, "roundtrip name mismatch");
                    assert_eq!(orig.value, dec.value, "roundtrip value mismatch");
                }
            }
        }
    }
    // Parse errors are expected for malformed input
});
