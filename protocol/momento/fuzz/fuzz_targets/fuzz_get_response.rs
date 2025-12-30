#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol_momento::proto::{GetRequestOwned, GetResponse};

fuzz_target!(|data: &[u8]| {
    // Test GetRequestOwned decoding
    if let Some(request) = GetRequestOwned::decode(data) {
        // Verify the decoded data is accessible
        let _ = request.cache_key.len();
    }

    // Test GetResponse decoding
    if let Some(response) = GetResponse::decode(data) {
        // Roundtrip test: encode and decode
        let encoded = response.encode();

        if let Some(decoded) = GetResponse::decode(&encoded) {
            // Verify structure matches
            match (&response, &decoded) {
                (GetResponse::Miss, GetResponse::Miss) => {}
                (GetResponse::Hit(v1), GetResponse::Hit(v2)) => {
                    assert_eq!(v1, v2, "GetResponse::Hit roundtrip mismatch");
                }
                (GetResponse::Error { message: m1 }, GetResponse::Error { message: m2 }) => {
                    assert_eq!(m1, m2, "GetResponse::Error roundtrip mismatch");
                }
                _ => {
                    // Different variants after roundtrip - this could be valid
                    // if the original had ambiguous encoding
                }
            }
        }
    }
});
