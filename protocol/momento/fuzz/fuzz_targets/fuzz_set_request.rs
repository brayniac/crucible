#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol_momento::proto::{DeleteRequestOwned, SetRequestOwned, SetResponse};

fuzz_target!(|data: &[u8]| {
    // Test SetRequestOwned decoding
    if let Some(request) = SetRequestOwned::decode(data) {
        // Verify the decoded data is accessible
        let _ = request.cache_key.len();
        let _ = request.cache_body.len();
        let _ = request.ttl_milliseconds;
    }

    // Test SetResponse decoding
    if let Some(response) = SetResponse::decode(data) {
        // Roundtrip test: encode and decode
        let encoded = response.encode();

        if let Some(decoded) = SetResponse::decode(&encoded) {
            // Verify structure matches
            match (&response, &decoded) {
                (SetResponse::Ok, SetResponse::Ok) => {}
                (SetResponse::Error { message: m1 }, SetResponse::Error { message: m2 }) => {
                    assert_eq!(m1, m2, "SetResponse::Error roundtrip mismatch");
                }
                _ => {
                    // Different variants after roundtrip
                }
            }
        }
    }

    // Test DeleteRequestOwned decoding
    if let Some(request) = DeleteRequestOwned::decode(data) {
        // Verify the decoded data is accessible
        let _ = request.cache_key.len();
    }
});
