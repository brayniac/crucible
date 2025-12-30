#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol_memcache::binary::ParsedBinaryResponse;

fuzz_target!(|data: &[u8]| {
    // Try to parse the input as a Memcache binary response
    if let Ok((response, consumed)) = ParsedBinaryResponse::parse(data) {
        // Verify consumed bytes is reasonable
        assert!(consumed <= data.len());

        // Verify opaque can be retrieved
        let _ = response.opaque();

        // Verify response-specific invariants
        match &response {
            ParsedBinaryResponse::Value { key, value, .. } => {
                if let Some(k) = key {
                    let _ = k;
                }
                let _ = value;
            }
            ParsedBinaryResponse::Counter { .. } => {}
            ParsedBinaryResponse::Success { .. } => {}
            ParsedBinaryResponse::Error { message, .. } => {
                let _ = message;
            }
            ParsedBinaryResponse::Version { version, .. } => {
                let _ = version;
            }
            ParsedBinaryResponse::Stat { key, value, .. } => {
                let _ = key;
                let _ = value;
            }
        }
    }
    // Parse errors are expected for malformed input - not a bug
});
