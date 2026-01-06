#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol_ping::Response;

fuzz_target!(|data: &[u8]| {
    // Try to parse the input as a PING response
    match Response::parse(data) {
        Ok((response, consumed)) => {
            // Verify consumed bytes is reasonable
            assert!(consumed <= data.len());
            assert!(consumed > 0);

            // Verify response invariants
            let _ = response.is_error();

            // Verify roundtrip for successful parse
            let mut buf = [0u8; 64];
            let encoded_len = response.encode(&mut buf);
            if let Ok((reparsed, _)) = Response::parse(&buf[..encoded_len]) {
                assert_eq!(response, reparsed);
            }
        }
        Err(_) => {
            // Parse errors are expected for malformed input
        }
    }
});
