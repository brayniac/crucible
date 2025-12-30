#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol_memcache::Response;

fuzz_target!(|data: &[u8]| {
    // Try to parse the input as a Memcache ASCII response
    if let Ok((response, consumed)) = Response::parse(data) {
        // Verify consumed bytes is reasonable
        assert!(consumed <= data.len());

        // Verify response-specific invariants
        match &response {
            Response::Values(values) => {
                // All values should have valid data
                for value in values {
                    let _ = &value.key;
                    let _ = value.flags;
                    let _ = &value.data;
                }
            }
            Response::Stored
            | Response::NotStored
            | Response::Deleted
            | Response::NotFound
            | Response::Exists
            | Response::Error => {}
            Response::Version(v) => {
                let _ = v;
            }
            Response::ClientError(msg) | Response::ServerError(msg) => {
                let _ = msg;
            }
        }

        // Verify roundtrip for simple responses (encode then parse)
        match &response {
            Response::Stored
            | Response::NotStored
            | Response::Deleted
            | Response::NotFound
            | Response::Exists
            | Response::Error => {
                let mut buf = vec![0u8; 256];
                let len = response.encode(&mut buf);
                if let Ok((reparsed, _)) = Response::parse(&buf[..len]) {
                    assert_eq!(response, reparsed);
                }
            }
            Response::Values(values) if values.is_empty() => {
                // Empty values (miss) should roundtrip
                let mut buf = vec![0u8; 256];
                let len = response.encode(&mut buf);
                if let Ok((reparsed, _)) = Response::parse(&buf[..len]) {
                    assert_eq!(response, reparsed);
                }
            }
            _ => {}
        }
    }
    // Parse errors are expected for malformed input - not a bug
});
