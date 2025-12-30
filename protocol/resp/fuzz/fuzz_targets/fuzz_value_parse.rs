#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol_resp::Value;

fuzz_target!(|data: &[u8]| {
    // Try to parse the input as a RESP value
    if let Ok((value, consumed)) = Value::parse(data) {
        // Verify consumed bytes is reasonable
        assert!(consumed <= data.len());

        // Verify encoded length calculation matches actual encoding
        let encoded_len = value.encoded_len();
        let mut buf = vec![0u8; encoded_len];
        let actual_len = value.encode(&mut buf);
        assert_eq!(encoded_len, actual_len);

        // Verify roundtrip: parse(encode(value)) == value
        // Note: Some values may not roundtrip exactly (e.g., NaN floats)
        if let Ok((reparsed, reparsed_consumed)) = Value::parse(&buf) {
            assert_eq!(actual_len, reparsed_consumed);

            // For most values, the reparsed value should equal the original
            // Skip comparison for Double with NaN since NaN != NaN
            if matches!(&value, Value::Double(d) if d.is_nan()) {
                // Just verify we got a NaN back
                if let Value::Double(d) = &reparsed {
                    assert!(d.is_nan());
                }
            } else {
                assert_eq!(value, reparsed);
            }
        }
    }
    // Parse errors are expected for malformed input - not a bug
});
