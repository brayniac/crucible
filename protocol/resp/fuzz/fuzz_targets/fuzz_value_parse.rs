#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol_resp::Value;

/// Check if a value contains any NaN floats (which don't compare equal to themselves).
fn contains_nan(value: &Value) -> bool {
    match value {
        Value::Double(d) => d.is_nan(),
        Value::Array(elements) => elements.iter().any(contains_nan),
        Value::Map(entries) => entries.iter().any(|(k, v)| contains_nan(k) || contains_nan(v)),
        Value::Set(elements) => elements.iter().any(contains_nan),
        Value::Push(elements) => elements.iter().any(contains_nan),
        Value::Attribute { attrs, value } => {
            attrs.iter().any(|(k, v)| contains_nan(k) || contains_nan(v)) || contains_nan(value)
        }
        _ => false,
    }
}

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
        // Skip comparison for values containing NaN since NaN != NaN
        if !contains_nan(&value) {
            if let Ok((reparsed, reparsed_consumed)) = Value::parse(&buf) {
                assert_eq!(actual_len, reparsed_consumed);
                assert_eq!(value, reparsed);
            }
        }
    }
    // Parse errors are expected for malformed input - not a bug
});
