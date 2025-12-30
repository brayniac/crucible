#![no_main]

use bytes::BytesMut;
use grpc::{decode_message, encode_message};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);

    // Try to decode message(s) from the input
    loop {
        match decode_message(&mut buf) {
            Ok(Some((message, compressed))) => {
                // Successfully decoded a message
                let _ = compressed;
                let _ = message.len();

                // Roundtrip test: encode the decoded message and verify structure
                let reencoded = encode_message(&message);
                assert!(reencoded.len() >= 5); // At least header size

                // The re-encoded message should decode successfully
                let mut rebuf = BytesMut::from(&reencoded[..]);
                if let Ok(Some((redecoded, _))) = decode_message(&mut rebuf) {
                    assert_eq!(message, redecoded);
                }
            }
            Ok(None) => {
                // Need more data
                break;
            }
            Err(_) => {
                // Parse error - expected for malformed input
                break;
            }
        }
    }
});
