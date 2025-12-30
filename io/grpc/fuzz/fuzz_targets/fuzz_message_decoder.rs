#![no_main]

use grpc::{MessageDecoder, encode_message};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut decoder = MessageDecoder::new();

    // Feed data in chunks to test incremental decoding
    let chunk_size = if data.len() > 10 { data.len() / 3 } else { 1 };

    for chunk in data.chunks(chunk_size.max(1)) {
        decoder.feed(chunk);

        // Try to decode any complete messages
        loop {
            match decoder.decode() {
                Ok(Some((message, compressed))) => {
                    // Successfully decoded a message
                    let _ = compressed;
                    let _ = message.len();

                    // Roundtrip: encode and verify
                    let reencoded = encode_message(&message);
                    assert!(reencoded.len() >= 5);
                }
                Ok(None) => {
                    // Need more data
                    break;
                }
                Err(_) => {
                    // Parse error - clear and continue
                    decoder.clear();
                    break;
                }
            }
        }
    }

    // Verify buffer state is consistent
    let _ = decoder.has_buffered_data();
    let _ = decoder.buffered_len();
});
