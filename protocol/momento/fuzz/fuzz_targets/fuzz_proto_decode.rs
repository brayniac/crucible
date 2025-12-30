#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol_momento::proto::{
    decode_length_delimited, decode_tag, decode_varint, encode_bytes, encode_tag, encode_uint64,
    encode_varint, skip_field,
};

fuzz_target!(|data: &[u8]| {
    // Test varint decoding
    let mut buf = data;
    if let Some(value) = decode_varint(&mut buf) {
        // Roundtrip test for varint
        let mut encoded = Vec::new();
        encode_varint(value, &mut encoded);

        let mut rebuf = &encoded[..];
        if let Some(decoded) = decode_varint(&mut rebuf) {
            assert_eq!(value, decoded, "varint roundtrip mismatch");
        }
    }

    // Test tag decoding
    let mut buf = data;
    if let Some((field_number, wire_type)) = decode_tag(&mut buf) {
        // Roundtrip test for tag
        let mut encoded = Vec::new();
        encode_tag(field_number, wire_type, &mut encoded);

        let mut rebuf = &encoded[..];
        if let Some((dec_field, dec_wire)) = decode_tag(&mut rebuf) {
            assert_eq!(field_number, dec_field, "tag field_number mismatch");
            assert_eq!(wire_type, dec_wire, "tag wire_type mismatch");
        }
    }

    // Test length-delimited decoding
    let mut buf = data;
    if let Some(decoded_bytes) = decode_length_delimited(&mut buf) {
        // Roundtrip: encode as bytes field and decode
        let mut encoded = Vec::new();
        encode_bytes(1, decoded_bytes, &mut encoded);

        let mut rebuf = &encoded[..];
        // Skip the tag
        if decode_tag(&mut rebuf).is_some() {
            if let Some(redecoded) = decode_length_delimited(&mut rebuf) {
                assert_eq!(decoded_bytes, redecoded, "length_delimited roundtrip mismatch");
            }
        }
    }

    // Test skip_field for various wire types
    for wire_type in 0..6u8 {
        let mut buf = data;
        // skip_field should not panic
        let _ = skip_field(wire_type, &mut buf);
    }

    // Test uint64 encoding/decoding roundtrip
    if data.len() >= 8 {
        let value = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let mut encoded = Vec::new();
        encode_uint64(1, value, &mut encoded);

        let mut rebuf = &encoded[..];
        if decode_tag(&mut rebuf).is_some() {
            if let Some(decoded) = decode_varint(&mut rebuf) {
                assert_eq!(value, decoded, "uint64 roundtrip mismatch");
            }
        }
    }
});
