#![no_main]

use bytes::BytesMut;
use http2::{Frame, FrameDecoder};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let decoder = FrameDecoder::new();
    let mut buf = BytesMut::from(data);

    // Try to decode frames until we run out of data or hit an error
    loop {
        match decoder.decode(&mut buf) {
            Ok(Some(frame)) => {
                // Successfully decoded a frame - verify we can access all fields
                let _ = frame.stream_id();

                match frame {
                    Frame::Data(f) => {
                        let _ = f.stream_id;
                        let _ = f.end_stream;
                        let _ = f.data;
                    }
                    Frame::Headers(f) => {
                        let _ = f.stream_id;
                        let _ = f.end_stream;
                        let _ = f.end_headers;
                        let _ = f.priority;
                        let _ = f.header_block;
                    }
                    Frame::Priority(f) => {
                        let _ = f.stream_id;
                        let _ = f.priority.exclusive;
                        let _ = f.priority.dependency;
                        let _ = f.priority.weight;
                    }
                    Frame::RstStream(f) => {
                        let _ = f.stream_id;
                        let _ = f.error_code;
                    }
                    Frame::Settings(f) => {
                        let _ = f.ack;
                        for setting in &f.settings {
                            let _ = setting.id;
                            let _ = setting.value;
                        }
                    }
                    Frame::PushPromise(f) => {
                        let _ = f.stream_id;
                        let _ = f.end_headers;
                        let _ = f.promised_stream_id;
                        let _ = f.header_block;
                    }
                    Frame::Ping(f) => {
                        let _ = f.ack;
                        let _ = f.data;
                    }
                    Frame::GoAway(f) => {
                        let _ = f.last_stream_id;
                        let _ = f.error_code;
                        let _ = f.debug_data;
                    }
                    Frame::WindowUpdate(f) => {
                        let _ = f.stream_id;
                        let _ = f.increment;
                    }
                    Frame::Continuation(f) => {
                        let _ = f.stream_id;
                        let _ = f.end_headers;
                        let _ = f.header_block;
                    }
                    Frame::Unknown(f) => {
                        let _ = f.frame_type;
                        let _ = f.flags;
                        let _ = f.stream_id;
                        let _ = f.payload;
                    }
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
