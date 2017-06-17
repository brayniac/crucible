#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
#[macro_use]
extern crate log;
extern crate getopts;
extern crate hmac;
extern crate sha_1;
extern crate json;
extern crate tic;
extern crate tiny_http;
extern crate rustc_serialize;

#[path = "../../src/common/metrics.rs"]
mod metrics;

#[path = "../../src/webhook/event/mod.rs"]
mod event;

use std::str::FromStr;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here
    if let Ok(s) = String::from_utf8(data.to_vec()) {
        let _ = event::Create::from_str(&s);
    }
});
