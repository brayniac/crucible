#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
#[macro_use]
extern crate log;
extern crate getopts;
extern crate json;
extern crate tic;
extern crate tiny_http;

#[path = "../../src/metrics.rs"]
mod metrics;

#[path = "../../src/webhook.rs"]
mod webhook;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here
    if let Ok(s) = String::from_utf8(data.to_vec()) {
    	webhook::handle_push(&s);
    }
});
