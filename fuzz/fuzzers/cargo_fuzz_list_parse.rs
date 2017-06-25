#![no_main]
#![allow(dead_code)]
#[macro_use]
extern crate libfuzzer_sys;
#[macro_use]
extern crate log;
extern crate curl;
extern crate getopts;
extern crate hmac;
extern crate mktemp;
extern crate mpmc;
extern crate sha_1;
extern crate shuteye;
#[macro_use]
extern crate json;
extern crate tic;
extern crate tiny_http;
extern crate toml;
extern crate regex;
extern crate rustc_serialize;

#[path = "../../src/publisher/mod.rs"]
mod publisher;

#[path = "../../src/common/mod.rs"]
mod common;

#[path = "../../src/common/metrics.rs"]
mod metrics;

#[path = "../../src/webhook/mod.rs"]
mod webhook;

#[path = "../../src/consumer/mod.rs"]
mod consumer;

#[path = "../../src/consumer/cargo/mod.rs"]
mod cargo;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here
    let _ = cargo::fuzz_list_parse(data.to_vec());
});
