#![no_main]
#![allow(dead_code)]
#[macro_use]
extern crate libfuzzer_sys;
#[macro_use]
extern crate log;
extern crate getopts;
extern crate hmac;
extern crate sha_1;
extern crate json;
extern crate shuteye;
extern crate tic;
extern crate tiny_http;
extern crate toml;
extern crate rustc_serialize;

#[path = "../../src/common/mod.rs"]
mod common;

#[path = "../../src/api/mod.rs"]
mod api;

use std::str::FromStr;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here
    if let Ok(s) = String::from_utf8(data.to_vec()) {
        let _ = api::server::get_params(s);
    }
});
