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
use std::collections::HashMap;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here
    if let Ok(s) = String::from_utf8(data.to_vec()) {
        if let Ok(params) = api::server::get_params(s) {
        	let mut index = HashMap::new();
			index.insert("octocat/Spoon-Knife".to_owned(), 1);
			index.insert("octocat/test-repo1".to_owned(), 2);
	        let _ = api::server::get_repo_id(&index, params);
        }
    }
});
