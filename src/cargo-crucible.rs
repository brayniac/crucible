#![allow(dead_code)]
#[macro_use]
extern crate log;
#[macro_use]
extern crate json;
extern crate curl;
extern crate getopts;
extern crate mktemp;
extern crate mpmc;
extern crate regex;
extern crate shuteye;
extern crate tic;
extern crate tiny_http;
extern crate toml;
extern crate sha_1;
extern crate hmac;
extern crate rustc_serialize;

mod common;
mod consumer;
mod webhook;
mod publisher;

use common::logging::set_log_level;
use common::repoconfig;
use consumer::cargo;
use getopts::Options;
use std::{env, process};
use std::path::Path;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
pub const PROGRAM: &'static str = env!("CARGO_PKG_NAME");

fn print_usage(program: &str, opts: &Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn opts() -> Options {
    let mut opts = Options::new();

    // general
    opts.optflag("h", "help", "print this help menu");
    opts.optflag("", "version", "show version and exit");
    opts.optflagmulti("v", "verbose", "verbosity (stacking)");

    // clippy
    opts.optflag("", "no-clippy", "skip clippy");

    // fuzzing
    opts.optflag("", "no-fuzz", "skip fuzzing");
    opts.optopt("d", "fuzz-duration", "time per fuzz target", "[SECONDS]");
    opts.optopt(
        "c",
        "fuzz-cores",
        "number of cores to use during fuzzing",
        "[CORES]",
    );

    // cross targets
    opts.optflag("", "no-cross", "skip cross targets");

    opts
}

pub fn init() -> getopts::Matches {
    let args: Vec<String> = env::args().collect();
    let program = &args[0];
    let opts = opts();

    let options = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            println!("Failed to parse command line args: {}", f);
            process::exit(1);
        }
    };

    if options.opt_present("help") {
        print_usage(program, &opts);
        process::exit(0);
    }

    if options.opt_present("version") {
        println!("{} {}", PROGRAM, VERSION);
        process::exit(0);
    }

    options
}

fn main() {
    let options = init();

    // initialize logging
    set_log_level(options.opt_count("verbose"));
    info!("{} {}", PROGRAM, VERSION);

    let config = repoconfig::load_config(Path::new(".crucible.toml")).unwrap_or_default();

    // complete set of tests - native target
    let mut cargo = cargo::Cargo::new(".".to_owned());
    cargo.build().expect("cargo build: failed");
    cargo.test().expect("cargo test: failed");
    cargo.fmt().expect("cargo fmt: failed");
    if !options.opt_present("no-clippy") {
        cargo.clippy().expect("cargo clippy: failed");
    }
    if !options.opt_present("no-fuzz") {
        cargo
            .fuzz_all(config.fuzz_seconds(), config.fuzz_cores())
            .expect("cargo fuzz: failed");
    }
    cargo.set_release(true);
    cargo.build().expect("cargo build --release: failed");
    cargo.test().expect("cargo test --release: failed");

    if !options.opt_present("no-cross") && config.cross() {
        let targets = vec![
            "aarch64-unknown-linux-gnu", // Tier-2
            "arm-unknown-linux-gnueabi", // Tier-2
            "arm-unknown-linux-gnueabihf", // Tier-2
            "armv7-unknown-linux-gnueabihf", // Tier-2
            "i686-unknown-linux-gnu", // Tier-1
            "i686-unknown-linux-musl", // Tier-2
            "x86_64-unknown-linux-gnu", // Tier-1
            "x86_64-unknown-linux-musl", // Tier-2
        ];

        let channels = vec!["stable", "beta", "nightly"];

        // cross target tests
        for target in targets {
            for channel in &channels {
                info!("channel: {} target: {}", channel, target);
                let mut cargo = cargo::Cargo::new(".".to_owned());
                cargo.set_target(target.to_owned());
                cargo.build().expect("cargo build: failed");
                cargo.test().expect("cargo test: failed");
                cargo.set_release(true);
                cargo.build().expect("cargo build --release: failed");
                cargo.test().expect("cargo test --release: failed");
            }
        }
    }
}
