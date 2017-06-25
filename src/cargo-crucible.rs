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
use consumer::cargo::{Cargo, Channel, Profile, Triple};
use getopts::Options;
use std::{env, process};
use std::path::{Path, PathBuf};

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
    opts.optopt("d", "fuzz-length", "max length of fuzz input", "[BYTES]");

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

    // load config from file, override with command line
    let mut config = repoconfig::load_config(Path::new(".crucible.toml")).unwrap_or_default();
    if let Some(s) = options.opt_str("fuzz-duration") {
        let v = s.parse().expect("ERROR: fuzz-duration invalid");
        config.set_fuzz_seconds(v);
    }
    if let Some(s) = options.opt_str("fuzz-cores") {
        let v = s.parse().expect("ERROR: fuzz-cores invalid");
        config.set_fuzz_cores(v);
    }
    if let Some(s) = options.opt_str("fuzz-length") {
        let v = s.parse().expect("ERROR: fuzz-length invalid");
        config.set_fuzz_max_len(v);
    }

    // complete set of tests - native target
    let mut cargo = Cargo::new(PathBuf::from("."));
    build_test(&mut cargo);
    cargo.fmt().expect("cargo fmt: failed");
    if !options.opt_present("no-clippy") {
        cargo.clippy().expect("cargo clippy: failed");
    }
    if !options.opt_present("no-fuzz") {
        cargo.fuzz_seconds(config.fuzz_seconds());
        cargo.fuzz_cores(config.fuzz_cores());
        cargo.fuzz_max_len(config.fuzz_max_len());
        cargo.fuzz_all().expect("cargo fuzz: failed");
    }

    if !options.opt_present("no-cross") && config.cross() {
        let channels = vec![Channel::Stable, Channel::Nightly];
        let triples = vec![
            Triple::Aarch64LinuxGnu,
            Triple::ArmLinuxGnueabi,
            Triple::ArmLinuxGnueabihf,
            Triple::Armv7LinuxGnueabihf,
            Triple::I686LinuxGnu,
            Triple::I686LinuxMusl,
            Triple::X86_64LinuxGnu,
            Triple::X86_64LinuxMusl,
        ];

        if config.cross() {
            for channel in channels {
                cargo.channel(channel);
                for triple in &triples {
                    cargo.triple(*triple);
                    build_test(&mut cargo);
                }
            }
        }
    }
}

fn build_test(cargo: &mut Cargo) {
    let mut errors = 0;

    cargo.build().expect("cargo build failure");
    cargo.test().expect("cargo test failure");
    cargo.profile(Profile::Release);
    cargo.build().expect("cargo release build failure");
    cargo.test().expect("cargo release test failure");
    cargo.profile(Profile::Debug);
}
