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
use consumer::cargo;
use getopts::Matches;
use getopts::Options;
use std::{env, process};
use std::collections::BTreeMap;
use std::default::Default;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use toml::{Parser, Value};
use toml::Value::Table;

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

pub struct Config {
    fuzz: bool,
    fuzz_cores: usize,
    fuzz_seconds: usize,
    cross: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            fuzz: true,
            fuzz_cores: 1,
            fuzz_seconds: 10,
            cross: true,
        }
    }
}

impl Config {
    pub fn set_fuzz_cores(&mut self, cores: usize) -> &mut Self {
        self.fuzz_cores = cores;
        self
    }

    pub fn fuzz_cores(&self) -> usize {
        self.fuzz_cores
    }

    pub fn set_fuzz_seconds(&mut self, seconds: usize) -> &mut Self {
        self.fuzz_seconds = seconds;
        self
    }

    pub fn fuzz_seconds(&self) -> usize {
        self.fuzz_seconds
    }

    pub fn set_fuzz(&mut self, enable: bool) -> &mut Self {
        self.fuzz = enable;
        self
    }

    pub fn fuzz(&self) -> bool {
        self.fuzz
    }

    pub fn set_cross(&mut self, enable: bool) -> &mut Self {
        self.cross = enable;
        self
    }

    pub fn cross(&self) -> bool {
        self.cross
    }
}

pub fn load_config(path: &Path, matches: &Matches) -> Result<Config, String> {
    let cfg_txt = match File::open(path) {
        Ok(mut f) => {
            let mut cfg_txt = String::new();
            f.read_to_string(&mut cfg_txt).unwrap();
            cfg_txt
        }
        Err(e) => return Err(format!("Error opening config: {}", e)),
    };

    let mut p = Parser::new(&cfg_txt);

    match p.parse() {
        Some(table) => {
            debug!("toml parsed successfully. creating config");
            load_config_table(&table)
        }

        None => {
            for err in &p.errors {
                let (loline, locol) = p.to_linecol(err.lo);
                let (hiline, hicol) = p.to_linecol(err.hi);
                println!(
                    "{:?}:{}:{}-{}:{} error: {}",
                    path,
                    loline,
                    locol,
                    hiline,
                    hicol,
                    err.desc
                );
            }
            Err("failed to load config".to_owned())
        }
    }
}

fn load_config_table(table: &BTreeMap<String, Value>) -> Result<Config, String> {

    let mut config = Config::default();

    if let Some(&Table(ref general)) = table.get("fuzz") {
        if let Some(v) = general.get("enable").and_then(|k| k.as_bool()) {
            config.set_fuzz(v as bool);
        }
    }

    if let Some(&Table(ref general)) = table.get("fuzz") {
        if let Some(v) = general.get("cores").and_then(|k| k.as_integer()) {
            config.set_fuzz_cores(v as usize);
        }
    }

    if let Some(&Table(ref general)) = table.get("fuzz") {
        if let Some(v) = general.get("seconds").and_then(|k| k.as_integer()) {
            config.set_fuzz_seconds(v as usize);
        }
    }

    if let Some(&Table(ref general)) = table.get("cross") {
        if let Some(v) = general.get("enable").and_then(|k| k.as_bool()) {
            config.set_cross(v as bool);
        }
    }

    Ok(config)
}

fn main() {
    let options = init();

    // initialize logging
    set_log_level(options.opt_count("verbose"));
    info!("{} {}", PROGRAM, VERSION);

    let config = load_config(Path::new(".crucible.toml"), &options).unwrap_or(Config::default());

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

        // cross target tests
        for target in targets {
            info!("target: {}", target);
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
