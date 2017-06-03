extern crate getopts;

use getopts::Options;
use std::{env, process};

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
pub const PROGRAM: &'static str = env!("CARGO_PKG_NAME");

fn print_usage(program: &str, opts: &Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn opts() -> Options {
    let mut opts = Options::new();

    opts.optflag("", "version", "show version and exit");
    opts.optopt("", "metrics", "listen address for stats", "IP:PORT");
    opts.optflagmulti("v", "verbose", "verbosity (stacking)");
    opts.optflag("h", "help", "print this help menu");

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
