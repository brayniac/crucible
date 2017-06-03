#[macro_use]
extern crate log;
extern crate getopts;
extern crate tic;

mod logging;
mod metrics;
mod options;

use logging::set_log_level;
use options::{PROGRAM, VERSION};
use std::thread;

fn main() {
    let options = options::init();

    // initialize logging
    set_log_level(options.opt_count("verbose"));
    info!("{} {}", PROGRAM, VERSION);

    // initialize metrics
    let mut metrics = metrics::init(options.opt_str("metrics"));
    thread::spawn(move || { metrics.run(); });

    // put actual program here
}
