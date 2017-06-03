#[macro_use]
extern crate log;
extern crate getopts;
extern crate json;
extern crate tic;
extern crate tiny_http;

mod logging;
mod metrics;
mod options;
mod webhook;

use logging::set_log_level;
use metrics::Metric;
use options::{PROGRAM, VERSION};
use std::{io, thread};
use tic::Sample;
use tiny_http::{Method, Request, Response};

fn main() {
    let options = options::init();

    // initialize logging
    set_log_level(options.opt_count("verbose"));
    info!("{} {}", PROGRAM, VERSION);

    // initialize metrics
    let mut metrics = metrics::init(options.opt_str("metrics"));
    let mut stats = metrics.get_sender();
    let clock = metrics.get_clocksource();
    thread::spawn(move || { metrics.run(); });

    // initialize http listener
    let http = options.opt_str("http").unwrap_or("0.0.0.0:4567".to_owned());
    let mut server = webhook::Server::configure()
        .listen(http)
        .clock(clock)
        .stats(stats)
        .build()
        .unwrap();
    server.run();
}
