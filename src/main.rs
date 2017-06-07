#[macro_use]
extern crate log;
#[macro_use]
extern crate json;
extern crate curl;
extern crate getopts;
extern crate mktemp;
extern crate mpmc;
extern crate shuteye;
extern crate tic;
extern crate tiny_http;

mod consumer;
mod logging;
mod metrics;
mod options;
mod webhook;

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
    let stats = metrics.get_sender();
    let clock = metrics.get_clocksource();
    thread::spawn(move || { metrics.run(); });

    // initialize http listener
    let http = options
        .opt_str("http")
        .unwrap_or_else(|| "0.0.0.0:4567".to_owned());
    let mut server = webhook::Server::configure()
        .listen(http)
        .clock(clock.clone())
        .stats(stats.clone())
        .build()
        .unwrap();
    let events = server.get_events();
    thread::spawn(move || { server.run(); });

    // initialize the event consumer
    let token = options.opt_str("token").expect("--token required");
    let mut consumer = consumer::Consumer::configure()
        .clock(clock)
        .stats(stats)
        .events(events)
        .token(token)
        .build()
        .unwrap();
    loop {
        consumer.run();
    }
}
