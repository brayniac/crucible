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

mod consumer;
mod logging;
mod metrics;
mod options;
mod webhook;
mod publisher;

use logging::set_log_level;
use mpmc::Queue;
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

    // initialize webhook listener
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

    let publish_queue = Queue::with_capacity(1024);

    // initialize the publisher
    let token = options.opt_str("token").expect("--token required");
    let mut publisher = publisher::Publisher::configure()
        .clock(clock.clone())
        .stats(stats.clone())
        .queue(publish_queue.clone())
        .token(token.clone())
        .build()
        .unwrap();
    thread::spawn(move || { publisher.run(); });

    // initialize the event consumer
    let mut consumer_config = consumer::Consumer::configure()
        .clock(clock)
        .stats(stats)
        .events(events)
        .publisher(publish_queue);
    if let Some(repo) = options.opt_str("repo") {
        info!("repo whitelist: {}", repo);
        consumer_config = consumer_config.repo(repo);
    }
    if let Some(author) = options.opt_str("author") {
        info!("author whitelist: {}", author);
        consumer_config = consumer_config.author(author);
    }
    let mut consumer = consumer_config.build().unwrap();
    loop {
        consumer.run();
    }
}
