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

mod config;
mod consumer;
mod logging;
mod metrics;
mod options;
mod webhook;
mod publisher;

use logging::set_log_level;
use mpmc::Queue;
use options::{PROGRAM, VERSION};
use std::{process, thread};

fn main() {
    let options = options::init();

    // initialize logging
    set_log_level(options.opt_count("verbose"));
    info!("{} {}", PROGRAM, VERSION);

    // load config
    let config = match config::load_config(&options) {
        Ok(c) => c,
        Err(e) => {
            error!("{}", e);
            process::exit(1);
        }
    };

    // initialize metrics
    let mut metrics = metrics::init(Some(config.stats()));
    let stats = metrics.get_sender();
    let clock = metrics.get_clocksource();
    thread::spawn(move || { metrics.run(); });

    // initialize webhook listener
    let http = config.http();
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
    let token = config.token().expect("--token required");
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
        .publisher(publish_queue.clone());
    if let Some(repo) = config.repo() {
        info!("repo whitelist: {}", repo);
        consumer_config = consumer_config.repo(repo);
    }
    if let Some(author) = config.author() {
        info!("author whitelist: {}", author);
        consumer_config = consumer_config.author(author);
    }
    let mut consumer = consumer_config.build().unwrap();
    loop {
        consumer.run();
    }
}
