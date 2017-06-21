use super::Consumer;
use common::metrics::Metric;
use mpmc::Queue;
use publisher;
use std::default::Default;
use tic::{Clocksource, Sender};
use webhook::event::Event;

pub struct Config {
    pub events: Option<Queue<Event>>,
    pub clock: Option<Clocksource>,
    pub stats: Option<Sender<Metric>>,
    pub publisher: Option<Queue<publisher::Event>>,
    pub repo: Option<String>,
    pub author: Option<String>,
    pub fuzz_seconds: usize,
    pub fuzz_cores: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            events: None,
            clock: None,
            stats: None,
            repo: None,
            author: None,
            publisher: None,
            fuzz_seconds: 60,
            fuzz_cores: 8,
        }
    }
}

impl Config {
    // try to build a `Consumer` from the given `Config`
    pub fn build(self) -> Result<Consumer, &'static str> {
        Consumer::configured(self)
    }

    // set the `Clocksource`
    pub fn clock(mut self, clock: Clocksource) -> Self {
        self.clock = Some(clock);
        self
    }

    // mpmc queue for receiving events
    pub fn events(mut self, queue: Queue<Event>) -> Self {
        self.events = Some(queue);
        self
    }

    // a tic::Sender for stats
    pub fn stats(mut self, sender: Sender<Metric>) -> Self {
        self.stats = Some(sender);
        self
    }

    // mpmc queue for publishing status back
    pub fn publisher(mut self, queue: Queue<publisher::Event>) -> Self {
        self.publisher = Some(queue);
        self
    }

    // set the repo whitelist
    pub fn repo(mut self, repo: String) -> Self {
        self.repo = Some(repo);
        self
    }

    // set the author whitelist
    pub fn author(mut self, author: String) -> Self {
        self.author = Some(author);
        self
    }

    // set the number of parallel fuzz jobs and cores
    pub fn fuzz_cores(mut self, cores: usize) -> Self {
        self.fuzz_cores = cores;
        self
    }

    // set the duration of each fuzz test in seconds
    pub fn fuzz_seconds(mut self, seconds: usize) -> Self {
        self.fuzz_seconds = seconds;
        self
    }
}
