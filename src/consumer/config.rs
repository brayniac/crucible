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

impl Config {
    pub fn build(self) -> Result<Consumer, &'static str> {
        Consumer::configured(self)
    }

    pub fn clock(mut self, clock: Clocksource) -> Self {
        self.clock = Some(clock);
        self
    }

    pub fn events(mut self, queue: Queue<Event>) -> Self {
        self.events = Some(queue);
        self
    }

    pub fn stats(mut self, sender: Sender<Metric>) -> Self {
        self.stats = Some(sender);
        self
    }

    pub fn publisher(mut self, queue: Queue<publisher::Event>) -> Self {
        self.publisher = Some(queue);
        self
    }

    pub fn repo(mut self, repo: String) -> Self {
        self.repo = Some(repo);
        self
    }

    pub fn author(mut self, author: String) -> Self {
        self.author = Some(author);
        self
    }

    pub fn fuzz_cores(mut self, cores: usize) -> Self {
        self.fuzz_cores = cores;
        self
    }

    pub fn fuzz_seconds(mut self, seconds: usize) -> Self {
        self.fuzz_seconds = seconds;
        self
    }
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
