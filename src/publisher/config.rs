use super::Event;
use super::Publisher;
use common::metrics::Metric;
use mpmc::Queue;
use std::default::Default;
use tic::{Clocksource, Sender};

pub struct Config {
    pub queue: Option<Queue<Event>>,
    pub clock: Option<Clocksource>,
    pub stats: Option<Sender<Metric>>,
    pub token: Option<String>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            queue: None,
            clock: None,
            stats: None,
            token: None,
        }
    }
}

impl Config {
    pub fn build(self) -> Result<Publisher, &'static str> {
        Publisher::configured(self)
    }

    pub fn clock(mut self, clock: Clocksource) -> Self {
        self.clock = Some(clock);
        self
    }

    pub fn queue(mut self, queue: Queue<Event>) -> Self {
        self.queue = Some(queue);
        self
    }

    pub fn stats(mut self, sender: Sender<Metric>) -> Self {
        self.stats = Some(sender);
        self
    }

    pub fn token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }
}
