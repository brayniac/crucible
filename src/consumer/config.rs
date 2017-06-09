

use super::Consumer;
use metrics::Metric;
use mpmc::Queue;
use std::default::Default;
use tic::{Clocksource, Sender};
use webhook::event::Event;

pub struct Config {
    pub events: Option<Queue<Event>>,
    pub clock: Option<Clocksource>,
    pub stats: Option<Sender<Metric>>,
    pub token: Option<String>,
    pub repo: Option<String>,
    pub author: Option<String>,
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

    pub fn token(mut self, token: String) -> Self {
        self.token = Some(token);
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
}


impl Default for Config {
    fn default() -> Config {
        Config {
            events: None,
            clock: None,
            stats: None,
            token: None,
            repo: None,
            author: None,
        }
    }
}
