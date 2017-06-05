extern crate mpmc;

use metrics::Metric;
use mpmc::Queue;
use std::default::Default;
use tic::{Clocksource, Sample, Sender};
use webhook::event;

pub struct Config {
    events: Option<Queue<event::Event>>,
    clock: Option<Clocksource>,
    stats: Option<Sender<Metric>>,
}

impl Config {
    pub fn build(self) -> Result<Consumer, &'static str> {
        Consumer::configured(self)
    }

    pub fn clock(mut self, clock: Clocksource) -> Self {
        self.clock = Some(clock);
        self
    }

    pub fn events(mut self, queue: Queue<event::Event>) -> Self {
        self.events = Some(queue);
        self
    }

    pub fn stats(mut self, sender: Sender<Metric>) -> Self {
        self.stats = Some(sender);
        self
    }
}


impl Default for Config {
    fn default() -> Config {
        Config {
            events: None,
            clock: None,
            stats: None,
        }
    }
}

pub struct Consumer {
    clock: Clocksource,
    stats: Sender<Metric>,
    events: Queue<event::Event>,
}

impl Consumer {
    pub fn configure() -> Config {
        Default::default()
    }

    pub fn configured(config: Config) -> Result<Consumer, &'static str> {
        let events = config.events.clone();
        let clock = config.clock.clone();
        let stats = config.stats.clone();

        if events.is_none() {
            return Err("need events queue");
        }
        if clock.is_none() {
            return Err("need clock");
        }
        if stats.is_none() {
            return Err("need stats");
        }
        Ok(Consumer {
               events: events.unwrap(),
               clock: clock.unwrap(),
               stats: stats.unwrap(),
           })
    }

    fn time(&self) -> u64 {
        self.clock.counter()
    }

    pub fn run(&mut self) {
        if let Some(event) = self.events.pop() {
            let t0 = self.time();
            info!("consume event: {:?}", event);
            // do processing
            let t1 = self.time();
            let _ = self.stats.send(Sample::new(t0, t1, Metric::Processed));
        }
    }
}
