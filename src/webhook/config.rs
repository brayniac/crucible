

use super::Server;
use common::metrics::Metric;
use std::default::Default;
use tic::{Clocksource, Sender};

pub struct Config {
    addr: String,
    clock: Option<Clocksource>,
    stats: Option<Sender<Metric>>,
    secret: Option<String>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            addr: "0.0.0.0:4567".to_owned(),
            clock: None,
            stats: None,
            secret: None,
        }
    }
}

impl Config {
    pub fn build(self) -> Result<Server, &'static str> {
        Server::configured(self)
    }

    pub fn clock(mut self, clock: Clocksource) -> Self {
        self.clock = Some(clock);
        self
    }

    pub fn get_clock(&self) -> Option<Clocksource> {
        self.clock.clone()
    }

    pub fn listen(mut self, addr: String) -> Self {
        self.addr = addr;
        self
    }

    pub fn get_listen(&self) -> String {
        self.addr.clone()
    }

    pub fn stats(mut self, sender: Sender<Metric>) -> Self {
        self.stats = Some(sender);
        self
    }

    pub fn get_stats(&self) -> Option<Sender<Metric>> {
        self.stats.clone()
    }

    pub fn secret(mut self, secret: Option<String>) -> Self {
        self.secret = secret;
        self
    }

    pub fn get_secret(&self) -> Option<String> {
        self.secret.clone()
    }
}
