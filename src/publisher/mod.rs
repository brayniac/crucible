use curl::easy::{Easy, List};
use metrics::Metric;
use mktemp::Temp;
use mpmc::Queue;
use regex::Regex;
use shuteye::sleep;
use std::default::Default;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use tic::{Clocksource, Sample, Sender};
use webhook::event::*;

mod config;

pub use self::config::Config;

#[derive(Clone, Debug)]
pub enum Event {
    Status(Status),
}

#[derive(Clone, Debug)]
pub struct Status {
    pub repo: String,
    pub sha: String,
    pub state: String,
    pub context: String,
    pub description: String,
    pub url: String,
}

pub struct Publisher {
    clock: Clocksource,
    stats: Sender<Metric>,
    queue: Queue<Event>,
    token: String,
}

impl Publisher {
    pub fn configure() -> Config {
        Default::default()
    }

    pub fn configured(config: Config) -> Result<Publisher, &'static str> {
        let queue = config.queue.clone();
        let clock = config.clock.clone();
        let stats = config.stats.clone();
        let token = config.token.clone();

        if queue.is_none() {
            return Err("need queue");
        }
        if clock.is_none() {
            return Err("need clock");
        }
        if stats.is_none() {
            return Err("need stats");
        }
        if token.is_none() {
            return Err("need token");
        }
        Ok(Publisher {
               queue: queue.unwrap(),
               clock: clock.unwrap(),
               stats: stats.unwrap(),
               token: token.unwrap(),
           })
    }

    fn time(&self) -> u64 {
        self.clock.counter()
    }

    pub fn run(&mut self) {
        loop {
            if let Some(event) = self.queue.pop() {
                let t0 = self.time();
                trace!("consume event: {:?}", event);
                // do processing
                self.handle_event(event);
                let t1 = self.time();
                let _ = self.stats.send(Sample::new(t0, t1, Metric::Processed));
            }
            // call sleep
            sleep(Duration::new(0, 1_000_000));
        }
    }

    fn send_status(&self, status: Status) {
        debug!("set status: {:?}", status);
        //info!("set: {} to: {}", sha, state);
        let endpoint = format!("https://api.github.com/repos/{}/statuses/{}",
                               status.repo,
                               status.sha);
        let auth = format!("Authorization: token {}", self.token);
        let mut list = List::new();
        list.append(&auth).unwrap();
        list.append("content-type: application/json").unwrap();

        let data = object!{
            "state" => status.state,
            "target_url" => status.url,
            "description" => status.description,
            "context" => status.context
        };

        trace!("sending: {}", data);

        let mut handle = Easy::new();
        let _ = handle.useragent("crucible");
        handle.url(&endpoint).unwrap();
        handle.http_headers(list).unwrap();
        handle.post(true).unwrap();
        let mut response = Vec::new();

        let _ = handle.post_fields_copy(data.dump().as_bytes());
        {
            let mut transfer = handle.transfer();
            transfer
                .write_function(|new_data| {
                                    response.extend_from_slice(new_data);
                                    Ok(new_data.len())
                                })
                .unwrap();
            transfer.perform().unwrap();
        }

        trace!("response: {}", forced_string(response));
    }

    fn handle_event(&mut self, event: Event) {
        info!("handle publish event");

        match event {
            Event::Status(status) => {
                self.send_status(status);
            }
        }
    }
}

fn forced_string(input: Vec<u8>) -> String {
    String::from_utf8(input).unwrap_or_else(|_| "invalid utf8".to_owned())
}
