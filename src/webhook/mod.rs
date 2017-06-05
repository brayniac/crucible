extern crate getopts;
extern crate json;
extern crate mpmc;
extern crate tic;
extern crate tiny_http;

use metrics::Metric;
use mpmc::Queue;
use std::default::Default;
use tic::{Clocksource, Sample, Sender};
use tiny_http::{Method, Request, Response};

pub mod event;

use self::event::Event;

pub struct Config {
    addr: String,
    clock: Option<Clocksource>,
    stats: Option<Sender<Metric>>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            addr: "0.0.0.0:4567".to_owned(),
            clock: None,
            stats: None,
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

    pub fn listen(mut self, addr: String) -> Self {
        self.addr = addr;
        self
    }

    pub fn stats(mut self, sender: Sender<Metric>) -> Self {
        self.stats = Some(sender);
        self
    }
}

pub struct Server {
    config: Config,
    server: tiny_http::Server,
    clock: Clocksource,
    stats: Sender<Metric>,
    events: Queue<Event>,
}

impl Server {
    // create a new config
    pub fn configure() -> Config {
        Default::default()
    }

    fn configured(config: Config) -> Result<Server, &'static str> {
        let server = tiny_http::Server::http(&config.addr).unwrap();
        let stats = config.stats.clone().unwrap();
        let clock = config.clock.clone().unwrap();
        let events = mpmc::Queue::with_capacity(1024);
        Ok(Server {
               config: config,
               clock: clock,
               stats: stats,
               server: server,
               events: events,
           })
    }

    fn time(&self) -> u64 {
        self.clock.counter()
    }

    pub fn get_events(&self) -> mpmc::Queue<Event> {
        self.events.clone()
    }

    fn send_stat(&mut self, t0: u64, t1: u64, metric: Metric) {
        let _ = self.stats.send(Sample::new(t0, t1, metric));
    }

    // create a new webhook server listening on the given address
    #[allow(dead_code)]
    pub fn new() -> Result<Server, &'static str> {
        Config::default().build()
    }

    // non-blocking receive
    pub fn try_recv(&mut self) {
        if let Ok(Some(request)) = self.server.try_recv() {
            trace!("handle request");
            let t0 = self.time();
            self.handle_http(request);
            let t1 = self.time();
            self.send_stat(t0, t1, Metric::Request);
        }
    }

    // run the server forever
    pub fn run(&mut self) {
        info!("listening HTTP {}", self.config.addr);
        loop {
            self.try_recv();
        }
    }

    // actually handle the http request
    fn handle_http(&mut self, mut request: Request) {
        let response = match *request.method() {
            Method::Post => {
                match request.url() {
                    "/payload" => {
                        trace!("payload received");
                        let event = Event::from_request(&mut request);
                        let _ = self.events.push(event);
                        Response::empty(200)
                    }
                    _ => Response::empty(404),
                }
            }
            _ => Response::empty(405),
        };

        let _ = request.respond(response);
    }
}
