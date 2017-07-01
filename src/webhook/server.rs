use super::{Event, EventFactory};

use super::Config;
use common::metrics::Metric;
use mpmc;
use mpmc::Queue;
use shuteye;
use std::default::Default;
use std::time::Duration;
use tic::{Clocksource, Sample, Sender};
use tiny_http;
use tiny_http::{Method, Request, Response};

pub struct Server {
    addr: String,
    server: tiny_http::Server,
    clock: Clocksource,
    stats: Sender<Metric>,
    events: Queue<Event>,
    factory: EventFactory,
}

impl Server {
    // create a new config
    pub fn configure() -> Config {
        Default::default()
    }

    pub fn configured(config: Config) -> Result<Server, &'static str> {
        let addr = config.get_listen();
        let clock = config.get_clock().clone().unwrap();
        let events = mpmc::Queue::with_capacity(1024);
        let server = tiny_http::Server::http(&addr.clone()).unwrap();
        let secret = config.get_secret();
        let stats = config.get_stats().clone().unwrap();

        if secret.is_none() {
            return Err("no secret provided to validate webhook signatures");
        }

        let factory = EventFactory::configure()
            .set_secret(secret.unwrap())
            .build()
            .unwrap();

        Ok(Server {
            addr: addr,
            clock: clock,
            stats: stats,
            server: server,
            events: events,
            factory: factory,
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

    // run the server forever
    pub fn run(&mut self) {
        info!("listening HTTP {}", self.addr);
        loop {
            if let Ok(Some(request)) = self.server.try_recv() {
                trace!("handle request");
                let t0 = self.time();
                self.handle_http(request);
                let t1 = self.time();
                self.send_stat(t0, t1, Metric::Request);
            } else {
                shuteye::sleep(Duration::new(0, 1_000_000));
            }
        }
    }

    // actually handle the http request
    fn handle_http(&mut self, mut request: Request) {
        let response = match *request.method() {
            Method::Post => {
                match request.url() {
                    "/payload" => {
                        trace!("payload received");
                        let event = self.factory.create(&mut request);
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
