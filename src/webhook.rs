extern crate getopts;
extern crate json;
extern crate tic;
extern crate tiny_http;

use metrics::Metric;
use std::io;
use std::default::Default;
use tiny_http::{Method, Request, Response};
use tic::{Clocksource, Sender, Sample};

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
		Ok(Server {
			config: config,
			clock: clock,
			stats: stats,
			server: server,
		})
	}

	fn time(&self) -> u64 {
		self.clock.counter()
	}

	fn send_stat(&mut self, t0: u64, t1: u64, metric: Metric) {
		let _ = self.stats.send(Sample::new(t0, t1, metric));
	}

	// create a new webhook server listening on the given address
	pub fn new(addr: &str) -> Result<Server, &'static str> {
		Config::default().build()
	}

	// non-blocking receive
	pub fn try_recv(&mut self) {
        if let Ok(Some(request)) = self.server.try_recv() {
            debug!("handle request");
            let t0 = self.time();
            handle_http(request);
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
}

// actually handle the http request
fn handle_http(mut request: Request) {
    let response = match *request.method() {
        Method::Post => {
            match request.url() {
                "/payload" => {
                    info!("payload received");
                    let mut content = String::new();
                    request.as_reader().read_to_string(&mut content).unwrap();
                    handle_payload(&content)
                }
                _ => Response::empty(404),
            }
        }
        _ => Response::empty(405),
    };

    let _ = request.respond(response);
}

// parse payload
fn handle_payload(payload: &str) -> Response<io::Empty> {
    info!("handle payload");
    let parsed = json::parse(payload).unwrap();
    Response::empty(200)
}