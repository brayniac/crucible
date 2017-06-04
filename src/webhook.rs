extern crate getopts;
extern crate json;
extern crate tic;
extern crate tiny_http;

use metrics::Metric;
use std::default::Default;
use std::io;
use tic::{Clocksource, Sample, Sender};
use tiny_http::{Header, Method, Request, Response};

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
    #[allow(dead_code)]
    pub fn new() -> Result<Server, &'static str> {
        Config::default().build()
    }

    // non-blocking receive
    pub fn try_recv(&mut self) {
        if let Ok(Some(request)) = self.server.try_recv() {
            trace!("handle request");
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

#[derive(Clone, Copy, Debug)]
pub enum EventType {
    Create,
    PullRequest,
    Push,
    Status,
    Unknown,
}

// actually handle the http request
fn handle_http(mut request: Request) {
    let response = match *request.method() {
        Method::Post => {
            match request.url() {
                "/payload" => {
                    trace!("payload received");
                    let mut content = String::new();
                    request.as_reader().read_to_string(&mut content).unwrap();
                    let event_type = event_type_from_headers(request.headers());
                    trace!("Event type is: {:?}", event_type);
                    match event_type {
                        EventType::Create => handle_create(&content),
                        EventType::PullRequest => handle_pull_request(&content),
                        EventType::Push => handle_push(&content),
                        _ => handle_unknown(&content),
                    }
                }
                _ => Response::empty(404),
            }
        }
        _ => Response::empty(405),
    };

    let _ = request.respond(response);
}

fn event_type_from_headers(headers: &[Header]) -> EventType {
    let mut event_type = EventType::Unknown;
    for header in headers {
        let field = header.field.as_str().as_str();
        if let "X-GitHub-Event" = field {
            let value = header.value.as_str();
            match value {
                "create" => {
                    event_type = EventType::Create;
                }
                "pull_request" => {
                    event_type = EventType::PullRequest;
                }
                "push" => {
                    event_type = EventType::Push;
                }
                "status" => {
                    event_type = EventType::Status;
                }
                _ => {
                    info!("unknown GitHub Event: {}", value);
                }
            }
        }
    }
    event_type
}

pub struct Event {
    git_ref: String,
}

// handle push events
pub fn handle_push(payload: &str) -> Response<io::Empty> {
    debug!("handle push");
    if let Ok(parsed) = json::parse(payload) {
        if let Some(git_ref) = parsed["ref"].as_str() {
            let event = Event { git_ref: git_ref.to_owned() };
            info!("push ref: {}", event.git_ref);
            Response::empty(200)
        } else {
            info!("bad push event");
            handle_unknown(payload)
        }
    } else {
        Response::empty(400)
    }
}

// handle pull request events
pub fn handle_pull_request(payload: &str) -> Response<io::Empty> {
    debug!("handle pull request");
    if let Ok(parsed) = json::parse(payload) {
        if let Some(action) = parsed["action"].as_str() {
            info!("pull request action: {}", action);
            Response::empty(200)
        } else {
            info!("bad pull request event");
            handle_unknown(payload)
        }
    } else {
        Response::empty(400)
    }
}

// handle push events
pub fn handle_create(payload: &str) -> Response<io::Empty> {
    info!("handle create");
    if let Ok(parsed) = json::parse(payload) {
        if let Some(git_ref) = parsed["ref"].as_str() {
            let event = Event { git_ref: git_ref.to_owned() };
            info!("create ref: {}", event.git_ref);
            Response::empty(200)
        } else {
            info!("bad create event");
            handle_unknown(payload)
        }
    } else {
        Response::empty(400)
    }
}

// handle unknown events
pub fn handle_unknown(_: &str) -> Response<io::Empty> {
    debug!("handle unknown");
    Response::empty(200)
}
