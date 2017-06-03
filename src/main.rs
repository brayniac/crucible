#[macro_use]
extern crate log;
extern crate getopts;
extern crate json;
extern crate tic;
extern crate tiny_http;

mod logging;
mod metrics;
mod options;

use logging::set_log_level;
use metrics::Metric;
use options::{PROGRAM, VERSION};
use std::{io, thread};
use tic::Sample;
use tiny_http::{Method, Request, Response, Server};

fn main() {
    let options = options::init();

    // initialize logging
    set_log_level(options.opt_count("verbose"));
    info!("{} {}", PROGRAM, VERSION);

    // initialize metrics
    let mut metrics = metrics::init(options.opt_str("metrics"));
    let mut stats = metrics.get_sender();
    let clock = metrics.get_clocksource();
    thread::spawn(move || { metrics.run(); });

    // initialize http listener
    let http = options.opt_str("http").unwrap_or("0.0.0.0:4567".to_owned());
    let server = tiny_http::Server::http(&http).unwrap();
    info!("listening HTTP {}", &http);

    // run the server
    loop {
        if let Ok(Some(request)) = server.try_recv() {
            debug!("handle request");
            let t0 = clock.counter();
            handle_http(request);
            let t1 = clock.counter();
            let _ = stats.send(Sample::new(t0, t1, Metric::Request));
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
				    handle_payload(content)
                }
                _ => Response::empty(404),
            }
        }
        _ => Response::empty(405),
    };

    let _ = request.respond(response);
}

// parse payload
fn handle_payload(payload: String) -> Response<io::Empty> {
	info!("handle payload");
	let parsed = json::parse(&payload).unwrap();
	Response::empty(200)
}