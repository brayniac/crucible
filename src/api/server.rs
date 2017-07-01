use super::Config;
use common::metrics::Metric;
use shuteye;
use std::collections::HashMap;
use std::default::Default;
use std::fs::File;
use std::path::Path;
use std::time::Duration;
use tic::{Clocksource, Sample, Sender};
use tiny_http;
use tiny_http::{Method, Request, Response};

const ONE_MILLISECOND: u32 = 1_000_000;

pub struct Server {
    addr: String,
    server: tiny_http::Server,
    clock: Clocksource,
    stats: Sender<Metric>,
}

impl Server {
    // create a new config
    pub fn configure() -> Config {
        Default::default()
    }

    pub fn configured(config: Config) -> Result<Server, &'static str> {
        let addr = config.get_listen();
        let clock = config.get_clock().clone().unwrap();
        let server = tiny_http::Server::http(&addr.clone()).unwrap();
        let stats = config.get_stats().clone().unwrap();

        Ok(Server {
            addr: addr,
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

    // run the server forever
    pub fn run(&mut self) {
        info!("listening HTTP {}", self.addr);
        let mut index = HashMap::new();
        index.insert("brayniac/histogram".to_owned(), 1);
        index.insert("brayniac/heatmap".to_owned(), 2);
        index.insert("brayniac/waterfall".to_owned(), 3);
        index.insert("brayniac/rpc-perf".to_owned(), 4);
        loop {
            if let Ok(Some(request)) = self.server.try_recv() {
                trace!("handle request");
                let t0 = self.time();
                self.handle_http(&index, request);
                let t1 = self.time();
                self.send_stat(t0, t1, Metric::Request);
            } else {
                shuteye::sleep(Duration::new(0, ONE_MILLISECOND));
            }
        }
    }

    // actually handle the http request
    fn handle_http(&mut self, index: &HashMap<String, usize>, mut request: Request) {
        let method = request.method().as_str().to_owned();
        let url = request.url().to_owned();
        match method.as_str() {
            "GET" => {
                info!("url: {}", url);
                let parts: Vec<&str> = url.split("?").collect();
                let tokens: Vec<&str> = parts[0].split("/").collect();
                if tokens[1] != "api" {
                    error!("tokens: {:?}", tokens);
                    info!("request is not on /api");
                    let response = Response::empty(404);
                    let _ = request.respond(response);
                    return;
                }
                match tokens[2] {
                    "builds" => {
                        if tokens.len() == 3 {
                            get_builds(request);
                            return;
                        }
                        if tokens.len() == 4 {
                            if let Ok(id) = tokens[3].parse::<usize>() {
                                get_build(request, id);
                                return;
                            } else {
                                bad_request(request);
                                return;
                            }
                        }
                    }
                    "repos" => {
                        if tokens.len() == 3 && parts.len() == 1 {
                            get_repos(request);
                            return;
                        }
                        if parts.len() == 2 {
                            if let Ok(params_map) = get_params(url.clone()) {
                                if let Some(id) = get_repo_id(index, params_map) {
                                    get_repo(request, id);
                                } else {
                                    not_found(request);
                                }
                                return;
                            } else {
                                bad_request(request);
                                return;
                            }
                        } else {
                            not_found(request);
                            return;
                        }
                    }
                    _ => {
                        not_found(request);
                        return;
                    }
                }
            }
            _ => {
                bad_request(request);
                return;
            }
        };
    }
}

fn add_headers(mut response: tiny_http::Response<File>) -> tiny_http::Response<File> {
    response.add_header(tiny_http::Header {
        field: "Access-Control-Allow-Origin".parse().unwrap(),
        value: "*".parse().unwrap(),
    });
    response
}

fn get_builds(request: tiny_http::Request) {
    let mut response =
        tiny_http::Response::from_file(File::open(&Path::new("builds.json")).unwrap());
    response = add_headers(response);
    let _ = request.respond(response);
}

fn get_build(request: tiny_http::Request, id: usize) {
    info!("get build {}", id);
    if let Ok(file) = File::open(&Path::new(&format!("builds_{}.json", id))) {
        let mut response = tiny_http::Response::from_file(file);
        response = add_headers(response);
        let _ = request.respond(response);
    } else {
        not_found(request);
    }
}

fn get_repos(request: tiny_http::Request) {
    let mut response =
        tiny_http::Response::from_file(File::open(&Path::new("repos.json")).unwrap());
    response = add_headers(response);
    let _ = request.respond(response);
}

fn get_repo(request: tiny_http::Request, id: usize) {
    info!("get repo {}", id);
    let mut response = tiny_http::Response::from_file(
        File::open(&Path::new(&format!("repos_{}.json", id))).unwrap(),
    );
    response = add_headers(response);
    let _ = request.respond(response);
}

pub fn get_params(url: String) -> Result<HashMap<String, String>, ()> {
    let mut params_map = HashMap::new();
    let parts: Vec<&str> = url.split("?").collect();
    if parts.len() != 2 {
        return Err(());
    }
    let params: Vec<&str> = parts[1].split("&").collect();
    for param in params {
        let param_tokens: Vec<&str> = param.split("=").collect();
        if param_tokens.len() == 2 {
            params_map.insert(param_tokens[0].to_owned(), param_tokens[1].to_owned());
        } else {
            return Err(());
        }
    }
    Ok(params_map)
}



pub fn get_repo_id(
    index: &HashMap<String, usize>,
    params: HashMap<String, String>,
) -> Option<usize> {
    let owner = params.get("owner");
    let name = params.get("name");

    if owner.is_none() || name.is_none() {
        return None;
    }

    let owner = owner.unwrap();
    let name = name.unwrap();

    // TODO: better string sanitization
    let bad_chars = vec![";", "/", "\"", "\'", "\\", ":", ",", "<", ">"];
    for bad_char in &bad_chars {
        if owner.contains(bad_char) || name.contains(bad_char) {
            return None;
        }
    }
    let key = format!("{}/{}", owner, name);

    if let Some(id) = index.get(&key) {
        Some(*id)
    } else {
        None
    }
}

fn not_found(request: tiny_http::Request) {
    let response = Response::empty(404);
    let _ = request.respond(response);
}

fn bad_request(request: tiny_http::Request) {
    let response = Response::empty(405);
    let _ = request.respond(response);
}
