use tiny_http::Request;

mod create;
mod pull_request;
mod push;

pub use self::create::Create;
pub use self::pull_request::PullRequest;
pub use self::push::Push;

#[derive(Clone, Debug)]
pub enum Event {
    Create(Create),
    Delete,
    PullRequest(PullRequest),
    Push(Push),
    Status,
    Unknown,
}

impl Event {
    pub fn from_request(request: &mut Request) -> Event {
        let mut e = Event::Unknown;
        let mut content = String::new();
        request.as_reader().read_to_string(&mut content).unwrap();
        for header in request.headers() {
            let field = header.field.as_str().as_str();
            if let "X-GitHub-Event" = field {
                let value = header.value.as_str();
                match value {
                    "create" => {
                        match Create::from_str(&content) {
                            Ok(i) => {
                                e = Event::Create(i);
                            }
                            Err(e) => {
                                info!("failed to parse create: {}", e);
                            }
                        }
                    }
                    "delete" => {
                        e = Event::Delete;
                    }
                    "pull_request" => {
                        match PullRequest::from_str(&content) {
                            Ok(i) => {
                                e = Event::PullRequest(i);
                            }
                            Err(e) => {
                                info!("failed to parse pull_request: {}", e);
                            }
                        }
                    }
                    "push" => {
                        match Push::from_str(&content) {
                            Ok(i) => {
                                e = Event::Push(i);
                            }
                            Err(e) => {
                                info!("failed to parse push: {}", e);
                            }
                        }
                    }
                    "status" => {
                        e = Event::Status;
                    }
                    _ => {
                        info!("unknown GitHub Event: {}", value);
                    }
                }
            }
        }
        e
    }
}
