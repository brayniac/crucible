use hmac;

use hmac::{Hmac, Mac};

use rustc_serialize::hex::ToHex;
use sha_1;
use sha_1::Sha1;
use tiny_http::Request;

mod create;
mod pull_request;
mod push;

pub use self::create::Create;
pub use self::pull_request::PullRequest;
pub use self::push::Push;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub enum Event {
    Create(Create),
    Delete,
    PullRequest(PullRequest),
    Push(Push),
    Status,
    Unknown,
    Invalid,
}

impl Event {
    pub fn from_request(request: &mut Request, secret: &Option<String>) -> Event {
        let mut e = Event::Unknown;
        let mut content = String::new();
        request.as_reader().read_to_string(&mut content).unwrap();
        let mut event_type = None;
        let mut signature = None;

        for header in request.headers() {
            match header.field.as_str().as_str() {
                "X-GitHub-Event" => {
                    event_type = Some(header.value.as_str());
                }
                "X-Hub-Signature" => {
                    signature = Some(header.value.as_str());
                }
                _ => {}
            }
        }

        if let Some(s) = signature {
            // valid signature or return

            if let Some(ref secret) = *secret {
                let mut mac = Hmac::<Sha1>::new(secret.as_bytes());
                mac.input(content.as_bytes());
                let result = mac.result();
                let code = result.code().to_hex();
                let code = "sha1=".to_owned() + &code;
                info!("signature: {}", s);
                info!("calculate: {}", code);

                if code != s {
                    debug!("bad signature received!");
                    return Event::Invalid;
                }
            }

        }

        if event_type.is_none() {
            // return early
            return Event::Invalid;
        }

        let event_type = event_type.unwrap();
        match event_type {
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
                info!("unknown GitHub Event: {}", event_type);
            }
        }
        e
    }

    pub fn repo(&self) -> Option<String> {
        match self {
            &Event::PullRequest(ref pr) => Some(pr.repo()),
            &Event::Push(ref push) => Some(push.repo()),
            _ => None,
        }
    }

    pub fn sha(&self) -> Option<String> {
        match self {
            &Event::PullRequest(ref pr) => Some(pr.sha()),
            &Event::Push(ref push) => Some(push.sha()),
            _ => None,
        }
    }

    pub fn url(&self) -> Option<String> {
        match self {
            &Event::PullRequest(ref pr) => Some(pr.url()),
            &Event::Push(ref push) => Some(push.url()),
            _ => None,
        }
    }

    pub fn author(&self) -> Option<String> {
        match self {
            &Event::PullRequest(ref pr) => Some(pr.author()),
            &Event::Push(ref push) => Some(push.author()),
            _ => None,
        }
    }
}
