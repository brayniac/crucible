extern crate json;

use std::fmt;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct PullRequest {
    action: String,
    number: u64,
    repository: String,
    sha: String,
    url: String,
}

impl PullRequest {
    pub fn action(&self) -> String {
        self.action.clone()
    }

    pub fn number(&self) -> u64 {
        self.number
    }

    pub fn repo(&self) -> String {
        self.repository.clone()
    }

    pub fn sha(&self) -> String {
        self.sha.clone()
    }

    pub fn url(&self) -> String {
        self.url.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    _priv: (),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "provided string is not a valid Push payload".fmt(f)
    }
}

impl FromStr for PullRequest {
    type Err = ParseError;

    fn from_str(payload: &str) -> Result<PullRequest, ParseError> {
        debug!("parse pull_request");
        if let Ok(parsed) = json::parse(payload) {
            let action = parsed["action"].as_str();
            let number = parsed["number"].as_u64();
            let repository = parsed["repository"]["full_name"].as_str();
            let sha = parsed["pull_request"]["head"]["sha"].as_str();
            let url = parsed["pull_request"]["head"]["repo"]["clone_url"].as_str();

            if action.is_none() || number.is_none() || repository.is_none() {
                return Err(ParseError { _priv: () });
            }

            let event = PullRequest {
                action: action.unwrap().to_owned(),
                number: number.unwrap(),
                repository: repository.unwrap().to_owned(),
                sha: sha.unwrap().to_owned(),
                url: url.unwrap().to_owned(),
            };

            debug!("{:?}", event);
            Ok(event)
        } else {
            Err(ParseError { _priv: () })
        }
    }
}

mod test {
    #[test]
    fn test_parse() {
        use super::PullRequest;
        use std::str::FromStr;

        let payload = include_str!("pull_request.json");
        let event = PullRequest::from_str(payload).unwrap();
        assert_eq!(event.action, "opened");
        assert_eq!(event.number, 1);
    }
}
