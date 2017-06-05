extern crate json;

use std::str::FromStr;
use std::fmt;

#[derive(Clone, Debug)]
pub struct PullRequest {
    action: String,
    number: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError { _priv: () }

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

            if action.is_none() || number.is_none() {
                return Err(ParseError { _priv: () });
            }

            let event = PullRequest {
                action: action.unwrap().to_owned(),
                number: number.unwrap(),
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
