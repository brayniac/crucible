extern crate json;

use std::fmt;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct Create {
    git_ref: String,
    ref_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError { _priv: () }

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "provided string is not a valid Push payload".fmt(f)
    }
}

impl FromStr for Create {
    type Err = ParseError;

    fn from_str(payload: &str) -> Result<Create, ParseError> {
        debug!("parse");
        if let Ok(parsed) = json::parse(payload) {
            let git_ref = parsed["ref"].as_str();
            let ref_type = parsed["ref_type"].as_str();

            if git_ref.is_none() || ref_type.is_none() {
                return Err(ParseError { _priv: () });
            }

            let event = Create {
                git_ref: git_ref.unwrap().to_owned(),
                ref_type: ref_type.unwrap().to_owned(),
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
        use super::Create;

        let payload = include_str!("create.json");
        let event = Create::from_str(payload).unwrap();
        assert_eq!(event.git_ref, "0.0.1");
        assert_eq!(event.ref_type, "tag");
    }
}
