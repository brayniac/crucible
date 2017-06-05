extern crate json;

use std::str::FromStr;
use std::fmt;

#[derive(Clone, Debug)]
pub struct Push {
    git_ref: String,
    repo: String,
    clone_url: String,
    sha: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError { _priv: () }

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "provided string is not a valid Push payload".fmt(f)
    }
}

impl FromStr for Push {
    type Err = ParseError;

    fn from_str(payload: &str) -> Result<Push, ParseError> {
        trace!("parse");
        if let Ok(parsed) = json::parse(payload) {
            let git_ref = parsed["ref"].as_str();
            let repo = parsed["repository"]["full_name"].as_str();
            let clone_url = parsed["repository"]["clone_url"].as_str();
            let sha = parsed["after"].as_str();

            if git_ref.is_none() || repo.is_none() || clone_url.is_none() || sha.is_none() {
                return Err(ParseError { _priv: () });
            }

            let event = Push {
                git_ref: git_ref.unwrap().to_owned(),
                repo: repo.unwrap().to_owned(),
                clone_url: clone_url.unwrap().to_owned(),
                sha: sha.unwrap().to_owned(),
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
        use super::Push;

        let payload = include_str!("push.json");
        let push = Push::from_str(payload).unwrap();
        assert_eq!(push.git_ref, "refs/heads/changes");
        assert_eq!(push.repo, "baxterthehacker/public-repo");
        assert_eq!(push.clone_url,
                   "https://github.com/baxterthehacker/public-repo.git");
        assert_eq!(push.sha, "0d1a26e67d8f5eaf1f6ba5c57fc3c7d91ac0fd1c");
    }
}
