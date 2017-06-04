extern crate json;

#[derive(Clone, Debug)]
pub struct Push {
    git_ref: String,
    repo: String,
}

impl Push {
    pub fn from_str(payload: &str) -> Result<Push, &'static str> {
        trace!("parse");
        if let Ok(parsed) = json::parse(payload) {
            let git_ref = parsed["ref"].as_str();
            let repo = parsed["repository"]["full_name"].as_str();
            if git_ref.is_none() {
                return Err("missing 'ref'");
            }
            if repo.is_none() {
                return Err("missing 'repository.full_name'");
            }
            let push = Push {
                git_ref: git_ref.unwrap().to_owned(),
                repo: repo.unwrap().to_owned(),
            };
            debug!("{:?}", push);
            Ok(push)
        } else {
            Err("malformed json")
        }
    }
}

mod test {
    use super::Push;

    #[test]
    fn test_parse() {
        let payload = include_str!("push.json");
        let push = Push::from_str(payload).unwrap();
        assert_eq!(push.git_ref, "refs/heads/changes");
        assert_eq!(push.repo, "baxterthehacker/public-repo");
    }
}
