extern crate json;

#[derive(Clone, Debug)]
pub struct Push {
    git_ref: String,
}

impl Push {
    pub fn from_str(payload: &str) -> Result<Push, &'static str> {
        debug!("parse push");
        if let Ok(parsed) = json::parse(payload) {
            if let Some(git_ref) = parsed["ref"].as_str() {
                let push = Push { git_ref: git_ref.to_owned() };
                debug!("{:?}", push);
                Ok(push)
            } else {
                Err("malformed event")
            }
        } else {
            Err("malformed json")
        }
    }
}
