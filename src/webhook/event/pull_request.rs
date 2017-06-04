extern crate json;

#[derive(Clone, Debug)]
pub struct PullRequest {
    action: String,
}

impl PullRequest {
    pub fn from_str(payload: &str) -> Result<PullRequest, &'static str> {
        debug!("parse pull_request");
        if let Ok(parsed) = json::parse(payload) {
            if let Some(action) = parsed["action"].as_str() {
                let pr = PullRequest { action: action.to_owned() };
                debug!("{:?}", pr);
                Ok(pr)
            } else {
                Err("malformed event")
            }
        } else {
            Err("malformed json")
        }
    }
}
