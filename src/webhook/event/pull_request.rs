extern crate json;

#[derive(Clone, Debug)]
pub struct PullRequest {
    action: String,
    number: u64,
}

impl PullRequest {
    pub fn from_str(payload: &str) -> Result<PullRequest, &'static str> {
        debug!("parse pull_request");
        if let Ok(parsed) = json::parse(payload) {
            let action = parsed["action"].as_str();
            let number = parsed["number"].as_u64();

            if action.is_none() {
                return Err("missing 'action'");
            }
            if number.is_none() {
                return Err("missing 'number'");
            }

            let event = PullRequest {
                action: action.unwrap().to_owned(),
                number: number.unwrap(),
            };

            debug!("{:?}", event);
            Ok(event)
        } else {
            Err("malformed json")
        }
    }
}

mod test {
    use super::PullRequest;

    #[test]
    fn test_parse() {
        let payload = include_str!("pull_request.json");
        let event = PullRequest::from_str(payload).unwrap();
        assert_eq!(event.action, "opened");
        assert_eq!(event.number, 1);
    }
}
