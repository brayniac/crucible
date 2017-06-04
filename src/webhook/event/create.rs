extern crate json;

#[derive(Clone, Debug)]
pub struct Create {
    git_ref: String,
    ref_type: String,
}

impl Create {
    pub fn from_str(payload: &str) -> Result<Create, &'static str> {
        debug!("parse");
        if let Ok(parsed) = json::parse(payload) {
            let git_ref = parsed["ref"].as_str();
            let ref_type = parsed["ref_type"].as_str();

            if git_ref.is_none() {
                return Err("missing 'ref'");
            }
            if ref_type.is_none() {
                return Err("missing 'ref_type'");
            }

            let event = Create {
                git_ref: git_ref.unwrap().to_owned(),
                ref_type: ref_type.unwrap().to_owned(),
            };

            debug!("{:?}", event);
            Ok(event)
        } else {
            Err("malformed json")
        }
    }
}

mod test {
    use super::Create;

    #[test]
    fn test_parse() {
        let payload = include_str!("create.json");
        let event = Create::from_str(payload).unwrap();
        assert_eq!(event.git_ref, "0.0.1");
        assert_eq!(event.ref_type, "tag");
    }
}