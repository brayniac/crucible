extern crate json;

#[derive(Clone, Debug)]
pub struct Create {
    git_ref: String,
}

impl Create {
    pub fn from_str(payload: &str) -> Result<Create, &'static str> {
        debug!("parse create");
        if let Ok(parsed) = json::parse(payload) {
            if let Some(git_ref) = parsed["ref"].as_str() {
                let create = Create { git_ref: git_ref.to_owned() };
                debug!("{:?}", create);
                Ok(create)
            } else {
                Err("malformed event")
            }
        } else {
            Err("malformed json")
        }
    }
}
