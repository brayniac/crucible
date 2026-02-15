use bytes::Bytes;

pub struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Bytes,
}

impl HttpResponse {
    pub(crate) fn new(status: u16, headers: Vec<(String, String)>, body: Bytes) -> Self {
        Self {
            status,
            headers,
            body,
        }
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    /// Case-insensitive header lookup.
    pub fn header(&self, name: &str) -> Option<&str> {
        let lower = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k.to_ascii_lowercase() == lower)
            .map(|(_, v)| v.as_str())
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn into_body(self) -> Bytes {
        self.body
    }
}
