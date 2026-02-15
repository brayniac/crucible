use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
    Head,
    Patch,
}

impl Method {
    pub fn as_bytes(&self) -> &'static [u8] {
        match self {
            Method::Get => b"GET",
            Method::Post => b"POST",
            Method::Put => b"PUT",
            Method::Delete => b"DELETE",
            Method::Head => b"HEAD",
            Method::Patch => b"PATCH",
        }
    }
}

pub struct Request {
    pub method: Method,
    pub path: Bytes,
    pub headers: Vec<(Bytes, Bytes)>,
    pub body: Option<Bytes>,
}

impl Request {
    pub fn get(path: &str) -> Self {
        Self {
            method: Method::Get,
            path: Bytes::copy_from_slice(path.as_bytes()),
            headers: Vec::new(),
            body: None,
        }
    }

    pub fn post(path: &str, body: impl Into<Bytes>) -> Self {
        Self {
            method: Method::Post,
            path: Bytes::copy_from_slice(path.as_bytes()),
            headers: Vec::new(),
            body: Some(body.into()),
        }
    }

    pub fn put(path: &str, body: impl Into<Bytes>) -> Self {
        Self {
            method: Method::Put,
            path: Bytes::copy_from_slice(path.as_bytes()),
            headers: Vec::new(),
            body: Some(body.into()),
        }
    }

    pub fn delete(path: &str) -> Self {
        Self {
            method: Method::Delete,
            path: Bytes::copy_from_slice(path.as_bytes()),
            headers: Vec::new(),
            body: None,
        }
    }

    pub fn header(mut self, name: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}
