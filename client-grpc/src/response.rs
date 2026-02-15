use bytes::Bytes;
use grpc::{Metadata, Status};

pub struct GrpcResponse {
    status: Status,
    metadata: Metadata,
    body: Bytes,
}

impl GrpcResponse {
    pub(crate) fn new(status: Status, metadata: Metadata, body: Bytes) -> Self {
        Self {
            status,
            metadata,
            body,
        }
    }

    pub fn status(&self) -> &Status {
        &self.status
    }

    pub fn is_ok(&self) -> bool {
        self.status.is_ok()
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn into_body(self) -> Bytes {
        self.body
    }
}
