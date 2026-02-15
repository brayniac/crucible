use bytes::Bytes;
use grpc::Metadata;
use tokio::sync::oneshot;

use crate::error::GrpcError;
use crate::response::GrpcResponse;

pub(crate) enum GrpcCommand {
    Unary {
        path: String,
        metadata: Metadata,
        body: Bytes,
        tx: oneshot::Sender<Result<GrpcResponse, GrpcError>>,
    },
}
