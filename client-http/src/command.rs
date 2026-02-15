use bytes::Bytes;
use tokio::sync::oneshot;

use crate::error::HttpError;
use crate::request::Request;
use crate::response::HttpResponse;

pub(crate) enum HttpCommand {
    Request {
        request: Request,
        authority: Bytes,
        scheme: Bytes,
        tx: oneshot::Sender<Result<HttpResponse, HttpError>>,
    },
}
