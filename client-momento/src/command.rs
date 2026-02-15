use std::time::Duration;

use tokio::sync::oneshot;

use crate::error::MomentoError;
use protocol_momento::CacheValue;

pub(crate) enum MomentoCommand {
    Get {
        key: Vec<u8>,
        tx: oneshot::Sender<Result<CacheValue, MomentoError>>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Duration,
        tx: oneshot::Sender<Result<(), MomentoError>>,
    },
    Delete {
        key: Vec<u8>,
        tx: oneshot::Sender<Result<(), MomentoError>>,
    },
}
