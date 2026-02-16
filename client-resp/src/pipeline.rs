//! Pipeline builder for batched command execution.
//!
//! All commands in a pipeline are sent to the same connection (routed by the
//! first key) to maintain ordering.

use protocol_resp::{Request, Value};
use tokio::sync::oneshot;

use crate::Client;
use crate::connection::InFlightCommand;
use crate::error::ClientError;

/// A pipeline accumulates commands and executes them as a batch.
///
/// # Example
///
/// ```no_run
/// # use crucible_resp_client::Client;
/// # async fn example(client: &Client) -> Result<(), crucible_resp_client::ClientError> {
/// let results = client.pipeline()
///     .set(b"k1", b"v1")
///     .set(b"k2", b"v2")
///     .get(b"k1")
///     .execute().await?;
/// # Ok(())
/// # }
/// ```
pub struct Pipeline {
    client: Client,
    commands: Vec<Vec<u8>>,
    first_key: Option<Vec<u8>>,
}

impl Pipeline {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            commands: Vec::new(),
            first_key: None,
        }
    }

    /// Add a custom command to the pipeline.
    pub fn cmd(mut self, key: &[u8], request: &Request<'_>) -> Self {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.commands.push(Client::encode_request(request));
        self
    }

    /// Add a SET command to the pipeline.
    pub fn set(mut self, key: &[u8], value: &[u8]) -> Self {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.commands
            .push(Client::encode_set_request(&Request::set(key, value)));
        self
    }

    /// Add a GET command to the pipeline.
    pub fn get(mut self, key: &[u8]) -> Self {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.commands
            .push(Client::encode_request(&Request::get(key)));
        self
    }

    /// Add a DEL command to the pipeline.
    pub fn del(mut self, key: &[u8]) -> Self {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.commands
            .push(Client::encode_request(&Request::del(key)));
        self
    }

    /// Add an INCR command to the pipeline.
    pub fn incr(mut self, key: &[u8]) -> Self {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.commands
            .push(Client::encode_request(&Request::cmd(b"INCR").arg(key)));
        self
    }

    /// Execute all commands in the pipeline and return their results.
    pub async fn execute(self) -> Result<Vec<Value>, ClientError> {
        if self.commands.is_empty() {
            return Ok(Vec::new());
        }

        let key = self.first_key.as_deref().unwrap_or(b"");
        let shard = self.client.inner.ring.route(key);
        let pool = &self.client.inner.pools[shard];
        let idx =
            pool.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % pool.connections.len();
        let sender = &pool.connections[idx];

        let mut receivers = Vec::with_capacity(self.commands.len());

        for encoded in self.commands {
            let (tx, rx) = oneshot::channel();
            let cmd = InFlightCommand { encoded, tx };
            sender
                .send(cmd)
                .await
                .map_err(|_| ClientError::PoolClosed)?;
            receivers.push(rx);
        }

        let mut results = Vec::with_capacity(receivers.len());
        for rx in receivers {
            let result = if let Some(timeout) = self.client.inner.command_timeout {
                tokio::time::timeout(timeout, rx)
                    .await
                    .map_err(|_| ClientError::Timeout)?
            } else {
                rx.await
            };
            let value = result.map_err(|_| ClientError::RequestCancelled)??;
            if let Value::Error(ref msg) = value {
                return Err(ClientError::Redis(
                    String::from_utf8_lossy(msg).into_owned(),
                ));
            }
            results.push(value);
        }

        Ok(results)
    }
}
