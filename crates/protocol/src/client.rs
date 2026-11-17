use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};

use super::*;

pub struct Client<S>(S);

impl<S: AsyncRead + AsyncWrite + Unpin> Client<S> {
    pub async fn negotiate(stream: S) -> Result<Self, Error> {
        let mut client = Self(stream);
        let hello = Command::Hello(Hello {
            versions: vec![VERSION],
            client_kind: ClientKind::Cli,
            capabilities: Vec::new(),
            required_capabilities: Vec::new(),
        });
        let negotiated: Negotiated =
            tokio::time::timeout(HANDSHAKE_TIMEOUT, client.call(hello, None))
                .await
                .map_err(|_| Error::Timeout)??;
        if negotiated.version != VERSION {
            return Err(Error::ReplyMismatch);
        }
        Ok(client)
    }

    pub async fn add(&mut self, request: Add, key: Uuid) -> Result<Added, Error> {
        self.call(Command::Add(request), Some(key)).await
    }

    pub async fn list(&mut self, after: Option<JobId>) -> Result<JobPage, Error> {
        self.call(Command::List(List { after }), None).await
    }

    pub async fn retry(&mut self, request: Retry, key: Uuid) -> Result<Retried, Error> {
        self.call(Command::Retry(request), Some(key)).await
    }

    pub async fn control(
        &mut self,
        action: JobAction,
        id: JobId,
        key: Uuid,
    ) -> Result<JobTransition, Error> {
        self.call(Command::Control(action, id), Some(key)).await
    }

    async fn call<T: DeserializeOwned>(
        &mut self,
        command: Command,
        idempotency_key: Option<Uuid>,
    ) -> Result<T, Error> {
        let (command, payload) = command.encode()?;
        let request = Request {
            protocol: VERSION,
            request_id: Uuid::new_v4(),
            idempotency_key,
            command,
            payload,
        };
        tokio::time::timeout(COMMAND_TIMEOUT, async {
            write_frame(&mut self.0, &request).await?;
            let reply: Reply<T> = read_frame(&mut self.0).await?;
            if reply.protocol != VERSION || reply.request_id != request.request_id {
                return Err(Error::ReplyMismatch);
            }
            match (reply.ok, reply.result, reply.error) {
                (true, Some(result), None) => Ok(result),
                (false, None, Some(error)) => Err(Error::Rejected(error)),
                _ => Err(Error::ReplyMismatch),
            }
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
}
