use std::{io, sync::Arc};

use download_platform::local::Listener;
use download_protocol::{
    COMMAND_TIMEOUT, Command, CommandError, Fault, HANDSHAKE_TIMEOUT, Negotiated, Reply, Request,
    Uuid, VERSION, read_frame, write_frame,
};
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Notify,
    task::JoinSet,
    time::timeout,
};

use crate::store::{Error, Store};

const MAX_CONNECTIONS: usize = 32;
const CAPABILITIES: &[&str] = &[
    "job.add",
    "job.list",
    "job.retry",
    "job.retry.restart",
    "job.pause",
    "job.resume",
    "job.cancel",
];

pub async fn serve(listener: Listener, store: &Store, wake: &Arc<Notify>) -> io::Result<()> {
    let epoch = Uuid::new_v4();
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            result = listener.accept(), if sessions.len() < MAX_CONNECTIONS => {
                let stream = result?;
                let store = store.clone();
                let wake = Arc::clone(wake);
                sessions.spawn(async move {
                    let _ = session(stream, epoch, store, wake).await;
                });
            }
            _ = sessions.join_next(), if !sessions.is_empty() => {}
        }
    }
}

async fn session(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    epoch: Uuid,
    store: Store,
    wake: Arc<Notify>,
) -> Result<(), download_protocol::Error> {
    let request: Request = timeout(HANDSHAKE_TIMEOUT, read_frame(&mut stream))
        .await
        .map_err(|_| download_protocol::Error::Timeout)??;
    let reply = Reply::new(request.request_id, negotiate(&request, epoch));
    timeout(HANDSHAKE_TIMEOUT, write_frame(&mut stream, &reply))
        .await
        .map_err(|_| download_protocol::Error::Timeout)??;
    if !reply.ok {
        return Ok(());
    }

    loop {
        let request: Request = timeout(COMMAND_TIMEOUT, read_frame(&mut stream))
            .await
            .map_err(|_| download_protocol::Error::Timeout)??;
        let result = timeout(COMMAND_TIMEOUT, dispatch(&request, &store, &wake))
            .await
            .unwrap_or_else(|_| {
                Err(fault(
                    "busy",
                    "queue command timed out; reuse the original key to retry a mutation",
                    true,
                ))
            });
        let reply = Reply::new(request.request_id, result);
        timeout(COMMAND_TIMEOUT, write_frame(&mut stream, &reply))
            .await
            .map_err(|_| download_protocol::Error::Timeout)??;
    }
}

fn negotiate(request: &Request, epoch: Uuid) -> Result<Negotiated, Fault> {
    match Command::decode(request) {
        Ok(Command::Hello(hello)) if request.idempotency_key.is_none() => {
            if request.protocol != VERSION || !hello.versions.contains(&VERSION) {
                return Err(fault(
                    "unsupported_version",
                    "no common protocol version",
                    false,
                ));
            }
            if hello
                .required_capabilities
                .iter()
                .any(|feature| !CAPABILITIES.contains(&feature.as_str()))
            {
                return Err(fault(
                    "unsupported_capability",
                    "service does not support the required client capabilities",
                    false,
                ));
            }
            Ok(Negotiated {
                version: VERSION,
                service_epoch: epoch,
                capabilities: CAPABILITIES
                    .iter()
                    .map(|&feature| feature.to_owned())
                    .collect(),
            })
        }
        _ => Err(fault("expected_hello", "send a hello request first", false)),
    }
}

async fn dispatch(request: &Request, store: &Store, wake: &Arc<Notify>) -> Result<Value, Fault> {
    if request.protocol != VERSION {
        return Err(fault(
            "unsupported_version",
            "protocol changed after hello",
            false,
        ));
    }
    let command = Command::decode(request).map_err(|error| match error {
        CommandError::Unknown => fault("unknown_command", "unknown command", false),
        CommandError::InvalidPayload(_) => {
            fault("invalid_request", "invalid command payload", false)
        }
    })?;
    let result = match (command, request.idempotency_key) {
        (Command::List(list), None) => {
            store
                .run(move |queue| Ok(serde_json::to_value(queue.list(list.after)?)?))
                .await
        }
        (Command::Add(add), Some(key)) => {
            let wake = Arc::clone(wake);
            store
                .run(move |queue| {
                    let result = queue.add(add, key)?;
                    wake.notify_one();
                    Ok(result)
                })
                .await
        }
        (Command::Retry(retry), Some(key)) => {
            let wake = Arc::clone(wake);
            store
                .run(move |queue| {
                    let result = queue.retry(retry, key)?;
                    wake.notify_one();
                    Ok(result)
                })
                .await
        }
        (Command::Control(action, id), Some(key)) => {
            let wake = Arc::clone(wake);
            store
                .run(move |queue| {
                    let result = queue.control(action, id, key)?;
                    wake.notify_one();
                    Ok(result)
                })
                .await
        }
        _ => {
            return Err(fault(
                "invalid_request",
                "unexpected command or idempotency key",
                false,
            ));
        }
    };
    result.map_err(|error| match error {
        Error::Invalid(message) => fault("invalid_request", message, false),
        Error::InvalidState => fault("invalid_state", &error.to_string(), false),
        Error::KeyConflict => fault("idempotency_conflict", &error.to_string(), false),
        Error::QueueFull => fault("queue_full", &error.to_string(), false),
        Error::Sql(_) | Error::Json(_) | Error::Unavailable => {
            fault("storage_unavailable", "queue storage is unavailable", true)
        }
    })
}

fn fault(code: &str, message: &str, retryable: bool) -> Fault {
    Fault {
        code: code.into(),
        message: message.into(),
        retryable,
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use download_protocol::{Add, Client, Error, JobId, Phase, Uuid};
    use tokio::sync::Notify;

    use super::*;

    async fn client(store: Store) -> Client<tokio::io::DuplexStream> {
        let (client, server) = tokio::io::duplex(128);
        tokio::spawn(session(
            server,
            Uuid::new_v4(),
            store,
            Arc::new(Notify::new()),
        ));
        Client::negotiate(client).await.unwrap()
    }

    fn add(path: &Path, name: &str) -> Add {
        Add {
            url: "https://example.com/download".into(),
            output: path.join(name).to_str().unwrap().into(),
            sha256: None,
        }
    }

    #[tokio::test]
    async fn receipts_survive_reconnect_and_storage_restart() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("queue.sqlite3");
        let key = Uuid::new_v4();
        let (store, thread) = Store::open(&database).await.unwrap();
        let mut origin = client(store.clone()).await;
        let id = origin
            .add(add(directory.path(), "one"), key)
            .await
            .unwrap()
            .job_id;
        drop(origin);
        drop(store);
        tokio::task::spawn_blocking(move || thread.join().unwrap())
            .await
            .unwrap();

        let (store, _) = Store::open(&database).await.unwrap();
        let mut reconnect = client(store).await;
        assert_eq!(
            reconnect
                .add(add(directory.path(), "one"), key)
                .await
                .unwrap()
                .job_id,
            id
        );
        assert!(matches!(
            reconnect.add(add(directory.path(), "two"), key).await,
            Err(Error::Rejected(fault)) if fault.code == "idempotency_conflict"
        ));
        let page = reconnect.list(None).await.unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert!(matches!(page.jobs[0].phase, Phase::Queued));
        assert!(
            reconnect
                .list(JobId::new(id.get()))
                .await
                .unwrap()
                .jobs
                .is_empty()
        );
    }
}
