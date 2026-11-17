//! Version 1 request/reply protocol shared by the local service and its clients.

use std::{io, time::Duration};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::{RawValue, to_raw_value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
pub use uuid::Uuid;

pub const VERSION: u16 = 1;
pub const MAX_FRAME_BYTES: usize = 256 * 1024;
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("local connection failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid local protocol message")]
    InvalidMessage(#[from] serde_json::Error),
    #[error("invalid local protocol frame length")]
    FrameLength,
    #[error("local protocol deadline elapsed")]
    Timeout,
    #[error("reply did not match the request")]
    ReplyMismatch,
    #[error("{0}")]
    Rejected(Fault),
}

pub async fn read_frame<T: DeserializeOwned>(
    stream: &mut (impl AsyncRead + Unpin),
) -> Result<T, Error> {
    let size = stream.read_u32_le().await? as usize;
    if size == 0 || size > MAX_FRAME_BYTES {
        return Err(Error::FrameLength);
    }
    let mut json = vec![0; size];
    stream.read_exact(&mut json).await?;
    Ok(serde_json::from_slice(&json)?)
}

pub async fn write_frame<T: Serialize>(
    stream: &mut (impl AsyncWrite + Unpin),
    message: &T,
) -> Result<(), Error> {
    let json = serde_json::to_vec(message)?;
    if json.is_empty() || json.len() > MAX_FRAME_BYTES {
        return Err(Error::FrameLength);
    }
    stream.write_u32_le(json.len() as u32).await?;
    stream.write_all(&json).await?;
    stream.flush().await?;
    Ok(())
}

// The starter queue uses positive SQLite row IDs. Keep them lossless in JS.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct JobId(i64);

impl JobId {
    pub fn new(id: i64) -> Option<Self> {
        (id > 0).then_some(Self(id))
    }

    pub fn get(self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for JobId {
    type Err = &'static str;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        id.parse::<i64>()
            .ok()
            .and_then(Self::new)
            .filter(|parsed| parsed.to_string() == id)
            .ok_or("expected a positive decimal job ID")
    }
}

impl TryFrom<String> for JobId {
    type Error = &'static str;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        id.parse()
    }
}

impl From<JobId> for String {
    fn from(id: JobId) -> Self {
        id.to_string()
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub protocol: u16,
    pub request_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<Uuid>,
    command: CommandName,
    payload: Box<RawValue>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum CommandName {
    #[serde(rename = "hello")]
    Hello,
    #[serde(rename = "job.add")]
    Add,
    #[serde(rename = "job.list")]
    List,
    #[serde(rename = "job.retry")]
    Retry,
    #[serde(rename = "job.pause")]
    Pause,
    #[serde(rename = "job.resume")]
    Resume,
    #[serde(rename = "job.cancel")]
    Cancel,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Hello {
    pub versions: Vec<u16>,
    pub client_kind: ClientKind,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientKind {
    Cli,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Add {
    pub url: String,
    pub output: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct List {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<JobId>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Retry {
    pub job_id: JobId,
    #[serde(default, skip_serializing_if = "is_false")]
    pub restart: bool,
}

fn is_false(value: &bool) -> bool {
    !value
}

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("unknown command")]
    Unknown,
    #[error("invalid command payload")]
    InvalidPayload(#[from] serde_json::Error),
}

#[derive(Clone, Copy, Debug)]
pub enum JobAction {
    Pause,
    Resume,
    Cancel,
}

impl JobAction {
    pub fn command_name(self) -> &'static str {
        match self {
            Self::Pause => "job.pause",
            Self::Resume => "job.resume",
            Self::Cancel => "job.cancel",
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct JobTarget {
    job_id: JobId,
}

pub enum Command {
    Hello(Hello),
    Add(Add),
    List(List),
    Retry(Retry),
    Control(JobAction, JobId),
}

impl Command {
    fn encode(&self) -> Result<(CommandName, Box<RawValue>), serde_json::Error> {
        let (name, payload) = match self {
            Self::Hello(payload) => (CommandName::Hello, to_raw_value(payload)?),
            Self::Add(payload) => (CommandName::Add, to_raw_value(payload)?),
            Self::List(payload) => (CommandName::List, to_raw_value(payload)?),
            Self::Retry(payload) => (CommandName::Retry, to_raw_value(payload)?),
            Self::Control(action, job_id) => (
                match action {
                    JobAction::Pause => CommandName::Pause,
                    JobAction::Resume => CommandName::Resume,
                    JobAction::Cancel => CommandName::Cancel,
                },
                to_raw_value(&JobTarget { job_id: *job_id })?,
            ),
        };
        Ok((name, payload))
    }

    pub fn decode(request: &Request) -> Result<Self, CommandError> {
        let json = request.payload.get();
        let command = match request.command {
            CommandName::Hello => Self::Hello(serde_json::from_str(json)?),
            CommandName::Add => Self::Add(serde_json::from_str(json)?),
            CommandName::List => Self::List(serde_json::from_str(json)?),
            CommandName::Retry => Self::Retry(serde_json::from_str(json)?),
            CommandName::Pause => Self::Control(
                JobAction::Pause,
                serde_json::from_str::<JobTarget>(json)?.job_id,
            ),
            CommandName::Resume => Self::Control(
                JobAction::Resume,
                serde_json::from_str::<JobTarget>(json)?.job_id,
            ),
            CommandName::Cancel => Self::Control(
                JobAction::Cancel,
                serde_json::from_str::<JobTarget>(json)?.job_id,
            ),
            CommandName::Unknown => return Err(CommandError::Unknown),
        };
        Ok(command)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Negotiated {
    pub version: u16,
    pub service_epoch: Uuid,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Added {
    pub job_id: JobId,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct JobTransition {
    pub job_id: JobId,
    pub phase: Phase,
}

pub type Retried = JobTransition;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Queued,
    Running,
    Completed,
    Failed,
    Pausing,
    Paused,
    Canceling,
    Canceled,
    Interrupted,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let phase = match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Pausing => "pausing",
            Self::Paused => "paused",
            Self::Canceling => "canceling",
            Self::Canceled => "canceled",
            Self::Interrupted => "interrupted",
        };
        formatter.write_str(phase)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobSummary {
    pub job_id: JobId,
    pub phase: Phase,
    pub output: String,
    pub bytes: Option<String>,
    pub durable_bytes: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobPage {
    pub jobs: Vec<JobSummary>,
    pub next_cursor: Option<JobId>,
}

#[derive(Debug, Deserialize, Serialize, thiserror::Error)]
#[error("{message}")]
pub struct Fault {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Reply<T> {
    pub protocol: u16,
    pub request_id: Uuid,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Fault>,
}

impl<T> Reply<T> {
    pub fn new(request_id: Uuid, outcome: Result<T, Fault>) -> Self {
        let (ok, result, error) = match outcome {
            Ok(result) => (true, Some(result), None),
            Err(error) => (false, None, Some(error)),
        };
        Self {
            protocol: VERSION,
            request_id,
            ok,
            result,
            error,
        }
    }
}

mod client;
#[cfg(test)]
mod tests;
pub use client::Client;

impl std::fmt::Display for JobSummary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}\t{}\t{}\t{:?}",
            self.job_id,
            self.phase,
            self.bytes.as_deref().unwrap_or("-"),
            self.output,
        )
    }
}
