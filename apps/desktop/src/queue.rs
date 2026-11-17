use std::path::PathBuf;

#[cfg(unix)]
use download_protocol::JobAction;
use download_protocol::{JobId, JobPage, Uuid};

#[derive(Clone)]
pub struct Endpoint {
    pub state_dir: PathBuf,
    #[cfg(windows)]
    pub service: PathBuf,
}

#[derive(Clone, Debug)]
pub enum Change {
    Add {
        url: String,
        output: String,
        sha256: Option<String>,
    },
    #[cfg(unix)]
    Control(JobAction, JobId),
    Retry {
        id: JobId,
        restart: bool,
    },
}

#[derive(Clone, Debug)]
pub enum ChangeError {
    Rejected(String),
    Uncertain(String),
}

#[cfg(unix)]
impl Endpoint {
    async fn connect(
        &self,
    ) -> Result<download_protocol::Client<download_platform::local::Stream>, String> {
        let stream = download_platform::local::connect(&self.state_dir)
            .await
            .map_err(|error| format!("Cannot reach the worker: {error}"))?;
        download_protocol::Client::negotiate(stream)
            .await
            .map_err(|error| error.to_string())
    }

    pub async fn list(&self, after: Option<JobId>) -> Result<JobPage, String> {
        self.connect()
            .await?
            .list(after)
            .await
            .map_err(|error| error.to_string())
    }

    pub async fn change(&self, change: Change, key: Uuid) -> Result<(), ChangeError> {
        use download_protocol::{Add, Retry};

        let mut client = self.connect().await.map_err(ChangeError::Rejected)?;
        match change {
            Change::Add {
                url,
                output,
                sha256,
            } => client
                .add(
                    Add {
                        url,
                        output,
                        sha256,
                    },
                    key,
                )
                .await
                .map(|_| ()),
            Change::Control(action, id) => client.control(action, id, key).await.map(|_| ()),
            Change::Retry { id, restart } => client
                .retry(
                    Retry {
                        job_id: id,
                        restart,
                    },
                    key,
                )
                .await
                .map(|_| ()),
        }
        .map_err(|error| match error {
            download_protocol::Error::Rejected(ref fault) if !fault.retryable => {
                ChangeError::Rejected(error.to_string())
            }
            _ => ChangeError::Uncertain(error.to_string()),
        })
    }
}

#[cfg(windows)]
impl Endpoint {
    pub async fn list(&self, after: Option<JobId>) -> Result<JobPage, String> {
        let mut args = vec!["list".into(), "--json".into()];
        if let Some(id) = after {
            args.extend(["--after".into(), id.to_string()]);
        }
        let output = self.invoke(args).await.map_err(|error| match error {
            ChangeError::Rejected(message) | ChangeError::Uncertain(message) => message,
        })?;
        serde_json::from_slice(&output).map_err(|_| "Worker returned an invalid job page".into())
    }

    pub async fn change(&self, change: Change, key: Uuid) -> Result<(), ChangeError> {
        let args = match change {
            Change::Add {
                url,
                output,
                sha256,
            } => {
                let mut args = vec!["add".into(), "--output".into(), output];
                if let Some(sha256) = sha256 {
                    args.extend(["--sha256".into(), sha256]);
                }
                args.extend([
                    "--idempotency-key".into(),
                    key.to_string(),
                    "--".into(),
                    url,
                ]);
                args
            }
            Change::Retry { id, restart } => {
                let mut args = vec![
                    "retry".into(),
                    id.to_string(),
                    "--idempotency-key".into(),
                    key.to_string(),
                ];
                if restart {
                    args.push("--restart".into());
                }
                args
            }
        };
        self.invoke(args).await.map(|_| ())
    }

    async fn invoke(&self, args: Vec<String>) -> Result<Vec<u8>, ChangeError> {
        use std::time::Duration;

        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let output = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::process::Command::new(&self.service)
                .arg("--state-dir")
                .arg(&self.state_dir)
                .args(args)
                .creation_flags(CREATE_NO_WINDOW)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| ChangeError::Uncertain("Worker command timed out".into()))?
        .map_err(|error| {
            ChangeError::Rejected(format!("Cannot run the worker command: {error}"))
        })?;
        if !output.status.success() {
            return Err(ChangeError::Rejected(
                String::from_utf8_lossy(&output.stderr).trim().to_string(),
            ));
        }
        Ok(output.stdout)
    }
}
