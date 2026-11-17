use std::{error::Error, path::PathBuf};

use clap::{Args, Subcommand};
use download_protocol::{Add, Client, JobAction, JobId, Retry, Uuid};

#[derive(Args)]
pub struct QueueArgs {
    /// State directory of the running download-managerd worker.
    #[arg(long)]
    state_dir: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Submit a public HTTP(S) URL.
    Add {
        url: String,
        #[arg(short, long)]
        output: PathBuf,
        /// Verify the file against a SHA-256 digest before publishing it.
        #[arg(long)]
        sha256: Option<String>,
        /// Reuse this key when repeating a submission after an uncertain reply.
        #[arg(long)]
        idempotency_key: Option<Uuid>,
    },
    /// List queued and previously run jobs.
    List,
    /// Stop a download and keep its validated progress.
    Pause(JobControl),
    /// Requeue a paused download.
    Resume(JobControl),
    /// Cancel a download and retain any staged data.
    Cancel(JobControl),
    /// Requeue a failed or interrupted job.
    Retry {
        id: JobId,
        /// Start over if this job has only a partial checkpoint.
        #[arg(long)]
        restart: bool,
        #[arg(long)]
        idempotency_key: Option<Uuid>,
    },
}

#[derive(Args)]
struct JobControl {
    id: JobId,
    #[arg(long)]
    idempotency_key: Option<Uuid>,
}

pub async fn run(args: QueueArgs) -> Result<(), Box<dyn Error>> {
    let stream = download_platform::local::connect(&args.state_dir).await?;
    let mut client = Client::negotiate(stream).await?;
    let args_command = args.command;
    match &args_command {
        Command::Add {
            url,
            output,
            sha256,
            idempotency_key,
        } => {
            let output = std::path::absolute(output)?
                .into_os_string()
                .into_string()
                .map_err(|_| "queue requires a UTF-8 destination")?;
            let key = idempotency_key.unwrap_or_else(Uuid::new_v4);
            let result = client
                .add(
                    Add {
                        url: url.clone(),
                        output,
                        sha256: sha256.clone(),
                    },
                    key,
                )
                .await
                .map_err(|error| mutation_error(error, key))?;
            println!("{}", result.job_id);
        }
        Command::List => {
            let mut after = None;
            loop {
                let page = client.list(after).await?;
                for job in page.jobs {
                    println!("{job}");
                }
                after = page.next_cursor;
                if after.is_none() {
                    break;
                }
            }
        }
        Command::Pause(args) | Command::Resume(args) | Command::Cancel(args) => {
            let key = args.idempotency_key.unwrap_or_else(Uuid::new_v4);
            let action = match &args_command {
                Command::Pause(_) => JobAction::Pause,
                Command::Resume(_) => JobAction::Resume,
                Command::Cancel(_) => JobAction::Cancel,
                _ => unreachable!(),
            };
            let result = client
                .control(action, args.id, key)
                .await
                .map_err(|error| mutation_error(error, key))?;
            println!("{} {}", result.job_id, result.phase);
        }
        Command::Retry {
            id,
            restart,
            idempotency_key,
        } => {
            let key = idempotency_key.unwrap_or_else(Uuid::new_v4);
            client
                .retry(
                    Retry {
                        job_id: *id,
                        restart: *restart,
                    },
                    key,
                )
                .await
                .map_err(|error| mutation_error(error, key))?;
        }
    }
    Ok(())
}

fn mutation_error(error: download_protocol::Error, key: Uuid) -> Box<dyn Error> {
    match error {
        download_protocol::Error::Rejected(ref fault) if !fault.retryable => error.into(),
        _ => format!("{error}. If you retry this command, pass --idempotency-key {key}.").into(),
    }
}
