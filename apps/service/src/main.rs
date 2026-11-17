use std::{
    error::Error,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use clap::{Parser, Subcommand};
use download_engine::Downloader;
use rusqlite::{Connection, OptionalExtension, params};
use tokio::task::JoinSet;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Parser)]
#[command(about = "Manage and run a persistent queue of public downloads")]
struct Args {
    /// Private local state directory shared by the worker and its clients.
    #[arg(long)]
    state_dir: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Queue a public HTTP(S) URL for local storage.
    Add {
        url: String,
        #[arg(short, long)]
        output: String,
    },
    List,
    Retry {
        id: i64,
    },
    /// Run the queue worker in the foreground.
    Work {
        #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u16).range(1..=32))]
        jobs: u16,
        /// Exit after draining the available jobs.
        #[arg(long)]
        once: bool,
    },
}

struct Queue(Connection);

struct Job {
    id: i64,
    url: String,
    output: String,
}

impl Queue {
    fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path)?;
        db.busy_timeout(Duration::from_secs(3))?;
        db.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             CREATE TABLE IF NOT EXISTS jobs (
                 id INTEGER PRIMARY KEY,
                 url TEXT NOT NULL,
                 output TEXT NOT NULL,
                 state TEXT NOT NULL CHECK (
                     state IN ('queued', 'running', 'completed', 'failed', 'interrupted')
                 ),
                 bytes INTEGER CHECK (bytes >= 0)
             );",
        )?;
        Ok(Self(db))
    }

    fn claim(&mut self) -> Result<Option<Job>> {
        let tx = self
            .0
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let job = tx
            .query_row(
                "UPDATE jobs SET state = 'running'
             WHERE id = (SELECT id FROM jobs WHERE state = 'queued' ORDER BY id LIMIT 1)
             RETURNING id, url, output",
                [],
                |row| {
                    Ok(Job {
                        id: row.get(0)?,
                        url: row.get(1)?,
                        output: row.get(2)?,
                    })
                },
            )
            .optional()?;
        tx.commit()?;
        Ok(job)
    }

    fn recover(&self) -> Result<()> {
        self.0.execute(
            "UPDATE jobs SET state = 'interrupted' WHERE state = 'running'",
            [],
        )?;
        Ok(())
    }

    fn retry(&self, id: i64) -> Result<()> {
        let affected = self.0.execute(
            "UPDATE jobs SET state = 'queued' WHERE id = ?1 AND state IN ('failed', 'interrupted')",
            [id],
        )?;
        if affected == 0 {
            return Err("only failed or interrupted jobs can be retried".into());
        }
        Ok(())
    }

    fn add(&self, url: &str, output: &str) -> Result<i64> {
        let source = reqwest::Url::parse(url).map_err(|_| "invalid download URL")?;
        if !matches!(source.scheme(), "http" | "https")
            || source.username() != ""
            || source.password().is_some()
            || source.query().is_some()
            || source.fragment().is_some()
        {
            return Err(
                "queue requires a public HTTP(S) URL without credentials, query or fragment".into(),
            );
        }
        let path = std::path::absolute(output)?;
        let name = path.file_name().ok_or("expected a destination filename")?;
        let parent = path
            .parent()
            .ok_or("expected a destination directory")?
            .canonicalize()?;
        let path = parent.join(name);
        let output = path.to_str().ok_or("queue requires a UTF-8 destination")?;
        self.0.execute(
            "INSERT INTO jobs(url, output, state) VALUES (?1, ?2, 'queued')",
            params![source.as_str(), output],
        )?;
        Ok(self.0.last_insert_rowid())
    }

    fn list(&self) -> Result<()> {
        let mut statement = self
            .0
            .prepare("SELECT id, state, output, bytes FROM jobs ORDER BY id")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let state: String = row.get(1)?;
            let output: String = row.get(2)?;
            let bytes: Option<i64> = row.get(3)?;
            println!(
                "{id}\t{state}\t{}\t{output:?}",
                bytes.map_or_else(|| "-".into(), |n| n.to_string())
            );
        }
        Ok(())
    }
}

fn create_state_dir(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        builder.mode(0o700).create(path)?;
        if fs::symlink_metadata(path)?.permissions().mode() & 0o077 != 0 {
            return Err("state directory must have mode 0700 or stricter".into());
        }
    }
    #[cfg(not(unix))]
    builder.create(path)?;
    if !fs::symlink_metadata(path)?.is_dir() {
        return Err("state path must be a real directory".into());
    }
    Ok(())
}

async fn work(mut queue: Queue, directory: &Path, limit: usize, once: bool) -> Result<bool> {
    let lock: File = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(directory.join("worker.lock"))?;
    lock.try_lock()
        .map_err(|error| format!("could not acquire the queue worker lock: {error}"))?;
    queue.recover()?;
    let downloader = Arc::new(Downloader::new()?);
    let mut active = JoinSet::new();
    let mut failed = false;
    loop {
        while active.len() < limit {
            let Some(job) = queue.claim()? else { break };
            let downloader = Arc::clone(&downloader);
            active.spawn(async move {
                let result = downloader.download(&job.url, Path::new(&job.output)).await;
                (job.id, result)
            });
        }
        if active.is_empty() && once {
            return Ok(!failed);
        }
        // Wait briefly for a completion, and periodically check for new submissions.
        if let Ok(Some(result)) =
            tokio::time::timeout(Duration::from_millis(250), active.join_next()).await
        {
            let (id, result) = result?;
            let (state, bytes) = match result {
                Ok(bytes) => ("completed", Some(i64::try_from(bytes)?)),
                Err(error) => {
                    failed = true;
                    eprintln!("Job {id} failed: {error}");
                    ("failed", None)
                }
            };
            queue.0.execute(
                "UPDATE jobs SET state = ?1, bytes = ?2 WHERE id = ?3 AND state = 'running'",
                params![state, bytes, id],
            )?;
        } else if active.is_empty() {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();
    match run(args).await {
        Ok(true) => std::process::ExitCode::SUCCESS,
        Ok(false) => std::process::ExitCode::FAILURE,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<bool> {
    create_state_dir(&args.state_dir)?;
    let queue = Queue::open(&args.state_dir.join("queue.sqlite3"))?;
    match args.command {
        Command::Add { url, output } => println!("{}", queue.add(&url, &output)?),
        Command::List => queue.list()?,
        Command::Retry { id } => queue.retry(id)?,
        Command::Work { jobs, once } => {
            return work(queue, &args.state_dir, jobs.into(), once).await;
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interrupted_work_requires_explicit_retry_and_preserves_pending_jobs() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("queue.sqlite3");
        let output = directory.path().join("file");
        let output = output.to_str().unwrap();
        let mut queue = Queue::open(&database).unwrap();
        assert!(
            queue
                .add("https://example.com/file?signature=secret", output)
                .is_err()
        );
        let first = queue.add("https://example.com/first", output).unwrap();
        let second = queue.add("https://example.com/second", output).unwrap();
        assert_eq!(queue.claim().unwrap().unwrap().id, first);
        assert!(queue.retry(first).is_err());
        drop(queue);

        let mut queue = Queue::open(&database).unwrap();
        queue.recover().unwrap();
        assert_eq!(queue.claim().unwrap().unwrap().id, second);
        assert!(queue.claim().unwrap().is_none());
        queue.retry(first).unwrap();
        assert_eq!(queue.claim().unwrap().unwrap().id, first);
    }
}
