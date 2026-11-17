//! Persistent queue coordination and foreground service entry point.

use std::{
    error::Error,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::{Parser, Subcommand};
use download_engine::Downloader;
use download_protocol::{Add, Added, JobId, JobPage, Retry, Uuid};
use tokio::{sync::Notify, task::JoinSet};

#[cfg(unix)]
mod ipc;
mod store;
#[cfg(unix)]
mod transfer;

use store::Store;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Parser)]
#[command(about = "Manage and run a persistent queue of public downloads")]
pub struct Args {
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
        output: PathBuf,
        /// Verify the file against a SHA-256 digest before publishing it.
        #[arg(long)]
        sha256: Option<String>,
        #[arg(long)]
        idempotency_key: Option<Uuid>,
    },
    List {
        /// Print one page in JSON.
        #[arg(long)]
        json: bool,
        /// Start after this job ID.
        #[arg(long)]
        after: Option<JobId>,
    },
    Retry {
        id: JobId,
        /// Start over if this job has only a partial checkpoint.
        #[arg(long)]
        restart: bool,
        #[arg(long)]
        idempotency_key: Option<Uuid>,
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

enum Control {
    Add(Add, Uuid),
    List { json: bool, after: Option<JobId> },
    Retry(Retry, Uuid),
}

fn create_state_dir(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    if !fs::symlink_metadata(path)?.is_dir() {
        return Err("state path must be a real directory".into());
    }
    #[cfg(unix)]
    download_platform::local::validate_directory(path)?;
    Ok(())
}

fn lock_instance(directory: &Path) -> Result<File> {
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(directory.join("worker.lock"))?;
    lock.try_lock()
        .map_err(|error| format!("could not acquire the queue worker lock: {error}"))?;
    Ok(lock)
}

async fn work(store: &Store, wake: &Notify, limit: usize, once: bool) -> Result<bool> {
    store.run(|queue| queue.recover()).await?;
    #[cfg(unix)]
    transfer::cleanup_completed(store).await?;
    let downloader = Arc::new(Downloader::new()?);
    let mut active = JoinSet::new();
    let mut failed = false;
    loop {
        while active.len() < limit {
            let Some(job) = store.run(|queue| queue.claim()).await? else {
                break;
            };
            let downloader = Arc::clone(&downloader);
            #[cfg(unix)]
            let store = store.clone();
            active.spawn(async move {
                let id = job.id;
                #[cfg(unix)]
                let result = transfer::run(&store, &downloader, job).await;
                #[cfg(not(unix))]
                let result = downloader
                    .download_verified(
                        &job.url,
                        Path::new(&job.output),
                        std::num::NonZeroUsize::MIN,
                        job.digest.as_ref(),
                    )
                    .await
                    .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>);
                (id, result)
            });
        }
        if active.is_empty() && once {
            return Ok(!failed);
        }
        tokio::select! {
            Some(result) = active.join_next(), if !active.is_empty() => {
                let (id, result) = result?;
                match result {
                    #[cfg(unix)]
                    Ok(()) => {}
                    #[cfg(not(unix))]
                    Ok(bytes) => store.run(move |queue| queue.finish(id, Some(bytes))).await?,
                    Err(error) => {
                        failed = true;
                        eprintln!("Job {id} failed: {error}");
                        store.run(move |queue| queue.finish(id, None)).await?;
                    }
                }
            }
            _ = wake.notified(), if active.len() < limit => {}
            _ = poll_legacy_clients(), if cfg!(not(unix)) && active.len() < limit => {}
        }
    }
}

async fn poll_legacy_clients() {
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
}

pub async fn run(args: Args) -> Result<bool> {
    create_state_dir(&args.state_dir)?;
    match args.command {
        Command::Work { jobs, once } => run_worker(&args.state_dir, jobs.into(), once).await,
        command => {
            let command = match command {
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
                    Control::Add(
                        Add {
                            url,
                            output,
                            sha256,
                        },
                        idempotency_key.unwrap_or_else(Uuid::new_v4),
                    )
                }
                Command::List { json, after } => Control::List { json, after },
                Command::Retry {
                    id,
                    restart,
                    idempotency_key,
                } => Control::Retry(
                    Retry {
                        job_id: id,
                        restart,
                    },
                    idempotency_key.unwrap_or_else(Uuid::new_v4),
                ),
                Command::Work { .. } => unreachable!(),
            };
            control(&args.state_dir, command).await?;
            Ok(true)
        }
    }
}

async fn run_worker(directory: &Path, limit: usize, once: bool) -> Result<bool> {
    let _lock = lock_instance(directory)?;
    #[cfg(unix)]
    let listener = download_platform::local::Listener::bind(directory).await?;
    let (store, thread) = Store::open(&directory.join("queue.sqlite3")).await?;
    let wake = Arc::new(Notify::new());
    #[cfg(unix)]
    let result = tokio::select! {
        result = work(&store, &wake, limit, once) => result,
        result = ipc::serve(listener, &store, &wake) => result.map(|_| true).map_err(Into::into),
    };
    #[cfg(not(unix))]
    let result = work(&store, &wake, limit, once).await;
    close_store(store, thread).await?;
    result
}

async fn close_store(store: Store, thread: std::thread::JoinHandle<()>) -> Result<()> {
    drop(store);
    tokio::task::spawn_blocking(move || thread.join().map_err(|_| "queue storage worker stopped"))
        .await??;
    Ok(())
}

async fn control(directory: &Path, command: Control) -> Result<()> {
    // On Unix, legacy commands go through the worker when it is running.
    // Offline queue setup takes the instance lock before opening SQLite.
    #[cfg(unix)]
    match download_platform::local::connect(directory).await {
        Ok(stream) => {
            let mut client = download_protocol::Client::negotiate(stream).await?;
            match command {
                Control::Add(request, key) => {
                    println!(
                        "{}",
                        client
                            .add(request, key)
                            .await
                            .map_err(|error| mutation_error(error, key))?
                            .job_id
                    )
                }
                Control::Retry(request, key) => {
                    client
                        .retry(request, key)
                        .await
                        .map_err(|error| mutation_error(error, key))?;
                }
                Control::List { json, mut after } => loop {
                    after = print_page(client.list(after).await?, json)?;
                    if after.is_none() {
                        break;
                    }
                },
            }
            return Ok(());
        }
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) => {}
        Err(error) => return Err(error.into()),
    }
    #[cfg(unix)]
    let _lock = lock_instance(directory)?;
    let (store, thread) = Store::open(&directory.join("queue.sqlite3")).await?;
    let result = control_offline(&store, command).await;
    close_store(store, thread).await?;
    result
}

async fn control_offline(store: &Store, command: Control) -> Result<()> {
    match command {
        Control::Add(request, key) => {
            let result = store.run(move |queue| queue.add(request, key)).await?;
            println!("{}", serde_json::from_value::<Added>(result)?.job_id);
        }
        Control::Retry(request, key) => {
            store.run(move |queue| queue.retry(request, key)).await?;
        }
        Control::List { json, mut after } => loop {
            after = print_page(store.run(move |queue| queue.list(after)).await?, json)?;
            if after.is_none() {
                break;
            }
        },
    }
    Ok(())
}

fn print_page(page: JobPage, json: bool) -> Result<Option<JobId>> {
    if json {
        println!("{}", serde_json::to_string(&page)?);
        return Ok(None);
    }
    for job in page.jobs {
        println!("{job}");
    }
    Ok(page.next_cursor)
}

#[cfg(unix)]
fn mutation_error(error: download_protocol::Error, key: Uuid) -> Box<dyn Error + Send + Sync> {
    match error {
        download_protocol::Error::Rejected(ref fault) if !fault.retryable => error.into(),
        _ => format!("{error}. If you retry this command, pass --idempotency-key {key}.").into(),
    }
}
