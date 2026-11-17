use std::{path::PathBuf, process::ExitCode};

use clap::Parser;
#[cfg(unix)]
use clap::Subcommand;
use download_engine::Downloader;
use futures_util::{StreamExt, stream};

#[cfg(unix)]
mod queue;

#[derive(Parser)]
#[command(
    version,
    about = "Download HTTP or HTTPS files",
    subcommand_negates_reqs = true,
    args_conflicts_with_subcommands = true
)]
struct Args {
    /// Source URLs, in the same order as the output options
    #[arg(required = true)]
    urls: Vec<String>,
    /// Destination file in an existing directory. Repeat once per URL.
    #[arg(short, long, required = true)]
    output: Vec<PathBuf>,
    /// Maximum simultaneously active files. Use 1 for sequential downloads.
    #[arg(short, long, default_value_t = 3, value_parser = clap::value_parser!(u16).range(1..=32))]
    jobs: u16,
    /// Concurrent ranges per file for URLs that can safely be requested again
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..=8))]
    ranges: u16,
    #[cfg(unix)]
    #[command(subcommand)]
    command: Option<Command>,
}

#[cfg(unix)]
#[derive(Subcommand)]
enum Command {
    /// Control the queue of a running local service.
    Queue(queue::QueueArgs),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = Args::parse();
    #[cfg(unix)]
    if let Some(Command::Queue(command)) = args.command {
        return match queue::run(command).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        };
    }
    if args.urls.len() != args.output.len() {
        eprintln!("Supply one --output option per source URL.");
        return ExitCode::from(2);
    }
    let downloader = match Downloader::new() {
        Ok(downloader) => downloader,
        Err(error) => {
            eprintln!("Could not initialize downloader: {error}");
            return ExitCode::FAILURE;
        }
    };

    let ranges = std::num::NonZeroUsize::new(usize::from(args.ranges)).unwrap();
    let jobs = args.urls.into_iter().zip(args.output).enumerate();
    let mut results = stream::iter(jobs)
        .map(|(index, (url, path))| {
            let downloader = &downloader;
            async move {
                (
                    index + 1,
                    downloader.download_with_ranges(&url, &path, ranges).await,
                )
            }
        })
        .buffer_unordered(usize::from(args.jobs));

    let mut failed = false;
    while let Some((index, result)) = results.next().await {
        match result {
            Ok(bytes) => println!("Download {index}: saved {bytes} bytes"),
            Err(error) => {
                failed = true;
                eprintln!("Download {index} failed: {error}");
            }
        }
    }
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
