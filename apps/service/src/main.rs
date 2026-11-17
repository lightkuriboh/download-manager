use std::process::ExitCode;

use clap::Parser;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match download_managerd::run(download_managerd::Args::parse()).await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
