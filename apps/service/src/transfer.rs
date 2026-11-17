use std::path::{Path, PathBuf};

use download_engine::{DownloadError, Downloader};
use download_platform::staging::Stage;
use download_protocol::Uuid;

use crate::{
    Result,
    store::{Job, Journal, Store},
};

pub async fn run(store: &Store, downloader: &Downloader, job: Job) -> Result<()> {
    let id = job.id;
    let destination = PathBuf::from(job.output);
    cleanup_retired(store, Some(id)).await?;
    let existing = store.run(move |queue| queue.journal(id)).await?;
    let (stage, mut journal) = if let Some(journal) = existing {
        let path = destination.clone();
        let nonce = journal.nonce.clone();
        let identity = journal.identity;
        let stage =
            tokio::task::spawn_blocking(move || Stage::open(&path, &nonce, &identity)).await??;
        (stage, journal)
    } else {
        open_new_stage(store, id, &destination).await?
    };

    let bytes = if let Some(bytes) = journal.prepared {
        bytes
    } else {
        let mut file = tokio::fs::File::from_std(stage.payload()?);
        let checkpoint_store = store.clone();
        let bytes = downloader
            .download_durable_until(
                &job.url,
                &mut file,
                journal.checkpoint.take(),
                move |checkpoint| {
                    let store = checkpoint_store.clone();
                    async move {
                        store
                            .run(move |queue| queue.checkpoint(id, checkpoint))
                            .await
                            .map_err(|error| DownloadError::Checkpoint(Box::new(error)))
                    }
                },
                stop_requested(store, id),
            )
            .await?;
        let bytes = match (bytes, job.digest.as_ref()) {
            (Some(size), Some(digest)) => tokio::select! {
                result = digest.verify(&mut file) => { result?; Some(size) }
                result = stop_requested(store, id) => { result?; None }
            },
            _ => bytes,
        };
        drop(file);
        if let Some(bytes) = bytes
            && store
                .run(move |queue| queue.prepare_publication(id, bytes))
                .await?
        {
            bytes
        } else {
            store.run(move |queue| queue.finish(id, None)).await?;
            return Ok(());
        }
    };

    tokio::task::spawn_blocking(move || stage.publish(bytes)).await??;
    store
        .run(move |queue| queue.confirm_publication(id, bytes))
        .await?;
    clean_stage(store, id, destination, journal).await;
    Ok(())
}

async fn stop_requested(store: &Store, id: i64) -> std::result::Result<(), DownloadError> {
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        if store
            .run(move |queue| queue.should_stop(id))
            .await
            .map_err(|error| DownloadError::Checkpoint(Box::new(error)))?
        {
            return Ok(());
        }
    }
}

async fn open_new_stage(store: &Store, id: i64, destination: &Path) -> Result<(Stage, Journal)> {
    let path = destination.to_owned();
    let nonce = Uuid::new_v4().simple().to_string();
    let (stage, journal) = tokio::task::spawn_blocking(move || {
        let stage = Stage::create(&path, &nonce)?;
        let identity = stage.identity()?;
        Ok::<_, std::io::Error>((
            stage,
            Journal {
                nonce,
                identity,
                checkpoint: None,
                prepared: None,
            },
        ))
    })
    .await??;
    let nonce = journal.nonce.clone();
    let identity = journal.identity;
    store
        .run(move |queue| queue.record_stage(id, &nonce, &identity))
        .await?;
    Ok((stage, journal))
}

pub async fn cleanup_completed(store: &Store) -> Result<()> {
    cleanup_retired(store, None).await?;
    let mut after = 0;
    loop {
        let page = store
            .run(move |queue| queue.completed_stages(after))
            .await?;
        if page.is_empty() {
            return Ok(());
        }
        for (id, output) in page {
            if let Some(journal) = store.run(move |queue| queue.journal(id)).await? {
                clean_stage(store, id, output.into(), journal).await;
            }
            after = id;
        }
    }
}

async fn cleanup_retired(store: &Store, job_id: Option<i64>) -> Result<()> {
    let mut after = String::new();
    loop {
        let page = store
            .run(move |queue| queue.retired_stages(job_id, &after))
            .await?;
        let Some((last, _, _)) = page.last() else {
            return Ok(());
        };
        after = last.clone();
        for (nonce, output, identity) in page {
            let stage = nonce.clone();
            let result = tokio::task::spawn_blocking(move || {
                Stage::cleanup_completed(Path::new(&output), &stage, &identity)
            })
            .await?;
            if result.is_ok() {
                store
                    .run(move |queue| queue.clear_retired_stage(&nonce))
                    .await?;
            } else {
                eprintln!("Could not safely clean up retired staging directory {nonce}");
            }
        }
    }
}

async fn clean_stage(store: &Store, id: i64, destination: PathBuf, journal: Journal) {
    let result = tokio::task::spawn_blocking(move || {
        Stage::cleanup_completed(&destination, &journal.nonce, &journal.identity)
    })
    .await;
    if matches!(result, Ok(Ok(()))) && store.run(move |queue| queue.clear_stage(id)).await.is_ok() {
        return;
    }
    eprintln!("Job {id}: could not clean up the staged file; will retry at startup");
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use download_protocol::{Add, Added, Phase};

    use super::*;

    #[tokio::test]
    async fn recovers_a_published_file_and_rejects_an_unrelated_destination() {
        for matching in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let database = dir.path().join("queue.sqlite3");
            let output = dir.path().canonicalize().unwrap().join("download");
            let destination = output.to_str().unwrap().to_owned();
            let (store, thread) = Store::open(&database).await.unwrap();
            let receipt = store
                .run(move |queue| {
                    queue.add(
                        Add {
                            url: "https://example.invalid/download".into(),
                            output: destination,
                            sha256: None,
                        },
                        Uuid::new_v4(),
                    )
                })
                .await
                .unwrap();
            let added: Added = serde_json::from_value(receipt).unwrap();
            let job = store.run(|queue| queue.claim()).await.unwrap().unwrap();
            let id = job.id;

            let (stage, journal) = open_new_stage(&store, id, &output).await.unwrap();
            let mut payload = stage.payload().unwrap();
            payload.write_all(b"download").unwrap();
            payload.sync_all().unwrap();
            assert!(
                store
                    .run(move |queue| queue.prepare_publication(id, 8))
                    .await
                    .unwrap()
            );
            assert!(
                store
                    .run(move |queue| queue.control(
                        download_protocol::JobAction::Pause,
                        added.job_id,
                        Uuid::new_v4()
                    ))
                    .await
                    .is_err()
            );
            if matching {
                stage.publish(8).unwrap();
            } else {
                std::fs::write(&output, b"conflict").unwrap();
            }
            drop(payload);
            drop(stage);
            crate::close_store(store, thread).await.unwrap();

            let (store, thread) = Store::open(&database).await.unwrap();
            store.run(|queue| queue.recover()).await.unwrap();
            let job = store.run(|queue| queue.claim()).await.unwrap().unwrap();
            assert_eq!(job.id, added.job_id.get());
            let result = run(&store, &Downloader::new().unwrap(), job).await;
            assert_eq!(result.is_ok(), matching);
            if matching {
                assert_eq!(std::fs::read(&output).unwrap(), b"download");
                assert!(
                    store
                        .run(move |queue| queue.journal(id))
                        .await
                        .unwrap()
                        .is_none()
                );
                let page = store.run(|queue| queue.list(None)).await.unwrap();
                assert!(matches!(page.jobs[0].phase, Phase::Completed));
            } else {
                assert_eq!(std::fs::read(&output).unwrap(), b"conflict");
                let saved = store
                    .run(move |queue| queue.journal(id))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(saved.identity, journal.identity);
                assert_eq!(saved.prepared, Some(8));
            }
            crate::close_store(store, thread).await.unwrap();
        }
    }
}
