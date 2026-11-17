#![cfg(unix)]

use std::{
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use download_platform::local;
use download_protocol::{Add, Client, JobSummary, Phase, Uuid};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, UnixStream},
    time::{sleep, timeout},
};

const MIB: usize = 1 << 20;

struct Worker(Child);

impl Worker {
    fn start(state: &Path) -> Self {
        Self(
            Command::new(env!("CARGO_BIN_EXE_download-managerd"))
                .arg("--state-dir")
                .arg(state)
                .args(["work", "--jobs", "1"])
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        self.0.wait().unwrap();
    }
}

async fn connect(state: &Path) -> Client<UnixStream> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(socket) = local::connect(state).await {
                return Client::negotiate(socket).await.unwrap();
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker did not open its local socket")
}

async fn request(stream: &mut TcpStream) -> String {
    let mut reader = BufReader::new(stream);
    let mut headers = String::new();
    while !headers.ends_with("\r\n\r\n") {
        assert_ne!(reader.read_line(&mut headers).await.unwrap(), 0);
    }
    headers.to_ascii_lowercase()
}

async fn find_job(
    client: &mut Client<UnixStream>,
    predicate: impl Fn(&JobSummary) -> bool,
) -> JobSummary {
    timeout(Duration::from_secs(8), async {
        loop {
            if let Some(job) = client
                .list(None)
                .await
                .unwrap()
                .jobs
                .into_iter()
                .find(&predicate)
            {
                return job;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("queue did not reach the expected state")
}

#[tokio::test]
async fn killed_worker_resumes_only_synced_bytes_and_reclaims_its_socket() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    let output = root.path().join("download.bin");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/download", listener.local_addr().unwrap());
    let body = Arc::new(
        (0..12 * MIB)
            .map(|offset| (offset % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let expected = Arc::clone(&body);
    let origin = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert!(!request(&mut stream).await.contains("\r\nrange:"));
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"stable\"\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        stream.write_all(&body[..9 * MIB]).await.unwrap();
        let _ = stream.read(&mut [0; 1]).await;
        drop(stream);

        let (mut stream, _) = listener.accept().await.unwrap();
        let request = request(&mut stream).await;
        assert!(request.contains("\r\nif-range: \"stable\"\r\n"));
        let (start, end) = request
            .lines()
            .find_map(|line| line.strip_prefix("range: bytes="))
            .and_then(|range| range.split_once('-'))
            .unwrap();
        let start: usize = start.parse().unwrap();
        let end: usize = end.parse().unwrap();
        assert!((8 * MIB..=9 * MIB).contains(&start));
        assert_eq!(end, body.len() - 1);
        stream
            .write_all(
                format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: \"stable\"\r\n\r\n",
                    body.len() - start, body.len(),
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        stream.write_all(&body[start..]).await.unwrap();
        start
    });

    let worker = Worker::start(&state);
    let mut client = connect(&state).await;
    let added = client
        .add(
            Add {
                url,
                output: output.to_str().unwrap().into(),
                sha256: None,
            },
            Uuid::new_v4(),
        )
        .await
        .unwrap();
    let partial = find_job(&mut client, |job| {
        job.durable_bytes.as_deref().is_some_and(|size| size != "0")
    })
    .await;
    assert_eq!(partial.job_id, added.job_id);
    assert!(!output.exists());
    let durable: usize = partial.durable_bytes.unwrap().parse().unwrap();
    drop(worker);

    let stage = std::fs::read_dir(root.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".download-manager-")
        })
        .unwrap();
    let payload = stage.join("payload");
    // Bytes beyond the committed offset include junk from an interrupted write.
    std::fs::OpenOptions::new()
        .write(true)
        .open(payload)
        .unwrap()
        .set_len((expected.len() + 16) as u64)
        .unwrap();

    let _restarted = Worker::start(&state);
    let mut client = connect(&state).await;
    let complete = find_job(&mut client, |job| matches!(job.phase, Phase::Completed)).await;
    assert_eq!(complete.job_id, added.job_id);
    assert_eq!(
        timeout(Duration::from_secs(5), origin)
            .await
            .unwrap()
            .unwrap(),
        durable
    );
    assert_eq!(&std::fs::read(output).unwrap(), expected.as_ref());
    timeout(Duration::from_secs(5), async {
        while stage.exists() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pause_saves_a_prefix_and_cancel_stops_the_resumed_request() {
    use download_protocol::JobAction;

    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    let output = root.path().join("download.bin");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/download", listener.local_addr().unwrap());
    let (range_sent, range_seen) = tokio::sync::oneshot::channel();
    let origin = tokio::spawn(async move {
        let (mut initial, _) = listener.accept().await.unwrap();
        request(&mut initial).await;
        initial
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1048576\r\nETag: \"stable\"\r\n\r\n")
            .await
            .unwrap();
        initial.write_all(&[1; 4096]).await.unwrap();
        assert_eq!(initial.read(&mut [0]).await.unwrap(), 0);

        let (mut resumed, _) = listener.accept().await.unwrap();
        range_sent.send(request(&mut resumed).await).unwrap();
        // Leave the response headers pending until the worker cancels the request.
        assert_eq!(resumed.read(&mut [0]).await.unwrap(), 0);
    });

    let worker = Worker::start(&state);
    let mut client = connect(&state).await;
    let id = client
        .add(
            Add {
                url,
                output: output.to_str().unwrap().into(),
                sha256: None,
            },
            Uuid::new_v4(),
        )
        .await
        .unwrap()
        .job_id;
    timeout(Duration::from_secs(5), async {
        loop {
            let written = std::fs::read_dir(root.path())
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| {
                    entry
                        .path()
                        .join("payload")
                        .metadata()
                        .is_ok_and(|file| file.len() == 4096)
                });
            if written {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let key = Uuid::new_v4();
    let pause = client.control(JobAction::Pause, id, key).await.unwrap();
    assert!(matches!(pause.phase, Phase::Pausing));
    let paused = find_job(&mut client, |job| matches!(job.phase, Phase::Paused)).await;
    assert_eq!(paused.durable_bytes.as_deref(), Some("4096"));
    let replay = client.control(JobAction::Pause, id, key).await.unwrap();
    assert!(matches!(replay.phase, Phase::Pausing));

    drop(worker);
    let _worker = Worker::start(&state);
    let mut client = connect(&state).await;
    find_job(&mut client, |job| matches!(job.phase, Phase::Paused)).await;
    client
        .control(JobAction::Resume, id, Uuid::new_v4())
        .await
        .unwrap();
    let range = timeout(Duration::from_secs(5), range_seen)
        .await
        .unwrap()
        .unwrap();
    assert!(range.contains("\r\nrange: bytes=4096-1048575\r\n"));
    assert!(range.contains("\r\nif-range: \"stable\"\r\n"));
    client
        .control(JobAction::Cancel, id, Uuid::new_v4())
        .await
        .unwrap();
    find_job(&mut client, |job| matches!(job.phase, Phase::Canceled)).await;
    assert!(!output.exists());
    timeout(Duration::from_secs(5), origin)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn publishes_only_when_the_queued_sha256_matches() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/download", listener.local_addr().unwrap());
    let origin = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            request(&mut stream).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc")
                .await
                .unwrap();
        }
    });

    let _worker = Worker::start(&state);
    let mut client = connect(&state).await;
    for (name, sha256, should_publish) in [
        ("mismatch", "0".repeat(64), false),
        (
            "match",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
            true,
        ),
    ] {
        let output = root.path().join(name);
        let id = client
            .add(
                Add {
                    url: url.clone(),
                    output: output.to_str().unwrap().into(),
                    sha256: Some(sha256),
                },
                Uuid::new_v4(),
            )
            .await
            .unwrap()
            .job_id;
        let job = find_job(&mut client, |job| {
            job.job_id == id && matches!(job.phase, Phase::Completed | Phase::Failed)
        })
        .await;
        assert_eq!(matches!(job.phase, Phase::Completed), should_publish);
        if should_publish {
            assert_eq!(std::fs::read(output).unwrap(), b"abc");
        } else {
            assert!(!output.exists());
        }
    }
    timeout(Duration::from_secs(5), origin)
        .await
        .unwrap()
        .unwrap();
}
