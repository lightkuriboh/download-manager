//! Streaming HTTP transfers and validated byte-range assembly.

mod checkpoint;
mod digest;
mod ranges;
mod transport;

pub use checkpoint::Checkpoint;
pub use digest::Sha256;

use transport::{RequestBudget, TransferResponse};

use std::{io, path::Path, time::Duration};

use reqwest::{Client, StatusCode, Url, header, redirect};
use tokio::{fs::File, io::AsyncWriteExt};

#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    #[error("expected an HTTP or HTTPS URL without embedded credentials")]
    InvalidUrl,
    #[error("HTTP transfer failed: {0}")]
    Http(reqwest::Error),
    #[error("expected HTTP 200, received {0}")]
    Status(StatusCode),
    #[error("server returned an unsupported content encoding")]
    ContentEncoding,
    #[error("response body did not match its declared length")]
    Length,
    #[error("range response did not match the requested representation and interval")]
    RangeMismatch,
    #[error("could not record the durable download checkpoint")]
    Checkpoint(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("expected a SHA-256 digest of 64 hexadecimal characters")]
    InvalidDigest,
    #[error("download does not match the expected SHA-256 digest")]
    DigestMismatch,
    #[error("invalid HTTP redirect")]
    Redirect,
    #[error("file was published but its directory could not be synced: {0}")]
    PublicationSync(io::Error),
    #[error("destination already exists")]
    DestinationExists,
    #[error("filesystem operation failed: {0}")]
    Io(#[from] io::Error),
    #[error("filesystem task could not finish: {0}")]
    Task(#[from] tokio::task::JoinError),
}

impl From<reqwest::Error> for DownloadError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error.without_url())
    }
}

pub struct Downloader {
    client: Client,
    budget: RequestBudget,
}

impl Downloader {
    pub fn new() -> Result<Self, DownloadError> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .read_timeout(Duration::from_secs(30))
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .referer(false)
            .redirect(redirect::Policy::none())
            .build()?;
        Ok(Self {
            client,
            budget: RequestBudget::default(),
        })
    }

    /// Download to an explicit path, leaving any existing destination untouched.
    pub async fn download(&self, source: &str, destination: &Path) -> Result<u64, DownloadError> {
        self.download_with_ranges(source, destination, std::num::NonZeroUsize::MIN)
            .await
    }

    /// Allow multiple range requests for a replayable source. The limit is clamped to eight.
    pub async fn download_with_ranges(
        &self,
        source: &str,
        destination: &Path,
        max_ranges: std::num::NonZeroUsize,
    ) -> Result<u64, DownloadError> {
        self.download_verified(source, destination, max_ranges, None)
            .await
    }

    pub async fn download_verified(
        &self,
        source: &str,
        destination: &Path,
        max_ranges: std::num::NonZeroUsize,
        expected: Option<&Sha256>,
    ) -> Result<u64, DownloadError> {
        let url = Url::parse(source).map_err(|_| DownloadError::InvalidUrl)?;
        if !allowed_url(&url) {
            return Err(DownloadError::InvalidUrl);
        }
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if destination.file_name().is_none() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "expected a file path").into());
        }

        let response = self.get(url, None).await?;
        if response.status() != StatusCode::OK {
            return Err(DownloadError::Status(response.status()));
        }
        validate_encoding(&response)?;
        let parent = parent.to_owned();
        let staging = tokio::task::spawn_blocking(move || {
            tempfile::Builder::new()
                .prefix(".downloadctl-")
                .tempdir_in(parent)
        })
        .await??;
        let staged_path = staging.path().join("payload");
        let mut file = File::create_new(&staged_path).await?;
        let written = self
            .transfer(response, &mut file, &staged_path, max_ranges.get().min(8))
            .await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        if let Some(digest) = expected {
            digest.verify(&mut File::open(&staged_path).await?).await?;
        }

        // Both paths are on the destination filesystem. hard_link fails if the
        // target exists, including when another process creates it during transfer.
        let destination = destination.to_owned();
        tokio::task::spawn_blocking(move || {
            #[cfg(unix)]
            let directory = std::fs::File::open(
                destination
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )?;
            let result = std::fs::hard_link(staged_path, destination);
            #[cfg(unix)]
            if result.is_ok() {
                directory
                    .sync_all()
                    .map_err(DownloadError::PublicationSync)?;
            }
            drop(staging);
            match result {
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    Err(DownloadError::DestinationExists)
                }
                result => result.map_err(DownloadError::Io),
            }
        })
        .await??;
        Ok(written)
    }
}

fn validate_encoding(response: &reqwest::Response) -> Result<(), DownloadError> {
    if response
        .headers()
        .get_all(header::CONTENT_ENCODING)
        .iter()
        .any(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
    {
        return Err(DownloadError::ContentEncoding);
    }
    Ok(())
}

async fn copy_body(
    mut response: TransferResponse<'_>,
    file: &mut File,
    expected: Option<u64>,
) -> Result<u64, DownloadError> {
    let mut written = 0_u64;
    while let Some(chunk) = response.chunk().await? {
        written = written
            .checked_add(chunk.len() as u64)
            .ok_or(DownloadError::Length)?;
        if expected.is_some_and(|expected| written > expected) {
            return Err(DownloadError::Length);
        }
        file.write_all(&chunk).await?;
    }
    if expected.is_some_and(|expected| written != expected) {
        return Err(DownloadError::Length);
    }
    Ok(written)
}

fn allowed_url(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url.host().is_some()
        && url.username().is_empty()
        && url.password().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{BufRead, BufReader, Write},
        net::TcpListener,
        thread,
    };

    fn serve(response: impl AsRef<[u8]> + Send + 'static) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/file", listener.local_addr().unwrap());
        let worker = thread::spawn(move || {
            let (mut connection, _) = listener.accept().unwrap();
            connection
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(&mut connection);
            let mut line = String::new();
            loop {
                line.clear();
                if reader.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                    break;
                }
            }
            connection.write_all(response.as_ref()).unwrap();
        });
        (url, worker)
    }

    #[tokio::test]
    async fn resumed_transfer_rejects_mismatched_partial_responses() {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        for reply in [
            "HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\nETag: \"other\"\r\nContent-Range: bytes 3-5/6\r\n\r\nxyz",
            "HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\nETag: \"original\"\r\nContent-Range: bytes 2-4/6\r\n\r\nxyz",
            "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\n\r\n",
        ] {
            let (url, worker) = serve(reply);
            let mut file = File::from_std(tempfile::tempfile().unwrap());
            file.write_all(b"abc").await.unwrap();
            file.flush().await.unwrap();
            let checkpoint = Checkpoint {
                offset: 3,
                total: 6,
                etag: b"\"original\"".to_vec(),
            };
            let error = Downloader::new()
                .unwrap()
                .download_durable(&url, &mut file, Some(checkpoint), |_| async {
                    panic!("invalid range advanced the checkpoint")
                })
                .await
                .unwrap_err();
            assert!(matches!(error, DownloadError::RangeMismatch));
            file.rewind().await.unwrap();
            let mut body = Vec::new();
            file.read_to_end(&mut body).await.unwrap();
            assert_eq!(body, b"abc");
            worker.join().unwrap();
        }
    }

    #[tokio::test]
    async fn full_response_retires_a_checkpoint_before_replacing_its_bytes() {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let (url, worker) =
            serve("HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"new\"\r\n\r\nwxyz");
        let mut file = File::from_std(tempfile::tempfile().unwrap());
        file.write_all(b"abcde").await.unwrap();
        file.flush().await.unwrap();
        let checkpoint = Checkpoint {
            offset: 3,
            total: 6,
            etag: b"\"original\"".to_vec(),
        };
        let mut retired = false;
        let written = Downloader::new()
            .unwrap()
            .download_durable(&url, &mut file, Some(checkpoint), |replacement| {
                assert!(replacement.is_none());
                retired = true;
                std::future::ready(Ok(()))
            })
            .await
            .unwrap();
        assert!(retired);
        assert_eq!(written, 4);
        file.rewind().await.unwrap();
        let mut body = Vec::new();
        file.read_to_end(&mut body).await.unwrap();
        assert_eq!(body, b"wxyz");
        worker.join().unwrap();
    }

    #[tokio::test]
    async fn cross_origin_redirect_omits_source_referrer() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = listener.local_addr().unwrap();
        let redirect = format!(
            "HTTP/1.1 302 Found\r\nLocation: http://{target}/target\r\nContent-Length: 0\r\n\r\n"
        );
        let (source, redirect_server) = serve(redirect.into_bytes());
        let target_server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(&mut socket);
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                    break;
                }
                assert!(!line.to_ascii_lowercase().starts_with("referer:"));
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let result = Downloader::new()
            .unwrap()
            .download(
                &format!("{source}?token=fixture"),
                &dir.path().join("output"),
            )
            .await;
        assert_eq!(result.unwrap(), 0);
        redirect_server.join().unwrap();
        target_server.join().unwrap();
    }

    #[test]
    fn source_policy() {
        for source in ["https://example.com/archive", "http://localhost:8080/"] {
            assert!(allowed_url(&Url::parse(source).unwrap()));
        }
        for source in [
            "file:///etc/example",
            "ftp://example.com/archive",
            "http://name@example.com/",
            "https://name:secret@example.com/",
        ] {
            assert!(!allowed_url(&Url::parse(source).unwrap()));
        }
    }

    #[tokio::test]
    async fn publishes_fixed_and_chunked_bodies() {
        let downloader = Downloader::new().unwrap();
        for response in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello".as_slice(),
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhe\r\n3\r\nllo\r\n0\r\n\r\n",
        ] {
            let (url, worker) = serve(response);
            let dir = tempfile::tempdir().unwrap();
            let destination = dir.path().join("result");
            assert_eq!(downloader.download(&url, &destination).await.unwrap(), 5);
            assert_eq!(std::fs::read(destination).unwrap(), b"hello");
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
            worker.join().unwrap();
        }
    }

    #[tokio::test]
    async fn failures_never_publish_or_leave_staging_files() {
        let downloader = Downloader::new().unwrap();
        for (response, check) in [
            (
                b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhe".as_slice(),
                0,
            ),
            (
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 0\r\n\r\n",
                1,
            ),
            (
                b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n",
                2,
            ),
        ] {
            let (url, worker) = serve(response);
            let dir = tempfile::tempdir().unwrap();
            let error = downloader
                .download(&url, &dir.path().join("result"))
                .await
                .unwrap_err();
            assert!(
                match check {
                    0 => matches!(error, DownloadError::Http(_) | DownloadError::Length),
                    1 => matches!(error, DownloadError::Status(StatusCode::PARTIAL_CONTENT)),
                    _ => matches!(error, DownloadError::ContentEncoding),
                },
                "{error}"
            );
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
            worker.join().unwrap();
        }
    }

    #[tokio::test]
    async fn segmented_download_validates_each_response_before_publication() {
        const SIZE: usize = 14 * 1024 * 1024;
        let payload: Vec<u8> = (0..SIZE).map(|offset| (offset % 251) as u8).collect();
        for changed in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}/file", listener.local_addr().unwrap());
            let expected = payload.clone();
            let server = thread::spawn(move || {
                let mut starts = Vec::new();
                // Initial GET, validation range, and three additional ranges.
                for (index, connection) in (0..5).zip(listener.incoming()) {
                    let mut socket = connection.unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut request = BufReader::new(&mut socket);
                    let mut bounds = None;
                    let mut conditional = false;
                    loop {
                        let mut line = String::new();
                        if request.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                            break;
                        }
                        if let Some(value) = line.strip_prefix("range: bytes=") {
                            let (start, end) = value.trim().split_once('-').unwrap();
                            bounds = Some((
                                start.parse::<usize>().unwrap(),
                                end.parse::<usize>().unwrap(),
                            ));
                        }
                        conditional |= line.trim() == "if-range: \"original\"";
                    }
                    if let Some((start, end)) = bounds {
                        assert!(conditional);
                        starts.push(start);
                        let tag = if changed && index >= 2 {
                            "changed"
                        } else {
                            "original"
                        };
                        write!(socket, "HTTP/1.1 206 Partial Content\r\nConnection: close\r\nETag: \"{tag}\"\r\nContent-Range: bytes {start}-{end}/{SIZE}\r\nContent-Length: {}\r\n\r\n", end - start + 1).unwrap();
                        if changed && index >= 2 {
                            thread::sleep(Duration::from_millis(100));
                            break;
                        }
                        socket.write_all(&expected[start..=end]).unwrap();
                    } else {
                        assert_eq!(index, 0);
                        write!(socket, "HTTP/1.1 200 OK\r\nConnection: close\r\nETag: \"original\"\r\nContent-Length: {SIZE}\r\n\r\n").unwrap();
                        // A segmented client drops the original body after the prefix.
                        socket.write_all(&expected[..1024 * 1024]).unwrap();
                    }
                }
                starts
            });
            let dir = tempfile::tempdir().unwrap();
            let destination = dir.path().join("result");
            let result = Downloader::new()
                .unwrap()
                .download_with_ranges(&url, &destination, std::num::NonZeroUsize::new(3).unwrap())
                .await;
            if changed {
                assert!(matches!(result, Err(DownloadError::RangeMismatch)));
                assert!(!destination.exists());
            } else {
                assert_eq!(result.unwrap(), SIZE as u64);
                assert_eq!(std::fs::read(destination).unwrap(), payload);
            }
            let mut starts = server.join().unwrap();
            if !changed {
                starts.sort_unstable();
                assert_eq!(starts, [1, 5, 9, 13].map(|n| n * 1024 * 1024));
            }
        }
    }

    #[tokio::test]
    async fn existing_file_is_never_replaced() {
        let (url, worker) = serve(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nnew");
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("result");
        std::fs::write(&destination, b"original").unwrap();
        let result = Downloader::new()
            .unwrap()
            .download(&url, &destination)
            .await;
        assert!(matches!(result, Err(DownloadError::DestinationExists)));
        assert_eq!(std::fs::read(destination).unwrap(), b"original");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        worker.join().unwrap();
    }
}
