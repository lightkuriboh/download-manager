//! Streaming HTTP transfers and validated byte-range assembly.

mod ranges;

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
            .redirect(redirect::Policy::custom(|attempt| {
                let url = attempt.url();
                if attempt.previous().len() > 10 {
                    return attempt.error("too many redirects");
                }
                if !allowed_url(url)
                    || (url.scheme() == "http"
                        && attempt
                            .previous()
                            .last()
                            .is_some_and(|url| url.scheme() == "https"))
                {
                    return attempt.error("redirect violates URL policy");
                }
                attempt.follow()
            }))
            .build()?;
        Ok(Self { client })
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

        let response = self
            .client
            .get(url)
            .header(header::ACCEPT_ENCODING, "identity")
            .send()
            .await?;
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

        // Both paths are on the destination filesystem. hard_link fails if the
        // target exists, including when another process creates it during transfer.
        let destination = destination.to_owned();
        tokio::task::spawn_blocking(move || {
            let result = std::fs::hard_link(staged_path, destination);
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
    mut response: reqwest::Response,
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

    fn serve(response: &'static [u8]) -> (String, thread::JoinHandle<()>) {
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
            connection.write_all(response).unwrap();
        });
        (url, worker)
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
