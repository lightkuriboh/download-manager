//! Durable prefixes for a replayable, single-stream service transfer.

use std::{future::Future, io::SeekFrom, time::Duration};

use reqwest::{StatusCode, Url, header};
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
    time::{self, Instant},
};

use crate::{DownloadError, Downloader, allowed_url, ranges::strong_etag, validate_encoding};

const CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct Checkpoint {
    pub offset: u64,
    pub total: u64,
    pub etag: Vec<u8>,
}

impl Downloader {
    /// Write to a service-owned staging file, without publishing it. The caller must
    /// persist a reported checkpoint before treating any prefix as restart-safe.
    pub async fn download_durable<F>(
        &self,
        source: &str,
        file: &mut File,
        resume: Option<Checkpoint>,
        mut record: impl FnMut(Option<Checkpoint>) -> F,
    ) -> Result<u64, DownloadError>
    where
        F: Future<Output = Result<(), DownloadError>>,
    {
        self.download_durable_until(source, file, resume, &mut record, std::future::pending())
            .await?
            .ok_or(DownloadError::Length)
    }

    /// Return early after flushing a validated prefix when the caller requests a stop.
    pub async fn download_durable_until<F>(
        &self,
        source: &str,
        file: &mut File,
        resume: Option<Checkpoint>,
        mut record: impl FnMut(Option<Checkpoint>) -> F,
        stop: impl Future<Output = Result<(), DownloadError>>,
    ) -> Result<Option<u64>, DownloadError>
    where
        F: Future<Output = Result<(), DownloadError>>,
    {
        tokio::pin!(stop);
        let url = Url::parse(source).map_err(|_| DownloadError::InvalidUrl)?;
        if !allowed_url(&url) {
            return Err(DownloadError::InvalidUrl);
        }
        let mut offset = 0;
        let mut resumed_total = None;
        let mut response = if let Some(checkpoint) = resume {
            if checkpoint.offset == 0
                || checkpoint.offset >= checkpoint.total
                || checkpoint.total > i64::MAX as u64
                || file.metadata().await?.len() < checkpoint.offset
            {
                return Err(DownloadError::Length);
            }
            let etag = header::HeaderValue::from_bytes(&checkpoint.etag)
                .map_err(|_| DownloadError::RangeMismatch)?;
            let range = format!("bytes={}-{}", checkpoint.offset, checkpoint.total - 1);
            let response = tokio::select! {
                result = &mut stop => { result?; return Ok(None); }
                response = self.get(url.clone(), Some((&range, &etag))) => response?,
            };
            match response.status() {
                StatusCode::PARTIAL_CONTENT => {
                    let expected = format!(
                        "bytes {}-{}/{}",
                        checkpoint.offset,
                        checkpoint.total - 1,
                        checkpoint.total
                    );
                    if strong_etag(&response) != Some(&etag)
                        || crate::ranges::unique_header(&response, header::CONTENT_RANGE)
                            .map(header::HeaderValue::as_bytes)
                            != Some(expected.as_bytes())
                        || response
                            .content_length()
                            .is_some_and(|n| n != checkpoint.total - checkpoint.offset)
                    {
                        return Err(DownloadError::RangeMismatch);
                    }
                    offset = checkpoint.offset;
                    resumed_total = Some(checkpoint.total);
                    response
                }
                StatusCode::OK => {
                    validate_encoding(&response)?;
                    // Retire the old bytes in storage before overwriting them.
                    record(None).await?;
                    response
                }
                StatusCode::RANGE_NOT_SATISFIABLE => return Err(DownloadError::RangeMismatch),
                other => return Err(DownloadError::Status(other)),
            }
        } else {
            let response = tokio::select! {
                result = &mut stop => { result?; return Ok(None); }
                response = self.get(url.clone(), None) => response?,
            };
            if response.status() != StatusCode::OK {
                return Err(DownloadError::Status(response.status()));
            }
            response
        };
        validate_encoding(&response)?;
        let total = resumed_total.or_else(|| response.content_length());
        if total.is_some_and(|total| total > i64::MAX as u64) {
            return Err(DownloadError::Length);
        }
        // Redirect targets may contain credentials, and an ETag is scoped to a resource.
        let validator = (response.url() == &url)
            .then(|| strong_etag(&response))
            .flatten()
            .map(|header| header.as_bytes().to_owned());

        file.set_len(offset).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        let mut durable = offset;
        let mut deadline = Instant::now() + CHECKPOINT_INTERVAL;
        loop {
            let checkpoint = match (&validator, total) {
                (Some(etag), Some(total)) if offset > durable && offset < total => {
                    Some((etag, total))
                }
                _ => None,
            };
            if let Some((etag, total)) = checkpoint
                && (offset - durable >= CHECKPOINT_BYTES || deadline <= Instant::now())
            {
                save(file, offset, total, etag, &mut record).await?;
                durable = offset;
                continue;
            }
            let chunk = tokio::select! {
                result = &mut stop => {
                    result?;
                    if let Some((etag, total)) = checkpoint {
                        save(file, offset, total, etag, &mut record).await?;
                    } else {
                        file.flush().await?;
                    }
                    return Ok(None);
                }
                _ = time::sleep_until(deadline), if checkpoint.is_some() => continue,
                result = response.chunk() => result?,
            };
            let Some(chunk) = chunk else { break };
            if offset == durable {
                deadline = Instant::now() + CHECKPOINT_INTERVAL;
            }
            offset = offset
                .checked_add(chunk.len() as u64)
                .ok_or(DownloadError::Length)?;
            if offset > total.unwrap_or(i64::MAX as u64) {
                return Err(DownloadError::Length);
            }
            file.write_all(&chunk).await?;
        }
        if total.is_some_and(|total| offset != total) {
            return Err(DownloadError::Length);
        }
        file.flush().await?;
        file.sync_all().await?;
        Ok(Some(offset))
    }
}

async fn save<F>(
    file: &mut File,
    offset: u64,
    total: u64,
    etag: &[u8],
    record: &mut impl FnMut(Option<Checkpoint>) -> F,
) -> Result<(), DownloadError>
where
    F: Future<Output = Result<(), DownloadError>>,
{
    file.flush().await?;
    file.sync_data().await?;
    record(Some(Checkpoint {
        offset,
        total,
        etag: etag.to_vec(),
    }))
    .await
}
