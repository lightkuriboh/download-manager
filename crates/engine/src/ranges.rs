use std::{io::SeekFrom, ops::Range, path::Path};

use futures_util::{StreamExt, TryStreamExt, stream};
use reqwest::{Response, StatusCode, Url, header};
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
};

use crate::{DownloadError, Downloader, copy_body, validate_encoding};

const MIB: u64 = 1024 * 1024;
const CHUNK_BYTES: u64 = 4 * MIB;

struct Representation {
    url: Url,
    etag: header::HeaderValue,
    length: u64,
}

impl Representation {
    fn from_full_response(response: &Response) -> Option<Self> {
        let length = response.content_length()?;
        if !(8 * MIB..=i64::MAX as u64).contains(&length) {
            return None;
        }
        let etag = unique_header(response, header::ETAG)?;
        let bytes = etag.as_bytes();
        let [b'"', inner @ .., b'"'] = bytes else {
            return None;
        };
        if !inner
            .iter()
            .all(|&b| b == 0x21 || (0x23..=0x7e).contains(&b) || b >= 0x80)
        {
            return None;
        }
        Some(Self {
            url: response.url().clone(),
            etag: etag.clone(),
            length,
        })
    }

    fn validate(&self, response: &Response, range: &Range<u64>) -> Result<(), DownloadError> {
        let expected = format!("bytes {}-{}/{}", range.start, range.end - 1, self.length);
        if response.status() != StatusCode::PARTIAL_CONTENT
            || response.url() != &self.url
            || unique_header(response, header::ETAG) != Some(&self.etag)
            || unique_header(response, header::CONTENT_RANGE).map(header::HeaderValue::as_bytes)
                != Some(expected.as_bytes())
        {
            return Err(DownloadError::RangeMismatch);
        }
        validate_encoding(response)
    }
}

fn unique_header(response: &Response, name: header::HeaderName) -> Option<&header::HeaderValue> {
    let values = response.headers().get_all(name);
    let mut values = values.iter();
    let first = values.next()?;
    values.next().is_none().then_some(first)
}

impl Downloader {
    pub(super) async fn transfer(
        &self,
        mut response: Response,
        file: &mut File,
        staged_path: &Path,
        concurrency: usize,
    ) -> Result<u64, DownloadError> {
        let Some(representation) = (concurrency > 1)
            .then(|| Representation::from_full_response(&response))
            .flatten()
        else {
            let expected = response.content_length();
            return copy_body(response, file, expected).await;
        };

        // Keep the first MiB from the initial GET. Resume at the exact offset
        // using a conditional range request before launching additional workers.
        let mut prefix = 0;
        while prefix < MIB {
            let chunk = response.chunk().await?.ok_or(DownloadError::Length)?;
            let keep = (MIB - prefix).min(chunk.len() as u64) as usize;
            file.write_all(&chunk[..keep]).await?;
            prefix += keep as u64;
        }
        drop(response);
        file.flush().await?;
        let first = prefix..(prefix + CHUNK_BYTES).min(representation.length);
        let probe = self.request_range(&representation, &first).await?;
        if probe.status() == StatusCode::OK {
            validate_encoding(&probe)?;
            file.set_len(0).await?;
            file.seek(SeekFrom::Start(0)).await?;
            let expected = probe.content_length();
            return copy_body(probe, file, expected).await;
        }
        representation.validate(&probe, &first)?;
        copy_body(probe, file, Some(first.end - first.start)).await?;
        file.flush().await?;

        let remaining = (first.end..representation.length).step_by(CHUNK_BYTES as usize);
        let representation = &representation;
        stream::iter(remaining)
            .map(|start| async move {
                let range = start..(start + CHUNK_BYTES).min(representation.length);
                let response = self.request_range(representation, &range).await?;
                representation.validate(&response, &range)?;
                let mut writer = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(staged_path)
                    .await?;
                writer.seek(SeekFrom::Start(start)).await?;
                copy_body(response, &mut writer, Some(range.end - start)).await?;
                writer.flush().await?;
                Ok::<(), DownloadError>(())
            })
            .buffer_unordered(concurrency)
            .try_for_each(|()| async { Ok(()) })
            .await?;
        Ok(representation.length)
    }

    async fn request_range(
        &self,
        representation: &Representation,
        range: &Range<u64>,
    ) -> Result<Response, DownloadError> {
        Ok(self
            .client
            .get(representation.url.clone())
            .header(header::ACCEPT_ENCODING, "identity")
            .header(
                header::RANGE,
                format!("bytes={}-{}", range.start, range.end - 1),
            )
            .header(header::IF_RANGE, &representation.etag)
            .send()
            .await?)
    }
}
