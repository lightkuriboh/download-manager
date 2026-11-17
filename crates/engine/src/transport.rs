use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, Weak},
};

use reqwest::{Response, StatusCode, Url, header};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{DownloadError, Downloader, allowed_url};

pub(super) struct RequestBudget {
    global: Semaphore,
    origins: Mutex<HashMap<String, Weak<Semaphore>>>,
}

impl Default for RequestBudget {
    fn default() -> Self {
        Self {
            global: Semaphore::new(24),
            origins: Mutex::default(),
        }
    }
}

impl RequestBudget {
    fn origin(&self, url: &Url) -> Arc<Semaphore> {
        let key = url.origin().ascii_serialization();
        let mut origins = self.origins.lock().expect("request budget lock");
        if let Some(existing) = origins.get(&key).and_then(Weak::upgrade) {
            return existing;
        }
        origins.retain(|_, permits| permits.strong_count() > 0);
        let permits = Arc::new(Semaphore::new(8));
        origins.insert(key, Arc::downgrade(&permits));
        permits
    }
}

pub(super) struct TransferResponse<'a> {
    response: Response,
    _global: tokio::sync::SemaphorePermit<'a>,
    _origin: OwnedSemaphorePermit,
}

impl Deref for TransferResponse<'_> {
    type Target = Response;

    fn deref(&self) -> &Response {
        &self.response
    }
}

impl DerefMut for TransferResponse<'_> {
    fn deref_mut(&mut self) -> &mut Response {
        &mut self.response
    }
}

impl Downloader {
    pub(super) async fn get<'a>(
        &'a self,
        mut url: Url,
        range: Option<(&str, &header::HeaderValue)>,
    ) -> Result<TransferResponse<'a>, DownloadError> {
        for hop in 0..=10 {
            // Claim the origin first so a busy origin cannot occupy all global slots.
            let origin = self
                .budget
                .origin(&url)
                .acquire_owned()
                .await
                .expect("budget stays open");
            let global = self
                .budget
                .global
                .acquire()
                .await
                .expect("budget stays open");
            let mut request = self
                .client
                .get(url.clone())
                .header(header::ACCEPT_ENCODING, "identity");
            if let Some((bounds, validator)) = range {
                request = request
                    .header(header::RANGE, bounds)
                    .header(header::IF_RANGE, validator);
            }
            let response = request.send().await?;
            if range.is_some()
                || !matches!(
                    response.status(),
                    StatusCode::MOVED_PERMANENTLY
                        | StatusCode::FOUND
                        | StatusCode::SEE_OTHER
                        | StatusCode::TEMPORARY_REDIRECT
                        | StatusCode::PERMANENT_REDIRECT
                )
            {
                return Ok(TransferResponse {
                    response,
                    _global: global,
                    _origin: origin,
                });
            }
            let target = response
                .headers()
                .get(header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| url.join(value).ok())
                .filter(|target| {
                    allowed_url(target) && !(url.scheme() == "https" && target.scheme() == "http")
                })
                .ok_or(DownloadError::Redirect)?;
            if hop == 10 {
                return Err(DownloadError::Redirect);
            }
            url = target;
        }
        unreachable!("redirect budget is checked on each hop")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_slots_are_shared_independently_of_path() {
        let budget = RequestBudget::default();
        let first = budget.origin(&Url::parse("http://example.com/one").unwrap());
        let second = budget.origin(&Url::parse("http://example.com:80/two").unwrap());
        let unrelated = budget.origin(&Url::parse("http://example.com:8080/").unwrap());
        let all = first.try_acquire_many(8).unwrap();
        assert!(second.try_acquire().is_err());
        assert!(unrelated.try_acquire().is_ok());
        drop(all);
        assert!(second.try_acquire().is_ok());
    }
}
