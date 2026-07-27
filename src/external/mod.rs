pub mod intervals_icu;
pub mod strava;

use anyhow::{Result, anyhow};
use reqwest::Response;
use serde::de::DeserializeOwned;
use time::{Date, Duration, OffsetDateTime};

use crate::db;

/// Oldest date a poll-based `fetch` should look back to: `lookback_days` before
/// today, but never earlier than the configured `fetch_cutoff`.
pub(crate) fn fetch_window_start(db_config: &db::Config, lookback_days: u32) -> Date {
    let window_start = OffsetDateTime::now_utc().date() - Duration::days(lookback_days.into());
    match db_config.fetch_cutoff {
        Some(cutoff) => cutoff.max(window_start),
        None => window_start,
    }
}

/// Pass a successful HTTP response through, or turn a non-2xx status into an
/// error that includes the response body.
pub(crate) async fn check_status(res: Response) -> Result<Response> {
    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        return Err(anyhow!("HTTP request failed with status {status}: {body}"));
    }
    Ok(res)
}

/// Deserialize a JSON response body, erroring (with the body) on a non-2xx
/// status.
pub(crate) async fn unwrap_response<T: DeserializeOwned>(res: Response) -> Result<T> {
    Ok(check_status(res).await?.json().await?)
}
