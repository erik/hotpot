pub mod intervals_icu;

use anyhow::{Result, anyhow};
use reqwest::Response;
use serde::de::DeserializeOwned;

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
