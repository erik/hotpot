//! Poll-based activity ingestion from intervals.icu API.

use std::collections::{HashMap, HashSet};
use std::io::Cursor;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use time::{Date, Duration, OffsetDateTime};

use crate::activity::{self, RawActivity};
use crate::date::YYYY_MM_DD;
use crate::db::{self, Database};
use crate::external::{check_status, unwrap_response};
use crate::file::get_file_type;

const BASE_URL: &str = "https://intervals.icu/api/v1";

/// HTTP Basic auth username
const AUTH_USER_KEY: &str = "API_KEY";

#[derive(Clone)]
pub struct IntervalsIcuAuth {
    api_key: String,
}

impl IntervalsIcuAuth {
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("INTERVALS_ICU_API_KEY")
            .ok()
            .filter(|it| !it.is_empty())?;

        Some(Self { api_key })
    }
}

#[derive(Debug, Deserialize)]
pub struct ActivityRef {
    pub id: String,
    pub source: Option<String>,
    pub strava_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(dead_code)]
pub struct IntervalsIcuActivity {
    #[serde(skip_serializing)]
    name: Option<String>,

    #[serde(with = "time::serde::iso8601", skip_serializing)]
    start_date: OffsetDateTime,

    #[serde(rename(deserialize = "type", serialize = "activity_type"))]
    kind: Option<String>,

    #[serde(skip_serializing)]
    trainer: Option<bool>,

    #[serde(skip_serializing)]
    gear: Option<ActivityGear>,

    // Skip fields we compute consistently internally.
    #[serde(skip_serializing)]
    distance: Option<Value>,
    #[serde(skip_serializing)]
    average_speed: Option<Value>,
    #[serde(skip_serializing)]
    max_speed: Option<Value>,
    #[serde(skip_serializing)]
    elapsed_time: Option<Value>,
    #[serde(skip_serializing)]
    moving_time: Option<Value>,
    #[serde(skip_serializing)]
    total_elevation_gain: Option<Value>,
    #[serde(skip_serializing)]
    total_elevation_loss: Option<Value>,
    #[serde(skip_serializing)]
    min_altitude: Option<Value>,
    #[serde(skip_serializing)]
    max_altitude: Option<Value>,

    // Noisy base64-encoded data, not useful
    #[serde(skip_serializing)]
    skyline_chart_bytes: Option<Value>,

    // Catch-all for everything else.
    #[serde(flatten)]
    properties: Map<String, Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ActivityGear {
    id: String,
    name: Option<String>,
}

impl IntervalsIcuActivity {
    fn is_virtual(&self) -> bool {
        self.trainer == Some(true)
            || self
                .kind
                .as_deref()
                .is_some_and(|t| t.starts_with("Virtual"))
    }

    fn properties(&self) -> HashMap<String, Value> {
        let mut map = match serde_json::to_value(self) {
            Ok(Value::Object(map)) => map,
            _ => Map::new(),
        };

        if let Some(ref gear) = self.gear {
            if let Some(ref name) = gear.name {
                map.insert("activity_gear".to_string(), Value::String(name.clone()));
            }
            map.insert("gear_id".to_string(), Value::String(gear.id.clone()));
        }

        // Drop nested and null properties which won't be useful for filtering.
        map.into_iter()
            .filter(|(_, v)| !(v.is_null() || v.is_array() || v.is_object()))
            .collect()
    }
}

pub struct IntervalsIcuClient {
    http: reqwest::Client,
    api_key: String,
}

impl IntervalsIcuClient {
    pub fn new(auth: &IntervalsIcuAuth) -> Self {
        Self {
            http: reqwest::Client::new(),
            api_key: auth.api_key.clone(),
        }
    }

    /// List activities newer than `oldest`
    pub async fn list_activity_ids(&self, oldest: Date) -> Result<Vec<ActivityRef>> {
        let oldest = oldest.format(YYYY_MM_DD)?;

        let res = self
            .http
            .get(format!("{BASE_URL}/athlete/0/activities"))
            .basic_auth(AUTH_USER_KEY, Some(&self.api_key))
            .query(&[
                ("oldest", oldest.as_str()),
                ("fields", "id,strava_id,source"),
            ])
            .send()
            .await?;

        unwrap_response(res).await
    }

    /// Fetch full metadata for a single activity.
    pub async fn get_activity_metadata(&self, id: &str) -> Result<IntervalsIcuActivity> {
        let res = self
            .http
            .get(format!("{BASE_URL}/activity/{id}"))
            .basic_auth(AUTH_USER_KEY, Some(&self.api_key))
            .send()
            .await?;

        unwrap_response(res).await
    }

    /// Download the original GPX/FIT/TCX file, returning the filename and content
    pub async fn download_file(&self, id: &str) -> Result<(String, Vec<u8>)> {
        let res = self
            .http
            .get(format!("{BASE_URL}/activity/{id}/file"))
            .basic_auth(AUTH_USER_KEY, Some(&self.api_key))
            .send()
            .await?;

        let res = check_status(res).await?;

        let filename = res
            .headers()
            .get(reqwest::header::CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok())
            .and_then(content_disposition_filename)
            .ok_or_else(|| anyhow!("no filename in Content-Disposition for activity {id}"))?;

        let bytes = res.bytes().await?.to_vec();
        Ok((filename, bytes))
    }
}

/// `attachment; filename="12345.fit"` -> `12345.fit`.
fn content_disposition_filename(value: &str) -> Option<String> {
    value
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix("filename="))
        .map(|f| f.trim_matches('"').to_string())
        .filter(|f| !f.is_empty())
}

impl IntervalsIcuClient {
    /// Poll intervals.icu and ingest any new activities. Returns the number added.
    pub async fn fetch(
        &self,
        db: &Database,
        db_config: &db::Config,
        lookback_days: u32,
    ) -> Result<usize> {
        let mut window_start =
            OffsetDateTime::now_utc().date() - Duration::days(lookback_days.into());

        if let Some(cutoff) = db_config.fetch_cutoff {
            window_start = cutoff.max(window_start);
        }

        let refs = self.list_activity_ids(window_start).await?;
        let known = {
            let conn = db.connection()?;
            activity::known_files(&conn)?
        };

        let mut added = 0;
        for id in filter_new_activities(refs, &known) {
            match ingest_activity(db, db_config, self, &id).await {
                Ok(true) => {
                    tracing::info!("imported intervals.icu activity {}", id);
                    added += 1;
                }
                Ok(false) => {
                    // skipped (e.g. virtual)
                }
                Err(e) => {
                    tracing::error!("failed to ingest intervals.icu activity {}: {}", id, e);
                }
            }
        }

        Ok(added)
    }
}

fn filter_new_activities(
    refs: Vec<ActivityRef>,
    known: &HashSet<String>,
) -> impl Iterator<Item = String> {
    refs.into_iter()
        .filter(|r| {
            // Strava activities are not available via Intervals API
            r.source.as_deref() != Some("STRAVA")
                // Already imported this file
                && !known.contains(&format!("intervals_icu:{}", r.id))
                // Already imported (via Strava)
                && !r.strava_id.as_deref().is_some_and(|id| {
                    known.contains(&format!("strava:{}", id))
                })
        })
        .map(|r| r.id)
}

async fn ingest_activity(
    db: &Database,
    db_config: &db::Config,
    client: &IntervalsIcuClient,
    activity_id: &str,
) -> Result<bool> {
    let meta = client.get_activity_metadata(activity_id).await?;

    if meta.is_virtual() {
        tracing::info!("skipping indoor intervals.icu activity {activity_id}");
        return Ok(false);
    }

    let title = meta.name.clone();
    let start_time = Some(meta.start_date);

    let (filename, bytes) = client.download_file(activity_id).await?;
    let (media_type, comp) =
        get_file_type(&filename).ok_or_else(|| anyhow!("unrecognized file type: {filename}"))?;
    let parsed = crate::file::read(Cursor::new(bytes), media_type, comp)?
        .ok_or_else(|| anyhow!("no activity data in file {filename}"))?;

    let mut properties = parsed.properties;
    properties.extend(meta.properties());

    let raw = RawActivity {
        title,
        start_time,
        tracks: parsed.tracks,
        properties,
    };

    let conn = db.connection()?;
    let activity = raw.split_tiles(format!("intervals_icu:{activity_id}"), db_config)?;
    db::upsert_activity(&conn, &activity)?;

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use time::macros::datetime;

    fn activity_ref(id: &str, source: Option<&str>, strava_id: Option<&str>) -> ActivityRef {
        ActivityRef {
            id: id.to_string(),
            source: source.map(str::to_string),
            strava_id: strava_id.map(str::to_string),
        }
    }

    #[test]
    fn content_disposition_parsing() {
        assert_eq!(
            content_disposition_filename("attachment; filename=\"12345.fit\""),
            Some("12345.fit".to_string())
        );
        assert_eq!(
            content_disposition_filename("inline; filename=ride.gpx"),
            Some("ride.gpx".to_string())
        );
        assert_eq!(content_disposition_filename("attachment"), None);
    }

    #[test]
    fn metadata_maps_to_canonical_keys() {
        let meta: IntervalsIcuActivity = serde_json::from_str(
            r#"{
                "id": "i167402559",
                "name": "Coffee badge",
                "type": "Ride",
                "start_date": "2026-07-20T10:59:09Z",
                "distance": 3055.5,
                "average_speed": 6.054,
                "max_speed": 8.276,
                "total_elevation_gain": 12.0,
                "gear": { "id": "b12310355", "name": "Road bike" },
                "icu_ftp": 275,
                "icu_hr_zones": [138, 153, 160]
            }"#,
        )
        .unwrap();

        assert_eq!(meta.name.as_deref(), Some("Coffee badge"));
        assert_eq!(meta.start_date, datetime!(2026-07-20 10:59:09 UTC));

        let props = meta.properties();

        assert_eq!(props["activity_type"], json!("Ride"));
        assert_eq!(props["activity_gear"], json!("Road bike"));
        assert_eq!(props["gear_id"], json!("b12310355"));
        assert_eq!(props["icu_ftp"], json!(275)); // passthrough
    }

    #[test]
    fn selects_only_new_non_strava_activities() {
        let known: HashSet<String> = ["intervals_icu:id_existing", "strava:19387121985"]
            .into_iter()
            .map(String::from)
            .collect();

        let refs = vec![
            activity_ref("id_from_strava", Some("STRAVA"), None),
            activity_ref("id_existing", None, None),
            activity_ref(
                "id_strava_dupe",
                Some("GARMIN_CONNECT"),
                Some("19387121985"),
            ),
            activity_ref("id_new_with_strava_id", Some("GARMIN_CONNECT"), Some("222")),
            activity_ref("id_new", None, None),
        ];

        assert_eq!(
            filter_new_activities(refs, &known).collect::<Vec<_>>(),
            vec!["id_new_with_strava_id".to_string(), "id_new".to_string()]
        );
    }
}
