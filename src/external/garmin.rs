//! Poll-based activity ingestion from the Garmin Connect Personal API.
//!
//! API docs: <https://developer.garmin.com/gc-developer-program/personal/swagger-docs/>

use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read};

use anyhow::{Result, anyhow};
use serde::Deserialize;
use serde_json::{Map, Value};
use time::{Date, OffsetDateTime};
use zip::ZipArchive;

use crate::activity::{self, RawActivity};
use crate::date::YYYY_MM_DD;
use crate::db::{self, Database};
use crate::external::{check_status, unwrap_response};

const BASE_URL: &str = "https://personal.connectapi.garmin.com";

const PAGE_SIZE: usize = 100;

/// Summary fields we compute consistently internally.
const SKIPPED_PROPERTIES: &[&str] = &[
    "distance",
    "duration",
    "elapsedDuration",
    "movingDuration",
    "averageSpeed",
    "maxSpeed",
    "elevationGain",
    "elevationLoss",
    "minElevation",
    "maxElevation",
];

#[derive(Clone)]
pub struct GarminAuth {
    api_key: String,
}

impl GarminAuth {
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("GARMIN_API_KEY")
            .ok()
            .filter(|it| !it.is_empty())?;

        Some(Self { api_key })
    }
}

/// An entry from the activity list. The list response already carries the
/// full summary, so no per-activity metadata request is needed.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GarminActivity {
    activity_id: u64,
    activity_name: Option<String>,
    begin_timestamp: Option<i64>,
    activity_type: Option<TypeKey>,
    event_type: Option<TypeKey>,
    start_latitude: Option<f64>,

    #[serde(flatten)]
    properties: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TypeKey {
    type_key: String,
}

impl GarminActivity {
    fn type_key(&self) -> Option<&str> {
        self.activity_type.as_ref().map(|t| t.type_key.as_str())
    }

    /// Activities without a start position (manual entries, indoor
    /// workouts) have no GPS track worth downloading.
    fn has_track(&self) -> bool {
        self.start_latitude.is_some()
            && !self
                .type_key()
                .is_some_and(|t| t.contains("virtual") || t.contains("indoor"))
    }

    fn start_time(&self) -> Option<OffsetDateTime> {
        OffsetDateTime::from_unix_timestamp(self.begin_timestamp? / 1000).ok()
    }

    fn properties(&self) -> HashMap<String, Value> {
        let mut map: HashMap<String, Value> = self
            .properties
            .iter()
            .filter(|(k, v)| {
                !(SKIPPED_PROPERTIES.contains(&k.as_str())
                    // ownerId, ownerDisplayName, ownerFullName, etc. etc.
                    || k.starts_with("owner")
                    || v.is_null()
                    || v.is_array()
                    || v.is_object())
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        if let Some(ty) = self.type_key() {
            map.insert("activity_type".to_string(), Value::String(ty.to_string()));
        }

        if let Some(ref ev) = self.event_type {
            map.insert("event_type".to_string(), Value::String(ev.type_key.clone()));
        }

        map
    }
}

pub struct GarminClient {
    http: reqwest::Client,
    access_token: String,
}

impl GarminClient {
    pub async fn connect(auth: &GarminAuth) -> Result<Self> {
        let http = reqwest::Client::new();
        let res = http
            .post(format!(
                "{BASE_URL}/personalapitoken-service/personal-api/personal-api-key/token"
            ))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("ApiKey {}", auth.api_key),
            )
            .send()
            .await?;

        let res = check_status(res).await.map_err(|e| {
            anyhow!("failed to obtain Garmin access token (did your key expire?): {e}")
        })?;

        // The response body is the bare JWT, not a JSON object.
        let access_token = res.text().await?.trim().to_string();

        Ok(Self { http, access_token })
    }

    /// Exchange the API key for a token, then fetch. Returns the number added.
    pub async fn fetch_with(
        auth: &GarminAuth,
        db: &Database,
        db_config: &db::Config,
        lookback_days: u32,
    ) -> Result<usize> {
        Self::connect(auth)
            .await?
            .fetch(db, db_config, lookback_days)
            .await
    }

    pub async fn list_activities(&self, oldest: Date) -> Result<Vec<GarminActivity>> {
        let oldest = oldest.format(YYYY_MM_DD)?;
        let mut all = Vec::new();

        loop {
            let res = self
                .http
                .get(format!(
                    "{BASE_URL}/activitylist-service/activities/search/activities"
                ))
                .bearer_auth(&self.access_token)
                .query(&[
                    ("startDate", oldest.as_str()),
                    ("start", &all.len().to_string()),
                    ("limit", &PAGE_SIZE.to_string()),
                ])
                .send()
                .await?;

            let page: Vec<GarminActivity> = unwrap_response(res).await?;
            let is_last_page = page.len() < PAGE_SIZE;
            all.extend(page);

            if is_last_page {
                return Ok(all);
            }
        }
    }

    /// Download the original GPX/FIT/TCX file, returning the filename and content.
    pub async fn download_file(&self, id: u64) -> Result<(String, Vec<u8>)> {
        let res = self
            .http
            .get(format!("{BASE_URL}/download-service/files/activity/{id}"))
            .bearer_auth(&self.access_token)
            .send()
            .await?;

        let bytes = check_status(res).await?.bytes().await?;
        extract_activity_file(&bytes)
    }

    /// Poll Garmin and ingest any new activities. Returns the number added.
    pub async fn fetch(
        &self,
        db: &Database,
        db_config: &db::Config,
        lookback_days: u32,
    ) -> Result<usize> {
        let activities = self
            .list_activities(db_config.fetch_window_start(lookback_days))
            .await?;
        let known = {
            let conn = db.connection()?;
            activity::known_files(&conn)?
        };

        let mut added = 0;
        for meta in filter_new_activities(activities, &known) {
            let id = meta.activity_id;
            match self.ingest(db, db_config, meta).await {
                Ok(()) => {
                    tracing::info!("imported Garmin activity {}", id);
                    added += 1;
                }
                Err(e) => {
                    tracing::error!("failed to ingest Garmin activity {}: {}", id, e);
                }
            }
        }

        Ok(added)
    }

    async fn ingest(
        &self,
        db: &Database,
        db_config: &db::Config,
        meta: GarminActivity,
    ) -> Result<()> {
        let (filename, bytes) = self.download_file(meta.activity_id).await?;
        let (media_type, comp) = activity::get_file_type(&filename)
            .ok_or_else(|| anyhow!("unrecognized file type: {filename}"))?;
        let parsed = activity::read(Cursor::new(bytes), media_type, comp)?
            .ok_or_else(|| anyhow!("no activity data in file {filename}"))?;

        let mut properties = parsed.properties;
        properties.extend(meta.properties());

        let raw = RawActivity {
            start_time: meta.start_time(),
            title: meta.activity_name,
            tracks: parsed.tracks,
            properties,
        };

        let mut conn = db.connection()?;
        activity::upsert(
            &mut conn,
            &format!("garmin:{}", meta.activity_id),
            raw,
            db_config,
        )?;

        Ok(())
    }
}

/// Garmin always serves downloads as a zip, even for a single file.
fn extract_activity_file(zip_bytes: &[u8]) -> Result<(String, Vec<u8>)> {
    let mut archive = ZipArchive::new(Cursor::new(zip_bytes))?;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;

        // Check if this is a file we can parse, return the first one we can
        if activity::get_file_type(entry.name()).is_some() {
            let mut buf = vec![];
            entry.read_to_end(&mut buf)?;

            return Ok((entry.name().to_string(), buf));
        }
    }

    Err(anyhow!("no activity file in archive"))
}

fn filter_new_activities(
    activities: Vec<GarminActivity>,
    known: &HashSet<String>,
) -> Vec<GarminActivity> {
    activities
        .into_iter()
        .filter(|a| a.has_track() && !known.contains(&format!("garmin:{}", a.activity_id)))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use serde_json::json;
    use time::macros::datetime;

    fn activity(id: u64, type_key: &str, lat: Option<f64>) -> GarminActivity {
        serde_json::from_value(json!({
            "activityId": id,
            "activityType": { "typeKey": type_key },
            "startLatitude": lat,
        }))
        .unwrap()
    }

    #[test]
    fn metadata_maps_to_canonical_keys() {
        let meta: GarminActivity = serde_json::from_str(
            r#"{
                "activityId": 24824121573,
                "activityName": "Berlin Radfahren",
                "startTimeGMT": "2026-10-22 19:15:42",
                "beginTimestamp": 1792696542000,
                "activityType": { "typeId": 2, "typeKey": "cycling", "parentTypeId": 17 },
                "eventType": { "typeId": 9, "typeKey": "uncategorized" },
                "distance": 3856.78,
                "averageSpeed": 4.646,
                "startLatitude": 52.4895,
                "ownerDisplayName": "someone",
                "privacy": { "typeKey": "private" },
                "splitSummaries": [],
                "calories": 94.0,
                "manufacturer": "GARMIN"
            }"#,
        )
        .unwrap();

        assert_eq!(meta.activity_name.as_deref(), Some("Berlin Radfahren"));
        assert_eq!(meta.start_time(), Some(datetime!(2026-10-22 19:15:42 UTC)));

        let props = meta.properties();

        assert_eq!(props["activity_type"], json!("cycling"));
        assert_eq!(props["event_type"], json!("uncategorized"));
        assert_eq!(props["calories"], json!(94.0));
        assert_eq!(props["manufacturer"], json!("GARMIN"));
        assert!(!props.contains_key("distance"));
        assert!(!props.contains_key("averageSpeed"));
        assert!(!props.contains_key("activityId"));
        assert!(!props.contains_key("ownerDisplayName"));
        assert!(!props.contains_key("privacy"));
    }

    #[test]
    fn selects_only_new_outdoor_activities() {
        let known: HashSet<String> = ["garmin:2".to_string(), "garmin:3".to_string()].into();

        let activities = vec![
            activity(1, "cycling", Some(52.0)),
            activity(2, "cycling", Some(52.0)),
            activity(3, "gravel_cycling", Some(52.0)),
            activity(4, "virtual_ride", Some(-11.6)),
            activity(5, "indoor_cycling", None),
            activity(6, "strength_training", None),
            activity(7, "running", Some(52.0)),
        ];

        let ids: Vec<u64> = filter_new_activities(activities, &known)
            .into_iter()
            .map(|a| a.activity_id)
            .collect();

        assert_eq!(ids, vec![1, 7]);
    }

    #[test]
    fn extracts_fit_from_zip() {
        let mut buf = Cursor::new(Vec::new());
        {
            let mut zw = zip::ZipWriter::new(&mut buf);
            let opts = zip::write::SimpleFileOptions::default();
            zw.start_file("readme.txt", opts).unwrap();
            zw.write_all(b"hi").unwrap();
            zw.start_file("123_ACTIVITY.fit", opts).unwrap();
            zw.write_all(b"fitdata").unwrap();
            zw.finish().unwrap();
        }

        let (name, bytes) = extract_activity_file(buf.get_ref()).unwrap();
        assert_eq!(name, "123_ACTIVITY.fit");
        assert_eq!(bytes, b"fitdata");
    }
}
