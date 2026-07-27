//! Strava API client: OAuth token management, activity ingestion via webhook,
//! and poll-based `fetch`.

use std::collections::HashMap;

use anyhow::{Result, anyhow};
use geo_types::MultiLineString;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::activity::{self, RawActivity};
use crate::db::{self, Database};
use crate::external::{fetch_window_start, unwrap_response};
use crate::track_stats::METERS_PER_SEC_TO_KMH;

#[derive(Deserialize)]
struct AuthToken {
    access_token: String,
    refresh_token: String,
    expires_at: i64,
}

#[derive(Deserialize)]
struct Athlete {
    id: u64,
}

#[derive(Deserialize)]
struct AuthTokenWithAthlete {
    #[serde(flatten)]
    token: AuthToken,
    athlete: Athlete,
}

#[derive(Serialize)]
struct AuthTokenRefreshRequestBody<'a> {
    client_id: u64,
    client_secret: &'a str,
    refresh_token: &'a str,
    grant_type: &'static str,
}

#[derive(Serialize)]
struct AuthTokenExchangeRequestBody<'a> {
    client_id: u64,
    client_secret: &'a str,
    code: &'a str,
    grant_type: &'static str,
}

#[derive(Deserialize)]
struct PolyLineMap {
    polyline: String,
}

#[derive(Deserialize)]
struct ActivityGear {
    id: String,
    name: String,
}

/// https://developers.strava.com/docs/reference/#api-models-SummaryActivity
#[allow(dead_code)]
#[derive(Deserialize, Serialize)]
pub(crate) struct SummaryActivity {
    id: u64,
    #[serde(skip_serializing)]
    name: String,
    #[serde(skip_serializing)]
    map: PolyLineMap,
    #[serde(with = "time::serde::iso8601")]
    start_date: OffsetDateTime,

    // Remap Strava field names to match our canonical property names
    #[serde(default, rename(serialize = "elevation_gain"))]
    total_elevation_gain: f64,
    #[serde(default, rename(serialize = "min_elevation"))]
    elev_low: f64,
    #[serde(default, rename(serialize = "max_elevation"))]
    elev_high: f64,
    #[serde(rename(deserialize = "type", serialize = "activity_type"))]
    kind: String,

    // Properties that will need conversion to match internally calculated
    // units.
    #[serde(skip_serializing)]
    distance: f64, // meters
    #[serde(skip_serializing)]
    average_speed: f64, // meters/sec
    #[serde(skip_serializing)]
    max_speed: f64, // meters/sec

    // Custom serialization to flatten
    #[serde(skip_serializing)]
    gear: Option<ActivityGear>,

    // Catch all for everything else
    #[serde(flatten)]
    properties: HashMap<String, Value>,
}

impl SummaryActivity {
    /// Merge the activity's properties with the gear's properties.
    fn properties(&self) -> HashMap<String, Value> {
        // TODO: use custom serializer instead
        let mut map = serde_json::to_value(self)
            .ok()
            .and_then(|it| it.as_object().cloned())
            .unwrap();

        // Unnest gear since it could be useful
        if let Some(ref gear) = self.gear {
            map.insert(
                "activity_gear".to_string(),
                Value::String(gear.name.clone()),
            );
            map.insert("gear_id".to_string(), Value::String(gear.id.clone()));
        }

        // Convert units to match internally computed metrics
        map.insert(
            "total_distance".to_string(),
            (self.distance / 1000.0).into(),
        );
        map.insert(
            "average_speed".to_string(),
            (self.average_speed * METERS_PER_SEC_TO_KMH).into(),
        );
        map.insert(
            "max_speed".to_string(),
            (self.max_speed * METERS_PER_SEC_TO_KMH).into(),
        );

        // Remove the most verbose of the properties (deeply nested JSON that
        // won't be useful for filtering)
        let noisy_props = &[
            "laps",
            "segment_efforts",
            "splits_metric",
            "splits_standard",
            "photos",
            "highlighted_kudosers",
        ];
        for &prop in noisy_props {
            map.remove(prop);
        }

        HashMap::from_iter(map)
    }
}

#[derive(Clone)]
pub struct StravaAuth {
    pub(crate) client_id: u64,
    pub(crate) client_secret: String,
    pub(crate) webhook_secret: String,
}

impl StravaAuth {
    pub fn from_env() -> Result<StravaAuth> {
        let get_env = |k| {
            std::env::var(k).map_err(|_| anyhow!("required environment variable not set: {}", k))
        };

        let client_id = get_env("STRAVA_CLIENT_ID")?;
        let client_secret = get_env("STRAVA_CLIENT_SECRET")?;
        let webhook_secret = get_env("STRAVA_WEBHOOK_SECRET")?;

        let client_id = client_id.parse().map_err(|_| {
            anyhow!(
                "expected valid integer for STRAVA_CLIENT_ID, got: {}",
                client_id
            )
        })?;

        Ok(Self {
            client_id,
            client_secret,
            webhook_secret,
        })
    }
}

pub(crate) struct StravaClient<'a> {
    http: reqwest::Client,
    auth: &'a StravaAuth,
    db: &'a Database,
}

impl<'a> StravaClient<'a> {
    pub(crate) fn new(auth: &'a StravaAuth, db: &'a Database) -> Self {
        Self {
            http: reqwest::Client::new(),
            auth,
            db,
        }
    }

    pub(crate) async fn exchange_token(&self, code: &str) -> Result<()> {
        let res = self
            .http
            .post("https://www.strava.com/oauth/token")
            .json(&AuthTokenExchangeRequestBody {
                client_id: self.auth.client_id,
                client_secret: &self.auth.client_secret,
                code,
                grant_type: "authorization_code",
            })
            .send()
            .await?;

        let token: AuthTokenWithAthlete = unwrap_response(res).await?;

        self.store_token(token.athlete.id, &token.token)?;
        Ok(())
    }

    pub(crate) async fn get_activity(
        &self,
        athlete_id: u64,
        activity_id: u64,
    ) -> Result<SummaryActivity> {
        let token = self.get_token(athlete_id).await?;

        let res = self
            .http
            .get(format!(
                "https://www.strava.com/api/v3/activities/{}",
                activity_id
            ))
            .bearer_auth(&token.access_token)
            .send()
            .await?;

        let activity: SummaryActivity = unwrap_response(res).await?;
        Ok(activity)
    }

    async fn get_token(&self, athlete_id: u64) -> Result<AuthToken> {
        let token = {
            let conn = self.db.connection()?;
            let mut stmt = conn.prepare(
                "\
                SELECT access_token, refresh_token, expires_at \
                FROM strava_tokens \
                WHERE athlete_id = ?",
            )?;

            stmt.query_row([athlete_id], |row| {
                Ok(AuthToken {
                    access_token: row.get_unwrap(0),
                    refresh_token: row.get_unwrap(1),
                    expires_at: row.get_unwrap(2),
                })
            })
            .map_err(|_| anyhow!("no credentials available for: {athlete_id}"))?
        };

        // Make sure we have at least a minute left on the token
        let now = OffsetDateTime::now_utc().unix_timestamp();
        if token.expires_at - 60 >= now {
            return Ok(token);
        }

        self.refresh_token(athlete_id, &token).await
    }

    fn store_token(&self, athlete_id: u64, token: &AuthToken) -> Result<()> {
        let conn = self.db.connection()?;
        conn.execute(
            "\
            INSERT OR REPLACE \
            INTO strava_tokens (athlete_id, access_token, refresh_token, expires_at) \
            VALUES (?, ?, ?, ?)",
            params![
                athlete_id,
                token.access_token,
                token.refresh_token,
                token.expires_at
            ],
        )?;

        Ok(())
    }

    async fn refresh_token(&self, athlete_id: u64, prev: &AuthToken) -> Result<AuthToken> {
        let token = self
            .http
            .post("https://www.strava.com/api/v3/oauth/token")
            .json(&AuthTokenRefreshRequestBody {
                client_id: self.auth.client_id,
                client_secret: &self.auth.client_secret,
                refresh_token: &prev.refresh_token,
                grant_type: "refresh_token",
            })
            .send()
            .await?
            .json::<AuthToken>()
            .await?;

        self.store_token(athlete_id, &token)?;

        Ok(token)
    }
}

/// Decode an activity's polyline, skip virtual activities, and upsert it into
/// the database. Returns `true` if imported, `false` if skipped.
///
/// Shared by the webhook handler and the poll-based `fetch`.
pub(crate) fn ingest_activity(
    db: &Database,
    db_config: &db::Config,
    activity: SummaryActivity,
) -> Result<bool> {
    let polyline = polyline::decode_polyline(&activity.map.polyline, 5)
        .map_err(|e| anyhow!("invalid polyline for strava activity {}: {e}", activity.id))?;
    let properties = activity.properties();

    // Filter out virtual activities. In my own data I see both "Virtual Ride"
    // and "VirtualRide", so be defensive about the matching.
    if properties
        .get("activity_type")
        .and_then(|t| t.as_str())
        .is_some_and(|ty| ty.starts_with("Virtual"))
    {
        tracing::info!("skipping virtual strava activity {}", activity.id);
        return Ok(false);
    }

    let mut conn = db.connection()?;
    activity::upsert(
        &mut conn,
        &format!("strava:{}", activity.id),
        RawActivity {
            title: Some(activity.name),
            start_time: Some(activity.start_date),
            tracks: MultiLineString::from(polyline),
            properties,
        },
        db_config,
    )?;

    Ok(true)
}

/// Minimal shape of an entry in the athlete activity list.
#[derive(Deserialize)]
struct ActivityRef {
    id: u64,
}

impl StravaClient<'_> {
    /// Athlete IDs we have stored OAuth credentials for.
    fn stored_athlete_ids(&self) -> Result<Vec<u64>> {
        let conn = self.db.connection()?;
        let mut stmt = conn.prepare("SELECT athlete_id FROM strava_tokens")?;
        let ids = stmt
            .query_map([], |row| row.get::<_, u64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(ids)
    }

    /// List an athlete's activity IDs recorded after `after` (epoch seconds).
    async fn list_activity_ids(&self, athlete_id: u64, after: i64) -> Result<Vec<u64>> {
        let token = self.get_token(athlete_id).await?;

        const PER_PAGE: usize = 200;
        let mut ids = Vec::new();
        let mut page = 1;
        loop {
            let res = self
                .http
                .get("https://www.strava.com/api/v3/athlete/activities")
                .bearer_auth(&token.access_token)
                .query(&[
                    ("after", after.to_string()),
                    ("per_page", PER_PAGE.to_string()),
                    ("page", page.to_string()),
                ])
                .send()
                .await?;

            let refs: Vec<ActivityRef> = unwrap_response(res).await?;
            let count = refs.len();
            ids.extend(refs.into_iter().map(|r| r.id));

            if count < PER_PAGE {
                break;
            }
            page += 1;
        }

        Ok(ids)
    }

    /// Poll Strava for all stored athletes and ingest any new activities.
    /// Returns the number added.
    pub(crate) async fn fetch(&self, db_config: &db::Config, lookback_days: u32) -> Result<usize> {
        let after = fetch_window_start(db_config, lookback_days)
            .midnight()
            .assume_utc()
            .unix_timestamp();

        let athletes = self.stored_athlete_ids()?;
        if athletes.is_empty() {
            return Err(anyhow!(
                "no Strava credentials stored; run the `strava-auth` command first"
            ));
        }

        let known = {
            let conn = self.db.connection()?;
            activity::known_files(&conn)?
        };

        let mut added = 0;
        for athlete_id in athletes {
            for id in self.list_activity_ids(athlete_id, after).await? {
                if known.contains(&format!("strava:{id}")) {
                    continue;
                }

                match self.fetch_and_ingest(db_config, athlete_id, id).await {
                    Ok(true) => {
                        tracing::info!("imported strava activity {id}");
                        added += 1;
                    }
                    Ok(false) => {
                        // skipped (e.g. virtual)
                    }
                    Err(e) => {
                        tracing::error!("failed to ingest strava activity {id}: {e}");
                    }
                }
            }
        }

        Ok(added)
    }

    /// Fetch a single activity's full detail and ingest it. Returns `true` if
    /// imported, `false` if skipped.
    async fn fetch_and_ingest(
        &self,
        db_config: &db::Config,
        athlete_id: u64,
        activity_id: u64,
    ) -> Result<bool> {
        let activity = self.get_activity(athlete_id, activity_id).await?;
        ingest_activity(self.db, db_config, activity)
    }
}
