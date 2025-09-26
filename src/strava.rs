use std::collections::HashMap;

use anyhow::{Result, anyhow};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect};
use axum::routing::{get, post};
use axum::{Json, Router, TypedHeader, headers};
use geo_types::MultiLineString;
use reqwest::Response;
use rusqlite::params;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::activity;
use crate::activity::RawActivity;
use crate::db::Database;
use crate::web::AppState;

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
struct SummaryActivity {
    id: u64,
    #[serde(skip_serializing)]
    name: String,
    #[serde(skip_serializing)]
    map: PolyLineMap,
    #[serde(with = "time::serde::iso8601")]
    start_date: OffsetDateTime,
    #[serde(rename(serialize = "elevation_gain"))]
    total_elevation_gain: f64,
    #[serde(rename(deserialize = "type", serialize = "activity_type"))]
    kind: String,
    // Custom serialization to flatten
    #[serde(skip_serializing)]
    gear: Option<ActivityGear>,

    // Skip the useless properties
    #[serde(skip)]
    segment_efforts: Vec<Value>,
    #[serde(skip)]
    splits_metric: Vec<Value>,
    #[serde(skip)]
    laps: Vec<Value>,
    #[serde(skip)]
    photos: Vec<Value>,
    #[serde(skip)]
    highlighted_kudosers: Vec<Value>,

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

        HashMap::from_iter(map)
    }
}

#[derive(Clone)]
pub struct StravaAuth {
    client_id: u64,
    client_secret: String,
    webhook_secret: String,
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

struct StravaClient<'a> {
    auth: &'a StravaAuth,
    db: &'a Database,
}

impl<'a> StravaClient<'a> {
    async fn exchange_token(&self, code: &str) -> Result<AuthToken> {
        let client = reqwest::Client::new();

        let res = client
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
        Ok(token.token)
    }
    async fn get_activity(&self, athlete_id: u64, activity_id: u64) -> Result<SummaryActivity> {
        let token = self.get_token(athlete_id).await?;
        let client = reqwest::Client::new();

        let res = client
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
        let client = reqwest::Client::new();

        let token = client
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

async fn unwrap_response<T: DeserializeOwned>(res: Response) -> Result<T> {
    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await?;
        return Err(anyhow!("HTTP request failed with status {status}: {body}"));
    }

    Ok(res.json().await?)
}

pub fn webhook_routes() -> Router<AppState> {
    Router::new()
        .route("/webhook", get(confirm_webhook))
        .route("/webhook", post(receive_webhook))
}

pub fn auth_routes() -> Router<AppState> {
    Router::new()
        .route("/auth", get(auth_redirect))
        .route("/auth/exchange_token", get(exchange_token))
}

async fn auth_redirect(
    TypedHeader(host): TypedHeader<headers::Host>,
    State(AppState { strava, .. }): State<AppState>,
) -> impl IntoResponse {
    let strava = strava.expect("strava auth creds missing");
    let url = format!(
        "https://www.strava.com/oauth/authorize\
?client_id={}\
&approval_prompt=force\
&scope=activity:read_all\
&response_type=code\
&redirect_uri=http://{}/strava/auth/exchange_token",
        strava.client_id, host,
    );

    Redirect::to(&url)
}

#[derive(Deserialize)]
struct ExchangeTokenQuery {
    code: String,
}

async fn exchange_token(
    State(AppState { db, strava, .. }): State<AppState>,
    Query(params): Query<ExchangeTokenQuery>,
) -> impl IntoResponse {
    let strava = strava.expect("strava auth creds missing");

    let client = StravaClient {
        auth: &strava,
        db: &db,
    };

    if let Err(e) = client.exchange_token(&params.code).await {
        tracing::error!("failed to exchange token: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, "error exchanging token").into_response();
    }

    (
        StatusCode::OK,
        format!(
            "Successfully authenticated with Strava.

Next, make sure the webhook is set up to be called for new activities:

    curl https://www.strava.com/api/v3/push_subscriptions \\
         -F \"client_id={0}\" \\
         -F \"client_secret={1}\" \\
         -F \"callback_url=https://[example.com]/strava/webhook\" \\
         -F \"verify_token={2}\"

Confirm the webhook was set up correctly with:

    curl --get https://www.strava.com/api/v3/push_subscriptions \\
         -d \"client_id={0}\" \\
         -d \"client_secret={1}\"

More information: https://developers.strava.com/docs/getting-started
",
            strava.client_id, strava.client_secret, strava.webhook_secret,
        ),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
struct ConfirmWebhookQuery {
    #[serde(rename = "hub.mode")]
    mode: String,
    #[serde(rename = "hub.challenge")]
    challenge: String,
    #[serde(rename = "hub.verify_token")]
    verify_token: String,
}

#[derive(Serialize)]
struct ConfirmWebhookResponse {
    #[serde(rename = "hub.challenge")]
    challenge: String,
}

async fn confirm_webhook(
    State(AppState { strava, .. }): State<AppState>,
    Query(params): Query<ConfirmWebhookQuery>,
) -> impl IntoResponse {
    let strava = strava.expect("strava auth creds missing");
    if params.mode != "subscribe" {
        return (StatusCode::BAD_REQUEST, "invalid mode").into_response();
    }

    if params.verify_token != strava.webhook_secret {
        return (StatusCode::UNAUTHORIZED, "invalid verify token").into_response();
    }

    Json(ConfirmWebhookResponse {
        challenge: params.challenge,
    })
    .into_response()
}

#[derive(Deserialize)]
struct WebhookBody {
    /// Athlete ID
    owner_id: u64,
    /// Activity or Athlete ID
    object_id: u64,
    /// "activity", "athlete"
    object_type: String,
    // TODO: handle these
    // "create", "update", "delete"
    // aspect_type: String,
}

// TODO: look at subscription_id or something to verify request.
async fn receive_webhook(
    State(AppState { db, strava, .. }): State<AppState>,
    Json(body): Json<WebhookBody>,
) -> impl IntoResponse {
    let strava = strava.expect("strava auth creds missing");
    if body.object_type != "activity" {
        return (StatusCode::OK, "nothing to do");
    }

    let client = StravaClient {
        auth: &strava,
        db: &db,
    };
    let activity = match client.get_activity(body.owner_id, body.object_id).await {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("error getting activity: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, "error getting activity");
        }
    };

    let polyline = polyline::decode_polyline(&activity.map.polyline, 5).expect("valid polyline");
    let properties = activity.properties();

    if let Err(e) = activity::upsert(
        &mut db.connection().unwrap(),
        &format!("strava:{}", activity.id),
        &RawActivity {
            title: Some(activity.name),
            start_time: Some(activity.start_date),
            tracks: MultiLineString::from(polyline),
            properties,
        },
        &db.config,
    ) {
        tracing::error!("error writing activity: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, "error writing activity");
    }

    (StatusCode::OK, "added!")
}
