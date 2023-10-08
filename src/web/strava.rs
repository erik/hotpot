use anyhow::{anyhow, Result};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect};
use axum::routing::{get, post};
use axum::{headers, Json, Router, TypedHeader};
use geo_types::MultiLineString;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::activity;
use crate::activity::RawActivity;
use crate::db::{Database, SqlDateTime};
use crate::web::AppState;

#[derive(Debug, Deserialize)]
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

/// https://developers.strava.com/docs/reference/#api-models-SummaryActivity
#[derive(Deserialize)]
struct SummaryActivity {
    id: u64,
    name: String,
    elapsed_time: u64,
    map: PolyLineMap,
    #[serde(with = "time::serde::iso8601")]
    start_date: OffsetDateTime,
}

#[derive(Clone)]
pub struct StravaAuth {
    client_id: u64,
    client_secret: String,
    webhook_secret: String,
}

impl StravaAuth {
    pub fn from_env() -> Result<StravaAuth> {
        let client_id = std::env::var("STRAVA_CLIENT_ID")?.parse()?;
        let client_secret = std::env::var("STRAVA_CLIENT_SECRET")?;
        let webhook_secret = std::env::var("STRAVA_WEBHOOK_SECRET")?;

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

        if !res.status().is_success() {
            let status = res.status();
            let body = res.text().await?;
            return Err(anyhow::anyhow!("{}: {}", status, body));
        }

        let token = res.json::<AuthTokenWithAthlete>().await?;

        self.store_token(token.athlete.id, &token.token)?;
        println!("got a token! {:#?}", token.token);
        Ok(token.token)
    }
    async fn get_activity(&self, athlete_id: u64, activity_id: u64) -> Result<SummaryActivity> {
        let token = self.get_token(athlete_id).await?;
        let client = reqwest::Client::new();

        let res = client
            .get(&format!(
                "https://www.strava.com/api/v3/activities/{}",
                activity_id
            ))
            .bearer_auth(&token.access_token)
            .send()
            .await?;

        if !res.status().is_success() {
            let status = res.status();
            let body = res.text().await?;
            return Err(anyhow::anyhow!("{}: {}", status, body));
        }

        let activity = res.json::<SummaryActivity>().await?;

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
            .map_err(|_| anyhow!("no credentials available for: {}", athlete_id))?
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

pub fn routes(with_login: bool) -> Router<AppState> {
    let router = Router::new()
        .route("/webhook", get(confirm_webhook))
        .route("/webhook", post(receive_webhook));

    // TODO: messy, let's split this out
    if with_login {
        println!("To auth with Strava, visit: http://127.0.0.1:8080/strava/auth");

        router
            .route("/auth", get(auth_redirect))
            .route("/auth/exchange_token", get(exchange_token))
    } else {
        router
    }
}

#[derive(Deserialize)]
struct ExchangeTokenQuery {
    code: String,
    // TODO: not currently used.
    // scope: String,
    // state: String,
}

async fn auth_redirect(
    TypedHeader(host): TypedHeader<headers::Host>,
    State(AppState { strava, .. }): State<AppState>,
) -> impl IntoResponse {
    let url = format!(
        "https://www.strava.com/oauth/authorize?client_id={}&approval_prompt=force&scope=activity:read_all&response_type=code&redirect_uri=http://{}/strava/auth/exchange_token",
        strava.client_id,
        host,
    );

    Redirect::to(&url)
}

async fn exchange_token(
    State(AppState { db, strava, .. }): State<AppState>,
    Query(params): Query<ExchangeTokenQuery>,
) -> impl IntoResponse {
    let client = StravaClient {
        auth: &strava,
        db: &db,
    };

    match client.exchange_token(&params.code).await {
        Ok(_) => (StatusCode::OK, "OK"),
        Err(e) => {
            tracing::error!("error exchanging token: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "error exchanging token")
        }
    }
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

    activity::upsert(
        &mut db.connection().unwrap(),
        &format!("strava:{}", activity.id),
        &RawActivity {
            title: Some(activity.name),
            start_time: Some(activity.start_date).map(SqlDateTime),
            duration_secs: Some(activity.elapsed_time),
            tracks: MultiLineString::from(polyline),
        },
        // TODO: where does this come from?
        0.0,
    )
    .unwrap();

    (StatusCode::OK, "added!")
}
