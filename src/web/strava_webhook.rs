//! Strava web routes: OAuth authorization + activity upload webhook.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect};
use axum::routing::{get, post};
use axum::{Json, Router, TypedHeader, headers};
use serde::{Deserialize, Serialize};

use crate::external::strava::{StravaClient, ingest_activity};
use crate::web::AppState;

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

    let client = StravaClient::new(&strava, &db);

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
    State(AppState {
        db,
        db_config,
        strava,
        ..
    }): State<AppState>,
    Json(body): Json<WebhookBody>,
) -> impl IntoResponse {
    let strava = strava.expect("strava auth creds missing");
    if body.object_type != "activity" {
        return (StatusCode::OK, "nothing to do");
    }

    let client = StravaClient::new(&strava, &db);
    let activity = match client.get_activity(body.owner_id, body.object_id).await {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("error getting activity: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, "error getting activity");
        }
    };

    match ingest_activity(&db, &db_config, activity) {
        Ok(true) => (StatusCode::OK, "added!"),
        Ok(false) => (StatusCode::NO_CONTENT, "skipped"),
        Err(e) => {
            tracing::error!("error writing activity: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "error writing activity")
        }
    }
}
