use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::body::HttpBody;
use axum::extract::{Multipart, Path, Query, State};
use axum::headers::authorization::Bearer;
use axum::http::{header, Method, Request, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Router, Server, TypedHeader};
use image::codecs::png::{CompressionType, FilterType, PngEncoder};
use serde::Deserialize;
use time::Date;
use tokio::runtime::Runtime;
use tower_http::trace::TraceLayer;
use tracing::log::info;

use crate::db::ActivityFilter;
use crate::db::Database;
use crate::raster::DEFAULT_GRADIENT;
use crate::tile::Tile;
use crate::web::strava::StravaAuth;
use crate::{activity, raster};

mod strava;

struct RequestData {
    method: Method,
    uri: Uri,
}

async fn store_request_data<B>(req: Request<B>, next: Next<B>) -> Response {
    let data = RequestData {
        method: req.method().clone(),
        uri: req.uri().clone(),
    };

    let mut res = next.run(req).await;
    res.extensions_mut().insert(data);

    res
}

fn trace_response(res: &Response, latency: Duration, _span: &tracing::Span) {
    let data = res.extensions().get::<RequestData>().unwrap();

    tracing::info!(
        status = %res.status().as_u16(),
        method = %data.method,
        uri = %data.uri,
        latency = ?latency,
        size = res.size_hint().exact(),
        "response"
    );
}

#[derive(Clone)]
pub struct AppState {
    db: Arc<Database>,
    strava: StravaAuth,
}

pub fn run_blocking(addr: SocketAddr, db: Database, routes: RouteConfig) -> Result<()> {
    let rt = Runtime::new()?;
    let fut = run_async(addr, db, routes);
    rt.block_on(fut)?;
    Ok(())
}

pub struct RouteConfig {
    pub tiles: bool,
    pub strava_webhook: bool,
    pub strava_auth: bool,
    pub allow_upload: bool,
}

impl RouteConfig {
    fn build<S>(&self, db: Database) -> Result<Router<S>> {
        // TODO: MVT endpoint?
        let mut router = Router::new()
            .layer(axum::middleware::from_fn(store_request_data))
            // TODO: .on_failure(trace_failure)
            .layer(TraceLayer::new_for_http().on_response(trace_response));

        if self.tiles {
            router = router
                .route("/", get(index))
                .route("/tile/:z/:x/:y", get(render_tile));
        }

        if self.strava_webhook {
            router = router.nest("/strava", strava::webhook_routes());
        }

        if self.strava_auth {
            router = router.nest("/strava", strava::auth_routes());
        }

        if self.allow_upload {
            router = router.route("/upload", post(upload_activity));
        }

        let strava = if self.strava_webhook || self.strava_auth {
            StravaAuth::from_env()?
        } else {
            // TODO: possibly better better as an Option
            StravaAuth::unset()
        };

        Ok(router.with_state(AppState {
            strava,
            db: Arc::new(db),
        }))
    }
}

async fn run_async(addr: SocketAddr, db: Database, routes: RouteConfig) -> Result<()> {
    tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("Listening on http://{}", addr);

    let router = routes.build(db)?;
    Server::bind(&addr)
        .serve(router.into_make_service())
        .await?;

    Ok(())
}

async fn index() -> impl IntoResponse {
    let index = include_str!("./web/index.html");
    axum::response::Html(index)
}

#[derive(Debug, Deserialize)]
struct RenderQueryParams {
    #[serde(default)]
    color: Option<String>,
    #[serde(default, with = "crate::date::parse")]
    before: Option<Date>,
    #[serde(default, with = "crate::date::parse")]
    after: Option<Date>,
}

async fn render_tile(
    State(AppState { db, .. }): State<AppState>,
    Path((z, x, y)): Path<(u8, u32, u32)>,
    Query(params): Query<RenderQueryParams>,
) -> impl IntoResponse {
    if z > *db.meta.zoom_levels.iter().max().unwrap_or(&0) {
        return StatusCode::NO_CONTENT.into_response();
    }

    // TODO: this should be supported by CLI as well
    let color = match params.color.as_deref() {
        Some("blue-red") => &raster::BLUE_RED,
        Some("red") => &raster::RED,
        Some("orange") => &raster::ORANGE,
        _ => &DEFAULT_GRADIENT,
    };

    let filter = ActivityFilter::new(params.before, params.after);
    let tile = Tile::new(x, y, z);

    match raster::render_tile(tile, color, 512, &filter, &db) {
        Ok(Some(image)) => {
            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);

            image
                .write_with_encoder(PngEncoder::new_with_quality(
                    &mut cursor,
                    CompressionType::Fast,
                    FilterType::NoFilter,
                ))
                .unwrap();

            // TODO: seems hacky
            (
                axum::response::AppendHeaders([
                    (header::CONTENT_TYPE, "image/png"),
                    (header::CACHE_CONTROL, "max-age=3600"),
                    // TODO: should be configurable.
                    (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
                ]),
                bytes,
            )
                .into_response()
        }
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!("error rendering tile: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn upload_activity(
    State(AppState { db, .. }): State<AppState>,
    TypedHeader(auth): TypedHeader<axum::headers::Authorization<Bearer>>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    // TODO: real auth
    if auth.token() != "magic" {
        return (StatusCode::UNAUTHORIZED, "bad token");
    }

    while let Some(field) = multipart.next_field().await.unwrap() {
        if Some("file") != field.name() {
            continue;
        }

        let file_name = match field.file_name() {
            Some(f) => f.to_string(),
            None => return (StatusCode::BAD_REQUEST, "no filename"),
        };

        let Some((kind, comp)) = activity::get_file_type(&file_name) else {
            return (StatusCode::BAD_REQUEST, "unsupported file type");
        };

        tracing::info!("uploading {}", file_name);

        // TODO: Should be possible to use a streaming reader here.
        let bytes = field.bytes().await.unwrap();
        let reader = Cursor::new(bytes);

        let activity = activity::read(reader, kind, comp).unwrap();
        if let Some(activity) = activity {
            let mut conn = db.connection().unwrap();
            let id = format!("upload:{}", file_name);

            // TODO: where does this come from?
            let trim_dist = 0.0;

            activity::upsert(&mut conn, &id, &activity, trim_dist).unwrap();
        }
    }

    (StatusCode::OK, "added!")
}
