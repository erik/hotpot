use std::io::Cursor;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::body::HttpBody;
use axum::extract::{DefaultBodyLimit, Multipart, Path, Query, State};
use axum::headers::authorization::Bearer;
use axum::http::{header, Method, Request, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Router, Server, TypedHeader};
use image::codecs::png::{CompressionType, FilterType, PngEncoder};
use reqwest::header::CONTENT_TYPE;
use rust_embed::Embed;
use serde::{Deserialize, Deserializer, Serialize};
use time::Date;
use tokio::runtime::Runtime;
use tower_http::trace::{DefaultOnFailure, TraceLayer};

use crate::db::{ActivityFilter, Database, PropertyFilter};
use crate::raster::LinearGradient;
use crate::strava;
use crate::strava::StravaAuth;
use crate::tile::Tile;
use crate::{activity, raster};

#[derive(Clone)]
pub struct Config {
    pub cors: bool,
    pub upload_token: Option<String>,
    pub routes: RouteConfig,
}

#[derive(Clone)]
pub struct RouteConfig {
    pub tiles: bool,
    pub strava_webhook: bool,
    pub strava_auth: bool,
    pub upload: bool,
}

#[derive(Embed)]
#[folder = "src/web/"]
struct StaticAsset;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Database>,
    pub strava: Option<StravaAuth>,
    pub config: Config,
}

pub fn run_blocking(addr: SocketAddr, db: Database, config: Config) -> Result<()> {
    let rt = Runtime::new()?;
    let fut = run_async(addr, db, config);
    rt.block_on(fut)?;
    Ok(())
}

impl Config {
    fn build_router<S>(&self, db: Database) -> Result<Router<S>> {
        let trace = TraceLayer::new_for_http()
            .on_response(trace_request)
            .on_failure(DefaultOnFailure::new());

        let mut router = Router::new();
        if self.routes.tiles {
            router = router
                .route("/", get(index))
                .route("/static/*path", get(static_file))
                .route("/tile/:z/:x/:y", get(render_tile))
                .route("/api/activity-count", get(get_activity_count));
        }

        let mut use_strava_auth = false;
        if self.routes.strava_webhook {
            router = router.nest("/strava", strava::webhook_routes());
            use_strava_auth = true;
        }

        if self.routes.strava_auth {
            router = router.nest("/strava", strava::auth_routes());
            use_strava_auth = true;
        }

        if self.routes.upload {
            if self.upload_token.is_none() {
                tracing::warn!(
                    "HOTPOT_UPLOAD_TOKEN not set, unauthenticated uploads will be allowed"
                );
            }

            router = router
                .route("/upload", post(upload_activity))
                .layer(DefaultBodyLimit::max(15 * 1024 * 1024));
        }

        // TODO: possibly better better as an Option
        let strava = if use_strava_auth {
            Some(StravaAuth::from_env()?)
        } else {
            None
        };

        let router = router
            .layer(axum::middleware::from_fn(store_request_data))
            .layer(trace)
            .with_state(AppState {
                config: self.clone(),
                strava,
                db: Arc::new(db),
            });

        Ok(router)
    }
}

async fn run_async(addr: SocketAddr, db: Database, config: Config) -> Result<()> {
    tracing::info!("starting server on http://{}", addr);
    let router = config.build_router(db)?;
    Server::bind(&addr)
        .serve(router.into_make_service())
        .await?;

    Ok(())
}

async fn index(State(AppState { config, db, .. }): State<AppState>) -> impl IntoResponse {
    let index_file = StaticAsset::get("index.html").expect("missing file");
    let html = std::str::from_utf8(&index_file.data).expect("valid utf8");
    let properties = load_activity_properties(&db)
        .await
        .and_then(|props| Ok(serde_json::to_string(&props)?))
        .unwrap_or_else(|err| {
            tracing::error!("failed to generate activity properties: {:?}", err);
            "{}".to_string()
        });

    // Dynamically inject config
    let html = html.replace(
        "// $INJECT$",
        format!(
            "\
            globalThis.UPLOADS_ENABLED = {};
            globalThis.ACTIVITY_PROPERTIES = {};
        ",
            config.routes.upload, properties,
        )
        .as_str(),
    );

    axum::response::Html(html)
}

async fn static_file(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    match StaticAsset::get(path) {
        Some(content) => {
            let mime = match path.split_once('.') {
                Some((_, "js")) => "text/javascript",
                Some((_, "css")) => "text/plain",
                _ => "application/octet-stream",
            };
            ([(CONTENT_TYPE, mime)], content.data).into_response()
        }

        None => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct RenderQueryParams {
    #[serde(default)]
    color: Option<String>,
    #[serde(default)]
    gradient: Option<LinearGradient>,
    #[serde(default, with = "crate::date::parse")]
    before: Option<Date>,
    #[serde(default, with = "crate::date::parse")]
    after: Option<Date>,
    #[serde(default)]
    filter: Option<PropertyFilter>,
}

/// Handle the `y` part of an `/z/x/y` or `/z/x/y@2x` URL
struct TileYParam {
    y: u32,
    tile_size: u32,
}

impl<'de> Deserialize<'de> for TileYParam {
    fn deserialize<D>(deserializer: D) -> Result<TileYParam, D::Error>
    where
        D: Deserializer<'de>,
    {
        let param = String::deserialize(deserializer)?;
        let (y_str, size) = param.split_once('@').unwrap_or((&param, "1x"));

        let y = u32::from_str(y_str).map_err(serde::de::Error::custom)?;
        let tile_size = match size {
            "small" => 256,
            "1x" => 512,
            "2x" => 1024,
            _ => {
                return Err(serde::de::Error::custom(format!(
                    "invalid tile size: {}",
                    size
                )))
            }
        };

        Ok(TileYParam { tile_size, y })
    }
}

#[derive(Debug, Serialize)]
struct ActivityProperty {
    key: String,
    activity_count: usize,
}

#[derive(Debug, Serialize)]
struct ActivityProperties(Vec<ActivityProperty>);

// TODO: shouldn't live here
async fn load_activity_properties(db: &Database) -> Result<ActivityProperties> {
    let conn = db.connection()?;
    let mut stmt = conn.prepare(
        "\
            SELECT
                key,
                sum(count) as num_activities
            FROM (
                SELECT
                    prop.key,
                    count(*) as count
                FROM (
                    SELECT props.*
                    FROM activities, json_each(properties) props
                ) prop
                GROUP BY 1
            )
            GROUP BY 1
            ORDER BY 1;
        ",
    )?;
    let mut rows = stmt.query([])?;

    let mut properties = vec![];
    while let Some(row) = rows.next()? {
        properties.push(ActivityProperty {
            key: row.get_unwrap(0),
            activity_count: row.get_unwrap(1),
        });
    }

    Ok(ActivityProperties(properties))
}

async fn get_activity_count(
    State(AppState { db, .. }): State<AppState>,
    Query(params): Query<RenderQueryParams>,
) -> impl IntoResponse {
    let filter = ActivityFilter::new(params.before, params.after, params.filter);
    let num_activities = filter.count(&db).unwrap();

    (StatusCode::OK, num_activities.to_string()).into_response()
}

async fn render_tile(
    State(AppState { db, config, .. }): State<AppState>,
    Path((z, x, y_param)): Path<(u8, u32, TileYParam)>,
    Query(params): Query<RenderQueryParams>,
) -> impl IntoResponse {
    // Fail fast when tile is higher zoom level than we store data for.
    if db.config.source_level(z).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }

    // TODO: Clean up this mess
    let gradient: &LinearGradient = match (&params.gradient, params.color.as_deref()) {
        (Some(gradient), None) => gradient,
        (Some(_), Some(_)) => {
            return (
                StatusCode::BAD_REQUEST,
                "cannot specify both gradient and color",
            )
                .into_response()
        }
        (None, None) => &raster::ORANGE,
        (None, Some("pinkish")) => &raster::PINKISH,
        (None, Some("blue-red")) => &raster::BLUE_RED,
        (None, Some("red")) => &raster::RED,
        (None, Some("orange")) => &raster::ORANGE,
        (None, Some(_)) => return (StatusCode::BAD_REQUEST, "invalid color").into_response(),
    };

    let filter = ActivityFilter::new(params.before, params.after, params.filter);
    let tile = Tile::new(x, y_param.y, z);

    match raster::render_tile(tile, gradient, y_param.tile_size, &filter, &db) {
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

            let mut res = axum::response::Response::builder()
                .header(header::CONTENT_TYPE, "image/png")
                .header(header::CACHE_CONTROL, "max-age=86400");

            if config.cors {
                res = res.header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*");
            }

            res.body(bytes).unwrap().into_parts().into_response()
        }
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!("error rendering tile: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn is_authenticated(
    config: Config,
    auth_header: Option<TypedHeader<axum::headers::Authorization<Bearer>>>,
) -> bool {
    match (config.upload_token, auth_header) {
        (Some(expected), Some(actual)) => actual.0.token() == expected.as_str(),
        (Some(_), None) => false,
        (None, _) => true,
    }
}

async fn upload_activity(
    State(AppState { db, config, .. }): State<AppState>,
    auth_header: Option<TypedHeader<axum::headers::Authorization<Bearer>>>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    if !is_authenticated(config, auth_header) {
        return (StatusCode::UNAUTHORIZED, "bad token");
    }

    while let Some(field) = multipart.next_field().await.expect("to get form field") {
        if field.name() != Some("file") {
            continue;
        }

        let file_name = match field.file_name() {
            Some(f) => f.to_string(),
            None => return (StatusCode::BAD_REQUEST, "no filename"),
        };

        let Some((media_type, comp)) = activity::get_file_type(&file_name) else {
            return (StatusCode::UNSUPPORTED_MEDIA_TYPE, "unrecognized file type");
        };

        tracing::info!(
            "uploading file: {} (type: {:?}, compression: {:?})",
            file_name,
            media_type,
            comp
        );

        let bytes = field.bytes().await.unwrap();
        let reader = Cursor::new(bytes);
        let Ok(Some(activity)) = activity::read(reader, media_type, comp) else {
            return (StatusCode::UNPROCESSABLE_ENTITY, "couldn't read file");
        };

        let activity_id = format!("upload:{}", file_name);

        if let Err(err) = db
            .connection()
            .and_then(|mut conn| activity::upsert(&mut conn, &activity_id, &activity, &db.config))
        {
            tracing::error!("failed to insert activity: {:?}", err);
            return (StatusCode::INTERNAL_SERVER_ERROR, "something went wrong");
        }
    }

    (StatusCode::OK, "activity added")
}

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

fn trace_request(res: &Response, latency: Duration, _span: &tracing::Span) {
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
