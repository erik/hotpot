use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Cursor;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use axum::body::{Body, HttpBody};
use axum::extract::{DefaultBodyLimit, Multipart, Path, Query, State};
use axum::headers::authorization::Bearer;
use axum::http::{HeaderMap, Method, Request, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Router, Server, TypedHeader};
use image::codecs::png::{CompressionType, FilterType, PngEncoder};
use image::codecs::webp::WebPEncoder;
use rust_embed::Embed;
use serde::{Deserialize, Deserializer};
use time::Date;
use tokio::runtime::Runtime;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::{DefaultOnFailure, TraceLayer};
use tracing::Level;

use crate::db::Config as DbConfig;
use crate::db::{ActivityFilter, Database};
use crate::filter::PropertyFilter;
use crate::raster::LinearGradient;
use crate::strava;
use crate::strava::StravaAuth;
use crate::tile::{Tile, WebMercatorViewport};
use crate::{activity, raster};

#[derive(Debug, Clone, Copy, PartialEq)]
enum ImageFormat {
    Png,
    WebP,
}

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
    pub render: bool,
}

#[derive(Embed)]
#[folder = "src/web/"]
struct StaticAsset;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Database>,
    pub db_config: Arc<DbConfig>,
    pub strava: Option<StravaAuth>,
    pub config: Config,
}

impl Config {
    fn build_router<S>(&self, db: Database) -> Result<Router<S>> {
        let span = tracing::span!(Level::INFO, "build_router");
        let _guard = span.enter();

        let trace = TraceLayer::new_for_http()
            .on_response(trace_request)
            .on_failure(DefaultOnFailure::new());

        let mut router = Router::new();
        if self.routes.tiles {
            tracing::info!("/ (web ui)");
            tracing::info!("/tile/:z/:x/:y (tile rendering)");

            router = router
                .route("/", get(index))
                .route("/static/*path", get(static_file))
                .route("/tile/:z/:x/:y", get(render_tile))
                .route("/api/activity-count", get(get_activity_count));
        }

        let mut use_strava_auth = false;
        if self.routes.strava_webhook {
            tracing::info!("/strava/webhook (strava activity upload webhook)");

            router = router.nest("/strava", strava::webhook_routes());
            use_strava_auth = true;
        }

        if self.routes.strava_auth {
            tracing::info!("/strava/auth (strava api oauth)");

            router = router.nest("/strava", strava::auth_routes());
            use_strava_auth = true;
        }

        if self.routes.upload {
            tracing::info!("/upload (http activity upload)");

            if self.upload_token.is_none() {
                tracing::warn!(
                    "HOTPOT_UPLOAD_TOKEN not set, unauthenticated uploads will be allowed"
                );
            }

            router = router
                .route("/upload", post(upload_activity))
                .layer(DefaultBodyLimit::max(15 * 1024 * 1024));
        }

        if self.routes.render {
            tracing::info!("/render (image export)");

            router = router.route("/render", get(render_viewport));
        }

        if self.cors {
            let cors = CorsLayer::new()
                .allow_methods([Method::GET])
                .allow_origin(Any);

            router = router.layer(cors);
        }

        // TODO: possibly better better as an Option
        let strava = if use_strava_auth {
            let auth = StravaAuth::from_env().map_err(|err| {
                anyhow!(
                    "Failed to load Strava credentials from environment: {}",
                    err
                )
            })?;

            Some(auth)
        } else {
            None
        };

        let db_config = db.load_config()?;

        let router = router
            .layer(axum::middleware::from_fn(store_request_data))
            .layer(trace)
            .with_state(AppState {
                config: self.clone(),
                strava,
                db: Arc::new(db),
                db_config: Arc::new(db_config),
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

pub fn run_blocking(addr: SocketAddr, db: Database, config: Config) -> Result<()> {
    let rt = Runtime::new()?;
    let fut = run_async(addr, db, config);
    rt.block_on(fut)?;
    Ok(())
}

async fn index(State(AppState { config, db, .. }): State<AppState>) -> impl IntoResponse {
    let index_file = StaticAsset::get("index.html").expect("missing file");
    let html = std::str::from_utf8(&index_file.data).expect("valid utf8");
    let properties = db
        .count_properties()
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
            globalThis.RENDER_ENABLED = {};
            globalThis.ACTIVITY_PROPERTIES = {};
        ",
            config.routes.upload, config.routes.render, properties,
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
                Some((_, "css")) => "text/css",
                _ => "application/octet-stream",
            };
            ([(header::CONTENT_TYPE, mime)], content.data).into_response()
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

#[derive(Debug, Deserialize)]
struct RenderViewQueryParams {
    bounds: String,
    width: u32,
    height: u32,

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
                )));
            }
        };

        Ok(TileYParam { tile_size, y })
    }
}

async fn get_activity_count(
    State(AppState { db, .. }): State<AppState>,
    Query(params): Query<RenderQueryParams>,
) -> impl IntoResponse {
    let filter = ActivityFilter::new(params.before, params.after, params.filter);
    let num_activities = db
        .activity_count(&filter)
        .expect("failed to count activities");

    (StatusCode::OK, num_activities.to_string()).into_response()
}

async fn render_viewport(
    State(AppState { db, db_config, .. }): State<AppState>,
    Query(params): Query<RenderViewQueryParams>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let viewport = match WebMercatorViewport::from_str(&params.bounds) {
        Ok(viewport) => viewport,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid viewport given: {:?}", err),
            )
                .into_response();
        }
    };

    if params.height == 0 || params.height > 3000 || params.width == 0 || params.width > 3000 {
        return (
            StatusCode::BAD_REQUEST,
            "width/height must be in bounds [1, 3000]",
        )
            .into_response();
    }

    let filter = ActivityFilter::new(params.before, params.after, params.filter);

    let etag = match check_etag(&db, &headers) {
        Ok(etag) => etag,
        Err(status) => return status.into_response(),
    };

    let gradient = match choose_gradient(&params.gradient, params.color) {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    let image_format = get_image_format(&headers);
    raster::render_view(
        viewport,
        gradient,
        params.width,
        params.height,
        &filter,
        &db,
        &db_config,
    )
    .and_then(|image| render_image_response(image, image_format, &etag))
    .unwrap_or_else(|err| {
        tracing::error!("error rendering tile: {:?}", err);
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    })
}

async fn render_tile(
    State(AppState { db, db_config, .. }): State<AppState>,
    Path((z, x, y_param)): Path<(u8, u32, TileYParam)>,
    Query(params): Query<RenderQueryParams>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Fail fast when tile is higher zoom level than we store data for.
    if db_config.source_level(z).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }

    let filter = ActivityFilter::new(params.before, params.after, params.filter);

    let etag = match check_etag(&db, &headers) {
        Ok(etag) => etag,
        Err(status) => return status.into_response(),
    };

    let tile = Tile::new(x, y_param.y, z);
    let gradient = match choose_gradient(&params.gradient, params.color) {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    let image_format = get_image_format(&headers);
    raster::rasterize_tile(tile, y_param.tile_size, &filter, &db, &db_config)
        .and_then(|raster| {
            raster
                .map(|raster| raster.apply_gradient(gradient))
                .map(|image| render_image_response(image, image_format, &etag))
                .unwrap_or_else(|| {
                    Ok(cached_response(StatusCode::NO_CONTENT, &etag)
                        .body(Body::empty())
                        .unwrap()
                        .into_response())
                })
        })
        .unwrap_or_else(|err| {
            tracing::error!("error rendering tile: {:?}", err);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })
}

fn render_image_response(
    image: image::ImageBuffer<image::Rgba<u8>, Vec<u8>>,
    format: ImageFormat,
    etag: &str,
) -> Result<Response> {
    let mut bytes = Vec::new();
    let mut cursor = Cursor::new(&mut bytes);

    let (content_type, result) = match format {
        ImageFormat::WebP => {
            let encoder = WebPEncoder::new_lossless(&mut cursor);
            ("image/webp", image.write_with_encoder(encoder))
        }
        ImageFormat::Png => {
            let encoder = PngEncoder::new_with_quality(
                &mut cursor,
                CompressionType::Fast,
                FilterType::NoFilter,
            );
            ("image/png", image.write_with_encoder(encoder))
        }
    };

    result?;

    Ok(cached_response(StatusCode::OK, etag)
        .header(header::CONTENT_TYPE, content_type)
        .body(bytes)?
        .into_parts()
        .into_response())
}

// Build a partial response with Cache-Control and Etag headers set.
//
// - `public`: Allows caching by any cache (CDNs, proxies, browsers)
// - `max-age=0`: Forces revalidation on every request to ensure freshness
// - `stale-while-revalidate=86400`: Allows using stale content for 24 hours
//
// This provides immediate responses to users while ensuring they eventually see fresh data,
// with graceful degradation if the server is unavailable.
fn cached_response(status: StatusCode, etag: &str) -> axum::http::response::Builder {
    Response::builder()
        .status(status)
        .header(header::ETAG, etag)
        .header(
            header::CACHE_CONTROL,
            "public, max-age=0, stale-while-revalidate=86400",
        )
}

// Right now we assume that if the activity count changes then new data was
// added. You could trivially come up with a counter-example for this, but why
// would you want to?
fn generate_etag(db: &Database) -> Result<String, StatusCode> {
    let activity_count = db
        .activity_count(&ActivityFilter::default())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut hasher = DefaultHasher::new();
    activity_count.hash(&mut hasher);

    Ok(hasher.finish().to_string())
}

// Check if the request's ETag matches the current content
fn check_etag(db: &Database, headers: &HeaderMap) -> Result<String, StatusCode> {
    let client_etag = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|s| s.to_str().ok())
        .map(|s| s.trim_matches('"'));

    let etag = generate_etag(db)?;
    if let Some(t) = client_etag
        && t == etag
    {
        return Err(StatusCode::NOT_MODIFIED);
    }

    Ok(etag)
}

fn get_image_format(headers: &HeaderMap) -> ImageFormat {
    let accepts_webp = headers
        .get(header::ACCEPT)
        .and_then(|h| h.to_str().ok())
        .map(|accept| accept.to_lowercase().contains("image/webp"))
        .unwrap_or(false);

    if accepts_webp {
        ImageFormat::WebP
    } else {
        ImageFormat::Png
    }
}

fn choose_gradient(
    gradient: &Option<LinearGradient>,
    color: Option<String>,
) -> Result<&LinearGradient, &'static str> {
    match (gradient, color.as_deref()) {
        (Some(gradient), None) => Ok(gradient),
        (Some(_), Some(_)) => Err("cannot specify both gradient and color"),
        (None, None) => Ok(&raster::ORANGE),
        (None, Some("pinkish")) => Ok(&raster::PINKISH),
        (None, Some("blue-red")) => Ok(&raster::BLUE_RED),
        (None, Some("red")) => Ok(&raster::RED),
        (None, Some("orange")) => Ok(&raster::ORANGE),
        (None, Some(_)) => Err("invalid color name"),
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
    State(AppState {
        db,
        db_config,
        config,
        ..
    }): State<AppState>,
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
            .and_then(|mut conn| activity::upsert(&mut conn, &activity_id, &activity, &db_config))
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

#[cfg(test)]
mod tests {
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn test_tile_caching() {
        let db = Database::memory().expect("db create");
        let conn = db.connection().expect("db connection");
        let router = Config {
            cors: false,
            upload_token: None,
            routes: RouteConfig {
                tiles: true,
                strava_webhook: false,
                strava_auth: false,
                upload: false,
                render: true,
            },
        }
        .build_router(db)
        .unwrap();

        let request = Request::builder()
            .uri("/tile/0/0/0")
            .body(Body::empty())
            .expect("tile request");

        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let etag = response.headers().get(header::ETAG).expect("to see etag");

        let request = Request::builder()
            .uri("/tile/0/0/0")
            .header(header::IF_NONE_MATCH, etag.clone())
            .body(Body::empty())
            .expect("tile request w/ if-none-match");

        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);

        // now, modify the database and ensure we get a new etag
        conn.execute("INSERT INTO activities (file) VALUES (?)", ["foo"])
            .expect("to insert dummy activity");

        let request = Request::builder()
            .uri("/tile/0/0/0")
            .header(header::IF_NONE_MATCH, etag.clone())
            .body(Body::empty())
            .expect("tile request");

        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let etag2 = response.headers().get(header::ETAG).unwrap().clone();
        assert_ne!(etag, etag2);
    }
}
