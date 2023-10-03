use std::io::Cursor;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use axum::extract::{Path, State};
use axum::http::header;
use axum::{response::IntoResponse, routing::get, Router};
use image::codecs::png::{CompressionType, FilterType, PngEncoder};
use tokio::runtime::Runtime;

use crate::db::Database;
use crate::raster::LinearGradient;
use crate::tile::Tile;

pub fn run(db: Database, host: &str, port: u16) -> Result<()> {
    let rt = Runtime::new()?;

    rt.block_on(run_async(db, host, port))?;
    Ok(())
}

async fn run_async(db: Database, host: &str, port: u16) -> Result<()> {
    // TODO: MVT endpoint?
    let app = Router::new()
        .route("/tile/:z/:x/:y", get(render_tile))
        .with_state(Arc::new(db));

    let host = host.parse::<IpAddr>()?;
    let addr = SocketAddr::from((host, port));

    println!("Listening on http://{}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

// TODO: time based filters etc., caching
async fn render_tile(
    State(db): State<Arc<Database>>,
    Path((z, x, y)): Path<(u8, u32, u32)>,
) -> impl IntoResponse {
    if z > *db.meta.zoom_levels.iter().max().unwrap_or(&0) {
        return axum::http::StatusCode::NO_CONTENT.into_response();
    }

    let tile = Tile::new(x, y, z);

    let start = Instant::now();
    let raster = super::render_tile(tile, &db, 512).unwrap();
    let render_time = start.elapsed();

    // TODO: gradient  doesn't belong here.
    let image = raster.apply_gradient(&LinearGradient::from_stops(&[
        (0.00, [0xb2, 0x0a, 0x2c, 0xff]),
        (0.25, [0xff, 0xfb, 0xd5, 0xff]),
        (1.00, [0xff, 0xff, 0xff, 0xff]),
    ]));
    let grad_time = start.elapsed() - render_time;

    println!(
        "{:?}: render time: {:?}, gradient time {:?}",
        tile, render_time, grad_time
    );

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
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ]),
        bytes,
    )
        .into_response()
}
