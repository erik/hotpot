use std::io::Cursor;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use anyhow::Result;
use axum::extract::{Path, State};
use axum::http::header;
use axum::{response::IntoResponse, routing::get, Router};
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
    let tile = Tile::new(x, y, z);
    println!("rendering tile: {:?}", tile);

    let raster = super::render_tile(tile, &db, 512).unwrap();

    // TODO: gradient  doesn't belong here.
    let image = raster.apply_gradient(&LinearGradient::from_stops(&[
        (0.0, [0xff, 0xb1, 0xff, 0xff]),
        (0.05, [0xff, 0xb1, 0xff, 0xff]),
        (0.25, [0xff, 0xff, 0xff, 0xff]),
    ]));

    // TODO: compression
    let mut bytes = Vec::new();
    let mut cursor = Cursor::new(&mut bytes);
    image
        .write_to(&mut cursor, image::ImageOutputFormat::Png)
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
}
