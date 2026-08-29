use std::hint::black_box;
use std::io::Cursor;
use std::path::PathBuf;
use std::str::FromStr;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use image::ImageEncoder;
use image::codecs::png::{CompressionType, FilterType, PngEncoder};

use hotpot::db::{ActivityFilter, Config, Database};
use hotpot::filter::PropertyFilter;
use hotpot::raster::{PINKISH, rasterize_tile, render_view};
use hotpot::tile::{BBox, Tile, WebMercator, WebMercatorViewport};
use rusqlite::Row;
use time::Date;

const SIZES: [u32; 2] = [256, 1024];

#[derive(Debug)]
struct TileSamples {
    dense: Tile,
    sparse: Tile,
}

fn bench_db() -> (Database, Config, TileSamples) {
    let path = PathBuf::from(
        std::env::var("HOTPOT_BENCH_DB").unwrap_or_else(|_| "hotpot.sqlite3".to_string()),
    );

    let db = Database::open(&path).expect("open benchmark database");
    let config = db.load_config().expect("load database config");

    let conn = db.connection().expect("db");

    let uniq_tile_count: usize = conn
        .query_row(
            "SELECT count(distinct x || ',' || y || ',' || z) FROM activity_tiles;",
            [],
            |r| r.get(0),
        )
        .expect("query activity count");

    assert_ne!(
        uniq_tile_count, 0,
        "database has empty activity_tiles records"
    );

    const SAMPLE_SQL: &str = "
        WITH agg_tiles AS (
            SELECT x, y, z, sum(length(coords)) as total_size
            FROM activity_tiles
            GROUP BY x, y, z
        )
        SELECT x, y, z
        FROM agg_tiles
        ORDER BY total_size DESC
        LIMIT 1
        OFFSET ?
    ";

    let row_to_tile = |r: &Row<'_>| {
        let x: u32 = r.get_unwrap(0);
        let y: u32 = r.get_unwrap(1);
        let z: u8 = r.get_unwrap(2);

        // Take a tile that's a bit zoomed out from what's stored in the DB so
        // renderer needs to do more work.
        Ok(Tile::new(x >> 1, y >> 1, z - 1))
    };

    // Grab some representative tiles from this DB: most dense with activity
    // data and roughly the median
    let samples = TileSamples {
        dense: conn.query_row(SAMPLE_SQL, [0], row_to_tile).unwrap(),
        sparse: conn
            .query_row(SAMPLE_SQL, [uniq_tile_count / 2], row_to_tile)
            .unwrap(),
    };

    eprintln!("chose sample tiles: {:?}", samples);

    (db, config, samples)
}

fn encode_png(image: &image::RgbaImage) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut cursor = Cursor::new(&mut bytes);
    let encoder =
        PngEncoder::new_with_quality(&mut cursor, CompressionType::Fast, FilterType::NoFilter);
    encoder
        .write_image(
            image.as_raw(),
            image.width(),
            image.height(),
            image::ExtendedColorType::Rgba8,
        )
        .expect("encode png");
    bytes
}

fn bench_rasterize_tile(c: &mut Criterion) {
    let (db, config, samples) = bench_db();
    let mut group = c.benchmark_group("rasterize_tile");

    for size in SIZES {
        group.throughput(Throughput::Bytes((size as u64) * (size as u64)));
        for (name, tile) in [("dense", samples.dense), ("sparse", samples.sparse)] {
            group.bench_with_input(BenchmarkId::new(name, size), &size, |b, &size| {
                b.iter(|| {
                    let raster =
                        rasterize_tile(tile, size, &ActivityFilter::default(), &db, &config)
                            .expect("rasterize tile");

                    let bytes = raster
                        .map(|raster| encode_png(&raster.apply_gradient(&PINKISH)))
                        .unwrap_or_default();

                    black_box(bytes)
                })
            });
        }
    }

    let filter = ActivityFilter::new(
        None,
        Date::from_calendar_date(2021, time::Month::June, 1).ok(),
        PropertyFilter::from_str("elevation_gain > 1000").ok(),
    );
    group.throughput(Throughput::Bytes(512 * 512));

    for (name, tile) in [
        ("filtered/dense", samples.dense),
        ("filtered/sparse", samples.sparse),
    ] {
        group.bench_function(BenchmarkId::new(name, 512), |b| {
            b.iter(|| {
                let raster =
                    rasterize_tile(tile, 512, &filter, &db, &config).expect("rasterize tile");

                let bytes = raster
                    .map(|raster| encode_png(&raster.apply_gradient(&PINKISH)))
                    .unwrap_or_default();

                black_box(bytes)
            })
        });
    }

    group.finish();
}

fn bench_render_view(c: &mut Criterion) {
    let (db, config, samples) = bench_db();

    let mut group = c.benchmark_group("render_view");

    let size = 2048;
    group.throughput(Throughput::Bytes((size as u64) * (size as u64)));
    for (name, tile) in [("dense", samples.dense), ("sparse", samples.sparse)] {
        let bbox = tile.xy_bounds();
        let width = bbox.right - bbox.left;
        let height = bbox.top - bbox.bot;

        // fudge the bounds a bit so we're not rendering only one tile.
        let bbox = BBox {
            left: bbox.left - width * 0.15,
            right: bbox.right + width * 0.15,
            top: bbox.top + height * 0.15,
            bot: bbox.bot - height * 0.15,
        };

        let viewport = WebMercatorViewport {
            sw: WebMercator((bbox.left, bbox.bot).into()),
            ne: WebMercator((bbox.right, bbox.top).into()),
        };

        group.bench_function(BenchmarkId::new(name, size), |b| {
            b.iter(|| {
                let raster = render_view(
                    viewport.clone(),
                    &PINKISH,
                    size,
                    size,
                    &ActivityFilter::default(),
                    &db,
                    &config,
                )
                .expect("render viewport");

                black_box(raster)
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_rasterize_tile, bench_render_view);
criterion_main!(benches);
