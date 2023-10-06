use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use clap::{Args, Parser, Subcommand};
use rayon::prelude::*;
use rusqlite::params;
use time::Date;
use walkdir::WalkDir;

use db::ActivityFilter;

use crate::db::{decode_line, encode_line, Database};
use crate::raster::{TileRaster, DEFAULT_GRADIENT};
use crate::tile::{Tile, TileBounds};

mod activity;
mod date;
mod db;
mod raster;
mod tile;
mod web;

// TODO: make this configurable
const DEFAULT_ZOOM_LEVELS: [u8; 5] = [2, 6, 10, 14, 16];
const DEFAULT_TILE_EXTENT: u32 = 2048;

#[derive(Subcommand, Debug)]
enum Commands {
    /// Import GPX and FIT files from a directory
    Import {
        /// Path to directory of activities
        path: PathBuf,

        /// Reset the database before importing
        #[arg(short, long, default_value = "false")]
        create: bool,
    },

    /// Render a tile
    Tile {
        /// Tile to render, in "z/x/y" format
        zxy: Tile,

        /// Select activities before this date
        #[arg(short, long)]
        before: Option<String>,

        /// Select activities after this date
        #[arg(short, long)]
        after: Option<String>,

        /// Width of output image
        #[arg(short, long, default_value = "1024")]
        width: u32,

        /// Path to output image
        #[arg(short, long, default_value = "tile.png")]
        output: PathBuf,
    },

    /// Start a raster tile server
    Serve {
        /// Host to listen on
        #[arg(short = 'H', long, default_value = "127.0.0.1")]
        host: String,

        /// Port to listen on
        #[arg(short, long, default_value = "8080")]
        port: u16,
    },
}

#[derive(Args, Debug)]
struct GlobalOpts {
    /// Path to database
    #[arg(default_value = "./hotpot.sqlite3")]
    db_path: PathBuf,
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Opts {
    #[clap(flatten)]
    global: GlobalOpts,

    /// Subcommand
    #[command(subcommand)]
    cmd: Commands,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let opts = Opts::parse();

    // TODO: pull out into separate function
    match opts.cmd {
        Commands::Import { path, create } => {
            if create {
                Database::delete(&opts.global.db_path)?;
            }

            ingest_dir(&path, &Database::new(&opts.global.db_path)?)?;
        }

        Commands::Tile {
            zxy,
            width,
            output,
            before,
            after,
        } => {
            let db = Database::open(&opts.global.db_path)?;

            // TODO: can we reuse the parser in web.rs?
            let parse = |t: String| {
                Date::parse(
                    t.as_str(),
                    &time::format_description::well_known::Iso8601::DATE,
                )
            };

            let before = before.map(parse).transpose()?;
            let after = after.map(parse).transpose()?;

            let filter = ActivityFilter::new(before, after);
            let raster = render_tile(zxy, &db, &filter, width)?;
            let image = raster.apply_gradient(&DEFAULT_GRADIENT);

            image.write_to(&mut File::create(output)?, image::ImageOutputFormat::Png)?;
        }

        Commands::Serve { host, port } => {
            let db = Database::open(&opts.global.db_path)?;
            web::run(db, &host, port)?;
        }
    };

    Ok(())
}

// TODO: doesn't belong in main
pub fn render_tile(
    tile: Tile,
    db: &Database,
    filter: &ActivityFilter,
    width: u32,
) -> Result<TileRaster> {
    let zoom_level = db
        .meta
        .source_level(tile.z)
        .ok_or_else(|| anyhow!("no source level for tile: {:?}", tile))?;

    let bounds = TileBounds::from(zoom_level, &tile);
    let mut raster = TileRaster::new(tile, bounds, width);
    let conn = db.connection()?;

    let mut params = params![bounds.z, bounds.xmin, bounds.xmax, bounds.ymin, bounds.ymax].to_vec();
    let filter_clause = filter.to_query(&mut params);

    // TODO: don't always need to join
    let mut stmt = conn.prepare(
        format!(
            "\
                SELECT x, y, z, coords \
                FROM activity_tiles \
                JOIN activities ON activities.id = activity_tiles.activity_id \
                WHERE z = ? \
                    AND (x >= ? AND x < ?) \
                    AND (y >= ? AND y < ?) \
                    AND {};",
            filter_clause,
        )
        .as_str(),
    )?;

    let mut rows = stmt.query(params.as_slice())?;
    while let Some(row) = rows.next()? {
        let tile = Tile::new(row.get_unwrap(0), row.get_unwrap(1), row.get_unwrap(2));

        let bytes: Vec<u8> = row.get_unwrap(3);
        raster.add_activity(&tile, &decode_line(&bytes)?);
    }

    Ok(raster)
}

fn ingest_dir(p: &Path, db: &Database) -> Result<()> {
    let conn = db.connection()?;

    // Skip any files that are already in the database.
    // TODO: avoid the collect call here?
    let known_files: HashSet<String> = conn
        .prepare("SELECT file FROM activities")?
        .query_map([], |row| row.get(0))?
        .filter_map(|n| n.ok())
        .collect();

    WalkDir::new(p)
        .into_iter()
        .par_bridge()
        .filter_map(|dir| {
            let dir = dir.ok()?;
            let path = dir.path();

            if !known_files.contains(path.to_str()?) {
                Some(path.to_owned())
            } else {
                None
            }
        })
        .filter_map(|path| {
            let activity = activity::read_file(&path)?;
            Some((path, activity))
        })
        .for_each_init(
            || db.shared_pool(),
            |pool, (path, activity)| {
                let conn = pool.get().expect("db connection pool timed out");

                let mut insert_coords = conn
                    .prepare_cached(
                        "\
                        INSERT INTO activity_tiles (activity_id, z, x, y, coords) \
                        VALUES (?, ?, ?, ?, ?)",
                    )
                    .unwrap();

                conn.execute(
                    "\
                    INSERT INTO activities (file, title, start_time, duration_secs, dist_meters)\
                    VALUES (?, ?, ?, ?, ?)",
                    params![
                        path.to_str().unwrap(),
                        activity.title,
                        activity.start_time,
                        activity.duration_secs,
                        activity.length(),
                    ],
                )
                .expect("insert activity");

                let activity_id = conn.last_insert_rowid();

                // TODO: split out into separate function
                // TODO: encode multiline strings together in same blob?
                let tiles = activity.clip_to_tiles(&db.meta.zoom_levels);
                for (tile, line) in tiles.iter() {
                    // TODO: can consider storing post rasterization for faster renders.
                    let simplified = activity::simplify(&line.0, 4.0);
                    let encoded = encode_line(&simplified).expect("encode line");

                    insert_coords
                        .insert(params![activity_id, tile.z, tile.x, tile.y, encoded])
                        .expect("insert coords");
                }

                print!(".");
            },
        );

    conn.execute_batch("VACUUM")?;
    Ok(())
}
