use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use image::RgbaImage;
use rayon::prelude::*;
use time::Date;
use walkdir::WalkDir;

use db::ActivityFilter;

use crate::db::Database;
use crate::raster::DEFAULT_GRADIENT;
use crate::tile::Tile;

mod activity;
mod date;
mod db;
mod raster;
mod simplify;
mod tile;
mod web;

// TODO: make this configurable
const DEFAULT_ZOOM_LEVELS: [u8; 5] = [2, 6, 10, 14, 16];
const DEFAULT_TILE_EXTENT: u32 = 2048;

#[derive(Subcommand, Debug)]
enum Commands {
    /// Import GPX and FIT files from a directory
    Import {
        /// Path to directory of activities.
        path: PathBuf,

        /// Remove all existing activity data before importing.
        #[arg(long, default_value = "false")]
        reset: bool,

        /// Hide points within given distance (in meters) of start/end of activity.
        #[arg(short, long, default_value = "200.0")]
        trim: f64,
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

        /// Allow uploading of activities via `/upload`.
        ///
        /// Remember to set `HOTPOT_UPLOAD_TOKEN` environment variable.
        #[arg(long, default_value = "false")]
        upload: bool,

        /// Enable Strava activity webhook
        ///
        /// Use `strava-auth` subcommand to grab OAuth tokens.
        #[arg(long, default_value = "false")]
        strava_webhook: bool,

        /// Allow cross origin requests (CORS headers)
        #[arg(long, default_value = "false")]
        cors: bool,
    },

    /// Authenticate with Strava to fetch OAuth tokens for webhook.
    StravaAuth {
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
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
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

    let log_level = if opts.global.verbose {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };

    tracing_subscriber::fmt()
        .compact()
        .with_max_level(log_level)
        .init();

    // TODO: pull out into separate function
    match opts.cmd {
        Commands::Import { path, reset, trim } => {
            let db = Database::new(&opts.global.db_path)?;

            if reset {
                db.reset_activities()?;
            }

            ingest_dir(&path, &db, trim)?;
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

            let mut file = File::create(output)?;

            let filter = ActivityFilter::new(before, after);

            let image = raster::render_tile(zxy, &DEFAULT_GRADIENT, width, &filter, &db)?
                .unwrap_or_else(|| {
                    // note: could also just use RgbaImage::default() here if we don't care about size.
                    RgbaImage::new(width, width)
                });

            image.write_to(&mut file, image::ImageOutputFormat::Png)?;
        }

        Commands::Serve {
            host,
            port,
            upload,
            strava_webhook,
            cors,
        } => {
            let db = Database::new(&opts.global.db_path)?;
            let addr = format!("{}:{}", host, port).parse()?;
            let routes = web::RouteConfig {
                strava_webhook,
                upload,
                tiles: true,
                strava_auth: false,
            };

            let config = web::Config {
                cors,
                upload_token: std::env::var("HOTPOT_UPLOAD_TOKEN").ok(),
            };

            web::run_blocking(addr, db, config, routes)?;
        }

        Commands::StravaAuth { host, port } => {
            let db = Database::new(&opts.global.db_path)?;
            let addr = format!("{}:{}", host, port).parse()?;
            let routes = web::RouteConfig {
                strava_auth: true,
                tiles: false,
                strava_webhook: false,
                upload: false,
            };

            let config = web::Config {
                cors: false,
                upload_token: None,
            };

            println!(
                "==============================\
                \nOpen http://{}/strava/auth in your browser.\
                \n==============================",
                addr
            );
            web::run_blocking(addr, db, config, routes)?;
        }
    };

    Ok(())
}

fn ingest_dir(p: &Path, db: &Database, trim_dist: f64) -> Result<()> {
    let conn = db.connection()?;

    // Skip any files that are already in the database.
    let known_files: HashSet<String> = conn
        .prepare("SELECT file FROM activities")?
        .query_map([], |row| row.get(0))?
        .filter_map(|n| n.ok())
        .collect();

    tracing::info!(
        path = ?p,
        num_known = known_files.len(),
        "starting activity import"
    );

    let num_imported = AtomicU32::new(0);
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
            let activity = activity::read_file(&path)
                .map_err(|err| tracing::error!(?path, ?err, "failed to read activity"))
                .ok()??;

            Some((path, activity))
        })
        .for_each_init(
            || db.shared_pool(),
            |pool, (path, activity)| {
                tracing::debug!(?path, "importing activity");

                let mut conn = pool.get().expect("db connection pool timed out");
                activity::upsert(&mut conn, path.to_str().unwrap(), &activity, trim_dist)
                    .expect("insert activity");

                num_imported.fetch_add(1, Ordering::Relaxed);
            },
        );

    conn.execute_batch("VACUUM")?;
    tracing::info!(?num_imported, "finished import");
    Ok(())
}
