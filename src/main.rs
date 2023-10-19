use std::fs::File;
use std::path::PathBuf;
use std::str::FromStr;
use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use image::RgbaImage;
use rayon::prelude::*;
use time::Date;
use activity::PropertySource;

use crate::db::{ActivityFilter, Database};
use crate::raster::DEFAULT_GRADIENT;
use crate::tile::Tile;

mod activity;
mod date;
mod db;
mod raster;
mod simplify;
mod tile;
mod web;

#[derive(Subcommand)]
enum Commands {
    /// Import activities from GPX, TCX, and FIT files.
    ///
    /// Imports will be deduplicated (based on file name), so it's safe to run
    /// this twice on the same directory.
    Import {
        /// Path to activity data.
        ///
        /// Can also pass a path to a single file.
        path: PathBuf,

        /// Remove all existing activity data before importing.
        #[arg(long, default_value = "false")]
        reset: bool,

        /// Hide points within given distance (meters) of start/end of activity.
        #[arg(short, long, default_value = "200.0")]
        trim: f64,

        /// Path to a CSV with additional activity metadata.
        ///
        /// The `filename` column contains paths (relative to the CSV file)
        /// which will assign properties to each parsed activity.
        #[arg(long)]
        join: Option<PathBuf>,
    },

    /// Render a single XYZ tile as a PNG.
    Tile {
        /// Tile to render, in "z/x/y" format.
        zxy: Tile,

        /// Select activities before this date (YYYY-MM-DD).
        #[arg(short, long)]
        before: Option<String>,

        /// Select activities after this date (YYYY-MM-DD).
        #[arg(short, long)]
        after: Option<String>,

        // TODO: not yet supported (need to write a from_str)
        // /// Filter activities by arbitrary metadata properties
        // #[arg(short, long)]
        // filter: Option<PropertyFilter>,
        /// Width of output image in pixels.
        #[arg(short, long, default_value = "1024")]
        width: u32,

        /// Path to output image.
        #[arg(short, long, default_value = "tile.png")]
        output: PathBuf,
    },

    /// Start an XYZ raster tile server.
    Serve {
        /// Host to listen on.
        #[arg(short = 'H', long, default_value = "127.0.0.1")]
        host: String,

        /// Port to listen on.
        #[arg(short, long, default_value = "8080")]
        port: u16,

        /// Allow uploading new activities via `/upload` endpoint.
        ///
        /// Remember to set `HOTPOT_UPLOAD_TOKEN` environment variable.
        #[arg(long, default_value = "false")]
        upload: bool,

        /// Enable Strava activity webhook
        ///
        /// Use `strava-auth` subcommand to grab OAuth tokens.
        #[arg(long, default_value = "false")]
        strava_webhook: bool,

        /// Allow cross origin requests (use CORS headers)
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

#[derive(Args)]
struct GlobalOpts {
    /// Path to database
    #[arg(short = 'D', long = "db", default_value = "./hotpot.sqlite3")]
    db_path: PathBuf,
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Parser)]
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

    tracing_subscriber::fmt()
        .compact()
        .with_max_level(if opts.global.verbose {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .init();

    // TODO: pull out into separate function
    match opts.cmd {
        Commands::Import {
            path,
            reset,
            join,
            ..
            // TODO: update the database config with this
            // trim,
        } => {
            let db = Database::new(&opts.global.db_path)?;

            let prop_source = join
                .map(|csv| PropertySource::from_csv(&csv))
                .transpose()?
                .unwrap_or_default();

            if reset {
                db.reset_activities()?;
            }

            activity::import_path(&path, &db, &prop_source)?;
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

            let filter = ActivityFilter::new(before, after, None);

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
