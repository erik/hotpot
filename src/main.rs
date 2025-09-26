use std::fs::File;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Args, Parser, Subcommand};
use image::RgbaImage;
use tile::WebMercatorViewport;
use time::Date;

use activity::PropertySource;

use crate::db::{ActivityFilter, Database, PropertyFilter};
use crate::raster::{LinearGradient, PINKISH};
use crate::tile::Tile;

mod activity;
mod date;
mod db;
mod raster;
mod strava;
mod tile;
mod web;

// TODO: move to `date` module, use a `FromStr` impl
fn try_parse_date(value: &str) -> Result<Date, &'static str> {
    Date::parse(value, &time::format_description::well_known::Iso8601::DATE)
        .map_err(|_| "invalid date")
}

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
        #[arg(short, long)]
        trim: Option<f64>,

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
        #[arg(short, long, value_parser = try_parse_date)]
        before: Option<Date>,

        /// Select activities after this date (YYYY-MM-DD).
        #[arg(short, long, value_parser = try_parse_date)]
        after: Option<Date>,

        /// Filter activities by arbitrary metadata properties
        #[arg(short, long)]
        filter: Option<PropertyFilter>,

        /// Custom color gradient to use for heatmap.
        ///
        /// Represented as a string of threshold values and colors, separated
        /// by `;`. Colors may be written as `RGB`, `RRGGBB`, or `RRGGBBAA`
        ///
        /// For example: `0:001122;25:789;50:334455;75:ffffff33`
        #[arg(short, long)]
        gradient: Option<LinearGradient>,

        /// Width of output image in pixels.
        #[arg(short, long, default_value = "1024")]
        width: u32,

        /// Path to output image.
        #[arg(short, long, default_value = "tile.png")]
        output: PathBuf,
    },

    /// Render an arbitrary region, defined by a bounding box
    Render {
        /// Coordinates in order of "west,south,east,north"
        ///
        /// Use a tool like https://boundingbox.klokantech.com/ to generate.
        #[arg(long = "bounds")]
        viewport: WebMercatorViewport,

        /// Width of output image in pixels.
        #[arg(short, long, default_value = "1024")]
        width: u32,

        /// Height of output image in pixels.
        #[arg(short = 'H', long, default_value = "1024")]
        height: u32,

        /// Select activities before this date (YYYY-MM-DD).
        #[arg(short, long, value_parser = try_parse_date)]
        before: Option<Date>,

        /// Select activities after this date (YYYY-MM-DD).
        #[arg(short, long, value_parser = try_parse_date)]
        after: Option<Date>,

        /// Filter activities by arbitrary metadata properties
        ///
        /// {"key": "elev_gain", ">": 1000}
        #[arg(short = 'f', long = "filter")]
        filter: Option<PropertyFilter>,

        /// Custom color gradient to use for heatmap.
        ///
        /// Represented as a string of threshold values and colors, separated
        /// by `;`. Colors may be written as `RGB`, `RRGGBB`, or `RRGGBBAA`
        ///
        /// For example: `0:001122;25:789;50:334455;75:ffffff33`
        #[arg(short, long)]
        gradient: Option<LinearGradient>,

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

        /// Allow exporting arbitrary viewports as images via `/render`
        /// endpoint.
        #[arg(long, default_value = "false")]
        render: bool,

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
    #[arg(
        short = 'D',
        long = "db",
        default_value = "./hotpot.sqlite3",
        conflicts_with = "in_memory"
    )]
    db_path: PathBuf,

    /// Create an in-memory database (data won't be persisted)
    #[arg(action, long, conflicts_with = "db_path")]
    in_memory: bool,

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

impl GlobalOpts {
    fn database_ro(&self) -> anyhow::Result<Database> {
        if self.in_memory {
            Err(anyhow!(
                "in-memory database is not supported for read-only operations"
            ))
        } else {
            Database::new(&self.db_path)
        }
    }

    fn database(&self) -> anyhow::Result<Database> {
        if self.in_memory {
            tracing::warn!("using empty in-memory DB, data will not be persisted");
            Database::memory()
        } else {
            Database::open(&self.db_path)
        }
    }
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
            trim,
        } => {
            let mut db = opts.global.database()?;

            // TODO: should be persisted to DB
            if let Some(trim) = trim {
                db.config.trim_dist = trim;
            }

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
            filter,
            before,
            after,
            gradient,
        } => {
            let db = opts.global.database_ro()?;
            let mut file = File::create(output)?;

            let filter = ActivityFilter::new(before, after, filter);
            let gradient = gradient.unwrap_or_else(|| PINKISH.clone());
            let image =
                raster::render_tile(zxy, &gradient, width, &filter, &db)?.unwrap_or_else(|| {
                    // note: could also just use RgbaImage::default() here if we don't care about size.
                    RgbaImage::new(width, width)
                });

            image.write_to(&mut file, image::ImageOutputFormat::Png)?;
        }

        Commands::Render {
            viewport,
            width,
            height,
            before,
            after,
            filter,
            gradient,
            output,
        } => {
            let db = opts.global.database_ro()?;
            let filter = ActivityFilter::new(before, after, filter);
            let gradient = gradient.unwrap_or_else(|| PINKISH.clone());
            let mut file = File::create(output)?;

            let image = raster::render_view(viewport, &gradient, width, height, &filter, &db)?;
            image.write_to(&mut file, image::ImageOutputFormat::Png)?;
        }

        Commands::Serve {
            host,
            port,
            upload,
            render,
            strava_webhook,
            cors,
        } => {
            let db = opts.global.database()?;

            let addr = format!("{}:{}", host, port).parse()?;
            let routes = web::RouteConfig {
                strava_webhook,
                upload,
                render,
                tiles: true,
                strava_auth: false,
            };

            let config = web::Config {
                cors,
                routes,
                upload_token: std::env::var("HOTPOT_UPLOAD_TOKEN").ok(),
            };

            web::run_blocking(addr, db, config)?;
        }

        Commands::StravaAuth { host, port } => {
            let db = opts.global.database()?;
            let addr = format!("{}:{}", host, port).parse()?;
            let routes = web::RouteConfig {
                strava_auth: true,
                tiles: false,
                strava_webhook: false,
                upload: false,
                render: false,
            };

            let config = web::Config {
                routes,
                cors: false,
                upload_token: None,
            };

            println!(
                "==============================\
                \nOpen http://{}/strava/auth in your browser.\
                \n==============================",
                addr
            );
            web::run_blocking(addr, db, config)?;
        }
    };

    Ok(())
}
