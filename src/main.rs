use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use image::RgbaImage;
use rayon::prelude::*;
use serde_json::Value;
use time::Date;
use walkdir::WalkDir;

use crate::activity::RawActivity;
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

// TODO: Remove all direct uses of these, replace with DB config.
const DEFAULT_ZOOM_LEVELS: [u8; 5] = [2, 6, 10, 14, 16];
const DEFAULT_TILE_EXTENT: u32 = 2048;
const DEFAULT_TRIM_DIST: f64 = 200.0;

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

        /// Path to CSV containing additional activity metadata.
        #[arg(long)]
        attrs_path: Option<PathBuf>,

        /// Column from attribute file to join to activity file.
        #[arg(long, default_value = "Filename")]
        attrs_file_col: String,
        // TODO: figure out if we should use this?
        // /// Column from attribute file to use as activity title.
        // #[arg(long, default_value = "Activity Name")]
        // attrs_title_col: String,
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
    #[arg(default_value = "./hotpot.sqlite3")]
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
            attrs_path,
            attrs_file_col,
            ..
            // TODO: update the database config with this
            // trim,
        } => {
            let db = Database::new(&opts.global.db_path)?;
            let attrs = attrs_path
                .map(|csv| AttributeSource::from_csv(
                    &csv,
                    &attrs_file_col,
                ))
                .transpose()?
                .unwrap_or_default();

            if reset {
                db.reset_activities()?;
            }

            import_activities(&path, &db, &attrs)?;
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

struct AttributeSource {
    base_dir: PathBuf,
    path_props: HashMap<PathBuf, HashMap<String, Value>>,
}

impl Default for AttributeSource {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::new(),
            path_props: HashMap::new(),
        }
    }
}

impl AttributeSource {
    fn from_csv(csv_path: &Path, file_col: &str) -> Result<Self> {
        let base_dir = csv_path.parent().unwrap_or(Path::new("/")).canonicalize()?;

        let mut rdr = csv::Reader::from_path(csv_path)?;
        let mut path_props = HashMap::new();
        for row in rdr.deserialize() {
            let mut row: HashMap<String, String> = row?;

            // Only keep the non-empty keys
            row.retain(|_k, v| !v.trim().is_empty());

            // TODO: report error if this is missing
            let Some(filename) = row.remove(file_col) else {
                continue;
            };

            let json_props = row
                .into_iter()
                .map(|(k, v)| (k, Value::String(v)))
                .collect();

            path_props.insert(PathBuf::from(filename), json_props);
        }

        Ok(Self {
            base_dir,
            path_props,
        })
    }

    /// Merge properties from the attribute source into the activity.
    fn enrich(&self, path: &Path, activity: &mut RawActivity) {
        let path = path.strip_prefix(&self.base_dir).ok();
        let Some(props) = path.and_then(|p| self.path_props.get(p)) else {
            // We'll get here if there are activities in the import directory which don't have
            // a corresponding line in the metadata file.
            return;
        };

        for (k, v) in props {
            activity.properties.insert(k.clone(), v.clone());
        }
    }
}

fn import_activities(p: &Path, db: &Database, prop_source: &AttributeSource) -> Result<()> {
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
            |pool, (path, mut activity)| {
                tracing::debug!(?path, "importing activity");

                // Merge with activity properties
                prop_source.enrich(&path, &mut activity);

                let mut conn = pool.get().expect("db connection pool timed out");
                activity::upsert(
                    &mut conn,
                    path.to_str().unwrap(),
                    &activity,
                    db.config.trim_dist,
                )
                .expect("insert activity");

                num_imported.fetch_add(1, Ordering::Relaxed);
            },
        );

    conn.execute_batch("VACUUM")?;
    tracing::info!(?num_imported, "finished import");
    Ok(())
}
