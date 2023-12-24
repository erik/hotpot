use std::fs::File;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use image::RgbaImage;
use tile::TileBounds;
use time::Date;

use activity::PropertySource;

use crate::db::{ActivityFilter, Database, PropertyFilter};
use crate::raster::PINKISH;
use crate::tile::Tile;

mod activity;
mod date;
mod db;
mod raster;
mod simplify;
mod tile;
mod web;

#[derive(Clone)]
struct LngLatBounds {
    sw: tile::LngLat,
    ne: tile::LngLat,
}

impl LngLatBounds {
    fn tile_viewport(
        &self,
        px_width: u32,
        px_height: u32,
        zoom_range: RangeInclusive<u32>,
    ) -> tile::TileBounds {
        let tile_extent = 256;
        let min_zoom = *zoom_range.start();
        let max_zoom = *zoom_range.end();

        let sw_xy = self.sw.xy().expect("invalid coord");
        let ne_xy = self.ne.xy().expect("invalid coord");

        let sw_px = sw_xy.to_global_pixel(max_zoom as u8, tile_extent);
        let ne_px = ne_xy.to_global_pixel(max_zoom as u8, tile_extent);

        let scale = f64::max(
            (ne_px.x() - sw_px.x()) as f64 / (px_width as f64),
            (sw_px.y() - ne_px.y()) as f64 / (px_height as f64),
        );

        let zoom = (max_zoom - scale.log2().floor() as u32).clamp(min_zoom, max_zoom) as u8;
        let sw_tile = sw_xy.tile(zoom);
        let ne_tile = ne_xy.tile(zoom);

        TileBounds {
            z: zoom,
            xmin: sw_tile.x,
            xmax: ne_tile.x,
            ymin: ne_tile.y,
            ymax: sw_tile.y,
        }
    }
}

impl FromStr for LngLatBounds {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let parts: Vec<_> = s
            .split(',')
            .filter_map(|it| it.parse::<f64>().ok())
            .collect();

        if parts.len() != 4 {
            return Err("expected coordinates as 'west,south,east,north'");
        }

        let sw = tile::LngLat((parts[0], parts[1]).into());
        let ne = tile::LngLat((parts[2], parts[3]).into());

        if sw.0.x() >= ne.0.x() {
            Err("invalid west/east bounds")
        } else if sw.0.y() >= ne.0.y() {
            Err("invalid south/north bounds")
        } else {
            Ok(LngLatBounds { sw, ne })
        }
    }
}

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
        #[arg(long)]
        bounds: LngLatBounds,

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
        #[arg(short, long)]
        filter: Option<PropertyFilter>,

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
            trim,
        } => {
            let mut db = Database::new(&opts.global.db_path)?;

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
            filter: props,
            before,
            after,
        } => {
            let db = Database::open(&opts.global.db_path)?;
            let mut file = File::create(output)?;

            let filter = ActivityFilter::new(before, after, props);
            let image =
                raster::render_tile(zxy, &PINKISH, width, &filter, &db)?.unwrap_or_else(|| {
                    // note: could also just use RgbaImage::default() here if we don't care about size.
                    RgbaImage::new(width, width)
                });

            image.write_to(&mut file, image::ImageOutputFormat::Png)?;
        }

        Commands::Render {
            bounds,
            mut width,
            mut height,
            before,
            after,
            filter: props,
            output,
        } => {
            let db = Database::open(&opts.global.db_path)?;
            let tiles = bounds.tile_viewport(width, height, 0..=16);
            let num_x = tiles.xmax - tiles.xmin + 1;
            let num_y = tiles.ymax - tiles.ymin + 1;

            let mosaic_width = num_x * 256;
            let mosaic_height = num_y * 256;
            if mosaic_width < width || mosaic_height < height {
                println!(
                    "[WARN] source data is not high resolution for requested image dimensions, clamping to {}x{}.",
                    mosaic_width, mosaic_height
                );

                height = u32::min(height, mosaic_height);
                width = u32::min(width, mosaic_width);
            }

            println!(
                "Rendering {} subtiles at zoom={}...",
                num_x * num_y,
                tiles.z
            );

            let filter = ActivityFilter::new(before, after, props);
            let mut mosaic = RgbaImage::new(mosaic_width, mosaic_height);

            for row in 0..num_y {
                for col in 0..num_x {
                    let tile = Tile::new(tiles.xmin + col, tiles.ymin + row, tiles.z);
                    let img = raster::render_tile(tile, &PINKISH, 256, &filter, &db)?;

                    if let Some(img) = img {
                        let orig_x = col * 256;
                        let orig_y = row * 256;

                        for x in 0..256 {
                            for y in 0..256 {
                                mosaic.put_pixel(orig_x + x, orig_y + y, *img.get_pixel(x, y));
                            }
                        }
                    }
                }
            }

            // The tile bounds will be aligned to the tile grid, so we need to trim
            // the excess pixels from the edges of the image.
            let off_x = (mosaic_width - width) / 2;
            let off_y = (mosaic_height - height) / 2;

            let crop = image::imageops::crop(&mut mosaic, off_x, off_y, width, height);
            let mut file = File::create(output)?;

            // TODO: should be able to avoid `.to_image()` here?
            crop.to_image()
                .write_to(&mut file, image::ImageOutputFormat::Png)?;
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
