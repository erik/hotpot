use std::fs::File;
use std::ops::RangeInclusive;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use image::RgbaImage;
use tile::TileBounds;
use time::Date;

use activity::PropertySource;

use crate::db::{ActivityFilter, Database};
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
struct Ullr {
    ul: tile::LngLat,
    br: tile::LngLat,
}

impl Ullr {
    fn tile_viewport(
        &self,
        px_width: usize,
        px_height: usize,
        zoom_range: RangeInclusive<usize>,
    ) -> tile::TileBounds {
        let tile_extent = 256;
        let min_zoom = *zoom_range.start();
        let max_zoom = *zoom_range.end();

        let ul_xy = self.ul.xy().expect("invalid coord");
        let br_xy = self.br.xy().expect("invalid coord");

        let ul = ul_xy.to_global_pixel(max_zoom as u8, tile_extent);
        let br = br_xy.to_global_pixel(max_zoom as u8, tile_extent);

        let scale = f64::min(
            (br.x() - ul.x()) as f64 / (px_width as f64),
            (br.y() - ul.y()) as f64 / (px_height as f64),
        );

        let zoom = (max_zoom - scale.log2().floor() as usize).clamp(min_zoom, max_zoom) as u8;
        let ul_tile = ul_xy.tile(zoom);
        let br_tile = br_xy.tile(zoom);

        println!(
            "
        tile_extent:  {:?}
        min/max zoom: {:?} / {:?}
        ul_xy         {:?} -> ul px {:?} -> ul tile {:?}
        br_xy         {:?} -> br px {:?} -> br tile {:?}
        scale         {:?}
        scale_log     {:?}
        ",
            tile_extent,
            min_zoom,
            max_zoom,
            ul_xy,
            ul,
            ul_tile,
            br_xy,
            br,
            br_tile,
            scale,
            scale.log2()
        );

        TileBounds {
            z: zoom,
            xmin: ul_tile.x,
            xmax: br_tile.x,
            ymin: ul_tile.y,
            ymax: br_tile.y,
        }
    }
}

// TODO: this should be TryFrom (or whatever clap's parser thing is)
impl From<String> for Ullr {
    fn from(value: String) -> Self {
        let parts: Vec<_> = value
            .split(',')
            .filter_map(|it| it.parse::<f64>().ok())
            .collect();

        if parts.len() != 4 {
            // TODO: better error here
            panic!("invalid ULLR")
        } else {
            Ullr {
                ul: tile::LngLat((parts[1], parts[0]).into()),
                br: tile::LngLat((parts[3], parts[2]).into()),
            }
        }
    }
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

    /// Render an arbitrary region, defined by a bounding box
    Render {
        // Coordinates: upper left, lower right (lat, lng, lat, lng)
        #[arg(long)]
        ullr: Ullr,

        /// Width of output image in pixels.
        #[arg(short, long, default_value = "1024")]
        width: usize,

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

            let image =
                raster::render_tile(zxy, &PINKISH, width, &filter, &db)?.unwrap_or_else(|| {
                    // note: could also just use RgbaImage::default() here if we don't care about size.
                    RgbaImage::new(width, width)
                });

            image.write_to(&mut file, image::ImageOutputFormat::Png)?;
        }

        Commands::Render {
            ullr,
            width,
            output,
        } => {
            let db = Database::open(&opts.global.db_path)?;
            let tiles = ullr.tile_viewport(width, 512, 0..=14);
            let num_x = 1 + tiles.xmax - tiles.xmin;
            let num_y = 1 + tiles.ymax - tiles.ymin;
            println!("tiles: {:?}", tiles);
            println!("Have to render {} subtiles...", num_x * num_y);
            let filter = ActivityFilter::new(None, None, None);

            // TODO: Use the given width/height
            let mut mosaic = RgbaImage::new(num_x * 256, num_y * 256);

            for row in 0..num_y {
                for col in 0..num_x {
                    let tile = Tile::new(tiles.xmin + col, tiles.ymin + row, tiles.z);
                    let img = raster::render_tile(tile, &PINKISH, 256, &filter, &db)?;

                    if let Some(img) = img {
                        let orig_x = col * 256;
                        let orig_y = row * 256;
                        println!(
                            "write:
                            row    {}\tcol {}
                            orig_x {}\t    {}
                        ",
                            row, col, orig_x, orig_y,
                        );

                        for x in 0..256 {
                            for y in 0..256 {
                                mosaic.put_pixel(orig_x + x, orig_y + y, *img.get_pixel(x, y));
                            }
                        }
                    }
                }
            }

            let mut file = File::create(output)?;
            mosaic.write_to(&mut file, image::ImageOutputFormat::Png)?;
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
