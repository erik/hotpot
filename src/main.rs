use std::backtrace::BacktraceStatus;
use std::fs::File;
use std::io::BufWriter;
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

#[derive(Subcommand)]
enum Commands {
    /// Query information about activities currently stored in the database.
    Activities(ActivitiesCmdArgs),

    /// Import activities from GPX, TCX, and FIT files.
    ///
    /// Imports will be deduplicated (based on file name), so it's safe to run
    /// this twice on the same directory.
    Import(ImportCmdArgs),

    /// Render a single XYZ tile as a PNG.
    Tile(TileCmdArgs),

    /// Render an arbitrary region, defined by a bounding box.
    Render(RenderCmdArgs),

    /// Start an XYZ raster tile server.
    Serve(ServeCmdArgs),

    /// Authenticate with Strava to fetch OAuth tokens for webhook.
    StravaAuth(StravaAuthCmdArgs),

    /// Add or remove areas to hide from rendered heatmaps.
    Mask(MaskCmdArgs),
}

#[derive(Args)]
struct ActivitiesCmdArgs {
    /// Select activities before this date (YYYY-MM-DD).
    #[arg(short, long, value_parser = date::try_parse)]
    before: Option<Date>,

    /// Select activities after this date (YYYY-MM-DD).
    #[arg(short, long, value_parser = date::try_parse)]
    after: Option<Date>,

    /// Filter activities by arbitrary metadata properties
    ///
    /// {"elev_gain": { ">": 1000 }}
    #[arg(short = 'f', long = "filter")]
    filter: Option<PropertyFilter>,

    /// Print count of matching activities rather than printing them out
    #[arg(short = 'c', long = "count", default_value = "false")]
    print_count: bool,
}

#[derive(Args)]
struct ImportCmdArgs {
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
}

#[derive(Args)]
struct TileCmdArgs {
    /// Tile to render, in "z/x/y" format.
    zxy: Tile,

    /// Select activities before this date (YYYY-MM-DD).
    #[arg(short, long, value_parser = date::try_parse)]
    before: Option<Date>,

    /// Select activities after this date (YYYY-MM-DD).
    #[arg(short, long, value_parser = date::try_parse)]
    after: Option<Date>,

    /// Filter activities by arbitrary metadata properties
    ///
    /// {"elev_gain": { ">": 1000 }}
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
}

#[derive(Args)]
struct RenderCmdArgs {
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
    #[arg(short, long, value_parser = date::try_parse)]
    before: Option<Date>,

    /// Select activities after this date (YYYY-MM-DD).
    #[arg(short, long, value_parser = date::try_parse)]
    after: Option<Date>,

    /// Filter activities by arbitrary metadata properties
    ///
    /// {"elev_gain": { ">": 1000 }}
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
}

#[derive(Args)]
struct ServeCmdArgs {
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
}

#[derive(Args)]
struct StravaAuthCmdArgs {
    /// Host to listen on
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on
    #[arg(short, long, default_value = "8080")]
    port: u16,
}

#[derive(Args)]
struct MaskCmdArgs {
    #[command(subcommand)]
    action: Option<MaskAction>,
}

#[derive(Subcommand)]
enum MaskAction {
    /// Add a hidden (masked) area
    Add {
        /// Name for this area
        name: String,

        /// Center coordinates as "longitude,latitude" in decimal degrees
        #[arg(short, long)]
        lnglat: tile::LngLat,

        /// Radius in meters
        #[arg(short, long, default_value = "500")]
        radius: f64,
    },

    /// Remove a mask by name.
    Remove {
        /// Name of the mask to remove
        name: String,
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
            Database::open(&self.db_path)
        }
    }

    fn database(&self) -> anyhow::Result<Database> {
        if self.in_memory {
            tracing::warn!("using empty in-memory DB, data will not be persisted");
            Database::memory()
        } else {
            Database::new(&self.db_path)
        }
    }
}

fn main() {
    if let Err(err) = run() {
        eprintln!("[ERROR] {:?}", err);
        let bt = err.backtrace();
        if bt.status() == BacktraceStatus::Captured {
            eprintln!("{}", bt);
        }
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let opts = Opts::parse();

    tracing_subscriber::fmt()
        .compact()
        .with_writer(std::io::stderr)
        .with_max_level(if opts.global.verbose {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .init();

    match opts.cmd {
        Commands::Activities(args) => command_activity_info(opts.global, args)?,
        Commands::Import(args) => command_import_activities(opts.global, args)?,
        Commands::Mask(args) => command_mask(opts.global, args)?,
        Commands::Render(args) => command_render_view(opts.global, args)?,
        Commands::Serve(args) => command_serve(opts.global, args)?,
        Commands::StravaAuth(args) => command_strava_auth(opts.global, args)?,
        Commands::Tile(args) => command_render_tile(opts.global, args)?,
    };

    Ok(())
}

fn command_activity_info(global: GlobalOpts, args: ActivitiesCmdArgs) -> Result<()> {
    let ActivitiesCmdArgs {
        before,
        after,
        filter,
        print_count,
    } = args;
    let db = global.database_ro()?;
    let filter = ActivityFilter::new(before, after, filter);

    if print_count {
        let num_activities = db.activity_count(&filter)?;
        println!("{}", num_activities);
    } else {
        for info in db.activity_info(&filter)? {
            println!(
                "{}",
                serde_json::to_string(&info).expect("ActivityStat should serialize to JSON")
            );
        }
    }
    Ok(())
}

fn command_import_activities(global: GlobalOpts, args: ImportCmdArgs) -> Result<()> {
    let ImportCmdArgs {
        path,
        reset,
        join,
        trim,
    } = args;
    let mut db = global.database()?;

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

    // Use absolute path for imports to dedupe more effectively
    let path = path
        .canonicalize()
        .map_err(|err| anyhow!("{:?} {}", path, err))?;

    activity::import_path(&path, &db, &prop_source)
}

fn command_render_tile(global: GlobalOpts, args: TileCmdArgs) -> Result<()> {
    let TileCmdArgs {
        zxy,
        width,
        output,
        filter,
        before,
        after,
        gradient,
    } = args;
    let db = global.database_ro()?;
    let mut file = BufWriter::new(File::create(output)?);

    let filter = ActivityFilter::new(before, after, filter);
    let gradient = gradient.unwrap_or_else(|| PINKISH.clone());
    let image = raster::rasterize_tile(zxy, width, &filter, &db)?
        .map(|raster| raster.apply_gradient(&gradient))
        .unwrap_or_else(|| {
            // note: could also just use RgbaImage::default() here if we don't care about size.
            RgbaImage::new(width, width)
        });

    image.write_to(&mut file, image::ImageFormat::Png)?;
    Ok(())
}

fn command_render_view(global: GlobalOpts, args: RenderCmdArgs) -> Result<()> {
    let RenderCmdArgs {
        viewport,
        width,
        height,
        before,
        after,
        filter,
        gradient,
        output,
    } = args;
    let db = global.database_ro()?;
    let filter = ActivityFilter::new(before, after, filter);
    let gradient = gradient.unwrap_or_else(|| PINKISH.clone());
    let mut file = BufWriter::new(File::create(output)?);

    let image = raster::render_view(viewport, &gradient, width, height, &filter, &db)?;
    image.write_to(&mut file, image::ImageFormat::Png)?;
    Ok(())
}

fn command_serve(global: GlobalOpts, args: ServeCmdArgs) -> Result<()> {
    let ServeCmdArgs {
        host,
        port,
        upload,
        render,
        strava_webhook,
        cors,
    } = args;
    let db = global.database()?;

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

    web::run_blocking(addr, db, config)
}

fn command_strava_auth(global: GlobalOpts, args: StravaAuthCmdArgs) -> Result<()> {
    let StravaAuthCmdArgs { host, port } = args;
    let db = global.database()?;
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
    web::run_blocking(addr, db, config)
}

fn command_mask(global: GlobalOpts, args: MaskCmdArgs) -> Result<()> {
    let mut db = global.database()?;

    match args.action {
        Some(MaskAction::Add {
            name,
            lnglat,
            radius,
        }) => {
            db.add_activity_mask(db::ActivityMask {
                name: name.clone(),
                lat: lnglat.0.y(),
                lng: lnglat.0.x(),
                radius,
            })?;
            println!(
                "Added masked area '{}' at {:.5},{:.5} (radius: {}m)",
                name,
                lnglat.0.x(),
                lnglat.0.y(),
                radius
            );
        }
        Some(MaskAction::Remove { name }) => {
            db.remove_mask(&name)?;
            println!("Removed masked area '{}'", name);
        }
        None => {
            if db.config.activity_mask.is_empty() {
                println!("No masked areas added yet");
            }

            for m in db.config.activity_mask.iter() {
                println!(
                    "  {} - {:.5},{:.5} (radius: {}m)",
                    m.name, m.lat, m.lng, m.radius
                );
            }
        }
    }

    Ok(())
}
