use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use clap::{Args, Parser, Subcommand};
use fitparser::de::{from_reader_with_options, DecodeOption};
use fitparser::profile::MesgNum;
use flate2::read::GzDecoder;
use geo::{EuclideanDistance, HaversineLength};
use geo_types::{Coord, LineString, MultiLineString, Point};
use rayon::prelude::*;
use rusqlite::{params, ToSql};
use time::OffsetDateTime;
use walkdir::WalkDir;

use crate::db::{decode_line, encode_line, Database};
use crate::raster::{TileRaster, DEFAULT_GRADIENT};
use crate::tile::{BBox, LngLat, Tile, TileBounds, WebMercator};

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

            // TODO: `time` crate is too restrictive, switch to `chrono`
            let parse = |t: String| {
                OffsetDateTime::parse(
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

// TODO: move to db.rs
#[derive(Default)]
pub struct ActivityFilter {
    before: Option<i64>,
    after: Option<i64>,
}

impl ActivityFilter {
    pub fn new(before: Option<OffsetDateTime>, after: Option<OffsetDateTime>) -> Self {
        Self {
            before: before.map(OffsetDateTime::unix_timestamp),
            after: after.map(OffsetDateTime::unix_timestamp),
        }
    }
    fn to_query<'a>(&'a self, params: &mut Vec<&'a dyn ToSql>) -> String {
        let mut clauses = vec![];

        if let Some(ref before) = self.before {
            clauses.push("start_time < ?");
            params.push(before);
        }

        if let Some(ref after) = self.after {
            clauses.push("start_time > ?");
            params.push(after);
        }

        if clauses.is_empty() {
            return String::from("1 = 1");
        }

        clauses.join(" AND ")
    }
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
            let activity = parse_activity_data(&path)?;
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
                        activity.distance(),
                    ],
                )
                .expect("insert activity");

                let activity_id = conn.last_insert_rowid();

                // TODO: split out into separate function
                for clip in activity.clip_to_tiles(&db.meta.zoom_levels) {
                    for (tile, lines) in &clip.tiles {
                        // TODO: encode multiline strings together in same blob.
                        for pixels in lines {
                            if pixels.0.is_empty() {
                                continue;
                            }

                            // TODO: can consider storing post rasterization for faster renders.
                            let simplified = simplify(&pixels.0, 4.0);
                            let encoded = encode_line(&simplified).expect("encode line");

                            insert_coords
                                .insert(params![activity_id, tile.z, tile.x, tile.y, encoded])
                                .expect("insert coords");
                        }
                    }
                }

                print!(".");
            },
        );

    conn.execute_batch("VACUUM")?;
    Ok(())
}

struct TileClipper {
    zoom: u8,
    current: Option<(Tile, BBox)>,
    tiles: HashMap<Tile, MultiLineString<u16>>,
}

impl TileClipper {
    fn new(zoom: u8) -> Self {
        Self {
            zoom,
            tiles: HashMap::new(),
            current: None,
        }
    }

    fn bounding_tile(&self, pt: &WebMercator) -> (Tile, BBox) {
        let tile = pt.tile(self.zoom);
        let bbox = tile.xy_bounds();
        (tile, bbox)
    }

    fn last_line(&mut self, tile: &Tile) -> &mut LineString<u16> {
        let multiline = self
            .tiles
            .entry(*tile)
            .or_insert_with(|| MultiLineString::new(vec![]));

        if multiline.0.is_empty() {
            multiline.0.push(LineString::new(vec![]));
        }

        // TODO: avoid the unwrap
        multiline.0.last_mut().unwrap()
    }

    fn add_line_segment(&mut self, start: WebMercator, end: WebMercator) {
        let (tile, bbox) = match self.current {
            Some(pair) => pair,
            None => {
                let pair = self.bounding_tile(&start);
                self.current = Some(pair);
                pair
            }
        };

        match bbox.clip_line(&start, &end) {
            // [start, end] doesn't intersect with the current tile at all, reposition it.
            None => {
                self.finish_segment();
                self.current = Some(self.bounding_tile(&start));
                // todo: should we add new segment after shifting bbox?
                // self.add_line_segment(start, end, c+1);
            }

            // [start, end] is at least partially contained within the current tile.
            Some((a, b)) => {
                let line = self.last_line(&tile);
                if line.0.is_empty() {
                    line.0
                        .push(a.to_pixel(&bbox, DEFAULT_TILE_EXTENT as u16).into());
                }

                line.0
                    .push(b.to_pixel(&bbox, DEFAULT_TILE_EXTENT as u16).into());

                // If we've modified the end point, we've left the current tile.
                if b != end {
                    self.finish_segment();

                    // TODO: theoretically could jump large distances here
                    //   (requiring supercover iterator), but unlikely.
                    let (next_tile, next_bbox) = self.bounding_tile(&end);
                    if next_tile != tile {
                        self.current = Some((next_tile, next_bbox));
                        self.add_line_segment(b, end);
                    }
                }
            }
        }
    }

    fn finish_segment(&mut self) {
        if let Some((tile, _)) = self.current {
            self.tiles.entry(tile).and_modify(|lines| {
                lines.0.push(LineString::new(vec![]));
            });
        }
    }
}

// FIXME: casts gone mad! let's stick to a data type.
fn point_to_line_dist(pt: &Coord<u16>, start: &Coord<u16>, end: &Coord<u16>) -> f32 {
    let (sx, sy) = (start.x as f32, start.y as f32);
    let (ex, ey) = (end.x as f32, end.y as f32);
    let (px, py) = (pt.x as f32, pt.y as f32);

    let dx = ex - sx;
    let dy = ey - sy;

    // Line start and ends on same point, so just return euclidean distance to that point.
    if dx == 0.0 && dy == 0.0 {
        return (sx - px).hypot(sy - py);
    }

    let dist = (dx * (sy - py)) - (dy * (sx - px));
    dist.abs() / (dx * dx + dy * dy).sqrt()
}

/// "foo.bar.gz" -> Some("bar", true)
/// "foo.bar" -> Some("bar", false)
/// "foo" -> None
fn get_extensions(p: &Path) -> Option<(&str, bool)> {
    let mut exts = p
        .file_name()
        .and_then(OsStr::to_str)
        .map(|f| f.split('.'))?;

    Some(match exts.next_back()? {
        "gz" => (exts.next_back()?, true),
        ext => (ext, false),
    })
}

fn open_reader(p: &Path, gzip: bool) -> Box<dyn BufRead> {
    let file = File::open(p).expect("open file");

    if gzip {
        Box::new(BufReader::new(GzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    }
}

#[derive(Clone)]
struct RawActivity {
    title: Option<String>,
    start_time: Option<u64>,
    duration_secs: Option<u64>,
    tracks: MultiLineString,
}

impl RawActivity {
    fn distance(&self) -> f64 {
        self.tracks.iter().map(LineString::haversine_length).sum()
    }

    fn clip_to_tiles(&self, zooms: &[u8]) -> Vec<TileClipper> {
        let mut clippers: Vec<_> = zooms.iter().map(|zoom| TileClipper::new(*zoom)).collect();

        for line in self.tracks.iter() {
            let points = line.points().map(LngLat::from).filter_map(|pt| pt.xy());

            let mut prev: Option<WebMercator> = None;
            for next in points {
                if let Some(prev) = prev {
                    // TODO: hacky?
                    // Skip over large jumps
                    if prev.0.euclidean_distance(&next.0) <= 5000.0 {
                        for clip in clippers.iter_mut() {
                            clip.add_line_segment(prev, next);
                        }
                    } else {
                        println!("Skipping: {:?} -> {:?}", prev, next);
                    }
                }
                prev = Some(next);
            }

            for clip in clippers.iter_mut() {
                clip.finish_segment();
            }
        }

        clippers
    }
}

// TODO: should return a Result
fn parse_activity_data(p: &Path) -> Option<RawActivity> {
    match get_extensions(p) {
        Some(("gpx", compressed)) => {
            let mut reader = open_reader(p, compressed);
            parse_gpx(&mut reader)
        }

        Some(("fit", compressed)) => {
            let mut reader = open_reader(p, compressed);
            parse_fit(&mut reader)
        }

        Some(("tcx", _compressed)) => {
            // TODO: parse tcx
            None
        }

        _ => None,
    }
}

fn parse_fit<R: Read>(r: &mut R) -> Option<RawActivity> {
    const SCALE_FACTOR: f64 = (1u64 << 32) as f64 / 360.0;

    let opts = [
        DecodeOption::SkipDataCrcValidation,
        DecodeOption::SkipHeaderCrcValidation,
    ]
    .into();

    let (mut start_time, mut duration_secs) = (None, None);
    let mut points = vec![];
    for data in from_reader_with_options(r, &opts).unwrap() {
        if data.kind() == MesgNum::Record {
            let mut lat: Option<i64> = None;
            let mut lng: Option<i64> = None;

            for f in data.fields() {
                match f.name() {
                    "position_lat" => lat = f.value().try_into().ok(),
                    "position_long" => lng = f.value().try_into().ok(),
                    "timestamp" => {
                        let ts: i64 = f.value().try_into().unwrap();

                        match start_time {
                            None => start_time = Some(ts),
                            Some(t) => duration_secs = Some((ts - t) as u64),
                        }
                    }
                    _ => {}
                }
            }

            if let (Some(lat), Some(lng)) = (lat, lng) {
                let pt = Point::new(lng as f64, lat as f64) / SCALE_FACTOR;
                points.push(pt);
            }
        }
    }

    if points.is_empty() {
        return None;
    }

    let line = points.into_iter().collect::<LineString>();
    Some(RawActivity {
        duration_secs,
        title: None,
        start_time: start_time.map(|it| it as u64),
        tracks: MultiLineString::from(line),
    })
}

fn parse_gpx<R: Read>(reader: &mut R) -> Option<RawActivity> {
    let gpx = gpx::read(reader).ok()?;

    // Just take the first track (generally the only one).
    let track = gpx.tracks.first()?;

    let start_time = gpx
        .metadata
        .and_then(|m| m.time)
        .map(|t| OffsetDateTime::from(t).unix_timestamp() as u64);

    // Grab the timestamp from the last point to calculate duration
    let end_time = track
        .segments
        .last()
        .and_then(|seg| seg.points.last())
        .and_then(|wpt| wpt.time)
        .map(|t| OffsetDateTime::from(t).unix_timestamp() as u64);

    let duration_secs = start_time
        .zip(end_time)
        .filter(|(start, end)| end > start)
        .map(|(start, end)| end - start);

    Some(RawActivity {
        start_time,
        duration_secs,
        title: track.name.clone(),
        tracks: track.multilinestring(),
    })
}

// Ramer–Douglas–Peucker algorithm
fn simplify(line: &[Coord<u16>], epsilon: f32) -> Vec<Coord<u16>> {
    if line.len() < 3 {
        return line.to_vec();
    }

    fn simplify_inner(line: &[Coord<u16>], epsilon: f32, buffer: &mut Vec<Coord<u16>>) {
        if let [start, rest @ .., end] = line {
            let mut max_dist = 0.0;
            let mut max_idx = 0;

            for (idx, pt) in rest.iter().enumerate() {
                let dist = point_to_line_dist(pt, start, end);
                if dist > max_dist {
                    max_dist = dist;
                    max_idx = idx + 1;
                }
            }

            if max_dist > epsilon {
                simplify_inner(&line[..=max_idx], epsilon, buffer);
                buffer.push(line[max_idx]);
                simplify_inner(&line[max_idx..], epsilon, buffer);
            }
        }
    }

    let mut buf = vec![line[0]];
    simplify_inner(line, epsilon, &mut buf);
    buf.push(line[line.len() - 1]);

    buf
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_simplify() {
        let line = vec![
            Coord { x: 0, y: 0 },
            Coord { x: 1, y: 1 },
            Coord { x: 2, y: 2 },
            Coord { x: 3, y: 3 },
            Coord { x: 4, y: 4 },
            Coord { x: 5, y: 5 },
            Coord { x: 6, y: 6 },
            Coord { x: 7, y: 7 },
            Coord { x: 8, y: 8 },
            Coord { x: 9, y: 9 },
        ];

        let simplified = simplify(&line, 0.5);
        assert_eq!(simplified.len(), 2);
        assert_eq!(simplified[0], Coord { x: 0, y: 0 });
        assert_eq!(simplified[1], Coord { x: 9, y: 9 });
    }

    #[test]
    fn test_simplify_retains_points() {
        let line = vec![
            Coord { x: 0, y: 0 },
            Coord { x: 5, y: 5 },
            Coord { x: 0, y: 0 },
            Coord { x: 1, y: 1 },
            Coord { x: 0, y: 0 },
        ];

        let simplified = simplify(&line, 2.0);
        assert_eq!(simplified.len(), 3);
        assert_eq!(simplified[0], Coord { x: 0, y: 0 });
        assert_eq!(simplified[1], Coord { x: 5, y: 5 });
        assert_eq!(simplified[2], Coord { x: 0, y: 0 });
    }

    #[test]
    fn test_point_to_line_dist() {
        let start = Coord { x: 0, y: 0 };
        let end = Coord { x: 10, y: 10 };

        assert_eq!(point_to_line_dist(&Coord { x: 5, y: 5 }, &start, &end), 0.0);
        assert_eq!(
            point_to_line_dist(&Coord { x: 5, y: 0 }, &start, &end),
            (5.0 * 2.0_f32.sqrt()) / 2.0
        );
        assert_eq!(
            point_to_line_dist(&Coord { x: 0, y: 5 }, &start, &end),
            (5.0 * 2.0_f32.sqrt()) / 2.0
        );
        assert_eq!(
            point_to_line_dist(&Coord { x: 0, y: 10 }, &start, &end),
            (10.0 * 2.0_f32.sqrt()) / 2.0
        );
        assert_eq!(
            point_to_line_dist(&Coord { x: 10, y: 0 }, &start, &end),
            (10.0 * 2.0_f32.sqrt()) / 2.0
        );
    }

    #[test]
    fn test_point_to_line_same_point() {
        let start = Coord { x: 0, y: 0 };
        let end = Coord { x: 0, y: 0 };

        assert_eq!(point_to_line_dist(&Coord { x: 0, y: 0 }, &start, &end), 0.0);
        assert_eq!(
            point_to_line_dist(&Coord { x: 1, y: 1 }, &start, &end),
            (2_f32).sqrt()
        );
    }
}
