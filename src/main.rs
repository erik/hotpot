use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::time::Instant;

use fitparser;
use fitparser::de::{from_reader_with_options, DecodeOption};
use fitparser::profile::MesgNum;
use flate2::read::GzDecoder;
use geo_types::{Coord, LineString, MultiLineString, Point};
use rayon::prelude::*;
use rusqlite;
use rusqlite::params;
use walkdir::WalkDir;

use crate::tiles::{BBox, LngLat, Tile, WebMercator};

mod tiles;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        println!("Usage: {} <db_path> <import_path>", args[0]);
        return;
    }

    let db_path = &args[1];
    let import_path = &args[2];

    let mut conn = rusqlite::Connection::open(db_path).unwrap();
    let _ = init_db(&mut conn).expect("init db");

    ingest_dir(Path::new(import_path), &mut conn);
}

fn init_db(conn: &mut rusqlite::Connection) -> rusqlite::Result<HashMap<String, String>> {
    const MIGRATIONS: &[&str] = &[
        "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL );",
        "CREATE TABLE activities (
            id INTEGER PRIMARY KEY,
            name TEXT

            -- TODO:
            -- kind TEXT,
            -- start_timestamp INTEGER NOT NULL,
            -- end_timestamp INTEGER NOT NULL,
            -- distance REAL NOT NULL,
        );

        CREATE TABLE activity_tiles (
            ID INTEGER PRIMARY KEY,
            activity_id INTEGER NOT NULL,
            tile_z INTEGER NOT NULL,
            tile_x INTEGER NOT NULL,
            tile_y INTEGER NOT NULL,

            mercator_coords BLOB NOT NULL
        );

        CREATE INDEX activity_tiles_activity_id ON activity_tiles (activity_id);
        ",
    ];

    // Run initial migration (idempotent)
    let tx = conn.transaction()?;
    tx.execute_batch(MIGRATIONS[0])?;
    tx.commit()?;

    let cur_migration = conn
        .query_row("SELECT value FROM meta WHERE key = 'version'", [], |row| {
            let s: String = row.get_unwrap(0);
            Ok(s.parse::<usize>().unwrap_or(0))
        })
        .unwrap_or(0);

    println!("Current migration: {}", cur_migration);

    if cur_migration < MIGRATIONS.len() {
        println!("Migrating database...");

        let tx = conn.transaction()?;
        for m in &MIGRATIONS[cur_migration..] {
            println!("Running migration: {}", m);
            tx.execute_batch(m)?;
        }
        tx.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('version', ?)",
            [cur_migration + 1],
        )?;
        tx.commit()?;
    }

    // Get all of the metadata key/values into a HashMap
    let mut meta: HashMap<String, String> = HashMap::new();
    let mut stmt = conn.prepare("SELECT key, value FROM meta")?;
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        meta.insert(row.get_unwrap(0), row.get_unwrap(1));
    }

    Ok(meta)
}

fn ingest_dir(p: &Path, conn: &mut rusqlite::Connection) {
    let walk = WalkDir::new(p);

    let (tx, rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        walk.into_iter()
            .par_bridge()
            // TODO: why is this result?
            .map(|e| e.expect("todo"))
            .for_each_with(tx, |tx, entry| {
                let path = entry.path();

                let start = Instant::now();
                let lines = match get_extensions(path) {
                    Some(("gpx", compressed)) => {
                        let mut reader = open_reader(path, compressed);
                        parse_gpx(&mut reader)
                    }

                    Some(("fit", compressed)) => {
                        let mut reader = open_reader(path, compressed);
                        parse_fit(&mut reader)
                    }

                    _ => return,
                };

                if let Some(lines) = lines {
                    let parse = start.elapsed();
                    let tile_zoom = 14;
                    let mut clipper = TileClipper::new(tile_zoom);

                    for line in lines {
                        // TODO: should perform a simplification step first.
                        let points = line
                            .points()
                            .map(|pt| LngLat::new(pt.x() as f32, pt.y() as f32))
                            .filter_map(|pt| pt.xy());

                        let mut prev = None;
                        for next in points {
                            if let Some(prev) = prev {
                                clipper.add_line_segment(prev, next);
                            }
                            prev = Some(next);
                        }

                        clipper.finish_line();
                    }

                    tx.send((path.to_owned(), clipper)).unwrap();

                    let intersect = start.elapsed() - parse;
                    println!(
                        "  --> READ:\t{:?}\tINTERSECT:{:?}\t{:?}",
                        parse, intersect, path
                    );
                }
            });
    });

    let mut insert_activity = conn
        .prepare("INSERT INTO activities (name) VALUES (?)")
        .expect("blah");
    let mut insert_coords = conn.prepare("INSERT INTO activity_tiles (activity_id, tile_z, tile_x, tile_y, mercator_coords) VALUES (?, ?, ?, ?, ?)").expect("blah");
    for (path, clipper) in rx {
        let row = insert_activity
            .insert([path.to_str().unwrap()])
            .expect("insert activity");

        for (tile, lines) in clipper.tiles {
            let bbox = tile.xy_bounds();

            for line in lines {
                let pixels: Vec<_> = line
                    .iter()
                    .map(|pt| bbox.project(&pt, 4096.0))
                    .map(|Coord { x, y }| format!("{},{}", x, y))
                    .collect();

                insert_coords
                    .insert(params![row, tile.z, tile.x, tile.y, pixels.join(";")])
                    .expect("insert coords");
            }
        }
        println!("Inserted: {:?}", path);
    }
}

struct TileClipper {
    zoom: u8,
    tiles: HashMap<Tile, Vec<Vec<WebMercator>>>,
    tile: Tile,
    bbox: Option<BBox>,
    line: Vec<WebMercator>,
}

impl TileClipper {
    fn new(zoom: u8) -> Self {
        Self {
            tiles: HashMap::new(),
            tile: Tile::new(0, 0, zoom),
            bbox: None,
            line: vec![],
            zoom,
        }
    }

    fn add_line_segment(&mut self, start: WebMercator, end: WebMercator) {
        let bbox = self.bbox.unwrap_or_else(|| {
            self.move_bbox(&start);
            self.bbox.unwrap()
        });

        match bbox.clip_line(&start, &end) {
            None => {
                // line segment is completely outside of the current tile
                self.finish_line();
                self.move_bbox(&start);
                // todo: should add line segment?? it overflows though?
            }

            Some((a, b)) => {
                if self.line.is_empty() {
                    self.line.push(a);
                }

                self.line.push(b);

                // We've moved, update the bbox.
                if b != end {
                    self.finish_line();
                    // TODO: should use tile iterator here...
                    self.move_bbox(&end);

                    if Some(bbox) != self.bbox {
                        self.add_line_segment(b, end);
                    }
                }
            }
        }
    }

    fn move_bbox(&mut self, pt: &WebMercator) {
        let tile = pt.tile(self.zoom);
        self.bbox = Some(tile.xy_bounds());
        self.tile = tile;
    }

    fn finish_line(&mut self) {
        if self.line.len() < 2 {
            self.line.clear();
            return;
        }

        let line = self.line.clone();
        self.line.clear();

        self.tiles
            .entry(self.tile)
            .or_insert_with(Vec::new)
            .push(line);
    }
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

fn parse_fit<R: Read>(r: &mut R) -> Option<MultiLineString> {
    const SCALE_FACTOR: f64 = ((1u64 << 32) / 360) as f64;

    let opts = [
        DecodeOption::SkipDataCrcValidation,
        DecodeOption::SkipHeaderCrcValidation,
    ]
    .into();

    let mut points = vec![];
    for data in from_reader_with_options(r, &opts).unwrap() {
        match data.kind() {
            MesgNum::Record => {
                let mut lat: Option<i64> = None;
                let mut lng: Option<i64> = None;

                for f in data.fields() {
                    match f.name() {
                        "position_lat" => lat = f.value().try_into().ok(),
                        "position_long" => lng = f.value().try_into().ok(),
                        _ => {}
                    }
                }

                if let (Some(lat), Some(lng)) = (lat, lng) {
                    let pt = Point::new(lng as f64, lat as f64) / SCALE_FACTOR;
                    points.push(pt);
                }
            }
            _ => {}
        }
    }

    if points.is_empty() {
        return None;
    }

    let line = points.into_iter().collect::<LineString>();
    Some(MultiLineString::from(line))
}

fn parse_gpx<R: Read>(reader: &mut R) -> Option<MultiLineString> {
    let gpx = gpx::read(reader).ok()?;
    // Just take the first track.
    gpx.tracks.first().map(|t| t.multilinestring())
}
