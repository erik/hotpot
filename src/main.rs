use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::ops::Range;
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fitparser::de::{DecodeOption, from_reader_with_options};
use fitparser::profile::MesgNum;
use flate2::read::GzDecoder;
use geo_types::{Coord, LineString, MultiLineString, Point};
use rayon::prelude::*;
use rusqlite::params;
use walkdir::WalkDir;

use crate::tiles::{BBox, haversine_dist, LngLat, Tile, WebMercator};

mod tiles;

// TODO: consider piping this through a compression step.
fn encode_raw(data: &[Coord<u16>]) -> anyhow::Result<Vec<u8>> {
    let mut w = Vec::with_capacity(data.len() * 2);
    for pt in data {
        w.write_u16::<LittleEndian>(pt.x)?;
        w.write_u16::<LittleEndian>(pt.y)?;
    }
    Ok(w)
}

fn decode_raw(bytes: &[u8]) -> anyhow::Result<Vec<Coord<u32>>> {
    let mut coords = Vec::with_capacity(bytes.len() / 4);
    let mut reader = Cursor::new(bytes);
    while reader.position() < bytes.len() as u64 {
        let x = reader.read_u16::<LittleEndian>()?;
        let y = reader.read_u16::<LittleEndian>()?;
        coords.push(Coord { x: x as u32, y: y as u32 });
    }

    Ok(coords)
}

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

    let tiles = [
        Tile::new(0, 0, 1),
        Tile::new(43, 102, 8),
        Tile::new(2802, 6542, 14),
        Tile::new(2801, 6542, 14),
        Tile::new(2800, 6542, 14),
    ];
    for t in tiles {
        let pixels = render_tile(t, &mut conn).expect("render tile");
        let out = format!("{}_{}_{}.pgm", t.z, t.x, t.y);
        render_pgm(&pixels, 1024, Path::new(&out)).expect("render pgm");
        println!("Rendered {}", out);
    }
}

fn render_pgm(data: &[u8], width: usize, out: &Path) -> anyhow::Result<()> {
    let mut file = File::create(out)?;
    // Grayscale, binary
    file.write_all(b"P5\n")?;
    file.write_all(format!("{} {} {}\n", width, data.len() / width, 255).as_bytes())?;

    for row in data.chunks(width) {
        for pixel in row {
            let scaled_pixel = 255.0 - 255.0 * (*pixel as f32 / 255.0).powf(1.0 / 9.2);
            file.write_u8(scaled_pixel as u8)?;
        }
    }
    Ok(())
}

// TODO: handle case where tile zoom is lower than stored zoom
// FIXME: riddled with magic numbers
fn render_tile(
    tile: Tile,
    conn: &mut rusqlite::Connection,
) -> rusqlite::Result<Vec<u8>> {
    let mut select_stmt = conn
        .prepare("SELECT coords FROM activity_tiles WHERE z = ? AND x = ? AND y = ?;")?;

    const TILE_WIDTH: usize = 1024;
    const TILE_SIZE: usize = TILE_WIDTH * TILE_WIDTH;
    const BOUNDS: Range<isize> = 0..TILE_WIDTH as isize;
    let mut pixels: Vec<u8> = vec![0; TILE_SIZE];

    let mut rows = select_stmt.query(params![tile.z, tile.x, tile.y])?;
    while let Some(row) = rows.next()? {
        let bytes: Vec<u8> = row.get_unwrap(0);
        let line = decode_raw(&bytes).expect("decode raw");
        let mut prev = None;
        for Coord { x, y } in line {
            if x >= 4096 || y >= 4096 {
                continue;
            }
            // TODO: real scaling.
            let x = (x >> 2) as usize;
            let y = 0x3ff - (y >> 2) as usize;

            if let Some(Coord { x: px, y: py }) = prev {
                // TODO: is the perf hit of this worth it?
                let line_iter = line_drawing::XiaolinWu::<f32, isize>::new(
                    (px as f32, py as f32),
                    (x as f32, y as f32),
                );

                for ((ix, iy), _) in line_iter {
                    if !BOUNDS.contains(&ix) || !BOUNDS.contains(&iy) {
                        continue;
                    }

                    let idx = iy as usize * TILE_WIDTH + ix as usize;
                    pixels[idx] = pixels[idx].saturating_add(1);
                }
            }
            prev = Some(Coord { x, y });
        }
    }

    Ok(pixels)
}


fn init_db(conn: &mut rusqlite::Connection) -> rusqlite::Result<HashMap<String, String>> {
    const MIGRATIONS: &[&str] = &[
        "
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);", "
CREATE TABLE activities (
      id            INTEGER PRIMARY KEY
    -- TODO: maybe do a hash of contents?
    , file          TEXT NOT NULL
    , title         TEXT
    , start_time    INTEGER
    , duration_secs INTEGER
    , dist_meters   REAL NOT NULL

    -- TODO:
    -- , kind     TEXT -- run, bike, etc
    -- , polyline TEXT
);

CREATE TABLE activity_tiles (
      id          INTEGER PRIMARY KEY
    , activity_id INTEGER NOT NULL
    , z           INTEGER NOT NULL
    , x           INTEGER NOT NULL
    , y           INTEGER NOT NULL
    , coords      BLOB NOT NULL
);

CREATE INDEX activity_tiles_activity_id ON activity_tiles (activity_id);
CREATE INDEX activity_tiles_zxy ON activity_tiles (z, x, y);
        "
    ];

    let metadata = load_metadata(conn);
    let cur_migration = metadata.get("version")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // If we're up to date, return.
    if cur_migration == MIGRATIONS.len() {
        return Ok(metadata);
    }

    println!("Migrating database (have {} to apply)...", MIGRATIONS.len() - cur_migration);

    let tx = conn.transaction()?;
    for m in &MIGRATIONS[cur_migration..] {
        println!("Running migration: {}", m);
        tx.execute_batch(m)?;
    }
    tx.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('version', ?)",
        [MIGRATIONS.len()],
    )?;
    tx.commit()?;

    // Reload metadata after applying migrations.
    Ok(load_metadata(conn))
}

fn load_metadata(conn: &mut rusqlite::Connection) -> HashMap<String, String> {
    let mut meta: HashMap<String, String> = HashMap::new();

    // Would fail on first run before migrations are applied.
    let _ = conn.query_row(
        "SELECT key, value FROM meta",
        [],
        |row| {
            let (k, v) = (row.get_unwrap(0), row.get_unwrap(1));
            meta.insert(k, v);
            Ok(())
        },
    );

    meta
}

fn ingest_dir(p: &Path, conn: &mut rusqlite::Connection) {
    // Skip any files that are already in the database.
    // TODO: avoid the collect call here?
    let known_files: HashSet<String> = conn
        .prepare("SELECT file FROM activities").unwrap()
        .query_map([], |row| row.get(0)).unwrap()
        .filter_map(|n| n.ok())
        .collect();


    let (tx, rx) = std::sync::mpsc::channel();

    let walk = WalkDir::new(p);
    std::thread::spawn(move || {
        walk
            .into_iter()
            .par_bridge()
            .filter_map(|dir| {
                let dir = dir.expect("walkdir error");
                let path = dir.path().to_str().expect("non utf8 path");

                if !known_files.contains(path) {
                    Some(dir)
                } else {
                    None
                }
            })
            .for_each_with(tx, |tx, entry| {
                let path = entry.path();

                if let Some(activity) = parse_file(path) {
                    // TODO: should be configurable.
                    const STORED_LEVELS: &[u8] = &[1, 8, 14];
                    let clippers = activity.clip_to_tiles(STORED_LEVELS);
                    tx.send((path.to_owned(), activity, clippers))
                        .expect("send");
                }
            });
    });

    let mut insert_activity = conn
        .prepare("INSERT INTO activities (file, title, start_time, duration_secs, dist_meters) VALUES (?, ?, ?, ?, ?)")
        .unwrap();

    let mut insert_coords = conn
        .prepare("INSERT INTO activity_tiles (activity_id, z, x, y, coords) VALUES (?, ?, ?, ?, ?)")
        .unwrap();

    for (path, activity, clippers) in rx {
        let activity_id = insert_activity
            .insert(params![path.to_str().unwrap(), activity.title, activity.start_time, activity.duration_secs, activity.distance()])
            .expect("insert activity");

        for clipper in clippers {
            for (tile, lines) in &clipper.tiles {
                // TODO: encode multiline strings together in same blob.
                for pixels in lines {
                    if pixels.0.is_empty() {
                        continue;
                    }

                    let encoded = encode_raw(
                        pixels.0.as_slice()
                    ).expect("encode raw");

                    insert_coords
                        .insert(params![activity_id, tile.z, tile.x, tile.y, encoded])
                        .expect("insert coords");
                }
            }
        }

        println!("Imported {}", path.to_str().unwrap());
    }
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
        let multiline = self.tiles
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
                // todo: should we add new segment after shifting bbox?
                self.finish_segment();
                self.current = Some(self.bounding_tile(&end));
            }

            // [start, end] is at least partially contained within the current tile.
            Some((a, b)) => {
                let line = self.last_line(&tile);
                if line.0.is_empty() {
                    // TODO: extract magic num
                    line.0.push(a.to_pixel(&bbox, 4096).into());
                }

                line.0.push(b.to_pixel(&bbox, 4096).into());

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
            self.tiles
                .entry(tile)
                .and_modify(|lines| {
                    // TODO: it's broken.
                    // if let Some(line) = lines.0.last_mut() {
                    //     line.0 = simplify(&line.0, 128.0);
                    // }

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

    let dist = (dx * (sy - py)) - (dy * (sx - px)).abs();
    dist  / (dx * dx + dy * dy).sqrt()
}

// Ramer–Douglas–Peucker algorithm
fn simplify(line: &[Coord<u16>], epsilon: f32) -> Vec<Coord<u16>> {
    let mut stack = vec![(0, line.len() - 1)];
    let mut result = vec![];

    while let Some((start, end)) = stack.pop() {
        let mut max_dist = 0.0;
        let mut max_index = start;

        let start_pt = line[start];
        let end_pt = line[end];

        for i in start + 1..end {
            let dist = point_to_line_dist(&line[i], &start_pt, &end_pt);
            if dist > max_dist {
                max_dist = dist;
                max_index = i;
            }
        }

        if max_dist > epsilon {
            stack.push((start, max_index));
            stack.push((max_index, end));
        } else {
            result.push(start_pt);
            result.push(end_pt);
        }
    }

    result
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
        self.tracks.iter().map(|line| {
            let mut prev = None;
            let mut sum = 0.0;
            for next in line.points() {
                if let Some(prev) = prev {
                    sum += haversine_dist(&prev, &next);
                }

                prev = Some(next);
            }
            sum
        })
            .sum()
    }

    fn clip_to_tiles(&self, zooms: &[u8]) -> Vec<TileClipper> {
        let mut clippers: Vec<_> = zooms.iter().map(|zoom| TileClipper::new(*zoom)).collect();

        for line in self.tracks.iter() {
            let points = line
                .points()
                .map(LngLat::from)
                .filter_map(|pt| pt.xy());

            let mut prev = None;
            for next in points {
                // TODO: should try to filter based on distance to previous point.
                if let Some(prev) = prev {
                    for clip in clippers.iter_mut() {
                        clip.add_line_segment(prev, next);
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
fn parse_file(p: &Path) -> Option<RawActivity> {
    match get_extensions(p) {
        Some(("gpx", compressed)) => {
            let mut reader = open_reader(p, compressed);
            parse_gpx(&mut reader)
        }

        Some(("fit", compressed)) => {
            let mut reader = open_reader(p, compressed);
            parse_fit(&mut reader)
        }

        _ => None
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
        match data.kind() {
            MesgNum::Record => {
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
            _ => {}
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

    Some(
        RawActivity {
            title: track.name.clone(),
            tracks: track.multilinestring(),
            // TODO: parse these out. Library supports, just need to type dance.
            start_time: None,
            duration_secs: None,
        }
    )
}
