use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::path::Path;
use std::path::PathBuf;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use clap::Parser;
use fitparser::de::{DecodeOption, from_reader_with_options};
use fitparser::profile::MesgNum;
use flate2::read::GzDecoder;
use geo::HaversineLength;
use geo_types::{Coord, LineString, MultiLineString, Point};
use image::Rgba;
use r2d2_sqlite::SqliteConnectionManager;
use rayon::prelude::*;
use rusqlite::params;
use walkdir::WalkDir;

use crate::tiles::{BBox, LngLat, Tile, TileBounds, WebMercator};

mod tiles;

// TODO: make this configurable
const STORED_ZOOM_LEVELS: [u8; 4] = [2, 6, 10, 14];
const STORED_TILE_WIDTH: u32 = 4096;


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

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    db_path: PathBuf,
    import_path: PathBuf,

    /// Reset the database before importing
    #[arg(short, long, default_value = "false")]
    reset: bool,
}


fn main() {
    let cli = Cli::parse();

    // TODO: move this out of here.
    if cli.reset {
        let db_files = &[
            &cli.db_path,
            &cli.db_path.join("-wal"),
            &cli.db_path.join("-shm"),
        ];

        for p in db_files {
            match std::fs::remove_file(p) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => panic!("error removing db: {}", e),
            }
        }
    }

    let db_pool = connect_database(&cli.db_path);

    // TODO: remove this
    if cli.reset {
        ingest_dir(&cli.import_path, &db_pool);
    }

    let base = Tile::new(2800, 6542, 14);
    let tiles = (0..=14).rev()
        .map(|z| Tile::new(base.x >> (14 - z), base.y >> (14 - z), z))
        .collect::<Vec<_>>();
    let mut conn = db_pool.get().expect("db conn");
    let render_width = 2048;
    for t in tiles {
        let pixels = render_tile(t, &mut conn, render_width).expect("render tile");
        let out = format!("{}_{}_{}.png", t.z, t.x, t.y);
        render_image(&pixels, render_width, Path::new(&out)).expect("render pgm");
        println!("Rendered {}", out);
    }
}

fn connect_database(path: &Path) -> r2d2::Pool<SqliteConnectionManager> {
    let manager = SqliteConnectionManager::file(path);
    let pool = r2d2::Pool::new(manager).expect("db pool");

    // TODO: should return metadata or something.
    pool.get().and_then(|mut conn| {
        let _metadata = init_db(&mut conn).expect("init db");

        //  TODO: test performance
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = OFF")
            .expect("pragma");
        Ok(())
    }).expect("init db");

    pool
}

struct LinearGradient([Rgba<u8>; 256]);

impl LinearGradient {
    // TODO: clean this up
    fn from_stops(stops: &[(f32, Rgba<u8>)]) -> Self {
        let mut buf = [Rgba::from([0, 0, 0, 0]); 256];
        let mut i = 1;

        for stop in stops.windows(2) {
            let (start, end) = (stop[0], stop[1]);
            let start_idx = (start.0 * 256.0).floor() as usize;
            let end_idx = (end.0 * 256.0).ceil() as usize;

            while i < end_idx {
                let t = (i - start_idx) as f32 / (end_idx - start_idx) as f32;
                buf[i] = image::Rgba::from([
                    (start.1[0] as f32 * (1.0 - t) + end.1[0] as f32 * t) as u8,
                    (start.1[1] as f32 * (1.0 - t) + end.1[1] as f32 * t) as u8,
                    (start.1[2] as f32 * (1.0 - t) + end.1[2] as f32 * t) as u8,
                    0xff,
                ]);

                i += 1;
            }
        }

        let (_, last) = stops.last().unwrap();
        while i < 256 {
            buf[i] = *last;
            i += 1;
        }

        LinearGradient(buf)
    }

    fn sample(&self, val: u8) -> Rgba<u8> {
        self.0[val as usize]
    }
}


fn render_image(data: &[u8], width: u32, out: &Path) -> anyhow::Result<()> {
    let mut img = image::RgbaImage::new(width, width);

    // TODO: should be configurable
    let gradient = LinearGradient::from_stops(&[
        (0.0, Rgba::from([0xff, 0xb1, 0xff, 0x7f])),
        (0.05, Rgba::from([0xff, 0xb1, 0xff, 0xff])),
        (0.25, Rgba::from([0xff, 0xff, 0xff, 0xff])),
    ]);
    for x in 0..width {
        for y in 0..width {
            let pixel = data[(y * width + x) as usize];
            let color = gradient.sample(pixel);
            img.put_pixel(x, y, color);
        }
    }

    img.write_to(
        &mut File::create(out)?,
        image::ImageOutputFormat::Png,
    )?;

    Ok(())
}

fn stored_tile_bounds(tile: &Tile) -> Option<TileBounds> {
    // Find the stored zoom level closest to (and higher than) the requested zoom.
    let zoom = STORED_ZOOM_LEVELS
        .iter()
        .find(|&&z| tile.z <= z)?;

    let zoom_steps = zoom - tile.z;

    Some(
        TileBounds {
            z: *zoom,
            xmin: tile.x << zoom_steps,
            ymin: tile.y << zoom_steps,
            xmax: (tile.x + 1) << zoom_steps,
            ymax: (tile.y + 1) << zoom_steps,
        }
    )
}

fn render_tile(
    tile: Tile,
    conn: &mut rusqlite::Connection,
    width: u32,
) -> rusqlite::Result<Vec<u8>> {
    // TODO: support upscaling
    assert!(width <= STORED_TILE_WIDTH, "Upscaling not supported");
    assert!(width.is_power_of_two(), "width must be power of two");

    let bounds = stored_tile_bounds(&tile).unwrap();
    let zoom_steps = (bounds.z - tile.z) as u32;
    let width_steps = STORED_TILE_WIDTH.ilog2() - width.ilog2();

    let mut pixels: Vec<u8> = vec![0; (width * width) as usize];

    let mut select_stmt = conn
        .prepare("
SELECT x, y, coords
FROM activity_tiles
WHERE z = ?
  AND (x >= ? AND x < ?)
  AND (y >= ? AND y < ?);
        ")?;

    let mut rows = select_stmt
        .query(params![bounds.z, bounds.xmin, bounds.xmax, bounds.ymin, bounds.ymax])?;

    while let Some(row) = rows.next()? {
        let (source_tile_x, source_tile_y): (u32, u32) = (
            row.get_unwrap(0),
            row.get_unwrap(1),
        );

        // Origin of source tile within target tile
        let x_offset = STORED_TILE_WIDTH * (source_tile_x - bounds.xmin);
        let y_offset = STORED_TILE_WIDTH * (source_tile_y - bounds.ymin);

        let bytes: Vec<u8> = row.get_unwrap(2);
        let line = decode_raw(&bytes).expect("decode raw");

        let mut prev = None;
        for Coord { x, y } in line {
            // Translate (x,y) to location in target tile.
            // [0..(width * STORED_TILE_WIDTH)]
            let x = x + x_offset;
            let y = (STORED_TILE_WIDTH - y) + y_offset;

            // Scale the coordinates back down to [0..width]
            let x = x >> (zoom_steps + width_steps);
            let y = y >> (zoom_steps + width_steps);

            if x >= width || y >= width {
                continue;
            }

            if let Some(Coord { x: px, y: py }) = prev {
                if x == px && y == py {
                    continue;
                }

                // TODO: is the perf hit of this worth it?
                let line_iter = line_drawing::Bresenham::<i32>::new(
                    (px as i32, py as i32),
                    (x as i32, y as i32),
                );

                for (ix, iy) in line_iter {
                    if ix < 0 || iy < 0 || ix >= (width as i32) || iy >= (width as i32) {
                        continue;
                    }

                    let (ix, iy) = (ix as u32, iy as u32);
                    let idx = (iy * width + ix) as usize;
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

fn ingest_dir(p: &Path, pool: &r2d2::Pool<SqliteConnectionManager>) {
    // Skip any files that are already in the database.
    // TODO: avoid the collect call here?
    let known_files: HashSet<String> = pool.get()
        .unwrap()
        .prepare("SELECT file FROM activities").unwrap()
        .query_map([], |row| row.get(0)).unwrap()
        .filter_map(|n| n.ok())
        .collect();

    WalkDir::new(p)
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
        .filter_map(|entry| parse_activity_data(entry.path()))
        .for_each_init(
            || pool.clone(),
            |pool, activity| {
                let conn = pool.get().expect("db conn");

                let mut insert_coords = conn
                    .prepare("INSERT INTO activity_tiles (activity_id, z, x, y, coords) VALUES (?, ?, ?, ?, ?)")
                    .unwrap();

                conn.execute(
                    "INSERT INTO activities (file, title, start_time, duration_secs, dist_meters) VALUES (?, ?, ?, ?, ?)",
                    params!["todo", activity.title, activity.start_time, activity.duration_secs, activity.distance()],
                ).expect("insert activity");

                let activity_id = conn.last_insert_rowid();

                // TODO: split out into separate function
                for clip in activity.clip_to_tiles(&STORED_ZOOM_LEVELS) {
                    for (tile, lines) in &clip.tiles {

                        // TODO: encode multiline strings together in same blob.
                        for pixels in lines {
                            if pixels.0.is_empty() {
                                continue;
                            }

                            // TODO: can consider storing post rasterization for faster renders.
                            let simplified = simplify(&pixels.0, 4.0);
                            let encoded = encode_raw(&simplified).expect("encode raw");

                            insert_coords
                                .insert(params![activity_id, tile.z, tile.x, tile.y, encoded])
                                .expect("insert coords");
                        }
                    }
                }
            },
        );

    pool.get().unwrap().execute_batch("VACUUM").expect("vacuum");
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
                    line.0.push(a.to_pixel(&bbox, STORED_TILE_WIDTH as u16).into());
                }

                line.0.push(b.to_pixel(&bbox, STORED_TILE_WIDTH as u16).into());

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
        self.tracks.iter()
            .map(LineString::haversine_length)
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
        assert_eq!(point_to_line_dist(&Coord { x: 5, y: 0 }, &start, &end), (5.0 * 2.0_f32.sqrt()) / 2.0);
        assert_eq!(point_to_line_dist(&Coord { x: 0, y: 5 }, &start, &end), (5.0 * 2.0_f32.sqrt()) / 2.0);
        assert_eq!(point_to_line_dist(&Coord { x: 0, y: 10 }, &start, &end), (10.0 * 2.0_f32.sqrt()) / 2.0);
        assert_eq!(point_to_line_dist(&Coord { x: 10, y: 0 }, &start, &end), (10.0 * 2.0_f32.sqrt()) / 2.0);
    }

    #[test]
    fn test_point_to_line_same_point() {
        let start = Coord { x: 0, y: 0 };
        let end = Coord { x: 0, y: 0 };

        assert_eq!(point_to_line_dist(&Coord { x: 0, y: 0 }, &start, &end), 0.0);
        assert_eq!(point_to_line_dist(&Coord { x: 1, y: 1 }, &start, &end), (2_f32).sqrt());
    }
}
