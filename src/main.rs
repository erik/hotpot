use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::path::Path;
use std::thread;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fitparser;
use fitparser::de::{DecodeOption, from_reader_with_options};
use fitparser::profile::MesgNum;
use flate2::read::GzDecoder;
use flate2::write::ZlibEncoder;
use geo_types::{Coord, LineString, MultiLineString, Point};
use rayon::prelude::*;
use rusqlite;
use rusqlite::params;
use walkdir::WalkDir;

use crate::tiles::{BBox, LngLat, Tile, WebMercator};

mod tiles;

fn encode_compressed(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut enc = ZlibEncoder::new(
        vec![],
        flate2::Compression::default(),
    );
    enc.write_all(data)?;
    Ok(enc.finish()?)
}

fn encode_raw(data: &[Coord<u32>]) -> anyhow::Result<Vec<u8>> {
    let mut w = Vec::with_capacity(data.len() * 2);
    for pt in data {
        // TODO: we don't need u32 pixel coords anyway
        w.write_u16::<LittleEndian>(pt.x as u16)?;
        w.write_u16::<LittleEndian>(pt.y as u16)?;
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

// FIXME: riddled with magic numbers
fn render_tile(
    tile: Tile,
    conn: &mut rusqlite::Connection,
) -> rusqlite::Result<Vec<u8>> {
    let mut select_stmt = conn
        .prepare("SELECT coords FROM activity_tiles WHERE z = ? AND x = ? AND y = ?;")?;

    const TILE_WIDTH: usize = 1024;
    const TILE_SIZE: usize = TILE_WIDTH * TILE_WIDTH;
    let mut pixels: Vec<u16> = vec![0; TILE_SIZE];

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
            let x = ((x >> 2) & 0x3ff) as usize;
            let y = 0x3ff - ((y >> 2) & 0x3ff) as usize;

            // TODO: we want to do more interesting visualizations here.
            if let Some(Coord { x: px, y: py }) = prev {
                if x == px && y == py {
                    continue;
                }

                let line_iter = line_drawing::XiaolinWu::<f32, isize>::new(
                    (px as f32, py as f32),
                    (x as f32, y as f32),
                );

                for ((ix, iy), value) in line_iter {
                    if ix < 0 || ix >= TILE_WIDTH as isize || iy < 0 || iy >= TILE_WIDTH as isize {
                        continue;
                    }

                    // TODO: figure out good anti-aliasing
                    let incr = (value * 1024.0) as u16;
                    let (x, y) = (ix as usize, iy as usize);
                    pixels[y * TILE_WIDTH + x] = pixels[y * TILE_WIDTH + x].saturating_add(incr);
                }
            }
            prev = Some(Coord { x, y });
        }
    }

    // TODO: mega jank
    let pixels = pixels.iter().map(|v| (v / 256) as u8).collect::<Vec<u8>>();
    Ok(pixels)
}


fn init_db(conn: &mut rusqlite::Connection) -> rusqlite::Result<HashMap<String, String>> {
    const MIGRATIONS: &[&str] = &[
        "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL );",
        "
CREATE TABLE activities (
      id   INTEGER PRIMARY KEY
    , name TEXT

    -- TODO:
    -- kind TEXT,
    -- start_timestamp INTEGER NOT NULL,
    -- end_timestamp INTEGER NOT NULL,
    -- distance REAL NOT NULL,
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
        for m in &MIGRATIONS[cur_migration + 1..] {
            println!("Running migration: {}", m);
            tx.execute_batch(m)?;
        }
        tx.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('version', ?)",
            [MIGRATIONS.len()],
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
    // Skip any files that are already in the database.
    // TODO: avoid the collect call here.
    let walk: Vec<_> = WalkDir::new(p)
        .into_iter()
        .filter_entry(|d| {
            conn.query_row(
                "SELECT 1 FROM activities WHERE name = ? LIMIT 1",
                params![d.path().to_str().unwrap()],
                |_row| Ok(()),
            ).is_err()
        })
        .collect();

    let (tx, rx) = std::sync::mpsc::channel();

    thread::spawn(move || {
        walk
            .into_par_iter()
            .map(|e| e.expect("todo"))
            // TODO: why is this result?
            .for_each_with(tx, |tx, entry| {
                let path = entry.path();

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
                    const STORED_LEVELS: &[u8] = &[1, 8, 14];
                    for level in STORED_LEVELS {
                        let mut clipper = TileClipper::new(*level);

                        for line in &lines {
                            let points = line
                                .points()
                                .map(|pt| LngLat::new(pt.x() as f32, pt.y() as f32))
                                .filter_map(|pt| pt.xy());

                            let mut prev = None;
                            for next in points {
                                // TODO: should try to filter based on distance to previous point.
                                if let Some(prev) = prev {
                                    clipper.add_line_segment(prev, next);
                                }
                                prev = Some(next);
                            }

                            clipper.finish_line();
                        }

                        // TODO: should perform a simplification step first.
                        tx.send((path.to_owned(), clipper)).unwrap();
                    }
                }
            });
    });

    let mut insert_activity = conn
        .prepare("INSERT INTO activities (name) VALUES (?)")
        .expect("prepare statement");

    let mut insert_coords = conn
        .prepare("INSERT INTO activity_tiles (activity_id, z, x, y, coords) VALUES (?, ?, ?, ?, ?)")
        .expect("prepare statement");

    for (path, clipper) in rx {
        let activity_id = insert_activity
            .insert([path.to_str().unwrap()])
            .expect("insert activity");

        for (tile, lines) in clipper.tiles {
            for pixels in lines {
                let encoded = encode_raw(pixels.as_slice()).expect("encode raw");

                insert_coords
                    .insert(params![activity_id, tile.z, tile.x, tile.y, encoded])
                    .expect("insert coords");
            }
        }

        println!("Imported {}", path.to_str().unwrap());
    }
}

struct TileClipper {
    zoom: u8,
    tiles: HashMap<Tile, Vec<Vec<Coord<u32>>>>,
    tile: Tile,
    bbox: Option<BBox>,
    line: Vec<Coord<u32>>,
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
                    self.line.push(bbox.project(&a, 4096.0));
                }

                self.line.push(bbox.project(&b, 4096.0));

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
