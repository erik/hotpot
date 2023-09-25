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
use geo_types::{LineString, MultiLineString, Point};
use rayon::prelude::*;
use rusqlite;
use walkdir::WalkDir;

use crate::tiles::{BBox, LngLat, Tile, WebMercator};

mod tiles;

fn main() {
    let conn = rusqlite::Connection::open("hotpot.sqlite3").unwrap();

    conn.execute_batch("CREATE TABLE IF NOT EXISTS foo(bar int);")
        .unwrap();

    println!("Hello, world!");

    ingest_dir(Path::new("/Users/erik/Downloads/"));
}

fn ingest_dir(p: &Path) {
    let walk = WalkDir::new(p);

    walk.into_iter()
        .par_bridge()
        // TODO: why is this result?
        .map(|e| e.expect("todo"))
        .for_each(|entry| {
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

                for line in lines {
                    let mut clipper = TileClipper::new(tile_zoom);

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

                    for (tile, segments) in clipper.tiles {
                        // TODO: write to DB
                    }
                }

                let intersect = start.elapsed() - parse;
                println!(
                    "  --> READ:\t{:?}\tINTERSECT:{:?}\t{:?}",
                    parse, intersect, path
                );
            }
        });
}

struct TileClipper {
    zoom: u8,
    tiles: HashMap<Tile, Vec<Vec<WebMercator>>>,
    tile: Tile,
    bbox: BBox,
    line: Vec<WebMercator>,
}

impl TileClipper {
    fn new(zoom: u8) -> Self {
        Self {
            tiles: HashMap::new(),
            tile: Tile::new(0, 0, zoom),
            bbox: BBox::zero(),
            line: vec![],
            zoom,
        }
    }

    fn add_line_segment(&mut self, start: WebMercator, end: WebMercator) {
        match self.bbox.clip_line(&start, &end) {
            None => {
                // line segment is completely outside of the current tile
                self.finish_line();
                self.move_bbox(&start);
                self.add_line_segment(start, end);
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
                    self.add_line_segment(b, end);
                }
            }
        }
    }

    fn move_bbox(&mut self, pt: &WebMercator) {
        let tile = pt.tile(self.zoom);
        self.bbox = tile.xy_bounds();
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
    let gpx = gpx::read(reader).unwrap();
    // Just take the first track.
    gpx.tracks.first().map(|t| t.multilinestring())
}
