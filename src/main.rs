use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::time::Instant;

use fitparser;
use fitparser::de::{DecodeOption, from_reader_with_options};
use fitparser::profile::MesgNum;
use flate2::read::GzDecoder;
use geo_types::{LineString, MultiLineString, Point};
use rayon::prelude::*;
use rusqlite;
use walkdir::WalkDir;
use crate::tiles::{MercatorPixel, Tile};

mod tiles;

fn main() {
    let conn = rusqlite::Connection::open("hotpot.sqlite3").unwrap();

    conn.execute_batch("CREATE TABLE IF NOT EXISTS foo(bar int);")
        .unwrap();

    println!("Hello, world!");

    ingest_dir(Path::new("/Users/erik/Downloads/strava_export_20230912/activities"));
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

                _ => return
            };

            if let Some(lines) = lines {
                let parse = start.elapsed();
                let tile_width = 4096;
                let tile_zoom = 14;

                for line in lines {
                    let mut clipper = TileClipper::new(tile_width, tile_zoom);

                    line.points()
                        .map(|pt| tiles::LngLat::new(pt.x() as f32, pt.y() as f32))
                        .filter_map(|pt| pt.xy().map(|xy| xy.pixel_xy(tile_width, tile_zoom)))
                        .for_each(|pt| clipper.add_point(&pt));

                    for (tile, segments) in clipper.tiles {
                        // TODO: write to DB
                    }
                }

                let intersect = start.elapsed() - parse;
                println!("  --> READ:\t{:?}\tINTERSECT:{:?}\t{:?}", parse, intersect,  path);
            }
        });
}

struct TileClipper {
    width: u32,
    zoom: u8,
    tiles: HashMap<Tile, Vec<()>>,
    prev: Option<Tile>,
}

impl TileClipper {
    fn new(tile_width: u32, tile_zoom: u8) -> Self {
        Self {
            width: tile_width,
            zoom: tile_zoom,
            tiles: HashMap::new(),
            prev: None,
        }
    }

    fn add_point(&mut self, pt: &MercatorPixel) {
        todo!()
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
