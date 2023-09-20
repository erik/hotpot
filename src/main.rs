mod tiles;

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

const MAX_TILE_ZOOM: usize = 14;
const TILE_EXTENT: usize = 4096;

fn main() {
    let conn = rusqlite::Connection::open("hotpot.sqlite3").unwrap();

    conn.execute_batch("CREATE TABLE IF NOT EXISTS foo(bar int);")
        .unwrap();

    println!("Hello, world!");

    ingest_dir(Path::new("/Users/erik/Downloads"));
}

fn ingest_dir(p: &Path) {
    let walk = WalkDir::new(p);

    walk.into_iter()
        .par_bridge()
        // TODO: why is this result?
        .map(|e| e.expect("todo"))
        .for_each(|entry| {
            let path = entry.path();

            match get_extensions(path) {
                Some(("gpx", compressed)) => {
                    let mut reader = open_reader(path, compressed);
                    let start = Instant::now();
                    parse_gpx(&mut reader);
                    let duration = start.elapsed();

                    println!("--> have gpx: {:?} (took: {:?})", path, duration);
                }

                Some(("fit", compressed)) => {
                    let mut reader = open_reader(path, compressed);
                    println!("--> have fit: {:?}", path);
                    parse_fit(&mut reader);
                }

                _ => {}
            }
        });
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
