use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use csv::StringRecord;
use fitparser::de::{from_reader_with_options, DecodeOption};
use fitparser::profile::MesgNum;
use fitparser::Value;
use flate2::read::GzDecoder;
use geo::EuclideanDistance;
use geo_types::{LineString, MultiLineString, Point};
use rayon::iter::{ParallelBridge, ParallelIterator};
use rusqlite::params;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use time::OffsetDateTime;
use walkdir::WalkDir;

use crate::db;
use crate::db::{encode_line, Database};
use crate::simplify::simplify_line;
use crate::tile::{BBox, LngLat, Tile, WebMercator};

// TODO: not happy with the ergonomics of this.
struct TileClipper {
    zoom: u8,
    tile_extent: u16,
    current: Option<(Tile, BBox)>,
    tiles: HashMap<Tile, Vec<LineString<u16>>>,
}

impl TileClipper {
    fn new(zoom: u8, tile_extent: u16) -> Self {
        Self {
            zoom,
            tile_extent,
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
        let lines = self.tiles.entry(*tile).or_insert_with(Vec::new);

        if lines.is_empty() {
            lines.push(LineString::new(vec![]));
        }

        lines.last_mut().unwrap()
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
            }

            // [start, end] is at least partially contained within the current tile.
            Some((a, b)) => {
                let extent = self.tile_extent;
                let line = self.last_line(&tile);
                if line.0.is_empty() {
                    line.0.push(a.to_pixel(&bbox, extent).into());
                }

                line.0.push(b.to_pixel(&bbox, extent).into());

                // If we've modified the end point, we've left the current tile.
                if b != end {
                    self.finish_segment();

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
                lines.push(LineString::new(vec![]));
            });
        }
    }
}

pub struct ClippedTiles(Vec<TileClipper>);

impl ClippedTiles {
    pub fn iter(&self) -> impl Iterator<Item = (&Tile, &LineString<u16>)> {
        self.0
            .iter()
            .flat_map(|clip| clip.tiles.iter())
            .filter(|(_, lines)| !lines.is_empty())
            .flat_map(|(tile, lines)| lines.iter().map(move |line| (tile, line)))
    }
}

#[derive(Clone)]
pub struct RawActivity {
    // TODO: should we treat this specially or just part of metadata?
    pub title: Option<String>,

    pub start_time: Option<OffsetDateTime>,
    pub tracks: MultiLineString,
    pub properties: HashMap<String, serde_json::Value>,
}

impl RawActivity {
    /// How far apart two points can be before we consider them to be
    /// a separate line segment.
    ///
    /// TODO: move to db config?
    const MAX_POINT_DISTANCE: f64 = 5000.0;

    pub fn clip_to_tiles(
        &self,
        db::Config {
            ref zoom_levels,
            ref trim_dist,
            ref tile_extent,
        }: &db::Config,
    ) -> ClippedTiles {
        let mut clippers: Vec<_> = zoom_levels
            .iter()
            .map(|z| TileClipper::new(*z, *tile_extent as u16))
            .collect();

        for line in self.tracks.iter() {
            let points: Vec<_> = line
                .points()
                .map(LngLat::from)
                .filter_map(|pt| pt.xy())
                .collect();

            if points.len() < 2 {
                continue;
            }

            let first = &points[0].0;
            let last = &points[points.len() - 1].0;

            // Find points which are >= trim_dist away from start/end
            let start_idx = points
                .iter()
                .enumerate()
                .find(|(_, pt)| pt.0.euclidean_distance(first) >= *trim_dist)
                .map(|(i, _)| i);

            let end_idx = points
                .iter()
                .rev()
                .enumerate()
                .find(|(_, pt)| pt.0.euclidean_distance(last) >= *trim_dist)
                .map(|(i, _)| points.len() - 1 - i);

            if let Some((i, j)) = start_idx.zip(end_idx) {
                if i >= j {
                    continue;
                }

                let mut pairs = points[i..j].windows(2);
                while let Some(&[p0, p1]) = pairs.next() {
                    // Skip over large jumps
                    let len = p0.0.euclidean_distance(&p1.0);
                    if len > Self::MAX_POINT_DISTANCE {
                        continue;
                    }

                    for clip in clippers.iter_mut() {
                        clip.add_line_segment(p0, p1);
                    }
                }

                for clip in clippers.iter_mut() {
                    clip.finish_segment();
                }
            }
        }

        ClippedTiles(clippers)
    }
}

pub enum MediaType {
    Gpx,
    Fit,
    Tcx,
}

pub enum Compression {
    None,
    Gzip,
}

pub fn read<R>(rdr: R, kind: MediaType, comp: Compression) -> Result<Option<RawActivity>>
where
    R: Read + 'static,
{
    let mut reader: BufReader<Box<dyn Read>> = BufReader::new(match comp {
        Compression::None => Box::new(rdr),
        Compression::Gzip => Box::new(GzDecoder::new(rdr)),
    });

    match kind {
        MediaType::Gpx => parse_gpx(&mut reader),
        MediaType::Fit => parse_fit(&mut reader),
        MediaType::Tcx => parse_tcx(&mut reader),
    }
}

pub fn read_file(p: &Path) -> Result<Option<RawActivity>> {
    let Some(file_name) = p.file_name().and_then(|f| f.to_str()) else {
        return Err(anyhow!("no file name"));
    };

    let Some((media_type, comp)) = get_file_type(file_name) else {
        // Just skip over unsupported file types.
        return Ok(None);
    };

    let file = File::open(p)?;
    read(file, media_type, comp)
}

fn parse_fit<R: Read>(r: &mut R) -> Result<Option<RawActivity>> {
    const SCALE_FACTOR: f64 = (1u64 << 32) as f64 / 360.0;

    let opts = [
        DecodeOption::SkipDataCrcValidation,
        DecodeOption::SkipHeaderCrcValidation,
    ]
    .into();

    let mut start_time = None;
    let mut points = vec![];
    for data in from_reader_with_options(r, &opts)? {
        match data.kind() {
            MesgNum::FileId => {
                for f in data.fields() {
                    // Skip over virtual rides (not an exhaustive check)
                    if f.name() == "manufacturer" {
                        match f.value() {
                            Value::String(val) if val.as_str() == "zwift" => return Ok(None),
                            _ => {}
                        }
                    }
                }
            }
            MesgNum::Record => {
                let mut lat: Option<i64> = None;
                let mut lng: Option<i64> = None;

                for f in data.fields() {
                    match f.name() {
                        "position_lat" => lat = f.value().try_into().ok(),
                        "position_long" => lng = f.value().try_into().ok(),
                        "timestamp" => {
                            if start_time.is_none() {
                                let ts: i64 = f.value().try_into()?;
                                start_time = Some(ts);
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
        return Ok(None);
    }

    let line = points.into_iter().collect::<LineString>();
    Ok(Some(RawActivity {
        title: None,
        start_time: start_time.map(|ts| OffsetDateTime::from_unix_timestamp(ts).unwrap()),
        tracks: MultiLineString::from(line),
        // TODO: populate metadata
        properties: HashMap::new(),
    }))
}

fn parse_gpx<R: Read>(reader: &mut R) -> Result<Option<RawActivity>> {
    let gpx = gpx::read(reader)?;

    // Just take the first track (generally the only one).
    let Some(track) = gpx.tracks.first() else {
        return Ok(None);
    };

    let start_time = gpx.metadata.and_then(|m| m.time).map(OffsetDateTime::from);

    Ok(Some(RawActivity {
        start_time,
        title: track.name.clone(),
        tracks: track.multilinestring(),
        // TODO: metadata - already have a serde-friendly value in gpx.metadata
        properties: HashMap::new(),
    }))
}

// FIXME: this is a mess
fn parse_tcx<R: Read>(reader: &mut BufReader<R>) -> Result<Option<RawActivity>> {
    // For some reason all my TCX files start with a bunch of spaces?
    reader.fill_buf()?;
    while let Some(&b' ') = reader.buffer().first() {
        reader.consume(1);
    }

    let tcx = tcx::read(reader)?;
    let Some(activities) = tcx.activities.map(|it| it.activities) else {
        return Ok(None);
    };

    let Some(activity) = activities.first() else {
        return Ok(None);
    };

    let start_time = activity
        .laps
        .first()
        .and_then(|lap| lap.tracks.first())
        .and_then(|track| track.trackpoints.first())
        .map(|pt| OffsetDateTime::from_unix_timestamp(pt.time.timestamp()).unwrap());

    let tracks = activity
        .laps
        .iter()
        .flat_map(|lap| &lap.tracks)
        .map(|track| &track.trackpoints)
        .map(|points| {
            points
                .iter()
                .filter_map(|pt| pt.position.as_ref())
                .map(|pt| Point::new(pt.longitude, pt.latitude))
                .collect::<LineString>()
        })
        .filter(|line| !line.0.is_empty())
        .collect::<MultiLineString>();

    if tracks.0.is_empty() {
        return Ok(None);
    }

    Ok(Some(RawActivity {
        start_time,
        tracks,
        title: None,
        // TODO: populate metadata
        properties: HashMap::new(),
    }))
}

/// Allows us to treat `bar.gpx.gz` the same as `bar.gpx`.
pub fn get_file_type(file_name: &str) -> Option<(MediaType, Compression)> {
    let mut exts = file_name.rsplit('.');

    let (comp, ext) = match exts.next()? {
        "gz" => (Compression::Gzip, exts.next()?),
        ext => (Compression::None, ext),
    };

    match ext {
        "gpx" => Some((MediaType::Gpx, comp)),
        "fit" => Some((MediaType::Fit, comp)),
        "tcx" => Some((MediaType::Tcx, comp)),
        _ => None,
    }
}

pub fn upsert(
    conn: &mut rusqlite::Connection,
    name: &str,
    activity: &RawActivity,
    config: &db::Config,
) -> Result<i64> {
    let mut insert_tile = conn.prepare_cached(
        "\
        INSERT INTO activity_tiles (activity_id, z, x, y, coords) \
        VALUES (?, ?, ?, ?, ?)",
    )?;

    let num_rows = conn.execute(
        "\
        INSERT OR REPLACE \
        INTO activities (file, title, start_time, properties) \
        VALUES (?, ?, ?, ?)",
        params![
            name,
            activity.title,
            activity.start_time,
            serde_json::to_string(&activity.properties)?,
        ],
    )?;

    let activity_id = conn.last_insert_rowid();

    // If we've affected more than one row, we've replaced an existing one... so we need to
    // delete the existing tiles.
    if num_rows != 1 {
        conn.execute(
            "DELETE FROM activity_tiles WHERE activity_id = ?",
            params![activity_id],
        )?;
    }

    let tiles = activity.clip_to_tiles(config);
    for (tile, line) in tiles.iter() {
        let coords = encode_line(&simplify_line(&line.0, 4.0))?;
        insert_tile.insert(params![activity_id, tile.z, tile.x, tile.y, coords])?;
    }

    Ok(activity_id)
}

pub struct PropertySource {
    base_dir: PathBuf,
    path_props: HashMap<PathBuf, HashMap<String, serde_json::Value>>,
}

impl Default for PropertySource {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::new(),
            path_props: HashMap::new(),
        }
    }
}

impl PropertySource {
    pub(crate) fn from_csv(csv_path: &Path) -> Result<Self> {
        const JOIN_COL: &str = "filename";

        let base_dir = csv_path.parent().unwrap_or(Path::new("/")).canonicalize()?;

        let mut rdr = csv::Reader::from_path(csv_path)?;
        let mut path_props = HashMap::new();

        // Normalize header naming
        let headers = StringRecord::from_iter(
            rdr.headers()?
                .iter()
                .map(|hdr| hdr.to_lowercase().replace(' ', "_")),
        );
        rdr.set_headers(headers);

        for row in rdr.deserialize() {
            let mut row: HashMap<String, String> = row?;

            // Only keep the non-empty keys
            row.retain(|_k, v| !v.trim().is_empty());

            // TODO: report error if this is missing
            let Some(filename) = row.remove(JOIN_COL) else {
                tracing::warn!(?row, "missing {JOIN_COL} column");
                continue;
            };

            let json_props = row
                .into_iter()
                .map(|(k, v)| {
                    let val =
                        serde_json::Value::from_str(&v).unwrap_or(serde_json::Value::String(v));
                    (k, val)
                })
                .collect();

            path_props.insert(PathBuf::from(filename), json_props);
        }

        Ok(Self {
            base_dir,
            path_props,
        })
    }

    /// Merge properties from the attribute source into the activity.
    fn enrich(&self, path: &Path, activity: &mut RawActivity) {
        let path = path.strip_prefix(&self.base_dir).ok();
        let Some(props) = path.and_then(|p| self.path_props.get(p)) else {
            // We'll get here if there are activities in the import directory which don't have
            // a corresponding line in the metadata file.
            return;
        };

        for (k, v) in props {
            activity.properties.insert(k.clone(), v.clone());
        }
    }
}

pub fn import_path(p: &Path, db: &Database, prop_source: &PropertySource) -> Result<()> {
    let conn = db.connection()?;

    // Skip any files that are already in the database.
    let known_files: HashSet<String> = conn
        .prepare("SELECT file FROM activities")?
        .query_map([], |row| row.get(0))?
        .filter_map(|n| n.ok())
        .collect();

    tracing::info!(
        path = ?p,
        num_known = known_files.len(),
        "starting activity import"
    );

    let num_imported = AtomicU32::new(0);
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
            let activity = read_file(&path)
                .map_err(|err| tracing::error!(?path, ?err, "failed to read activity"))
                .ok()??;

            Some((path, activity))
        })
        .for_each_init(
            || db.shared_pool(),
            |pool, (path, mut activity)| {
                tracing::debug!(?path, "importing activity");

                // Merge with activity properties
                prop_source.enrich(&path, &mut activity);

                let mut conn = pool.get().expect("db connection pool timed out");
                upsert(&mut conn, path.to_str().unwrap(), &activity, &db.config)
                    .expect("insert activity");

                num_imported.fetch_add(1, Ordering::Relaxed);
            },
        );

    conn.execute_batch("VACUUM")?;
    tracing::info!(?num_imported, "finished import");
    Ok(())
}
