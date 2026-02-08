use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{Result, anyhow};
use csv::StringRecord;
use fitparser::de::{DecodeOption, from_reader_with_options};
use fitparser::profile::MesgNum;
use flate2::read::GzDecoder;
use geo::{EuclideanDistance, HasDimensions, MapCoords, Simplify};
use geo_types::{LineString, MultiLineString, Point};
use rayon::iter::{ParallelBridge, ParallelIterator};
use rusqlite::params;
use time::OffsetDateTime;
use walkdir::WalkDir;

use crate::db;
use crate::db::{Config, Database, encode_line};
use crate::tile::{BBox, LngLat, Tile, WebMercator};
use crate::track_stats::{self, TrackPoint, TrackStats};

struct TileClipper {
    zoom: u8,
    tile_extent: i32,
    current: Option<(Tile, BBox)>,
    tiles: HashMap<Tile, Vec<LineString<f64>>>,
}

impl TileClipper {
    fn new(zoom: u8, tile_extent: i32) -> Self {
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

    fn last_line(&mut self, tile: &Tile) -> &mut LineString<f64> {
        let lines = self.tiles.entry(*tile).or_default();

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
                if line.is_empty() {
                    line.0.push(a.to_tile_pixel(&bbox, extent));
                }

                line.0.push(b.to_tile_pixel(&bbox, extent));

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
    pub fn iter(&self) -> impl Iterator<Item = (&Tile, &LineString<f64>)> {
        self.0
            .iter()
            .flat_map(|clip| clip.tiles.iter())
            .filter(|(_, lines)| !lines.is_empty())
            .flat_map(|(tile, lines)| lines.iter().map(move |line| (tile, line)))
    }
}

#[derive(Clone)]
pub struct RawActivity {
    pub title: Option<String>,
    pub start_time: Option<OffsetDateTime>,
    pub tracks: MultiLineString,
    pub properties: HashMap<String, serde_json::Value>,
}

/// How far apart two points can be before we consider them to be
/// a separate line segment.
///
pub const MAX_POINT_DISTANCE: f64 = 5000.0;

impl RawActivity {
    pub fn clip_to_tiles(
        &self,
        db::Config {
            zoom_levels,
            trim_dist,
            tile_extent,
            ..
        }: &db::Config,
    ) -> ClippedTiles {
        let mut clippers: Vec<_> = zoom_levels
            .iter()
            .map(|z| TileClipper::new(*z, *tile_extent as i32))
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
                    if len > MAX_POINT_DISTANCE {
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

#[derive(Debug)]
pub enum MediaType {
    Gpx,
    Fit,
    Tcx,
}

#[derive(Debug)]
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

// Not an exhaustive list, but the most obvious of the FIT "sub_sports" which it
// doesn't make sense to include in a heatmap.
const FIT_VIRTUAL_SPORTS: [&str; 4] = [
    "virtual_activity",
    "indoor_cycling",
    "indoor_rowing",
    "indoor_running",
];

fn parse_fit<R: Read>(r: &mut R) -> Result<Option<RawActivity>> {
    const SCALE_FACTOR: f64 = (1u64 << 32) as f64 / 360.0;

    let opts = [
        DecodeOption::SkipDataCrcValidation,
        DecodeOption::SkipHeaderCrcValidation,
    ]
    .into();

    let mut properties = HashMap::new();
    let mut start_time = None;
    let mut track_points = vec![];
    for data in from_reader_with_options(r, &opts)? {
        match data.kind() {
            // There's one FileId block per file and one or more sessions.
            // Currently not really supporting the concept of multi-session
            // files, so don't try to be clever with parsing.
            MesgNum::FileId | MesgNum::Session => {
                for f in data.into_vec().into_iter() {
                    match f.name() {
                        "sub_sport" => {
                            // Skip over virtual activity types
                            if let fitparser::Value::String(ty) = f.value()
                                && FIT_VIRTUAL_SPORTS.contains(&ty.as_str())
                            {
                                return Ok(None);
                            }
                        }

                        "start_time" => {
                            let fitparser::Value::Timestamp(ts) = f.value() else {
                                continue;
                            };
                            start_time = Some(ts.timestamp());
                        }

                        key if key.starts_with("unknown_field_") => {
                            // Skip anything the fitparser library doesn't know
                            // about.
                        }

                        // Blindly stuff the remaining attributes into properties
                        key => {
                            properties.insert(key.to_owned(), serde_json::to_value(f.value())?);
                        }
                    }
                }
            }
            MesgNum::Record => {
                let mut lat: Option<i64> = None;
                let mut lng: Option<i64> = None;
                let mut elevation: Option<f64> = None;
                let mut timestamp: Option<i64> = None;

                for f in data.into_vec().into_iter() {
                    match f.name() {
                        "position_lat" => lat = f.value().try_into().ok(),
                        "position_long" => lng = f.value().try_into().ok(),
                        // Prefer enhanced_altitude over altitude
                        "altitude" if elevation.is_none() => {
                            elevation = f.into_value().try_into().ok()
                        }
                        "enhanced_altitude" => elevation = f.into_value().try_into().ok(),
                        "timestamp" => {
                            timestamp = f.value().try_into().ok();
                            if timestamp.is_some() && start_time.is_none() {
                                start_time = timestamp;
                            }
                        }
                        _ => {}
                    }
                }

                if let (Some(lat), Some(lng)) = (lat, lng) {
                    let pt = Point::new(lng as f64, lat as f64) / SCALE_FACTOR;
                    track_points.push(track_stats::TrackPoint {
                        point: pt,
                        elevation,
                        timestamp,
                    });
                }
            }
            _ => {}
        }
    }

    if track_points.is_empty() {
        return Ok(None);
    }

    let stats = track_stats::TrackStats::from_points(&track_points);
    stats.merge_into(&mut properties);

    let line: Vec<_> = track_points.iter().map(|pt| pt.point).collect();
    Ok(Some(RawActivity {
        properties,
        title: None,
        start_time: start_time.map(|ts| OffsetDateTime::from_unix_timestamp(ts).unwrap()),
        tracks: MultiLineString::from(line),
    }))
}

fn parse_gpx<R: Read>(reader: &mut R) -> Result<Option<RawActivity>> {
    let gpx = gpx::read(reader)?;

    // Just take the first track (generally the only one).
    let Some(track) = gpx.tracks.first() else {
        return Ok(None);
    };

    let mut properties = HashMap::new();

    if let Some(ref ty) = track.type_ {
        // Skip virtual activities. <type> is free form, so this won't be exhaustive.
        if ty.starts_with("Virtual") {
            return Ok(None);
        }

        properties.insert(
            "activity_type".to_owned(),
            serde_json::Value::String(ty.to_owned()),
        );
    }

    let start_time = gpx.metadata.and_then(|m| m.time).map(OffsetDateTime::from);

    // Iterate segments manually to extract elevation and time data
    let mut track_points = vec![];
    let mut lines = vec![];

    for segment in &track.segments {
        let mut line = vec![];
        for pt in &segment.points {
            let point = pt.point();
            line.push(point);
            track_points.push(TrackPoint {
                point,
                elevation: pt.elevation,
                timestamp: pt.time.map(|t| OffsetDateTime::from(t).unix_timestamp()),
            });
        }

        if !line.is_empty() {
            lines.push(LineString::from(line));
        }
    }

    if track_points.is_empty() {
        return Ok(None);
    }

    let stats = TrackStats::from_points(&track_points);
    stats.merge_into(&mut properties);

    Ok(Some(RawActivity {
        start_time,
        properties,
        title: track.name.clone(),
        tracks: MultiLineString::new(lines),
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

    let mut track_points = vec![];
    let mut lines = vec![];

    for lap in &activity.laps {
        for track in &lap.tracks {
            let mut line = vec![];
            for pt in &track.trackpoints {
                let Some(pos) = pt.position.as_ref() else {
                    continue;
                };

                let point = Point::new(pos.longitude, pos.latitude);
                line.push(point);

                track_points.push(TrackPoint {
                    point,
                    elevation: pt.altitude_meters,
                    timestamp: Some(pt.time.timestamp()),
                });
            }

            if !line.is_empty() {
                lines.push(LineString::from(line));
            }
        }
    }

    if track_points.is_empty() {
        return Ok(None);
    }

    let mut properties = HashMap::new();
    let stats = TrackStats::from_points(&track_points);
    stats.merge_into(&mut properties);

    Ok(Some(RawActivity {
        start_time,
        tracks: MultiLineString::new(lines),
        title: None,
        properties,
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
        INTO activities (file, title, start_time, properties, created_at) \
        VALUES (?, ?, ?, ?, ?)",
        params![
            name,
            activity.title,
            activity.start_time,
            serde_json::to_string(&activity.properties)?,
            OffsetDateTime::now_utc(),
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

    let tile_size = config.tile_extent as f64;
    let tiles = activity.clip_to_tiles(config);
    for (tile, line) in tiles.iter() {
        // Have to type-dance a bit because geo::Simplify requires f64
        let simplified_line = line
            .map_coords(|c| {
                // For reasons I cannot remember, we store tile activity data
                // with inverted Y coordinates from the pixel data.
                let flip_y = tile_size - c.y;
                (c.x, flip_y).into()
            })
            .simplify(&4.0);

        let coords = encode_line(&simplified_line)?;
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
        // /../../export/activities/file.gpx => activities/file.gpx
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

pub fn import_path(
    path: &Path,
    db: &Database,
    config: &Config,
    prop_source: &PropertySource,
) -> Result<()> {
    let conn = db.connection()?;

    // Skip any files that are already in the database.
    let known_files: HashSet<String> = conn
        .prepare("SELECT DISTINCT file FROM activities")?
        .query_map([], |row| row.get(0))?
        .filter_map(|n| n.ok())
        .collect();

    tracing::info!(
        path = ?path,
        count_known_files = known_files.len(),
        "starting activity import"
    );

    let imported = AtomicU32::new(0);
    let skipped = AtomicU32::new(0);
    let failed = AtomicU32::new(0);

    WalkDir::new(path)
        .into_iter()
        .par_bridge()
        .filter_map(|dir| {
            let dir = dir.ok()?;
            if !dir.file_type().is_file() {
                return None;
            }

            let path = dir.path();

            if !known_files.contains(path.to_str()?) {
                Some(path.to_owned())
            } else {
                tracing::debug!(?path, "skipping, already imported");
                skipped.fetch_add(1, Ordering::Relaxed);
                None
            }
        })
        .filter_map(|path| {
            let activity = read_file(&path)
                .inspect_err(|err| {
                    tracing::error!(?path, ?err, "failed to read activity");
                    failed.fetch_add(1, Ordering::Relaxed);
                })
                .inspect(|activity| {
                    if activity.is_none() {
                        tracing::debug!(?path, "skipping, no track data");
                        skipped.fetch_add(1, Ordering::Relaxed);
                    }
                })
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
                upsert(&mut conn, path.to_str().unwrap(), &activity, config)
                    .expect("insert activity");

                imported.fetch_add(1, Ordering::Relaxed);
            },
        );

    conn.execute_batch("VACUUM")?;
    tracing::info!(
        ?imported,
        ?skipped,
        ?failed,
        "finished import from {:?}",
        path
    );

    Ok(())
}
