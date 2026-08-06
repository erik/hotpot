use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::Result;
use csv::StringRecord;
use geo::{EuclideanDistance, HasDimensions, MapCoords, Simplify};
use geo_types::{LineString, MultiLineString};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use time::OffsetDateTime;
use walkdir::WalkDir;

use crate::db;
use crate::db::{Config, Database, encode_line};
use crate::file::read_file;
use crate::tile::{BBox, LngLat, Tile, WebMercator};

struct TileClipper {
    zoom: u8,
    tile_extent: i32,
    current_tile: Option<(Tile, BBox)>,
    current_segment: LineString<f64>,
    segments: Vec<(Tile, LineString<f64>)>,
}

impl TileClipper {
    fn new(zoom: u8, tile_extent: i32) -> Self {
        Self {
            zoom,
            tile_extent,
            current_tile: None,
            current_segment: LineString::new(vec![]),
            segments: Vec::new(),
        }
    }

    fn bounding_tile(&self, pt: &WebMercator) -> (Tile, BBox) {
        let tile = pt.tile(self.zoom);
        let bbox = tile.xy_bounds();
        (tile, bbox)
    }

    fn add_line_segment(&mut self, start: WebMercator, end: WebMercator) {
        let (tile, bbox) = match self.current_tile {
            Some(pair) => pair,
            None => {
                let pair = self.bounding_tile(&start);
                self.current_tile = Some(pair);
                pair
            }
        };

        match bbox.clip_line(&start, &end) {
            // [start, end] doesn't intersect with the current tile at all, reposition it.
            None => {
                self.finish_segment();
                self.current_tile = Some(self.bounding_tile(&start));
            }

            // [start, end] is at least partially contained within the current tile.
            Some((a, b)) => {
                let extent = self.tile_extent;
                let line = &mut self.current_segment;

                let pb = b.to_tile_pixel(&bbox, extent);

                if let Some(&last) = line.0.last()
                    && pb != last
                {
                    // Cheap de-dupe for points that would be thrown away by RDP anyway
                    line.0.push(pb);
                } else {
                    // Otherwise we're at the start of a line segment, and we want
                    // to guarantee we have at least two points
                    line.0.push(a.to_tile_pixel(&bbox, extent));
                    line.0.push(pb);
                }

                // If we've modified the end point, we've left the current tile.
                if b != end {
                    self.finish_segment();

                    let (next_tile, next_bbox) = self.bounding_tile(&end);
                    if next_tile != tile {
                        self.current_tile = Some((next_tile, next_bbox));
                        self.add_line_segment(b, end);
                    }
                }
            }
        }
    }

    fn finish_segment(&mut self) {
        if let Some((tile, _bbox)) = self.current_tile
            && !self.current_segment.is_empty()
        {
            let segment = std::mem::replace(&mut self.current_segment, LineString::new(vec![]));
            self.segments.push((tile, segment));
        }
    }
}

pub struct ClippedTiles(Vec<TileClipper>);

impl ClippedTiles {
    pub fn iter(&self) -> impl Iterator<Item = (&Tile, &LineString<f64>)> {
        self.0
            .iter()
            .flat_map(|clip| clip.segments.iter())
            .map(|(tile, line)| (tile, line))
    }
}

#[derive(Clone)]
pub struct RawActivity {
    pub title: Option<String>,
    pub start_time: Option<OffsetDateTime>,
    pub tracks: MultiLineString,
    pub properties: HashMap<String, serde_json::Value>,
}

/// How far apart two points can be (meters) before we consider them to be a
/// separate line segment.
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

impl RawActivity {
    pub fn split_tiles(
        mut self,
        file_key: String,
        config: &db::Config,
    ) -> Result<db::TiledActivity> {
        // Round floats in a JSON value to reduce storage precision noise
        for val in self.properties.values_mut() {
            if let Some(n) = val.as_f64()
                && !val.is_i64()
                && !val.is_u64()
            {
                let mult = 10_000.0;
                *val = ((n * mult).round() / mult).into();
            }
        }
        let properties = serde_json::to_string(&self.properties)?;

        let tile_size = config.tile_extent as f64;
        let tiles = self
            .clip_to_tiles(config)
            .iter()
            .map(|(tile, line)| {
                let simplified_line = line.simplify(&4.0).map_coords(|c| {
                    // For reasons I cannot remember, we store tile activity data
                    // with inverted Y coordinates from the pixel data.
                    let flip_y = tile_size - c.y;
                    (c.x, flip_y).into()
                });

                (*tile, encode_line(&simplified_line))
            })
            .collect();

        Ok(db::TiledActivity {
            file_key,
            title: self.title,
            start_time: self.start_time,
            properties,
            tiles,
        })
    }
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
    pub fn from_csv(csv_path: &Path) -> Result<Self> {
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

    fn file_key(&self, path: &Path) -> Option<String> {
        // When we're importing from a Strava activity export, normalize the
        // file names so that we can de-dupe imports more effectively later (via
        // API or some such).
        if let Some(id) = self.strava_activity_id(path) {
            return Some(format!("strava:{id}"));
        }

        Some(path.to_str()?.to_owned())
    }

    fn lookup_props(&self, path: &Path) -> Option<&HashMap<String, serde_json::Value>> {
        // Convert /../../export/activities/file.gpx => activities/file.gpx
        let basename = path.strip_prefix(&self.base_dir).ok()?;
        self.path_props.get(basename)
    }

    /// Check if metadata structure matches Strava activity export format and
    /// return the `Activity ID` if so.
    fn strava_activity_id(&self, path: &Path) -> Option<String> {
        const STRAVA_ACTIVITY_ID_COL: &str = "activity_id";

        let props = self.lookup_props(path)?;
        match props.get(STRAVA_ACTIVITY_ID_COL)? {
            serde_json::Value::Number(n) => Some(n.to_string()),
            serde_json::Value::String(s) => Some(s.clone()),
            _ => None,
        }
    }

    /// Merge properties from the attribute source into the activity.
    fn enrich(&self, path: &Path, activity: &mut RawActivity) {
        let Some(props) = self.lookup_props(path) else {
            // We'll get here if there are activities in the import directory which don't have
            // a corresponding line in the metadata file.
            return;
        };

        for (k, v) in props {
            activity.properties.insert(k.clone(), v.clone());
        }
    }
}

/// The set of `file` keys already stored, used to skip re-importing activities.
pub fn known_files(conn: &rusqlite::Connection) -> Result<HashSet<String>> {
    let files = conn
        .prepare("SELECT DISTINCT file FROM activities")?
        .query_map([], |row| row.get(0))?
        .filter_map(|n| n.ok())
        .collect();
    Ok(files)
}

pub fn import_path(
    path: &Path,
    db: &Database,
    config: &Config,
    prop_source: &PropertySource,
) -> Result<()> {
    let conn = db.connection()?;

    // Skip any files that are already in the database.
    let known_files = known_files(&conn)?;

    tracing::info!(
        path = ?path,
        count_known_files = known_files.len(),
        "starting activity import"
    );

    let imported = AtomicU32::new(0);
    let skipped = AtomicU32::new(0);
    let failed = AtomicU32::new(0);

    // Gather all files we haven't yet ingested
    let files: Vec<_> = WalkDir::new(path)
        .into_iter()
        .filter_map(|dir| {
            let dir = dir.ok()?;
            if !dir.file_type().is_file() {
                return None;
            }

            let path = dir.path();
            let key = prop_source.file_key(path)?;
            if !known_files.contains(&key) {
                tracing::debug!(?path, "importing activity");
                Some((path.to_owned(), key))
            } else {
                tracing::debug!(?path, "skipping, already imported");
                skipped.fetch_add(1, Ordering::Relaxed);
                None
            }
        })
        .collect();

    let (tx, rx) = std::sync::mpsc::channel::<db::TiledActivity>();

    // Parse files in paralle, insert into DB sequentially (in single transaction)
    std::thread::scope(|thread| {
        thread.spawn(|| {
            let conn = db.connection().expect("open writer db connection");
            conn.execute_batch("BEGIN").expect("begin transaction");
            for activity in rx {
                db::upsert_activity(&conn, &activity).expect("insert activity");
                imported.fetch_add(1, Ordering::Relaxed);
            }
            conn.execute_batch("COMMIT").expect("commit transaction");
        });

        files.into_par_iter().for_each(|(path, key)| {
            let mut activity = match read_file(&path) {
                Ok(Some(activity)) => activity,

                Err(err) => {
                    tracing::error!(?path, ?err, "failed to read activity");
                    failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }

                Ok(None) => {
                    tracing::debug!(?path, "skipping, no track data");
                    skipped.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            // Merge with activity properties
            prop_source.enrich(&path, &mut activity);

            if let Err(err) = activity
                .split_tiles(key, config)
                .and_then(|a| tx.send(a).map_err(Into::into))
            {
                tracing::error!(?path, ?err, "failed to prepare activity");
                failed.fetch_add(1, Ordering::Relaxed);
            };
        });

        // Close sender so receiver can finish
        drop(tx);
    });

    // Update table statistics post-import
    conn.execute_batch("ANALYZE;")?;

    tracing::info!(
        ?imported,
        ?skipped,
        ?failed,
        "finished import from {:?}",
        path
    );

    Ok(())
}
