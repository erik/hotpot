use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use geo::{CoordNum, LineString};
use geo_types::Coord;
use num_traits::AsPrimitive;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{ToSql, params};
use serde::{Deserialize, Serialize};
use time::{Date, OffsetDateTime};

use crate::filter::PropertyFilter;

const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS config (
      key   TEXT NOT NULL PRIMARY KEY
    , value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS activities (
      id            INTEGER PRIMARY KEY
    , file          TEXT     NOT NULL
    , title         TEXT
    , start_time    TEXT
    , properties    BLOB     NOT NULL DEFAULT '{}'
);

CREATE UNIQUE INDEX IF NOT EXISTS activities_file ON activities (file);

CREATE TABLE IF NOT EXISTS activity_tiles (
      id          INTEGER PRIMARY KEY
    , activity_id INTEGER NOT NULL
    , z           INTEGER NOT NULL
    , x           INTEGER NOT NULL
    , y           INTEGER NOT NULL
    , coords      BLOB    NOT NULL
);

CREATE INDEX IF NOT EXISTS activity_tiles_activity_id ON activity_tiles (activity_id);
CREATE INDEX IF NOT EXISTS activity_tiles_zxy ON activity_tiles (z, x, y);

CREATE TABLE IF NOT EXISTS strava_tokens (
      athlete_id    INTEGER PRIMARY KEY
    , access_token  TEXT    NOT NULL
    , refresh_token TEXT    NOT NULL
    , expires_at    INTEGER NOT NULL
);
";

const MIGRATIONS: [&str; 2] = [
    // Keep track of when activities are added to the DB separately from when
    // they occurred.
    "ALTER TABLE activities ADD COLUMN created_at TEXT;",
    // JSONB requires less parsing, so is significantly faster for unindexed
    // lookups (as with our property filters). Requires sqlite 3.45.0+
    // (2024-01-15)
    "UPDATE activities SET properties = jsonb(properties);",
];

pub struct Database {
    pool: r2d2::Pool<SqliteConnectionManager>,
}

impl Database {
    pub fn new(path: &Path) -> Result<Self> {
        let manager = SqliteConnectionManager::file(path).with_init(|conn| {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "OFF")?;
            Ok(())
        });

        Self::from_connection(manager)
    }

    pub fn memory() -> Result<Self> {
        let manager = SqliteConnectionManager::memory();
        Self::from_connection(manager)
    }

    fn from_connection(manager: SqliteConnectionManager) -> Result<Self> {
        // Recent(ish) sqlite features we rely on:
        //   - JSON extraction with `->>` (3.38.0 released 2022)
        //   - JSONB support (3.45.0 released 2024)
        if rusqlite::version_number() < 3_045_000 {
            anyhow::bail!(
                "insufficient sqlite3 version {}, need >= 3.45.0",
                rusqlite::version()
            );
        }

        let pool = r2d2::Pool::new(manager)?;
        let mut conn = pool.get()?;

        apply_schema(&mut conn)?;

        // Load config to ensure defaults are saved
        let config = Config::load_from(&mut conn)?;
        config.save(&mut conn)?;

        Ok(Database { pool })
    }

    /// Open an existing database, fail if it doesn't exist
    pub fn open(path: &Path) -> Result<Self> {
        if !path.exists() {
            anyhow::bail!("database does not exist: {}", path.display());
        }

        Self::new(path)
    }

    pub fn reset_activities(&self) -> Result<()> {
        let conn = self.connection()?;
        conn.execute_batch("DELETE FROM activities; DELETE FROM activity_tiles; VACUUM;")?;
        tracing::info!("database reset");

        Ok(())
    }

    pub fn load_config(&self) -> Result<Config> {
        let mut conn = self.connection()?;
        Config::load_from(&mut conn)
    }

    pub fn save_config(&self, config: &Config) -> Result<()> {
        let mut conn = self.connection()?;
        config.save(&mut conn)
    }

    pub fn connection(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        let conn = self.pool.get()?;
        Ok(conn)
    }

    pub fn shared_pool(&self) -> r2d2::Pool<SqliteConnectionManager> {
        self.pool.clone()
    }
}

fn apply_schema(conn: &mut rusqlite::Connection) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute_batch(SCHEMA)?;

    let version: usize = tx.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if version < MIGRATIONS.len() {
        for sql in MIGRATIONS.iter().skip(version) {
            tracing::info!("applying migration: `{}`", sql);
            tx.execute_batch(sql)?;
        }

        tx.pragma_update(None, "user_version", MIGRATIONS.len())?;
    }

    tx.commit()?;
    Ok(())
}

const DEFAULT_TILE_EXTENT: u32 = 2048;
const DEFAULT_ZOOM_LEVELS: [u8; 5] = [2, 6, 10, 14, 16];
const DEFAULT_TRIM_DIST: f64 = 0.0;

/// A circular mask that hides activity data within its radius.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivityMask {
    pub name: String,
    pub lat: f64,
    pub lng: f64,
    pub radius: f64,
}

impl std::fmt::Display for ActivityMask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} - {:.5},{:.5} (radius: {}m)",
            self.name, self.lat, self.lng, self.radius
        )
    }
}

pub struct Config {
    /// Zoom levels that we store activity tiles for.
    pub zoom_levels: Vec<u8>,
    /// Width of the stored tiles, in pixels.
    pub tile_extent: u32,
    /// Distance to trim start/end of activities, in meters.
    pub trim_dist: f64,
    /// Areas to hide activity data.
    pub activity_mask: Vec<ActivityMask>,
}

impl Config {
    fn load_from(conn: &mut rusqlite::Connection) -> Result<Self> {
        let mut cfg = Config::default();

        let mut stmt = conn.prepare("SELECT key, value FROM config")?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let key: String = row.get_unwrap(0);
            let value: String = row.get_unwrap(1);

            match key.as_str() {
                "zoom_levels" => cfg.zoom_levels = serde_json::from_str(&value)?,
                "tile_extent" => cfg.tile_extent = value.parse()?,
                "trim_dist" => cfg.trim_dist = value.parse()?,
                "activity_masks" => cfg.activity_mask = serde_json::from_str(&value)?,
                key => tracing::warn!("Ignoring unknown config key: {}", key),
            }
        }

        Ok(cfg)
    }

    fn save(&self, conn: &mut rusqlite::Connection) -> Result<()> {
        let zoom_levels = serde_json::to_string(&self.zoom_levels)?;
        let activity_masks = serde_json::to_string(&self.activity_mask)?;

        let mut stmt = conn.prepare(
            "\
            INSERT OR REPLACE INTO config (key, value) \
            VALUES (?, ?)",
        )?;
        stmt.execute(params!["zoom_levels", &zoom_levels])?;
        stmt.execute(params!["tile_extent", &self.tile_extent])?;
        stmt.execute(params!["trim_dist", &self.trim_dist])?;
        stmt.execute(params!["activity_masks", &activity_masks])?;

        Ok(())
    }

    pub fn source_level(&self, target_zoom: u8) -> Option<u8> {
        for z in &self.zoom_levels {
            if *z >= target_zoom {
                return Some(*z);
            }
        }
        None
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            zoom_levels: DEFAULT_ZOOM_LEVELS.to_vec(),
            tile_extent: DEFAULT_TILE_EXTENT,
            trim_dist: DEFAULT_TRIM_DIST,
            activity_mask: vec![],
        }
    }
}

pub fn encode_line<T>(line: &LineString<T>) -> Result<Vec<u8>>
where
    T: CoordNum + AsPrimitive<u16>,
{
    let mut w = Vec::with_capacity(line.0.len() * 2);
    for pt in line.coords() {
        w.write_u16::<LittleEndian>(pt.x.as_())?;
        w.write_u16::<LittleEndian>(pt.y.as_())?;
    }
    Ok(w)
}

pub fn decode_line(bytes: &[u8]) -> Result<Vec<Coord<u32>>> {
    let mut coords = Vec::with_capacity(bytes.len() / (2 * 2));
    let mut reader = Cursor::new(bytes);
    while reader.position() < bytes.len() as u64 {
        let x = reader.read_u16::<LittleEndian>()? as u32;
        let y = reader.read_u16::<LittleEndian>()? as u32;
        coords.push(Coord { x, y });
    }
    Ok(coords)
}

#[derive(Serialize)]
pub struct PropertyStats {
    pub count: usize,
    pub types: Vec<String>,
}

#[derive(Default)]
pub struct ActivityFilter {
    before: Option<OffsetDateTime>,
    after: Option<OffsetDateTime>,
    props: Option<PropertyFilter>,
}

impl ActivityFilter {
    pub fn new(before: Option<Date>, after: Option<Date>, props: Option<PropertyFilter>) -> Self {
        Self {
            props,
            before: before.map(|date| date.midnight().assume_utc()),
            after: after.map(|date| date.midnight().assume_utc()),
        }
    }

    pub fn to_query<'a>(&'a self, params: &mut Vec<&'a dyn ToSql>) -> String {
        let mut clauses = String::from("true");

        if let Some(ref before) = self.before {
            clauses.push_str(" AND start_time < ?");
            params.push(before);
        }

        if let Some(ref after) = self.after {
            clauses.push_str(" AND start_time > ?");
            params.push(after);
        }

        if let Some(ref props) = self.props {
            let (prop_sql, prop_params) = props.to_sql();

            clauses.push_str(" AND ");
            clauses.push_str(&prop_sql);
            params.extend(prop_params);
        }

        clauses
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ActivityInfo {
    file_name: String,
    title: Option<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    start_time: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    created_at: Option<OffsetDateTime>,
    properties: serde_json::Map<String, serde_json::Value>,
    tile_count: usize,
}

impl Database {
    pub fn activity_count(&self, filter: &ActivityFilter) -> Result<usize, anyhow::Error> {
        let mut params = vec![];

        let count = self.connection()?.query_row(
            &format!(
                "SELECT count(*) FROM activities WHERE {};",
                filter.to_query(&mut params)
            ),
            &params[..],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    pub fn activity_info(
        &self,
        filter: &ActivityFilter,
    ) -> Result<Vec<ActivityInfo>, anyhow::Error> {
        let mut params = vec![];
        let conn = self.connection()?;
        let mut stmt = conn.prepare(&format!(
            "
               SELECT
                   a.file,
                   a.title,
                   a.start_time,
                   JSON(a.properties) as properties,
                   a.created_at,
                   COALESCE(
                      COUNT(DISTINCT format('%d/%d/%d', z, x, y)),
                      0
                   ) as cnt_tiles
                FROM activities a
                LEFT JOIN activity_tiles t ON (a.id = t.activity_id)
                WHERE {}
                GROUP BY 1, 2, 3, 4, 5
                ORDER BY COALESCE(a.created_at, a.start_time, a.id) ASC
            ",
            filter.to_query(&mut params)
        ))?;

        let mut info = vec![];
        let mut rows = stmt.query(&params[..])?;
        while let Some(row) = rows.next()? {
            info.push(ActivityInfo {
                file_name: row.get_unwrap(0),
                title: row.get_unwrap(1),
                start_time: row.get_unwrap(2),
                properties: serde_json::from_str(&row.get_unwrap::<_, String>(3))
                    .expect("activity `properties` should contain valid JSON"),
                created_at: row.get_unwrap(4),
                tile_count: row.get_unwrap(5),
            });
        }

        Ok(info)
    }

    pub fn count_properties(&self) -> Result<HashMap<String, PropertyStats>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(
            "SELECT
                p.key,
                COUNT(DISTINCT a.id),
                GROUP_CONCAT(DISTINCT
                    CASE p.type
                        WHEN 'text'    THEN 'string'
                        WHEN 'integer' THEN 'number'
                        WHEN 'real'    THEN 'number'
                        WHEN 'true'    THEN 'bool'
                        WHEN 'false'   THEN 'bool'
                    END
                )
            FROM activities a, json_each(a.properties) p
            -- exclude types that aren't currently useful for filtering
            WHERE p.type NOT IN ('null', 'object', 'array')
            GROUP BY 1;",
        )?;

        let mut properties = HashMap::new();

        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let count: usize = row.get_unwrap(1);
            let types_raw: String = row.get_unwrap(2);
            let mut types: Vec<_> = types_raw.split(',').map(String::from).collect();
            types.sort();
            properties.insert(row.get_unwrap(0), PropertyStats { count, types });
        }

        Ok(properties)
    }
}
