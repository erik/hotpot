use std::io::Cursor;
use std::path::Path;

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use geo_types::Coord;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::{ToSqlOutput, Value};
use rusqlite::{params, ToSql};
use time::format_description::well_known::Iso8601;
use time::{Date, OffsetDateTime};

use crate::{DEFAULT_TILE_EXTENT, DEFAULT_TRIM_DIST, DEFAULT_ZOOM_LEVELS};

const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS metadata (
      key   TEXT NOT NULL PRIMARY KEY
    , value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS activities (
      id            INTEGER PRIMARY KEY
    , file          TEXT    NOT NULL
    , title         TEXT
    , start_time    INTEGER
    , duration_secs INTEGER
    , dist_meters   REAL

    -- TODO: other metadata for filtering?
    -- , kind     TEXT -- run, bike, etc
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

pub struct Database {
    pool: r2d2::Pool<SqliteConnectionManager>,
    pub meta: Metadata,
}

impl Database {
    pub fn new(path: &Path) -> Result<Self> {
        let manager = SqliteConnectionManager::file(path);
        let pool = r2d2::Pool::new(manager)?;
        let mut conn = pool.get()?;

        let pragmas = [("journal_mode", "WAL"), ("synchronous", "OFF")];
        for (k, v) in &pragmas {
            conn.pragma_update(None, k, v)?;
        }

        apply_schema(&mut conn)?;

        let meta = read_metadata(&mut conn)?;
        set_metadata(&mut conn, &meta)?;

        Ok(Database { pool, meta })
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

        let num_activities = conn.execute("DELETE FROM activities", [])?;
        let num_tiles = conn.execute("DELETE FROM activity_tiles", [])?;
        conn.execute_batch("VACUUM")?;

        tracing::info!(num_activities, num_tiles, "Reset database",);

        Ok(())
    }

    pub fn connection(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        self.pool.get().map_err(Into::into)
    }

    pub fn shared_pool(&self) -> r2d2::Pool<SqliteConnectionManager> {
        self.pool.clone()
    }
}

// NOTE: we can use PRAGMA.user_version to track schema versions
// https://www.sqlite.org/pragma.html#pragma_user_version
fn apply_schema(conn: &mut rusqlite::Connection) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute_batch(SCHEMA)?;
    tx.commit()?;

    Ok(())
}

pub struct Metadata {
    /// Zoom levels that we store activity tiles for.
    pub zoom_levels: Vec<u8>,
    /// Width of the stored tiles, in pixels.
    pub tile_extent: u32,
    /// Distance to trim start/end of activities, in meters.
    pub trim_dist: f64,
}

impl Metadata {
    pub fn source_level(&self, target_zoom: u8) -> Option<u8> {
        for z in &self.zoom_levels {
            if *z >= target_zoom {
                return Some(*z);
            }
        }
        None
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Metadata {
            zoom_levels: DEFAULT_ZOOM_LEVELS.to_vec(),
            tile_extent: DEFAULT_TILE_EXTENT,
            trim_dist: DEFAULT_TRIM_DIST,
        }
    }
}

fn read_metadata(conn: &mut rusqlite::Connection) -> Result<Metadata> {
    let mut meta = Metadata::default();

    let mut stmt = conn.prepare("SELECT key, value FROM metadata")?;
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let key: String = row.get_unwrap(0);
        let value: String = row.get_unwrap(1);

        match key.as_str() {
            "zoom_levels" => {
                meta.zoom_levels = value
                    .split(',')
                    .map(|s| s.parse::<u8>().expect("zoom level"))
                    .collect();
            }
            "tile_extent" => meta.tile_extent = value.parse()?,
            "trim_dist" => meta.trim_dist = value.parse()?,
            key => tracing::warn!("Ignoring unknown metadata key: {}", key),
        }
    }

    Ok(meta)
}

fn set_metadata(conn: &mut rusqlite::Connection, meta: &Metadata) -> Result<()> {
    let zoom_levels = meta
        .zoom_levels
        .iter()
        .map(u8::to_string)
        .collect::<Vec<_>>()
        .join(",");

    conn.execute(
        "\
        INSERT OR REPLACE INTO metadata (key, value) \
        VALUES (?, ?) \
             , (?, ?) \
             , (?, ?)",
        params![
            "zoom_levels",
            &zoom_levels,
            "tile_extent",
            &meta.tile_extent,
            "trim_dist",
            &meta.trim_dist,
        ],
    )?;

    Ok(())
}

// TODO: consider piping this through a compression step.
pub fn encode_line(data: &[Coord<u16>]) -> Result<Vec<u8>> {
    let mut w = Vec::with_capacity(data.len() * 2);
    for pt in data {
        w.write_u16::<LittleEndian>(pt.x)?;
        w.write_u16::<LittleEndian>(pt.y)?;
    }
    Ok(w)
}

pub fn decode_line(bytes: &[u8]) -> Result<Vec<Coord<u32>>> {
    let mut coords = Vec::with_capacity(bytes.len() / 4);
    let mut reader = Cursor::new(bytes);
    while reader.position() < bytes.len() as u64 {
        let x = reader.read_u16::<LittleEndian>()?;
        let y = reader.read_u16::<LittleEndian>()?;
        coords.push(Coord {
            x: x as u32,
            y: y as u32,
        });
    }
    Ok(coords)
}

pub struct SqlDate(Date);

#[derive(Clone, Debug)]
pub struct SqlDateTime(pub OffsetDateTime);

impl ToSql for SqlDate {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0
            .format(&Iso8601::DATE)
            // TODO: InvalidParameterName is not the right error type
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))
            .map(|s| ToSqlOutput::Owned(Value::Text(s)))
    }
}

impl ToSql for SqlDateTime {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0
            .format(&Iso8601::DATE_TIME)
            // TODO: InvalidParameterName is not the right error type
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))
            .map(|s| ToSqlOutput::Owned(Value::Text(s)))
    }
}

#[derive(Default)]
pub struct ActivityFilter {
    before: Option<SqlDate>,
    after: Option<SqlDate>,
}

impl ActivityFilter {
    pub fn new(before: Option<Date>, after: Option<Date>) -> Self {
        Self {
            before: before.map(SqlDate),
            after: after.map(SqlDate),
        }
    }
    pub fn to_query<'a>(&'a self, params: &mut Vec<&'a dyn ToSql>) -> String {
        let mut clauses = vec![];

        if let Some(ref before) = self.before {
            clauses.push("start_time < ?");
            params.push(before);
        }

        if let Some(ref after) = self.after {
            clauses.push("start_time > ?");
            params.push(after);
        }

        if clauses.is_empty() {
            return String::from("true");
        }

        clauses.join(" AND ")
    }
}
