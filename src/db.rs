use std::io::Cursor;
use std::path::Path;

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use geo_types::Coord;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, ToSql};
use time::{Date, OffsetDateTime};

use crate::{DEFAULT_TILE_EXTENT, DEFAULT_TRIM_DIST, DEFAULT_ZOOM_LEVELS};

const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS config (
      key   TEXT NOT NULL PRIMARY KEY
    , value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS activities (
      id            INTEGER PRIMARY KEY
    , file          TEXT     NOT NULL
    , title         TEXT
    , start_time    INTEGER
    , properties    TEXT    NOT NULL DEFAULT '{}'
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
    pub config: Config,
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

        let cfg = Config::load(&mut conn)?;
        cfg.save(&mut conn)?;

        Ok(Database { pool, config: cfg })
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

        tracing::info!(num_activities, num_tiles, "Reset database");

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

pub struct Config {
    /// Zoom levels that we store activity tiles for.
    pub zoom_levels: Vec<u8>,
    /// Width of the stored tiles, in pixels.
    pub tile_extent: u32,
    /// Distance to trim start/end of activities, in meters.
    pub trim_dist: f64,
}

impl Config {
    fn load(conn: &mut rusqlite::Connection) -> Result<Self> {
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
                key => tracing::warn!("Ignoring unknown config key: {}", key),
            }
        }

        Ok(cfg)
    }

    fn save(&self, conn: &mut rusqlite::Connection) -> Result<()> {
        let zoom_levels = serde_json::to_string(&self.zoom_levels)?;

        let mut stmt = conn.prepare(
            "\
            INSERT OR REPLACE INTO config (key, value) \
            VALUES (?, ?)",
        )?;
        stmt.execute(params!["zoom_levels", &zoom_levels])?;
        stmt.execute(params!["tile_extent", &self.tile_extent])?;
        stmt.execute(params!["trim_dist", &self.trim_dist])?;

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
        }
    }
}

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

#[derive(Default)]
pub struct ActivityFilter {
    before: Option<OffsetDateTime>,
    after: Option<OffsetDateTime>,
}

impl ActivityFilter {
    pub fn new(before: Option<Date>, after: Option<Date>) -> Self {
        Self {
            before: before.map(|date| date.midnight().assume_utc()),
            after: after.map(|date| date.midnight().assume_utc()),
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
