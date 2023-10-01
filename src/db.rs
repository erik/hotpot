use std::io::Cursor;
use std::path::Path;

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use geo_types::Coord;
use r2d2_sqlite::SqliteConnectionManager;

use crate::{STORED_TILE_WIDTH, STORED_ZOOM_LEVELS};

const MIGRATIONS: [&str; 2] = [
    "-- Create migrations table
CREATE TABLE IF NOT EXISTS migrations (
    id         INTEGER PRIMARY KEY,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);",
    "-- Initial schema
CREATE TABLE metadata (
      key   TEXT NOT NULL
    , value TEXT NOT NULL
);

CREATE TABLE activities (
      id            INTEGER PRIMARY KEY
    -- TODO: maybe do a hash of contents?
    , file          TEXT NOT NULL
    , title         TEXT
    , start_time    INTEGER
    , duration_secs INTEGER
    , dist_meters   REAL NOT NULL

    -- TODO:
    -- , kind     TEXT -- run, bike, etc
    -- , polyline TEXT
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
CREATE INDEX activity_tiles_zxy ON activity_tiles (z, x, y);",
];

pub struct Database {
    pool: r2d2::Pool<SqliteConnectionManager>,
    pub meta: Metadata,
}

impl Database {
    pub fn delete(path: &Path) -> Result<()> {
        let db_files = [path, &path.join("-wal"), &path.join("-shm")];

        for p in &db_files {
            match std::fs::remove_file(p) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => panic!("error removing db: {}", e),
            }
        }

        Ok(())
    }

    pub fn new(path: &Path) -> Result<Self> {
        let manager = SqliteConnectionManager::file(path);
        let pool = r2d2::Pool::new(manager)?;
        let mut conn = pool.get()?;

        let pragmas = [("journal_mode", "WAL"), ("synchronous", "OFF")];
        for (k, v) in &pragmas {
            conn.pragma_update(None, k, v)?;
        }

        apply_migrations(&mut conn)?;
        let metadata = load_metadata(&mut conn)?;

        Ok(Database {
            pool,
            meta: metadata,
        })
    }

    pub fn connection(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        self.pool.get().map_err(Into::into)
    }

    pub fn shared_pool(&self) -> r2d2::Pool<SqliteConnectionManager> {
        self.pool.clone()
    }
}

fn apply_migrations(conn: &mut rusqlite::Connection) -> Result<()> {
    let cur_migration: usize = conn
        .query_row("SELECT max(id) FROM migrations", [], |row| row.get(0))
        .unwrap_or(0);

    if cur_migration == MIGRATIONS.len() {
        return Ok(());
    }

    let tx = conn.transaction()?;
    for (i, m) in MIGRATIONS[cur_migration..].iter().enumerate() {
        tx.execute_batch(m)?;
        tx.execute(
            "INSERT INTO migrations (id) VALUES (?)",
            [cur_migration + i + 1],
        )?;
    }
    tx.commit()?;

    Ok(())
}

pub struct Metadata {
    pub zoom_levels: Vec<u8>,
    pub stored_width: u32,
}

impl Default for Metadata {
    fn default() -> Self {
        Metadata {
            zoom_levels: STORED_ZOOM_LEVELS.to_vec(),
            stored_width: STORED_TILE_WIDTH,
        }
    }
}

fn load_metadata(conn: &mut rusqlite::Connection) -> Result<Metadata> {
    let mut meta = Metadata::default();

    let mut stmt = conn.prepare("SELECT key, value FROM metadata")?;
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let key: String = row.get(0)?;
        match key.as_str() {
            "zoom_levels" => {
                meta.zoom_levels = row
                    .get_unwrap::<_, String>(1)
                    .split(',')
                    .map(|s| s.parse::<u8>().expect("zoom level"))
                    .collect();
            }

            "stored_width" => {
                meta.stored_width = row.get_unwrap(1);
            }

            _ => {}
        }
    }

    Ok(meta)
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
