use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use geo_types::Coord;
use r2d2_sqlite::SqliteConnectionManager;

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

pub fn connect_database(path: &Path) -> r2d2::Pool<SqliteConnectionManager> {
    let manager = SqliteConnectionManager::file(path);
    let pool = r2d2::Pool::new(manager).expect("db pool");

    // TODO: should return metadata or something.
    pool.get()
        .and_then(|mut conn| {
            let _metadata = init_db(&mut conn).expect("init db");

            //  TODO: test performance
            conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = OFF")
                .expect("pragma");
            Ok(())
        })
        .expect("init db");

    pool
}

const MIGRATIONS: [&str; 2] = [
    "-- Initial schema
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);",
    "-- Add activities and activity_tiles
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
CREATE INDEX activity_tiles_zxy ON activity_tiles (z, x, y);
        ",
];

fn init_db(conn: &mut rusqlite::Connection) -> rusqlite::Result<HashMap<String, String>> {
    let metadata = load_metadata(conn);
    let cur_migration = metadata
        .get("version")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // If we're up to date, return.
    if cur_migration == MIGRATIONS.len() {
        return Ok(metadata);
    }

    println!(
        "Migrating database (have {} to apply)...",
        MIGRATIONS.len() - cur_migration
    );

    let tx = conn.transaction()?;
    for m in &MIGRATIONS[cur_migration..] {
        println!("  {}", m.lines().next().unwrap());
        tx.execute_batch(m)?;
    }
    tx.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('version', ?)",
        [MIGRATIONS.len()],
    )?;
    tx.commit()?;

    // Reload metadata after applying migrations.
    Ok(load_metadata(conn))
}

fn load_metadata(conn: &mut rusqlite::Connection) -> HashMap<String, String> {
    let mut meta: HashMap<String, String> = HashMap::new();

    // Would fail on first run before migrations are applied.
    let _ = conn.query_row("SELECT key, value FROM meta", [], |row| {
        let (k, v) = (row.get_unwrap(0), row.get_unwrap(1));
        meta.insert(k, v);
        Ok(())
    });

    meta
}
