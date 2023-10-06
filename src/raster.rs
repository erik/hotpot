use anyhow::{anyhow, Result};
use geo_types::Coord;
use image::{Rgba, RgbaImage};
use once_cell::sync::Lazy;
use rusqlite::{params, ToSql};

use crate::db::{ActivityFilter, Database, decode_line};
use crate::DEFAULT_TILE_EXTENT;
use crate::tile::{Tile, TileBounds};

pub static DEFAULT_GRADIENT: Lazy<LinearGradient> = Lazy::new(|| {
    LinearGradient::from_stops(&[
        (0.0, [0xff, 0xb1, 0xff, 0x7f]),
        (0.05, [0xff, 0xb1, 0xff, 0xff]),
        (0.25, [0xff, 0xff, 0xff, 0xff]),
        (1.0, [0xff, 0xff, 0xff, 0xff]),
    ])
});

pub static BLUE_RED: Lazy<LinearGradient> = Lazy::new(|| {
    LinearGradient::from_stops(&[
        (0.0, [0x3f, 0x5e, 0xfb, 0xff]),
        (0.05, [0xfc, 0x46, 0x6b, 0xff]),
        (0.25, [0xff, 0xff, 0xff, 0xff]),
        (1.0, [0xff, 0xff, 0xff, 0xff]),
    ])
});

pub static RED: Lazy<LinearGradient> = Lazy::new(|| {
    LinearGradient::from_stops(&[
        (0.0, [0xb2, 0x0a, 0x2c, 0xff]),
        (0.05, [0xff, 0xfb, 0xd5, 0xff]),
        (0.25, [0xff, 0xff, 0xff, 0xff]),
        (1.0, [0xff, 0xff, 0xff, 0xff]),
    ])
});

pub static ORANGE: Lazy<LinearGradient> = Lazy::new(|| {
    LinearGradient::from_stops(&[
        (0.0, [0xfc, 0x4a, 0x1a, 0xff]),
        (0.25, [0xf7, 0xb7, 0x33, 0xff]),
        (1.0, [0xf7, 0xb7, 0x33, 0xff]),
    ])
});

pub struct LinearGradient {
    empty_value: Rgba<u8>,
    palette: [Rgba<u8>; 256],
}

struct TileRaster {
    bounds: TileBounds,
    scale: u32,
    width: u32,
    pixels: Vec<u8>,
}

impl TileRaster {
    fn new(tile: Tile, source: TileBounds, width: u32) -> Self {
        // TODO: support upscaling
        assert!(width <= DEFAULT_TILE_EXTENT, "Upscaling not supported");
        assert!(width.is_power_of_two(), "width must be power of two");
        assert!(source.z >= tile.z, "source zoom must be >= target zoom");

        let zoom_steps = (source.z - tile.z) as u32;
        let width_steps = DEFAULT_TILE_EXTENT.ilog2() - width.ilog2();

        Self {
            width,
            pixels: vec![0; (width * width) as usize],
            bounds: source,
            scale: zoom_steps + width_steps,
        }
    }

    fn add_activity(&mut self, source_tile: &Tile, coords: &[Coord<u32>]) {
        debug_assert_eq!(source_tile.z, self.bounds.z);

        // Origin of source tile within target tile
        let x_offset = DEFAULT_TILE_EXTENT * (source_tile.x - self.bounds.xmin);
        let y_offset = DEFAULT_TILE_EXTENT * (source_tile.y - self.bounds.ymin);

        let mut prev = None;
        for Coord { x, y } in coords {
            // Translate (x,y) to location in target tile.
            // [0..(width * STORED_TILE_WIDTH)]
            let x = x + x_offset;
            let y = (DEFAULT_TILE_EXTENT - y) + y_offset;

            // Scale the coordinates back down to [0..width]
            let x = x >> self.scale;
            let y = y >> self.scale;

            if let Some(Coord { x: px, y: py }) = prev {
                if x == px && y == py {
                    continue;
                }

                let line_iter = line_drawing::Bresenham::<i32>::new(
                    (px as i32, py as i32),
                    (x as i32, y as i32),
                );

                for (ix, iy) in line_iter {
                    if ix < 0 || iy < 0 || ix >= self.width as i32 || iy >= self.width as i32 {
                        continue;
                    }

                    let idx = (iy as u32 * self.width + ix as u32) as usize;
                    self.pixels[idx] = self.pixels[idx].saturating_add(1);
                }
            }
            prev = Some(Coord { x, y });
        }
    }

    fn apply_gradient(&self, gradient: &LinearGradient) -> RgbaImage {
        RgbaImage::from_fn(self.width, self.width, |x, y| {
            let idx = (y * self.width + x) as usize;
            gradient.sample(self.pixels[idx])
        })
    }
}

fn interpolate(a: Rgba<u8>, b: Rgba<u8>, t: f32) -> Rgba<u8> {
    Rgba::from([
        (a[0] as f32 * (1.0 - t) + b[0] as f32 * t) as u8,
        (a[1] as f32 * (1.0 - t) + b[1] as f32 * t) as u8,
        (a[2] as f32 * (1.0 - t) + b[2] as f32 * t) as u8,
        0xff,
    ])
}

impl LinearGradient {
    // TODO: clean this up
    pub fn from_stops<P>(stops: &[(f32, P)]) -> Self
    where
        P: Copy + Into<Rgba<u8>>,
    {
        let mut palette = [Rgba::from([0, 0, 0, 0]); 256];
        let mut i = 0;

        for stop in stops.windows(2) {
            let (a, b) = (&stop[0], &stop[1]);
            let width = (b.0 - a.0) * 256.0;

            let start_idx = (a.0 * 256.0).floor() as usize;
            let end_idx = (b.0 * 256.0).ceil() as usize;

            while i < end_idx {
                let t = (i - start_idx) as f32 / width;
                palette[i] = interpolate(a.1.into(), b.1.into(), t);

                i += 1;
            }
        }

        LinearGradient {
            palette,
            empty_value: Rgba::from([0, 0, 0, 0]),
        }
    }

    pub fn sample(&self, val: u8) -> Rgba<u8> {
        if val == 0 {
            return self.empty_value;
        }

        self.palette[val as usize]
    }
}

pub fn render_tile(
    tile: Tile,
    gradient: &LinearGradient,
    width: u32,
    filter: &ActivityFilter,
    db: &Database,
) -> Result<Option<RgbaImage>> {
    let zoom_level = db
        .meta
        .source_level(tile.z)
        .ok_or_else(|| anyhow!("no source level for tile: {:?}", tile))?;

    let bounds = TileBounds::from(zoom_level, &tile);
    let mut raster = TileRaster::new(tile, bounds, width);

    let mut have_activity = false;

    let conn = db.connection()?;
    let (mut stmt, params) = prepare_query_activities(&conn, filter, &bounds)?;
    let mut rows = stmt.query(params.as_slice())?;
    while let Some(row) = rows.next()? {
        let source_tile = Tile::new(row.get_unwrap(0), row.get_unwrap(1), row.get_unwrap(2));

        let bytes: Vec<u8> = row.get_unwrap(3);
        raster.add_activity(&source_tile, &decode_line(&bytes)?);

        have_activity = true;
    }

    if !have_activity {
        return Ok(None);
    }

    Ok(Some(raster.apply_gradient(gradient)))
}

fn prepare_query_activities<'a>(
    conn: &'a rusqlite::Connection,
    filter: &'a ActivityFilter,
    bounds: &'a TileBounds,
) -> Result<(rusqlite::Statement<'a>, Vec<&'a dyn ToSql>)> {
    let mut params = params![bounds.z, bounds.xmin, bounds.xmax, bounds.ymin, bounds.ymax].to_vec();
    let filter_clause = filter.to_query(&mut params);

    // TODO: don't always need to join
    let stmt = conn.prepare(
        &format!("SELECT x, y, z, coords \
                      FROM activity_tiles \
                      JOIN activities ON activities.id = activity_tiles.activity_id \
                      WHERE z = ? \
                          AND (x >= ? AND x < ?) \
                          AND (y >= ? AND y < ?) \
                          AND {};",
                 filter_clause,
        )
    )?;

    Ok((stmt, params))
}

