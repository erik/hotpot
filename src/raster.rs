use std::error::Error;
use std::fmt::Display;
use std::ops::RangeInclusive;
use std::str::FromStr;

use anyhow::{Result, anyhow};
use geo_types::Coord;
use image::{Rgba, RgbaImage};
use once_cell::sync::Lazy;
use rayon::prelude::*;
use rusqlite::{ToSql, params};
use serde::{Deserialize, Deserializer};

use crate::WebMercatorViewport;
use crate::db::{ActivityFilter, Database, decode_line};
use crate::tile::{Tile, TileBounds};

pub static PINKISH: Lazy<LinearGradient> = Lazy::new(|| {
    LinearGradient::from_stops(&[
        (1, [0xff, 0xb1, 0xff, 0x7f]),
        (10, [0xff, 0xb1, 0xff, 0xff]),
        (50, [0xff, 0xff, 0xff, 0xff]),
    ])
});

pub static BLUE_RED: Lazy<LinearGradient> = Lazy::new(|| {
    LinearGradient::from_stops(&[
        (1, [0x3f, 0x5e, 0xfb, 0xff]),
        (10, [0xfc, 0x46, 0x6b, 0xff]),
        (50, [0xff, 0xff, 0xff, 0xff]),
    ])
});

pub static RED: Lazy<LinearGradient> = Lazy::new(|| {
    LinearGradient::from_stops(&[
        (1, [0xb2, 0x0a, 0x2c, 0xff]),
        (10, [0xff, 0xfb, 0xd5, 0xff]),
        (50, [0xff, 0xff, 0xff, 0xff]),
    ])
});

pub static ORANGE: Lazy<LinearGradient> = Lazy::new(|| {
    LinearGradient::from_stops(&[
        (1, [0xfc, 0x4a, 0x1a, 0xff]),
        (10, [0xf7, 0xb7, 0x33, 0xff]),
    ])
});

pub struct TileRaster {
    bounds: TileBounds,
    scale: u32,
    width: u32,
    tile_extent: u32,
    pixels: Vec<u8>,
}

impl TileRaster {
    fn new(tile: Tile, source: TileBounds, width: u32, tile_extent: u32) -> Self {
        // TODO: support upscaling
        assert!(width <= tile_extent, "Upscaling not supported");
        assert!(width.is_power_of_two(), "width must be power of two");
        assert!(source.z >= tile.z, "source zoom must be >= target zoom");

        let zoom_steps = (source.z - tile.z) as u32;
        let width_steps = tile_extent.ilog2() - width.ilog2();

        Self {
            width,
            tile_extent,
            pixels: vec![0; (width * width) as usize],
            bounds: source,
            scale: zoom_steps + width_steps,
        }
    }

    fn add_activity(&mut self, source_tile: &Tile, coords: &[Coord<u32>]) {
        debug_assert_eq!(source_tile.z, self.bounds.z);

        // Origin of source tile within target tile
        let x_offset = self.tile_extent * (source_tile.x - self.bounds.xmin);
        let y_offset = self.tile_extent * (source_tile.y - self.bounds.ymin);

        let tile_bbox = crate::tile::BBox::square(self.width as f64 - 1.0);

        let mut prev = None;
        for Coord { x, y } in coords {
            // Translate (x,y) to location in target tile.
            // [0..(width * STORED_TILE_WIDTH)]
            let x = x + x_offset;
            let y = (self.tile_extent - y) + y_offset;

            // Scale the coordinates back down to [0..width]
            let x = x >> self.scale;
            let y = y >> self.scale;

            let Some(Coord { x: px, y: py }) = prev else {
                prev = Some(Coord { x, y });
                continue;
            };

            if x == px && y == py {
                continue;
            }

            // Pre-clamp the coordinates to the target tile bounds so we can
            // avoid a bounds check in the loop
            let Some((start, end)) = tile_bbox.clip_line(
                &geo::Point::new(px as f64, py as f64).into(),
                &geo::Point::new(x as f64, y as f64).into(),
            ) else {
                continue;
            };

            let line_iter = line_drawing::Bresenham::<i32>::new(
                (start.0.x() as i32, start.0.y() as i32),
                (end.0.x() as i32, end.0.y() as i32),
            );

            for (ix, iy) in line_iter {
                let idx = (iy as u32 * self.width + ix as u32) as usize;
                self.pixels[idx] = self.pixels[idx].saturating_add(1);
            }
            prev = Some(Coord { x, y });
        }
    }

    fn enumerate_pixels(&self) -> EnumerateRasterPixels<'_> {
        EnumerateRasterPixels {
            width: self.width as usize,
            idx: 0,
            pixels: self.pixels.as_ref(),
        }
    }

    pub fn apply_gradient(&self, gradient: &LinearGradient) -> RgbaImage {
        RgbaImage::from_fn(self.width, self.width, |x, y| {
            let idx = (y * self.width + x) as usize;
            gradient.sample(self.pixels[idx])
        })
    }
}

/// Linearly interpolate between two colors
fn lerp(a: Rgba<u8>, b: Rgba<u8>, t: f32) -> Rgba<u8> {
    Rgba::from([
        (a[0] as f32 * (1.0 - t) + b[0] as f32 * t) as u8,
        (a[1] as f32 * (1.0 - t) + b[1] as f32 * t) as u8,
        (a[2] as f32 * (1.0 - t) + b[2] as f32 * t) as u8,
        (a[3] as f32 * (1.0 - t) + b[3] as f32 * t) as u8,
    ])
}

struct EnumerateRasterPixels<'a> {
    width: usize,
    idx: usize,
    pixels: &'a [u8],
}

impl Iterator for EnumerateRasterPixels<'_> {
    type Item = (usize, usize, u8);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= (self.width * self.width) {
            None
        } else {
            let pixel = self.pixels[self.idx];
            let x = self.idx % self.width;
            let y = self.idx / self.width;
            self.idx += 1;
            Some((x, y, pixel))
        }
    }
}

#[derive(Clone, Debug)]
pub struct LinearGradient([Rgba<u8>; 256]);

impl LinearGradient {
    pub fn from_stops<P>(stops: &[(u8, P)]) -> Self
    where
        P: Copy + Into<Rgba<u8>>,
    {
        let mut palette = [Rgba::from([0, 0, 0, 0]); 256];

        for window in stops.windows(2) {
            let (start_idx, start_color) = window[0];
            let (end_idx, end_color) = window[1];

            for i in start_idx..=end_idx {
                palette[i as usize] = lerp(
                    start_color.into(),
                    end_color.into(),
                    (i - start_idx) as f32 / (end_idx - start_idx) as f32,
                );
            }
        }

        // Copy the last color to the end of the palette
        if let Some(&(last_idx, color)) = stops.last() {
            for p in palette.iter_mut().skip(last_idx as usize) {
                *p = color.into();
            }
        }

        LinearGradient(palette)
    }

    pub fn sample(&self, val: u8) -> Rgba<u8> {
        self.0[val as usize]
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct LinearGradientParseError;
impl Display for LinearGradientParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("invalid linear gradient"))
    }
}
impl Error for LinearGradientParseError {}

/*
TODO: support varying stops per-zoom level. Possible format:

   {
       "palette": ["789", "334455", "ffffff33"],
       "stops": [
           [0,  [75, 175, 250]],
           [10, [25, 50, 75]],
           [15, [5, 10, 15]]
       ]
   }
*/
impl FromStr for LinearGradient {
    type Err = LinearGradientParseError;

    /// Parse a string containing a list of stop points and colors, separated by
    /// a `;`.
    ///
    /// Colors may be written as `RGB`, `RRGGBB`, or `RRGGBBAA`
    ///
    /// For example: `0:001122;25:789;50:334455;75:ffffff33`
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let stops: Vec<(u8, Rgba<u8>)> = s
            .split(';')
            .map(|part| {
                let (threshold, color) = part.split_once(':').ok_or(LinearGradientParseError)?;
                let threshold = threshold
                    .parse::<u8>()
                    .map_err(|_| LinearGradientParseError)?;
                let color = {
                    let rgba = match color.len() {
                        3 => {
                            let rgb: String = color.chars().flat_map(|ch| [ch, ch]).collect();
                            format!("{}FF", rgb)
                        }
                        6 => format!("{color}FF"),
                        8 => color.to_string(),
                        _ => return Err(LinearGradientParseError),
                    };

                    u32::from_str_radix(&rgba, 16).map_err(|_| LinearGradientParseError)?
                };

                Ok((threshold, Rgba::from(color.to_be_bytes())))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(LinearGradient::from_stops(&stops))
    }
}

impl<'de> Deserialize<'de> for LinearGradient {
    fn deserialize<D>(deserializer: D) -> Result<LinearGradient, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        LinearGradient::from_str(&s).map_err(|_| serde::de::Error::custom("invalid gradient"))
    }
}

pub fn render_view(
    viewport: WebMercatorViewport,
    gradient: &LinearGradient,
    width: u32,
    height: u32,
    filter: &ActivityFilter,
    db: &Database,
) -> Result<RgbaImage> {
    let tile_size = 256;
    let zoom_range = RangeInclusive::new(
        *db.config.zoom_levels.iter().min().unwrap() as u32,
        *db.config.zoom_levels.iter().max().unwrap() as u32,
    );

    let tile_bounds = TileBounds::from_viewport(&viewport, width, height, zoom_range);

    let num_x = tile_bounds.xmax - tile_bounds.xmin + 1;
    let num_y = tile_bounds.ymax - tile_bounds.ymin + 1;

    let (src_w, src_h) = (num_x * tile_size, num_y * tile_size);
    let (img_w, img_h) = (u32::min(width, src_w), u32::min(height, src_h));

    if img_w < width || img_h < height {
        println!(
            "[WARN] source data is not high resolution for requested image dimensions, clamping to {}x{}.",
            img_w, img_h
        );
    }

    println!(
        "Rendering {} subtiles at zoom={}...",
        num_x * num_y,
        tile_bounds.z
    );

    let mut mosaic = RgbaImage::new(img_w, img_h);

    // The tile bounds will be aligned to the tile grid, so we need to trim
    // the excess pixels from the edges of the image.
    let margin_x = (src_w - img_w) / 2;
    let margin_y = (src_h - img_h) / 2;

    // Collect all tile positions for parallel processing
    let tile_positions: Vec<_> = (0..num_y)
        .flat_map(|row| (0..num_x).map(move |col| (row, col)))
        .collect();

    // Render tiles in parallel
    let tile_results: Vec<_> = tile_positions
        .par_iter()
        .map(|(row, col)| {
            let tile = Tile::new(
                tile_bounds.xmin + col,
                tile_bounds.ymin + row,
                tile_bounds.z,
            );

            // Position of the tile in the mosaic
            let tile_origin_y = row * tile_size;
            let tile_origin_x = col * tile_size;

            rasterize_tile(tile, tile_size, filter, db)
                .map(|img| img.map(|img| (tile_origin_x, tile_origin_y, img)))
        })
        .collect();

    for result in tile_results {
        if let Some((tile_origin_x, tile_origin_y, raster)) = result? {
            for (x, y, pixel) in raster.enumerate_pixels() {
                let x = tile_origin_x + x as u32;
                let y = tile_origin_y + y as u32;

                // Ignore pixels which fall into the margins
                if x >= margin_x && x < margin_x + img_w && y >= margin_y && y < margin_y + img_h {
                    mosaic.put_pixel(x - margin_x, y - margin_y, gradient.sample(pixel));
                }
            }
        }
    }

    Ok(mosaic)
}

pub fn rasterize_tile(
    tile: Tile,
    width: u32,
    filter: &ActivityFilter,
    db: &Database,
) -> Result<Option<TileRaster>> {
    let zoom_level = db
        .config
        .source_level(tile.z)
        .ok_or_else(|| anyhow!("no source level for tile: {:?}", tile))?;

    let bounds = TileBounds::from(zoom_level, &tile);
    let mut raster = TileRaster::new(tile, bounds, width, db.config.tile_extent);

    let mut have_activity = false;

    let conn = db.connection()?;
    let (mut stmt, params) = prepare_activities_query(&conn, filter, &bounds)?;
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

    Ok(Some(raster))
}

fn prepare_activities_query<'a>(
    conn: &'a rusqlite::Connection,
    filter: &'a ActivityFilter,
    bounds: &'a TileBounds,
) -> Result<(rusqlite::Statement<'a>, Vec<&'a dyn ToSql>)> {
    let mut params = params![bounds.z, bounds.xmin, bounds.xmax, bounds.ymin, bounds.ymax].to_vec();
    let filter_clause = filter.to_query(&mut params);

    let stmt = conn.prepare(&format!(
        "\
        SELECT x, y, z, coords \
        FROM activity_tiles \
        JOIN activities ON activities.id = activity_tiles.activity_id \
        WHERE z = ? \
            AND (x >= ? AND x < ?) \
            AND (y >= ? AND y < ?) \
            AND {};",
        filter_clause,
    ))?;

    Ok((stmt, params))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_gradient_parse() {
        let gradient = "1:001122;10:789;100:334455;200:ffffff33"
            .parse::<LinearGradient>()
            .unwrap();
        assert_eq!(gradient.0[0], Rgba::from([0x00, 0x00, 0x00, 0x00]));
        assert_eq!(gradient.0[1], Rgba::from([0x00, 0x11, 0x22, 0xff]));
        assert_eq!(gradient.0[10], Rgba::from([0x77, 0x88, 0x99, 0xff]));
        assert_eq!(gradient.0[100], Rgba::from([0x33, 0x44, 0x55, 0xff]));
        // Last value should be copied to end
        assert_eq!(gradient.0[255], Rgba::from([0xff, 0xff, 0xff, 0x33]));
    }
}
