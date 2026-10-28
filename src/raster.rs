use std::error::Error;
use std::fmt::Display;
use std::str::FromStr;

use anyhow::{Result, anyhow};
use geo_types::Coord;
use rayon::prelude::*;
use rusqlite::{ToSql, params};
use serde::{Deserialize, Deserializer};

use crate::db::{ActivityFilter, Config, Database, decode_line};
use crate::tile::{Tile, TileActivityMask, TileBounds, WebMercatorViewport};

pub static PINKISH: LinearGradient = LinearGradient::from_stops(&[
    (1, [0xff, 0xb1, 0xff, 0x7f]),
    (10, [0xff, 0xb1, 0xff, 0xff]),
    (50, [0xff, 0xff, 0xff, 0xff]),
]);

pub static BLUE_RED: LinearGradient = LinearGradient::from_stops(&[
    (1, [0x3f, 0x5e, 0xfb, 0xff]),
    (10, [0xfc, 0x46, 0x6b, 0xff]),
    (50, [0xff, 0xff, 0xff, 0xff]),
]);

pub static RED: LinearGradient = LinearGradient::from_stops(&[
    (1, [0xb2, 0x0a, 0x2c, 0xff]),
    (10, [0xff, 0xfb, 0xd5, 0xff]),
    (50, [0xff, 0xff, 0xff, 0xff]),
]);

pub static ORANGE: LinearGradient = LinearGradient::from_stops(&[
    (1, [0xfc, 0x4a, 0x1a, 0xff]),
    (10, [0xf7, 0xb7, 0x33, 0xff]),
    (50, [0xfd, 0xed, 0xce, 0xff]),
]);

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
        debug_assert!(width <= tile_extent, "Upscaling not supported");
        debug_assert!(width.is_power_of_two(), "width must be power of two");
        debug_assert!(source.z >= tile.z, "source zoom must be >= target zoom");

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

    fn add_activity(
        &mut self,
        source_tile: &Tile,
        coords: impl IntoIterator<Item = Coord<u32>>,
        mask: &TileActivityMask,
    ) {
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

            // Apply mask in tile pixel space
            if mask.is_hidden(x as i32, y as i32) {
                // Break the line
                prev = None;
                continue;
            }

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

    pub fn encode_png(&self, gradient: &LinearGradient) -> Vec<u8> {
        encode_indexed_png(&self.pixels, self.width, self.width, gradient)
    }
}

fn encode_indexed_png(
    pixels: &[u8],
    width: u32,
    height: u32,
    gradient: &LinearGradient,
) -> Vec<u8> {
    debug_assert_eq!(pixels.len(), (width * height) as usize);

    let (palette, trns) = gradient.as_png_palette();

    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, width, height);
        encoder.set_color(png::ColorType::Indexed);
        encoder.set_depth(png::BitDepth::Eight);
        encoder.set_palette(palette.as_slice());
        encoder.set_trns(trns.as_slice());
        encoder.set_compression(png::Compression::Fast);
        encoder.set_filter(png::Filter::NoFilter);

        let mut writer = encoder.write_header().expect("write png header");
        writer.write_image_data(pixels).expect("write png data");
    }

    bytes
}

/// Encode an image of the given size with no activity data, i.e. every pixel at
/// palette index 0.
pub fn encode_empty_png(width: u32, height: u32, gradient: &LinearGradient) -> Vec<u8> {
    encode_indexed_png(&vec![0; (width * height) as usize], width, height, gradient)
}

/// Linearly interpolate between two colors
const fn lerp(a: [u8; 4], b: [u8; 4], t: f32) -> [u8; 4] {
    [
        (a[0] as f32 * (1.0 - t) + b[0] as f32 * t) as u8,
        (a[1] as f32 * (1.0 - t) + b[1] as f32 * t) as u8,
        (a[2] as f32 * (1.0 - t) + b[2] as f32 * t) as u8,
        (a[3] as f32 * (1.0 - t) + b[3] as f32 * t) as u8,
    ]
}

#[derive(Clone, Debug)]
pub struct LinearGradient {
    rgb: [u8; 768],
    alpha: [u8; 256],
}

impl LinearGradient {
    pub const fn from_stops(stops: &[(u8, [u8; 4])]) -> Self {
        let mut gradient = LinearGradient {
            rgb: [0; 768],
            alpha: [0; 256],
        };

        let mut w = 0;
        while w + 1 < stops.len() {
            let (start_idx, start_color) = stops[w];
            let (end_idx, end_color) = stops[w + 1];

            if start_idx <= end_idx {
                let mut i = start_idx;
                loop {
                    let color = lerp(
                        start_color,
                        end_color,
                        (i - start_idx) as f32 / (end_idx - start_idx) as f32,
                    );
                    gradient.set(i, color);
                    if i == end_idx {
                        break;
                    }
                    i += 1;
                }
            }
            w += 1;
        }

        // Copy the last color to the end of the palette
        if !stops.is_empty() {
            let (last_idx, color) = stops[stops.len() - 1];
            let mut i = last_idx;
            loop {
                gradient.set(i, color);
                if i == u8::MAX {
                    break;
                }
                i += 1;
            }
        }

        gradient
    }

    #[inline]
    const fn set(&mut self, idx: u8, color: [u8; 4]) {
        let i = idx as usize;
        self.rgb[i * 3] = color[0];
        self.rgb[i * 3 + 1] = color[1];
        self.rgb[i * 3 + 2] = color[2];
        self.alpha[i] = color[3];
    }

    pub fn as_png_palette(&self) -> (&[u8; 768], &[u8; 256]) {
        (&self.rgb, &self.alpha)
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
        let stops: Vec<(u8, [u8; 4])> = s
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

                Ok((threshold, color.to_be_bytes()))
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

/// Render an arbitrary viewport composed of many subtiles.
pub fn render_view(
    viewport: WebMercatorViewport,
    gradient: &LinearGradient,
    out_width: u32,
    out_height: u32,
    filter: &ActivityFilter,
    db: &Database,
    config: &Config,
) -> Result<Vec<u8>> {
    let tile_size = config.tile_extent;
    let max_zoom = *config
        .zoom_levels
        .last()
        .expect("empty list of stored zoom levels");

    // Viewport corners in z0 pixels.
    let sw = viewport.sw.to_mercator_pixel(0, tile_size);
    let ne = viewport.ne.to_mercator_pixel(0, tile_size);

    // How much the z0 viewport would need to be scaled to fit in the requested
    // output image.
    let target_scale = f64::min(
        out_width as f64 / (ne.x - sw.x),
        out_height as f64 / (sw.y - ne.y),
    );

    // Pick the lowest zoom level which contains the full stored tile data at
    // full resolution for this image.
    let source_zoom = target_scale.log2().ceil().clamp(0.0, max_zoom as f64) as u8;

    // Source pixels per output pixel. Since the source data is chosen to always
    // be at least full resolution, this only ever downsamples.
    let zoom_scale = (1 << source_zoom) as f64;
    let out_scale = (zoom_scale / target_scale).max(1.0);

    let margin_width = out_width as f64 * out_scale;
    let margin_height = out_height as f64 * out_scale;

    // Region of source-zoom pixels to render, centered on the viewport.
    let margin_left = (((sw.x + ne.x) * zoom_scale - margin_width) / 2.0).round() as i64;
    let margin_top = (((sw.y + ne.y) * zoom_scale - margin_height) / 2.0).round() as i64;

    let max_tile = (1 << source_zoom) - 1;
    let tile_at = |px: i64| (px / tile_size as i64).clamp(0, max_tile) as u32;

    let margin_width = margin_width.round() as i64;
    let margin_height = margin_height.round() as i64;
    let bounds = TileBounds {
        z: source_zoom,
        xmin: tile_at(margin_left),
        ymin: tile_at(margin_top),
        xmax: tile_at(margin_left + margin_width - 1),
        ymax: tile_at(margin_top + margin_height - 1),
    };

    tracing::debug!(
        num_tiles = (bounds.xmax - bounds.xmin + 1) * (bounds.ymax - bounds.ymin + 1),
        source_zoom,
        "rendering subtiles"
    );

    let mut out_pixels = vec![0u8; (out_width * out_height) as usize];
    for (tile, raster) in rasterize_tiles(&bounds, tile_size, filter, db, config)? {
        let offset_x = (tile.x * tile_size) as i64 - margin_left;
        let offset_y = (tile.y * tile_size) as i64 - margin_top;

        draw_tile(
            &mut out_pixels,
            &raster,
            (out_width as i64, out_height as i64),
            (margin_width, margin_height),
            (offset_x, offset_y),
        );
    }

    Ok(encode_indexed_png(
        &out_pixels,
        out_width,
        out_height,
        gradient,
    ))
}

fn rasterize_tiles(
    tiles: &TileBounds,
    tile_size: u32,
    filter: &ActivityFilter,
    db: &Database,
    config: &Config,
) -> Result<Vec<(Tile, TileRaster)>> {
    let tiles: Vec<_> = (tiles.ymin..=tiles.ymax)
        .flat_map(|y| (tiles.xmin..=tiles.xmax).map(move |x| Tile::new(x, y, tiles.z)))
        .collect();

    tiles
        .par_iter()
        .filter_map(
            |&tile| match rasterize_tile(tile, tile_size, filter, db, config) {
                Ok(Some(raster)) => Some(Ok((tile, raster))),
                Ok(None) => None,
                Err(err) => Some(Err(err)),
            },
        )
        .collect()
}

/// Render `raster` into larger composite mosaic `dst_pixels`, scaling down the
/// output where necessary.
fn draw_tile(
    dst_pixels: &mut [u8],
    raster: &TileRaster,
    (dst_w, dst_h): (i64, i64),
    (src_w, src_h): (i64, i64),
    (offset_x, offset_y): (i64, i64),
) {
    let size = raster.width as i64;

    // Overlap of the tile with the source region, in source region pixels.
    let min_x = offset_x.max(0);
    let max_x = (offset_x + size).min(src_w);
    let min_y = offset_y.max(0);
    let max_y = (offset_y + size).min(src_h);

    if min_x >= max_x {
        return;
    }

    for y in min_y..max_y {
        let dst_start = (y * dst_h / src_h * dst_w) as usize;
        let src_start = ((y - offset_y) * size + (min_x - offset_x)) as usize;

        let dst_row = &mut dst_pixels[dst_start..dst_start + dst_w as usize];
        let src_row = &raster.pixels[src_start..src_start + (max_x - min_x) as usize];

        for (src_x, &px) in (min_x..max_x).zip(src_row) {
            let dst_x = (src_x * dst_w / src_w) as usize;
            dst_row[dst_x] = dst_row[dst_x].max(px);
        }
    }
}

pub fn rasterize_tile(
    tile: Tile,
    width: u32,
    filter: &ActivityFilter,
    db: &Database,
    config: &Config,
) -> Result<Option<TileRaster>> {
    let masks = &config.activity_mask;
    let zoom_level = config
        .source_level(tile.z)
        .ok_or_else(|| anyhow!("no source level for tile: {:?}", tile))?;

    let bounds = TileBounds::from(zoom_level, &tile);
    let mut raster = TileRaster::new(tile, bounds, width, config.tile_extent);

    let mask = tile.build_mask(masks, width as i32);

    let mut have_activity = false;

    let conn = db.connection()?;
    let (mut stmt, params) = prepare_activities_query(&conn, filter, &bounds)?;
    let mut rows = stmt.query(params.as_slice())?;
    while let Some(row) = rows.next()? {
        let source_tile = Tile::new(row.get_unwrap(0), row.get_unwrap(1), row.get_unwrap(2));

        let bytes = row
            .get_ref(3)?
            .as_bytes()
            .map_err(|_| anyhow!("expected blob for tile coordinates"))?;
        let coords = decode_line(bytes);

        raster.add_activity(&source_tile, coords, &mask);

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

    let (expr, join) = if filter.is_empty() {
        (String::from("true"), "")
    } else {
        (
            filter.to_query(&mut params),
            "JOIN activities ON activities.id = activity_tiles.activity_id ",
        )
    };

    let stmt = conn.prepare(&format!(
        "\
        SELECT x, y, z, coords \
        FROM activity_tiles \
        {join}\
        WHERE z = ? \
            AND (x >= ? AND x < ?) \
            AND (y >= ? AND y < ?) \
            AND {expr};",
    ))?;

    Ok((stmt, params))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn palette_color(gradient: &LinearGradient, idx: u8) -> [u8; 4] {
        let (palette, trns) = gradient.as_png_palette();
        let i = idx as usize;
        [
            palette[i * 3],
            palette[i * 3 + 1],
            palette[i * 3 + 2],
            trns[i],
        ]
    }

    #[test]
    fn test_linear_gradient_parse() {
        let gradient = "1:001122;10:789;100:334455;200:ffffff33"
            .parse::<LinearGradient>()
            .unwrap();
        assert_eq!(palette_color(&gradient, 0), [0x00, 0x00, 0x00, 0x00]);
        assert_eq!(palette_color(&gradient, 1), [0x00, 0x11, 0x22, 0xff]);
        assert_eq!(palette_color(&gradient, 10), [0x77, 0x88, 0x99, 0xff]);
        assert_eq!(palette_color(&gradient, 100), [0x33, 0x44, 0x55, 0xff]);
        // Last value should be copied to end
        assert_eq!(palette_color(&gradient, 255), [0xff, 0xff, 0xff, 0x33]);
    }

    #[test]
    fn test_linear_gradient_parse_out_of_order_stops() {
        let gradient = "50:ff0000;10:00ff00".parse::<LinearGradient>().unwrap();
        assert_eq!(palette_color(&gradient, 0), [0x00, 0x00, 0x00, 0x00]);
        assert_eq!(palette_color(&gradient, 9), [0x00, 0x00, 0x00, 0x00]);
        assert_eq!(palette_color(&gradient, 10), [0x00, 0xff, 0x00, 0xff]);
        assert_eq!(palette_color(&gradient, 50), [0x00, 0xff, 0x00, 0xff]);
        assert_eq!(palette_color(&gradient, 255), [0x00, 0xff, 0x00, 0xff]);
    }

    #[test]
    fn test_indexed_png_palette_round_trips_indices_and_colors() {
        let gradient = "1:001122;10:789;100:334455;200:ffffff33"
            .parse::<LinearGradient>()
            .unwrap();

        let pixels: Vec<u8> = (0..=255u8).collect();
        let encoded = encode_indexed_png(&pixels, 256, 1, &gradient);

        assert_eq!(
            &encoded[..8],
            &[0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a]
        );

        let decoder = png::Decoder::new(std::io::Cursor::new(&encoded));
        let mut reader = decoder.read_info().unwrap();
        let info = reader.info();
        assert_eq!(info.color_type, png::ColorType::Indexed);
        assert_eq!(info.bit_depth, png::BitDepth::Eight);

        let decoded_palette = info.palette.clone().unwrap().into_owned();
        let decoded_trns = info.trns.clone().unwrap().into_owned();

        let mut buf = vec![0; reader.output_buffer_size().unwrap()];
        let frame = reader.next_frame(&mut buf).unwrap();
        let indices = &buf[..frame.buffer_size()];

        assert_eq!(indices, pixels.as_slice());

        for (i, &idx) in indices.iter().enumerate() {
            let idx = idx as usize;
            assert_eq!(
                [
                    decoded_palette[idx * 3],
                    decoded_palette[idx * 3 + 1],
                    decoded_palette[idx * 3 + 2],
                    decoded_trns[idx]
                ],
                palette_color(&gradient, i as u8)
            );
        }
    }

    fn raster_4x4() -> TileRaster {
        let bounds = TileBounds {
            z: 0,
            xmin: 0,
            ymin: 0,
            xmax: 0,
            ymax: 0,
        };
        let mut raster = TileRaster::new(Tile::new(0, 0, 0), bounds, 4, 4);
        raster.pixels = (1..=16).collect();
        raster
    }

    #[test]
    fn test_draw_tile_full_resolution() {
        let raster = raster_4x4();
        let mut out = vec![0; 16];
        draw_tile(&mut out, &raster, (4, 4), (4, 4), (0, 0));
        // No dimension change => pixels copied directly
        assert_eq!(out, raster.pixels);
    }

    #[test]
    fn test_draw_tile_clips_to_region() {
        // 1   2   3   4
        // 5   6 [ 7   8 x ]
        // 9  10 [ 11 12 x ]
        // 13 14 [ 15 16 x ]
        let raster = raster_4x4();
        let mut out = vec![0; 9];
        draw_tile(&mut out, &raster, (3, 3), (3, 3), (-2, -1));
        #[rustfmt::skip]
        assert_eq!(out, [
            7, 8, 0,
            11, 12, 0,
            15, 16, 0,
        ]);
    }

    #[test]
    fn test_draw_tile_downsample_keeps_max() {
        let raster = raster_4x4();
        let mut out = vec![10, 0, 0, 20];
        draw_tile(&mut out, &raster, (2, 2), (4, 4), (0, 0));
        assert_eq!(out, [10, 8, 14, 20]);
    }

    #[test]
    fn test_unfiltered_query_matches_filtered_query_raster() {
        let db = Database::memory().unwrap();
        let config = db.load_config().unwrap();

        let tile = Tile::new(511, 340, 10);
        let source_zoom = config.source_level(tile.z).unwrap();
        let bounds = TileBounds::from(source_zoom, &tile);

        let points: [(u16, u16); 5] = [(10, 10), (900, 400), (1500, 1900), (300, 1700), (60, 25)];
        let mut coords = Vec::with_capacity(points.len() * 4);
        for (x, y) in points {
            coords.extend_from_slice(&x.to_le_bytes());
            coords.extend_from_slice(&y.to_le_bytes());
        }

        {
            let conn = db.connection().unwrap();
            conn.execute(
                "INSERT INTO activities (id, file, title, start_time, properties) \
                 VALUES (1, 'test.gpx', 'test', '2020-06-01T12:00:00Z', jsonb('{}'))",
                [],
            )
            .unwrap();

            conn.execute(
                "INSERT INTO activity_tiles (activity_id, z, x, y, coords) VALUES (1, ?, ?, ?, ?)",
                params![bounds.z, bounds.xmin, bounds.ymin, &coords],
            )
            .unwrap();
        }

        let unfiltered = ActivityFilter::default();
        assert!(unfiltered.is_empty());

        let matches_everything = ActivityFilter::new(
            None,
            time::Date::from_calendar_date(2000, time::Month::January, 1).ok(),
            None,
        );
        assert!(!matches_everything.is_empty());

        let without_join = rasterize_tile(tile, 256, &unfiltered, &db, &config)
            .unwrap()
            .expect("expected activity data");
        let with_join = rasterize_tile(tile, 256, &matches_everything, &db, &config)
            .unwrap()
            .expect("expected activity data");

        assert!(without_join.pixels.iter().any(|&p| p > 0));
        assert_eq!(without_join.pixels, with_join.pixels);
    }
}
