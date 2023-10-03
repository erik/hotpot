use geo_types::Coord;
use image::{Rgba, RgbaImage};
use once_cell::sync::Lazy;

use crate::tile::{Tile, TileBounds};
use crate::DEFAULT_TILE_EXTENT;

// Redish to whiteish
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

pub struct TileRaster {
    bounds: TileBounds,
    scale: u32,
    width: u32,
    pixels: Vec<u8>,
}

impl TileRaster {
    pub fn new(tile: Tile, source: TileBounds, width: u32) -> Self {
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

    pub fn add_activity(&mut self, source_tile: &Tile, coords: &[Coord<u32>]) {
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

                // TODO: is the perf hit of this worth it?
                let line_iter = line_drawing::Bresenham::<i32>::new(
                    (px as i32, py as i32),
                    (x as i32, y as i32),
                );

                for (ix, iy) in line_iter {
                    // TODO: exclude rather than clamp?
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

    pub fn apply_gradient(&self, gradient: &LinearGradient) -> RgbaImage {
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
