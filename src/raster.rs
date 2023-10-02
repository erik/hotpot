use crate::tile::{Tile, TileBounds};
use crate::STORED_TILE_WIDTH;
use geo_types::Coord;
use image::{Rgba, RgbaImage};

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
        assert!(width <= STORED_TILE_WIDTH, "Upscaling not supported");
        assert!(width.is_power_of_two(), "width must be power of two");
        assert!(source.z >= tile.z, "source zoom must be >= target zoom");

        let zoom_steps = (source.z - tile.z) as u32;
        let width_steps = STORED_TILE_WIDTH.ilog2() - width.ilog2();

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
        let x_offset = STORED_TILE_WIDTH * (source_tile.x - self.bounds.xmin);
        let y_offset = STORED_TILE_WIDTH * (source_tile.y - self.bounds.ymin);

        let mut prev = None;
        for Coord { x, y } in coords {
            // Translate (x,y) to location in target tile.
            // [0..(width * STORED_TILE_WIDTH)]
            let x = x + x_offset;
            let y = (STORED_TILE_WIDTH - y) + y_offset;

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
                    let ix = ix.clamp(0, self.width as i32 - 1) as u32;
                    let iy = iy.clamp(0, self.width as i32 - 1) as u32;

                    let idx = (iy * self.width + ix) as usize;
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
