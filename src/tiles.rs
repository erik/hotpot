use std::f32::consts::PI;
use std::ops::Range;

use derive_more::{From, Into};
use geo_types::{Coord, Point};

const TILE_EXTENT: f32 = 4096.0;
const EARTH_RADIUS_METERS: f32 = 6_378_137.0;
const EARTH_CIRCUMFERENCE: f32 = 2.0 * PI * EARTH_RADIUS_METERS;
const ORIGIN_OFFSET: f32 = EARTH_CIRCUMFERENCE / 2.0;


#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
pub struct Tile {
    x: u32,
    y: u32,
    z: u8,
}

#[derive(Copy, Clone, PartialEq, Debug, From, Into)]
pub struct LngLat(pub Point<f32>);

#[derive(Copy, Clone, PartialEq, Debug, From, Into)]
pub struct WebMercator(pub Point<f32>);

#[derive(Copy, Clone, PartialEq, Debug, From, Into)]
pub struct MercatorPixel {
    pub pixel: Coord<u32>,
    pub tile: Tile,
}

struct TileClipper {}

impl WebMercator {
    // todo: untested
    pub fn pixel_xy(&self, tile_size: u32, zoom: u8) -> MercatorPixel {
        let num_tiles = 1u32 << zoom;
        let scale = EARTH_CIRCUMFERENCE / (tile_size * num_tiles) as f32;

        let max_pixels = tile_size * num_tiles;
        let center = max_pixels / 2;

        let pixel = Coord {
            x: (self.0.x() / scale).round() as u32 ,
            y: (self.0.y() / scale).round() as u32 ,
        };

        let tile = Tile::new(
            pixel.x / tile_size,
            pixel.y / tile_size,
            zoom,
        );

        MercatorPixel { pixel, tile }
    }

    // todo: untested
    pub fn tile(&self, zoom: u8) -> Tile {
        let num_tiles = (1u32 << zoom) as f32;
        let scale = num_tiles / EARTH_CIRCUMFERENCE;

        let x = (scale * self.0.x()).floor() as u32;
        let y = (scale * self.0.y()).floor() as u32;

        Tile::new(x, y, zoom)
    }
}

impl LngLat {
    const LAT_BOUNDS: Range<f32> = -89.99999..90.0;

    pub fn new(mut x: f32, y: f32) -> LngLat {
        while x < -180.0 {
            x += 360.0;
        }

        Self(Point::new(x, y))
    }

    pub fn xy(&self) -> Option<WebMercator> {
        const QUARTER_PI: f32 = PI * 0.25;

        if !Self::LAT_BOUNDS.contains(&self.0.y()) {
            return None;
        }

        let x = self.0.x().to_radians();
        let y = (QUARTER_PI + 0.5 * self.0.y().to_radians()).tan().ln();

        Some(
            Point::new(
                x * EARTH_RADIUS_METERS,
                y * EARTH_RADIUS_METERS,
            ).into()
        )
    }
}


impl Tile {
    pub fn new(x: u32, y: u32, z: u8) -> Self {
        const MAX_ZOOM: u8 = 18;
        let num_tiles = 1u32 << z;
        assert!(x < num_tiles);
        assert!(y < num_tiles);
        // note: arbitrary restriction
        assert!(z < MAX_ZOOM);

        Self { x, y, z }
    }

    pub fn parent(&self) -> Self {
        if self.z == 0 {
            *self
        } else {
            Tile::new(self.x / 2, self.y / 2, self.z - 1)
        }
    }

    pub fn children(&self) -> [Self; 4] {
        [
            Tile::new(0 + self.x * 2, 0 + self.y * 2, self.z + 1),
            Tile::new(1 + self.x * 2, 0 + self.y * 2, self.z + 1),
            Tile::new(0 + self.x * 2, 1 + self.y * 2, self.z + 1),
            Tile::new(1 + self.x * 2, 1 + self.y * 2, self.z + 1),
        ]
    }
}

pub struct CoveringTileIter {
    dx: f32,
    dy: f32,
    nx: u32,
    ny: u32,
    ix: u32,
    iy: u32,
    cur: Tile,
}

// https://www.redblobgames.com/grids/line-drawing/
impl Iterator for CoveringTileIter {
    type Item = Tile;

    fn next(&mut self) -> Option<Self::Item> {
        // Reached destination.
        if self.ix >= self.nx || self.iy >= self.ny {
            return None;
        }

        let acc_x = (1 + 2 * self.ix) * self.ny;
        let acc_y = (1 + 2 * self.iy) * self.nx;

        if acc_x < acc_y {
            // Horizontal step
            self.cur = Tile::new(
                if self.dx > 0.0 { self.cur.x + 1 } else { self.cur.x - 1 },
                self.cur.y,
                self.cur.z,
            );

            self.ix += 1;
        } else if acc_x > acc_y {
            // Vertical step
            self.cur = Tile::new(
                self.cur.x,
                if self.dy > 0.0 { self.cur.y + 1 } else { self.cur.y - 1 },
                self.cur.z,
            );

            self.iy += 1;
        } else {
            // Diagonal step
            self.cur = Tile::new(
                if self.dx > 0.0 { self.cur.x + 1 } else { self.cur.x - 1 },
                if self.dy > 0.0 { self.cur.y + 1 } else { self.cur.y - 1 },
                self.cur.z,
            );

            self.ix += 1;
            self.iy += 1;
        }

        Some(self.cur)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xy() {
        // Out of bounds.
        assert!(LngLat::new(0.0, -90.0).xy().is_none());

        let max = ORIGIN_OFFSET;
        let min = -ORIGIN_OFFSET;
        let mid = 0.0;

        let cases = [
            ((0.0, 0.0), (mid, mid)),
            ((-180.0, 0.0), (min, mid)),
            ((180.0, 0.0), (max, mid)),
            ((0.0, 85.051128), (mid, max)),
            ((0.0, -85.051128), (mid, min)),
            // Random points sourced from https://www.maptiler.com/google-maps-coordinates-tile-bounds-projection/#13/-118.24/34.08
            ((-118.256838, 34.052659), (-13164291.0, 4035875.0))
        ];

        for ((lng, lat), (x, y)) in &cases {
            let p = LngLat::new(*lng, *lat);
            let xy = p.xy().expect("xy");

            // Going to be off by a bit, but is this too much?
            assert!((xy.0.x() - *x).abs() < 15.0, "actual:{} != expected:{}", xy.0.x(), *x);
            assert!((xy.0.y() - *y).abs() < 15.0, "actual:{} != expected:{}", xy.0.y(), *y);
        }
    }

    #[test]
    fn test_tile_parent() {
        let tile = Tile::new(0, 0, 0);
        assert_eq!(tile.parent(), tile);

        let tile = Tile::new(1, 1, 1);
        assert_eq!(tile.parent(), Tile::new(0, 0, 0));
    }

    #[test]
    fn test_lat_lng_to_pixel() {
        let cases = [
           // (3, (0.0, 0.0), (1024, 1024)),
            (13, (-118.256838, 34.052659), (359680, 837120))
        ];

        for (zoom, (lng, lat), (ex, ey)) in cases {
            let p = LngLat::new(lng, lat);
            let Coord { x, y } = p.xy().map(|xy| xy.pixel_xy(512, zoom)).unwrap().pixel;

            assert_eq!(x, ex);
            assert_eq!(y, ey);
        }
    }
}