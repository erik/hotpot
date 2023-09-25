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

#[derive(Copy, Clone, PartialEq, Debug)]
pub struct BBox {
    pub left: f32,
    pub bot: f32,
    pub right: f32,
    pub top: f32,
}

impl BBox {
    pub fn zero() -> Self {
        Self {
            left: 0.0,
            bot: 0.0,
            right: 0.0,
            top: 0.0,
        }
    }

    pub fn contains(&self, pt: &WebMercator) -> bool {
        pt.0.x() >= self.left
            && pt.0.x() <= self.right
            && pt.0.y() >= self.bot
            && pt.0.y() <= self.top
    }

    const INSIDE: u8 = 0b0000;
    const LEFT: u8 = 0b0001;
    const RIGHT: u8 = 0b0010;
    const BOTTOM: u8 = 0b0100;
    const TOP: u8 = 0b1000;

    fn compute_edges(&self, x: f32, y: f32) -> u8 {
        let mut code = 0;

        if x < self.left {
            code |= Self::LEFT;
        } else if x > self.right {
            code |= Self::RIGHT;
        }

        if y < self.bot {
            code |= Self::BOTTOM;
        } else if y > self.top {
            code |= Self::TOP;
        }

        code
    }

    // Clip the line segment [start, end] to tile boundaries.
    // Return updated end point.
    //
    // Implementation of the Cohen-Sutherland line clipping algorithm.
    pub fn clip_line(
        &self,
        start: &WebMercator,
        end: &WebMercator,
    ) -> Option<(WebMercator, WebMercator)> {
        let (mut x0, mut y0) = (start.0.x(), start.0.y());
        let (mut x1, mut y1) = (end.0.x(), end.0.y());

        let mut edge_start = self.compute_edges(x0, y0);
        let mut edge_end = self.compute_edges(x1, y1);

        loop {
            if edge_end == Self::INSIDE {
                // Both points inside, no clipping needed
                return Some((Point::new(x0, y0).into(), Point::new(x1, y1).into()));
            } else if (edge_start & edge_end) != Self::INSIDE {
                // Both points outside on the same edge, no intersection.
                return None;
            } else {
                // failed both tests, so calculate the line segment to clip
                // from an outside point to an intersection with clip edge

                // At least one endpoint is outside the clip rectangle; pick it.
                let intersect = if edge_start != Self::INSIDE {
                    edge_start
                } else {
                    edge_end
                };

                let dx = x1 - x0;
                let dy = y1 - y0;

                let (x, y) = if (intersect & Self::TOP) != 0 {
                    (x0 + (self.top - y0) * (dx / dy), self.top)
                } else if (intersect & Self::BOTTOM) != 0 {
                    (x0 + (self.bot - y0) * (dx / dy), self.bot)
                } else if (intersect & Self::RIGHT) != 0 {
                    (self.right, y0 + (self.right - x0) * (dy / dx))
                } else if (intersect & Self::LEFT) != 0 {
                    (self.left, y0 + (self.left - x0) * (dy / dx))
                } else {
                    unreachable!()
                };

                if intersect == edge_start {
                    x0 = x;
                    y0 = y;
                    edge_start = self.compute_edges(x0, y0);
                } else {
                    x1 = x;
                    y1 = y;
                    edge_end = self.compute_edges(x1, y1);
                }
            }
        }
    }
}

impl WebMercator {
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

        Some(Point::new(x * EARTH_RADIUS_METERS, y * EARTH_RADIUS_METERS).into())
    }
}

impl Tile {
    pub fn new(x: u32, y: u32, z: u8) -> Self {
        const MAX_ZOOM: u8 = 15;
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

    // Given a web mercator coordinate, scale it to a pixel coordinate relative to its tile.
    pub fn project_point(&self, pt: &WebMercator, tile_size: u32) -> Point<u16> {
        let num_tiles = (1u32 << self.z) as f32;
        let scale = num_tiles / EARTH_CIRCUMFERENCE;

        let x = (scale * pt.0.x()).floor() as u32;
        let y = (scale * pt.0.y()).floor() as u32;

        Point::new(
            (x - self.x * tile_size) as u16,
            (y - self.y * tile_size) as u16,
        )
    }

    pub fn xy_bounds(&self) -> BBox {
        let num_tiles = (1u32 << self.z) as f32;
        let tile_size = EARTH_CIRCUMFERENCE / num_tiles;

        let left = (self.x as f32 * tile_size) - ORIGIN_OFFSET;
        let bot = ORIGIN_OFFSET - (self.y as f32 * tile_size);
        BBox {
            left,
            bot,
            top: bot - tile_size,
            right: left + tile_size,
        }
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
                if self.dx > 0.0 {
                    self.cur.x + 1
                } else {
                    self.cur.x - 1
                },
                self.cur.y,
                self.cur.z,
            );

            self.ix += 1;
        } else if acc_x > acc_y {
            // Vertical step
            self.cur = Tile::new(
                self.cur.x,
                if self.dy > 0.0 {
                    self.cur.y + 1
                } else {
                    self.cur.y - 1
                },
                self.cur.z,
            );

            self.iy += 1;
        } else {
            // Diagonal step
            self.cur = Tile::new(
                if self.dx > 0.0 {
                    self.cur.x + 1
                } else {
                    self.cur.x - 1
                },
                if self.dy > 0.0 {
                    self.cur.y + 1
                } else {
                    self.cur.y - 1
                },
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

    macro_rules! close_enough {
        ( $x:expr, $y:expr, $z:expr ) => {
            assert!(($y - $x).abs() < $z, "{} != {} (within {})", $x, $y, $z);
        };
    }

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
            ((-118.256838, 34.052659), (-13164291.0, 4035875.0)),
        ];

        for ((lng, lat), (x, y)) in &cases {
            let p = LngLat::new(*lng, *lat);
            let xy = p.xy().expect("xy");

            // Going to be off by a bit, but is this too much?
            close_enough!(xy.0.x(), *x, 15.0);
            close_enough!(xy.0.y(), *y, 15.0);
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
    fn test_xy_bounds() {
        // Taken from Mercantile
        let tile = Tile::new(486, 332, 10);
        let bounds = tile.xy_bounds();

        // TODO: don't love the inaccuracy here
        close_enough!(bounds.left, -1017529.7205322663, 0.5);
        close_enough!(bounds.top, 7005300.768279833, 2.0);
        close_enough!(bounds.right, -978393.962050256, 0.5);
        close_enough!(bounds.bot, 7044436.526761846, 1.0);
    }
}
