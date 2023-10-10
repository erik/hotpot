use std::f64::consts::PI;
use std::ops::Range;
use std::str::FromStr;

use derive_more::{From, Into};
use geo_types::{Coord, Point};

const EARTH_RADIUS_METERS: f64 = 6_378_137.0;
const EARTH_CIRCUMFERENCE: f64 = 2.0 * PI * EARTH_RADIUS_METERS;
const ORIGIN_OFFSET: f64 = EARTH_CIRCUMFERENCE / 2.0;

#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
pub struct Tile {
    pub x: u32,
    pub y: u32,
    pub z: u8,
}

#[derive(Copy, Clone, Debug)]
pub struct TileBounds {
    pub z: u8,
    pub xmin: u32,
    pub ymin: u32,
    pub xmax: u32,
    pub ymax: u32,
}

impl TileBounds {
    pub fn from(source_zoom: u8, tile: &Tile) -> TileBounds {
        assert!(
            source_zoom >= tile.z,
            "source level must be >= target level"
        );

        let zoom_steps = source_zoom - tile.z;

        TileBounds {
            z: source_zoom,
            xmin: tile.x << zoom_steps,
            ymin: tile.y << zoom_steps,
            xmax: (tile.x + 1) << zoom_steps,
            ymax: (tile.y + 1) << zoom_steps,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug, From, Into)]
pub struct LngLat(pub Point<f64>);

#[derive(Copy, Clone, PartialEq, Debug, From, Into)]
pub struct WebMercator(pub Point<f64>);

#[derive(Copy, Clone, PartialEq, Debug, From, Into)]
pub struct TilePixel(pub Coord<u16>);

#[derive(Copy, Clone, PartialEq, Debug)]
pub struct BBox {
    pub left: f64,
    pub bot: f64,
    pub right: f64,
    pub top: f64,
}

impl BBox {
    const INSIDE: u8 = 0b0000;
    const LEFT: u8 = 0b0001;
    const RIGHT: u8 = 0b0010;
    const BOTTOM: u8 = 0b0100;
    const TOP: u8 = 0b1000;

    pub fn contains(&self, pt: &WebMercator) -> bool {
        pt.0.x() >= self.left
            && pt.0.y() >= self.bot
            && pt.0.x() <= self.right
            && pt.0.y() <= self.top
    }

    fn compute_edges(&self, x: f64, y: f64) -> u8 {
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

    /// Clip the line segment [start, end] to tile boundaries and return updated points.
    ///
    /// Implementation of the Cohen-Sutherland line clipping algorithm.
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
            if (edge_start | edge_end) == Self::INSIDE {
                // Both points inside, no clipping needed
                return Some((Point::new(x0, y0).into(), Point::new(x1, y1).into()));
            } else if (edge_start & edge_end) != Self::INSIDE {
                // Both points outside on the same edge, no intersection.
                return None;
            } else {
                // failed both tests, so calculate the line segment to clip
                // from an outside point to an intersection with clip edge

                // At least one endpoint is outside the clip rectangle; pick it.
                let intersect = if edge_start > edge_end {
                    edge_start
                } else {
                    edge_end
                };

                let dx = x1 - x0;
                let dy = y1 - y0;

                let x: f64;
                let y: f64;

                if (intersect & Self::TOP) != 0 {
                    x = x0 + (dx * (self.top - y0)) / dy;
                    y = self.top;
                } else if (intersect & Self::BOTTOM) != 0 {
                    x = x0 + (dx * (self.bot - y0)) / dy;
                    y = self.bot;
                } else if (intersect & Self::RIGHT) != 0 {
                    x = self.right;
                    y = y0 + (dy * (self.right - x0)) / dx;
                } else if (intersect & Self::LEFT) != 0 {
                    x = self.left;
                    y = y0 + (dy * (self.left - x0)) / dx;
                } else {
                    unreachable!("no intersection")
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
    pub fn tile(&self, zoom: u8) -> Tile {
        let num_tiles = (1u32 << zoom) as f64;
        let scale = num_tiles / EARTH_CIRCUMFERENCE;

        let x = (scale * (self.0.x() + ORIGIN_OFFSET)).floor() as u32;
        let y = (scale * (ORIGIN_OFFSET - self.0.y())).floor() as u32;

        Tile::new(x, y, zoom)
    }

    pub fn to_pixel(self, bbox: &BBox, tile_width: u16) -> TilePixel {
        let Coord { x, y } = self.0.into();

        let width = bbox.right - bbox.left;
        let height = bbox.top - bbox.bot;

        let px = ((x - bbox.left) / width * tile_width as f64).round() as u16;
        let py = ((y - bbox.bot) / height * tile_width as f64).round() as u16;

        TilePixel((px, py).into())
    }
}

impl LngLat {
    const LAT_BOUNDS: Range<f64> = -89.99999..90.0;

    pub fn new(mut x: f64, y: f64) -> LngLat {
        while x < -180.0 {
            x += 360.0;
        }

        Self(Point::new(x, y))
    }

    pub fn xy(&self) -> Option<WebMercator> {
        const QUARTER_PI: f64 = PI * 0.25;

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
        let num_tiles = 1u32 << z;
        debug_assert!(x < num_tiles);
        debug_assert!(y < num_tiles);

        Self { x, y, z }
    }

    pub fn xy_bounds(&self) -> BBox {
        let num_tiles = (1u64 << self.z) as f64;
        let tile_size = EARTH_CIRCUMFERENCE / num_tiles;

        let left = (self.x as f64 * tile_size) - ORIGIN_OFFSET;
        let top = ORIGIN_OFFSET - (self.y as f64 * tile_size);
        BBox {
            left,
            top,
            bot: top - tile_size,
            right: left + tile_size,
        }
    }
}

impl FromStr for Tile {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 3 {
            return Err("invalid tile format");
        }

        let z = parts[0].parse::<u8>().map_err(|_| "invalid z")?;
        let x = parts[1].parse::<u32>().map_err(|_| "invalid x")?;
        let y = parts[2].parse::<u32>().map_err(|_| "invalid y")?;

        Ok(Tile::new(x, y, z))
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
            close_enough!(xy.0.x(), *x, 2.0);
            close_enough!(xy.0.y(), *y, 2.0);
        }
    }

    #[test]
    fn test_xy_bounds() {
        // Taken from Mercantile
        let tile = Tile::new(486, 332, 10);
        let bounds = tile.xy_bounds();

        close_enough!(bounds.left, -1017529.7205322663, 0.001);
        close_enough!(bounds.bot, 7005300.768279833, 0.001);
        close_enough!(bounds.right, -978393.962050256, 0.001);
        close_enough!(bounds.top, 7044436.526761846, 0.001);
    }

    #[test]
    fn test_lat_lng_to_tile() {
        let ll: LngLat = Point::new(20.6852, 40.1222).into();
        let xy = ll.xy().expect("xy");
        let tile = xy.tile(9);

        assert_eq!(tile, Tile::new(285, 193, 9));
    }

    #[test]
    fn test_bbox_clipping() {
        let bbox = BBox {
            left: 0.0,
            bot: 0.0,
            right: 10.0,
            top: 10.0,
        };

        // Completely within
        let clipped = bbox.clip_line(&Point::new(1.0, 1.0).into(), &Point::new(9.0, 9.0).into());
        assert_eq!(
            clipped,
            Some((Point::new(1.0, 1.0).into(), Point::new(9.0, 9.0).into()))
        );

        // No intersection
        let outside_cases = &[((-1.0, 0.0), (-1.0, 11.0)), ((0.0, -1.0), (11.0, -1.0))];
        for ((x0, y0), (x1, y1)) in outside_cases {
            let clipped =
                bbox.clip_line(&Point::new(*x0, *y0).into(), &Point::new(*x1, *y1).into());
            assert_eq!(clipped, None);
        }

        // Outside horizontal
        let clipped = bbox.clip_line(&Point::new(-1.0, 5.0).into(), &Point::new(11.0, 5.0).into());
        assert_eq!(
            clipped,
            Some((Point::new(0.0, 5.0).into(), Point::new(10.0, 5.0).into()))
        );

        // Outside vertical
        let clipped = bbox.clip_line(&Point::new(5.0, -1.0).into(), &Point::new(5.0, 11.0).into());
        assert_eq!(
            clipped,
            Some((Point::new(5.0, 0.0).into(), Point::new(5.0, 10.0).into()))
        );

        // Outside diagonal
        let clipped = bbox.clip_line(
            &Point::new(-1.0, -1.0).into(),
            &Point::new(11.0, 11.0).into(),
        );
        assert_eq!(
            clipped,
            Some((Point::new(0.0, 0.0).into(), Point::new(10.0, 10.0).into()))
        );
    }
}
