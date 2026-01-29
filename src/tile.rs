use std::ops::Range;
use std::str::FromStr;
use std::{f64::consts::PI, ops::RangeInclusive};

use anyhow::{Result, anyhow};
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

    pub fn from_viewport(
        viewport: &WebMercatorViewport,
        viewport_width: u32,
        viewport_height: u32,
        zoom_range: RangeInclusive<u32>,
    ) -> Self {
        let tile_size = 256;

        let min_zoom = *zoom_range.start();
        let max_zoom = *zoom_range.end();

        let sw_px = viewport.sw.to_global_pixel(max_zoom as u8, tile_size);
        let ne_px = viewport.ne.to_global_pixel(max_zoom as u8, tile_size);

        let scale = f64::max(
            (ne_px.x() - sw_px.x()) as f64 / (viewport_width as f64),
            (sw_px.y() - ne_px.y()) as f64 / (viewport_height as f64),
        );

        // Find the smallest zoom level which will cover our viewport at full
        // resolution.
        let zoom = (max_zoom - scale.log2().floor() as u32).clamp(min_zoom, max_zoom) as u8;
        let sw_tile = viewport.sw.tile(zoom);
        let ne_tile = viewport.ne.tile(zoom);

        TileBounds {
            z: zoom,
            xmin: sw_tile.x,
            xmax: ne_tile.x,
            ymin: ne_tile.y,
            ymax: sw_tile.y,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug, From, Into)]
pub struct LngLat(pub Point<f64>);

#[derive(Copy, Clone, PartialEq, Debug, From, Into)]
pub struct WebMercator(pub Point<f64>);

#[derive(Copy, Clone, PartialEq, Debug, From, Into)]
pub struct TilePixel(pub Coord<u16>);

#[derive(Debug, Clone)]
pub struct WebMercatorViewport {
    sw: WebMercator,
    ne: WebMercator,
}

impl FromStr for WebMercatorViewport {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let parts: Vec<_> = s
            .split(',')
            .filter_map(|it| it.parse::<f64>().ok())
            .collect();

        if parts.len() != 4 {
            return Err(anyhow!("expected coordinates as 'west,south,east,north'"));
        }

        let sw = LngLat((parts[0], parts[1]).into())
            .xy()
            .ok_or_else(|| anyhow!("south/western coord out of WebMercator bounds"))?;

        let ne = LngLat((parts[2], parts[3]).into())
            .xy()
            .ok_or_else(|| anyhow!("north/eastern coord out of WebMercator bounds"))?;

        if sw.0.x() >= ne.0.x() {
            Err(anyhow!("east/west appear to be swapped"))
        } else if sw.0.y() >= ne.0.y() {
            Err(anyhow!("north/south appear to be swapped"))
        } else {
            Ok(WebMercatorViewport { sw, ne })
        }
    }
}

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

    pub fn square(width: f64) -> Self {
        Self {
            left: 0.0,
            bot: 0.0,
            right: width,
            top: width,
        }
    }

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
    /// Return the tile coordinate for this point at the given zoom level.
    pub fn tile(&self, zoom: u8) -> Tile {
        let num_tiles = (1u32 << zoom) as f64;
        let scale = num_tiles / EARTH_CIRCUMFERENCE;

        let x = (scale * (self.0.x() + ORIGIN_OFFSET)).floor() as u32;
        let y = (scale * (ORIGIN_OFFSET - self.0.y())).floor() as u32;

        Tile::new(x, y, zoom)
    }

    pub fn to_global_pixel(self, zoom: u8, tile_extent: u32) -> Point<u32> {
        let num_tiles = 1u32 << zoom;
        let scale = (num_tiles * tile_extent) as f64 / EARTH_CIRCUMFERENCE;

        Point::from((
            (scale * (self.0.x() + ORIGIN_OFFSET)) as u32,
            (scale * (ORIGIN_OFFSET - self.0.y())) as u32,
        ))
    }

    pub fn to_tile_pixel(self, bbox: &BBox, tile_width: u16) -> TilePixel {
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

use crate::db::PrivacyZone;

struct ProjectedZone {
    center_x: f64,
    center_y: f64,
    radius_sq: f64,
}

/// Pre-computed privacy filter for a specific tile.
pub struct TilePrivacyFilter {
    zones: Vec<ProjectedZone>,
}

impl TilePrivacyFilter {
    /// Create a filter for the given tile. Returns None if no zones intersect.
    pub fn new(zones: &[PrivacyZone], tile: &Tile, tile_extent: u32) -> Option<Self> {
        let bbox = tile.xy_bounds();
        let tile_width_m = EARTH_CIRCUMFERENCE / (1u64 << tile.z) as f64;
        let px_per_m = tile_extent as f64 / tile_width_m;

        let zones: Vec<_> = zones
            .iter()
            .filter_map(|zone| {
                let center_wm = LngLat::new(zone.lng, zone.lat).xy()?;

                // Check if zone intersects tile (expand bbox by radius)
                let radius_wm = zone.radius_m;
                let expanded_bbox = BBox {
                    left: bbox.left - radius_wm,
                    right: bbox.right + radius_wm,
                    bot: bbox.bot - radius_wm,
                    top: bbox.top + radius_wm,
                };
                if !expanded_bbox.contains(&center_wm) {
                    return None;
                }

                let center_px = center_wm.to_tile_pixel(&bbox, tile_extent as u16);
                let radius_px = zone.radius_m * px_per_m;

                Some(ProjectedZone {
                    center_x: center_px.0.x as f64,
                    center_y: center_px.0.y as f64,
                    radius_sq: radius_px * radius_px,
                })
            })
            .collect();

        if zones.is_empty() {
            None
        } else {
            Some(Self { zones })
        }
    }

    /// Check if a point should be hidden (is within any privacy zone).
    #[inline]
    pub fn is_hidden(&self, px: u32, py: u32) -> bool {
        self.zones.iter().any(|z| {
            let dx = px as f64 - z.center_x;
            let dy = py as f64 - z.center_y;
            dx * dx + dy * dy <= z.radius_sq
        })
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

    #[test]
    fn test_privacy_filter_hides_point_in_zone() {
        // Berlin, 15km radius
        let zone = PrivacyZone {
            lat: 52.528125,
            lng: 13.3643882,
            radius_m: 15000.0,
        };

        // Point inside the circle
        let inside = LngLat::new(13.3382299, 52.528125).xy().unwrap();
        let tile = inside.tile(10);
        let tile_extent = 256u32;

        let filter = TilePrivacyFilter::new(&[zone], &tile, tile_extent).unwrap();

        let inside_px = inside.to_tile_pixel(&tile.xy_bounds(), tile_extent as u16);
        assert!(
            filter.is_hidden(inside_px.0.x as u32, inside_px.0.y as u32),
            "point inside zone should be hidden"
        );
    }

    #[test]
    fn test_privacy_filter_allows_point_outside_zone() {
        // Berlin, 15km radius
        let zone = PrivacyZone {
            lat: 52.528125,
            lng: 13.3643882,
            radius_m: 15000.0,
        };

        // Point outside the circle
        let outside = LngLat::new(13.8221594, 52.6044458).xy().unwrap();
        let tile = outside.tile(10);
        let tile_extent = 256u32;

        let filter = TilePrivacyFilter::new(&[zone], &tile, tile_extent);

        // Filter may or may not exist for this tile, but if it does, the point should not be hidden
        if let Some(f) = filter {
            let outside_px = outside.to_tile_pixel(&tile.xy_bounds(), tile_extent as u16);
            assert!(
                !f.is_hidden(outside_px.0.x as u32, outside_px.0.y as u32),
                "point outside zone should not be hidden"
            );
        }
    }

    #[test]
    fn test_privacy_filter_no_zones() {
        let tile = Tile::new(0, 0, 10);
        assert!(TilePrivacyFilter::new(&[], &tile, 256).is_none());
    }

    #[test]
    fn test_privacy_filter_zone_crosses_tile_boundary() {
        // Berlin, 15km radius - should span multiple tiles
        let zone = PrivacyZone {
            lat: 52.528125,
            lng: 13.3643882,
            radius_m: 15000.0,
        };

        let center = LngLat::new(13.3643882, 52.528125).xy().unwrap();
        let center_tile = center.tile(10);

        assert!(
            TilePrivacyFilter::new(&[zone.clone()], &center_tile, 256).is_some(),
            "zone should be in center tile"
        );

        let adjacent_tiles = [
            Tile::new(center_tile.x + 1, center_tile.y, 10),
            Tile::new(center_tile.x.saturating_sub(1), center_tile.y, 10),
            Tile::new(center_tile.x, center_tile.y + 1, 10),
            Tile::new(center_tile.x, center_tile.y.saturating_sub(1), 10),
        ];

        let zones_in_adjacent = adjacent_tiles
            .iter()
            .filter(|t| TilePrivacyFilter::new(&[zone.clone()], t, 256).is_some())
            .count();

        assert!(
            zones_in_adjacent > 0,
            "15km zone should span into adjacent tiles at z10"
        );

        // Point outside the circle should not be hidden even in tiles that have the zone
        let outside = LngLat::new(13.8221594, 52.6044458).xy().unwrap();
        let outside_tile = outside.tile(10);
        if let Some(f) = TilePrivacyFilter::new(&[zone], &outside_tile, 256) {
            let outside_px = outside.to_tile_pixel(&outside_tile.xy_bounds(), 256);
            assert!(
                !f.is_hidden(outside_px.0.x as u32, outside_px.0.y as u32),
                "point outside zone should not be hidden"
            );
        }
    }
}
