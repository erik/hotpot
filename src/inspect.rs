use std::collections::HashSet;

use anyhow::{Result, anyhow};
use geo::EuclideanDistance;
use geo_types::{Line, Point};
use rusqlite::{Connection, params};
use serde::Serialize;
use time::OffsetDateTime;

use crate::db::{ActivityFilter, Config, Database, decode_line};
use crate::tile::{LngLat, Tile, TileBounds, WebMercator, mercator_pixel_scale};

#[derive(Serialize)]
pub struct FeatureCollection {
    #[serde(rename = "type")]
    kind: &'static str,
    features: Vec<Feature>,
}

#[derive(Serialize)]
struct Feature {
    #[serde(rename = "type")]
    kind: &'static str,
    id: i64,
    geometry: MultiLineString,
    properties: FeatureProperties,
}

#[derive(Serialize)]
struct MultiLineString {
    #[serde(rename = "type")]
    kind: &'static str,
    coordinates: Vec<Vec<[f64; 2]>>,
}

#[derive(Serialize)]
struct FeatureProperties {
    title: Option<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    start_time: Option<OffsetDateTime>,
    total_distance: Option<f64>,
    elevation_gain: Option<f64>,
}

impl FeatureCollection {
    fn new(features: Vec<Feature>) -> Self {
        Self {
            kind: "FeatureCollection",
            features,
        }
    }
}

fn masked_coords<'a>(
    tile: Tile,
    bytes: &'a [u8],
    config: &Config,
) -> impl Iterator<Item = Option<Point<f64>>> + 'a {
    let extent = config.tile_extent;
    let mask = tile.build_mask(&config.activity_mask, extent as i32);
    let (x_offset, y_offset) = (tile.x * extent, tile.y * extent);

    decode_line(bytes).map(move |c| {
        let (x, y) = (c.x, extent - c.y);
        if mask.is_hidden(x as i32, y as i32) {
            None
        } else {
            Some(Point::new((x + x_offset) as f64, (y + y_offset) as f64))
        }
    })
}

pub fn activities_at(
    db: &Database,
    config: &Config,
    filter: &ActivityFilter,
    center: WebMercator,
    radius_meters: f64,
    zoom: u8,
) -> Result<FeatureCollection> {
    let Some(z) = config
        .source_level(zoom)
        .or_else(|| config.zoom_levels.last().copied())
    else {
        return Ok(FeatureCollection::new(vec![]));
    };

    let conn = db.connection()?;
    let radius = radius_meters * center.meter_scale();
    let nearby_activities = find_nearby_activities(&conn, config, filter, center, radius, z)?;
    if nearby_activities.is_empty() {
        return Ok(FeatureCollection::new(vec![]));
    }

    let features = load_activity_geojson(&conn, config, z, &nearby_activities)?;
    Ok(FeatureCollection::new(features))
}

fn find_nearby_activities(
    conn: &Connection,
    config: &Config,
    filter: &ActivityFilter,
    center: WebMercator,
    radius: f64,
    z: u8,
) -> Result<HashSet<i64>> {
    // Bound our activity tile search to just the tiles in/near the click.
    let bounds = {
        let (cx, cy) = (center.0.x(), center.0.y());
        let sw = WebMercator(Point::new(cx - radius, cy - radius)).tile(z);
        let ne = WebMercator(Point::new(cx + radius, cy + radius)).tile(z);
        TileBounds {
            z,
            xmin: sw.x,
            xmax: ne.x + 1,
            ymin: ne.y,
            ymax: sw.y + 1,
        }
    };

    let mut params = params![bounds.z, bounds.xmin, bounds.xmax, bounds.ymin, bounds.ymax].to_vec();
    let filter_expr = filter.to_query(&mut params);

    let mut stmt = conn.prepare(&format!(
        "\
        SELECT t1.x, t1.y, t1.coords, t1.activity_id
        FROM activity_tiles t1
        JOIN activities t0 ON t0.id = t1.activity_id
        WHERE t1.z = ?
            AND (t1.x >= ? AND t1.x < ?)
            AND (t1.y >= ? AND t1.y < ?)
            AND {filter_expr}
        ORDER BY t0.start_time DESC, t0.id DESC;",
    ))?;

    let center_px: Point<f64> = center.to_mercator_pixel(z, config.tile_extent).into();
    let radius_px = radius * mercator_pixel_scale(z, config.tile_extent);

    let mut nearby_activities: HashSet<i64> = HashSet::new();
    let mut rows = stmt.query(params.as_slice())?;

    while let Some(row) = rows.next()? {
        let tile = Tile::new(row.get_unwrap(0), row.get_unwrap(1), z);
        let encoded_coords = row
            .get_ref(2)?
            .as_bytes()
            .map_err(|_| anyhow!("expected blob for tile coordinates"))?;
        let activity_id = row.get_unwrap(3);

        if nearby_activities.contains(&activity_id) {
            continue;
        }

        let mut prev = None;
        for pt in masked_coords(tile, encoded_coords, config) {
            if let (Some(a), Some(b)) = (prev, pt)
                && center_px.euclidean_distance(&Line::new(a, b)) <= radius_px
            {
                nearby_activities.insert(activity_id);
                break;
            }
            prev = pt;
        }

        const MAX_RESULTS: usize = 10;
        if nearby_activities.len() >= MAX_RESULTS {
            break;
        }
    }

    Ok(nearby_activities)
}

fn load_activity_geojson(
    conn: &Connection,
    config: &Config,
    zoom: u8,
    activity_ids: &HashSet<i64>,
) -> Result<Vec<Feature>> {
    // Low zooms are too coarse for route data, higher zoom levels are too
    // detailed.
    const MIN_ZOOM: u8 = 10;
    const MAX_ZOOM: u8 = 14;

    let tile_zoom = config
        .source_level(zoom.clamp(MIN_ZOOM, MAX_ZOOM))
        .unwrap_or_else(|| *config.zoom_levels.last().expect("no stored zoom levels"));

    let mut stmt = conn.prepare_cached(
        "\
        SELECT
            t0.id,
            t0.title,
            t0.start_time,
            CAST(t0.properties ->> 'total_distance' AS REAL),
            CAST(t0.properties ->> 'elevation_gain' AS REAL),
            t1.x,
            t1.y,
            t1.coords
        FROM activities t0
        LEFT JOIN activity_tiles t1 ON (
            t1.activity_id = t0.id AND t1.z = ?
        )
        WHERE t0.id IN (SELECT value FROM json_each(?))
        ORDER BY t0.start_time DESC, t0.id DESC;",
    )?;

    let mut features: Vec<Feature> = vec![];
    let mut curr: Option<Feature> = None;

    let ids = serde_json::to_string(activity_ids)?;
    let mut rows = stmt.query(params![tile_zoom, ids])?;

    while let Some(row) = rows.next()? {
        let activity_id: i64 = row.get_unwrap(0);

        // Moving to next activity, commit current one
        if let Some(ref feat) = curr
            && feat.id != activity_id
        {
            features.extend(curr.take_if(|f| !f.geometry.coordinates.is_empty()));
        }

        let feature = curr.get_or_insert_with(|| Feature {
            kind: "Feature",
            id: activity_id,
            geometry: MultiLineString {
                kind: "MultiLineString",
                coordinates: vec![],
            },
            properties: FeatureProperties {
                title: row.get_unwrap(1),
                start_time: row.get_unwrap(2),
                total_distance: row.get_unwrap(3),
                elevation_gain: row.get_unwrap(4),
            },
        });

        let Some(encoded_coords) = row
            .get_ref(7)?
            .as_bytes_or_null()
            .map_err(|_| anyhow!("expected blob for tile coordinates"))?
        else {
            continue;
        };

        let tile = Tile::new(row.get_unwrap(5), row.get_unwrap(6), tile_zoom);
        let coordinates = &mut feature.geometry.coordinates;

        let mut seg = vec![];
        for pt in masked_coords(tile, encoded_coords, config) {
            match pt {
                Some(pt) => {
                    let ll =
                        LngLat::from_mercator_pixel(pt.into(), tile_zoom, config.tile_extent).0;
                    seg.push([round_6dp(ll.x()), round_6dp(ll.y())]);
                }

                // Entering masked area, don't include point
                None => {
                    if seg.len() >= 2 {
                        coordinates.push(std::mem::take(&mut seg));
                    } else {
                        seg.clear();
                    }
                }
            }
        }

        if seg.len() >= 2 {
            coordinates.push(seg);
        }
    }

    features.extend(curr.take_if(|f| !f.geometry.coordinates.is_empty()));

    Ok(features)
}

/// Avoid sending back floating point numbers with unnecessary precision
fn round_6dp(v: f64) -> f64 {
    (v * 1e6).round() / 1e6
}

#[cfg(test)]
mod tests {
    use time::{Date, Month};

    use super::*;
    use crate::activity::{self, RawActivity};
    use crate::db::ActivityMask;

    const CENTER_POINT: (f64, f64) = (13.41, 52.53);

    fn insert_test_activity(
        db: &Database,
        config: &Config,
        name: &str,
        start_ts: i64,
        ends: [(f64, f64); 2],
    ) {
        let [(x0, y0), (x1, y1)] = ends;
        let coords: Vec<(f64, f64)> = (0..=100)
            .map(|i| {
                let t = i as f64 / 100.0;
                let dy = if i % 2 == 0 { 0.0001 } else { -0.0001 };
                (x0 + (x1 - x0) * t, y0 + (y1 - y0) * t + dy)
            })
            .collect();

        let activity = RawActivity {
            title: Some(name.to_string()),
            start_time: Some(OffsetDateTime::from_unix_timestamp(start_ts).unwrap()),
            properties: [("total_distance".to_string(), 2.6.into())].into(),
            tracks: geo_types::MultiLineString::from(geo_types::LineString::from(coords)),
        };
        let mut conn = db.connection().unwrap();
        activity::upsert(&mut conn, name, activity, config).unwrap();
    }

    fn inspect(
        db: &Database,
        config: &Config,
        filter: &ActivityFilter,
        (lng, lat): (f64, f64),
        radius: f64,
        zoom: u8,
    ) -> Vec<Feature> {
        let center = LngLat::new(lng, lat).xy().unwrap();
        activities_at(db, config, filter, center, radius, zoom)
            .unwrap()
            .features
    }

    fn points(feature: &Feature) -> impl Iterator<Item = (f64, f64)> + '_ {
        feature
            .geometry
            .coordinates
            .iter()
            .flatten()
            .map(|&[lng, lat]| (lng, lat))
    }

    fn setup() -> (Database, Config) {
        let db = Database::memory().unwrap();
        let config = db.load_config().unwrap();
        (db, config)
    }

    #[test]
    fn test_matches_within_radius_newest_first() {
        let (db, config) = setup();
        insert_test_activity(
            &db,
            &config,
            "older",
            1_600_000_000,
            [(13.40, 52.5201), (13.42, 52.5401)],
        );
        insert_test_activity(
            &db,
            &config,
            "newer",
            1_700_000_000,
            [(13.40, 52.52), (13.42, 52.54)],
        );
        insert_test_activity(
            &db,
            &config,
            "too far",
            1_800_000_000,
            [(13.60, 52.70), (13.62, 52.72)],
        );

        let features = inspect(
            &db,
            &config,
            &ActivityFilter::default(),
            CENTER_POINT,
            25.0,
            14,
        );
        let titles: Vec<_> = features
            .iter()
            .map(|f| f.properties.title.as_deref().unwrap())
            .collect();
        assert_eq!(titles, ["newer", "older"]);

        let props = &features[0].properties;
        assert_eq!(props.total_distance, Some(2.6));
        assert_eq!(props.elevation_gain, None);
        assert_eq!(
            props.start_time,
            Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap())
        );
    }

    #[test]
    fn test_respects_filter() {
        let (db, config) = setup();
        insert_test_activity(
            &db,
            &config,
            "activity title",
            1_700_000_000,
            [(13.40, 52.52), (13.42, 52.54)],
        );

        let before = Date::from_calendar_date(2000, Month::January, 1).unwrap();
        let filter = ActivityFilter::new(Some(before), None, None);
        assert!(inspect(&db, &config, &filter, CENTER_POINT, 25.0, 14).is_empty());
    }

    #[test]
    fn test_respects_masks() {
        let (db, mut config) = setup();
        insert_test_activity(
            &db,
            &config,
            "activity title",
            0,
            [(13.40, 52.52), (13.42, 52.54)],
        );
        config.activity_mask.push(ActivityMask {
            name: "home".to_string(),
            lat: CENTER_POINT.1,
            lng: CENTER_POINT.0,
            radius: 300.0,
        });

        let filter = ActivityFilter::default();
        assert!(inspect(&db, &config, &filter, CENTER_POINT, 6.0, 16).is_empty());

        let features = inspect(&db, &config, &filter, (13.40, 52.5201), 6.0, 16);
        assert_eq!(features.len(), 1);

        for (lng, lat) in points(&features[0]) {
            let dx = (lng - CENTER_POINT.0) * 111_320.0 * CENTER_POINT.1.to_radians().cos();
            let dy = (lat - CENTER_POINT.1) * 110_574.0;
            assert!(
                (dx * dx + dy * dy).sqrt() > 300.0,
                "masked point leaked: {lng},{lat}"
            );
        }
    }
}
