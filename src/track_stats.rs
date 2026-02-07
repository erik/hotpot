use geo::HaversineDistance;
use geo_types::Point;

pub struct TrackPoint {
    pub lat: f64,
    pub lng: f64,
    pub elevation: Option<f64>,
    pub timestamp: Option<i64>,
}

pub struct TrackStats {
    pub total_distance: Option<f64>,
    pub elapsed_time: Option<i64>,
    pub moving_time: Option<i64>,
    pub elevation_gain: Option<f64>,
    pub elevation_loss: Option<f64>,
    pub max_elevation: Option<f64>,
    pub min_elevation: Option<f64>,
}

/// Minimum elevation change (in meters) to count as real gain/loss.
/// Filters GPS elevation noise.
const ELEVATION_THRESHOLD: f64 = 2.0;

/// Maximum distance (meters) between consecutive points before we consider
/// it a teleport/transport jump and exclude it from distance totals.
/// Matches RawActivity::MAX_POINT_DISTANCE used in tile clipping.
const MAX_SEGMENT_DISTANCE: f64 = 5000.0;

/// Maximum time gap (seconds) between consecutive points before we consider
/// it a pause and exclude it from moving time.
const MAX_TIME_GAP: i64 = 300;

pub fn compute_stats(points: &[TrackPoint]) -> TrackStats {
    TrackStats {
        total_distance: compute_distance(points),
        elapsed_time: compute_elapsed_time(points),
        moving_time: compute_moving_time(points),
        elevation_gain: compute_elevation_gain(points),
        elevation_loss: compute_elevation_loss(points),
        max_elevation: compute_max_elevation(points),
        min_elevation: compute_min_elevation(points),
    }
}

fn segment_distance(a: &TrackPoint, b: &TrackPoint) -> f64 {
    let pa = Point::new(a.lng, a.lat);
    let pb = Point::new(b.lng, b.lat);
    pa.haversine_distance(&pb)
}

fn compute_distance(points: &[TrackPoint]) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }

    let total: f64 = points
        .windows(2)
        .map(|w| segment_distance(&w[0], &w[1]))
        .filter(|d| *d <= MAX_SEGMENT_DISTANCE)
        .sum();

    Some(total)
}

fn compute_elapsed_time(points: &[TrackPoint]) -> Option<i64> {
    let first = points.iter().find_map(|p| p.timestamp)?;
    let last = points.iter().rev().find_map(|p| p.timestamp)?;
    let duration = last - first;
    if duration > 0 { Some(duration) } else { None }
}

/// Sum of time gaps between consecutive points, excluding gaps that
/// look like pauses (time gap > MAX_TIME_GAP) or transport jumps
/// (distance > MAX_SEGMENT_DISTANCE).
fn compute_moving_time(points: &[TrackPoint]) -> Option<i64> {
    if points.len() < 2 {
        return None;
    }

    let mut total: i64 = 0;
    let mut any = false;

    for w in points.windows(2) {
        let (Some(t0), Some(t1)) = (w[0].timestamp, w[1].timestamp) else {
            continue;
        };

        let gap = t1 - t0;
        if gap <= 0 {
            continue;
        }

        let dist = segment_distance(&w[0], &w[1]);
        if dist > MAX_SEGMENT_DISTANCE {
            continue;
        }
        if gap > MAX_TIME_GAP {
            continue;
        }

        total += gap;
        any = true;
    }

    if any { Some(total) } else { None }
}

/// Accumulate positive elevation changes using threshold-based smoothing.
fn compute_elevation_gain(points: &[TrackPoint]) -> Option<f64> {
    let elevations: Vec<f64> = points.iter().filter_map(|p| p.elevation).collect();
    if elevations.len() < 2 {
        return None;
    }

    let mut gain = 0.0;
    let mut reference = elevations[0];

    for &elev in &elevations[1..] {
        let diff = elev - reference;
        if diff >= ELEVATION_THRESHOLD {
            gain += diff;
            reference = elev;
        } else if diff <= -ELEVATION_THRESHOLD {
            reference = elev;
        }
    }

    Some(gain)
}

/// Accumulate negative elevation changes using threshold-based smoothing.
fn compute_elevation_loss(points: &[TrackPoint]) -> Option<f64> {
    let elevations: Vec<f64> = points.iter().filter_map(|p| p.elevation).collect();
    if elevations.len() < 2 {
        return None;
    }

    let mut loss = 0.0;
    let mut reference = elevations[0];

    for &elev in &elevations[1..] {
        let diff = elev - reference;
        if diff <= -ELEVATION_THRESHOLD {
            loss += diff.abs();
            reference = elev;
        } else if diff >= ELEVATION_THRESHOLD {
            reference = elev;
        }
    }

    Some(loss)
}

fn compute_max_elevation(points: &[TrackPoint]) -> Option<f64> {
    points
        .iter()
        .filter_map(|p| p.elevation)
        .max_by(|a, b| a.partial_cmp(b).unwrap())
}

fn compute_min_elevation(points: &[TrackPoint]) -> Option<f64> {
    points
        .iter()
        .filter_map(|p| p.elevation)
        .min_by(|a, b| a.partial_cmp(b).unwrap())
}

impl TrackStats {
    /// Merge derived stats into a properties map, only setting keys that
    /// are not already present (file-provided values take precedence).
    pub fn merge_into(&self, properties: &mut std::collections::HashMap<String, serde_json::Value>) {
        let entries: &[(&str, Option<serde_json::Value>)] = &[
            (
                "total_distance",
                self.total_distance
                    .map(|v| serde_json::Value::from(v.round() as i64)),
            ),
            (
                "elapsed_time",
                self.elapsed_time.map(serde_json::Value::from),
            ),
            (
                "moving_time",
                self.moving_time.map(serde_json::Value::from),
            ),
            (
                "elevation_gain",
                self.elevation_gain
                    .map(|v| serde_json::Value::from(v.round() as i64)),
            ),
            (
                "elevation_loss",
                self.elevation_loss
                    .map(|v| serde_json::Value::from(v.round() as i64)),
            ),
            (
                "max_elevation",
                self.max_elevation
                    .map(|v| serde_json::Value::from(v.round() as i64)),
            ),
            (
                "min_elevation",
                self.min_elevation
                    .map(|v| serde_json::Value::from(v.round() as i64)),
            ),
        ];

        for (key, value) in entries {
            if let Some(val) = value {
                properties.entry(key.to_string()).or_insert(val.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_stats_empty() {
        let stats = compute_stats(&[]);
        assert!(stats.total_distance.is_none());
        assert!(stats.elapsed_time.is_none());
        assert!(stats.moving_time.is_none());
        assert!(stats.elevation_gain.is_none());
    }

    #[test]
    fn test_compute_stats_single_point() {
        let points = vec![TrackPoint {
            lat: 52.5,
            lng: 13.4,
            elevation: Some(50.0),
            timestamp: Some(1000),
        }];
        let stats = compute_stats(&points);
        assert!(stats.total_distance.is_none());
        assert!(stats.elapsed_time.is_none());
        assert!(stats.moving_time.is_none());
        assert!(stats.elevation_gain.is_none());
    }

    #[test]
    fn test_compute_distance() {
        // Two points ~100m apart (close enough to not be filtered)
        let points = vec![
            TrackPoint { lat: 52.5200, lng: 13.4050, elevation: None, timestamp: None },
            TrackPoint { lat: 52.5209, lng: 13.4050, elevation: None, timestamp: None },
        ];
        let stats = compute_stats(&points);
        let dist = stats.total_distance.unwrap();
        assert!((dist - 100.0).abs() < 5.0, "distance was {}", dist);
    }

    #[test]
    fn test_distance_skips_teleport_jumps() {
        // Three points: A -> B (100m) -> C (100km away, should be skipped)
        let points = vec![
            TrackPoint { lat: 52.5200, lng: 13.4050, elevation: None, timestamp: None },
            TrackPoint { lat: 52.5209, lng: 13.4050, elevation: None, timestamp: None },
            TrackPoint { lat: 53.5200, lng: 13.4050, elevation: None, timestamp: None },
        ];
        let stats = compute_stats(&points);
        let dist = stats.total_distance.unwrap();
        // Only the first segment (~100m) should count, the 100km jump is filtered
        assert!(dist < 200.0, "distance should exclude the jump, was {}", dist);
    }

    #[test]
    fn test_elapsed_time() {
        let points = vec![
            TrackPoint { lat: 0.0, lng: 0.0, elevation: None, timestamp: Some(1000) },
            TrackPoint { lat: 0.0, lng: 0.0, elevation: None, timestamp: Some(1300) },
        ];
        let stats = compute_stats(&points);
        assert_eq!(stats.elapsed_time, Some(300));
    }

    #[test]
    fn test_moving_time_excludes_pauses() {
        // Simulate: ride 60s, pause 600s (>MAX_TIME_GAP), ride 60s
        let points = vec![
            TrackPoint { lat: 52.5200, lng: 13.4050, elevation: None, timestamp: Some(1000) },
            TrackPoint { lat: 52.5205, lng: 13.4050, elevation: None, timestamp: Some(1060) },
            // 600 second gap = pause
            TrackPoint { lat: 52.5210, lng: 13.4050, elevation: None, timestamp: Some(1660) },
            TrackPoint { lat: 52.5215, lng: 13.4050, elevation: None, timestamp: Some(1720) },
        ];
        let stats = compute_stats(&points);
        // Elapsed: 1720 - 1000 = 720s
        assert_eq!(stats.elapsed_time, Some(720));
        // Moving: 60s + 60s = 120s (the 600s gap is excluded)
        assert_eq!(stats.moving_time, Some(120));
    }

    #[test]
    fn test_moving_time_excludes_transport_jumps() {
        // Simulate: ride nearby, then teleport far away
        let points = vec![
            TrackPoint { lat: 52.5200, lng: 13.4050, elevation: None, timestamp: Some(1000) },
            TrackPoint { lat: 52.5205, lng: 13.4050, elevation: None, timestamp: Some(1060) },
            // Jump to a point >5km away with a 120s gap (within time threshold but beyond distance)
            TrackPoint { lat: 53.5200, lng: 13.4050, elevation: None, timestamp: Some(1180) },
            TrackPoint { lat: 53.5205, lng: 13.4050, elevation: None, timestamp: Some(1240) },
        ];
        let stats = compute_stats(&points);
        // Moving time: 60s + 60s = 120s (the transport jump segment is excluded)
        assert_eq!(stats.moving_time, Some(120));
    }

    #[test]
    fn test_elevation_gain_with_threshold() {
        // 50 -> 53 (+3, above threshold) -> 52 (-1, below threshold) -> 55 (+3 from 53)
        let points = vec![
            TrackPoint { lat: 0.0, lng: 0.0, elevation: Some(50.0), timestamp: None },
            TrackPoint { lat: 0.0, lng: 0.0, elevation: Some(53.0), timestamp: None },
            TrackPoint { lat: 0.0, lng: 0.0, elevation: Some(52.0), timestamp: None },
            TrackPoint { lat: 0.0, lng: 0.0, elevation: Some(55.0), timestamp: None },
        ];
        let stats = compute_stats(&points);
        // 50->53 = +3, then reference stays at 53 (52 is <2m drop), 53->55 = +2
        assert_eq!(stats.elevation_gain.unwrap(), 5.0);
    }

    #[test]
    fn test_elevation_loss_with_threshold() {
        // 55 -> 52 (-3, above threshold) -> 53 (+1, below threshold) -> 50 (-2 from 52)
        let points = vec![
            TrackPoint { lat: 0.0, lng: 0.0, elevation: Some(55.0), timestamp: None },
            TrackPoint { lat: 0.0, lng: 0.0, elevation: Some(52.0), timestamp: None },
            TrackPoint { lat: 0.0, lng: 0.0, elevation: Some(53.0), timestamp: None },
            TrackPoint { lat: 0.0, lng: 0.0, elevation: Some(50.0), timestamp: None },
        ];
        let stats = compute_stats(&points);
        // 55->52 = -3, then reference stays at 52 (53 is <2m rise), 52->50 = -2
        assert_eq!(stats.elevation_loss.unwrap(), 5.0);
    }

    #[test]
    fn test_min_max_elevation() {
        let points = vec![
            TrackPoint { lat: 0.0, lng: 0.0, elevation: Some(100.0), timestamp: None },
            TrackPoint { lat: 0.0, lng: 0.0, elevation: Some(200.0), timestamp: None },
            TrackPoint { lat: 0.0, lng: 0.0, elevation: Some(50.0), timestamp: None },
        ];
        let stats = compute_stats(&points);
        assert_eq!(stats.max_elevation.unwrap(), 200.0);
        assert_eq!(stats.min_elevation.unwrap(), 50.0);
    }

    #[test]
    fn test_merge_does_not_overwrite() {
        let stats = TrackStats {
            total_distance: Some(5000.0),
            elapsed_time: Some(3600),
            moving_time: Some(3000),
            elevation_gain: Some(100.0),
            elevation_loss: Some(80.0),
            max_elevation: Some(500.0),
            min_elevation: Some(400.0),
        };
        let mut props = std::collections::HashMap::new();
        props.insert("total_distance".to_string(), serde_json::json!(9999));

        stats.merge_into(&mut props);

        // Existing value should be preserved
        assert_eq!(props["total_distance"], serde_json::json!(9999));
        // New values should be added
        assert_eq!(props["elapsed_time"], serde_json::json!(3600));
        assert_eq!(props["moving_time"], serde_json::json!(3000));
        assert_eq!(props["elevation_gain"], serde_json::json!(100));
    }

    #[test]
    fn test_no_elevation_data() {
        let points = vec![
            TrackPoint { lat: 52.5200, lng: 13.4050, elevation: None, timestamp: Some(1000) },
            TrackPoint { lat: 52.5209, lng: 13.4050, elevation: None, timestamp: Some(1060) },
        ];
        let stats = compute_stats(&points);
        assert!(stats.total_distance.is_some());
        assert!(stats.elapsed_time.is_some());
        assert!(stats.moving_time.is_some());
        assert!(stats.elevation_gain.is_none());
        assert!(stats.elevation_loss.is_none());
        assert!(stats.max_elevation.is_none());
        assert!(stats.min_elevation.is_none());
    }
}
