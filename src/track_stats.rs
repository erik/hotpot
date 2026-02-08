use geo::HaversineDistance;
use geo_types::{LineString, Point};

pub struct TrackPoint {
    pub point: Point,
    pub elevation: Option<f64>,
    pub timestamp: Option<i64>,
}

pub fn to_line_string(points: &[TrackPoint]) -> LineString {
    points.iter().map(|p| p.point).collect()
}

pub struct TrackStats {
    pub total_distance: Option<f64>,
    pub elapsed_time: Option<i64>,
    pub moving_time: Option<i64>,
    /// (gain, loss) in meters, with threshold-based smoothing
    pub elevation_gain_loss: Option<(f64, f64)>,
    /// (min, max) in meters
    pub elevation_range: Option<(f64, f64)>,
    /// (avg, max) in km/h
    pub speed: Option<(f64, f64)>,
}

/// Minimum elevation change (in meters) to count as real gain/loss.
/// Filters GPS elevation noise.
const ELEVATION_THRESHOLD: f64 = 2.0;

/// Maximum distance (meters) between consecutive points before we consider
/// it a teleport/transport jump and exclude it from distance totals.
/// Matches RawActivity::MAX_POINT_DISTANCE used in tile clipping.
const MAX_SEGMENT_DISTANCE: f64 = 5000.0;

/// Maximum time gap (seconds) between consecutive points before we consider
/// it a pause and exclude it from moving time. Most GPS devices record every
/// 1-5s (up to ~30s with smart recording), so 60s comfortably exceeds any
/// recording interval while catching actual stops.
const MAX_TIME_GAP: i64 = 60;

/// Meters per second to kilometers per hour.
const MPS_TO_KMH: f64 = 3.6;

pub fn compute_stats(points: &[TrackPoint]) -> TrackStats {
    let total_distance = compute_distance(points);
    let moving_time = compute_moving_time(points);
    let max_speed = compute_max_speed(points);

    let average_speed = match (total_distance, moving_time) {
        (Some(d), Some(t)) if t > 0 => Some(d / t as f64 * MPS_TO_KMH),
        _ => None,
    };

    let speed = match (average_speed, max_speed) {
        (Some(avg), Some(max)) => Some((avg, max)),
        (Some(avg), None) => Some((avg, avg)),
        _ => None,
    };

    TrackStats {
        total_distance,
        elapsed_time: compute_elapsed_time(points),
        moving_time,
        elevation_gain_loss: compute_elevation_gain_loss(points),
        elevation_range: compute_elevation_range(points),
        speed,
    }
}

fn segment_distance(a: &TrackPoint, b: &TrackPoint) -> f64 {
    a.point.haversine_distance(&b.point)
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

/// Highest instantaneous speed (km/h) across valid segments.
fn compute_max_speed(points: &[TrackPoint]) -> Option<f64> {
    let mut max: f64 = 0.0;

    for w in points.windows(2) {
        let (Some(t0), Some(t1)) = (w[0].timestamp, w[1].timestamp) else {
            continue;
        };
        let gap = t1 - t0;
        if gap <= 0 || gap > MAX_TIME_GAP {
            continue;
        }
        let dist = segment_distance(&w[0], &w[1]);
        if dist > MAX_SEGMENT_DISTANCE {
            continue;
        }
        let speed = dist / gap as f64 * MPS_TO_KMH;
        max = max.max(speed);
    }

    if max > 0.0 { Some(max) } else { None }
}

/// Accumulate elevation gain and loss in a single pass using threshold-based smoothing.
fn compute_elevation_gain_loss(points: &[TrackPoint]) -> Option<(f64, f64)> {
    let elevations: Vec<f64> = points.iter().filter_map(|p| p.elevation).collect();
    if elevations.len() < 2 {
        return None;
    }

    let mut gain = 0.0;
    let mut loss = 0.0;
    let mut reference = elevations[0];

    for &elev in &elevations[1..] {
        let diff = elev - reference;
        if diff >= ELEVATION_THRESHOLD {
            gain += diff;
            reference = elev;
        } else if diff <= -ELEVATION_THRESHOLD {
            loss += diff.abs();
            reference = elev;
        }
    }

    Some((gain, loss))
}

/// Find min and max elevation in a single pass.
fn compute_elevation_range(points: &[TrackPoint]) -> Option<(f64, f64)> {
    let mut iter = points.iter().filter_map(|p| p.elevation);
    let first = iter.next()?;

    let (min, max) = iter.fold((first, first), |(min, max), v| {
        (min.min(v), max.max(v))
    });

    Some((min, max))
}

impl TrackStats {
    /// Merge derived stats into a properties map, only setting keys that
    /// are not already present (file-provided values take precedence).
    pub fn merge_into(&self, properties: &mut std::collections::HashMap<String, serde_json::Value>) {
        let f = serde_json::Value::from;

        let entries: &[(&str, Option<serde_json::Value>)] = &[
            ("total_distance", self.total_distance.map(f)),
            ("elapsed_time", self.elapsed_time.map(serde_json::Value::from)),
            ("moving_time", self.moving_time.map(serde_json::Value::from)),
            ("elevation_gain", self.elevation_gain_loss.map(|(g, _)| f(g))),
            ("elevation_loss", self.elevation_gain_loss.map(|(_, l)| f(l))),
            ("min_elevation", self.elevation_range.map(|(min, _)| f(min))),
            ("max_elevation", self.elevation_range.map(|(_, max)| f(max))),
            ("average_speed", self.speed.map(|(avg, _)| f(avg))),
            ("max_speed", self.speed.map(|(_, max)| f(max))),
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

    fn tp(lat: f64, lng: f64, elevation: Option<f64>, timestamp: Option<i64>) -> TrackPoint {
        TrackPoint { point: Point::new(lng, lat), elevation, timestamp }
    }

    #[test]
    fn test_compute_stats_empty() {
        let stats = compute_stats(&[]);
        assert!(stats.total_distance.is_none());
        assert!(stats.elapsed_time.is_none());
        assert!(stats.moving_time.is_none());
        assert!(stats.elevation_gain_loss.is_none());
        assert!(stats.elevation_range.is_none());
        assert!(stats.speed.is_none());
    }

    #[test]
    fn test_compute_stats_single_point() {
        let points = vec![tp(52.5, 13.4, Some(50.0), Some(1000))];
        let stats = compute_stats(&points);
        assert!(stats.total_distance.is_none());
        assert!(stats.elapsed_time.is_none());
        assert!(stats.moving_time.is_none());
        assert!(stats.elevation_gain_loss.is_none());
        // Single point still has a valid elevation range (min == max)
        assert_eq!(stats.elevation_range, Some((50.0, 50.0)));
        assert!(stats.speed.is_none());
    }

    #[test]
    fn test_compute_distance() {
        // Two points ~100m apart (close enough to not be filtered)
        let points = vec![
            tp(52.5200, 13.4050, None, None),
            tp(52.5209, 13.4050, None, None),
        ];
        let stats = compute_stats(&points);
        let dist = stats.total_distance.unwrap();
        assert!((dist - 100.0).abs() < 5.0, "distance was {}", dist);
    }

    #[test]
    fn test_distance_skips_teleport_jumps() {
        // Three points: A -> B (100m) -> C (100km away, should be skipped)
        let points = vec![
            tp(52.5200, 13.4050, None, None),
            tp(52.5209, 13.4050, None, None),
            tp(53.5200, 13.4050, None, None),
        ];
        let stats = compute_stats(&points);
        let dist = stats.total_distance.unwrap();
        // Only the first segment (~100m) should count, the 100km jump is filtered
        assert!(dist < 200.0, "distance should exclude the jump, was {}", dist);
    }

    #[test]
    fn test_elapsed_time() {
        let points = vec![
            tp(0.0, 0.0, None, Some(1000)),
            tp(0.0, 0.0, None, Some(1300)),
        ];
        let stats = compute_stats(&points);
        assert_eq!(stats.elapsed_time, Some(300));
    }

    #[test]
    fn test_moving_time_excludes_pauses() {
        // Simulate: ride 10s, pause 120s (>MAX_TIME_GAP), ride 10s
        let points = vec![
            tp(52.5200, 13.4050, None, Some(1000)),
            tp(52.5205, 13.4050, None, Some(1010)),
            // 120 second gap = pause
            tp(52.5210, 13.4050, None, Some(1130)),
            tp(52.5215, 13.4050, None, Some(1140)),
        ];
        let stats = compute_stats(&points);
        // Elapsed: 1140 - 1000 = 140s
        assert_eq!(stats.elapsed_time, Some(140));
        // Moving: 10s + 10s = 20s (the 120s gap is excluded)
        assert_eq!(stats.moving_time, Some(20));
    }

    #[test]
    fn test_moving_time_excludes_transport_jumps() {
        // Simulate: ride nearby, then teleport far away
        let points = vec![
            tp(52.5200, 13.4050, None, Some(1000)),
            tp(52.5205, 13.4050, None, Some(1010)),
            // Jump to a point >5km away with a 30s gap (within time threshold but beyond distance)
            tp(53.5200, 13.4050, None, Some(1040)),
            tp(53.5205, 13.4050, None, Some(1050)),
        ];
        let stats = compute_stats(&points);
        // Moving time: 10s + 10s = 20s (the transport jump segment is excluded)
        assert_eq!(stats.moving_time, Some(20));
    }

    #[test]
    fn test_elevation_gain_loss_with_threshold() {
        // 50 -> 53 (+3) -> 52 (-1, below threshold) -> 55 (+2) -> 50 (-5)
        let points = vec![
            tp(0.0, 0.0, Some(50.0), None),
            tp(0.0, 0.0, Some(53.0), None),
            tp(0.0, 0.0, Some(52.0), None),
            tp(0.0, 0.0, Some(55.0), None),
            tp(0.0, 0.0, Some(50.0), None),
        ];
        let stats = compute_stats(&points);
        // gain: 50->53 (+3), 53->55 (+2) = 5
        // loss: 55->50 (-5) = 5
        let (gain, loss) = stats.elevation_gain_loss.unwrap();
        assert_eq!(gain, 5.0);
        assert_eq!(loss, 5.0);
    }

    #[test]
    fn test_elevation_range() {
        let points = vec![
            tp(0.0, 0.0, Some(100.0), None),
            tp(0.0, 0.0, Some(200.0), None),
            tp(0.0, 0.0, Some(50.0), None),
        ];
        let stats = compute_stats(&points);
        let (min, max) = stats.elevation_range.unwrap();
        assert_eq!(min, 50.0);
        assert_eq!(max, 200.0);
    }

    #[test]
    fn test_speed() {
        // Two points ~100m apart, 10s gap => 10 m/s => 36 km/h
        let points = vec![
            tp(52.5200, 13.4050, None, Some(1000)),
            tp(52.5209, 13.4050, None, Some(1010)),
        ];
        let stats = compute_stats(&points);
        let (avg, max) = stats.speed.unwrap();
        assert!((avg - 36.0).abs() < 2.0, "average_speed was {}", avg);
        assert!((max - 36.0).abs() < 2.0, "max_speed was {}", max);
    }

    #[test]
    fn test_max_speed_ignores_jumps() {
        // Normal segment, then a teleport jump that would be absurdly fast
        let points = vec![
            tp(52.5200, 13.4050, None, Some(1000)),
            tp(52.5209, 13.4050, None, Some(1010)),
            // 100km away in 60s = would be 6000 km/h, but filtered out
            tp(53.5200, 13.4050, None, Some(1070)),
        ];
        let stats = compute_stats(&points);
        let (_, max) = stats.speed.unwrap();
        assert!(max < 100.0, "max_speed should exclude teleport, was {}", max);
    }

    #[test]
    fn test_merge_does_not_overwrite() {
        let stats = TrackStats {
            total_distance: Some(5000.0),
            elapsed_time: Some(3600),
            moving_time: Some(3000),
            elevation_gain_loss: Some((100.0, 80.0)),
            elevation_range: Some((400.0, 500.0)),
            speed: Some((25.0, 45.0)),
        };
        let mut props = std::collections::HashMap::new();
        props.insert("total_distance".to_string(), serde_json::json!(9999));

        stats.merge_into(&mut props);

        // Existing value should be preserved
        assert_eq!(props["total_distance"], serde_json::json!(9999));
        // New values should be added (raw f64, rounding happens at serialization)
        assert_eq!(props["elapsed_time"], serde_json::json!(3600));
        assert_eq!(props["moving_time"], serde_json::json!(3000));
        assert_eq!(props["elevation_gain"], serde_json::json!(100.0));
        assert_eq!(props["elevation_loss"], serde_json::json!(80.0));
        assert_eq!(props["min_elevation"], serde_json::json!(400.0));
        assert_eq!(props["max_elevation"], serde_json::json!(500.0));
        assert_eq!(props["average_speed"], serde_json::json!(25.0));
        assert_eq!(props["max_speed"], serde_json::json!(45.0));
    }

    #[test]
    fn test_no_elevation_data() {
        let points = vec![
            tp(52.5200, 13.4050, None, Some(1000)),
            tp(52.5209, 13.4050, None, Some(1060)),
        ];
        let stats = compute_stats(&points);
        assert!(stats.total_distance.is_some());
        assert!(stats.elapsed_time.is_some());
        assert!(stats.moving_time.is_some());
        assert!(stats.elevation_gain_loss.is_none());
        assert!(stats.elevation_range.is_none());
    }
}
