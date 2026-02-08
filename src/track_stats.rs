use geo::HaversineDistance;
use geo_types::Point;

use crate::activity::MAX_POINT_DISTANCE;

pub struct TrackPoint {
    pub point: Point,
    pub elevation: Option<f64>,
    pub timestamp: Option<i64>,
}

impl TrackPoint {
    /// Distance (meters) between this point and another
    fn distance(&self, other: &TrackPoint) -> f64 {
        self.point.haversine_distance(&other.point)
    }
}

pub struct TrackStats {
    // TODO: meters, convert to km?
    pub total_distance: Option<f64>,
    pub elapsed_time: Option<i64>,
    pub moving_time: Option<i64>,
    pub elevation_gain: Option<f64>,
    pub elevation_loss: Option<f64>,
    pub min_elevation: Option<f64>,
    pub max_elevation: Option<f64>,
    pub average_speed: Option<f64>,
    pub max_speed: Option<f64>,
}

impl TrackStats {
    pub fn from_points(points: &[TrackPoint]) -> Self {
        let speed_time = TimeSpeedStats::from_points(points);
        let elevation = ElevationStats::from_points(points);
        let distance = compute_distance(points);

        TrackStats {
            total_distance: distance,
            elapsed_time: speed_time.map(|f| f.elapsed_time),
            moving_time: speed_time.map(|f| f.moving_time),
            elevation_gain: elevation.map(|t| t.gain),
            elevation_loss: elevation.map(|t| t.loss),
            min_elevation: elevation.map(|t| t.min_val),
            max_elevation: elevation.map(|t| t.max_val),
            average_speed: speed_time
                .zip(distance)
                .map(|(t, dist)| (dist / t.moving_time as f64) * METERS_PER_SEC_TO_KMH),
            max_speed: speed_time.map(|t| t.max_speed),
        }
    }

    /// Merge derived stats into a properties map, only setting keys that
    /// are not already present (file-provided values take precedence).
    pub fn merge_into(
        &self,
        properties: &mut std::collections::HashMap<String, serde_json::Value>,
    ) {
        let entries: [(&str, serde_json::Value); 9] = [
            ("total_distance", self.total_distance.into()),
            ("elapsed_time", self.elapsed_time.into()),
            ("moving_time", self.moving_time.into()),
            ("elevation_gain", self.elevation_gain.into()),
            ("elevation_loss", self.elevation_loss.into()),
            ("min_elevation", self.min_elevation.into()),
            ("max_elevation", self.max_elevation.into()),
            ("average_speed", self.average_speed.into()),
            ("max_speed", self.max_speed.into()),
        ];

        for (key, value) in entries.into_iter() {
            if !value.is_null() {
                properties.entry(key.to_string()).or_insert(value);
            }
        }
    }
}

/// Minimum elevation change (in meters) to count as real gain/loss.
/// Filters GPS elevation noise.
const ELEVATION_CHANGE_THRESHOLD: f64 = 2.0;

/// Max time (seconds) between two GPS points before we consider it a pause in
/// the recording
const PAUSE_THRESHOLD_SECS: i64 = 60;

/// Meters per second to kilometers per hour.
pub const METERS_PER_SEC_TO_KMH: f64 = 3.6;

fn compute_distance(points: &[TrackPoint]) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }

    let total: f64 = points
        .windows(2)
        .map(|w| w[0].distance(&w[1]))
        .filter(|d| *d <= MAX_POINT_DISTANCE)
        .sum();

    Some(total)
}

#[derive(Copy, Clone)]
struct TimeSpeedStats {
    max_speed: f64,
    moving_time: i64,
    elapsed_time: i64,
}

impl TimeSpeedStats {
    fn from_points(points: &[TrackPoint]) -> Option<Self> {
        if points.len() < 2 {
            return None;
        }

        let first = points.iter().find_map(|p| p.timestamp)?;
        let last = points.iter().rev().find_map(|p| p.timestamp)?;

        let elapsed_time = last - first;
        let mut max_speed: f64 = 0.0;
        let mut moving_time: i64 = 0;

        for w in points.windows(2) {
            let (Some(start_time), Some(end_time)) = (w[0].timestamp, w[1].timestamp) else {
                continue;
            };

            let time_diff = end_time - start_time;
            if time_diff <= 0 || time_diff > PAUSE_THRESHOLD_SECS {
                continue;
            }

            let dist_diff = w[0].distance(&w[1]);
            if dist_diff > MAX_POINT_DISTANCE {
                continue;
            }

            let speed = dist_diff / time_diff as f64 * METERS_PER_SEC_TO_KMH;
            max_speed = max_speed.max(speed);
            moving_time += time_diff;
        }

        Some(TimeSpeedStats {
            max_speed,
            moving_time,
            elapsed_time,
        })
    }
}

#[derive(Copy, Clone)]
struct ElevationStats {
    gain: f64,
    loss: f64,
    min_val: f64,
    max_val: f64,
}

impl ElevationStats {
    fn from_points(points: &[TrackPoint]) -> Option<Self> {
        let elevations: Vec<f64> = points.iter().filter_map(|p| p.elevation).collect();
        if elevations.len() < 2 {
            return None;
        }

        let mut base = elevations[0];
        let mut gain = 0.0;
        let mut loss = 0.0;
        let mut min_val = base;
        let mut max_val = base;

        for &elev in &elevations[1..] {
            min_val = min_val.min(elev);
            max_val = max_val.max(elev);

            let diff = elev - base;
            if diff >= ELEVATION_CHANGE_THRESHOLD {
                gain += diff;
                base = elev;
            } else if diff <= -ELEVATION_CHANGE_THRESHOLD {
                loss += diff.abs();
                base = elev;
            }
        }

        Some(ElevationStats {
            gain,
            loss,
            min_val,
            max_val,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trackpoint(
        lat: f64,
        lng: f64,
        elevation: Option<f64>,
        timestamp: Option<i64>,
    ) -> TrackPoint {
        TrackPoint {
            point: Point::new(lng, lat),
            elevation,
            timestamp,
        }
    }

    #[test]
    fn test_compute_stats_empty() {
        let stats = TrackStats::from_points(&[]);
        assert!(stats.total_distance.is_none());
        assert!(stats.elapsed_time.is_none());
        assert!(stats.moving_time.is_none());
        assert!(stats.elevation_gain.is_none());
        assert!(stats.elevation_loss.is_none());
        assert!(stats.min_elevation.is_none());
        assert!(stats.max_elevation.is_none());
        assert!(stats.average_speed.is_none());
        assert!(stats.max_speed.is_none());
    }

    #[test]
    fn test_compute_stats_single_point() {
        let points = vec![trackpoint(52.5, 13.4, Some(50.0), Some(1000))];
        let stats = TrackStats::from_points(&points);
        assert!(stats.total_distance.is_none());
        assert!(stats.elapsed_time.is_none());
        assert!(stats.moving_time.is_none());
        assert!(stats.elevation_gain.is_none());
        assert!(stats.elevation_loss.is_none());
        assert!(stats.min_elevation.is_none());
        assert!(stats.max_elevation.is_none());
        assert!(stats.average_speed.is_none());
        assert!(stats.max_speed.is_none());
    }

    #[test]
    fn test_compute_distance() {
        // Two points ~100m apart (close enough to not be filtered)
        let points = vec![
            trackpoint(52.5200, 13.4050, None, None),
            trackpoint(52.5209, 13.4050, None, None),
        ];
        let stats = TrackStats::from_points(&points);
        let dist = stats.total_distance.unwrap();
        assert!((dist - 100.0).abs() < 5.0, "distance was {}", dist);
    }

    #[test]
    fn test_distance_skips_teleport_jumps() {
        // Three points: A -> B (100m) -> C (100km away, should be skipped)
        let points = vec![
            trackpoint(52.5200, 13.4050, None, None),
            trackpoint(52.5209, 13.4050, None, None),
            trackpoint(53.5200, 13.4050, None, None),
        ];
        let stats = TrackStats::from_points(&points);
        let dist = stats.total_distance.unwrap();
        // Only the first segment (~100m) should count, the 100km jump is filtered
        assert!(
            dist < 200.0,
            "distance should exclude the jump, was {}",
            dist
        );
    }

    #[test]
    fn test_elapsed_time() {
        let points = vec![
            trackpoint(0.0, 0.0, None, Some(1000)),
            trackpoint(0.0, 0.0, None, Some(1300)),
        ];
        let stats = TrackStats::from_points(&points);
        assert_eq!(stats.elapsed_time, Some(300));
    }

    #[test]
    fn test_moving_time_excludes_pauses() {
        // Simulate: ride 10s, pause 120s (>MAX_TIME_GAP), ride 10s
        let points = vec![
            trackpoint(52.5200, 13.4050, None, Some(1000)),
            trackpoint(52.5205, 13.4050, None, Some(1010)),
            // 120 second gap = pause
            trackpoint(52.5210, 13.4050, None, Some(1130)),
            trackpoint(52.5215, 13.4050, None, Some(1140)),
        ];
        let stats = TrackStats::from_points(&points);
        // Elapsed: 1140 - 1000 = 140s
        assert_eq!(stats.elapsed_time, Some(140));
        // Moving: 10s + 10s = 20s (the 120s gap is excluded)
        assert_eq!(stats.moving_time, Some(20));
    }

    #[test]
    fn test_moving_time_excludes_transport_jumps() {
        // Simulate: ride nearby, then teleport far away
        let points = vec![
            trackpoint(52.5200, 13.4050, None, Some(1000)),
            trackpoint(52.5205, 13.4050, None, Some(1010)),
            // Jump to a point >5km away with a 30s gap (within time threshold but beyond distance)
            trackpoint(53.5200, 13.4050, None, Some(1040)),
            trackpoint(53.5205, 13.4050, None, Some(1050)),
        ];
        let stats = TrackStats::from_points(&points);
        // Moving time: 10s + 10s = 20s (the transport jump segment is excluded)
        assert_eq!(stats.moving_time, Some(20));
    }

    #[test]
    fn test_elevation_gain_loss_with_threshold() {
        // 50 -> 53 (+3) -> 52 (-1, below threshold) -> 55 (+2) -> 50 (-5)
        let points = vec![
            trackpoint(0.0, 0.0, Some(50.0), None),
            trackpoint(0.0, 0.0, Some(53.0), None),
            trackpoint(0.0, 0.0, Some(52.0), None),
            trackpoint(0.0, 0.0, Some(55.0), None),
            trackpoint(0.0, 0.0, Some(50.0), None),
        ];
        let stats = TrackStats::from_points(&points);
        // gain: 50->53 (+3), 53->55 (+2) = 5
        // loss: 55->50 (-5) = 5
        assert_eq!(stats.elevation_gain.unwrap(), 5.0);
        assert_eq!(stats.elevation_loss.unwrap(), 5.0);
        assert_eq!(stats.min_elevation.unwrap(), 50.0);
        assert_eq!(stats.max_elevation.unwrap(), 55.0);
    }

    #[test]
    fn test_speed() {
        // Two points ~100m apart, 10s gap => 10 m/s => 36 km/h
        let points = vec![
            trackpoint(52.5200, 13.4050, None, Some(1000)),
            trackpoint(52.5209, 13.4050, None, Some(1010)),
        ];
        let stats = TrackStats::from_points(&points);
        let avg = stats.average_speed.unwrap();
        let max = stats.max_speed.unwrap();
        assert!((avg - 36.0).abs() < 2.0, "average_speed was {}", avg);
        assert!((max - 36.0).abs() < 2.0, "max_speed was {}", max);
    }

    #[test]
    fn test_max_speed_ignores_jumps() {
        // Normal segment, then a teleport jump that would be absurdly fast
        let points = vec![
            trackpoint(52.5200, 13.4050, None, Some(1000)),
            trackpoint(52.5209, 13.4050, None, Some(1010)),
            // 100km away in 60s = would be 6000 km/h, but filtered out
            trackpoint(53.5200, 13.4050, None, Some(1070)),
        ];
        let stats = TrackStats::from_points(&points);
        let max = stats.max_speed.unwrap();
        assert!(
            max < 100.0,
            "max_speed should exclude teleport, was {}",
            max
        );
    }

    #[test]
    fn test_merge_does_not_overwrite() {
        let stats = TrackStats {
            total_distance: Some(5000.0),
            elapsed_time: Some(3600),
            moving_time: Some(3000),
            elevation_gain: Some(100.0),
            elevation_loss: Some(80.0),
            min_elevation: Some(400.0),
            max_elevation: Some(500.0),
            average_speed: Some(25.0),
            max_speed: Some(45.0),
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
            trackpoint(52.5200, 13.4050, None, Some(1000)),
            trackpoint(52.5209, 13.4050, None, Some(1060)),
        ];
        let stats = TrackStats::from_points(&points);
        assert!(stats.total_distance.is_some());
        assert!(stats.elapsed_time.is_some());
        assert!(stats.moving_time.is_some());
        assert!(stats.elevation_gain.is_none());
        assert!(stats.elevation_loss.is_none());
        assert!(stats.min_elevation.is_none());
        assert!(stats.max_elevation.is_none());
    }
}
