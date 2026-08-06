use std::collections::HashMap;

use geo::HaversineDistance;
use geo_types::Point;

use crate::activity::MAX_POINT_DISTANCE;

#[derive(Debug, PartialEq, Clone, Copy)]
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
    pub total_distance: Option<f64>, // km
    pub elapsed_time: Option<i64>,   // seconds
    pub moving_time: Option<i64>,    // seconds
    pub elevation_gain: Option<f64>, // meters
    pub elevation_loss: Option<f64>, // meters
    pub min_elevation: Option<f64>,  // meters
    pub max_elevation: Option<f64>,  // meters
    pub average_speed: Option<f64>,  // km/h
    pub max_speed: Option<f64>,      // km/h
}

impl TrackStats {
    pub fn from_points(points: &[TrackPoint]) -> Self {
        let base = BaseTrackStats::from_points(points);
        let elevation = ElevationStats::from_points(points);

        TrackStats {
            total_distance: base.map(|d| d.distance / 1000.0),
            elapsed_time: base.map(|f| f.elapsed_time),
            moving_time: base.map(|f| f.moving_time),
            elevation_gain: elevation.map(|t| t.gain),
            elevation_loss: elevation.map(|t| t.loss),
            min_elevation: elevation.map(|t| t.min_val),
            max_elevation: elevation.map(|t| t.max_val),
            average_speed: base
                .filter(|t| t.moving_time > 0)
                .map(|t| (t.distance / t.moving_time as f64) * METERS_PER_SEC_TO_KMH),
            max_speed: base.map(|t| t.max_speed),
        }
    }

    pub fn combine(self, other: TrackStats) -> Self {
        fn c<T, F: Fn(T, T) -> T>(a: Option<T>, b: Option<T>, f: F) -> Option<T> {
            match (a, b) {
                (Some(x), Some(y)) => Some(f(x, y)),
                (x, None) => x,
                (None, y) => y,
            }
        }
        let total_distance = c(self.total_distance, other.total_distance, |a, b| a + b);
        let moving_time = c(self.moving_time, other.moving_time, |a, b| a + b);

        TrackStats {
            total_distance,
            moving_time,
            elapsed_time: c(self.elapsed_time, other.elapsed_time, |a, b| a + b),
            elevation_gain: c(self.elevation_gain, other.elevation_gain, |a, b| a + b),
            elevation_loss: c(self.elevation_loss, other.elevation_loss, |a, b| a + b),
            min_elevation: c(self.min_elevation, other.min_elevation, f64::min),
            max_elevation: c(self.max_elevation, other.max_elevation, f64::max),
            average_speed: total_distance
                .zip(moving_time)
                .filter(|&(_, t)| t > 0)
                .map(|(d, t)| d / (t as f64 / 3600.0)),
            max_speed: c(self.max_speed, other.max_speed, f64::max),
        }
    }

    /// Merge derived stats into a properties map, overwriting existing keys so
    /// that we have have a consistent set of units. e.g. Strava activity export
    /// will have a "total_distance" in meters
    pub fn merge_into(&self, properties: &mut HashMap<String, serde_json::Value>) {
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
                properties.insert(key.to_string(), value);
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

#[derive(Copy, Clone)]
struct BaseTrackStats {
    distance: f64,
    max_speed: f64,
    moving_time: i64,
    elapsed_time: i64,
}

impl BaseTrackStats {
    fn from_points(points: &[TrackPoint]) -> Option<BaseTrackStats> {
        if points.len() < 2 {
            return None;
        }

        let mut total_distance = 0.0;
        let mut max_speed: f64 = 0.0;
        let mut moving_time: i64 = 0;

        for w in points.windows(2) {
            let dist = w[0].distance(&w[1]);

            // Large jumps are treated as gaps and excluded
            if dist > MAX_POINT_DISTANCE {
                continue;
            }

            total_distance += dist;

            let (Some(start_time), Some(end_time)) = (w[0].timestamp, w[1].timestamp) else {
                continue;
            };

            let time_diff = end_time - start_time;
            if time_diff <= 0 || time_diff > PAUSE_THRESHOLD_SECS {
                continue;
            }

            let speed = dist / time_diff as f64 * METERS_PER_SEC_TO_KMH;
            max_speed = max_speed.max(speed);
            moving_time += time_diff;
        }

        let first = points.iter().find_map(|p| p.timestamp).unwrap_or(0);
        let last = points.iter().rev().find_map(|p| p.timestamp).unwrap_or(0);

        Some(BaseTrackStats {
            distance: total_distance,
            max_speed,
            moving_time,
            elapsed_time: last - first,
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
        let mut elevations = points.iter().filter_map(|p| p.elevation);

        let mut base = elevations.next()?;
        let mut gain = 0.0;
        let mut loss = 0.0;
        let mut min_val = base;
        let mut max_val = base;

        for elev in elevations {
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
    fn test_compute_distance() {
        let points = vec![
            // Two points ~100m apart
            trackpoint(52.0, 13.0, None, None),
            trackpoint(52.0009, 13.0, None, None),
            // Point far away, should be skipped
            trackpoint(53.0, 13.0, None, None),
        ];
        let stats = TrackStats::from_points(&points);
        let dist = stats.total_distance.unwrap();
        assert!((dist - 0.100).abs() < 0.005, "distance was {}", dist);
    }

    #[test]
    fn test_elapsed_time() {
        let points = vec![
            trackpoint(0.0, 0.0, None, Some(1000)),
            trackpoint(0.0, 0.0, None, Some(1005)),
            // big gap pause
            trackpoint(0.0, 0.0, None, Some(1300)),
        ];
        let stats = TrackStats::from_points(&points);
        assert_eq!(stats.elapsed_time, Some(300));
        assert_eq!(stats.moving_time, Some(5));
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
}
