use std::collections::HashMap;
use std::io::Read;

use anyhow::Result;
use fitparser::de::{DecodeOption, FitObject, FitStreamProcessor};
use fitparser::profile::MesgNum;
use geo_types::{MultiLineString, Point};
use time::OffsetDateTime;

use crate::activity::RawActivity;
use crate::track_stats::{TrackPoint, TrackStats};

// Not an exhaustive list, but the most obvious of the FIT "sub_sports" which it
// doesn't make sense to include in a heatmap.
const FIT_VIRTUAL_SPORTS: [&str; 4] = [
    "virtual_activity",
    "indoor_cycling",
    "indoor_rowing",
    "indoor_running",
];

pub fn parse_fit<R: Read>(reader: &mut R) -> Result<Option<RawActivity>> {
    const SCALE_FACTOR: f64 = (1u64 << 32) as f64 / 360.0;

    let mut fit_stream = FitStreamProcessor::new();
    fit_stream.add_option(DecodeOption::SkipDataCrcValidation);
    fit_stream.add_option(DecodeOption::SkipHeaderCrcValidation);

    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    let mut input = buffer.as_slice();

    let mut properties = HashMap::new();
    let mut start_time = None;
    let mut track_points = vec![];

    while !input.is_empty() {
        let (rest, obj) = fit_stream.deserialize_next(input)?;
        input = rest;

        let msg = match obj {
            FitObject::DataMessage(message) => message,

            // Reset accumulator/definition state between chained FIT files.
            FitObject::Crc(_) => {
                fit_stream.reset();
                continue;
            }

            FitObject::Header(_) | FitObject::DefinitionMessage(_) => continue,
        };

        let data = fit_stream.decode_message(msg)?;
        match data.kind() {
            // There's one FileId block per file and one or more sessions.
            // Currently not really supporting the concept of multi-session
            // files, so don't try to be clever with parsing.
            MesgNum::FileId | MesgNum::Session => {
                for f in data.into_vec().into_iter() {
                    match f.name() {
                        "sub_sport" => {
                            // Skip over virtual activity types
                            if let fitparser::Value::String(ty) = f.value()
                                && FIT_VIRTUAL_SPORTS.contains(&ty.as_str())
                            {
                                return Ok(None);
                            }
                        }

                        "start_time" => {
                            let fitparser::Value::Timestamp(ts) = f.value() else {
                                continue;
                            };
                            start_time = Some(ts.timestamp());
                        }

                        key if key.starts_with("unknown_field_") => {
                            // Skip anything the fitparser library doesn't know
                            // about.
                        }

                        // Blindly stuff the remaining attributes into properties
                        key => {
                            properties.insert(key.to_owned(), serde_json::to_value(f.value())?);
                        }
                    }
                }
            }
            MesgNum::Record => {
                let mut lat: Option<i64> = None;
                let mut lng: Option<i64> = None;
                let mut elevation: Option<f64> = None;
                let mut timestamp: Option<i64> = None;

                for f in data.into_vec().into_iter() {
                    match f.name() {
                        "position_lat" => lat = f.value().try_into().ok(),
                        "position_long" => lng = f.value().try_into().ok(),
                        // Prefer enhanced_altitude over altitude
                        "altitude" if elevation.is_none() => {
                            elevation = f.into_value().try_into().ok()
                        }
                        "enhanced_altitude" => elevation = f.into_value().try_into().ok(),
                        "timestamp" => {
                            timestamp = f.value().try_into().ok();
                            if timestamp.is_some() && start_time.is_none() {
                                start_time = timestamp;
                            }
                        }
                        _ => {}
                    }
                }

                if let (Some(lat), Some(lng)) = (lat, lng) {
                    let pt = Point::new(lng as f64, lat as f64) / SCALE_FACTOR;
                    track_points.push(TrackPoint {
                        point: pt,
                        elevation,
                        timestamp,
                    });
                }
            }
            _ => {}
        }
    }

    if track_points.is_empty() {
        return Ok(None);
    }

    let stats = TrackStats::from_points(&track_points);
    stats.merge_into(&mut properties);

    let line: Vec<_> = track_points.iter().map(|pt| pt.point).collect();
    Ok(Some(RawActivity {
        properties,
        title: None,
        start_time: start_time.map(|ts| OffsetDateTime::from_unix_timestamp(ts).unwrap()),
        tracks: MultiLineString::from(line),
    }))
}
