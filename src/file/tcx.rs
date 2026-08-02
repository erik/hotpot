use std::collections::HashMap;
use std::io::BufRead;

use anyhow::{Context, Result};
use geo::{LineString, MultiLineString};
use geo_types::Point;
use quick_xml::XmlVersion;
use quick_xml::events::BytesText;
use quick_xml::events::Event;
use quick_xml::events::attributes::Attributes;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::{
    activity::RawActivity,
    track_stats::{TrackPoint, TrackStats},
};

struct TcxMachine {
    state: Section,
    start_time: Option<OffsetDateTime>,
    activity_type: Option<String>,

    tracks: Vec<Vec<TrackPoint>>,
    current_track: Vec<TrackPoint>,
}

/// Pull the `Sport` attribute (e.g. "Biking", "Running") off an `<Activity>`.
fn sport(attrs: Attributes) -> Option<String> {
    for attr in attrs.flatten() {
        if attr.key.as_ref() == b"Sport" {
            return attr
                .normalized_value(XmlVersion::Implicit1_0)
                .ok()
                .map(|v| v.trim().to_owned());
        }
    }
    None
}

/// A `<Trackpoint>` with no `<Position>` yet — coordinates are NaN so we can tell
/// it apart from a point that legitimately sits at (0, 0) and drop it later.
fn pending_point() -> TrackPoint {
    TrackPoint {
        point: Point::new(f64::NAN, f64::NAN),
        elevation: None,
        timestamp: None,
    }
}

#[derive(Debug, PartialEq)]
enum SubSection {
    None,
    Id,
    Time,
    Lat,
    Lng,
    Elev,
}

#[derive(Debug, PartialEq)]
enum Section {
    Root,
    Activity(SubSection),
    Lap,
    Track,
    Trackpoint(SubSection, TrackPoint),
    Position(SubSection, TrackPoint),
}

impl TcxMachine {
    fn open(&mut self, tag: &[u8], attr: Attributes) {
        match &self.state {
            // The `<Activities>` wrapper is transparent; we jump straight from the
            // root to each `<Activity>`.
            Section::Root if tag == b"Activity" => {
                if self.activity_type.is_none() {
                    self.activity_type = sport(attr);
                }
                self.state = Section::Activity(SubSection::None);
            }

            Section::Activity(SubSection::None) => match tag {
                b"Id" => self.state = Section::Activity(SubSection::Id),
                b"Lap" => self.state = Section::Lap,
                _ => {}
            },

            Section::Lap if tag == b"Track" => self.state = Section::Track,

            Section::Track if tag == b"Trackpoint" => {
                self.state = Section::Trackpoint(SubSection::None, pending_point())
            }

            Section::Trackpoint(SubSection::None, pt) => match tag {
                b"Position" => self.state = Section::Position(SubSection::None, *pt),
                b"AltitudeMeters" => self.state = Section::Trackpoint(SubSection::Elev, *pt),
                b"Time" => self.state = Section::Trackpoint(SubSection::Time, *pt),
                _ => {}
            },

            Section::Position(SubSection::None, pt) => match tag {
                b"LatitudeDegrees" => self.state = Section::Position(SubSection::Lat, *pt),
                b"LongitudeDegrees" => self.state = Section::Position(SubSection::Lng, *pt),
                _ => {}
            },

            _ => {}
        }
    }

    fn close(&mut self, tag: &[u8]) {
        match &self.state {
            Section::Root => {}

            Section::Activity(_) => match tag {
                b"Activity" => self.state = Section::Root,
                b"Id" => self.state = Section::Activity(SubSection::None),
                _ => {}
            },

            Section::Lap if tag == b"Lap" => self.state = Section::Activity(SubSection::None),

            Section::Track if tag == b"Track" => {
                let track = std::mem::take(&mut self.current_track);
                if track.len() > 1 {
                    self.tracks.push(track);
                }
                self.state = Section::Lap;
            }

            Section::Trackpoint(_, pt) => match tag {
                b"Trackpoint" => {
                    // Skip points that never got a `<Position>` (e.g. indoor
                    // trainer samples that only carry time/heart rate).
                    if pt.point.0.x.is_finite() && pt.point.0.y.is_finite() {
                        self.current_track.push(*pt);
                    }
                    self.state = Section::Track;
                }
                b"AltitudeMeters" | b"Time" => {
                    self.state = Section::Trackpoint(SubSection::None, *pt)
                }
                _ => {}
            },

            Section::Position(_, pt) => match tag {
                b"Position" => self.state = Section::Trackpoint(SubSection::None, *pt),
                b"LatitudeDegrees" | b"LongitudeDegrees" => {
                    self.state = Section::Position(SubSection::None, *pt)
                }
                _ => {}
            },

            _ => {}
        }
    }

    fn text(&mut self, t: &BytesText) -> Result<()> {
        match &mut self.state {
            Section::Activity(SubSection::Id) => {
                // The activity `<Id>` is conventionally its start timestamp. First
                // one wins for multi-activity files.
                if self.start_time.is_none() {
                    self.start_time = OffsetDateTime::parse(t.decode()?.trim(), &Rfc3339).ok();
                }
            }

            Section::Trackpoint(SubSection::Time, pt) => {
                pt.timestamp = OffsetDateTime::parse(t.decode()?.trim(), &Rfc3339)
                    .map(OffsetDateTime::unix_timestamp)
                    .ok();
            }

            Section::Trackpoint(SubSection::Elev, pt) => {
                pt.elevation = t.decode()?.trim().parse().ok();
            }

            Section::Position(SubSection::Lat, pt) => {
                pt.point.0.y = t.decode()?.trim().parse().context("invalid latitude")?;
            }

            Section::Position(SubSection::Lng, pt) => {
                pt.point.0.x = t.decode()?.trim().parse().context("invalid longitude")?;
            }

            _ => {}
        }
        Ok(())
    }
}

pub fn parse_tcx<R: BufRead>(reader: R) -> Result<Option<RawActivity>> {
    let mut xml = quick_xml::Reader::from_reader(reader);
    let mut buf = Vec::new();

    let mut machine = TcxMachine {
        state: Section::Root,
        start_time: None,
        activity_type: None,

        tracks: vec![],
        current_track: vec![],
    };

    loop {
        match xml.read_event_into(&mut buf)? {
            Event::Start(e) => machine.open(e.name().as_ref(), e.attributes()),

            // self-closing tag
            Event::Empty(e) => {
                let tag = e.name();
                machine.open(tag.as_ref(), e.attributes());
                machine.close(tag.as_ref());
            }

            Event::Text(t) => machine.text(&t)?,
            Event::End(e) => machine.close(e.name().as_ref()),
            Event::Eof => break,
            _ => {}
        }

        buf.clear();
    }

    if machine.tracks.is_empty() {
        return Ok(None);
    }

    // Fall back to the first recorded fix if `<Id>` was missing or unparseable.
    let start_time = machine.start_time.or_else(|| {
        machine
            .tracks
            .iter()
            .flatten()
            .find_map(|pt| pt.timestamp)
            .and_then(|ts| OffsetDateTime::from_unix_timestamp(ts).ok())
    });

    let mut properties = HashMap::new();
    if let Some(ref ty) = machine.activity_type {
        // Skip virtual activities. Sport is free form, so this won't be exhaustive.
        if ty.starts_with("Virtual") || ty.starts_with("virtual") {
            return Ok(None);
        }

        properties.insert(
            "activity_type".to_owned(),
            serde_json::Value::String(ty.to_owned()),
        );
    }

    if let Some(stats) = machine
        .tracks
        .iter()
        .map(|t| t.as_slice())
        .map(TrackStats::from_points)
        .reduce(TrackStats::combine)
    {
        stats.merge_into(&mut properties);
    }

    let tracks = machine
        .tracks
        .into_iter()
        .map(|t| {
            let pts = t.into_iter().map(|pt| pt.point.0).collect();
            LineString::new(pts)
        })
        .collect();

    Ok(Some(RawActivity {
        title: None,
        start_time,
        tracks: MultiLineString::new(tracks),
        properties,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::format_description::well_known::Rfc3339;

    fn parse(tcx: &str) -> Option<RawActivity> {
        parse_tcx(tcx.as_bytes()).expect("parse should not error")
    }

    #[test]
    fn invalid_coordinate_errors_the_file() {
        // A `<Position>` with an unparseable coordinate fails the whole parse,
        // matching the old serde-based tcx crate. Compare with
        // `trackpoints_without_position_are_dropped`, where the point is simply
        // skipped.
        let tcx = r#"<TrainingCenterDatabase><Activities><Activity Sport="Biking">
          <Lap><Track>
            <Trackpoint><Position><LatitudeDegrees>nope</LatitudeDegrees><LongitudeDegrees>2.0</LongitudeDegrees></Position></Trackpoint>
          </Track></Lap>
        </Activity></Activities></TrainingCenterDatabase>"#;
        assert!(parse_tcx(tcx.as_bytes()).is_err());
    }

    const BASIC: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities>
    <Activity Sport="Biking">
      <Id>2026-01-02T10:00:00Z</Id>
      <Lap StartTime="2026-01-02T10:00:00Z">
        <TotalTimeSeconds>300</TotalTimeSeconds>
        <DistanceMeters>2500</DistanceMeters>
        <AverageHeartRateBpm><Value>140</Value></AverageHeartRateBpm>
        <Track>
          <Trackpoint>
            <Time>2026-01-02T10:00:00Z</Time>
            <Position><LatitudeDegrees>42.0001</LatitudeDegrees><LongitudeDegrees>-71.0001</LongitudeDegrees></Position>
            <AltitudeMeters>10.0</AltitudeMeters>
            <HeartRateBpm><Value>138</Value></HeartRateBpm>
          </Trackpoint>
          <Trackpoint>
            <Time>2026-01-02T10:02:30Z</Time>
            <Position><LatitudeDegrees>42.0005</LatitudeDegrees><LongitudeDegrees>-71.0005</LongitudeDegrees></Position>
            <AltitudeMeters>15.0</AltitudeMeters>
          </Trackpoint>
          <Trackpoint>
            <Time>2026-01-02T10:05:00Z</Time>
            <Position><LatitudeDegrees>42.0009</LatitudeDegrees><LongitudeDegrees>-71.0009</LongitudeDegrees></Position>
            <AltitudeMeters>13.0</AltitudeMeters>
          </Trackpoint>
        </Track>
      </Lap>
    </Activity>
  </Activities>
</TrainingCenterDatabase>"#;

    #[test]
    fn parses_basic_activity() {
        let activity = parse(BASIC).expect("expected an activity");

        // TCX has no track name.
        assert!(activity.title.is_none());

        let expected_start = OffsetDateTime::parse("2026-01-02T10:00:00Z", &Rfc3339).unwrap();
        assert_eq!(activity.start_time, Some(expected_start));

        assert_eq!(
            activity.properties.get("activity_type"),
            Some(&serde_json::Value::String("Biking".to_string()))
        );

        assert_eq!(activity.tracks.0.len(), 1, "one track => one line");
        let line = &activity.tracks.0[0];
        assert_eq!(line.0.len(), 3);

        // Coordinates are stored as (lng, lat).
        assert!((line.0[0].x - (-71.0001)).abs() < 1e-6);
        assert!((line.0[0].y - 42.0001).abs() < 1e-6);
    }

    #[test]
    fn altitude_feeds_elevation_stats() {
        let activity = parse(BASIC).expect("expected an activity");
        assert_eq!(
            activity
                .properties
                .get("min_elevation")
                .and_then(|v| v.as_f64()),
            Some(10.0)
        );
        assert_eq!(
            activity
                .properties
                .get("max_elevation")
                .and_then(|v| v.as_f64()),
            Some(15.0)
        );
    }

    #[test]
    fn trackpoints_without_position_are_dropped() {
        // Indoor-trainer style samples: time + heart rate, but no <Position>.
        let tcx = r#"<TrainingCenterDatabase><Activities><Activity Sport="Biking">
          <Id>2022-06-02T16:22:25Z</Id>
          <Lap><Track>
            <Trackpoint>
              <Time>2022-06-02T16:22:25Z</Time>
              <HeartRateBpm><Value>93</Value></HeartRateBpm>
            </Trackpoint>
            <Trackpoint>
              <Time>2022-06-02T16:22:26Z</Time>
              <HeartRateBpm><Value>94</Value></HeartRateBpm>
            </Trackpoint>
          </Track></Lap>
        </Activity></Activities></TrainingCenterDatabase>"#;
        assert!(parse(tcx).is_none());
    }

    #[test]
    fn virtual_activities_are_skipped() {
        let tcx = BASIC.replace(r#"Sport="Biking""#, r#"Sport="Virtual""#);
        assert!(parse(&tcx).is_none());
    }

    #[test]
    fn single_point_track_is_dropped() {
        let tcx = r#"<TrainingCenterDatabase><Activities><Activity Sport="Biking">
          <Lap><Track>
            <Trackpoint>
              <Position><LatitudeDegrees>1.0</LatitudeDegrees><LongitudeDegrees>2.0</LongitudeDegrees></Position>
            </Trackpoint>
          </Track></Lap>
        </Activity></Activities></TrainingCenterDatabase>"#;
        assert!(parse(tcx).is_none());
    }

    #[test]
    fn multiple_laps_become_multiple_tracks() {
        let tcx = r#"<TrainingCenterDatabase><Activities><Activity Sport="Running">
          <Lap><Track>
            <Trackpoint><Position><LatitudeDegrees>1.0</LatitudeDegrees><LongitudeDegrees>2.0</LongitudeDegrees></Position></Trackpoint>
            <Trackpoint><Position><LatitudeDegrees>1.001</LatitudeDegrees><LongitudeDegrees>2.001</LongitudeDegrees></Position></Trackpoint>
          </Track></Lap>
          <Lap><Track>
            <Trackpoint><Position><LatitudeDegrees>3.0</LatitudeDegrees><LongitudeDegrees>4.0</LongitudeDegrees></Position></Trackpoint>
            <Trackpoint><Position><LatitudeDegrees>3.001</LatitudeDegrees><LongitudeDegrees>4.001</LongitudeDegrees></Position></Trackpoint>
          </Track></Lap>
        </Activity></Activities></TrainingCenterDatabase>"#;
        let activity = parse(tcx).expect("expected an activity");
        assert_eq!(activity.tracks.0.len(), 2);
    }

    #[test]
    fn missing_id_falls_back_to_first_trackpoint_time() {
        let tcx = BASIC.replace("<Id>2026-01-02T10:00:00Z</Id>", "");
        let activity = parse(&tcx).expect("expected an activity");
        let expected = OffsetDateTime::parse("2026-01-02T10:00:00Z", &Rfc3339).unwrap();
        assert_eq!(activity.start_time, Some(expected));
    }
}
