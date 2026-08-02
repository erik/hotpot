use std::collections::HashMap;
use std::io::BufRead;

use anyhow::{Context, Result};
use geo::{LineString, MultiLineString};
use geo_types::Point;
use quick_xml::events::Event;
use quick_xml::events::attributes::Attributes;
use quick_xml::events::{BytesRef, BytesText};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::{
    activity::RawActivity,
    track_stats::{TrackPoint, TrackStats},
};

impl TryFrom<Attributes<'_>> for TrackPoint {
    type Error = anyhow::Error;

    fn try_from(attrs: Attributes) -> Result<Self> {
        let mut lat = None;
        let mut lng = None;

        for attr in attrs.flatten() {
            let Ok(value) = str::from_utf8(&attr.value) else {
                continue;
            };
            match attr.key.as_ref() {
                b"lat" => lat = Some(value.parse::<f64>().context("invalid latitude")?),
                b"lon" => lng = Some(value.parse::<f64>().context("invalid longitude")?),
                _ => {}
            }
        }

        Ok(TrackPoint {
            point: Point::new(
                lng.context("trackpoint missing longitude")?,
                lat.context("trackpoint missing latitude")?,
            ),
            elevation: None,
            timestamp: None,
        })
    }
}

#[derive(Debug, PartialEq)]
enum SubTag {
    None,
    Time,
    Name,
    Type,
    Elevation,
}

#[derive(Debug, PartialEq)]
enum Tag {
    Root,
    Metadata(SubTag),
    Trk(SubTag),
    TrkSeg,
    TrkPt(SubTag, TrackPoint),
}

struct GpxMachine {
    state: Tag,
    start_time: Option<OffsetDateTime>,
    title: Option<String>,
    activity_type: Option<String>,

    text_buf: String,

    tracks: Vec<Vec<TrackPoint>>,
    current_track: Vec<TrackPoint>,
}

impl GpxMachine {
    fn open(&mut self, tag: &[u8], attr: Attributes) -> Result<()> {
        match &self.state {
            Tag::Root => match tag {
                b"metadata" => self.state = Tag::Metadata(SubTag::None),
                b"trk" => self.state = Tag::Trk(SubTag::None),
                _ => {}
            },

            Tag::Metadata(SubTag::None) if tag == b"time" => {
                self.state = Tag::Metadata(SubTag::Time)
            }

            Tag::Trk(SubTag::None) => match tag {
                b"name" => {
                    self.text_buf.clear();
                    self.state = Tag::Trk(SubTag::Name);
                }
                b"type" => {
                    self.text_buf.clear();
                    self.state = Tag::Trk(SubTag::Type);
                }
                b"trkseg" => self.state = Tag::TrkSeg,
                _ => {}
            },

            Tag::TrkSeg if tag == b"trkpt" => {
                let pt = TrackPoint::try_from(attr)?;
                self.state = Tag::TrkPt(SubTag::None, pt)
            }

            Tag::TrkPt(SubTag::None, pt) => match tag {
                b"ele" => self.state = Tag::TrkPt(SubTag::Elevation, *pt),
                b"time" => self.state = Tag::TrkPt(SubTag::Time, *pt),
                _ => {}
            },

            _ => {}
        }
        Ok(())
    }

    fn close(&mut self, tag: &[u8]) {
        match &self.state {
            Tag::Root => {}

            Tag::Metadata(_) => match tag {
                b"metadata" => self.state = Tag::Root,
                b"time" => self.state = Tag::Metadata(SubTag::None),
                _ => {}
            },

            Tag::Trk(_) => match tag {
                b"trk" => self.state = Tag::Root,
                b"name" => {
                    self.title = self.take_text();
                    self.state = Tag::Trk(SubTag::None);
                }
                b"type" => {
                    self.activity_type = self.take_text();
                    self.state = Tag::Trk(SubTag::None);
                }
                _ => {}
            },

            Tag::TrkSeg => {
                if tag == b"trkseg" {
                    let track = std::mem::take(&mut self.current_track);
                    if track.len() > 1 {
                        self.tracks.push(track);
                    }
                    self.state = Tag::Trk(SubTag::None);
                }
            }

            Tag::TrkPt(_, pt) => match tag {
                b"trkpt" => {
                    self.current_track.push(*pt);
                    self.state = Tag::TrkSeg
                }
                b"ele" | b"time" => self.state = Tag::TrkPt(SubTag::None, *pt),
                _ => {}
            },
        }
    }

    fn text(&mut self, t: &BytesText) -> Result<()> {
        match &mut self.state {
            Tag::Metadata(SubTag::Time) => {
                self.start_time = OffsetDateTime::parse(t.decode()?.trim(), &Rfc3339).ok();
            }

            // Free text fields can contain escaped content, build up a buffer
            // that we flush when tag is closed.
            Tag::Trk(SubTag::Name) | Tag::Trk(SubTag::Type) => {
                self.text_buf.push_str(&t.decode()?);
            }

            Tag::TrkPt(SubTag::Time, pt) => {
                pt.timestamp = OffsetDateTime::parse(t.decode()?.trim(), &Rfc3339)
                    .map(OffsetDateTime::unix_timestamp)
                    .ok();
            }

            Tag::TrkPt(SubTag::Elevation, pt) => {
                pt.elevation = t.decode()?.trim().parse().ok();
            }

            _ => {}
        }
        Ok(())
    }

    fn entity(&mut self, r: &BytesRef) -> Result<()> {
        match &self.state {
            Tag::Trk(SubTag::Name) | Tag::Trk(SubTag::Type) => {
                if let Some(text) = quick_xml::escape::resolve_xml_entity(&r.decode()?) {
                    self.text_buf.push_str(text);
                }
            }

            _ => (),
        }

        Ok(())
    }

    fn take_text(&mut self) -> Option<String> {
        let text = self.text_buf.trim();
        let result = (!text.is_empty()).then(|| String::from(text));
        self.text_buf.clear();

        result
    }
}

pub fn parse_gpx<R: BufRead>(reader: R) -> Result<Option<RawActivity>> {
    let mut xml = quick_xml::Reader::from_reader(reader);
    let mut buf = Vec::new();

    let mut machine = GpxMachine {
        state: Tag::Root,
        start_time: None,
        title: None,
        activity_type: None,
        text_buf: String::new(),

        tracks: vec![],
        current_track: vec![],
    };

    loop {
        match xml.read_event_into(&mut buf)? {
            Event::Start(e) => machine.open(e.name().as_ref(), e.attributes())?,

            // self-closing tag
            Event::Empty(e) => {
                let tag = e.name();
                machine.open(tag.as_ref(), e.attributes())?;
                machine.close(tag.as_ref());
            }

            Event::Text(t) => machine.text(&t)?,
            Event::GeneralRef(r) => machine.entity(&r)?,
            Event::End(e) => machine.close(e.name().as_ref()),
            Event::Eof => break,
            _ => {}
        }

        buf.clear();
    }

    if machine.tracks.is_empty() {
        return Ok(None);
    }

    let mut properties = HashMap::new();
    if let Some(ref ty) = machine.activity_type {
        // Skip virtual activities. <type> is free form, so this won't be exhaustive.
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
        title: machine.title,
        start_time: machine.start_time,
        tracks: MultiLineString::new(tracks),
        properties,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::format_description::well_known::Rfc3339;

    fn parse(gpx: &str) -> Option<RawActivity> {
        parse_gpx(gpx.as_bytes()).expect("parse should not error")
    }

    #[test]
    fn invalid_coordinate_errors_the_file() {
        let gpx = r#"<gpx><trk><trkseg>
            <trkpt lat="1.0" lon="2.0"/>
            <trkpt lat="not-a-number" lon="2.001"/>
        </trkseg></trk></gpx>"#;
        assert!(parse_gpx(gpx.as_bytes()).is_err());
    }

    #[test]
    fn missing_coordinate_errors_the_file() {
        let gpx = r#"<gpx><trk><trkseg>
            <trkpt lat="1.0" lon="2.0"/>
            <trkpt lat="1.001"/>
        </trkseg></trk></gpx>"#;
        assert!(parse_gpx(gpx.as_bytes()).is_err());
    }

    const BASIC: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<gpx creator="test" version="1.1" xmlns="http://www.topografix.com/GPX/1/1">
  <metadata>
    <name>ignored metadata name</name>
    <time>2014-10-06T22:20:33Z</time>
  </metadata>
  <trk>
    <name>Afternoon Run</name>
    <type>running</type>
    <trkseg>
      <trkpt lat="43.0702820" lon="-89.3907330">
        <ele>260.2</ele>
        <time>2014-10-06T22:20:33Z</time>
      </trkpt>
      <trkpt lat="43.0702320" lon="-89.3907800">
        <ele>260.3</ele>
        <time>2014-10-06T22:20:43Z</time>
      </trkpt>
      <trkpt lat="43.0701880" lon="-89.3908150">
        <ele>262.3</ele>
        <time>2014-10-06T22:20:53Z</time>
      </trkpt>
    </trkseg>
  </trk>
</gpx>"#;

    #[test]
    fn parses_basic_activity() {
        let activity = parse(BASIC).expect("expected an activity");

        // Title comes from <trk><name>, not the <metadata><name>.
        assert_eq!(activity.title.as_deref(), Some("Afternoon Run"));

        let expected_start = OffsetDateTime::parse("2014-10-06T22:20:33Z", &Rfc3339).unwrap();
        assert_eq!(activity.start_time, Some(expected_start));

        assert_eq!(
            activity.properties.get("activity_type"),
            Some(&serde_json::Value::String("running".to_string()))
        );

        assert_eq!(activity.tracks.0.len(), 1, "one segment => one line");
        let line = &activity.tracks.0[0];
        assert_eq!(line.0.len(), 3);

        // Coordinates are stored as (lng, lat).
        assert!((line.0[0].x - (-89.390_733)).abs() < 1e-6);
        assert!((line.0[0].y - 43.070_282).abs() < 1e-6);
    }

    #[test]
    fn entities_in_free_text_are_resolved() {
        let gpx = BASIC
            .replace("<name>Afternoon Run</name>", "<name>x&amp;y</name>")
            .replace("<type>running</type>", "<type>trail &lt;run&gt;</type>");
        let activity = parse(&gpx).expect("expected an activity");
        assert_eq!(activity.title.as_deref(), Some("x&y"));
        assert_eq!(
            activity.properties.get("activity_type"),
            Some(&serde_json::Value::String("trail <run>".to_string()))
        );
    }

    #[test]
    fn no_track_data_returns_none() {
        let gpx = r#"<gpx>
            <metadata><time>2014-10-06T22:20:33Z</time></metadata>
        </gpx>"#;
        assert!(parse(gpx).is_none());
    }

    #[test]
    fn virtual_activities_are_skipped() {
        for ty in ["VirtualRide", "virtual_ride"] {
            let gpx = BASIC.replace("<type>running</type>", &format!("<type>{ty}</type>"));
            assert!(parse(&gpx).is_none(), "{ty} should be skipped");
        }
    }

    #[test]
    fn single_point_segment_is_dropped() {
        // A segment with fewer than two points can't form a line.
        let gpx = r#"<gpx><trk><trkseg>
            <trkpt lat="1.0" lon="2.0"><ele>10</ele></trkpt>
        </trkseg></trk></gpx>"#;
        assert!(parse(gpx).is_none());
    }

    #[test]
    fn self_closing_trackpoints_are_parsed() {
        // Points with no children arrive as `Event::Empty` rather than a
        // Start/End pair.
        let gpx = r#"<gpx><trk><name>x</name><trkseg>
            <trkpt lat="1.0" lon="2.0"/>
            <trkpt lat="1.001" lon="2.001"/>
        </trkseg></trk></gpx>"#;
        let activity = parse(gpx).expect("expected an activity");
        assert_eq!(activity.tracks.0.len(), 1);
        assert_eq!(activity.tracks.0[0].0.len(), 2);
    }

    #[test]
    fn multiple_segments_become_multiple_tracks() {
        let gpx = r#"<gpx><trk>
          <trkseg>
            <trkpt lat="1.0" lon="2.0"/>
            <trkpt lat="1.001" lon="2.001"/>
          </trkseg>
          <trkseg>
            <trkpt lat="3.0" lon="4.0"/>
            <trkpt lat="3.001" lon="4.001"/>
          </trkseg>
        </trk></gpx>"#;
        let activity = parse(gpx).expect("expected an activity");
        assert_eq!(activity.tracks.0.len(), 2);
    }

    #[test]
    fn missing_metadata_time_leaves_start_time_unset() {
        let gpx = r#"<gpx><trk><trkseg>
            <trkpt lat="1.0" lon="2.0"/>
            <trkpt lat="1.001" lon="2.001"/>
        </trkseg></trk></gpx>"#;
        let activity = parse(gpx).expect("expected an activity");
        assert!(activity.start_time.is_none());
        assert!(activity.title.is_none());
    }
}
