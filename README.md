# hotpot

A spicy little heatmap tile server.

![](https://user-images.githubusercontent.com/188935/273125894-7f76eabb-585b-405d-af16-a93df2d85cb4.png)

## Overview

Renders customizable activity heatmap images from GPS tracks extracted
from GPX, TCX, and FIT files. There's also a built-in web server to
serve up [XYZ tiles], and endpoints to add new data via HTTP POST or
[Strava webhooks].

Designed to be self-hosted. It's lightweight and snappy enough to fit onto the
free tier of pretty much anything that can run a Docker container. Even with
100,000 km of activity data, [Fly.io]'s smallest instance can render tiles in
~1 ms.

Tracks are efficiently stored in a local `sqlite3` database.

[XYZ tiles]: https://en.wikipedia.org/wiki/Tiled_web_map
[Strava webhooks]: https://developers.strava.com/docs/webhooks/
[Fly.io]: https://fly.io/

```console
$ du -sh activities/ hotpot.sqlite3
789M    activities/
 47M    hotpot.sqlite3
```

## Quick Start

To get started, use the `import` command to quickly process an entire
directory of activities in parallel.

```
hotpot import [path/to/files/]
```

If importing activities from a [Strava data export], use
`--join [path/to/activities.csv]` to include metadata about your
activities usually not stored in the GPX (title, which bike you used, the
weather, ...)

```
hotpot import \
    strava_export/activities/ \
    --join strava_export/activities.csv
```

[Strava data export]: https://support.strava.com/hc/en-us/articles/216918437-Exporting-your-Data-and-Bulk-Export

After the initial import, you'll have a `sqlite3` database, and can start
creating heatmaps.

Now run the tile server:

```
hotpot serve
```

Open `http://127.0.0.1:8080/` in your browser to see a map view with the tile
layer loaded.

See `hotpot --help` for more.

## Customization

### Gradients

There are several built in palettes available for converting the raw frequency
data into colored pixels, which can be set via the `?color={...}` query
parameter. A list of these is available in the map view.

In addition to the presets, custom gradients can also be used via the
`?gradient={...}` parameter.

For example, to smoothly transition from red (least activity) to white
(most), we could use `0:f00;1:fff`. Pixels with no activity will be left
transparent. Color codes are interpreted as hex RGB values in the following
formats: `RGB`, `RRGGBB`, `RRGGBBAA`.

If alpha values are not given, they are assumed to be `0xff` (fully opaque).

### Filters

We can also choose which activities we're interested in visualizing
dynamically through the `?filter={...}` parameter.

Any properties available when the activity was added (either via webhook
or bulk import) can be used in the filter expression, but the exact names
will vary based on your data.

For example, we may want to generate different tiles for cycling vs hiking,
exclude commutes, which gear we used, a minimum elevation gain, etc.

```json5
# Basic numeric comparisons: <, <=, >, >=
{"key": "elev_gain", ">": 1000}

# Match/exclude multiple values
{"key": "activity_gear", "any_of": ["gravel", "mtb"]}
{"key": "activity_gear", "none_of": ["gravel", "mtb"]}

# Substring matches (e.g. match "Gravel Ride" + "Ride")
{"key": "activity_type", "matches": "Ride"}

# Property key exists
{"key", "max_hr", "has_key": true}
```

## Activity Uploads

Hotpot supports two mechanisms for adding new data to the `sqlite3` database
directly over HTTP:

1. `POST /upload`: Manually upload a single GPX, TCX, or FIT file
2. Strava webhook: Subscribe to new activity uploads automatically

### `/upload`

todo

### Strava Webhook

todo

## Deployment

todo

## License

todo: gplv3+