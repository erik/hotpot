# hotpot

A spicy little heatmap tile server.

![](https://user-images.githubusercontent.com/188935/273125894-7f76eabb-585b-405d-af16-a93df2d85cb4.png)

## Overview

Hotpot renders customizable activity heatmap images from GPS tracks extracted
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

Now let's run the tile server:

```
hotpot serve
```

Open `http://127.0.0.1:8080/` in your browser to see a map view with the tile
layer loaded.

See `hotpot --help` for more.

## Customization

### Gradients

todo

### Filters

todo

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