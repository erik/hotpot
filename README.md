# hotpot

![](https://user-images.githubusercontent.com/188935/273125894-7f76eabb-585b-405d-af16-a93df2d85cb4.png)

Render customizable activity heatmap images from GPS tracks extracted from GPX,
TCX, and FIT files. Includes a built-in web server for [XYZ tiles] and endpoints
to add new data via HTTP POST or from external APIs.

Designed to run locally or be self-hosted. Lightweight enough to run on free
tiers of most Docker-compatible platforms. Even with 100,000 km of activity
data, tiles render in a few milliseconds.

[XYZ tiles]: https://en.wikipedia.org/wiki/Tiled_web_map

## Quick start

```bash
# Pull the pre-built container from GitHub Container Registry
docker pull ghcr.io/erik/hotpot

# Or build the Docker image yourself
docker build -t ghcr.io/erik/hotpot .

# Or build from source
cargo build --release

# When using Docker, always mount a volume at /data for the database
docker run -p 8080:8080 -v ./data:/data ghcr.io/erik/hotpot

# Visit http://localhost:8080 to browse the map
```

### Import activities

To import an entire directory of activities in parallel, run the following
command:

```bash
hotpot import path/to/activities/
```

If you're pulling from a [Strava bulk data export], include metadata such as the
gear you used, the weather, etc. with `--join`:

```bash
hotpot import \
    strava_export/activities/ \
    --join strava_export/activities.csv
```

By default, the database is called `hotpot.sqlite3`. Change the name or location
with `--db`:

```bash
hotpot --db /somewhere/else/heatmap.db import ...
```

You can also import activities from your browser. Note that this isn't run in
parallel, and is significantly slower than the command line import:

1. Optional: Set an upload token so that only you can upload:

   ```bash
   export HOTPOT_UPLOAD_TOKEN=xyz123...
   ```

2. Uploads are disabled by default. To enable them, start the server with
   `--upload`:

   ```bash
   hotpot serve --upload
   ```

3. Go to `http://localhost:8080`, and drag files into the upload modal.

[Strava bulk data export]: https://support.strava.com/en-us/articles/15401919-exporting-your-data-and-bulk-export

### Create heatmaps

After the initial import, you have a database with all your activities and can
start visualizing them.

To run a tile server and web UI on `http://localhost:8080`:

```bash
hotpot serve
```

To generate a static image instead:

```bash
hotpot render \
    --bounds='-120.7196,32.2459,-116.9234,35.1454' \
    --width 2000 \
    --height 2000 \
    --output heatmap.png
```

To construct the bounds, use a tool such as
[Klokantech bounding box](https://boundingbox.klokantech.com/).

For full details about the CLI, run `hotpot --help`.

## Customization

### Gradients

Several built-in palettes are available in the web UI to control the colors used
in the rendered heatmap.

You can also define a custom color gradient by specifying a sequence of
threshold values (how many times a particular pixel was visited) along with an
associated color. Hotpot interpolates values that fall between the thresholds to
a reasonable color.

For example, to display pure red for a pixel visited once and white for a pixel
visited 255 times or more, use `1:FF0000;255:FFFFFF`.

Color codes are given as hex RGBA values in `RGB`, `RRGGBB`, or `RRGGBBAA`
format. If you omit the alpha value, it's assumed to be fully opaque.

The following table shows the same tile rendered with three different example gradients:

<details>
  <summary>Example gradients</summary>

| Gradient                          | Rendered                                                                                                 |
| --------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `1:000;10:fff`                    | ![](https://user-images.githubusercontent.com/188935/277203430-269317c9-8539-4bc7-822c-fc199867d830.png) |
| `1:f00;5:ff0;10:ffff22;20:ffffff` | ![](https://user-images.githubusercontent.com/188935/277203443-ef63926a-0316-4a9b-ba5e-2cfdf0281581.png) |
| `1:322bb3;10:9894e5;20:fff`       | ![](https://user-images.githubusercontent.com/188935/277203450-bd929ee0-db3d-4653-9fed-5b3982829091.png) |

</details>

### Filters

You can choose which activities are visualized with filter expressions.

For example, you can generate different tiles for cycling and hiking, exclude
commutes, only select activities above a certain average speed, and so on.

```python
# Comparisons: =, !=, <, <=, >, >=
elevation_gain > 1000

# Use quotes for keys with spaces
"Average Temperature" < 5

# Match multiple values
activity_type in [ride, "gravel ride"]

# Pattern matching (% is wildcard, as with SQL's `LIKE`)
name like "Morning%"

# Check if property exists
has? heart_rate

# Combine multiple expressions
elapsed_time < 3600 && elevation_gain > 300
elevation_gain > 1000 || (average_speed > 30 && commute = true)
```

You can use any property key that was included when the activity was imported, but
the exact names vary based on your data.

In addition, several standard properties are computed automatically from the GPS
data for all activities, regardless of how they were imported:

| Property         | Unit    | Description                 |
| ---------------- | ------- | --------------------------- |
| `total_distance` | km      | Total activity distance     |
| `elapsed_time`   | seconds | Total time including pauses |
| `moving_time`    | seconds | Time spent moving           |
| `elevation_gain` | meters  | Total ascent                |
| `elevation_loss` | meters  | Total descent               |
| `min_elevation`  | meters  | Lowest elevation            |
| `max_elevation`  | meters  | Highest elevation           |
| `average_speed`  | km/h    | Average speed while moving  |
| `max_speed`      | km/h    | Maximum instantaneous speed |

### Privacy

If you run an internet-facing tile server or render heatmaps to share with
others, you might want to hide activities near sensitive locations such as your
home or work.

Use the `mask` command to define circular areas that Hotpot removes entirely
from the rendered heatmap. Give the radius in meters and the coordinates as
`latitude,longitude`.

```bash
hotpot mask add "home" --latlng 52.5200,13.4050 --radius 500
hotpot mask list
hotpot mask remove "home"
```

Area masks are dynamic, and can be repositioned, added, or removed without
needing to re-import data.

Alternatively, the `--trim N` argument for the `import` command trims the first
and last `N` meters from an activity. Unlike area masks, this affects the stored
activity data, so changing the value requires you to re-import your data. The
`--trim` value used for the initial import is stored and applies to future
activity imports as well. You can modify this value directly, if necessary:

```bash
sqlite3 hotpot.sqlite3 \
    "INSERT OR REPLACE INTO config (key, value) VALUES ('trim_dist', '500.0')"
```

## Activity uploads

Hotpot supports several ways to keep your database up to date by adding new data
to the database:

- Command line
- HTTP upload
- intervals.icu API integration
- Strava webhook

### Command line

To add new activities to the database with the CLI, use the `import` command:

```bash
hotpot import path/to/activities/
```

You can re-run `import` multiple times on the same directory, Hotpot
deduplicates imported activities as long as they share the same name.

### HTTP upload

To enable HTTP uploads, run the server with the `--upload` flag. You can then `POST`
any file that Hotpot imports on the command line to the `/upload` endpoint,
using `multipart/form-data` encoding.

```bash
# Remember to set this to a secret value if you expose your server to the
# internet.
export HOTPOT_UPLOAD_TOKEN=xyz123...

hotpot serve --upload

curl -X POST \
  http://hotpot.example.com/upload \
  --header "Authorization: Bearer $HOTPOT_UPLOAD_TOKEN" \
  --form file=@activity.gpx
```

### intervals.icu API integration

[intervals.icu](https://intervals.icu) aggregates activity data from sources
such as Garmin, Wahoo, and Strava, and exposes it through a single API. To get
an API key, go to intervals.icu and select **Settings > Developer Settings**.
For more information, see
[API access to intervals.icu](https://forum.intervals.icu/t/api-access-to-intervals-icu/609).

This is a pull-based source, so you need to trigger a fetch to pull new
activities, which can be done either on the command line or over HTTP. Both
paths deduplicate imported activities and are safe to re-run.

```bash
export INTERVALS_ICU_API_KEY=API_KEY \
       HOTPOT_UPLOAD_TOKEN=UPLOAD_TOKEN

# Import activities uploaded in the last 30 days
hotpot fetch intervals-icu --lookback 30

# Start the server with the fetch endpoint enabled
hotpot serve --fetch

# Trigger an import of the last 30 days
curl -X POST \
  https://hotpot.example.com/fetch/intervals.icu?lookback=30 \
  --header "Authorization: Bearer $HOTPOT_UPLOAD_TOKEN"
```

Due to Strava API limitations, activities sourced _solely_ from Strava aren't
available for import through intervals.icu. An activity that was uploaded to
both Garmin and Strava is accessible.

To configure the earliest date to pull activities, set `fetch_cutoff`. This can
be helpful if you want to first import from local files, and then set up the
intervals API for future activities.

```bash
sqlite3 hotpot.sqlite3 \
    "INSERT OR REPLACE INTO config (key, value) VALUES ('fetch_cutoff', '2026-01-01')"
```

### Strava webhook

> [!WARNING]
>
> Strava no longer supports API access without a premium membership.
>
> Additionally, only the owner of the API (i.e. you) can authenticate.
> You can't share this with multiple people.

If you already upload activity data to Strava, you can use the Strava activity
webhook to import new activities automatically.

To get started, follow the
[Strava API documentation](https://developers.strava.com/) to create your own
application.

Next, authenticate your account through OAuth and save the API tokens in the
database:

```bash
export STRAVA_CLIENT_ID=... \
       STRAVA_CLIENT_SECRET=...\
       STRAVA_WEBHOOK_SECRET=...

hotpot strava-auth

# Authenticate in the browser
open http://localhost:8080/strava/auth
```

After you authenticate, register your server's callback URL with the Strava
API. To complete setup, follow the `curl` commands shown on the success page.

To enable the webhook endpoints, use the `--strava-webhook` flag:

```bash
export STRAVA_CLIENT_ID=... \
       STRAVA_CLIENT_SECRET=... \
       STRAVA_WEBHOOK_SECRET=...

hotpot serve --strava-webhook
```

## License

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program. If not, see <https://www.gnu.org/licenses/>.
