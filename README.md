# hotpot

![](https://user-images.githubusercontent.com/188935/273125894-7f76eabb-585b-405d-af16-a93df2d85cb4.png)

Render customizable activity heatmap images from GPS tracks extracted from GPX,
TCX, and FIT files. Includes a built-in web server for [XYZ tiles] and endpoints
to add new data via HTTP POST or [Strava webhooks].

Designed to run locally or be self-hosted. Lightweight enough to run on free
tiers of most Docker-compatible platforms. Even with 100,000 km of activity
data, [Fly.io]'s smallest instance can render tiles in a few ms.

[XYZ tiles]: https://en.wikipedia.org/wiki/Tiled_web_map
[Strava webhooks]: https://developers.strava.com/docs/webhooks/
[Fly.io]: https://fly.io/

## Installation

### Build from source
```bash
# Build with Cargo (requires Rust toolchain)
cargo build --release

# The binary will be available at ./target/release/hotpot
hotpot serve

# Visit http://127.0.0.1:8080 to browse the map
```

### Docker
```bash
# Either pull the pre-built container from GitHub Container Registry
docker pull ghcr.io/erik/hotpot:main

# Or build the Docker image yourself
docker build -t hotpot .

# Run the container (always mount a volume at /data for the database)
docker run -p 8080:8080 -v ./data:/data hotpot

# Visit http://127.0.0.1:8080 to browse the map
```

## Quick Start

### Import Activities

```bash
# Import an entire directory of activities in parallel
hotpot import [path/to/files/]

# Import from Strava data export, including Strava metadata (title, which bike
# you used, the weather, ...)
hotpot import \
    strava_export/activities/ \
    --join strava_export/activities.csv
```

Or use the browser UI by running:

```bash
# If your server is accessible to the internet, set this environment variable so
# that only you can upload.
export HOTPOT_UPLOAD_TOKEN=xyz...

hotpot serve --upload

# Open the browser and open the file upload dialog by clicking the "Add activity
# files" button
open http://localhost:8080
```

### Create Heatmaps

After importing, you'll have a SQLite database with all your activities and can
start visualizing them.

```bash
# Run a tile server and web UI on http://127.0,0,1:8080
hotpot serve

# Or generate a static image (to create the bounds, use a tool like
# https://boundingbox.klokantech.com/)
hotpot render \
    --bounds='-120.7196,32.2459,-116.9234,35.1454' \
    --width 2000 \
    --height 2000 \
    --output heatmap.png
```

See `hotpot --help` for full details on how to use the CLI.

## Customization

### Gradients

There are several built in palettes available for converting the raw frequency
data into colored pixels, which can be set via the `?color={...}` query
parameter. A list of these is available in the map view.

In addition to the presets, custom gradients can also be used via the
`?gradient={...}` parameter. With this, we specify a sequence of threshold
values (how many times a particular pixel was visited) along with an associated
color. Values falling between the thresholds will be smoothly interpolated to a
reasonable color.

For example, if we want to display pure red when we've visited a pixel once, and
white when we've visited 255 times (or more), we'd use `1:FF0000;255:FFFFFF`.

Color codes are interpreted as hex RGBA values in `RGB`, `RRGGBB` or `RRGGBBAA`
formats. If alpha values are not given, they are assumed to be `FF` (fully
opaque).

<details>
  <summary>Example Gradients</summary>

| Gradient                          | Rendered                                                                                                 |
| --------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `1:000;10:fff`                    | ![](https://user-images.githubusercontent.com/188935/277203430-269317c9-8539-4bc7-822c-fc199867d830.png) |
| `1:f00;5:ff0;10:ffff22;20:ffffff` | ![](https://user-images.githubusercontent.com/188935/277203443-ef63926a-0316-4a9b-ba5e-2cfdf0281581.png) |
| `1:322bb3;10:9894e5;20:fff`       | ![](https://user-images.githubusercontent.com/188935/277203450-bd929ee0-db3d-4653-9fed-5b3982829091.png) |

</details>

### Filters

We can also choose which activities we're interested in visualizing
dynamically through the `?filter={...}` parameter.

Any properties available when the activity was added (either via webhook
or bulk import) can be used in the filter expression, but the exact names
will vary based on your data.

For example, we may want to generate different tiles for cycling vs hiking,
exclude commutes, which gear we used, a minimum elevation gain, etc.

```json5
{
  // Basic numeric comparisons: <, <=, >, >=
  elevation_gain: { ">": 1000 },

  // Match/exclude multiple values
  bike: { any_of: ["gravel", "mtb"] },
  activity_type: { none_of: ["Run"] },

  // Substring match (e.g. match "morning commute" + "commute #9")
  title: { matches: "commute" },

  // Property key exists
  max_hr: { exists: true },

  // Multiple expressions can be applied (evaluated as an AND)
  distance: { ">": 100, "<": 200 },
}
```

## Activity Uploads

Hotpot supports two mechanisms for adding new data to the `sqlite3` database
over HTTP:

1. `POST /upload`: Manually upload a single GPX, TCX, or FIT file
2. Strava webhook: Subscribe to new activity uploads automatically

### HTTP Upload

Run the server with the `--upload` flag. Any files that can be imported on the
command line can be `POST`ed to the server via the `/upload` endpoint using
`multipart/form-data` encoding.

```bash
curl -X POST \
  http://hotpot.example.com/upload \
  --header 'Authorization: Bearer MY_TOKEN_HERE' \
  --form file=@activity.gpx
```

The `Authorization` header is required only when `HOTPOT_UPLOAD_TOKEN` is set.
When not provided, unauthenticated uploads are enabled.

### Strava Webhook

If you're already uploading activity data to Strava, you can use their activity
webhook to import new activities automatically.

To get started, follow the [Strava API
documentation](https://developers.strava.com/) to create your own application.

> **NOTE**
>
> Strava limits new APIs to only allow the owner of the API to authenticate.
> You won't be able to share this with multiple people.

Next, we can use oauth to authenticate our account and save the API tokens in
the database.

```bash
export STRAVA_CLIENT_ID=... \
       STRAVA_CLIENT_SECRET=...\
       STRAVA_WEBHOOK_SECRET=...

hotpot strava-auth

# Authenticate via browser
open http://127.0.0.1:8080/strava/auth
```

Once you've authenticated successfully, you'll need to register the callback
URL of your server with Strava's API. Follow the `curl` commands shown on the
success page to complete setup.

## Deployment

To simplify things, a basic `Dockerfile` is included. Mount a volume at
`/data/` to persist the sqlite database between runs.

```console
docker build -t hotpot .
docker run -p 8080:8080 -v ./data:/data hotpot
```

Since we're using sqlite as our data store, it's easy to first run the bulk
import locally, then copy the database over to a remote host.

### Fly Quick Start

Hotpot should comfortably fit within Fly.io's free tier, and handles the
scale-to-zero behavior gracefully. Follow their [setup
instructions](https://fly.io/docs/hands-on/install-flyctl/) first.

Steps below assume you've cloned this repo locally and already created a local
database.

```bash
# Create app
fly launch --ha false

# Create and attach volume
fly volumes create hotpot_db -a YOUR_APP_NAME --size 1
echo '
[mounts]
  source="hotpot_db"
  destination="/data"
' >> fly.toml

# Set secrets if using Strava
fly secrets set \
    STRAVA_CLIENT_ID=... \
    STRAVA_CLIENT_SECRET=...\
    STRAVA_WEBHOOK_SECRET=...

# Deploy and copy local database to remote host
fly deploy
fly proxy 10022:22 &
scp -P 10022 ./hotpot.sqlite3* root@localhost:/data/
fly app restart
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
