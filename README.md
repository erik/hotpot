# hotpot

![](https://user-images.githubusercontent.com/188935/273125894-7f76eabb-585b-405d-af16-a93df2d85cb4.png)

Render customizable activity heatmap images from GPS tracks extracted from GPX,
TCX, and FIT files. There's also a built-in web server to serve up [XYZ tiles],
and endpoints to add new data via HTTP POST or [Strava webhooks].

Designed to be self-hosted. It's lightweight and snappy enough to fit onto the
free tier of pretty much anything that can run a Docker container. Even with
100,000 km of activity data, [Fly.io]'s smallest instance can render tiles in
~1 ms.

[XYZ tiles]: https://en.wikipedia.org/wiki/Tiled_web_map
[Strava webhooks]: https://developers.strava.com/docs/webhooks/
[Fly.io]: https://fly.io/

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

<details>
  <summary>Example Gradients</summary>

| Gradient | Rendered |
| -------- | -------- |
| `0:000;0.25:fff`| ![](https://user-images.githubusercontent.com/188935/277203430-269317c9-8539-4bc7-822c-fc199867d830.png) |
| `0:f00;0.1:ff0;0.2:ffff22;0.3:ffffff`| ![](https://user-images.githubusercontent.com/188935/277203443-ef63926a-0316-4a9b-ba5e-2cfdf0281581.png) |
| `0:322bb3;0.10:9894e5;0.15:fff` | ![](https://user-images.githubusercontent.com/188935/277203450-bd929ee0-db3d-4653-9fed-5b3982829091.png) |

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

todo document

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

``` bash
export STRAVA_CLIENT_ID=... \
       STRAVA_CLIENT_SECRET=...\
       STRAVA_WEBHOOK_SECRET=...

hotpot strava-auth

# Grant permission to your app via OAuth
open http://127.0.0.1:8080/strava/auth
```

Once you've authenticated successfully, you'll need to register the callback
URL of your server with Strava's API. Follow the `curl` commands shown on the
success page to complete setup.

## Deployment

To simplify things, a basic `Dockerfile` is included. Mount a volume at
`/data/` to persist the sqlite database between runs.

Since we're using sqlite as our data store, it's easy to first run the bulk
import locally, then copy the database over to a remote host.

### Fly Quick Start

Hotpot should comfortably fit within Fly.io's free tier, and handles the
scale-to-zero behavior gracefully. Follow their [setup
instructions](https://fly.io/docs/hands-on/install-flyctl/) first.

Steps below assume you've cloned this repo locally and already created a local
database.

``` bash
# Create the application
fly launch --ha false

# Create a persistent volume for the DB
fly volumes create hotpot_db -a YOUR_APP_NAME --size 1

# Attach the volume
echo '
[mounts]
  source="hotpot_db"
  destination="/data"
' >> fly.toml

# If you're using the Strava webhook
fly secrets set \
    STRAVA_CLIENT_ID=... \
    STRAVA_CLIENT_SECRET=...\
    STRAVA_WEBHOOK_SECRET=...

# Deploy the app
fly deploy

# Copy local DB over to the app
fly proxy 10022:22 &
scp -P 10022 ./hotpot.sqlite3* root@localhost:/data/

# Restart the app, and we're done.
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

