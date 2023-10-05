# hotpot

Heatmap tile server.

## Usage

```
# Create a new activity database
cargo run --release -- import [path to GPX files]

# Start a web server
cargo run --release -- serve

open http://localhost:8080
```


## TODO

- Trimming initial start/end of activity (distance, privacy zones, etc.)
- Dynamic filtering for web endpoint (activity based, etc.)
- MVT endpoint
- TCX support
- Webhook for new activities from Strava
- Import single file
- Pull additional metadata from Strava's `activities.csv` file.
- Render CLI based on bounding box rather than individual tile
- Arbitrary filters on CLI (unsafe interpolation)
- Better logging / debug output split.
