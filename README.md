# hotpot

A zippy little heatmap tile server.

![](https://user-images.githubusercontent.com/188935/273125894-7f76eabb-585b-405d-af16-a93df2d85cb4.png)

## Usage

```
# Create a new activity database
cargo run --release -- import [path to GPX files]

# Start a web server
cargo run --release -- serve

open http://localhost:8080
```


## TODO

- Dynamic filtering for web endpoint (activity based, etc.)
- MVT endpoint
- Import single file
- Pull additional metadata from Strava's `activities.csv` file.
- Render CLI based on bounding box rather than individual tile
- Arbitrary filters on CLI (unsafe SQL interpolation)
- Strip noisy points from GPX files (e.g. inside buildings, etc.)
- Try varying gradient stops based on zoom level
- Dynamic gradients (as query param)
