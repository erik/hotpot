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

- Add filters based on arbitrary activity properties (sport type, bike name, commutes, etc.)
- MVT endpoint
- GeoJSON endpoint?
- Import single file
- Render CLI based on bounding box rather than individual tile
- Strip noisy points from GPX files (e.g. inside buildings, etc.)
- Try varying gradient stops based on zoom level
- Dynamic gradients (as query param)
