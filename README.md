# hotpot

Heatmap tile server.

```
Usage: hotpot [DB_PATH] <COMMAND>

Commands:
import  Import GPX and FIT files from a directory
tile    Render a tile
serve   Start a raster tile server
help    Print this message or the help of the given subcommand(s)

Arguments:
[DB_PATH]  Path to database [default: ./hotpot.sqlite3]

Options:
-h, --help     Print help
-V, --version  Print version
```

## TODO

- Trimming initial start/end of activity (distance, privacy zones, etc.)
- Dynamic filtering for web endpoint (time based, activity based)
- MVT endpoint
- TCX support
- Webhook for new activities from Strava
- Import single file
- Pull additional metadata from Strava's `activities.csv` file.