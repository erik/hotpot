import sys
import sqlite3

import mapbox_vector_tile as mvt
import flask

app = flask.Flask(__name__)

def get_db():
    if 'db' not in flask.g:
        flask.g.db = sqlite3.connect(sys.argv[1])
    return flask.g.db


# Add CORS headers
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route('/<int:z>/<int:x>/<int:y>')
def get_tile(z, x, y):
    cur = get_db().cursor()
    cur.execute('''
SELECT name, mercator_coords
FROM activity_tiles
JOIN activities ON activity_tiles.activity_id = activities.id
WHERE tile_z = ?
    AND tile_x = ?
    AND tile_y = ?
''', (z, x, y))

    features = []

    for (name, coords) in cur.fetchall():
        # Jank way of converting to WKT
        coords = coords.replace(',', ' ').replace(';', ',')
        features.append({
            'geometry': f"LINESTRING({coords})",
            'properties': { 'name': name },
        })

    if len(features) == 0:
        return flask.Response(status=204)

    pbf = mvt.encode([{
        "name": "activities",
        "features": features,
    }])

    return flask.Response(pbf, mimetype='application/vnd.mapbox-vector-tile')


@app.route('/')
def index():
    return 'ok'


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
