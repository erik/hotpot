// Schedule microtask (wait until all synchrounous work is done)
const deferOnce = (obj, fn) => {
  obj.$$defer =
    obj.$$defer ||
    Promise.resolve()
      .then(() => fn())
      .finally(() => delete obj.$$defer);
};

const debounce = (fn, timeoutMs = 500) => {
  let id;
  return () => {
    if (id) window.clearTimeout(id);
    id = window.setTimeout(() => fn(), timeoutMs);
  };
};

const livewire = (props) => {
  const live = Object.entries(props).filter(([k, _fn]) => k.startsWith("$"));
  const state = { ...props };
  const watchers = [];

  const derive = () => live.forEach(([key, fn]) => (state[key] = fn(state)));

  // Populate initial live values
  derive();

  return new Proxy(state, {
    set(target, _key, _value) {
      Reflect.set(...arguments);
      derive();

      deferOnce(this, () => watchers.forEach((fn) => fn(target)));
      return true;
    },

    get(target, key, proxy) {
      if (key === "watch") {
        return (...args) => {
          let fn = args[0];

          // Only watch for changes in specific properties
          if (args.length === 2) {
            const [watchedKeys, wrappedFn] = args;

            fn = (obj) => {
              const cur = JSON.stringify(
                Object.entries(obj).filter(([k, _]) => watchedKeys.includes(k)),
              );

              if (cur === wrappedFn.$$cache) return;
              wrappedFn.$$cache = cur;
              wrappedFn(obj);
            };
          }

          // invoke the function once with current state
          deferOnce(fn, () => fn(target));
          watchers.push(fn);

          return proxy;
        };
      }
      return Reflect.get(...arguments);
    },
  });
};

function _createElement(tag, attrs, children) {
  attrs = attrs || {};
  let el = document.createElement(tag);

  for (const [k, v] of Object.entries(attrs)) {
    if (typeof v === "function") {
      el.addEventListener(k, v.bind(this));
    } else if (k === "style" && typeof v === "object") {
      Object.entries(v).forEach(([k, v]) => el.style.setProperty(k, v));
    } else {
      el.setAttribute(k, v);
    }
  }

  const nodes = [children]
    .flat(Infinity)
    .filter((it) => it != null)
    .map((it) =>
      it instanceof Node ? it : document.createTextNode(it.toString()),
    );

  el.append(...nodes);
  return el;
}

// magic curry sauce
const createElement = new Proxy(_createElement, {
  get: (_target, prop, receiver) => (attrs, children) =>
    receiver(prop, attrs, children),
});

// {a: "foo", b: null, c: " "} => "a=foo&c=%20"
function encodeQueryString(obj) {
  return Object.entries(obj)
    .filter(([_k, v]) => v != null && v !== "")
    .map((kv) => kv.map(encodeURIComponent))
    .map(([k, v]) => `${k}=${v}`)
    .join("&");
}

function createUploadModal() {
  const { div, form, input, label, p, code } = createElement;

  const resultContainer = div({ class: "__results" });
  const progressBar = div({ class: "__progress" });

  const uploader = new FileUploader({
    onProgress({ fileName, success, message, progress }) {
      resultContainer.style.display = "block";
      progressBar.style.opacity = 1;
      progressBar.style.width = `${progress}%`;
      resultContainer.prepend(
        div({ class: `__row ${success ? "--success" : "--error"}` }, [
          div({ class: "__file", title: fileName }, fileName),
          message,
        ]),
      );
    },

    onComplete() {
      progressBar.style.opacity = 0;
    },
  });

  const dragHandler = (ev) => {
    ev.preventDefault();
    ev.stopPropagation();

    if (["dragenter", "dragover"].includes(ev.type)) {
      uploadForm.classList.add("--highlight");
    } else if (ev.type !== "dragleave" || ev.currentTarget === uploadForm) {
      uploadForm.classList.remove("--highlight");
    }

    if (ev.type === "drop") {
      uploader.enqueueAll(ev.dataTransfer.files);
    }
  };

  const uploadForm = form({ class: "__target" }, [
    input({
      id: "__file-input",
      type: "file",
      multiple: true,
      style: { display: "none" },
      change: (ev) => uploader.enqueueAll(ev.target.files),
    }),
    label({ for: "__file-input" }, [
      progressBar,
      p({}, [
        "Drop ",
        code({}, ".gpx"),
        ", ",
        code({}, ".tcx"),
        ", and ",
        code({}, ".fit"),
        " files here",
      ]),
    ]),
  ]);

  // lots of redundant event logic
  ["drop", "submit", "dragenter", "dragover", "dragleave"].forEach((type) =>
    uploadForm.addEventListener(type, dragHandler),
  );

  createModal(
    "Add Files",
    div({ class: "drop-area" }, [uploadForm, resultContainer]),
  );
}

class FileUploader {
  constructor(hooks) {
    this.queue = [];
    this.token = window.localStorage.getItem("api-token");
    this.onProgress = hooks.onProgress || (() => {});
    this.onComplete = hooks.onComplete || (() => {});
  }

  updateAPIToken() {
    this.token = window.prompt("Enter API token to continue uploading") || "";
    window.localStorage.setItem("api-token", this.token);
    return this.token !== "";
  }

  enqueueAll(fileList) {
    [...fileList].forEach((f) => this.enqueue(f));
  }

  enqueue(file) {
    this.queue.push(file);
    deferOnce(this, () => this._consumeQueue());
  }

  async _consumeQueue() {
    const numFiles = this.queue.length;
    let numProcessed = 0;

    while (this.queue.length !== 0) {
      const file = this.queue.pop();
      const { success, message } = await this._uploadFile(file);

      this.onProgress({
        success,
        message,
        progress: (++numProcessed / numFiles) * 100,
        fileName: file.name,
      });
    }

    this.onComplete();
  }

  static STATUS_TO_ERROR_MESSAGE = {
    400: "bad HTTP request (bug?)",
    401: "bad upload token",
    404: "uploads not enabled",
    415: "unsupported media type",
    422: "failed to read activity data",
  };

  async _uploadFile(file) {
    let res;
    try {
      const formData = new FormData();
      formData.append("file", file);
      res = await fetch("/upload", {
        method: "POST",
        body: formData,
        headers: {
          Authorization: `Bearer ${this.token}`,
        },
      });
    } catch (err) {
      // NOTE: Can be triggered by backend returning an error before consuming
      // the entire request body (as in the case of invalid file type)
      // https://github.com/hyperium/hyper/issues/2384
      console.error("Network error", err);
      return { success: false, message: err.toString() };
    }

    if (res.status === 200) {
      return { success: true, message: null };
    } else if (res.status === 401) {
      // Unauthorized -- retriable if user updates the token
      if (this.updateAPIToken()) {
        this.enqueue(file);
      } else {
        this.queue = [];
      }
    }

    return {
      success: false,
      message:
        FileUploader.STATUS_TO_ERROR_MESSAGE[res.status] ||
        "(bug) bad server response",
    };
  }
}

function createModal(header, body) {
  const { dialog, div, button } = createElement;

  const closeBtn = button({ class: "__button" }, "Close");
  const node = dialog({ class: "modal", open: "" }, [
    div({ class: "__panel" }, [
      div({ class: "__header" }, header),
      div({ class: "__body" }, body),
      div({ class: "__footer" }, [closeBtn]),
    ]),
  ]);

  // backdrop click or Escape (native <dialog>) closes; close event cleans up
  node.addEventListener("click", (ev) => {
    if (ev.target === node) node.close();
  });
  node.addEventListener("close", () => node.remove());
  closeBtn.addEventListener("click", () => node.close());

  document.body.appendChild(node);
}

class ExportButton {
  constructor(options) {
    this.options = options;
  }

  onAdd(map) {
    const { div, button } = createElement;

    // Too lazy to make a createElementNS implementation
    const btn = button({
      title: "Export image",
    });
    btn.innerHTML = `
        <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" xmlns="http://www.w3.org/2000/svg" style="display: block; margin: auto">
          <path d="M4 8.5A1.5 1.5 0 0 1 5.5 7h2.3l1.5-2h5.4l1.5 2h2.3A1.5 1.5 0 0 1 20 8.5v9a1.5 1.5 0 0 1-1.5 1.5h-13A1.5 1.5 0 0 1 4 17.5z" />
          <circle cx="12" cy="13" r="3.25" />
        </svg>
    `;

    return div(
      {
        class: "maplibregl-ctrl maplibregl-ctrl-group",
        contextmenu: (ev) => ev.preventDefault(),
        click: () => this.onClick(map),
      },
      btn,
    );
  }

  onClick(map) {
    const canvasStyle = map.getCanvas().style;
    const originalCursor = canvasStyle.cursor;

    map.dragPan.disable();
    canvasStyle.cursor = "crosshair";

    map.once("mousedown", (e) => {
      const start = e;
      const geojson = {
        type: "FeatureCollection",
        features: [
          {
            type: "Feature",
            geometry: { type: "Polygon", coordinates: [] },
          },
        ],
      };

      map.addSource("bbox", { type: "geojson", data: geojson }).addLayer({
        id: "bbox",
        source: "bbox",
        type: "fill",
        paint: {
          "fill-outline-color": "white",
          "fill-color": "#00000099",
        },
      });

      const move = (e) => {
        const bounds = new maplibregl.LngLatBounds(start.lngLat, e.lngLat);

        geojson.features[0].geometry.coordinates = [
          [
            bounds.getNorthEast().toArray(),
            bounds.getNorthWest().toArray(),
            bounds.getSouthWest().toArray(),
            bounds.getSouthEast().toArray(),
            bounds.getNorthEast().toArray(),
          ],
        ];
        map.getSource("bbox").setData(geojson);
      };

      map.on("mousemove", move).once("mouseup", (e) => {
        const aspectRatio = Math.abs(
          (start.point.y - e.point.y) / (e.point.x - start.point.x),
        );

        // west, south, east, north
        const bbox = [
          Math.min(start.lngLat.lng, e.lngLat.lng),
          Math.min(start.lngLat.lat, e.lngLat.lat),
          Math.max(start.lngLat.lng, e.lngLat.lng),
          Math.max(start.lngLat.lat, e.lngLat.lat),
        ].join(",");

        // Ensure the largest side doesn't exceed limits
        const [width, height] =
          aspectRatio <= 1
            ? [2000, Math.round(aspectRatio * 2000)]
            : [Math.round(2000 / aspectRatio), 2000];

        const qs = encodeQueryString({ bounds: bbox, width, height });
        const renderUrl = `/render?${qs}&${this.options.$queryString}`;
        window.open(renderUrl, "_blank");

        // Reset map state
        map
          .removeLayer("bbox")
          .removeSource("bbox")
          .off("mousemove", move)
          .dragPan.enable();

        canvasStyle.cursor = originalCursor;
      });
    });
  }
}

const EMPTY_COLLECTION = { type: "FeatureCollection", features: [] };

const DATE_FORMAT = new Intl.DateTimeFormat(undefined, { dateStyle: "medium" });
const NUMBER_FORMAT = new Intl.NumberFormat(undefined, {
  maximumFractionDigits: 1,
});

function activityStats(props) {
  const num = (v, unit) => `${NUMBER_FORMAT.format(v)} ${unit}`;
  return [
    ["Distance", props.total_distance, (v) => num(v, "km")],
    [
      "Elevation gain",
      props.elevation_gain,
      (v) => `+${num(Math.round(v), "m")}`,
    ],
  ]
    .filter(([_label, value]) => value != null)
    .map(([label, value, fmt]) => [label, fmt(value)]);
}

class ActivityInspector {
  constructor(map, options) {
    this.map = map;
    this.options = options;
    this.data = EMPTY_COLLECTION;
    this.popup = null;
    this.hovered = null;
    this.selected = null;
    this.rows = new Map();
    this.request = null;

    map.on("click", (ev) => this.inspect(ev.lngLat, ev.point));
    options.watch(["$queryString"], () => this.clear());
  }

  addLayers() {
    for (const id of ["selection-highlight", "selection"]) {
      if (this.map.getLayer(id)) {
        this.map.removeLayer(id);
      }
    }

    if (this.map.getSource("selection")) {
      this.map.removeSource("selection");
    }

    const layout = { "line-cap": "round", "line-join": "round" };

    this.map
      .addSource("selection", { type: "geojson", data: this.data })
      .addLayer({
        id: "selection",
        type: "line",
        source: "selection",
        layout,
        paint: { "line-color": "#6b8fff", "line-width": 3 },
      })
      .addLayer({
        id: "selection-highlight",
        type: "line",
        source: "selection",
        layout,
        paint: { "line-color": "#6b8fff", "line-width": 3 },
      });

    this.updateHighlight();
  }

  setData(data) {
    this.data = data;
    this.hovered = null;
    this.selected = null;
    this.rows.clear();
    this.map.getSource("selection")?.setData(data);
    this.updateHighlight();
  }

  setHovered(id) {
    this.hovered = id;
    this.updateHighlight();
  }

  setSelected(id) {
    this.selected = id;
    this.updateHighlight();
  }

  updateHighlight() {
    const activeId = this.hovered ?? this.selected;

    if (this.map.getLayer("selection-highlight")) {
      this.map.setFilter("selection-highlight", ["==", ["id"], activeId ?? -1]);
      this.map.setPaintProperty(
        "selection",
        "line-color",
        activeId == null ? "#6b8fff" : "#5f636b",
      );
      this.map.setPaintProperty(
        "selection",
        "line-width",
        activeId == null ? 3 : 1.5,
      );
    }

    this.rows.forEach((row, rowId) => {
      row.classList.toggle("--active", rowId === activeId);
    });
  }

  clear() {
    this.request?.abort();
    this.popup?.remove();
  }

  async inspect(lngLat, point) {
    this.clear();
    const request = new AbortController();
    this.request = request;

    const canvasStyle = this.map.getCanvas().style;
    canvasStyle.cursor = "progress";

    const qs = encodeQueryString({
      lng: lngLat.lng,
      lat: lngLat.lat,

      // Add pixel buffer to click point so we can convert to meters
      // consistently
      radius: lngLat.distanceTo(this.map.unproject([point.x + 8, point.y])),
      zoom: Math.ceil(this.map.getZoom()),
    });

    let data = EMPTY_COLLECTION;
    try {
      const res = await fetch(
        `/api/activities/inspect?${qs}&${this.options.$queryString}`,
        { signal: request.signal },
      );

      if (!res.ok) {
        throw new Error(await res.text());
      }

      data = await res.json();
    } catch (err) {
      if (request.signal.aborted) {
        return;
      }

      console.error("Activity lookup failed", err);
    } finally {
      if (this.request === request) {
        canvasStyle.cursor = "";
      }
    }

    if (data.features.length === 0) return;

    this.setData(data);

    const list = this.renderList(data.features);
    this.popup = new maplibregl.Popup({
      className: "activity-popup",
      closeButton: false,
      maxWidth: "none",
    })
      .setLngLat(lngLat)
      .setDOMContent(list)
      .addTo(this.map)
      .on("close", () => {
        this.popup = null;
        this.setData(EMPTY_COLLECTION);
      });
  }

  renderList(features) {
    const { div, span } = createElement;

    return div(
      { class: "__list" },
      features.map((feature) => {
        const { title, start_time } = feature.properties;
        const name = title || "Untitled Activity";
        const stats = activityStats(feature.properties);

        const row = div(
          {
            class: "__row",
            mouseenter: () => this.setHovered(feature.id),
            mouseleave: () => this.setHovered(null),
            click: () => {
              if (this.selected === feature.id) {
                this.setSelected(null);
              } else {
                this.setSelected(feature.id);
                this.zoomTo(feature);
              }
            },
          },
          [
            div({ class: "__summary" }, [
              div({ class: "__title", title: name }, name),
              div(
                { class: "__date" },
                start_time && DATE_FORMAT.format(new Date(start_time)),
              ),
            ]),
            stats.length > 0
              ? div(
                  { class: "__stats" },
                  stats.map(([label, value]) => span({ title: label }, value)),
                )
              : null,
          ],
        );

        this.rows.set(feature.id, row);
        return row;
      }),
    );
  }

  zoomTo(feature) {
    const bounds = new maplibregl.LngLatBounds();
    feature.geometry.coordinates.flat().forEach((pt) => bounds.extend(pt));

    if (!bounds.isEmpty()) {
      this.map.fitBounds(bounds, { padding: 64 });
    }
  }
}

class UploadButton {
  onAdd(_map) {
    const { div, button } = createElement;

    // Too lazy to make a createElementNS implementation
    const btn = button({
      title: "Add activity files",
    });
    btn.innerHTML = `
        <svg viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M62.75 18H49.5H31C26.5817 18 23 21.5817 23 26V74C23 78.4183 26.5817 82 31 82H68C72.4183 82 76 78.4183 76 74V50C76 50 76 50 76 50C76 50 76 40.2484 76 34" stroke="black" stroke-width="8"/>
            <path d="M62.5 18L76 34" stroke="black" stroke-width="8" stroke-linecap="round"/>
            <path d="M62 18V27.5V28C62 31.3137 64.6863 34 68 34V34H72.0625H76" stroke="black" stroke-width="8" stroke-linecap="round"/>
            <path d="M48 62C48 63.1046 48.8954 64 50 64C51.1046 64 52 63.1046 52 62H48ZM51.4142 36.5858C50.6332 35.8047 49.3668 35.8047 48.5858 36.5858L35.8579 49.3137C35.0768 50.0948 35.0768 51.3611 35.8579 52.1421C36.6389 52.9232 37.9052 52.9232 38.6863 52.1421L50 40.8284L61.3137 52.1421C62.0948 52.9232 63.3611 52.9232 64.1421 52.1421C64.9232 51.3611 64.9232 50.0948 64.1421 49.3137L51.4142 36.5858ZM52 62L52 38H48L48 62H52Z" fill="black"/>
        </svg>
    `;

    return div(
      {
        class: "maplibregl-ctrl maplibregl-ctrl-group",
        contextmenu: (ev) => ev.preventDefault(),
        click: () => createUploadModal(),
      },
      btn,
    );
  }
}

function createPropertyModal(props) {
  const { div, span } = createElement;

  const fmt = new Intl.NumberFormat();

  // null = alphabetical by key; "desc" / "asc" = by count
  let order = null;

  const body = div({ class: "__body" });
  const arrow = span({ class: "__sort-arrow" });
  const countHeader = div({ class: "__sort" }, ["Activities", arrow]);

  countHeader.addEventListener("click", () => {
    order = order === null ? "desc" : order === "desc" ? "asc" : null;
    arrow.textContent = order === "desc" ? "↓" : order === "asc" ? "↑" : "";
    renderRows();
  });

  function renderRows() {
    const sorted = Object.entries(props).sort((a, b) =>
      order === "asc"
        ? a[1].count - b[1].count
        : order === "desc"
          ? b[1].count - a[1].count
          : a[0].localeCompare(b[0]),
    );

    body.replaceChildren(
      ...sorted.map(([key, { count, types }]) =>
        div({ class: "__row" }, [
          div({ class: "__prop", title: key }, key),
          div(
            { class: "__types" },
            types.map((t) => div({ class: "__type" }, t)),
          ),
          div({ class: "__count" }, fmt.format(count)),
        ]),
      ),
    );
  }

  renderRows();

  createModal(
    "Filterable Properties",
    div({ class: "property-table" }, [
      div({ class: "__header __row" }, [
        div({}, "Key"),
        div({}, "Types"),
        countHeader,
      ]),
      body,
    ]),
  );
}

// No string escape, make sure it's trusted
function unsafeHTML(strings, ...values) {
  const htmlString = strings.reduce(
    (acc, str, i) => acc + str + (values[i] ?? ""),
    "",
  );
  const template = document.createElement("template");
  template.innerHTML = htmlString;
  return template.content;
}

function createHelpModal() {
  const { div, span, button } = createElement;

  const tabs = [
    { label: "Filters", content: filterHelpContent() },
    { label: "Gradients", content: gradientHelpContent() },
    { label: "Basemaps", content: basemapHelpContent() },
  ];

  const panels = tabs.map(({ content }) =>
    div({ class: "help-tab-panel" }, content),
  );

  const tabBtns = tabs.map(({ label }, i) => {
    const btn = button({ class: "help-tab-btn" }, label);
    btn.addEventListener("click", () => {
      tabBtns.forEach((b, j) => b.classList.toggle("--active", j === i));
      panels.forEach((p, j) => p.classList.toggle("--active", j === i));
    });
    return btn;
  });

  tabBtns[0].classList.add("--active");
  panels[0].classList.add("--active");

  createModal(
    div({ class: "help-modal-header" }, [
      span({ class: "__title" }, "Help"),
      div({ class: "help-tab-bar" }, tabBtns),
    ]),
    div({ class: "help-panels" }, panels),
  );
}

function filterHelpContent() {
  return unsafeHTML`<div class="help-content">
    <p>
      Choose which activities are included in the heatmap by providing a filter
      expression.
    </p>

    <p>
      These could be used to generate different visualizations for cycling vs
      running, exclude commutes, filter by gear, elevation, etc.
    </p>

    <p>
      Any metadata imported from your activity data can be used, as well as the
      computed properties below, which are automatically added for each
      activity.
    </p>

    <div class="__heading">Filter Syntax</div>
    <ul>
      <li>
        <code>=</code>
        <code>!=</code>
        <code>&lt;</code>
        <code>&lt;=</code>
        <code>&gt;</code>
        <code>&gt;=</code>                    — supported comparisons</li>
      <li><code>key in [a, "b c"]</code>      — match multiple string values</li>
      <li><code>key like "pattern%"</code>    — match a pattern, <code>%</code> is a wildcard</li>
      <li><code>has? "key with spaces"</code> — check if a property exists</li>
    </ul>

    <div class="__heading">Computed Properties</div>
    <ul class="__properties">
      <li><code>average_speed</code>  — average moving speed (km/h)</li>
      <li><code>elapsed_time</code>   — total activity time including pauses (seconds)</li>
      <li><code>elevation_gain</code> — total ascent (meters)</li>
      <li><code>elevation_loss</code> — total descent (meters)</li>
      <li><code>max_elevation</code>  — highest elevation (meters)</li>
      <li><code>max_speed</code>      — fastest instantaneous speed (km/h)</li>
      <li><code>min_elevation</code>  — lowest elevation (meters)</li>
      <li><code>moving_time</code>    — time spent moving (seconds)</li>
      <li><code>total_distance</code> — total distance (km)</li>
    </ul>

    <div class="__heading">Examples</div>
    <div class="__examples">
      <div class="__example">
        <code>elevation_gain > 1000</code>
        <div class="__desc">basic comparison</div>
      </div>
      <div class="__example">
        <code>"Average Temperature" < 5</code>
        <div class="__desc">keys with spaces need quotes</div>
      </div>
      <div class="__example">
        <code>activity_type in [ride, "gravel ride"]</code>
        <div class="__desc">match one of multiple values</div>
      </div>
      <div class="__example">
        <code>name like "Morning%"</code>
        <div class="__desc">wildcard pattern</div>
      </div>
      <div class="__example">
        <code>has? heart_rate</code>
        <div class="__desc">property exists</div>
      </div>
      <div class="__example">
        <code>elapsed_time < 3600 && elevation_gain > 300</code>
        <div class="__desc">combine with &&</div>
      </div>
      <div class="__example">
        <code>!(activity_type in [walk, hike])</code>
        <div class="__desc">negation</div>
      </div>
      <div class="__example">
        <code>elevation_gain > 1000 || (moving_speed > 30 && commute = true)</code>
        <div class="__desc">grouping</div>
      </div>
    </div>
  </div>`;
}

function gradientHelpContent() {
  return unsafeHTML`<div class="help-content">
    <p>
      The gradient controls the color palette used on the heatmap. It's a simple
      mapping of how many times a given tile pixel was visited at each zoom
      level to a color.
    </p>

    <p>
      This can either be one of the built-in gradients or a semicolon separated
      list of <code>threshold:color</code> stops. Pixels visited fewer than the
      first threshold are transparent; pixels visited more than the last
      threshold use the last color.
    </p>

    <p>
      Colors are given as one of <code>RGB</code>, <code>RRGGBB</code>, or
      <code>RRGGBBAA</code>. When an alpha value is not given, it's assumed to
      be fully opaque.
    </p>

    <p>
      The threshold can be as high as 255, but generally the full range won't be
      needed. At higher zoom levels, unless you ride the same route every single
      day for multiple years, generally the visit count will be relatively low.
    </p>

    <div class="__heading">Examples</div>
    <div class="__examples">
      <div class="__example">
        <code>1:FF0000;255:FFFFFF</code>
        <div class="__swatch" style="background-image: linear-gradient(in oklab to right, rgb(255 0 0), white)"></div>
      </div>
      <div class="__example">
        <code>1:000;10:fff</code>
        <div class="__swatch" style="background-image: linear-gradient(in oklab to right, black, white)"></div>
      </div>
      <div class="__example">
        <code>1:0d0221;10:7b2d8b;50:f7644a</code>
        <div class="__swatch" style="background-image: linear-gradient(in oklab to right, rgb(13 2 33), rgb(123 45 139) 18%, rgb(247 100 74))"></div>
      </div>
      <div class="__example">
        <code>1:322bb3;10:9894e5;20:fff</code>
        <div class="__swatch" style="background-image: linear-gradient(in oklab to right, rgb(50 43 179), rgb(152 148 229) 47%, white)"></div>
      </div>
      <div class="__example">
        <code>1:FF000080;10:FF0000</code>
        <div class="__swatch" style="background-image: linear-gradient(in oklab to right, rgb(255 0 0 / 50%), rgb(255 0 0))"></div>
      </div>
      <div class="__example">
        <code>1:00000077;15:6a040f;40:f00</code>
        <div class="__swatch" style="background-image: linear-gradient(in oklab to right, rgb(0 0 0 / 47%), rgb(106 4 15) 36%, rgb(255 0 0))"></div>
      </div>
    </div>
  </div>`;
}

function basemapHelpContent() {
  return unsafeHTML`<div class="help-content">
    <p>
      In addition to the minimal built-in basemaps, custom raster tile layers
      can be used as the background layer. First set the basemap to <code>Custom
      Raster</code>, then paste in the tile URL, which will look something like
      <code>https://example.com/{z}/{x}/{y}.png</code>.
    </p>

    <p>
      <a href="https://leaflet-extras.github.io/leaflet-providers/preview/">Leaflet-providers</a>
      has a large list of (mostly) freely available tile layers, although some
      may require an API key.
    </p>

    <p>
      This is also a tile server, which means you can render two separate
      heatmaps on top of each other. For example, set the date filters to select
      last year's activities with one gradient, copy the formatted tile URL as
      the basemap, then set the date filters to select this year's activities
      with a different gradient.
    </p>
  </div>`;
}
