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
  const {
    "modal-dialog": modal,
    div,
    form,
    input,
    label,
    p,
    code,
  } = createElement;

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

  const node = modal({}, [
    div({ slot: "header" }, "Add Files"),
    div({ slot: "body", class: "drop-area" }, [uploadForm, resultContainer]),
  ]);

  document.body.appendChild(node);
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

customElements.define(
  "modal-dialog",
  class extends HTMLElement {
    constructor() {
      super()
        .attachShadow({ mode: "open" })
        .appendChild(
          document.getElementById("template-modal").content.cloneNode(true),
        );
    }

    connectedCallback() {
      const dialog = this.shadowRoot.querySelector("dialog");
      dialog.addEventListener("click", (ev) => {
        ev.stopPropagation();

        // Only clicks outside the content window should close the dialog
        if (ev.originalTarget === dialog) {
          dialog.close();
        }
      });

      dialog.addEventListener("close", () => this.remove());
    }
  },
);

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
        <svg viewBox="0 0 512 512" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" fill="#000000">
          <g stroke-linecap="round" stroke-linejoin="round" stroke="#CCCCCC" stroke-width="5"></g>
          <g stroke-width="0.00512" fill="none" fill-rule="evenodd">
            <g fill="#000000" transform="translate(42.666667, 42.666667)">
              <path d="M106.666667,7.10542736e-15 L106.666667,64 L362.666667,64 L362.666667,320 L426.666667,320 L426.666667,362.666667 L362.666667,362.666667 L362.666667,426.666667 L320,426.666667 L320,362.666667 L64,362.666667 L64,7.10542736e-15 L106.666667,7.10542736e-15 Z M166.336,232.64 L106.666,296.422 L106.666667,320 L320,320 L320,308.725 L274.432,263.168 L235.659405,301.959634 L166.336,232.64 Z M320,106.666667 L106.666667,106.666667 L106.666,233.982 L165.332883,171.293333 L235.648,241.621333 L274.447284,202.831976 L320,248.385 L320,106.666667 Z M245.333333,128 C263.006445,128 277.333333,142.326888 277.333333,160 C277.333333,177.673112 263.006445,192 245.333333,192 C227.660221,192 213.333333,177.673112 213.333333,160 C213.333333,142.326888 227.660221,128 245.333333,128 Z M64,64 L64,106.666667 L7.10542736e-15,106.666667 L7.10542736e-15,64 L64,64 Z"></path>
            </g>
          </g>
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
  const { "modal-dialog": modal, div, style } = createElement;

  const fmt = new Intl.NumberFormat();

  // TODO: maybe display property type
  const rows = props.map(({ key, activity_count }) => {
    return div({ class: "__row" }, [
      div({ class: "__prop", title: key }, key),
      div({ class: "__count" }, fmt.format(activity_count)),
    ]);
  });

  const node = modal({}, [
    style(
      {},
      `
      .property-table {
        width: 100%;
        font-size: small;

        .__row {
          display: grid;
          justify-content: space-between;
          grid-template-columns: 1fr 25%;

          &:nth-child(even) {
            background-color: #fafafa;
          }

          .__prop {
            font-family: monospace;
            text-overflow: ellipsis;
            white-space: nowrap;
            overflow: hidden;
          }

          .__count {
            text-align: right;
          }
        }

        .__header {
          font-weight: bold;
        }
      }
    `,
    ),
    div({ slot: "header" }, "Filterable Properties"),
    div({ slot: "body" }, [
      // TODO: Add docs about how filters work etc.
      div({ class: "property-table" }, [
        div({ class: "__header __row" }, [
          div({}, "Key"),
          div({}, "Num Activities"),
        ]),
        div({ class: "__body" }, rows),
      ]),
    ]),
  ]);

  document.body.appendChild(node);
}
