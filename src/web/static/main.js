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
