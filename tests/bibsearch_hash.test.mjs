import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";

const testDir = path.dirname(fileURLToPath(import.meta.url));
const moduleUrl = pathToFileURL(
  path.resolve(testDir, "../assets/js/bibsearch.js")
).href;

let domReady;

const makeClassList = () => {
  const classes = new Set();
  return {
    add: (name) => classes.add(name),
    contains: (name) => classes.has(name),
    remove: (name) => classes.delete(name),
  };
};

// bibsearch.js registers its DOMContentLoaded work at import time.
globalThis.document = {
  addEventListener: (type, callback) => {
    if (type === "DOMContentLoaded") domReady = callback;
  },
};
await import(moduleUrl);

const runPage = ({ hash = "", elementIds = [], text = "Alpha publication" }) => {
  const input = { value: "", addEventListener: () => {} };
  const item = { innerText: text, classList: makeClassList() };
  const elements = new Map(elementIds.map((id) => [id, { id }]));
  let hashChange;

  globalThis.CSS = {};
  globalThis.document = {
    addEventListener: () => {},
    getElementById: (id) => {
      if (id === "bibsearch") return input;
      return elements.get(id) ?? null;
    },
    querySelectorAll: (selector) => {
      if (selector === ".bibliography > li") return [item];
      return [];
    },
  };
  globalThis.window = {
    addEventListener: (type, callback) => {
      if (type === "hashchange") hashChange = callback;
    },
    location: { hash },
  };

  domReady();

  return {
    input,
    item,
    changeHash: (nextHash) => {
      globalThis.window.location.hash = nextHash;
      hashChange();
    },
  };
};

test("a hash that names a document element is not used as a publication query", () => {
  const page = runPage({ hash: "#main-content", elementIds: ["main-content"] });

  assert.equal(page.input.value, "");
  assert.equal(page.item.classList.contains("unloaded"), false);
});

test("an arbitrary element id is treated as navigation, not filtering", () => {
  const page = runPage({ hash: "#methodology", elementIds: ["methodology"] });

  assert.equal(page.input.value, "");
  assert.equal(page.item.classList.contains("unloaded"), false);
});

test("an encoded element id is decoded before the element lookup", () => {
  const page = runPage({ hash: "#main%2Dcontent", elementIds: ["main-content"] });

  assert.equal(page.input.value, "");
  assert.equal(page.item.classList.contains("unloaded"), false);
});

test("a query hash still filters publications and fills the input", () => {
  const matching = runPage({ hash: "#alpha" });
  assert.equal(matching.input.value, "alpha");
  assert.equal(matching.item.classList.contains("unloaded"), false);

  const missing = runPage({ hash: "#zebra" });
  assert.equal(missing.input.value, "zebra");
  assert.equal(missing.item.classList.contains("unloaded"), true);
});

test("an empty hash keeps the existing show-all behavior", () => {
  const page = runPage({ hash: "" });

  assert.equal(page.input.value, "");
  assert.equal(page.item.classList.contains("unloaded"), false);
});

test("a hash is only treated as navigation when the element exists", () => {
  const page = runPage({ hash: "#main-content", elementIds: [] });

  assert.equal(page.input.value, "main-content");
  assert.equal(page.item.classList.contains("unloaded"), true);
});

test("hashchange to an element does not apply the hash as a query", () => {
  const page = runPage({ hash: "#alpha", elementIds: ["main-content"] });
  assert.equal(page.input.value, "alpha");

  page.changeHash("#main-content");

  assert.notEqual(page.input.value, "main-content");
  assert.equal(page.item.classList.contains("unloaded"), false);
});
