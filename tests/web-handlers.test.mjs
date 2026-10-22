import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Guards a bug that shipped: markup called `toggleVideoNotePlayback(this)` from
// an inline onclick, but the function was never defined, so every video note
// was silently unplayable. Inline handlers resolve on `window`, so any name used
// there must be assigned to window somewhere in the browser bundle.

const webDir = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../web",
);

const browserFiles = [
  "app.js",
  "views.js",
  "ui.js",
  "media.js",
  "chat-actions.js",
  "chat-runtime.js",
  "folders.js",
  "local-data.js",
  "notifications.js",
  "message-list.js",
  "errors.js",
  "auth-flow.js",
];

function read(file) {
  return fs.readFileSync(path.join(webDir, file), "utf8");
}

function handlerNames(source) {
  const JS_KEYWORDS = new Set([
    "if", "else", "for", "while", "switch", "return", "function", "typeof",
    "new", "delete", "void", "await", "try", "catch", "event", "this",
  ]);
  const names = new Set();
  // onclick="name(...)", onclick='name(...)', and template-literal values that
  // still contain a literal name before the first interpolation.
  const re = /\bon[a-z]+\s*=\s*(["'])([\s\S]*?)\1/g;
  let match;
  while ((match = re.exec(source)) !== null) {
    const value = match[2];
    // Only the first token of the handler is the function being invoked. Skip
    // inline statements such as onkeydown="if(event.key===' ') …".
    const call = /^\s*([A-Za-z_$][\w$]*)\s*\(/.exec(value);
    if (!call) continue;
    const name = call[1];
    if (JS_KEYWORDS.has(name)) continue;
    names.add(name);
  }
  return names;
}

function windowAssignments(source) {
  const names = new Set();
  const re = /(?:window|globalThis|self)\.([A-Za-z_$][\w$]*)\s*=/g;
  let match;
  while ((match = re.exec(source)) !== null) names.add(match[1]);
  return names;
}

test("every inline handler in the web bundle resolves to a window function", () => {
  const sources = browserFiles.map((file) => ({ file, source: read(file) }));
  const defined = new Set();
  for (const { source } of sources) {
    for (const name of windowAssignments(source)) defined.add(name);
  }

  const missing = [];
  for (const { file, source } of sources) {
    for (const name of handlerNames(source)) {
      if (!defined.has(name)) missing.push(`${file}: ${name}`);
    }
  }

  assert.deepEqual(missing, [], `handlers with no window function: ${missing.join(", ")}`);
});

test("video notes are playable: the toggle handler exists and the markup uses it", () => {
  const app = read("app.js");
  assert.match(app, /window\.toggleVideoNotePlayback\s*=/, "toggleVideoNotePlayback must be defined");
  assert.match(app, /toggleVideoNotePlayback\(this\)/, "video note markup must call the handler");

  // A video note without a play handler and without native controls is dead UI.
  const noteMarkup = /<div class="video-note"[\s\S]*?<\/div>/.exec(app)?.[0] || "";
  assert.ok(noteMarkup.length > 0, "video note markup missing");
  assert.match(noteMarkup, /toggleVideoNotePlayback/, "video note must be tappable");
  assert.match(noteMarkup, /vn-ring-progress/, "video note must render a progress ring");
  assert.doesNotMatch(
    noteMarkup,
    /border:3px solid/,
    "video notes must not have the old accent border",
  );
});

test("the dead legacy renderers are gone", () => {
  const app = read("app.js");
  assert.doesNotMatch(app, /function legacyRender\s*\(/, "legacyRender is dead code");
  assert.doesNotMatch(app, /function legacyRenderMessages\s*\(/, "legacyRenderMessages is dead code");
  // The live path must still be intact.
  assert.match(app, /function buildRenderedMessagesHtml\s*\(/);
  assert.match(app, /function renderMessageBody\s*\(/);
});
