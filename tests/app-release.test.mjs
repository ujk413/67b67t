import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { createRequire } from "node:module";
import { installAppRelease } from "../api/app-release.js";

const require = createRequire(import.meta.url),
  Fastify = require("../api/node_modules/fastify");

function tempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "messenger67-release-"));
}

function publish(dir, { versionCode = 11, version = "1.3.0", file = "67chat.apk", bytes = "APK-BYTES", manifest } = {}) {
  fs.writeFileSync(path.join(dir, file), bytes);
  const doc =
    manifest ??
    {
      version,
      versionCode,
      file,
      size: Buffer.byteLength(bytes),
      sha256: crypto.createHash("sha256").update(bytes).digest("hex"),
      notes: "test release",
      publishedAt: "2026-10-22T00:00:00Z",
    };
  fs.writeFileSync(path.join(dir, "latest.json"), JSON.stringify(doc));
  return doc;
}

async function boot(dir) {
  const app = Fastify();
  installAppRelease(app, { dir, logger: null });
  await app.ready();
  return app;
}

test("no published release answers 204 so the client stays quiet", async () => {
  const app = await boot(tempDir());
  const res = await app.inject({ url: "/app/version" });
  assert.equal(res.statusCode, 204);
  assert.equal(res.body, "");
  assert.equal((await app.inject({ url: "/app/android.apk" })).statusCode, 404);
  await app.close();
});

test("a published release is announced with a relative path and its hash", async () => {
  const dir = tempDir();
  const doc = publish(dir);
  const app = await boot(dir);
  const res = await app.inject({ url: "/app/version" });
  assert.equal(res.statusCode, 200);
  const body = res.json();
  assert.equal(body.versionCode, 11);
  assert.equal(body.version, "1.3.0");
  // Relative on purpose: the same client base URL works over Tor too.
  assert.equal(body.path, "/app/android.apk");
  assert.equal(body.size, Buffer.byteLength("APK-BYTES"));
  assert.equal(body.sha256, doc.sha256);
  assert.equal(body.mandatory, false);
  await app.close();
});

test("the APK is served whole, with an ETag, and supports resumable ranges", async () => {
  const dir = tempDir();
  const bytes = "0123456789abcdef";
  const doc = publish(dir, { bytes });
  const app = await boot(dir);

  const full = await app.inject({ url: "/app/android.apk" });
  assert.equal(full.statusCode, 200);
  assert.equal(full.headers["content-type"], "application/vnd.android.package-archive");
  assert.equal(full.rawPayload.toString(), bytes);
  assert.equal(full.headers.etag, `"${doc.sha256}"`);

  const cached = await app.inject({
    url: "/app/android.apk",
    headers: { "if-none-match": `"${doc.sha256}"` },
  });
  assert.equal(cached.statusCode, 304);

  const middle = await app.inject({
    url: "/app/android.apk",
    headers: { range: "bytes=4-7" },
  });
  assert.equal(middle.statusCode, 206);
  assert.equal(middle.rawPayload.toString(), "4567");
  assert.equal(middle.headers["content-range"], `bytes 4-7/${bytes.length}`);

  const suffix = await app.inject({
    url: "/app/android.apk",
    headers: { range: "bytes=-3" },
  });
  assert.equal(suffix.rawPayload.toString(), "def");

  const open = await app.inject({
    url: "/app/android.apk",
    headers: { range: "bytes=10-" },
  });
  assert.equal(open.rawPayload.toString(), "abcdef");

  const past = await app.inject({
    url: "/app/android.apk",
    headers: { range: "bytes=99-120" },
  });
  assert.equal(past.statusCode, 416);
  await app.close();
});

test("a manifest pointing outside its directory or at a missing file is ignored", async () => {
  const dir = tempDir();
  const app = await boot(dir);

  // Path traversal must not be able to serve /etc/passwd or the .env file.
  publish(dir, { manifest: { versionCode: 12, file: "../../.env" } });
  assert.equal((await app.inject({ url: "/app/version" })).statusCode, 204);
  assert.equal((await app.inject({ url: "/app/android.apk" })).statusCode, 404);

  // Manifest present, APK missing: still nothing to offer.
  fs.writeFileSync(
    path.join(dir, "latest.json"),
    JSON.stringify({ versionCode: 12, file: "gone.apk" }),
  );
  assert.equal((await app.inject({ url: "/app/version" })).statusCode, 204);

  // Corrupt JSON is treated as "no release", never a 500.
  fs.writeFileSync(path.join(dir, "latest.json"), "{not json");
  assert.equal((await app.inject({ url: "/app/version" })).statusCode, 204);

  // A nonsensical build number cannot trigger an update.
  fs.writeFileSync(
    path.join(dir, "latest.json"),
    JSON.stringify({ versionCode: 0, file: "67chat.apk" }),
  );
  assert.equal((await app.inject({ url: "/app/version" })).statusCode, 204);
  await app.close();
});

test("publishing a new build is picked up without restarting the API", async () => {
  const dir = tempDir();
  const app = await boot(dir);
  assert.equal((await app.inject({ url: "/app/version" })).statusCode, 204);

  publish(dir, { versionCode: 13, version: "1.4.0" });
  // The manifest is cached for a couple of seconds to keep the endpoint cheap.
  await new Promise((resolve) => setTimeout(resolve, 2100));
  const res = await app.inject({ url: "/app/version" });
  assert.equal(res.statusCode, 200);
  assert.equal(res.json().versionCode, 13);
  await app.close();
});
