import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url),
  ts = require("../mobile/node_modules/typescript");

// Load the TypeScript update client with its native dependencies stubbed, so
// the decision logic (which build counts as newer) runs under plain Node.
function loadUpdates({ buildVersion, platform = "android", fetchImpl, executionEnvironment = "bare", configBuildVersion = 999 }) {
  const source = fs.readFileSync(
    new URL("../mobile/src/updates.ts", import.meta.url),
    "utf8",
  );
  const exports = {};
  const context = {
    exports,
    fetch: fetchImpl,
    AbortController,
    setTimeout,
    clearTimeout,
    console,
    require(name) {
      if (name === "react-native") return { Platform: { OS: platform } };
      if (name === "expo-constants") {
        return {
          default: {
            // Expo 55 ConstantsService exports no nativeBuildVersion or nativeAppVersion.
            executionEnvironment,
            expoConfig: { version: "9.9.9", android: { package: "ru.b67t.messenger.mobile", versionCode: configBuildVersion } },
          },
        };
      }
      if (name === "expo-application") {
        return {
          nativeBuildVersion: buildVersion,
          nativeApplicationVersion: "1.2.0",
          applicationId: "ru.b67t.messenger.mobile",
        };
      }
      if (name === "expo-intent-launcher") {
        return { startActivityAsync: async () => ({ resultCode: 0 }) };
      }
      if (name === "expo-file-system/legacy") {
        return {
          cacheDirectory: "file:///cache/",
          deleteAsync: async () => {},
          getInfoAsync: async () => ({ exists: true, size: 10 }),
          createDownloadResumable: () => ({ downloadAsync: async () => ({ uri: "file:///cache/x.apk" }) }),
          getContentUriAsync: async (uri) => `content://${uri}`,
        };
      }
      throw new Error(`Unexpected import ${name}`);
    },
  };
  const js = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2022,
    },
  }).outputText;
  // eslint-disable-next-line no-new-func
  new Function("exports", "require", "fetch", "AbortController", "setTimeout", "clearTimeout", "console", js)(
    exports,
    context.require,
    context.fetch,
    context.AbortController,
    context.setTimeout,
    context.clearTimeout,
    context.console,
  );
  return exports;
}

const jsonResponse = (status, body) => ({
  ok: status >= 200 && status < 300,
  status,
  json: async () => body,
});

test("a newer build number is offered and the URL stays on the configured host", async () => {
  let requested = "";
  const updates = loadUpdates({
    buildVersion: 10,
    fetchImpl: async (url) => {
      requested = url;
      return jsonResponse(200, {
        version: "1.3.0",
        versionCode: 11,
        path: "/app/android.apk",
        size: 1234,
        sha256: "abc",
        notes: "fixes",
      });
    },
  });
  const result = await updates.checkForUpdate("https://67chat.ru/api");
  assert.equal(requested, "https://67chat.ru/api/app/version");
  assert.equal(result.status, "available");
  assert.equal(result.current, 10);
  assert.equal(result.release.versionCode, 11);
  assert.equal(result.url, "https://67chat.ru/api/app/android.apk");
});

test("the same or an older build is never offered", async () => {
  for (const versionCode of [10, 9]) {
    const updates = loadUpdates({
      buildVersion: 10,
      fetchImpl: async () => jsonResponse(200, { versionCode, path: "/app/android.apk" }),
    });
    const result = await updates.checkForUpdate("https://67chat.ru/api");
    assert.equal(result.status, "up-to-date");
  }
});

test("nothing published (204) is up to date, and so is a broken answer", async () => {
  const noContent = loadUpdates({
    buildVersion: 10,
    fetchImpl: async () => ({ ok: false, status: 204, json: async () => null }),
  });
  assert.equal((await noContent.checkForUpdate("https://67chat.ru/api")).status, "up-to-date");

  const serverError = loadUpdates({
    buildVersion: 10,
    fetchImpl: async () => ({ ok: false, status: 503, json: async () => ({}) }),
  });
  assert.equal((await serverError.checkForUpdate("https://67chat.ru/api")).status, "up-to-date");

  // A release without a download path cannot be installed, so it is ignored.
  const noPath = loadUpdates({
    buildVersion: 10,
    fetchImpl: async () => jsonResponse(200, { versionCode: 99 }),
  });
  assert.equal((await noPath.checkForUpdate("https://67chat.ru/api")).status, "up-to-date");
});

test("a network failure or timeout never surfaces as an update prompt", async () => {
  const offline = loadUpdates({
    buildVersion: 10,
    fetchImpl: async () => {
      throw new Error("network down");
    },
  });
  assert.equal((await offline.checkForUpdate("https://67chat.ru/api")).status, "up-to-date");

  const hanging = loadUpdates({
    buildVersion: 10,
    fetchImpl: (_url, options) =>
      new Promise((_resolve, reject) => {
        options.signal.addEventListener("abort", () => reject(new Error("aborted")));
      }),
  });
  // The client gives up after 8s instead of blocking the UI.
  const originalSetTimeout = globalThis.setTimeout;
  assert.equal((await hanging.checkForUpdate("https://67chat.ru/api")).status, "up-to-date");
  assert.equal(typeof originalSetTimeout, "function");
});

test("iOS and builds without a native build number are not updatable", async () => {
  const ios = loadUpdates({ buildVersion: 10, platform: "ios", fetchImpl: async () => jsonResponse(204) });
  assert.equal((await ios.checkForUpdate("https://67chat.ru/api")).status, "unsupported");

  const expoGo = loadUpdates({ buildVersion: undefined, fetchImpl: async () => jsonResponse(204) });
  assert.equal((await expoGo.checkForUpdate("https://67chat.ru/api")).status, "unsupported");
});

test("Expo 55 resolves the installed binary version, not the embedded config version", () => {
  const updates = loadUpdates({ buildVersion: "12" });
  assert.equal(updates.currentVersionCode(), 12);
  assert.equal(updates.currentVersionName(), "1.2.0");
});

test("Expo Go's host build number must not trigger a messenger APK update", async () => {
  const updates = loadUpdates({
    buildVersion: "1",
    executionEnvironment: "storeClient",
    fetchImpl: () => { throw new Error("Expo Go must not request releases"); },
  });
  assert.equal((await updates.checkForUpdate("https://67chat.ru/api")).status, "unsupported");
});

test("missing or invalid native versions never fall back to app config", () => {
  for (const buildVersion of [undefined, null, "", "garbage", "0", "-1", "1.5"]) {
    assert.equal(loadUpdates({ buildVersion }).currentVersionCode(), null);
  }
});
