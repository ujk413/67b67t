// Android app releases: version manifest + APK download.
//
// The phone asks GET /app/version on start and on foreground; when the build
// number there is higher than the installed one, it downloads the APK from
// GET /app/android.apk and hands it to the system installer.
//
// Everything is driven by two files on disk, written by the release job:
//   <dir>/latest.json   {version, versionCode, file, size, sha256, notes, publishedAt}
//   <dir>/<file>        the signed APK
// No database row and no restart are involved: publishing a release is copying
// the two files onto the server.
//
// Trust: the APK must be signed with the same key as the installed app or
// Android refuses to install it, so a tampered file fails closed at install
// time. sha256/size are exposed so the client can reject a truncated download
// before it bothers the user with an installer prompt.

import fs from "fs";
import path from "path";

const MAX_APK_BYTES = 400 * 1024 * 1024;
const MANIFEST_CACHE_MS = 2000;
const APK_MIME = "application/vnd.android.package-archive";

function readManifest(dir) {
  const manifestPath = path.join(dir, "latest.json");
  let stat;
  try {
    stat = fs.statSync(manifestPath);
  } catch {
    return null;
  }
  let raw;
  try {
    raw = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  } catch {
    return null;
  }
  const versionCode = Number(raw?.versionCode);
  const file = String(raw?.file || "");
  if (!Number.isInteger(versionCode) || versionCode <= 0) return null;
  // A manifest that points outside its own directory is treated as absent.
  if (!file || file !== path.basename(file)) return null;
  const apkPath = path.join(dir, file);
  let apkStat;
  try {
    apkStat = fs.statSync(apkPath);
  } catch {
    return null;
  }
  if (!apkStat.isFile() || apkStat.size <= 0 || apkStat.size > MAX_APK_BYTES) return null;
  return {
    version: String(raw?.version || ""),
    versionCode,
    file,
    apkPath,
    size: apkStat.size,
    sha256: String(raw?.sha256 || ""),
    notes: String(raw?.notes || ""),
    publishedAt: String(raw?.publishedAt || ""),
    mandatory: raw?.mandatory === true,
    minSupportedVersionCode: Number.isInteger(Number(raw?.minSupportedVersionCode))
      ? Number(raw.minSupportedVersionCode)
      : 0,
  };
}

export function installAppRelease(app, { logger, dir = process.env.APP_RELEASE_DIR || "/app/releases" }) {
  let cache = { at: 0, value: null };

  function manifest() {
    const now = Date.now();
    if (now - cache.at < MANIFEST_CACHE_MS) return cache.value;
    cache = { at: now, value: readManifest(dir) };
    return cache.value;
  }

  // Public: the app must be able to look for an update before it has a session
  // (and on a device whose session expired). Rate limited to keep it cheap.
  app.get(
    "/app/version",
    { config: { rateLimit: { max: 120, timeWindow: "1 minute" } } },
    async (req, reply) => {
      const release = manifest();
      reply.header("Cache-Control", "no-cache");
      if (!release) {
        // 204: "nothing published yet" — the client treats it as up to date.
        return reply.code(204).send();
      }
      return {
        version: release.version,
        versionCode: release.versionCode,
        // Relative on purpose: the same client base URL works for the domain
        // and for the Tor hidden service.
        path: "/app/android.apk",
        size: release.size,
        sha256: release.sha256,
        notes: release.notes,
        publishedAt: release.publishedAt,
        mandatory: release.mandatory,
        minSupportedVersionCode: release.minSupportedVersionCode,
      };
    },
  );

  app.get(
    "/app/android.apk",
    { config: { rateLimit: { max: 60, timeWindow: "1 minute" } } },
    async (req, reply) => {
      const release = manifest();
      if (!release) return reply.code(404).send({ error: "not_found" });

      const etag = release.sha256 ? `"${release.sha256}"` : undefined;
      reply
        .header("Content-Disposition", `attachment; filename="${release.file}"`)
        .header("Accept-Ranges", "bytes")
        .header("Cache-Control", "no-cache");
      if (etag) reply.header("ETag", etag);
      if (etag && req.headers["if-none-match"] === etag) return reply.code(304).send();

      // Range support so a dropped download can resume instead of restarting
      // a 170 MB transfer on a phone connection.
      const range = String(req.headers.range || "");
      const match = /^bytes=(\d*)-(\d*)$/.exec(range.trim());
      if (match && (match[1] || match[2])) {
        const total = release.size;
        let start = match[1] ? Number(match[1]) : NaN;
        let end = match[2] ? Number(match[2]) : NaN;
        if (!Number.isFinite(start) && Number.isFinite(end)) {
          // Suffix range: last N bytes.
          start = Math.max(0, total - end);
          end = total - 1;
        } else if (Number.isFinite(start) && !Number.isFinite(end)) {
          end = total - 1;
        }
        if (
          !Number.isFinite(start) ||
          !Number.isFinite(end) ||
          start < 0 ||
          end < start ||
          start >= total
        ) {
          return reply
            .code(416)
            .header("Content-Range", `bytes */${total}`)
            .type("application/json")
            .send({ error: "range_not_satisfiable" });
        }
        end = Math.min(end, total - 1);
        reply
          .code(206)
          .type(APK_MIME)
          .header("Content-Range", `bytes ${start}-${end}/${total}`)
          .header("Content-Length", String(end - start + 1));
        return reply.send(fs.createReadStream(release.apkPath, { start, end }));
      }

      reply.type(APK_MIME).header("Content-Length", String(release.size));
      return reply.send(fs.createReadStream(release.apkPath));
    },
  );

  logger?.info?.(`App release endpoint ready (dir=${dir})`);
  return { manifest };
}
