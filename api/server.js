import {
  installMobileRoutes,
  getMobileMessage,
  createDirectRoom,
} from "./mobile.js";
import { createPushSender, installNativePush } from "./push.js";
import { installSocketSession } from "./socket-session.js";
import { installWebPush } from './web-push.js';
import { installCalls } from "./calls.js";
import { installAppRelease } from "./app-release.js";
import Fastify from "fastify";
import cors from "@fastify/cors";
import cookie from "@fastify/cookie";
import multipart from "@fastify/multipart";
import rateLimit from "@fastify/rate-limit";
import websocket from "@fastify/websocket";
import argon2 from "argon2";
import crypto from "crypto";
import fs from "fs";
import path from "path";
import { Pool } from "pg";
import { spawn } from "child_process";
import {
  S3Client,
  PutObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";

const {
  DATABASE_URL,
  HCAPTCHA_SECRET = "",
  NODE_ENV = "production",
  PORT = "3000",
  ORIGIN = process.env.ORIGIN ||
    (NODE_ENV === "development" ? "http://localhost" : "https://67chat.ru"),
  TOKEN_TTL_DAYS = "30",
  COOKIE_SECURE = "auto",
  UPLOAD_DIR = "/app/uploads",
  USE_S3 = "false",
  S3_ENDPOINT = "https://s3.twcstorage.ru",
  S3_BUCKET = "",
  S3_ACCESS_KEY = "",
  S3_SECRET_KEY = "",
  S3_REGION = "ru-1",
  SWIFT_ENDPOINT = "https://swift.twcstorage.ru",
  SWIFT_ACCESS_KEY = "",
  SWIFT_SECRET_KEY = "",
  ADMIN_USERNAMES = "",
  SYSTEM_ADMIN_USERNAME = "",
  SYSTEM_ADMIN_PASSWORD = "",
  SYSTEM_CHANNEL_TITLE = "",
  ENCRYPTION_KEY = process.env.ENCRYPTION_KEY || "",
  FCM_SERVER_KEY = process.env.FCM_SERVER_KEY || "",
  // TURN REST credentials (coturn: use-auth-secret / static-auth-secret=TURN_SECRET)
  TURN_SECRET = "",
  TURN_URLS = "",
  TURN_EXTERNAL_IP = "",
  TURN_CREDENTIAL_TTL_SEC = "3600",
} = process.env;

// Upper bound for a single E2E ciphertext blob (message create/edit).
const MAX_ENCRYPTED_CONTENT_BYTES = 100 * 1024;
// Upper bounds for E2E public key material (base64 strings) on key upload.
const MAX_E2E_KEY_CHARS = 1024;
const MAX_E2E_DEVICE_ID_CHARS = 64;
const MAX_E2E_PREKEYS_PER_UPLOAD = 100;

if (!DATABASE_URL) throw new Error("DATABASE_URL is required");
if (!ENCRYPTION_KEY) {
  throw new Error(
    "ENCRYPTION_KEY is required. Generate with: openssl rand -base64 32",
  );
}
if (["1", "true", "yes"].includes(String(USE_S3).toLowerCase()) && !S3_BUCKET) {
  throw new Error("S3_BUCKET is required when USE_S3 is enabled");
}

// Tiny logger wrapper. Uses the Fastify (pino) logger once `app` exists and falls
// back to console during early startup. Errors are reduced to name/code/message so
// request payloads, tokens and ciphertext never end up in the logs.
let appLogger = null;
function safeErr(err) {
  if (!err || typeof err !== "object") return { message: String(err ?? "") };
  return { name: err.name, code: err.code, message: err.message };
}
function emitLog(level, msg, data) {
  const payload =
    data instanceof Error
      ? { err: safeErr(data) }
      : data && typeof data === "object"
        ? data
        : undefined;
  if (appLogger) {
    if (payload) appLogger[level](payload, msg);
    else appLogger[level](msg);
    return;
  }
  const fn = level === "info" ? console.log : console[level] || console.log;
  if (payload) fn(msg, payload);
  else fn(msg);
}
const log = {
  info: (msg, data) => emitLog("info", msg, data),
  warn: (msg, data) => emitLog("warn", msg, data),
  error: (msg, data) => emitLog("error", msg, data),
};

// Encryption key for metadata (display names, room titles, etc.)
const METADATA_ENCRYPTION_KEY = ENCRYPTION_KEY
  ? Buffer.from(ENCRYPTION_KEY, "base64")
  : null;

// Encrypt metadata (display_name, room title, etc.)
function encryptMetadata(plaintext) {
  if (!METADATA_ENCRYPTION_KEY || !plaintext) return plaintext;

  try {
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv(
      "aes-256-gcm",
      METADATA_ENCRYPTION_KEY,
      iv,
    );

    let encrypted = cipher.update(plaintext, "utf8", "base64");
    encrypted += cipher.final("base64");

    const authTag = cipher.getAuthTag();

    // Format: iv:authTag:encrypted
    return `${iv.toString("base64")}:${authTag.toString("base64")}:${encrypted}`;
  } catch (e) {
    log.error("Metadata encryption failed", e);
    return plaintext;
  }
}

// Decrypt metadata
function decryptMetadata(encrypted) {
  if (!METADATA_ENCRYPTION_KEY || !encrypted || !encrypted.includes(":"))
    return encrypted;

  try {
    const parts = encrypted.split(":");
    if (parts.length !== 3) return encrypted;

    const iv = Buffer.from(parts[0], "base64");
    const authTag = Buffer.from(parts[1], "base64");
    const encryptedData = parts[2];

    const decipher = crypto.createDecipheriv(
      "aes-256-gcm",
      METADATA_ENCRYPTION_KEY,
      iv,
    );
    decipher.setAuthTag(authTag);

    let decrypted = decipher.update(encryptedData, "base64", "utf8");
    decrypted += decipher.final("utf8");

    return decrypted;
  } catch (e) {
    log.error("Metadata decryption failed", e);
    return encrypted;
  }
}

fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const pool = new Pool({ connectionString: DATABASE_URL });

// S3 Client
const s3Client = new S3Client({
  endpoint: S3_ENDPOINT,
  region: S3_REGION,
  credentials: {
    accessKeyId: S3_ACCESS_KEY,
    secretAccessKey: S3_SECRET_KEY,
  },
  forcePathStyle: true,
});

const useS3Storage = ["1", "true", "yes"].includes(
  String(USE_S3).toLowerCase(),
);
const adminUsernames = new Set(
  String(ADMIN_USERNAMES)
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean),
);
if (SYSTEM_ADMIN_USERNAME)
  adminUsernames.add(String(SYSTEM_ADMIN_USERNAME).trim().toLowerCase());

// ORIGIN may be a single origin, "*", or a comma-separated list
// (e.g. "https://67chat.ru,http://abcdef...onion"). Trailing slashes are stripped
// because browsers send the Origin header without one.
function publicOriginList() {
  const list = String(ORIGIN)
    .split(",")
    .map((s) => s.trim().replace(/\/+$/, ""))
    .filter(Boolean);
  if (list.includes("*")) return true;
  return list;
}

function logError(event, details = {}) {
  log.error(event, details);
}

function maskEmailLike(value) {
  const s = String(value || "");
  if (s.length <= 3) return "***";
  return `${s.slice(0, 2)}***${s.slice(-1)}`;
}

function isSafeMime(kind, mime) {
  const normalized = String(mime || "").toLowerCase();
  if (!normalized) return false;
  if (kind === "avatar" || kind === "image")
    return normalized.startsWith("image/");
  if (kind === "voice") return normalized.startsWith("audio/");
  if (kind === "video_note" || kind === "video")
    return normalized.startsWith("video/");
  return true;
}

// S3 Helper functions
async function uploadToS3(key, filePath, contentType) {
  const fileStream = fs.createReadStream(filePath);
  const command = new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: key,
    Body: fileStream,
    ContentType: contentType,
  });
  await s3Client.send(command);
  return S3_ENDPOINT + "/" + S3_BUCKET + "/" + key;
}

async function deleteFromS3(key) {
  const command = new DeleteObjectCommand({
    Bucket: S3_BUCKET,
    Key: key,
  });
  await s3Client.send(command);
}

function getS3Key(mediaId, filename) {
  return "media/" + mediaId + "/" + filename;
}

// Swift Helper functions (backup)
async function uploadToSwift(key, filePath, contentType) {
  // TODO: Implement Swift client if needed
  // For now, use S3 as primary
  return await uploadToS3(key, filePath, contentType);
}

// Database migrations
async function runMigrations() {
  const migrations = [
    `CREATE TABLE IF NOT EXISTS room_secrets (room_id uuid PRIMARY KEY REFERENCES rooms(id) ON DELETE CASCADE, secret text NOT NULL, created_at timestamptz NOT NULL DEFAULT now())`,
    `CREATE TABLE IF NOT EXISTS user_blocks (blocker_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE, blocked_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (blocker_id, blocked_id), CHECK (blocker_id <> blocked_id))`,
    `CREATE INDEX IF NOT EXISTS user_blocks_blocked_idx ON user_blocks(blocked_id)`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen timestamptz`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name text`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_id uuid`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_tagline text`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS merged_into_user_id uuid REFERENCES users(id) ON DELETE SET NULL`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS login_disabled_at timestamptz`,
    `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS device_name text`,
    `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS device_type text`,
    `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS ip_address inet`,
    `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS user_agent text`,
    `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS last_activity timestamptz DEFAULT now()`,
    `CREATE INDEX IF NOT EXISTS sessions_user_last_activity_idx ON sessions(user_id, last_activity DESC)`,
    `CREATE TABLE IF NOT EXISTS user_public_names (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE, name citext NOT NULL UNIQUE, kind text NOT NULL CHECK (kind IN ('primary_tag', 'alias')), created_at timestamptz NOT NULL DEFAULT now())`,
    `CREATE INDEX IF NOT EXISTS user_public_names_user_idx ON user_public_names(user_id)`,
    `CREATE UNIQUE INDEX IF NOT EXISTS user_public_names_one_primary_tag_idx ON user_public_names(user_id) WHERE kind='primary_tag'`,
    `ALTER TABLE rooms ADD COLUMN IF NOT EXISTS avatar_id uuid`,
    `ALTER TABLE media ADD COLUMN IF NOT EXISTS thumb_path text`,
    `ALTER TABLE media ADD COLUMN IF NOT EXISTS duration_sec int`,
    `ALTER TABLE media ADD COLUMN IF NOT EXISTS width int`,
    `ALTER TABLE media ADD COLUMN IF NOT EXISTS height int`,
    `ALTER TABLE media DROP CONSTRAINT IF EXISTS media_kind_check`,
    `ALTER TABLE media ADD CONSTRAINT media_kind_check CHECK (kind IN ('image', 'file', 'voice', 'video_note', 'avatar', 'video'))`,
    `ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_type_check`,
    `ALTER TABLE messages ADD CONSTRAINT messages_type_check CHECK (type IN ('text', 'media', 'voice', 'video_note', 'call'))`,
    `ALTER TABLE messages ADD COLUMN IF NOT EXISTS read_by jsonb DEFAULT '[]'::jsonb`,
    `ALTER TABLE rooms DROP CONSTRAINT IF EXISTS rooms_kind_check`,
    `ALTER TABLE rooms ADD CONSTRAINT rooms_kind_check CHECK (kind IN ('dm', 'group', 'channel'))`,
    `ALTER TABLE media DROP CONSTRAINT IF EXISTS media_kind_check`,
    `ALTER TABLE media ADD CONSTRAINT media_kind_check CHECK (kind IN ('image', 'file', 'voice', 'video_note', 'avatar', 'video'))`,
    `UPDATE messages SET read_by = '[]'::jsonb WHERE read_by IS NULL`,
    `ALTER TABLE media ADD CONSTRAINT media_kind_check CHECK (kind IN ('image', 'file', 'voice', 'video_note', 'avatar', 'video'))`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS banned boolean DEFAULT false`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS fcm_token text`,
    `ALTER TABLE rooms ADD COLUMN IF NOT EXISTS e2e_enabled boolean NOT NULL DEFAULT false`,
    `CREATE TABLE IF NOT EXISTS group_sender_keys (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), room_id uuid NOT NULL REFERENCES rooms(id) ON DELETE CASCADE, from_user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE, to_user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE, key_id text NOT NULL, payload text NOT NULL, created_at timestamptz NOT NULL DEFAULT now())`,
    `ALTER TABLE group_sender_keys ADD COLUMN IF NOT EXISTS to_device_id text`,
    `CREATE INDEX IF NOT EXISTS group_sender_keys_inbox_idx ON group_sender_keys(room_id, to_user_id, created_at)`,
    `CREATE INDEX IF NOT EXISTS users_fcm_token_idx ON users(fcm_token) WHERE fcm_token IS NOT NULL`,
    `ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to jsonb`,
    `CREATE TABLE IF NOT EXISTS reactions (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), message_id uuid REFERENCES messages(id) ON DELETE CASCADE, user_id uuid REFERENCES users(id) ON DELETE CASCADE, emoji text NOT NULL, created_at timestamptz DEFAULT now(), UNIQUE(message_id, user_id))`,
    // Migration: change unique constraint from (message_id, user_id, emoji) to (message_id, user_id)
    `ALTER TABLE reactions DROP CONSTRAINT IF EXISTS reactions_message_id_user_id_key`,
    `ALTER TABLE reactions DROP CONSTRAINT IF EXISTS reactions_message_id_user_id_emoji_key`,
    `ALTER TABLE reactions ADD CONSTRAINT reactions_one_per_user UNIQUE (message_id, user_id)`,
    `ALTER TABLE media ADD COLUMN IF NOT EXISTS s3_path text`,
    `ALTER TABLE user_devices ADD COLUMN IF NOT EXISTS registration_id int`,
    `ALTER TABLE user_identity_keys ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now()`,
    // Ban users with too long username/display_name
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS banned boolean DEFAULT false`,
    `ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to jsonb`,
    `CREATE TABLE IF NOT EXISTS reactions (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), message_id uuid REFERENCES messages(id) ON DELETE CASCADE, user_id uuid REFERENCES users(id) ON DELETE CASCADE, emoji text NOT NULL, created_at timestamptz DEFAULT now(), UNIQUE(message_id, user_id))`,
    // Migration: change unique constraint from (message_id, user_id, emoji) to (message_id, user_id)
    `ALTER TABLE reactions DROP CONSTRAINT IF EXISTS reactions_message_id_user_id_key`,
    `ALTER TABLE reactions DROP CONSTRAINT IF EXISTS reactions_message_id_user_id_emoji_key`,
    `ALTER TABLE reactions ADD CONSTRAINT reactions_one_per_user UNIQUE (message_id, user_id)`,
    // Comments for channel posts
    `CREATE TABLE IF NOT EXISTS post_comments (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), post_id uuid REFERENCES messages(id) ON DELETE CASCADE, comment_id uuid REFERENCES messages(id) ON DELETE CASCADE, created_at timestamptz DEFAULT now(), UNIQUE(post_id, comment_id))`,
    `ALTER TABLE messages ADD COLUMN IF NOT EXISTS comment_count int DEFAULT 0`,
    `ALTER TABLE messages ADD COLUMN IF NOT EXISTS is_post boolean DEFAULT false`,
    // Admin flag (in addition to ADMIN_USERNAMES)
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin boolean NOT NULL DEFAULT false`,
    // Upload de-duplication: content hash per owner
    `ALTER TABLE media ADD COLUMN IF NOT EXISTS sha256 text`,
    `CREATE INDEX IF NOT EXISTS media_owner_sha256_idx ON media(owner_id, sha256) WHERE sha256 IS NOT NULL`,
    `UPDATE users SET banned = true WHERE length(username) > 30 OR length(display_name) > 30`,
    `INSERT INTO user_public_names(user_id, name, kind)
     SELECT u.id, u.username, 'primary_tag'
     FROM users u
     WHERE u.merged_into_user_id IS NULL
       AND NOT EXISTS (
         SELECT 1 FROM user_public_names upn WHERE upn.user_id = u.id
       )
     ON CONFLICT (name) DO NOTHING`,
    // "Delete for me": a per-user, per-message tombstone. The message stays
    // intact for everyone else, including in their history.
    `CREATE TABLE IF NOT EXISTS hidden_messages (user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE, message_id uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (user_id, message_id))`,
    `CREATE INDEX IF NOT EXISTS hidden_messages_user_idx ON hidden_messages(user_id)`,
    // Pinned message per chat (Telegram keeps one).
    `ALTER TABLE rooms ADD COLUMN IF NOT EXISTS pinned_message_id uuid REFERENCES messages(id) ON DELETE SET NULL`,
    `ALTER TABLE rooms ADD COLUMN IF NOT EXISTS pinned_at timestamptz`,
    `ALTER TABLE rooms ADD COLUMN IF NOT EXISTS pinned_by uuid REFERENCES users(id) ON DELETE SET NULL`,
  ];
  for (const sql of migrations) {
    try {
      await pool.query(sql);
    } catch (e) {
      log.warn("Migration note: " + e.message);
    }
  }
}
await runMigrations();

// Optionally create a bootstrap admin and news channel.
async function setupSystemChannel() {
  if (!SYSTEM_ADMIN_USERNAME || !SYSTEM_ADMIN_PASSWORD || !SYSTEM_CHANNEL_TITLE)
    return;
  try {
    // Check if bootstrap admin exists
    const { rows: adminRows } = await pool.query(
      `SELECT id FROM users WHERE username=$1 LIMIT 1`,
      [SYSTEM_ADMIN_USERNAME],
    );
    let adminId;
    if (adminRows.length === 0) {
      const hash = await argon2.hash(SYSTEM_ADMIN_PASSWORD);
      const { rows: createdAdmin } = await pool.query(
        `INSERT INTO users(username, password_hash, display_name) VALUES($1, $2, 'Администратор') RETURNING id`,
        [SYSTEM_ADMIN_USERNAME, hash],
      );
      adminId = createdAdmin[0].id;
      log.info("Created bootstrap admin user");
    } else {
      adminId = adminRows[0].id;
    }

    // Check if news channel exists
    const { rows: channelRows } = await pool.query(
      `SELECT id FROM rooms WHERE kind='channel' AND title=$1 LIMIT 1`,
      [SYSTEM_CHANNEL_TITLE],
    );
    if (channelRows.length === 0) {
      const { rows: newChannel } = await pool.query(
        `INSERT INTO rooms(kind, title, created_by) VALUES('channel', $1, $2) RETURNING id`,
        [SYSTEM_CHANNEL_TITLE, adminId],
      );
      const channelId = newChannel[0].id;

      // Add bootstrap admin as owner
      await pool.query(
        `INSERT INTO room_members(room_id, user_id, role) VALUES($1, $2, 'owner') ON CONFLICT DO NOTHING`,
        [channelId, adminId],
      );

      log.info("Created bootstrap channel");
    }
  } catch (e) {
    log.error("Setup system channel error", e);
  }
}
await setupSystemChannel();

const app = Fastify({ logger: true });
appLogger = app.log;

await app.register(cors, { origin: publicOriginList(), credentials: true });
await app.register(cookie);
await app.register(multipart, {
  limits: {
    fileSize: 100 * 1024 * 1024, // 100MB max file size
    files: 1, // Only 1 file per request
  },
});
await app.register(rateLimit, {
  global: false,
  max: 100,
  timeWindow: "1 minute",
});
await app.register(websocket);

function isHttpsLike(req) {
  const xfProto = String(
    req.headers?.["x-forwarded-proto"] || "",
  ).toLowerCase();
  if (xfProto) return xfProto.split(",")[0].trim() === "https";
  const proto = String(req.protocol || "").toLowerCase();
  if (proto) return proto === "https";
  return false;
}

function cookieSecureFlag(req) {
  const mode = String(COOKIE_SECURE).toLowerCase();
  if (mode === "true" || mode === "1" || mode === "yes") return true;
  if (mode === "false" || mode === "0" || mode === "no") return false;
  return isHttpsLike(req);
}

function authCookieOptions(req, ttlDays) {
  return {
    httpOnly: true,
    sameSite: "lax",
    path: "/",
    secure: cookieSecureFlag(req),
    maxAge: ttlDays * 24 * 60 * 60,
  };
}

function wantsBearerSession(req) {
  return (
    String(req.headers["x-session-transport"] || "").toLowerCase() === "bearer"
  );
}

function sha256(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}
function randToken() {
  return crypto.randomBytes(32).toString("hex");
}

// Parse User-Agent to get device info
function parseUserAgent(userAgent) {
  if (!userAgent)
    return { deviceName: "Unknown Device", deviceType: "desktop" };

  const ua = userAgent.toLowerCase();
  let browser = "Unknown";
  let os = "Unknown";
  let deviceType = "desktop";

  // Detect browser
  if (ua.includes("firefox")) browser = "Firefox";
  else if (ua.includes("edg")) browser = "Edge";
  else if (ua.includes("chrome")) browser = "Chrome";
  else if (ua.includes("safari")) browser = "Safari";
  else if (ua.includes("opera")) browser = "Opera";

  // Detect OS
  if (ua.includes("windows")) os = "Windows";
  else if (ua.includes("mac")) os = "macOS";
  else if (ua.includes("linux")) os = "Linux";
  else if (ua.includes("android")) {
    os = "Android";
    deviceType = "mobile";
  } else if (ua.includes("iphone") || ua.includes("ipad")) {
    os = ua.includes("ipad") ? "iPad" : "iPhone";
    deviceType = "mobile";
  }

  // Detect tablet
  if (ua.includes("tablet") || ua.includes("ipad")) deviceType = "tablet";

  const deviceName = `${browser} on ${os}`;
  return { deviceName, deviceType };
}

function getClientIp(req) {
  return (
    req.headers["x-forwarded-for"]?.split(",")[0]?.trim() ||
    req.headers["x-real-ip"] ||
    req.ip ||
    req.socket.remoteAddress
  );
}
function roomSecret() {
  return crypto.randomBytes(32).toString("base64url");
}

// Verify hCaptcha token
async function verifyCaptcha(token) {
  if (!HCAPTCHA_SECRET) return true; // Skip if not configured
  if (!token) return false;
  try {
    const response = await fetch("https://hcaptcha.com/siteverify", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ secret: HCAPTCHA_SECRET, response: token }),
    });
    const data = await response.json();
    return data.success === true;
  } catch (e) {
    log.error("Captcha verification error", e);
    return false;
  }
}

function httpError(statusCode, publicCode) {
  const e = new Error(publicCode);
  e.statusCode = statusCode;
  e.publicCode = publicCode;
  return e;
}

app.setErrorHandler((err, req, reply) => {
  const statusCode = Number(err.statusCode) || 500;
  const publicCode =
    statusCode >= 500 ? "server_error" : err.publicCode || "error";
  if (reply.sent) return;

  logError("API Error", {
    error: err.message,
    statusCode,
    url: req.url,
    method: req.method,
  });

  reply.code(statusCode).send({ error: publicCode });
});

async function requireAuth(req) {
  const auth = req.headers.authorization || "";
  let token = "";
  if (auth.toLowerCase().startsWith("bearer ")) token = auth.slice(7).trim();
  if (!token && req.cookies?.token) token = String(req.cookies.token);
  if (!token && req.headers?.cookie) {
    const raw = String(req.headers.cookie);
    const part = raw
      .split(";")
      .map((s) => s.trim())
      .find((s) => s.toLowerCase().startsWith("token="));
    if (part) token = decodeURIComponent(part.slice("token=".length));
  }
  if (!token) throw httpError(401, "unauthorized");

  const th = sha256(token);
  const { rows } = await pool.query(
    `SELECT s.id AS session_id, s.user_id, u.username, u.display_name, u.avatar_id, u.banned
     FROM sessions s
     JOIN users u ON u.id = s.user_id
     WHERE s.token_hash=$1 AND s.expires_at > now()
     LIMIT 1`,
    [th],
  );
  if (!rows.length) throw httpError(401, "unauthorized");
  if (rows[0].banned) throw httpError(403, "account_banned");

  const user = {
    sessionId: rows[0].session_id,
    id: rows[0].user_id,
    username: rows[0].username,
    display_name: rows[0].display_name,
    avatar_id: rows[0].avatar_id,
    token,
  };
  pool
    .query(`UPDATE users SET last_seen=now() WHERE id=$1`, [user.id])
    .catch(() => {});
  pool
    .query(`UPDATE sessions SET last_activity=now() WHERE token_hash=$1`, [th])
    .catch(() => {});
  return user;
}

async function isRoomMember(roomId, userId) {
  const { rows } = await pool.query(
    `SELECT 1 FROM room_members WHERE room_id=$1 AND user_id=$2 LIMIT 1`,
    [roomId, userId],
  );
  return rows.length > 0;
}

async function getRoomRole(roomId, userId) {
  const { rows } = await pool.query(
    `SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2 LIMIT 1`,
    [roomId, userId],
  );
  return rows[0]?.role || null;
}

// Group E2E (Sender Keys): opt-in per group. Channels and DMs are not affected.
async function isGroupE2eEnabled(roomId) {
  const { rows } = await pool.query(
    `SELECT 1 FROM rooms WHERE id=$1 AND kind='group' AND e2e_enabled=true LIMIT 1`,
    [roomId],
  );
  return rows.length > 0;
}

async function ensureRoomSecret(roomId) {
  const { rows } = await pool.query(
    `SELECT secret FROM room_secrets WHERE room_id=$1 LIMIT 1`,
    [roomId],
  );
  if (rows.length) return rows[0].secret;
  const secret = roomSecret();
  await pool.query(
    `INSERT INTO room_secrets(room_id, secret) VALUES ($1,$2) ON CONFLICT (room_id) DO NOTHING`,
    [roomId, secret],
  );
  const { rows: rows2 } = await pool.query(
    `SELECT secret FROM room_secrets WHERE room_id=$1 LIMIT 1`,
    [roomId],
  );
  return rows2[0].secret;
}

// --- FFmpeg helpers ---
async function convertToM4A(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn("ffmpeg", [
      "-i",
      inputPath,
      "-c:a",
      "aac",
      "-b:a",
      "128k",
      "-movflags",
      "+faststart",
      "-y",
      outputPath,
    ]);
    ffmpeg.on("close", (code) =>
      code === 0 ? resolve() : reject(new Error("ffmpeg exit " + code)),
    );
    ffmpeg.on("error", reject);
  });
}

async function convertVideoNote(inputPath, outputPath, thumbPath) {
  return new Promise((resolve, reject) => {
    // Convert to MP4 with circular crop and create thumbnail
    const ffmpeg = spawn("ffmpeg", [
      "-i",
      inputPath,
      "-c:v",
      "libx264",
      "-preset",
      "fast",
      "-crf",
      "28",
      "-c:a",
      "aac",
      "-b:a",
      "64k",
      "-vf",
      "crop=min(iw\\,ih):min(iw\\,ih),scale=480:480",
      "-pix_fmt",
      "yuv420p",
      "-movflags",
      "+faststart",
      "-t",
      "60",
      "-y",
      outputPath,
    ]);
    ffmpeg.on("close", async (code) => {
      if (code !== 0) return reject(new Error("ffmpeg exit " + code));
      // Create thumbnail
      const thumbFfmpeg = spawn("ffmpeg", [
        "-i",
        outputPath,
        "-vframes",
        "1",
        "-vf",
        "scale=160:160",
        "-y",
        thumbPath,
      ]);
      thumbFfmpeg.on("close", () => resolve());
      thumbFfmpeg.on("error", () => resolve()); // Thumbnail optional
    });
    ffmpeg.on("error", reject);
  });
}

async function createImageThumb(inputPath, thumbPath) {
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn("ffmpeg", [
      "-i",
      inputPath,
      "-vf",
      "scale=200:-1",
      "-y",
      thumbPath,
    ]);
    ffmpeg.on("close", () => resolve());
    ffmpeg.on("error", reject);
  });
}

async function getMediaInfo(filePath) {
  return new Promise((resolve) => {
    const ffprobe = spawn("ffprobe", [
      "-v",
      "quiet",
      "-print_format",
      "json",
      "-show_format",
      "-show_streams",
      filePath,
    ]);
    let data = "";
    ffprobe.stdout.on("data", (chunk) => (data += chunk));
    ffprobe.on("close", () => {
      try {
        const info = JSON.parse(data);
        const video = info.streams?.find((s) => s.codec_type === "video");
        const duration = parseFloat(info.format?.duration) || 0;
        resolve({
          width: video?.width,
          height: video?.height,
          duration: Math.round(duration),
        });
      } catch {
        resolve({});
      }
    });
    ffprobe.on("error", () => resolve({}));
  });
}

// --- WebSocket state ---
const socketsByUser = new Map();
const socketsByRoom = new Map();

function addSocket(userId, ws) {
  if (!socketsByUser.has(userId)) socketsByUser.set(userId, new Set());
  socketsByUser.get(userId).add(ws);
}
function removeSocket(userId, ws) {
  const set = socketsByUser.get(userId);
  if (set) {
    set.delete(ws);
    if (!set.size) socketsByUser.delete(userId);
  }
}
function joinRoom(roomId, ws) {
  if (!socketsByRoom.has(roomId)) socketsByRoom.set(roomId, new Set());
  socketsByRoom.get(roomId).add(ws);
}
function leaveAllRooms(ws) {
  for (const set of socketsByRoom.values()) set.delete(ws);
}
function broadcastToRoom(roomId, payload, excludedUsers = new Set()) {
  const set = socketsByRoom.get(roomId);
  if (!set) return;
  const s = JSON.stringify(payload);
  for (const ws of set) {
    if([...excludedUsers].some(userId=>socketsByUser.get(userId)?.has(ws)))continue;
    try {
      ws.send(s);
    } catch {}
  }
}
function sendToUser(userId, payload) {
  const set = socketsByUser.get(userId);
  if (!set) return;
  const s = JSON.stringify(payload);
  for (const ws of set) {
    try {
      ws.send(s);
    } catch {}
  }
}
async function sendToRoomMembersExcept(roomId, excludeUserId, payload) {
  const { rows } = await pool.query(
    `SELECT user_id FROM room_members WHERE room_id=$1 AND user_id <> $2`,
    [roomId, excludeUserId],
  );
  for (const row of rows) {
    sendToUser(String(row.user_id), payload);
  }
}
function isUserOnline(userId) {
  const set = socketsByUser.get(userId);
  return !!(set && set.size);
}

// Send FCM push notification
const sendPushNotification = createPushSender(pool, log);
const relayCall = installCalls(app, {pool,requireAuth,sendToUser,push:sendPushNotification});

// Admin if the username is listed in ADMIN_USERNAMES (or is the bootstrap admin)
// OR the users.is_admin flag is set.
async function isAdmin(userId) {
  const { rows } = await pool.query(
    `SELECT username, is_admin FROM users WHERE id=$1 LIMIT 1`,
    [userId],
  );
  if (rows[0]?.is_admin === true) return true;
  return adminUsernames.has(String(rows[0]?.username || "").toLowerCase());
}

async function getBlockState(viewerId, targetId) {
  const { rows } = await pool.query(
    `SELECT blocker_id, blocked_id
     FROM user_blocks
     WHERE (blocker_id=$1 AND blocked_id=$2) OR (blocker_id=$2 AND blocked_id=$1)`,
    [viewerId, targetId],
  );
  return {
    blocked_by_me: rows.some(
      (r) =>
        String(r.blocker_id) === String(viewerId) &&
        String(r.blocked_id) === String(targetId),
    ),
    blocks_me: rows.some(
      (r) =>
        String(r.blocker_id) === String(targetId) &&
        String(r.blocked_id) === String(viewerId),
    ),
  };
}

async function getDmPeerUserId(roomId, userId) {
  const { rows } = await pool.query(
    `SELECT rm2.user_id
     FROM rooms r
     JOIN room_members rm1 ON rm1.room_id = r.id AND rm1.user_id = $2
     JOIN room_members rm2 ON rm2.room_id = r.id AND rm2.user_id <> $2
     WHERE r.id = $1 AND r.kind = 'dm'
     LIMIT 1`,
    [roomId, userId],
  );
  return rows[0]?.user_id || null;
}

async function canUsersExchangeDm(viewerId, targetId) {
  const blockState = await getBlockState(viewerId, targetId);
  return !(blockState.blocked_by_me || blockState.blocks_me);
}

function normalizePublicName(value) {
  return String(value || "").trim();
}

async function verifyPasswordHash(storedHash, password) {
  const normalizedHash = String(storedHash || "");
  if (normalizedHash.startsWith("$argon2")) {
    return argon2.verify(normalizedHash, password);
  }
  const [salt, stored] = normalizedHash.split(":");
  const check = sha256(salt + ":" + password);
  return check === stored;
}

async function upgradeLegacyPasswordIfNeeded(
  client,
  userId,
  storedHash,
  password,
) {
  const normalizedHash = String(storedHash || "");
  if (normalizedHash.startsWith("$argon2")) return;
  const upgraded = await argon2.hash(password);
  await client.query(`UPDATE users SET password_hash=$2 WHERE id=$1`, [
    userId,
    upgraded,
  ]);
}

async function resolveUserIdByPublicName(name, client = pool) {
  const normalized = normalizePublicName(name);
  if (!normalized) return null;
  const { rows } = await client.query(
    `SELECT id
     FROM users
     WHERE lower(username)=lower($1)
     UNION
     SELECT upn.user_id AS id
     FROM user_public_names upn
     JOIN users u ON u.id = upn.user_id
     WHERE lower(upn.name)=lower($1)
       AND u.merged_into_user_id IS NULL
     LIMIT 1`,
    [normalized],
  );
  return rows[0]?.id || null;
}

async function getUserPublicNames(userId, client = pool) {
  const { rows } = await client.query(
    `SELECT id, name, kind
     FROM user_public_names
     WHERE user_id=$1
     ORDER BY CASE WHEN kind='primary_tag' THEN 0 ELSE 1 END, created_at, name`,
    [userId],
  );
  const primary = rows.find((r) => r.kind === "primary_tag") || null;
  return {
    primary_tag: primary ? String(primary.name) : null,
    aliases: rows
      .filter((r) => r.kind === "alias")
      .map((r) => ({ id: r.id, name: String(r.name), kind: "alias" })),
    public_names: rows.map((r) => ({
      id: r.id,
      name: String(r.name),
      kind: r.kind,
    })),
  };
}

async function ensurePrimaryTag(client, userId) {
  const publicNames = await getUserPublicNames(userId, client);
  if (publicNames.primary_tag) return publicNames;
  const alias = publicNames.aliases[0];
  if (alias) {
    await client.query(
      `UPDATE user_public_names SET kind='primary_tag' WHERE id=$1`,
      [alias.id],
    );
    return getUserPublicNames(userId, client);
  }
  const { rows } = await client.query(
    `SELECT username FROM users WHERE id=$1 LIMIT 1`,
    [userId],
  );
  if (!rows.length) return publicNames;
  await client.query(
    `INSERT INTO user_public_names(user_id, name, kind)
     VALUES ($1, $2, 'primary_tag')
     ON CONFLICT (name) DO NOTHING`,
    [userId, rows[0].username],
  );
  return getUserPublicNames(userId, client);
}

async function countUserAliases(userId, client = pool) {
  const { rows } = await client.query(
    `SELECT COUNT(*)::int AS count
     FROM user_public_names
     WHERE user_id=$1 AND kind='alias'`,
    [userId],
  );
  return Number(rows[0]?.count || 0);
}

async function addPublicName(client, userId, name, kind = "alias") {
  const normalized = normalizePublicName(name);
  if (!normalized) return null;
  return client.query(
    `INSERT INTO user_public_names(user_id, name, kind)
     SELECT $1, $2::citext, $3
     WHERE NOT EXISTS (
       SELECT 1
       FROM users u
       WHERE lower(u.username::text)=lower($2::text)
         AND u.id <> $1
     )
     ON CONFLICT (name) DO NOTHING
     RETURNING id, name, kind`,
    [userId, normalized, kind],
  );
}

async function getUserProfileForViewer(viewerId, targetId) {
  const { rows } = await pool.query(
    `SELECT id, username, display_name, avatar_id, profile_tagline, last_seen, created_at, banned
     FROM users
     WHERE id=$1
     LIMIT 1`,
    [targetId],
  );
  if (!rows.length) return null;
  const blockState = await getBlockState(viewerId, targetId);
  const publicNames = await ensurePrimaryTag(pool, rows[0].id);

  // Decrypt metadata
  const decryptedDisplayName = decryptMetadata(rows[0].display_name);
  const decryptedTagline = decryptMetadata(rows[0].profile_tagline);

  return {
    ...rows[0],
    display_name: decryptedDisplayName,
    profile_tagline: decryptedTagline,
    online: isUserOnline(rows[0].id),
    ...blockState,
    ...publicNames,
    isMe: String(rows[0].id) === String(viewerId),
  };
}

function roleRank(role) {
  if (role === "owner") return 3;
  if (role === "admin") return 2;
  return 1;
}

function pickHigherRole(a, b) {
  return roleRank(a) >= roleRank(b) ? a : b;
}

function buildMergedUsername(userId) {
  const compact =
    String(userId || "")
      .replace(/-/g, "")
      .slice(0, 8) || crypto.randomBytes(4).toString("hex");
  return `merged_${compact}`;
}

async function collectOwnedPublicNames(userId, client = pool) {
  const publicNames = await getUserPublicNames(userId, client);
  const { rows } = await client.query(
    `SELECT username FROM users WHERE id=$1 LIMIT 1`,
    [userId],
  );
  const names = new Set(
    publicNames.public_names.map((item) => String(item.name)),
  );
  if (rows[0]?.username) names.add(String(rows[0].username));
  return Array.from(names);
}

async function resolveRoomMembersByNames(names, client = pool) {
  const uniqueNames = Array.from(
    new Set((names || []).map(normalizePublicName).filter(Boolean)),
  );
  const resolved = [];
  for (const name of uniqueNames) {
    const userId = await resolveUserIdByPublicName(name, client);
    if (!userId) return { ok: false, missing: [name], users: [] };
    const { rows } = await client.query(
      `SELECT id, username FROM users WHERE id=$1 LIMIT 1`,
      [userId],
    );
    if (!rows.length) return { ok: false, missing: [name], users: [] };
    resolved.push(rows[0]);
  }
  return { ok: true, missing: [], users: resolved };
}

async function mergeDmRoomIntoExisting(
  client,
  sourceRoomId,
  targetRoomId,
  secondaryUserId,
  primaryUserId,
) {
  await client.query(`UPDATE messages SET room_id=$2 WHERE room_id=$1`, [
    sourceRoomId,
    targetRoomId,
  ]);
  await client.query(
    `DELETE FROM room_members WHERE room_id=$1 AND user_id=$2`,
    [sourceRoomId, secondaryUserId],
  );
  await client.query(`DELETE FROM rooms WHERE id=$1`, [sourceRoomId]);
}

async function mergeSecondaryAccountIntoPrimary(
  primaryUserId,
  secondaryUserId,
  secondaryUsername,
) {
  const client = await pool.connect();
  const affectedUsers = new Set([
    String(primaryUserId),
    String(secondaryUserId),
  ]);
  try {
    await client.query("BEGIN");

    const primaryPublic = await ensurePrimaryTag(client, primaryUserId);
    const secondaryNames = await collectOwnedPublicNames(
      secondaryUserId,
      client,
    );
    const existingPrimaryNames = new Set(
      primaryPublic.public_names.map((item) => String(item.name).toLowerCase()),
    );
    const namesToTransfer = secondaryNames.filter(
      (name) => !existingPrimaryNames.has(String(name).toLowerCase()),
    );
    const currentAliasCount = await countUserAliases(primaryUserId, client);
    if (currentAliasCount + namesToTransfer.length > 50) {
      await client.query("ROLLBACK");
      return { ok: false, error: "alias_limit_exceeded" };
    }

    const { rows: membershipRows } = await client.query(
      `SELECT rm.room_id, rm.role, r.kind, r.created_by
       FROM room_members rm
       JOIN rooms r ON r.id = rm.room_id
       WHERE rm.user_id=$1`,
      [secondaryUserId],
    );

    for (const membership of membershipRows) {
      const roomId = String(membership.room_id);
      const roomKind = String(membership.kind);
      const role = String(membership.role || "member");
      const { rows: roomUsers } = await client.query(
        `SELECT user_id, role FROM room_members WHERE room_id=$1`,
        [roomId],
      );
      roomUsers.forEach((row) => affectedUsers.add(String(row.user_id)));

      if (roomKind === "dm") {
        const other = roomUsers.find(
          (row) => String(row.user_id) !== String(secondaryUserId),
        );
        if (!other) {
          await client.query(`DELETE FROM rooms WHERE id=$1`, [roomId]);
          continue;
        }
        const otherUserId = String(other.user_id);
        if (otherUserId === String(primaryUserId)) {
          await client.query(`DELETE FROM rooms WHERE id=$1`, [roomId]);
          continue;
        }

        const { rows: existingDm } = await client.query(
          `SELECT r.id
           FROM rooms r
           JOIN room_members m1 ON m1.room_id=r.id AND m1.user_id=$1
           JOIN room_members m2 ON m2.room_id=r.id AND m2.user_id=$2
           WHERE r.kind='dm'
             AND r.id <> $3
           LIMIT 1`,
          [primaryUserId, otherUserId, roomId],
        );

        if (existingDm.length) {
          await mergeDmRoomIntoExisting(
            client,
            roomId,
            existingDm[0].id,
            secondaryUserId,
            primaryUserId,
          );
          continue;
        }

        const { rows: primaryMembership } = await client.query(
          `SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2 LIMIT 1`,
          [roomId, primaryUserId],
        );
        if (primaryMembership.length) {
          const mergedRole = pickHigherRole(primaryMembership[0].role, role);
          await client.query(
            `UPDATE room_members SET role=$3 WHERE room_id=$1 AND user_id=$2`,
            [roomId, primaryUserId, mergedRole],
          );
          await client.query(
            `DELETE FROM room_members WHERE room_id=$1 AND user_id=$2`,
            [roomId, secondaryUserId],
          );
        } else {
          await client.query(
            `UPDATE room_members SET user_id=$3 WHERE room_id=$1 AND user_id=$2`,
            [roomId, secondaryUserId, primaryUserId],
          );
          await client.query(
            `UPDATE room_members SET role=$3 WHERE room_id=$1 AND user_id=$2`,
            [roomId, primaryUserId, role],
          );
        }
        continue;
      }

      const { rows: primaryMembership } = await client.query(
        `SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2 LIMIT 1`,
        [roomId, primaryUserId],
      );
      if (primaryMembership.length) {
        const mergedRole = pickHigherRole(primaryMembership[0].role, role);
        await client.query(
          `UPDATE room_members SET role=$3 WHERE room_id=$1 AND user_id=$2`,
          [roomId, primaryUserId, mergedRole],
        );
        await client.query(
          `DELETE FROM room_members WHERE room_id=$1 AND user_id=$2`,
          [roomId, secondaryUserId],
        );
      } else {
        await client.query(
          `UPDATE room_members SET user_id=$3 WHERE room_id=$1 AND user_id=$2`,
          [roomId, secondaryUserId, primaryUserId],
        );
        await client.query(
          `UPDATE room_members SET role=$3 WHERE room_id=$1 AND user_id=$2`,
          [roomId, primaryUserId, role],
        );
      }
    }

    await client.query(`UPDATE messages SET sender_id=$2 WHERE sender_id=$1`, [
      secondaryUserId,
      primaryUserId,
    ]);
    await client.query(`UPDATE media SET owner_id=$2 WHERE owner_id=$1`, [
      secondaryUserId,
      primaryUserId,
    ]);
    await client.query(`UPDATE rooms SET created_by=$2 WHERE created_by=$1`, [
      secondaryUserId,
      primaryUserId,
    ]);
    await client.query(`DELETE FROM sessions WHERE user_id=$1`, [
      secondaryUserId,
    ]);

    await client.query(`DELETE FROM user_public_names WHERE user_id=$1`, [
      secondaryUserId,
    ]);
    await client.query(
      `UPDATE users
       SET username=$2,
           merged_into_user_id=$3,
           login_disabled_at=now()
       WHERE id=$1`,
      [secondaryUserId, buildMergedUsername(secondaryUserId), primaryUserId],
    );

    for (const name of namesToTransfer) {
      await addPublicName(client, primaryUserId, name, "alias");
    }

    await ensurePrimaryTag(client, primaryUserId);
    await client.query("COMMIT");
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    throw e;
  } finally {
    client.release();
  }

  for (const userId of affectedUsers) {
    await notifyRoomListUpdate(userId);
  }
  return { ok: true };
}

// Send room list update to specific user
async function notifyRoomListUpdate(userId) {
  const { rows } = await pool.query(
    `
    SELECT r.id, r.pinned_message_id, r.is_saved, coalesce((SELECT to_jsonb(p) FROM room_preferences p WHERE p.room_id=r.id AND p.user_id=$1),'{}'::jsonb) AS preferences, rm.role AS my_role, r.kind, r.title, r.avatar_id, r.created_by, r.created_at,
           lm.id as last_msg_id, lm.type as last_msg_type, lm.text as last_msg_text, lm.meta as last_msg_meta, lm.encrypted_content as last_msg_encrypted_content,
           lm.sender_id as last_msg_sender_id, lmu.username as last_msg_sender_username, lmu.display_name as last_msg_sender_name,
           lm.created_at as last_msg_at,
           ou.id as other_user_id, ou.username as other_username, ou.display_name as other_display_name, ou.avatar_id as other_avatar_id,
           (SELECT COUNT(*)::int FROM messages m
            WHERE m.room_id = r.id
            AND m.sender_id <> $1
            AND NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.message_id=m.id AND h.user_id=$1)
            AND (m.read_by IS NULL OR NOT m.read_by @> $2::jsonb)
           ) as unread_count
    FROM rooms r
    JOIN room_members rm ON rm.room_id=r.id
    LEFT JOIN LATERAL (
      SELECT m.id, m.type, m.text, m.meta, m.encrypted_content, m.sender_id, m.created_at
      FROM messages m WHERE m.room_id = r.id AND NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.message_id=m.id AND h.user_id=$1) ORDER BY m.created_at DESC,m.id DESC LIMIT 1
    ) lm ON true
    LEFT JOIN users lmu ON lmu.id = lm.sender_id
    LEFT JOIN LATERAL (
      SELECT u.id, u.username, u.display_name, u.avatar_id
      FROM room_members rm2 JOIN users u ON u.id = rm2.user_id
      WHERE rm2.room_id = r.id AND rm2.user_id <> $1
      LIMIT 1
    ) ou ON r.kind = 'dm'
    WHERE rm.user_id=$1
    ORDER BY COALESCE(lm.created_at, r.created_at) DESC
    LIMIT 200
  `,
    [userId, JSON.stringify([userId])],
  );

  sendToUser(userId, { type: "rooms.update", rooms: rows.map(room=>({...room,title:decryptMetadata(room.title),other_display_name:decryptMetadata(room.other_display_name),last_msg_sender_name:decryptMetadata(room.last_msg_sender_name)})) });
}

async function notifyRoomListUpdateForRoom(roomId) {
  const { rows } = await pool.query(
    `SELECT user_id FROM room_members WHERE room_id=$1`,
    [roomId],
  );
  await Promise.all(
    rows.map((row) => notifyRoomListUpdate(String(row.user_id))),
  );
}

// ===================== ROUTES =====================

// Static files
app.get("/app.js", async (req, reply) => {
  reply.header("Content-Type", "application/javascript");
  reply.header("Cache-Control", "no-cache, no-store, must-revalidate");
  return reply.send(fs.readFileSync("/srv/app.js", "utf8"));
});

app.get("/app.css", async (req, reply) => {
  reply.header("Content-Type", "text/css");
  reply.header("Cache-Control", "no-cache, no-store, must-revalidate");
  return reply.send(fs.readFileSync("/srv/app.css", "utf8"));
});

app.get("/manifest.webmanifest", async (req, reply) => {
  reply.header("Content-Type", "application/manifest+json");
  reply.header("Cache-Control", "no-cache, no-store, must-revalidate");
  return reply.send(fs.readFileSync("/srv/manifest.webmanifest", "utf8"));
});

app.get("/", async (req, reply) => {
  reply.header("Content-Type", "text/html");
  reply.header("Cache-Control", "no-cache, no-store, must-revalidate");
  return reply.send(fs.readFileSync("/srv/index.html", "utf8"));
});

app.get("/health", async () => ({ ok: true }));

// --- ICE / TURN configuration (time-limited coturn REST credentials) ---
// coturn runs with use-auth-secret / static-auth-secret=TURN_SECRET.
//   username   = "<expiry-unix>:<userid>"
//   credential = base64(HMAC-SHA1(TURN_SECRET, username))
function turnUrlList() {
  const explicit = String(TURN_URLS)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (explicit.length) return explicit;
  if (!TURN_EXTERNAL_IP) return [];
  return [
    `turn:${TURN_EXTERNAL_IP}:3478?transport=udp`,
    `turn:${TURN_EXTERNAL_IP}:3478?transport=tcp`,
  ];
}

function buildTurnCredentials(userId) {
  const urls = turnUrlList();
  if (!TURN_SECRET || !urls.length) return null;
  const ttl = Math.max(
    60,
    Number.parseInt(TURN_CREDENTIAL_TTL_SEC, 10) || 3600,
  );
  const username = `${Math.floor(Date.now() / 1000) + ttl}:${userId}`;
  const credential = crypto
    .createHmac("sha1", TURN_SECRET)
    .update(username)
    .digest("base64");
  return { urls, username, credential, ttl };
}

// Response shape: `turn` matches window.__APP_CONFIG__.turn ({urls, username,
// credential}) and `iceServers` can be passed straight to RTCPeerConnection.
app.get(
  "/rtc/ice-servers",
  {
    config: { rateLimit: { max: 30, timeWindow: "1 minute" } },
  },
  async (req, reply) => {
    const me = await requireAuth(req);
    const turn = buildTurnCredentials(me.id);
    reply.header("Cache-Control", "no-store");
    const iceServers = [
      { urls: "stun:stun.l.google.com:19302" },
      { urls: "stun:stun1.l.google.com:19302" },
    ];
    if (turn)
      iceServers.push({
        urls: turn.urls,
        username: turn.username,
        credential: turn.credential,
      });
    return { turn, iceServers, ttl: turn ? turn.ttl : null };
  },
);

// --- Group E2E (Sender Keys) ---
// Each member publishes one sender key per group; it is delivered to every other
// member encrypted with the pairwise Signal session. The server stores opaque
// payloads only and never sees key material.
const GROUP_SENDER_KEY_MAX_RECIPIENTS = 500;
const GROUP_SENDER_KEY_MAX_PAYLOAD_BYTES = 16 * 1024;
const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

app.get("/rooms/:roomId/e2e", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.roomId);
  if (!UUID_RE.test(roomId))
    return reply.code(400).send({ error: "bad_room_id" });
  if (!(await isRoomMember(roomId, me.id)))
    return reply.code(404).send({ error: "not_found" });
  const { rows } = await pool.query(
    `SELECT kind, e2e_enabled FROM rooms WHERE id=$1 LIMIT 1`,
    [roomId],
  );
  return {
    enabled: rows[0]?.kind === "group" && rows[0]?.e2e_enabled === true,
  };
});

app.put("/rooms/:roomId/e2e", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.roomId);
  if (!UUID_RE.test(roomId))
    return reply.code(400).send({ error: "bad_room_id" });
  const role = await getRoomRole(roomId, me.id);
  if (!role) return reply.code(404).send({ error: "not_found" });
  if (role !== "owner" && role !== "admin")
    return reply.code(403).send({ error: "forbidden" });
  const { rows } = await pool.query(
    `SELECT kind FROM rooms WHERE id=$1 LIMIT 1`,
    [roomId],
  );
  if (rows[0]?.kind !== "group")
    return reply.code(400).send({ error: "e2e_group_only" });
  const enabled = req.body?.enabled === true;
  await pool.query(`UPDATE rooms SET e2e_enabled=$2 WHERE id=$1`, [
    roomId,
    enabled,
  ]);
  broadcastToRoom(roomId, { type: "room.e2e", room_id: roomId, enabled });
  return { enabled };
});

app.post("/rooms/:roomId/sender-keys", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.roomId);
  if (!UUID_RE.test(roomId))
    return reply.code(400).send({ error: "bad_room_id" });
  if (!(await isRoomMember(roomId, me.id)))
    return reply.code(404).send({ error: "not_found" });
  const retry = hitUserLimit("sender-keys", me.id, 60, 60 * 60 * 1000);
  if (retry)
    return reply
      .code(429)
      .header("Retry-After", String(retry))
      .send({ error: "rate_limited" });

  const body = req.body || {};
  const keyId = String(body.key_id || "").slice(0, 64);
  const recipients = Array.isArray(body.recipients) ? body.recipients : [];
  if (!keyId) return reply.code(400).send({ error: "key_id_required" });
  if (
    !recipients.length ||
    recipients.length > GROUP_SENDER_KEY_MAX_RECIPIENTS
  ) {
    return reply.code(400).send({ error: "bad_recipients" });
  }
  const ids = [];
  for (const r of recipients) {
    const uid = String(r?.user_id || "");
    const payload = String(r?.payload || "");
    if (!uid || !payload)
      return reply.code(400).send({ error: "bad_recipients" });
    if (
      Buffer.byteLength(payload, "utf8") > GROUP_SENDER_KEY_MAX_PAYLOAD_BYTES
    ) {
      return reply.code(413).send({ error: "sender_key_too_large" });
    }
    ids.push(uid);
  }
  // Every recipient must be a current member; no keys to outsiders.
  const { rows: memberRows } = await pool.query(
    `SELECT user_id FROM room_members WHERE room_id=$1 AND user_id = ANY($2::uuid[])`,
    [roomId, ids],
  );
  if (memberRows.length !== new Set(ids).size)
    return reply.code(400).send({ error: "recipient_not_member" });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    // Retain each rotation until its recipient device acknowledges it. Offline history needs older keys.
    for (const r of recipients) {
      const device = r.device_id ? String(r.device_id) : null;
      const valid = device ? await client.query(`SELECT 1 FROM user_devices WHERE user_id=$1 AND device_id=$2`, [String(r.user_id), device]) : {rows:[1]};
      if (!valid.rows.length) throw Object.assign(new Error('Unknown recipient device'), {statusCode:400});
      await client.query(`INSERT INTO group_sender_keys(room_id,from_user_id,to_user_id,key_id,payload,to_device_id)
        SELECT $1,$2,$3,$4,$5,$6 WHERE NOT EXISTS(SELECT 1 FROM group_sender_keys WHERE room_id=$1 AND from_user_id=$2 AND to_user_id=$3 AND key_id=$4 AND to_device_id IS NOT DISTINCT FROM $6::text)`,
        [roomId,me.id,String(r.user_id),keyId,String(r.payload),device]);
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
  for (const uid of new Set(ids)) {
    sendToUser(uid, {
      type: "sender_key.available",
      room_id: roomId,
      from_user_id: me.id,
      key_id: keyId,
    });
  }
  return { stored: recipients.length };
});

app.get("/rooms/:roomId/sender-keys", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.roomId);
  if (!UUID_RE.test(roomId))
    return reply.code(400).send({ error: "bad_room_id" });
  if (!(await isRoomMember(roomId, me.id)))
    return reply.code(404).send({ error: "not_found" });
  const { rows } = await pool.query(
    `SELECT id, from_user_id, key_id, payload, created_at FROM group_sender_keys
     WHERE room_id=$1 AND to_user_id=$2 AND (to_device_id IS NULL OR to_device_id=$3) ORDER BY created_at ASC LIMIT 200`,
    [roomId, me.id, req.query.device_id ? String(req.query.device_id) : null],
  );
  return rows;
});

app.delete("/rooms/:roomId/sender-keys/:id", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.roomId);
  if (!UUID_RE.test(roomId))
    return reply.code(400).send({ error: "bad_room_id" });
  const rowId = String(req.params.id);
  if (!UUID_RE.test(rowId)) return reply.code(400).send({ error: "bad_id" });
  await pool.query(
    `DELETE FROM group_sender_keys WHERE id=$1 AND room_id=$2 AND to_user_id=$3 AND (to_device_id IS NULL OR to_device_id=$4)`,
    [rowId, roomId, me.id, req.query.device_id ? String(req.query.device_id) : null],
  );
  return { ok: true };
});

// --- Auth ---
app.get("/auth/config", async () => ({ captcha_required: !!HCAPTCHA_SECRET, captcha_site_key: process.env.HCAPTCHA_SITE_KEY || "11dc04d6-78e8-44f8-9814-c5c032fc68be" }));
app.post(
  "/auth/register",
  {
    config: {
      rateLimit: {
        max: 10,
        timeWindow: "1 minute",
      },
    },
  },
  async (req, reply) => {
    const body = req.body || {};
    const username = String(body.username || "").trim();
    const password = String(body.password || "");
    const captchaToken = String(body.captcha || "");

    // Verify captcha
    const captchaValid = await verifyCaptcha(captchaToken);
    if (!captchaValid) return reply.code(400).send({ error: "captcha_failed" });

    if (username.length < 3)
      return reply.code(400).send({ error: "username_too_short" });
    if (username.length > 30)
      return reply.code(400).send({ error: "username_too_long" });
    if (!/^[a-zA-Z0-9_]+$/.test(username))
      return reply.code(400).send({ error: "username_invalid" });
    if (password.length < 6)
      return reply.code(400).send({ error: "password_too_short" });
    const takenId = await resolveUserIdByPublicName(username);
    if (takenId) return reply.code(409).send({ error: "username_taken" });

    const password_hash = await argon2.hash(password);

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const { rows } = await client.query(
        `INSERT INTO users(username, password_hash) VALUES ($1,$2) RETURNING id, username, created_at`,
        [username, password_hash],
      );
      const userId = rows[0].id;
      await client.query(
        `INSERT INTO user_public_names(user_id, name, kind) VALUES ($1, $2, 'primary_tag') ON CONFLICT (name) DO NOTHING`,
        [userId, username],
      );
      const token = randToken();
      const token_hash = sha256(token);
      const ttlDays = Number(TOKEN_TTL_DAYS) || 30;

      await client.query(
        `INSERT INTO sessions(user_id, token_hash, expires_at) VALUES ($1,$2, now() + ($3::int || ' days')::interval)`,
        [userId, token_hash, ttlDays],
      );

      // News membership is optional; account and session are atomic.
      await client.query("SAVEPOINT news_membership");
      try {
        const { rows: newsChannel } = await client.query(
          `SELECT id FROM rooms WHERE kind='channel' AND title=$1 LIMIT 1`,
          [SYSTEM_CHANNEL_TITLE],
        );
        if (newsChannel.length > 0) {
          await client.query(
            `INSERT INTO room_members(room_id, user_id, role) VALUES($1, $2, 'member') ON CONFLICT DO NOTHING`,
            [newsChannel[0].id, userId],
          );
        }
        await client.query("RELEASE SAVEPOINT news_membership");
      } catch (channelErr) {
        await client.query("ROLLBACK TO SAVEPOINT news_membership");
        log.error("Failed to add user to news channel", channelErr);
      }

      await client.query("COMMIT");
      reply.setCookie("token", token, authCookieOptions(req, ttlDays));
      return wantsBearerSession(req)
        ? { user: rows[0], token }
        : { user: rows[0] };
    } catch (e) {
      await client.query("ROLLBACK").catch(() => {});
      if (String(e.code) === "23505")
        return reply.code(409).send({ error: "username_taken" });
      throw e;
    } finally {
      client.release();
    }
  },
);

app.post(
  "/auth/login",
  {
    config: {
      rateLimit: {
        max: 10,
        timeWindow: "1 minute",
      },
    },
  },
  async (req, reply) => {
    const body = req.body || {};
    const username = String(body.username || "").trim();
    const password = String(body.password || "");

    // Validate username length
    if (username.length > 30)
      return reply.code(400).send({ error: "username_too_long" });

    const { rows } = await pool.query(
      `SELECT id, username, password_hash, banned, merged_into_user_id, login_disabled_at
     FROM users
     WHERE username=$1
     LIMIT 1`,
      [username],
    );
    if (!rows.length) return reply.code(401).send({ error: "bad_credentials" });
    if (rows[0].banned)
      return reply.code(403).send({ error: "account_banned" });
    if (rows[0].merged_into_user_id || rows[0].login_disabled_at)
      return reply.code(403).send({ error: "account_merged" });

    const storedHash = String(rows[0].password_hash || "");
    const ok = await verifyPasswordHash(storedHash, password);
    if (!ok) return reply.code(401).send({ error: "bad_credentials" });
    await upgradeLegacyPasswordIfNeeded(pool, rows[0].id, storedHash, password);

    const token = randToken();
    const token_hash = sha256(token);
    const ttlDays = Number(TOKEN_TTL_DAYS) || 30;

    // Get device info
    const userAgent = req.headers["user-agent"] || "";
    const { deviceName, deviceType } = parseUserAgent(userAgent);
    const ipAddress = getClientIp(req);

    await pool.query(
      `INSERT INTO sessions(user_id, token_hash, expires_at, device_name, device_type, ip_address, user_agent, last_activity)
     VALUES ($1, $2, now() + ($3::int || ' days')::interval, $4, $5, $6, $7, now())`,
      [
        rows[0].id,
        token_hash,
        ttlDays,
        deviceName,
        deviceType,
        ipAddress,
        userAgent,
      ],
    );

    reply.setCookie("token", token, authCookieOptions(req, ttlDays));
    return wantsBearerSession(req)
      ? { user: { id: rows[0].id, username: rows[0].username }, token }
      : { user: { id: rows[0].id, username: rows[0].username } };
  },
);

app.post("/auth/logout", async (req, reply) => {
  const me = await requireAuth(req);
  const th = sha256(me.token);
  await pool.query(`DELETE FROM sessions WHERE token_hash=$1`, [th]);
  reply.clearCookie("token", { path: "/" });
  return { ok: true };
});

// Get all active sessions for current user
app.get("/auth/sessions", async (req, reply) => {
  const me = await requireAuth(req);
  const currentTokenHash = sha256(me.token);

  const { rows } = await pool.query(
    `SELECT id, device_name, device_type, ip_address, last_activity, created_at,
            token_hash = $2 as is_current
     FROM sessions
     WHERE user_id = $1 AND expires_at > now()
     ORDER BY last_activity DESC`,
    [me.id, currentTokenHash],
  );

  return rows.map((s) => ({
    id: s.id,
    deviceName: s.device_name || "Unknown Device",
    deviceType: s.device_type || "desktop",
    ipAddress: s.ip_address,
    lastActivity: s.last_activity,
    createdAt: s.created_at,
    isCurrent: s.is_current,
  }));
});

// Terminate a specific session
app.delete("/auth/sessions/:sessionId", async (req, reply) => {
  const me = await requireAuth(req);
  const sessionId = req.params.sessionId;

  // Verify session belongs to user
  const { rows } = await pool.query(
    `SELECT id FROM sessions WHERE id = $1 AND user_id = $2`,
    [sessionId, me.id],
  );

  if (!rows.length) {
    return reply.code(404).send({ error: "session_not_found" });
  }

  await pool.query(`DELETE FROM sessions WHERE id = $1`, [sessionId]);
  return { ok: true };
});

// Terminate all sessions except current
app.post("/auth/sessions/terminate-others", async (req, reply) => {
  const me = await requireAuth(req);
  const currentTokenHash = sha256(me.token);

  const { rowCount } = await pool.query(
    `DELETE FROM sessions WHERE user_id = $1 AND token_hash != $2`,
    [me.id, currentTokenHash],
  );

  return { ok: true, terminated: rowCount };
});

// Terminate all sessions (including current)
app.post("/auth/sessions/terminate-all", async (req, reply) => {
  const me = await requireAuth(req);

  const { rowCount } = await pool.query(
    `DELETE FROM sessions WHERE user_id = $1`,
    [me.id],
  );

  reply.clearCookie("token", { path: "/" });
  return { ok: true, terminated: rowCount };
});

// --- Profile ---
app.get("/me", async (req, reply) => {
  const me = await requireAuth(req);
  const { rows } = await pool.query(
    `SELECT id, username, display_name, avatar_id, profile_tagline, last_seen, created_at FROM users WHERE id=$1`,
    [me.id],
  );
  const u = rows[0];
  const admin = await isAdmin(me.id);
  const publicNames = await ensurePrimaryTag(pool, me.id);

  // Decrypt metadata
  const decryptedDisplayName = decryptMetadata(u.display_name);
  const decryptedTagline = decryptMetadata(u.profile_tagline);

  return {
    id: u.id,
    username: u.username,
    display_name: decryptedDisplayName,
    avatar_id: u.avatar_id,
    profile_tagline: decryptedTagline,
    last_seen: u.last_seen,
    created_at: u.created_at,
    online: true,
    isAdmin: admin,
    ...publicNames,
  };
});

app.put("/me", async (req, reply) => {
  const me = await requireAuth(req);
  const body = req.body || {};
  const display_name =
    body.display_name !== undefined
      ? body.display_name
        ? String(body.display_name).trim().slice(0, 64)
        : null
      : undefined;
  const profile_tagline =
    body.profile_tagline !== undefined
      ? body.profile_tagline
        ? String(body.profile_tagline).trim().slice(0, 140)
        : null
      : undefined;

  if (display_name !== undefined) {
    const encryptedDisplayName = encryptMetadata(display_name);
    await pool.query(`UPDATE users SET display_name=$2 WHERE id=$1`, [
      me.id,
      encryptedDisplayName,
    ]);
  }
  if (profile_tagline !== undefined) {
    const encryptedTagline = encryptMetadata(profile_tagline);
    await pool.query(`UPDATE users SET profile_tagline=$2 WHERE id=$1`, [
      me.id,
      encryptedTagline,
    ]);
  }
  return { ok: true };
});

app.put("/me/password", async (req, reply) => {
  const me = await requireAuth(req);
  const body = req.body || {};
  const new_password = String(body.new_password || "");

  if (new_password.length < 6)
    return reply.code(400).send({ error: "password_too_short" });

  const password_hash = await argon2.hash(new_password);
  await pool.query(`UPDATE users SET password_hash=$2 WHERE id=$1`, [
    me.id,
    password_hash,
  ]);

  // Invalidate all sessions except current
  const token = req.cookies?.token;
  const token_hash = token ? sha256(token) : null;
  if (token_hash) {
    await pool.query(
      `DELETE FROM sessions WHERE user_id=$1 AND token_hash<>$2`,
      [me.id, token_hash],
    );
  }

  return { ok: true };
});

app.post("/me/avatar", async (req, reply) => {
  const me = await requireAuth(req);
  const file = await req.file();
  if (!file) return reply.code(400).send({ error: "file_required" });
  if (!isSafeMime("avatar", file.mimetype))
    return reply.code(400).send({ error: "bad_mime" });

  const ext = path.extname(file.filename || "") || ".jpg";
  const id = crypto.randomUUID();
  const rel = id + ext;
  const abs = path.join(UPLOAD_DIR, rel);

  const ws = fs.createWriteStream(abs);
  let size = 0;
  await new Promise((resolve, reject) => {
    file.file.on("data", (buf) => {
      size += buf.length;
    });
    file.file.on("error", reject);
    ws.on("error", reject);
    ws.on("finish", resolve);
    file.file.pipe(ws);
  });

  const mime = file.mimetype || "image/jpeg";
  const { rows } = await pool.query(
    `INSERT INTO media(owner_id, kind, mime, size_bytes, path) VALUES ($1,'avatar',$2,$3,$4) RETURNING id`,
    [me.id, mime, size, rel],
  );

  await pool.query(`UPDATE users SET avatar_id=$2 WHERE id=$1`, [
    me.id,
    rows[0].id,
  ]);
  return { avatar_id: rows[0].id };
});

app.post("/me/fcm-token", async (req, reply) => {
  const me = await requireAuth(req);
  const body = req.body || {};
  const fcm_token = body.fcm_token ? String(body.fcm_token).trim() : null;

  if (!fcm_token) return reply.code(400).send({ error: "fcm_token_required" });

  await pool.query(`UPDATE users SET fcm_token=$2 WHERE id=$1`, [
    me.id,
    fcm_token,
  ]);
  return { ok: true };
});

app.get("/me/public-names", async (req, reply) => {
  const me = await requireAuth(req);
  const { rows } = await pool.query(
    `SELECT profile_tagline FROM users WHERE id=$1 LIMIT 1`,
    [me.id],
  );
  const publicNames = await ensurePrimaryTag(pool, me.id);
  return {
    profile_tagline: rows[0]?.profile_tagline || null,
    ...publicNames,
  };
});

app.post("/me/public-names/merge", async (req, reply) => {
  const me = await requireAuth(req);
  const secondaryUsername = normalizePublicName(req.body?.secondary_username);
  const secondaryPassword = String(req.body?.secondary_password || "");
  if (!secondaryUsername || !secondaryPassword)
    return reply.code(400).send({ error: "secondary_credentials_required" });
  if (secondaryUsername.length > 30)
    return reply.code(400).send({ error: "username_too_long" });
  if (String(me.username).toLowerCase() === secondaryUsername.toLowerCase())
    return reply.code(400).send({ error: "cannot_merge_self" });

  const { rows } = await pool.query(
    `SELECT id, username, password_hash, banned, merged_into_user_id, login_disabled_at
     FROM users
     WHERE username=$1
     LIMIT 1`,
    [secondaryUsername],
  );
  if (!rows.length)
    return reply.code(404).send({ error: "secondary_not_found" });
  const secondary = rows[0];
  if (String(secondary.id) === String(me.id))
    return reply.code(400).send({ error: "cannot_merge_self" });
  if (secondary.banned)
    return reply.code(403).send({ error: "secondary_banned" });
  if (secondary.merged_into_user_id || secondary.login_disabled_at)
    return reply.code(400).send({ error: "already_merged" });
  if (await isAdmin(secondary.id))
    return reply.code(403).send({ error: "cannot_merge_admin" });
  const verified = await verifyPasswordHash(
    secondary.password_hash,
    secondaryPassword,
  );
  if (!verified)
    return reply.code(401).send({ error: "bad_secondary_credentials" });
  await upgradeLegacyPasswordIfNeeded(
    pool,
    secondary.id,
    secondary.password_hash,
    secondaryPassword,
  );

  const result = await mergeSecondaryAccountIntoPrimary(
    me.id,
    secondary.id,
    secondary.username,
  );
  if (!result.ok)
    return reply.code(400).send({ error: result.error || "merge_failed" });
  const profile = await getUserProfileForViewer(me.id, me.id);
  return { ok: true, profile };
});

app.post("/me/public-names/:id/promote", async (req, reply) => {
  const me = await requireAuth(req);
  const publicNameId = String(req.params.id || "");
  const { rows } = await pool.query(
    `SELECT id, user_id, name, kind FROM user_public_names WHERE id=$1 LIMIT 1`,
    [publicNameId],
  );
  if (!rows.length || String(rows[0].user_id) !== String(me.id))
    return reply.code(404).send({ error: "not_found" });
  const current = await ensurePrimaryTag(pool, me.id);
  const currentPrimary = current.public_names.find(
    (item) => item.kind === "primary_tag",
  );
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    if (currentPrimary && String(currentPrimary.id) !== publicNameId) {
      await client.query(
        `UPDATE user_public_names SET kind='alias' WHERE id=$1`,
        [currentPrimary.id],
      );
    }
    await client.query(
      `UPDATE user_public_names SET kind='primary_tag' WHERE id=$1`,
      [publicNameId],
    );
    await client.query("COMMIT");
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    throw e;
  } finally {
    client.release();
  }
  return { ok: true, public_names: await ensurePrimaryTag(pool, me.id) };
});

app.delete("/me/public-names/:id", async (req, reply) => {
  const me = await requireAuth(req);
  const publicNameId = String(req.params.id || "");
  const { rows } = await pool.query(
    `SELECT id, user_id, name, kind FROM user_public_names WHERE id=$1 LIMIT 1`,
    [publicNameId],
  );
  if (!rows.length || String(rows[0].user_id) !== String(me.id))
    return reply.code(404).send({ error: "not_found" });
  const row = rows[0];
  if (String(row.name).toLowerCase() === String(me.username).toLowerCase()) {
    return reply.code(400).send({ error: "cannot_delete_canonical_name" });
  }
  await pool.query(`DELETE FROM user_public_names WHERE id=$1`, [publicNameId]);
  const publicNames = await ensurePrimaryTag(pool, me.id);
  return { ok: true, public_names: publicNames };
});

// --- Admin API ---
app.get("/admin/users", async (req, reply) => {
  const me = await requireAuth(req);
  if (!(await isAdmin(me.id)))
    return reply.code(403).send({ error: "forbidden" });
  const { rows } = await pool.query(
    `SELECT id, username, display_name, avatar_id, last_seen, created_at, banned FROM users ORDER BY created_at DESC LIMIT 500`,
  );
  return rows.map((r) => ({ ...r, online: isUserOnline(r.id) }));
});

// Search users for adding to groups/channels
app.get("/users/search", async (req, reply) => {
  const me = await requireAuth(req);
  const q = String(req.query.q || "")
    .trim()
    .toLowerCase();
  if (!q || q.length < 2)
    return reply.code(400).send({ error: "query_too_short" });

  const { rows } = await pool.query(
    `SELECT DISTINCT u.id, u.username, u.display_name, u.avatar_id
     FROM users u
     LEFT JOIN user_public_names upn ON upn.user_id = u.id
     WHERE (
       lower(u.username) LIKE $1
       OR lower(coalesce(u.display_name, '')) LIKE $1
       OR lower(coalesce(u.profile_tagline, '')) LIKE $1
       OR lower(coalesce(upn.name::text, '')) LIKE $1
     )
       AND u.id != $2
       AND coalesce(u.banned, false) != true
       AND u.merged_into_user_id IS NULL
     ORDER BY u.username
     LIMIT 20`,
    [`%${q}%`, me.id],
  );
  return rows.map((user) => ({
    ...user,
    display_name: decryptMetadata(user.display_name),
  }));
});

// Reactions API
app.post(
  "/messages/:id/reactions",
  {
    config: {
      rateLimit: {
        max: 30,
        timeWindow: "1 minute",
      },
    },
  },
  async (req, reply) => {
    const me = await requireAuth(req);
    const messageId = String(req.params.id);
    const { emoji } = req.body || {};
    if (!emoji) return reply.code(400).send({ error: "missing_emoji" });

    // Check if user can see this message
    const { rows: msgCheck } = await pool.query(
      `SELECT m.id, m.room_id FROM messages m JOIN room_members rm ON rm.room_id = m.room_id WHERE m.id=$1 AND rm.user_id=$2 LIMIT 1`,
      [messageId, me.id],
    );
    if (!msgCheck[0]) return reply.code(404).send({ error: "not_found" });

    // Add reaction - replace any existing reaction from this user (one reaction per user per message)
    await pool.query(
      `INSERT INTO reactions(message_id, user_id, emoji) VALUES ($1,$2,$3) ON CONFLICT (message_id, user_id) DO UPDATE SET emoji = EXCLUDED.emoji, created_at = now()`,
      [messageId, me.id, emoji],
    );

    // Broadcast to room
    broadcastToRoom(msgCheck[0].room_id, {
      type: "reaction.add",
      message_id: messageId,
      user_id: me.id,
      emoji,
    });
    return { ok: true };
  },
);

app.delete("/messages/:id/reactions/:emoji", async (req, reply) => {
  const me = await requireAuth(req);
  const messageId = String(req.params.id);
  const emoji = decodeURIComponent(req.params.emoji);

  const { rows: msgCheck } = await pool.query(
    `SELECT m.id, m.room_id FROM messages m JOIN room_members rm ON rm.room_id = m.room_id WHERE m.id=$1 AND rm.user_id=$2 LIMIT 1`,
    [messageId, me.id],
  );
  if (!msgCheck[0]) return reply.code(404).send({ error: "not_found" });

  await pool.query(
    `DELETE FROM reactions WHERE message_id=$1 AND user_id=$2 AND emoji=$3`,
    [messageId, me.id, emoji],
  );
  broadcastToRoom(msgCheck[0].room_id, {
    type: "reaction.remove",
    message_id: messageId,
    user_id: me.id,
    emoji,
  });
  return { ok: true };
});

app.get("/messages/:id/reactions", async (req, reply) => {
  const me = await requireAuth(req);
  const messageId = String(req.params.id);

  const { rows: msgCheck } = await pool.query(
    `SELECT m.id FROM messages m JOIN room_members rm ON rm.room_id = m.room_id WHERE m.id=$1 AND rm.user_id=$2 LIMIT 1`,
    [messageId, me.id],
  );
  if (!msgCheck[0]) return reply.code(404).send({ error: "not_found" });

  const { rows } = await pool.query(
    `SELECT emoji, user_id FROM reactions WHERE message_id=$1`,
    [messageId],
  );

  // Group by emoji
  const grouped = {};
  for (const r of rows) {
    if (!grouped[r.emoji]) grouped[r.emoji] = [];
    grouped[r.emoji].push(r.user_id);
  }
  return grouped;
});

app.put("/admin/users/:id/password", async (req, reply) => {
  const me = await requireAuth(req);
  if (!(await isAdmin(me.id)))
    return reply.code(403).send({ error: "forbidden" });
  const userId = String(req.params.id);
  const newPassword = String(req.body?.new_password || "");
  if (newPassword.length < 6)
    return reply.code(400).send({ error: "password_too_short" });
  const hash = await argon2.hash(newPassword);
  await pool.query(`UPDATE users SET password_hash=$2 WHERE id=$1`, [
    userId,
    hash,
  ]);
  // Invalidate all sessions for this user
  await pool.query(`DELETE FROM sessions WHERE user_id=$1`, [userId]);
  return { ok: true };
});

app.put("/admin/users/:id/ban", async (req, reply) => {
  const me = await requireAuth(req);
  if (!(await isAdmin(me.id)))
    return reply.code(403).send({ error: "forbidden" });
  const userId = String(req.params.id);
  const banned = req.body?.banned === true;
  if (userId === me.id)
    return reply.code(400).send({ error: "cannot_ban_self" });
  await pool.query(`UPDATE users SET banned=$2 WHERE id=$1`, [userId, banned]);
  if (banned) {
    // Invalidate all sessions for banned user
    await pool.query(`DELETE FROM sessions WHERE user_id=$1`, [userId]);
  }
  return { ok: true };
});

app.delete("/admin/users/:id", async (req, reply) => {
  const me = await requireAuth(req);
  if (!(await isAdmin(me.id)))
    return reply.code(403).send({ error: "forbidden" });
  const userId = String(req.params.id);
  if (userId === me.id)
    return reply.code(400).send({ error: "cannot_delete_self" });

  // Check if user is banned
  const { rows } = await pool.query(
    `SELECT banned, username, is_admin FROM users WHERE id=$1`,
    [userId],
  );
  if (!rows.length) return reply.code(404).send({ error: "not_found" });
  if (!rows[0].banned)
    return reply.code(400).send({ error: "user_not_banned" });
  if (
    rows[0].is_admin === true ||
    adminUsernames.has(String(rows[0].username || "").toLowerCase())
  )
    return reply.code(400).send({ error: "cannot_delete_admin" });

  // Delete user (cascade will handle rooms, messages, media, sessions, etc.)
  await pool.query(`DELETE FROM users WHERE id=$1`, [userId]);
  return { ok: true };
});

// --- Users ---
app.get("/users/by-username/:username", async (req, reply) => {
  const me = await requireAuth(req);
  const username = String(req.params.username || "").trim();
  if (!username) return reply.code(404).send({ error: "not_found" });

  const userId = await resolveUserIdByPublicName(username);
  if (!userId) return reply.code(404).send({ error: "not_found" });

  const profile = await getUserProfileForViewer(me.id, userId);
  if (!profile) return reply.code(404).send({ error: "not_found" });
  return profile;
});

app.post("/users/:id/block", async (req, reply) => {
  const me = await requireAuth(req);
  const targetUserId = String(req.params.id);
  if (String(targetUserId) === String(me.id))
    return reply.code(400).send({ error: "cannot_block_self" });

  const { rows } = await pool.query(
    `SELECT id FROM users WHERE id=$1 LIMIT 1`,
    [targetUserId],
  );
  if (!rows.length) return reply.code(404).send({ error: "not_found" });

  await pool.query(
    `INSERT INTO user_blocks(blocker_id, blocked_id)
     VALUES ($1, $2)
     ON CONFLICT (blocker_id, blocked_id) DO NOTHING`,
    [me.id, targetUserId],
  );

  return { ok: true };
});

app.delete("/users/:id/block", async (req, reply) => {
  const me = await requireAuth(req);
  const targetUserId = String(req.params.id);
  await pool.query(
    `DELETE FROM user_blocks WHERE blocker_id=$1 AND blocked_id=$2`,
    [me.id, targetUserId],
  );
  return { ok: true };
});

app.get("/users/:id", async (req, reply) => {
  const me = await requireAuth(req);
  const userId = String(req.params.id);
  const profile = await getUserProfileForViewer(me.id, userId);
  if (!profile) return reply.code(404).send({ error: "not_found" });
  return profile;
});

// --- Rooms ---
app.post(
  "/rooms",
  {
    config: {
      rateLimit: {
        max: 20,
        timeWindow: "1 minute",
      },
    },
  },
  async (req, reply) => {
    const me = await requireAuth(req);
    const body = req.body || {};
    const kind = String(body.kind || "").trim();
    const title = body.title == null ? null : String(body.title);
    const members = Array.isArray(body.members) ? body.members.map(String) : [];

    if (kind !== "dm" && kind !== "group" && kind !== "channel")
      return reply.code(400).send({ error: "bad_kind" });

    const resolveResult = await resolveRoomMembersByNames(members);
    if (!resolveResult.ok) {
      return reply
        .code(404)
        .send({ error: "users_not_found", missing: resolveResult.missing });
    }
    const resolved = resolveResult.users;

    const memberIds = Array.from(
      new Set([me.id, ...resolved.map((u) => u.id)]),
    );

    if (kind === "dm") {
      if (memberIds.length !== 2)
        return reply
          .code(400)
          .send({ error: "dm_requires_exactly_1_other_user" });
      const [a, b] = memberIds;
      const canExchange = await canUsersExchangeDm(a, b);
      if (!canExchange) return reply.code(403).send({ error: "blocked" });
      const roomId = await createDirectRoom(pool, a, b);
      await ensureRoomSecret(roomId);
      // Notify both users about new DM
      notifyRoomListUpdate(a);
      notifyRoomListUpdate(b);
      return { id: roomId };
    }

    const roomKind = kind === "channel" ? "channel" : "group";
    const { rows: created } = await pool.query(
      `INSERT INTO rooms(kind, title, created_by) VALUES ($1, $2, $3) RETURNING id`,
      [roomKind, title, me.id],
    );
    const roomId = created[0].id;

    const values = [];
    const params = [];
    let i = 1;
    for (const uid of memberIds) {
      params.push(roomId, uid, uid === me.id ? "owner" : "member");
      values.push(`($${i++},$${i++},$${i++})`);
    }
    await pool.query(
      "INSERT INTO room_members(room_id, user_id, role) VALUES " +
        values.join(","),
      params,
    );
    await ensureRoomSecret(roomId);
    // Room list updates will happen on next fetch
    return { id: roomId };
  },
);

app.get("/rooms", async (req, reply) => {
  const me = await requireAuth(req);
  // Get rooms with last message and other member info for DMs, sorted by last message time
  const { rows } = await pool.query(
    `
    SELECT r.id, r.pinned_message_id, r.is_saved, coalesce((SELECT to_jsonb(p) FROM room_preferences p WHERE p.room_id=r.id AND p.user_id=$1),'{}'::jsonb) AS preferences, rm.role AS my_role, r.kind, r.title, r.avatar_id, r.created_by, r.created_at,
           lm.id as last_msg_id, lm.type as last_msg_type, lm.text as last_msg_text, lm.meta as last_msg_meta, lm.encrypted_content as last_msg_encrypted_content,
           lm.sender_id as last_msg_sender_id, lmu.username as last_msg_sender_username, lmu.display_name as last_msg_sender_name,
           lm.created_at as last_msg_at,
           ou.id as other_user_id, ou.username as other_username, ou.display_name as other_display_name, ou.avatar_id as other_avatar_id,
           (SELECT COUNT(*)::int FROM messages m
            WHERE m.room_id = r.id
            AND m.sender_id <> $1
            AND NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.message_id=m.id AND h.user_id=$1)
            AND (m.read_by IS NULL OR NOT m.read_by @> $2::jsonb)
           ) as unread_count
    FROM rooms r
    JOIN room_members rm ON rm.room_id=r.id
    LEFT JOIN LATERAL (
      SELECT m.id, m.type, m.text, m.meta, m.encrypted_content, m.sender_id, m.created_at
      FROM messages m WHERE m.room_id = r.id AND NOT EXISTS(SELECT 1 FROM hidden_messages h WHERE h.message_id=m.id AND h.user_id=$1) ORDER BY m.created_at DESC,m.id DESC LIMIT 1
    ) lm ON true
    LEFT JOIN users lmu ON lmu.id = lm.sender_id
    LEFT JOIN LATERAL (
      SELECT u.id, u.username, u.display_name, u.avatar_id
      FROM room_members rm2 JOIN users u ON u.id = rm2.user_id
      WHERE rm2.room_id = r.id AND rm2.user_id <> $1
      LIMIT 1
    ) ou ON r.kind = 'dm'
    WHERE rm.user_id=$1
    ORDER BY COALESCE(lm.created_at, r.created_at) DESC
    LIMIT 200
  `,
    [me.id, JSON.stringify([me.id])],
  );

  // Decrypt metadata for all rooms
  const decryptedRows = rows.map((room) => ({
    ...room,
    title: decryptMetadata(room.title),
    other_display_name: decryptMetadata(room.other_display_name),
    last_msg_sender_name: decryptMetadata(room.last_msg_sender_name),
  }));

  return decryptedRows;
});

app.get("/rooms/:id", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const ok = await isRoomMember(roomId, me.id);
  if (!ok) return reply.code(404).send({ error: "not_found" });

  const { rows } = await pool.query(
    `SELECT id, kind, title, avatar_id, created_by, created_at, pinned_message_id FROM rooms WHERE id=$1 LIMIT 1`,
    [roomId],
  );
  if (!rows.length) return reply.code(404).send({ error: "not_found" });

  const role = await getRoomRole(roomId, me.id);
  let peer = {};
  if (rows[0].kind === "dm") {
    const { rows: others } = await pool.query(
      "SELECT u.id,u.username,u.display_name,u.avatar_id FROM room_members rm JOIN users u ON u.id=rm.user_id WHERE rm.room_id=$1 AND rm.user_id<>$2 LIMIT 1",
      [roomId, me.id],
    );
    if (others[0])
      peer = {
        other_user_id: others[0].id,
        other_username: others[0].username,
        other_display_name: decryptMetadata(others[0].display_name),
        other_avatar_id: others[0].avatar_id,
      };
  }
  return {
    ...rows[0],
    title: decryptMetadata(rows[0].title),
    ...peer,
    my_role: role,
  };
});

app.put("/rooms/:id", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const role = await getRoomRole(roomId, me.id);
  if (role !== "owner" && role !== "admin")
    return reply.code(403).send({ error: "forbidden" });

  const body = req.body || {};
  if (body.title !== undefined) {
    const encryptedTitle = encryptMetadata(
      body.title ? String(body.title).slice(0, 100) : null,
    );
    await pool.query(`UPDATE rooms SET title=$2 WHERE id=$1`, [
      roomId,
      encryptedTitle,
    ]);
  }
  return { ok: true };
});

app.post("/rooms/:id/avatar", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const role = await getRoomRole(roomId, me.id);
  if (role !== "owner" && role !== "admin")
    return reply.code(403).send({ error: "forbidden" });

  const file = await req.file();
  if (!file) return reply.code(400).send({ error: "file_required" });
  if (!isSafeMime("avatar", file.mimetype))
    return reply.code(400).send({ error: "bad_mime" });

  const ext = path.extname(file.filename || "") || ".jpg";
  const id = crypto.randomUUID();
  const rel = id + ext;
  const abs = path.join(UPLOAD_DIR, rel);

  const ws = fs.createWriteStream(abs);
  let size = 0;
  await new Promise((resolve, reject) => {
    file.file.on("data", (buf) => {
      size += buf.length;
    });
    file.file.on("error", reject);
    ws.on("error", reject);
    ws.on("finish", resolve);
    file.file.pipe(ws);
  });

  const mime = file.mimetype || "image/jpeg";
  const { rows } = await pool.query(
    `INSERT INTO media(owner_id, kind, mime, size_bytes, path) VALUES ($1,'avatar',$2,$3,$4) RETURNING id`,
    [me.id, mime, size, rel],
  );

  await pool.query(`UPDATE rooms SET avatar_id=$2 WHERE id=$1`, [
    roomId,
    rows[0].id,
  ]);
  return { avatar_id: rows[0].id };
});

app.get("/rooms/:id/key", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const ok = await isRoomMember(roomId, me.id);
  if (!ok) return reply.code(404).send({ error: "not_found" });
  const secret = await ensureRoomSecret(roomId);
  return { room_id: roomId, key: secret };
});

app.get("/rooms/:id/members", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const ok = await isRoomMember(roomId, me.id);
  if (!ok) return reply.code(404).send({ error: "not_found" });

  // Pagination: ?limit (default 50, max 200) & ?offset. The response stays a plain
  // array for backward compatibility; the total is exposed via X-Total-Count.
  const q = req.query || {};
  const limitRaw = Number.parseInt(q.limit, 10);
  const offsetRaw = Number.parseInt(q.offset, 10);
  const limit = Number.isFinite(limitRaw)
    ? Math.min(Math.max(limitRaw, 1), 200)
    : 50;
  const offset = Number.isFinite(offsetRaw) ? Math.max(offsetRaw, 0) : 0;

  const { rows } = await pool.query(
    `
    SELECT u.id, u.username, u.display_name, u.avatar_id, u.last_seen, rm.role, rm.joined_at
    FROM room_members rm JOIN users u ON u.id = rm.user_id
    WHERE rm.room_id=$1 ORDER BY rm.joined_at ASC, u.id ASC
    LIMIT $2 OFFSET $3
  `,
    [roomId, limit, offset],
  );
  const { rows: totalRows } = await pool.query(
    `SELECT COUNT(*)::int AS n FROM room_members WHERE room_id=$1`,
    [roomId],
  );

  reply.header("X-Total-Count", String(totalRows[0]?.n || 0));
  reply.header("Access-Control-Expose-Headers", "X-Total-Count");
  return rows.map((r) => ({ ...r, online: isUserOnline(r.id) }));
});

app.post("/rooms/:id/members", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const myRole = await getRoomRole(roomId, me.id);
  if (myRole !== "owner" && myRole !== "admin")
    return reply.code(403).send({ error: "forbidden" });

  const body = req.body || {};
  const username = String(body.username || "").trim();
  const directUserId = body.user_id ? String(body.user_id).trim() : "";

  if (!username && !directUserId)
    return reply.code(400).send({ error: "username_required" });

  const resolvedUserId =
    directUserId || (await resolveUserIdByPublicName(username));
  const { rows: users } = await pool.query(
    `SELECT id FROM users WHERE id=$1 LIMIT 1`,
    [resolvedUserId],
  );
  if (!users.length) return reply.code(404).send({ error: "user_not_found" });

  const userId = users[0].id;
  const already = await isRoomMember(roomId, userId);
  if (already) return reply.code(409).send({ error: "already_member" });

  await pool.query(
    `INSERT INTO room_members(room_id, user_id, role) VALUES ($1,$2,'member')`,
    [roomId, userId],
  );

  // Notify the added user about new room
  notifyRoomListUpdate(userId);

  return { ok: true };
});

app.delete("/rooms/:id/members/:userId", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const userId = String(req.params.userId);
  const myRole = await getRoomRole(roomId, me.id);

  if (userId === me.id) {
    // Leave room
    if (myRole === "owner")
      return reply.code(400).send({ error: "owner_cannot_leave" });
    await pool.query(
      `DELETE FROM room_members WHERE room_id=$1 AND user_id=$2`,
      [roomId, me.id],
    );
    await pool.query(
      `DELETE FROM group_sender_keys WHERE room_id=$1 AND to_user_id=$2`,
      [roomId, me.id],
    );
    // Remaining members re-key now (online ones) so the departed member loses access.
    broadcastToRoom(roomId, {
      type: "room.members_changed",
      room_id: roomId,
      removed_user_id: me.id,
    });
    return { ok: true };
  }

  if (myRole !== "owner" && myRole !== "admin")
    return reply.code(403).send({ error: "forbidden" });

  const targetRole = await getRoomRole(roomId, userId);
  if (targetRole === "owner")
    return reply.code(403).send({ error: "cannot_remove_owner" });
  if (targetRole === "admin" && myRole !== "owner")
    return reply.code(403).send({ error: "forbidden" });

  await pool.query(`DELETE FROM room_members WHERE room_id=$1 AND user_id=$2`, [
    roomId,
    userId,
  ]);
  await pool.query(
    `DELETE FROM group_sender_keys WHERE room_id=$1 AND to_user_id=$2`,
    [roomId, userId],
  );
  broadcastToRoom(roomId, {
    type: "room.members_changed",
    room_id: roomId,
    removed_user_id: userId,
  });
  return { ok: true };
});

app.put("/rooms/:id/members/:userId/role", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const userId = String(req.params.userId);
  const myRole = await getRoomRole(roomId, me.id);
  if (myRole !== "owner") return reply.code(403).send({ error: "forbidden" });

  const body = req.body || {};
  const newRole = String(body.role || "");
  if (!["admin", "member"].includes(newRole))
    return reply.code(400).send({ error: "bad_role" });

  await pool.query(
    `UPDATE room_members SET role=$3 WHERE room_id=$1 AND user_id=$2`,
    [roomId, userId, newRole],
  );
  return { ok: true };
});

// Delete room (owner only)
app.delete("/rooms/:id", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const { rows: roomRows } = await pool.query(
    `SELECT id, kind FROM rooms WHERE id=$1 LIMIT 1`,
    [roomId],
  );
  if (!roomRows.length) return reply.code(404).send({ error: "not_found" });
  const room = roomRows[0];
  const myRole = await getRoomRole(roomId, me.id);
  if (!myRole) return reply.code(404).send({ error: "not_found" });
  if (room.kind !== "dm" && myRole !== "owner")
    return reply.code(403).send({ error: "forbidden" });

  const { rows: memberRows } = await pool.query(
    `SELECT user_id FROM room_members WHERE room_id=$1`,
    [roomId],
  );
  await pool.query(`DELETE FROM rooms WHERE id=$1`, [roomId]);

  for (const row of memberRows) {
    sendToUser(String(row.user_id), { type: "room.deleted", room_id: roomId });
  }
  await Promise.all(
    memberRows.map((row) => notifyRoomListUpdate(String(row.user_id))),
  );
  return { ok: true };
});

// --- Messages ---
app.get("/rooms/:id/messages", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const ok = await isRoomMember(roomId, me.id);
  if (!ok) return reply.code(404).send({ error: "not_found" });

  const limit = Math.min(
    200,
    Math.max(1, Number((req.query || {}).limit || 50)),
  );
  const before = (req.query || {}).before
    ? String((req.query || {}).before)
    : null;

  let q = `
    SELECT m.id, m.room_id, m.sender_id, u.username as sender_username, u.display_name as sender_display_name, u.avatar_id as sender_avatar_id,
           m.type, m.text, m.media_id, m.meta, m.created_at, m.read_by, m.reply_to, m.encrypted_content, m.client_message_id, m.edited_at,
           md.kind as media_kind, md.mime as media_mime, md.size_bytes as media_size, md.path as media_path, md.thumb_path, md.duration_sec, md.width, md.height
    FROM messages m
    JOIN users u ON u.id = m.sender_id
    LEFT JOIN media md ON md.id = m.media_id
    WHERE m.room_id=$1
      AND NOT EXISTS (SELECT 1 FROM hidden_messages h WHERE h.message_id=m.id AND h.user_id=$2)
  `;
  const args = [roomId, me.id];

  if (before) {
    q += ` AND m.created_at < (SELECT created_at FROM messages WHERE id=$3) `;
    args.push(before);
  }

  q += ` ORDER BY m.created_at DESC LIMIT $${args.length + 1} `;
  args.push(limit);

  const { rows } = await pool.query(q, args);

  // Load reactions for these messages
  const messageIds = rows.map((r) => r.id);
  let reactionsMap = {};
  if (messageIds.length > 0) {
    const { rows: reactions } = await pool.query(
      `SELECT message_id, emoji, user_id FROM reactions WHERE message_id = ANY($1)`,
      [messageIds],
    );
    for (const r of reactions) {
      if (!reactionsMap[r.message_id]) reactionsMap[r.message_id] = {};
      if (!reactionsMap[r.message_id][r.emoji])
        reactionsMap[r.message_id][r.emoji] = [];
      reactionsMap[r.message_id][r.emoji].push(r.user_id);
    }
  }

  // Ensure read_by is properly serialized and add reactions
  const normalized = rows.map((r) => ({
    ...r,
    read_by: r.read_by || [],
    reactions: reactionsMap[r.id] || {},
  }));
  return normalized.reverse();
});

app.post(
  "/rooms/:id/messages",
  {
    config: {
      rateLimit: {
        max: 60,
        timeWindow: "1 minute",
      },
    },
  },
  async (req, reply) => {
    const me = await requireAuth(req);
    const roomId = String(req.params.id);
    const ok = await isRoomMember(roomId, me.id);
    if (!ok) return reply.code(404).send({ error: "not_found" });

    const dmPeerId = await getDmPeerUserId(roomId, me.id);
    if (dmPeerId && !(await canUsersExchangeDm(me.id, dmPeerId))) {
      return reply.code(403).send({ error: "blocked" });
    }

    const body = req.body || {};
    const clientId =
      body.client_message_id == null ? null : String(body.client_message_id);
    if (clientId && !/^[a-zA-Z0-9_-]{8,100}$/.test(clientId))
      return reply.code(400).send({ error: "bad_client_message_id" });
    const role = await getRoomRole(roomId, me.id);
    const { rows: roomKind } = await pool.query(
      "SELECT kind FROM rooms WHERE id=$1",
      [roomId],
    );
    if (roomKind[0]?.kind === "channel" && !["owner", "admin"].includes(role))
      return reply.code(403).send({ error: "channel_read_only" });
    const type = String(body.type || "text");
    const text = body.text == null ? null : String(body.text);
    const media_id = body.media_id ? String(body.media_id) : null;
    const meta = body.meta && typeof body.meta === "object" ? body.meta : {};
    const reply_to =
      body.reply_to && typeof body.reply_to === "object" ? body.reply_to : null;
    const encrypted_content = body.encrypted_content
      ? String(body.encrypted_content)
      : null;
    if (
      encrypted_content &&
      Buffer.byteLength(encrypted_content, "utf8") > MAX_ENCRYPTED_CONTENT_BYTES
    ) {
      return reply
        .code(413)
        .send({
          error: "encrypted_content_too_large",
          max_bytes: MAX_ENCRYPTED_CONTENT_BYTES,
        });
    }

    if (!["text", "media", "voice", "video_note", "call"].includes(type))
      return reply.code(400).send({ error: "bad_type" });
    if (type === "text" && !encrypted_content && (!text || !text.trim()))
      return reply.code(400).send({ error: "empty_text" });
    if (
      type === "text" &&
      !encrypted_content &&
      (await isGroupE2eEnabled(roomId))
    )
      return reply.code(400).send({ error: "e2e_required" });
    if (["media", "voice", "video_note"].includes(type) && !media_id)
      return reply.code(400).send({ error: "media_required" });

    const { rows } = await pool.query(
      `INSERT INTO messages(room_id, sender_id, type, text, media_id, meta, reply_to, encrypted_content, client_message_id) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9) ON CONFLICT(sender_id,client_message_id) DO NOTHING RETURNING id, created_at`,
      [
        roomId,
        me.id,
        type,
        encrypted_content ? null : text,
        media_id,
        JSON.stringify(meta),
        reply_to ? JSON.stringify(reply_to) : null,
        encrypted_content,
        clientId,
      ],
    );
    if (!rows.length) {
      const { rows: prior } = await pool.query(
        "SELECT id,room_id FROM messages WHERE sender_id=$1 AND client_message_id=$2",
        [me.id, clientId],
      );
      if (prior[0]?.room_id !== roomId)
        return reply.code(409).send({ error: "client_message_id_conflict" });
      return getMobileMessage(pool, prior[0].id, decryptMetadata);
    }

    const { rows: full } = await pool.query(
      `
    SELECT m.id, m.room_id, m.sender_id, u.username as sender_username, u.display_name as sender_display_name, u.avatar_id as sender_avatar_id,
           m.type, m.text, m.media_id, m.meta, m.created_at, m.read_by, m.reply_to, m.encrypted_content, m.client_message_id, m.edited_at,
           md.kind as media_kind, md.mime as media_mime, md.size_bytes as media_size, md.path as media_path, md.thumb_path, md.duration_sec, md.width, md.height
    FROM messages m JOIN users u ON u.id = m.sender_id LEFT JOIN media md ON md.id = m.media_id WHERE m.id=$1 LIMIT 1
  `,
      [rows[0].id],
    );

    broadcastToRoom(roomId, {
      type: "message.new",
      message: { ...full[0], reactions: {} },
    });
    await notifyRoomListUpdateForRoom(roomId);

    // Send push notifications to offline members
    const { rows: members } = await pool.query(
      `SELECT user_id FROM room_members WHERE room_id=$1 AND user_id <> $2`,
      [roomId, me.id],
    );

    for (const member of members) {
      const memberId = String(member.user_id);
      {
        // Get room info for notification
        const { rows: roomInfo } = await pool.query(
          `SELECT kind, title FROM rooms WHERE id=$1 LIMIT 1`,
          [roomId],
        );

        const senderName =
          decryptMetadata(full[0].sender_display_name) ||
          full[0].sender_username;
        const roomTitle =
          roomInfo[0]?.kind === "dm"
            ? senderName
            : decryptMetadata(roomInfo[0]?.title);
        const messageText = full[0].encrypted_content
          ? "New message"
          : full[0].text || "Media";
        const preview =
          messageText.length > 100
            ? messageText.substring(0, 100) + "..."
            : messageText;

        // Provider retries must not delay the acknowledgment of a saved message.
        void sendPushNotification(
          memberId,
          roomTitle || "New message",
          `${senderName}: ${preview}`,
          {
            room_id: roomId,
            message_id: full[0].id,
            sender_id: me.id,
          },
        );
      }
    }

    return { ...full[0], reactions: {} };
  },
);

app.put("/rooms/:roomId/messages/:msgId", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.roomId);
  const msgId = String(req.params.msgId);
  const ok = await isRoomMember(roomId, me.id);
  if (!ok) return reply.code(404).send({ error: "not_found" });

  const body = req.body || {};
  const nextText = body.text == null ? null : String(body.text);
  const nextMeta = body.meta && typeof body.meta === "object" ? body.meta : {};
  const nextEncrypted = body.encrypted_content
    ? String(body.encrypted_content)
    : null;

  if (nextEncrypted) {
    if (
      Buffer.byteLength(nextEncrypted, "utf8") > MAX_ENCRYPTED_CONTENT_BYTES
    ) {
      return reply
        .code(413)
        .send({
          error: "encrypted_content_too_large",
          max_bytes: MAX_ENCRYPTED_CONTENT_BYTES,
        });
    }
    // Valid shape: a JSON object (clients JSON.stringify the Signal ciphertext envelope).
    let parsed = null;
    try {
      parsed = JSON.parse(nextEncrypted);
    } catch {
      parsed = null;
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return reply.code(400).send({ error: "bad_encrypted_content" });
    }
  }

  const { rows: currentRows } = await pool.query(
    `SELECT id, meta, type, encrypted_content FROM messages WHERE id=$1 AND room_id=$2 AND sender_id=$3 LIMIT 1`,
    [msgId, roomId, me.id],
  );
  if (!currentRows.length) return reply.code(404).send({ error: "not_found" });
  if (String(currentRows[0].type) !== "text")
    return reply.code(400).send({ error: "edit_not_supported" });

  // E2E messages: the server must never receive/store plaintext. Editing is only
  // possible by supplying a freshly encrypted payload.
  if (currentRows[0].encrypted_content && !nextEncrypted) {
    return reply.code(400).send({ error: "e2e_message_not_editable" });
  }
  if (nextEncrypted) {
    if (nextText)
      return reply
        .code(400)
        .send({ error: "plaintext_not_allowed_with_encrypted_content" });
  } else if (!nextText || !nextText.trim()) {
    return reply.code(400).send({ error: "empty_text" });
  }

  const mergedMeta = {
    ...(currentRows[0].meta || {}),
    ...nextMeta,
    edited: true,
    edited_at: new Date().toISOString(),
  };

  // read_by is intentionally untouched: editing must not change unread counters.
  if (nextEncrypted) {
    await pool.query(
      `UPDATE messages
       SET text=NULL, encrypted_content=$4, meta=$5::jsonb
       WHERE id=$1 AND room_id=$2 AND sender_id=$3`,
      [msgId, roomId, me.id, nextEncrypted, JSON.stringify(mergedMeta)],
    );
  } else {
    await pool.query(
      `UPDATE messages
       SET text=$4, meta=$5::jsonb
       WHERE id=$1 AND room_id=$2 AND sender_id=$3`,
      [msgId, roomId, me.id, nextText, JSON.stringify(mergedMeta)],
    );
  }

  const { rows: full } = await pool.query(
    `
    SELECT m.id, m.room_id, m.sender_id, u.username as sender_username, u.display_name as sender_display_name, u.avatar_id as sender_avatar_id,
           m.type, m.text, m.media_id, m.meta, m.created_at, m.read_by, m.reply_to, m.encrypted_content, m.client_message_id, m.edited_at,
           md.kind as media_kind, md.mime as media_mime, md.size_bytes as media_size, md.path as media_path, md.thumb_path, md.duration_sec, md.width, md.height
    FROM messages m JOIN users u ON u.id = m.sender_id LEFT JOIN media md ON md.id = m.media_id WHERE m.id=$1 LIMIT 1
  `,
    [msgId],
  );

  let reactions = {};
  const { rows: reactionRows } = await pool.query(
    `SELECT emoji, user_id FROM reactions WHERE message_id=$1`,
    [msgId],
  );
  for (const reaction of reactionRows) {
    if (!reactions[reaction.emoji]) reactions[reaction.emoji] = [];
    reactions[reaction.emoji].push(reaction.user_id);
  }

  await pool.query("UPDATE messages SET edited_at=now() WHERE id=$1", [msgId]);
  const updated = {
    ...full[0],
    edited_at: new Date().toISOString(),
    reactions,
  };
  const {rows:hiddenReaders}=await pool.query('SELECT user_id FROM hidden_messages WHERE message_id=$1',[msgId]);
  broadcastToRoom(roomId, { type: "message.update", message: updated },new Set(hiddenReaders.map(row=>row.user_id)));
  await notifyRoomListUpdateForRoom(roomId);
  return updated;
});

// Mark messages as read
app.post("/rooms/:roomId/read", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.roomId);
  const ok = await isRoomMember(roomId, me.id);
  if (!ok) return reply.code(200).send({ ok: true }); // Silently ignore if not member

  const upTo = req.body?.up_to_message_id || null;
  if (upTo) {
    const { rows: anchor } = await pool.query(
      "SELECT id FROM messages WHERE id=$1 AND room_id=$2",
      [upTo, roomId],
    );
    if (!anchor.length)
      return reply.code(404).send({ error: "message_not_found" });
  }
  const { rows: readRows } = await pool.query(
    `UPDATE messages SET read_by = COALESCE(read_by, '[]'::jsonb) || $3::jsonb
     WHERE room_id=$1 AND sender_id <> $2 AND (read_by IS NULL OR NOT read_by @> $3::jsonb)
       AND ($4::uuid IS NULL OR (created_at,id)<=(SELECT created_at,id FROM messages WHERE id=$4 AND room_id=$1)) RETURNING id`,
    [roomId, me.id, JSON.stringify([me.id]), upTo],
  );

  await pool.query(
    "UPDATE room_preferences SET marked_unread=false WHERE user_id=$1 AND room_id=$2",
    [me.id, roomId],
  );
  // Notify room about read status
  broadcastToRoom(roomId, {
    type: "messages.read",
    user_id: me.id,
    message_ids: readRows.map((r) => r.id),
    room_id: roomId,
  });
  await notifyRoomListUpdate(me.id);

  return { ok: true };
});

// Hide a message for the current user only. The message stays intact for
// everyone else; history queries filter it out via hidden_messages.
app.delete("/rooms/:roomId/messages/:msgId/hide", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.roomId);
  const msgId = String(req.params.msgId);
  if (!(await isRoomMember(roomId, me.id))) return reply.code(404).send({ error: "not_found" });

  const { rows } = await pool.query(
    `SELECT 1 FROM messages WHERE id=$1 AND room_id=$2 LIMIT 1`,
    [msgId, roomId],
  );
  if (!rows.length) return reply.code(404).send({ error: "not_found" });

  await pool.query(
    `WITH locked AS MATERIALIZED (SELECT pg_advisory_xact_lock(670024)),
     inserted AS (INSERT INTO hidden_messages(user_id,message_id) SELECT $1,$2 FROM locked ON CONFLICT DO NOTHING RETURNING message_id)
     INSERT INTO message_changes(room_id,message_id,operation) SELECT $3,message_id,'hide' FROM inserted`,
    [me.id, msgId, roomId],
  );
  sendToUser(me.id,{type:'message.hide',room_id:roomId,message_id:msgId});
  await notifyRoomListUpdate(me.id);
  return { ok: true };
});

// Pin (or unpin) a single message in a chat. Owners/admins pin in groups and
// channels; in a DM either participant may pin.
app.put("/rooms/:roomId/pin", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.roomId);
  if (!(await isRoomMember(roomId, me.id))) return reply.code(404).send({ error: "not_found" });

  const { rows: roomRows } = await pool.query(
    `SELECT kind FROM rooms WHERE id=$1 LIMIT 1`,
    [roomId],
  );
  if (!roomRows.length) return reply.code(404).send({ error: "not_found" });
  const kind = roomRows[0].kind;

  if (kind !== "dm") {
    const role = await getRoomRole(roomId, me.id);
    if (role !== "owner" && role !== "admin") return reply.code(403).send({ error: "forbidden" });
  }

  const messageId = req.body?.message_id ? String(req.body.message_id) : null;
  if (!messageId) {
    await pool.query(
      `UPDATE rooms SET pinned_message_id=NULL, pinned_at=NULL, pinned_by=NULL WHERE id=$1`,
      [roomId],
    );
    broadcastToRoom(roomId, { type: "room.pin", room_id: roomId, message_id: null });
    return { ok: true, message_id: null };
  }

  const { rows } = await pool.query(
    `SELECT id FROM messages WHERE id=$1 AND room_id=$2 LIMIT 1`,
    [messageId, roomId],
  );
  if (!rows.length) return reply.code(404).send({ error: "not_found" });

  await pool.query(
    `UPDATE rooms SET pinned_message_id=$2, pinned_at=now(), pinned_by=$3 WHERE id=$1`,
    [roomId, messageId, me.id],
  );
  broadcastToRoom(roomId, { type: "room.pin", room_id: roomId, message_id: messageId });
  return { ok: true, message_id: messageId };
});

// Delete message (sender only, hard delete)
app.delete("/rooms/:roomId/messages/:msgId", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.roomId);
  const msgId = String(req.params.msgId);

  // Check membership
  const ok = await isRoomMember(roomId, me.id);
  if (!ok) return reply.code(404).send({ error: "not_found" });

  // Verify sender and delete
  const { rows } = await pool.query(
    `DELETE FROM messages WHERE id=$1 AND room_id=$2 AND sender_id=$3 RETURNING id`,
    [msgId, roomId, me.id],
  );

  if (!rows.length) return reply.code(404).send({ error: "not_found" });

  // Notify room about deletion (optional - for UI sync)
  broadcastToRoom(roomId, { type: "message.delete", message_id: msgId });

  return { ok: true };
});

// --- Media ---
app.post(
  "/media/upload",
  {
    config: {
      rateLimit: {
        max: 30,
        timeWindow: "1 minute",
      },
    },
  },
  async (req, reply) => {
    const me = await requireAuth(req);
    const file = await req.file();
    if (!file) return reply.code(400).send({ error: "file_required" });

    const kind = String((req.query || {}).kind || "file");
    if (!["image", "file", "voice", "video_note", "video"].includes(kind))
      return reply.code(400).send({ error: "bad_kind" });
    if (!isSafeMime(kind, file.mimetype))
      return reply.code(400).send({ error: "bad_mime" });

    const ext = path.extname(file.filename || "") || "";
    const id = crypto.randomUUID();
    const rel = id + ext;
    const abs = path.join(UPLOAD_DIR, rel);

    const ws = fs.createWriteStream(abs);
    const hasher = crypto.createHash("sha256");
    let size = 0;
    await new Promise((resolve, reject) => {
      file.file.on("data", (buf) => {
        size += buf.length;
        hasher.update(buf);
      });
      file.file.on("error", reject);
      ws.on("error", reject);
      ws.on("finish", resolve);
      file.file.pipe(ws);
    });
    const contentSha256 = hasher.digest("hex");

    // Multipart limit hit: fastify truncates the stream; do not store a partial file.
    if (file.file.truncated) {
      try {
        fs.unlinkSync(abs);
      } catch {}
      return reply.code(413).send({ error: "file_too_large" });
    }

    // De-duplication: if this user already uploaded identical content as the same
    // kind, reuse that media row and drop the temp file (works for local and S3
    // storage since the existing row already points at the stored object).
    try {
      const { rows: dup } = await pool.query(
        `SELECT id, kind, mime, size_bytes, path, thumb_path, duration_sec, width, height, created_at, s3_path
       FROM media WHERE owner_id=$1 AND kind=$2 AND sha256=$3 ORDER BY created_at ASC LIMIT 1`,
        [me.id, kind, contentSha256],
      );
      if (dup.length) {
        const existing = dup[0];
        const stillThere =
          existing.s3_path ||
          fs.existsSync(path.join(UPLOAD_DIR, existing.path));
        if (stillThere) {
          try {
            fs.unlinkSync(abs);
          } catch {}
          return existing;
        }
      }
    } catch (e) {
      log.warn("Upload dedup lookup failed", e);
    }

    let finalPath = rel;
    let finalMime = file.mimetype || "application/octet-stream";
    let finalSize = size;
    let thumbPath = null;
    let duration = null;
    let width = null;
    let height = null;

    // Convert voice to M4A for iOS
    if (kind === "voice" && [".webm", ".ogg", ".oga"].includes(ext)) {
      try {
        const m4aPath = path.join(UPLOAD_DIR, `${id}.m4a`);
        await convertToM4A(abs, m4aPath);
        fs.unlinkSync(abs);
        finalPath = `${id}.m4a`;
        finalMime = "audio/mp4";
        finalSize = fs.statSync(m4aPath).size;
        // Check max duration: 3 minutes (180 seconds) for voice
        const info = await getMediaInfo(m4aPath);
        if (info.duration && info.duration > 180) {
          fs.unlinkSync(m4aPath);
          return reply
            .code(400)
            .send({ error: "voice_too_long", max_seconds: 180 });
        }
        duration = info.duration;
      } catch (e) {
        log.error("Audio conversion failed", e);
      }
    }

    // Check duration for already-M4A voice messages (iOS)
    if (kind === "voice" && ext === ".m4a") {
      try {
        const info = await getMediaInfo(abs);
        if (info.duration && info.duration > 180) {
          fs.unlinkSync(abs);
          return reply
            .code(400)
            .send({ error: "voice_too_long", max_seconds: 180 });
        }
        duration = info.duration;
      } catch (e) {
        log.error("Voice duration check failed", e);
      }
    }

    // Convert video_note to circular MP4
    if (kind === "video_note") {
      try {
        const mp4Path = path.join(UPLOAD_DIR, `${id}_vn.mp4`);
        const tPath = path.join(UPLOAD_DIR, `${id}_thumb.jpg`);
        await convertVideoNote(abs, mp4Path, tPath);
        fs.unlinkSync(abs);
        finalPath = `${id}_vn.mp4`;
        finalMime = "video/mp4";
        finalSize = fs.statSync(mp4Path).size;
        if (fs.existsSync(tPath)) thumbPath = `${id}_thumb.jpg`;
        const info = await getMediaInfo(mp4Path);
        duration = info.duration;
        width = info.width;
        height = info.height;
        // Check max duration: 60 seconds for video notes
        if (duration && duration > 60) {
          fs.unlinkSync(mp4Path);
          if (thumbPath && fs.existsSync(path.join(UPLOAD_DIR, thumbPath)))
            fs.unlinkSync(path.join(UPLOAD_DIR, thumbPath));
          return reply
            .code(400)
            .send({ error: "video_note_too_long", max_seconds: 60 });
        }
      } catch (e) {
        log.error("Video note conversion failed", e);
      }
    }

    // Create thumbnail for images
    if (kind === "image") {
      try {
        const tPath = path.join(UPLOAD_DIR, `${id}_thumb.jpg`);
        await createImageThumb(abs, tPath);
        if (fs.existsSync(tPath)) thumbPath = `${id}_thumb.jpg`;
        const info = await getMediaInfo(abs);
        width = info.width;
        height = info.height;
      } catch (e) {
        log.error("Image thumb failed", e);
      }
    }

    // Get video info
    if (kind === "video") {
      try {
        const info = await getMediaInfo(abs);
        duration = info.duration;
        width = info.width;
        height = info.height;
        // Create thumbnail
        const tPath = path.join(UPLOAD_DIR, `${id}_thumb.jpg`);
        const ffmpeg = spawn("ffmpeg", [
          "-i",
          abs,
          "-vframes",
          "1",
          "-vf",
          "scale=320:-1",
          "-y",
          tPath,
        ]);
        await new Promise((r) => ffmpeg.on("close", r));
        if (fs.existsSync(tPath)) thumbPath = `${id}_thumb.jpg`;
      } catch (e) {
        log.error("Video info failed", e);
      }
    }

    // Upload to S3 if enabled
    let s3Path = null;
    let s3ThumbPath = null;
    let localPath = finalPath;
    let localThumbPath = thumbPath;

    if (useS3Storage) {
      try {
        const filePath = path.join(UPLOAD_DIR, finalPath);
        s3Path = await uploadToS3(getS3Key(id, finalPath), filePath, finalMime);
        // Delete local file after S3 upload
        fs.unlinkSync(filePath);
        localPath = null; // File is on S3, no local path needed

        // Upload thumbnail if exists
        if (thumbPath) {
          const thumbFullPath = path.join(UPLOAD_DIR, thumbPath);
          s3ThumbPath = await uploadToS3(
            getS3Key(id, thumbPath),
            thumbFullPath,
            "image/jpeg",
          );
          fs.unlinkSync(thumbFullPath);
          localThumbPath = null;
        }
      } catch (e) {
        log.error("S3 upload failed", e);
        // Continue with local storage if S3 fails - keep localPath as is
      }
    }

    const { rows } = await pool.query(
      `INSERT INTO media(owner_id, kind, mime, size_bytes, path, thumb_path, duration_sec, width, height, s3_path, sha256) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id, kind, mime, size_bytes, path, thumb_path, duration_sec, width, height, created_at, s3_path`,
      [
        me.id,
        kind,
        finalMime,
        finalSize,
        localPath ?? finalPath,
        localThumbPath,
        duration,
        width,
        height,
        s3Path,
        contentSha256,
      ],
    );

    return rows[0];
  },
);

app.get("/media/:id", async (req, reply) => {
  const me = await requireAuth(req);
  const mediaId = String(req.params.id);

  const { rows } = await pool.query(`SELECT * FROM media WHERE id=$1 LIMIT 1`, [
    mediaId,
  ]);
  if (!rows.length) return reply.code(404).send({ error: "not_found" });

  const m = rows[0];
  // Check access: owner, avatar user, avatar room, or room member with message
  if (m.owner_id !== me.id && m.kind !== "avatar") {
    const { rows: ok } = await pool.query(
      `SELECT 1 FROM messages msg JOIN room_members rm ON rm.room_id = msg.room_id WHERE msg.media_id=$1 AND rm.user_id=$2 LIMIT 1`,
      [mediaId, me.id],
    );
    if (!ok.length) return reply.code(403).send({ error: "forbidden" });
  }

  // If S3 path exists, redirect to S3
  if (m.s3_path) {
    return reply.redirect(m.s3_path);
  }

  const abs = path.join(UPLOAD_DIR, m.path);
  if (!fs.existsSync(abs))
    return reply.code(404).send({ error: "file_missing" });

  const stat = fs.statSync(abs);
  const range = req.headers.range;

  // Add caching headers
  const maxAge = m.kind === "avatar" ? 86400 : 3600; // 24h for avatars, 1h for others
  reply.header("Cache-Control", "public, max-age=" + maxAge);
  reply.header(
    "ETag",
    '"' + mediaId + "-" + stat.size + "-" + stat.mtime.getTime() + '"',
  );
  reply.header("X-Content-Type-Options", "nosniff");
  if (m.kind === "file")
    reply.header(
      "Content-Disposition",
      `attachment; filename="${path.basename(m.path)}"`,
    );

  if (range) {
    const parts = range.replace(/bytes=/, "").split("-");
    const start = parseInt(parts[0], 10);
    const end = parts[1] ? parseInt(parts[1], 10) : stat.size - 1;
    const chunksize = end - start + 1;

    reply.header(
      "Content-Range",
      "bytes " + start + "-" + end + "/" + stat.size,
    );
    reply.header("Accept-Ranges", "bytes");
    reply.header("Content-Length", chunksize);
    reply.header("Content-Type", m.mime);
    reply.code(206);
    return reply.send(fs.createReadStream(abs, { start, end }));
  }

  reply.header("Accept-Ranges", "bytes");
  reply.header("Content-Length", stat.size);
  reply.header("Content-Type", m.mime);
  return reply.send(fs.createReadStream(abs));
});

app.get("/media/:id/thumb", async (req, reply) => {
  const mediaId = String(req.params.id);
  const { rows } = await pool.query(
    `SELECT thumb_path, s3_path FROM media WHERE id=$1 AND (thumb_path IS NOT NULL OR s3_path IS NOT NULL) LIMIT 1`,
    [mediaId],
  );
  if (!rows.length) return reply.code(404).send({ error: "not_found" });

  const m = rows[0];

  // If S3 path exists, construct thumb URL from S3 path
  if (m.s3_path) {
    const thumbS3Path = m.s3_path.replace(/\/[^\/]+$/, "/thumb.jpg");
    return reply.redirect(thumbS3Path);
  }

  const abs = path.join(UPLOAD_DIR, m.thumb_path);
  if (!fs.existsSync(abs))
    return reply.code(404).send({ error: "file_missing" });

  reply.header("Content-Type", "image/jpeg");
  reply.header("Cache-Control", "public, max-age=31536000");
  reply.header("X-Content-Type-Options", "nosniff");
  return reply.send(fs.createReadStream(abs));
});

// --- Posts and Comments ---
app.post("/posts/:postId/comments", async (req, reply) => {
  const me = await requireAuth(req);
  const postId = String(req.params.postId);

  // Check if post exists and is a post in a channel
  const { rows: postCheck } = await pool.query(
    `
    SELECT m.id, m.room_id, r.kind as room_kind
    FROM messages m
    JOIN rooms r ON r.id = m.room_id
    WHERE m.id=$1 AND m.is_post=true AND r.kind='channel'
    LIMIT 1
  `,
    [postId],
  );

  if (!postCheck[0]) return reply.code(404).send({ error: "post_not_found" });

  const channelId = postCheck[0].room_id;

  // Check if user is member of channel
  const isMember = await isRoomMember(channelId, me.id);
  if (!isMember) return reply.code(403).send({ error: "not_member" });

  const message = req.body && typeof req.body === "object" ? req.body : null;
  const type = String(message?.type || "text");
  const text = message?.text || "";

  if (!text || !text.trim())
    return reply.code(400).send({ error: "empty_comment" });

  // Create comment message
  const { rows: commentRows } = await pool.query(
    `INSERT INTO messages(room_id, sender_id, type, text, meta, reply_to, is_post)
     VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,false) RETURNING id`,
    [channelId, me.id, type, text, JSON.stringify({}), null],
  );

  const commentId = commentRows[0].id;

  // Link comment to post
  await pool.query(
    `INSERT INTO post_comments(post_id, comment_id) VALUES ($1,$2) ON CONFLICT (post_id, comment_id) DO NOTHING`,
    [postId, commentId],
  );

  // Update comment count
  await pool.query(
    `UPDATE messages SET comment_count = comment_count + 1 WHERE id=$1`,
    [postId],
  );

  // Get full comment data
  const { rows: fullComment } = await pool.query(
    `
    SELECT m.id, m.room_id, m.sender_id, u.username as sender_username, u.display_name as sender_display_name, u.avatar_id as sender_avatar_id,
           m.type, m.text, m.media_id, m.meta, m.created_at, m.reply_to, m.is_post, m.comment_count,
           md.kind as media_kind, md.mime as media_mime, md.size_bytes as media_size, md.path as media_path, md.thumb_path, md.duration_sec, md.width, md.height
    FROM messages m JOIN users u ON u.id = m.sender_id LEFT JOIN media md ON md.id = m.media_id WHERE m.id=$1 LIMIT 1
  `,
    [commentId],
  );

  // Broadcast to channel
  broadcastToRoom(channelId, {
    type: "message.new",
    message: { ...fullComment[0], reactions: {} },
  });
  await notifyRoomListUpdateForRoom(channelId);

  // Update post comment count for all viewers
  broadcastToRoom(channelId, {
    type: "post.comment_count",
    post_id: postId,
    comment_count: (
      await pool.query(`SELECT comment_count FROM messages WHERE id=$1`, [
        postId,
      ])
    ).rows[0].comment_count,
  });

  return { ok: true, comment: { ...fullComment[0], reactions: {} } };
});

app.get("/posts/:postId/comments", async (req, reply) => {
  const me = await requireAuth(req);
  const postId = String(req.params.postId);

  // Check if post exists and user has access
  const { rows: postCheck } = await pool.query(
    `
    SELECT m.id, m.room_id, r.kind as room_kind
    FROM messages m
    JOIN rooms r ON r.id = m.room_id
    WHERE m.id=$1 AND m.is_post=true AND r.kind='channel'
    LIMIT 1
  `,
    [postId],
  );

  if (!postCheck[0]) return reply.code(404).send({ error: "post_not_found" });

  const channelId = postCheck[0].room_id;

  // Check if user is member of channel
  const isMember = await isRoomMember(channelId, me.id);
  if (!isMember) return reply.code(403).send({ error: "not_member" });

  // Get comments
  const { rows: comments } = await pool.query(
    `
    SELECT m.id, m.room_id, m.sender_id, u.username as sender_username, u.display_name as sender_display_name, u.avatar_id as sender_avatar_id,
           m.type, m.text, m.media_id, m.meta, m.created_at, m.reply_to, m.is_post, m.comment_count,
           md.kind as media_kind, md.mime as media_mime, md.size_bytes as media_size, md.path as media_path, md.thumb_path, md.duration_sec, md.width, md.height
    FROM post_comments pc
    JOIN messages m ON m.id = pc.comment_id
    JOIN users u ON u.id = m.sender_id
    LEFT JOIN media md ON md.id = m.media_id
    WHERE pc.post_id=$1
    ORDER BY m.created_at ASC
    LIMIT 100
  `,
    [postId],
  );

  // Add reactions to each comment
  const commentsWithReactions = [];
  for (const comment of comments) {
    const { rows: reactions } = await pool.query(
      `SELECT emoji, user_id FROM reactions WHERE message_id=$1`,
      [comment.id],
    );

    const grouped = {};
    for (const r of reactions) {
      if (!grouped[r.emoji]) grouped[r.emoji] = [];
      grouped[r.emoji].push(r.user_id);
    }

    commentsWithReactions.push({ ...comment, reactions: grouped });
  }

  return commentsWithReactions;
});

// --- WebSocket ---
app.get("/ws", { websocket: true }, (socket, req) => {
  let me = null;
  installSocketSession(socket, req, {
    authenticate: requireAuth,
    onAuthenticated(user) { me = user; addSocket(user.id, socket); },
    async onMessage(msg) {
      const t = String(msg.type || "");
      try {
      if (t === "rooms.join") {
        const roomId = String(msg.room_id || msg.roomId || "");
        const ok = await isRoomMember(roomId, me.id);
        if (!ok) {
          socket.send(
            JSON.stringify({ type: "rooms.join.denied", room_id: roomId }),
          );
          return;
        }
        joinRoom(roomId, socket);
        socket.send(JSON.stringify({ type: "rooms.join.ok", room_id: roomId }));
        return;
      }

      if (t === "typing") {
        const roomId = String(msg.room_id || "");
        const isTyping = !!msg.is_typing;
        if (!roomId) return;
        const ok = await isRoomMember(roomId, me.id);
        if (!ok) return;
        const { rows } = await pool.query(
          `SELECT user_id FROM room_members WHERE room_id=$1 AND user_id <> $2`,
          [roomId, me.id],
        );
        for (const r of rows) {
          sendToUser(String(r.user_id), {
            type: "typing",
            room_id: roomId,
            user_id: me.id,
            username: me.username,
            is_typing: isTyping,
          });
        }
        return;
      }

      if (t === "message.send") {
        const roomId = String(msg.room_id || msg.roomId || "");
        const ok = await isRoomMember(roomId, me.id);
        if (!ok) {
          socket.send(JSON.stringify({ type: "error", error: "not_found" }));
          return;
        }

        // Check if room is channel and user can write (owner or admin only)
        const { rows: roomCheck } = await pool.query(
          `SELECT kind FROM rooms WHERE id=$1 LIMIT 1`,
          [roomId],
        );
        const dmPeerId = await getDmPeerUserId(roomId, me.id);
        if (dmPeerId && !(await canUsersExchangeDm(me.id, dmPeerId))) {
          socket.send(JSON.stringify({ type: "error", error: "blocked" }));
          return;
        }
        const message =
          msg.message && typeof msg.message === "object" ? msg.message : null;
        const type = String(
          message?.type ||
            (msg.kind === "voice"
              ? "voice"
              : msg.kind === "video_note"
                ? "video_note"
                : msg.kind === "media"
                  ? "media"
                  : "text"),
        );

        if (roomCheck[0]?.kind === "channel") {
          // For all messages in channels, check admin rights
          const { rows: roleCheck } = await pool.query(
            `SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2 LIMIT 1`,
            [roomId, me.id],
          );
          const role = roleCheck[0]?.role;
          if (role !== "owner" && role !== "admin") {
            socket.send(
              JSON.stringify({ type: "error", error: "channel_read_only" }),
            );
            return;
          }
          // Admins can send: text, media, voice, video_note
        }
        const text =
          (message?.text ?? msg.text) == null
            ? null
            : String(message?.text ?? msg.text);
        const rawMediaId =
          message?.media_id ?? msg.media_id ?? msg.mediaId ?? null;
        const media_id = rawMediaId ? String(rawMediaId) : null;
        const metaRaw = message?.meta ?? msg.meta ?? {};
        const meta = metaRaw && typeof metaRaw === "object" ? metaRaw : {};
        const reply_to = message?.reply_to ?? null;
        const encrypted_content =
          message?.encrypted_content ?? msg.encrypted_content ?? null;

        if (!["text", "media", "voice", "video_note", "call"].includes(type))
          return;
        if (type === "text" && !encrypted_content && (!text || !text.trim()))
          return;
        if (
          type === "text" &&
          !encrypted_content &&
          (await isGroupE2eEnabled(roomId))
        ) {
          socket.send(JSON.stringify({ type: "error", error: "e2e_required" }));
          return;
        }
        if (["media", "voice", "video_note"].includes(type) && !media_id)
          return;

        const clientId =
          message?.client_message_id ?? msg.client_message_id ?? null;
        if (clientId && !/^[a-zA-Z0-9_-]{8,100}$/.test(String(clientId))) {
          socket.send(
            JSON.stringify({ type: "error", error: "bad_client_message_id" }),
          );
          return;
        }
        const { rows } = await pool.query(
          `INSERT INTO messages(room_id, sender_id, type, text, media_id, meta, reply_to, is_post, encrypted_content, client_message_id) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9,$10) ON CONFLICT(sender_id,client_message_id) DO NOTHING RETURNING id`,
          [
            roomId,
            me.id,
            type,
            encrypted_content ? null : text,
            media_id,
            JSON.stringify(meta),
            reply_to ? JSON.stringify(reply_to) : null,
            roomCheck[0]?.kind === "channel",
            encrypted_content,
            clientId,
          ],
        );
        if (!rows.length) {
          const { rows: prior } = await pool.query(
            "SELECT id,room_id FROM messages WHERE sender_id=$1 AND client_message_id=$2",
            [me.id, clientId],
          );
          if (prior[0]?.room_id !== roomId) {
            socket.send(
              JSON.stringify({
                type: "error",
                error: "client_message_id_conflict",
              }),
            );
            return;
          }
          socket.send(
            JSON.stringify({
              type: "message.ack",
              message: await getMobileMessage(pool, prior[0].id, decryptMetadata),
            }),
          );
          return;
        }

        const { rows: full } = await pool.query(
          `
          SELECT m.id, m.room_id, m.sender_id, u.username as sender_username, u.display_name as sender_display_name, u.avatar_id as sender_avatar_id,
                 m.type, m.text, m.media_id, m.meta, m.created_at, m.reply_to, m.is_post, m.comment_count, m.encrypted_content, m.client_message_id, m.edited_at,
                 md.kind as media_kind, md.mime as media_mime, md.size_bytes as media_size, md.path as media_path, md.thumb_path, md.duration_sec, md.width, md.height
          FROM messages m JOIN users u ON u.id = m.sender_id LEFT JOIN media md ON md.id = m.media_id WHERE m.id=$1 LIMIT 1
        `,
          [rows[0].id],
        );

        broadcastToRoom(roomId, {
          type: "message.new",
          message: { ...full[0], reactions: {} },
        });
        await notifyRoomListUpdateForRoom(roomId);
        const { rows: recipients } = await pool.query(
          "SELECT user_id FROM room_members WHERE room_id=$1 AND user_id<>$2",
          [roomId, me.id],
        );
        for (const recipient of recipients)
          void sendPushNotification(
            recipient.user_id,
            "Messenger67",
            "Новое сообщение",
            { room_id: roomId, message_id: full[0].id },
          );
        return;
      }

      // Handle reactions
      if (t === "reaction.add") {
        const messageId = String(msg.message_id || "");
        const emoji = msg.emoji;
        if (!messageId || !emoji) return;

        const { rows: msgCheck } = await pool.query(
          `SELECT m.id, m.room_id FROM messages m JOIN room_members rm ON rm.room_id = m.room_id WHERE m.id=$1 AND rm.user_id=$2 LIMIT 1`,
          [messageId, me.id],
        );
        if (!msgCheck[0]) return;

        await pool.query(
          `INSERT INTO reactions(message_id, user_id, emoji) VALUES ($1,$2,$3) ON CONFLICT (message_id, user_id) DO UPDATE SET emoji = EXCLUDED.emoji, created_at = now()`,
          [messageId, me.id, emoji],
        );
        broadcastToRoom(msgCheck[0].room_id, {
          type: "reaction.add",
          message_id: messageId,
          user_id: me.id,
          emoji,
        });
        return;
      }

      if (t === "reaction.remove") {
        const messageId = String(msg.message_id || "");
        const emoji = msg.emoji;
        if (!messageId || !emoji) return;

        const { rows: msgCheck } = await pool.query(
          `SELECT m.id, m.room_id FROM messages m JOIN room_members rm ON rm.room_id = m.room_id WHERE m.id=$1 AND rm.user_id=$2 LIMIT 1`,
          [messageId, me.id],
        );
        if (!msgCheck[0]) return;

        await pool.query(
          `DELETE FROM reactions WHERE message_id=$1 AND user_id=$2 AND emoji=$3`,
          [messageId, me.id, emoji],
        );
        broadcastToRoom(msgCheck[0].room_id, {
          type: "reaction.remove",
          message_id: messageId,
          user_id: me.id,
          emoji,
        });
        return;
      }

      if (
        t === "call.offer" ||
        t === "call.answer" ||
        t === "call.ice" ||
        t === "call.hangup"
      ) {
        await relayCall(msg, me);
        return;
      }
      } catch (e) {
        log.error("WebSocket message error", e);
        socket.send(JSON.stringify({ type: "error", error: "server_error" }));
      }
    },
    onClose(user) { leaveAllRooms(socket); if (user?.id) removeSocket(user.id, socket); },
    onError(error) { log.error("WebSocket session error", error); },
  });
});

// --- E2E Encryption Key Exchange API ---
// Small in-memory sliding-window limiter keyed per user (the global
// @fastify/rate-limit plugin is IP-based and 'global: false').
const e2eUploadHits = new Map();
function hitUserLimit(bucket, userId, max, windowMs) {
  const key = `${bucket}:${userId}`;
  const now = Date.now();
  const hits = (e2eUploadHits.get(key) || []).filter((t) => now - t < windowMs);
  if (hits.length >= max) {
    e2eUploadHits.set(key, hits);
    return Math.ceil((windowMs - (now - hits[0])) / 1000);
  }
  hits.push(now);
  e2eUploadHits.set(key, hits);
  return 0;
}
setInterval(
  () => {
    const now = Date.now();
    for (const [key, hits] of e2eUploadHits) {
      if (!hits.length || now - hits[hits.length - 1] > 2 * 60 * 60 * 1000)
        e2eUploadHits.delete(key);
    }
  },
  10 * 60 * 1000,
).unref();

app.post(
  "/e2e/keys",
  {
    config: { rateLimit: { max: 60, timeWindow: "1 hour" } },
  },
  async (req, reply) => {
    const me = await requireAuth(req);
    const body = req.body || {};
    const deviceId = String(body.device_id || "1");
    const identityKey = String(body.identity_key || "");
    const signedPreKey = body.signed_pre_key || {
      key_id: body.signed_pre_key_id,
      public_key: body.signed_pre_key_public,
      signature: body.signed_pre_key_signature,
    };
    const signedPreKeyId = signedPreKey.key_id || signedPreKey.keyId || 1;
    const signedPreKeyPublic =
      signedPreKey.public_key || signedPreKey.publicKey || "";
    const signedPreKeySignature = signedPreKey.signature || "";
    const preKeys = Array.isArray(body.pre_keys) ? body.pre_keys : [];
    const registrationId = Number.isFinite(Number(body.registration_id))
      ? Math.trunc(Number(body.registration_id)) & 0x3fff
      : null;

    if (!identityKey || !signedPreKeyPublic) {
      return reply.code(400).send({ error: "missing_keys" });
    }
    if (!signedPreKeySignature) {
      return reply.code(400).send({ error: "missing_signed_prekey_signature" });
    }

    // Payload size sanity checks (all key material is short base64 text).
    if (deviceId.length > MAX_E2E_DEVICE_ID_CHARS) {
      return reply.code(400).send({ error: "device_id_too_long" });
    }
    if (
      identityKey.length > MAX_E2E_KEY_CHARS ||
      String(signedPreKeyPublic).length > MAX_E2E_KEY_CHARS ||
      String(signedPreKeySignature).length > MAX_E2E_KEY_CHARS
    ) {
      return reply.code(413).send({ error: "key_too_large" });
    }
    if (preKeys.length > MAX_E2E_PREKEYS_PER_UPLOAD) {
      return reply
        .code(413)
        .send({ error: "too_many_prekeys", max: MAX_E2E_PREKEYS_PER_UPLOAD });
    }
    for (const pk of preKeys) {
      const pub = pk?.public_key ?? pk?.publicKey;
      if (pub != null && String(pub).length > MAX_E2E_KEY_CHARS) {
        return reply.code(413).send({ error: "key_too_large" });
      }
    }

    // Rate limits (per user). Re-uploading a changed identity key is rare and
    // security-relevant (it resets trust for every peer), so it is limited tightly;
    // prekey-only replenishment (same identity key) gets a looser budget.
    const { rows: existingIdentity } = await pool.query(
      `SELECT public_key FROM user_identity_keys WHERE user_id=$1 LIMIT 1`,
      [me.id],
    );
    const identityChanged =
      !existingIdentity.length ||
      existingIdentity[0].public_key !== identityKey;
    const retryAfter = identityChanged
      ? hitUserLimit("e2e-identity", me.id, 10, 60 * 60 * 1000)
      : hitUserLimit("e2e-prekeys", me.id, 30, 60 * 60 * 1000);
    if (retryAfter) {
      reply.header("Retry-After", String(retryAfter));
      return reply
        .code(429)
        .send({ error: "rate_limited", retry_after: retryAfter });
    }

    // Store identity key
    await pool.query(
      `INSERT INTO user_identity_keys(user_id, public_key, updated_at) VALUES ($1, $2, now())
     ON CONFLICT (user_id) DO UPDATE SET public_key = EXCLUDED.public_key, updated_at = now()`,
      [me.id, identityKey],
    );

    // Store device
    const { rows: deviceRows } = await pool.query(
      `INSERT INTO user_devices(user_id, device_id, public_key, signed_pre_key_id, signed_pre_key_public, signed_pre_key_signature, registration_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (user_id, device_id) DO UPDATE
     SET public_key = EXCLUDED.public_key,
         signed_pre_key_id = EXCLUDED.signed_pre_key_id,
         signed_pre_key_public = EXCLUDED.signed_pre_key_public,
         signed_pre_key_signature = EXCLUDED.signed_pre_key_signature,
         registration_id = EXCLUDED.registration_id
     RETURNING id`,
      [
        me.id,
        deviceId,
        identityKey,
        signedPreKeyId,
        signedPreKeyPublic,
        signedPreKeySignature,
        registrationId,
      ],
    );

    const dbDeviceId = deviceRows[0].id;

    // Store pre-keys. Use ?? (not ||) so key_id = 0 is preserved — 0 is falsy and
    // the old `||` fallback silently dropped the first pre-key into a NOT NULL error.
    for (const preKey of preKeys.slice(0, MAX_E2E_PREKEYS_PER_UPLOAD)) {
      const keyId = preKey.key_id ?? preKey.keyId;
      const publicKey = preKey.public_key ?? preKey.publicKey;
      if (keyId == null || !publicKey) continue;
      await pool.query(
        `INSERT INTO user_prekeys(device_id, key_id, public_key) VALUES ($1, $2, $3)
       ON CONFLICT (device_id, key_id) DO NOTHING`,
        [dbDeviceId, keyId, publicKey],
      );
    }

    return { ok: true };
  },
);

// Share at least one room (dm/group/channel) with the target user.
// Without this, any authenticated user could enumerate key bundles and burn
// one-time pre-keys of arbitrary accounts (DoS on their sessions).
async function sharesRoomWith(viewerId, targetId) {
  const { rows } = await pool.query(
    `SELECT 1 FROM room_members a
     JOIN room_members b ON b.room_id = a.room_id
     WHERE a.user_id = $1 AND b.user_id = $2
     LIMIT 1`,
    [viewerId, targetId],
  );
  return rows.length > 0;
}

app.get('/e2e/devices/:userId', async (req, reply) => {
  const me = await requireAuth(req), target = String(req.params.userId);
  if (target !== String(me.id) && !(await sharesRoomWith(me.id, target))) return reply.code(404).send({error:'keys_not_found'});
  const {rows} = await pool.query(`SELECT device_id FROM user_devices WHERE user_id=$1 AND device_id ~ '^[0-9]+$' AND length(public_key)=44 AND public_key NOT LIKE '%=%' AND length(signed_pre_key_signature)=88 ORDER BY created_at DESC LIMIT 10`,[target]);
  return rows;
});

app.get(
  "/e2e/keys/:userId",
  {
    config: { rateLimit: { max: 30, timeWindow: "1 minute" } },
  },
  async (req, reply) => {
    const me = await requireAuth(req);
    const targetUserId = String(req.params.userId);

        if (targetUserId !== String(me.id) && !(await sharesRoomWith(me.id, targetUserId))) {
      // Same error as "no keys" so the endpoint can't be used to probe who exists.
      return reply.code(404).send({ error: "keys_not_found" });
    }

    // Get identity key
    const { rows: identityRows } = await pool.query(
      `SELECT public_key FROM user_identity_keys WHERE user_id=$1 LIMIT 1`,
      [targetUserId],
    );

    if (!identityRows.length) {
      return reply.code(404).send({ error: "keys_not_found" });
    }

    // Get device and signed pre-key
    const { rows: deviceRows } = await pool.query(
      `SELECT id, device_id, public_key, signed_pre_key_id, signed_pre_key_public, signed_pre_key_signature, registration_id
     FROM user_devices WHERE user_id=$1 AND ($2::text IS NULL OR device_id=$2) ORDER BY created_at DESC LIMIT 1`,
      [targetUserId, req.query.device_id ? String(req.query.device_id) : null],
    );

    if (!deviceRows.length) {
      return reply.code(404).send({ error: "device_not_found" });
    }

    const device = deviceRows[0];

    // Atomically claim one unused pre-key. The UPDATE ... RETURNING with
    // FOR UPDATE SKIP LOCKED guarantees two concurrent consumers never receive
    // the same one-time key (which would break X3DH forward secrecy).
    const { rows: claimed } = await pool.query(
      `UPDATE user_prekeys SET used=true
     WHERE id = (
       SELECT id FROM user_prekeys
       WHERE device_id=$1 AND used=false
       ORDER BY created_at ASC
       LIMIT 1
       FOR UPDATE SKIP LOCKED
     )
     RETURNING key_id, public_key`,
      [device.id],
    );

    const preKey = claimed.length
      ? { keyId: claimed[0].key_id, publicKey: claimed[0].public_key }
      : null;

    return {
      user_id: targetUserId,
      device_id: device.device_id,
      identity_key: device.public_key,
      registration_id: device.registration_id ?? 0,
      signed_pre_key: {
        keyId: device.signed_pre_key_id,
        publicKey: device.signed_pre_key_public,
        signature: device.signed_pre_key_signature,
      },
      pre_key: preKey,
    };
  },
);

// Report how many unused pre-keys a device still has, so the client can top
// them up before running dry (a depleted bundle forces fresh session builds
// without a one-time key, weakening forward secrecy).
app.get("/e2e/prekeys/count", async (req, reply) => {
  const me = await requireAuth(req);
  const deviceId = String(req.query.device_id || "1");
  const { rows } = await pool.query(
    `SELECT COUNT(*)::int AS n FROM user_prekeys p
     JOIN user_devices d ON d.id = p.device_id
     WHERE d.user_id=$1 AND d.device_id=$2 AND p.used=false`,
    [me.id, deviceId],
  );
  return { device_id: deviceId, unused: rows[0]?.n || 0 };
});

await installMobileRoutes(app, {
  decryptMetadata,
  pool,
  requireAuth,
  isRoomMember,
  getRoomRole,
  notifyRoomListUpdate,
  broadcastToRoom,
});

// Start server
await installWebPush(app, { pool, requireAuth, isRoomMember, logger: log });
installNativePush(app, { pool, requireAuth, logger: log });
installAppRelease(app, { logger: log, dir: process.env.APP_RELEASE_DIR || "/app/releases" });
const start = async () => {
  try {
    await app.listen({ host: "0.0.0.0", port: Number(PORT) });
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
};

start();

// Graceful shutdown
let shuttingDown = false;
for (const sig of ["SIGTERM", "SIGINT"]) {
  process.on(sig, async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    try {
      await app.close();
      await pool.end();
    } catch (e) {
      app.log.error(e);
    }
    process.exit(0);
  });
}
