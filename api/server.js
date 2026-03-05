import Fastify from "fastify";
import cors from "@fastify/cors";
import cookie from "@fastify/cookie";
import multipart from "@fastify/multipart";
import websocket from "@fastify/websocket";
import argon2 from "argon2";
import crypto from "crypto";
import fs from "fs";
import path from "path";
import { Pool } from "pg";
import { spawn } from "child_process";
import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

const {
  DATABASE_URL,
  COOKIE_SECRET = "",
  RETURN_TOKEN = "false",
  HCAPTCHA_SECRET = "",
  NODE_ENV = "production",
  PORT = "3000",
  ORIGIN = "https://67b67t.ru",
  TOKEN_TTL_DAYS = "30",
  COOKIE_SECURE = "auto",
  UPLOAD_DIR = "/app/uploads",
  S3_ENDPOINT = "https://s3.twcstorage.ru",
  S3_BUCKET = "07926a34-8ef1-41d3-a318-583c00dae033",
  S3_ACCESS_KEY = "QFUZXU5JKWW8771QDQXG",
  S3_SECRET_KEY = "MQdCxzBsW1DfDkqiDxph31Lh4jyiohFonKmnCcYm",
  S3_REGION = "ru-1",
  SWIFT_ENDPOINT = "https://swift.twcstorage.ru",
  SWIFT_ACCESS_KEY = "fg636452:swift",
  SWIFT_SECRET_KEY = "3OcyOTleGW0d3UKS6bhYUm9g46UYb6KXurvcM0U5"
} = process.env;

if (!DATABASE_URL) throw new Error("DATABASE_URL is required");

fs.mkdirSync("/app/uploads", { recursive: true });

const pool = new Pool({ connectionString: DATABASE_URL });

// S3 Client
const s3Client = new S3Client({
  endpoint: S3_ENDPOINT,
  region: S3_REGION,
  credentials: {
    accessKeyId: S3_ACCESS_KEY,
    secretAccessKey: S3_SECRET_KEY
  },
  forcePathStyle: true
});

const USE_S3 = false; // Disabled - use local storage

// S3 Helper functions
async function uploadToS3(key, filePath, contentType) {
  const fileStream = fs.createReadStream(filePath);
  const command = new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: key,
    Body: fileStream,
    ContentType: contentType
  });
  await s3Client.send(command);
  return S3_ENDPOINT + '/' + S3_BUCKET + '/' + key;
}

async function deleteFromS3(key) {
  const command = new DeleteObjectCommand({
    Bucket: S3_BUCKET,
    Key: key
  });
  await s3Client.send(command);
}

function getS3Key(mediaId, filename) {
  return 'media/' + mediaId + '/' + filename;
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
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen timestamptz`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name text`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_id uuid`,
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
    `ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to jsonb`,
    `CREATE TABLE IF NOT EXISTS reactions (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), message_id uuid REFERENCES messages(id) ON DELETE CASCADE, user_id uuid REFERENCES users(id) ON DELETE CASCADE, emoji text NOT NULL, created_at timestamptz DEFAULT now(), UNIQUE(message_id, user_id))`,
    // Migration: change unique constraint from (message_id, user_id, emoji) to (message_id, user_id)
    `ALTER TABLE reactions DROP CONSTRAINT IF EXISTS reactions_message_id_user_id_key`,
    `ALTER TABLE reactions DROP CONSTRAINT IF EXISTS reactions_message_id_user_id_emoji_key`,
    `ALTER TABLE reactions ADD CONSTRAINT reactions_one_per_user UNIQUE (message_id, user_id)`,
    `ALTER TABLE media ADD COLUMN IF NOT EXISTS s3_path text`,
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
    `UPDATE users SET banned = true WHERE length(username) > 30 OR length(display_name) > 30`,
  ];
  for (const sql of migrations) {
    try { await pool.query(sql); } catch (e) { console.log("Migration note:", e.message); }
  }
}
await runMigrations();

// Create system user Hitler and news channel if not exists
async function setupSystemChannel() {
  try {
    // Check if Hitler exists
    const { rows: hitlerRows } = await pool.query(`SELECT id FROM users WHERE username='Hitler' LIMIT 1`);
    let hitlerId;
    if (hitlerRows.length === 0) {
      const hash = await argon2.hash('hitler123');
      const { rows: newHitler } = await pool.query(
        `INSERT INTO users(username, password_hash, display_name) VALUES('Hitler', $1, 'Администратор') RETURNING id`,
        [hash]
      );
      hitlerId = newHitler[0].id;
      console.log('Created system user: Hitler');
    } else {
      hitlerId = hitlerRows[0].id;
    }

    // Check if news channel exists
    const { rows: channelRows } = await pool.query(`SELECT id FROM rooms WHERE kind='channel' AND title='НОВОСТИ МЕССЕНДЖЕРА' LIMIT 1`);
    if (channelRows.length === 0) {
      const { rows: newChannel } = await pool.query(
        `INSERT INTO rooms(kind, title, created_by) VALUES('channel', 'НОВОСТИ МЕССЕНДЖЕРА', $1) RETURNING id`,
        [hitlerId]
      );
      const channelId = newChannel[0].id;
      
      // Add Hitler as owner
      await pool.query(
        `INSERT INTO room_members(room_id, user_id, role) VALUES($1, $2, 'owner') ON CONFLICT DO NOTHING`,
        [channelId, hitlerId]
      );
      
      console.log('Created system channel: НОВОСТИ МЕССЕНДЖЕРА');
    }
  } catch (e) {
    console.error('Setup system channel error:', e.message);
  }
}
await setupSystemChannel();

const app = Fastify({ logger: true });

await app.register(cors, { origin: ORIGIN === "*" ? true : ORIGIN.split(",").map(s => s.trim()), credentials: true });
await app.register(cookie);
await app.register(multipart, { limits: { fileSize: 50 * 1024 * 1024 } }); // 50MB max file size
await app.register(websocket);

function isHttpsLike(req) {
  const xfProto = String(req.headers?.["x-forwarded-proto"] || "").toLowerCase();
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
  return { httpOnly: true, sameSite: "lax", path: "/", secure: cookieSecureFlag(req), maxAge: ttlDays * 24 * 60 * 60 };
}

function sha256(s) { return crypto.createHash("sha256").update(s).digest("hex"); }
function randToken() { return crypto.randomBytes(32).toString("hex"); }
function roomSecret() { return crypto.randomBytes(32).toString("base64url"); }

// Verify hCaptcha token
async function verifyCaptcha(token) {
  if (!HCAPTCHA_SECRET) return true; // Skip if not configured
  if (!token) return false;
  try {
    const response = await fetch('https://hcaptcha.com/siteverify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ secret: HCAPTCHA_SECRET, response: token })
    });
    const data = await response.json();
    return data.success === true;
  } catch (e) {
    console.error('Captcha verification error:', e);
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
  const publicCode = statusCode >= 500 ? "server_error" : (err.publicCode || "error");
  if (reply.sent) return;
  
  // Log detailed error info
  console.error("API Error:", {
    error: err.message,
    stack: err.stack,
    url: req.url,
    method: req.method,
    body: req.body
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
    const part = raw.split(";").map(s => s.trim()).find(s => s.toLowerCase().startsWith("token="));
    if (part) token = decodeURIComponent(part.slice("token=".length));
  }
  if (!token) throw httpError(401, "unauthorized");

  const th = sha256(token);
  const { rows } = await pool.query(
    `SELECT s.user_id, u.username, u.display_name, u.avatar_id FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token_hash=$1 AND s.expires_at > now() LIMIT 1`,
    [th]
  );
  console.log("requireAuth: token hash:", th, "rows:", rows.length);
  if (!rows.length) throw httpError(401, "unauthorized");

  console.log("requireAuth: user_id:", rows[0].user_id, "type:", typeof rows[0].user_id);
  const user = { id: rows[0].user_id, username: rows[0].username, display_name: rows[0].display_name, avatar_id: rows[0].avatar_id, token };
  pool.query(`UPDATE users SET last_seen=now() WHERE id=$1`, [user.id]).catch(() => {});
  return user;
}

async function isRoomMember(roomId, userId) {
  const { rows } = await pool.query(`SELECT 1 FROM room_members WHERE room_id=$1 AND user_id=$2 LIMIT 1`, [roomId, userId]);
  return rows.length > 0;
}

async function getRoomRole(roomId, userId) {
  const { rows } = await pool.query(`SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2 LIMIT 1`, [roomId, userId]);
  return rows[0]?.role || null;
}

async function ensureRoomSecret(roomId) {
  const { rows } = await pool.query(`SELECT secret FROM room_secrets WHERE room_id=$1 LIMIT 1`, [roomId]);
  if (rows.length) return rows[0].secret;
  const secret = roomSecret();
  await pool.query(`INSERT INTO room_secrets(room_id, secret) VALUES ($1,$2) ON CONFLICT (room_id) DO NOTHING`, [roomId, secret]);
  const { rows: rows2 } = await pool.query(`SELECT secret FROM room_secrets WHERE room_id=$1 LIMIT 1`, [roomId]);
  return rows2[0].secret;
}

// --- FFmpeg helpers ---
async function convertToM4A(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn("ffmpeg", ["-i", inputPath, "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-y", outputPath]);
    ffmpeg.on("close", code => code === 0 ? resolve() : reject(new Error('ffmpeg exit ' + code)));
    ffmpeg.on("error", reject);
  });
}

async function convertVideoNote(inputPath, outputPath, thumbPath) {
  return new Promise((resolve, reject) => {
    // Convert to MP4 with circular crop and create thumbnail
    const ffmpeg = spawn("ffmpeg", [
      "-i", inputPath,
      "-c:v", "libx264", "-preset", "fast", "-crf", "28",
      "-c:a", "aac", "-b:a", "64k",
      "-vf", "crop=min(iw\\,ih):min(iw\\,ih),scale=320:320",
      "-movflags", "+faststart", "-t", "60", "-y", outputPath
    ]);
    ffmpeg.on("close", async code => {
      if (code !== 0) return reject(new Error('ffmpeg exit ' + code));
      // Create thumbnail
      const thumbFfmpeg = spawn("ffmpeg", ["-i", outputPath, "-vframes", "1", "-vf", "scale=160:160", "-y", thumbPath]);
      thumbFfmpeg.on("close", () => resolve());
      thumbFfmpeg.on("error", () => resolve()); // Thumbnail optional
    });
    ffmpeg.on("error", reject);
  });
}

async function createImageThumb(inputPath, thumbPath) {
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn("ffmpeg", ["-i", inputPath, "-vf", "scale=200:-1", "-y", thumbPath]);
    ffmpeg.on("close", () => resolve());
    ffmpeg.on("error", reject);
  });
}

async function getMediaInfo(filePath) {
  return new Promise((resolve) => {
    const ffprobe = spawn("ffprobe", ["-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", filePath]);
    let data = "";
    ffprobe.stdout.on("data", chunk => data += chunk);
    ffprobe.on("close", () => {
      try {
        const info = JSON.parse(data);
        const video = info.streams?.find(s => s.codec_type === "video");
        const duration = parseFloat(info.format?.duration) || 0;
        resolve({ width: video?.width, height: video?.height, duration: Math.round(duration) });
      } catch { resolve({}); }
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
  if (set) { set.delete(ws); if (!set.size) socketsByUser.delete(userId); }
}
function joinRoom(roomId, ws) {
  if (!socketsByRoom.has(roomId)) socketsByRoom.set(roomId, new Set());
  socketsByRoom.get(roomId).add(ws);
}
function leaveAllRooms(ws) { for (const set of socketsByRoom.values()) set.delete(ws); }
function broadcastToRoom(roomId, payload) {
  const set = socketsByRoom.get(roomId);
  if (!set) return;
  const s = JSON.stringify(payload);
  for (const ws of set) { try { ws.send(s); } catch {} }
}
function sendToUser(userId, payload) {
  const set = socketsByUser.get(userId);
  if (!set) return;
  const s = JSON.stringify(payload);
  for (const ws of set) { try { ws.send(s); } catch {} }
}
function isUserOnline(userId) {
  const set = socketsByUser.get(userId);
  return !!(set && set.size);
}

// Check if user is admin (Hitler)
async function isAdmin(userId) {
  const { rows } = await pool.query(`SELECT username FROM users WHERE id=$1 LIMIT 1`, [userId]);
  return rows[0]?.username === 'Hitler';
}

// Send room list update to specific user
async function notifyRoomListUpdate(userId) {
  const { rows } = await pool.query(`
    SELECT r.id, r.kind, r.title, r.avatar_id, r.created_by, r.created_at,
           lm.id as last_msg_id, lm.type as last_msg_type, lm.text as last_msg_text,
           lm.sender_id as last_msg_sender_id, lmu.username as last_msg_sender_username, lmu.display_name as last_msg_sender_name,
           lm.created_at as last_msg_at,
           ou.id as other_user_id, ou.username as other_username, ou.display_name as other_display_name, ou.avatar_id as other_avatar_id
    FROM rooms r
    JOIN room_members rm ON rm.room_id=r.id
    LEFT JOIN LATERAL (
      SELECT m.id, m.type, m.text, m.sender_id, m.created_at
      FROM messages m WHERE m.room_id = r.id ORDER BY m.created_at DESC LIMIT 1
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
  `, [userId]);
  
  sendToUser(userId, { type: "rooms.update", rooms: rows });
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

// --- Auth ---
app.post("/auth/register", async (req, reply) => {
  const body = req.body || {};
  const username = String(body.username || "").trim();
  const password = String(body.password || "");
  const captchaToken = String(body.captcha || "");

  // Verify captcha
  const captchaValid = await verifyCaptcha(captchaToken);
  if (!captchaValid) return reply.code(400).send({ error: "captcha_failed" });

  if (username.length < 3) return reply.code(400).send({ error: "username_too_short" });
  if (username.length > 30) return reply.code(400).send({ error: "username_too_long" });
  if (password.length < 6) return reply.code(400).send({ error: "password_too_short" });

  const password_hash = await argon2.hash(password);

  try {
    const { rows } = await pool.query(
      `INSERT INTO users(username, password_hash) VALUES ($1,$2) RETURNING id, username, created_at`,
      [username, password_hash]
    );
    const userId = rows[0].id;
    const token = randToken();
    const token_hash = sha256(token);
    const ttlDays = Number(TOKEN_TTL_DAYS) || 30;

    await pool.query(
      `INSERT INTO sessions(user_id, token_hash, expires_at) VALUES ($1,$2, now() + ($3::int || ' days')::interval)`,
      [userId, token_hash, ttlDays]
    );

    // Auto-join news channel
    try {
      const { rows: newsChannel } = await pool.query(`SELECT id FROM rooms WHERE kind='channel' AND title='НОВОСТИ МЕССЕНДЖЕРА' LIMIT 1`);
      if (newsChannel.length > 0) {
        await pool.query(
          `INSERT INTO room_members(room_id, user_id, role) VALUES($1, $2, 'member') ON CONFLICT DO NOTHING`,
          [newsChannel[0].id, userId]
        );
      }
    } catch (channelErr) {
      console.error('Failed to add user to news channel:', channelErr.message);
    }

    reply.setCookie("token", token, authCookieOptions(req, ttlDays));
    return { user: rows[0] };
  } catch (e) {
    if (String(e.code) === "23505") return reply.code(409).send({ error: "username_taken" });
    throw e;
  }
});

app.post("/auth/login", async (req, reply) => {
  const body = req.body || {};
  const username = String(body.username || "").trim();
  const password = String(body.password || "");

  // Validate username length
  if (username.length > 30) return reply.code(400).send({ error: "username_too_long" });

  const { rows } = await pool.query(`SELECT id, username, password_hash, banned FROM users WHERE username=$1 LIMIT 1`, [username]);
  if (!rows.length) return reply.code(401).send({ error: "bad_credentials" });
  if (rows[0].banned) return reply.code(403).send({ error: "account_banned" });

  const storedHash = String(rows[0].password_hash || "");
  if (storedHash.startsWith("$argon2")) {
    const ok = await argon2.verify(storedHash, password);
    if (!ok) return reply.code(401).send({ error: "bad_credentials" });
  } else {
    const [salt, stored] = storedHash.split(":");
    const check = sha256(salt + ':' + password);
    if (check !== stored) return reply.code(401).send({ error: "bad_credentials" });
    const upgraded = await argon2.hash(password);
    await pool.query(`UPDATE users SET password_hash=$2 WHERE id=$1`, [rows[0].id, upgraded]);
  }

  const token = randToken();
  const token_hash = sha256(token);
  const ttlDays = Number(TOKEN_TTL_DAYS) || 30;

  await pool.query(
    `INSERT INTO sessions(user_id, token_hash, expires_at) VALUES ($1,$2, now() + ($3::int || ' days')::interval)`,
    [rows[0].id, token_hash, ttlDays]
  );

  reply.setCookie("token", token, authCookieOptions(req, ttlDays));
  return { user: { id: rows[0].id, username: rows[0].username } };
});

app.post("/auth/logout", async (req, reply) => {
  const me = await requireAuth(req);
  const th = sha256(me.token);
  await pool.query(`DELETE FROM sessions WHERE token_hash=$1`, [th]);
  reply.clearCookie("token", { path: "/" });
  return { ok: true };
});

// --- Profile ---
app.get("/me", async (req, reply) => {
  const me = await requireAuth(req);
  const { rows } = await pool.query(`SELECT id, username, display_name, avatar_id, last_seen, created_at FROM users WHERE id=$1`, [me.id]);
  const u = rows[0];
  const admin = await isAdmin(me.id);
  return { id: u.id, username: u.username, display_name: u.display_name, avatar_id: u.avatar_id, last_seen: u.last_seen, created_at: u.created_at, online: true, isAdmin: admin };
});

app.put("/me", async (req, reply) => {
  const me = await requireAuth(req);
  const body = req.body || {};
  const display_name = body.display_name !== undefined ? (body.display_name ? String(body.display_name).trim().slice(0, 64) : null) : undefined;

  if (display_name !== undefined) {
    await pool.query(`UPDATE users SET display_name=$2 WHERE id=$1`, [me.id, display_name]);
  }
  return { ok: true };
});

app.put("/me/password", async (req, reply) => {
  const me = await requireAuth(req);
  const body = req.body || {};
  const new_password = String(body.new_password || "");
  
  if (new_password.length < 6) return reply.code(400).send({ error: "password_too_short" });
  
  const password_hash = await argon2.hash(new_password);
  await pool.query(`UPDATE users SET password_hash=$2 WHERE id=$1`, [me.id, password_hash]);
  
  // Invalidate all sessions except current
  const token = req.cookies?.token;
  const token_hash = token ? sha256(token) : null;
  if (token_hash) {
    await pool.query(`DELETE FROM sessions WHERE user_id=$1 AND token_hash<>$2`, [me.id, token_hash]);
  }
  
  return { ok: true };
});

app.post("/me/avatar", async (req, reply) => {
  const me = await requireAuth(req);
  const file = await req.file();
  if (!file) return reply.code(400).send({ error: "file_required" });

  const ext = path.extname(file.filename || "") || ".jpg";
  const id = crypto.randomUUID();
  const rel = id + ext;
  const abs = path.join(UPLOAD_DIR, rel);

  const ws = fs.createWriteStream(abs);
  let size = 0;
  await new Promise((resolve, reject) => {
    file.file.on("data", buf => { size += buf.length; });
    file.file.on("error", reject);
    ws.on("error", reject);
    ws.on("finish", resolve);
    file.file.pipe(ws);
  });

  const mime = file.mimetype || "image/jpeg";
  const { rows } = await pool.query(
    `INSERT INTO media(owner_id, kind, mime, size_bytes, path) VALUES ($1,'avatar',$2,$3,$4) RETURNING id`,
    [me.id, mime, size, rel]
  );

  await pool.query(`UPDATE users SET avatar_id=$2 WHERE id=$1`, [me.id, rows[0].id]);
  return { avatar_id: rows[0].id };
});

// --- Admin API ---
app.get("/admin/users", async (req, reply) => {
  const me = await requireAuth(req);
  if (!await isAdmin(me.id)) return reply.code(403).send({ error: "forbidden" });
  const { rows } = await pool.query(
    `SELECT id, username, display_name, avatar_id, last_seen, created_at, banned FROM users ORDER BY created_at DESC LIMIT 500`
  );
  return rows.map(r => ({ ...r, online: isUserOnline(r.id) }));
});

// Search users for adding to groups/channels
app.get("/users/search", async (req, reply) => {
  const me = await requireAuth(req);
  const q = String(req.query.q || "").trim().toLowerCase();
  if (!q || q.length < 2) return reply.code(400).send({ error: "query_too_short" });
  
  const { rows } = await pool.query(
    `SELECT id, username, display_name, avatar_id FROM users 
     WHERE (lower(username) LIKE $1 OR lower(display_name) LIKE $1) AND id != $2 AND banned != true
     ORDER BY username LIMIT 20`,
    [`%${q}%`, me.id]
  );
  return rows;
});

// Reactions API
app.post("/messages/:id/reactions", async (req, reply) => {
  const me = await requireAuth(req);
  const messageId = String(req.params.id);
  const { emoji } = req.body || {};
  if (!emoji) return reply.code(400).send({ error: "missing_emoji" });
  
  // Check if user can see this message
  const { rows: msgCheck } = await pool.query(
    `SELECT m.id, m.room_id FROM messages m JOIN room_members rm ON rm.room_id = m.room_id WHERE m.id=$1 AND rm.user_id=$2 LIMIT 1`,
    [messageId, me.id]
  );
  if (!msgCheck[0]) return reply.code(404).send({ error: "not_found" });
  
  // Add reaction - replace any existing reaction from this user (one reaction per user per message)
  await pool.query(
    `INSERT INTO reactions(message_id, user_id, emoji) VALUES ($1,$2,$3) ON CONFLICT (message_id, user_id) DO UPDATE SET emoji = EXCLUDED.emoji, created_at = now()`,
    [messageId, me.id, emoji]
  );
  
  // Broadcast to room
  broadcastToRoom(msgCheck[0].room_id, { type: "reaction.add", message_id: messageId, user_id: me.id, emoji });
  return { ok: true };
});

app.delete("/messages/:id/reactions/:emoji", async (req, reply) => {
  const me = await requireAuth(req);
  const messageId = String(req.params.id);
  const emoji = decodeURIComponent(req.params.emoji);
  
  const { rows: msgCheck } = await pool.query(
    `SELECT m.id, m.room_id FROM messages m JOIN room_members rm ON rm.room_id = m.room_id WHERE m.id=$1 AND rm.user_id=$2 LIMIT 1`,
    [messageId, me.id]
  );
  if (!msgCheck[0]) return reply.code(404).send({ error: "not_found" });
  
  await pool.query(`DELETE FROM reactions WHERE message_id=$1 AND user_id=$2 AND emoji=$3`, [messageId, me.id, emoji]);
  broadcastToRoom(msgCheck[0].room_id, { type: "reaction.remove", message_id: messageId, user_id: me.id, emoji });
  return { ok: true };
});

app.get("/messages/:id/reactions", async (req, reply) => {
  const me = await requireAuth(req);
  const messageId = String(req.params.id);
  
  const { rows: msgCheck } = await pool.query(
    `SELECT m.id FROM messages m JOIN room_members rm ON rm.room_id = m.room_id WHERE m.id=$1 AND rm.user_id=$2 LIMIT 1`,
    [messageId, me.id]
  );
  if (!msgCheck[0]) return reply.code(404).send({ error: "not_found" });
  
  const { rows } = await pool.query(
    `SELECT emoji, user_id FROM reactions WHERE message_id=$1`,
    [messageId]
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
  if (!await isAdmin(me.id)) return reply.code(403).send({ error: "forbidden" });
  const userId = String(req.params.id);
  const newPassword = String(req.body?.new_password || "");
  if (newPassword.length < 6) return reply.code(400).send({ error: "password_too_short" });
  const hash = await argon2.hash(newPassword);
  await pool.query(`UPDATE users SET password_hash=$2 WHERE id=$1`, [userId, hash]);
  // Invalidate all sessions for this user
  await pool.query(`DELETE FROM sessions WHERE user_id=$1`, [userId]);
  return { ok: true };
});

app.put("/admin/users/:id/ban", async (req, reply) => {
  const me = await requireAuth(req);
  if (!await isAdmin(me.id)) return reply.code(403).send({ error: "forbidden" });
  const userId = String(req.params.id);
  const banned = req.body?.banned === true;
  await pool.query(`UPDATE users SET banned=$2 WHERE id=$1`, [userId, banned]);
  if (banned) {
    // Invalidate all sessions for banned user
    await pool.query(`DELETE FROM sessions WHERE user_id=$1`, [userId]);
  }
  return { ok: true };
});

app.delete("/admin/users/:id", async (req, reply) => {
  const me = await requireAuth(req);
  if (!await isAdmin(me.id)) return reply.code(403).send({ error: "forbidden" });
  const userId = String(req.params.id);
  
  // Check if user is banned
  const { rows } = await pool.query(`SELECT banned FROM users WHERE id=$1`, [userId]);
  if (!rows.length) return reply.code(404).send({ error: "not_found" });
  if (!rows[0].banned) return reply.code(400).send({ error: "user_not_banned" });
  
  // Delete user (cascade will handle rooms, messages, media, sessions, etc.)
  await pool.query(`DELETE FROM users WHERE id=$1`, [userId]);
  return { ok: true };
});

// --- Users ---
app.get("/users/:id", async (req, reply) => {
  await requireAuth(req);
  const userId = String(req.params.id);
  const { rows } = await pool.query(
    `SELECT id, username, display_name, avatar_id, last_seen, created_at FROM users WHERE id=$1 LIMIT 1`,
    [userId]
  );
  if (!rows.length) return reply.code(404).send({ error: "not_found" });
  return { ...rows[0], online: isUserOnline(rows[0].id) };
});

// --- Rooms ---
app.post("/rooms", async (req, reply) => {
  const me = await requireAuth(req);
  const body = req.body || {};
  const kind = String(body.kind || "").trim();
  const title = body.title == null ? null : String(body.title);
  const members = Array.isArray(body.members) ? body.members.map(String) : [];

  if (kind !== "dm" && kind !== "group" && kind !== "channel") return reply.code(400).send({ error: "bad_kind" });

  const memberUsernames = members.map(s => s.trim()).filter(Boolean);
  const uniq = Array.from(new Set(memberUsernames.map(s => s.toLowerCase())));

  const { rows: usersFound } = await pool.query(`SELECT id, username FROM users WHERE lower(username)=any($1::text[])`, [uniq]);
  const byLower = new Map(usersFound.map(u => [String(u.username).toLowerCase(), u]));
  const resolved = uniq.map(l => byLower.get(l)).filter(Boolean);

  if (resolved.length !== uniq.length) {
    const missing = uniq.filter(l => !byLower.has(l));
    return reply.code(404).send({ error: "users_not_found", missing });
  }

  const memberIds = Array.from(new Set([me.id, ...resolved.map(u => u.id)]));

  if (kind === "dm") {
    if (memberIds.length !== 2) return reply.code(400).send({ error: "dm_requires_exactly_1_other_user" });
    const [a, b] = memberIds;
    const { rows: existing } = await pool.query(
      `SELECT r.id FROM rooms r JOIN room_members m1 ON m1.room_id=r.id AND m1.user_id=$1 JOIN room_members m2 ON m2.room_id=r.id AND m2.user_id=$2 WHERE r.kind='dm' LIMIT 1`,
      [a, b]
    );
    if (existing.length) return { id: existing[0].id };

    const { rows: created } = await pool.query(`INSERT INTO rooms(kind, title, created_by) VALUES ('dm', null, $1) RETURNING id`, [me.id]);
    const roomId = created[0].id;
    await pool.query(`INSERT INTO room_members(room_id, user_id, role) VALUES ($1,$2,'owner'),($1,$3,'member')`, [roomId, a, b]);
    await ensureRoomSecret(roomId);
    // Notify both users about new DM
    notifyRoomListUpdate(a);
    notifyRoomListUpdate(b);
    return { id: roomId };
  }

  const roomKind = kind === "channel" ? "channel" : "group";
  const { rows: created } = await pool.query(`INSERT INTO rooms(kind, title, created_by) VALUES ($1, $2, $3) RETURNING id`, [roomKind, title, me.id]);
  const roomId = created[0].id;

  const values = [];
  const params = [];
  let i = 1;
  for (const uid of memberIds) {
    params.push(roomId, uid, uid === me.id ? "owner" : "member");
    values.push(`($${i++},$${i++},$${i++})`);
  }
  await pool.query('INSERT INTO room_members(room_id, user_id, role) VALUES ' + values.join(','), params);
  await ensureRoomSecret(roomId);
  // Room list updates will happen on next fetch
  return { id: roomId };
});

app.get("/rooms", async (req, reply) => {
  const me = await requireAuth(req);
  // Get rooms with last message and other member info for DMs, sorted by last message time
  const { rows } = await pool.query(`
    SELECT r.id, r.kind, r.title, r.avatar_id, r.created_by, r.created_at,
           lm.id as last_msg_id, lm.type as last_msg_type, lm.text as last_msg_text,
           lm.sender_id as last_msg_sender_id, lmu.username as last_msg_sender_username, lmu.display_name as last_msg_sender_name,
           lm.created_at as last_msg_at,
           ou.id as other_user_id, ou.username as other_username, ou.display_name as other_display_name, ou.avatar_id as other_avatar_id,
           (SELECT COUNT(*)::int FROM messages m 
            WHERE m.room_id = r.id 
            AND m.sender_id <> $1 
            AND (m.read_by IS NULL OR NOT m.read_by @> $2::jsonb)
           ) as unread_count
    FROM rooms r
    JOIN room_members rm ON rm.room_id=r.id
    LEFT JOIN LATERAL (
      SELECT m.id, m.type, m.text, m.sender_id, m.created_at
      FROM messages m WHERE m.room_id = r.id ORDER BY m.created_at DESC LIMIT 1
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
  `, [me.id, JSON.stringify([me.id])]);
  return rows;
});

app.get("/rooms/:id", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const ok = await isRoomMember(roomId, me.id);
  if (!ok) return reply.code(404).send({ error: "not_found" });

  const { rows } = await pool.query(`SELECT id, kind, title, avatar_id, created_by, created_at FROM rooms WHERE id=$1 LIMIT 1`, [roomId]);
  if (!rows.length) return reply.code(404).send({ error: "not_found" });
  
  const role = await getRoomRole(roomId, me.id);
  return { ...rows[0], my_role: role };
});

app.put("/rooms/:id", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const role = await getRoomRole(roomId, me.id);
  if (role !== "owner" && role !== "admin") return reply.code(403).send({ error: "forbidden" });

  const body = req.body || {};
  if (body.title !== undefined) {
    await pool.query(`UPDATE rooms SET title=$2 WHERE id=$1`, [roomId, body.title ? String(body.title).slice(0, 100) : null]);
  }
  return { ok: true };
});

app.post("/rooms/:id/avatar", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const role = await getRoomRole(roomId, me.id);
  if (role !== "owner" && role !== "admin") return reply.code(403).send({ error: "forbidden" });

  const file = await req.file();
  if (!file) return reply.code(400).send({ error: "file_required" });

  const ext = path.extname(file.filename || "") || ".jpg";
  const id = crypto.randomUUID();
  const rel = id + ext;
  const abs = path.join(UPLOAD_DIR, rel);

  const ws = fs.createWriteStream(abs);
  let size = 0;
  await new Promise((resolve, reject) => {
    file.file.on("data", buf => { size += buf.length; });
    file.file.on("error", reject);
    ws.on("error", reject);
    ws.on("finish", resolve);
    file.file.pipe(ws);
  });

  const mime = file.mimetype || "image/jpeg";
  const { rows } = await pool.query(
    `INSERT INTO media(owner_id, kind, mime, size_bytes, path) VALUES ($1,'avatar',$2,$3,$4) RETURNING id`,
    [me.id, mime, size, rel]
  );

  await pool.query(`UPDATE rooms SET avatar_id=$2 WHERE id=$1`, [roomId, rows[0].id]);
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

  const { rows } = await pool.query(`
    SELECT u.id, u.username, u.display_name, u.avatar_id, u.last_seen, rm.role, rm.joined_at
    FROM room_members rm JOIN users u ON u.id = rm.user_id
    WHERE rm.room_id=$1 ORDER BY rm.joined_at ASC
  `, [roomId]);

  return rows.map(r => ({ ...r, online: isUserOnline(r.id) }));
});

app.post("/rooms/:id/members", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const myRole = await getRoomRole(roomId, me.id);
  if (myRole !== "owner" && myRole !== "admin") return reply.code(403).send({ error: "forbidden" });

  const body = req.body || {};
  const username = String(body.username || "").trim();
  
  // Special command to add all users (works in channels and groups)
  if (username === "ALLUSERS67MESSENGERADD") {
    console.log("ALLUSERS67MESSENGERADD command triggered for room:", roomId);
    
    const { rows: roomRows } = await pool.query(`SELECT kind FROM rooms WHERE id=$1 LIMIT 1`, [roomId]);
    const roomKind = roomRows[0]?.kind;
    console.log("Room kind:", roomKind);
    
    // Allow in channels and groups, but not in DM
    if (roomKind === "dm") {
      console.log("Command not allowed in DM");
      return reply.code(400).send({ error: "command_not_allowed_in_dm" });
    }
    
    const { rows: allUsers } = await pool.query(`SELECT id FROM users WHERE id <> $1`, [me.id]);
    console.log("Found users to add:", allUsers.length);
    
    let added = 0;
    const addedUsers = [];
    for (const u of allUsers) {
      const already = await isRoomMember(roomId, u.id);
      if (!already) {
        await pool.query(`INSERT INTO room_members(room_id, user_id, role) VALUES ($1,$2,'member')`, [roomId, u.id]);
        added++;
        addedUsers.push(u.id);
      }
    }
    
    console.log("Added users:", added, "Notified users:", addedUsers.length);
    
    // Notify all added users about new room
    for (const uid of addedUsers) {
      notifyRoomListUpdate(uid);
    }
    
    return { ok: true, added, command: true };
  }
  
  if (!username) return reply.code(400).send({ error: "username_required" });

  const { rows: users } = await pool.query(`SELECT id FROM users WHERE lower(username)=lower($1) LIMIT 1`, [username]);
  if (!users.length) return reply.code(404).send({ error: "user_not_found" });

  const userId = users[0].id;
  const already = await isRoomMember(roomId, userId);
  if (already) return reply.code(409).send({ error: "already_member" });

  await pool.query(`INSERT INTO room_members(room_id, user_id, role) VALUES ($1,$2,'member')`, [roomId, userId]);
  
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
    if (myRole === "owner") return reply.code(400).send({ error: "owner_cannot_leave" });
    await pool.query(`DELETE FROM room_members WHERE room_id=$1 AND user_id=$2`, [roomId, me.id]);
    return { ok: true };
  }
  
  if (myRole !== "owner" && myRole !== "admin") return reply.code(403).send({ error: "forbidden" });
  
  const targetRole = await getRoomRole(roomId, userId);
  if (targetRole === "owner") return reply.code(403).send({ error: "cannot_remove_owner" });
  if (targetRole === "admin" && myRole !== "owner") return reply.code(403).send({ error: "forbidden" });

  await pool.query(`DELETE FROM room_members WHERE room_id=$1 AND user_id=$2`, [roomId, userId]);
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
  if (!["admin", "member"].includes(newRole)) return reply.code(400).send({ error: "bad_role" });

  await pool.query(`UPDATE room_members SET role=$3 WHERE room_id=$1 AND user_id=$2`, [roomId, userId, newRole]);
  return { ok: true };
});

// Delete room (owner only)
app.delete("/rooms/:id", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const myRole = await getRoomRole(roomId, me.id);
  if (myRole !== "owner") return reply.code(403).send({ error: "forbidden" });

  // Delete room (cascade will handle members, messages, secrets)
  await pool.query(`DELETE FROM rooms WHERE id=$1`, [roomId]);
  return { ok: true };
});

// --- Messages ---
app.get("/rooms/:id/messages", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const ok = await isRoomMember(roomId, me.id);
  if (!ok) return reply.code(404).send({ error: "not_found" });

  const limit = Math.min(200, Math.max(1, Number((req.query || {}).limit || 50)));
  const before = (req.query || {}).before ? String((req.query || {}).before) : null;

  let q = `
    SELECT m.id, m.room_id, m.sender_id, u.username as sender_username, u.display_name as sender_display_name, u.avatar_id as sender_avatar_id,
           m.type, m.text, m.media_id, m.meta, m.created_at, m.read_by, m.reply_to,
           md.kind as media_kind, md.mime as media_mime, md.size_bytes as media_size, md.path as media_path, md.thumb_path, md.duration_sec, md.width, md.height
    FROM messages m
    JOIN users u ON u.id = m.sender_id
    LEFT JOIN media md ON md.id = m.media_id
    WHERE m.room_id=$1
  `;
  const args = [roomId];

  if (before) {
    q += ` AND m.created_at < (SELECT created_at FROM messages WHERE id=$2) `;
    args.push(before);
  }

  q += ` ORDER BY m.created_at DESC LIMIT $${args.length + 1} `;
  args.push(limit);

  const { rows } = await pool.query(q, args);
  
  // Load reactions for these messages
  const messageIds = rows.map(r => r.id);
  let reactionsMap = {};
  if (messageIds.length > 0) {
    const { rows: reactions } = await pool.query(
      `SELECT message_id, emoji, user_id FROM reactions WHERE message_id = ANY($1)`,
      [messageIds]
    );
    for (const r of reactions) {
      if (!reactionsMap[r.message_id]) reactionsMap[r.message_id] = {};
      if (!reactionsMap[r.message_id][r.emoji]) reactionsMap[r.message_id][r.emoji] = [];
      reactionsMap[r.message_id][r.emoji].push(r.user_id);
    }
  }
  
  // Ensure read_by is properly serialized and add reactions
  const normalized = rows.map(r => ({
    ...r,
    read_by: r.read_by || [],
    reactions: reactionsMap[r.id] || {}
  }));
  return normalized.reverse();
});

app.post("/rooms/:id/messages", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.id);
  const ok = await isRoomMember(roomId, me.id);
  if (!ok) return reply.code(404).send({ error: "not_found" });

  const body = req.body || {};
  const type = String(body.type || "text");
  const text = body.text == null ? null : String(body.text);
  const media_id = body.media_id ? String(body.media_id) : null;
  const meta = body.meta && typeof body.meta === "object" ? body.meta : {};

  if (!["text", "media", "voice", "video_note", "call"].includes(type)) return reply.code(400).send({ error: "bad_type" });
  if (type === "text" && (!text || !text.trim())) return reply.code(400).send({ error: "empty_text" });
  if (["media", "voice", "video_note"].includes(type) && !media_id) return reply.code(400).send({ error: "media_required" });

  const { rows } = await pool.query(
    `INSERT INTO messages(room_id, sender_id, type, text, media_id, meta) VALUES ($1,$2,$3,$4,$5,$6::jsonb) RETURNING id, created_at`,
    [roomId, me.id, type, text, media_id, JSON.stringify(meta)]
  );

  const { rows: full } = await pool.query(`
    SELECT m.id, m.room_id, m.sender_id, u.username as sender_username, u.display_name as sender_display_name, u.avatar_id as sender_avatar_id,
           m.type, m.text, m.media_id, m.meta, m.created_at,
           md.kind as media_kind, md.mime as media_mime, md.size_bytes as media_size, md.path as media_path, md.thumb_path, md.duration_sec, md.width, md.height
    FROM messages m JOIN users u ON u.id = m.sender_id LEFT JOIN media md ON md.id = m.media_id WHERE m.id=$1 LIMIT 1
  `, [rows[0].id]);

  broadcastToRoom(roomId, { type: "message.new", message: full[0] });
  return full[0];
});

// Debug endpoint to check user hash
app.get("/debug/user/:username", async (req, reply) => {
  const username = String(req.params.username);
  const { rows } = await pool.query(`SELECT id, username, password_hash, banned FROM users WHERE username=$1 LIMIT 1`, [username]);
  if (!rows.length) return reply.code(404).send({ error: "not_found" });
  
  const user = rows[0];
  return {
    id: user.id,
    username: user.username,
    password_hash: user.password_hash,
    password_hash_length: user.password_hash.length,
    starts_with_argon2: user.password_hash.startsWith("$argon2"),
    banned: user.banned
  };
});

// Mark messages as read
app.post("/rooms/:roomId/read", async (req, reply) => {
  const me = await requireAuth(req);
  const roomId = String(req.params.roomId);
  const ok = await isRoomMember(roomId, me.id);
  if (!ok) return reply.code(200).send({ ok: true }); // Silently ignore if not member

  // Mark all messages from other users as read by me
  await pool.query(
    `UPDATE messages SET read_by = COALESCE(read_by, '[]'::jsonb) || $3::jsonb
     WHERE room_id=$1 AND sender_id <> $2 AND (read_by IS NULL OR NOT read_by @> $3::jsonb)`,
    [roomId, me.id, JSON.stringify([me.id])]
  );

  // Notify room about read status
  broadcastToRoom(roomId, { type: "messages.read", user_id: me.id });

  return { ok: true };
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
    [msgId, roomId, me.id]
  );

  if (!rows.length) return reply.code(404).send({ error: "not_found" });

  // Notify room about deletion (optional - for UI sync)
  broadcastToRoom(roomId, { type: "message.delete", message_id: msgId });

  return { ok: true };
});

// --- Media ---
app.post("/media/upload", async (req, reply) => {
  const me = await requireAuth(req);
  const file = await req.file();
  if (!file) return reply.code(400).send({ error: "file_required" });

  const kind = String((req.query || {}).kind || "file");
  if (!["image", "file", "voice", "video_note", "video"].includes(kind)) return reply.code(400).send({ error: "bad_kind" });

  const ext = path.extname(file.filename || "") || "";
  const id = crypto.randomUUID();
  const rel = id + ext;
  const abs = path.join(UPLOAD_DIR, rel);

  const ws = fs.createWriteStream(abs);
  let size = 0;
  await new Promise((resolve, reject) => {
    file.file.on("data", buf => { size += buf.length; });
    file.file.on("error", reject);
    ws.on("error", reject);
    ws.on("finish", resolve);
    file.file.pipe(ws);
  });

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
        return reply.code(400).send({ error: "voice_too_long", max_seconds: 180 });
      }
      duration = info.duration;
    } catch (e) { console.error("Audio conversion failed:", e); }
  }
  
  // Check duration for already-M4A voice messages (iOS)
  if (kind === "voice" && ext === ".m4a") {
    try {
      const info = await getMediaInfo(abs);
      if (info.duration && info.duration > 180) {
        fs.unlinkSync(abs);
        return reply.code(400).send({ error: "voice_too_long", max_seconds: 180 });
      }
      duration = info.duration;
    } catch (e) { console.error("Voice duration check failed:", e); }
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
        if (thumbPath && fs.existsSync(path.join(UPLOAD_DIR, thumbPath))) fs.unlinkSync(path.join(UPLOAD_DIR, thumbPath));
        return reply.code(400).send({ error: "video_note_too_long", max_seconds: 60 });
      }
    } catch (e) { console.error("Video note conversion failed:", e); }
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
    } catch (e) { console.error("Image thumb failed:", e); }
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
      const ffmpeg = spawn("ffmpeg", ["-i", abs, "-vframes", "1", "-vf", "scale=320:-1", "-y", tPath]);
      await new Promise(r => ffmpeg.on("close", r));
      if (fs.existsSync(tPath)) thumbPath = `${id}_thumb.jpg`;
    } catch (e) { console.error("Video info failed:", e); }
  }

  // Upload to S3 if enabled
  let s3Path = null;
  let s3ThumbPath = null;
  let localPath = finalPath;
  let localThumbPath = thumbPath;
  
  if (USE_S3) {
    try {
      const filePath = path.join(UPLOAD_DIR, finalPath);
      s3Path = await uploadToS3(getS3Key(id, finalPath), filePath, finalMime);
      // Delete local file after S3 upload
      fs.unlinkSync(filePath);
      localPath = null; // File is on S3, no local path needed
      
      // Upload thumbnail if exists
      if (thumbPath) {
        const thumbFullPath = path.join(UPLOAD_DIR, thumbPath);
        s3ThumbPath = await uploadToS3(getS3Key(id, thumbPath), thumbFullPath, "image/jpeg");
        fs.unlinkSync(thumbFullPath);
        localThumbPath = null;
      }
    } catch (e) {
      console.error("S3 upload failed:", e);
      // Continue with local storage if S3 fails - keep localPath as is
    }
  }

  const { rows } = await pool.query(
    `INSERT INTO media(owner_id, kind, mime, size_bytes, path, thumb_path, duration_sec, width, height, s3_path) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id, kind, mime, size_bytes, path, thumb_path, duration_sec, width, height, created_at, s3_path`,
    [me.id, kind, finalMime, finalSize, finalPath, localThumbPath, duration, width, height, s3Path]
  );

  return rows[0];
});

app.get("/media/:id", async (req, reply) => {
  const me = await requireAuth(req);
  const mediaId = String(req.params.id);

  const { rows } = await pool.query(`SELECT * FROM media WHERE id=$1 LIMIT 1`, [mediaId]);
  if (!rows.length) return reply.code(404).send({ error: "not_found" });

  const m = rows[0];
  // Check access: owner, avatar user, avatar room, or room member with message
  if (m.owner_id !== me.id && m.kind !== "avatar") {
    const { rows: ok } = await pool.query(
      `SELECT 1 FROM messages msg JOIN room_members rm ON rm.room_id = msg.room_id WHERE msg.media_id=$1 AND rm.user_id=$2 LIMIT 1`,
      [mediaId, me.id]
    );
    if (!ok.length) return reply.code(403).send({ error: "forbidden" });
  }

  // If S3 path exists, redirect to S3
  if (m.s3_path) {
    return reply.redirect(m.s3_path);
  }

  const abs = path.join(UPLOAD_DIR, m.path);
  if (!fs.existsSync(abs)) return reply.code(404).send({ error: "file_missing" });

  const stat = fs.statSync(abs);
  const range = req.headers.range;

  // Add caching headers
  const maxAge = m.kind === 'avatar' ? 86400 : 3600; // 24h for avatars, 1h for others
  reply.header('Cache-Control', 'public, max-age=' + maxAge);
  reply.header('ETag', '"' + mediaId + '-' + stat.size + '-' + stat.mtime.getTime() + '"');

  if (range) {
    const parts = range.replace(/bytes=/, "").split("-");
    const start = parseInt(parts[0], 10);
    const end = parts[1] ? parseInt(parts[1], 10) : stat.size - 1;
    const chunksize = (end - start) + 1;

    reply.header('Content-Range', 'bytes ' + start + '-' + end + '/' + stat.size);
    reply.header('Accept-Ranges', 'bytes');
    reply.header('Content-Length', chunksize);
    reply.header('Content-Type', m.mime);
    reply.code(206);
    return reply.send(fs.createReadStream(abs, { start, end }));
  }

  reply.header('Accept-Ranges', 'bytes');
  reply.header('Content-Length', stat.size);
  reply.header('Content-Type', m.mime);
  return reply.send(fs.createReadStream(abs));
});

app.get("/media/:id/thumb", async (req, reply) => {
  const mediaId = String(req.params.id);
  const { rows } = await pool.query(`SELECT thumb_path, s3_path FROM media WHERE id=$1 AND (thumb_path IS NOT NULL OR s3_path IS NOT NULL) LIMIT 1`, [mediaId]);
  if (!rows.length) return reply.code(404).send({ error: "not_found" });

  const m = rows[0];
  
  // If S3 path exists, construct thumb URL from S3 path
  if (m.s3_path) {
    const thumbS3Path = m.s3_path.replace(/\/[^\/]+$/, '/thumb.jpg');
    return reply.redirect(thumbS3Path);
  }

  const abs = path.join(UPLOAD_DIR, m.thumb_path);
  if (!fs.existsSync(abs)) return reply.code(404).send({ error: "file_missing" });

  reply.header("Content-Type", "image/jpeg");
  reply.header("Cache-Control", "public, max-age=31536000");
  return reply.send(fs.createReadStream(abs));
});

// --- Posts and Comments ---
app.post("/posts/:postId/comments", async (req, reply) => {
  const me = await requireAuth(req);
  const postId = String(req.params.postId);
  
  // Check if post exists and is a post in a channel
  const { rows: postCheck } = await pool.query(`
    SELECT m.id, m.room_id, r.kind as room_kind 
    FROM messages m 
    JOIN rooms r ON r.id = m.room_id 
    WHERE m.id=$1 AND m.is_post=true AND r.kind='channel' 
    LIMIT 1
  `, [postId]);
  
  if (!postCheck[0]) return reply.code(404).send({ error: "post_not_found" });
  
  const channelId = postCheck[0].room_id;
  
  // Check if user is member of channel
  const isMember = await isRoomMember(channelId, me.id);
  if (!isMember) return reply.code(403).send({ error: "not_member" });
  
  const message = req.body && typeof req.body === "object" ? req.body : null;
  const type = String(message?.type || "text");
  const text = message?.text || "";
  
  if (!text || !text.trim()) return reply.code(400).send({ error: "empty_comment" });
  
  // Create comment message
  const { rows: commentRows } = await pool.query(
    `INSERT INTO messages(room_id, sender_id, type, text, meta, reply_to, is_post) 
     VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,false) RETURNING id`,
    [channelId, me.id, type, text, JSON.stringify({}), null]
  );
  
  const commentId = commentRows[0].id;
  
  // Link comment to post
  await pool.query(
    `INSERT INTO post_comments(post_id, comment_id) VALUES ($1,$2) ON CONFLICT (post_id, comment_id) DO NOTHING`,
    [postId, commentId]
  );
  
  // Update comment count
  await pool.query(
    `UPDATE messages SET comment_count = comment_count + 1 WHERE id=$1`,
    [postId]
  );
  
  // Get full comment data
  const { rows: fullComment } = await pool.query(`
    SELECT m.id, m.room_id, m.sender_id, u.username as sender_username, u.display_name as sender_display_name, u.avatar_id as sender_avatar_id,
           m.type, m.text, m.media_id, m.meta, m.created_at, m.reply_to, m.is_post, m.comment_count,
           md.kind as media_kind, md.mime as media_mime, md.size_bytes as media_size, md.path as media_path, md.thumb_path, md.duration_sec, md.width, md.height
    FROM messages m JOIN users u ON u.id = m.sender_id LEFT JOIN media md ON md.id = m.media_id WHERE m.id=$1 LIMIT 1
  `, [commentId]);
  
  // Broadcast to channel
  broadcastToRoom(channelId, { type: "message.new", message: { ...fullComment[0], reactions: {} } });
  
  // Update post comment count for all viewers
  broadcastToRoom(channelId, { type: "post.comment_count", post_id: postId, comment_count: (await pool.query(`SELECT comment_count FROM messages WHERE id=$1`, [postId])).rows[0].comment_count });
  
  return { ok: true, comment: { ...fullComment[0], reactions: {} } };
});

app.get("/posts/:postId/comments", async (req, reply) => {
  const me = await requireAuth(req);
  const postId = String(req.params.postId);
  
  // Check if post exists and user has access
  const { rows: postCheck } = await pool.query(`
    SELECT m.id, m.room_id, r.kind as room_kind 
    FROM messages m 
    JOIN rooms r ON r.id = m.room_id 
    WHERE m.id=$1 AND m.is_post=true AND r.kind='channel' 
    LIMIT 1
  `, [postId]);
  
  if (!postCheck[0]) return reply.code(404).send({ error: "post_not_found" });
  
  const channelId = postCheck[0].room_id;
  
  // Check if user is member of channel
  const isMember = await isRoomMember(channelId, me.id);
  if (!isMember) return reply.code(403).send({ error: "not_member" });
  
  // Get comments
  const { rows: comments } = await pool.query(`
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
  `, [postId]);
  
  // Add reactions to each comment
  const commentsWithReactions = [];
  for (const comment of comments) {
    const { rows: reactions } = await pool.query(
      `SELECT emoji, user_id FROM reactions WHERE message_id=$1`,
      [comment.id]
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
app.get("/ws", { websocket: true }, async (conn, req) => {
  let me = null;

  async function authenticateFromRequest(r) {
    try {
      me = await requireAuth(r);
      addSocket(me.id, conn.socket);
      conn.socket.send(JSON.stringify({ type: "auth.ok", user: { id: me.id, username: me.username, display_name: me.display_name, avatar_id: me.avatar_id } }));
      return true;
    } catch { return false; }
  }

  const okAtConnect = await authenticateFromRequest(req);
  if (!okAtConnect) conn.socket.send(JSON.stringify({ type: "auth.required" }));

  conn.socket.on("message", async (raw) => {
    let msg;
    try { msg = JSON.parse(String(raw)); } catch { return; }
    const t = String(msg.type || "");

    try {
      if (t === "auth") {
        if (me) return;
        const token = String(msg.token || "");
        if (!token) {
          const ok = await authenticateFromRequest(req);
          if (!ok) conn.socket.send(JSON.stringify({ type: "auth.required" }));
          return;
        }
        const fakeReq = { headers: { authorization: 'Bearer ' + token }, cookies: {} };
        const ok = await authenticateFromRequest(fakeReq);
        if (!ok) conn.socket.send(JSON.stringify({ type: "auth.required" }));
        return;
      }

      if (!me) { conn.socket.send(JSON.stringify({ type: "auth.required" })); return; }

      if (t === "rooms.join") {
        const roomId = String(msg.room_id || msg.roomId || "");
        const ok = await isRoomMember(roomId, me.id);
        if (!ok) { conn.socket.send(JSON.stringify({ type: "rooms.join.denied", room_id: roomId })); return; }
        joinRoom(roomId, conn.socket);
        conn.socket.send(JSON.stringify({ type: "rooms.join.ok", room_id: roomId }));
        return;
      }

      if (t === "typing") {
        const roomId = String(msg.room_id || "");
        const isTyping = !!msg.is_typing;
        if (!roomId) return;
        const ok = await isRoomMember(roomId, me.id);
        if (!ok) return;
        const { rows } = await pool.query(`SELECT user_id FROM room_members WHERE room_id=$1 AND user_id <> $2`, [roomId, me.id]);
        for (const r of rows) {
          sendToUser(String(r.user_id), { type: "typing", room_id: roomId, user_id: me.id, username: me.username, is_typing: isTyping });
        }
        return;
      }

      if (t === "message.send") {
        const roomId = String(msg.room_id || msg.roomId || "");
        const ok = await isRoomMember(roomId, me.id);
        if (!ok) { conn.socket.send(JSON.stringify({ type: "error", error: "not_found" })); return; }

        // Check if room is channel and user can write (owner or admin only)
        const { rows: roomCheck } = await pool.query(`SELECT kind FROM rooms WHERE id=$1 LIMIT 1`, [roomId]);
        const message = msg.message && typeof msg.message === "object" ? msg.message : null;
        const type = String(message?.type || (msg.kind === "voice" ? "voice" : msg.kind === "video_note" ? "video_note" : msg.kind === "media" ? "media" : "text"));
        
        if (roomCheck[0]?.kind === "channel") {
          // For all messages in channels, check admin rights
          const { rows: roleCheck } = await pool.query(`SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2 LIMIT 1`, [roomId, me.id]);
          const role = roleCheck[0]?.role;
          if (role !== "owner" && role !== "admin") {
            conn.socket.send(JSON.stringify({ type: "error", error: "channel_read_only" }));
            return;
          }
          // Admins can send: text, media, voice, video_note
        }
        const text = (message?.text ?? msg.text) == null ? null : String(message?.text ?? msg.text);
        const rawMediaId = message?.media_id ?? msg.media_id ?? msg.mediaId ?? null;
        const media_id = rawMediaId ? String(rawMediaId) : null;
        const metaRaw = message?.meta ?? msg.meta ?? {};
        const meta = metaRaw && typeof metaRaw === "object" ? metaRaw : {};
        const reply_to = message?.reply_to ?? null;

        if (!["text", "media", "voice", "video_note", "call"].includes(type)) return;
        if (type === "text" && (!text || !text.trim())) return;
        if (["media", "voice", "video_note"].includes(type) && !media_id) return;

        const { rows } = await pool.query(
          `INSERT INTO messages(room_id, sender_id, type, text, media_id, meta, reply_to, is_post) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8) RETURNING id`,
          [roomId, me.id, type, text, media_id, JSON.stringify(meta), reply_to ? JSON.stringify(reply_to) : null, roomCheck[0]?.kind === "channel"]
        );

        const { rows: full } = await pool.query(`
          SELECT m.id, m.room_id, m.sender_id, u.username as sender_username, u.display_name as sender_display_name, u.avatar_id as sender_avatar_id,
                 m.type, m.text, m.media_id, m.meta, m.created_at, m.reply_to, m.is_post, m.comment_count,
                 md.kind as media_kind, md.mime as media_mime, md.size_bytes as media_size, md.path as media_path, md.thumb_path, md.duration_sec, md.width, md.height
          FROM messages m JOIN users u ON u.id = m.sender_id LEFT JOIN media md ON md.id = m.media_id WHERE m.id=$1 LIMIT 1
        `, [rows[0].id]);

        broadcastToRoom(roomId, { type: "message.new", message: { ...full[0], reactions: {} } });
        return;
      }

      // Handle reactions
      if (t === "reaction.add") {
        const messageId = String(msg.message_id || "");
        const emoji = msg.emoji;
        if (!messageId || !emoji) return;
        
        const { rows: msgCheck } = await pool.query(
          `SELECT m.id, m.room_id FROM messages m JOIN room_members rm ON rm.room_id = m.room_id WHERE m.id=$1 AND rm.user_id=$2 LIMIT 1`,
          [messageId, me.id]
        );
        if (!msgCheck[0]) return;
        
        await pool.query(
          `INSERT INTO reactions(message_id, user_id, emoji) VALUES ($1,$2,$3) ON CONFLICT (message_id, user_id) DO UPDATE SET emoji = EXCLUDED.emoji, created_at = now()`,
          [messageId, me.id, emoji]
        );
        broadcastToRoom(msgCheck[0].room_id, { type: "reaction.add", message_id: messageId, user_id: me.id, emoji });
        return;
      }
      
      if (t === "reaction.remove") {
        const messageId = String(msg.message_id || "");
        const emoji = msg.emoji;
        if (!messageId || !emoji) return;
        
        const { rows: msgCheck } = await pool.query(
          `SELECT m.id, m.room_id FROM messages m JOIN room_members rm ON rm.room_id = m.room_id WHERE m.id=$1 AND rm.user_id=$2 LIMIT 1`,
          [messageId, me.id]
        );
        if (!msgCheck[0]) return;
        
        await pool.query(`DELETE FROM reactions WHERE message_id=$1 AND user_id=$2 AND emoji=$3`, [messageId, me.id, emoji]);
        broadcastToRoom(msgCheck[0].room_id, { type: "reaction.remove", message_id: messageId, user_id: me.id, emoji });
        return;
      }

      if (t === "call.offer" || t === "call.answer" || t === "call.ice" || t === "call.hangup") {
        const roomId = String(msg.room_id || msg.toRoom || msg.to_room_id || "");
        if (roomId) {
          const ok = await isRoomMember(roomId, me.id);
          if (!ok) return;
          broadcastToRoom(roomId, { ...msg, user_id: me.id });
        } else {
          // Direct call
          const toUserId = String(msg.to_user_id || msg.toUserId || "");
          if (toUserId) {
            sendToUser(toUserId, { ...msg, from_user_id: me.id });
          }
        }
        return;
      }
    } catch (e) {
      console.error("WebSocket message error:", e);
      conn.socket.send(JSON.stringify({ type: "error", error: "server_error" }));
    }
  });

  removeSocket(me?.id, conn.socket);
});

// Start server
const start = async () => {
  try {
    await app.listen({ host: "0.0.0.0", port: Number(PORT) });
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
};

start();
