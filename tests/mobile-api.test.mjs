import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import fs from "node:fs";
import { createDirectRoom, installMobileRoutes } from "../api/mobile.js";
const require = createRequire(import.meta.url),
  { PGlite } = require("../api/node_modules/@electric-sql/pglite"),
  Fastify = require("../api/node_modules/fastify");
const A = "00000000-0000-4000-8000-000000000001",
  B = "00000000-0000-4000-8000-000000000002",
  C = "00000000-0000-4000-8000-000000000003",
  R = "00000000-0000-4000-8000-000000000010";
let db, app;
const pool = {
  connect: async () => ({ query: (sql, args) => db.query(sql, args), release() {} }),
  query: async (sql, args) =>
    args ? db.query(sql, args) : { rows: await db.exec(sql) },
};
before(async () => {
  db = new PGlite();
  await db.exec(`
    CREATE TABLE users(id uuid PRIMARY KEY,username text,display_name text,avatar_id uuid);
    CREATE TABLE sessions(id uuid PRIMARY KEY,user_id uuid REFERENCES users(id),expires_at timestamptz DEFAULT now()+interval '1 day');
    CREATE TABLE rooms(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),kind text,title text,created_by uuid);
    CREATE TABLE room_members(room_id uuid REFERENCES rooms(id),user_id uuid REFERENCES users(id),role text,PRIMARY KEY(room_id,user_id));
    CREATE TABLE media(id uuid PRIMARY KEY,kind text,mime text,size_bytes int,thumb_path text,duration_sec int,width int,height int);
    CREATE TABLE messages(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),room_id uuid REFERENCES rooms(id),sender_id uuid REFERENCES users(id),type text,text text,media_id uuid,meta jsonb DEFAULT '{}',reply_to jsonb,encrypted_content text,read_by jsonb DEFAULT '[]',created_at timestamptz DEFAULT now());
    CREATE TABLE reactions(message_id uuid,user_id uuid,emoji text);
    INSERT INTO users(id,username) VALUES('${A}','alice'),('${B}','bob'),('${C}','outsider');
    INSERT INTO rooms(id,kind) VALUES('${R}','group');INSERT INTO room_members VALUES('${R}','${A}','owner'),('${R}','${B}','member');
    INSERT INTO sessions(id,user_id)VALUES('${A}','${A}'),('${B}','${B}');
  `);
  app = Fastify();
  await installMobileRoutes(app, {
    pool,
    requireAuth: async (req) => {
      const id = req.headers["x-user"];
      if (![A, B, C].includes(id))
        throw Object.assign(new Error("unauthorized"), { statusCode: 401 });
      return { id, sessionId: req.headers['x-session'] || id };
    },
    isRoomMember: async (room, user) =>
      (
        await db.query(
          "SELECT 1 FROM room_members WHERE room_id=$1 AND user_id=$2",
          [room, user],
        )
      ).rows.length > 0,
    getRoomRole: async (room, user) =>
      (
        await db.query(
          "SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2",
          [room, user],
        )
      ).rows[0]?.role,
    notifyRoomListUpdate: async () => {},
    broadcastToRoom() {},
  });
  await app.ready();
});
after(async () => {
  await app?.close();
  await db?.close();
});
const call = (url, user = A, method = "GET", payload) =>
  app.inject({ url, method, headers: { "x-user": user }, payload });
test("saved messages reuse one private room and folders remain private to their owner", async () => {
  const first = await call('/me/saved', A, 'POST', {}), second = await call('/me/saved', A, 'POST', {});
  assert.equal(first.statusCode, 200); assert.equal(first.json().id, second.json().id);
  assert.equal((await call(`/rooms/${first.json().id}/history`, B)).statusCode, 404);
  const created = await call('/me/folders', A, 'POST', { title: 'Друзья', room_ids: [R] });
  assert.equal(created.statusCode, 200); const folder = created.json();
  assert.equal((await call('/me/folders', B)).json().length, 0);
  assert.equal((await call('/me/folders/' + folder.id, B, 'PUT', { title: 'Чужая', room_ids: [R] })).statusCode, 404);
  assert.equal((await call('/me/folders', C, 'POST', { title: 'Чужой чат', room_ids: [R] })).statusCode, 403);
  assert.equal((await call('/me/folders', A, 'POST', {title:'Неверная папка',room_ids:{}})).statusCode,400);
  await call('/me/folders/' + folder.id, A, 'DELETE'); assert.equal((await call('/me/folders', A)).json().length, 0);
  assert.equal((await db.query('SELECT count(*)::int AS n FROM rooms WHERE id=$1', [R])).rows[0].n, 1);
});
test("mobile routes require authentication and room membership", async () => {
  assert.equal((await app.inject({ url: "/sync" })).statusCode, 401);
  assert.equal((await call(`/rooms/${R}/history`, C)).statusCode, 404);
  assert.equal(
    (await call(`/rooms/${R}/preferences`, C, "PUT", { pinned: true }))
      .statusCode,
    404,
  );
});
test("production insert SQL deduplicates the same sender/client ID and keeps the original payload", async () => {
  const source = fs.readFileSync(
    new URL("../api/server.js", import.meta.url),
    "utf8",
  );
  const sql = source.match(
    /`(INSERT INTO messages\(room_id, sender_id, type, text, media_id, meta, reply_to, encrypted_content, client_message_id\)[^`]+)`/,
  )[1];
  const args = [
    R,
    A,
    "text",
    "encrypted1",
    null,
    "{}",
    null,
    null,
    "request-0001",
  ];
  const first = await db.query(sql, args);
  args[3] = "must-not-overwrite";
  const second = await db.query(sql, args);
  assert.equal(first.rows.length, 1);
  assert.equal(second.rows.length, 0);
  assert.equal(
    (
      await db.query("SELECT text FROM messages WHERE id=$1", [
        first.rows[0].id,
      ])
    ).rows[0].text,
    "encrypted1",
  );
});
test("history is stable when multiple messages have exactly the same timestamp", async () => {
  for (let i = 20; i < 26; i++)
    await db.query(
      "INSERT INTO messages(id,room_id,sender_id,type,text,created_at) VALUES($1,$2,$3,$4,$5,$6)",
      [
        `00000000-0000-4000-8000-${String(i).padStart(12, "0")}`,
        R,
        B,
        "text",
        `m${i}`,
        "2030-01-01T00:00:00Z",
      ],
    );
  const page = (await call(`/rooms/${R}/history?limit=3`)).json();
  assert.equal(page.messages.length, 3);
  assert.equal(page.has_older, true);
  const previous = (
    await call(`/rooms/${R}/history?limit=3&before=${page.messages[0].id}`)
  ).json();
  assert.equal(previous.messages.length, 3);
  assert.equal(
    new Set([...page.messages, ...previous.messages].map((m) => m.id)).size,
    6,
  );
  assert.equal(
    (await call(`/rooms/${R}/history?around=${page.messages[0].id}`))
      .statusCode,
    200,
  );
});
test("history cursors preserve PostgreSQL microseconds and stop at the newest message", async () => {
  const ids = [40,41,42].map(i=>`00000000-0000-4000-8000-${String(i).padStart(12,'0')}`);
  for (const [index,id] of ids.entries())
    await db.query("INSERT INTO messages(id,room_id,sender_id,type,text,created_at) VALUES($1,$2,$3,'text','precision',$4)",
      [id,R,B,`2031-01-01T00:00:00.123${String(index+1).padStart(3,'0')}Z`]);
  const latest=(await call(`/rooms/${R}/history?limit=2`)).json();
  assert.deepEqual(latest.messages.map(m=>m.id),ids.slice(1));
  assert.equal(latest.has_newer,false);
  const previous=(await call(`/rooms/${R}/history?limit=1&before=${ids[1]}`)).json();
  assert.equal(previous.messages[0].id,ids[0]);
  const next=(await call(`/rooms/${R}/history?limit=1&after=${ids[0]}`)).json();
  assert.equal(next.messages[0].id,ids[1]);
  const end=(await call(`/rooms/${R}/history?after=${ids[2]}`)).json();
  assert.equal(end.messages.length,0);
  assert.equal(end.has_newer,false);
});

test("sync records edits and deletes, but exposes no messages from rooms without membership", async () => {
  const initial = (await call("/sync")).json();
  assert.ok(initial.changes.length);
  let cursor = initial.cursor;
  const { id } = initial.changes.find((x) => x.message)?.message || {};
  await db.query("UPDATE messages SET text=$2 WHERE id=$1", [id, "updated"]);
  let response = (await call(`/sync?cursor=${cursor}`)).json();
  assert.equal(response.changes[0].message.text, "updated");
  cursor = response.cursor;
  await db.query("DELETE FROM messages WHERE id=$1", [id]);
  response = (await call(`/sync?cursor=${cursor}`)).json();
  assert.equal(response.changes[0].operation, "delete");
  assert.equal((await call("/sync", C)).json().changes.length, 0);
});
test("personal hiding never deletes messages or changes another member preferences", async () => {
  const before = (await db.query("SELECT count(*) AS n FROM messages")).rows[0]
    .n;
  assert.equal(
    (
      await call(`/rooms/${R}/preferences`, A, "PUT", {
        hidden: true,
        muted: true,
      })
    ).statusCode,
    200,
  );
  assert.equal((await call("/me/room-preferences", B)).json().length, 0);
  assert.equal(
    (await db.query("SELECT count(*) AS n FROM messages")).rows[0].n,
    before,
  );
  await db.query(
    "INSERT INTO messages(room_id,sender_id,type,text) VALUES($1,$2,'text','new')",
    [R, B],
  );
  assert.equal((await call("/me/room-preferences")).json()[0].hidden, false);
});
test("only owners/admins issue invites, revoked invites cannot be used", async () => {
  assert.equal(
    (await call(`/rooms/${R}/invites`, B, "POST", {})).statusCode,
    403,
  );
  const response = (await call(`/rooms/${R}/invites`, A, "POST", {})).json();
  assert.ok(response.token);
  assert.equal(
    (await call(`/invites/${response.token}/join`, C, "POST", {})).statusCode,
    200,
  );
  await call(`/rooms/${R}/invites/${response.token}`, A, "DELETE");
  assert.equal(
    (await call(`/invites/${response.token}/join`, C, "POST", {})).statusCode,
    404,
  );
});
test("push registrations are scoped to sessions and cannot be deleted by another account", async () => {
  assert.equal(
    (
      await call("/me/push-devices/device-00001", A, "PUT", {
        token: "fcm-test",
      })
    ).statusCode,
    200,
  );
  await call("/me/push-devices/device-00001", B, "DELETE");
  assert.equal((await db.query("SELECT * FROM push_devices")).rows.length, 1);
  const newer='00000000-0000-4000-8000-000000000099';
  await db.query('INSERT INTO sessions(id,user_id) VALUES($1,$2)',[newer,A]);
  await db.query('UPDATE push_devices SET session_id=$1 WHERE user_id=$2',[newer,A]);
  await call('/me/push-devices/device-00001', A, 'DELETE');
  assert.equal((await db.query('SELECT * FROM push_devices')).rows.length,1,'late logout of an older session must preserve the new registration');
  await app.inject({url:'/me/push-devices/device-00001',method:'DELETE',headers:{'x-user':A,'x-session':newer}});
  assert.equal((await db.query('SELECT * FROM push_devices')).rows.length,0);
  await call('/me/push-devices/device-00001', A, 'PUT', {token:'fcm-test'});
  await db.query("DELETE FROM sessions WHERE id=$1", [A]);
  assert.equal((await db.query("SELECT * FROM push_devices")).rows.length, 0);
});

test("opening the same direct conversation in either direction reuses the room", async () => {
  const poolWithClient = {
    connect: async () => ({
      query: (sql, args) => pool.query(sql, args),
      release() {},
    }),
  };
  const first = await createDirectRoom(poolWithClient, A, B);
  const reverse = await createDirectRoom(poolWithClient, B, A);
  assert.equal(first, reverse);
  assert.equal(
    (await db.query("SELECT * FROM room_members WHERE room_id=$1", [first]))
      .rows.length,
    2,
  );
  assert.equal(
    (await call("/mobile/capabilities")).json().idempotent_send,
    true,
  );
});

test('shared media pages encrypted photos without gaps at equal timestamps and checks membership',async()=>{
  const mediaId='00000000-0000-4000-8000-000000000055',room='00000000-0000-4000-8000-000000000056';
  await db.query("INSERT INTO rooms(id,kind,created_by)VALUES($1,'group',$2)",[room,A]);await db.query("INSERT INTO room_members VALUES($1,$2,'owner')",[room,A]);
  await db.query("INSERT INTO media(id,kind,mime,size_bytes)VALUES($1,'file','application/octet-stream',16)",[mediaId]);
  await db.query(`INSERT INTO messages(room_id,sender_id,type,media_id,meta,created_at)
    SELECT $1,$2,'media',$3,'{"media_encryption":{"mime":"image/png"}}','2024-01-01T00:00:00Z' FROM generate_series(1,105)`,[room,A,mediaId]);
  const first=(await call(`/rooms/${room}/media?kind=image`)).json();assert.equal(first.length,100);
  const second=(await call(`/rooms/${room}/media?kind=image&before=${first.at(-1).id}`)).json();assert.equal(second.length,5);
  assert.equal(new Set([...first,...second].map(m=>m.id)).size,105);
  assert.equal((await call(`/rooms/${room}/media`,C)).statusCode,404);
});
