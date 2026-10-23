import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import fs from "node:fs";
import { installPushPolicy } from "../api/account-push-policy.js";
import { createDirectRoom, installMobileRoutes } from "../api/mobile.js";
const require = createRequire(import.meta.url),
  { PGlite } = require("../api/node_modules/@electric-sql/pglite"),
  Fastify = require("../api/node_modules/fastify");
const A = "00000000-0000-4000-8000-000000000001",
  B = "00000000-0000-4000-8000-000000000002",
  C = "00000000-0000-4000-8000-000000000003",
  R = "00000000-0000-4000-8000-000000000010";
let db, app;
const queryLog=[];
const pool = {
  connect: async () => ({ query: (sql, args) => db.query(sql, args), release() {} }),
  query: async (sql, args) => {
    queryLog.push(sql);
    return args ? db.query(sql, args) : { rows: await db.exec(sql) };
  },
};
before(async () => {
  db = new PGlite();
  await db.exec(`
    CREATE TABLE users(id uuid PRIMARY KEY,username text,display_name text,avatar_id uuid);
    CREATE TABLE sessions(id uuid PRIMARY KEY,user_id uuid REFERENCES users(id),expires_at timestamptz DEFAULT now()+interval '1 day');
    CREATE TABLE rooms(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),kind text,title text,created_by uuid);
    CREATE TABLE room_members(room_id uuid REFERENCES rooms(id),user_id uuid REFERENCES users(id),role text,PRIMARY KEY(room_id,user_id));
    CREATE TABLE media(id uuid PRIMARY KEY,kind text,mime text,size_bytes int,thumb_path text,duration_sec int,width int,height int);
    CREATE TABLE messages(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),room_id uuid REFERENCES rooms(id),sender_id uuid REFERENCES users(id),type text,text text,media_id uuid,meta jsonb DEFAULT '{}',reply_to jsonb,encrypted_content text,read_by jsonb DEFAULT '[]',is_post boolean DEFAULT false,created_at timestamptz DEFAULT now());
    CREATE TABLE reactions(message_id uuid,user_id uuid,emoji text);
    INSERT INTO users(id,username) VALUES('${A}','alice'),('${B}','bob'),('${C}','outsider');
    INSERT INTO rooms(id,kind) VALUES('${R}','group');INSERT INTO room_members VALUES('${R}','${A}','owner'),('${R}','${B}','member');
    INSERT INTO sessions(id,user_id)VALUES('${A}','${A}'),('${B}','${B}');
  `);
  await db.exec(`CREATE TABLE push_devices(installation_id text PRIMARY KEY,user_id uuid REFERENCES users(id)ON DELETE CASCADE,session_id uuid REFERENCES sessions(id)ON DELETE CASCADE,token text NOT NULL,active_room_id uuid,updated_at timestamptz NOT NULL DEFAULT now());INSERT INTO push_devices(installation_id,user_id,session_id,token)VALUES('legacy-migrate','${A}','${A}','legacy-token');`);
  await db.exec(fs.readFileSync(new URL("../api/account-push-policy.sql",import.meta.url),"utf8"));
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
  installPushPolicy(app,{pool,requireAuth:async req=>{const id=req.headers["x-user"];if(![A,B].includes(id))throw Object.assign(Error("unauthorized"),{statusCode:401});return{id,sessionId:req.headers["x-session"]||id};}});
  await app.ready();
});
after(async () => {
  await app?.close();
  await db?.close();
});

test('legacy push migration preserves subscriptions and one installation subscribes multiple accounts',async()=>{
 const initial=(await db.query("SELECT user_id,token FROM push_devices WHERE installation_id='legacy-migrate'")).rows;assert.deepEqual(initial,[{user_id:A,token:'legacy-token'}]);
 const register=(user,token)=>app.inject({url:'/me/push-devices/legacy-migrate',method:'PUT',headers:{'x-user':user},payload:{token}});
 assert.equal((await register(B,'same-physical-token')).statusCode,200);
 assert.equal((await register(A,'same-physical-token')).statusCode,200);
 assert.equal((await db.query("SELECT * FROM push_devices WHERE installation_id='legacy-migrate'")).rows.length,2);
 await app.inject({url:'/me/push-devices/legacy-migrate',method:'DELETE',headers:{'x-user':A}});
 assert.deepEqual((await db.query("SELECT user_id FROM push_devices WHERE installation_id='legacy-migrate'")).rows,[{user_id:B}]);
 await db.query("DELETE FROM push_devices WHERE installation_id='legacy-migrate'");
});

test('native registration shares the installation pause epoch across all account subscriptions',async()=>{
 const id='fenced-installation',secret='a'.repeat(64),path='/me/push-devices/'+id;
 const claim=await app.inject({method:'POST',url:'/me/account/push-installation',headers:{'x-user':A},payload:{installation_id:id,secret}});assert.equal(claim.statusCode,200);assert.equal(claim.json().epoch,1);
 const register=(user,epoch,resume_push)=>app.inject({method:'PUT',url:path,headers:{'x-user':user},payload:{token:'shared-token',installation_secret:secret,installation_epoch:epoch,resume_push}});
 assert.equal((await register(A,1)).statusCode,200);assert.equal((await register(B,1)).statusCode,200);
 const disabled=await app.inject({method:'POST',url:'/auth/account-push-disable',payload:{installation_id:id,secret}});assert.equal(disabled.statusCode,200);assert.equal(disabled.json().epoch,2);assert.equal((await db.query('SELECT * FROM push_devices WHERE installation_id=$1',[id])).rows.length,0);
 assert.equal((await register(A,1,true)).statusCode,409);assert.equal((await app.inject({method:'PUT',url:path,headers:{'x-user':A},payload:{token:'legacy'}})).statusCode,409);
 assert.equal((await register(B,2,true)).statusCode,200);assert.equal((await register(A,1,true)).statusCode,409);
 assert.deepEqual((await db.query('SELECT user_id FROM push_devices WHERE installation_id=$1',[id])).rows,[{user_id:B}]);await db.query('DELETE FROM push_devices WHERE installation_id=$1',[id]);
});

test('a history page fetches all reactions in one query and retains each author',async()=>{
 const mids=[];for(let i=0;i<12;i++){const mid=crypto.randomUUID();mids.push(mid);await db.query("INSERT INTO messages(id,room_id,sender_id,type,text)VALUES($1,$2,$3,'text',$4)",[mid,R,A,'batch-'+i]);await db.query('INSERT INTO reactions(message_id,user_id,emoji)VALUES($1,$2,$3)',[mid,B,'👍']);}
 queryLog.length=0;
 const result=await app.inject({url:`/rooms/${R}/history?limit=100`,headers:{'x-user':A}});assert.equal(result.statusCode,200);
 assert.equal(queryLog.filter(sql=>/FROM reactions\b/.test(sql)).length,1);
 for(const mid of mids)assert.deepEqual(result.json().messages.find(m=>m.id===mid).reactions,{'👍':[B]});
 await db.query('DELETE FROM reactions WHERE message_id=ANY($1::uuid[])',[mids]);await db.query('DELETE FROM messages WHERE id=ANY($1::uuid[])',[mids]);
});

test('listening receipts require room access, exclude personally hidden messages and survive history sync',async()=>{
  const id=crypto.randomUUID();await db.query("INSERT INTO messages(id,room_id,sender_id,type)VALUES($1,$2,$3,'voice')",[id,R,A]);
  const path=`/rooms/${R}/messages/${id}/listened`;
  assert.equal((await app.inject({method:'POST',url:path,headers:{'x-user':C}})).statusCode,404);
  assert.equal((await app.inject({method:'POST',url:path,headers:{'x-user':B}})).statusCode,200);
  assert.equal((await app.inject({method:'POST',url:path,headers:{'x-user':B}})).statusCode,200);
  assert.deepEqual((await db.query('SELECT listened_by FROM messages WHERE id=$1',[id])).rows[0].listened_by,[B]);
  await db.query('INSERT INTO hidden_messages(user_id,message_id)VALUES($1,$2)',[A,id]);
  assert.equal((await app.inject({method:'POST',url:path,headers:{'x-user':A}})).statusCode,404);
  await db.query('DELETE FROM messages WHERE id=$1',[id]);
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

test('personal tombstones filter pages, unread anchors and sync without hiding messages from other members',async()=>{
  await db.exec(`CREATE TABLE IF NOT EXISTS hidden_messages(user_id uuid REFERENCES users(id),message_id uuid REFERENCES messages(id),created_at timestamptz DEFAULT now(),PRIMARY KEY(user_id,message_id));`);
  const id='00000000-0000-4000-8000-000000009991';
  await db.query('INSERT INTO messages(id,room_id,sender_id,type,text) VALUES($1,$2,$3,$4,$5)',[id,R,B,'text','hidden test']);
  await db.query('INSERT INTO hidden_messages(user_id,message_id) VALUES($1,$2)',[A,id]);
  await db.query("INSERT INTO message_changes(room_id,message_id,operation)VALUES($1,$2,'hide')",[R,id]);
  const history=(await call(`/rooms/${R}/history`)).json();
  assert.equal(history.messages.some(m=>m.id===id),false);
  assert.notEqual(history.first_unread_id,id);
  assert.equal((await call(`/rooms/${R}/history?around=${id}`)).statusCode,404);
  assert.equal((await call(`/rooms/${R}/history`,B)).json().messages.some(m=>m.id===id),true);
  const hidden=(await call('/sync')).json().changes.filter(c=>c.message_id===id);
  assert.ok(hidden.length);assert.ok(hidden.every(c=>c.operation==='delete' && c.message===null));
  assert.ok((await call('/sync',B)).json().changes.some(c=>c.message_id===id && c.message));
  await db.query('DELETE FROM hidden_messages WHERE message_id=$1',[id]);
  await db.query('DELETE FROM messages WHERE id=$1',[id]);
});

test('production chat lists include the pinned message and omit hidden previews and unread counts',async()=>{
  await db.exec('ALTER TABLE rooms ADD COLUMN IF NOT EXISTS avatar_id uuid; ALTER TABLE rooms ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now();');
  const room=(await db.query("INSERT INTO rooms(kind)VALUES('dm') RETURNING id")).rows[0].id;
  await db.query("INSERT INTO room_members VALUES($1,$2,'owner'),($1,$3,'member')",[room,A,B]);
  const visible=(await db.query("INSERT INTO messages(room_id,sender_id,type,text,created_at)VALUES($1,$2,'text','visible',now()-interval '1 minute') RETURNING id",[room,B])).rows[0].id;
  const hidden=(await db.query("INSERT INTO messages(room_id,sender_id,type,text)VALUES($1,$2,'text','hidden') RETURNING id",[room,B])).rows[0].id;
  await db.query('INSERT INTO hidden_messages(user_id,message_id)VALUES($1,$2)',[A,hidden]);
  await db.query('UPDATE rooms SET pinned_message_id=$2 WHERE id=$1',[room,visible]);
  const source=fs.readFileSync(new URL('../api/server.js',import.meta.url),'utf8');
  const block=source.slice(source.indexOf('async function notifyRoomListUpdate(userId)'),source.indexOf('async function notifyRoomListUpdateForRoom'));
  const sql=block.match(/`([\s\S]*?)`/)[1];
  const rows=(await db.query(sql,[A,JSON.stringify([A])])).rows;
  const result=rows.find(r=>r.id===room);
  assert.equal(result.pinned_message_id,visible);
  assert.equal(result.last_msg_id,visible);assert.equal(result.unread_count,1);
});

test('hidden attachments are private in media pages and tombstone catch-up',async()=>{
 const mediaId='00000000-0000-4000-8000-000000009992',id='00000000-0000-4000-8000-000000009993';
 await db.query("INSERT INTO media(id,kind,mime)VALUES($1,'image','image/png')",[mediaId]);
 await db.query("INSERT INTO messages(id,room_id,sender_id,type,media_id)VALUES($1,$2,$3,'media',$4)",[id,R,B,mediaId]);
 await db.query('INSERT INTO hidden_messages(user_id,message_id)VALUES($1,$2)',[A,id]);
 assert.equal((await call(`/rooms/${R}/media`)).json().some(m=>m.id===id),false);
 assert.equal((await call(`/rooms/${R}/media`,B)).json().some(m=>m.id===id),true);
 const catchup=await call('/me/hidden-messages');assert.equal(catchup.statusCode,200);
 assert.ok(catchup.json().items.some(m=>m.message_id===id));
 assert.equal((await call('/me/hidden-messages',B)).json().items.some(m=>m.message_id===id),false);
});
test("production insert SQL deduplicates the same sender/client ID and keeps the original payload", async () => {
  const source = fs.readFileSync(
    new URL("../api/server.js", import.meta.url),
    "utf8",
  );
  const sql = source.match(
    /`(INSERT INTO messages\(room_id, sender_id, type, text, media_id, meta, reply_to, encrypted_content, client_message_id,is_post\)[^`]+)`/,
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
    false,
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
  assert.equal((await db.query("SELECT quick_reply FROM push_devices")).rows[0].quick_reply,false,'older APKs keep standard notifications');
  await call('/me/push-devices/device-00001', A, 'PUT', {token:'fcm-test',quick_reply:true});
  assert.equal((await db.query("SELECT quick_reply FROM push_devices")).rows[0].quick_reply,true,'new APK opts into native replies');
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
