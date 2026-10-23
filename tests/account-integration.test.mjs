import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";
import {
  applyAccountMigrations,
  installAccountIntegration,
  createSessionIssuer,
} from "../api/account-integration.js";
const require = createRequire(import.meta.url),
  { PGlite } = require("../api/node_modules/@electric-sql/pglite"),
  Fastify = require("../api/node_modules/fastify");
const A = "00000000-0000-4000-8000-000000000001",
  B = "00000000-0000-4000-8000-000000000002",
  C = "00000000-0000-4000-8000-000000000003";
let db, app, account, curve, pairA, pairB;
let onRevokeSession=async()=>{},onRevokeUser=async()=>{};
const serialized = (pair) =>
  Buffer.concat([Buffer.from([5]), Buffer.from(pair.pubKey)]).toString(
    "base64",
  );
async function proofFor(user, session, device, pair) {
  const res = await app.inject({
    method: "POST",
    url: "/e2e/device-proof",
    headers: { "x-user": user, "x-session": session },
    payload: { device_id: device, identity_key: serialized(pair) },
  });
  assert.equal(res.statusCode, 200, res.body);
  const challenge = res.json();
  return {
    id: challenge.id,
    signature: Buffer.from(
      curve.sign(
        pair.privKey,
        new TextEncoder().encode(challenge.challenge).buffer,
      ),
    ).toString("base64"),
  };
}
const pool = {
  query: (s, a) =>
    a === undefined && s.includes(";")
      ? db.exec(s).then((rows) => ({ rows }))
      : db.query(s, a),
  connect: async () => ({ query: (s, a) => db.query(s, a), release() {} }),
};
before(async () => {
  curve =
    await require("../mobile/node_modules/@privacyresearch/curve25519-typescript").Curve25519Wrapper.create();
  pairA = curve.keyPair(new Uint8Array(32).fill(42).buffer);
  pairB = curve.keyPair(new Uint8Array(32).fill(17).buffer);
  db = new PGlite();
  await db.exec(
    `CREATE TABLE users(id uuid PRIMARY KEY,username text,password_hash text,banned boolean DEFAULT false,login_disabled_at timestamptz,merged_into_user_id uuid);CREATE TABLE sessions(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),user_id uuid REFERENCES users(id),token_hash text,expires_at timestamptz,device_name text,device_type text,ip_address text,user_agent text,last_activity timestamptz);CREATE TABLE user_devices(id uuid PRIMARY KEY,user_id uuid REFERENCES users(id),device_id text,public_key text,UNIQUE(user_id,device_id));CREATE TABLE user_blocks(blocker_id uuid,blocked_id uuid);CREATE TABLE rooms(id uuid PRIMARY KEY,created_by uuid REFERENCES users(id),avatar_id uuid,pinned_by uuid REFERENCES users(id));CREATE TABLE room_members(room_id uuid REFERENCES rooms(id),user_id uuid REFERENCES users(id),role text,PRIMARY KEY(room_id,user_id));CREATE TABLE messages(id uuid PRIMARY KEY,room_id uuid REFERENCES rooms(id),sender_id uuid REFERENCES users(id),media_id uuid,text text,encrypted_content text,meta jsonb,read_by jsonb,listened_by jsonb);CREATE TABLE media(id uuid PRIMARY KEY,owner_id uuid REFERENCES users(id),path text,thumb_path text,s3_path text);CREATE TABLE arbitrary_personal_data(user_id uuid REFERENCES users(id),secret text);CREATE TABLE message_edit_history(id serial PRIMARY KEY,message_id uuid REFERENCES messages(id),encrypted_content text);INSERT INTO users(id,username,password_hash)VALUES('${A}','alice','pw'),('${B}','bob','pw'),('${C}','charlie','pw');INSERT INTO sessions(id,user_id,expires_at)VALUES('${A}','${A}',now()+interval '1 day'),('${B}','${B}',now()+interval '1 day'),('${C}','${C}',now()+interval '1 day');INSERT INTO user_devices VALUES('${A}','${A}','device-a','${serialized(pairA)}'),('${B}','${B}','device-b','${serialized(pairB)}');`,
  );
  await applyAccountMigrations(pool);
  app = Fastify();
  account = await installAccountIntegration(app, {
    pool,
    securityKey: Buffer.alloc(32, 7),
    requireAuth: async (req) => {
      const me = {
        id: req.headers["x-user"],
        sessionId: req.headers["x-session"] || req.headers["x-user"],
      };
      if (!me.id)
        throw Object.assign(Error("unauthorized"), { statusCode: 401 });
      req.accountViewer = me;
      return me;
    },
    verifyPassword: async (a, b) => a === b,
    issueSession: async () => ({ token: "issued" }),
    revokeUserSessions: async id=>onRevokeUser(id),
    revokeSession:async(user,session)=>onRevokeSession(user,session),
  });
  app.get("/sample", async (req) => {
    await account.requireSession({ id: A, sessionId: A });
    req.accountViewer = { id: A };
    return {
      members: [{ id: B, last_seen: "yesterday", online: true }],
      messages: [{ sender_id: B, read_by: [A, B], listened_by: [B] }],
    };
  });
  await app.ready();
});
after(async () => {
  await app?.close();
  await db?.close();
});

test('push disable is account owned and removes its installation across an older session',async()=>{
 await db.exec('CREATE TABLE push_devices(installation_id text PRIMARY KEY,user_id uuid REFERENCES users(id),session_id uuid)');
 await db.query('INSERT INTO push_devices VALUES($1,$2,$3),($4,$5,$6)',['own-installation',A,C,'other-installation',B,B]);
 for(const[id,removed]of[['other-installation',0],['own-installation',1]]){const res=await app.inject({method:'POST',url:'/me/account/push-disable',headers:{'x-user':A},payload:{installation_id:id}});assert.equal(res.statusCode,200,res.body);assert.equal(res.json().removed,removed);}
 assert.equal((await db.query('SELECT * FROM push_devices')).rows[0].user_id,B);
});
test("device binding follows authenticated publication and cannot bind somebody elses device or replace a session binding", async () => {
  await account.bindPublishedDevice(
    { id: A, sessionId: A },
    "device-a",
    null,
    await proofFor(A, A, "device-a", pairA),
  );
  assert.equal(
    await account.getSessionDevice({ id: A, sessionId: A }),
    "device-a",
  );
  assert.equal(await account.getSessionDevice({ id: B, sessionId: A }), null);
  await assert.rejects(
    account.bindPublishedDevice({ id: A, sessionId: A }, "device-b"),
    /device_not_owned/,
  );
  await db.query("INSERT INTO user_devices VALUES($1,$2,$3,$4)", [
    C,
    A,
    "device-second",
    "second-key",
  ]);
  await assert.rejects(
    account.bindPublishedDevice({ id: A, sessionId: A }, "device-second"),
    /session_device_already_bound/,
  );
});
test("HTTP serialization suppresses derived online and mutual receipt identities", async () => {
  await db.query(
    "INSERT INTO account_privacy(user_id,last_seen,read_receipts)VALUES($1,'nobody',false)",
    [B],
  );
  const res = await app.inject("/sample");
  assert.equal(res.statusCode, 200);
  assert.equal(res.json().members[0].online, false);
  assert.equal(res.json().members[0].last_seen, null);
  assert.deepEqual(res.json().messages[0].read_by, [A]);
  assert.deepEqual(res.json().messages[0].listened_by, []);
  assert.equal(
    await account.filterPayload(A, {
      type: "messages.read",
      user_id: B,
      message_ids: [A],
    }),
    null,
  );
});
test("revoked sessions fail every subsequent websocket frame DB check", async () => {
  await account.requireSession({ id: C, sessionId: C });
  await db.query("DELETE FROM sessions WHERE id=$1", [C]);
  await assert.rejects(
    account.requireSession({ id: C, sessionId: C }),
    /unauthorized/,
  );
});
test("purge discovers ancillary personal tables, keeps others shared message and queues only unshared media", async () => {
  await db.query("INSERT INTO rooms(id,created_by)VALUES($1,$2)", [A, A]);
  await db.query(
    "INSERT INTO room_members VALUES($1,$2,'owner'),($1,$3,'member')",
    [A, A, B],
  );
  await db.query(
    "INSERT INTO media VALUES($1,$2,'private.jpg',NULL,NULL),($3,$2,'shared.jpg',NULL,NULL)",
    [A, A, B],
  );
  await db.query(
    "INSERT INTO messages VALUES($1,$2,$3,$4,'own',NULL,'{}','[]','[]'),($5,$2,$6,$7,'other persons content',NULL,'{}','[]','[]')",
    [A, A, A, A, B, B, B],
  );
  await db.query("INSERT INTO arbitrary_personal_data VALUES($1,'secret')", [
    A,
  ]);
  await db.query(
    "INSERT INTO message_edit_history(message_id,encrypted_content)VALUES($1,'old-own-ciphertext')",
    [A],
  );
  const client = await pool.connect();
  await client.query("BEGIN");
  await account.purgeAccountData(client, A);
  await client.query("COMMIT");
  assert.equal(
    (await db.query("SELECT * FROM arbitrary_personal_data")).rows.length,
    0,
  );
  assert.equal(
    (await db.query("SELECT * FROM message_edit_history")).rows.length,
    0,
  );
  assert.equal(
    (await db.query("SELECT text FROM messages WHERE id=$1", [B])).rows[0].text,
    "other persons content",
  );
  assert.equal((await db.query("SELECT * FROM rooms")).rows.length, 1);
  assert.equal(
    (await db.query("SELECT * FROM account_media_purge_jobs")).rows.length,
    1,
  );
  assert.equal(
    (await db.query("SELECT * FROM media WHERE id=$1", [B])).rows.length,
    1,
  );
});
test("shared session issuer refuses a disabled account before persisting any token", async () => {
  const issue = createSessionIssuer({
    pool,
    ttlDays: 30,
    wantsBearer: () => true,
    parseDevice: () => ({ deviceName: "test", deviceType: "test" }),
    getClientIp: () => "",
    setCookie: () => {},
  });
  await db.query("UPDATE users SET banned=true WHERE id=$1", [C]);
  await assert.rejects(
    issue({ id: C }, { headers: {} }, {}),
    /account_disabled/,
  );
  const session = await issue({ id: B }, { headers: {} }, {});
  assert.ok(session.token);
  assert.equal(
    (
      await db.query(
        "SELECT * FROM sessions WHERE token_hash IS NOT NULL AND user_id=$1",
        [B],
      )
    ).rows.length,
    1,
  );
});

test("actual server login handler cannot issue a session before its configured factor gate", async () => {
  const ts = require("../mobile/node_modules/typescript"),
    vm = await import("node:vm"),
    code = await readFile(new URL("../api/server.js", import.meta.url), "utf8"),
    source = ts.createSourceFile(
      "server.js",
      code,
      ts.ScriptTarget.Latest,
      true,
      ts.ScriptKind.JS,
    );
  let handler;
  function visit(node) {
    if (
      ts.isCallExpression(node) &&
      node.expression.getText(source) === "app.post" &&
      node.arguments[0]?.text === "/auth/login"
    )
      handler = node.arguments[2];
    ts.forEachChild(node, visit);
  }
  visit(source);
  assert.ok(handler);
  const issuer = createSessionIssuer({
    pool,
    wantsBearer: () => true,
    parseDevice: () => ({ deviceName: "test", deviceType: "mobile" }),
    getClientIp: () => null,
    setCookie: () => {},
  });
  const login = vm.runInNewContext("(" + handler.getText(source) + ")", {
    pool,
    account,
    issueAccountSession: issuer,
    verifyPasswordHash: async (hash, password) => hash === password,
    upgradeLegacyPasswordIfNeeded: async () => {},
    parseUserAgent: () => ({ deviceName: "test" }),
    getClientIp: () => null,
  });
  const reply = () => ({
    statusCode: 200,
    code(n) {
      this.statusCode = n;
      return this;
    },
    send(body) {
      return body;
    },
  });
  let r = reply();
  const first = await login(
    { body: { username: "bob", password: "pw" }, headers: {} },
    r,
  );
  assert.ok(first.token);
  const setup = (
    await app.inject({
      method: "POST",
      url: "/me/account/totp/setup",
      headers: { "x-user": B },
      payload: { password: "pw" },
    })
  ).json();
  const { totpCode } = await import("../api/account-features.js");
  assert.ok(setup.secret);
  await app.inject({
    method: "POST",
    url: "/me/account/totp/enable",
    headers: { "x-user": B },
    payload: { code: totpCode(setup.secret) },
  });
  const before = Number(
    (await db.query("SELECT count(*)AS n FROM sessions WHERE user_id=$1", [B]))
      .rows[0].n,
  );
  r = reply();
  const challenge = await login(
    { body: { username: "bob", password: "pw" }, headers: {} },
    r,
  );
  assert.equal(r.statusCode, 202);
  assert.equal(challenge.error, "account_challenge_required");
  assert.equal(challenge.token, undefined);
  assert.ok(challenge.challenge.secret);
  assert.equal(
    Number(
      (
        await db.query("SELECT count(*)AS n FROM sessions WHERE user_id=$1", [
          B,
        ])
      ).rows[0].n,
    ),
    before,
  );
});

test("actual key publication rolls back both bundle and device binding if prekey storage fails", async () => {
  await db.exec(
    `CREATE TABLE user_identity_keys(user_id uuid PRIMARY KEY,public_key text,updated_at timestamptz);ALTER TABLE user_devices ALTER COLUMN id SET DEFAULT gen_random_uuid();ALTER TABLE user_devices ADD COLUMN signed_pre_key_id int;ALTER TABLE user_devices ADD COLUMN signed_pre_key_public text;ALTER TABLE user_devices ADD COLUMN signed_pre_key_signature text;ALTER TABLE user_devices ADD COLUMN registration_id int;CREATE TABLE user_prekeys(device_id uuid,key_id int NOT NULL,public_key text,UNIQUE(device_id,key_id));`,
  );
  const ts = require("../mobile/node_modules/typescript"),
    vm = await import("node:vm"),
    code = await readFile(new URL("../api/server.js", import.meta.url), "utf8"),
    source = ts.createSourceFile(
      "server.js",
      code,
      ts.ScriptTarget.Latest,
      true,
      ts.ScriptKind.JS,
    );
  let handler;
  function visit(node) {
    if (
      ts.isCallExpression(node) &&
      node.expression.getText(source) === "app.post" &&
      node.arguments[0]?.text === "/e2e/keys"
    )
      handler = node.arguments[2];
    ts.forEachChild(node, visit);
  }
  visit(source);
  assert.ok(handler);
  const publish = vm.runInNewContext("(" + handler.getText(source) + ")", {
    pool,
    account,
    requireAuth: async () => ({ id: B, sessionId: B }),
    MAX_E2E_DEVICE_ID_CHARS: 64,
    MAX_E2E_KEY_CHARS: 1024,
    MAX_E2E_PREKEYS_PER_UPLOAD: 100,
    hitUserLimit: () => 0,
  });
  await assert.rejects(
    publish(
      {
        body: {
          device_id: "device-b",
          identity_key: serialized(pairB),
          device_proof: await proofFor(B, B, "device-b", pairB),
          signed_pre_key: {
            key_id: 1,
            public_key: "signed",
            signature: "signature",
          },
          pre_keys: [{ key_id: "not-an-integer", public_key: "key" }],
        },
      },
      {
        code() {
          return this;
        },
        send(body) {
          return body;
        },
      },
    ),
    /invalid input syntax/,
  );
  assert.equal(
    (await db.query("SELECT public_key FROM user_devices WHERE id=$1", [B]))
      .rows[0].public_key,
    serialized(pairB),
  );
  assert.equal(
    (
      await db.query(
        "SELECT * FROM account_session_devices WHERE session_id=$1",
        [B],
      )
    ).rows.length,
    0,
  );
  assert.equal(
    (await db.query("SELECT * FROM user_identity_keys WHERE user_id=$1", [B]))
      .rows.length,
    0,
  );
});

test("account deletion erases edit history after the real conversation capture trigger runs", async () => {
  const D = "00000000-0000-4000-8000-000000000004",
    M = "00000000-0000-4000-8000-000000000005";
  await db.exec(
    `ALTER TABLE users ADD COLUMN display_name text;ALTER TABLE users ADD COLUMN avatar_id uuid;ALTER TABLE users ADD COLUMN profile_tagline text;ALTER TABLE users ADD COLUMN fcm_token text;ALTER TABLE users ADD COLUMN last_seen timestamptz;`,
  );
  const migration = await readFile(
      new URL("../api/conversation-features.sql", import.meta.url),
      "utf8",
    ),
    start = migration.indexOf(
      "CREATE OR REPLACE FUNCTION conversation_capture_edit()",
    ),
    end = migration.indexOf(
      "CREATE OR REPLACE FUNCTION conversation_start_self_destruct()",
      start,
    );
  await db.exec(migration.slice(start, end));
  await db.query(
    "INSERT INTO users(id,username,password_hash)VALUES($1,'deleting','pw')",
    [D],
  );
  await db.query(
    "INSERT INTO sessions(id,user_id,expires_at)VALUES($1,$1,now()+interval '1 day')",
    [D],
  );
  await db.exec("CREATE TABLE group_call_leases(user_id uuid REFERENCES users(id),room_id uuid,lease_id uuid,session_id uuid,revoking_at timestamptz)");
  await db.query('INSERT INTO group_call_leases VALUES($1,$2,$3,$1,NULL)',[D,A,M]);
  let kicked=false;onRevokeUser=async user=>{if(user!==D)return;const rows=(await db.query('SELECT * FROM group_call_leases WHERE user_id=$1',[user])).rows;assert.equal(rows.length,1,'SFU kick record must survive account purge');assert.ok(rows[0].revoking_at);assert.equal(rows[0].lease_id,M);assert.equal((await db.query('SELECT * FROM sessions WHERE user_id=$1',[D])).rows.length,0);kicked=true;await db.query('DELETE FROM group_call_leases WHERE user_id=$1',[user]);};
  await db.query(
    "INSERT INTO messages(id,room_id,sender_id,text,encrypted_content,meta,read_by,listened_by)VALUES($1,$2,$3,NULL,'private-before-delete','{}','[]','[]')",
    [M, A, D],
  );
  const result = await app.inject({
    method: "POST",
    url: "/me/account/delete",
    headers: { "x-user": D },
    payload: { password: "pw" },
  });
  assert.equal(result.statusCode, 200, result.body);
  assert.equal(kicked,true);onRevokeUser=async()=>{};
  assert.equal(
    (
      await db.query("SELECT * FROM message_edit_history WHERE message_id=$1", [
        M,
      ])
    ).rows.length,
    0,
  );
  assert.equal(
    (await db.query("SELECT encrypted_content FROM messages WHERE id=$1", [M]))
      .rows[0].encrypted_content,
    null,
  );
});

test('backup abort revokes only the committed staged session sockets/call hooks after its database deletion',async()=>{
 const staged=crypto.randomUUID();await db.query("INSERT INTO sessions(id,user_id,expires_at)VALUES($1,$2,now()+interval '1 day')",[staged,A]);
 const preflight=await app.inject({method:'POST',url:'/me/account/backup/preflight',headers:{'x-user':A,'x-session':staged},payload:{device_id:'184',identity_key:serialized(pairA)}});assert.equal(preflight.statusCode,200,preflight.body);
 const {createRequire}=await import('node:module'),require=createRequire(import.meta.url),ts=require('../mobile/node_modules/typescript');const source=ts.createSourceFile('server.js',await readFile(new URL('../api/server.js',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true);const fn=source.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='revokeUserSockets');
 const stagedSocket={accountSessionId:staged,close(){this.closed=true;}},workingSocket={accountSessionId:A,close(){this.closed=true;}},sockets=new Map([[A,new Set([stagedSocket,workingSocket])]]),calls=[];const revoke=new Function('socketsByUser','leaveAllRooms','removeSocket','relayCall',fn.getText(source)+';return revokeUserSockets;')(sockets,ws=>{ws.unsubscribed=true;},(user,ws)=>sockets.get(user).delete(ws),{revokeSession:async(user,id)=>calls.push([user,id])});
 onRevokeSession=async(user,id)=>{assert.equal((await db.query('SELECT * FROM sessions WHERE id=$1',[id])).rows.length,0);await revoke(user,null,id);};
 const result=await app.inject({method:'POST',url:'/me/account/backup/abort',headers:{'x-user':A},payload:{restore_id:staged}});assert.equal(result.statusCode,200,result.body);assert.equal(stagedSocket.closed,true);assert.equal(stagedSocket.unsubscribed,true);assert.equal(workingSocket.closed,undefined);assert.deepEqual(calls,[[A,staged]]);onRevokeSession=async()=>{};
});

test('a copied device bundle cannot read or acknowledge an existing secret mailbox from a new session',async()=>{const E='00000000-0000-4000-8000-000000000006',F='00000000-0000-4000-8000-000000000007',CHAT='00000000-0000-4000-8000-000000000008';await db.query("INSERT INTO users(id,username,password_hash)VALUES($1,'owner-proof','pw')",[E]);await db.query("INSERT INTO sessions(id,user_id,expires_at)VALUES($1,$1,now()+interval '1 day'),($2,$1,now()+interval '1 day')",[E,F]);await db.query('INSERT INTO user_devices(id,user_id,device_id,public_key)VALUES($1,$1,$2,$3)',[E,'proven-device',serialized(pairA)]);const proof=await proofFor(E,E,'proven-device',pairA);await account.bindPublishedDevice({id:E,sessionId:E},'proven-device',null,proof);await assert.rejects(account.bindPublishedDevice({id:E,sessionId:F},'proven-device'),/device_proof_required/);await assert.rejects(account.bindPublishedDevice({id:E,sessionId:F},'proven-device',null,proof),/invalid_device_proof/);const fresh=await proofFor(E,F,'proven-device',pairA);await assert.rejects(account.bindPublishedDevice({id:E,sessionId:F},'proven-device',null,{...fresh,signature:Buffer.alloc(64).toString('base64')}),/invalid_device_proof/);await db.query("UPDATE account_device_proofs SET expires_at=now()-interval '1 second' WHERE id=$1",[fresh.id]);await assert.rejects(account.bindPublishedDevice({id:E,sessionId:F},'proven-device',null,fresh),/invalid_device_proof/);assert.equal(await account.getSessionDevice({id:E,sessionId:F}),null);await db.query('INSERT INTO account_secret_chats(id,initiator,recipient,initiator_device,recipient_device,ttl)VALUES($1,$2,$3,$4,$5,60)',[CHAT,E,B,'proven-device','device-b']);const row=(await db.query("INSERT INTO account_secret_mailbox(chat_id,sender_id,nonce,ciphertext,expires_at)VALUES($1,$2,'one',$3,now()+interval '1 minute')RETURNING id",[CHAT,B,'A'.repeat(44)])).rows[0];for(const[method,url,body]of[['GET',`/me/account/secret-chats/${CHAT}/messages?device_id=proven-device`,undefined],['POST',`/me/account/secret-chats/${CHAT}/ack`,{device_id:'proven-device',ids:[row.id]}]]){const res=await app.inject({method,url,payload:body,headers:{'x-user':E,'x-session':F}});assert.equal(res.statusCode,403,res.body);}assert.equal((await db.query('SELECT * FROM account_secret_mailbox WHERE id=$1',[row.id])).rows.length,1);});
