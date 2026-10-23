import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";
import {
  installAccountFeatures,
  authGate,
  totpCode,
  canWrite,
  canReadReceipts,
  filterProfile,
} from "../api/account-features.js";
const require = createRequire(import.meta.url),
  { PGlite } = require("../api/node_modules/@electric-sql/pglite"),
  Fastify = require("../api/node_modules/fastify");
const A = "00000000-0000-4000-8000-000000000001",
  B = "00000000-0000-4000-8000-000000000002",
  C = "00000000-0000-4000-8000-000000000003";
let db, app, service;
const key = Buffer.alloc(32, 7),
  events = [];
const pool = {
  query: (s, a) => db.query(s, a),
  connect: async () => ({ query: (s, a) => db.query(s, a), release() {} }),
};
const request = (method, url, body, user = A) =>
  app.inject({
    method,
    url,
    payload: body,
    headers: user ? { "x-user": user, "x-session": user } : {},
  });
before(async () => {
  db = new PGlite();
  await db.exec(
    `CREATE TABLE users(id uuid PRIMARY KEY,username text,password_hash text,display_name text,avatar_id uuid,profile_tagline text,fcm_token text,login_disabled_at timestamptz,banned boolean DEFAULT false,last_seen timestamptz);CREATE TABLE sessions(id uuid PRIMARY KEY,user_id uuid REFERENCES users(id));CREATE TABLE user_blocks(blocker_id uuid,blocked_id uuid,PRIMARY KEY(blocker_id,blocked_id));CREATE TABLE user_devices(id uuid PRIMARY KEY,user_id uuid,device_id text,UNIQUE(user_id,device_id));CREATE TABLE messages(id uuid PRIMARY KEY,sender_id uuid,text text,encrypted_content text,meta jsonb,media_id uuid);CREATE TABLE rooms(id uuid PRIMARY KEY,created_by uuid);CREATE TABLE room_members(room_id uuid,user_id uuid);INSERT INTO users(id,username,password_hash)VALUES('${A}','alice','correct'),('${B}','bob','correct'),('${C}','eve','correct');INSERT INTO sessions VALUES('${A}','${A}'),('${B}','${B}');INSERT INTO user_devices VALUES('${A}','${A}','device-a'),('${B}','${B}','device-b');`,
  );
  await db.exec(
    await readFile(
      new URL("../api/account-features.sql", import.meta.url),
      "utf8",
    ),
  );
  app = Fastify();
  service = await installAccountFeatures(app, {
    pool,
    securityKey: key,
    getSessionDevice: async (me) => (me.id === A ? "device-a" : "device-b"),
    requireAuth: async (r) => {
      if (!r.headers["x-user"])
        throw Object.assign(Error("unauthorized"), { statusCode: 401 });
      return { id: r.headers["x-user"], sessionId: r.headers["x-session"] };
    },
    verifyPassword: async (hash, password) => hash === password,
    issueSession: async (u) => ({ user: { id: u.id }, token: "new-session" }),
    sendToUser: (u, e) => events.push({ u, e }),
    revokeUserSessions: async () => {},
    purgeAccountData: async () => {},
  });
  await app.ready();
});
after(async () => {
  await app?.close();
  await db?.close();
});
test("contacts are account owned; recipient contact rule governs messaging and presence", async () => {
  assert.equal(
    (
      await request(
        "PUT",
        "/me/account/privacy",
        { write: "contacts", last_seen: "contacts" },
        B,
      )
    ).statusCode,
    200,
  );
  assert.equal(await canWrite(pool, A, B), false);
  assert.equal(
    (await request("PUT", `/me/account/contacts/${A}`, { name: "Alice" }, B))
      .statusCode,
    200,
  );
  assert.equal(await canWrite(pool, A, B), true);
  assert.equal(
    (await request("GET", "/me/account/contacts", undefined, A)).json().length,
    0,
  );
  assert.equal(
    (await filterProfile(pool, { id: B, last_seen: "now" }, A)).last_seen,
    "now",
  );
  assert.equal(
    (await filterProfile(pool, { id: B, last_seen: "now" }, C)).last_seen,
    null,
  );
});
test("receipts require both users and blocks override contacts", async () => {
  assert.equal(await canReadReceipts(pool, A, B), true);
  await request("PUT", "/me/account/privacy", { read_receipts: false }, B);
  assert.equal(await canReadReceipts(pool, A, B), false);
  await request("PUT", `/me/account/blocks/${A}`, {}, B);
  assert.equal(await canWrite(pool, A, B), false);
  assert.equal(
    (await request("GET", "/me/account/blocks", undefined, B)).json()[0].id,
    A,
  );
  await request("DELETE", `/me/account/blocks/${A}`, undefined, B);
  assert.equal(await canWrite(pool, A, B), true);
});
test("TOTP RFC vector and enrollment requires password, stores sealed secret, consumes codes", async () => {
  assert.equal(
    totpCode("GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ", 59000, 8),
    "94287082",
  );
  assert.equal(
    (await request("POST", "/me/account/totp/setup", { password: "wrong" }))
      .statusCode,
    403,
  );
  const setup = (
    await request("POST", "/me/account/totp/setup", { password: "correct" })
  ).json();
  assert.ok(setup.secret);
  const result = await request("POST", "/me/account/totp/enable", {
    code: totpCode(setup.secret),
  });
  assert.equal(result.statusCode, 200);
  const codes = result.json().backup_codes;
  assert.equal(codes.length, 10);
  const state = (
    await db.query("SELECT * FROM account_security WHERE user_id=$1", [A])
  ).rows[0];
  assert.equal(JSON.stringify(state).includes(setup.secret), false);
  assert.equal(JSON.stringify(state).includes(codes[0]), false);
  assert.equal(
    (
      await authGate(
        { id: A },
        { pool, securityKey: key, backup_code: codes[0] },
      )
    ).allowed,
    true,
  );
  const repeat = await authGate(
    { id: A },
    { pool, securityKey: key, backup_code: codes[0] },
  );
  assert.equal(repeat.allowed, false);
});
test("password-proven TOTP challenge cannot mint session without valid factor", async () => {
  const gate = await service.authGate({ id: A }, { device_key: "new-device" });
  assert.equal(gate.allowed, false);
  const challenge = gate.challenge;
  assert.equal(
    (
      await request(
        "POST",
        "/auth/account-challenge/complete",
        { ...challenge, secret: "wrong", code: "000000" },
        null,
      )
    ).statusCode,
    403,
  );
  assert.equal(
    (
      await request(
        "POST",
        "/auth/account-challenge/complete",
        { id: challenge.id, secret: challenge.secret, code: "000000" },
        null,
      )
    ).statusCode,
    403,
  );
});

test('waiting for device approval does not consume failed-factor attempts',async()=>{
 await request('GET','/me/account/security',undefined,C);await db.query('UPDATE account_security SET enabled=false,approval_required=true WHERE user_id=$1',[C]);
 const {challenge}=await service.authGate({id:C},{device_key:'new-waiting-device'});
 for(let i=0;i<7;i++){const result=await request('POST','/auth/account-challenge/complete',{id:challenge.id,secret:challenge.secret},null);assert.equal(result.statusCode,409,result.body);}
 await request('POST',`/me/account/challenges/${challenge.id}/approve`,{},C);
 assert.equal((await request('POST','/auth/account-challenge/complete',{id:challenge.id,secret:challenge.secret},null)).statusCode,200);
 await db.query('UPDATE account_security SET approval_required=false WHERE user_id=$1',[C]);
});
test("QR handoff has no session token; requires authenticated approval and is single use", async () => {
  const qr = (await request("POST", "/auth/account-handoff", {}, null)).json();
  assert.ok(qr.id && qr.secret);
  assert.equal(qr.token, undefined);
  assert.equal(
    (
      await request(
        "POST",
        "/auth/account-handoff/claim",
        { id: qr.id, secret: qr.secret },
        null,
      )
    ).statusCode,
    409,
  );
  assert.equal(
    (
      await request(
        "POST",
        `/me/account/handoff/${qr.id}/approve`,
        { approval: qr.approval },
        B,
      )
    ).statusCode,
    200,
  );
  const claim = await request(
    "POST",
    "/auth/account-handoff/claim",
    { id: qr.id, secret: qr.secret },
    null,
  );
  assert.equal(claim.json().user.id, B);
  assert.equal(
    (
      await request(
        "POST",
        "/auth/account-handoff/claim",
        { id: qr.id, secret: qr.secret },
        null,
      )
    ).statusCode,
    403,
  );
});
test("secret mailbox is device bound, ciphertext only, bounded, ack deletes payload", async () => {
  const created = await request("POST", "/me/account/secret-chats", {
    peer_id: B,
    device_id: "device-a",
    peer_device_id: "device-b",
    ttl: 60,
  });
  assert.equal(created.statusCode, 200);
  const id = created.json().id;
  assert.equal(
    (
      await request("POST", `/me/account/secret-chats/${id}/messages`, {
        device_id: "device-a",
        body: "plaintext",
      })
    ).statusCode,
    400,
  );
  assert.equal(
    (
      await request(
        "GET",
        `/me/account/secret-chats/${id}/messages?device_id=device-b`,
        undefined,
        C,
      )
    ).statusCode,
    403,
  );
  const sent = await request(
    "POST",
    `/me/account/secret-chats/${id}/messages`,
    {
      device_id: "device-a",
      ciphertext: "A".repeat(44),
      nonce: "first-message",
    },
  );
  assert.equal(sent.statusCode, 200);
  const inbox = await request(
    "GET",
    `/me/account/secret-chats/${id}/messages?device_id=device-b`,
    undefined,
    B,
  );
  assert.equal(inbox.json().length, 1);
  await request(
    "POST",
    `/me/account/secret-chats/${id}/ack`,
    { device_id: "device-b", ids: [sent.json().id] },
    B,
  );
  assert.equal(
    (
      await request(
        "GET",
        `/me/account/secret-chats/${id}/messages?device_id=device-b`,
        undefined,
        B,
      )
    ).json().length,
    0,
  );
});
test("delete password protects account; tombstone preserves shared room and revokes sessions", async () => {
  await db.query("INSERT INTO rooms VALUES($1,$2)", [A, B]);
  await db.query("INSERT INTO messages VALUES($1,$2,$3,NULL,$4,NULL)", [
    B,
    B,
    "personal",
    {},
  ]);
  assert.equal(
    (await request("POST", "/me/account/delete", { password: "wrong" }, B))
      .statusCode,
    403,
  );
  assert.equal(
    (await request("POST", "/me/account/delete", { password: "correct" }, B))
      .statusCode,
    200,
  );
  assert.equal(
    (await db.query("SELECT * FROM sessions WHERE user_id=$1", [B])).rows
      .length,
    0,
  );
  assert.equal(
    (await db.query("SELECT * FROM rooms WHERE id=$1", [A])).rows.length,
    1,
  );
  assert.equal(
    (await db.query("SELECT text FROM messages WHERE id=$1", [B])).rows[0].text,
    null,
  );
});

test("secret mailbox refuses another device even for the same account", async () => {
  const r = await request("POST", "/me/account/secret-chats", {
    peer_id: C,
    device_id: "device-b",
    peer_device_id: "device-c",
    ttl: 60,
  });
  assert.equal(r.statusCode, 403);
});
test("expired handoff cannot approve or claim", async () => {
  const qr = (await request("POST", "/auth/account-handoff", {}, null)).json();
  await db.query(
    "UPDATE account_auth_challenges SET expires_at=now()-interval '1 second' WHERE id=$1",
    [qr.id],
  );
  assert.equal(
    (
      await request("POST", `/me/account/handoff/${qr.id}/approve`, {
        approval: qr.approval,
      })
    ).statusCode,
    403,
  );
  assert.equal(
    (
      await request(
        "POST",
        "/auth/account-handoff/claim",
        { id: qr.id, secret: qr.secret },
        null,
      )
    ).statusCode,
    403,
  );
});

test("portable encrypted backup authenticates identity, password and ciphertext", async () => {
  const { encryptBackup, decryptBackup } = await import(
    "../web/account-features.js"
  );
  const keys = {
    identityKey: {
      pubKey: new Uint8Array([1, 2]),
      privKey: new Uint8Array([3, 4]),
    },
    registrationId: 5,
    preKeys: {},
    signedPreKeys: {},
    sessions: {},
  };
  const backup = await encryptBackup(keys, "password123", { userId: A });
  assert.equal(JSON.parse(backup).format, "messenger67-e2e-backup");
  const restored = await decryptBackup(backup, "password123", A);
  assert.deepEqual(
    [...new Uint8Array(restored.keys.identityKey.pubKey)],
    [1, 2],
  );
  await assert.rejects(decryptBackup(backup, "wrong", A));
  await assert.rejects(
    decryptBackup(backup, "password123", C),
    /USER_MISMATCH/,
  );
  const corrupt = JSON.parse(backup);
  corrupt.ct = corrupt.ct.slice(0, -8) + "AAAAAAAA";
  await assert.rejects(decryptBackup(corrupt, "password123", A));
});

test("secret session encrypts only after fingerprint verification and rejects replay/expiry", async () => {
  const { SecretSession } = await import("../web/account-features.js");
  const a = new SecretSession("chat-one", "device-a"),
    b = new SecretSession("chat-one", "device-b");
  const ap = await a.offer(),
    bp = await b.offer(),
    fa = await a.accept(bp),
    fb = await b.accept(ap);
  assert.equal(fa, fb);
  await assert.rejects(
    a.encrypt("hello", Date.now() + 10000),
    /код безопасности/,
  );
  a.verify(fa);
  b.verify(fb);
  const ct = await a.encrypt("hello", Date.now() + 10000);
  assert.equal(ct.includes("hello"), false);
  assert.equal((await b.decrypt(ct)).text, "hello");
  await assert.rejects(b.decrypt(ct), /Повтор/);
  a.destroy();
  await assert.rejects(a.encrypt("hello", Date.now() + 10000));
});
test("proxy policy rejects transport that does not prove DNS routing and calls fail closed", async () => {
  const { assertTransport } = await import("../web/account-features.js");
  assert.throws(
    () => assertTransport({ mode: "tor" }, { tor: true, failClosed: true }),
    /прямое соединение/,
  );
  assert.throws(
    () =>
      assertTransport(
        { mode: "tor" },
        { tor: true, failClosed: true, dnsThroughProxy: true },
        "call",
      ),
    /Звонки отключены/,
  );
  assert.doesNotThrow(() => assertTransport({ mode: "direct" }, {}));
});

test("account registry keeps tokens in SecureStore and closes old resources before switch", async () => {
  const ts = require("../mobile/node_modules/typescript"),
    vm = await import("node:vm"),
    source = await readFile(
      new URL("../mobile/src/accountRegistry.ts", import.meta.url),
      "utf8",
    ),
    secure = new Map(),
    metadata = new Map(),
    trace = [];
  const mobileRequire = createRequire(
    new URL("../mobile/package.json", import.meta.url),
  );
  const exports = {};
  const fakeRequire = (name) =>
    name === "expo-secure-store"
      ? {
          getItemAsync: async (k) => secure.get(k) || null,
          setItemAsync: async (k, v) => {
            secure.set(k, v);
          },
          deleteItemAsync: async (k) => {
            secure.delete(k);
          },
          WHEN_UNLOCKED_THIS_DEVICE_ONLY: "device",
        }
      : name === "@react-native-async-storage/async-storage"
        ? {
            __esModule: true,
            default: {
              getItem: async (k) => metadata.get(k) || null,
              setItem: async (k, v) => {
                metadata.set(k, v);
              },
              removeItem: async (k) => {
                metadata.delete(k);
              },
            },
          }
        : name === "expo"
          ? { requireOptionalNativeModule: () => null }
          : mobileRequire(name);
  vm.runInNewContext(
    ts.transpileModule(source, {
      compilerOptions: {
        module: ts.ModuleKind.CommonJS,
        target: ts.ScriptTarget.ES2022,
        esModuleInterop: true,
      },
    }).outputText,
    {
      exports,
      require: fakeRequire,
      crypto,
      TextEncoder,
      TextDecoder,
      Uint8Array,
      ArrayBuffer,
      Map,
      Set,
      URL,
      JSON,
      console,
    },
  );
  const r = new exports.AccountRegistry();
  await r.save(
    { id: "a", baseUrl: "https://example.org", userId: A, username: "alice" },
    "token-a",
  );
  await r.save(
    { id: "b", baseUrl: "https://example.org", userId: C, username: "eve" },
    "token-b",
  );
  assert.equal(JSON.stringify([...metadata]).includes("token-a"), false);
  await r.activate("a", {
    close: async () => trace.push("close"),
    open: async (account, token) => trace.push(token),
  });
  await r.activate("b", {
    close: async () => trace.push("close"),
    open: async (account, token) => trace.push(token),
  });
  assert.deepEqual(trace, ["close", "token-a", "close", "token-b"]);
  assert.equal(await r.token("a"), "token-a");
  await r.remove("a");
  assert.equal(await r.token("a"), null);
});

test("challenge factor then device approval mints exactly one session", async () => {
  const setup = (
    await request("POST", "/me/account/totp/setup", { password: "correct" }, C)
  ).json();
  const codes = (
    await request(
      "POST",
      "/me/account/totp/enable",
      { code: totpCode(setup.secret) },
      C,
    )
  ).json().backup_codes;
  const gate = await service.authGate({ id: C }, { device_key: "c-first" });
  const done = await request(
    "POST",
    "/auth/account-challenge/complete",
    {
      id: gate.challenge.id,
      secret: gate.challenge.secret,
      backup_code: codes[0],
    },
    null,
  );
  assert.equal(done.statusCode, 200);
  assert.equal(done.json().token, "new-session");
  assert.equal(
    (
      await request(
        "POST",
        "/auth/account-challenge/complete",
        {
          id: gate.challenge.id,
          secret: gate.challenge.secret,
          backup_code: codes[1],
        },
        null,
      )
    ).statusCode,
    403,
  );
  assert.equal(
    (
      await request(
        "PUT",
        "/me/account/device-approval",
        {
          password: "correct",
          backup_code: codes[1],
          enabled: true,
          device_key: "c-trusted",
        },
        C,
      )
    ).statusCode,
    200,
  );
  const next = await service.authGate(
    { id: C },
    { device_key: "c-unknown", backup_code: codes[2] },
  );
  assert.equal(next.allowed, false);
  assert.equal(next.challenge.kind, "device");
  assert.equal(
    (
      await request(
        "POST",
        "/auth/account-challenge/complete",
        next.challenge,
        null,
      )
    ).statusCode,
    409,
  );
  assert.equal(
    (
      await request(
        "POST",
        "/me/account/challenges/" + next.challenge.id + "/approve",
        {},
        C,
      )
    ).statusCode,
    200,
  );
  assert.equal(
    (
      await request(
        "POST",
        "/auth/account-challenge/complete",
        next.challenge,
        null,
      )
    ).statusCode,
    200,
  );
});
test("portable codec can import a real crypto.js envelope and crypto.js can import portable envelope", async () => {
  const vm = await import("node:vm"),
    { encryptBackup, decryptBackup } = await import(
      "../web/account-features.js"
    ),
    scope = {
      window: {},
      crypto,
      TextEncoder,
      TextDecoder,
      Uint8Array,
      ArrayBuffer,
      btoa,
      atob,
      console,
    };
  vm.runInNewContext(
    await readFile(new URL("../web/crypto.js", import.meta.url), "utf8"),
    scope,
  );
  const Manager = scope.window.E2EManager,
    manager = Object.create(Manager.prototype),
    keys = {
      identityKey: {
        pubKey: new Uint8Array([1, 2]).buffer,
        privKey: new Uint8Array([3, 4]).buffer,
      },
      registrationId: 5,
      preKeys: { 0: { pubKey: new Uint8Array([8]).buffer } },
      signedPreKeys: {},
      sessions: { sessionAlice: "opaque-js-record" },
      groupKeys: {
        "gk:alice:room:sender": { chainKey: new Uint8Array([6, 7]).buffer },
      },
      mobile_key_state: { "group:key": "retained" },
    };
  manager.ready = true;
  manager.exportKeys = async () => keys;
  const original = await manager.exportKeysBackup("password123", { userId: A });
  const imported = await decryptBackup(original, "password123", A);
  assert.equal(imported.keys.sessions.sessionAlice, "opaque-js-record");
  assert.deepEqual(
    [
      ...new Uint8Array(
        imported.keys.groupKeys["gk:alice:room:sender"].chainKey,
      ),
    ],
    [6, 7],
  );
  let installed;
  manager.importKeys = async (k) => {
    installed = k;
  };
  const portable = await encryptBackup(keys, "password123", { userId: A });
  await manager.importKeysBackup(portable, "password123", { userId: A });
  assert.equal(installed.registrationId, 5);
  assert.deepEqual([...new Uint8Array(installed.identityKey.pubKey)], [1, 2]);
});
