import { test, after } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import vm from "node:vm";
import { webcrypto } from "node:crypto";
import { createRequire } from "node:module";
import { DatabaseSync } from "node:sqlite";
import * as uiI18n from '../web/i18n.js';
const require = createRequire(import.meta.url),
  ts = require("../mobile/node_modules/typescript");
function load(file, deps = {}, globals = {}) {
  const source = fs.readFileSync(new URL(file, import.meta.url), "utf8");
  const exports = {};
  vm.runInNewContext(
    ts.transpileModule(source, {
      compilerOptions: {
        module: ts.ModuleKind.CommonJS,
        target: ts.ScriptTarget.ES2022,
      },
    }).outputText,
    {
      exports,
      crypto: webcrypto,
      AbortController,
      ...globals,
      require(name) {
        if(name.endsWith('/i18n.js'))return uiI18n;
        assert.ok(name in deps, `Unexpected import ${name}`);
        return deps[name];
      },
    },
  );
  return exports;
}
const { Outbox } = load("../mobile/src/data/outbox.ts");

test('cancelling an in-flight media upload aborts it and does not resurrect the queue item',async()=>{
  const items=new Map();let started,fail;const waiting=new Promise(r=>started=r);
  const repo={outgoing:async()=>[...items.values()],putOutgoing:async item=>items.set(item.id,structuredClone(item)),removeOutgoing:async id=>items.delete(id),saveMessages:async()=>{throw Error('cancelled send must not be saved');}};
  items.set('media',{id:'media',roomId:'room-a',text:'caption',state:'queued',upload:{uri:'file:///draft/one.jpg'}});
  let aborted=false;
  const queue=new Outbox(repo,async(item,signal)=>{started();return new Promise((_,reject)=>{fail=reject;signal.addEventListener('abort',()=>{aborted=true;reject(Error('offline'));});});},()=>{});
  const task=queue.flush();await waiting;await queue.cancel('media');fail(Error('offline'));await task;
  assert.equal(aborted,true);assert.equal(items.size,0);
});

test('a queued attachment transfers ownership atomically, survives restart and protects its prepared ciphertext from cache cleanup',async()=>{
  const user='11111111-1111-4111-8111-111111111111',file={id:'stable-attachment',uri:'file:///original.jpg'};
  let repo=new AccountRepository(user);await repo.open();await repo.set('attachments:room-media',{items:[file],caption:'caption'});
  const item={id:file.id,roomId:'room-media',text:'caption',state:'queued',body:{type:'media'},upload:{...file,name:'one.jpg',mime:'image/jpeg',prepared:{uri:'file:///encrypted.bin',meta:{media_encryption:{v:2}}}}};
  await repo.enqueueMedia(item);await repo.enqueueMedia({...item,text:'must not replace the original'});
  assert.equal((await repo.get('attachments:room-media',null)).items.length,0);
  await repo.close();repo=new AccountRepository(user);await repo.open();
  assert.equal((await repo.outgoing()).length,1);assert.equal((await repo.outgoing())[0].text,'caption');
  assert.deepEqual(Array.from(await repo.privateFiles()).sort(),['file:///encrypted.bin','file:///original.jpg']);
  await repo.clearCache();assert.equal((await repo.outgoing()).length,1);await repo.close();
});
const { mergeMessages } = load("../mobile/src/data/messages.ts");
const temp = fs.mkdtempSync(path.join(os.tmpdir(), "messenger67-data-test-"));
const keys = new Map(),
  connections = [],
  events = [];
let cipher = true;
let pauseHiddenRead=null;
const { AccountRepository } = load("../mobile/src/data/repository.ts", {
  "expo-secure-store": {
    getItemAsync: async (key) => keys.get(key) || null,
    setItemAsync: async (key, value) => keys.set(key, value),
  },
  "expo-sqlite": {
    openDatabaseAsync: async (name) => {
      const db = new DatabaseSync(path.join(temp, name));
      connections.push(db);
      return {
        execAsync: async (sql) => {
          events.push(sql.startsWith("PRAGMA key") ? "key" : sql);
          db.exec(sql);
        },
        getFirstAsync: async (sql, ...args) =>
          sql === "PRAGMA cipher_version"
            ? cipher
              ? { cipher_version: "native-adapter-test" }
              : null
            : db.prepare(sql).get(...args),
        getAllAsync: async (sql, ...args) => {
          const rows=db.prepare(sql).all(...args);
          if(sql==='SELECT id FROM hidden_messages_local' && pauseHiddenRead)await pauseHiddenRead();
          return rows;
        },
        runAsync: async (sql, ...args) => db.prepare(sql).run(...args),
        closeAsync: async () => db.close(),
      };
    },
  },
});
after(() => {
  for (const db of connections)
    try {
      db.close();
    } catch {}
  const resolved = fs.realpathSync(temp);
  assert.ok(
    resolved.startsWith(path.resolve(os.tmpdir())) &&
      path.basename(resolved).startsWith("messenger67-data-test-"),
  );
  fs.rmSync(resolved, { recursive: true, force: true });
});
const msg = (id, room_id, text, extra = {}) => ({
  id,
  room_id,
  text,
  sender_id: "alice",
  type: "text",
  created_at: "2026-10-19T00:00:00Z",
  ...extra,
});
test("repository keys the connection before schema creation and refuses a build without SQLCipher", async () => {
  events.length = 0;
  const repo = new AccountRepository("encrypted");
  await repo.open();
  assert.equal(events[0], "key");
  await repo.close();
  cipher = false;
  await assert.rejects(new AccountRepository("unsupported").open(), /APK/);
  cipher = true;
});

test('personal tombstones prevent stale updates from returning to native history and FTS after reopen',async()=>{
 let repo=new AccountRepository('personal-hidden');await repo.open();
 await repo.saveMessages([msg('hidden','room','private text')]);
 assert.equal(typeof repo.hideMessage,'function');await repo.hideMessage('hidden');
 await repo.saveMessages([msg('hidden','room','edited text')]);
 assert.equal((await repo.messages('room')).length,0);assert.equal((await repo.search('edited')).length,0);
 await repo.close();repo=new AccountRepository('personal-hidden');await repo.open();
 await repo.saveMessages([msg('hidden','room','late edit')]);assert.equal((await repo.messages('room')).length,0);
 await repo.close();
});

test('native message handler cannot resurrect a hide queued behind its visibility snapshot',async()=>{
 const repo=new AccountRepository('native-ui-race');await repo.open();let rendered=[msg('hidden','room','before')],resume,started;
 const began=new Promise(r=>started=r);pauseHiddenRead=()=>{started();return new Promise(r=>resume=r);};
 const source=fs.readFileSync(new URL('../mobile/App.tsx',import.meta.url),'utf8'),start=source.indexOf('    if (data.type === "message.new" || data.type === "message.update") {');
 const block=source.slice(start,source.indexOf("    if (data.type === 'sender_key.available'",start));
 const scope={mergeMessages,exports:{},data:{type:'message.update',message:msg('hidden','room','edited')},baseUrl:'test',token:'t',receivingToken:'t',tokenRef:{current:'t'},activeRoomIdRef:{current:'room'},local:{repository:{current:repo}},me:{id:'alice'},prepareMessage:async m=>m,setRooms(){},reorderRooms:x=>x,updateRoomPreview:x=>x,setMessages:f=>rendered=typeof f==='function'?f(rendered):f};
 vm.runInNewContext(ts.transpileModule('async function receive(){'+block+'}\nexports.receive=receive;', {compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText,scope);
 const incoming=scope.exports.receive();await began;const hiding=repo.hideMessage('hidden');rendered=[];pauseHiddenRead=null;resume();await incoming;await hiding;
 assert.equal(rendered.length,0);await repo.close();
});

test('FTS history is finalized once when logout and cleanup close the same repository together',async()=>{
 const repo=new AccountRepository('close-once');await repo.open();await repo.saveMessages([msg('close-message','room','Searchable history')]);await repo.search('Searchable');
 await Promise.all([repo.close(),repo.close(),repo.close()]);await assert.rejects(repo.get('rooms',[]),/закрыто/);
});
test("drafts, pending messages and positions survive reopen and stay isolated between accounts", async () => {
  let alice = new AccountRepository("alice");
  const bob = new AccountRepository("bob");
  await alice.set("draft:room1", { text: "Черновик", reply: { id: "m0" } });
  await alice.set("position:room1", "m1");
  await alice.putOutgoing({
    id: "req1",
    roomId: "room1",
    text: "Не отправлено",
    state: "queued",
  });
  assert.equal(await bob.get("draft:room1", null), null);
  await alice.close();
  alice = new AccountRepository("alice");
  assert.equal((await alice.get("draft:room1", null)).text, "Черновик");
  assert.equal((await alice.outgoing()).length, 1);
  assert.equal(await alice.get("position:room1", null), "m1");
  assert.notEqual(keys.get("account-alice"), keys.get("account-bob"));
  await alice.close();
  await bob.close();
});

test('upgrade preserves group key state and partial attachment queue; malformed backup cannot overwrite keys',async()=>{
 const user='upgrade-key-state',room='11111111-1111-4111-8111-111111111111',key='group:'+room+':own';let repo=new AccountRepository(user);
 const keys={chain:'private-chain',n:12};await repo.set(key,keys);await repo.set('group:'+room+':plain:cached','private cleartext');
 await repo.enqueueMedia({id:'durable-file',roomId:room,text:'caption',state:'queued',upload:{uri:'file:///source.jpg',name:'source.jpg',mime:'image/jpeg',prepared:{uri:'file:///prepared.bin',meta:{protected:true}}}});
 const exported=await repo.exportKeyState();assert.equal(exported[key].n,12);assert.equal(Object.keys(exported).length,1);
 await assert.rejects(repo.importKeyState({[key]:null}),/Некорректная/);assert.equal((await repo.get(key,null)).n,12);
 await repo.close();repo=new AccountRepository(user);assert.equal((await repo.get(key,null)).chain,keys.chain);assert.equal((await repo.outgoing())[0].upload.prepared.uri,'file:///prepared.bin');
 await repo.clearCache();assert.equal((await repo.get(key,null)).n,12);assert.equal((await repo.outgoing()).length,1);await repo.close();
});

test('stable forward IDs do not replace queued work or resurrect a delivered message',async()=>{
 const repo=new AccountRepository('forward-idempotency'),item={id:'forward-operation_room_original',roomId:'room',text:'forwarded',state:'queued'};
 assert.equal(await repo.enqueueMedia(item),true);assert.equal(await repo.enqueueMedia({...item,text:'changed retry'}),false);assert.equal((await repo.outgoing())[0].text,'forwarded');
 await repo.saveMessages([msg('delivered','room','forwarded',{client_message_id:item.id})]);await repo.removeOutgoing(item.id);assert.equal(await repo.enqueueMedia(item),false);assert.equal((await repo.outgoing()).length,0);await repo.close();
});
test("real SQLite FTS search respects rooms, edits, deletion and encrypted-message placeholders", async () => {
  const repo = new AccountRepository("search");
  await repo.saveMessages([
    msg("a", "r1", "Привет мир"),
    msg("b", "r2", "Привет друг"),
    msg("secret", "r1", "ciphertext", { meta: { encrypted: true } }),
  ]);
  assert.equal((await repo.search("Привет")).length, 2);
  assert.equal((await repo.search("Привет", "r1")).length, 1);
  assert.equal((await repo.search("ciphertext")).length, 0);
  await repo.saveMessages([msg("a", "r1", "Новый текст")]);
  assert.equal((await repo.search("мир")).length, 0);
  await repo.removeMessage("b");
  assert.equal((await repo.search("друг")).length, 0);
  await repo.putOutgoing({
    id: "pending",
    roomId: "r1",
    text: "queue",
    state: "queued",
  });
  await repo.set("draft:r1", { text: "draft" });
  await repo.clearCache();
  assert.equal((await repo.search("Новый")).length, 0);
  assert.equal((await repo.outgoing()).length, 1);
  assert.equal((await repo.get("draft:r1", null)).text, "draft");
  await repo.close();
});
test("simultaneous cache writes are serialized and revoked rooms are pruned", async () => {
  const repo = new AccountRepository("parallel");
  await Promise.all([
    repo.saveMessages([msg("a", "allowed", "one")]),
    repo.saveMessages([msg("b", "revoked", "two")]),
    repo.set("draft:revoked", { text: "private" }),
    repo.set("draft:empty-revoked", { text: "draft-only room" }),
    repo.set("scan:revoked", { before: "b", complete: true }),
    repo.putOutgoing({
      id: "unsent",
      roomId: "revoked",
      text: "own unsent text",
      state: "queued",
      reply: msg("b", "revoked", "private quote"),
    }),
  ]);
  await repo.pruneRooms(["allowed"]);
  assert.equal((await repo.messages("revoked")).length, 0);
  assert.equal(await repo.get("draft:revoked", null), null);
  assert.equal((await repo.messages("allowed")).length, 1);
  assert.equal(await repo.get("draft:empty-revoked", null), null);
  assert.equal(await repo.get("scan:revoked", null), null);
  const [unsent] = await repo.outgoing();
  assert.equal(unsent.state, "failed");
  assert.equal(unsent.text, "own unsent text");
  assert.equal(unsent.reply, undefined);
  await repo.close();
});
function queueRepo(items) {
  return {
    items: new Map(items.map((x) => [x.id, { ...x }])),
    saved: [],
    async outgoing() {
      return [...this.items.values()].map((x) => ({ ...x }));
    },
    async putOutgoing(item) {
      this.items.set(item.id, { ...item });
    },
    async removeOutgoing(id) {
      this.items.delete(id);
    },
    async saveMessages(items) {
      this.saved.push(...items);
    },
  };
}
test("lost response retains request ID for retry and offline failures stay queued", async () => {
  const item = {
      id: "same-request",
      roomId: "r",
      text: "hello",
      state: "queued",
    },
    repo = queueRepo([item]);
  let online = false;
  const ids = [];
  const queue = new Outbox(
    repo,
    async (outgoing) => {
      ids.push(outgoing.id);
      if (!online) throw new Error("Network request failed");
      return msg("server", "r", outgoing.text, {
        client_message_id: outgoing.id,
      });
    },
    () => {},
  );
  await queue.flush();
  assert.equal(repo.items.get(item.id).state, "queued");
  online = true;
  await queue.flush();
  assert.deepEqual(ids, ["same-request", "same-request"]);
  assert.equal(repo.items.size, 0);
  assert.equal(repo.saved.length, 1);
});
test("failed sends need explicit retry; stopping an account prevents further sends", async () => {
  const repo = queueRepo([
    { id: "a", roomId: "r", text: "a", state: "queued" },
  ]);
  let attempts = 0;
  const queue = new Outbox(
    repo,
    async () => {
      attempts++;
      throw Object.assign(new Error("blocked"), { status: 403 });
    },
    () => {},
  );
  await queue.flush();
  await queue.flush();
  assert.equal(attempts, 1);
  await queue.retry("a");
  assert.equal(attempts, 2);
  await queue.cancel("a");
  assert.equal(repo.items.size, 0);
  queue.stop();
  repo.items.set("b", { id: "b", state: "queued" });
  await queue.flush();
  assert.equal(attempts, 2);
});
test("merging realtime acknowledgment replaces the local bubble and respects sender identity", () => {
  const own = msg("local", "r", "pending", { client_message_id: "request" }),
    server = msg("server", "r", "sent", { client_message_id: "request" });
  assert.equal(mergeMessages([own], [server]).length, 1);
  assert.equal(mergeMessages([own], [server])[0].id, "server");
  assert.equal(
    mergeMessages([own], [{ ...server, sender_id: "bob" }]).length,
    2,
  );
});

const { groupAlbums } = load("../mobile/src/data/albums.ts");
test("photo albums group by sender and album while keeping non-photo attachments visible", () => {
  const photos = [
    msg("p1", "r", "caption", {
      media_mime: "image/jpeg",
      meta: { album_id: "batch" },
    }),
    msg("p2", "r", "", {
      media_mime: "image/png",
      meta: { album_id: "batch" },
    }),
    msg("other", "r", "", {
      sender_id: "bob",
      media_mime: "image/png",
      meta: { album_id: "batch" },
    }),
    msg("file", "r", "", {
      media_mime: "application/pdf",
      meta: { album_id: "batch" },
    }),
  ];
  const grouped = groupAlbums(photos);
  assert.deepEqual(
    Array.from(grouped.display, (m) => m.id),
    ["p1", "other", "file"],
  );
  assert.equal(grouped.groupFor(photos[1]).length, 2);
});

test("clearing local media only deletes account-listed private files within the app directory", async () => {
  const deleted = [];
  const { removePrivateMedia } = load("../mobile/src/mediaFiles.ts", {
    "./storage": { storage: { get: async () => null } },
    "expo-file-system/legacy": {
      documentDirectory: "file:///app/documents/",
      deleteAsync: async (uri) => deleted.push(uri),
    },
    "expo-sharing": {}, "expo-intent-launcher": {}, "react-native": {Platform:{OS:"android"}}, "./mediaLifecycle": {},
  });
  const voice = "file:///app/documents/voice-" + "a".repeat(32) + ".m4a";
  const photo =
    "file:///app/documents/pending-media/" + "b".repeat(32) + "-photo.jpg";
  await removePrivateMedia([
    voice,
    photo,
    voice,
    "file:///outside/private.txt",
    "file:///app/documents/pending-media/../private.txt",
    "file:///app/documents/account.db",
  ]);
  assert.deepEqual(deleted, [voice, photo]);
});

test("media accounting removes only this account's orphan drafts and counts cached files", async () => {
  const uid = "00000000-0000-4000-8000-000000000001", other = "00000000-0000-4000-8000-000000000002";
  const ownDir = `file:///app/documents/draft-media/${uid}/`, kept = ownDir + 'a'.repeat(32) + '-photo.jpg', orphan = ownDir + 'b'.repeat(32) + '-photo.jpg';
  const cache = `file:///app/cache/decoded-${uid}/file.jpg`, foreign = `file:///app/documents/draft-media/${other}/private.jpg`, deleted = [];
  const data = new Map([[kept, 10], [orphan, 20], [cache, 30], [foreign, 40]]);
  const files = { documentDirectory: 'file:///app/documents/', cacheDirectory: 'file:///app/cache/',
    getInfoAsync: async uri => ({ exists: data.has(uri) || [...data.keys()].some(key => key.startsWith(uri.replace(/\/?$/, '/'))), isDirectory: !data.has(uri), size: data.get(uri) || 0, modificationTime: 1 }),
    readDirectoryAsync: async directory => [...new Set([...data.keys()].filter(key => key.startsWith(directory)).map(key => key.slice(directory.length).split('/')[0]))],
    deleteAsync: async uri => { deleted.push(uri); data.delete(uri); } };
  const { mediaUsage, clearMediaCache } = load('../mobile/src/mediaFiles.ts', { 'expo-file-system/legacy': files, 'expo-sharing': {}, 'expo-intent-launcher': {}, 'react-native': {Platform:{OS:'android'}}, './mediaLifecycle': {}, './storage': { storage: { get: async () => null } } });
  const usage = await mediaUsage({ userId: uid, privateFiles: async () => [kept], size: async () => 50 });
  assert.equal(usage.total, 90); assert.deepEqual(deleted, [orphan]); assert.ok(data.has(foreign));
  await clearMediaCache(); assert.ok(!data.has(cache)); assert.ok(data.has(kept)); assert.ok(data.has(foreign));
});

const { presenceText, reactionPeople, sameMessageGroup } = load(
  "../mobile/src/presentation.ts",
);
test("presence distinguishes real online, offline, missing and invalid status data", () => {
  const now = Date.parse("2026-10-20T09:41:00Z");
  assert.equal(presenceText(null, now), "статус недоступен");
  assert.equal(presenceText({ online: true }, now), "в сети");
  assert.equal(
    presenceText({ online: false, last_seen: "2026-10-20T09:36:00Z" }, now),
    "был(а) 5 мин. назад",
  );
  assert.equal(
    presenceText({ online: false, last_seen: "broken" }, now),
    "не в сети",
  );
});
test("reaction identities retain each participant avatar without duplicate IDs", () => {
  const people = [
    { id: "me", username: "alex", avatar_id: "my-avatar" },
    { id: "peer", username: "mira", avatar_id: "her-avatar" },
  ];
  const users = reactionPeople(["peer", "me", "peer"], people);
  assert.deepEqual(
    Array.from(users, (user) => [user.id, user.avatar_id]),
    [
      ["peer", "her-avatar"],
      ["me", "my-avatar"],
    ],
  );
  assert.equal(reactionPeople(["unknown"], people)[0].display_name, "Участник");
});
test("message groups stop at a different sender, room, date or five-minute gap", () => {
  const first = msg("1", "r", "a", { created_at: "2026-10-20T09:00:00Z" });
  const next = msg("2", "r", "b", { created_at: "2026-10-20T09:04:59Z" });
  assert.equal(sameMessageGroup(first, next), true);
  assert.equal(sameMessageGroup(first, { ...next, sender_id: "bob" }), false);
  assert.equal(sameMessageGroup(first, { ...next, room_id: "other" }), false);
  assert.equal(
    sameMessageGroup(first, { ...next, created_at: "2026-10-20T09:05:00Z" }),
    false,
  );
  assert.equal(
    sameMessageGroup(first, { ...next, created_at: "2026-10-21T09:00:01Z" }),
    false,
  );
});
test("presence polling ignores stale profiles after switching chats or accounts and pauses in background", async () => {
  let state = null,
    dependencies = [],
    cleanup,
    poll,
    foreground;
  const requests = [];
  const app = {
    currentState: "active",
    addEventListener: (_, callback) => {
      foreground = callback;
      return {
        remove() {
          foreground = null;
        },
      };
    },
  };
  const { useChatPresence } = load(
    "../mobile/src/hooks/useChatPresence.ts",
    {
      react: {
        useState: () => [
          state,
          (value) => {
            state = value;
          },
        ],
        useEffect: (effect, values) => {
          if (values.some((value, index) => value !== dependencies[index])) {
            cleanup?.();
            dependencies = values;
            cleanup = effect();
          }
        },
      },
      "react-native": { AppState: app },
      "../mobileApi": {
        mobileApi: (base, token) => ({
          profile: (id) =>
            new Promise((resolve, reject) =>
              requests.push({ base, token, id, resolve, reject }),
            ),
        }),
      },
    },
    {
      setInterval: (callback) => {
        poll = callback;
        return 1;
      },
      clearInterval() {
        poll = null;
      },
    },
  );
  const tick = async () => {
    await Promise.resolve();
    await Promise.resolve();
  };
  assert.equal(useChatPresence("server", "token1", "mira"), null);
  assert.equal(useChatPresence("server", "token1", "anton"), null);
  requests[0].resolve({ id: "mira", online: true });
  await tick();
  assert.equal(useChatPresence("server", "token1", "anton"), null);
  requests[1].resolve({ id: "anton", online: false });
  await tick();
  assert.equal(useChatPresence("server", "token1", "anton").id, "anton");
  assert.equal(useChatPresence("server", "token2", "anton"), null);
  requests[2].resolve({ id: "anton", online: true });
  await tick();
  app.currentState = "background";
  poll();
  assert.equal(requests.length, 3);
  app.currentState = "active";
  foreground("active");
  poll();
  assert.equal(requests.length, 4);
  requests[3].reject(new Error("Network request failed"));
  await tick();
  assert.equal(useChatPresence("server", "token2", "anton"), null);
  useChatPresence("server", null, null);
  assert.equal(poll, null);
  assert.equal(foreground, null);
});

const format = load("../mobile/src/format.ts");

test("mobile list time and day labels follow Telegram wording", () => {
  const now = new Date(2026, 9, 22, 12, 0, 0);
  const at = (y, m, d, h = 10, min = 30) =>
    new Date(y, m, d, h, min).toISOString();
  assert.equal(format.listTime(at(2026, 9, 22), now), "10:30");
  assert.equal(format.listTime(at(2026, 9, 21), now), "Вчера");
  assert.match(format.listTime(at(2026, 9, 19), now), /^(пн|вт|ср|чт|пт|сб|вс)\.?$/);
  assert.equal(format.listTime(at(2026, 8, 1), now), "01.09.26");
  assert.equal(format.dayLabel(at(2026, 9, 22), now), "Сегодня");
  assert.equal(format.dayLabel(at(2026, 9, 21), now), "Вчера");
  assert.equal(format.dayLabel(at(2026, 8, 5), now), "5 сентября");
});

test("preview is not truncated in code and participant counts are pluralised", () => {
  const long = "а".repeat(80);
  assert.equal(
    format.roomPreview({ id: "r", kind: "dm", last_msg_text: long }, "me"),
    long,
  );
  assert.equal(format.participantsLabel(1), "1 участник");
  assert.equal(format.participantsLabel(3), "3 участника");
  assert.equal(format.participantsLabel(11), "11 участников");
});

test('key-state replacement rolls back added keys exactly and preserves drafts while clearing derived plaintext caches',async()=>{const repo=new AccountRepository('77777777-7777-4777-8777-777777777777');await repo.open();const a='group:00000000-0000-4000-8000-000000000001:sender',b='group:00000000-0000-4000-8000-000000000001:receiver',plain='group:00000000-0000-4000-8000-000000000001:plain:message';await repo.set(a,{chain:'old'});await repo.set(plain,{text:'derived cache'});await repo.set('draft:room',{text:'retain draft'});const original=await repo.exportKeyState();await repo.replaceKeyState({[b]:{chain:'new'}});assert.equal(await repo.get(a,null),null);assert.equal(await repo.get(plain,null),null);assert.equal((await repo.get('draft:room',null)).text,'retain draft');await repo.replaceKeyState(original);assert.equal(await repo.get(b,null),null);assert.equal((await repo.get(a,null)).chain,'old');await assert.rejects(repo.replaceKeyState({'draft:room':{text:'malicious'}}));assert.equal((await repo.get(a,null)).chain,'old');await repo.close();});

test('committed repository events are isolated snapshots, never roll back writes, unsubscribe and invalidate',async()=>{
 const repo=new AccountRepository('event-observer'),other=new AccountRepository('unrelated-observer'),seen=[];
 const stop=repo.observeMessages(change=>{assert.equal(events.at(-1),'COMMIT');seen.push(change);if(change.upserts?.length)change.upserts[0].text='observer mutation';});
 const noisy=repo.observeMessages(()=>{throw Error('observer failure');});
 await repo.saveMessages([msg('event','r','original')]);assert.equal((await repo.messages('r'))[0].text,'original');assert.equal(seen.length,1);assert.equal(seen[0].upserts[0].text,'original');
 await other.saveMessages([msg('else','r','unrelated')]);assert.equal(seen.length,1);
 await repo.hideMessage('event');assert.deepEqual(Array.from(seen.at(-1).removed),['event']);
 stop();noisy();await repo.saveMessages([msg('more','r','next')]);assert.equal(seen.length,2);await repo.close();await other.close();
});

test('native display window stays bounded during 10000 incoming events, preserves an older anchor and advances only explicitly',()=>{
 const {mergeMessageWindow}=load('../mobile/src/data/messages.ts');
 const row=i=>msg(String(i),'large','row '+i,{created_at:new Date(Date.UTC(2026,0,1,0,0,i)).toISOString(),client_message_id:'client-'+i});
 const prior=Array.from({length:300},(_,i)=>row(i+100)),incoming=Array.from({length:9600},(_,i)=>row(i+400));
 const stable=mergeMessageWindow(prior,incoming,{direction:'incoming',atBottom:false});assert.equal(stable.messages.length,300);assert.equal(stable.messages[0].id,'100');assert.equal(stable.messages.at(-1).id,'399');assert.equal(stable.hasNewer,true);
 const next=mergeMessageWindow(stable.messages,incoming.slice(0,100),{direction:'newer'});assert.equal(next.messages.length,300);assert.equal(next.messages[0].id,'200');assert.equal(next.messages.at(-1).id,'499');assert.equal(next.hasOlder,true);
 const older=mergeMessageWindow(prior,Array.from({length:100},(_,i)=>row(i)),{direction:'older'});assert.equal(older.messages[0].id,'0');assert.equal(older.messages.at(-1).id,'299');assert.equal(older.hasNewer,true);
 const latest=mergeMessageWindow(prior,incoming,{direction:'incoming',atBottom:true});assert.equal(latest.messages.at(-1).id,'9999');assert.equal(latest.messages.length,300);
 const all=mergeMessages([...prior,...incoming],incoming.map(m=>({...m,id:'server-'+m.id})));assert.equal(all.length,9900);assert.equal(new Set(all.map(m=>m.client_message_id)).size,9900);
});

test('under-limit historical windows keep a contiguous newer cursor when a live tail arrives',()=>{
 const helpers=load('../mobile/src/data/messages.ts'),room='gap-room',row=i=>msg(String(i),room,'row '+i,{created_at:new Date(Date.UTC(2026,0,1,0,0,i)).toISOString()});
 helpers.clearMessageWindows();const original=Array.from({length:100},(_,i)=>row(i+100));helpers.updateMessageWindowPage(room,{has_older:true,has_newer:true});
 const displayed=helpers.mergeDisplayMessages(original,[row(10000),{...row(150),text:'edited'}]);assert.equal(displayed.length,100);assert.equal(displayed.at(-1).id,'199');assert.equal(displayed.find(m=>m.id==='150').text,'edited');
 const nextCursor=displayed.filter(m=>!m.delivery).at(-1).id;assert.equal(nextCursor,'199');
 const next=helpers.mergeDisplayWindow(displayed,Array.from({length:100},(_,i)=>row(i+200)),'newer');assert.equal(next.at(-1).id,'299');assert.equal(next.length,200);
});
test('bounded incoming tails count unseen peers once, paging does not count old rows, and visible receipt consumes them',()=>{
 const h=load('../mobile/src/data/messages.ts'),room='unseen-room',row=(i,sender='peer')=>msg(String(i),room,'row '+i,{sender_id:sender,created_at:new Date(Date.UTC(2026,0,1,0,0,i)).toISOString()});
 h.clearMessageWindows();h.setMessageViewport(room,false);const original=Array.from({length:300},(_,i)=>row(i));let shown=h.mergeDisplayMessages(original,[row(300)]);
 assert.equal(shown.length,300);assert.equal(shown.at(-1).id,'299');assert.equal(h.messageWindowUnseenCount(room,'me'),1);
 shown=h.mergeDisplayMessages(shown,[row(300),row(301,'me'),row(302)]);assert.equal(h.messageWindowUnseenCount(room,'me'),2);
 shown=h.mergeDisplayWindow(shown,[row(300),row(301,'me'),row(302)],'newer');assert.equal(h.messageWindowUnseenCount(room,'me'),2);
 h.consumeMessageWindowUnseen(room,row(300));assert.equal(h.messageWindowUnseenCount(room,'me'),1);h.consumeMessageWindowUnseen(room,row(302));assert.equal(h.messageWindowUnseenCount(room,'me'),0);
});

test('actual ChatScreen viewability receipt consumes deferred arrivals only when their range is reached',()=>{
 const h=load('../mobile/src/data/messages.ts'),room='screen-unseen',row=i=>msg(String(i),room,'row '+i,{sender_id:'peer',created_at:new Date(Date.UTC(2026,0,1,0,0,i)).toISOString()});
 h.clearMessageWindows();h.updateMessageWindowPage(room,{has_newer:true});h.mergeDisplayMessages([row(100),row(199)],[row(10000)]);assert.equal(h.messageWindowUnseenCount(room,'me'),1);
 const source=fs.readFileSync(new URL('../mobile/src/screens/ChatScreen.tsx',import.meta.url),'utf8'),ast=ts.createSourceFile('ChatScreen.tsx',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TSX);let arrow;function visit(node){if(ts.isVariableDeclaration(node)&&node.name.getText(ast)==='visible')arrow=node.initializer.expression.arguments[0];ts.forEachChild(node,visit);}visit(ast);assert.ok(arrow);let refreshes=0;
 const scope={consumeMessageWindowUnseen:h.consumeMessageWindowUnseen,latest:{current:{room:{id:room},repository:null}},groupedRef:{current:{groupFor:()=>null}},anchor:{current:null},positionTimer:{current:null},refreshWindow:()=>refreshes++,clearTimeout(){},setTimeout(){return 1;}};
 const visible=vm.runInNewContext(ts.transpileModule('('+arrow.getText(ast)+')',{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText,scope);
 visible({viewableItems:[{item:row(199)}]});assert.equal(h.messageWindowUnseenCount(room,'me'),1);assert.equal(refreshes,0);
 visible({viewableItems:[{item:row(10000)}]});assert.equal(h.messageWindowUnseenCount(room,'me'),0);assert.equal(refreshes,1);
});
