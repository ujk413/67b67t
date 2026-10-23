import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { webcrypto } from 'node:crypto';
import { createAccountStore, eraseAccountStore, DurableOutbox, mergeMessages } from '../web/local-data.js';
import { plural, listTime } from '../web/ui.js';
const require = createRequire(import.meta.url), fake = require('../api/node_modules/fake-indexeddb');
globalThis.indexedDB = fake.indexedDB; globalThis.IDBKeyRange = fake.IDBKeyRange;
Object.defineProperty(globalThis, 'crypto', { configurable: true, value: webcrypto });
Object.defineProperty(globalThis, 'navigator', { configurable: true, value: { onLine: true } });

test('erasing a deleted account drains pending writes, destroys its cache key and invalidates old handles',async()=>{
 const user=crypto.randomUUID(),other=createAccountStore('erase-unrelated'),store=createAccountStore(user);
 await other.set('draft:room','preserve');await store.set('draft:room','erase');
 const write=store.setBlob('attachment',new Blob(['private file']));
 const erased=eraseAccountStore(user);await Promise.allSettled([write]);await erased;
 await assert.rejects(()=>store.set('draft:room','resurrect'),/удалён/);
 await assert.rejects(()=>store.get('draft:room'),/удалён/);
 assert.equal(await other.get('draft:room'),'preserve');
 const db=await new Promise(resolve=>{const r=indexedDB.open('67chat-local-v1');r.onsuccess=()=>resolve(r.result);});
 const keys=await new Promise(resolve=>{const r=db.transaction('records').objectStore('records').getAllKeys();r.onsuccess=()=>resolve(r.result);});db.close();
 assert.ok(!keys.some(key=>String(key).startsWith('user:'+user+':')));
});
test('encrypted web history, drafts and outbox persist and stay isolated by account', async () => {
  const a = createAccountStore('A'), b = createAccountStore('B');
  await a.set('draft:room', 'Private draft'); await a.saveMessages('room', [{ id: 'one', created_at: '2026-10-21', text: 'Private message' }]);
  await a.set('outbox:pending', { id: 'pending', text: 'Private queued message' });
  assert.equal(await createAccountStore('A').get('draft:room'), 'Private draft'); assert.equal(await b.get('draft:room'), null);
  assert.equal((await a.entries('outbox:'))[0][1].id, 'pending'); assert.equal((await a.get('messages:room'))[0].text, 'Private message');
  const db = await new Promise((resolve, reject) => { const r = indexedDB.open('67chat-local-v1'); r.onsuccess = () => resolve(r.result); r.onerror = () => reject(r.error); });
  const records = await new Promise(resolve => { const r = db.transaction('records').objectStore('records').getAll(); r.onsuccess = () => resolve(r.result); });
  assert.ok(!JSON.stringify(records).includes('Private')); db.close();
});
test('lost response retains one stable request id and retries the same persisted payload', async () => {
  const store = createAccountStore('retry'), sent = [], updates = []; let fail = true;
  const queue = new DurableOutbox({ store, changed: (...args) => updates.push(args), report: e => { throw e; }, send: async item => { sent.push({ id: item.id, room: item.room_id, body: item.body }); if (fail) throw Error('network'); return { id: 'server', client_message_id: item.id, room_id: item.room_id, sender_id: 'me', created_at: item.created_at }; } });
  await queue.enqueue({ id: 'request', room_id: 'original-room', body: { text: 'ciphertext' }, delivery: 'queued', created_at: '2026-10-21' });
  while (queue.running) await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal((await queue.items())[0].delivery, 'queued'); fail = false; await queue.flush();
  assert.equal((await queue.items()).length, 0); assert.equal(sent.length, 2); assert.deepEqual(sent[0], sent[1]); assert.equal(updates.at(-1)[1].id, 'server');
});
test('concurrent cache writes and realtime acknowledgments preserve all messages without duplicate bubbles', async () => {
  const store = createAccountStore('concurrent');
  await Promise.all([store.saveMessages('room', [{ id: 'one', created_at: '2026-10-21T01:00Z' }]), store.saveMessages('room', [{ id: 'two', created_at: '2026-10-21T02:00Z' }])]);
  assert.equal((await store.get('messages:room')).length, 2);
  const merged = mergeMessages([{ id: 'local', sender_id: 'me', client_message_id: 'stable', delivery: 'sending', created_at: '2026' }], [{ id: 'server', sender_id: 'me', client_message_id: 'stable', created_at: '2026' }]);
  assert.equal(merged.length, 1); assert.equal(merged[0].id, 'server');
});
test('chat-list time follows Telegram: time today, «Вчера», weekday, date', () => {
  const now = new Date(2026, 9, 22, 12, 0, 0);
  const at = (y, m, d, h = 10, min = 30) => new Date(y, m, d, h, min).toISOString();
  assert.equal(listTime(at(2026, 9, 22), now), '10:30');
  assert.equal(listTime(at(2026, 9, 21), now), 'Вчера');
  assert.match(listTime(at(2026, 9, 19), now), /^(пн|вт|ср|чт|пт|сб|вс)\.?$/);
  assert.equal(listTime(at(2026, 8, 1), now), '01.09.26');
  assert.equal(listTime(''), '');
  assert.equal(listTime('not-a-date'), '');
});
test('russian plural picks the right form for 1, 2, 5 and the teens', () => {
  const form = n => plural(n, 'участник', 'участника', 'участников');
  assert.equal(form(1), 'участник');
  assert.equal(form(2), 'участника');
  assert.equal(form(5), 'участников');
  assert.equal(form(11), 'участников');
  assert.equal(form(21), 'участник');
  assert.equal(form(102), 'участника');
  assert.equal(form(0), 'участников');
});

test('actual browser runtime feeds incremental sync, receive, hide and session cleanup into the shared event cache',async()=>{
 const {createChatRuntime}=await import('../web/chat-runtime.js'),{expressiveHistoryLoader:load,expressiveHistoryKey}=await import('../web/expressive-event-history.js'),{browserSessionGeneration}=await import('../web/browser-accounts.js');
 const prior={window:globalThis.window,document:globalThis.document,location:globalThis.location};globalThis.window={addEventListener(){}};globalThis.document={hidden:false,addEventListener(){},getElementById(){return null;}};globalThis.location={origin:'https://event.test'};
 const account='event-runtime-'+crypto.randomUUID(),key=expressiveHistoryKey(account,location.origin,String(browserSessionGeneration()),'room'),options={key,version:'session',roomId:'room',fetchPage:async()=>({messages:[{id:'old',room_id:'room',text:'old'},{id:'deleted',room_id:'room',text:'delete'}],has_older:false}),decode:async m=>m};
 let cursor=0,calls=0;const state={me:{id:account},messages:[],rooms:[],activeRoomId:'room'},errors=[];
 const runtime=createChatRuntime({state,eventHistory:true,request:async path=>{assert.match(path,/^\/sync\?cursor=/);calls++;return{room_ids:['room'],cursor:String(++cursor),has_more:false,changes:cursor===1?[{room_id:'room',message_id:'old',message:{id:'old',room_id:'room',created_at:'2026-01-01',text:'edited'}},{room_id:'room',message_id:'deleted',message:null}]:[]};},decode:async(_room,m)=>({...m,_text:m.text}),buildText:async()=>{},mount:async()=>{},refresh:async()=>{},render(){},report:e=>errors.push(e),drafts:new Map(),loadRooms:async()=>{}});
 try{await runtime.start(state.me);await load(options).promise;await runtime.syncEvents();let rows=await load(options).promise;assert.equal(rows.find(m=>m.id==='old').text,'edited');assert.ok(!rows.some(m=>m.id==='deleted'));
 await runtime.received({id:'new',room_id:'room',created_at:'2026-01-02',text:'new'});assert.equal((await load(options).promise).length,2);await runtime.hide('room','old');assert.deepEqual((await load(options).promise).map(m=>m.id),['new']);assert.deepEqual(errors,[]);assert.ok(calls<=2);
 runtime.stop();assert.equal(load.has(key),false);
 }finally{runtime.stop();for(const [name,value]of Object.entries(prior))if(value===undefined)delete globalThis[name];else globalThis[name]=value;}
});
