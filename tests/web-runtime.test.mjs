import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { webcrypto } from 'node:crypto';
import { createAccountStore, DurableOutbox, mergeMessages } from '../web/local-data.js';
import { plural, listTime } from '../web/ui.js';
const require = createRequire(import.meta.url), fake = require('../api/node_modules/fake-indexeddb');
globalThis.indexedDB = fake.indexedDB; globalThis.IDBKeyRange = fake.IDBKeyRange;
Object.defineProperty(globalThis, 'crypto', { configurable: true, value: webcrypto });
Object.defineProperty(globalThis, 'navigator', { configurable: true, value: { onLine: true } });
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
