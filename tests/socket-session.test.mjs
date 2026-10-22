import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { installSocketSession } from '../api/socket-session.js';

class Socket extends EventEmitter {
  sent = []; closed = null;
  send(value) { this.sent.push(JSON.parse(value)); }
  close(code) { this.closed = code; this.emit('close'); }
  receive(value) { this.emit('message', JSON.stringify(value)); }
}
const tick = () => new Promise(resolve => setImmediate(resolve));

test('native auth prompt does not terminate a valid session and pipelined room joins wait for DB auth', async () => {
  let finishAuth;
  const socket = new Socket(), received = [];
  installSocketSession(socket, { headers: {}, cookies: {} }, {
    authenticate: () => new Promise(resolve => { finishAuth = resolve; }),
    onAuthenticated() {}, onMessage: message => received.push(message), onClose() {},
  });
  assert.deepEqual(socket.sent, []);
  socket.receive({ type: 'auth', token: 'test-token' });
  socket.receive({ type: 'rooms.join', room_id: 'room' });
  await tick();
  assert.equal(received.length, 0);
  finishAuth({ id: 'user', username: 'alice' });
  await tick();
  assert.equal(socket.sent[0].type, 'auth.ok');
  assert.deepEqual(received, [{ type: 'rooms.join', room_id: 'room' }]);
  socket.close(1000);
});
test('bad credentials close only that socket with an explicit authentication failure', async () => {
  const socket = new Socket();
  installSocketSession(socket, { headers: {} }, {
    authenticate: async () => { throw new Error('invalid'); },
    onAuthenticated() { assert.fail(); }, onMessage() { assert.fail(); }, onClose() {},
  });
  socket.receive({ type: 'auth', token: 'bad-token' });
  await tick();
  assert.equal(socket.sent[0].type, 'auth.error');
  assert.equal(socket.closed, 4401);
});
test('a browser cookie authenticates frames arriving while the initial query is pending', async () => {
  let finishAuth;
  const socket = new Socket(), received = [];
  installSocketSession(socket, { headers: {}, cookies: { token: 'cookie' } }, {
    authenticate: () => new Promise(resolve => { finishAuth = resolve; }),
    onAuthenticated() {}, onMessage: message => received.push(message), onClose() {},
  });
  socket.receive({ type: 'rooms.join', room_id: 'room' });
  finishAuth({ id: 'user' });
  await tick();
  assert.equal(socket.sent[0].type, 'auth.ok');
  assert.equal(received.length, 1);
  socket.close(1000);
});
test('closing while DB auth is pending does not register a stale socket', async () => {
  let finishAuth;
  const socket = new Socket();
  installSocketSession(socket, { headers: { authorization: 'Bearer cookie' } }, {
    authenticate: () => new Promise(resolve => { finishAuth = resolve; }),
    onAuthenticated() { assert.fail(); }, onMessage() { assert.fail(); }, onClose() {},
  });
  socket.close(1000);
  finishAuth({ id: 'user' });
  await tick();
  assert.deepEqual(socket.sent, []);
});
