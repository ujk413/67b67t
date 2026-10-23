// Run on the fresh server with:
// docker exec -i messenger67-api node --input-type=module < scripts/live_server_check.mjs
// Creates two temporary accounts and removes their data in finally. Never prints tokens.
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import { Pool } from 'pg';

const origin = (process.env.ORIGIN || '').split(',')[0];
assert.match(origin, /^https:\/\//, 'HTTPS ORIGIN required');
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const accounts = [], sessions = [], sockets = [];
const suffix = crypto.randomBytes(6).toString('hex');
const password = crypto.randomBytes(24).toString('hex');
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
async function request(route, { token, body, method = 'GET', status = 200, form } = {}) {
  const response = await fetch(origin + '/api' + route, {
    method, signal: AbortSignal.timeout(15000),
    headers: {
      'X-Session-Transport': 'bearer',
      ...(token ? { Authorization: 'Bearer ' + token } : {}),
      ...(body ? { 'Content-Type': 'application/json' } : {}),
    },
    body: form || (body ? JSON.stringify(body) : undefined),
  });
  assert.equal(response.status, status, `${method} ${route}: unexpected HTTP status`);
  assert.match(response.headers.get('content-type') || '', /application\/json/);
  return response.json();
}
async function socket(token) {
  const ws = new WebSocket(origin.replace(/^https:/, 'wss:') + '/ws');
  sockets.push(ws);
  const messages = [];
  ws.addEventListener('message', event => messages.push(JSON.parse(event.data)));
  const wait = async predicate => {
    const deadline = Date.now() + 12000;
    while (Date.now() < deadline) {
      const index = messages.findIndex(predicate);
      if (index >= 0) return messages.splice(index, 1)[0];
      assert.notEqual(ws.readyState, WebSocket.CLOSED, 'WebSocket closed unexpectedly');
      await pause(50);
    }
    throw new Error('Timed out waiting for WebSocket event');
  };
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('WebSocket open timeout')), 12000);
    ws.addEventListener('open', () => { clearTimeout(timer); resolve(); }, { once: true });
    ws.addEventListener('error', () => { clearTimeout(timer); reject(new Error('WebSocket error')); }, { once: true });
  });
  const send = data => ws.send(JSON.stringify(data));
  send({ type: 'auth', token });
  await wait(event => event.type === 'auth.ok');
  return { send, wait };
}

try {
  assert.equal((await request('/health')).ok, true);
  await request('/me', { status: 401 });
  for (const route of ['/', '/user/deploy_check']) {
    const response = await fetch(origin + route, { signal: AbortSignal.timeout(15000) });
    assert.equal(response.status, 200);
    assert.match(await response.text(), /<!doctype html>/i);
  }
  const js = await fetch(origin + '/app.js');
  assert.equal(js.status, 200);
  assert.match(js.headers.get('content-type') || '', /javascript/);
  assert.doesNotMatch(await js.text(), /<!doctype html>/i);
  console.log('PASS HTTPS, web shell, static JS, API routing and anonymous authentication');

  for (const letter of ['a', 'b']) {
    const username = `deploy_${suffix}_${letter}`;
    const registered = await request('/auth/register', { method: 'POST', body: { username, password } });
    accounts.push(registered.user.id);
    assert.ok(registered.token);
    const login = await request('/auth/login', { method: 'POST', body: { username, password } });
    assert.equal(login.user.id, registered.user.id);
    registered.token = login.token;
    registered.user.username = username;
    accounts[accounts.length - 1] = registered.user.id;
    sessions.push(registered);
  }
  const [a, b] = sessions;
  assert.equal((await request('/me', { token: a.token })).id, a.user.id);
  const webLogin = await fetch(origin + '/api/auth/login', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: a.user.username, password }),
    signal: AbortSignal.timeout(15000),
  });
  assert.equal(webLogin.status, 200);
  const cookie = webLogin.headers.get('set-cookie');
  assert.match(cookie || '', /HttpOnly/i);
  assert.match(cookie || '', /Secure/i);
  const webMe = await fetch(origin + '/api/me', {
    headers: { Cookie: cookie.split(';')[0] }, signal: AbortSignal.timeout(15000),
  });
  assert.equal(webMe.status, 200);
  assert.equal((await webMe.json()).id, a.user.id);
  const caps = await request('/mobile/capabilities', { token: a.token });
  assert.equal(caps.idempotent_send, true);
  assert.equal(caps.sync, true);
  console.log('PASS registration, login, web cookies, bearer session and mobile capabilities');

  const room = await request('/rooms', { token: a.token, method: 'POST', body: { kind: 'dm', members: [b.user.username] } });
  const reverse = await request('/rooms', { token: b.token, method: 'POST', body: { kind: 'dm', members: [a.user.username] } });
  assert.equal(room.id, reverse.id);
  const first = await socket(a.token), second = await socket(b.token);
  for (const ws of [first, second]) {
    ws.send({ type: 'rooms.join', room_id: room.id });
    await ws.wait(event => event.type === 'rooms.join.ok' && event.room_id === room.id);
  }
  const payload = { type: 'message.send', room_id: room.id, message: {
    type: 'text', text: 'Temporary deployment check', client_message_id: `deploy_${suffix}`,
  } };
  first.send(payload);
  const incoming = await second.wait(event => event.type === 'message.new' && event.message.client_message_id === payload.message.client_message_id);
  assert.equal(incoming.message.text, payload.message.text);
  first.send(payload);
  const ack = await first.wait(event => event.type === 'message.ack' && event.message.id === incoming.message.id);
  assert.equal(ack.message.id, incoming.message.id);
  const history = await request(`/rooms/${room.id}/history`, { token: b.token });
  assert.equal(history.messages.filter(message => message.client_message_id === payload.message.client_message_id).length, 1);
  assert.equal(history.has_newer, false);
  const end = await request(`/rooms/${room.id}/history?after=${incoming.message.id}`, { token: b.token });
  assert.equal(end.messages.length, 0);
  assert.equal(end.has_newer, false);
  console.log('PASS direct conversation reuse, WSS authentication, message delivery, retry deduplication and history');
  const webPush = await request('/web-push/config', { token: a.token });
  if (process.env.WEB_PUSH_PUBLIC_KEY && process.env.WEB_PUSH_PRIVATE_KEY) {
    assert.equal(webPush.enabled, true);
    assert.match(webPush.public_key, /^[A-Za-z0-9_-]{87}$/);
    console.log('PASS authenticated Web Push configuration (provider delivery needs a browser subscription)');
  }

  const content = 'Deployment upload check ' + suffix;
  const form = new FormData();
  form.append('file', new Blob([content], { type: 'text/plain' }), 'deployment-check.txt');
  const media = await request('/media/upload?kind=file', { token: a.token, method: 'POST', form });
  assert.ok(media.id);
  const downloaded = await fetch(origin + '/api/media/' + media.id, { headers: { Authorization: 'Bearer ' + a.token } });
  assert.equal(downloaded.status, 200);
  assert.equal(await downloaded.text(), content);
  const ice = await request('/rtc/ice-servers', { token: a.token });
  assert.ok(ice.turn?.credential);
  assert.ok(ice.turn?.urls?.length);
  assert.equal(ice.turn.credential, crypto.createHmac('sha1', process.env.TURN_SECRET).update(ice.turn.username).digest('base64'));
  console.log('PASS writable uploads, authenticated download and short-lived TURN credentials');
  const messageId=incoming.message.id;
  await request(`/rooms/${room.id}/pin`,{token:a.token,method:'PUT',body:{message_id:messageId}});
  assert.equal((await request('/rooms',{token:b.token})).find(r=>r.id===room.id).pinned_message_id,messageId);
  assert.equal((await request(`/rooms/${room.id}`,{token:b.token})).pinned_message_id,messageId);
  await first.wait(event=>event.type==='room.pin' && event.message_id===messageId);
  await request(`/rooms/${room.id}/messages/${messageId}/hide`,{token:a.token,method:'DELETE'});
  await first.wait(event=>event.type==='message.hide' && event.message_id===messageId);
  assert.equal((await request(`/rooms/${room.id}/history`,{token:a.token})).messages.some(m=>m.id===messageId),false);
  assert.equal((await request(`/rooms/${room.id}/history`,{token:b.token})).messages.some(m=>m.id===messageId),true);
  assert.equal((await request('/rooms',{token:a.token})).find(r=>r.id===room.id).last_msg_id,null);
  assert.ok((await request('/sync',{token:a.token})).changes.some(c=>c.message_id===messageId && c.operation==='delete'));
  await request(`/rooms/${room.id}/pin`,{token:b.token,method:'PUT',body:{message_id:null}});
  console.log('PASS pin/unpin, both room read paths, private hide realtime, history, sync and preview');
  console.log('NOTE real two-device calls and Firebase push delivery are separate checks');
} finally {
  for (const ws of sockets) ws.close();
  // Restrict cleanup to IDs created by this process. Never touch existing accounts.
  if (accounts.length) {
    const { rows } = await pool.query('SELECT path,thumb_path FROM media WHERE owner_id=ANY($1::uuid[])', [accounts]);
    const uploadRoot = path.resolve(process.env.UPLOAD_DIR || '/app/uploads');
    await pool.query('DELETE FROM users WHERE id=ANY($1::uuid[])', [accounts]);
    for (const media of rows) for (const item of [media.path, media.thumb_path]) {
      if (!item) continue;
      const target = path.resolve(uploadRoot, item);
      if (target.startsWith(uploadRoot + path.sep)) await fs.unlink(target).catch(error => {
        if (error.code !== 'ENOENT') throw error;
      });
    }
    console.log('CLEANUP temporary accounts, messages and uploaded files removed');
  }
  await pool.end();
}
