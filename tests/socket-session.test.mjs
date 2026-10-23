import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { installSocketSession } from '../api/socket-session.js';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { createHash, randomUUID } from 'node:crypto';

test('password change closes quiet revoked sockets and awaits call revocation while retaining this session', async () => {
  const source=readFileSync(new URL('../api/server.js',import.meta.url),'utf8'),routes=new Map(),sessions=new Set(['current','old']),calls=[];
  let finishRevocation,started;const entered=new Promise(r=>{started=r;}),pending=new Promise(r=>{finishRevocation=r;});
  const scope={Map,Set,Promise,crypto:{randomUUID},log:{warn:assert.fail},account:{filterPayload:async(_id,p)=>p},relayCall:{revokeUser:async(id,options)=>{calls.push([id,options]);started();await pending;}},app:{put:(path,fn)=>routes.set(path,fn)},requireAuth:async()=>({id:'alice',sessionId:'current',token:'current-token'}),argon2:{hash:async()=> 'new-hash'},sha256:value=>createHash('sha256').update(value).digest('hex'),pool:{query:async(sql,args)=>{if(sql.startsWith('DELETE')){assert.deepEqual(Array.from(args),['alice',createHash('sha256').update('current-token').digest('hex')]);sessions.delete('old');}return{rows:[]};}}};
  const socketStart=source.indexOf('const socketsByUser ='),socketEnd=source.indexOf('// Send FCM push notification',socketStart);
  const revokeStart=source.indexOf('async function revokeUserSockets('),revokeEnd=source.indexOf('async function removeAccountMediaFiles',revokeStart);
  const routeStart=source.indexOf('app.put("/me/password"'),routeEnd=source.indexOf('app.post("/me/avatar"',routeStart);
  vm.createContext(scope);vm.runInContext(source.slice(socketStart,socketEnd)+source.slice(revokeStart,revokeEnd)+source.slice(routeStart,routeEnd)+';globalThis.socketApi={addSocket,joinRoom,sendToUser,broadcastToRoom};',scope);
  const socket=()=>({readyState:1,sent:[],send(raw){this.sent.push(JSON.parse(raw));},close(code){this.readyState=3;this.closed=code;}}),current=socket(),old=socket();
  for(const [s,id]of [[current,'current'],[old,'old']]){scope.socketApi.addSocket('alice',s,id);scope.socketApi.joinRoom('room',s);}
  let done=false;const changing=routes.get('/me/password')({body:{new_password:'new-password'}},{}).then(value=>{done=true;return value;});
  // A missing hook must fail instead of hanging while waiting for its entry.
  await Promise.race([entered,changing.then(()=>{assert.fail('password route completed without awaiting session/call revocation');})]);
  assert.equal(done,false);assert.equal(old.closed,4401);assert.equal(current.readyState,1);assert.deepEqual([...sessions],['current']);
  scope.socketApi.sendToUser('alice',{type:'message.new'});scope.socketApi.broadcastToRoom('room',{type:'room.event'});await tick();
  assert.deepEqual(old.sent,[{type:'auth.error',error:'session_revoked'}]);assert.deepEqual(current.sent,[{type:'message.new'},{type:'room.event'}]);
  assert.equal(calls[0][0],'alice');assert.equal(calls[0][1].exceptSessionId,'current');finishRevocation();assert.equal((await changing).ok,true);
});

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
