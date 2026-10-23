import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { createRequire } from 'node:module';
import { createAuthFlow } from '../web/auth-flow.js';
const require = createRequire(import.meta.url), ts = require('../mobile/node_modules/typescript');
const { PGlite } = require('../api/node_modules/@electric-sql/pglite');
test('successful registration stays successful when subsequent initialization fails', async () => {
  const reports = [], users = [], states = []; let rendered = false;
  const authenticate = createAuthFlow({ request: async () => ({ user: { id: 'new' } }), setUser: u => users.push(u),
    render: async () => { rendered = true; }, initialize: async () => { throw Error('network_timeout'); },
    report: (...args) => reports.push(args), busy: value => states.push(value) });
  await authenticate('register', { username: 'fixture' }); await new Promise(resolve => setImmediate(resolve));
  assert.equal(users[0].id, 'new'); assert.ok(rendered); assert.deepEqual(states, [true, false]);
  assert.ok(reports.at(-1)[0].startsWith('Вход выполнен.')); assert.equal(reports.at(-1)[1], true);
});
test('auth flow prevents duplicate submissions and preserves the specific server error', async () => {
  let release, requests = 0; const reports = [];
  const authenticate = createAuthFlow({ request: async () => { requests++; await new Promise(r => { release = r; }); throw Object.assign(Error('username_taken'), { code: 'username_taken' }); }, setUser() {}, render() {}, initialize() {}, report: value => reports.push(value), busy() {} });
  const first = authenticate('register', {}); await authenticate('register', {}); release(); await first;
  assert.equal(requests, 1); assert.match(reports.at(-1), /Имя уже занято/);
});

test('account challenge and key restore finish before exposing the authenticated user or starting sync',async()=>{
 const trace=[];const authenticate=createAuthFlow({request:async()=>({challenge:{id:'challenge'}}),resolveChallenge:async challenge=>{assert.equal(challenge.id,'challenge');trace.push('factor');return{user:{id:'alice'}};},prepareUser:async()=>{trace.push('keys');},setUser:()=>trace.push('user'),initialize:async()=>trace.push('sync'),render:()=>trace.push('render'),report(){},busy(){}});
 await authenticate('login',{});assert.deepEqual(trace,['factor','keys','user','render','sync']);
});
test('registration rolls back the account when creation of its session fails', async () => {
  const db = new PGlite(); await db.exec(`CREATE TABLE users(id text DEFAULT 'new-user',username text,password_hash text,created_at timestamptz DEFAULT now());
    CREATE TABLE user_public_names(user_id text,name text UNIQUE,kind text); CREATE TABLE rooms(id text,kind text,title text); CREATE TABLE room_members(room_id text,user_id text,role text);`);
  const source = ts.createSourceFile('server.js', fs.readFileSync(new URL('../api/server.js', import.meta.url), 'utf8'), ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
  let handler; function visit(n) { if (ts.isCallExpression(n) && n.expression.getText(source) === 'app.post' && n.arguments[0]?.text === '/auth/register') handler = n.arguments[2]; ts.forEachChild(n, visit); } visit(source); assert.ok(handler);
  let released = false;
  const query = (sql, args) => { if (sql.includes('INSERT INTO sessions')) throw Error('simulated session write failure'); return db.query(sql, args); };
  const scope = { verifyCaptcha: async () => true, resolveUserIdByPublicName: async () => null, argon2: { hash: async () => 'test-hash' },
    pool: { connect: async () => ({ query, release: () => { released = true; } }) }, randToken: () => 'test-token', sha256: () => 'test-hash', TOKEN_TTL_DAYS: '30', SYSTEM_CHANNEL_TITLE: 'News', log: { error() {} }, authCookieOptions: () => ({}), wantsBearerSession: () => true };
  const register = vm.runInNewContext(`(${handler.getText(source)})`, scope);
  await assert.rejects(register({ body: { username: 'fixture', password: 'password' } }, {}), /session write failure/);
  assert.equal((await db.query('SELECT count(*)::int AS n FROM users')).rows[0].n, 0); assert.ok(released); await db.close();
});
