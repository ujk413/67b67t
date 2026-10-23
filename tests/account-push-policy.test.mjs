import {test,before,after} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFile} from 'node:fs/promises';
import {installPushPolicy,withPushRegistration,withPushDelivery} from '../api/account-push-policy.js';
const require=createRequire(import.meta.url),{PGlite}=require('../api/node_modules/@electric-sql/pglite'),Fastify=require('../api/node_modules/fastify');
let db,app;const A='alice',B='bob',secret='a'.repeat(64),installation='installation-123';
const pool={query:(s,a)=>db.query(s,a),connect:async()=>({query:(s,a)=>db.query(s,a),release(){}})};
before(async()=>{db=new PGlite();await db.exec('CREATE TABLE push_devices(user_id text,installation_id text,token text,PRIMARY KEY(user_id,installation_id))');await db.exec(await readFile(new URL('../api/account-push-policy.sql',import.meta.url),'utf8'));app=Fastify();installPushPolicy(app,{pool,requireAuth:async req=>{if(!req.headers['x-user'])throw Object.assign(Error('unauthorized'),{statusCode:401});return{id:req.headers['x-user']};}});app.put('/register',async req=>withPushRegistration(pool,{id:req.headers['x-user']},req.body.installation_id,req.body,async c=>{await c.query('INSERT INTO push_devices VALUES($1,$2,$3) ON CONFLICT(user_id,installation_id) DO UPDATE SET token=EXCLUDED.token',[req.headers['x-user'],req.body.installation_id,'token']);return{ok:true};}));await app.ready();});
after(async()=>{await app?.close();await db?.close();});
const claim=(user=A,id=installation,key=secret)=>app.inject({method:'POST',url:'/me/account/push-installation',headers:{'x-user':user},payload:{installation_id:id,secret:key}});
const register=(user,body={})=>app.inject({method:'PUT',url:'/register',headers:{'x-user':user},payload:{installation_id:installation,...body}});
const disable=(key=secret)=>app.inject({method:'POST',url:'/auth/account-push-disable',payload:{installation_id:installation,secret:key}});
test('capability fence revokes every account and rejects stale/legacy registrations until explicit direct resume',async()=>{
 const state=await claim();assert.equal(state.statusCode,200,state.body);const epoch=state.json().epoch;
 for(const user of[A,B])assert.equal((await register(user,{installation_secret:secret,installation_epoch:epoch,resume_push:true})).statusCode,200);
 assert.equal((await db.query('SELECT * FROM push_devices')).rows.length,2);
 const off=await disable();assert.equal(off.statusCode,200,off.body);assert.equal(off.json().epoch,epoch+1);assert.equal((await db.query('SELECT * FROM push_devices')).rows.length,0);
 assert.equal((await register(A)).statusCode,409);
 assert.equal((await register(A,{installation_secret:secret,installation_epoch:epoch,resume_push:true})).statusCode,409);
 const paused=await claim();assert.equal(paused.json().paused,true);
 assert.equal((await register(A,{installation_secret:secret,installation_epoch:epoch+1})).statusCode,409);
 assert.equal((await register(B,{installation_secret:secret,installation_epoch:epoch+1,resume_push:true})).statusCode,200);
 assert.equal((await db.query('SELECT user_id FROM push_devices')).rows[0].user_id,B);
});
test('wrong capability neither revokes nor replaces an existing installation',async()=>{
 assert.equal((await disable('b'.repeat(64))).statusCode,403);assert.equal((await claim(A,installation,'b'.repeat(64))).statusCode,403);
 assert.equal((await db.query('SELECT * FROM push_devices')).rows.length,1);
 const row=(await db.query('SELECT * FROM account_push_installations')).rows[0];assert.equal(JSON.stringify(row).includes(secret),false);
});
test('legacy owner can claim its installation but another account cannot take it',async()=>{
 await db.query('INSERT INTO push_devices VALUES($1,$2,$3)',[A,'legacy-installation','token']);
 assert.equal((await claim(B,'legacy-installation')).statusCode,403);
 assert.equal((await claim(A,'legacy-installation')).statusCode,200);
 assert.equal((await app.inject({method:'POST',url:'/me/account/push-installation',payload:{installation_id:'fresh-installation',secret}})).statusCode,401);
});
test('failed registration rolls back unpause and payload validation fails closed',async()=>{
 await disable();const state=(await claim()).json();
 await assert.rejects(withPushRegistration(pool,{id:A},installation,{installation_secret:secret,installation_epoch:state.epoch,resume_push:true},async()=>{throw Error('failed insert');}),/failed insert/);
 assert.equal((await claim()).json().paused,true);
 assert.equal((await claim(A,'bad',secret)).statusCode,400);
 assert.equal((await register(A,{installation_secret:secret,installation_epoch:'bad',resume_push:true})).statusCode,400);
});
test('delivery selected before revocation rechecks the paused fence before any external send',async()=>{
 let sends=0;const result=await withPushDelivery(pool,installation,async()=>{sends++;return true;});assert.equal(result,false);assert.equal(sends,0);
 const state=(await claim()).json();await register(A,{installation_secret:secret,installation_epoch:state.epoch,resume_push:true});
 assert.equal(await withPushDelivery(pool,installation,async()=>{sends++;return true;}),true);assert.equal(sends,1);
});
test('logged-out clean installation can establish a paused fence but cannot claim legacy registrations',async()=>{
 const clean=await app.inject({method:'POST',url:'/auth/account-push-disable',payload:{installation_id:'unused-installation',secret}});assert.equal(clean.statusCode,200,clean.body);
 assert.equal((await db.query('SELECT paused FROM account_push_installations WHERE installation_id=$1',['unused-installation'])).rows[0].paused,true);
 await db.query('INSERT INTO push_devices VALUES($1,$2,$3)',[A,'unclaimed-legacy','token']);
 const blocked=await app.inject({method:'POST',url:'/auth/account-push-disable',payload:{installation_id:'unclaimed-legacy',secret}});assert.equal(blocked.statusCode,409);
 assert.equal((await db.query('SELECT * FROM push_devices WHERE installation_id=$1',['unclaimed-legacy'])).rows.length,1);
});
