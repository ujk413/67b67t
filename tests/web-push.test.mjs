import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { installWebPush, createWebPushSender, validateSubscription, webPushSchema } from '../api/web-push.js';
const require = createRequire(import.meta.url), { PGlite } = require('../api/node_modules/@electric-sql/pglite'), Fastify = require('../api/node_modules/fastify');
const U='00000000-0000-4000-8000-000000000001', V='00000000-0000-4000-8000-000000000002', S='00000000-0000-4000-8000-000000000003', R='00000000-0000-4000-8000-000000000004';
const subscription={endpoint:'https://fcm.googleapis.com/fcm/send/test-endpoint',keys:{p256dh:'B'.repeat(87),auth:'A'.repeat(22)}};
let db, app;
const pool={query:async(sql,params)=>params?db.query(sql,params):{rows:await db.exec(sql)}};
const logger={warn(){}};
before(async()=>{
  db=new PGlite();
  await db.exec(`CREATE TABLE users(id uuid PRIMARY KEY);CREATE TABLE sessions(id uuid PRIMARY KEY,user_id uuid REFERENCES users(id) ON DELETE CASCADE,expires_at timestamptz DEFAULT now()+interval '1 day');
  CREATE TABLE room_members(room_id uuid,user_id uuid);CREATE TABLE room_preferences(user_id uuid,room_id uuid,muted boolean DEFAULT false);
  INSERT INTO users VALUES('${U}'),('${V}');INSERT INTO sessions(id,user_id)VALUES('${S}','${U}');INSERT INTO room_members VALUES('${R}','${U}');`);
  await db.exec(`CREATE TABLE web_push_subscriptions(endpoint text PRIMARY KEY,user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,session_id uuid NOT NULL REFERENCES sessions(id)ON DELETE CASCADE,subscription jsonb NOT NULL,active_room_id uuid,updated_at timestamptz NOT NULL DEFAULT now());`);
  await db.query('INSERT INTO web_push_subscriptions(endpoint,user_id,session_id,subscription)VALUES($1,$2,$3,$4)',[subscription.endpoint,U,S,JSON.stringify(subscription)]);
  app=Fastify();
  await installWebPush(app,{pool,logger,requireAuth:async req=>{if(req.headers.authorization!=='Bearer test')throw Object.assign(new Error('unauthorized'),{statusCode:401});return{id:req.headers['x-user']||U,sessionId:req.headers['x-session']||S}},isRoomMember:async(room,user)=>room===R&&user===U});
});
after(async()=>{await app.close();await db.close()});
test('push subscription validation rejects private networks, credentials, alternate ports and invalid keys',()=>{
  assert.ok(validateSubscription(subscription));
  for(const endpoint of ['http://fcm.googleapis.com/send','https://127.0.0.1/','https://fcm.googleapis.com.evil.test/','https://user:pass@fcm.googleapis.com/send','https://fcm.googleapis.com:8443/send'])assert.ok(!validateSubscription({...subscription,endpoint}));
  assert.ok(!validateSubscription({...subscription,keys:{p256dh:'bad',auth:'bad'}}));
});
test('browser subscription migration preserves the old row and is idempotent',async()=>{
 assert.equal((await db.query('SELECT * FROM web_push_subscriptions')).rows.length,1);
 await db.exec(webPushSchema);await db.exec(webPushSchema);
 assert.deepEqual((await db.query('SELECT user_id,endpoint FROM web_push_subscriptions')).rows,[{user_id:U,endpoint:subscription.endpoint}]);
 const primary=(await db.query("SELECT pg_get_constraintdef(oid) AS key FROM pg_constraint WHERE conrelid='web_push_subscriptions'::regclass AND contype='p'")).rows[0].key;assert.equal(primary,'PRIMARY KEY (user_id, endpoint)');
});

test('push registration requires a session and validates the currently viewed chat',async()=>{
  assert.equal((await app.inject({method:'PUT',url:'/me/web-push',payload:{subscription}})).statusCode,401);
  assert.equal((await app.inject({method:'PUT',url:'/me/web-push',headers:{authorization:'Bearer test'},payload:{subscription,active_room_id:V}})).statusCode,404);
  assert.equal((await app.inject({method:'PUT',url:'/me/web-push',headers:{authorization:'Bearer test'},payload:{subscription,active_room_id:R}})).statusCode,200);
});
test('background delivery skips visible chats and muted rooms, carries no message content, removes expired subscriptions',async()=>{
  process.env.WEB_PUSH_PUBLIC_KEY='test-public';process.env.WEB_PUSH_PRIVATE_KEY='test-private';
  const deliveries=[];
  const send=createWebPushSender(pool,logger,{sendNotification:async(sub,payload)=>deliveries.push(JSON.parse(payload))});
  assert.equal((await send(U,{room_id:R,message_id:V})).sent,0);
  await pool.query('UPDATE web_push_subscriptions SET active_room_id=NULL');
  assert.equal((await send(U,{room_id:R,message_id:V,text:'private plaintext'})).sent,1);
  assert.equal(deliveries[0].body,'Новое сообщение');assert.ok(!JSON.stringify(deliveries).includes('private plaintext'));
  await pool.query('INSERT INTO room_preferences(user_id,room_id,muted) VALUES($1,$2,true)',[U,R]);
  assert.equal((await send(U,{room_id:R})).sent,0);
  await pool.query('UPDATE room_preferences SET muted=false');
  const expired=createWebPushSender(pool,logger,{sendNotification:async()=>{throw{statusCode:410}}});
  await expired(U,{room_id:R});
  assert.equal((await pool.query('SELECT * FROM web_push_subscriptions',[])).rows.length,0);
  delete process.env.WEB_PUSH_PUBLIC_KEY;delete process.env.WEB_PUSH_PRIVATE_KEY;
});

test('one browser endpoint retains both accounts and stale delivery/logout cannot erase another subscription',async()=>{
 process.env.WEB_PUSH_PUBLIC_KEY='test-public';process.env.WEB_PUSH_PRIVATE_KEY='test-private';
 const otherSession='00000000-0000-4000-8000-000000000055',newSession='00000000-0000-4000-8000-000000000056';
 await db.query('INSERT INTO sessions(id,user_id)VALUES($1,$3),($2,$4)',[otherSession,newSession,V,U]);
 const headers=(user,session)=>({authorization:'Bearer test','x-user':user,'x-session':session});
 try{
  for(const [user,session]of[[U,S],[V,otherSession]])assert.equal((await app.inject({method:'PUT',url:'/me/web-push',headers:headers(user,session),payload:{subscription}})).statusCode,200);
  assert.equal((await pool.query('SELECT * FROM web_push_subscriptions',[])).rows.length,2);
  const deliveries=[],send=createWebPushSender(pool,logger,{sendNotification:async(sub,payload)=>deliveries.push(JSON.parse(payload))});
  await send(U,{test:true});await send(V,{test:true});assert.deepEqual(deliveries.map(p=>p.recipientId),[U,V]);assert.notEqual(deliveries[0].tag,deliveries[1].tag);assert.ok(deliveries[0].url.includes('account_id='+U));
  const replacement={...subscription,keys:{...subscription.keys,auth:'C'.repeat(22)}};
  const late=createWebPushSender(pool,logger,{sendNotification:async()=>{await app.inject({method:'PUT',url:'/me/web-push',headers:headers(U,newSession),payload:{subscription:replacement}});throw{statusCode:410};}});
  await late(U,{test:true});assert.equal((await pool.query('SELECT * FROM web_push_subscriptions',[])).rows.length,2);
  await app.inject({method:'DELETE',url:'/me/web-push',headers:headers(U,S),payload:{endpoint:subscription.endpoint}});assert.equal((await pool.query('SELECT * FROM web_push_subscriptions',[])).rows.length,2);
  await app.inject({method:'DELETE',url:'/me/web-push',headers:headers(U,newSession),payload:{endpoint:subscription.endpoint}});assert.deepEqual((await pool.query('SELECT user_id FROM web_push_subscriptions',[])).rows,[{user_id:V}]);
 }finally{delete process.env.WEB_PUSH_PUBLIC_KEY;delete process.env.WEB_PUSH_PRIVATE_KEY;await pool.query('DELETE FROM web_push_subscriptions',[]);}
});
