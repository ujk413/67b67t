import {test,before,after} from 'node:test';
import fs from 'node:fs';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createNativePushSender,installNativePush} from '../api/push.js';
const require=createRequire(import.meta.url),{PGlite}=require('../api/node_modules/@electric-sql/pglite'),Fastify=require('../api/node_modules/fastify');
const U='00000000-0000-4000-8000-000000000001',V='00000000-0000-4000-8000-000000000002',S='00000000-0000-4000-8000-000000000003',T='00000000-0000-4000-8000-000000000004',R='00000000-0000-4000-8000-000000000005';
let db,app;const deliveries=[];
const pool={query:(sql,params)=>db.query(sql,params),connect:async()=>({query:(sql,params)=>db.query(sql,params),release(){}})},logger={warn(){}};
const options={configured:()=>true,messaging:()=>({send:async message=>{deliveries.push(message);return 'fcm-id';}})};
before(async()=>{
  db=new PGlite();await db.exec(`CREATE TABLE users(id uuid PRIMARY KEY);CREATE TABLE sessions(id uuid PRIMARY KEY,user_id uuid,expires_at timestamptz);
  CREATE TABLE push_devices(installation_id text,user_id uuid,session_id uuid,token text,active_room_id uuid,quick_reply boolean NOT NULL DEFAULT false,updated_at timestamptz DEFAULT now(),PRIMARY KEY(user_id,installation_id));
  CREATE TABLE room_members(room_id uuid,user_id uuid);CREATE TABLE room_preferences(user_id uuid,room_id uuid,muted boolean);
  CREATE TABLE messages(id uuid,room_id uuid,sender_id uuid,read_by jsonb);
  INSERT INTO users VALUES('${U}'),('${V}');INSERT INTO sessions VALUES('${S}','${U}',now()+interval '1 day'),('${T}','${U}',now()+interval '1 day');
  INSERT INTO push_devices(installation_id,user_id,session_id,token,quick_reply)VALUES('one-device','${U}','${S}','one-token',true),('two-device','${U}','${T}','two-token',false);
  INSERT INTO room_members VALUES('${R}','${U}');INSERT INTO messages VALUES('${S}','${R}','${V}','[]');`);
  await db.exec(fs.readFileSync(new URL("../api/account-push-policy.sql",import.meta.url),"utf8"));
  app=Fastify();installNativePush(app,{pool,logger,options,requireAuth:async req=>{
    if(req.headers.authorization!=='Bearer fixture')throw Object.assign(new Error('unauthorized'),{statusCode:401});return{id:U,sessionId:S};
  }});await app.ready();
});
after(async()=>{await app.close();await db.close();});

test('native push tests require login and target only the current session without leaking device tokens',async()=>{
  assert.equal((await app.inject({url:'/me/push-status'})).statusCode,401);
  assert.equal((await app.inject({url:'/me/push-test',method:'POST',payload:{}})).statusCode,401);
  const headers={authorization:'Bearer fixture'};
  const status=(await app.inject({url:'/me/push-status',headers})).json();assert.deepEqual(status,{available:true,registered:true});
  const result=(await app.inject({url:'/me/push-test',headers,method:'POST',payload:{}})).json();assert.equal(result.sent,1);
  assert.equal(deliveries.at(-1).token,'one-token');assert.equal(deliveries.at(-1).notification.body,'Уведомления работают');
});
test('native delivery respects active, muted, revoked chats and sets a private branded notification',async()=>{
  const send=createNativePushSender(pool,logger,options);
  await db.query('UPDATE push_devices SET active_room_id=$1',[R]);
  assert.equal((await send(U,'Secret sender','Private text',{room_id:R})).sent,0);
  await db.exec('UPDATE push_devices SET active_room_id=NULL');
  assert.equal((await send(U,'Secret sender','Private text',{room_id:R,message_id:S})).sent,2);
  const payload=deliveries.findLast(item=>item.token==='one-token');assert.equal(payload.notification,undefined);assert.equal(payload.data.quality_message,'true');
  assert.equal(payload.data.room_id,R);assert.equal(payload.data.recipient_id,U);assert.equal(payload.android.priority,'high');
  const legacy=deliveries.findLast(item=>item.token==='two-token');assert.equal(legacy.notification.title,'67chat');assert.equal(legacy.notification.body,'Новое сообщение');assert.equal(legacy.android.notification.channelId,'messages');
  assert.ok(!JSON.stringify(payload).includes('Private text'));assert.ok(!JSON.stringify(payload).includes('Secret sender'));
  await db.query('INSERT INTO room_preferences VALUES($1,$2,true)',[U,R]);assert.equal((await send(U,'','',{room_id:R})).sent,0);
  await db.exec('DELETE FROM room_preferences;DELETE FROM room_members');assert.equal((await send(U,'','',{room_id:R})).sent,0);
});
test('invalid device tokens are removed without deleting a replacement received during delivery',async()=>{
  const send=createNativePushSender(pool,logger,{configured:()=>true,messaging:()=>({send:async message=>{
    if(message.token==='one-token')await db.query("UPDATE push_devices SET token='replacement' WHERE installation_id='one-device'");
    throw Object.assign(new Error('expired'),{code:'messaging/registration-token-not-registered'});
  }})});
  await send(U,'','',{test:true});
  const devices=(await db.query('SELECT installation_id,token FROM push_devices')).rows;
  assert.deepEqual(devices,[{installation_id:'one-device',token:'replacement'}]);
});

test('the same physical FCM token delivers distinct account recipients and invalid cleanup is user scoped',async()=>{
 const other='00000000-0000-4000-8000-000000000055';await db.query("INSERT INTO sessions VALUES($1,$2,now()+interval '1 day')",[other,V]);
 await db.query("INSERT INTO push_devices(installation_id,user_id,session_id,token)VALUES('shared-device',$1,$3,'shared-token'),('shared-device',$2,$4,'shared-token')",[U,V,S,other]);
 const sent=[],send=createNativePushSender(pool,logger,{configured:()=>true,messaging:()=>({send:async p=>sent.push(p)})});await send(U,'','',{test:true});await send(V,'','',{test:true});
 const shared=sent.filter(p=>p.token==='shared-token');assert.deepEqual(shared.map(p=>p.data.recipientId),[U,V]);assert.deepEqual(shared.map(p=>p.data.recipient_id),[U,V]);assert.notEqual(shared[0].android.notification.tag,shared[1].android.notification.tag);
 const expired=createNativePushSender(pool,logger,{configured:()=>true,messaging:()=>({send:async()=>{throw Object.assign(Error('expired'),{code:'messaging/registration-token-not-registered'});}})});await expired(U,'','',{test:true});
 assert.deepEqual((await db.query("SELECT user_id FROM push_devices WHERE installation_id='shared-device'")).rows,[{user_id:V}]);
});

test('a policy pause after device selection fences every pending native delivery',async()=>{
 const installation='fence-device';await db.query("INSERT INTO push_devices(installation_id,user_id,session_id,token)VALUES($1,$2,$3,'fenced-token')",[installation,U,S]);await db.query('INSERT INTO account_push_installations(installation_id,secret_hash)VALUES($1,$2)',[installation,'a'.repeat(64)]);
 let paused=false,sent=0;const delayingPool={...pool,query:async(sql,args)=>{const result=await pool.query(sql,args);if(!paused && sql.includes('SELECT count(*)::int AS n FROM messages')){paused=true;await db.query('UPDATE account_push_installations SET paused=true WHERE installation_id=$1',[installation]);await db.query('DELETE FROM push_devices WHERE installation_id=$1',[installation]);}return result;}};
 const send=createNativePushSender(delayingPool,logger,{configured:()=>true,messaging:()=>({send:async()=>{sent++;}})});const result=await send(U,'','',{test:true});assert.equal(paused,true);assert.equal(sent,0);assert.equal(result.sent,0);
});
