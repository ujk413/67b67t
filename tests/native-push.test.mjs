import {test,before,after} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createNativePushSender,installNativePush} from '../api/push.js';
const require=createRequire(import.meta.url),{PGlite}=require('../api/node_modules/@electric-sql/pglite'),Fastify=require('../api/node_modules/fastify');
const U='00000000-0000-4000-8000-000000000001',V='00000000-0000-4000-8000-000000000002',S='00000000-0000-4000-8000-000000000003',T='00000000-0000-4000-8000-000000000004',R='00000000-0000-4000-8000-000000000005';
let db,app;const deliveries=[];
const pool={query:(sql,params)=>db.query(sql,params)},logger={warn(){}};
const options={configured:()=>true,messaging:()=>({send:async message=>{deliveries.push(message);return 'fcm-id';}})};
before(async()=>{
  db=new PGlite();await db.exec(`CREATE TABLE users(id uuid PRIMARY KEY);CREATE TABLE sessions(id uuid PRIMARY KEY,user_id uuid,expires_at timestamptz);
  CREATE TABLE push_devices(installation_id text PRIMARY KEY,user_id uuid,session_id uuid,token text,active_room_id uuid,updated_at timestamptz DEFAULT now());
  CREATE TABLE room_members(room_id uuid,user_id uuid);CREATE TABLE room_preferences(user_id uuid,room_id uuid,muted boolean);
  CREATE TABLE messages(id uuid,room_id uuid,sender_id uuid,read_by jsonb);
  INSERT INTO users VALUES('${U}'),('${V}');INSERT INTO sessions VALUES('${S}','${U}',now()+interval '1 day'),('${T}','${U}',now()+interval '1 day');
  INSERT INTO push_devices(installation_id,user_id,session_id,token)VALUES('one','${U}','${S}','one-token'),('two','${U}','${T}','two-token');
  INSERT INTO room_members VALUES('${R}','${U}');INSERT INTO messages VALUES('${S}','${R}','${V}','[]');`);
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
  const payload=deliveries.at(-1);assert.equal(payload.notification.title,'67chat');assert.equal(payload.notification.body,'Новое сообщение');
  assert.equal(payload.android.notification.channelId,'messages');assert.equal(payload.android.notification.notificationCount,1);
  assert.ok(!JSON.stringify(payload).includes('Private text'));assert.ok(!JSON.stringify(payload).includes('Secret sender'));
  await db.query('INSERT INTO room_preferences VALUES($1,$2,true)',[U,R]);assert.equal((await send(U,'','',{room_id:R})).sent,0);
  await db.exec('DELETE FROM room_preferences;DELETE FROM room_members');assert.equal((await send(U,'','',{room_id:R})).sent,0);
});
test('invalid device tokens are removed without deleting a replacement received during delivery',async()=>{
  const send=createNativePushSender(pool,logger,{configured:()=>true,messaging:()=>({send:async message=>{
    if(message.token==='one-token')await db.query("UPDATE push_devices SET token='replacement' WHERE installation_id='one'");
    throw Object.assign(new Error('expired'),{code:'messaging/registration-token-not-registered'});
  }})});
  await send(U,'','',{test:true});
  const devices=(await db.query('SELECT installation_id,token FROM push_devices')).rows;
  assert.deepEqual(devices,[{installation_id:'one',token:'replacement'}]);
});
