import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {createServer} from 'node:http';
import {once} from 'node:events';
import {createRequire} from 'node:module';
import {verifySfuGrant,installSfuGateway} from '../api/sfu-gateway.js';
const require=createRequire(new URL('../api/package.json',import.meta.url)),Fastify=require('fastify'),websocket=require('@fastify/websocket'),{WebSocket,WebSocketServer}=require('ws'),{PGlite}=require('@electric-sql/pglite');
const user='00000000-0000-4000-8000-000000000001',room='00000000-0000-4000-8000-000000000002',lease='00000000-0000-4000-8000-000000000003',session='00000000-0000-4000-8000-000000000004',key='test-key',secret='test-secret-'.repeat(4);
function token(extra={},header={alg:'HS256',typ:'JWT'}){const encode=x=>Buffer.from(JSON.stringify(x)).toString('base64url'),head=encode(header),body=encode({iss:key,sub:user+':'+lease,nbf:Math.floor(Date.now()/1000)-5,exp:Math.floor(Date.now()/1000)+90,video:{roomJoin:true,room:'67chat-'+room},...extra});return`${head}.${body}.${createHmac('sha256',secret).update(head+'.'+body).digest('base64url')}`;}
test('signaling grants require exact signed lease identity, current expiry and join-only role',()=>{
 assert.deepEqual(verifySfuGrant(token(),{key,secret}),{userId:user,roomId:room,leaseId:lease,identity:user+':'+lease});
 for(const value of [token({exp:1}),token({iss:'other'}),token({sub:user}),token({video:{roomJoin:true,room:'other'}}),token({video:{roomJoin:true,room:'67chat-'+room,roomAdmin:true}}),token({}, {alg:'none'}),token()+'x'])assert.throws(()=>verifySfuGrant(value,{key,secret}),/sfu_access_denied/);
});
test('real signaling proxy preserves initial/binary frames and rejects old tokens after lease removal',async t=>{
 const db=new PGlite();t.after(()=>db.close());await db.exec(`CREATE TABLE users(id uuid,banned boolean DEFAULT false,login_disabled_at timestamptz);CREATE TABLE rooms(id uuid,kind text);CREATE TABLE room_members(user_id uuid,room_id uuid);CREATE TABLE sessions(id uuid,user_id uuid,expires_at timestamptz);CREATE TABLE group_call_leases(user_id uuid,room_id uuid,lease_id uuid,session_id uuid,expires_at timestamptz,revoking_at timestamptz);INSERT INTO users(id)VALUES('${user}');INSERT INTO rooms VALUES('${room}','group');INSERT INTO room_members VALUES('${user}','${room}');INSERT INTO sessions VALUES('${session}','${user}',now()+interval '1 day');INSERT INTO group_call_leases VALUES('${user}','${room}','${lease}','${session}',now()+interval '2 minutes',NULL);`);
 let tokenForUpstream;
 const upstream=createServer((req,res)=>{res.setHeader('content-type','application/json');res.end('{"ok":true}');}),wss=new WebSocketServer({server:upstream});let upstreamJoins=0;
 wss.on('connection',(peer,req)=>{if(req.headers.authorization!=='Bearer '+tokenForUpstream&&new URL(req.url,'http://upstream').searchParams.get('access_token')!==tokenForUpstream){peer.close(1008,'missing upstream authorization');return;}upstreamJoins++;peer.send(Buffer.from([1,2,3]));peer.on('message',(data,binary)=>peer.send(data,{binary}));});
 await new Promise(resolve=>upstream.listen(0,'127.0.0.1',resolve));
 const app=Fastify();await app.register(websocket);const gateway=installSfuGateway(app,{pool:{query:(q,p)=>db.query(q,p)},key,secret,enabled:true,upstream:`http://127.0.0.1:${upstream.address().port}`});await app.listen({port:0,host:'127.0.0.1'});
 t.after(async()=>{await app.close();for(const c of wss.clients)c.terminate();await new Promise(resolve=>wss.close(resolve));await new Promise(resolve=>upstream.close(resolve));});
 const valid=token();tokenForUpstream=valid;const path='/livekit/rtc/v1?access_token='+valid;
 assert.equal((await app.inject({url:path+'/unused'})).statusCode,401);
 assert.equal((await app.inject({url:'/livekit/rtc/v1/validate?access_token='+valid})).statusCode,200);
 const client=new WebSocket(`ws://127.0.0.1:${app.server.address().port}${path}`),messages=[];client.on('message',data=>messages.push(Buffer.from(data)));await once(client,'open');client.send(Buffer.from([9,8,7]));
 for(let i=0;i<100&&messages.length<2;i++)await new Promise(resolve=>setTimeout(resolve,10));
 assert.deepEqual(messages.map(x=>[...x]),[[1,2,3],[9,8,7]]);assert.equal(upstreamJoins,1);
 const bearer=new WebSocket(`ws://127.0.0.1:${app.server.address().port}/livekit/rtc/v1`,{headers:{Authorization:'Bearer '+valid}}),bearerMessages=[];bearer.on('message',data=>bearerMessages.push(Buffer.from(data)));await once(bearer,'open');
 for(let i=0;i<100&&bearerMessages.length<1&&bearer.readyState!==WebSocket.CLOSED;i++)await new Promise(resolve=>setTimeout(resolve,10));
 assert.deepEqual(bearerMessages.map(x=>[...x]),[[1,2,3]],'header-authenticated SDK handshakes retain their validated bearer upstream');assert.equal(upstreamJoins,2);
 const closed=once(client,'close'),bearerClosed=once(bearer,'close');gateway.closeLease(user,lease);await db.exec('UPDATE group_call_leases SET revoking_at=now()');await Promise.all([closed,bearerClosed]);
 assert.equal((await app.inject({url:'/livekit/rtc/v1/validate?access_token='+valid})).statusCode,401);
 const rejected=new WebSocket(`ws://127.0.0.1:${app.server.address().port}${path}`);await once(rejected,'close');assert.equal(upstreamJoins,2);
});
