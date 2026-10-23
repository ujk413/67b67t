import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {randomUUID, webcrypto} from 'node:crypto';
const require=createRequire(import.meta.url);
const Fastify=require('../api/node_modules/fastify');
const {PGlite}=require('../api/node_modules/@electric-sql/pglite');
const payloads=await import('../web/expressive-payloads.js').catch(()=>({}));
const server=await import('../api/expressive-features.js').catch(()=>({}));
const history=await import('../web/expressive-history.js');
const map=await import('../web/expressive-map.js');
const uuid=()=>randomUUID();
test('full export traversal reads subsequent pages and refuses a stalled cursor',async()=>{
 const roomId=uuid(),rows=Array.from({length:401},(_,i)=>({id:uuid(),room_id:roomId,created_at:new Date(400-i).toISOString()}));
 const result=await history.fetchAllRoomMessages(roomId,async(_room,before,limit)=>{const start=before?rows.findIndex(r=>r.id===before)+1:0;return {messages:rows.slice(start,start+limit).reverse(),has_older:start+limit<rows.length};});
 assert.equal(result.length,401);assert.equal(new Set(result.map(x=>x.id)).size,401);
 await assert.rejects(history.fetchAllRoomMessages(roomId,async()=>({messages:rows.slice(0,100),has_older:true})),/stalled/);
});
test('private map renderer projects GeoJSON locally and rejects excessive geometry',()=>{
 assert.deepEqual(map.projectPoint(37,55),[217,35]);
 assert.deepEqual(map.mapPaths({type:'FeatureCollection',features:[{geometry:{type:'LineString',coordinates:[[37,55],[38,56]]}}]}),['M217.000 35.000L218.000 34.000']);
 assert.match(map.mapViewport(55,37),/^205 29 24 12$/);
 assert.throws(()=>map.mapPaths({type:'LineString',coordinates:[[37,55],[38,56]]},1),/invalid_map_bundle/);
});
test('typed payload transport exists without adding a parallel plaintext send path',()=>{
 assert.equal(typeof payloads.encodeMessagePayload,'function');
 assert.equal(typeof server.installExpressiveFeatures,'function');
});
test('emoji categories combine with bilingual search and retain category boundaries',()=>{
 assert.ok(payloads.EMOJI_CATEGORIES.some(category=>category.id==='animals'));
 assert.deepEqual(payloads.searchEmoji('cat',[],'animals').map(item=>item.emoji),['🐈']);
 assert.equal(payloads.searchEmoji('cat',[],'faces').length,0);
});

test('parser rejects unknown fields, coordinates outside Earth, keys and remote media URLs',()=>{
 assert.equal(typeof payloads.parseMessagePayload,'function');
 const parse=x=>payloads.parseMessagePayload('[67chat:v1]'+JSON.stringify(x));
 assert.equal(parse({kind:'location',version:1,id:uuid(),latitude:91,longitude:0,live:false,expires_at:new Date().toISOString()}),null);
 assert.equal(parse({kind:'contact',version:1,username:'alice',phone:'+123'}),null);
 assert.equal(parse({kind:'sticker',version:1,media_id:uuid(),url:'https://tracker.example/x'}),null);
 assert.equal(parse({kind:'contact',version:1,username:'alice',session:'secret'}),null);
 assert.equal(payloads.parseMessagePayload('normal text'),null);
 const value={kind:'contact',version:1,username:'alice',display_name:'Алиса'};
 assert.deepEqual(payloads.parseMessagePayload(payloads.encodeMessagePayload(value)),value);
});

test('poll reducers authenticate closing and voting, enforce expiry and show quiz answers only after a valid reveal',async()=>{
 assert.equal(typeof payloads.reducePoll,'function');
 const id=uuid(),room=uuid(),now=Date.parse('2026-10-23T10:00:00Z'),salt='secret-random-salt-32-characters!!';
 const commitment=await payloads.quizCommitment('a',salt,webcrypto);
 const poll={kind:'poll',version:1,id,question:'2+2?',options:[{id:'a',text:'4'},{id:'b',text:'5'}],multiple:false,quiz:true,answer_commitment:commitment,expires_at:new Date(now+60000).toISOString()};
 const message=(sender,p,at)=>({id:uuid(),room_id:room,sender_id:sender,created_at:new Date(now+at).toISOString(),text:payloads.encodeMessagePayload(p)});
 const origin=message('owner',poll,0),events=[message('voter',{kind:'poll_vote',version:1,poll_id:id,choices:['a'],revision:1},1000),message('intruder',{kind:'poll_close',version:1,poll_id:id},2000),message('voter',{kind:'poll_vote',version:1,poll_id:id,choices:['b'],revision:2},3000)];
 let state=await payloads.reducePoll(origin,events,now+4000,webcrypto);assert.equal(state.closed,false);assert.deepEqual(state.counts,{a:0,b:1});assert.equal(state.answer,null);
 events.push(message('owner',{kind:'poll_close',version:1,poll_id:id,answer:{option_id:'a',salt}},5000));
 events.push(message('late',{kind:'poll_vote',version:1,poll_id:id,choices:['a'],revision:1},6000));
 state=await payloads.reducePoll(origin,events,now+7000,webcrypto);assert.equal(state.closed,true);assert.equal(state.answer,'a');assert.equal(state.voters,1);
 assert.equal(payloads.parseMessagePayload(payloads.PAYLOAD_PREFIX+JSON.stringify({...poll,correct_option:'a'})),null);
});

test('live location rejects updates from other senders, prevents replay and stops at the original deadline',()=>{
 assert.equal(typeof payloads.reduceLocation,'function');
 const id=uuid(),now=Date.parse('2026-10-23T10:00:00Z'),base={id:uuid(),room_id:'room',sender_id:'owner',created_at:new Date(now).toISOString(),text:payloads.encodeMessagePayload({kind:'location',version:1,id,latitude:55,longitude:37,live:true,expires_at:new Date(now+60000).toISOString()})};
 const event=(sender,sequence,lat)=>({id:uuid(),room_id:'room',sender_id:sender,created_at:new Date(now+sequence*1000).toISOString(),text:payloads.encodeMessagePayload({kind:'location_update',version:1,location_id:id,latitude:lat,longitude:37,sequence})});
 let state=payloads.reduceLocation(base,[event('owner',2,56),event('owner',1,50),event('other',3,70)],now+5000);assert.equal(state.latitude,56);assert.equal(state.active,true);
 state=payloads.reduceLocation(base,[],now+60000);assert.equal(state.active,false);
 const stop={...event('owner',3,56),text:payloads.encodeMessagePayload({kind:'location_stop',version:1,location_id:id})};
 assert.equal(payloads.reduceLocation(base,[stop,event('owner',4,60)],now+5000).active,false);
});

test('export requires explicit warning acknowledgment, excludes secret chats and strips all key/session/meta fields',async()=>{
 assert.equal(typeof payloads.buildChatExport,'function');
 const room={id:'r',title:'<img src=x onerror=alert(1)>'},message={id:'m',room_id:'r',sender_id:'u',created_at:'2026-10-23T00:00:00Z',type:'text',text:'<script>secret</script>',encrypted_content:'cipher',meta:{key:'never'},session:'never'};
 await assert.rejects(()=>payloads.buildChatExport({room,messages:[message],decode:async m=>m}),/acknowledgment/);
 await assert.rejects(()=>payloads.buildChatExport({room:{...room,is_secret:true},messages:[message],acknowledged:true,decode:async m=>m}),/secret/);
 const result=await payloads.buildChatExport({room,messages:[message],acknowledged:true,decode:async m=>({...m,_decrypted:true}),format:'html'});
 assert.match(result,/&lt;script&gt;secret/);assert.doesNotMatch(result,/<script>|cipher|never|onerror=alert\(1\)>/);
 const json=JSON.parse(await payloads.buildChatExport({room,messages:[message],acknowledged:true,decode:async m=>({...m,_decrypted:true})}));
 assert.deepEqual(Object.keys(json.messages[0]).sort(),['created_at','id','sender_id','text','type']);
 await assert.rejects(()=>payloads.buildChatExport({room,messages:[message],acknowledged:true,decode:async m=>m}),/decrypt/);
});
test('export includes only selected visible attachments and prevents executable inline types',async()=>{
 const room={id:'r'},message={id:'m',room_id:'r',sender_id:'u',created_at:'2026-10-23T00:00:00Z',type:'media'};
 const attachments=[{message_id:'m',name:'<x>.html',mime:'text/html',base64:'aGVsbG8=',token:'secret'},{message_id:'hidden',name:'private',mime:'image/png',base64:'aA=='}];
 const json=JSON.parse(await payloads.buildChatExport({room,messages:[message],decode:async m=>m,acknowledged:true,attachments}));
 assert.equal(json.attachments.length,1);assert.equal(json.attachments[0].mime,'application/octet-stream');assert.equal(json.attachments[0].token,undefined);
 const html=await payloads.buildChatExport({room,messages:[message],decode:async m=>m,acknowledged:true,attachments,format:'html'});assert.match(html,/data:application\/octet-stream;base64,aGVsbG8=/);assert.doesNotMatch(html,/<x>|data:text\/html|private|secret/);
});

async function harness(){
 assert.equal(typeof server.installExpressiveFeatures,'function');
 const db=new PGlite();await db.exec(`CREATE TABLE users(id uuid PRIMARY KEY,username text);CREATE TABLE rooms(id uuid PRIMARY KEY,kind text,e2e_enabled boolean DEFAULT false);CREATE TABLE room_members(room_id uuid,user_id uuid,role text);CREATE TABLE media(id uuid PRIMARY KEY,owner_id uuid,mime text,size_bytes bigint);CREATE TABLE messages(id uuid PRIMARY KEY,room_id uuid,sender_id uuid,text text,meta jsonb DEFAULT '{}',encrypted_content text,created_at timestamptz DEFAULT now());CREATE TABLE hidden_messages(user_id uuid,message_id uuid);`);
 const owner=uuid(),other=uuid(),outsider=uuid(),room=uuid(),media=uuid();
 await db.query('INSERT INTO users VALUES ($1,$2),($3,$4),($5,$6)',[owner,'owner',other,'other',outsider,'outsider']);await db.query("INSERT INTO rooms VALUES ($1,'group',false)",[room]);await db.query("INSERT INTO room_members VALUES ($1,$2,'owner'),($1,$3,'member')",[room,owner,other]);await db.query("INSERT INTO media VALUES ($1,$2,'image/webp',1024)",[media,owner]);
 // pg's unparameterized query uses the simple protocol; PGlite exposes it as exec.
 const pool={query:(sql,args)=>args?db.query(sql,args):db.exec(sql)};
 const app=Fastify(),broadcasts=[];const deps={pool,requireAuth:async req=>{if(!req.headers['x-test-user'])throw Object.assign(Error('unauthorized'),{statusCode:401});return{id:req.headers['x-test-user']};},isRoomMember:async(r,u)=>(await db.query('SELECT 1 FROM room_members WHERE room_id=$1 AND user_id=$2',[r,u])).rows.length>0,getRoomRole:async(r,u)=>(await db.query('SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2',[r,u])).rows[0]?.role,broadcastToRoom:(...args)=>broadcasts.push(args)};
 let features;try{features=await server.installExpressiveFeatures(app,deps);}catch(error){await app.close();await db.close();throw error;}
 const request=(method,url,payload,user=owner)=>app.inject({method,url,payload,headers:{'x-test-user':user}});
 return{app,db,owner,other,outsider,room,media,features,request,close:async()=>{await app.close();await db.close();}};
}
test('catalog publication requires ownership and explicit public scope, supports search/cursors and private recent isolation',async()=>{
 const h=await harness();try{
  let response=await h.request('POST','/expressive/packs',{title:'Коты cats',tags:['кот','cat'],visibility:'private'});assert.equal(response.statusCode,200);const pack=response.json();
  response=await h.request('POST',`/expressive/packs/${pack.id}/items`,{media_id:h.media,kind:'sticker',label:'кот cat',tags:['кот']},h.other);assert.equal(response.statusCode,404);
  response=await h.request('POST',`/expressive/packs/${pack.id}/items`,{media_id:h.media,kind:'sticker',label:'кот cat',tags:['кот']});assert.equal(response.statusCode,200);const item=response.json();
  assert.equal((await h.request('GET','/expressive/catalog?q=кот',undefined,h.other)).json().items.length,0);
  assert.equal((await h.request('GET','/expressive/catalog?q=cat')).json().items[0].id,item.id);
  assert.equal((await h.request('GET','/expressive/catalog?cursor=broken')).statusCode,400);
  assert.equal((await h.request('GET','/expressive/catalog?limit=999999')).statusCode,400);
  response=await h.request('POST',`/expressive/recent/${item.id}`,{});assert.equal(response.statusCode,200);
  assert.equal((await h.request('GET','/expressive/recent',undefined,h.other)).json().items.length,0);
  assert.equal(await h.features.canReadCatalogMedia(h.media,h.other),false);
  assert.equal((await h.request('PATCH',`/expressive/packs/${pack.id}`,{visibility:'public'})).statusCode,200);
  assert.equal(await h.features.canReadCatalogMedia(h.media,h.other),true);
  assert.equal((await h.request('GET','/expressive/catalog?q=кот',undefined,h.other)).json().items.length,1);
 }finally{await h.close();}
});
test('installed packs are account scoped and private packs cannot be installed by other users',async()=>{
 const h=await harness();try{
  const pack=(await h.request('POST','/expressive/packs',{title:'Private'})).json(),path='/expressive/installed/'+pack.id;
  assert.equal((await h.request('PUT',path,{},h.other)).statusCode,404);
  assert.equal((await h.request('PUT',path,{})).statusCode,200);
  assert.equal((await h.request('GET','/expressive/installed')).json().items[0].id,pack.id);
  assert.equal((await h.request('GET','/expressive/installed',undefined,h.other)).json().items.length,0);
  assert.equal((await h.request('DELETE',path)).statusCode,200);
  assert.equal((await h.request('GET','/expressive/installed')).json().items.length,0);
 }finally{await h.close();}
});

test('ordinary dice are server issued, membership protected, immutable and cannot be moved between rooms or claimed by another sender',async()=>{
 const h=await harness();try{
  assert.equal((await h.request('POST',`/expressive/rooms/${h.room}/dice`,{},h.outsider)).statusCode,404);
  const result=await h.request('POST',`/expressive/rooms/${h.room}/dice`,{});assert.equal(result.statusCode,200);const dice=result.json();assert.ok(dice.value>=1&&dice.value<=6);
  const text=payloads.encodeMessagePayload({kind:'dice',version:1,id:dice.id,value:dice.value,source:'server'});
  await h.features.validatePlaintext({roomId:h.room,userId:h.owner,text});
  await assert.rejects(()=>h.features.validatePlaintext({roomId:h.room,userId:h.other,text}),/invalid_dice/);
  await assert.rejects(()=>h.features.validatePlaintext({roomId:h.room,userId:h.owner,text:payloads.encodeMessagePayload({kind:'dice',version:1,id:dice.id,value:7-dice.value,source:'server'})}),/invalid_dice/);
  assert.equal(h.features.canEditText(text),false);
  await h.db.query('UPDATE rooms SET e2e_enabled=true WHERE id=$1',[h.room]);assert.equal((await h.request('POST',`/expressive/rooms/${h.room}/dice`,{})).statusCode,409);
 }finally{await h.close();}
});

test('encrypted ordinary dice can bind once through an authenticated message reference and resist edits and hidden reads',async()=>{
 const h=await harness();try{
  const dice=(await h.request('POST',`/expressive/rooms/${h.room}/dice`,{})).json(),message=uuid(),second=uuid();
  await h.db.query("INSERT INTO messages(id,room_id,sender_id,text,encrypted_content) VALUES($1,$2,$3,NULL,'opaque'),($4,$2,$3,NULL,'another')",[message,h.room,h.owner,second]);
  const path=`/expressive/rooms/${h.room}/dice/${dice.id}`;
  let result=await h.request('POST',path+'/bind',{message_id:message});assert.equal(result.statusCode,200);
  assert.equal((await h.request('POST',path+'/bind',{message_id:second})).statusCode,409);
  assert.equal((await h.request('GET',path,undefined,h.other)).json().message_id,message);
  await assert.rejects(()=>h.db.query("UPDATE messages SET encrypted_content='forged' WHERE id=$1",[message]),/immutable_dice_message/);
  await h.db.query('INSERT INTO hidden_messages VALUES($1,$2)',[h.other,message]);
  assert.equal((await h.request('GET',path,undefined,h.other)).statusCode,404);
  await h.db.query('DELETE FROM messages WHERE id=$1',[message]);
  assert.equal((await h.request('GET',path)).statusCode,404);
 }finally{await h.close();}
});
