import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {installCalls} from '../api/calls.js';
const require=createRequire(import.meta.url),Fastify=require('../api/node_modules/fastify');
test('background calls expose SDP only to the callee, retain ICE, and invalidate decline capability',async()=>{
 const app=Fastify(),events=[],pushes=[];let time=1000;
 const relay=installCalls(app,{pool:{query:async()=>({rows:[{kind:'dm',user_id:'alice'},{kind:'dm',user_id:'bob'}]})},requireAuth:async req=>({id:req.headers['x-user']}),sendToUser:(user,data)=>events.push({user,...data}),sendToSession:(user,session,data)=>events.push({user,session,...data}),sendToUserExceptSession:(user,except,data)=>events.push({user,except,...data}),push:async(user,a,b,data)=>pushes.push({user,...data}),now:()=>time});
 await app.ready();
 await relay({type:'call.offer',room_id:'room',offer:{type:'offer',sdp:'synthetic-sdp'},is_video:true},{id:'outsider',sessionId:'session-outsider'});assert.equal(events.length,0);
 await relay({type:'call.offer',room_id:'room',offer:{type:'offer',sdp:'synthetic-sdp'},is_video:true},{id:'alice',sessionId:'session-alice',username:'Alice'});
 const incoming=events.find(e=>e.type==='call.offer'),push=pushes.find(e=>e.call_event==='ring');assert.ok(incoming.call_id);assert.equal(push.offer,undefined);assert.equal(push.sdp,undefined);
 assert.equal((await app.inject({url:'/calls/'+incoming.call_id,headers:{'x-user':'outsider'}})).statusCode,404);
 await relay({type:'call.ice',room_id:'room',candidate:{candidate:'ice'}},{id:'alice',sessionId:'session-alice'});
 const response=await app.inject({url:'/calls/'+incoming.call_id,headers:{'x-user':'bob'}});assert.equal(response.json().offer.sdp,'synthetic-sdp');assert.equal(response.json().candidates.length,1);
 assert.equal((await app.inject({method:'POST',url:`/calls/${incoming.call_id}/decline`,payload:{nonce:'wrong'}})).statusCode,404);
 assert.equal((await app.inject({method:'POST',url:`/calls/${incoming.call_id}/decline`,payload:{nonce:push.decline_nonce}})).statusCode,200);
 assert.equal((await app.inject({url:'/calls/'+incoming.call_id,headers:{'x-user':'bob'}})).statusCode,404);
 await relay({type:'call.offer',room_id:'room',offer:{type:'offer',sdp:'next'}},{id:'alice',sessionId:'session-alice'});time+=46000;
 const last=events.filter(e=>e.type==='call.offer').at(-1);assert.equal((await app.inject({url:'/calls/'+last.call_id,headers:{'x-user':'bob'}})).statusCode,404);
 await app.close();
});

test('busy spans rooms and devices, replacement ends old call, history classifies outcomes',async()=>{
 const app=Fastify(),events=[],history=[]; let time=1000;
 const relay=installCalls(app,{pool:{query:async(_,args)=>({rows:[{kind:'dm',user_id:args[0]==='ab'?'alice':'carol'},{kind:'dm',user_id:'bob'}]})},requireAuth:async()=>({id:'bob',sessionId:'session-bob'}),sendToUser:(user,data)=>events.push({user,...data}),sendToSession:(user,session,data)=>events.push({user,session,...data}),sendToUserExceptSession:(user,except,data)=>events.push({user,except,...data}),push:async()=>{},now:()=>time,history:{save:async call=>history.push({...call})}});
 await relay({type:'call.offer',room_id:'ab',offer:{sdp:'first'}},{id:'alice',sessionId:'session-alice'});
 await relay({type:'call.answer',room_id:'ab',answer:{sdp:'answer'}},{id:'bob',sessionId:'session-bob'});
 await relay({type:'call.offer',room_id:'cb',offer:{sdp:'second'}},{id:'carol',sessionId:'session-carol'});
 const waiting=events.find(e=>e.type==='call.waiting');assert.ok(waiting?.call_id);
 assert.equal(events.filter(e=>e.type==='call.offer'&&e.user==='bob').length,1);
 await relay({type:'call.waiting.accept',room_id:'cb',call_id:waiting.call_id},{id:'bob',sessionId:'session-bob'});
 assert.ok(events.find(e=>e.type==='call.hangup'&&e.reason==='replaced'));
 assert.equal(events.filter(e=>e.type==='call.offer'&&e.user==='bob').length,2);
 await relay({type:'call.hangup',room_id:'cb',reason:'rejected'},{id:'bob',sessionId:'session-bob'});
 assert.equal(history[0].status,'completed'); assert.equal(history[1].status,'rejected');
 await app.close();
});
test('ring expiry is persisted and releases globally busy user',async()=>{
 const app=Fastify(),events=[],history=[];let time=1000;
 const relay=installCalls(app,{pool:{query:async()=>({rows:[{kind:'dm',user_id:'alice'},{kind:'dm',user_id:'bob'}]})},requireAuth:async()=>({id:'bob',sessionId:'session-bob'}),sendToUser:(user,data)=>events.push({user,...data}),sendToSession:(user,session,data)=>events.push({user,session,...data}),sendToUserExceptSession:(user,except,data)=>events.push({user,except,...data}),push:async()=>{},now:()=>time,history:{save:async call=>history.push({...call})}});
 await relay({type:'call.offer',room_id:'r',offer:{sdp:'sdp'}},{id:'alice',sessionId:'session-alice'});time+=46000;
 await relay({type:'call.offer',room_id:'r',offer:{sdp:'sdp2'}},{id:'alice',sessionId:'session-alice'});
 assert.equal(history.length,1);assert.equal(history[0].status,'missed');assert.equal(history[0].reason,'timeout');
 assert.equal(events.filter(e=>e.type==='call.offer').length,2);await app.close();
});
test('privacy denial prevents offer and restricts renegotiation to active call',async()=>{
 const app=Fastify(),events=[];
 const relay=installCalls(app,{pool:{query:async()=>({rows:[{kind:'dm',user_id:'alice'},{kind:'dm',user_id:'bob'}]})},requireAuth:async()=>({id:'bob',sessionId:'session-bob'}),sendToUser:(user,data)=>events.push({user,...data}),sendToSession:(user,session,data)=>events.push({user,session,...data}),sendToUserExceptSession:(user,except,data)=>events.push({user,except,...data}),push:async()=>{},canCall:async()=>false});
 await relay({type:'call.offer',room_id:'r',offer:{sdp:'sdp'}},{id:'alice',sessionId:'session-alice'});
 assert.equal(events.filter(e=>e.type==='call.offer').length,0);assert.equal(events[0]?.reason,'not_allowed');await app.close();
});
test('session revocation ends user calls and frees other participants',async()=>{
 const app=Fastify(),events=[],history=[];
 const relay=installCalls(app,{pool:{query:async()=>({rows:[{kind:'dm',user_id:'alice'},{kind:'dm',user_id:'bob'}]})},requireAuth:async()=>({id:'bob',sessionId:'session-bob'}),sendToUser:(user,data)=>events.push({user,...data}),sendToSession:(user,session,data)=>events.push({user,session,...data}),sendToUserExceptSession:(user,except,data)=>events.push({user,except,...data}),push:async()=>{},history:{save:async call=>history.push(call)}});
 await relay({type:'call.offer',room_id:'r',offer:{sdp:'sdp'}},{id:'alice',sessionId:'session-alice'});await relay.revokeUser('alice');
 assert.equal(history[0].reason,'session_revoked');assert.equal(events.filter(e=>e.type==='call.hangup').length,2);await app.close();
});
test('immediate busy attempts are retained in history and invalid UTF8 nonce never crashes',async()=>{
 const app=Fastify(),events=[],history=[];
 const relay=installCalls(app,{pool:{query:async()=>({rows:[{kind:'dm',user_id:'alice'},{kind:'dm',user_id:'bob'}]})},requireAuth:async()=>({id:'bob',sessionId:'session-bob'}),sendToUser:(user,data)=>events.push({user,...data}),sendToSession:(user,session,data)=>events.push({user,session,...data}),sendToUserExceptSession:(user,except,data)=>events.push({user,except,...data}),push:async()=>{},history:{start:async()=>{},save:async call=>history.push(call)}});
 await relay({type:'call.offer',room_id:'r',offer:{sdp:'sdp'}},{id:'alice',sessionId:'session-alice'});
 await relay({type:'call.offer',room_id:'r',offer:{sdp:'another'}},{id:'alice',sessionId:'session-alice'});assert.equal(history[0]?.status,'busy');
 const offer=events.find(e=>e.type==='call.offer');assert.equal((await app.inject({method:'POST',url:'/calls/'+offer.call_id+'/decline',payload:{nonce:'я'.repeat(36)}})).statusCode,404);await app.close();
});

test('winning authenticated session owns signaling; sibling devices cannot kill or inject into call',async()=>{
 const app=Fastify(),events=[],pushes=[];
 const relay=installCalls(app,{pool:{query:async()=>({rows:[{kind:'dm',user_id:'alice'},{kind:'dm',user_id:'bob'}]})},requireAuth:async()=>({id:'bob',sessionId:'session-bob'}),sendToUser:(user,data)=>events.push({user,scope:'all',...data}),sendToSession:(user,session,data)=>events.push({user,session,scope:'session',...data}),sendToUserExceptSession:(user,except,data)=>events.push({user,except,scope:'siblings',...data}),push:async(user,a,b,data)=>pushes.push(data)});
 const a={id:'alice',sessionId:'alice-one'},a2={id:'alice',sessionId:'alice-two'},b={id:'bob',sessionId:'bob-one'},b2={id:'bob',sessionId:'bob-two'};
 await relay({type:'call.offer',room_id:'r',offer:{sdp:'offer'}},a);const id=events.find(e=>e.type==='call.offer').call_id;
 await relay({type:'call.answer',room_id:'r',call_id:id,answer:{sdp:'answer'}},b);
 const accepted=events.find(e=>e.type==='call.accepted');assert.equal(accepted.scope,'siblings');assert.equal(accepted.except,'bob-one');
 assert.equal(events.find(e=>e.type==='call.answer').session,'alice-one');
 const before=events.length;
 for(const actor of [a2,b2])for(const type of ['call.hangup','call.ice','call.renegotiate','call.renegotiate.answer'])await relay({type,room_id:'r',call_id:id,candidate:{candidate:'injected'},offer:{sdp:'injected'}},actor);
 assert.equal(events.filter(e=>e.type==='call.hangup').length,0);assert.equal(events.slice(before).filter(e=>['call.ice','call.renegotiate','call.renegotiate.answer'].includes(e.type)).length,0);
 const nonce=pushes.find(e=>e.call_event==='ring').decline_nonce;assert.equal((await app.inject({method:'POST',url:'/calls/'+id+'/decline',payload:{nonce}})).statusCode,404);
 await relay({type:'call.answer',room_id:'r',call_id:id,answer:{sdp:'loser'}},b2);assert.equal(events.filter(e=>e.type==='call.answer').length,1);
 await relay({type:'call.hangup',room_id:'r',call_id:id},b);assert.equal(events.filter(e=>e.type==='call.hangup').length,2);await app.close();
});
test('same-cookie tabs have separate endpoints and only secret resume capability transfers ownership',async()=>{
 const app=Fastify(),events=[];const endpoint=id=>({id,send:event=>events.push({endpoint:id,...event})});
 const relay=installCalls(app,{pool:{query:async()=>({rows:[{kind:'dm',user_id:'alice'},{kind:'dm',user_id:'bob'}]})},requireAuth:async()=>({id:'bob'}),sendToUser:(user,event)=>events.push({user,...event}),sendToSession:(user,session,event)=>events.push({user,session,...event}),sendToUserExceptSession:()=>{},sendToUserExceptEndpoint:(user,except,event)=>events.push({user,except,...event}),push:async()=>{}});
 const a={id:'alice',sessionId:'a-cookie'},b={id:'bob',sessionId:'b-cookie'};
 await relay({type:'call.offer',room_id:'r',offer:{sdp:'offer'}},a,endpoint('a-tab1'));const created=events.find(e=>e.type==='call.created');assert.equal(created.endpoint,'a-tab1');assert.ok(created.resume_token);
 await relay({type:'call.answer',room_id:'r',answer:{sdp:'answer'}},b,endpoint('b-tab1'));assert.equal(events.find(e=>e.type==='call.accepted').except,'b-tab1');
 await relay({type:'call.hangup',room_id:'r'},a,endpoint('a-tab2'));assert.equal(events.filter(e=>e.type==='call.hangup').length,0);
 await relay({type:'call.resume',room_id:'r',call_id:created.call_id,resume_token:'wrong'},a,endpoint('a-reconnected'));assert.equal(events.filter(e=>e.type==='call.resumed').length,0);
 await relay({type:'call.resume',room_id:'r',call_id:created.call_id,resume_token:'я'.repeat(36)},a,endpoint('a-reconnected'));assert.equal(events.filter(e=>e.type==='call.resumed').length,0);
 await relay({type:'call.resume',room_id:'r',call_id:created.call_id,resume_token:created.resume_token},a,endpoint('a-reconnected'));assert.equal(events.find(e=>e.type==='call.resumed').endpoint,'a-reconnected');
 await relay({type:'call.hangup',room_id:'r'},a,endpoint('a-tab1'));assert.equal(events.filter(e=>e.type==='call.hangup').length,0);
 await relay({type:'call.hangup',room_id:'r'},a,endpoint('a-reconnected'));assert.equal(events.filter(e=>e.type==='call.hangup').length,2);await app.close();
});
test('revoking a sibling session leaves winner active but revoking owning session ends call',async()=>{
 const app=Fastify(),events=[];const relay=installCalls(app,{pool:{query:async()=>({rows:[{kind:'dm',user_id:'alice'},{kind:'dm',user_id:'bob'}]})},requireAuth:async()=>({id:'bob'}),sendToUser:(user,event)=>events.push({user,...event}),sendToSession:(user,session,event)=>events.push({user,session,...event}),sendToUserExceptSession:()=>{},push:async()=>{}});
 const a={id:'alice',sessionId:'alice-one'},b={id:'bob',sessionId:'bob-one'};
 await relay({type:'call.offer',room_id:'r',offer:{sdp:'offer'}},a);await relay({type:'call.answer',room_id:'r',answer:{sdp:'answer'}},b);
 await relay.revokeSession('bob','bob-two');assert.equal(events.filter(e=>e.type==='call.hangup').length,0);
 await relay.revokeSession('bob','bob-one');assert.equal(events.filter(e=>e.type==='call.hangup').length,2);await app.close();
});
test('only owner socket close starts bounded reconnect grace and successful resume cancels it',async()=>{
 let time=1000;const app=Fastify(),events=[];const endpoint=id=>({id,send:event=>events.push({endpoint:id,...event})});
 const relay=installCalls(app,{pool:{query:async()=>({rows:[{kind:'dm',user_id:'alice'},{kind:'dm',user_id:'bob'}]})},requireAuth:async()=>({id:'bob'}),sendToUser:(user,event)=>events.push({user,...event}),sendToSession:()=>{},sendToUserExceptSession:()=>{},sendToUserExceptEndpoint:()=>{},push:async()=>{},now:()=>time});
 const a={id:'alice',sessionId:'a'},b={id:'bob',sessionId:'b'};await relay({type:'call.offer',room_id:'r',offer:{sdp:'offer'}},a,endpoint('a1'));const created=events.find(e=>e.type==='call.created');await relay({type:'call.answer',room_id:'r',answer:{sdp:'answer'}},b,endpoint('b1'));
 await relay.endpointClosed('a-sibling');time+=31000;await relay({type:'call.ice',room_id:'r',candidate:{candidate:'ok'}},a,endpoint('a1'));assert.equal(events.filter(e=>e.type==='call.hangup').length,0);
 await relay.endpointClosed('a1');time+=1000;await relay({type:'call.resume',room_id:'r',call_id:created.call_id,resume_token:created.resume_token},a,endpoint('a2'));time+=31000;await relay({type:'call.ice',room_id:'r',candidate:{candidate:'ok'}},a,endpoint('a2'));assert.equal(events.filter(e=>e.type==='call.hangup').length,0);
 await relay.endpointClosed('a2');time+=31000;await relay({type:'call.ice',room_id:'r',candidate:{candidate:'late'}},b,endpoint('b1'));assert.equal(events.filter(e=>e.type==='call.hangup'&&e.reason==='network_error').length,2);await app.close();
});
