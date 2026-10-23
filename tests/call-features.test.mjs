import {test} from 'node:test';
import {t as uiText,locale as uiLocale} from '../web/i18n.js';
import {stopVoicePlayers} from '../web/voice-player.js';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFile} from 'node:fs/promises';
import {installCallFeatures} from '../api/call-features.js';
import {installCalls} from '../api/calls.js';
const require=createRequire(import.meta.url),Fastify=require('../api/node_modules/fastify'),{PGlite}=require('../api/node_modules/@electric-sql/pglite');
async function setup(sfu,withRelay=false){
 const db=new PGlite();await db.exec("CREATE TABLE users(id uuid PRIMARY KEY);CREATE TABLE sessions(id uuid PRIMARY KEY,user_id uuid);CREATE TABLE rooms(id uuid PRIMARY KEY,kind text);CREATE TABLE room_members(room_id uuid,user_id uuid);CREATE TABLE messages(id uuid PRIMARY KEY,room_id uuid REFERENCES rooms(id),sender_id uuid REFERENCES users(id),type text,text text,meta jsonb,created_at timestamptz);");
 await db.exec(await readFile(new URL('../api/call-features.sql',import.meta.url),'utf8'));
 const alice='00000000-0000-0000-0000-000000000001',bob='00000000-0000-0000-0000-000000000002',room='00000000-0000-0000-0000-000000000003';
 const defaultSession='00000000-0000-0000-0000-000000000070';
 await db.query('INSERT INTO users VALUES ($1),($2)',[alice,bob]);await db.query('INSERT INTO sessions VALUES($1,$2)',[defaultSession,alice]);await db.query("INSERT INTO rooms VALUES ($1,'group')",[room]);await db.query('INSERT INTO room_members VALUES ($1,$2)',[room,alice]);
 const app=Fastify(),events=[];const features=installCallFeatures(app,{pool:db,requireAuth:async req=>({id:req.headers['x-user'],sessionId:req.headers['x-session']||defaultSession}),sendToUser:(id,event)=>events.push({id,...event}),sfu});
 const relay=withRelay?installCalls(app,{pool:db,requireAuth:async()=>({id:alice}),sendToUser:(user,event)=>events.push({user,...event}),sendToSession:(user,session,event)=>events.push({user,session,...event}),sendToUserExceptSession:(user,except,event)=>events.push({user,except,...event}),push:async()=>{},history:features.history}):null;
 if(relay)features.setRelay(relay);await app.ready();
 return {app,db,alice,bob,room,events,features,relay,close:async()=>{await app.close();await db.close();}};
}
test('persisted call history is participant private, cursor stable, chat event emitted once',async()=>{
 const x=await setup();try{const call={id:'00000000-0000-0000-0000-000000000004',room_id:x.room,from:x.alice,to:x.bob,video:false,started_at:1000};await x.features.history.start(call);await x.features.history.save({...call,ended_at:46000,duration_ms:0,status:'missed',reason:'timeout'});await x.features.history.save({...call,ended_at:46000,duration_ms:0,status:'missed',reason:'timeout'});
 const result=await x.app.inject({url:'/calls/history',headers:{'x-user':x.bob}});assert.equal(result.json().items[0].status,'missed');assert.equal(x.events.filter(e=>e.type==='room.call').length,2);
 const messages=(await x.db.query('SELECT * FROM messages')).rows;assert.equal(messages.length,1);assert.equal(messages[0].type,'call');assert.equal(messages[0].meta.status,'missed');assert.equal(messages[0].meta.offer,undefined);assert.equal(messages[0].meta.sdp,undefined);
 const outsider=await x.app.inject({url:'/calls/history',headers:{'x-user':'00000000-0000-0000-0000-000000000099'}});assert.deepEqual(outsider.json().items,[]);
 }finally{await x.close();}
});
test('SFU fails closed, checks membership, and limits room to six before signing scoped token',async()=>{
 let x=await setup();try{assert.equal((await x.app.inject({method:'POST',url:'/rooms/'+x.room+'/calls/join',headers:{'x-user':x.alice}})).statusCode,503);}finally{await x.close();}
 const issued=[];x=await setup({url:'wss://calls.self.test',createRoom:async options=>{assert.equal(options.maxParticipants,6);},token:async options=>{issued.push(options);return 'signed';}});
 try{assert.equal((await x.app.inject({method:'POST',url:'/rooms/'+x.room+'/calls/join',headers:{'x-user':x.bob}})).statusCode,403);
 const joined=await x.app.inject({method:'POST',url:'/rooms/'+x.room+'/calls/join',headers:{'x-user':x.alice}});assert.equal(joined.json().token,'signed');assert.equal(issued[0].identity,x.alice);assert.equal(issued[0].room,'67chat-'+x.room);assert.equal(issued[0].ttl,90);
 }finally{await x.close();}
});

// Execute production TypeScript with only the native WebRTC boundary replaced.
const ts=require('../mobile/node_modules/typescript');
async function nativeCall({protectedMode=false}={}){
 const source=await readFile(new URL('../mobile/src/webrtc.ts',import.meta.url),'utf8'),exports={},pcs=[],stopped=[],captures=[];
 const camera={kind:'video',id:'camera',enabled:true,stop(){stopped.push(this.id);}},screen={kind:'video',id:'screen',enabled:true,stop(){stopped.push(this.id);}},audio={kind:'audio',id:'mic',enabled:true,stop(){stopped.push(this.id);}};
 const stream=tracks=>({getTracks:()=>tracks,getVideoTracks:()=>tracks.filter(t=>t.kind==='video'),getAudioTracks:()=>tracks.filter(t=>t.kind==='audio'),removeTrack:t=>{tracks.splice(tracks.indexOf(t),1);},addTrack:t=>tracks.push(t)});
 class PC{constructor(){pcs.push(this);this.connectionState='connected';this.signalingState='stable';this.senders=[];}addTrack(track){this.senders.push({track,replaceTrack:async function(t){this.track=t;}});}getSenders(){return this.senders;}async createOffer(options){return {type:'offer',sdp:options?.iceRestart?'restart':'initial'};}async setLocalDescription(d){this.localDescription=d;}close(){this.connectionState='closed';}}
 const fake={RTCPeerConnection:PC,mediaDevices:{getUserMedia:async options=>{captures.push(options);return stream([audio,camera]);},getDisplayMedia:async()=>stream([screen])}};
 new Function('exports','require',ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText)(exports,name=>name==='./accountRegistry'?{assertCallsAllowed:async()=>{if(protectedMode)throw Error('Calls disabled for protected transport');}}:fake);
 const offers=[],call=new exports.WebRTCCall({audio:true,video:true},{onRenegotiation:offer=>offers.push(offer)},true,[{urls:'turn:self.test'}]);
 return {call,offers,pcs,stopped,captures,screen};
}
test('protected transport denies native capture and App call initialization before creating any peer',async()=>{const x=await nativeCall({protectedMode:true});await assert.rejects(x.call.start(),/protected transport/);assert.equal(x.captures.length,0);const app=nativeAppScope();app.assertCallsAllowed=async()=>{throw Error('Calls disabled for protected transport');};await app.startCall(true);assert.equal(app.calls.length,0);assert.equal(app.sent.length,0);assert.match(app.errors[0],/защищённой сети|protected transport/);});

test('native network restart signals SDP and screen share restores camera then releases capture',async()=>{
 const x=await nativeCall();await x.call.start();assert.equal(x.captures[0].audio.echoCancellation,true);assert.equal(x.captures[0].audio.noiseSuppression,true);
 await x.call.restartIce();assert.equal(x.offers[0].sdp,'restart');
 await x.call.startScreenShare();assert.equal(x.pcs[0].getSenders().find(s=>s.track.kind==='video').track.id,'screen');
 await x.call.stopScreenShare();assert.equal(x.pcs[0].getSenders().find(s=>s.track.kind==='video').track.id,'camera');assert.ok(x.stopped.includes('screen'));
 await x.call.end();assert.ok(x.stopped.includes('mic'));assert.equal(x.pcs[0].connectionState,'closed');
});
const webSource=await readFile(new URL('../web/call-features.js',import.meta.url),'utf8');
const {BrowserGroupCall}=await import('../web/call-features.js');
test('browser SFU joins only with server grant, publishes real tracks, and disconnects on leave',async()=>{
 const operations=[],listeners={},requests=[];class Room{constructor(){this.remoteParticipants=new Map();this.localParticipant={setMicrophoneEnabled:async value=>operations.push(['mic',value]),setScreenShareEnabled:async value=>operations.push(['screen',value])};}on(type,fn){listeners[type]=fn;return this;}async connect(url,token){operations.push(['connect',url,token]);}async disconnect(){operations.push(['disconnect']);}}
 const SDK={Room,RoomEvent:{Disconnected:'disconnected',Reconnecting:'reconnecting',Reconnected:'reconnected',TrackSubscribed:'track',TrackUnsubscribed:'untrack'}};
 const call=new BrowserGroupCall({request:async(path,options)=>{requests.push({path,method:options.method});return path.endsWith('/join')?{url:'wss://self.test',token:'authorized'}:{ok:true};},loadSDK:async()=>SDK});
 await call.join('group');assert.deepEqual(operations[0],['connect','wss://self.test','authorized']);assert.deepEqual(operations[1],['mic',true]);
 await call.setScreenShare(true);assert.deepEqual(operations.at(-1),['screen',true]);await call.close();assert.deepEqual(operations.at(-1),['disconnect']);assert.equal(call.state,'disconnected');
 assert.deepEqual(requests,[{path:'/rooms/group/calls/join',method:'POST'},{path:'/rooms/group/calls/leave',method:'POST'}]);
});
test('browser SFU never loads SDK or captures media when join API fails',async()=>{
 let loaded=false;const call=new BrowserGroupCall({request:async()=>{throw Error('group_calls_unavailable');},loadSDK:async()=>{loaded=true;}});
 await assert.rejects(()=>call.join('group'),/group_calls_unavailable/);assert.equal(loaded,false);assert.equal(call.state,'disconnected');
});
test('history rejects malformed cursor and restart recovery persists interrupted outcomes',async()=>{
 const x=await setup();try{
 assert.equal((await x.app.inject({url:'/calls/history?before_id=invalid',headers:{'x-user':x.alice}})).statusCode,400);
 const call={id:'00000000-0000-0000-0000-000000000004',room_id:x.room,from:x.alice,to:x.bob,video:false,started_at:1000,answered_at:2000};await x.features.history.start(call);await x.features.history.answer(call);await x.features.history.recover();
 const page=await x.app.inject({url:'/calls/history',headers:{'x-user':x.alice}});assert.equal(page.json().items[0].status,'interrupted');assert.equal(page.json().items[0].reason,'server_restart');
 }finally{await x.close();}
});
test('LiveKit token only grants participant room and expires in ninety seconds',async()=>{
 const {createLiveKitService}=await import('../api/call-features.js'),{createHmac}=await import('node:crypto');
 assert.equal(createLiveKitService({enabled:false,url:'wss://self.test',apiUrl:'http://self:7880',key:'test-key',secret:'test-secret'}),null);
 assert.equal(createLiveKitService({enabled:true,url:'wss://self.test',apiUrl:'http://self:7880',key:'test-key',secret:'test-secret'}),null,'raw signaling without a revocation gateway must stay unavailable');
 const lease='00000000-0000-4000-8000-000000000003',closed=[];
 const sfu=createLiveKitService({enabled:true,url:'wss://self.test/api/livekit',apiUrl:'http://self:7880',key:'test-key',secret:'test-secret',gateway:{enabled:true,closeLease:(...args)=>closed.push(args)}});const token=await sfu.token({identity:'bob',room:'67chat-room',leaseId:lease}),[head,body,signature]=token.split('.');const claims=JSON.parse(Buffer.from(body,'base64url'));
 assert.equal(claims.video.room,'67chat-room');assert.equal(claims.video.roomJoin,true);assert.equal(claims.video.roomAdmin,undefined);assert.equal(claims.video.canPublishData,false);assert.equal(claims.sub,'bob:'+lease);assert.ok(claims.exp-Math.floor(Date.now()/1000)<=90);assert.equal(signature,createHmac('sha256','test-secret').update(head+'.'+body).digest('base64url'));
});
test('SFU identity lease blocks a different room, is visible to P2P, and releases on leave',async()=>{
 const x=await setup({url:'wss://self.test',createRoom:async()=>{},token:async()=> 'signed',removeParticipant:async()=>{}},true);
 try{
 const relay=x.relay;
 let response=await x.app.inject({method:'POST',url:'/rooms/'+x.room+'/calls/join',headers:{'x-user':x.alice}});assert.equal(response.statusCode,200);assert.equal(await relay.isBusy(x.alice),true);const lease_id=response.json().lease_id;
 const other='00000000-0000-0000-0000-000000000005';await x.db.query("INSERT INTO rooms VALUES($1,'group')",[other]);await x.db.query('INSERT INTO room_members VALUES($1,$2)',[other,x.alice]);
 assert.equal((await x.app.inject({method:'POST',url:'/rooms/'+other+'/calls/join',headers:{'x-user':x.alice}})).statusCode,409);
 assert.equal((await x.app.inject({method:'POST',url:'/rooms/'+x.room+'/calls/join',headers:{'x-user':x.alice}})).statusCode,409);
 response=await x.app.inject({method:'POST',url:'/rooms/'+x.room+'/calls/heartbeat',headers:{'x-user':x.alice},payload:{lease_id}});assert.equal(response.statusCode,200);
 response=await x.app.inject({method:'POST',url:'/rooms/'+x.room+'/calls/leave',headers:{'x-user':x.alice},payload:{lease_id}});assert.equal(response.statusCode,200);assert.equal(await relay.isBusy(x.alice),false);
 }finally{await x.close();}
});
test('closing browser call during grant request releases delayed identity lease',async()=>{
 let grant;const requests=[];const call=new BrowserGroupCall({request:async path=>{requests.push(path);if(path.endsWith('/join'))return new Promise(resolve=>{grant=resolve;});return {ok:true};},loadSDK:async()=>{throw Error('must not capture after close');}});
 const joining=call.join('group');await new Promise(resolve=>setImmediate(resolve));await call.close();grant({url:'wss://self.test',token:'late'});await joining;
 assert.ok(requests.includes('/rooms/group/calls/leave'));assert.equal(call.roomId,null);
});
test('stale device leave and heartbeat cannot terminate a newer SFU lease',async()=>{
 const x=await setup({url:'wss://self.test',createRoom:async()=>{},token:async()=> 'signed',removeParticipant:async()=>{}},true);try{
 const path='/rooms/'+x.room+'/calls/',headers={'x-user':x.alice};const first=(await x.app.inject({method:'POST',url:path+'join',headers})).json();await x.db.query("UPDATE group_call_leases SET expires_at=now()-interval '1 second'");const second=(await x.app.inject({method:'POST',url:path+'join',headers})).json();assert.ok(second.lease_id);assert.notEqual(first.lease_id,second.lease_id);
 assert.equal((await x.app.inject({method:'POST',url:path+'heartbeat',headers,payload:{lease_id:first.lease_id}})).statusCode,409);
 await x.app.inject({method:'POST',url:path+'leave',headers,payload:{lease_id:first.lease_id}});assert.equal(await x.relay.isBusy(x.alice),true);
 await x.app.inject({method:'POST',url:path+'leave',headers,payload:{lease_id:second.lease_id}});assert.equal(await x.relay.isBusy(x.alice),false);
 }finally{await x.close();}
});
test('obsolete browser join rejection cannot disconnect the newer joined room',async()=>{
 let rejectFirst,loads=0;const rooms=[],states=[];
 class Room{constructor(){this.localParticipant={setMicrophoneEnabled:async()=>{}};this.disconnects=0;rooms.push(this);}on(){return this;}async connect(){}async disconnect(){this.disconnects++;}}
 const SDK={Room,RoomEvent:{Disconnected:'disconnected',Reconnecting:'reconnecting',Reconnected:'reconnected',TrackSubscribed:'track',TrackUnsubscribed:'untrack'}};
 const call=new BrowserGroupCall({request:async path=>path.endsWith('/join')?{url:'wss://self.test',token:'authorized',lease_id:path.includes('/a/')?'lease-a':'lease-b'}:{ok:true},onState:state=>states.push(state),loadSDK:async()=>++loads===1?new Promise((resolve,reject)=>{rejectFirst=reject;}):SDK});
 const first=call.join('a');await new Promise(resolve=>setImmediate(resolve));await call.join('b');const active=call.room;assert.equal(call.state,'connected');rejectFirst(Error('old sdk failed'));await assert.rejects(first,/old sdk failed/);assert.equal(call.room,active);assert.equal(active.disconnects,0);assert.equal(call.state,'connected');await call.close();
});
test('stale disconnect callbacks and microphone completion never revive or close another browser call',async()=>{
 let finishMic;const rooms=[];class Room{constructor(){this.listeners={};this.localParticipant={setMicrophoneEnabled:async()=>rooms.length===1?new Promise(resolve=>{finishMic=resolve;}):undefined};rooms.push(this);}on(event,fn){this.listeners[event]=fn;return this;}async connect(){}async disconnect(){this.listeners.disconnected?.();}}
 const SDK={Room,RoomEvent:{Disconnected:'disconnected',Reconnecting:'reconnecting',Reconnected:'reconnected',TrackSubscribed:'track',TrackUnsubscribed:'untrack'}};const call=new BrowserGroupCall({request:async path=>path.endsWith('/join')?{url:'wss://self.test',token:'authorized',lease_id:path}:{ok:true},loadSDK:async()=>SDK});
 const first=call.join('a');await new Promise(resolve=>setImmediate(resolve));await call.join('b');const current=call.room;rooms[0].listeners.disconnected?.();assert.equal(call.room,current);finishMic();await first;assert.equal(call.room,current);assert.equal(call.state,'connected');await call.close();
});
test('membership revoked while SFU creation awaits never receives a join token',async()=>{
 let x,tokens=0;const sfu={url:'wss://self.test',createRoom:async()=>{await x.features.revokeMembership(x.room,x.alice);await x.db.query('DELETE FROM room_members WHERE room_id=$1 AND user_id=$2',[x.room,x.alice]);},token:async()=>{tokens++;return 'revoked-token';},removeParticipant:async()=>{}};x=await setup(sfu,true);
 try{const response=await x.app.inject({method:'POST',url:'/rooms/'+x.room+'/calls/join',headers:{'x-user':x.alice}});assert.equal(response.statusCode,403);assert.equal(tokens,0);}finally{await x.close();}
});
test('expired SFU lease removes old room participant before acquiring a new room',async()=>{
 const removed=[];const x=await setup({url:'wss://self.test',createRoom:async()=>{},token:async()=> 'signed',removeParticipant:async(room,user)=>removed.push({room,user})},true);
 try{const headers={'x-user':x.alice};await x.app.inject({method:'POST',url:'/rooms/'+x.room+'/calls/join',headers});await x.db.query("UPDATE group_call_leases SET expires_at=now()-interval '1 second'");const other='00000000-0000-0000-0000-000000000005';await x.db.query("INSERT INTO rooms VALUES($1,'group')",[other]);await x.db.query('INSERT INTO room_members VALUES($1,$2)',[other,x.alice]);
 const response=await x.app.inject({method:'POST',url:'/rooms/'+other+'/calls/join',headers});assert.equal(response.statusCode,200);assert.deepEqual(removed,[{room:'67chat-'+x.room,user:x.alice}]);assert.equal((await x.db.query('SELECT room_id FROM group_call_leases')).rows[0].room_id,other);
 }finally{await x.close();}
});
import vm from 'node:vm';
function appCallFunction(name){const file=ts.createSourceFile('App.tsx',require('node:fs').readFileSync(new URL('../mobile/App.tsx',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true,ts.ScriptKind.TSX),app=file.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='App');const fn=app.body.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text===name);return ts.transpileModule('('+fn.getText(file)+')',{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;}
function nativeAppScope(){const calls=[],sent=[],errors=[];class Call{constructor(){this.starts=0;this.ends=0;calls.push(this);}async start(){this.starts++;}async end(){this.ends++;}getLocalDescription(){return {sdp:'offer'};}async addIceCandidate(){}}
 const ref=value=>({current:value}),scope={uiText,uiLocale,assertCallsAllowed:async()=>{},console,Date,JSON,WebSocket:{OPEN:1},WebRTCCall:Call,activeRoomId:'room',rooms:[],token:'token',baseUrl:'https://self.test',connection:'connected',incomingCall:null,tokenRef:ref('token'),callGeneration:ref(0),callRoomRef:ref(null),callIdRef:ref(null),callResumeRef:ref(null),waitingCallRef:ref(null),callAudioQueue:ref(Promise.resolve()),activeCallRef:ref(null),pendingCallIce:ref([]),callStartedAt:ref(null),callTimeout:ref(null),wsRef:ref({readyState:1,send:raw=>sent.push(JSON.parse(raw))}),api:{iceServers:async()=>({iceServers:[]})},nativeCalls:{start:async()=>{},speaker:async()=>{},stop:async()=>{},clear:async()=>{}},beginAudioCall:async()=>{},endAudioCall:async()=>{},userError:e=>e.message,clearTimeout,setTimeout,clearInterval,setInterval};
 for(const key of ['setIsAudioEnabled','setLocalStream','setRemoteStream','setCallNetwork','setCallState','setSpeakerEnabled','setActiveCall','setIsVideoEnabled','setCallDuration','setSharingScreen','setIncomingCall'])scope[key]=()=>{};scope.pauseAllMedia=async()=>{};scope.setError=error=>errors.push(error);scope.armCallTimeout=()=>{};scope.clearCallTimeout=()=>{};
 const context=vm.createContext(scope);for(const name of ['enqueueCallAudio','makeCallEvents','endCall','handleCallSignal']){try{scope[name]=vm.runInContext(appCallFunction(name),context);}catch{}}
 scope.startCall=vm.runInContext(appCallFunction('startCall'),context);return Object.assign(scope,{calls,sent,errors});}
test('native App cancellation during audio preparation never starts stale media',async()=>{
 const s=nativeAppScope();let finish;s.beginAudioCall=()=>new Promise(resolve=>{finish=resolve;});const pending=s.startCall(true);await new Promise(resolve=>setImmediate(resolve));s.endCall(false);finish();await pending;assert.equal(s.calls[0].starts,0);assert.equal(s.activeCallRef.current,null);assert.equal(s.sent.length,0);
});
test('native App late audio failure cannot end the next call or emit its hangup',async()=>{
 const s=nativeAppScope();let rejectStart;s.nativeCalls.start=()=>new Promise((resolve,reject)=>{rejectStart=reject;});const old=s.startCall(false);await new Promise(resolve=>setImmediate(resolve));s.endCall(false);const next={ends:0,async end(){this.ends++;}};s.activeCallRef.current=next;s.callRoomRef.current='next-room';rejectStart(Error('old audio failed'));await old;assert.equal(s.activeCallRef.current,next);assert.equal(next.ends,0);assert.equal(s.errors.length,0);assert.equal(s.sent.length,0);
});
test('native accepted on another endpoint releases pending peer without sending hangup; stale call id ignored',async()=>{
 const s=nativeAppScope();await s.startCall(false);await s.handleCallSignal({type:'call.created',room_id:'room',call_id:'call-one',resume_token:'secret'});assert.equal(s.callResumeRef.current,'secret');const call=s.activeCallRef.current;
 await s.handleCallSignal({type:'call.accepted',room_id:'room',call_id:'old-call'});assert.equal(s.activeCallRef.current,call);
 await s.handleCallSignal({type:'call.accepted',room_id:'room',call_id:'call-one'});assert.equal(s.activeCallRef.current,null);assert.equal(s.sent.filter(e=>e.type==='call.hangup').length,0);await s.callAudioQueue.current;
});
function browserAppScope(){
 const file=ts.createSourceFile('app.js',require('node:fs').readFileSync(new URL('../web/app.js',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true,ts.ScriptKind.JS),sent=[],stopped=[],pcs=[],mediaEvents=[];
 const state={me:{id:'me'},rooms:[],activeRoomId:'room',callState:'idle',callGeneration:0,currentCallId:null,currentCallRoomId:null,callResumeToken:null,callWaitingId:null,callRecovery:null,pendingIceCandidates:[],roomMembers:[],wsAuthed:true,ws:{send:raw=>sent.push(JSON.parse(raw))}};
 const scope={uiText,uiLocale,state,console,stopVoicePlayers,webMedia:{stop(){mediaEvents.push("media.stop");}},Date,JSON,Promise,clearTimeout,setTimeout,clearInterval,setInterval,toast:()=>{},userError:e=>e.message,alert:()=>{},document:{querySelectorAll:()=>[{pause(){mediaEvents.push("media.pause");}}],getElementById:()=>null,createElement:()=>({remove(){},append(){},addEventListener(){}}),body:{appendChild(){}}},$:()=>null,window:{},navigator:{mediaDevices:{getUserMedia:async()=>({getTracks:()=>[{stop(){stopped.push('track');}}]})}},loadRtcIceServers:async()=>{},getRtcIceServers:()=>[],getAudioConstraints:()=>({audio:true}),showVideoCallUI:()=>{},showAudioCallUI:()=>{},updateCallStatus:()=>{},wirePeerConnectionHandlers:()=>{},clearCallDisconnectTimer:()=>{},avatarUrl:()=>'',displayName:()=>'',escapeHtml:value=>value,RTCSessionDescription:function(value){return value;},RTCIceCandidate:function(value){return value;},RTCPeerConnection:class{constructor(){this.closed=0;pcs.push(this);}addTrack(){}close(){this.closed++;}async createOffer(){return {type:'offer',sdp:'offer'};}async setLocalDescription(){}}};
 const context=vm.createContext(scope),load=name=>{const fn=file.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text===name);const assignment=file.statements.find(n=>ts.isExpressionStatement(n)&&ts.isBinaryExpression(n.expression)&&n.expression.left.getText(file)==='window.'+name);return vm.runInContext(ts.transpileModule('('+(fn?fn.getText(file):assignment.expression.right.getText(file))+')',{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText,context);};
 scope.pauseMessageMedia=load('pauseMessageMedia');scope.hangupCall=load('hangupCall');scope.window.hangupCall=scope.hangupCall;scope.handleWebCallSignal=load('handleWebCallSignal');scope.handleCall=load('handleCall');scope.startRoomCall=load('startRoomCall');scope.flushPendingIceCandidates=load('flushPendingIceCandidates');return Object.assign(scope,{sent,stopped,pcs,mediaEvents});
}
test('browser outgoing capture resolving after cancellation is stopped and never opens a peer',async()=>{
 const s=browserAppScope();let finish;s.navigator.mediaDevices.getUserMedia=()=>{assert.deepEqual(s.mediaEvents,['media.pause','media.stop']);return new Promise(resolve=>{finish=resolve;});};const pending=s.startRoomCall(true);await new Promise(resolve=>setImmediate(resolve));s.hangupCall(false);finish({getTracks:()=>[{stop(){s.stopped.push('late');}}]});await pending;assert.deepEqual(s.stopped,['late']);assert.equal(s.pcs.length,0);assert.equal(s.sent.length,0);
});
test('browser autoanswer capture resolving after another endpoint wins is stopped',async()=>{
 const s=browserAppScope();let finish;s.state.autoAnswerCallId='incoming';s.navigator.mediaDevices.getUserMedia=()=>{assert.deepEqual(s.mediaEvents,['media.pause','media.stop']);return new Promise(resolve=>{finish=resolve;});};const pending=s.handleCall({type:'call.offer',room_id:'room',call_id:'incoming',from_user_id:'peer',is_video:false,offer:{sdp:'offer'}});await new Promise(resolve=>setImmediate(resolve));await s.handleWebCallSignal({type:'call.accepted',room_id:'room',call_id:'incoming'});finish({getTracks:()=>[{stop(){s.stopped.push('late');}}]});await pending;assert.deepEqual(s.stopped,['late']);assert.equal(s.pcs.length,0);assert.equal(s.sent.length,0);
});
test('browser accepted removes only matching local call and never sends winner a hangup',async()=>{
 const s=browserAppScope();s.state.callState='calling';s.state.currentCallRoomId='room';s.state.currentCallId='current';s.state.pc={close(){s.stopped.push('peer');}};s.state.localStream={getTracks:()=>[{stop(){s.stopped.push('track');}}]};
 await s.handleWebCallSignal({type:'call.accepted',room_id:'room',call_id:'old'});assert.equal(s.stopped.length,0);
 await s.handleWebCallSignal({type:'call.accepted',room_id:'room',call_id:'current'});assert.deepEqual(s.stopped,['peer','track']);assert.equal(s.state.callState,'idle');assert.equal(s.sent.length,0);
});
test('SFU leases bind owning authenticated session and sibling logout leaves its media active',async()=>{
 const removed=[],x=await setup({url:'wss://self.test',createRoom:async()=>{},token:async()=> 'signed',removeParticipant:async(room,user)=>removed.push(user)},true);
 try{const one='00000000-0000-0000-0000-000000000051',two='00000000-0000-0000-0000-000000000052';await x.db.query('INSERT INTO sessions VALUES($1,$3),($2,$3)',[one,two,x.alice]);
 const response=await x.app.inject({method:'POST',url:'/rooms/'+x.room+'/calls/join',headers:{'x-user':x.alice,'x-session':one}});assert.equal(response.statusCode,200);assert.equal((await x.db.query('SELECT session_id FROM group_call_leases')).rows[0].session_id,one);
 await x.relay.revokeSession(x.alice,two);assert.equal(await x.relay.isBusy(x.alice),true);assert.equal(removed.length,0);
 await x.relay.revokeSession(x.alice,one);assert.equal(await x.relay.isBusy(x.alice),false);assert.deepEqual(removed,[x.alice]);
 }finally{await x.close();}
});

test('SFU heartbeat and leave require the exact owning session and cannot revive a revoking lease',async()=>{
 const removed=[],x=await setup({url:'wss://self.test',createRoom:async()=>{},token:async()=> 'signed',removeParticipant:async(...args)=>removed.push(args)},true);
 try{
  const one='00000000-0000-0000-0000-000000000051',two='00000000-0000-0000-0000-000000000052';await x.db.query('INSERT INTO sessions VALUES($1,$3),($2,$3)',[one,two,x.alice]);
  const path='/rooms/'+x.room+'/calls/',headers={'x-user':x.alice,'x-session':one},sibling={'x-user':x.alice,'x-session':two};
  const joined=await x.app.inject({method:'POST',url:path+'join',headers});assert.equal(joined.statusCode,200);const payload={lease_id:joined.json().lease_id};
  assert.equal((await x.app.inject({method:'POST',url:path+'heartbeat',headers:sibling,payload})).statusCode,409);
  assert.equal((await x.app.inject({method:'POST',url:path+'leave',headers:sibling,payload})).statusCode,409);assert.equal(removed.length,0);assert.equal(await x.relay.isBusy(x.alice),true);
  assert.equal((await x.app.inject({method:'POST',url:path+'heartbeat',headers,payload})).statusCode,200);
  await x.db.query('UPDATE group_call_leases SET revoking_at=now() WHERE user_id=$1',[x.alice]);
  assert.equal((await x.app.inject({method:'POST',url:path+'heartbeat',headers,payload})).statusCode,409);
  assert.equal((await x.app.inject({method:'POST',url:path+'leave',headers,payload})).statusCode,409);assert.equal(removed.length,0);
 }finally{await x.close();}
});
test('a fresh native incoming offer pauses previous media once before displaying the call',async()=>{
 const s=nativeAppScope();let paused=0;s.pauseAllMedia=async()=>{paused++;};const offer={type:'call.offer',room_id:'room',call_id:'incoming',from_user_id:'peer',offer:{sdp:'incoming'}};
 await s.handleCallSignal(offer);assert.equal(paused,1);await s.handleCallSignal(offer);assert.equal(paused,1);
});
