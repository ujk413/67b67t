import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {createRequire} from 'node:module';
import * as uiI18n from '../web/i18n.js';
const require=createRequire(import.meta.url),ts=require('../mobile/node_modules/typescript');
function load(file,deps){
  const exports={};
  vm.runInNewContext(ts.transpileModule(fs.readFileSync(new URL(file,import.meta.url),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022,esModuleInterop:true}}).outputText,{exports,require:name=>{assert.ok(name in deps,`Unexpected import ${name}`);return deps[name];},setTimeout,clearTimeout,setInterval:()=>1,clearInterval(){},AbortController});
  return exports;
}
const tick=async()=>{for(let i=0;i<6;i++)await new Promise(resolve=>setImmediate(resolve));};
function harness(){
  let permission={status:'granted',canAskAgain:true},tokenFetch=async()=>({data:'fcm-token'}),fail=false,failUnregister=false,transport='direct',room='room-a',handler,response,refreshToken,change,network,nativeEvent,nativeReply=null,nativeOpen=null;
  const requests=[],opens=[],removed=[];let savedAccounts=[];const quiet=new Map();
  const appState={currentState:'active',addEventListener:(_event,fn)=>{change=fn;return{remove:()=>removed.push('app')};}};
  const notification={
    AndroidImportance:{HIGH:4},setNotificationChannelAsync:async(id,options)=>{assert.equal(id,'messages');assert.equal(options.sound,'default');},
    getPermissionsAsync:async()=>permission,requestPermissionsAsync:async()=>{permission={status:'granted'};return permission;},
    getDevicePushTokenAsync:()=>tokenFetch(),setNotificationHandler:fn=>{handler=fn;},
    addNotificationResponseReceivedListener:fn=>{response=fn;return{remove:()=>removed.push('responses')};},
    addPushTokenListener:fn=>{refreshToken=fn;return{remove:()=>removed.push('tokens')};},
    getLastNotificationResponseAsync:async()=>null,clearLastNotificationResponseAsync:async()=>{},setBadgeCountAsync:async()=>{},
  };
  const push=load('../mobile/src/notifications.ts',{
    '../../web/i18n.js':uiI18n,
    'expo':{requireOptionalNativeModule:()=>({consumeQuickReply:()=>{const item=nativeReply;nativeReply=null;return item;},consumeMessageOpen:()=>{const item=nativeOpen;nativeOpen=null;return item;},addListener:(name,fn)=>{if(name==='quickReply')nativeEvent=fn;return{remove:()=>removed.push(name==='quickReply'?'native':'native-open')};}})},
    'expo-notifications':notification,'expo-constants':{expoConfig:{extra:{fcmConfigured:true}}},
    'react-native':{Platform:{OS:'android'},AppState:appState},
    'expo-network':{addNetworkStateListener:fn=>{network=fn;return{remove:()=>removed.push('network')};}},
    './accountRegistry':{currentTransport:()=>({mode:transport}),installPushTransportHooks(){},pushInstallationCapability:async()=>({id:'installation-123',secret:'a'.repeat(64)}),accountRegistry:{list:async()=>savedAccounts,token:async id=>id+'-token'}},
    './storage':{storage:{get:async key=>quiet.get(key)||(key==='push.installation'?'installation-123':null),set:async()=>{}}},'./data/repository':{newId:()=> 'installation-123'},
    './requestTimeout':{requestWithTimeout:fn=>fn()},
    './mobileApi':{mobileApi:(_base,auth)=>({
      pushInstallation:async()=>({epoch:7,paused:false}),
      registerPush:async(id,token,active,policy)=>{requests.push({op:'register',auth,id,token,active,policy});if(fail)throw new Error('offline');},
      unregisterPush:async id=>{requests.push({op:'unregister',id,auth});if(failUnregister)throw Error('offline');},testPush:async()=>({sent:1}),
    })},
  }).pushNotifications;
  push.onMessage(data=>opens.push(data));
  return {push,requests,opens,removed,connect:()=>push.connect('https://server/api','session',()=>room,'alice'),
    setPermission:value=>{permission=value;},setTokenFetch:fn=>{tokenFetch=fn;},setFailure:value=>{fail=value;},setTransport:value=>{transport=value;},setUnregisterFailure:value=>{failUnregister=value;},
    accounts:value=>{savedAccounts=value;},room:value=>{room=value;},background:async()=>{appState.currentState='background';change('background');await tick();},
    foreground:async()=>{appState.currentState='active';change('active');await tick();},
    quiet:(user,value)=>quiet.set('quality.quiet:'+user,JSON.stringify(value)),behavior:(data={})=>handler.handleNotification({request:{content:{data:{room_id:room,...data}}}}),
    response:()=>response({notification:{request:{identifier:'notification-1',content:{data:{room_id:'room-b'}}}}}),
    refreshToken:value=>refreshToken({data:value}),network:()=>network({isConnected:true}),nativeReply:value=>{nativeReply=value;nativeEvent();},nativeOpen:value=>{nativeOpen=value;},
  };
}

test('native notifications request permission, follow the active room immediately, and allow background sound',async()=>{
  const h=harness();h.setPermission({status:'default',canAskAgain:true});await h.connect();await tick();
  assert.equal(h.push.status.status,'enabled');
  assert.equal(h.requests.at(-1).active,'room-a');
  assert.equal((await h.behavior()).shouldPlaySound,false);
  h.room('room-b');await h.push.refresh();assert.equal(h.requests.at(-1).active,'room-b');
  await h.background();assert.equal(h.requests.at(-1).active,null);
  assert.equal((await h.behavior()).shouldShowBanner,true);
});
test('Tor and proxy mode never acquire FCM tokens or register push',async()=>{
 const h=harness();h.setTransport('tor');let tokens=0;h.setTokenFetch(async()=>{tokens++;return{data:'forbidden'};});await h.connect();await h.push.initialize();await tick();assert.equal(tokens,0);assert.equal(h.requests.length,0);assert.match(h.push.status.message,/прокси|Tor/);
});
test('token rotation updates every stored account with fenced capability and only active room suppression',async()=>{
 const h=harness();h.accounts([{id:'inactive',userId:'other-user',baseUrl:'https://server/api'}]);await h.connect();await tick();h.refreshToken('rotated-all');await tick();
 const updated=h.requests.filter(row=>row.op==='register'&&row.token==='rotated-all');assert.deepEqual(updated.map(row=>[row.auth,row.active]),[['session','room-a'],['inactive-token',null]]);
 for(const row of updated){assert.equal(row.policy.installation_secret,'a'.repeat(64));assert.equal(row.policy.installation_epoch,7);assert.equal(row.policy.resume_push,true);}
});
test('account switching retains background registration but deactivates its foreground room',async()=>{
 const h=harness();await h.connect();await tick();await h.push.disconnect('https://server/api','session',false,true);
 assert.equal(h.requests.filter(row=>row.op==='unregister').length,0);assert.equal(h.requests.at(-1).active,null);
});
test('foreground notification suppression and quiet preferences follow the recipient account',async()=>{
 const h=harness();await h.connect();await tick();assert.equal((await h.behavior({recipient_id:'alice'})).shouldShowBanner,false);assert.equal((await h.behavior({recipient_id:'bob'})).shouldShowBanner,true);
 const now=new Date(),minute=now.getUTCHours()*60+now.getUTCMinutes(),clock=n=>`${String(Math.floor(n/60)).padStart(2,'0')}:${String(n%60).padStart(2,'0')}`;
 h.quiet('alice',{enabled:true,start:clock((minute+1439)%1440),end:clock((minute+1)%1440),time_zone:'UTC'});
 assert.equal((await h.behavior({recipient_id:'bob'})).shouldPlaySound,true);
 h.quiet('bob',{enabled:true,start:clock((minute+1439)%1440),end:clock((minute+1)%1440),time_zone:'UTC'});assert.equal((await h.behavior({recipient_id:'bob'})).shouldPlaySound,false);
});
test('an in-progress protected transition blocks new connections and retained registrations until explicit failure resume',async()=>{
 const h=harness();await h.connect();await tick();await h.push.pauseForTransport();h.requests.length=0;await h.connect();await tick();await h.push.disconnect('https://server/api','session',false,true);assert.equal(h.requests.length,0);
 await h.connect();await h.push.resumeAfterTransportFailure();await tick();assert.equal(h.requests.some(row=>row.op==='register'),true);
});
test('strict disconnect fails if server unregister failed before a transport switch',async()=>{
 const h=harness();await h.connect();await tick();h.setUnregisterFailure(true);await assert.rejects(h.push.disconnect('https://server/api','session',true),/offline/);
});
test('native RemoteInput text is consumed once and handed to the encrypted-send callback',async()=>{
  const h=harness();await h.connect();const accepted=[];h.push.onQuickReply(async item=>accepted.push(item));
  const reply={roomId:'00000000-0000-4000-8000-000000000005',recipientId:'00000000-0000-4000-8000-000000000001',text:'Привет'};
  h.nativeReply(reply);await tick();assert.equal(accepted.length,1);assert.equal(accepted[0].roomId,reply.roomId);assert.equal(accepted[0].recipientId,reply.recipientId);assert.equal(accepted[0].text,reply.text);
  await h.foreground();assert.equal(accepted.length,1);
});
test('native notification body tap opens its original account and room without reply text',async()=>{
  const h=harness(),item={roomId:'00000000-0000-4000-8000-000000000005',recipientId:'00000000-0000-4000-8000-000000000001'};
  h.nativeOpen(item);await h.connect();await tick();assert.equal(h.opens.length,1);assert.equal(h.opens[0].room_id,item.roomId);assert.equal(h.opens[0].recipient_id,item.recipientId);
});
test('a late failed reply cannot reappear after logout under another session',async()=>{
  const h=harness();await h.connect();let reject;
  h.push.onQuickReply(()=>new Promise((_resolve,rejection)=>{reject=rejection;}));
  h.nativeReply({roomId:'00000000-0000-4000-8000-000000000005',recipientId:'00000000-0000-4000-8000-000000000001',text:'private'});
  await h.push.disconnect('https://server/api','session');reject(Error('late'));await tick();const sent=[];h.push.onQuickReply(async item=>sent.push(item));await tick();assert.equal(sent.length,0);
});
test('denied permission unregisters the device and permission granted in settings reconnects',async()=>{
  const h=harness();h.setPermission({status:'denied',canAskAgain:false});await h.connect();await tick();
  assert.equal(h.push.status.status,'permission');assert.equal(h.requests.at(-1).op,'unregister');
  h.setPermission({status:'granted'});await h.foreground();assert.equal(h.push.status.status,'enabled');
});
test('a failed server registration is visible and network recovery retries it',async()=>{
  const h=harness();h.setFailure(true);await h.connect();await tick();assert.equal(h.push.status.status,'error');
  h.setFailure(false);h.network();await tick();assert.equal(h.push.status.status,'enabled');
  h.refreshToken('rotated-token');await tick();assert.equal(h.requests.at(-1).token,'rotated-token');
});
test('logout during token acquisition cancels stale registration and removes all listeners',async()=>{
  const h=harness();let finish;h.setTokenFetch(()=>new Promise(resolve=>{finish=resolve;}));
  await h.connect();await tick();await h.push.disconnect('https://server/api','session');
  finish({data:'late-token'});await tick();
  assert.equal(h.requests.filter(r=>r.op==='register').length,0);
  assert.deepEqual(h.removed.sort(),['app','native','native-open','network','responses','tokens']);
});
test('notification taps open a room once and detached listeners cannot reopen it',async()=>{
  const h=harness();const dispose=await h.connect();await tick();h.response();h.response();assert.equal(h.opens.length,1);
  dispose();h.response();assert.equal(h.opens.length,1);
});
test('the patched Expo SDK retries token acquisition after an offline first attempt',async()=>{
  let calls=0;
  const module=load('../mobile/node_modules/expo-notifications/src/getDevicePushTokenAsync.ts',{
    'expo-modules-core':{UnavailabilityError:Error,Platform:{OS:'android'}},
    './PushTokenManager':{getDevicePushTokenAsync:async()=>{calls++;if(calls===1)throw new Error('offline');return 'recovered-token';}},
    './warnOfExpoGoPushUsage':{warnOfExpoGoPushUsage(){}},
  });
  await assert.rejects(module.getDevicePushTokenAsync(),/offline/);
  assert.equal((await module.getDevicePushTokenAsync()).data,'recovered-token');assert.equal(calls,2);
});
