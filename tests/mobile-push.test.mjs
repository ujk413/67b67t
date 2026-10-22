import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),ts=require('../mobile/node_modules/typescript');
function load(file,deps){
  const exports={};
  vm.runInNewContext(ts.transpileModule(fs.readFileSync(new URL(file,import.meta.url),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022,esModuleInterop:true}}).outputText,{exports,require:name=>{assert.ok(name in deps,`Unexpected import ${name}`);return deps[name];},setTimeout,clearTimeout,setInterval:()=>1,clearInterval(){},AbortController});
  return exports;
}
const tick=async()=>{for(let i=0;i<6;i++)await new Promise(resolve=>setImmediate(resolve));};
function harness(){
  let permission={status:'granted',canAskAgain:true},tokenFetch=async()=>({data:'fcm-token'}),fail=false,room='room-a',handler,response,refreshToken,change,network;
  const requests=[],opens=[],removed=[];
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
    'expo-notifications':notification,'expo-constants':{expoConfig:{extra:{fcmConfigured:true}}},
    'react-native':{Platform:{OS:'android'},AppState:appState},
    'expo-network':{addNetworkStateListener:fn=>{network=fn;return{remove:()=>removed.push('network')};}},
    './storage':{storage:{get:async()=> 'installation-123',set:async()=>{}}},'./data/repository':{newId:()=> 'installation-123'},
    './requestTimeout':{requestWithTimeout:fn=>fn()},
    './mobileApi':{mobileApi:(_base,auth)=>({
      registerPush:async(id,token,active)=>{requests.push({op:'register',auth,id,token,active});if(fail)throw new Error('offline');},
      unregisterPush:async id=>requests.push({op:'unregister',id,auth}),testPush:async()=>({sent:1}),
    })},
  }).pushNotifications;
  push.onMessage(data=>opens.push(data));
  return {push,requests,opens,removed,connect:()=>push.connect('https://server/api','session',()=>room),
    setPermission:value=>{permission=value;},setTokenFetch:fn=>{tokenFetch=fn;},setFailure:value=>{fail=value;},
    room:value=>{room=value;},background:async()=>{appState.currentState='background';change('background');await tick();},
    foreground:async()=>{appState.currentState='active';change('active');await tick();},
    behavior:()=>handler.handleNotification({request:{content:{data:{room_id:room}}}}),
    response:()=>response({notification:{request:{identifier:'notification-1',content:{data:{room_id:'room-b'}}}}}),
    refreshToken:value=>refreshToken({data:value}),network:()=>network({isConnected:true}),
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
  assert.deepEqual(h.removed.sort(),['app','network','responses','tokens']);
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
