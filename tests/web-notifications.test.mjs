import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

function browser() {
  const requests = [], states = [], listeners = {}, storage = new Map();
  const subscription = { endpoint: 'browser-subscription', toJSON: () => ({endpoint:'browser-subscription'}), unsubscribe: async () => true };
  const permission = { permission: 'default', requestPermission: async () => { permission.permission='granted'; return 'granted'; } };
  const document = { hidden: false, hasFocus: () => true, addEventListener: (name, fn) => { listeners[name]=fn; } };
  const worker = { pushManager: { getSubscription: async () => subscription } };
  let fail = false;
  const context = vm.createContext({
    window: { Notification: permission, PushManager: {}, addEventListener:(name,fn)=>{listeners[name]=fn;} },
    Notification: permission, document, navigator:{ serviceWorker:{register:async()=>worker,ready:Promise.resolve(worker)} },
    localStorage:{getItem:key=>storage.get(key),setItem:(key,value)=>storage.set(key,value),removeItem:key=>storage.delete(key)},
    setInterval:()=>1,clearInterval(){},atob,Uint8Array,
  });
  vm.runInContext(fs.readFileSync(new URL('../web/notifications.js',import.meta.url),'utf8').replace('export function','function'),context);
  const controller=context.createWebNotifications({ activeRoom:()=> 'room',status:state=>states.push(state),api:async(path,options)=>{
    requests.push({path,options});
    if(path==='/web-push/config')return{enabled:true,public_key:'BA'};
    if(fail)throw new Error('offline');
    return {ok:true};
  }});
  return {controller,requests,states,permission,listeners,document,setFailure:value=>{fail=value;}};
}

test('Web Push asks permission only on request, marks hidden pages inactive and removes a disabled subscription',async()=>{
  const b=browser();
  await b.controller.start();
  assert.equal(b.permission.permission,'default');
  assert.equal(b.controller.subscribed(),false);
  await b.controller.enable();
  assert.equal(b.controller.subscribed(),true);
  assert.equal(JSON.parse(b.requests.at(-1).options.body).active_room_id,'room');
  b.document.hidden=true;
  b.listeners.visibilitychange();
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(JSON.parse(b.requests.at(-1).options.body).active_room_id,null);
  await b.controller.toggle();
  assert.equal(b.controller.subscribed(),false);
  assert.equal(b.requests.at(-1).options.method,'DELETE');
});

test('a failed server registration does not suppress ordinary notifications and can be retried',async()=>{
  const b=browser();b.setFailure(true);
  await assert.rejects(b.controller.enable(),/offline/);
  assert.equal(b.controller.subscribed(),false);
  assert.equal(b.states.at(-1).enabled,false);
  b.setFailure(false);
  await b.controller.toggle();
  assert.equal(b.controller.subscribed(),true);
  assert.equal(b.requests.at(-1).options.method,'PUT');
});

test('service worker displays the branded notification and focuses the correct conversation',async()=>{
  const handlers={},shown=[],opened=[],posts=[];
  let focused=false,closed=false,pending;
  const clients={matchAll:async()=>[{url:'https://67chat.ru/',focus:async()=>{focused=true;},postMessage:m=>posts.push(m)}],openWindow:async url=>opened.push(url)};
  const context=vm.createContext({self:{addEventListener:(name,fn)=>{handlers[name]=fn;},registration:{showNotification:async(title,options)=>shown.push({title,options})},location:{origin:'https://67chat.ru'}},clients,URL});
  vm.runInContext(fs.readFileSync(new URL('../web/sw.js',import.meta.url),'utf8'),context);
  handlers.push({data:{json:()=>({roomId:'room',messageId:'message',url:'/?room=room'})},waitUntil:p=>{pending=p;}});
  await pending;
  assert.equal(shown[0].title,'67chat');
  assert.equal(shown[0].options.body,'Новое сообщение');
  assert.equal(shown[0].options.icon,'/icons/app-192.png');
  handlers.notificationclick({notification:{data:shown[0].options.data,close:()=>{closed=true;}},waitUntil:p=>{pending=p;}});
  await pending;
  assert.ok(closed&&focused);
  assert.equal(posts[0].type,'OPEN_ROOM');assert.equal(posts[0].roomId,'room');
  assert.equal(opened.length,0);
});
