import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {webcrypto} from 'node:crypto';
import fs from 'node:fs';
import vm from 'node:vm';
import {createChatRuntime} from '../web/chat-runtime.js';
const require=createRequire(import.meta.url),fake=require('../api/node_modules/fake-indexeddb');
globalThis.indexedDB=fake.indexedDB;globalThis.IDBKeyRange=fake.IDBKeyRange;
Object.defineProperty(globalThis,'crypto',{value:webcrypto,configurable:true});
Object.defineProperty(globalThis,'navigator',{value:{onLine:true},configurable:true});
globalThis.window={addEventListener(){}};globalThis.document={getElementById:()=>null,addEventListener(){},hidden:false,hasFocus:()=>true};
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return{promise,resolve};};
const message=(room,id)=>({id,room_id:room,sender_id:'peer',text:id,type:'text',created_at:'2026-10-21T00:00:00Z'});
const page=messages=>({messages,has_older:false,has_newer:false});
async function harness(options={}) {
 const state={me:{id:crypto.randomUUID()},rooms:[{id:'A'},{id:'B'}],messages:[],activeRoomId:null},drafts=new Map();
 const request=options.request || (async path=>path.includes('/history')?page([]):[]);
 const runtime=createChatRuntime({state,request,decode:options.decode || (async(room,m)=>m),buildText:options.buildText || (async(room,r,text)=>({type:'text',text:'encrypted:'+text})),mount:async()=>{},refresh:async()=>{},render(){},report:error=>{throw error;},drafts,loadRooms:async()=>{}});
 await runtime.start(state.me);return{state,runtime,drafts};
}
test('a delayed history response cannot overwrite the newly opened room',async()=>{
 const a=deferred(),started=deferred();const h=await harness({request:async path=>{if(path.startsWith('/rooms/A/history')){started.resolve();return a.promise;}return path.includes('/history')?page([message('B','b')]):[];}});
 try {const first=h.runtime.open('A');await started.promise;await h.runtime.open('B');a.resolve(page([message('A','a')]));await first;assert.equal(h.state.activeRoomId,'B');assert.deepEqual(h.state.messages.map(m=>m.id),['b']);}finally{h.runtime.stop();}
});
test('a message decoded after switching rooms remains cached in its original room',async()=>{
 const decoded=deferred(),h=await harness({decode:async(room,m)=>m.id==='late'?decoded.promise:m});
 try {await h.runtime.open('A');const incoming=h.runtime.received(message('A','late'));await h.runtime.open('B');decoded.resolve(message('A','late'));await incoming;assert.equal(h.state.messages.length,0);assert.equal((await h.runtime.local().get('messages:A'))[0].id,'late');}finally{h.runtime.stop();}
});
test('slow encryption keeps the original recipient, draft and stable message id',async()=>{
 const encrypt=deferred(),started=deferred(),sent=[];const h=await harness({buildText:async()=>{started.resolve();return encrypt.promise;},request:async(path,options)=>{
   if(options?.method==='POST' && path.endsWith('/messages')){const body=JSON.parse(options.body);sent.push({path,body});return{...message('A','server'),client_message_id:body.client_message_id};}return path.includes('/history')?page([]):[];
 }});
 try {await h.runtime.open('A');const item=await h.runtime.enqueue({roomId:'A',text:'original'});await started.promise;await h.runtime.open('B');h.runtime.draft('B','keep this draft');encrypt.resolve({type:'text',text:'cipher-A'});
   for(let i=0;i<100 && (await h.runtime.local().entries('outbox:')).length;i++)await new Promise(r=>setTimeout(r,5));
   assert.equal(sent[0].path,'/rooms/A/messages');assert.equal(sent[0].body.client_message_id,item.id);assert.equal(h.drafts.get('B'),'keep this draft');assert.equal(h.state.messages.length,0);
 }finally{h.runtime.stop();}
});

test('a group decryption finishing after account switch writes plaintext only to its original account',async()=>{
 const requireMobile=createRequire(new URL('../mobile/package.json',import.meta.url)),ts=requireMobile('typescript'),source=fs.readFileSync(new URL('../web/app.js',import.meta.url),'utf8'),tree=ts.createSourceFile('app.js',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.JS);
 const functions=tree.statements.filter(node=>ts.isFunctionDeclaration(node)&&['groupKey','decryptGroupMsgUnlocked'].includes(node.name?.text)).map(node=>node.getText(tree)).join('\n');
 const decrypted=deferred(),started=deferred(),writes=[],state={me:{id:'original'}},store={get:async()=>null,set:async(key,value)=>writes.push({account:'original',key,value})};
 const scope={state,console,createAccountStore:account=>{assert.equal(account,'original');return store;},window:{SigStore:{get:async key=>key.includes(':own:')?{}:null},GroupCrypto:{ownReceiverState:()=>({}),decryptWithState:async()=>{started.resolve();return decrypted.promise;}}}};
 vm.runInNewContext(functions+'\nthis.decode=decryptGroupMsgUnlocked;',scope);
 const result=scope.decode('room',{sender_id:'original',encrypted_content:'{"kid":"key"}'},'original');await started.promise;state.me={id:'different'};decrypted.resolve({plaintext:'private original message',state:{}});assert.equal(await result,'private original message');assert.equal(writes.length,1);assert.equal(writes[0].account,'original');
});
