import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {createRequire} from 'node:module';
import {createSessionScope} from '../web/session-scope.js';

test('an awaited feature result is rejected after switching accounts, even back to the same account',async()=>{
 let account='alice',generation=1,finish;
 const scope=createSessionScope({account,generation,currentAccount:()=>account,currentGeneration:()=>generation});
 const pending=scope.wrap(()=>new Promise(resolve=>{finish=resolve;}))();
 account='bob';generation++;account='alice';generation++;finish('old private data');
 await assert.rejects(pending,/Сеанс изменился/);
 let called=false;await assert.rejects(scope.wrap(async()=>{called=true;})(),/Сеанс изменился/);assert.equal(called,false);
});

test('synchronous adapters preserve their values and cannot continue after a session change',()=>{
 let account='alice',generation=4;
 const scope=createSessionScope({account,generation,currentAccount:()=>account,currentGeneration:()=>generation});
 const value={text:'Сообщение: Отмена'};assert.equal(scope.sync(input=>input)(value),value);
 assert.throws(scope.sync(()=>{generation++;return 'changed';}),/Сеанс изменился/);
 assert.throws(scope.guard,/Сеанс изменился/);
 account=null;assert.throws(createSessionScope({account,generation,currentAccount:()=>account,currentGeneration:()=>generation}).guard,/Сеанс изменился/);
});

function featureHarness(){
 const require=createRequire(import.meta.url),ts=require('../mobile/node_modules/typescript'),source=fs.readFileSync(new URL('../web/app.js',import.meta.url),'utf8'),ast=ts.createSourceFile('app.js',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.JS),fn=ast.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='webFeatureContext');assert.ok(fn);
 const calls=[],state={me:{id:'alice'},rooms:[{id:'room'}],activeRoomId:'room'},session={generation:1};
 const scope={state,createSessionScope,captureWebSession:()=>createSessionScope({account:state.me?.id,generation:session.generation,currentAccount:()=>state.me?.id,currentGeneration:()=>session.generation}),
  chatRuntime:{local:()=>({get:async()=>null,set:async()=>{calls.push('write');}}),enqueue:async row=>{calls.push(['send',row]);return row;}},
  featureRequest:async(path,options)=>{calls.push(['request',path,options]);return{ok:true};},pauseMessageMedia:()=>calls.push('pause'),sendWebCatalogAsset:async()=>calls.push('catalog'),exportWebMedia:async()=>calls.push('export'),
  buildTextMessage:async(id,room,text)=>{calls.push(['encrypt',id,text]);return{text};},applyWebDraft:async()=>calls.push('draft'),queueWebDraftSync:()=>calls.push('sync'),webDraftReplies:new Map(),webMediaUrls:{clear:()=>calls.push('clear')},decodeForWeb:async(_room,message)=>message,
  applyQualityPreference:()=>calls.push('quality'),loadRooms:async()=>calls.push('rooms'),mountPreservingInput:async()=>calls.push('mount'),switchBrowserAccount:async()=>{},addBrowserAccount:async()=>{},toast:value=>calls.push(['toast',value]),userError:String};
 const context=vm.runInNewContext('('+fn.getText(ast)+')',scope)();return {context,scope,calls,state,session};
}

test('actual browser feature requests carry original account headers and drop stale responses',async()=>{
 const h=featureHarness();let finish;
 h.scope.featureRequest=(_path,options)=>{h.calls.push(options.headers);return new Promise(resolve=>{finish=resolve;});};
 const pending=h.context.request('/rooms/room/draft',{headers:{custom:'value'}});assert.equal(h.calls[0]['X-Account-Id'],'alice');assert.equal(h.calls[0].custom,'value');
 h.state.me={id:'bob'};h.session.generation++;finish({private:'alice'});await assert.rejects(pending,/Сеанс изменился/);
});

test('actual browser adapters reject old callbacks before encryption, outbox, drafts, media and local writes',async()=>{
 const h=featureHarness(),payload='Отмена, Маша';assert.equal((await h.context.buildText('room',payload)).text,payload);h.calls.length=0;
 h.state.me={id:'bob'};h.session.generation++;
 for(const action of [()=>h.context.buildText('room',payload),()=>h.context.enqueue('room',payload),()=>h.context.setDraft('room',payload),()=>h.context.sendCatalogAsset({id:'asset'}),()=>h.context.exportMedia({id:'file'}),()=>h.context.local.set('key','value'),()=>h.context.decode({room_id:'room',text:payload})])await assert.rejects(action(),/Сеанс изменился/);
 assert.throws(()=>h.context.local.clearMediaCache(),/Сеанс изменился/);assert.deepEqual(h.calls,[]);
});
test('a detached group call releases only its original account lease',async()=>{
 const h=featureHarness();h.state.me={id:'bob'};h.session.generation++;
 await h.context.request('/rooms/00000000-0000-4000-8000-000000000001/calls/leave',{method:'POST',body:{lease_id:'00000000-0000-4000-8000-000000000002'}});
 assert.equal(h.calls[0][2].headers['X-Account-Id'],'alice');
 await assert.rejects(()=>h.context.request('/rooms/00000000-0000-4000-8000-000000000001/calls/join',{method:'POST',body:{}}),/Сеанс изменился/);
});
