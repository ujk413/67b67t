import {test} from 'node:test';import assert from 'node:assert/strict';import {readFileSync} from 'node:fs';import {createRequire} from 'node:module';import vm from 'node:vm';
import {createSessionScope} from '../web/session-scope.js';
const ts=createRequire(new URL('../mobile/package.json',import.meta.url))('typescript'),tree=ts.createSourceFile('app.js',readFileSync(new URL('../web/app.js',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true,ts.ScriptKind.JS);
const fn=name=>tree.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text===name).getText(tree),deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return{promise,resolve};};
test('late room secrets never repopulate caches after logout and login to the same account',async()=>{
 const waiting=deferred(),state={me:{id:'alice'}},cache=new Map();let generation=1;
 const scope={state,roomSecretCache:cache,api:()=>waiting.promise,browserSessionGeneration:()=>generation,createSessionScope};
 vm.runInNewContext(fn('captureWebSession')+'\n'+fn('getRoomServerSecret')+'\nthis.load=getRoomServerSecret;',scope);
 const result=scope.load('room'),failure=assert.rejects(result,/Сеанс изменился/);generation++;waiting.resolve({key:'previous-session-secret'});await failure;assert.equal(cache.size,0);
});
test('Signal loading from an old session cannot publish or replace the current account manager',async()=>{
 const waiting=deferred(),state={me:{id:'alice'},e2eReady:false,e2eManager:null};let generation=1,published=0;
 class Manager{useStore(){}loadFromIndexedDB(){return waiting.promise;}getPublicBundle(){return{};}}
 const scope={state,browserSessionGeneration:()=>generation,createSessionScope,e2eInitTask:null,window:{libsignal:{},SigStore:{forAccount:()=>({})},E2EManager:Manager},getE2EDeviceId:()=>1,publishWebDeviceBundle:async()=>{published++;},maybeReplenishPreKeys:async()=>{},startPreKeyMonitor(){},uiText:text=>text,console:{warn(){}}};
 vm.runInNewContext(fn('captureWebSession')+'\n'+fn('initE2E')+'\nthis.start=initE2E;',scope);
 const result=scope.start();generation++;state.me={id:'bob'};waiting.resolve(true);await result;assert.equal(published,0);assert.equal(state.e2eManager,null);assert.equal(state.e2eReady,false);
});
