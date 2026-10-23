import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import vm from 'node:vm';
import {webcrypto} from 'node:crypto';
import {createSessionScope} from '../web/session-scope.js';

const mobileRequire=createRequire(new URL('../mobile/package.json',import.meta.url)),ts=mobileRequire('typescript');
const tree=ts.createSourceFile('App.tsx',readFileSync(new URL('../mobile/App.tsx',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true,ts.ScriptKind.TSX);
const body=tree.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='App').body;
function evaluate(source,scope){return vm.runInNewContext(ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText,scope);}
function callback(name,scope){const fn=body.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text===name);assert.ok(fn,name);return evaluate('('+fn.getText(tree)+')',scope);}
function initializer(name,scope){const declaration=body.statements.filter(ts.isVariableStatement).flatMap(n=>[...n.declarationList.declarations]).find(n=>n.name.getText(tree)===name);assert.ok(declaration,name);return evaluate('('+declaration.initializer.getText(tree)+')',scope);}
function deferred(){let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};}
function scope(){return {baseUrl:'server',token:'saved-a',me:{id:'a'},tokenRef:{current:'saved-a'},sessionGeneration:1,sessionGenerationRef:{current:1},setSessionGeneration:()=>{},setToken:()=>{},roomSecretCache:{current:new Map()},local:{repository:{current:null},repositoryGeneration:{current:1}},useMemo:fn=>fn(),createSessionScope};}
function reopenA(s){const activate=callback('activateNativeSession',s);activate('saved-b');s.local.repository.current=null;activate('saved-a');}

test('an old room-key response cannot recreate native cache after switching accounts',async()=>{
 const s=scope(),response=deferred();s.api={roomKey:()=>response.promise};const pending=callback('getRoomSecret',s)('room'),rejected=assert.rejects(pending,/Сеанс изменился/);
 s.tokenRef.current='saved-b';s.sessionGenerationRef.current++;response.resolve({key:'old-private-key'});await rejected;assert.equal(s.roomSecretCache.current.size,0);
});
test('A → B → A with the same saved bearer cannot revive a delayed room key',async()=>{
 const s=scope(),response=deferred();s.api={roomKey:()=>response.promise};const pending=callback('getRoomSecret',s)('room'),rejected=assert.rejects(pending,/Сеанс изменился/);
 reopenA(s);response.resolve({key:'old-private-key'});await rejected;assert.equal(s.roomSecretCache.current.size,0);
});
test('room key bootstrap accepts its explicit nextToken and caches during the active activation',async()=>{
 const s=scope();s.token=null;let requests=0;s.api={roomKey:async(base,token,room)=>{assert.equal(base,'next-server');assert.equal(token,'saved-a');assert.equal(room,'room');requests++;return {key:'current-key'};}};
 const get=callback('getRoomSecret',s);assert.equal(await get('room','next-server','saved-a'),'current-key');assert.equal(await get('room','next-server','saved-a'),'current-key');assert.equal(requests,1);
});

// Keep GroupEncryption's real async enabled()/repository write path. Only its external
// HTTP/Signal dependencies are replaced; importing the native crypto stack needs Expo.
function groupClass(api,ratchet={}){const exports={};evaluate(readFileSync(new URL('../mobile/src/groupEncryption.ts',import.meta.url),'utf8'),{exports,require:path=>path==='./api'?{api,request:api.request}:path==='./crypto'?{getMobileDeviceId:async()=> 'device',E2EEncryption:class{configure(){}async initialize(){}}}:ratchet});return exports.GroupEncryption;}
function repository(user='a'){const values=new Map();return {userId:user,values,async set(k,v){values.set(k,v);},async get(k,fallback){return values.has(k)?values.get(k):fallback;}};}
test('an old actual group enabled operation rejects before writing to a reopened A repository',async()=>{
 const s=scope(),response=deferred(),old=repository(),replacement=repository();s.local.repository.current=old;s.GroupEncryption=groupClass({roomE2e:()=>response.promise});
 const group=initializer('groupEncryption',s),pending=group.enabled('group'),rejected=assert.rejects(pending);reopenA(s);s.local.repository.current=replacement;response.resolve({enabled:true});await rejected;
 assert.equal(old.values.size,0);assert.equal(replacement.values.size,0);
});
test('a group engine pins its repository even if the same activation gets a replacement',async()=>{
 const s=scope(),old=repository(),replacement=repository();s.local.repository.current=old;s.GroupEncryption=groupClass({roomE2e:async()=>({enabled:true})});
 const group=initializer('groupEncryption',s);s.local.repository.current=replacement;await assert.rejects(group.enabled('group'));assert.equal(replacement.values.size,0);assert.equal(old.values.size,0);
});
test('a delayed actual group decrypt cannot advance the replacement A receiver ratchet',async()=>{
 const ratchet={};evaluate(readFileSync(new URL('../mobile/src/groupRatchet.ts',import.meta.url),'utf8'),{exports:ratchet,require:mobileRequire,crypto:webcrypto,Uint8Array,btoa,atob,setTimeout});
 const sender=await ratchet.createSenderKey(),initial=ratchet.receiverStateFromDistribution(ratchet.distributionFor(sender));
 const {envelope}=await ratchet.encryptWithState(sender,'group','sender','private');
 const s=scope(),old=repository(),replacement=repository(),started=deferred(),state=deferred(),key='group:group:in:sender:'+sender.keyId;
 old.values.set(key,{initial,current:initial});replacement.values.set(key,{initial,current:{...initial,n:17}});
 const read=old.get.bind(old);old.get=async(k,fallback)=>{if(k===key){started.resolve();return state.promise;}return read(k,fallback);};
 s.local.repository.current=old;s.GroupEncryption=groupClass({},ratchet);const group=initializer('groupEncryption',s);
 const pending=group.decrypt({room_id:'group',sender_id:'sender',encrypted_content:JSON.stringify(envelope)}),rejected=assert.rejects(pending);
 await started.promise;reopenA(s);s.local.repository.current=replacement;state.resolve(old.values.get(key));await rejected;
 assert.equal(replacement.values.get(key).current.n,17);assert.equal(replacement.values.size,1);assert.equal(old.values.get(key).current.n,0);assert.equal(old.values.size,1);
});
test('a group engine may bind a loading repository once, and rejects its later replacement',async()=>{
 const s=scope(),first=repository(),replacement=repository();s.GroupEncryption=groupClass({roomE2e:async()=>({enabled:true})});const group=initializer('groupEncryption',s);
 await assert.rejects(group.enabled('group'));s.local.repository.current=first;assert.equal(await group.enabled('group'),true);assert.equal(first.values.get('e2e:group').enabled,true);
 s.local.repository.current=replacement;await assert.rejects(group.enabled('group'));assert.equal(replacement.values.size,0);
});
test('encodeText never enters group encryption after a delayed enabled check across A → B → A',async()=>{
 const s=scope(),enabled=deferred();let encryptions=0;s.rooms=[{id:'group',kind:'group'}];s.groupEncryption={enabled:()=>enabled.promise,encrypt:async()=>{encryptions++;return {encrypted_content:'cipher'};}};
 const pending=callback('encodeText',s)('group','private'),rejected=assert.rejects(pending,/Сеанс изменился/);reopenA(s);enabled.resolve(true);await rejected;assert.equal(encryptions,0);
});
test('encodeText rejects a delayed direct encryption result after A → B → A',async()=>{
 const s=scope(),encrypted=deferred();s.rooms=[];s.getRoomSecret=async()=> 'current-key';s.encryptRoomText=()=>encrypted.promise;const pending=callback('encodeText',s)('room','private'),rejected=assert.rejects(pending,/Сеанс изменился/);
 await Promise.resolve();reopenA(s);encrypted.resolve({text:'cipher'});await rejected;
});
test('feature adapter rejects old callbacks even while the repository is null during loading',async()=>{
 const s=scope();s.featureRepository=null;const feature=initializer('featureScope',s);let calls=0;const old=feature.wrap(async()=>{calls++;});reopenA(s);await assert.rejects(old(),/Сеанс изменился/);assert.equal(calls,0);
});
test('persistSession reopens the saved token with a new generation and preserves account registration',async()=>{
 const s=scope(),saved=[],activated=[],stored=[];let bootstrapped=0;
 Object.assign(s,{resetUnauthorizedGuard(){},setBaseUrl(){},setMe(){},removeSessionTokens:async()=>{},storage:{set:async(...args)=>stored.push(args)},accountRegistry:{save:async(...args)=>saved.push(args),activate:async(id,hooks)=>{activated.push(id);await hooks.close();await hooks.open();}},bootstrap:async(base,token,legacy)=>{assert.equal(base,'next-server');assert.equal(token,'saved-a');assert.equal(legacy,false);assert.equal(s.sessionGenerationRef.current,2);bootstrapped++;},setError:error=>assert.fail(error)});
 s.activateNativeSession=callback('activateNativeSession',s);
 await callback('persistSession',s)('next-server','saved-a',{id:'a',username:'alice'});
 assert.equal(s.tokenRef.current,'saved-a');assert.equal(s.sessionGenerationRef.current,2);assert.equal(saved.length,1);assert.equal(saved[0][0].userId,'a');assert.equal(saved[0][1],'saved-a');assert.deepEqual(activated,['a']);assert.equal(stored[0][0],'mobile.user');assert.equal(bootstrapped,1);
});

// Execute the actual hook with React's render-before-effects ordering. Repository
// open is kept pending; lifecycle ownership is published at setup, not at ready.
function accountDataLifecycle(){
 const refs=[],states=[];let cursor=0,stateCursor=0,effect,dependencies,cleanup,pending;
 const react={useRef:value=>refs[cursor++]??(refs[cursor-1]={current:value}),useState:value=>{const index=stateCursor++;states[index]??=value;return [states[index],next=>{states[index]=next;}];},useEffect:(fn,deps)=>{if(!dependencies||deps.some((value,index)=>value!==dependencies[index]))pending={fn,deps};}};
 const hookTree=ts.createSourceFile('useAccountData.ts',readFileSync(new URL('../mobile/src/hooks/useAccountData.ts',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true);
 const fn=hookTree.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='useAccountData');
 const hook=evaluate('('+fn.getText(hookTree).replace(/^export\s+/,'')+')',{
  ...react,AccountRepository:class{constructor(user){Object.assign(this,repository(user));}open(){return new Promise(()=>{});}observeMessages(){return ()=>{};}async close(){this.closed=true;}},
  Outbox:class{stop(){}async flush(){}},expressiveHistoryPrefix:()=> 'scope',expressiveHistoryLoader:{clear(){}},clearMessageWindows(){},
  addNetworkStateListener:()=>({remove(){}}),setInterval:()=>1,clearInterval(){},clearTimeout(){},
 });
 return {render:params=>{cursor=0;stateCursor=0;effect=hook(params);return effect;},commit:()=>{if(pending){cleanup?.();cleanup=pending.fn();dependencies=pending.deps;pending=null;}},close:()=>cleanup?.()};
}
for(const restoredToken of ['restored-a','saved-a'])test(`restoring the same user with ${restoredToken} binds the new activation store across effect replacement`,async()=>{
 const ratchet={};evaluate(readFileSync(new URL('../mobile/src/groupRatchet.ts',import.meta.url),'utf8'),{exports:ratchet,require:mobileRequire,crypto:webcrypto,Uint8Array,btoa,atob,setTimeout});
 const s=scope(),lifecycle=accountDataLifecycle();const props=()=>({me:s.me,token:s.token,baseUrl:s.baseUrl,activationGeneration:s.sessionGeneration,onMessages(){},onRooms(){},onError:error=>assert.fail(error)});
 s.local=lifecycle.render(props());lifecycle.commit();const oldRepo=s.local.repository.current;
 s.GroupEncryption=groupClass({roomE2e:async()=>({enabled:true}),e2ePrekeyCount:async()=>({unused:20}),request:async()=>[]},ratchet);
 const oldGroup=initializer('groupEncryption',s);
 callback('activateNativeSession',s)(restoredToken);s.token=restoredToken;s.sessionGeneration=s.sessionGenerationRef.current;
 s.local=lifecycle.render(props());assert.equal(s.local.repository.current,oldRepo);const newGroup=initializer('groupEncryption',s);
 await assert.rejects(newGroup.enabled('group'));assert.equal(oldRepo.values.size,0);
 lifecycle.commit();const nextRepo=s.local.repository.current;assert.notEqual(nextRepo,oldRepo);assert.equal(oldRepo.closed,true);
 assert.equal(await newGroup.enabled('group'),true);const encrypted=await newGroup.encrypt('group','restored private text');
 assert.equal(await newGroup.decrypt({room_id:'group',sender_id:'a',encrypted_content:encrypted.encrypted_content}),'restored private text');
 const nextSize=nextRepo.values.size;await assert.rejects(oldGroup.encrypt('group','stale text'));assert.equal(nextRepo.values.size,nextSize);assert.equal(oldRepo.values.size,0);
 lifecycle.close();assert.equal(s.local.repository.current,null);assert.equal(s.local.repositoryGeneration.current,null);
});
test('teardown renders keep the account-data effect open until explicit repository cleanup',async()=>{
 const s=scope(),lifecycle=accountDataLifecycle();let activationState=1;
 s.useState=()=>[activationState,next=>{activationState=next;}];
 const declaration=body.statements.filter(ts.isVariableStatement).flatMap(n=>[...n.declarationList.declarations]).find(n=>n.name.getText(tree)==='sessionGeneration'||(ts.isArrayBindingPattern(n.name)&&n.name.elements.some(element=>element.name?.getText(tree)==='sessionGeneration')));
 function render(){const value=evaluate('('+declaration.initializer.getText(tree)+')',s);s.sessionGeneration=Array.isArray(value)?value[0]:value;s.local=lifecycle.render({me:s.me,token:s.token,baseUrl:s.baseUrl,activationGeneration:s.sessionGeneration,onMessages(){},onRooms(){},onError:error=>assert.fail(error)});lifecycle.commit();}
 render();const oldRepo=s.local.repository.current;
 Object.assign(s,{endCall(){},stopExpressiveLiveLocations(){},cancelDecryptedMedia:async()=>{},pushNotifications:{disconnect:async()=>{}},wsRef:{current:null},reconnectTimerRef:{current:null},typingTimersRef:{current:{}},signalSession:{current:null},clearTimeout(){},clearDerivedRoomKeys(){},removeSessionTokens:async()=>{},storage:{del:async()=>{}},setComposer:()=>{render();assert.equal(s.local.repository.current,oldRepo);assert.equal(oldRepo.closed,undefined);}});
 for(const name of ['Workspace','Forwarding','Editing','AuthChallenge','ViewerMessage','TypingUsers','Rooms','Messages','ActiveRoomId','ActiveRoomMembers','Me'])s['set'+name]=()=>{};
 await callback('clearSession',s)(true);assert.equal(s.sessionGenerationRef.current,2);assert.equal(oldRepo.closed,true);lifecycle.close();
});
