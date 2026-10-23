import {test} from 'node:test';
import assert from 'node:assert/strict';
import {openConversationFeature} from '../web/conversation-features.js';
import {mountSecretChats} from '../web/secret-screen.js';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import vm from 'node:vm';
import {t as uiText,locale as uiLocale,setLanguage} from '../web/i18n.js';
const selectors=await import('../web/feature-selectors.js').catch(()=>({}));
function deferred(){let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};}
test('secret selector searches escaped usernames and binds only an explicitly selected fetched device',async()=>{
 assert.equal(typeof selectors.SecretPeerSelector,'function');
 const paths=[],s=new selectors.SecretPeerSelector(async p=>{paths.push(p);return p.startsWith('/users/search')?[{id:'bob',username:'bob'}]:[{device_id:'12',platform:'android'},{device_id:'34'}];},()=>true);
 await s.search(' @bob & ');assert.equal(paths[0],'/users/search?q=bob%20%26');
 await s.selectUser('bob');assert.throws(()=>s.payload('own',3600),/устройство/i);
 assert.throws(()=>s.selectDevice('fake'),/устройство/i);s.selectDevice('34');
 assert.deepEqual(s.payload('own',3600),{peer_id:'bob',peer_device_id:'34',device_id:'own',ttl:3600});
 await s.search('alice');assert.throws(()=>s.payload('own',3600),/собеседника/i);
});
test('secret selector rejects delayed results after reset, a new search or account loss',async()=>{
 assert.equal(typeof selectors.SecretPeerSelector,'function');
 for(const invalidation of ['reset','search','account']){const wait=deferred();let live=true;const s=new selectors.SecretPeerSelector(()=>wait.promise,()=>live);const pending=s.search('bob'),rejection=assert.rejects(pending,/Сеанс/);if(invalidation==='reset')s.reset();else if(invalidation==='search')void s.search('a').catch(()=>{});else live=false;wait.resolve([{id:'bob'}]);await rejection;assert.equal(s.users.length,0);}
});
test('secret device selection cannot accept a result for a previously selected user',async()=>{
 assert.equal(typeof selectors.SecretPeerSelector,'function');const wait=deferred();const s=new selectors.SecretPeerSelector(async p=>p.startsWith('/users/search')?[{id:'bob'},{id:'alice'}]:wait.promise,()=>true);await s.search('name');const pending=s.selectUser('bob'),rejection=assert.rejects(pending,/Сеанс/);s.reset();wait.resolve([{device_id:'12'}]);await rejection;assert.equal(s.devices.length,0);
});
test('inaccessible device lookup gives an empty choice rather than accepting typed devices',async()=>{
 const s=new selectors.SecretPeerSelector(async p=>{if(p.startsWith('/users/search'))return [{id:'bob'}];throw Error('keys_not_found');},()=>true);await s.search('bob');assert.deepEqual(await s.selectUser('bob'),[]);assert.throws(()=>s.payload('own',3600),/устройство/i);
});
test('existing secret chat labels resolve peer names and keep protocol IDs internal',async()=>{
 assert.equal(typeof selectors.secretChatLabels,'function');const rows=await selectors.secretChatLabels([{id:'internal-chat-id',initiator:'alice',recipient:'bob',recipient_device:'123456',created_at:'2026-01-01'}],'alice',async path=>{assert.equal(path,'/users/bob');return {display_name:'Bob',username:'bob'};},()=>true);assert.ok(rows[0]._label.includes('Bob'));assert.ok(rows[0]._label.includes('…3456'));assert.ok(!rows[0]._label.includes('internal-chat-id'));assert.equal(rows[0].id,'internal-chat-id');
});
function dom(){const nodes=[];class Element{constructor(tag){this.tag=tag;this.style={};this.listeners={};this.children=[];this.isConnected=true;nodes.push(this);}append(...v){this.children.push(...v);}replaceChildren(...v){this.children=v;}setAttribute(){}addEventListener(n,fn){this.listeners[n]=fn;}showModal(){}remove(){this.isConnected=false;}close(){this.listeners.close?.();this.isConnected=false;}}return {nodes,document:{hidden:false,createElement:tag=>new Element(tag),body:new Element('body'),addEventListener(){},removeEventListener(){}}};}
test('history opens selected room message automatically and no selection makes no history request',async()=>{
 const before=globalThis.document;try{for(const target of [{id:'message',room_id:'room',text:'Selected preview'},null,{id:'foreign',room_id:'other'}]){const d=dom(),paths=[];globalThis.document=d.document;await openConversationFeature('history',{room:{id:'room'},me:{id:'me'},targetMessage:target,request:async p=>{paths.push(p);return [{id:'revision',encrypted_content:'opaque',created_at:'2026-01-01'}];},decode:async()=>({text:'Old content'})});assert.deepEqual(paths,target?.room_id==='room'?['/rooms/room/messages/message/history']:[]);assert.equal(d.nodes.filter(n=>n.tag==='input').length,0);if(target?.room_id==='room')assert.ok(d.nodes.some(n=>n.textContent?.includes('Old content')));}}finally{globalThis.document=before;}
});
test('channel comments use the selected post for listing and creation, without post-ID input',async()=>{
 const before=globalThis.document;try{const d=dom(),calls=[];globalThis.document=d.document;await openConversationFeature('comments',{room:{id:'channel',kind:'channel'},targetMessage:{id:'post',room_id:'channel',is_post:true,text:'Publication'},request:async(p,o)=>{calls.push([p,o]);return [];}});assert.equal(calls[0]?.[0],'/posts/post/comments');assert.equal(d.nodes.filter(n=>n.tag==='input').length,0);const input=d.nodes.find(n=>n.tag==='textarea');assert.ok(input);input.value='A comment';await d.nodes.find(n=>n.tag==='button'&&n.textContent==='Отправить комментарий').listeners.click();assert.deepEqual(calls[1],['/posts/post/comments',{method:'POST',body:{type:'text',text:'A comment'}}]);}finally{globalThis.document=before;}
});
test('rendered browser selector requires an explicit real device and sends its binding',async()=>{
 const before=globalThis.document,d=dom(),calls=[];globalThis.document=d.document;const content=d.document.createElement('section'),dialog=d.document.createElement('dialog');
 try{await mountSecretChats({content,dialog,say(){},context:{me:{id:'alice'},local:{get:async()=>null,set:async()=>{},deviceId:async()=> 'own',isCurrent:()=>true},request:async(p,o)=>{calls.push([p,o]);if(p==='/me/account/secret-chats'&&!o)return [];if(p.startsWith('/users/search'))return [{id:'bob',username:'bob'}];if(p==='/e2e/devices/bob')return [{device_id:'42',label:'Phone',platform:'Android'}];throw Error('stop before opening');}}});
 const click=async label=>{const b=d.nodes.find(n=>n.tag==='button'&&n.textContent===label);assert.ok(b,label);await b.onclick();};
 const q=d.nodes.find(n=>n.tag==='input');q.value='bob';await click('Найти собеседника');await click('bob');await click('Создать секретный чат');assert.equal(calls.filter(([,o])=>o?.method==='POST').length,0);await click('Phone · Android · …42');await click('Создать секретный чат');assert.deepEqual(calls.at(-1),['/me/account/secret-chats',{method:'POST',body:{peer_id:'bob',peer_device_id:'42',device_id:'own',ttl:3600}}]);
 }finally{dialog.close();globalThis.document=before;}
});
const ts=createRequire(new URL('../mobile/package.json',import.meta.url))('typescript');
const element=(type,props,...children)=>({type,props:props||{},children:children.flat(Infinity)});
function flatten(n){return n&&typeof n==='object'?[n,...(n.children||[]).flatMap(flatten)]:[];}
test('native message actions expose selected-post comments and selected-message history',()=>{
 const source=readFileSync(new URL('../mobile/src/screens/Workspace.tsx',import.meta.url),'utf8'),ast=ts.createSourceFile('Workspace.tsx',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TSX),fn=ast.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='MessageActions');
 const scope={React:{createElement:element,Fragment:'Fragment'},useTheme:()=>({colors:{text:{},misc:{}},fontSize:16}),useSafeAreaInsets:()=>({bottom:0}),useState:()=>[false,()=>{}],useEffect(){},uiText,Modal:'Modal',View:'View',Pressable:'Pressable',ScrollView:'ScrollView',Text:'Text',Row:'Row'};
 const component=vm.runInNewContext(ts.transpileModule('('+fn.getText(ast).replace(/^export /,'')+')',{compilerOptions:{target:ts.ScriptTarget.ES2022,jsx:ts.JsxEmit.React}}).outputText,scope);
 for(const isPost of [true,false]){const chosen=[],m={id:'post',room_id:'channel',type:'text',sender_id:'peer',text:'Publication',is_post:isPost};const tree=component({messages:[m],meId:'me',onClose:()=>chosen.push('close'),onHistory:v=>chosen.push(v),onComments:v=>chosen.push(v)}),rows=flatten(tree);const comments=rows.find(n=>n.props.label==='Комментарии');assert.equal(Boolean(comments),isPost);if(comments){comments.props.onPress();assert.equal(chosen[1],m);}assert.ok(rows.find(n=>n.props.label==='История правок'));}
});
function nativeConversation(request,account=false){
 const states=[],refs=[],effects=[];let cursor=0,refCursor=0,effectCursor=0,pending=[],tree;
 const react={createElement:element,Fragment:'Fragment',useState(initial){const i=cursor++;if(!(i in states))states[i]=initial;return [states[i],v=>{states[i]=typeof v==='function'?v(states[i]):v;}];},useRef(initial){const i=refCursor++;return refs[i]??(refs[i]={current:initial});},useEffect(fn,deps){const i=effectCursor++,prior=effects[i];if(!prior||deps.some((v,j)=>v!==prior.deps[j])){prior?.cleanup?.();pending.push(()=>{effects[i]={deps,cleanup:fn()};});}}};
 let background;const exports={},shared={...selectors},component=account?'AccountFeatures':'ConversationFeatures';vm.runInNewContext(ts.transpileModule(readFileSync(new URL('../mobile/src/features/'+component+'.tsx',import.meta.url),'utf8'),{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS,jsx:ts.JsxEmit.React,esModuleInterop:true}}).outputText,{exports,Date,encodeURIComponent,setInterval:()=>1,clearInterval(){},require:path=>path==='react'?react:path==='react-native'?{...Object.fromEntries(['ScrollView','View','Text','TextInput','Pressable','ActivityIndicator'].map(x=>[x,x])),AppState:{currentState:'active',addEventListener:(_event,fn)=>{background=fn;return {remove(){}};}}}:path==='../api'?{request}:path==='../accountRegistry'?{getAccountNativeHooks:()=>({deviceId:async()=> 'own'}),protectSecretScreen:async()=>{}}:path==='expo-camera'?{useCameraPermissions:()=>[null,()=>{}]}:path==='../ThemeProvider'?{useTheme:()=>({colors:{text:{}}})}:path.includes('feature-selectors')?shared:path.includes('i18n')?{t:uiText,locale:uiLocale}:{userError:e=>e.message}});
 return {render(mode,context){cursor=0;refCursor=0;effectCursor=0;tree=exports[component]({mode,context});const work=pending;pending=[];work.forEach(fn=>fn());return tree;},nodes:()=>flatten(tree),background:()=>background?.('background'),close(){effects.forEach(e=>e?.cleanup?.());}};
}
const settle=()=>new Promise(r=>setImmediate(r));
test('native history and comments load the selected context automatically and send to that same post',async()=>{
 const calls=[],h=nativeConversation(async(base,path,o)=>{calls.push([base,path,o]);return path.endsWith('/history')?[{id:'revision',encrypted_content:'opaque',created_at:'2026-01-01'}]:[{id:'comment',sender_username:'bob',text:'Reply'}];}),c={baseUrl:'server',token:'token',me:{id:'me'},room:{id:'channel',kind:'channel'},targetMessage:{id:'post',room_id:'channel',is_post:true,text:'Publication'},decode:async()=>({text:'Before edit'})};
 try{h.render('history',c);await settle();h.render('history',c);assert.equal(calls[0]?.[1],'/rooms/channel/messages/post/history');assert.ok(h.nodes().some(n=>n.children.some(x=>typeof x==='string'&&x.includes('Before edit'))));assert.equal(h.nodes().filter(n=>n.type==='TextInput').length,0);
 h.render('comments',c);await settle();h.render('comments',c);assert.equal(calls[1]?.[1],'/posts/post/comments');const input=h.nodes().find(n=>n.type==='TextInput');input.props.onChangeText('Chosen reply');h.render('comments',c);h.nodes().find(n=>n.type==='Pressable'&&n.children.some(ch=>ch?.children?.includes('Отправить комментарий'))).props.onPress();await settle();assert.deepEqual(JSON.parse(JSON.stringify(calls[2])),['server','/posts/post/comments',{token:'token',method:'POST',body:{type:'text',text:'Chosen reply'}}]);
 h.render('comments',{...c,targetMessage:null});await settle();h.render('comments',{...c,targetMessage:null});assert.equal(h.nodes().filter(n=>n.type==='TextInput').length,0);
 }finally{h.close();}
});
test('native selected-post callbacks cannot send after account and target replacement',async()=>{
 const calls=[],h=nativeConversation(async(base,path,o)=>{calls.push([base,path,o]);return [];}),c={baseUrl:'server',token:'a',me:{id:'a'},room:{id:'channel',kind:'channel'},targetMessage:{id:'post',room_id:'channel',is_post:true,text:'Private post'}};
 try{h.render('comments',c);await settle();h.render('comments',c);const stale=h.nodes().find(n=>n.type==='Pressable'&&n.children.some(ch=>ch?.children?.includes('Отправить комментарий'))).props.onPress;h.render('comments',{...c,token:'b',me:{id:'b'},targetMessage:null});await settle();stale();await settle();assert.equal(calls.filter(([, ,o])=>o.method==='POST').length,0);}finally{h.close();}
});
test('native history never paints the previous selection during a context replacement render',async()=>{
 const h=nativeConversation(async()=>[{id:'revision',created_at:'2026-01-01',encrypted_content:'opaque'}]),c={baseUrl:'server',token:'a',me:{id:'a'},room:{id:'room'},targetMessage:{id:'old',room_id:'room',text:'Selected'},decode:async()=>({text:'Private version'})};
 try{h.render('history',c);await settle();h.render('history',c);assert.ok(h.nodes().some(n=>n.children.includes('Private version')));h.render('history',{...c,token:'b',me:{id:'b'},targetMessage:null});assert.ok(!h.nodes().some(n=>n.children.includes('Private version')));}finally{h.close();}
});
test('native secret UI searches usernames and creates only with the chosen server device',async()=>{
 const calls=[],h=nativeConversation(async(base,path,o)=>{calls.push([path,o]);if(path==='/me/account/secret-chats'&&o.method==='GET')return [];if(path.startsWith('/users/search'))return [{id:'bob',username:'bob'}];if(path==='/e2e/devices/bob')return [{device_id:'42',label:'Phone',platform:'Android'}];throw Error('stop before opening');},true),c={baseUrl:'server',token:'a',me:{id:'alice'}};
 const render=()=>h.render('secret',c),press=async label=>{const b=h.nodes().find(n=>n.type==='Pressable'&&n.children.some(ch=>ch?.children?.includes(label)));assert.ok(b,label);b.props.onPress();await settle();render();};
 try{render();await settle();render();h.nodes().find(n=>n.type==='TextInput'&&n.props.accessibilityLabel==='Имя пользователя собеседника').props.onChangeText('bob');render();await press('Найти собеседника');await press('bob');await press('Создать секретный чат');assert.equal(calls.filter(([,o])=>o.method==='POST').length,0);await press('Phone · Android · …42');await press('Создать секретный чат');assert.deepEqual(JSON.parse(JSON.stringify(calls.at(-1))),['/me/account/secret-chats',{token:'a',method:'POST',body:{peer_id:'bob',peer_device_id:'42',device_id:'own',ttl:3600}}]);h.background();render();assert.equal(h.nodes().filter(n=>n.type==='Pressable'&&n.children.some(ch=>ch?.children?.includes('Phone · Android · …42'))).length,0);}finally{h.close();}
});
const matchingNames=[{id:'work',username:'alex_work',display_name:'Алексей'},{id:'home',username:'alex_home',display_name:'Алексей'},{id:'literal',display_name:'Найти'}];
test('browser people distinguish duplicate display names by username and preserve names literally in English',async()=>{
 const before=globalThis.document;try{for(const lang of ['ru','en']){setLanguage(lang);const d=dom(),paths=[];globalThis.document=d.document;const content=d.document.createElement('section'),dialog=d.document.createElement('dialog');
 try{await mountSecretChats({content,dialog,say(){},context:{me:{id:'alice'},local:{get:async()=>null,set:async()=>{},deviceId:async()=> 'own',isCurrent:()=>true},request:async p=>{paths.push(p);return p==='/me/account/secret-chats'?[]:p.startsWith('/users/search')?matchingNames:[];}}});const q=d.nodes.find(n=>n.tag==='input');q.value='alex';await d.nodes.find(n=>n.tag==='button'&&n.textContent===uiText('Найти собеседника')).onclick();const people=content.children.find(n=>n.tag==='div'&&n.children.some(ch=>ch.tag==='button'&&ch.textContent.includes('Алексей')));assert.ok(people);assert.deepEqual(people.children.map(n=>n.textContent),['Алексей · @alex_work','Алексей · @alex_home','Найти']);await people.children[1].onclick();assert.equal(paths.at(-1),'/e2e/devices/home');assert.ok(d.nodes.some(n=>n.tag==='p'&&n.textContent===uiText('Выберите устройство собеседника')+' · Алексей · @alex_home'));}finally{dialog.close();}}
 }finally{globalThis.document=before;setLanguage('ru');}
});
test('native people have stable person keys, distinguish usernames, and retain translated-key names literally',async()=>{
 try{for(const lang of ['ru','en']){setLanguage(lang);const paths=[],h=nativeConversation(async(_base,path)=>{paths.push(path);return path==='/me/account/secret-chats'?[]:path.startsWith('/users/search')?matchingNames:[];},true),c={baseUrl:'server',token:'token',me:{id:'alice'}};try{h.render('secret',c);await settle();h.render('secret',c);h.nodes().find(n=>n.type==='TextInput'&&n.props.accessibilityLabel===uiText('Имя пользователя собеседника')).props.onChangeText('alex');h.render('secret',c);h.nodes().find(n=>n.type==='Pressable'&&n.children.some(ch=>ch?.children?.includes(uiText('Найти собеседника')))).props.onPress();await settle();h.render('secret',c);const people=h.nodes().filter(n=>n.type==='Pressable'&&n.children.some(ch=>ch?.children?.some(value=>typeof value==='string'&&(value.includes('Алексей')||value==='Найти'))));assert.deepEqual(people.map(n=>n.children[0].children[0]),['Алексей · @alex_work','Алексей · @alex_home','Найти']);assert.deepEqual(people.map(n=>n.props.key),['work','home','literal']);people[1].props.onPress();await settle();h.render('secret',c);assert.equal(paths.at(-1),'/e2e/devices/home');assert.ok(h.nodes().some(n=>n.children.includes(uiText('Выберите устройство собеседника')+' · Алексей · @alex_home')));}finally{h.close();}}}finally{setLanguage('ru');}
});
