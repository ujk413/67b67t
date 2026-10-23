import {test} from 'node:test';
import {t as uiText,locale as uiLocale} from '../web/i18n.js';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),ts=require('../mobile/node_modules/typescript');
function loadNativeMedia(){
 const files=new Map(),actions=[],modules=new Map();let decryptResolve,decodeStarted,downloadPause=0;
 const Files={cacheDirectory:'file:///cache/',documentDirectory:'file:///documents/',getInfoAsync:async uri=>({exists:files.has(uri),isDirectory:uri.endsWith('/'),size:files.get(uri)?.length||0}),makeDirectoryAsync:async()=>{},readDirectoryAsync:async()=>[],deleteAsync:async uri=>{for(const key of files.keys())if(key===uri||key.startsWith(uri.replace(/\/$/,'')+'/'))files.delete(key);},moveAsync:async({from,to})=>{files.set(to,files.get(from));files.delete(from);},createDownloadResumable:(url,input)=>({pauseAsync:async()=>{downloadPause++;},downloadAsync:async()=>{files.set(input,'ciphertext');return {status:200,uri:input};}}),downloadAsync:async(url,target)=>{files.set(target,'downloaded');return {status:200,uri:target};},getContentUriAsync:async uri=>'content://file/'+uri.split('/').at(-1)};
 const native={decryptFileAsync:async(input,output)=>{decodeStarted?.();return new Promise(resolve=>{decryptResolve=()=>{files.set(output,'plaintext');resolve(output);};});}};
 function load(name){if(modules.has(name))return modules.get(name);const exports={};modules.set(name,exports);const source=readFileSync(new URL('../mobile/src/'+name+'.ts',import.meta.url),'utf8');new Function('exports','require',ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText)(exports,dep=>{
  if(dep==='expo')return {requireOptionalNativeModule:()=>native};if(dep==='expo-file-system/legacy')return Files;if(dep==='expo-sharing')return {isAvailableAsync:async()=>true,shareAsync:async(...args)=>actions.push(['share',...args])};if(dep==='expo-intent-launcher')return {startActivityAsync:async(...args)=>actions.push(['intent',...args])};if(dep==='react-native')return {Platform:{OS:'android'}};if(dep==='./storage')return {storage:{get:async()=>256}};if(dep.startsWith('@noble/hashes'))return require('../mobile/node_modules/'+dep);if(dep.startsWith('./'))return load(dep.slice(2));throw Error(dep);
 });return exports;}
 return {load,files,actions,Files,onDecode:fn=>{decodeStarted=fn;},finishDecrypt:()=>decryptResolve(),pauses:()=>downloadPause};
}
const user='00000000-0000-0000-0000-000000000001',message={id:'message',media_id:'00000000-0000-0000-0000-000000000002',room_id:'room',sender_id:'peer',meta:{media_encryption:{mime:'image/jpeg',name:'private.jpg',key_message:{}}}};
test('account media erasure drains inflight decrypt and prevents plaintext resurrection',async()=>{
 const h=loadNativeMedia(),media=h.load('decryptedMedia');assert.equal(typeof media.eraseDecryptedMedia,'function');let started;const ready=new Promise(resolve=>{started=resolve;});h.onDecode(started);
 const pending=media.resolveMediaSource(message,'https://self.test/api',user,{},async()=>({text:'{"key":"key","iv":"iv"}'}));const rejected=assert.rejects(pending,/заверш|отмен|удал/i);await ready;
 const erasing=media.eraseDecryptedMedia(user);h.finishDecrypt();await rejected;await erasing;assert.equal([...h.files.keys()].some(path=>path.includes(user)),false);
 await assert.rejects(()=>media.resolveMediaSource(message,'https://self.test/api',user,{},async()=>({text:'{}'})),/заверш|отмен|удал/i);
});
test('Android file action opens the actual system viewer with a granted content URI',async()=>{
 const h=loadNativeMedia(),media=h.load('mediaFiles');await media.openMediaFile('https://self.test/api/media/file',{},'application/pdf',user);
 assert.equal(h.actions[0][0],'intent');assert.equal(h.actions[0][1],'android.intent.action.VIEW');assert.equal(h.actions[0][2].type,'application/pdf');assert.equal(h.actions[0][2].flags&1,1);assert.match(h.actions[0][2].data,/^content:/);
});
function componentAction(file,component,name,scope){
 scope={uiText,uiLocale,...scope};
 const source=readFileSync(new URL('../mobile/src/components/'+file+'.tsx',import.meta.url),'utf8'),ast=ts.createSourceFile(file+'.tsx',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TSX),body=ast.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name.text===component).body;
 const declaration=body.statements.flatMap(n=>ts.isVariableStatement(n)?[...n.declarationList.declarations]:[]).find(n=>n.name.getText(ast)===name);assert.ok(declaration,name);
 return new Function(...Object.keys(scope),'return '+ts.transpileModule('('+declaration.initializer.getText(ast)+')',{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText)(...Object.values(scope));
}
const ref=current=>({current});
function circleHandoff({copyFailure=false,metadataFailure=false,holdMetadata=false}={}){
 const h=loadNativeMedia(),source='file:///cache/circle.mp4',current=ref([]),live=ref(true),generation=ref(1),records=new Map();h.files.set(source,'only recording');let closed=false,nextId=0,finishMetadata,metadataStarted;
 const waiting=new Promise(resolve=>{metadataStarted=resolve;}),held=new Promise(resolve=>{finishMetadata=resolve;});
 const scope={repository:{userId:user,set:(key,value)=>{const promise=(async()=>{metadataStarted();if(holdMetadata)await held;if(metadataFailure)throw Error('metadata storage full');records.set(key,structuredClone(value));})();promise.catch(()=>{});return promise;}},roomId:'room',caption:'',current,live,generation,Files:{...h.Files,copyAsync:async({from,to})=>{if(copyFailure)throw Error('ENOSPC');h.files.set(to,h.files.get(from));}},newId:()=>String(++nextId),runMediaTask:h.load('mediaLifecycle').runMediaTask,setItems:items=>{current.current=items;},setError(){},setCircle:value=>{closed=!value;}};
 scope.saveQueue=ref(Promise.resolve());scope.currentCaption=ref('');scope.save=componentAction('MediaTray','MediaTray','save',scope);scope.add=componentAction('MediaTray','MediaTray','add',scope);
 const traySource=readFileSync(new URL('../mobile/src/components/MediaTray.tsx',import.meta.url),'utf8'),ast=ts.createSourceFile('MediaTray.tsx',traySource,ts.ScriptTarget.Latest,true,ts.ScriptKind.TSX);let handoff;
 function visit(node){if(ts.isJsxAttribute(node)&&node.name.getText(ast)==='onRecorded')handoff=node.initializer.expression;ts.forEachChild(node,visit);}visit(ast);assert.ok(handoff);
 const onRecorded=new Function(...Object.keys(scope),'return '+ts.transpileModule('('+handoff.getText(ast)+')',{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText)(...Object.values(scope));
 const recorderSource=readFileSync(new URL('../mobile/src/components/CircleRecorder.tsx',import.meta.url),'utf8'),recorderAst=ts.createSourceFile('CircleRecorder.tsx',recorderSource,ts.ScriptTarget.Latest,true,ts.ScriptKind.TSX);let send;
 function find(node){if(ts.isArrowFunction(node)&&ts.isBlock(node.body)&&node.body.statements[0]?.getText(recorderAst)==='setSaving(true);')send=node;ts.forEachChild(node,find);}find(recorderAst);assert.ok(send);
 const recorderScope={uiText,clip:source,live,sending:ref(false),setSaving(){},setError(){},userError:String,onRecorded,Files:h.Files};
 const sendClip=new Function(...Object.keys(recorderScope),'return '+ts.transpileModule('('+send.getText(recorderAst)+')',{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText)(...Object.values(recorderScope));
 return{h,source,current,records,scope,live,generation,onRecorded,send:sendClip,closed:()=>closed,allowCopy:()=>{copyFailure=false;},allowMetadata:()=>{metadataFailure=false;},finishMetadata,waiting};
}
const settleMedia=async()=>{for(let i=0;i<5;i++)await new Promise(resolve=>setImmediate(resolve));};
function deferredTrayMutations(){
 const items=[{id:'a',uri:'file:///account-a/a.mp4'},{id:'b',uri:'file:///account-a/b.mp4'}],files=new Set(items.map(x=>x.uri)),writes=[],pending=[],errors=[],current=ref(items),currentCaption=ref('before'),generation=ref(1),live=ref(true);let record={items:structuredClone(items),caption:'before'},sequence=Promise.resolve(),display=items,caption='before';
 const scope={repository:{userId:user,set:(key,value)=>{assert.equal(key,'attachments:room');const captured=structuredClone(value);writes.push(captured);const task=sequence.then(()=>new Promise((resolve,reject)=>pending.push({finish:(failure)=>{if(failure)reject(Error('metadata full'));else{record=captured;resolve();}}})));sequence=task.catch(()=>{});return task;}},roomId:'room',caption,current,currentCaption,saveQueue:ref(Promise.resolve()),live,generation,setItems:next=>{display=next;},setCaption:value=>{caption=value;},setError:error=>{if(error)errors.push(String(error));},Files:{documentDirectory:'file:///documents/',cacheDirectory:'file:///cache/',copyAsync:async({to})=>{files.add(to);},deleteAsync:async uri=>{assert.ok(!record.items.some(item=>item.uri===uri),'file deletion must follow its durable metadata removal');files.delete(uri);}}};
 scope.save=componentAction('MediaTray','MediaTray','save',scope);
 const source=readFileSync(new URL('../mobile/src/components/MediaTray.tsx',import.meta.url),'utf8'),ast=ts.createSourceFile('MediaTray.tsx',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TSX);let remove,captionEdit;
 function hasSave(node){let found=false;function walk(n){if(ts.isCallExpression(n)&&n.expression.getText(ast)==='save')found=true;ts.forEachChild(n,walk);}walk(node);return found;}
 function visit(node){if(ts.isJsxAttribute(node)&&node.initializer&&ts.isJsxExpression(node.initializer)){const value=node.initializer.expression;if(node.name.getText(ast)==='onPress'&&value&&hasSave(value))remove=value;if(node.name.getText(ast)==='onChangeText'&&value&&hasSave(value))captionEdit=value;}ts.forEachChild(node,visit);}visit(ast);assert.ok(remove);assert.ok(captionEdit);
 const compile=(node,extra={})=>{const bound={...scope,...extra};return new Function(...Object.keys(bound),'return '+ts.transpileModule('('+node.getText(ast)+')',{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText)(...Object.values(bound));};
 const quality=componentAction('MediaTray','MediaTray','videoQuality',{...scope,stopped:ref(false),runMediaTask:loadNativeMedia().load('mediaLifecycle').runMediaTask,setBusy(){},setProgress(){},processVideo:()=>({promise:Promise.resolve({uri:'file:///cache/converted.mp4',width:480,height:720}),abort(){}}),xhr:ref(null),newId:()=> 'converted'});
 return{remove:id=>compile(remove,{item:items.find(x=>x.id===id)})(),caption:value=>compile(captionEdit)(value),quality:id=>quality(items.find(x=>x.id===id)),async finish(failure=false){await settleMedia();assert.ok(pending.length,'expected a pending real metadata mutation');pending.shift().finish(failure);await settleMedia();},files,writes,errors,current,currentCaption,generation,live,record:()=>record,display:()=>display,captionText:()=>caption};
}
test('overlapping Remove actions derive from the latest durable list before deleting either file',async()=>{
 const c=deferredTrayMutations();c.remove('a');await settleMedia();c.remove('b');await c.finish();await c.finish();assert.deepEqual(c.record().items,[]);assert.deepEqual(c.display(),[]);assert.deepEqual([...c.files],[]);assert.deepEqual(c.errors,[]);
});
for(const order of ['remove-first','caption-first'])test(`overlapping ${order} preserves the latest durable list and caption`,async()=>{
 const c=deferredTrayMutations();if(order==='remove-first'){c.remove('a');await settleMedia();c.caption('new caption');}else{c.caption('new caption');await settleMedia();c.remove('a');}await c.finish();await c.finish();assert.deepEqual(c.record().items.map(x=>x.id),['b'],order);assert.equal(c.record().caption,'new caption',order);assert.deepEqual(c.display().map(x=>x.id),['b']);assert.deepEqual([...c.files],['file:///account-a/b.mp4']);assert.deepEqual(c.errors,[]);
});
test('failed removal keeps its owned file and later caption plus removal retry commit from the retained state',async()=>{
 const c=deferredTrayMutations();c.remove('a');await settleMedia();c.caption('after failure');await c.finish(true);assert.equal(c.files.has('file:///account-a/a.mp4'),true);assert.deepEqual(c.display().map(x=>x.id),['a','b']);await c.finish();assert.equal(c.record().caption,'after failure');c.remove('a');await c.finish();assert.deepEqual(c.record().items.map(x=>x.id),['b']);assert.equal(c.record().caption,'after failure');assert.equal(c.files.has('file:///account-a/a.mp4'),false);assert.equal(c.errors.length,1);
});
test('queued old-account mutations cannot write or delete after generation replacement',async()=>{
 const c=deferredTrayMutations();c.remove('a');await settleMedia();c.remove('b');c.generation.current++;c.current.current=[{id:'replacement',uri:'file:///account-b/new.mp4'}];c.currentCaption.current='replacement caption';await c.finish();assert.equal(c.writes.length,1);assert.equal(c.files.has('file:///account-a/b.mp4'),true);assert.equal(c.current.current[0].id,'replacement');assert.equal(c.currentCaption.current,'replacement caption');assert.equal(c.errors.length,1);
});
test('starting video processing while removal persists cannot invalidate its committed list and resurrect a deleted file',async()=>{
 const c=deferredTrayMutations();c.remove('a');await settleMedia();const processing=c.quality('b');await settleMedia();await c.finish();await c.finish();await processing;assert.deepEqual(c.record().items.map(x=>x.id),['b']);assert.deepEqual(c.display().map(x=>x.id),['b']);assert.equal(c.files.has('file:///account-a/a.mp4'),false);assert.deepEqual(c.errors,[]);
});
test('circle copy failure preserves the source and preview; retry commits exactly one attachment',async()=>{
 const c=circleHandoff({copyFailure:true});c.send();await settleMedia();assert.equal(c.h.files.has(c.source),true);assert.equal(c.closed(),false);assert.equal(c.current.current.length,0);assert.equal(c.records.size,0);
 c.allowCopy();c.send();await settleMedia();assert.equal(c.h.files.has(c.source),false);assert.equal(c.closed(),true);assert.equal(c.current.current.length,1);assert.equal(c.records.get('attachments:room').items.length,1);assert.equal([...c.h.files.keys()].length,1);
});
test('circle metadata failure rolls back its private copy and UI, retains preview, and permits a duplicate-free retry',async()=>{
 const c=circleHandoff({metadataFailure:true});c.send();await settleMedia();assert.equal(c.h.files.has(c.source),true);assert.equal(c.closed(),false);assert.equal(c.current.current.length,0);assert.equal(c.records.size,0);assert.deepEqual([...c.h.files.keys()],[c.source]);
 c.allowMetadata();c.send();await settleMedia();assert.equal(c.records.get('attachments:room').items.length,1);assert.equal(c.current.current.length,1);assert.equal(c.closed(),true);assert.equal(c.h.files.has(c.source),false);assert.equal([...c.h.files.keys()].length,1);
});
test('circle handoff waits for durable metadata before closing preview or removing the original',async()=>{
 const c=circleHandoff({holdMetadata:true});c.send();await c.waiting;await settleMedia();assert.equal(c.closed(),false);assert.equal(c.h.files.has(c.source),true);assert.equal(c.current.current.length,0);c.finishMetadata();await settleMedia();assert.equal(c.closed(),true);assert.equal(c.h.files.has(c.source),false);assert.equal(c.records.get('attachments:room').items.length,1);
});
test('circle copy cancelled by account lifecycle rejects handoff and leaves no private orphan',async()=>{
 const c=circleHandoff();let copied,finish;const waiting=new Promise(r=>{copied=r;}),hold=new Promise(r=>{finish=r;});c.scope.Files.copyAsync=async({from,to})=>{c.h.files.set(to,c.h.files.get(from));copied();await hold;};
 const task=c.onRecorded(c.source),rejected=assert.rejects(task,/отмен|заверш|закрыт/i);await waiting;const cancelling=c.h.load('mediaLifecycle').cancelAccountMedia(user);finish();await rejected;await cancelling;assert.equal(c.closed(),false);assert.equal(c.records.size,0);assert.deepEqual([...c.h.files.keys()],[c.source]);
});
test('cancelled microphone preparation unloads the recorder and restores playback mode',async()=>{
 let prepared,finish,restored=0,unloaded=0,started=0;const waiting=new Promise(resolve=>{prepared=resolve;}),hold=new Promise(resolve=>{finish=resolve;}),mounted=ref(true),cancelled=ref(false),recording=ref(null);
 const start=componentAction('VoiceRecorder','VoiceRecorder','start',{starting:ref(false),latest:ref({disabled:false}),recording,held:ref(true),cancelled,lock:ref(false),mounted,setLocked(){},setPreparing(){},setError(){},setSeconds(){},Audio:{getPermissionsAsync:async()=>({granted:true}),RecordingOptionsPresets:{HIGH_QUALITY:{}},Recording:class{async prepareToRecordAsync(){prepared();await hold;}async stopAndUnloadAsync(){unloaded++;}async startAsync(){started++;}getURI(){return null;}setProgressUpdateInterval(){}setOnRecordingStatusUpdate(){}}},ensureRecordingMode:async()=>{},ensurePlaybackMode:async()=>{restored++;},samples:ref([]),stop:async()=>{},userError:String,mediaDetails:x=>x,Vibration:{vibrate(){}},setActive(){}});
 const task=start();await waiting;cancelled.current=true;mounted.current=false;finish();await task;assert.equal(unloaded,1);assert.equal(started,0);assert.equal(restored,1);
});
test('closing the photo editor during manipulation cannot apply a late result',async()=>{
 let finish,applied=0,closed=0;const result=new Promise(resolve=>{finish=resolve;}),generation=ref(1),live=ref(true);
 const apply=componentAction('PhotoEditor','PhotoEditor','apply',{photo:{uri:'file:///original.jpg'},crop:null,generation,live,setBusy(){},setError(){},Manipulator:{manipulateAsync:()=>result,SaveFormat:{JPEG:'jpeg'}},onApply:async()=>{applied++;},onClose:()=>{closed++;},close:()=>{closed++;},userError:String,Files:{deleteAsync:async()=>{}}});
 const pending=apply();generation.current++;live.current=false;finish({uri:'file:///late.jpg'});await pending;assert.equal(applied,0);assert.equal(closed,0);
});
test('cancelling video quality while copying cannot publish the old conversion',async()=>{
 let copyStarted,finishCopy,saved=0;const waiting=new Promise(resolve=>{copyStarted=resolve;}),copy=new Promise(resolve=>{finishCopy=resolve;}),stopped=ref(false),generation=ref(1),live=ref(true),current=ref([{id:'item'}]);
 const quality=componentAction('MediaTray','MediaTray','videoQuality',{runMediaTask:loadNativeMedia().load('mediaLifecycle').runMediaTask,repository:{userId:user},saveQueue:ref(Promise.resolve()),stopped,generation,live,current,xhr:ref(null),setBusy(){},setError(){},setProgress(){},processVideo:()=>({promise:Promise.resolve({uri:'file:///cache/result.mp4',width:480,height:720}),abort(){}}),newId:()=> 'converted',Files:{documentDirectory:'file:///documents/',cacheDirectory:'file:///cache/',copyAsync:async()=>{copyStarted();await copy;},deleteAsync:async()=>{}},save:()=>{saved++;}});
 const pending=quality({id:'item',uri:'file:///original.mp4'});await waiting;stopped.current=true;generation.current++;live.current=false;finishCopy();await pending;assert.equal(saved,0);
});
test('an active call owns the audio route across late recorder cleanup and playback requests',async()=>{
 const source=readFileSync(new URL('../mobile/src/audioSession.ts',import.meta.url),'utf8'),exports={},applied=[];
 new Function('exports','require',ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText)(exports,dep=>dep==='expo-av'?{Audio:{setAudioModeAsync:async mode=>{applied.push(mode);}}}:dep==='./playbackOwnership'?{setMediaCallActive:async()=>{},pauseAllMedia:async()=>{}}:{});
 await exports.beginCall();await exports.ensurePlaybackMode();assert.equal(exports.currentPhase(),'call');assert.equal(applied.at(-1).playThroughEarpieceAndroid,true);await assert.rejects(()=>exports.ensureRecordingMode(),/звон/i);
 await exports.endCall();assert.equal(exports.currentPhase(),'playback');
});
test('native video control events claim playback and calls pause every owner including PiP',async()=>{
 const source=readFileSync(new URL('../mobile/src/playbackOwnership.ts',import.meta.url),'utf8'),exports={};new Function('exports',ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText)(exports);
 function player(){let playing=false,paused=0,listener;return {get paused(){return paused;},get playing(){return playing;},pause(){paused++;playing=false;listener?.({isPlaying:false});},addListener(name,fn){assert.equal(name,'playingChange');listener=fn;return {remove(){listener=null;}};},play(){playing=true;listener?.({isPlaying:true});}};}
 const first=player(),second=player(),a=exports.ownVideoPlayer(first),b=exports.ownVideoPlayer(second);first.play();await new Promise(resolve=>setImmediate(resolve));second.play();await new Promise(resolve=>setImmediate(resolve));assert.equal(first.playing,false);assert.equal(second.playing,true);
 await exports.setMediaCallActive(true);assert.equal(second.playing,false);second.play();await new Promise(resolve=>setImmediate(resolve));assert.equal(second.playing,false);a.dispose();b.dispose();
});
test('closed viewer cannot save to the gallery after its permission dialog resolves',async()=>{
 const h=loadNativeMedia(),generation=ref(1),mounted=ref(true);let allow,saved=0;const permission=new Promise(resolve=>{allow=resolve;});
 const save=componentAction('MediaViewer','MediaViewer','save',{source:{uri:'file:///private.jpg',mime:'image/jpeg'},saving:false,generation,mounted,userId:user,runMediaTask:h.load('mediaLifecycle').runMediaTask,setSaving(){},setError(){},setNotice(){},MediaLibrary:{requestPermissionsAsync:()=>permission,saveToLibraryAsync:async()=>{saved++;}},Files:h.Files,headers:{},userError:String});
 const pending=save();await new Promise(resolve=>setImmediate(resolve));generation.current++;mounted.current=false;allow({granted:true});await pending;assert.equal(saved,0);
});
test('a slow voice load cannot steal playback from the newer voice player',async()=>{
 const ownership=loadNativeMedia().load('playbackOwnership'),loads=[];
 function voice(){const generation=ref(0),sound=ref(null),owner=ref(null),stats={played:0,unloaded:0};owner.current=ownership.registerPlayback(async()=>{generation.current++;await sound.current?.pauseAsync();});
 const instance={getStatusAsync:async()=>({isLoaded:true,positionMillis:0,durationMillis:100}),playAsync:async()=>{stats.played++;},pauseAsync:async()=>{},unloadAsync:async()=>{stats.unloaded++;}};
 const toggle=componentAction('VoicePlayer','VoicePlayer','toggle',{busy:ref(false),generation,sound,owner,playing:false,setError(){},uri:'file:///voice.m4a',headers:{},rate:1,ensurePlaybackMode:async()=>{},update(){},Audio:{Sound:{createAsync:async(source,options)=>{assert.equal(options.shouldPlay,false);return new Promise(resolve=>loads.push(()=>resolve({sound:instance})));}}}});return{toggle,stats,owner};}
 const a=voice(),b=voice(),first=a.toggle();await new Promise(resolve=>setImmediate(resolve));const second=b.toggle();await new Promise(resolve=>setImmediate(resolve));assert.equal(loads.length,2);loads[1]();await second;loads[0]();await first;assert.equal(a.stats.played,0);assert.equal(a.stats.unloaded,1);assert.equal(b.stats.played,1);a.owner.current.dispose();b.owner.current.dispose();
});
test('account erasure stops and drains a native circle recording before deleting its late result',async()=>{
 const h=loadNativeMedia(),lifecycle=h.load('mediaLifecycle'),media=h.load('decryptedMedia');let finish,recordingStarted;const ready=new Promise(resolve=>{recordingStarted=resolve;}),cancelled=ref(false),live=ref(true),held=ref(false),actions=ref(null),uri='file:///cache/circle-private.mp4';
 actions.current={stop:()=>{cancelled.current=true;held.current=false;}};
 const begin=componentAction('CircleRecorder','CircleRecorder','begin',{working:ref(false),ready:true,permission:{granted:true},microphone:{granted:true},held,lock:ref(false),cancelled,live,setLocked(){},setRecording(){},setSeconds(){},setError(){},setClip(){assert.fail('cancelled circle published');},userId:user,runMediaTask:lifecycle.runMediaTask,actions,camera:ref({recordAsync:async()=>{recordingStarted();return new Promise(resolve=>{finish=()=>{h.files.set(uri,'private video');resolve({uri});};});}}),ensureRecordingMode:async()=>{},ensurePlaybackMode:async()=>{},Files:h.Files,userError:String});
 const task=begin();await ready;const erase=media.eraseDecryptedMedia(user);finish();await task;await erase;assert.equal(h.files.has(uri),false);
});
test('erasure during Android content URI resolution cannot launch another app with old account data',async()=>{
 const h=loadNativeMedia(),media=h.load('mediaFiles');let finish,ready;const waiting=new Promise(resolve=>{ready=resolve;});h.Files.getContentUriAsync=async()=>{ready();return new Promise(resolve=>{finish=()=>resolve('content://private');});};
 const opening=media.openMediaFile('https://self.test/api/media/file',{},'application/pdf',user),rejected=assert.rejects(opening,/отмен|удал|заверш/i);await waiting;const erase=h.load('decryptedMedia').eraseDecryptedMedia(user);finish();await rejected;await erase;assert.equal(h.actions.length,0);assert.equal([...h.files.keys()].some(key=>key.includes(user)),false);
});
test('closing a viewer while Android resolves a content URI cannot launch a late file action',async()=>{
 const h=loadNativeMedia(),media=h.load('mediaFiles');let finish,ready,current=true;const waiting=new Promise(resolve=>{ready=resolve;});h.Files.getContentUriAsync=async()=>{ready();return new Promise(resolve=>{finish=()=>resolve('content://private');});};
 const opening=media.openMediaFile('https://self.test/api/media/file',{},'application/pdf',user,false,()=>current),rejected=assert.rejects(opening,/закрыт|отмен/i);await waiting;current=false;finish();await rejected;assert.equal(h.actions.length,0);
});
test('account erasure waits for native microphone preparation and its unload cleanup',async()=>{
 const h=loadNativeMedia(),media=h.load('decryptedMedia'),generation=ref(0),recording=ref(null);let finish,prepared,unloaded=0;const ready=new Promise(resolve=>{prepared=resolve;}),hold=new Promise(resolve=>{finish=resolve;}),uri='file:///cache/native-voice.m4a';
 const start=componentAction('VoiceRecorder','VoiceRecorder','start',{starting:ref(false),latest:ref({disabled:false,repository:{userId:user}}),recording,held:ref(true),cancelled:ref(false),lock:ref(false),mounted:ref(true),setLocked(){},setPreparing(){},setError(){},setSeconds(){},Audio:{getPermissionsAsync:async()=>({granted:true}),RecordingOptionsPresets:{HIGH_QUALITY:{}},Recording:class{async prepareToRecordAsync(){prepared();await hold;h.files.set(uri,'temporary');}async stopAndUnloadAsync(){unloaded++;}getURI(){return uri;}async startAsync(){assert.fail('cancelled preparation started');}setProgressUpdateInterval(){}setOnRecordingStatusUpdate(){}}},runMediaTask:h.load('mediaLifecycle').runMediaTask,ensureRecordingMode:async()=>{},ensurePlaybackMode:async()=>{},samples:ref([]),stop:async()=>{},userError:String,mediaDetails:x=>x,Vibration:{vibrate(){}},setActive(){},Files:h.Files});
 const task=start();await ready;let erased=false;const erase=media.eraseDecryptedMedia(user).then(()=>{erased=true;});await new Promise(resolve=>setImmediate(resolve));assert.equal(erased,false);finish();await task;await erase;assert.equal(unloaded,1);assert.equal(h.files.has(uri),false);
});
