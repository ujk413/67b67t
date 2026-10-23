import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {createRequire} from 'node:module';
import {t as uiText} from '../web/i18n.js';
import {accountMediaUrl} from '../web/browser-accounts.js';
import {createSessionScope} from '../web/session-scope.js';
const require=createRequire(new URL('../mobile/package.json',import.meta.url)),ts=require('typescript');
function callback(path,name,scope,nested=false){const text=fs.readFileSync(new URL('../'+path,import.meta.url),'utf8'),tree=ts.createSourceFile(path,text,ts.ScriptTarget.Latest,true,path.endsWith('tsx')?ts.ScriptKind.TSX:ts.ScriptKind.JS);const nodes=nested?tree.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='App').body.statements:tree.statements,fn=nodes.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text===name);assert.ok(fn,name);return vm.runInNewContext(ts.transpileModule('('+fn.getText(tree)+')',{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText,{uiText,accountMediaUrl,captureWebSession:()=>createSessionScope({account:scope.state?.me?.id,generation:0,currentAccount:()=>scope.state?.me?.id,currentGeneration:()=>0}),...scope});}
const asset={media_id:'00000000-0000-4000-8000-000000000001',kind:'sticker',mime:'image/webp',label:'Private sticker label'};
test('native catalog delivery copies the source into the account queue and never shares its private catalog media ID',async()=>{
 const repo={userId:'account'},sent=[],deleted=[],tokenRef={current:'token'},local={repository:{current:repo},send:async(...args)=>sent.push(args)},files={documentDirectory:'file:/private/',makeDirectoryAsync:async()=>{},downloadAsync:async()=>({status:200}),getInfoAsync:async()=>({exists:true,isDirectory:false,size:100}),deleteAsync:async uri=>deleted.push(uri)};
 const send=callback('mobile/App.tsx','sendCatalogAsset',{token:'token',tokenRef,baseUrl:'https://server/api',local,FileSystem:files,newId:()=> 'a'.repeat(32),mediaUrl:(base,id)=>base+'/media/'+id},true);
 await send('room',asset);assert.equal(sent.length,1);assert.equal(sent[0][2],'room');assert.equal(sent[0][1].media_id,undefined);assert.equal(sent[0][1].__upload.details.expressive.label,asset.label);assert.equal(deleted.length,0);
 files.downloadAsync=async()=>{tokenRef.current='different-account';return{status:200};};await assert.rejects(()=>send('room',asset),/Сеанс изменился/);assert.equal(sent.length,1);assert.equal(deleted.length,1);
});
test('web catalog delivery encrypts a private asset before the normal durable upload queue and cancels after account switch',async()=>{
 const state={me:{id:'owner'}},sent=[],protectedCalls=[];let switchDuringDownload=false;
 const send=callback('web/app.js','sendWebCatalogAsset',{state,fetch:async()=>{if(switchDuringDownload)state.me={id:'another'};return{ok:true,blob:async()=>new Blob(['asset'],{type:'image/webp'})};},AbortSignal,File,Blob,encodeURIComponent,encryptWebFile:async(file,room,details)=>{protectedCalls.push({file,room,details});return{file:new File(['ciphertext'],'attachment.bin'),kind:'file',meta:{media_encryption:{key_message:{encrypted_content:'protected'}}}};},chatRuntime:{enqueue:async item=>sent.push(item)}});
 await send('room',asset);assert.equal(sent.length,1);assert.equal(sent[0].body.media_id,undefined);assert.equal(sent[0].upload.file.name,'attachment.bin');assert.equal(protectedCalls[0].details.expressive.label,asset.label);
 switchDuringDownload=true;await assert.rejects(()=>send('room',asset),/Сеанс изменился/);assert.equal(sent.length,1);assert.equal(protectedCalls.length,1);
});
