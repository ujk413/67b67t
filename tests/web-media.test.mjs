import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createMediaController} from '../web/media.js';
import {webcrypto} from 'node:crypto';
class Element {
 constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.textContent='';this.value='';}
 append(...items){this.children.push(...items);for(const item of items)if(item && typeof item==='object')item.parent=this;}
 replaceChildren(...items){this.children=[];this.append(...items);}
 setAttribute(name,value){this[name]=value;}
 addEventListener(){} showModal(){this.open=true;} close(){this.open=false;}
 remove(){if(this.parent)this.parent.children=this.parent.children.filter(x=>x!==this);}
}
const all=node=>[node,...node.children.flatMap(all)];
const tick=()=>new Promise(resolve=>setImmediate(resolve));
test('voice and circles preview before sending, encrypt before upload and keep the original recipient',async()=>{
 const old={document:globalThis.document,window:globalThis.window,XMLHttpRequest:globalThis.XMLHttpRequest,MediaRecorder:globalThis.MediaRecorder,navigator:Object.getOwnPropertyDescriptor(globalThis,'navigator'),crypto:Object.getOwnPropertyDescriptor(globalThis,'crypto'),now:Date.now};
 let clock=1000;Date.now=()=>clock;
 const body=new Element('body'),recorders=[],uploads=[];
 class Recorder {static isTypeSupported(){return true;}constructor(){recorders.push(this);this.mimeType='audio/mp4';this.state='inactive';}start(){this.state='recording';}stop(){this.state='inactive';void this.onstop();}}
 class XHR {upload={};open(method,url){this.url=url;}send(form){uploads.push({url:this.url,file:form.get('file')});this.status=200;this.responseText='{"id":"uploaded"}';queueMicrotask(()=>this.onload());}abort(){this.onabort?.();}}
 globalThis.document={body,createElement:tag=>new Element(tag)};globalThis.window={MediaRecorder:Recorder};globalThis.MediaRecorder=Recorder;globalThis.XMLHttpRequest=XHR;
 Object.defineProperty(globalThis,'navigator',{configurable:true,value:{mediaDevices:{getUserMedia:async()=>({getTracks:()=>[{stop(){}}]})}}});Object.defineProperty(globalThis,'crypto',{configurable:true,value:webcrypto});
 try {
 for(const kind of ['voice','circle']) {
  const values=new Map(),blobs=new Map(),outgoing=[],encrypted=[];let room='original';
  const store={get:async k=>values.get(k),set:async(k,v)=>values.set(k,v),remove:async k=>{values.delete(k);blobs.delete(k);},getBlob:async k=>blobs.get(k),setBlob:async(k,v)=>blobs.set(k,v)};
  const controller=createMediaController({context:()=>({userId:'qa',roomId:room,title:'QA',store}),enqueue:async item=>outgoing.push(item),report:error=>{throw error;},encryptFile:async(file,recipient)=>{encrypted.push({file,recipient});return{file:new File(['encrypted'],'attachment.bin'),kind:'file',meta:{media_encryption:{key_message:{text:'wrapped-key'}}}};}});
  await controller.compose(kind);const click=label=>{const node=all(body).find(n=>n.tagName==='BUTTON'&&n.textContent===label);assert.ok(node);assert.ok(!node.disabled);node.onclick();};
  click('Начать запись');await tick();const recorder=recorders.at(-1);clock+=1500;recorder.ondataavailable({data:new Blob(['synthetic recording'],{type:'audio/mp4'})});click('Остановить');await tick();assert.equal(outgoing.length,0);assert.equal(blobs.size,1);
  room='different';click('Отправить');await tick();await tick();assert.equal(encrypted.length,1);assert.equal(encrypted[0].recipient,'original');assert.equal(outgoing[0].roomId,'original');assert.ok(outgoing[0].body.meta.media_encryption);assert.equal(uploads.length,0,'confirmation must enqueue without network');assert.equal(outgoing[0].upload.file.name,'attachment.bin');assert.equal(outgoing[0].upload.kind,'file');assert.equal(blobs.size,0);controller.stop();
 }
 const values=new Map(),blobs=new Map(),store={get:async k=>values.get(k),set:async(k,v)=>values.set(k,v),remove:async k=>{values.delete(k);blobs.delete(k);},getBlob:async k=>blobs.get(k),setBlob:async(k,v)=>blobs.set(k,v)};
 const controller=createMediaController({context:()=>({userId:'qa',roomId:'files',title:'QA',store}),enqueue:async()=>{},report:error=>{throw error;},encryptFile:async()=>{throw Error('Should not upload before confirmation');}});
 await controller.compose('files',[new File(['synthetic file'],'saved.txt',{type:'text/plain',lastModified:1})]);assert.equal(blobs.size,1);controller.close();await controller.compose('files');assert.ok(all(body).some(n=>n.textContent.startsWith('saved.txt')));controller.stop();
 }finally {Date.now=old.now;for(const key of ['document','window','XMLHttpRequest','MediaRecorder'])globalThis[key]=old[key];for(const key of ['navigator','crypto']){if(old[key])Object.defineProperty(globalThis,key,old[key]);else delete globalThis[key];}}
});
