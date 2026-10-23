import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {createRequire} from 'node:module';
import {openConversationFeature} from '../web/conversation-features.js';
import {t as uiText,setLanguage} from '../web/i18n.js';

test('selecting the server draft never mistakes actual message text for the empty-state label',async()=>{
 const original=globalThis.document,nodes=[];
 class Element{constructor(tag){this.tag=tag;this.style={};this.listeners={};nodes.push(this);}append(){}replaceChildren(){}addEventListener(name,fn){this.listeners[name]=fn;}showModal(){}remove(){}close(){}}
 globalThis.document={createElement:tag=>new Element(tag),body:new Element('body')};
 try{for(const language of ['ru','en']){setLanguage(language);nodes.length=0;const text=uiText('На сервере пусто'),writes=[];
  await openConversationFeature('drafts',{me:{id:'alice'},room:{id:'room'},local:{get:async()=>'',set:async()=>{}},request:async()=>({revision:1,encrypted_content:'encrypted'}),decode:async()=>({text}),setDraft:async(_id,value)=>writes.push(value)});
  await nodes.find(node=>node.tag==='button'&&node.textContent===uiText('Загрузить серверный в редактор')).listeners.click();
  assert.deepEqual(writes,[text]);
 }}finally{globalThis.document=original;setLanguage('ru');}
});

test('native forwarding completion survives changing the interface language before retry',async()=>{
 const require=createRequire(import.meta.url),ts=require('../mobile/node_modules/typescript'),source=fs.readFileSync(new URL('../mobile/src/screens/Workspace.tsx',import.meta.url),'utf8'),ast=ts.createSourceFile('Workspace.tsx',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TSX);let callback;
 function visit(node){if(ts.isVariableDeclaration(node)&&node.name.getText(ast)==='forward')callback=node.initializer;ts.forEachChild(node,visit);}visit(ast);assert.ok(callback);
 const sends=[],completedForwardRooms={current:new Set()},scope={uiText,selected:['room'],forwardComment:'',forwardResults:{},completedForwardRooms,p:{forwardOperation:'operation',rooms:[{id:'room'}],forwarding:[{id:'message'}],enqueueForward:async room=>sends.push(room.id)},setForwardResults:update=>{scope.forwardResults=update(scope.forwardResults);}};
 const forward=vm.runInNewContext(ts.transpileModule('('+callback.getText(ast)+')',{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText,scope);
 try{setLanguage('ru');await forward();setLanguage('en');await forward();assert.deepEqual(sends,['room']);}finally{setLanguage('ru');}
});

test('web forwarding never treats a displayed error as successful delivery',async()=>{
 const source=fs.readFileSync(new URL('../web/app.js',import.meta.url),'utf8'),start=source.indexOf('window.forwardToMany='),end=source.indexOf('window.openSharedMedia',start),nodes=[],buttons=new Map();
 class Element{append(){}close(){}}
 const selectedMessages=new Set(['message']),scope={uiText,window:{},crypto,Set,state:{me:{id:'alice'},messages:[{id:'message',type:'text',_text:'Отмена'}],rooms:[{id:'room'}],selectedMessages},document:{createElement:tag=>{const node=new Element();node.tag=tag;nodes.push(node);return node;},createTextNode:()=>new Element()},webActionDialog:()=>({dialog:new Element(),content:new Element(),status:new Element()}),dialogButton:(_parent,title,action)=>buttons.set(title,action),chatRuntime:{enqueue:async()=>{throw Error('offline');}},isExpressiveControl:()=>false,parseMessagePayload:()=>null,userError:()=>uiText('добавлено в очередь'),mountPreservingInput:async()=>{}};
 vm.runInNewContext(source.slice(start,end),scope);scope.window.forwardToMany(['message']);const check=nodes.find(node=>node.type==='checkbox');check.checked=true;check.onchange();await buttons.get(uiText('Переслать выбранным'))();assert.equal(selectedMessages.size,1);
});
