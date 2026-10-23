import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {renderMessengerShell,renderRoomRow} from '../web/views.js';
import {t,setLanguage} from '../web/i18n.js';
import {selectBrowserAccount} from '../web/browser-accounts.js';
import {accountCookieName,readSessionToken} from '../api/browser-accounts.js';
const a='00000000-0000-4000-8000-000000000001',b='00000000-0000-4000-8000-000000000002';
const state=darkTheme=>({me:{id:a,username:'alice'},rooms:[],roomMembers:[],settingsOpen:true,darkTheme,fontSize:16,folders:[],searchQ:'',peopleResults:[]});
test('logged-in settings expose Accounts in RU/EN and either theme and route to the existing switcher',async()=>{
 const source=readFileSync(new URL('../web/app.js',import.meta.url),'utf8'),start=source.indexOf('window.openBrowserAccounts=async()=>'),end=source.indexOf('const authenticate =',start),buttons=[],events=[];
 const scope={window:{},uiText:t,webActionDialog:()=>({dialog:{close:()=>events.push('closed')},content:{},status:{}}),api:async path=>{assert.equal(path,'/auth/accounts');return{accounts:[{id:a,username:'alice'},{id:b,username:'bob'}]};},dialogButton:(_content,label,action)=>buttons.push({label,action}),switchBrowserAccount:async id=>events.push(id),addBrowserAccount:async()=>events.push('add'),userError:String};
 vm.runInNewContext(source.slice(start,end),scope);
 try{for(const lang of ['ru','en'])for(const dark of [false,true]){setLanguage(lang);buttons.length=0;events.length=0;const markup=renderMessengerShell(state(dark),()=>''),entry=[...markup.matchAll(/<button\b[^>]*onclick="([^"]+)"[^>]*>([\s\S]*?)<\/button>/g)].find(m=>m[2].replace(/<[^>]*>/g,'')===t('Аккаунты'));
  assert.ok(entry,`${lang}/${dark?'dark':'light'}: logged-in Accounts entry`);await vm.runInNewContext(entry[1],{...scope.window});assert.deepEqual(buttons.map(x=>x.label),['alice','bob',t('Добавить аккаунт')]);await buttons[1].action();await buttons[2].action();await buttons[0].action();assert.deepEqual(events,['closed',b,'closed','add','closed',a]);
 }}finally{setLanguage('ru');}
});
test('private group row and active header capture selected-account avatar URLs despite another tab legacy cookie',()=>{
 const previous=Object.getOwnPropertyDescriptor(globalThis,'navigator');Object.defineProperty(globalThis,'navigator',{configurable:true,value:{onLine:true}});
 try{selectBrowserAccount(a);const room={id:'private-group',kind:'group',title:'<Private & group>',avatar_id:'private/avatar?"'};
  const row=renderRoomRow(room,{id:a}),shell=renderMessengerShell({...state(false),rooms:[room],activeRoomId:room.id,settingsOpen:false,wsAuthed:true},()=>'');
  const requests=[row,shell.slice(shell.indexOf('<main'))].map(markup=>{assert.ok(markup.includes('&lt;Private &amp; group&gt;'));const src=markup.match(/<img src="([^"]+)"/)[1].replaceAll('&amp;','&');return new URL(src,'https://self.test');});
  selectBrowserAccount(b);for(const url of requests){assert.equal(url.pathname,'/api/media/private%2Favatar%3F%22');assert.equal(url.searchParams.get('account_id'),a);assert.equal(readSessionToken({headers:{},query:Object.fromEntries(url.searchParams),cookies:{token:'session-b',[accountCookieName(a)]:'session-a',[accountCookieName(b)]:'session-b'}}),'session-a');}
 }finally{selectBrowserAccount(null);if(previous)Object.defineProperty(globalThis,'navigator',previous);else delete globalThis.navigator;}
});
