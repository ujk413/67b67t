import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {createRequire} from 'node:module';
import * as i18n from '../web/i18n.js';
import {renderLogin,renderRoomRow} from '../web/views.js';
import {formatConversationText} from '../web/conversation-features.js';
import {t,setLanguage,getLanguage,locale,subscribeLanguage} from '../web/i18n.js';
test('explicit UI translations preserve dynamic message and username parameters',()=>{
 setLanguage('ru');assert.equal(t('Отмена'),'Отмена');let changes=0;const off=subscribeLanguage(()=>changes++);
 setLanguage('en');assert.equal(getLanguage(),'en');assert.equal(t('Отмена'),'Cancel');assert.equal(locale(),'en-US');
 const payload='Маша: ничего не переводить <script>';assert.equal(t('Черновик: {0}',payload),'Draft: '+payload);assert.equal(t('{0}',payload),payload);
 setLanguage('en');assert.equal(changes,1);off();setLanguage('ru');assert.equal(locale(),'ru-RU');
});
test('date formatting follows the chosen locale without changing stored timestamps',()=>{
 const timestamp='2026-10-23T12:34:00Z',date=new Date(timestamp);
 setLanguage('ru');const ru=new Intl.DateTimeFormat(locale(),{timeZone:'UTC',day:'numeric',month:'long'}).format(date);
 setLanguage('en');const en=new Intl.DateTimeFormat(locale(),{timeZone:'UTC',day:'numeric',month:'long'}).format(date);
 assert.match(ru,/октября/);assert.match(en,/October/);assert.equal(date.toISOString(),'2026-10-23T12:34:00.000Z');setLanguage('ru');
});
test('actual native formatters translate labels and dates while preserving message and profile contents',()=>{
 const require=createRequire(import.meta.url),ts=require('../mobile/node_modules/typescript'),exports={};
 vm.runInNewContext(ts.transpileModule(fs.readFileSync(new URL('../mobile/src/format.ts',import.meta.url),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText,{exports,Date,require:name=>{assert.match(name,/i18n\.js$/);return i18n;}});
 setLanguage('en');assert.equal(exports.displayName({display_name:'Избранное'}),'Избранное');assert.equal(exports.roomTitle({kind:'dm',other_display_name:'Пользователь'}),'Пользователь');assert.equal(exports.roomPreview({last_msg_text:'Отмена',last_msg_sender_id:'me'},'me'),'You: Отмена');assert.equal(exports.participantsLabel(3),'3 members');
 assert.equal(exports.dayLabel('2026-10-22T12:00:00Z',new Date('2026-10-23T12:00:00Z')),'Yesterday');assert.match(exports.dayLabel('2026-10-10T12:00:00Z',new Date('2026-10-23T12:00:00Z')),/October/);setLanguage('ru');
});
test('native error presentation translates known UI errors and never exposes unknown technical English',()=>{
 const require=createRequire(import.meta.url),ts=require('../mobile/node_modules/typescript'),exports={};
 vm.runInNewContext(ts.transpileModule(fs.readFileSync(new URL('../mobile/src/errors.ts',import.meta.url),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText,{exports,Error,require:()=>i18n});
 setLanguage('en');assert.equal(exports.userError(Error('Не удалось загрузить файл')),'Could not download the file');
 setLanguage('ru');assert.equal(exports.userError(Error('SQL failed bearer secret')),'Не удалось выполнить действие. Повторите попытку.');
});
test('actual web login renders English and chat rows retain Cyrillic user content verbatim',()=>{
 setLanguage('en');const login=renderLogin();assert.match(login,/Sign in to 67/);assert.doesNotMatch(login,/[А-Яа-яЁё]/);
 const room={id:'room',kind:'dm',other_display_name:'Избранное',other_username:'Пользователь',last_msg_text:'Отмена',last_msg_type:'text',last_msg_sender_id:'other',created_at:'2026-10-23T12:00:00Z'};
 const row=renderRoomRow(room,{id:'me'});assert.match(row,/Избранное/);assert.match(row,/Отмена/);setLanguage('ru');assert.match(renderLogin(),/Вход в 67/);
 setLanguage('en');assert.match(formatConversationText('||Отмена||'),/<summary>Spoiler<\/summary>Отмена/);setLanguage('ru');
});

test('native language bridge updates background notifications and widget without depending on a new installed binary',()=>{
 const require=createRequire(import.meta.url),ts=require('../mobile/node_modules/typescript'),exports={},calls=[];
 vm.runInNewContext(ts.transpileModule(fs.readFileSync(new URL('../mobile/src/nativeUiLanguage.ts',import.meta.url),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText,{exports,require:name=>{assert.equal(name,'expo');return{requireOptionalNativeModule:name=>({setUiLanguage:language=>calls.push([name,language])})};}});
 exports.setNativeUiLanguage('en');assert.deepEqual(calls,[['RoomKey','en'],['ExpressiveIntegration','en']]);
 const older={};vm.runInNewContext(ts.transpileModule(fs.readFileSync(new URL('../mobile/src/nativeUiLanguage.ts',import.meta.url),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText,{exports:older,require:()=>({requireOptionalNativeModule:()=>null})});assert.doesNotThrow(()=>older.setNativeUiLanguage('ru'));
});
