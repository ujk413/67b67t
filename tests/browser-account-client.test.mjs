import {test} from 'node:test';import assert from 'node:assert/strict';
import {selectBrowserAccount,selectedBrowserAccount,browserAccountHeaders,browserSessionGeneration,invalidateBrowserSession,accountMediaUrl,notificationTarget,initialBrowserAccount} from '../web/browser-accounts.js';
test('push routing and cold start select only explicit account identifiers, preserving the message target',()=>{
 const account='00000000-0000-4000-8000-000000000001',room='00000000-0000-4000-8000-000000000002',message='00000000-0000-4000-8000-000000000003';
 assert.deepEqual(notificationTarget({recipientId:account,roomId:room,messageId:message}),{accountId:account,roomId:room,messageId:message});
 assert.equal(initialBrowserAccount('https://67chat.ru/?account_id='+account),account);
 assert.equal(initialBrowserAccount('https://67chat.ru/?account_id=none'),null);
 assert.equal(notificationTarget({recipientId:'bad',roomId:room}),null);
 assert.equal(notificationTarget({recipientId:account,roomId:'bad'}),null);
 assert.throws(()=>selectBrowserAccount('-'.repeat(36)));
});
test('per-tab selection stores only an account ID, scopes media and invalidates pending old-account work',()=>{
 const values=new Map();globalThis.sessionStorage={getItem:k=>values.get(k),setItem:(k,v)=>values.set(k,v),removeItem:k=>values.delete(k)};
 const a='00000000-0000-4000-8000-000000000001',b='00000000-0000-4000-8000-000000000002';selectBrowserAccount(a);const generation=browserSessionGeneration();assert.equal(selectedBrowserAccount(),a);assert.deepEqual(browserAccountHeaders(),{'X-Account-Id':a});assert.equal(accountMediaUrl('/api/media/file?existing=1'),'/api/media/file?existing=1&account_id='+a);
 selectBrowserAccount(b);assert.notEqual(browserSessionGeneration(),generation);assert.ok(!JSON.stringify([...values]).includes('token'));assert.equal(accountMediaUrl('blob:private'),'blob:private');invalidateBrowserSession();selectBrowserAccount('none');assert.equal(browserAccountHeaders()['X-Account-Id'],'none');assert.equal(accountMediaUrl('/api/media/file'),'/api/media/file');selectBrowserAccount(null);
});
