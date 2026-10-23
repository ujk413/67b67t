import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {SigStore} from '../web/keystore.js';
const require=createRequire(new URL('../api/package.json',import.meta.url)),fake=require('fake-indexeddb');
globalThis.indexedDB=fake.indexedDB;globalThis.IDBKeyRange=fake.IDBKeyRange;
test('account deletion removes Signal identities, sessions, backup and group keys without touching another account',async()=>{
 const user=crypto.randomUUID(),scope=SigStore.forAccount(user),other=SigStore.forAccount('unrelated');
 await scope.put('identityKey',{secret:'remove'});await scope.put('session:peer','remove');await other.put('identityKey',{secret:'keep'});
 await SigStore.put('backup:'+user,{private:'remove'});await SigStore.put('gk:'+user+':room:own','remove');
 const late=Promise.allSettled([scope.put('late','remove')]);await SigStore.eraseAccount(user);await late;
 await assert.rejects(()=>scope.put('identityKey','resurrect'),/удалён/);await assert.rejects(()=>scope.get('identityKey'),/удалён/);
 const keys=await SigStore.keys();assert.ok(!keys.some(key=>key.startsWith('signal:'+user+':')||key.startsWith('gk:'+user+':')||key==='backup:'+user));assert.ok(keys.includes('erased:'+user));assert.deepEqual(await other.get('identityKey'),{secret:'keep'});
});
test('durable erasure prevents another tab from recreating Signal keys or plaintext history',async()=>{
 const user=crypto.randomUUID(),tabA=await import('../web/keystore.js?tabA'),tabB=await import('../web/keystore.js?tabB');
 await tabB.SigStore.forAccount(user).put('identityKey','private');await tabA.SigStore.eraseAccount(user);await assert.rejects(()=>tabB.SigStore.forAccount(user).put('identityKey','resurrect'),/удалён/);
 const localA=await import('../web/local-data.js?tabA'),localB=await import('../web/local-data.js?tabB');const stale=localB.createAccountStore(user);await stale.set('draft:room','private');await localA.eraseAccountStore(user);await assert.rejects(()=>stale.set('draft:room','resurrect'),/удалён/);await assert.rejects(()=>stale.get('draft:room'),/удалён/);
});
