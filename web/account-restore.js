import {createWebDeviceProof,bytesToBase64,base64ToBytes} from './account-features.js';
import {SigStore} from './keystore.js';

// All staging uses a memory store. A single IDB transaction installs the entire
// validated account archive and its recovery marker after proven publication.
const journal=user=>'sig:restore:'+user;
const accountPrefix=user=>'sig:signal:'+user+':';
const groupPrefix=user=>'sig:gk:'+user+':';
const open=()=>new Promise((resolve,reject)=>{const r=indexedDB.open('e2e-keys',1);r.onupgradeneeded=()=>{if(!r.result.objectStoreNames.contains('records'))r.result.createObjectStore('records',{keyPath:'id'});};r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});
async function transaction(user,run){const db=await open();try{return await new Promise((resolve,reject)=>{const tx=db.transaction('records','readwrite'),store=tx.objectStore('records'),erased=store.get('sig:erased:'+user);let failure=null;erased.onsuccess=()=>{if(erased.result?.value===true){failure=Error('Аккаунт удалён с этого устройства');tx.abort();return;}try{run(store);}catch(error){failure=error;tx.abort();}};tx.oncomplete=resolve;tx.onerror=()=>reject(failure||tx.error);tx.onabort=()=>reject(failure||tx.error||Error('Не удалось сохранить ключи'));});}finally{db.close();}}
async function readJournal(user){const db=await open();try{return await new Promise((resolve,reject)=>{const r=db.transaction('records').objectStore('records').get(journal(user));r.onsuccess=()=>resolve(r.result?.value);r.onerror=()=>reject(r.error);});}finally{db.close();}}
const same=(a,b)=>a.length===b.length&&a.every((v,i)=>v===b[i]);
const raw=v=>{const a=new Uint8Array(v);if(a.length===33&&a[0]===5)return a.slice(1);if(a.length===32)return a;throw Error('Ключ имеет неверный формат');};
const binary=v=>{if(!(v instanceof ArrayBuffer)&&!ArrayBuffer.isView(v))throw Error('Ключ имеет неверный формат');return v instanceof ArrayBuffer?v:v.buffer.slice(v.byteOffset,v.byteOffset+v.byteLength);};
export async function stageWebBackup({keys,userId,Manager=window.E2EManager}){
 if(!userId||keys?.account_id&&keys.account_id!==userId)throw Error('USER_MISMATCH');
 if(keys?.secretChats||keys?.secret_chats)throw Error('Секретные чаты нельзя восстановить из файла');
 if(!Number.isInteger(keys?.registrationId)||keys.registrationId<1||keys.registrationId>16383||!keys.sessions||Object.keys(keys.sessions).length>10000)throw Error('BAD_FORMAT');
 const records=new Map(),backend={get:async(k,f)=>records.has(k)?records.get(k):f,put:async(k,v)=>records.set(k,v),remove:async k=>records.delete(k),keys:async()=>[...records.keys()]};
 const deviceId=String(2+crypto.getRandomValues(new Uint32Array(1))[0]%2147483645),manager=new Manager();manager.useStore(userId,deviceId,backend);
 const curve=await manager._getCurveWrapper();if(!curve?.keyPair)throw Error('Проверка ключей недоступна');
 const validatePair=pair=>{const privateKey=binary(pair?.privKey);if(privateKey.byteLength!==32||!same(raw(curve.keyPair(privateKey).pubKey),raw(binary(pair.pubKey))))throw Error('Ключ не соответствует закрытой части');};
 validatePair(keys.identityKey);
 for(const[k,v]of Object.entries(keys.sessions)){if(!k.startsWith('session')||typeof v!=='string'||v.length>1024*1024)throw Error('Неверный формат сеанса');}
 for(const[k,v]of Object.entries(keys.peerIdentities||{})){if(!k.startsWith('identityKey')||k==='identityKey')throw Error('Неверный ключ собеседника');raw(binary(v));}
 for(const [name,collection,max]of[['preKeys',keys.preKeys,10000],['signedPreKeys',keys.signedPreKeys,100]]){
  if(!collection||Object.keys(collection).length>max)throw Error('BAD_FORMAT');
  for(const[id,pair]of Object.entries(collection)){if(!/^\d+$/.test(id)||Number(id)>0xffffff)throw Error('BAD_FORMAT');validatePair(pair);if(name==='signedPreKeys'){const value=pair.signature??keys.meta?.signedPreKeySignature,signature=typeof value==='string'?base64ToBytes(value).buffer:binary(value);if(!await manager.verifySignedPreKey(binary(keys.identityKey.pubKey),binary(pair.pubKey),signature))throw Error('Ключ подписи повреждён');}}
 }
 if(!keys.signedPreKeys[1])throw Error('Ключ signedPreKey 1 отсутствует');
 const signature=keys.signedPreKeys[1].signature??keys.meta?.signedPreKeySignature;
 const normalized={...keys,meta:{...keys.meta,signedPreKeySignature:typeof signature==='string'?signature:bytesToBase64(signature)}};
 const groups={};for(const[k,v]of Object.entries(keys.groupKeys||{})){if(!k.startsWith('gk:'+userId+':')||k.includes(':plain:'))throw Error('Ключ группы принадлежит другому аккаунту');groups[k]=v;}
 // Native sessions and SQLCipher room/group state travel opaquely through web
 // archives; this client never pretends it can run native Signal ratchets.
 await manager.importKeys(normalized);
 for(const k of [...records.keys()])if(k.startsWith('gk:'))records.delete(k);
 records.set('accountBackupForeign',{native_sessions:keys.native_sessions??null,mobile_key_state:keys.mobile_key_state??null});
 return{userId,deviceId,manager,records,groups,bundle:await manager.getPublicBundle(),warning:keys.native_sessions?'Нативные сессии сохранены в копии; веб создаст собственные сессии при общении.':null};
}
export async function exportWebAccountKeys(manager,userId){
 const keys=await manager.exportKeys();keys.account_id=userId;
 for(const k of await SigStore.keys())if(k.startsWith('gk:'+userId+':')&&!k.includes(':plain:'))keys.groupKeys[k]=await SigStore.get(k);
 const foreign=await manager.store.get('accountBackupForeign');if(foreign){keys.native_sessions=foreign.native_sessions;keys.mobile_key_state=foreign.mobile_key_state;}
 return keys;
}
export async function restoreWebBackup({stage,user,request,storage=localStorage}){
 if(stage.userId!==user?.id)throw Error('USER_MISMATCH');
 if(await readJournal(user.id))throw Error('Сначала завершите предыдущую попытку восстановления');
 let restoreId=null,committed=false;
 try{
  const preflight=await request('/me/account/backup/preflight',{method:'POST',body:{device_id:stage.deviceId,identity_key:stage.bundle.identityKey}});restoreId=preflight.restore_id;
  const marker={phase:'prepared',deviceId:stage.deviceId,restoreId};await transaction(user.id,store=>store.put({id:journal(user.id),value:marker}));
  const proof=await createWebDeviceProof(request,stage.manager,stage.deviceId,stage.bundle.identityKey),b=stage.bundle;
  await request('/e2e/keys',{method:'POST',body:{device_id:stage.deviceId,identity_key:b.identityKey,registration_id:b.registrationId,signed_pre_key:{key_id:b.signedPreKey.keyId,public_key:b.signedPreKey.publicKey,signature:b.signedPreKey.signature},pre_keys:b.preKeys.map(k=>({key_id:k.keyId,public_key:k.publicKey})),device_proof:proof}});
  await transaction(user.id,store=>{for(const p of[accountPrefix(user.id),groupPrefix(user.id)])store.delete(IDBKeyRange.bound(p,p+'\uffff'));store.delete('sig:backup:'+user.id);for(const[k,value]of stage.records)store.put({id:accountPrefix(user.id)+k,value});for(const[k,value]of Object.entries(stage.groups))store.put({id:'sig:'+k,value});store.put({id:journal(user.id),value:{...marker,phase:'committed'}});});committed=true;
  storage.setItem('e2e_device_id_'+user.id,stage.deviceId);
  await request('/me/account/backup/finalize',{method:'POST',body:{}});await transaction(user.id,store=>store.delete(journal(user.id)));
  return{warning:stage.warning};
 }catch(error){
  if(committed)return{warning:'Ключи восстановлены. Завершение синхронизации повторится при следующем подключении.'};
  // Abort only the new restore device/session. No local key writes occurred.
  if(restoreId){try{await request('/me/account/backup/abort',{method:'POST',body:{restore_id:restoreId}});await transaction(user.id,store=>store.delete(journal(user.id)));}catch{throw Error('Прежние ключи сохранены. Войдите снова для отзыва незавершённого сеанса.');}}
  throw error;
 }
}
export async function recoverWebBackup({user,request,storage=localStorage}){
 if(!user?.id)return;const p=await readJournal(user.id);if(!p)return;
 if(p.phase==='committed'){
  storage.setItem('e2e_device_id_'+user.id,p.deviceId);
  try{await request('/me/account/backup/finalize',{method:'POST',body:{}});}catch{return;}
 }else{
  await request('/me/account/backup/abort',{method:'POST',body:{restore_id:p.restoreId}});
  await transaction(user.id,store=>store.delete(journal(user.id)));
  throw Error('Незавершённое восстановление отменено. Прежние ключи сохранены. Войдите снова.');
 }
 await transaction(user.id,store=>store.delete(journal(user.id)));
}
