import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import fs from 'node:fs';
import vm from 'node:vm';
const require=createRequire(new URL('../mobile/package.json',import.meta.url));
const ts=require('typescript'), {pbkdf2}=require('@noble/hashes/pbkdf2'),{sha256}=require('@noble/hashes/sha2'),{gcm}=require('@noble/ciphers/aes');
const source=fs.readFileSync(new URL('../mobile/src/roomCrypto.ts',import.meta.url),'utf8');
function load(file, dependencies = {}) {
  dependencies = { 'react-native': { Platform: { OS: 'android', Version: 36 } }, ...dependencies };
  const exports = {};
  vm.runInNewContext(ts.transpileModule(fs.readFileSync(new URL(file,import.meta.url),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText,{
    exports, require: name => name in dependencies ? dependencies[name] : require(name), crypto,btoa,atob,setTimeout,clearTimeout, Uint8Array,
  });
  return exports;
}
const derivation=load('../mobile/src/roomKeyDerivation.ts', {'./nativeRoomKdf':load('../mobile/src/nativeRoomKdf.ts')});
const exports={};vm.runInNewContext(ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText,{exports,require:name=>name==='./roomKeyDerivation'?derivation:require(name),crypto,btoa,atob,setTimeout,clearTimeout});
test('asynchronous key derivation decrypts the existing 250000-iteration format and yields during calculation',async()=>{
  const room='room',secret='server-secret',plain='Сообщение не потеряно';
  const passphrase=Buffer.from(sha256(new TextEncoder().encode(`room-passphrase-v2:${room}:${secret}`))).toString('base64');
  const key=pbkdf2(sha256,new TextEncoder().encode(passphrase),new TextEncoder().encode(`room-v2:${room}:${secret}`),{c:250000,dkLen:32});
  const iv=crypto.getRandomValues(new Uint8Array(12)), encrypted=gcm(key,iv).encrypt(new TextEncoder().encode(plain));
  let yielded=false;setTimeout(()=>{yielded=true},0);
  assert.equal(await exports.decryptRoomText(room,secret,Buffer.from(encrypted).toString('base64'),Buffer.from(iv).toString('base64')),plain);
  assert.ok(yielded);
  const next=await exports.encryptRoomText(room,secret,plain);
  assert.equal(await exports.decryptRoomText(room,secret,next.text,next.meta.iv),plain);
});
test('Hermes fallback matches native PBKDF2 vectors and yields to timers',async()=>{
  const {pbkdf2Sync}=await import('node:crypto');
  for(const iterations of [1,2,4096]) {
    const password=new TextEncoder().encode('пароль'),salt=new TextEncoder().encode('salt');
    const actual=await derivation.portableRoomPbkdf2(password,salt,iterations);
    assert.equal(Buffer.from(actual).toString('hex'),pbkdf2Sync(password,salt,iterations,32,'sha256').toString('hex'));
  }
  let tick=false;setTimeout(()=>{tick=true},0);
  await derivation.portableRoomPbkdf2(new TextEncoder().encode('password'),new TextEncoder().encode('salt'),20000);
  assert.ok(tick);
});

test('Android bridge uses the native worker and preserves UTF-8 salt and key bytes',async()=>{
  const {pbkdf2Sync}=await import('node:crypto');
  let calls=0;
  const native=load('../mobile/src/nativeRoomKdf.native.ts', {expo:{requireOptionalNativeModule: name => {
    assert.equal(name,'RoomKey');
    return {deriveAsync: async (password, salt) => {
      calls++;
      return pbkdf2Sync(password,Buffer.from(salt,'base64'),250000,32,'sha256').toString('base64');
    }};
  }}});
  const android=load('../mobile/src/roomKeyDerivation.ts', {'./nativeRoomKdf':native});
  const password=new TextEncoder().encode('cGFzc3BocmFzZQ=='),salt=new TextEncoder().encode('room-v2:комната:secret');
  const key=await android.deriveRoomPbkdf2(password,salt);
  assert.equal(Buffer.from(key).toString('hex'),pbkdf2Sync(password,salt,250000,32,'sha256').toString('hex'));
  assert.equal(calls,1);
});

test('Expo Go can fall back, while an actual native failure is reported instead of retrying heavy JS work',async()=>{
  const missing=load('../mobile/src/nativeRoomKdf.native.ts',{expo:{requireOptionalNativeModule:()=>null}});
  assert.equal(missing.nativeRoomKdf(new Uint8Array(),new Uint8Array()),null);
  const oldAndroid=load('../mobile/src/nativeRoomKdf.native.ts',{
    'react-native':{Platform:{OS:'android',Version:25}},
    expo:{requireOptionalNativeModule:()=>({deriveAsync:()=>{throw new Error('unsupported native algorithm must not be called')}})},
  });
  assert.equal(oldAndroid.nativeRoomKdf(new Uint8Array(),new Uint8Array()),null);
  const failure=load('../mobile/src/nativeRoomKdf.native.ts',{expo:{requireOptionalNativeModule:()=>({deriveAsync:async()=>{throw new Error('native failure')}})}});
  await assert.rejects(failure.nativeRoomKdf(new TextEncoder().encode('password'),new Uint8Array()),/native failure/);
});
