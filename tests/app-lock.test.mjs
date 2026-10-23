import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),ts=require('../mobile/node_modules/typescript');
test('a phone with a device PIN can enable the lock without enrolled biometrics',async()=>{
  const exp={},values=new Map();
  const storage={get:async key=>values.get(key),set:async(k,v)=>values.set(k,v)};
  const native={hasHardwareAsync:async()=>false,isEnrolledAsync:async()=>false,supportedAuthenticationTypesAsync:async()=>[],getEnrolledLevelAsync:async()=>1,SecurityLevel:{NONE:0,SECRET:1},authenticateAsync:async()=>({success:false})};
  const code=ts.transpileModule(fs.readFileSync(new URL('../mobile/src/appLock.ts',import.meta.url),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
  new Function('exports','require',code)(exp,name=>name==='react-native'?{Platform:{OS:'android'}}:name==='expo-local-authentication'?native:{storage,STORAGE_KEYS:{}});
  assert.equal((await exp.describeLock()).available,true);
  assert.equal(await exp.unlockApp(),false);
  await exp.setLockPreference(true);assert.equal((await exp.describeLock()).enabled,true);
});
