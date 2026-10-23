import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),ts=require('../mobile/node_modules/typescript');
function harness(overrides={}) {
  const slots=[], effects=[], listeners=[]; let index=0;
  const calls={checks:0,downloads:0,installs:0};
  const release={version:'1.3.3',versionCode:14,size:10,mandatory:false};
  const native={currentVersionCode:()=>12,checkForUpdate:async()=>{calls.checks++;return {status:'available',release,url:'https://example.test/api/app/android.apk'};},discardDownload:async()=>{},downloadRelease:async()=>{calls.downloads++;return 'file:///cache/update.apk';},downloadedSize:async()=>10,installRelease:async()=>{calls.installs++;return {resultCode:0};},openInstallPermissionSettings:async()=>{},...overrides};
  const react={useState(v){const i=index++;if(!(i in slots))slots[i]=typeof v==='function'?v():v;return [slots[i],v=>slots[i]=typeof v==='function'?v(slots[i]):v];},useRef(v){const i=index++;return slots[i]??={current:v};},useCallback:f=>f,useEffect:f=>effects.push(f)};
  const exp={};const code=ts.transpileModule(fs.readFileSync(new URL('../mobile/src/hooks/useAppUpdate.ts',import.meta.url),'utf8'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
  new Function('exports','require',code)(exp,name=>name==='react'?react:name==='react-native'?{Platform:{OS:'android'},AppState:{addEventListener:(_e,f)=>{listeners.push(f);return{remove(){}};}}}:native);
  const render=()=>{index=0;effects.length=0;return exp.useAppUpdate('https://example.test/api',true);};
  return {render,calls,native,release,mount(){render();for(const f of effects)f();},foreground:async()=>{for(const f of listeners)f('active');await new Promise(r=>setImmediate(r));}};
}
test('a failed APK download can be retried and dismissed',async()=>{
  let fail=true;const h=harness({downloadRelease:async()=>{if(fail)throw Error('offline');return 'file:///cache/update.apk';}});
  await h.render().recheck();await h.render().start();assert.equal(h.render().state.phase,'error');
  fail=false;await h.render().retryInstall();assert.equal(h.calls.installs,1);
  h.render().dismiss();assert.equal(h.render().visible,false);
});
test('permission failure retains the downloaded APK for retry without downloading it again',async()=>{
  let denied=true;const h=harness({installRelease:async()=>{h.calls.installs++;if(denied)throw Error('permission denied');return {resultCode:0};}});
  await h.render().recheck();await h.render().start();assert.equal(h.render().state.needsPermission,true);
  denied=false;await h.render().retryInstall();assert.equal(h.calls.installs,2);assert.equal(h.calls.downloads,1);
});
test('return from a cancelled installer restores an actionable state',async()=>{
  const h=harness();await h.render().recheck();await h.render().start();
  assert.equal(h.render().state.phase,'ready');
  h.render().dismiss();assert.equal(h.render().visible,false);
});
test('dismissing during asynchronous APK size validation never opens the installer',async()=>{
  let resolveSize,started;const validating=new Promise(r=>started=r);
  const h=harness({downloadedSize:()=>{started();return new Promise(r=>resolveSize=r);}});
  await h.render().recheck();const task=h.render().start();await validating;
  h.render().dismiss();resolveSize(10);await task;
  assert.equal(h.calls.installs,0);assert.equal(h.render().state.phase,'idle');
});
test('a failed initial release check is retried on foreground rather than throttled for an hour',async()=>{
  const h=harness({checkForUpdate:async()=>{h.calls.checks++;return {status:'unavailable',current:12};}});
  h.mount();await new Promise(r=>setImmediate(r));await h.foreground();assert.equal(h.calls.checks,2);
});
