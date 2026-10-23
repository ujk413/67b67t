import {test} from 'node:test';
import assert from 'node:assert/strict';
const policy=await import('../web/quality-policy.js').catch(()=>({}));
const i18n=await import('../web/quality-i18n.js').catch(()=>({}));
test('auto download follows media type and actual network with cellular fail-closed',()=>{assert.equal(typeof policy.shouldAutoDownload,'function');const rule={wifi:{image:true,video:false,voice:true,file:false},mobile:{image:false,video:false,voice:false,file:false}};assert.equal(policy.shouldAutoDownload(rule,'image','wifi'),true);assert.equal(policy.shouldAutoDownload(rule,'image','cellular'),false);assert.equal(policy.shouldAutoDownload(rule,'video','wifi'),false);assert.equal(policy.shouldAutoDownload(rule,'image','unknown'),false);});
test('client quiet-hour decision matches midnight crossing without clock sharing',()=>{const quiet={enabled:true,start:'22:30',end:'07:00',time_zone:'Europe/Moscow'};assert.equal(policy.isQuietHours(quiet,new Date('2026-10-23T20:00:00Z')),true);assert.equal(policy.isQuietHours(quiet,new Date('2026-10-23T10:00:00Z')),false);});
test('crash preview drops arbitrary strings and material even before sending',()=>{assert.equal(typeof policy.crashPreview,'function');assert.deepEqual(policy.crashPreview({platform:'web',code:'render_failure',component:'chat',app_version:'1.2.3',at:'2026-10-23T10:00:00Z',token:'secret',message:'private text'}),{platform:'web',code:'render_failure',component:'chat',app_version:'1.2.3',at:'2026-10-23T10:00:00Z'});});
test('RU and EN dictionaries cover the same accessible setting labels',()=>{assert.equal(typeof i18n.t,'function');for(const key of ['quality.title','quality.quiet','quality.language','quality.download','quality.wallpaper','quality.crash','quality.save','quality.cache']){assert.notEqual(i18n.t('ru',key),key);assert.notEqual(i18n.t('en',key),key);}});
test('clearing media preserves offline shell and unrelated caches',async()=>{
 const removed=[],previous=globalThis.caches;
 globalThis.caches={keys:async()=>['67chat-20261023e','67chat-media-v1','another-app'],open:async()=>({keys:async()=>['file']}),delete:async name=>{removed.push(name);return true;}};
 try{assert.equal(await policy.clearWebMediaCache(),1);assert.deepEqual(removed,['67chat-media-v1']);}finally{globalThis.caches=previous;}
});
