import {test} from 'node:test';import assert from 'node:assert/strict';
import {createMediaObjectCache} from '../web/media-object-cache.js';
test('concurrent media views share one decrypt and decoded URLs are bounded by LRU and byte size',async()=>{
 let loads=0,next=0;const revoked=[],cache=createMediaObjectCache({maxEntries:2,maxBytes:10,createURL:()=>`blob:${++next}`,revokeURL:url=>revoked.push(url)}),load=async()=>{loads++;return{blob:new Blob(['1234']),mime:'image/png'};};
 const [a,b]=await Promise.all([cache.resolve('a',load),cache.resolve('a',load)]);assert.equal(a,b);assert.equal(loads,1);
 await cache.resolve('b',load);cache.get('a');await cache.resolve('c',load);assert.equal(cache.has('b'),false);assert.deepEqual(revoked,['blob:2']);cache.clear();assert.equal(revoked.length,3);
});
test('logout aborts pending downloads and late decryption never recreates an object URL',async()=>{
 let finish,created=0;const cache=createMediaObjectCache({createURL:()=>{created++;return'blob:private';}});let signal;
 const pending=cache.resolve('old-account',s=>{signal=s;return new Promise(resolve=>{finish=resolve;});});cache.clear();assert.equal(signal.aborted,true);finish({blob:new Blob(['secret'])});await assert.rejects(pending,{name:'AbortError'});assert.equal(created,0);assert.equal(cache.has('old-account'),false);
});
