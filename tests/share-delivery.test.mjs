import {test} from 'node:test';
import assert from 'node:assert/strict';
import {deliverShare,confirmShareDelivery} from '../web/share-delivery.js';
const job=(id='share-1')=>({id,roomId:'room',recipientId:'alice',text:'hello',uris:['one','two']});
function disk(){let json='{}';return{read:id=>JSON.parse(json)[id]||null,save:async(id,state)=>{json=JSON.stringify({...JSON.parse(json),[id]:state});}};}
test('unconfirmed external share never invokes the outbox',async()=>{let calls=0;await assert.rejects(deliverShare(job(),async()=>calls++,disk()),/share_confirmation_required/);assert.equal(calls,0);});
test('process restart after partial share preserves original progress and IDs',async()=>{
 const j=job(),store=disk(),calls=[];await confirmShareDelivery(j,store);
 await assert.rejects(deliverShare(j,async(item,id)=>{calls.push([item.value,id]);if(item.value==='two')throw Error('offline');},store),/offline/);
 const {deliverShare:restarted}=await import('../web/share-delivery.js?restart='+Date.now());
 await restarted(j,async(item,id)=>calls.push([item.value,id]),store);
 assert.deepEqual(calls.map(x=>x[0]),['hello','one','two','two']);assert.equal(calls[2][1],calls[3][1]);assert.equal(store.read(j.id).next,3);
});
test('account or room changes cannot retarget a confirmed or partly delivered share',async()=>{
 const j=job('share-2'),store=disk();await confirmShareDelivery(j,store);
 await assert.rejects(deliverShare({...j,recipientId:'bob'},async()=>{},store),/share_target_changed/);
 await assert.rejects(confirmShareDelivery({...j,roomId:'elsewhere'},store),/share_target_changed/);
});
test('journal advances only after durable outbox registration and blocks concurrent retries',async()=>{
 const j=job('share-3'),store=disk();await confirmShareDelivery(j,store);let release;const calls=[];
 const first=deliverShare(j,async(item,id)=>{calls.push(id);if(item.value==='hello')await new Promise(resolve=>release=resolve);},store);
 await new Promise(resolve=>setImmediate(resolve));assert.equal(store.read(j.id).next,0);
 await assert.rejects(deliverShare(j,async()=>{},store),/share_delivery_busy/);release();await first;assert.equal(new Set(calls).size,3);
});
