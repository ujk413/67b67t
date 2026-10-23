import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createExpressiveHistoryLoader} from '../web/expressive-event-history.js';
test('shared poll history reads older events beyond the visible window and invalidates on deletion',async()=>{
 const load=createExpressiveHistoryLoader(),rows=Array.from({length:230},(_,i)=>({id:String(i),room_id:'room'}));let calls=0;
 const fetchPage=async(_room,before,limit)=>{calls++;const end=before===null?rows.length:rows.findIndex(row=>row.id===before);return{messages:rows.slice(Math.max(0,end-limit),end),has_older:end>limit};};
 const options={key:'alice:token:room',version:'v1',roomId:'room',fetchPage,decode:async m=>({...m,_decrypted:true})};
 const a=load(options),b=load(options);assert.equal((await a.promise).length,230);assert.equal((await b.promise).length,230);assert.equal(calls,3);
 rows.splice(20,1);await load({...options,version:'v2'}).promise;assert.equal(calls,6);
});
test('cancelled last consumer stops pagination and another account never receives cached results',async()=>{
 const load=createExpressiveHistoryLoader();let release,calls=0;
 const first=load({key:'alice:t:room',version:'v',roomId:'room',fetchPage:async()=>{calls++;await new Promise(resolve=>{release=resolve;});return{messages:[{id:'1',room_id:'room'}],has_older:true};},decode:async m=>m});
 await new Promise(resolve=>setImmediate(resolve));first.cancel();release();await assert.rejects(first.promise,/cancelled/);assert.equal(calls,1);
 const second=load({key:'bob:t:room',version:'v',roomId:'room',fetchPage:async()=>({messages:[],has_older:false}),decode:async m=>m});assert.deepEqual(await second.promise,[]);
});

test('10000 history initial scan then incoming edit/delete/hidden updates never fetch all history again',async()=>{
 const load=createExpressiveHistoryLoader(),rows=Array.from({length:10000},(_,i)=>({id:String(i),room_id:'room',text:'event '+i}));let calls=0;
 const options={key:'large',version:'session',roomId:'room',fetchPage:async(_room,before,limit)=>{calls++;const end=before===null?rows.length:Number(before);return{messages:rows.slice(Math.max(0,end-limit),end),has_older:end>limit};},decode:async m=>({...m,_decrypted:true})};
 assert.equal((await load(options).promise).length,10000);assert.equal(calls,100);
 load.reconcile('large',{upserts:[{id:'10000',room_id:'room',text:'new',_decrypted:true},{id:'2',room_id:'room',text:'edited',_decrypted:true}],removed:['3','4']});
 const updated=await load(options).promise;assert.equal(calls,100);assert.equal(updated.length,9999);assert.equal(updated.find(m=>m.id==='2').text,'edited');assert.ok(!updated.some(m=>['3','4'].includes(m.id)));
});
test('an incoming delete during pagination cannot be resurrected by an older page',async()=>{
 const load=createExpressiveHistoryLoader();let release;
 const job=load({key:'race',version:'v',roomId:'r',fetchPage:async()=>{await new Promise(resolve=>release=resolve);return{messages:[{id:'gone',room_id:'r'},{id:'stay',room_id:'r'}],has_older:false};},decode:async m=>m});
 await new Promise(resolve=>setImmediate(resolve));load.reconcile('race',{removed:['gone']});release();assert.deepEqual((await job.promise).map(m=>m.id),['stay']);
});
test('event cache rejects another room and failed E2E decrypt, with explicit retry',async()=>{
 const load=createExpressiveHistoryLoader(),options={key:'secure',version:'v',roomId:'r',fetchPage:async()=>({messages:[{id:'1',room_id:'r',meta:{encrypted:true}}],has_older:false}),decode:async m=>m};
 await assert.rejects(load(options).promise,/decryption/);assert.equal((await load({...options,decode:async m=>({...m,_decrypted:true})}).promise).length,1);
 assert.throws(()=>load.reconcile('secure',{upserts:[{id:'x',room_id:'else'}]}),/history_wrong_room/);
});
test('event-only retention is bounded and refuses overflow instead of silently returning incomplete results',async()=>{
 const load=createExpressiveHistoryLoader({maxMessages:2});
 await assert.rejects(load({key:'limit',version:'v',roomId:'r',fetchPage:async()=>({messages:[1,2,3].map(id=>({id:String(id),room_id:'r'})),has_older:false}),decode:async m=>m}).promise,/history_cache_limit/);
});


test('incremental materialization never accepts failed E2E updates and notifies consumers to hide stale results',async()=>{
 const load=createExpressiveHistoryLoader(),options={key:'failed-delta',version:'s',roomId:'r',fetchPage:async()=>({messages:[{id:'a',room_id:'r',text:'valid'}],has_older:false}),decode:async m=>m};
 await load(options).promise;let current='old';load.subscribe('failed-delta',(rows,error)=>{current=rows;assert.match(error.message,/decryption/);});
 assert.throws(()=>load.reconcile('failed-delta',{upserts:[{id:'a',room_id:'r',text:'bad',meta:{encrypted:true}}]}),/decryption/);assert.equal(current,null);
 await assert.rejects(load(options).promise,/decryption/);
});

test('a stale visible window cannot resurrect a deleted event or overwrite a newer edit',async()=>{
 const load=createExpressiveHistoryLoader(),options={key:'stale',version:'v',roomId:'r',fetchPage:async()=>({messages:[{id:'a',room_id:'r',text:'original',edited_at:'2026-10-20'}],has_older:false}),decode:async m=>m};
 await load(options).promise;load.reconcile('stale',{upserts:[{id:'a',room_id:'r',text:'newer',edited_at:'2026-10-23'}]});load.reconcile('stale',{upserts:[{id:'a',room_id:'r',text:'stale',edited_at:'2026-10-21'}]});assert.equal((await load(options).promise)[0].text,'newer');
 load.reconcile('stale',{removed:['a']});load.reconcile('stale',{upserts:[{id:'a',room_id:'r',text:'stale'}]});assert.deepEqual(await load(options).promise,[]);
});
