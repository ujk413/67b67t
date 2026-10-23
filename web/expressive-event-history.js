/** Decrypted materialization: one authorized initial scan, then normal receive/sync deltas.
 * Never infer deletions from a bounded visible window. Session keys include the credential.
 * Retain only event rows in integrations; limits fail explicitly instead of truncating votes. */
export function createExpressiveHistoryLoader({maxMessages=20000,maxBytes=16*1024*1024,maxRooms=6}={}){
 const entries=new Map(),watchers=new Map();
 const emit=(key,entry)=>{if(entry.ready)for(const fn of watchers.get(key)||[])fn(entry.error?null:[...entry.rows.values()],entry.error);};
 function check(entry){if(entry.rows.size>maxMessages||entry.bytes>maxBytes)throw Error('history_cache_limit');}
 function put(entry,row){
  if(row.room_id!==entry.roomId||typeof row.id!=='string')throw Error('history_wrong_room');
  if((row.encrypted_content||row.meta?.encrypted||row.meta?.e2e_group)&&row._decrypted!==true)throw Error('history_decryption_failed');
  const previous=entry.rows.get(row.id);
  if(entry.removed.has(row.id)||previous?.edited_at&&(!row.edited_at||Date.parse(previous.edited_at)>Date.parse(row.edited_at)))return;
  if(previous)entry.bytes-=JSON.stringify(previous).length*2;
  if(row.hidden||row.deleted||row.deleted_at||!entry.retain(row)){entry.rows.delete(row.id);return;}
  entry.rows.set(row.id,row);entry.bytes+=JSON.stringify(row).length*2;check(entry);
 }
 function load({key,version,roomId,fetchPage,decode,onProgress,retain=()=>true,retry=false}){
  let entry=entries.get(key);const listener={cancelled:false,onProgress};
  if(!entry||entry.version!==version||entry.error&&retry){
   if(entry){entry.disposed=true;for(const subscriber of entry.listeners)subscriber.cancelled=true;}
   entry={version,roomId,retain,rows:new Map(),removed:new Set(),overrides:new Map(),bytes:0,listeners:new Set([listener]),ready:false,disposed:false};
   const own=entry;entries.delete(key);entries.set(key,own);
   const cancelled=()=>own.disposed||(!own.ready&&![...own.listeners].some(item=>!item.cancelled));
   own.promise=(async()=>{
    let before=null,count=0;
    for(let page=0;page<10000;page++){
     if(cancelled())throw Error('history_cancelled');
     const response=await fetchPage(roomId,before,100),rows=response?.messages;
     if(cancelled())throw Error('history_cancelled');
     if(!Array.isArray(rows)||typeof response.has_older!=='boolean'||rows.length>100)throw Error('history_bad_page');
     for(const row of rows){
      if(cancelled())throw Error('history_cancelled');
      if(row.room_id!==roomId||typeof row.id!=='string')throw Error('history_wrong_room');
      const message=await decode(row);
      if(!message||((row.encrypted_content||row.meta?.encrypted||row.meta?.e2e_group)&&message._decrypted!==true))throw Error('history_decryption_failed');
      if(!own.overrides.has(row.id))put(own,message);
     }
     count+=rows.length;for(const subscriber of own.listeners)if(!subscriber.cancelled)subscriber.onProgress?.(count);
     if(!response.has_older){own.ready=true;own.overrides.clear();return;}
     const next=rows[0]?.id;if(!next||next===before)throw Error('history_pagination_stalled');before=next;
    }
    throw Error('history_too_large');
   })().catch(error=>{if(entries.get(key)===own)entries.delete(key);throw error;});
   while(entries.size>maxRooms){const oldest=entries.keys().next().value;const evicted=entries.get(oldest);evicted.disposed=true;entries.delete(oldest);}
  }else {entry.listeners.add(listener);entries.delete(key);entries.set(key,entry);}
  const current=entry;
  return{promise:current.promise.then(()=>{if(listener.cancelled||current.disposed)throw Error('history_cancelled');if(current.error)throw current.error;return[...current.rows.values()];}).finally(()=>current.listeners.delete(listener)),cancel(){listener.cancelled=true;current.listeners.delete(listener);}};
 }
 load.reconcile=(key,{upserts=[],removed=[]}={})=>{
  const entry=entries.get(key);if(!entry)return;
  try{
   for(const id of removed){const previous=entry.rows.get(id);if(previous)entry.bytes-=JSON.stringify(previous).length*2;entry.rows.delete(id);entry.removed.add(id);if(entry.removed.size>maxMessages)throw Error('history_cache_limit');if(!entry.ready)entry.overrides.set(id,null);}
   for(const message of upserts){put(entry,message);if(!entry.ready)entry.overrides.set(message.id,message);}
   if(entry.overrides.size>maxMessages)throw Error('history_cache_limit');emit(key,entry);
  }catch(error){entry.error=error;entry.rows.clear();entry.bytes=0;emit(key,entry);throw error;}
 };
 load.removeMessages=(prefix,removed)=>{for(const key of entries.keys())if(key.startsWith(prefix))load.reconcile(key,{removed});};
 load.has=key=>entries.has(key);
 load.subscribe=(key,fn)=>{let set=watchers.get(key);if(!set){set=new Set();watchers.set(key,set);}set.add(fn);return()=>{set.delete(fn);if(!set.size)watchers.delete(key);};};
 load.clear=prefix=>{for(const [key,entry] of entries)if(!prefix||key.startsWith(prefix)){entry.disposed=true;for(const listener of entry.listeners)listener.cancelled=true;entries.delete(key);for(const fn of watchers.get(key)||[])fn(null,Error('history_cancelled'));}};
 load.prune=(prefix,rooms)=>{for(const [key,entry] of entries)if(key.startsWith(prefix)&&!rooms.includes(entry.roomId))load.clear(key);};
 return load;
}
export const expressiveHistoryLoader=createExpressiveHistoryLoader();
export const expressiveHistoryPrefix=(user,base,session)=>JSON.stringify([user,base,session])+':';
export const expressiveHistoryKey=(user,base,session,room)=>expressiveHistoryPrefix(user,base,session)+room;
