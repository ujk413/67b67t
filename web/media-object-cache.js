/** Bound decoded Blob URLs and share concurrent loads; clearing invalidates late work. */
export function createMediaObjectCache({maxEntries=40,maxBytes=128*1024*1024,createURL=blob=>URL.createObjectURL(blob),revokeURL=url=>URL.revokeObjectURL(url)}={}){
 const values=new Map(),pending=new Map();let generation=0,bytes=0;
 const get=key=>{const value=values.get(key);if(value){values.delete(key);values.set(key,value);}return value;};
 const remove=key=>{const value=values.get(key);if(value){bytes-=value.bytes;values.delete(key);revokeURL(value.url);}};
 return{
  get,has:key=>values.has(key),values:()=>values.values(),
  clear(){generation++;for(const task of pending.values())task.controller.abort();pending.clear();for(const key of [...values.keys()])remove(key);},
  resolve(key,load){
   const hit=get(key);if(hit)return Promise.resolve(hit);if(pending.has(key))return pending.get(key).promise;
   const mine=generation,controller=new AbortController();let promise;
   promise=(async()=>{
    const {blob,...details}=await load(controller.signal);
    if(mine!==generation||controller.signal.aborted)throw Object.assign(Error('Загрузка отменена: сеанс завершён'),{name:'AbortError'});
    if(blob.size>maxBytes)throw Error('Вложение слишком велико для просмотра');
    const value={...details,url:createURL(blob),bytes:blob.size};values.set(key,value);bytes+=blob.size;
    while(values.size>maxEntries||bytes>maxBytes)remove(values.keys().next().value);
    return value;
   })().finally(()=>{if(pending.get(key)?.promise===promise)pending.delete(key);});
   pending.set(key,{controller,promise});return promise;
  }
 };
}
